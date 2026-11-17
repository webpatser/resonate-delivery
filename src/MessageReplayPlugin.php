<?php

namespace Webpatser\ResonateDelivery;

use Fledge\Async\Redis\RedisClient;
use Fledge\Async\Redis\RedisConfig;
use Throwable;
use Webpatser\Resonate\Contracts\Connection;
use Webpatser\Resonate\Loggers\Log;
use Webpatser\Resonate\Plugins\Contracts\ConnectionLifecycle;
use Webpatser\Resonate\Plugins\Contracts\MessageInterceptor;
use Webpatser\Resonate\Plugins\Contracts\ServerPlugin;
use Webpatser\Resonate\Plugins\MessageDisposition;
use Webpatser\Resonate\Plugins\PluginContext;
use Webpatser\Resonate\Protocols\Pusher\Channels\Channel;
use Webpatser\ResonateDelivery\Broadcasting\ReplayBroadcaster;

use function Fledge\Async\Redis\createRedisClient;

/**
 * The server-side half of resonate-delivery.
 *
 * Two phases, and which phase does the Redis work is the whole design:
 *
 *  - `onMessage(pusher:subscribe)` notices the optional `last_event_id` field
 *    in the subscribe data and remembers it on the connection. It performs no
 *    I/O: core has not authorized the subscribe yet, so nothing it does may
 *    cost Redis a round trip or hold more than a few bytes. It always returns
 *    `Relay` so the standard subscribe flow runs untouched.
 *
 *  - `onSubscribe(Connection, Channel)` fires after core authorized the
 *    subscribe, joined the connection and sent `subscription_succeeded`. It
 *    takes the remembered cursor, reads every missed message from the
 *    channel's Redis Stream and writes it to the connection page by page.
 *
 * ## Why reading in onSubscribe keeps the ordering
 *
 * The ordering guarantee is that replayed messages arrive after
 * `subscription_succeeded` and before any live broadcast. From the moment
 * core (Resonate 0.7.1+) adds the connection to the channel, it holds every
 * live frame for that connection until `subscription_succeeded` is out and
 * every plugin's `onSubscribe` has returned, then delivers the held frames in
 * arrival order. That holds however long `onSubscribe` suspends, so the
 * `XRANGE` round trips done here can never let a live broadcast overtake the
 * replay. (Before 0.7.1 they could, which is why earlier releases read the
 * window from the interceptor, ahead of authorization.)
 *
 * Nothing is lost at the seam either. The connection joins the channel before
 * the first read, so every broadcast from then on is held and delivered after
 * the replay, and everything published before the join is already in the
 * stream the read walks. A message published between the join and the final
 * read arrives twice (once replayed, once live, the live copy second); that is
 * the documented duplicate, and the client deduplicates on `_replay_id`.
 *
 * Connections that subscribe without `last_event_id` get no replay (the
 * normal Pusher experience), and neither does a connection already subscribed
 * to the channel, because a live subscriber has missed nothing. A remembered
 * cursor is cleared when it is used, on `pusher:unsubscribe`, on a
 * `pusher:subscribe` without `last_event_id`, in `onUnsubscribe` and on
 * connection close. Cursors left by subscribes core rejected are bounded by
 * `replay_max_pending`, oldest dropped first. Replays per connection are
 * bounded by a budget per window (`replay_budget_*`), and `private-encrypted-`
 * channels are never replayed.
 */
class MessageReplayPlugin implements ConnectionLifecycle, MessageInterceptor, ServerPlugin
{
    /**
     * The connection state key holding the pending replay cursor per channel.
     */
    protected const string CURSORS = 'delivery.cursors';

    /**
     * The connection state key holding the replay budget of the current window.
     */
    protected const string BUDGET = 'delivery.budget';

    /**
     * The shape of a Redis Stream id a client may send as `last_event_id`.
     */
    protected const string CURSOR_PATTERN = '/^\d{1,20}(?:-\d{1,20})?$/';

    /**
     * The server API surface handed in at boot.
     */
    protected PluginContext $context;

    /**
     * The async Redis client used for `XRANGE` reads.
     */
    protected ?RedisClient $redis = null;

    /**
     * The shared key schema.
     */
    protected DeliveryKeys $keys;

    /**
     * The field name carrying the replay id inside `data`.
     */
    protected string $replayIdField;

    /**
     * Batch size for paginated `XRANGE` reads.
     */
    protected int $batchSize;

    /**
     * The most messages one replay may send. 0 removes the limit.
     */
    protected int $maxMessages;

    /**
     * The most cursors one connection may hold for subscribes not yet confirmed. 0 removes the limit.
     */
    protected int $maxPending;

    /**
     * The length of one replay budget window, in seconds.
     */
    protected int $budgetWindow;

    /**
     * The most replays one connection may start per window. 0 removes the limit.
     */
    protected int $budgetReplays;

    /**
     * The most replayed messages one connection may receive per window. 0 removes the limit.
     */
    protected int $budgetMessages;

    /**
     * Boot the plugin: open the Redis client and resolve config.
     */
    public function boot(PluginContext $context): void
    {
        $this->context = $context;

        $config = config('resonate-delivery', []);

        $this->keys = new DeliveryKeys($config['key_prefix'] ?? 'delivery');
        $this->replayIdField = (string) ($config['replay_id_field'] ?? '_replay_id');
        $this->batchSize = max(1, (int) ($config['replay_batch_size'] ?? 100));
        $this->maxMessages = (int) ($config['replay_max_messages'] ?? 10000);
        $this->maxPending = (int) ($config['replay_max_pending'] ?? 100);
        $this->budgetWindow = max(1, (int) ($config['replay_budget_window'] ?? 60));
        $this->budgetReplays = (int) ($config['replay_budget_replays'] ?? 100);
        $this->budgetMessages = (int) ($config['replay_budget_messages'] ?? 10000);
        $this->redis = createRedisClient($this->makeConfig($config['connection'] ?? []));
    }

    /**
     * Remember the cursor of a subscribe carrying `last_event_id`; relay everything.
     *
     * Core has not authorized the subscribe at this point, so this only
     * records the cursor. The read waits for `onSubscribe`, which core calls
     * for an authorized subscribe only.
     *
     * A `pusher:unsubscribe` drops the channel's cursor here as well. Core
     * only calls `onUnsubscribe` for a subscription the plugins were told
     * about, so an unsubscribe that overtakes its own subscribe would
     * otherwise leave the cursor behind for a later subscribe without
     * `last_event_id` to replay.
     *
     * @param  array{event?:mixed,data?:mixed}  $event
     */
    public function onMessage(Connection $from, array $event): MessageDisposition
    {
        $type = $event['event'] ?? null;

        if ($type !== 'pusher:subscribe' && $type !== 'pusher:unsubscribe') {
            return MessageDisposition::Relay;
        }

        $data = (array) ($event['data'] ?? []);
        $channel = (string) ($data['channel'] ?? '');

        if ($channel === '') {
            return MessageDisposition::Relay;
        }

        if ($type === 'pusher:unsubscribe') {
            $this->forget($from, $channel);

            return MessageDisposition::Relay;
        }

        $lastId = $data['last_event_id'] ?? null;

        // A subscribe without a usable cursor asks for no replay. Any cursor
        // still held for the channel belongs to an earlier subscribe that core
        // rejected (bad auth, subscription limit) and must not be used.
        if (! is_string($lastId) || preg_match(self::CURSOR_PATTERN, $lastId) !== 1) {
            $this->forget($from, $channel);

            return MessageDisposition::Relay;
        }

        // A connection already in the channel is receiving live broadcasts, so
        // it has missed nothing and a replay would only reorder its stream.
        // Encrypted channels are live-only and never replayed, even from a
        // stream an older release may have written for them.
        if ($this->redis === null
            || str_starts_with($channel, ReplayBroadcaster::ENCRYPTED_PREFIX)
            || $this->subscribedAlready($from, $channel)) {
            $this->forget($from, $channel);

            return MessageDisposition::Relay;
        }

        $this->remember($from, $channel, $lastId);

        return MessageDisposition::Relay;
    }

    /**
     * Replay the missed window for a channel the connection just joined.
     *
     * Core calls this only after it authorized the subscribe, and (0.7.1+)
     * holds live frames for the connection from the join until every
     * plugin's `onSubscribe` has returned, so the replay written here always
     * precedes the held and live frames, however long the reads take. A live
     * frame that overlaps the replay arrives after it and is deduplicated on
     * `_replay_id`.
     */
    public function onSubscribe(Connection $connection, Channel $channel): void
    {
        $name = $channel->name();
        $cursors = $this->cursors($connection);

        if (! array_key_exists($name, $cursors)) {
            return;
        }

        $lastId = $cursors[$name];

        // One-shot: drop the cursor before any I/O so neither a later
        // resubscribe without a new cursor nor a subscribe of the same socket
        // that runs while this one waits on Redis can replay it again.
        unset($cursors[$name]);
        $connection->setState(self::CURSORS, $cursors);

        if ($this->redis === null || ! $this->admitReplay($connection, $name)) {
            return;
        }

        $this->replay($connection, $channel, $lastId, $this->redis);
    }

    /**
     * Drop any pending cursor for a channel a connection leaves explicitly.
     */
    public function onUnsubscribe(Connection $connection, Channel $channel): void
    {
        $this->forget($connection, $channel->name());
    }

    /**
     * Handle a connection opening. No replay state to set up yet.
     */
    public function onOpen(Connection $connection): void
    {
        //
    }

    /**
     * Drop every pending cursor on close.
     */
    public function onClose(Connection $connection): void
    {
        $connection->forgetState(self::CURSORS);
        $connection->forgetState(self::BUDGET);
    }

    /**
     * Send every message newer than the cursor to the connection, page by page.
     *
     * A read failure stops the replay where it is rather than throwing: a
     * short replay degrades the way an out-of-retention cursor already does.
     * Each page is sent as soon as it is read, so a replay holds at most one
     * page in memory. A connection that left the channel while a page was
     * being read (it closed) is not read for again.
     */
    protected function replay(Connection $connection, Channel $channel, string $lastId, RedisClient $redis): void
    {
        $name = $channel->name();
        $key = $this->keys->streamKey($connection->app()->id(), $name);
        $cursor = '('.$lastId;
        $sent = 0;

        while (true) {
            try {
                $raw = $redis->execute('XRANGE', $key, $cursor, '+', 'COUNT', (string) $this->batchSize);
            } catch (Throwable $e) {
                Log::error('Delivery replay read failed on '.$name.': '.$e->getMessage());

                return;
            }

            if (! is_array($raw) || $raw === []) {
                return;
            }

            foreach ($raw as $entry) {
                $decoded = is_array($entry) ? $this->decodeEntry($entry) : null;

                if ($decoded === null) {
                    continue;
                }

                // Charged per message, so replays of the same connection that
                // run concurrently share one budget.
                if (! $this->chargeMessage($connection, $name)) {
                    return;
                }

                $augmented = $decoded['data'];
                $augmented[$this->replayIdField] = $decoded['id'];

                $connection->send(json_encode([
                    'event' => $decoded['event'],
                    'channel' => $name,
                    'data' => $augmented,
                ], JSON_THROW_ON_ERROR));

                $cursor = '('.$decoded['id'];

                // With the default retention the stream's MAXLEN (about 1000
                // entries) ends a replay well before this cap (default 10000),
                // so reaching it means retention is set above the cap.
                if ($this->maxMessages > 0 && ++$sent >= $this->maxMessages) {
                    Log::error('Delivery replay on '.$name.' truncated at '.$this->maxMessages.' messages.');

                    return;
                }
            }

            // Less than a full batch means we caught up. A connection that
            // closed while that page was read is owed nothing more.
            if (count($raw) < $this->batchSize || ! $channel->subscribed($connection)) {
                return;
            }
        }
    }

    /**
     * Count one replay against the connection's budget; false when it is spent.
     *
     * Without a budget a client could loop subscribe(`last_event_id` = 0) and
     * unsubscribe to pull the whole stream again and again. Over budget the
     * subscribe still succeeds, it just gets no replay (the plain Pusher
     * experience).
     */
    protected function admitReplay(Connection $connection, string $channel): bool
    {
        if ($this->budgetReplays <= 0 && $this->budgetMessages <= 0) {
            return true;
        }

        $budget = $this->budget($connection);

        if (($this->budgetReplays > 0 && $budget['replays'] >= $this->budgetReplays)
            || ($this->budgetMessages > 0 && $budget['messages'] >= $this->budgetMessages)) {
            $this->exhausted($connection, $budget, $channel);

            return false;
        }

        $budget['replays']++;
        $connection->setState(self::BUDGET, $budget);

        return true;
    }

    /**
     * Count one replayed message against the connection's budget; false when it is spent.
     */
    protected function chargeMessage(Connection $connection, string $channel): bool
    {
        if ($this->budgetMessages <= 0) {
            return true;
        }

        $budget = $this->budget($connection);

        if ($budget['messages'] >= $this->budgetMessages) {
            $this->exhausted($connection, $budget, $channel);

            return false;
        }

        $budget['messages']++;
        $connection->setState(self::BUDGET, $budget);

        return true;
    }

    /**
     * Log a spent budget once per window, so a looping client cannot flood the log.
     *
     * @param  array{start: float, replays: int, messages: int, warned: bool}  $budget
     */
    protected function exhausted(Connection $connection, array $budget, string $channel): void
    {
        if ($budget['warned']) {
            return;
        }

        $budget['warned'] = true;
        $connection->setState(self::BUDGET, $budget);

        Log::error('Delivery replay budget spent for connection '.$connection->id().' on '.$channel.'; replays skipped or truncated until the window ends.');
    }

    /**
     * The connection's replay budget for the current window, reset once the window elapsed.
     *
     * @return array{start: float, replays: int, messages: int, warned: bool}
     */
    protected function budget(Connection $connection): array
    {
        $now = hrtime(true) / 1e9;
        $state = $connection->state(self::BUDGET);

        if (! is_array($state)
            || ! is_float($state['start'] ?? null)
            || $now - $state['start'] >= $this->budgetWindow) {
            return ['start' => $now, 'replays' => 0, 'messages' => 0, 'warned' => false];
        }

        return [
            'start' => $state['start'],
            'replays' => (int) ($state['replays'] ?? 0),
            'messages' => (int) ($state['messages'] ?? 0),
            'warned' => (bool) ($state['warned'] ?? false),
        ];
    }

    /**
     * Determine whether a connection is already subscribed to a channel.
     */
    protected function subscribedAlready(Connection $connection, string $channel): bool
    {
        return array_key_exists(
            $connection->id(),
            $this->context->connectionsOn($connection->app(), $channel),
        );
    }

    /**
     * Remember the cursor for a channel, dropping the oldest beyond the pending cap.
     */
    protected function remember(Connection $connection, string $channel, string $lastId): void
    {
        $cursors = $this->cursors($connection);

        // Re-inserting moves the channel to the end, so the cap drops the
        // cursors of the oldest unconfirmed subscribes first.
        unset($cursors[$channel]);
        $cursors[$channel] = $lastId;

        while ($this->maxPending > 0 && count($cursors) > $this->maxPending) {
            unset($cursors[array_key_first($cursors)]);
        }

        $connection->setState(self::CURSORS, $cursors);
    }

    /**
     * Drop the pending cursor for one channel. A no-op when there is none.
     */
    protected function forget(Connection $connection, string $channel): void
    {
        $cursors = $this->cursors($connection);

        if (! array_key_exists($channel, $cursors)) {
            return;
        }

        unset($cursors[$channel]);
        $connection->setState(self::CURSORS, $cursors);
    }

    /**
     * The pending replay cursors held on a connection, keyed by channel.
     *
     * @return array<string, string>
     */
    protected function cursors(Connection $connection): array
    {
        $state = $connection->state(self::CURSORS, []);

        if (! is_array($state)) {
            return [];
        }

        $cursors = [];

        foreach ($state as $channel => $lastId) {
            if (is_string($lastId)) {
                $cursors[(string) $channel] = $lastId;
            }
        }

        return $cursors;
    }

    /**
     * Decode one `XRANGE` entry into a usable shape.
     *
     * @param  array{0?: mixed, 1?: mixed}  $entry
     * @return array{id: string, event: string, data: array<string, mixed>}|null
     */
    protected function decodeEntry(array $entry): ?array
    {
        $id = (string) ($entry[0] ?? '');
        $pairs = array_values((array) ($entry[1] ?? []));

        if ($id === '') {
            return null;
        }

        $fields = [];

        for ($i = 0; $i + 1 < count($pairs); $i += 2) {
            $fields[(string) $pairs[$i]] = (string) $pairs[$i + 1];
        }

        try {
            $data = json_decode($fields['data'] ?? 'null', associative: true, flags: JSON_THROW_ON_ERROR);
        } catch (Throwable) {
            $data = [];
        }

        return [
            'id' => $id,
            'event' => $fields['event'] ?? '',
            'data' => is_array($data) ? $data : [],
        ];
    }

    /**
     * Build the fledge-fiber Redis configuration from the connection config.
     *
     * `RedisConfig::fromParameters()` reads the Laravel-shaped connection array
     * directly. This used to assemble a `redis://user:pass@host:port/db` string
     * by hand, and everything a URI cannot carry was dropped on the way: the
     * `tls` / `rediss` scheme, unix socket paths, `read_timeout`, the retry
     * settings, the client name and tcp keepalive. A configured `url` still
     * wins, since that form is a URI to begin with.
     *
     * @param  array<string, mixed>  $server
     */
    protected function makeConfig(array $server): RedisConfig
    {
        if (! empty($server['url'])) {
            return RedisConfig::fromUri(
                (string) $server['url'],
                (float) ($server['timeout'] ?? RedisConfig::DEFAULT_TIMEOUT),
            );
        }

        return RedisConfig::fromParameters($server);
    }
}
