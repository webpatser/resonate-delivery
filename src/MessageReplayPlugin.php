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

use function Fledge\Async\Redis\createRedisClient;

/**
 * The server-side half of resonate-delivery.
 *
 * Two phases, and which phase does the Redis work is the whole design:
 *
 *  - `onMessage(pusher:subscribe)` notices the optional `last_event_id` field
 *    in the subscribe data and, right there, reads every missed message from
 *    the channel's Redis Stream into an in-memory buffer on the connection.
 *    It always returns `Relay` so the standard subscribe flow runs untouched.
 *
 *  - `onSubscribe(Connection, Channel)` fires after `subscription_succeeded`
 *    has been sent. It writes the buffered frames to the connection and
 *    clears the buffer. It performs no I/O of its own.
 *
 * ## Why the read happens before the subscribe
 *
 * The ordering guarantee is that replayed messages arrive after
 * `subscription_succeeded` and before any live broadcast. Before Resonate
 * 0.7.1, reading during `onSubscribe` could not honour that: the connection
 * was already in the channel by then, so every `XRANGE` round trip suspended
 * this fiber and let another fiber deliver a live broadcast to the same
 * socket. A client could see
 * replayed 5 and 6, then live 12, then replayed 7 through 11, and a monotonic
 * `_replay_id` cannot repair an inversion the client has already applied.
 *
 * Doing the reads from the interceptor removes the race rather than papering
 * over it. Two properties combine:
 *
 *  1. While the buffer is being filled the connection has not yet joined the
 *     channel, and a broadcast only walks the channel's connection list, so no
 *     live frame for that channel can reach the socket however long the reads
 *     take.
 *  2. From the moment core (Resonate 0.7.1+) adds the connection to the
 *     channel, it holds every live frame for that connection until
 *     `subscription_succeeded` is out and every plugin's `onSubscribe` has
 *     returned, then delivers the held frames in arrival order. That holds
 *     even when core suspends in between (a scaled presence channel asking
 *     the fleet for its members) or another plugin's `onSubscribe` suspends
 *     on I/O, so the replay written from `onSubscribe` always precedes both
 *     the held and the live frames.
 *
 * Nothing is lost at the far seam either. A message appended after the final
 * `XRANGE` reaches the connection live (held first if it lands during the
 * subscribe), after the replay and in order. A message appended just before
 * the final read may arrive twice (once replayed, once live, the live copy
 * second); that is the documented duplicate, and the client deduplicates on
 * `_replay_id`.
 *
 * Connections that subscribe without `last_event_id` get no replay (the
 * normal Pusher experience), and neither does a connection already subscribed
 * to the channel, because a live subscriber has missed nothing. Buffered state
 * is cleared on flush, on `pusher:unsubscribe`, on a `pusher:subscribe`
 * without `last_event_id`, in `onUnsubscribe` and on connection close.
 */
class MessageReplayPlugin implements ConnectionLifecycle, MessageInterceptor, ServerPlugin
{
    /**
     * The connection state key holding buffered replay frames per channel.
     */
    protected const string BUFFER = 'delivery.replay';

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
     * The most messages one replay may buffer. 0 removes the limit.
     */
    protected int $maxMessages;

    /**
     * Boot the plugin: open the Redis client and resolve config.
     */
    public function boot(PluginContext $context): void
    {
        $this->context = $context;

        $config = config('resonate-delivery', []);

        $this->keys = new DeliveryKeys($config['key_prefix'] ?? 'delivery');
        $this->replayIdField = (string) ($config['replay_id_field'] ?? '_replay_id');
        $this->batchSize = (int) ($config['replay_batch_size'] ?? 100);
        $this->maxMessages = (int) ($config['replay_max_messages'] ?? 10000);
        $this->redis = createRedisClient($this->makeConfig($config['connection'] ?? []));
    }

    /**
     * Buffer the replay for a subscribe carrying `last_event_id`; relay everything.
     *
     * The read happens here, before the connection joins the channel, so it
     * can take as many round trips as it needs without a live broadcast
     * slipping in front of the replay.
     *
     * A `pusher:unsubscribe` drops the channel's buffer here as well. Core
     * only calls `onUnsubscribe` for a subscription the plugins were told
     * about, so an unsubscribe that overtakes its own subscribe would
     * otherwise leave the buffer behind for a later subscribe without
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

        if ($type === 'pusher:unsubscribe') {
            if ($channel !== '') {
                $this->forget($from, $channel);
            }

            return MessageDisposition::Relay;
        }

        $lastId = $data['last_event_id'] ?? null;

        // A subscribe without a cursor asks for no replay. Any buffer still
        // held for the channel belongs to an earlier subscribe that core
        // rejected (bad auth, subscription limit) and must not be flushed.
        if ($channel === '') {
            return MessageDisposition::Relay;
        }

        if (! is_string($lastId) || $lastId === '') {
            $this->forget($from, $channel);

            return MessageDisposition::Relay;
        }

        if ($this->redis === null) {
            return MessageDisposition::Relay;
        }

        // A connection already in the channel is receiving live broadcasts, so
        // it has missed nothing and a replay would only reorder its stream.
        if ($this->subscribedAlready($from, $channel)) {
            return MessageDisposition::Relay;
        }

        $buffered = $this->buffered($from);
        $buffered[$channel] = $this->read($from->app()->id(), $channel, $lastId, $this->redis);

        $from->setState(self::BUFFER, $buffered);

        return MessageDisposition::Relay;
    }

    /**
     * Flush the buffered replay for a channel the connection just joined.
     *
     * Core (0.7.1+) holds live frames for the connection from the moment it
     * joins the channel until every plugin's `onSubscribe` has returned, so
     * these replay frames always precede the held and live ones, even when
     * core or another plugin suspends first. A live frame that overlaps the
     * replay arrives after it and is deduplicated on `_replay_id`.
     */
    public function onSubscribe(Connection $connection, Channel $channel): void
    {
        $buffered = $this->buffered($connection);
        $name = $channel->name();

        if (! array_key_exists($name, $buffered)) {
            return;
        }

        $frames = $buffered[$name];

        // One-shot: drop the buffer before sending so a later resubscribe
        // without a new cursor cannot replay the same messages twice.
        unset($buffered[$name]);
        $connection->setState(self::BUFFER, $buffered);

        foreach ($frames as $frame) {
            $connection->send($frame);
        }
    }

    /**
     * Drop any buffered replay for a channel a connection leaves explicitly.
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
     * Drop every buffered replay on close.
     *
     * A subscribe that core then rejects (bad auth, subscription limit) never
     * reaches `onSubscribe`, so its buffer is released here rather than living
     * as long as the socket.
     */
    public function onClose(Connection $connection): void
    {
        $connection->forgetState(self::BUFFER);
    }

    /**
     * Read every message newer than the cursor into ready-to-send frames.
     *
     * A read failure returns what was gathered so far rather than throwing:
     * a short replay degrades the way an out-of-retention cursor already does,
     * where a thrown interceptor would cost the client its subscribe.
     *
     * @return list<string>
     */
    protected function read(string $appId, string $channel, string $lastId, RedisClient $redis): array
    {
        $key = $this->keys->streamKey($appId, $channel);
        $cursor = '('.$lastId;
        $frames = [];

        while (true) {
            try {
                $raw = $redis->execute('XRANGE', $key, $cursor, '+', 'COUNT', (string) $this->batchSize);
            } catch (Throwable $e) {
                Log::error('Delivery replay read failed on '.$channel.': '.$e->getMessage());

                return $frames;
            }

            if (! is_array($raw) || $raw === []) {
                return $frames;
            }

            $latestId = $lastId;

            foreach ($raw as $entry) {
                $decoded = is_array($entry) ? $this->decodeEntry($entry) : null;

                if ($decoded === null) {
                    continue;
                }

                $augmented = $decoded['data'];
                $augmented[$this->replayIdField] = $decoded['id'];

                $frames[] = json_encode([
                    'event' => $decoded['event'],
                    'channel' => $channel,
                    'data' => $augmented,
                ], JSON_THROW_ON_ERROR);

                $latestId = $decoded['id'];

                // The stream's own MAXLEN already bounds a replay, so reaching
                // this means retention is far larger than any client can use.
                // Stop rather than hold an unbounded buffer per connection.
                if ($this->maxMessages > 0 && count($frames) >= $this->maxMessages) {
                    Log::error('Delivery replay on '.$channel.' truncated at '.$this->maxMessages.' messages.');

                    return $frames;
                }
            }

            // Less than a full batch means we caught up.
            if (count($raw) < $this->batchSize) {
                return $frames;
            }

            $cursor = '('.$latestId;
        }
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
     * Drop the buffered replay for one channel. A no-op when there is none.
     */
    protected function forget(Connection $connection, string $channel): void
    {
        $buffered = $this->buffered($connection);

        if (! array_key_exists($channel, $buffered)) {
            return;
        }

        unset($buffered[$channel]);
        $connection->setState(self::BUFFER, $buffered);
    }

    /**
     * The buffered replay frames held on a connection, keyed by channel.
     *
     * @return array<string, list<string>>
     */
    protected function buffered(Connection $connection): array
    {
        $state = $connection->state(self::BUFFER, []);

        if (! is_array($state)) {
            return [];
        }

        $buffered = [];

        foreach ($state as $channel => $frames) {
            $buffered[(string) $channel] = is_array($frames)
                ? array_values(array_filter($frames, is_string(...)))
                : [];
        }

        return $buffered;
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
