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
 *    clears the buffer. It performs no I/O, so it cannot suspend.
 *
 * ## Why the read happens before the subscribe
 *
 * The ordering guarantee is that replayed messages arrive after
 * `subscription_succeeded` and before any live broadcast. Reading during
 * `onSubscribe` cannot honour that: the connection is already in the channel
 * by then, so every `XRANGE` round trip suspends this fiber and lets another
 * fiber deliver a live broadcast to the same socket. A client could see
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
 *  2. From the moment core adds the connection to the channel, through
 *     `subscription_succeeded` and on into this plugin's flush, Resonate runs
 *     straight-line synchronous code with no suspension point. No other fiber
 *     gets to run in that window, so nothing can wedge a live frame between
 *     the subscribe and the buffered replay.
 *
 * Nothing is lost at the far seam either. A message appended after the final
 * `XRANGE` is broadcast by a fiber that cannot run until this one suspends,
 * which is after the flush, so the client receives it live and in order. A
 * message appended just before the final read may arrive twice (once replayed,
 * once live, the live copy second); that is the documented duplicate, and the
 * client deduplicates on `_replay_id`.
 *
 * Connections that subscribe without `last_event_id` get no replay (the
 * normal Pusher experience), and neither does a connection already subscribed
 * to the channel, because a live subscriber has missed nothing. Buffered state
 * is cleared on flush, on unsubscribe and on connection close.
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
     * @param  array{event?:mixed,data?:mixed}  $event
     */
    public function onMessage(Connection $from, array $event): MessageDisposition
    {
        if (($event['event'] ?? null) !== 'pusher:subscribe') {
            return MessageDisposition::Relay;
        }

        $data = (array) ($event['data'] ?? []);
        $channel = (string) ($data['channel'] ?? '');
        $lastId = $data['last_event_id'] ?? null;

        if ($this->redis === null || $channel === '' || ! is_string($lastId) || $lastId === '') {
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
     * This method must never suspend. It runs in the synchronous window that
     * starts when core adds the connection to the channel, and a suspension
     * here would hand control to a fiber that could deliver a live broadcast
     * ahead of the replay, which is exactly the inversion the buffer exists
     * to prevent.
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
        $buffered = $this->buffered($connection);

        if (array_key_exists($channel->name(), $buffered)) {
            unset($buffered[$channel->name()]);
            $connection->setState(self::BUFFER, $buffered);
        }
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
     * @param  array<string, mixed>  $server
     */
    protected function makeConfig(array $server): RedisConfig
    {
        $timeout = (float) ($server['timeout'] ?? RedisConfig::DEFAULT_TIMEOUT);

        if (! empty($server['url'])) {
            return RedisConfig::fromUri($server['url'], $timeout);
        }

        $host = $server['host'] ?? '127.0.0.1';
        $port = $server['port'] ?? 6379;
        $database = $server['database'] ?? 0;

        $userInfo = '';

        if (! empty($server['password'])) {
            $userInfo = rawurlencode((string) ($server['username'] ?? ''))
                .':'.rawurlencode((string) $server['password']).'@';
        }

        return RedisConfig::fromUri(
            sprintf('redis://%s%s:%s/%s', $userInfo, $host, $port, $database),
            $timeout,
        );
    }
}
