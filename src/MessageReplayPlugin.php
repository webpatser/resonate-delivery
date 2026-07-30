<?php

namespace Webpatser\ResonateDelivery;

use Fledge\Async\Redis\RedisClient;
use Fledge\Async\Redis\RedisConfig;
use Throwable;
use Webpatser\Resonate\Contracts\Connection;
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
 * Two phases:
 *
 *  - `onMessage(pusher:subscribe)` notices the optional `last_event_id` field
 *    in the subscribe data and stashes it on the connection's state bag,
 *    keyed by the channel name. It always returns `Relay` so the standard
 *    subscribe flow runs untouched.
 *
 *  - `onSubscribe(Connection, Channel)` fires after `subscription_succeeded`
 *    has been sent. It reads the stashed id for this channel, walks the
 *    Redis Stream from there to the present via `XRANGE`, and sends every
 *    missed message to the connection in original order. Replayed messages
 *    arrive after `subscription_succeeded` and before any live broadcast.
 *
 * Connections that subscribe without `last_event_id` get no replay (the
 * normal Pusher experience). Per-subscribe state is cleared on unsubscribe
 * and on connection close.
 */
class MessageReplayPlugin implements ConnectionLifecycle, MessageInterceptor, ServerPlugin
{
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
     * Boot the plugin: open the Redis client and resolve config.
     */
    public function boot(PluginContext $context): void
    {
        $this->context = $context;

        $config = config('resonate-delivery', []);

        $this->keys = new DeliveryKeys($config['key_prefix'] ?? 'delivery');
        $this->replayIdField = (string) ($config['replay_id_field'] ?? '_replay_id');
        $this->batchSize = (int) ($config['replay_batch_size'] ?? 100);
        $this->redis = createRedisClient($this->makeConfig($config['connection'] ?? []));
    }

    /**
     * Notice and stash the `last_event_id` for a subscribe; relay everything.
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

        if ($channel !== '' && is_string($lastId) && $lastId !== '') {
            $stash = $from->state('delivery.subscribes', []);
            $stash[$channel] = $lastId;
            $from->setState('delivery.subscribes', $stash);
        }

        return MessageDisposition::Relay;
    }

    /**
     * Replay every message the connection missed on this channel.
     */
    public function onSubscribe(Connection $connection, Channel $channel): void
    {
        if ($this->redis === null) {
            return;
        }

        $stash = $connection->state('delivery.subscribes', []);
        $name = $channel->name();
        $lastId = $stash[$name] ?? null;

        if (! is_string($lastId) || $lastId === '') {
            return;
        }

        // One-shot: clear the stash so a later resubscribe without a new id
        // does not trigger a duplicate replay.
        unset($stash[$name]);
        $connection->setState('delivery.subscribes', $stash);

        $this->replay($connection, $channel, $lastId, $this->redis);
    }

    /**
     * Clear stash for a channel a connection leaves explicitly.
     */
    public function onUnsubscribe(Connection $connection, Channel $channel): void
    {
        $stash = $connection->state('delivery.subscribes', []);

        if (isset($stash[$channel->name()])) {
            unset($stash[$channel->name()]);
            $connection->setState('delivery.subscribes', $stash);
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
     * Clear stash entirely on close.
     */
    public function onClose(Connection $connection): void
    {
        $connection->forgetState('delivery.subscribes');
    }

    /**
     * Read missed messages and send each to the connection in order.
     */
    protected function replay(Connection $connection, Channel $channel, string $lastId, RedisClient $redis): void
    {
        $key = $this->keys->streamKey($connection->app()->id(), $channel->name());
        $cursor = '('.$lastId;

        while (true) {
            try {
                $raw = $redis->execute('XRANGE', $key, $cursor, '+', 'COUNT', (string) $this->batchSize);
            } catch (Throwable) {
                return;
            }

            if (! is_array($raw) || $raw === []) {
                return;
            }

            $latestId = $lastId;

            foreach ($raw as $entry) {
                $decoded = $this->decodeEntry($entry);

                if ($decoded === null) {
                    continue;
                }

                $augmented = $decoded['data'];
                $augmented[$this->replayIdField] = $decoded['id'];

                $connection->send(json_encode([
                    'event' => $decoded['event'],
                    'channel' => $channel->name(),
                    'data' => $augmented,
                ], JSON_THROW_ON_ERROR));

                $latestId = $decoded['id'];
            }

            // Less than a full batch means we caught up.
            if (count($raw) < $this->batchSize) {
                return;
            }

            $cursor = '('.$latestId;
        }
    }

    /**
     * Decode one `XRANGE` entry into a usable shape.
     *
     * @param  array{0?: string, 1?: list<string>}  $entry
     * @return array{id: string, event: string, data: array<string, mixed>}|null
     */
    protected function decodeEntry(array $entry): ?array
    {
        $id = (string) ($entry[0] ?? '');
        $pairs = (array) ($entry[1] ?? []);

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
