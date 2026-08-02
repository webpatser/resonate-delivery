<?php

namespace Webpatser\ResonateDelivery;

use JsonException;
use Predis\Client;

/**
 * Append-only per-channel message log backed by Redis Streams.
 *
 * `append()` is the broadcaster's write surface (sync, predis). `replaySince()`
 * is provided for test introspection and for any host-side consumer that wants
 * to read the log without the Resonate fiber runtime; the server-side plugin
 * does its own reads via the fledge-fiber async client so it does not block
 * the event loop.
 *
 * The retention cap is approximate (`MAXLEN ~ N`): Redis trims in batches for
 * O(1) inserts. The actual stream may briefly hold a few entries beyond the
 * configured cap, never fewer.
 *
 * Length is only half of retention. `MAXLEN` bounds one stream; it says nothing
 * about how many streams exist, and a per-entity channel scheme mints a new key
 * per entity. `append()` therefore also stamps a TTL on the key, refreshed on
 * every append, so a channel that stops being written to eventually disappears
 * while an active one is never at risk.
 */
class MessageLog
{
    /**
     * Create a new log.
     */
    public function __construct(
        protected Client $redis,
        protected DeliveryKeys $keys,
        protected DeliveryRetention $retention,
    ) {
        //
    }

    /**
     * Append a message to a channel's stream and return its assigned id.
     *
     * The id is monotonic (Redis assigns `{ms-time}-{seq}`); a reader holding
     * the id of the last message it saw can use `replaySince()` to get
     * everything newer.
     *
     * The configured TTL is re-stamped on every append, which costs one extra
     * round trip and buys a bound on the number of live stream keys. A channel
     * written to more often than the window never expires; one that goes quiet
     * takes its key with it.
     *
     * @param  array<string, mixed>  $data
     *
     * @throws JsonException
     */
    public function append(string $appId, string $channel, string $event, array $data): string
    {
        $key = $this->keys->streamKey($appId, $channel);
        $max = $this->retention->forChannel($channel);

        $id = $this->redis->executeRaw([
            'XADD', $key, 'MAXLEN', '~', (string) $max, '*',
            'event', $event,
            'data', json_encode($data, JSON_THROW_ON_ERROR),
        ]);

        $ttl = $this->retention->ttl();

        if ($ttl > 0) {
            $this->redis->executeRaw(['EXPIRE', $key, (string) $ttl]);
        }

        return (string) $id;
    }

    /**
     * Read every entry strictly newer than `$sinceId`, oldest first.
     *
     * Pass an exclusive cursor: `XRANGE` accepts `(id` to skip the cursor
     * itself, so a client that resubscribes with the id of its last seen
     * message does not receive that message a second time.
     *
     * @return list<array{id: string, event: string, data: array<string, mixed>}>
     */
    public function replaySince(string $appId, string $channel, string $sinceId, int $count = 100): array
    {
        $key = $this->keys->streamKey($appId, $channel);

        $raw = $this->redis->executeRaw([
            'XRANGE', $key, '('.$sinceId, '+', 'COUNT', (string) $count,
        ]);

        if (! is_array($raw)) {
            return [];
        }

        return array_values(array_map(static fn (array $entry) => self::decode($entry), $raw));
    }

    /**
     * Decode one raw `[id, [field1, val1, field2, val2, ...]]` entry into a
     * convenient associative shape.
     *
     * @param  array<int, mixed>  $entry
     * @return array{id: string, event: string, data: array<string, mixed>}
     */
    private static function decode(array $entry): array
    {
        $id = (string) $entry[0];

        $fields = [];
        $pairs = (array) ($entry[1] ?? []);

        for ($i = 0; $i + 1 < count($pairs); $i += 2) {
            $fields[(string) $pairs[$i]] = (string) $pairs[$i + 1];
        }

        try {
            $data = json_decode($fields['data'] ?? 'null', associative: true, flags: JSON_THROW_ON_ERROR);
        } catch (JsonException) {
            $data = [];
        }

        return [
            'id' => $id,
            'event' => $fields['event'] ?? '',
            'data' => is_array($data) ? $data : [],
        ];
    }
}
