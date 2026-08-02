<?php

namespace Webpatser\ResonateDelivery;

/**
 * Per-channel retention policy.
 *
 * Two independent bounds, because `MAXLEN` alone only limits how long one
 * stream gets, never how many streams exist:
 *
 *  - Length. Walks the configured pattern list (fnmatch) in declaration order,
 *    falling back to `default_max_messages` when nothing matches. Used by the
 *    log to set `MAXLEN ~ N` on each `XADD`.
 *  - Lifetime. A single TTL, in seconds, refreshed on every append. Per-entity
 *    channels (`private-orders.{id}`) are created faster than they are ever
 *    revisited, so without an expiry their stream keys accumulate for the life
 *    of the Redis instance. 0 disables the expiry and restores the unbounded
 *    behaviour.
 */
class DeliveryRetention
{
    /**
     * Create a new retention policy.
     *
     * @param  array<string, int>  $perChannel  fnmatch pattern => max messages
     * @param  int  $ttl  seconds a stream key survives its last append; 0 never expires
     */
    public function __construct(
        protected int $default,
        protected array $perChannel = [],
        protected int $ttl = 0,
    ) {
        //
    }

    /**
     * The maximum number of messages to keep for one channel.
     */
    public function forChannel(string $channel): int
    {
        foreach ($this->perChannel as $pattern => $max) {
            if (fnmatch($pattern, $channel)) {
                return (int) $max;
            }
        }

        return $this->default;
    }

    /**
     * The lifetime, in seconds, a stream key gets on each append. 0 disables it.
     */
    public function ttl(): int
    {
        return max(0, $this->ttl);
    }
}
