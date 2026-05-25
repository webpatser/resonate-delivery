<?php

namespace Webpatser\ResonateDelivery;

/**
 * Per-channel retention policy.
 *
 * Walks the configured pattern list (fnmatch) in declaration order, falling
 * back to `default_max_messages` when nothing matches. Used by the broadcaster
 * to set `MAXLEN ~ N` on each `XADD`.
 */
class DeliveryRetention
{
    /**
     * Create a new retention policy.
     *
     * @param  array<string, int>  $perChannel  fnmatch pattern => max messages
     */
    public function __construct(
        protected int $default,
        protected array $perChannel = [],
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
}
