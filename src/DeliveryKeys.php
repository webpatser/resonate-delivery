<?php

namespace Webpatser\ResonateDelivery;

/**
 * The Redis Stream key schema for the delivery log.
 *
 * One stream per channel per application: "{prefix}:{appId}:{channel}".
 * Both the Laravel-side broadcaster and the server-side replay plugin compute
 * the same key from this class so the log is one artifact from both ends.
 */
class DeliveryKeys
{
    /**
     * Create a new key schema.
     */
    public function __construct(protected string $prefix = 'delivery')
    {
        //
    }

    /**
     * The stream key for one channel on one application.
     */
    public function streamKey(string $appId, string $channel): string
    {
        return $this->prefix.':'.$appId.':'.$channel;
    }
}
