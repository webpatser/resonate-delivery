<?php

namespace Webpatser\ResonateDelivery\Tests\Support;

use Webpatser\Resonate\Contracts\ServerProvider;

/**
 * A server provider that reports horizontal scaling as enabled.
 */
class ScaledServerProvider extends ServerProvider
{
    public function shouldPublishEvents(): bool
    {
        return true;
    }
}
