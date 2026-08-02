<?php

namespace Webpatser\ResonateDelivery\Tests\Support;

use RuntimeException;
use Webpatser\ResonateDelivery\MessageLog;

/**
 * A {@see MessageLog} whose every append fails, standing in for an unreachable
 * or failing delivery Redis.
 */
class FailingMessageLog extends MessageLog
{
    /**
     * Create a log that needs no Redis connection, because it never gets that far.
     */
    public function __construct(public string $reason = 'Connection refused [tcp://127.0.0.1:6379]')
    {
        //
    }

    /**
     * Every append blows up the way predis does when Redis is unreachable.
     */
    public function append(string $appId, string $channel, string $event, array $data): string
    {
        throw new RuntimeException($this->reason);
    }
}
