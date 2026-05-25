<?php

namespace Webpatser\ResonateDelivery\Tests\Support;

use Webpatser\Resonate\Application;
use Webpatser\Resonate\Contracts\Connection;

/**
 * A minimal in-memory Connection for driving the plugin in tests.
 *
 * Records every message sent so tests can assert that the plugin replayed
 * the right Pusher frames in the right order.
 */
class FakeConnection extends Connection
{
    /**
     * Every message the server sent through this connection.
     *
     * @var list<string>
     */
    public array $messages = [];

    /**
     * Create a new fake connection.
     */
    public function __construct(protected string $socketId, protected Application $appInstance)
    {
        $this->origin = 'http://localhost';
    }

    public function identifier(): string
    {
        return $this->socketId;
    }

    public function id(): string
    {
        return $this->socketId;
    }

    public function app(): Application
    {
        return $this->appInstance;
    }

    public function send(string $message): void
    {
        $this->messages[] = $message;
    }

    public function control(string $type = self::CONTROL_PING): void
    {
        //
    }

    public function terminate(): void
    {
        //
    }
}
