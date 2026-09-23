<?php

namespace Webpatser\ResonateDelivery\Tests\Support;

use Webpatser\Resonate\Scaling\Contracts\PubSubProvider;

/**
 * An in-memory pub/sub bus that behaves like Redis pub/sub.
 *
 * Adapted from resonate core's test fakes, which are not autoloaded from
 * vendor. A publisher receives its own publications (the subscriber is a
 * separate connection on the same channel), and `publish()` answers with the
 * number of subscribers the envelope was delivered to.
 */
class FakePubSubBus implements PubSubProvider
{
    /**
     * Every envelope published on the bus, in order.
     *
     * @var array<int, array<string, mixed>>
     */
    public array $published = [];

    /**
     * The callbacks that receive every published envelope.
     *
     * @var array<int, callable>
     */
    public array $subscribers = [];

    /**
     * The subscriber count `publish()` reports back.
     */
    public int $receivers = 1;

    public function connect(): void {}

    public function disconnect(): void {}

    public function subscribe(): void {}

    public function on(string $event, callable $callback): void {}

    public function listen(string $event, callable $callback): void {}

    public function stopListening(string $event): void {}

    public function publish(array $payload): int
    {
        $this->published[] = $payload;

        foreach ($this->subscribers as $subscriber) {
            $subscriber($payload);
        }

        return $this->receivers;
    }
}
