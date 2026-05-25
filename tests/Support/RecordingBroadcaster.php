<?php

namespace Webpatser\ResonateDelivery\Tests\Support;

use Illuminate\Contracts\Broadcasting\Broadcaster;

/**
 * A stand-in `Broadcaster` that records every call. Used to verify the
 * `ReplayBroadcaster` wraps a delegate correctly.
 */
class RecordingBroadcaster implements Broadcaster
{
    /**
     * Every broadcast call: [channels, event, payload].
     *
     * @var list<array{0: array<int, string|object>, 1: string, 2: array<string, mixed>}>
     */
    public array $calls = [];

    public function auth($request)
    {
        return ['delegated' => true];
    }

    public function validAuthenticationResponse($request, $result)
    {
        return $result;
    }

    public function broadcast(array $channels, $event, array $payload = [])
    {
        $this->calls[] = [$channels, $event, $payload];
    }
}
