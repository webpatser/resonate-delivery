<?php

namespace Webpatser\ResonateDelivery\Broadcasting;

use Illuminate\Contracts\Broadcasting\Broadcaster;
use Webpatser\ResonateDelivery\MessageLog;

/**
 * Laravel broadcaster that wraps an underlying driver (typically Reverb) and
 * appends every broadcast to a per-channel Redis Stream before delegating
 * the actual send.
 *
 * The augmented payload includes the assigned stream id under the configured
 * `replay_id_field` (default `_replay_id`) so the client can remember the
 * last id it saw and ask for a replay on reconnect.
 */
class ReplayBroadcaster implements Broadcaster
{
    /**
     * Create a new broadcaster.
     */
    public function __construct(
        protected Broadcaster $underlying,
        protected MessageLog $log,
        protected string $appId,
        protected string $replayIdField = '_replay_id',
    ) {
        //
    }

    /**
     * Authenticate the incoming subscription request (delegated unchanged).
     */
    public function auth($request)
    {
        return $this->underlying->auth($request);
    }

    /**
     * Build the authentication response (delegated unchanged).
     */
    public function validAuthenticationResponse($request, $result)
    {
        return $this->underlying->validAuthenticationResponse($request, $result);
    }

    /**
     * Broadcast the given event: append to each channel's log, then delegate
     * to the underlying driver with the augmented payload (per channel, so
     * each subscriber sees a `_replay_id` that addresses its own stream).
     *
     * Signature matches Laravel's `Broadcaster::broadcast` (untyped `$event`,
     * no return type) so it can be plugged in as a regular broadcaster.
     *
     * @param  array<int, string|object>  $channels
     * @param  array<string, mixed>  $payload
     */
    public function broadcast(array $channels, $event, array $payload = [])
    {
        foreach ($channels as $channel) {
            $name = (string) $channel;

            $id = $this->log->append($this->appId, $name, (string) $event, $payload);

            $augmented = $payload;
            $augmented[$this->replayIdField] = $id;

            $this->underlying->broadcast([$channel], $event, $augmented);
        }
    }
}
