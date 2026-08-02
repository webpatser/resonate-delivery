<?php

namespace Webpatser\ResonateDelivery\Broadcasting;

use Illuminate\Contracts\Broadcasting\Broadcaster;
use Illuminate\Support\Facades\Log;
use Throwable;
use Webpatser\ResonateDelivery\MessageLog;

/**
 * Laravel broadcaster that wraps an underlying driver (typically Reverb) and
 * appends every broadcast to a per-channel Redis Stream before delegating
 * the actual send.
 *
 * The augmented payload includes the assigned stream id under the configured
 * `replay_id_field` (default `_replay_id`) so the client can remember the
 * last id it saw and ask for a replay on reconnect.
 *
 * Logging is best-effort by design. This package is a durability add-on, and an
 * add-on must not be able to take the base service down: if the delivery Redis
 * is unreachable, an unguarded append would throw straight out of every
 * `broadcast()` call and stop live delivery for an application that was working
 * perfectly well before the package was installed. A failed append is logged
 * and the broadcast is delegated anyway, without a `_replay_id`. The missing
 * field is the signal: a client only advances its cursor on messages that carry
 * one, so it never mistakes an unlogged message for a replayable one.
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
     * The append is caught per channel: one channel whose log write fails does
     * not cost the other channels their `_replay_id`, and no channel loses its
     * live broadcast.
     *
     * Signature matches Laravel's `Broadcaster::broadcast` (untyped `$event`,
     * no return type) so it can be plugged in as a regular broadcaster.
     *
     * @param  array<int, string|\Stringable>  $channels
     * @param  array<string, mixed>  $payload
     */
    public function broadcast(array $channels, $event, array $payload = [])
    {
        foreach ($channels as $channel) {
            $name = (string) $channel;
            $augmented = $payload;

            try {
                $augmented[$this->replayIdField] = $this->log->append($this->appId, $name, (string) $event, $payload);
            } catch (Throwable $e) {
                Log::error('resonate-delivery could not log a broadcast on '.$name.', delivering it live only: '.$e->getMessage());
            }

            $this->underlying->broadcast([$channel], $event, $augmented);
        }
    }
}
