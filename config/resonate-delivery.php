<?php

return [

    /*
    |--------------------------------------------------------------------------
    | Redis connection
    |--------------------------------------------------------------------------
    |
    | Where the message logs live. The Laravel broadcaster writes here over
    | predis; the Resonate plugin reads here over the fledge-fiber async
    | client. Both ends must point at the same server and database.
    |
    */

    'connection' => [
        'url' => env('RESONATE_DELIVERY_REDIS_URL', env('REDIS_URL')),
        'host' => env('RESONATE_DELIVERY_REDIS_HOST', env('REDIS_HOST', '127.0.0.1')),
        'port' => env('RESONATE_DELIVERY_REDIS_PORT', env('REDIS_PORT', '6379')),
        'username' => env('RESONATE_DELIVERY_REDIS_USERNAME', env('REDIS_USERNAME')),
        'password' => env('RESONATE_DELIVERY_REDIS_PASSWORD', env('REDIS_PASSWORD')),
        'database' => env('RESONATE_DELIVERY_REDIS_DB', env('REDIS_DB', '0')),
        'timeout' => env('RESONATE_DELIVERY_REDIS_TIMEOUT', 5),
    ],

    /*
    |--------------------------------------------------------------------------
    | Key prefix
    |--------------------------------------------------------------------------
    |
    | Every stream key is namespaced "{prefix}:{appId}:{channel}". Avoid
    | colons in the prefix.
    |
    */

    'key_prefix' => env('RESONATE_DELIVERY_PREFIX', 'delivery'),

    /*
    |--------------------------------------------------------------------------
    | Retention
    |--------------------------------------------------------------------------
    |
    | The maximum number of messages kept per channel. XADD uses MAXLEN ~ N
    | (approximate trim) for O(1) inserts; oldest messages drop as new ones
    | arrive. Override per channel with fnmatch glob patterns.
    |
    | Plan for: expected reconnect duration * expected publish rate.
    |
    */

    'retention' => [
        'default_max_messages' => (int) env('RESONATE_DELIVERY_RETENTION', 1000),

        'per_channel' => [
            // 'presence-chat.*' => 5000,
            // 'private-billing.*' => 100,
        ],
    ],

    /*
    |--------------------------------------------------------------------------
    | Replay id field
    |--------------------------------------------------------------------------
    |
    | The broadcaster augments every event's `data` with this field, set to
    | the Redis Stream id that addresses the message. The client reads it,
    | remembers the highest seen, and sends it back as `last_event_id` on
    | reconnect.
    |
    */

    'replay_id_field' => env('RESONATE_DELIVERY_ID_FIELD', '_replay_id'),

    /*
    |--------------------------------------------------------------------------
    | Underlying broadcaster
    |--------------------------------------------------------------------------
    |
    | The custom `resonate-delivery` broadcaster wraps an existing Laravel
    | broadcaster (Reverb in a Resonate setup). Name the connection here, or
    | override per-connection by adding `'underlying' => 'name'` to the
    | broadcaster's connection config in `config/broadcasting.php`.
    |
    */

    'underlying' => env('RESONATE_DELIVERY_UNDERLYING', 'reverb'),

];
