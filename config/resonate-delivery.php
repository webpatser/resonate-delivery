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
    | `ttl` bounds the other axis: how long a stream key survives its last
    | append, in seconds. MAXLEN caps how long one stream gets but never how
    | many exist, so a per-entity channel scheme ("private-orders.{id}") leaves
    | a key behind for every entity that was ever broadcast to. The TTL is
    | refreshed on each append, so an active channel never expires and an idle
    | one is reclaimed. Set 0 to keep every stream forever.
    |
    */

    'retention' => [
        'default_max_messages' => (int) env('RESONATE_DELIVERY_RETENTION', 1000),

        'per_channel' => [
            // 'presence-chat.*' => 5000,
            // 'private-billing.*' => 100,
        ],

        'ttl' => (int) env('RESONATE_DELIVERY_TTL', 604800),
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
    | Replay reads
    |--------------------------------------------------------------------------
    |
    | `replay_batch_size` is the XRANGE page size used while reading a missed
    | window.
    |
    | `replay_max_messages` caps how many messages a single replay buffers for
    | one connection. The stream's own MAXLEN already bounds this, so the cap
    | only matters when retention is set far higher than a reconnecting client
    | could ever use; beyond it the replay is truncated and a warning logged.
    | Set 0 to remove the cap.
    |
    */

    'replay_batch_size' => (int) env('RESONATE_DELIVERY_BATCH_SIZE', 100),

    'replay_max_messages' => (int) env('RESONATE_DELIVERY_MAX_REPLAY', 10000),

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
