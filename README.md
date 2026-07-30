# Resonate Delivery

At-least-once message delivery for [Resonate](https://github.com/webpatser/resonate) channels. Every broadcast is logged to a per-channel Redis Stream; a reconnecting client sends the id of the last message it saw, and the plugin replays everything since before live delivery resumes.

Solves the single most common WebSocket complaint: "I dropped for 20 seconds and missed messages."

## What you get

- A Laravel broadcaster (`resonate-delivery` driver) that wraps Reverb, logs every broadcast, and tags the payload with a monotonic `_replay_id`.
- A Resonate server plugin that, when a subscribe carries `last_event_id`, replays the missed messages on that channel after `subscription_succeeded` and before any live broadcast.
- Bounded retention via Redis Streams `MAXLEN ~ N`, configurable per-channel.

## What this is (and isn't)

- **At-least-once within retention.** With the default cap of 1000 messages per channel, a client that drops for 5 minutes on a low-traffic channel reconnects seamlessly. A client offline for hours on a high-traffic channel sees a clean break; design retention based on expected disconnect duration and your publish rate.
- **Duplicates can happen at the seam.** A message published exactly as a reconnecting client is mid-replay may arrive both in the replay and as a live broadcast. Each message carries a monotonic `_replay_id`; the client deduplicates by it.
- **No ACK protocol.** This is not a message queue. A subscriber that goes away forever does not hold a slot in any per-subscriber outbox. Per-subscriber state lives only on the connection.
- **Server broadcasts only in v0.1.** `client-*` whispers between clients are not logged; they reach connected subscribers only.

## Installation

```bash
composer require webpatser/resonate-delivery
```

Publish the config to set retention defaults or override the underlying broadcaster:

```bash
php artisan vendor:publish --tag=resonate-delivery-config
```

## Configuring the broadcaster

Add the wrapping driver to `config/broadcasting.php` and point your default at it:

```php
'default' => env('BROADCAST_DRIVER', 'resonate-delivery'),

'connections' => [
    // ... keep your existing 'reverb' connection as-is

    'resonate-delivery' => [
        'driver' => 'resonate-delivery',
        'underlying' => 'reverb',
    ],
],
```

The wrapper delegates auth and the actual WebSocket send to the underlying connection (so your existing Reverb setup is unchanged); it adds the log write and the `_replay_id` augmentation around it.

## Registering the server plugin

In `config/reverb.php`, list the plugin alongside any others you have running:

```php
'plugins' => [
    \Webpatser\ResonateDelivery\MessageReplayPlugin::class,
],
```

Restart Resonate (`resonate:reload` for a zero-downtime swap).

## Client protocol

A client that wants replay sends `last_event_id` in the subscribe payload:

```json
{"event": "pusher:subscribe",
 "data": {"channel": "presence-chat.42", "auth": "...", "last_event_id": "1700000000000-0"}}
```

On every message it receives, the client reads `data._replay_id` and remembers the highest. On reconnect, it sends that value. First-time subscribers omit the field and get no replay (normal Pusher behaviour).

For Laravel Echo and the JS Pusher client there is no built-in plug for this; consumers must subscribe with the custom data field, which both libraries support via their lower-level subscription APIs.

## Configuration reference

| Key | Default | Purpose |
|-----|---------|---------|
| `connection` | `REDIS_*` env | Redis server hosting the streams. The broadcaster (predis) and the plugin (fledge async) must point at the same server/database. |
| `key_prefix` | `delivery` | Namespace for stream keys (`{prefix}:{appId}:{channel}`). |
| `retention.default_max_messages` | `1000` | Default per-channel cap. |
| `retention.per_channel` | `[]` | fnmatch glob => max messages overrides. |
| `replay_id_field` | `_replay_id` | The `data` key that carries each message's stream id. |
| `underlying` | `reverb` | Name of the broadcaster the wrapper delegates to. |
| `replay_batch_size` | `100` | XRANGE page size during replay. |

## Requirements

- PHP 8.5+
- Laravel 13
- Resonate 0.4+
- Redis 5+ (Redis Streams; included in any modern Redis)

## Testing

```bash
composer test
```

Tests that touch Redis expect a server on `127.0.0.1:6379` and use database 15; they skip cleanly when no Redis is reachable.

## License

MIT. See [LICENSE](LICENSE).
