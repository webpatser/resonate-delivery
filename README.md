# Resonate Delivery

At-least-once message delivery for [Resonate](https://github.com/webpatser/resonate) channels. Every broadcast is logged to a per-channel Redis Stream; a reconnecting client sends the id of the last message it saw, and the plugin replays everything since before live delivery resumes.

Solves the single most common WebSocket complaint: "I dropped for 20 seconds and missed messages."

## What you get

- A Laravel broadcaster (`resonate-delivery` driver) that wraps Reverb, logs every broadcast, and tags the payload with a monotonic `_replay_id`.
- A Resonate server plugin that, when a subscribe carries `last_event_id`, replays the missed messages on that channel after `subscription_succeeded` and before any live broadcast.
- Bounded retention on both axes: `MAXLEN ~ N` caps how long each stream gets, a refreshed TTL caps how long an idle stream key survives.

## What this is (and isn't)

- **At-least-once within retention.** With the default cap of 1000 messages per channel, a client that drops for 5 minutes on a low-traffic channel reconnects seamlessly. A client offline for hours on a high-traffic channel sees a clean break; design retention based on expected disconnect duration and your publish rate.
- **Ordered.** Everything replayed reaches the client before any live broadcast on that channel, and each side is in publish order. See [Ordering](#ordering).
- **Duplicates can happen at the seam.** A message published exactly as a reconnecting client is subscribing may arrive both in the replay and as a live broadcast. The live copy always arrives second, so it never reorders anything; the client deduplicates on the monotonic `_replay_id`.
- **Best-effort logging, never an availability risk.** If the delivery Redis is unreachable, the broadcast still goes out live; it is simply not logged and carries no `_replay_id`. A durability add-on must not be able to take down the broadcasting it was added to.
- **Bounded replays.** One replay buffers at most `replay_max_messages` (default 10000) messages for a connection; beyond that it is truncated and logged. The stream's own `MAXLEN` normally bites first.
- **No ACK protocol.** This is not a message queue. A subscriber that goes away forever does not hold a slot in any per-subscriber outbox. Per-subscriber state lives only on the connection.
- **Server broadcasts only.** `client-*` whispers between clients are not logged; they reach connected subscribers only.

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

## Ordering

The guarantee is that a reconnecting client sees every replayed message before any live broadcast on that channel, with no interleaving.

That is a stronger claim than it looks, because the replay reads Redis and the connection is a live socket the whole time. The plugin gets it by reading before the connection joins the channel, not after:

1. The `pusher:subscribe` interceptor reads the whole missed window into memory. The connection has not joined the channel yet, so a broadcast walking the channel's connection list cannot reach it; the reads can take as many round trips as they need.
2. Resonate then joins the connection, sends `subscription_succeeded`, and calls the plugin's `onSubscribe`, all as straight-line synchronous code. The plugin writes the buffered frames there and performs no I/O, so no other fiber can run in that window and wedge a live message into the middle of the replay.

A message published after the final read is broadcast by a fiber that cannot run until the flush is finished, so it arrives live, after the replay, in order. Nothing is dropped at that seam.

Two consequences worth knowing:

- A subscribe carrying `last_event_id` completes slightly later than a plain one, by roughly the time it takes to read the missed window. Only that connection waits; the event loop is never blocked.
- A connection that is already subscribed to the channel gets no replay, because it has missed nothing and replaying would push messages it has already seen behind ones it has not.

## Retention has two axes

`MAXLEN ~ N` bounds how long a single stream gets. It says nothing about how many streams exist, and a per-entity channel scheme (`private-orders.{id}`) mints a stream key per entity: broadcast to 100,000 orders and 100,000 keys stay in Redis forever, each holding up to `N` messages.

So every append also stamps `retention.ttl` on the key (default 7 days). Because it is refreshed on each append, an actively used channel never expires, while one that goes quiet is reclaimed a window later. Set `ttl` to `0` to keep every stream forever.

Pick a window longer than the longest disconnect you intend to support. A client returning after the TTL has elapsed finds an empty stream and gets no replay, exactly as if it had fallen out of the `MAXLEN` window.

## Configuration reference

| Key | Default | Purpose |
|-----|---------|---------|
| `connection` | `REDIS_*` env | Redis server hosting the streams. The broadcaster (predis) and the plugin (fledge async) must point at the same server/database. |
| `key_prefix` | `delivery` | Namespace for stream keys (`{prefix}:{appId}:{channel}`). |
| `retention.default_max_messages` | `1000` | Default per-channel cap. |
| `retention.per_channel` | `[]` | fnmatch glob => max messages overrides. |
| `retention.ttl` | `604800` | Seconds a stream key survives its last append, refreshed on every append. `0` never expires. |
| `replay_id_field` | `_replay_id` | The `data` key that carries each message's stream id. |
| `underlying` | `reverb` | Name of the broadcaster the wrapper delegates to. |
| `replay_batch_size` | `100` | XRANGE page size during replay. |
| `replay_max_messages` | `10000` | Most messages one replay buffers for a connection; beyond it the replay is truncated and logged. `0` removes the cap. |

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
