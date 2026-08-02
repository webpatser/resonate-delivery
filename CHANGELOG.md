# Changelog

All notable changes to `webpatser/resonate-delivery` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-08-02

### Added

- `retention.ttl`: seconds a stream key survives its last append, refreshed on
  every append (default `604800`, `0` never expires).
- `replay_max_messages`: most messages one replay buffers for a connection
  (default `10000`, `0` removes the cap).
- `replay_batch_size` to the published config; it was documented in the README
  only.
- `DeliveryRetention::ttl()`.

### Changed

- Read the missed window in the `pusher:subscribe` interceptor and flush the
  buffered frames in `onSubscribe`.
- A subscribe carrying `last_event_id` completes only after its missed window
  has been read, so it takes slightly longer than a plain subscribe. Only that
  connection waits; the event loop is not blocked.
- A connection already subscribed to a channel gets no replay when it
  resubscribes with a cursor.
- A broadcast whose log write failed is delivered without a `_replay_id` field.
- `DeliveryRetention::__construct()` takes a third argument, `$ttl`, defaulting
  to `0`.
- The plugin's connection state key is `delivery.replay` (buffered frames per
  channel) instead of `delivery.subscribes` (a cursor per channel).

### Fixed

- Replayed messages now reach the client before any live broadcast on that
  channel, the ordering the README and the class docblock already claimed.
- A failed log write no longer aborts the broadcast; the append is caught per
  channel and the event still reaches the underlying broadcaster.

### Upgrading

No configuration change is required to upgrade. Four things behave differently.

**Replay ordering is now what the documentation said it was.** The Redis reads
moved out of `onSubscribe`, where each `XRANGE` round trip let another fiber
deliver a live broadcast into the middle of the replay, and into the
`pusher:subscribe` interceptor, which runs before the connection joins the
channel. The buffered frames are flushed in `onSubscribe`, which performs no
I/O and therefore cannot suspend. Replayed messages arrive after
`subscription_succeeded` and before any live broadcast; a client that saw
replay and live traffic interleaved will stop seeing it.

**Streams expire.** `retention.ttl` (default `604800`, seven days) is stamped on
the key on every append. `MAXLEN` bounds how long a single stream gets, never
how many streams exist, so per-entity channels such as `private-orders.{id}`
accumulated keys in Redis for the life of the instance. An actively written
channel never expires; a quiet one is reclaimed a window later. Set `0` to
restore the previous unbounded behaviour, and pick a window longer than the
longest disconnect you intend to support.

**Delivery Redis is no longer an availability dependency.** An unreachable
Redis used to throw out of every `broadcast()` call and stop live broadcasting
for the whole application. The append is now caught per channel: the failure is
logged and the event is delegated anyway, without a `_replay_id`. The absent
field is the signal to clients, which advance their cursor only on messages
that carry one.

**Replays are bounded.** `replay_max_messages` (default `10000`) caps how many
messages one replay buffers for a connection; beyond it the replay is truncated
and a warning logged. This only bites when `MAXLEN` retention is set far higher
than a reconnecting client could use. Set `0` to remove the cap.

## [0.1.2] - 2026-07-30

### Changed

- Widen the `webpatser/resonate` constraint to `^0.4|^0.5`. Composer treats a
  `^0.4` caret on a 0.x package as `>=0.4 <0.5`, so this package could not be
  installed next to a server running Resonate v0.5 even though the suite passes
  against it. Both major lines are now accepted.

## [0.1.1] - 2026-07-30

### Fixed

- `MessageLog::decode()` carried an array-shape docblock that did not match what
  it returns.
- `Broadcasting\ReplayBroadcaster::broadcast()` narrowed its object cast to
  `Stringable` instead of casting an arbitrary object.
- `MessageReplayPlugin::replay()` no longer re-reads the nullable `RedisClient`
  property. The already null-checked client is passed in from the caller.

### Changed

- CI runs the suite against a `redis:7` service container mapped to
  `127.0.0.1:6379`. The `fsockopen()` probe in `tests/Pest.php` had been
  self-skipping most of the suite in the pipeline; all 19 tests now run.
- CI gates on Laravel Pint and on PHPStan at level 8, with no baseline and no
  suppressions. Plain `phpstan/phpstan` is used rather than larastan, whose
  Testbench bootstrap crashes under this package's `fledge-fiber` dependency
  (`FiberHttpServiceProvider` calls `Illuminate\Http\Client\Factory::globalHandler`,
  which does not exist outside a Fledge application).
- Added a tests and dependency-audit workflow: the suite runs on PHP 8.5 and
  `composer audit` fails the build on known-vulnerable production dependencies,
  on push to the default branch and on pull requests.

## [0.1.0] - 2026-05-25

Initial release.

### Added

- `Broadcasting\ReplayBroadcaster`: a Laravel broadcaster that wraps an
  underlying driver (typically Reverb), appends every broadcast to a per-
  channel Redis Stream (`XADD MAXLEN ~ N`), and decorates the payload's
  `data` with `_replay_id` so the client can resume from there on reconnect.
- `MessageReplayPlugin`: a Resonate server plugin that intercepts
  `pusher:subscribe` carrying `last_event_id`, stashes the cursor on the
  connection, and replays every missed message after `subscription_succeeded`
  via `XRANGE`.
- `MessageLog`: predis-backed append + replay surface, shared by the
  broadcaster and any host-side consumer.
- `DeliveryKeys` and `DeliveryRetention`: stream key schema and per-channel
  retention policy (default + fnmatch patterns).
- `DeliveryServiceProvider`: registers the `resonate-delivery` broadcasting
  driver and binds the support services; publishes config via
  `vendor:publish --tag=resonate-delivery-config`.

### Semantics

- **At-least-once within retention.** A client offline longer than the
  configured window sees a clean break, not a partial replay.
- **Duplicates possible at the seam.** A message published just as a
  reconnecting client is mid-replay may arrive both ways; clients dedup by
  the monotonic `_replay_id`.
- **Server broadcasts only.** `client-*` whispers are not logged in v0.1.

[Unreleased]: https://github.com/webpatser/resonate-delivery/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/webpatser/resonate-delivery/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/webpatser/resonate-delivery/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/webpatser/resonate-delivery/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/webpatser/resonate-delivery/releases/tag/v0.1.0
