# Changelog

All notable changes to `webpatser/resonate-delivery` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **The replay ordering guarantee is now true.** `MessageReplayPlugin` read the
  Redis Stream from `onSubscribe`, by which point the connection had already
  joined the channel, so every `XRANGE` round trip was a window for another
  fiber to deliver a live broadcast to the same socket. A client could receive
  replayed 5 and 6, then live 12, then replayed 7 through 11, and `_replay_id`
  cannot repair an inversion the client has already applied. The reads now
  happen in the `pusher:subscribe` interceptor, before the connection joins the
  channel (where no broadcast can reach it), and the buffered frames are
  flushed in `onSubscribe`, which performs no I/O and therefore cannot be
  suspended. Both the README and the class docblock had documented the
  guarantee the code did not provide.
- `Broadcasting\ReplayBroadcaster::broadcast()` no longer lets a failed log
  write abort the broadcast. An unreachable delivery Redis took down live
  broadcasting for the whole application, so a durability add-on became an
  availability dependency. The append is now caught per channel: the failure is
  logged, and the event is still delegated to the underlying broadcaster.

### Added

- `retention.ttl`: the number of seconds a stream key survives its last append,
  refreshed on every append (default 7 days, `0` disables). `MAXLEN` bounds how
  long one stream gets but not how many streams exist, so per-entity channels
  (`private-orders.{id}`) accumulated keys in Redis forever.
- `replay_max_messages`: a cap on how many messages one replay buffers for a
  connection (default 10000, `0` removes it), and `replay_batch_size` is now
  present in the published config rather than documented only in the README.

### Changed

- **Behaviour.** A subscribe carrying `last_event_id` now completes after its
  missed window has been read, so it takes slightly longer than a plain
  subscribe. Only that connection waits; the event loop is not blocked.
- **Behaviour.** A connection already subscribed to a channel gets no replay
  when it resubscribes with a cursor. It has missed nothing, and replaying
  would order messages it has already seen behind ones it has not.
- **Behaviour.** A broadcast whose log write failed is delivered without a
  `_replay_id` field. The absent field is the signal: clients advance their
  cursor only on messages that carry one, so an unlogged message is never
  mistaken for a replayable one.
- **API.** `DeliveryRetention::__construct()` takes a third argument, `$ttl`
  (defaults to `0`, so existing callers are unaffected), and exposes
  `DeliveryRetention::ttl()`.
- **Internal.** The plugin's connection state key changed from
  `delivery.subscribes` (a cursor per channel) to `delivery.replay` (buffered
  frames per channel). Nothing outside the plugin reads it.

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

[Unreleased]: https://github.com/webpatser/resonate-delivery/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/webpatser/resonate-delivery/releases/tag/v0.1.0
