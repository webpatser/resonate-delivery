# Changelog

All notable changes to `webpatser/resonate-delivery` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
