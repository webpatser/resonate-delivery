# Changelog

All notable changes to `webpatser/resonate-delivery` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
