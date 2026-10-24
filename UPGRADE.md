# Upgrade guide

## From 0.7 to 1.0

1.0 is a constraint bump: there are no API, config or behaviour changes.

1. Upgrade `webpatser/resonate` to `^1.0` together with this package:

   ```bash
   composer require webpatser/resonate:^1.0 webpatser/resonate-delivery:^1.0
   ```

2. Nothing else changes. Existing configuration, published config files and environment variables keep working.

Resonate 0.7 is no longer supported by this release line; stay on `0.7.x` if you cannot move to resonate 1.0 yet.
