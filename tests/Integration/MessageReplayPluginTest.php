<?php

use Predis\Client;
use Webpatser\Resonate\Contracts\ApplicationProvider;
use Webpatser\Resonate\Plugins\MessageDisposition;
use Webpatser\Resonate\Plugins\PluginContext;
use Webpatser\Resonate\Protocols\Pusher\Contracts\ChannelManager;
use Webpatser\ResonateDelivery\MessageLog;
use Webpatser\ResonateDelivery\MessageReplayPlugin;
use Webpatser\ResonateDelivery\Tests\Support\FakeConnection;

beforeEach(function () {
    if (! redisReachable()) {
        $this->markTestSkipped('Redis not reachable');
    }

    $this->redis = new Client(['host' => '127.0.0.1', 'port' => 6379, 'database' => 15]);

    foreach ($this->redis->keys('delivery-test:*') as $key) {
        $this->redis->del($key);
    }
});

afterEach(function () {
    if (isset($this->redis)) {
        foreach ($this->redis->keys('delivery-test:*') as $key) {
            $this->redis->del($key);
        }
    }
});

it('relays a subscribe with no last_event_id and replays nothing', function () {
    $plugin = new MessageReplayPlugin;
    $app = app(ApplicationProvider::class)->findById('app-id');
    $connection = new FakeConnection('sock-1', $app);

    runLoop(function () use ($plugin, $connection) {
        $plugin->boot(new PluginContext(app(ChannelManager::class)));

        $disposition = $plugin->onMessage($connection, [
            'event' => 'pusher:subscribe',
            'data' => ['channel' => 'private-foo'],
        ]);

        expect($disposition)->toBe(MessageDisposition::Relay)
            ->and($connection->state('delivery.replay', []))->toBe([]);
    });

    expect($connection->messages)->toBe([]);
});

it('only remembers the cursor at intercept time, before core authorized the subscribe', function () {
    $plugin = new MessageReplayPlugin;
    $app = app(ApplicationProvider::class)->findById('app-id');
    $connection = new FakeConnection('sock-1', $app);
    $log = app(MessageLog::class);

    $first = $log->append('app-id', 'private-foo', 'A', ['n' => 1]);
    $log->append('app-id', 'private-foo', 'B', ['n' => 2]);

    runLoop(function () use ($plugin, $connection, $first) {
        $plugin->boot(new PluginContext(app(ChannelManager::class)));

        $disposition = $plugin->onMessage($connection, [
            'event' => 'pusher:subscribe',
            'data' => ['channel' => 'private-foo', 'last_event_id' => $first],
        ]);

        // Nothing is read and nothing is buffered: only the cursor waits for
        // onSubscribe, which core calls for an authorized subscribe only.
        expect($disposition)->toBe(MessageDisposition::Relay)
            ->and($connection->state('delivery.cursors'))->toBe(['private-foo' => $first])
            ->and($connection->hasState('delivery.replay'))->toBeFalse()
            ->and($connection->messages)->toBe([]);
    });
});

it('caps the pending cursors per connection and drops the oldest first', function () {
    config()->set('resonate-delivery.replay_max_pending', 3);

    $plugin = new MessageReplayPlugin;
    $connection = new FakeConnection('sock-1', app(ApplicationProvider::class)->findById('app-id'));

    runLoop(function () use ($plugin, $connection) {
        $plugin->boot(new PluginContext(app(ChannelManager::class)));

        foreach (range(1, 10) as $n) {
            $plugin->onMessage($connection, [
                'event' => 'pusher:subscribe',
                'data' => ['channel' => 'private-orders.'.$n, 'last_event_id' => '0-0'],
            ]);
        }

        // A repeat moves its channel to the newest slot.
        $plugin->onMessage($connection, [
            'event' => 'pusher:subscribe',
            'data' => ['channel' => 'private-orders.8', 'last_event_id' => '5-0'],
        ]);
    });

    expect($connection->state('delivery.cursors'))->toBe([
        'private-orders.9' => '0-0',
        'private-orders.10' => '0-0',
        'private-orders.8' => '5-0',
    ]);
});

it('ignores a malformed last_event_id and drops an earlier cursor for the channel', function (mixed $cursor) {
    $plugin = new MessageReplayPlugin;
    $connection = new FakeConnection('sock-1', app(ApplicationProvider::class)->findById('app-id'));

    runLoop(function () use ($plugin, $connection, $cursor) {
        $plugin->boot(new PluginContext(app(ChannelManager::class)));

        $plugin->onMessage($connection, [
            'event' => 'pusher:subscribe',
            'data' => ['channel' => 'private-foo', 'last_event_id' => '0-0'],
        ]);

        $plugin->onMessage($connection, [
            'event' => 'pusher:subscribe',
            'data' => ['channel' => 'private-foo', 'last_event_id' => $cursor],
        ]);
    });

    expect($connection->state('delivery.cursors'))->toBe([]);
})->with([
    'empty' => '',
    'not an id' => 'latest',
    'redis special id' => '+',
    'trailing garbage' => '1-0 extra',
    'oversized' => str_repeat('9', 300),
    'not a string' => 12,
]);

it('replays every message newer than the stashed cursor on onSubscribe', function () {
    $plugin = new MessageReplayPlugin;
    $app = app(ApplicationProvider::class)->findById('app-id');
    $connection = new FakeConnection('sock-1', $app);
    $log = app(MessageLog::class);

    $first = $log->append('app-id', 'private-foo', 'A', ['n' => 1]);
    $second = $log->append('app-id', 'private-foo', 'B', ['n' => 2]);
    $third = $log->append('app-id', 'private-foo', 'C', ['n' => 3]);

    $manager = app(ChannelManager::class);
    $channel = $manager->for($app)->findOrCreate('private-foo');

    runLoop(function () use ($plugin, $connection, $channel, $first) {
        $plugin->boot(new PluginContext(app(ChannelManager::class)));

        // Pretend the client just resubscribed after seeing $first.
        $plugin->onMessage($connection, [
            'event' => 'pusher:subscribe',
            'data' => ['channel' => 'private-foo', 'last_event_id' => $first],
        ]);

        $plugin->onSubscribe($connection, $channel);
    });

    expect($connection->messages)->toHaveCount(2);

    $decoded = array_map(fn ($m) => json_decode($m, true), $connection->messages);

    expect($decoded[0]['event'])->toBe('B')
        ->and($decoded[0]['channel'])->toBe('private-foo')
        ->and($decoded[0]['data']['n'])->toBe(2)
        ->and($decoded[0]['data']['_replay_id'])->toBe($second)
        ->and($decoded[1]['event'])->toBe('C')
        ->and($decoded[1]['data']['_replay_id'])->toBe($third);
});

it('does not replay twice if onSubscribe fires again after a fresh subscribe', function () {
    $plugin = new MessageReplayPlugin;
    $app = app(ApplicationProvider::class)->findById('app-id');
    $connection = new FakeConnection('sock-1', $app);
    $log = app(MessageLog::class);

    $log->append('app-id', 'private-foo', 'A', []);
    $log->append('app-id', 'private-foo', 'B', []);

    $channel = app(ChannelManager::class)->for($app)->findOrCreate('private-foo');

    runLoop(function () use ($plugin, $connection, $channel) {
        $plugin->boot(new PluginContext(app(ChannelManager::class)));

        $plugin->onMessage($connection, [
            'event' => 'pusher:subscribe',
            'data' => ['channel' => 'private-foo', 'last_event_id' => '0-0'],
        ]);

        $plugin->onSubscribe($connection, $channel);
        $plugin->onSubscribe($connection, $channel); // second time: no stash, no replay
    });

    expect($connection->messages)->toHaveCount(2);
});

it('skips replay when nothing is newer than the cursor', function () {
    $plugin = new MessageReplayPlugin;
    $app = app(ApplicationProvider::class)->findById('app-id');
    $connection = new FakeConnection('sock-1', $app);
    $log = app(MessageLog::class);

    $last = $log->append('app-id', 'private-foo', 'A', []);
    $channel = app(ChannelManager::class)->for($app)->findOrCreate('private-foo');

    runLoop(function () use ($plugin, $connection, $channel, $last) {
        $plugin->boot(new PluginContext(app(ChannelManager::class)));

        $plugin->onMessage($connection, [
            'event' => 'pusher:subscribe',
            'data' => ['channel' => 'private-foo', 'last_event_id' => $last],
        ]);

        $plugin->onSubscribe($connection, $channel);
    });

    expect($connection->messages)->toBe([]);
});

it('forgets pending cursors entirely on connection close', function () {
    $plugin = new MessageReplayPlugin;
    $app = app(ApplicationProvider::class)->findById('app-id');
    $connection = new FakeConnection('sock-1', $app);

    runLoop(function () use ($plugin, $connection) {
        $plugin->boot(new PluginContext(app(ChannelManager::class)));

        $plugin->onMessage($connection, [
            'event' => 'pusher:subscribe',
            'data' => ['channel' => 'private-foo', 'last_event_id' => '0-0'],
        ]);

        $plugin->onClose($connection);
    });

    expect($connection->hasState('delivery.cursors'))->toBeFalse();
});

it('never remembers a cursor for a private-encrypted channel', function () {
    $plugin = new MessageReplayPlugin;
    $app = app(ApplicationProvider::class)->findById('app-id');
    $connection = new FakeConnection('sock-1', $app);

    // A stream written by an older release that still logged encrypted channels.
    app(MessageLog::class)->append('app-id', 'private-encrypted-foo', 'Secret', ['pin' => '1234']);

    $channel = app(ChannelManager::class)->for($app)->findOrCreate('private-encrypted-foo');

    runLoop(function () use ($plugin, $connection, $channel) {
        $plugin->boot(new PluginContext(app(ChannelManager::class)));

        $plugin->onMessage($connection, [
            'event' => 'pusher:subscribe',
            'data' => ['channel' => 'private-encrypted-foo', 'last_event_id' => '0-0'],
        ]);

        expect($connection->state('delivery.cursors', []))->toBe([]);

        $plugin->onSubscribe($connection, $channel);
    });

    expect($connection->messages)->toBe([]);
});

it('stops replaying once a connection used up its replays for the window', function () {
    config()->set('resonate-delivery.replay_budget_replays', 3);
    config()->set('resonate-delivery.replay_budget_messages', 0);

    $plugin = new MessageReplayPlugin;
    $app = app(ApplicationProvider::class)->findById('app-id');
    $connection = new FakeConnection('sock-1', $app);
    $log = app(MessageLog::class);

    $log->append('app-id', 'private-foo', 'A', []);
    $log->append('app-id', 'private-foo', 'B', []);

    $channel = app(ChannelManager::class)->for($app)->findOrCreate('private-foo');

    runLoop(function () use ($plugin, $connection, $channel) {
        $plugin->boot(new PluginContext(app(ChannelManager::class)));

        // The amplification loop: subscribe from the start, unsubscribe, repeat.
        foreach (range(1, 10) as $n) {
            $plugin->onMessage($connection, [
                'event' => 'pusher:subscribe',
                'data' => ['channel' => 'private-foo', 'last_event_id' => '0'],
            ]);
            $plugin->onSubscribe($connection, $channel);
            $plugin->onMessage($connection, [
                'event' => 'pusher:unsubscribe',
                'data' => ['channel' => 'private-foo'],
            ]);
        }
    });

    // Three replays of two messages each, not ten.
    expect($connection->messages)->toHaveCount(6);
});

it('stops replaying once a connection used up its message budget for the window', function () {
    config()->set('resonate-delivery.replay_budget_replays', 0);
    config()->set('resonate-delivery.replay_budget_messages', 5);

    $plugin = new MessageReplayPlugin;
    $app = app(ApplicationProvider::class)->findById('app-id');
    $connection = new FakeConnection('sock-1', $app);
    $log = app(MessageLog::class);

    foreach (['A', 'B', 'C'] as $event) {
        $log->append('app-id', 'private-foo', $event, []);
    }

    $channel = app(ChannelManager::class)->for($app)->findOrCreate('private-foo');

    runLoop(function () use ($plugin, $connection, $channel) {
        $plugin->boot(new PluginContext(app(ChannelManager::class)));

        foreach (range(1, 4) as $n) {
            $plugin->onMessage($connection, [
                'event' => 'pusher:subscribe',
                'data' => ['channel' => 'private-foo', 'last_event_id' => '0'],
            ]);
            $plugin->onSubscribe($connection, $channel);
        }
    });

    // 3 + 2, then the budget is spent: 5 messages instead of 12.
    expect($connection->messages)->toHaveCount(5);
});

it('replays without limit when both budgets are disabled', function () {
    config()->set('resonate-delivery.replay_budget_replays', 0);
    config()->set('resonate-delivery.replay_budget_messages', 0);

    $plugin = new MessageReplayPlugin;
    $app = app(ApplicationProvider::class)->findById('app-id');
    $connection = new FakeConnection('sock-1', $app);

    app(MessageLog::class)->append('app-id', 'private-foo', 'A', []);

    $channel = app(ChannelManager::class)->for($app)->findOrCreate('private-foo');

    runLoop(function () use ($plugin, $connection, $channel) {
        $plugin->boot(new PluginContext(app(ChannelManager::class)));

        foreach (range(1, 5) as $n) {
            $plugin->onMessage($connection, [
                'event' => 'pusher:subscribe',
                'data' => ['channel' => 'private-foo', 'last_event_id' => '0'],
            ]);
            $plugin->onSubscribe($connection, $channel);
        }
    });

    expect($connection->messages)->toHaveCount(5);
});

it('drops a pending cursor for a channel the connection leaves', function () {
    $plugin = new MessageReplayPlugin;
    $app = app(ApplicationProvider::class)->findById('app-id');
    $connection = new FakeConnection('sock-1', $app);
    $log = app(MessageLog::class);

    $log->append('app-id', 'private-foo', 'A', []);

    $channel = app(ChannelManager::class)->for($app)->findOrCreate('private-foo');

    runLoop(function () use ($plugin, $connection, $channel) {
        $plugin->boot(new PluginContext(app(ChannelManager::class)));

        $plugin->onMessage($connection, [
            'event' => 'pusher:subscribe',
            'data' => ['channel' => 'private-foo', 'last_event_id' => '0-0'],
        ]);

        $plugin->onUnsubscribe($connection, $channel);
        $plugin->onSubscribe($connection, $channel);
    });

    expect($connection->messages)->toBe([]);
});
