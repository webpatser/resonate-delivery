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

it('buffers the replay at intercept time and sends nothing before the subscribe', function () {
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

        // The read is done, the frames are held, and the connection has not
        // been written to: it has not joined the channel yet.
        expect($disposition)->toBe(MessageDisposition::Relay)
            ->and($connection->state('delivery.replay'))->toHaveKey('private-foo')
            ->and($connection->state('delivery.replay')['private-foo'])->toHaveCount(1)
            ->and($connection->messages)->toBe([]);
    });
});

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

it('forgets buffered replays entirely on connection close', function () {
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

    expect($connection->hasState('delivery.replay'))->toBeFalse();
});

it('drops a buffered replay for a channel the connection leaves', function () {
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
