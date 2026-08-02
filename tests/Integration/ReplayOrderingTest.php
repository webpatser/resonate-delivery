<?php

use Predis\Client;
use Revolt\EventLoop;
use Webpatser\Resonate\Contracts\ApplicationProvider;
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

    // One entry per XRANGE page, so a replay of several messages takes several
    // round trips. Every one of those is a chance for another fiber to run,
    // which is precisely the window these tests are about.
    config()->set('resonate-delivery.replay_batch_size', 1);
});

afterEach(function () {
    if (isset($this->redis)) {
        foreach ($this->redis->keys('delivery-test:*') as $key) {
            $this->redis->del($key);
        }
    }
});

/**
 * Decode the connection's frames into `event => data` pairs, in send order.
 *
 * @return list<array{event: string, data: array<string, mixed>}>
 */
function framesOf(FakeConnection $connection): array
{
    return array_map(static function (string $message): array {
        $decoded = json_decode($message, true);

        return [
            'event' => $decoded['event'] ?? '',
            'data' => $decoded['data'] ?? [],
        ];
    }, $connection->messages);
}

it('delivers every replayed message before a live broadcast that races the replay', function () {
    $app = app(ApplicationProvider::class)->findById('app-id');
    $connection = new FakeConnection('sock-1', $app);
    $log = app(MessageLog::class);

    $first = $log->append('app-id', 'orders', 'A', ['n' => 1]);
    $log->append('app-id', 'orders', 'B', ['n' => 2]);
    $log->append('app-id', 'orders', 'C', ['n' => 3]);
    $log->append('app-id', 'orders', 'D', ['n' => 4]);

    $channel = app(ChannelManager::class)->for($app)->findOrCreate('orders');

    runLoop(function () use ($connection, $channel, $first) {
        $plugin = new MessageReplayPlugin;
        $plugin->boot(new PluginContext(app(ChannelManager::class)));

        // 1. The client resubscribes, telling us it last saw $first.
        $plugin->onMessage($connection, [
            'event' => 'pusher:subscribe',
            'data' => ['channel' => 'orders', 'last_event_id' => $first],
        ]);

        // 2. Core joins the connection to the channel and sends
        //    subscription_succeeded. From here the connection is live.
        $channel->subscribe($connection);

        // 3. Another fiber has a broadcast ready to go out on this channel.
        //    It runs at the first suspension point, whenever that comes.
        EventLoop::queue(static fn () => $channel->broadcast([
            'event' => 'LIVE',
            'channel' => 'orders',
            'data' => ['n' => 99],
        ]));

        // 4. The plugin flushes the replay.
        $plugin->onSubscribe($connection, $channel);

        // Let the queued broadcast run before we assert.
        settle();
    });

    $frames = framesOf($connection);

    expect(array_column($frames, 'event'))->toBe(['B', 'C', 'D', 'LIVE']);

    // The replayed frames carry a cursor; the live one is delivered as-is.
    expect($frames[0]['data'])->toHaveKey('_replay_id')
        ->and($frames[3]['data'])->not->toHaveKey('_replay_id');
});

it('holds the replay back even when the live broadcast is queued before the subscribe', function () {
    $app = app(ApplicationProvider::class)->findById('app-id');
    $connection = new FakeConnection('sock-1', $app);
    $log = app(MessageLog::class);

    $first = $log->append('app-id', 'orders', 'A', ['n' => 1]);
    $log->append('app-id', 'orders', 'B', ['n' => 2]);
    $log->append('app-id', 'orders', 'C', ['n' => 3]);

    $channel = app(ChannelManager::class)->for($app)->findOrCreate('orders');

    runLoop(function () use ($connection, $channel, $first) {
        $plugin = new MessageReplayPlugin;
        $plugin->boot(new PluginContext(app(ChannelManager::class)));

        // The broadcast is pending before the plugin does any work at all. It
        // reaches nobody while it runs, because the connection has not joined
        // the channel yet, which is what makes the read phase safe to suspend.
        EventLoop::queue(static fn () => $channel->broadcast([
            'event' => 'EARLY',
            'channel' => 'orders',
            'data' => ['n' => 98],
        ]));

        $plugin->onMessage($connection, [
            'event' => 'pusher:subscribe',
            'data' => ['channel' => 'orders', 'last_event_id' => $first],
        ]);

        $channel->subscribe($connection);

        $plugin->onSubscribe($connection, $channel);

        settle();
    });

    expect(array_column(framesOf($connection), 'event'))->toBe(['B', 'C']);
});

it('skips the replay for a connection already subscribed to the channel', function () {
    $app = app(ApplicationProvider::class)->findById('app-id');
    $connection = new FakeConnection('sock-1', $app);
    $log = app(MessageLog::class);

    $first = $log->append('app-id', 'orders', 'A', ['n' => 1]);
    $log->append('app-id', 'orders', 'B', ['n' => 2]);

    $channel = app(ChannelManager::class)->for($app)->findOrCreate('orders');

    runLoop(function () use ($connection, $channel, $first) {
        $plugin = new MessageReplayPlugin;
        $plugin->boot(new PluginContext(app(ChannelManager::class)));

        // Already live on the channel: it has missed nothing, so replaying
        // would push messages it has already seen behind ones it has not.
        $channel->subscribe($connection);

        $plugin->onMessage($connection, [
            'event' => 'pusher:subscribe',
            'data' => ['channel' => 'orders', 'last_event_id' => $first],
        ]);

        $plugin->onSubscribe($connection, $channel);

        settle();
    });

    expect($connection->messages)->toBe([]);
});
