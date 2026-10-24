<?php

use Fledge\Async\Redis\RedisClient;
use Predis\Client;
use Revolt\EventLoop;
use Webpatser\Resonate\Contracts\ApplicationProvider;
use Webpatser\Resonate\Contracts\Connection;
use Webpatser\Resonate\Plugins\PluginContext;
use Webpatser\Resonate\Plugins\PluginManager;
use Webpatser\Resonate\Protocols\Pusher\Channels\Channel;
use Webpatser\Resonate\Protocols\Pusher\Contracts\ChannelManager;
use Webpatser\Resonate\Protocols\Pusher\EventDispatcher;
use Webpatser\Resonate\Protocols\Pusher\Server;
use Webpatser\ResonateDelivery\MessageLog;
use Webpatser\ResonateDelivery\MessageReplayPlugin;
use Webpatser\ResonateDelivery\Tests\Support\FakeConnection;

/*
 * A subscribe carrying `last_event_id` must not cost the server a stream read
 * before core has authorized it. Subscribes run through the real
 * Server::message() path, so core's authorization decides what reaches the
 * plugin's onSubscribe.
 */

beforeEach(function () {
    if (! redisReachable()) {
        $this->markTestSkipped('Redis not reachable');
    }

    $this->redis = new Client(['host' => '127.0.0.1', 'port' => 6379, 'database' => 15]);

    foreach ($this->redis->keys('delivery-test:*') as $key) {
        $this->redis->del($key);
    }

    $this->application = app(ApplicationProvider::class)->findById('app-id');
});

afterEach(function () {
    if (isset($this->redis)) {
        foreach ($this->redis->keys('delivery-test:*') as $key) {
            $this->redis->del($key);
        }
    }
});

/**
 * Boot a replay plugin that counts its stream reads, and register it with the server.
 */
function authReplayPlugin(): MessageReplayPlugin
{
    $plugin = new class extends MessageReplayPlugin
    {
        public int $reads = 0;

        protected function replay(Connection $connection, Channel $channel, string $lastId, RedisClient $redis): void
        {
            $this->reads++;

            parent::replay($connection, $channel, $lastId, $redis);
        }
    };

    $plugin->boot(new PluginContext(app(ChannelManager::class)));

    app(PluginManager::class)->register($plugin);

    return $plugin;
}

/**
 * Send a `pusher:subscribe` through the server.
 *
 * @param  array<string, mixed>  $data
 */
function authSubscribe(FakeConnection $connection, array $data): void
{
    app(Server::class)->message($connection, (string) json_encode(['event' => 'pusher:subscribe', 'data' => $data]));
}

/**
 * Log a message on a channel and broadcast it live, the way the replay broadcaster does.
 *
 * @param  array<string, mixed>  $data
 */
function authLive(string $channel, string $event, array $data): string
{
    $id = app(MessageLog::class)->append('app-id', $channel, $event, $data);

    EventDispatcher::dispatchSynchronously(
        app(ApplicationProvider::class)->findById('app-id'),
        ['event' => $event, 'channel' => $channel, 'data' => (string) json_encode($data + ['_replay_id' => $id])],
    );

    return $id;
}

/**
 * Decode the connection's frames; a replayed frame carries its data inline, a live one as a JSON string.
 *
 * @return list<array{event: string, replayed: bool, data: array<string, mixed>}>
 */
function authFrames(FakeConnection $connection): array
{
    return array_map(static function (string $message): array {
        $frame = (array) json_decode($message, true);
        $data = $frame['data'] ?? [];

        return [
            'event' => (string) ($frame['event'] ?? ''),
            'replayed' => is_array($data),
            'data' => is_string($data) ? (array) json_decode($data, true) : (array) $data,
        ];
    }, $connection->messages);
}

it('reads no stream and buffers no frames for a flood of rejected subscribes', function () {
    $log = app(MessageLog::class);

    foreach (range(1, 20) as $n) {
        foreach (range(1, 5) as $i) {
            $log->append('app-id', 'private-orders.'.$n, 'OrderUpdated', ['order' => $n, 'i' => $i]);
        }
    }

    $attacker = new FakeConnection('attacker-1', $this->application);

    runLoop(function () use ($attacker) {
        $plugin = authReplayPlugin();

        foreach (range(1, 500) as $n) {
            authSubscribe($attacker, [
                'channel' => 'private-orders.'.(($n % 20) + 1).'.'.$n,
                'auth' => 'app-key:not-a-valid-signature',
                'last_event_id' => '0-0',
            ]);
        }

        foreach (range(1, 20) as $n) {
            authSubscribe($attacker, [
                'channel' => 'private-orders.'.$n,
                'auth' => 'app-key:not-a-valid-signature',
                'last_event_id' => '0-0',
            ]);
        }

        expect($plugin->reads)->toBe(0);
    });

    $events = array_unique(array_column(authFrames($attacker), 'event'));
    $cursors = $attacker->state('delivery.cursors');

    // Only the error replies went out, and what the connection holds is the
    // newest `replay_max_pending` cursors: a few bytes each, no stream frames.
    expect(array_values($events))->toBe(['pusher:error'])
        ->and($attacker->hasState('delivery.replay'))->toBeFalse()
        ->and($cursors)->toHaveCount(100)
        ->and(array_key_last($cursors))->toBe('private-orders.20')
        ->and(array_values(array_unique($cursors)))->toBe(['0-0']);
});

it('replays the window once core authorized the subscribe', function () {
    $log = app(MessageLog::class);

    $cursor = $log->append('app-id', 'private-orders.1', 'A', ['n' => 1]);
    $second = $log->append('app-id', 'private-orders.1', 'B', ['n' => 2]);

    $connection = new FakeConnection('sock-1', $this->application);

    runLoop(function () use ($connection, $cursor) {
        $plugin = authReplayPlugin();

        authSubscribe($connection, [
            'channel' => 'private-orders.1',
            'auth' => 'app-key:'.hash_hmac('sha256', 'sock-1:private-orders.1', 'app-secret'),
            'last_event_id' => $cursor,
        ]);

        expect($plugin->reads)->toBe(1);
    });

    $frames = authFrames($connection);

    expect(array_column($frames, 'event'))->toBe(['pusher_internal:subscription_succeeded', 'B'])
        ->and($frames[1]['replayed'])->toBeTrue()
        ->and($frames[1]['data']['_replay_id'])->toBe($second)
        ->and($connection->state('delivery.cursors'))->toBe([]);
});

it('leaves no gap and no inversion when messages are published during the replay read', function () {
    // One entry per XRANGE page, so the read suspends between every message.
    config()->set('resonate-delivery.replay_batch_size', 1);

    $log = app(MessageLog::class);

    $cursor = $log->append('app-id', 'orders', 'A', ['n' => 1]);
    $log->append('app-id', 'orders', 'B', ['n' => 2]);
    $log->append('app-id', 'orders', 'C', ['n' => 3]);
    $log->append('app-id', 'orders', 'D', ['n' => 4]);

    $connection = new FakeConnection('sock-1', $this->application);

    runLoop(function () use ($connection, $cursor) {
        authReplayPlugin();

        // Published while the replay is being read: once at the first
        // suspension, once a little later, both after the connection joined.
        EventLoop::queue(static function (): void {
            authLive('orders', 'E', ['n' => 5]);
        });
        EventLoop::delay(0.001, static function (): void {
            authLive('orders', 'F', ['n' => 6]);
        });

        authSubscribe($connection, ['channel' => 'orders', 'last_event_id' => $cursor]);

        settle();

        authLive('orders', 'G', ['n' => 7]);
    });

    $frames = authFrames($connection);

    expect($frames[0]['event'])->toBe('pusher_internal:subscription_succeeded');

    $rest = array_slice($frames, 1);
    $replayed = array_keys(array_filter($rest, fn (array $frame) => $frame['replayed']));
    $live = array_keys(array_filter($rest, fn (array $frame) => ! $frame['replayed']));

    // Every replayed frame precedes every live one.
    expect($replayed)->not->toBeEmpty()
        ->and($live)->not->toBeEmpty()
        ->and(max($replayed))->toBeLessThan(min($live));

    // Deduplicated on _replay_id, the client sees the whole stream after its
    // cursor exactly once and in order: nothing was lost at the seam.
    $seen = [];

    foreach ($rest as $frame) {
        $id = $frame['data']['_replay_id'] ?? null;

        expect($id)->toBeString();

        if (! in_array($id, $seen, true)) {
            $seen[] = $id;
        }
    }

    expect($seen)->toBe(array_column($log->replaySince('app-id', 'orders', $cursor), 'id'))
        ->and(array_column(array_slice($rest, 0, 3), 'event'))->toBe(['B', 'C', 'D']);
});
