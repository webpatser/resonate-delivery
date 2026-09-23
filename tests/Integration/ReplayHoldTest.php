<?php

use Fledge\Async\DeferredFuture;
use Predis\Client;
use Webpatser\Resonate\Application;
use Webpatser\Resonate\Contracts\ApplicationProvider;
use Webpatser\Resonate\Contracts\Connection;
use Webpatser\Resonate\Contracts\ServerProvider;
use Webpatser\Resonate\Plugins\Contracts\ConnectionLifecycle;
use Webpatser\Resonate\Plugins\Contracts\ServerPlugin;
use Webpatser\Resonate\Plugins\MessageDisposition;
use Webpatser\Resonate\Plugins\PluginContext;
use Webpatser\Resonate\Plugins\PluginManager;
use Webpatser\Resonate\Protocols\Pusher\Channels\Channel;
use Webpatser\Resonate\Protocols\Pusher\Contracts\ChannelManager;
use Webpatser\Resonate\Protocols\Pusher\EventDispatcher;
use Webpatser\Resonate\Protocols\Pusher\MetricsHandler;
use Webpatser\Resonate\Protocols\Pusher\Server;
use Webpatser\Resonate\Scaling\Contracts\PubSubProvider;
use Webpatser\ResonateDelivery\MessageLog;
use Webpatser\ResonateDelivery\MessageReplayPlugin;
use Webpatser\ResonateDelivery\Tests\Support\FakeConnection;
use Webpatser\ResonateDelivery\Tests\Support\FakePubSubBus;
use Webpatser\ResonateDelivery\Tests\Support\ScaledServerProvider;

use function Fledge\Async\async;

/*
 * The replay against core's subscribe hold (Resonate 0.7.1+). On a scaled
 * presence channel the subscribe suspends while the fleet is asked about the
 * user, and a plugin ahead of the replay plugin may suspend in onSubscribe.
 * Broadcasts landing in either window are held by core, so the replay written
 * from onSubscribe still precedes them. Subscribes run through the real
 * Server::message() path, so the interceptor and the handler both run.
 */

beforeEach(function () {
    if (! redisReachable()) {
        $this->markTestSkipped('Redis not reachable');
    }

    $this->redis = new Client(['host' => '127.0.0.1', 'port' => 6379, 'database' => 15]);

    foreach ($this->redis->keys('delivery-test:*') as $key) {
        $this->redis->del($key);
    }

    // One entry per XRANGE page, so the read phase suspends several times.
    config()->set('resonate-delivery.replay_batch_size', 1);

    $this->bus = new FakePubSubBus;

    $this->app->instance(PubSubProvider::class, $this->bus);
    $this->app->instance(ServerProvider::class, new ScaledServerProvider);

    // What the rest of the fleet answers, set per test.
    $this->fleet = fn () => [];

    $this->application = app(ApplicationProvider::class)->findById('app-id');

    $metrics = new MetricsHandler(app(ChannelManager::class));

    $this->app->instance(MetricsHandler::class, $metrics);

    // Redis delivers a node its own publications, and one sibling node
    // answers every metrics request with what the fleet closure returns.
    $this->bus->receivers = 2;

    $application = $this->application;

    $this->bus->subscribers[] = fn (array $envelope) => $metrics->publish([
        'application' => $application,
        'payload' => $envelope['payload'],
    ]);

    $this->bus->subscribers[] = function (array $envelope) use ($application) {
        $request = $envelope['payload'] ?? [];

        if (! isset($request['type'], $request['key'])) {
            return;
        }

        $this->bus->publish([
            'type' => 'metrics',
            'application' => $application->id(),
            'payload' => [
                'key' => $request['key'],
                'node' => 'sibling-node',
                'metrics' => ($this->fleet)($application, $request['type'], $request['options'] ?? []),
            ],
        ]);
    };
});

afterEach(function () {
    if (isset($this->redis)) {
        foreach ($this->redis->keys('delivery-test:*') as $key) {
            $this->redis->del($key);
        }
    }
});

/**
 * Send `pusher:subscribe` for the presence room through the server, optionally with a replay cursor.
 */
function deliveryJoin(FakeConnection $connection, int $userId, ?string $lastEventId = null): void
{
    $channelData = (string) json_encode(['user_id' => $userId, 'user_info' => ['name' => 'Joe']]);

    $data = [
        'channel' => 'presence-room',
        'auth' => presenceAuth($connection->id(), 'presence-room', $channelData),
        'channel_data' => $channelData,
    ];

    if ($lastEventId !== null) {
        $data['last_event_id'] = $lastEventId;
    }

    app(Server::class)->message($connection, (string) json_encode(['event' => 'pusher:subscribe', 'data' => $data]));
}

/**
 * Send `pusher:unsubscribe` for the presence room through the server.
 */
function deliveryLeave(FakeConnection $connection): void
{
    app(Server::class)->message($connection, (string) json_encode([
        'event' => 'pusher:unsubscribe',
        'data' => ['channel' => 'presence-room'],
    ]));
}

/**
 * Boot the real replay plugin and register it with the server's plugin manager.
 */
function deliveryReplayPlugin(): MessageReplayPlugin
{
    $plugin = new MessageReplayPlugin;

    $plugin->boot(new PluginContext(app(ChannelManager::class)));

    app(PluginManager::class)->register($plugin);

    return $plugin;
}

/**
 * Register a lifecycle plugin that records its hooks and runs the given onSubscribe work.
 *
 * @param  (Closure(Connection, Channel): void)|null  $onSubscribe
 */
function deliveryLifecyclePlugin(?Closure $onSubscribe = null): ConnectionLifecycle
{
    $plugin = new class($onSubscribe) implements ConnectionLifecycle, ServerPlugin
    {
        /** @var array<int, string> */
        public array $hooks = [];

        public function __construct(protected ?Closure $work) {}

        public function boot(PluginContext $context): void {}

        public function onOpen(Connection $connection): void {}

        public function onClose(Connection $connection): void
        {
            $this->hooks[] = 'onClose';
        }

        public function onSubscribe(Connection $connection, Channel $channel): void
        {
            $this->hooks[] = 'onSubscribe '.$channel->name();

            if ($this->work !== null) {
                ($this->work)($connection, $channel);
            }
        }

        public function onUnsubscribe(Connection $connection, Channel $channel): void
        {
            $this->hooks[] = 'onUnsubscribe '.$channel->name();
        }
    };

    app(PluginManager::class)->register($plugin);

    return $plugin;
}

/**
 * Broadcast a live frame to the presence room the way the replay broadcaster does.
 *
 * A new message is appended to the stream first and carries the id it got;
 * passing an id rebroadcasts an already logged message instead.
 *
 * @param  array<string, mixed>  $data
 */
function deliveryLive(string $event, array $data, ?string $id = null): string
{
    $id ??= app(MessageLog::class)->append('app-id', 'presence-room', $event, $data);

    EventDispatcher::dispatchSynchronously(
        app(ApplicationProvider::class)->findById('app-id'),
        [
            'event' => $event,
            'channel' => 'presence-room',
            'data' => (string) json_encode($data + ['_replay_id' => $id]),
        ],
    );

    return $id;
}

/**
 * Decode the connection's frames in send order.
 *
 * The plugin writes a replayed frame's data inline, while a live Pusher frame
 * carries it as a JSON string, which is what tells the two apart here.
 *
 * @return list<array{event: string, replayed: bool, data: array<string, mixed>}>
 */
function deliveryFrames(FakeConnection $connection): array
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

/**
 * Yield the loop until the condition holds.
 */
function deliveryUntil(Closure $condition, float $timeout = 2.0): void
{
    $deadline = microtime(true) + $timeout;

    while (! $condition()) {
        if (microtime(true) > $deadline) {
            throw new RuntimeException('Condition not reached in time');
        }

        settle(0.005);
    }
}

it('replays before every held broadcast on a scaled presence channel behind a suspending plugin', function () {
    $log = app(MessageLog::class);

    $cursor = $log->append('app-id', 'presence-room', 'A', ['n' => 1]);
    $log->append('app-id', 'presence-room', 'B', ['n' => 2]);
    $overlap = $log->append('app-id', 'presence-room', 'C', ['n' => 3]);

    $gate = new DeferredFuture;
    $suspended = false;

    // Registered ahead of the replay plugin, and suspends in onSubscribe.
    deliveryLifecyclePlugin(function () use ($gate, &$suspended) {
        $suspended = true;

        $gate->getFuture()->await();
    });

    $held = [];
    $asked = false;

    // While the fleet is asked about the user the joiner is already in the
    // channel: C goes out live again (it is inside the replayed window), and
    // D is new.
    $this->fleet = function (Application $app, string $type) use ($overlap, &$held, &$asked) {
        if ($type === 'presence_connections' && ! $asked) {
            $asked = true;

            deliveryLive('C', ['n' => 3], $overlap);

            $held[] = deliveryLive('D', ['n' => 4]);
        }

        return [];
    };

    $joiner = new FakeConnection('joiner-1', $this->application);

    runLoop(function () use ($joiner, $cursor, $gate, &$suspended, &$held) {
        deliveryReplayPlugin();

        $join = async(fn () => deliveryJoin($joiner, 1, $cursor));

        deliveryUntil(function () use (&$suspended) {
            return $suspended;
        });

        // Only the confirmation is out: the fleet-wait broadcasts are held.
        expect($joiner->messages)->toHaveCount(1);

        $held[] = deliveryLive('E', ['n' => 5]);

        expect($joiner->messages)->toHaveCount(1);

        $gate->complete();

        $join->await();

        deliveryLive('F', ['n' => 6]);
    });

    $frames = deliveryFrames($joiner);

    expect($frames[0]['event'])->toBe('pusher_internal:subscription_succeeded')
        ->and(array_column($frames, 'event'))->not->toContain('pusher:error');

    $rest = array_slice($frames, 1);

    expect(array_map(fn (array $frame) => [$frame['event'], $frame['replayed']], $rest))->toBe([
        ['B', true],
        ['C', true],
        ['C', false],
        ['D', false],
        ['E', false],
        ['F', false],
    ]);

    $replayed = array_keys(array_filter($rest, fn (array $frame) => $frame['replayed']));
    $live = array_keys(array_filter($rest, fn (array $frame) => ! $frame['replayed']));

    expect(max($replayed))->toBeLessThan(min($live));

    // Deduplicated on _replay_id, each message arrives once and in stream order.
    $seen = [];

    foreach ($rest as $frame) {
        $id = $frame['data']['_replay_id'] ?? null;

        expect($id)->toBeString();

        if (! in_array($id, $seen, true)) {
            $seen[] = $id;
        }
    }

    expect($seen)->toBe(array_column($log->replaySince('app-id', 'presence-room', $cursor), 'id'));

    // The two held broadcasts each arrive exactly once.
    $ids = array_map(fn (array $frame) => $frame['data']['_replay_id'] ?? null, $rest);

    expect($held)->toHaveCount(2);

    foreach ($held as $id) {
        expect(array_keys($ids, $id, true))->toHaveCount(1);
    }
});

it('does not replay a stale buffer after an unsubscribe overtook its own subscribe', function () {
    $log = app(MessageLog::class);

    $cursor = $log->append('app-id', 'presence-room', 'A', ['n' => 1]);
    $log->append('app-id', 'presence-room', 'B', ['n' => 2]);
    $log->append('app-id', 'presence-room', 'C', ['n' => 3]);

    $recorder = deliveryLifecyclePlugin();

    $gate = new DeferredFuture;
    $reached = false;
    $asked = 0;

    // The first question to the fleet stays out until the gate opens.
    $this->fleet = function (Application $app, string $type) use ($gate, &$reached, &$asked) {
        if ($type === 'presence_connections' && ++$asked === 1) {
            $reached = true;

            $gate->getFuture()->await();
        }

        return [];
    };

    $joiner = new FakeConnection('joiner-1', $this->application);

    runLoop(function () use ($joiner, $cursor, $gate, $recorder, &$reached) {
        deliveryReplayPlugin();

        $first = async(fn () => deliveryJoin($joiner, 1, $cursor));

        deliveryUntil(function () use (&$reached) {
            return $reached;
        });

        // The replay was read at intercept time and waits for onSubscribe.
        expect($joiner->state('delivery.replay'))->toHaveKey('presence-room');

        deliveryLeave($joiner);

        $gate->complete();

        $first->await();

        // Core dropped the overtaken subscribe without telling the plugins.
        expect($joiner->messages)->toBe([])
            ->and($recorder->hooks)->toBe([])
            ->and($joiner->state('delivery.replay', []))->not->toHaveKey('presence-room');

        // A fresh subscribe without a cursor must not pick up the old window.
        deliveryJoin($joiner, 1);
    });

    $frames = deliveryFrames($joiner);

    expect($frames)->toHaveCount(1)
        ->and($frames[0]['event'])->toBe('pusher_internal:subscription_succeeded')
        ->and(array_filter($frames, fn (array $frame) => isset($frame['data']['_replay_id'])))->toBe([])
        ->and($joiner->state('delivery.replay', []))->not->toHaveKey('presence-room')
        ->and($recorder->hooks)->toBe(['onSubscribe presence-room']);
});

it('drops a channel buffer on pusher:unsubscribe and tolerates repeats', function () {
    $plugin = new MessageReplayPlugin;
    $connection = new FakeConnection('sock-1', $this->application);

    $connection->setState('delivery.replay', [
        'private-foo' => ['{"event":"A"}'],
        'private-bar' => ['{"event":"B"}'],
    ]);

    $unsubscribe = ['event' => 'pusher:unsubscribe', 'data' => ['channel' => 'private-foo']];

    expect($plugin->onMessage($connection, $unsubscribe))->toBe(MessageDisposition::Relay)
        ->and($plugin->onMessage($connection, $unsubscribe))->toBe(MessageDisposition::Relay);

    $plugin->onUnsubscribe(
        $connection,
        app(ChannelManager::class)->for($this->application)->findOrCreate('private-foo'),
    );

    expect($connection->state('delivery.replay'))->toBe([
        'private-bar' => ['{"event":"B"}'],
    ]);
});

it('does not replay the buffer of a rejected subscribe to a later subscribe without a cursor', function () {
    $log = app(MessageLog::class);

    $cursor = $log->append('app-id', 'presence-room', 'A', ['n' => 1]);
    $log->append('app-id', 'presence-room', 'B', ['n' => 2]);
    $log->append('app-id', 'presence-room', 'C', ['n' => 3]);

    $joiner = new FakeConnection('joiner-1', $this->application);

    runLoop(function () use ($joiner, $cursor) {
        deliveryReplayPlugin();

        $channelData = (string) json_encode(['user_id' => 1, 'user_info' => ['name' => 'Joe']]);

        // Bad auth: the interceptor reads the window, then core rejects the subscribe.
        app(Server::class)->message($joiner, (string) json_encode([
            'event' => 'pusher:subscribe',
            'data' => [
                'channel' => 'presence-room',
                'auth' => 'app-key:not-a-valid-signature',
                'channel_data' => $channelData,
                'last_event_id' => $cursor,
            ],
        ]));

        expect(app(ChannelManager::class)->for($joiner->app())->find('presence-room')?->subscribed($joiner) ?? false)->toBeFalse()
            ->and($joiner->state('delivery.replay'))->toHaveKey('presence-room');

        deliveryJoin($joiner, 1);
    });

    $frames = array_values(array_filter(
        deliveryFrames($joiner),
        fn (array $frame) => $frame['event'] !== 'pusher:error',
    ));

    expect(array_column($frames, 'event'))->toBe(['pusher_internal:subscription_succeeded'])
        ->and(array_filter($frames, fn (array $frame) => isset($frame['data']['_replay_id'])))->toBe([])
        ->and($joiner->state('delivery.replay', []))->not->toHaveKey('presence-room');
});
