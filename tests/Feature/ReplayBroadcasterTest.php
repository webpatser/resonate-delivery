<?php

use Predis\Client;
use Webpatser\ResonateDelivery\Broadcasting\ReplayBroadcaster;
use Webpatser\ResonateDelivery\MessageLog;
use Webpatser\ResonateDelivery\Tests\Support\FailingMessageLog;
use Webpatser\ResonateDelivery\Tests\Support\RecordingBroadcaster;

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

it('appends to the log and delegates with an augmented payload', function () {
    $underlying = new RecordingBroadcaster;
    $log = app(MessageLog::class);

    $broadcaster = new ReplayBroadcaster(
        underlying: $underlying,
        log: $log,
        appId: 'app-id',
    );

    $broadcaster->broadcast(['private-foo'], 'OrderShipped', ['order' => 1]);

    expect($underlying->calls)->toHaveCount(1);

    [$channels, $event, $payload] = $underlying->calls[0];

    expect($channels)->toBe(['private-foo'])
        ->and($event)->toBe('OrderShipped')
        ->and($payload['order'])->toBe(1)
        ->and($payload['_replay_id'])->toMatch('/^\d+-\d+$/');

    // The original payload is in the stream; the _replay_id is the assigned id.
    $entries = $log->replaySince('app-id', 'private-foo', '0-0');

    expect($entries)->toHaveCount(1)
        ->and($entries[0]['event'])->toBe('OrderShipped')
        ->and($entries[0]['data'])->toBe(['order' => 1])
        ->and($entries[0]['id'])->toBe($payload['_replay_id']);
});

it('broadcasts per channel so each gets its own replay id', function () {
    $underlying = new RecordingBroadcaster;
    $log = app(MessageLog::class);

    $broadcaster = new ReplayBroadcaster(
        underlying: $underlying,
        log: $log,
        appId: 'app-id',
    );

    $broadcaster->broadcast(['private-a', 'private-b'], 'Hi', ['n' => 1]);

    expect($underlying->calls)->toHaveCount(2)
        ->and($underlying->calls[0][0])->toBe(['private-a'])
        ->and($underlying->calls[1][0])->toBe(['private-b']);

    $idA = $underlying->calls[0][2]['_replay_id'];
    $idB = $underlying->calls[1][2]['_replay_id'];

    // Each id addresses an entry in its own stream. The id strings may collide
    // across streams (Redis Streams scopes ids per key), so don't assert
    // inequality; instead verify each stream actually contains the message
    // its id points at.
    expect($log->replaySince('app-id', 'private-a', '0-0'))->toHaveCount(1)
        ->and($log->replaySince('app-id', 'private-a', '0-0')[0]['id'])->toBe($idA)
        ->and($log->replaySince('app-id', 'private-b', '0-0'))->toHaveCount(1)
        ->and($log->replaySince('app-id', 'private-b', '0-0')[0]['id'])->toBe($idB);
});

it('still broadcasts live when the log write fails', function () {
    $underlying = new RecordingBroadcaster;

    $broadcaster = new ReplayBroadcaster(
        underlying: $underlying,
        log: new FailingMessageLog,
        appId: 'app-id',
    );

    $broadcaster->broadcast(['private-foo'], 'OrderShipped', ['order' => 1]);

    // The durability add-on failed; live delivery carried on regardless.
    expect($underlying->calls)->toHaveCount(1);

    [$channels, $event, $payload] = $underlying->calls[0];

    expect($channels)->toBe(['private-foo'])
        ->and($event)->toBe('OrderShipped')
        ->and($payload['order'])->toBe(1)
        // No id, because nothing was logged: the client must not advance its
        // cursor to a message it could never replay.
        ->and($payload)->not->toHaveKey('_replay_id');
});

it('keeps delivering the remaining channels when one channel fails to log', function () {
    $underlying = new RecordingBroadcaster;

    $broadcaster = new ReplayBroadcaster(
        underlying: $underlying,
        log: new class extends FailingMessageLog
        {
            public function append(string $appId, string $channel, string $event, array $data): string
            {
                if ($channel === 'private-a') {
                    throw new RuntimeException('Connection refused');
                }

                return '1700000000000-0';
            }
        },
        appId: 'app-id',
    );

    $broadcaster->broadcast(['private-a', 'private-b'], 'Hi', ['n' => 1]);

    expect($underlying->calls)->toHaveCount(2)
        ->and($underlying->calls[0][2])->not->toHaveKey('_replay_id')
        ->and($underlying->calls[1][2]['_replay_id'])->toBe('1700000000000-0');
});

it('delegates auth unchanged', function () {
    $underlying = new RecordingBroadcaster;
    $broadcaster = new ReplayBroadcaster(
        underlying: $underlying,
        log: app(MessageLog::class),
        appId: 'app-id',
    );

    expect($broadcaster->auth(null))->toBe(['delegated' => true]);
});
