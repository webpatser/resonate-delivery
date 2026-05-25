<?php

use Predis\Client;
use Webpatser\ResonateDelivery\DeliveryKeys;
use Webpatser\ResonateDelivery\DeliveryRetention;
use Webpatser\ResonateDelivery\MessageLog;

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

function makeLog(int $default = 1000): MessageLog
{
    /** @var Client $redis */
    $redis = new Client(['host' => '127.0.0.1', 'port' => 6379, 'database' => 15]);

    return new MessageLog(
        redis: $redis,
        keys: new DeliveryKeys('delivery-test'),
        retention: new DeliveryRetention(default: $default),
    );
}

it('appends a message and returns a Redis Stream id', function () {
    $log = makeLog();

    $id = $log->append('app-id', 'private-foo', 'OrderShipped', ['order' => 1]);

    expect($id)->toMatch('/^\d+-\d+$/'); // {ms}-{seq}
});

it('preserves order across multiple appends', function () {
    $log = makeLog();

    $first = $log->append('app-id', 'private-foo', 'A', []);
    $second = $log->append('app-id', 'private-foo', 'B', []);
    $third = $log->append('app-id', 'private-foo', 'C', []);

    expect($first < $second)->toBeTrue()
        ->and($second < $third)->toBeTrue();
});

it('replays messages strictly newer than the cursor', function () {
    $log = makeLog();

    $first = $log->append('app-id', 'private-foo', 'A', ['n' => 1]);
    $second = $log->append('app-id', 'private-foo', 'B', ['n' => 2]);
    $third = $log->append('app-id', 'private-foo', 'C', ['n' => 3]);

    $entries = $log->replaySince('app-id', 'private-foo', $first);

    expect($entries)->toHaveCount(2)
        ->and($entries[0]['id'])->toBe($second)
        ->and($entries[0]['event'])->toBe('B')
        ->and($entries[0]['data'])->toBe(['n' => 2])
        ->and($entries[1]['id'])->toBe($third);
});

it('returns an empty list when nothing is newer than the cursor', function () {
    $log = makeLog();

    $only = $log->append('app-id', 'private-foo', 'A', []);

    expect($log->replaySince('app-id', 'private-foo', $only))->toBe([]);
});

it('caps the stream length under a heavy write load', function () {
    // `MAXLEN ~ N` trims approximately, on radix-tree node boundaries (~100
    // entries each), so a tiny burst may not trigger any trim at all. Write
    // enough to cross several node boundaries to exercise the cap, then
    // assert the result is bounded well below the volume we wrote.
    $log = makeLog(default: 100);

    for ($i = 0; $i < 3000; $i++) {
        $log->append('app-id', 'private-foo', 'tick', ['n' => $i]);
    }

    $len = (int) $this->redis->executeRaw(['XLEN', 'delivery-test:app-id:private-foo']);

    expect($len)->toBeGreaterThanOrEqual(100)
        ->and($len)->toBeLessThanOrEqual(500); // ~ trim tolerance, well below 3000
});
