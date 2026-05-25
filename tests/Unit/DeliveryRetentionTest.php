<?php

use Webpatser\ResonateDelivery\DeliveryRetention;

it('returns the default when nothing matches', function () {
    $retention = new DeliveryRetention(default: 1000, perChannel: []);

    expect($retention->forChannel('presence-foo'))->toBe(1000);
});

it('honours a matching glob pattern', function () {
    $retention = new DeliveryRetention(default: 1000, perChannel: [
        'presence-chat.*' => 5000,
        'private-billing.*' => 100,
    ]);

    expect($retention->forChannel('presence-chat.42'))->toBe(5000)
        ->and($retention->forChannel('private-billing.99'))->toBe(100)
        ->and($retention->forChannel('presence-other'))->toBe(1000);
});

it('picks the first matching pattern in declaration order', function () {
    $retention = new DeliveryRetention(default: 1, perChannel: [
        'presence-*' => 200,
        'presence-chat.*' => 999,
    ]);

    expect($retention->forChannel('presence-chat.42'))->toBe(200);
});
