<?php

use Webpatser\ResonateDelivery\DeliveryKeys;

it('builds a stream key per application and channel', function () {
    $keys = new DeliveryKeys('delivery');

    expect($keys->streamKey('app-id', 'presence-chat.42'))
        ->toBe('delivery:app-id:presence-chat.42');
});

it('honours a custom prefix', function () {
    $keys = new DeliveryKeys('mg');

    expect($keys->streamKey('a', 'private-x'))->toBe('mg:a:private-x');
});
