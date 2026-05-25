<?php

use Revolt\EventLoop;
use Webpatser\Resonate\Protocols\Pusher\Contracts\ChannelConnectionManager;
use Webpatser\Resonate\Protocols\Pusher\Contracts\ChannelManager;
use Webpatser\Resonate\Protocols\Pusher\Managers\ArrayChannelConnectionManager;
use Webpatser\Resonate\Protocols\Pusher\Managers\ArrayChannelManager;
use Webpatser\ResonateDelivery\Tests\TestCase;

uses(TestCase::class)->in(__DIR__.'/Feature', __DIR__.'/Integration');

uses()->beforeEach(function () {
    $this->app->singleton(ChannelManager::class, fn () => new ArrayChannelManager);
    $this->app->bind(ChannelConnectionManager::class, fn () => new ArrayChannelConnectionManager);
})->in(__DIR__.'/Integration');

function redisReachable(): bool
{
    $connection = @fsockopen('127.0.0.1', 6379, $errno, $errstr, 0.5);

    if ($connection === false) {
        return false;
    }

    fclose($connection);

    return true;
}

function presenceAuth(string $socketId, string $channel, string $data, string $secret = 'app-secret'): string
{
    return 'app-key:'.hash_hmac('sha256', "{$socketId}:{$channel}:{$data}", $secret);
}

function runLoop(Closure $body): void
{
    $error = null;

    $watchdog = EventLoop::delay(5.0, function () use (&$error) {
        $error = 'event loop timed out';
        EventLoop::getDriver()->stop();
    });

    EventLoop::queue(function () use ($body, &$error, $watchdog) {
        try {
            $body();
        } catch (Throwable $e) {
            $error = $e->getMessage()."\n".$e->getTraceAsString();
        } finally {
            EventLoop::cancel($watchdog);
            EventLoop::getDriver()->stop();
        }
    });

    EventLoop::run();

    if ($error !== null) {
        throw new RuntimeException($error);
    }
}
