<?php

namespace Webpatser\ResonateDelivery;

use Illuminate\Contracts\Broadcasting\Factory as BroadcastFactory;
use Illuminate\Support\Facades\Broadcast;
use Illuminate\Support\ServiceProvider;
use Predis\Client;
use Webpatser\ResonateDelivery\Broadcasting\ReplayBroadcaster;

/**
 * Wires resonate-delivery into a host Laravel application.
 *
 * Registers the `resonate-delivery` broadcasting driver, binds the support
 * services, and publishes the config. The server-side {@see MessageReplayPlugin}
 * is registered by the host in `config/reverb.php`'s `plugins` array.
 */
class DeliveryServiceProvider extends ServiceProvider
{
    /**
     * Register the package services.
     */
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__.'/../config/resonate-delivery.php', 'resonate-delivery');

        $this->app->singleton(DeliveryKeys::class, function ($app) {
            return new DeliveryKeys((string) $app['config']->get('resonate-delivery.key_prefix', 'delivery'));
        });

        $this->app->singleton(DeliveryRetention::class, function ($app) {
            $retention = (array) $app['config']->get('resonate-delivery.retention', []);

            return new DeliveryRetention(
                default: (int) ($retention['default_max_messages'] ?? 1000),
                perChannel: (array) ($retention['per_channel'] ?? []),
            );
        });

        $this->app->singleton(MessageLog::class, function ($app) {
            return new MessageLog(
                redis: new Client($this->predisParameters((array) $app['config']->get('resonate-delivery.connection', []))),
                keys: $app->make(DeliveryKeys::class),
                retention: $app->make(DeliveryRetention::class),
            );
        });
    }

    /**
     * Bootstrap the package services.
     */
    public function boot(): void
    {
        $this->registerBroadcasterDriver();

        if ($this->app->runningInConsole()) {
            $this->publishes([
                __DIR__.'/../config/resonate-delivery.php' => $this->app->configPath('resonate-delivery.php'),
            ], 'resonate-delivery-config');
        }
    }

    /**
     * Register the `resonate-delivery` broadcasting driver.
     */
    protected function registerBroadcasterDriver(): void
    {
        Broadcast::extend('resonate-delivery', function ($app, array $config) {
            $underlyingName = (string) ($config['underlying']
                ?? $app['config']->get('resonate-delivery.underlying', 'reverb'));

            $underlying = $app->make(BroadcastFactory::class)->connection($underlyingName);

            $appId = (string) ($config['app_id']
                ?? $app['config']->get("broadcasting.connections.{$underlyingName}.app_id", ''));

            return new ReplayBroadcaster(
                underlying: $underlying,
                log: $app->make(MessageLog::class),
                appId: $appId,
                replayIdField: (string) $app['config']->get('resonate-delivery.replay_id_field', '_replay_id'),
            );
        });
    }

    /**
     * Translate the connection config into predis connection parameters.
     *
     * @param  array<string, mixed>  $server
     * @return array<string, mixed>|string
     */
    protected function predisParameters(array $server): array|string
    {
        if (! empty($server['url'])) {
            return $server['url'];
        }

        $parameters = [
            'scheme' => 'tcp',
            'host' => $server['host'] ?? '127.0.0.1',
            'port' => (int) ($server['port'] ?? 6379),
            'database' => (int) ($server['database'] ?? 0),
        ];

        if (! empty($server['username'])) {
            $parameters['username'] = $server['username'];
        }

        if (! empty($server['password'])) {
            $parameters['password'] = $server['password'];
        }

        if (! empty($server['timeout'])) {
            $parameters['timeout'] = (float) $server['timeout'];
        }

        return $parameters;
    }
}
