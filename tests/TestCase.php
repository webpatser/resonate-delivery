<?php

namespace Webpatser\ResonateDelivery\Tests;

use Illuminate\Support\ServiceProvider;
use Orchestra\Testbench\TestCase as Testbench;
use Webpatser\Resonate\ResonateServiceProvider;
use Webpatser\ResonateDelivery\DeliveryServiceProvider;

class TestCase extends Testbench
{
    /**
     * Get the package providers.
     *
     * @return array<int, class-string<ServiceProvider>>
     */
    protected function getPackageProviders($app)
    {
        return [
            ResonateServiceProvider::class,
            DeliveryServiceProvider::class,
        ];
    }

    /**
     * Define the test environment.
     */
    protected function defineEnvironment($app): void
    {
        $app['config']->set('reverb.default', 'reverb');

        $app['config']->set('reverb.servers.reverb', [
            'host' => '0.0.0.0',
            'port' => 8080,
            'path' => '',
            'hostname' => null,
            'options' => ['tls' => []],
            'max_request_size' => 10_000,
            'scaling' => [
                'enabled' => false,
                'channel' => 'reverb',
                'server' => [
                    'url' => null,
                    'host' => '127.0.0.1',
                    'port' => '6379',
                    'username' => null,
                    'password' => null,
                    'database' => '15',
                    'timeout' => 60,
                ],
            ],
            'pulse_ingest_interval' => 15,
            'telescope_ingest_interval' => 15,
        ]);

        $app['config']->set('reverb.apps', [
            'provider' => 'config',
            'apps' => [
                [
                    'key' => 'app-key',
                    'secret' => 'app-secret',
                    'app_id' => 'app-id',
                    'options' => [
                        'host' => 'localhost',
                        'port' => 8080,
                        'scheme' => 'http',
                        'useTLS' => false,
                    ],
                    'allowed_origins' => ['*'],
                    'ping_interval' => 60,
                    'activity_timeout' => 30,
                    'max_connections' => null,
                    'max_message_size' => 10_000,
                    'accept_client_events_from' => 'members',
                    'rate_limiting' => [
                        'enabled' => false,
                        'max_attempts' => 60,
                        'decay_seconds' => 60,
                        'terminate_on_limit' => false,
                    ],
                ],
            ],
        ]);

        $app['config']->set('resonate-delivery', [
            'connection' => [
                'url' => null,
                'host' => '127.0.0.1',
                'port' => '6379',
                'username' => null,
                'password' => null,
                'database' => '15',
                'timeout' => 5,
            ],
            'key_prefix' => 'delivery-test',
            'retention' => [
                'default_max_messages' => 1000,
                'per_channel' => [],
            ],
            'replay_id_field' => '_replay_id',
            'replay_batch_size' => 100,
            'underlying' => 'reverb',
        ]);
    }
}
