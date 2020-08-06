# postgres-spiral-driver
MakiseCo PostgreSQL client adapter for Spiral DBAL

WARNING: This driver could be used only in the Coroutine context

Usage:
```php
<?php

declare(strict_types=1);

use Spiral\Database;

$dbConfig = new Database\Config\DatabaseConfig([
    'default'     => 'default',
    'databases'   => [
        'default' => [
            'connection' => 'pgsql'
        ]
    ],
    'connections' => [
        'pgsql' => [
            'driver'  => \MakiseCo\Database\Driver\MakisePostgres\MakisePostgresDriver::class,
            'options' => [
                'connection' => 'host=127.0.0.1;port=5432;dbname=makise',
                'username' => 'postgres',
                'password' => 'postgres',

                // or specify config parts directly
                'host' => '127.0.0.1',
                'port' => 5432,
                'database' => 'makise',
            ]
        ]
    ]
]);

$dbal = new Database\DatabaseManager($dbConfig);
```