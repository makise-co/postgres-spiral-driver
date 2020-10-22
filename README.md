# postgres-spiral-driver
[MakiseCo PostgreSQL client](https://github.com/makise-co/postgres) adapter for [Spiral DBAL](https://github.com/spiral/database)

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
                'host' => '127.0.0.1',
                'port' => 5432,
                'username' => 'makise',
                'password' => 'el-psy-congroo',
                'database' => 'makise',
                // or 'connection' => 'host=127.0.0.1;dbname=makise',
                'schema' => ['public'],
                'timezone' => 'UTC',
                'charset' => 'utf8',
                'application_name' => 'MakiseCo Framework',
                
                'connector' => \MakiseCo\Postgres\Driver\Pq\PqConnector::class,

                // connection pool configuration
                'poolMinActive' => 0,
                'poolMaxActive' => 2,
                'poolMaxIdleTime' => 30,
                'poolValidationInterval' => 15.0,
            ]
        ]
    ]
]);

$dbal = new Database\DatabaseManager($dbConfig);
```
