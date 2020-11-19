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
            'driver'  => \MakiseCo\Database\Driver\MakisePostgres\PooledMakisePostgresDriver::class,
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
                'connect_timeout' => 0, // float, connection attempt timeout in seconds (0 means no timeout)

                // connection pool configuration
                'poolMinActive' => 0, // The minimum number of established connections that should be kept in the pool at all times
                'poolMaxActive' => 2, // The maximum number of active connections that can be allocated from this pool at the same time
                'poolMaxWaitTime' => 5.0, // The maximum number of seconds that can be awaited for a free connection from the pool
                'poolMaxIdleTime' => 30, // The minimum amount of time (seconds) a connection may sit idle in the pool before it is eligible for closing
                'poolValidationInterval' => 15.0, // The number of seconds to sleep between runs of the idle connection validation/cleaner timer
            ]
        ]
    ]
]);

$dbal = new Database\DatabaseManager($dbConfig);
```
