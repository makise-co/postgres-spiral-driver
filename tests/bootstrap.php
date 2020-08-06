<?php

/*
 * This file is part of the Makise-Co Framework
 *
 * World line: 0.571024a
 * (c) Dmitry K. <coder1994@gmail.com>
 */

/**
 * Spiral Framework, SpiralScout LLC.
 *
 * @author    Anton Titov (Wolfy-J)
 */

declare(strict_types=1);

use MakiseCo\Database;

// phpcs:disable
define('SPIRAL_INITIAL_TIME', microtime(true));

error_reporting(E_ALL | E_STRICT);
ini_set('display_errors', '1');
mb_internal_encoding('UTF-8');

//Composer
require dirname(__DIR__) . '/vendor/autoload.php';

Database\Tests\BaseTest::$config = [
    'debug'     => false,
    'makisepostgres'  => [
        'driver'     => Database\Driver\MakisePostgres\MakisePostgresDriver::class,
        'check'      => static function () {
            return \extension_loaded('swoole') && \extension_loaded('pq');
        },
        'conn'       => 'host=host.docker.internal;port=15432;dbname=spiral',

        'user' => 'postgres',
        'pass' => 'postgres',
        'queryCache' => 100
    ],
];

if (!empty(getenv('DB'))) {
    switch (getenv('DB')) {
        case 'makisepostgres':
            Database\Tests\BaseTest::$config = [
                'debug'    => false,
                'postgres' => [
                    'driver' => Database\Driver\MakisePostgres\MakisePostgresDriver::class,
                    'check'  => static function () {
                        return true;
                    },
                    'conn'   => 'host=127.0.0.1;port=5432;dbname=spiral',
                    'user'   => 'postgres',
                    'pass'   => 'postgres'
                ],
            ];
            break;
    }
}
