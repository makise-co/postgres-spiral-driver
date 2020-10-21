<?php
/*
 * This file is part of the Makise-Co Framework
 *
 * World line: 0.571024a
 * (c) Dmitry K. <coder1994@gmail.com>
 */

declare(strict_types=1);

namespace MakiseCo\Database\Driver\MakisePostgres\Bridge;

use MakiseCo\Connection\ConnectionInterface;
use MakiseCo\Postgres\Connection;

class PostgresPool extends \MakiseCo\Postgres\PostgresPool
{
    public function pop(): Connection
    {
        return parent::pop();
    }

    public function push(ConnectionInterface $connection): int
    {
        return parent::push($connection);
    }

    public function runOnce(\Closure $closure)
    {
        $conn = parent::pop();

        try {
            return $closure($conn);
        } finally {
            parent::push($conn);
        }
    }
}