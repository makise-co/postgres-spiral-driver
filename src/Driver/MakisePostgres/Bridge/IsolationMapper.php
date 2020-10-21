<?php
/*
 * This file is part of the Makise-Co Framework
 *
 * World line: 0.571024a
 * (c) Dmitry K. <coder1994@gmail.com>
 */

declare(strict_types=1);

namespace MakiseCo\Database\Driver\MakisePostgres\Bridge;

use MakiseCo\SqlCommon\Contracts\Transaction as SqlTransaction;
use Spiral\Database\Driver\DriverInterface;

class IsolationMapper
{
    private function __construct()
    {
    }

    public static function map(?string $spiralTransactionIsolation): int
    {
        switch ($spiralTransactionIsolation) {
            case DriverInterface::ISOLATION_REPEATABLE_READ:
                return SqlTransaction::ISOLATION_REPEATABLE;

            case DriverInterface::ISOLATION_SERIALIZABLE:
                return SqlTransaction::ISOLATION_SERIALIZABLE;

            case DriverInterface::ISOLATION_READ_UNCOMMITTED:
                return SqlTransaction::ISOLATION_UNCOMMITTED;

            case DriverInterface::ISOLATION_READ_COMMITTED:
            default:
                return SqlTransaction::ISOLATION_COMMITTED;
        }
    }
}