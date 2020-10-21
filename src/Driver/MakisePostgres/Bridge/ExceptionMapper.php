<?php
/*
 * This file is part of the Makise-Co Framework
 *
 * World line: 0.571024a
 * (c) Dmitry K. <coder1994@gmail.com>
 */

declare(strict_types=1);

namespace MakiseCo\Database\Driver\MakisePostgres\Bridge;

use MakiseCo\Postgres\Exception\QueryExecutionError;
use MakiseCo\SqlCommon\Exception\ConnectionException;
use Spiral\Database\Exception\StatementException;
use Throwable;

class ExceptionMapper
{
    private function __construct()
    {
    }

    public static function map(Throwable $exception, string $query): StatementException
    {
        if ($exception instanceof ConnectionException) {
            return new StatementException\ConnectionException($exception, $query);
        }

        if ($exception instanceof QueryExecutionError) {
            $sqlState = $exception->getDiagnostics()['sqlstate'] ?? '';

            switch ($sqlState) {
                case '23000':
                case '23001':
                case '23502':
                case '23503':
                case '23505':
                case '23514':
                case '23P01':
                    return new StatementException\ConstrainException($exception, $query);
            }
        }

        return new StatementException($exception, $query);
    }
}