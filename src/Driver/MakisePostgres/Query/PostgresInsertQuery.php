<?php

/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

declare(strict_types=1);

namespace MakiseCo\Database\Driver\MakisePostgres\Query;

use MakiseCo\Database\Driver\MakisePostgres\MakisePostgresDriver;
use Spiral\Database\Driver\DriverInterface;
use Spiral\Database\Driver\Postgres\Query\PostgresInsertQuery as BasePostgresInsertQuery;
use Spiral\Database\Exception\BuilderException;
use Spiral\Database\Query\ActiveQuery;
use Spiral\Database\Query\QueryInterface;

/**
 * Postgres driver requires little bit different way to handle last insert id.
 */
class PostgresInsertQuery extends BasePostgresInsertQuery
{
    /**
     * @param DriverInterface $driver
     * @param string|null $prefix
     * @return QueryInterface
     */
    public function withDriver(DriverInterface $driver, string $prefix = null): QueryInterface
    {
        if (!$driver instanceof MakisePostgresDriver) {
            throw new BuilderException(
                'Postgres InsertQuery can be used only with MakisePostgresDriver driver'
            );
        }

        return ActiveQuery::withDriver($driver, $prefix);
    }
}
