<?php

/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

declare(strict_types=1);

namespace MakiseCo\Database\Tests;

use MakiseCo\Postgres\Exception\QueryExecutionError;
use Spiral\Database\Database;
use Spiral\Database\Exception\HandlerException;
use Spiral\Database\Exception\StatementException;
use Spiral\Database\Schema\AbstractTable;

/**
 * Add exception versions in a future versions.
 */
abstract class ExceptionsTest extends BaseTest
{
    /**
     * @var Database
     */
    protected $database;

    public function setUp(): void
    {
        $this->database = $this->db();
    }

    public function testSelectionException(): void
    {
        $select = $this->database->select()->from('udnefinedTable');
        try {
            $select->run();
        } catch (StatementException $e) {
            $this->assertInstanceOf(QueryExecutionError::class, $e->getPrevious());

            $this->assertSame(
                $e->getQuery(),
                $select->sqlStatement()
            );
        }
    }

    public function testHandlerException(): void
    {
        $select = $this->database->select()->from('udnefinedTable');
        try {
            $select->run();
        } catch (StatementException $e) {
            $h = new HandlerException($e);

            $this->assertInstanceOf(StatementException::class, $h->getPrevious());

            $this->assertSame(
                $h->getQuery(),
                $select->sqlStatement()
            );
        }
    }

    public function testInsertNotNullable(): void
    {
        /** @var AbstractTable $schema $schema */
        $schema = $this->getDriver()->getSchema('test');
        $schema->primary('id');
        $schema->string('value')->nullable(false)->defaultValue(null);
        $schema->save();

        $this->getDriver()->insertQuery('', 'test')->values(['value' => 'value'])->run();

        try {
            $this->getDriver()->insertQuery('', 'test')->values(['value' => null])->run();
        } catch (StatementException\ConstrainException $e) {
            $this->assertInstanceOf(StatementException\ConstrainException::class, $e);
        }
    }
}
