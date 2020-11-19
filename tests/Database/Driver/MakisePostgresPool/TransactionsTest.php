<?php

/*
 * This file is part of the Makise-Co Framework
 *
 * World line: 0.571024a
 * (c) Dmitry K. <coder1994@gmail.com>
 */

declare(strict_types=1);

namespace MakiseCo\Database\Tests\Driver\MakisePostgresPool;

use Spiral\Database\Database;
use Spiral\Database\Exception\StatementException\ConstrainException;

class TransactionsTest extends \MakiseCo\Database\Tests\TransactionsTest
{
    public const DRIVER = 'makisepostgrespool';

    public function testConstraintExceptionOnTransaction(): void
    {
        $this->expectException(ConstrainException::class);

        $this->database->transaction(
            function (Database $db) {
                $res = $db->query(
                    <<<SQL
CREATE TABLE uniq_table (
id int8 PRIMARY KEY
);
SQL
                );
                $db->insert('uniq_table')->values(['id' => 1])->run();
                $db->insert('uniq_table')->values(['id' => 1])->run();
            }
        );
    }

    public function testConstraintExceptionOnNestedTransaction(): void
    {
        $this->expectException(ConstrainException::class);

        $this->database->transaction(
            function (Database $db) {
                $res = $db->query(
                    <<<SQL
CREATE TABLE uniq_table (
id int8 PRIMARY KEY
);
SQL
                );
                $db->insert('uniq_table')->values(['id' => 1])->run();

                $db->transaction(function (Database $db) {
                    $db->insert('uniq_table')->values(['id' => 1])->run();
                });
            }
        );
    }
}
