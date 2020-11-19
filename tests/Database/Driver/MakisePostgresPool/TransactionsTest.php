<?php

/*
 * This file is part of the Makise-Co Framework
 *
 * World line: 0.571024a
 * (c) Dmitry K. <coder1994@gmail.com>
 */

declare(strict_types=1);

namespace MakiseCo\Database\Tests\Driver\MakisePostgresPool;

use MakiseCo\Database\Driver\MakisePostgres\Bridge\PostgresPool;
use Swoole\Coroutine;

class TransactionsTest extends \MakiseCo\Database\Tests\TransactionsTest
{
    public const DRIVER = 'makisepostgrespool';

    public function testConcurrentTransactions(): void
    {
        $db = $this->database;
        /** @var PostgresPool $pool */
        $pool = $db->getDriver()->getPool();

        self::assertSame(2, $pool->getMaxActive());

        $ch = new Coroutine\Channel(2);

        Coroutine::create(function () use ($db, $ch) {
            $this->database->transaction(
                function () use ($db, $ch): void {
                    try {
                        $id = $db->table->insertOne(['name' => 'Anton', 'value' => 123]);
                        $this->assertSame($id, $this->database->table->count());
                        $db->query('SELECT pg_sleep(0.5)');

                        $ch->push($id);
                    } catch (\Throwable $e) {
                        $ch->push($e);
                    }
                }
            );
        });

        Coroutine::create(function () use ($db, $ch) {
            $this->database->transaction(
                function () use ($db, $ch): void {
                    try {
                        $id = $db->table->insertOne(['name' => 'Dmitry', 'value' => 456]);
                        $this->assertSame(1, $this->database->table->count());
                        $db->query('SELECT pg_sleep(0.5)');

                        $ch->push($id);
                    } catch (\Throwable $e) {
                        $ch->push($e);
                    }
                }
            );
        });

        if (($err = $ch->pop()) instanceof \Throwable) {
            throw $err;
        }
        $ids[] = $err;

        if (($err = $ch->pop()) instanceof \Throwable) {
            throw $err;
        }
        $ids[] = $err;

        self::assertSame(2, $this->database->table->count());
        self::assertSame(2, $pool->getIdleCount());
        self::assertContains(1, $ids);
        self::assertContains(2, $ids);
    }
}
