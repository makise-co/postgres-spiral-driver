<?php
/*
 * This file is part of the Makise-Co Framework
 *
 * World line: 0.571024a
 * (c) Dmitry K. <coder1994@gmail.com>
 */

declare(strict_types=1);

namespace Database\Driver\MakisePostgresPool;

use MakiseCo\Database\Tests\BaseTest;
use Spiral\Database\Database;
use Swoole\Coroutine;

class PrimaryKeyTest extends BaseTest
{
    public const DRIVER = 'makisepostgrespool';

    /**
     * @var Database
     */
    protected $database;

    protected $table;

    public function setUp(): void
    {
        $this->database = $this->db();
        $this->table = $this->database->table('sample_table');

        $schema = $this->table->getSchema();
        $schema->primary('id');
        $schema->string('name', 64);
        $schema->integer('value');
        $schema->save();
    }

    public function tearDown(): void
    {
        $schema = $this->table->getSchema();
        $schema->declareDropped();
        $schema->save();

        parent::tearDown();
    }

    public function testPrimaryKeysNotOverwritten(): void
    {
        $table = $this->database->table('sample_table');

        $ch = new Coroutine\Channel(2);

        Coroutine::create(function () use ($table, $ch) {
            $coro1Id = $table->insertOne(['id' => 1, 'name'  => '1', 'value' => '1']);
            $ch->push([1 => $coro1Id]);
        });

        Coroutine::create(function () use ($table, $ch) {
            $coro2Id = $table->insertOne(['id' => 2, 'name'  => '2', 'value' => '2']);
            $ch->push([2 => $coro2Id]);
        });

        $results = [];

        $result = $ch->pop();
        $results[\key($result)] = \current($result);

        $result = $ch->pop();
        $results[\key($result)] = \current($result);

        self::assertSame(1, $results[1]);
        self::assertSame(2, $results[2]);
    }
}
