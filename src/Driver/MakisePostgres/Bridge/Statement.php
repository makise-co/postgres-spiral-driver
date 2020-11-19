<?php

/*
 * This file is part of the Makise-Co Framework
 *
 * World line: 0.571024a
 * (c) Dmitry K. <coder1994@gmail.com>
 */

declare(strict_types=1);

namespace MakiseCo\Database\Driver\MakisePostgres\Bridge;

use Generator;
use IteratorAggregate;
use MakiseCo\SqlCommon\Contracts\CommandResult;
use MakiseCo\SqlCommon\Contracts\ResultSet;
use MakiseCo\SqlCommon\Contracts\Statement as PostgresStatement;
use pq\Result;
use Spiral\Database\StatementInterface;

/**
 * Adds few quick methods to PDOStatement and fully compatible with it. By default uses
 * PDO::FETCH_ASSOC mode.
 *
 * @internal Do not use this class directly.
 */
final class Statement implements StatementInterface, IteratorAggregate
{
    private ?PostgresStatement $pdoStatement;

    private string $queryString;

    /**
     * @var ResultSet|null
     */
    private ?ResultSet $resultSet = null;

    private int $affectedRowsCount = 0;

    /**
     * Statement constructor.
     * @param PostgresStatement $pdoStatement
     * @param ResultSet|CommandResult $result
     */
    public function __construct(PostgresStatement $pdoStatement, $result)
    {
        // TODO: This preventing from infinite locking, statement should be release immediately
//        $this->pdoStatement = $pdoStatement;
        $this->queryString = $pdoStatement->getQuery();

        if ($result instanceof ResultSet) {
            $this->resultSet = $result;
        } elseif ($result instanceof CommandResult) {
            $this->affectedRowsCount = $result->getAffectedRowCount();
        }
    }

    public function __destruct()
    {
        try {
            $this->pdoStatement = null;
        } catch (\Throwable $e) {
        }
    }

    /**
     * @return string
     */
    public function getQueryString(): string
    {
        return $this->queryString;
    }

    /**
     * @return PostgresStatement
     */
    public function getPDOStatement(): PostgresStatement
    {
        throw new \RuntimeException('This is not a PDO');
        return $this->pdoStatement;
    }

    /**
     * @inheritDoc
     */
    public function fetch(int $mode = self::FETCH_ASSOC)
    {
        if (null === $this->resultSet) {
            return null;
        }

        return $this->resultSet->fetch(
            $this->pdoFetchStyleToPostgresFetchStyle($mode)
        );
    }

    /**
     * @inheritDoc
     */
    public function fetchColumn(int $columnNumber = null)
    {
        if (null === $this->resultSet) {
            return null;
        }

        $result = null;
        $this->resultSet->fetchColumn($columnNumber, $result);

        return $result;
    }

    /**
     * @param int $mode
     * @return array
     */
    public function fetchAll(int $mode = self::FETCH_ASSOC): array
    {
        if (null === $this->resultSet) {
            return [];
        }

        return $this->resultSet->fetchAll(
            $this->pdoFetchStyleToPostgresFetchStyle($mode)
        );
    }

    /**
     * @return int
     */
    public function rowCount(): int
    {
        return $this->affectedRowsCount;
    }

    /**
     * @return int
     */
    public function columnCount(): int
    {
        if (null === $this->resultSet) {
            return 0;
        }

        return $this->resultSet->getFieldCount();
    }

    /**
     * @return Generator
     */
    public function getIterator(): Generator
    {
        if (null === $this->resultSet) {
            return;
        }

        while ($row = $this->resultSet->fetch(Result::FETCH_ASSOC)) {
            yield $row;
        }

        $this->close();
    }

    /**
     * {@inheritdoc}
     */
    public function close(): void
    {
        $this->resultSet = null;
        $this->affectedRowsCount = 0;

        try {
            $this->pdoStatement = null;
        } catch (\Throwable $e) {
        }
    }

    protected function pdoFetchStyleToPostgresFetchStyle($fetchStyle): int
    {
        if (null === $fetchStyle || $fetchStyle === self::FETCH_ASSOC) {
            return Result::FETCH_ASSOC;
        }

        if ($fetchStyle === 5/*\PDO::FETCH_OBJ*/) {
            return Result::FETCH_OBJECT;
        }

        if ($fetchStyle === self::FETCH_NUM) {
            return Result::FETCH_ARRAY;
        }

        // fallback other fetch styles to assoc
        return Result::FETCH_ASSOC;
    }
}
