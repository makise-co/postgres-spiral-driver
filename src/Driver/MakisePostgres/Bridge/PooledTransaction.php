<?php
/*
 * This file is part of the Makise-Co Framework
 *
 * World line: 0.571024a
 * (c) Dmitry K. <coder1994@gmail.com>
 */

declare(strict_types=1);

namespace MakiseCo\Database\Driver\MakisePostgres\Bridge;

use MakiseCo\Postgres\Contracts\Transaction;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\LoggerInterface;
use Throwable;

use function array_keys;

class PooledTransaction
{
    use LoggerAwareTrait;

    private Transaction $transaction;
    private int $level = 1;

    /**
     * Statements list that should be deallocated when transaction error occurred
     * We cannot deallocate statements inside broken transaction
     *
     * @var \MakiseCo\SqlCommon\Contracts\Statement[]
     */
    private array $statementsToDeallocate = [];

    public function __construct(Transaction $transaction, ?LoggerInterface $logger = null)
    {
        $this->transaction = $transaction;

        if (null !== $logger) {
            $this->setLogger($logger);
        }
    }

    public function getTransactionLevel(): int
    {
        return $this->level;
    }

    public function getTransaction(): Transaction
    {
        return $this->transaction;
    }

    public function beginTransaction(): void
    {
        $this->createSavepoint($this->level + 1);
    }

    public function commit(): void
    {
        if ($this->level === 1) {
            if ($this->logger !== null) {
                $this->logger->info('Commit transaction');
            }

            try {
                $this->transaction->commit();
                return;
            } catch (Throwable $e) {
                throw ExceptionMapper::map($e, 'COMMIT');
            } finally {
                $this->level = 0;
            }
        }

        $this->releaseSavepoint($this->level);
    }

    public function rollbackTransaction(): void
    {
        if ($this->level === 1) {
            if ($this->logger !== null) {
                $this->logger->info('Rollback transaction');
            }

            try {
                $this->transaction->rollback();

                return;
            } catch (Throwable $e) {
                throw ExceptionMapper::map($e, 'ROLLBACK');
            } finally {
                $this->level = 0;

                foreach (array_keys($this->statementsToDeallocate) as $key) {
                    try {
                        unset($this->statementsToDeallocate[$key]);
                    } catch (Throwable $deallocEx) {
                        $this->logger->notice(
                            "Failed to deallocate statement on transaction rollback: {$deallocEx->getMessage()}"
                        );
                    }
                }
                $this->statementsToDeallocate = [];
            }
        }

        $this->rollbackSavepoint($this->level);
    }

    public function createSavepoint(int $level): void
    {
        if ($this->logger !== null) {
            $this->logger->info("Transaction: new savepoint 'SVP{$level}'");
        }

        try {
            $this->transaction->createSavepoint("SVP{$level}");
        } catch (Throwable $e) {
            throw ExceptionMapper::map($e, "SAVEPOINT 'SVP{$level}'");
        }

        $this->level++;
    }

    public function releaseSavepoint(int $level): void
    {
        if ($this->logger !== null) {
            $this->logger->info("Transaction: release savepoint 'SVP{$level}'");
        }

        try {
            $this->transaction->releaseSavepoint("SVP{$level}");
        } catch (Throwable $e) {
            throw ExceptionMapper::map($e, "RELEASE SAVEPOINT 'SVP{$level}'");
        }

        $this->level--;
    }

    public function rollbackSavepoint(int $level): void
    {
        if ($this->logger !== null) {
            $this->logger->info("Transaction: rollback savepoint 'SVP{$level}'");
        }

        try {
            $this->transaction->rollbackTo("SVP{$level}");
        } catch (Throwable $e) {
            throw ExceptionMapper::map($e, "ROLLBACK TO SAVEPOINT 'SVP{$level}'");
        }

        $this->level--;
    }

    public function addStatementToDeallocate(\MakiseCo\SqlCommon\Contracts\Statement $statement): void
    {
        $this->statementsToDeallocate[] = $statement;
    }
}
