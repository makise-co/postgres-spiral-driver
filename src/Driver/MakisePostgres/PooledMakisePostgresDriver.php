<?php
/*
 * This file is part of the Makise-Co Framework
 *
 * World line: 0.571024a
 * (c) Dmitry K. <coder1994@gmail.com>
 */

declare(strict_types=1);

namespace MakiseCo\Database\Driver\MakisePostgres;

use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;
use MakiseCo\Database\Driver\MakisePostgres\Bridge\OptionsProcessor;
use MakiseCo\Database\Driver\MakisePostgres\Bridge\Statement;
use MakiseCo\EvPrimitives\Lock;
use MakiseCo\Postgres\ConnectionConfig;
use MakiseCo\Postgres\ConnectionConfigBuilder;
use MakiseCo\Postgres\Contracts\Quoter;
use MakiseCo\SqlCommon\Contracts\Statement as PostgresStatement;
use MakiseCo\SqlCommon\Exception\FailureException;
use Psr\Log\LoggerAwareTrait;
use Spiral\Database\Driver\CachingCompilerInterface;
use Spiral\Database\Driver\CompilerCache;
use Spiral\Database\Driver\CompilerInterface;
use Spiral\Database\Driver\DriverInterface;
use Spiral\Database\Driver\HandlerInterface;
use Spiral\Database\Driver\Postgres\PostgresCompiler;
use Spiral\Database\Driver\ReadonlyHandler;
use Spiral\Database\Exception\DriverException;
use Spiral\Database\Exception\HandlerException;
use Spiral\Database\Exception\StatementException;
use Spiral\Database\Injection\ParameterInterface;
use Spiral\Database\Query\BuilderInterface;
use Spiral\Database\Query\DeleteQuery;
use Spiral\Database\Query\Interpolator;
use Spiral\Database\Query\QueryBuilder;
use Spiral\Database\Query\SelectQuery;
use Spiral\Database\Query\UpdateQuery;
use Spiral\Database\Schema\AbstractTable;
use Spiral\Database\StatementInterface;
use Swoole\Coroutine;
use Throwable;

use function array_key_exists;
use function array_merge;
use function is_string;
use function strpos;
use function substr;

class PooledMakisePostgresDriver implements DriverInterface
{
    use LoggerAwareTrait;

    // DateTime format to be used to perform automatic conversion of DateTime objects.
    protected const DATETIME = 'Y-m-d H:i:s';

    private ConnectionConfig $connectionConfig;
    private Bridge\PostgresPool $pool;

    private HandlerInterface $schemaHandler;
    private BuilderInterface $queryBuilder;
    private CompilerInterface $queryCompiler;

    /**
     * @var Bridge\PooledTransaction[]
     */
    private array $transactions = [];

    private array $options;

    private Lock $pkCacheLock;

    /**
     * Cached list of primary keys associated with their table names. Used by InsertBuilder to
     * emulate last insert id.
     *
     * @var array
     */
    private array $primaryKeys = [];

    private const DEFAULT_CONFIG = [
        // allow reconnects
        'reconnect' => true,

        // all datetime objects will be converted relative to
        // this timezone (must match with DB timezone!)
        'timezone' => 'UTC',

        // utf-8 by default
        'charset' => 'utf-8',

        // public schema by default
        'schema' => ['public'],

        // unbuffered mode is disabled by default
        'unbuffered' => false,

        // enables query caching
        'queryCache' => true,

        // disable schema modifications
        'readonlySchema' => false,

        // default connector is Pq connector
        'connector' => \MakiseCo\Postgres\Driver\Pq\PqConnector::class,

        // minimal connection count in the pool
        'poolMinActive' => 0,

        // maximum connection count in the pool
        'poolMaxActive' => 2,

        // maximum connection idle time (seconds, int)
        'poolMaxIdleTime' => 30,

        // how often pool will check idle connections
        'poolValidationInterval' => 15.0,
    ];

    public function __construct(array $options)
    {
        $this->schemaHandler = (new PostgresHandler())->withDriver($this);
        $this->queryBuilder = (new QueryBuilder(
            new SelectQuery(),
            new Query\PostgresInsertQuery(),
            new UpdateQuery(),
            new DeleteQuery()
        ))->withDriver($this);
        $this->queryCompiler = new PostgresCompiler('""');

        if (array_key_exists('connection', $options)) {
            OptionsProcessor::parseDsn($options['connection'], $options);
        }

        $options = array_merge(self::DEFAULT_CONFIG, $options);

        $this->connectionConfig = (new ConnectionConfigBuilder())
            ->fromArray($options)
            ->build();
        $this->options = $options;

        if ($this->options['queryCache'] && $this->queryCompiler instanceof CachingCompilerInterface) {
            $this->queryCompiler = new CompilerCache($this->queryCompiler);
        }

        if ($this->options['readonlySchema']) {
            $this->schemaHandler = new ReadonlyHandler($this->schemaHandler);
        }

        $this->pool = new Bridge\PostgresPool(
            $this->connectionConfig,
            new $options['connector']()
        );

        $this->pool->setMaxActive($this->options['poolMaxActive']);
        $this->pool->setMinActive($this->options['poolMinActive']);
        $this->pool->setMaxIdleTime($this->options['poolMaxIdleTime']);
        $this->pool->setValidationInterval($this->options['poolValidationInterval']);

        if (array_key_exists('poolMaxWaitTime', $this->options)) {
            $this->pool->setMaxWaitTime($this->options['poolMaxWaitTime']);
        }

        if (array_key_exists('poolMaxLifeTime', $this->options)) {
            $this->pool->setMaxLifeTime($this->options['poolMaxLifeTime']);
        }

        $this->pkCacheLock = new Lock();
    }

    public function __destruct()
    {
        $this->disconnect();
    }

    public function getPool(): Bridge\PostgresPool
    {
        return $this->pool;
    }

    public function getType(): string
    {
        return 'MakisePostgresPool';
    }

    public function getTimezone(): DateTimeZone
    {
        return new DateTimeZone($this->connectionConfig->getOptions()['options']['timezone']);
    }

    public function getSchemaHandler(): HandlerInterface
    {
        return $this->schemaHandler;
    }

    public function getQueryCompiler(): CompilerInterface
    {
        return $this->queryCompiler;
    }

    public function getQueryBuilder(): BuilderInterface
    {
        return $this->queryBuilder;
    }

    public function connect(): void
    {
        $this->pool->init();
    }

    public function isConnected(): bool
    {
        return $this->pool->isAlive();
    }

    public function disconnect(): void
    {
        try {
            $this->pool->close();
        } finally {
            $this->primaryKeys = [];
            $this->transactions = [];
        }
    }

    public function quote($value, int $type = 2): string
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        if (null !== ($transaction = $this->getTransactionConn())) {
            return $this->quoteString($transaction->getTransaction(), $value);
        }

        $conn = $this->pool->pop();

        try {
            return $this->quoteString($conn, $value);
        } finally {
            $this->pool->push($conn);
        }
    }

    public function query(string $statement, array $parameters = []): StatementInterface
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        return $this->statement($statement, $parameters);
    }

    public function execute(string $query, array $parameters = []): int
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        return $this->statement($query, $parameters)->rowCount();
    }

    public function lastInsertID(string $sequence = null)
    {
        return null;
    }

    public function beginTransaction(string $isolationLevel = null): bool
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        $transaction = $this->getTransactionConn();
        if ($transaction !== null) {
            $transaction->beginTransaction();

            return true;
        }

        if ($this->logger !== null) {
            $this->logger->info(
                'Begin transaction',
                [
                    'isolation' => $isolationLevel,
                    'cid' => Coroutine::getCid(),
                ]
            );
        }

        $isolation = Bridge\IsolationMapper::map($isolationLevel);

        try {
            $transaction = $this->pool->beginTransaction($isolation);
        } catch (Throwable $e) {
            $e = Bridge\ExceptionMapper::map($e, 'BEGIN TRANSACTION');

            if (!$e instanceof StatementException\ConnectionException || !$this->options['reconnect']) {
                throw $e;
            }

            try {
                $transaction = $this->pool->beginTransaction($isolation);
            } catch (Throwable $e) {
                throw Bridge\ExceptionMapper::map($e, 'BEGIN TRANSACTION');
            }
        }

        $this->transactions[Coroutine::getCid()] = new Bridge\PooledTransaction(
            $transaction,
            $this->logger
        );

        return $transaction->isActive();
    }

    public function commitTransaction(): bool
    {
        $transaction = $this->getTransactionConn();
        if ($transaction === null) {
            return false;
        }

        try {
            $transaction->commit();
        } finally {
            if ($transaction->getTransactionLevel() === 0) {
                unset($this->transactions[Coroutine::getCid()]);
            }
        }

        return true;
    }

    public function rollbackTransaction(): bool
    {
        $transaction = $this->getTransactionConn();
        if ($transaction === null) {
            return false;
        }

        try {
            $transaction->rollbackTransaction();
        } finally {
            if ($transaction->getTransactionLevel() === 0) {
                unset($this->transactions[Coroutine::getCid()]);
            }
        }

        return true;
    }

    /**
     * @inheritdoc
     */
    public function identifier(string $identifier): string
    {
        return $this->queryCompiler->quoteIdentifier($identifier);
    }

    /**
     * Get driver source database or file name.
     *
     * @return string
     */
    public function getSource(): string
    {
        return $this->connectionConfig->getDatabase() ?? '*';
    }

    /**
     * Get or create table schema.
     *
     * @param string $table
     * @param string|null $prefix
     * @return AbstractTable
     *
     * @throws HandlerException
     */
    public function getSchema(string $table, ?string $prefix = null): AbstractTable
    {
        return $this->getSchemaHandler()->getSchema(
            $table,
            $prefix
        );
    }

    /**
     * Compatibility with deprecated methods.
     *
     * @param string $name
     * @param array $arguments
     * @return mixed
     *
     * @deprecated this method will be removed in a future releases.
     */
    public function __call(string $name, array $arguments)
    {
        switch ($name) {
            case 'isProfiling':
                return true;
            case 'setProfiling':
                return null;
            case 'getSchema':
                return $this->getSchemaHandler()->getSchema(
                    $arguments[0],
                    $arguments[1] ?? null
                );
            case 'tableNames':
                return $this->getSchemaHandler()->getTableNames();
            case 'hasTable':
                return $this->getSchemaHandler()->hasTable($arguments[0]);
            case 'identifier':
                return $this->getQueryCompiler()->quoteIdentifier($arguments[0]);
            case 'eraseData':
                return $this->getSchemaHandler()->eraseTable(
                    $this->getSchemaHandler()->getSchema($arguments[0])
                );

            case 'insertQuery':
            case 'selectQuery':
            case 'updateQuery':
            case 'deleteQuery':
                return call_user_func_array(
                    [$this->queryBuilder, $name],
                    $arguments
                );
        }

        throw new DriverException("Undefined driver method `{$name}`");
    }

    /**
     * Get singular primary key associated with desired table. Used to emulate last insert id.
     *
     * @param string $prefix Database prefix if any.
     * @param string $table Fully specified table name, including postfix.
     *
     * @return string|null
     *
     * @throws DriverException
     */
    public function getPrimaryKey(string $prefix, string $table): ?string
    {
        $name = $prefix . $table;
        if (isset($this->primaryKeys[$name])) {
            return $this->primaryKeys[$name];
        }

        $this->pkCacheLock->lock();

        if (isset($this->primaryKeys[$name])) {
            $this->pkCacheLock->unlock();

            return $this->primaryKeys[$name];
        }

        if (!$this->getSchemaHandler()->hasTable($name)) {
            $this->pkCacheLock->unlock();

            throw new DriverException(
                "Unable to fetch table primary key, no such table '{$name}' exists"
            );
        }

        $this->primaryKeys[$name] = $this->getSchemaHandler()
            ->getSchema($table, $prefix)
            ->getPrimaryKeys();

        if (count($this->primaryKeys[$name]) === 1) {
            //We do support only single primary key
            $this->primaryKeys[$name] = $this->primaryKeys[$name][0];
        } else {
            $this->primaryKeys[$name] = null;
        }

        $this->pkCacheLock->unlock();

        return $this->primaryKeys[$name];
    }

    /**
     * Reset primary keys cache.
     */
    public function resetPrimaryKeys(): void
    {
        $this->pkCacheLock->lock();

        $this->primaryKeys = [];

        $this->pkCacheLock->unlock();
    }

    /**
     * Create instance of PDOStatement using provided SQL query and set of parameters and execute
     * it. Will attempt singular reconnect.
     *
     * @param string $query
     * @param iterable $parameters
     * @param bool|null $retry
     * @return StatementInterface
     *
     * @throws StatementException
     */
    protected function statement(
        string $query,
        iterable $parameters = [],
        bool $retry = true
    ): StatementInterface {
        $queryStart = microtime(true);

        try {
            $parsedParameters = $this->getParameters($parameters);

            $statement = $this->prepare($query);
            $result = $statement->execute($parsedParameters);

            return new Statement($statement, $result);
        } catch (Throwable $e) {
            $e = Bridge\ExceptionMapper::map($e, Interpolator::interpolate($query, $parameters));

            if (
                $retry
                && $this->getTransactionConn() === null
                && $e instanceof StatementException\ConnectionException
            ) {
                unset($statement);

                return $this->statement($query, $parameters, false);
            }

            // an exception occurred during transaction
            if ($this->getTransactionConn() !== null) {
                try {
                    unset($statement);
                } catch (\Throwable $stmtCloseEx) {
                }
            }

            throw $e;
        } finally {
            if ($this->logger !== null) {
                $queryString = Interpolator::interpolate($query, $parameters);
                $context = [
                    'elapsed' => microtime(true) - $queryStart
                ];

                if (isset($e)) {
                    $this->logger->error($queryString, $context);
                    $this->logger->alert($e->getMessage());
                } else {
                    $this->logger->info($queryString, $context);
                }
            }
        }
    }

    /**
     * @param string $query
     *
     * @return PostgresStatement
     * @throws FailureException
     */
    protected function prepare(string $query): PostgresStatement
    {
        if (null !== ($transaction = $this->getTransactionConn())) {
            return $transaction->getTransaction()->prepare($query);
        }

        return $this->pool->prepare($query);
    }

    /**
     * Normalizer parameters to use as statement parameters
     *
     * @param iterable $parameters
     * @return array
     */
    protected function getParameters(iterable $parameters): array
    {
        $normParams = [];

        foreach ($parameters as $key => $param) {
            if (is_string($key) && 0 === strpos($key, ':')) {
                $key = substr($key, 1);
            }

            if ($param instanceof ParameterInterface) {
                $value = $param->getValue();
                if ($value instanceof DateTimeInterface) {
                    $value = $this->formatDatetime($value);
                }

                $normParams[$key] = $value;
            } elseif ($param instanceof DateTimeInterface) {
                $normParams[$key] = $this->formatDatetime($param);
            } else {
                $normParams[$key] = $param;
            }
        }

        return $normParams;
    }

    private function getTransactionConn(): ?Bridge\PooledTransaction
    {
        return $this->transactions[Coroutine::getCid()] ?? null;
    }

    private function quoteString(Quoter $quoter, $value): string
    {
        if ($value instanceof DateTimeInterface) {
            $value = $this->formatDatetime($value);
        }

        return $quoter->quoteString($value);
    }

    /**
     * Convert DateTime object into local database representation. Driver will automatically force
     * needed timezone.
     *
     * @param DateTimeInterface $value
     * @return string
     *
     * @throws DriverException
     */
    protected function formatDatetime(DateTimeInterface $value): string
    {
        try {
            $datetime = new DateTimeImmutable('now', $this->getTimezone());
        } catch (Throwable $e) {
            throw new DriverException($e->getMessage(), $e->getCode(), $e);
        }

        return $datetime->setTimestamp($value->getTimestamp())->format(static::DATETIME);
    }
}
