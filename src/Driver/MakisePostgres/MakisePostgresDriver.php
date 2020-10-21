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
use MakiseCo\Postgres\Connection;
use MakiseCo\Postgres\ConnectionConfig;
use MakiseCo\Postgres\ConnectionConfigBuilder;
use MakiseCo\Postgres\Contracts\Transaction;
use MakiseCo\Postgres\Driver\Pq\PqConnection;
use MakiseCo\Postgres\Exception\QueryExecutionError;
use MakiseCo\SqlCommon\Contracts\Statement as PostgresStatement;
use MakiseCo\SqlCommon\Exception\ConnectionException;
use MakiseCo\SqlCommon\Exception\FailureException;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use Spiral\Database\Driver\CachingCompilerInterface;
use Spiral\Database\Driver\CompilerCache;
use Spiral\Database\Driver\CompilerInterface;
use Spiral\Database\Driver\DriverInterface;
use Spiral\Database\Driver\HandlerInterface;
use Spiral\Database\Driver\Postgres\PostgresCompiler;
use Spiral\Database\Driver\ReadonlyHandler;
use Spiral\Database\Exception\DriverException;
use Spiral\Database\Exception\StatementException;
use Spiral\Database\Injection\ParameterInterface;
use Spiral\Database\Query\BuilderInterface;
use Spiral\Database\Query\DeleteQuery;
use Spiral\Database\Query\Interpolator;
use Spiral\Database\Query\QueryBuilder;
use Spiral\Database\Query\SelectQuery;
use Spiral\Database\Query\UpdateQuery;
use Spiral\Database\StatementInterface;
use Throwable;

class MakisePostgresDriver implements DriverInterface, LoggerAwareInterface
{
    use LoggerAwareTrait;

    // DateTime format to be used to perform automatic conversion of DateTime objects.
    protected const DATETIME = 'Y-m-d H:i:s';

    /**
     * Connection configuration described in DBAL config file. Any driver can be used as data source
     * for multiple databases as table prefix and quotation defined on Database instance level.
     *
     * @var array
     */
    protected $options = [
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

        // DSN
        'connection' => '',
        'username' => '',
        'password' => '',

        // pdo options
        'options' => [],

        // enables query caching
        'queryCache' => true,

        // disable schema modifications
        'readonlySchema' => false
    ];

    private ?Connection $connection = null;

    private ?Transaction $transaction = null;

    private int $transactionLevel = 0;

    private HandlerInterface $schemaHandler;

    private CompilerInterface $queryCompiler;

    private BuilderInterface $queryBuilder;

    /** @var PostgresStatement[] */
    private array $queryCache = [];

    /**
     * Cached list of primary keys associated with their table names. Used by InsertBuilder to
     * emulate last insert id.
     *
     * @var array
     */
    private array $primaryKeys = [];

    private ConnectionConfig $config;

    /**
     * @param array $options
     */
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

        $this->options = array_replace(
            $this->options,
            $options
        );

        $this->config = (new ConnectionConfigBuilder())
            ->fromArray(OptionsProcessor::cleanOptions($this->options))
            ->build();

        if ($this->options['queryCache'] && $this->queryCompiler instanceof CachingCompilerInterface) {
            $this->queryCompiler = new CompilerCache($this->queryCompiler);
        }

        if ($this->options['readonlySchema']) {
            $this->schemaHandler = new ReadonlyHandler($this->schemaHandler);
        }
    }

    /**
     * Disconnect and destruct.
     */
    public function __destruct()
    {
        $this->disconnect();
    }

    /**
     * @return array
     */
    public function __debugInfo()
    {
        return [
            'addr' => $this->getDSN(),
            'source' => $this->getSource(),
            'connected' => $this->isConnected(),
            'options' => $this->options['options'],
        ];
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
     * {@inheritdoc}
     */
    public function getType(): string
    {
        return 'MakisePostgres';
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

        if (!$this->getSchemaHandler()->hasTable($name)) {
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

        return $this->primaryKeys[$name];
    }

    /**
     * Reset primary keys cache.
     */
    public function resetPrimaryKeys(): void
    {
        $this->primaryKeys = [];
    }

    /**
     * Get driver source database or file name.
     *
     * @return string
     *
     * @throws DriverException
     */
    public function getSource(): string
    {
        return $this->config->getDatabase() ?? '*';
    }

    /**
     * @inheritDoc
     */
    public function getTimezone(): DateTimeZone
    {
        return new DateTimeZone($this->options['timezone']);
    }

    /**
     * @inheritdoc
     */
    public function getSchemaHandler(): HandlerInterface
    {
        // do not allow to carry prepared statements between schema changes
        $this->queryCache = [];

        return $this->schemaHandler;
    }

    /**
     * @inheritdoc
     */
    public function getQueryCompiler(): CompilerInterface
    {
        return $this->queryCompiler;
    }

    /**
     * @return BuilderInterface
     */
    public function getQueryBuilder(): BuilderInterface
    {
        return $this->queryBuilder;
    }

    /**
     * Force driver connection.
     *
     * @throws DriverException
     */
    public function connect(): void
    {
        if ($this->connection === null || !$this->connection->isAlive()) {
            $this->connection = $this->createPDO();
        }
    }

    /**
     * Check if driver already connected.
     *
     * @return bool
     */
    public function isConnected(): bool
    {
        return $this->connection !== null && $this->connection->isAlive();
    }

    /**
     * Disconnect driver.
     */
    public function disconnect(): void
    {
        try {
            $this->transaction = null;
        } catch (Throwable $e) {
            // disconnect error
            if ($this->logger !== null) {
                $this->logger->error($e->getMessage());
            }
        }

        try {
            $this->connection->close();
        } catch (Throwable $e) {
            // disconnect error
            if ($this->logger !== null) {
                $this->logger->error($e->getMessage());
            }
        }

        try {
            $this->queryCache = [];
        } catch (Throwable $e) {
            // disconnect error
            if ($this->logger !== null) {
                $this->logger->error($e->getMessage());
            }
        }

        $this->transactionLevel = 0;
    }

    /**
     * @inheritdoc
     */
    public function quote($value, int $type = 2): string
    {
        if ($value instanceof DateTimeInterface) {
            $value = $this->formatDatetime($value);
        }

        return $this->getPDO()->quoteString($value);
    }

    /**
     * Execute query and return query statement.
     *
     * @param string $statement
     * @param array $parameters
     * @return StatementInterface
     *
     * @throws StatementException
     */
    public function query(string $statement, array $parameters = []): StatementInterface
    {
        return $this->statement($statement, $parameters);
    }

    /**
     * Execute query and return number of affected rows.
     *
     * @param string $query
     * @param array $parameters
     * @return int
     *
     * @throws StatementException
     */
    public function execute(string $query, array $parameters = []): int
    {
        return $this->statement($query, $parameters)->rowCount();
    }

    /**
     * Get id of last inserted row, this method must be called after insert query. Attention,
     * such functionality may not work in some DBMS property (Postgres).
     *
     * @param string|null $sequence Name of the sequence object from which the ID should be returned.
     * @return mixed
     */
    public function lastInsertID(string $sequence = null)
    {
        return null;
//        $result = $this->getPDO()->lastInsertId();
//        if ($this->logger !== null) {
//            $this->logger->debug("Insert ID: {$result}");
//        }
//
//        return $result;
    }

    /**
     * Start SQL transaction with specified isolation level (not all DBMS support it). Nested
     * transactions are processed using savepoints.
     *
     * @link http://en.wikipedia.org/wiki/Database_transaction
     * @link http://en.wikipedia.org/wiki/Isolation_(database_systems)
     *
     * @param string|null $isolationLevel
     * @return bool
     */
    public function beginTransaction(?string $isolationLevel = null): bool
    {
        $this->transactionLevel++;

        if ($this->transactionLevel === 1) {
            if ($isolationLevel !== null) {
                $this->setIsolationLevel($isolationLevel);
            }

            if ($this->logger !== null) {
                $this->logger->info('Begin transaction');
            }

            try {
                $this->transaction = $this->getPDO()->beginTransaction();

                return $this->transaction->isActive();
            } catch (Throwable $e) {
                $e = $this->mapException($e, 'BEGIN TRANSACTION');

                if (
                    $e instanceof StatementException\ConnectionException
                    && $this->options['reconnect']
                ) {
                    $this->disconnect();

                    try {
                        $this->transaction = $this->getPDO()->beginTransaction();

                        return $this->transaction->isActive();
                    } catch (Throwable $e) {
                        throw $this->mapException($e, 'BEGIN TRANSACTION');
                    }
                }
            }
        }

        $this->createSavepoint($this->transactionLevel);

        return true;
    }

    /**
     * Commit the active database transaction.
     *
     * @return bool
     */
    public function commitTransaction(): bool
    {
        $this->transactionLevel--;

        if ($this->transactionLevel === 0) {
            if ($this->logger !== null) {
                $this->logger->info('Commit transaction');
            }

            try {
                $this->transaction->commit();

                return true;
            } catch (Throwable $e) {
                throw $this->mapException($e, 'COMMIT TRANSACTION');
            }
        }

        $this->releaseSavepoint($this->transactionLevel + 1);

        return true;
    }

    /**
     * Rollback the active database transaction.
     *
     * @return bool
     */
    public function rollbackTransaction(): bool
    {
        $this->transactionLevel--;

        if ($this->transactionLevel === 0) {
            if ($this->logger !== null) {
                $this->logger->info('Rollback transaction');
            }

            try {
                $this->transaction->rollback();

                return true;
            } catch (Throwable $e) {
                throw $this->mapException($e, 'ROLLBACK TRANSACTION');
            }
        }

        $this->rollbackSavepoint($this->transactionLevel + 1);

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
            $parsedParameters = $this->getParameters($query, $parameters);

            $statement = $this->prepare($query);
            $result = $statement->execute($parsedParameters);

            return new Statement($statement, $result);
        } catch (Throwable $e) {
            $e = $this->mapException($e, Interpolator::interpolate($query, $parameters));

            if (
                $retry
                && $this->transactionLevel === 0
                && $e instanceof StatementException\ConnectionException
            ) {
                $this->disconnect();

                return $this->statement($query, $parameters, false);
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
        if ($this->options['queryCache'] && isset($this->queryCache[$query])) {
            return $this->queryCache[$query];
        }

        $statement = $this->getPDO()->prepare($query);
        if ($this->options['queryCache']) {
            $this->queryCache[$query] = $statement;
        }

        return $statement;
    }

    /**
     * Normalizer parameters to use as statement parameters
     *
     * @param string $query
     * @param iterable $parameters
     * @return array
     */
    protected function getParameters(string $query, iterable $parameters): array
    {
        $normParams = [];
        foreach ($parameters as $key => $param) {
            if (\is_string($key) && 0 === \strpos($key, ':')) {
                $key = \substr($key, 1);
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

    /**
     * Convert PDO exception into query or integrity exception.
     *
     * @param Throwable $exception
     * @param string $query
     * @return StatementException
     */
    protected function mapException(
        Throwable $exception,
        string $query
    ): StatementException {
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

    /**
     * Set transaction isolation level, this feature may not be supported by specific database
     * driver.
     *
     * @param string $level
     */
    protected function setIsolationLevel(string $level): void
    {
        if ($this->logger !== null) {
            $this->logger->info("Transaction isolation level '{$level}'");
        }

        $this->execute("SET TRANSACTION ISOLATION LEVEL {$level}");
    }

    /**
     * Create nested transaction save point.
     *
     * @link http://en.wikipedia.org/wiki/Savepoint
     *
     * @param int $level Savepoint name/id, must not contain spaces and be valid database identifier.
     */
    protected function createSavepoint(int $level): void
    {
        if ($this->logger !== null) {
            $this->logger->info("Transaction: new savepoint 'SVP{$level}'");
        }

        $this->execute('SAVEPOINT ' . $this->identifier("SVP{$level}"));
    }

    /**
     * Commit/release savepoint.
     *
     * @link http://en.wikipedia.org/wiki/Savepoint
     *
     * @param int $level Savepoint name/id, must not contain spaces and be valid database identifier.
     */
    protected function releaseSavepoint(int $level): void
    {
        if ($this->logger !== null) {
            $this->logger->info("Transaction: release savepoint 'SVP{$level}'");
        }

        $this->execute('RELEASE SAVEPOINT ' . $this->identifier("SVP{$level}"));
    }

    /**
     * Rollback savepoint.
     *
     * @link http://en.wikipedia.org/wiki/Savepoint
     *
     * @param int $level Savepoint name/id, must not contain spaces and be valid database identifier.
     */
    protected function rollbackSavepoint(int $level): void
    {
        if ($this->logger !== null) {
            $this->logger->info("Transaction: rollback savepoint 'SVP{$level}'");
        }

        $this->execute('ROLLBACK TO SAVEPOINT ' . $this->identifier("SVP{$level}"));
    }

    /**
     * Create instance of configured PDO class.
     *
     * @return Connection
     */
    protected function createPDO(): Connection
    {
        return PqConnection::connect($this->config);
    }

    /**
     * Get associated PDO connection. Must automatically connect if such connection does not exists.
     *
     * @return Connection
     *
     * @throws DriverException
     */
    protected function getPDO(): Connection
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        return $this->connection;
    }

    /**
     * Connection DSN.
     *
     * @return string
     */
    protected function getDSN(): string
    {
        return $this->config->getConnectionString();
    }
}