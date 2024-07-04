<?php

declare(strict_types=1);

namespace App\Database\BigQuery\Driver;

use App\Database\BigQuery\Schema\BigQuerySchemaDialect;
use Cake\Database\DriverInterface;
use Cake\Database\Driver;
use Closure;
use Exception;
use Google\Cloud\BigQuery\BigQueryClient;

class BigQuery extends Driver implements DriverInterface
{
    protected $_config;
    public $connected = false;
    protected $_db = null;

    protected $_baseConfig = [
        'projectId' => null,
        'dataSet' => null,
        'keyFile' => [], // json
        'keyFilePath' => null,
        'requestTimeout' => 0,
        'retries' => 3,
        'location' => '',
        'maximumBytesBilled' => 1000000,
    ];

    /**
     * Constructor
     */
    public function __construct($config)
    {
        $this->_config = array_merge($this->_baseConfig, $config);
    }

    /**
     * {@inheritdoc}
     *
     * Returns the name of the configuration used by this driver.
     *
     * @return string The name of the configuration.
     */
    public function configName(): string
    {
        return $this->_config['name'] ?? '';
    }

    /**
     * {@inheritdoc}
     *
     * Returns the configuration value for a given key.
     * If no key is provided, it returns the entire configuration array.
     *
     * @param string $key The key of the configuration value to retrieve.
     * @return mixed The configuration value for the given key, or the entire configuration array if no key is provided.
     */
    public function getConfig(string $key = '')
    {
        if ($key) {
            return $this->_config[$key];
        } else {
            return $this->_config;
        }
    }

    /**
     * {@inheritdoc}
     *
     * Connects to the BigQuery service using the provided configuration.
     *
     * @return bool Returns true if the connection is successful, false otherwise.
     * @throws Exception If any error occurs during the connection process.
     */
    public function connect(): bool
    {
        try {
            $config = [
                'projectId' => $this->_config['projectId'],
                'requestTimeout' => $this->_config['requestTimeout'],
                'retries' => $this->_config['retries'],
            ];

            if ($this->_config['keyFile']) {
                $config['keyFile'] = $this->_config['keyFile'];
            }
            if ($this->_config['keyFilePath']) {
                $config['keyFilePath'] = $this->_config['keyFilePath'];
            }
            if ($this->_config['location']) {
                $config['location'] = $this->_config['location'];
            }

            $this->_db = new BigQueryClient($config);
            $this->connected = true;
        } catch (Exception $e) {
            trigger_error($e->getMessage());
        }

        return $this->connected;
    }

    /**
     * {@inheritdoc}
     *
     * Returns the BigQuery client instance.
     * If the client is not connected, it will attempt to establish a connection.
     *
     * @return \Google\Cloud\BigQuery\BigQueryClient The BigQuery client instance.
     * @throws \Exception If any error occurs during the connection process.
     */
    public function getConnection()
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        return $this->_db;
    }

    /**
    * {@inheritdoc}
    * Disconnects from the BigQuery service.
    *
    * @return void
    */
    public function disconnect(): void
    {
        if ($this->connected) {
            $this->connected = false;
        }
    }

    /**
    * {@inheritdoc}
    *
    * Checks if the BigQuery client is connected.
    *
    * @return bool Returns true if the client is connected, false otherwise.
    */
    public function isConnected(): bool
    {
        return $this->connected;
    }

    /**
     * {@inheritdoc}
     *
     * Checks if the driver is enabled.
     *
     * @return bool Returns true if the driver is enabled, false otherwise.
     *
     * @throws \Exception If any error occurs during the check.
     */
    public function enabled(): bool
    {
        return true;
    }

    /**
     * {@inheritdoc}
     *
     * Checks if the driver supports dynamic constraints.
     *
     * BigQuery does not support dynamic constraints.
     *
     * @return bool Returns false, indicating that dynamic constraints are not supported.
     */
    public function supportsDynamicConstraints(): bool
    {
        return false;
    }

    /**
    * {@inheritdoc}
    *
    * Checks if the driver supports save points.
    *
    * BigQuery does not support save points.
    *
    * @return bool Returns false, indicating that save points are not supported.
    */
    public function supportsSavePoints(): bool
    {
        return false;
    }

    /**
     * {@inheritdoc}
     * Returns an instance of BigQuerySchemaDialect for schema operations.
     *
     * @return \App\Database\BigQuery\Schema\BigQuerySchemaDialect An instance of BigQuerySchemaDialect.
     */
    public function schemaDialect(): BigQuerySchemaDialect
    {
        return new BigQuerySchemaDialect($this);
    }

    /**
     * {@inheritdoc}
     *
     * Returns the last inserted ID for a table.
     *
     * BigQuery does not support getting the last inserted ID for a table.
     * This method always returns null.
     *
     * @param string|null $table The name of the table. This parameter is ignored in BigQuery.
     * @param string|null $column The name of the column.
     */
    public function lastInsertId($table = null, $column = null)
    {
        return null;
    }

    /**
     * {@inheritdoc}
     *
     * Attempts to start a transaction.
     *
     * BigQuery does not support transactions. This method always returns false.
     *
     * @return bool Returns false, indicating that transactions are not supported in BigQuery.
     */
    public function startTransaction(): bool
    {
        // BigQuery no support transaction
        return false;
    }

    /**
     * {@inheritdoc}
     *
     * Commits the current transaction.
     *
     * BigQuery does not support transactions. This method always returns false.
     *
     * @return bool Returns false, indicating that transactions are not supported in BigQuery.
     */
    public function commitTransaction(): bool
    {
        // BigQuery no support transaction
        return false;
    }

    /**
     * {@inheritdoc}
     *
     * Rolls back the current transaction.
     *
     * BigQuery does not support transactions. This method always returns false.
     *
     * @return bool Returns false, indicating that transactions are not supported in BigQuery.
     */
    public function rollbackTransaction(): bool
    {
        // BigQuery no support transaction
        return false;
    }

    /**
     *{@inheritdoc}
     *
     *  Returns a closure that translates queries based on the given type.
     *
     * @param string $type The type of query translation.
     * @return \Closure A closure that takes a query as input and returns the translated query.
     *
     * @throws \InvalidArgumentException If the given type is not supported.
     */
    public function queryTranslator(string $type): Closure
    {
        return function ($query) {
            // Implement your query translation logic here
            return $query;
        };
    }

    /**
     * {@inheritdoc}
     *
     * Returns the SQL statement to release a savepoint.
     *
     * BigQuery does not support save points.
     * This method always returns an empty string.
     *
     * @param string $name The name of the savepoint. This parameter is ignored in BigQuery.
     * @return string An empty string, indicating that save points are not supported in BigQuery.
     */
    public function quoteIdentifier($identifier): string
    {
        return $identifier;
    }

    public function releaseSavePointSQL($name): string
    {
        return '';
    }

    /**
     * {@inheritdoc}
     *
     * Returns the SQL statement to create a savepoint.
     *
     * BigQuery does not support save points.
     * This method always returns an empty string.
     *
     * @param string $name The name of the savepoint. This parameter is ignored in BigQuery.
     * @return string An empty string, indicating that save points are not supported in BigQuery.
     */
    public function savePointSQL($name): string
    {
        return '';
    }

    /**
     * {@inheritdoc}
     *
     * Returns the SQL statement to rollback to a savepoint.
     *
     * BigQuery does not support save points.
     * This method always returns an empty string.
     *
     * @param string $name The name of the savepoint. This parameter is ignored in BigQuery.
     * @return string An empty string, indicating that save points are not supported in BigQuery.
     */
    public function rollbackSavePointSQL($name): string
    {
        return '';
    }

    /**
     * {@inheritdoc}
     *
     * Returns the SQL statement to disable foreign key constraints.
     *
     * BigQuery does not support foreign key constraints.
     * This method always returns an empty string.
     *
     * @return string An empty string, indicating that foreign key constraints are not supported in BigQuery.
     */
    public function disableForeignKeySQL(): string
    {
        return '';
    }

    /**
     * {@inheritdoc}
     *
     * Returns the SQL statement to enable foreign key constraints.
     *
     * BigQuery does not support foreign key constraints.
     * This method always returns an empty string.
     *
     * @return string An empty string, indicating that foreign key constraints are not supported in BigQuery.
     */
    public function enableForeignKeySQL(): string
    {
        return '';
    }
}
