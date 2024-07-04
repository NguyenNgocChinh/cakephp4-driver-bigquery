<?php

declare(strict_types=1);

namespace App\Database\BigQuery;

use App\Database\BigQuery\Driver\BigQuery;
use Cake\Database\Exception\MissingConnectionException;
use Cake\Database\Connection as CakeConnection;
use Exception;

class Connection extends CakeConnection
{
    /**
     * Contains the configuration param for this connection
     *
     * @var array
     */
    protected $_config;

    /**
     * Database Driver object
     *
     */
    protected $_driver = null;

    /**
     * MongoSchema
     *
     * @var array BigQuerySchema
     * @access protected
     */
    protected $_schemaCollection;

    /**
     * disconnect existent connection
     *
     * @access public
     * @return void
     */
    public function __destruct()
    {
        if ($this->_driver->connected) {
            $this->_driver->disconnect();
            unset($this->_driver);
        }
    }

    /**
     * return configuration name
     *
     * @return string
     * @access public
     */
    public function configName(): string
    {
        return 'bigquery';
    }

    /**
     * @param BigQuery $driver Driver
     * @param array $config Configure
     * @return BigQuery
     */
    public function driver($driver = null, $config = [])
    {
        if ($driver === null) {
            return $this->_driver;
        }
        $this->_driver = new BigQuery($config);

        return $this->_driver;
    }

    /**
     * connect to the database
     *
     * @access public
     * @return bool
     */
    public function connect(): bool
    {
        try {
            $this->_driver->connect();

            return true;
        } catch (Exception $e) {
            throw new MissingConnectionException(['reason' => $e->getMessage()]);
        }
    }

    /**
     * disconnect from the database
     *
     * @access public
     * @return bool
     */
    public function disconnect(): void
    {
        if ($this->_driver->isConnected()) {
            $this->_driver->disconnect();
        }

    }
}
