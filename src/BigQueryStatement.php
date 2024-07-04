<?php
declare(strict_types=1);

namespace Chinh\BigQuery;

use Cake\Database\StatementInterface;
use Google\Cloud\BigQuery\QueryResults;

class BigQueryStatement implements StatementInterface
{
    protected $results;
    protected $iterator;

    public function __construct(QueryResults $results)
    {
        $this->results = $results;
        $this->iterator = $results->getIterator();
    }

    /**
     * {@inheritdoc}
     * 
     * Executes the statement.
     *
     * In the case of BigQuery, as the results are already fetched from the server,
     * this method does nothing and always returns true.
     *
     * @param array|null $params The parameters to bind to the statement.
     * @return bool Returns true to indicate that the statement was successfully executed.
     */
    public function execute(?array $params = null): bool
    {
        // BigQuery results are already executed
        return true;
    }

    /**
     * {@inheritdoc}
     * 
    * Fetches the next row from the result set.
    *
    * @param string $type The type of array to return: 'assoc' (default) for associative array, 'num' for numeric array.
    * @return array|false Returns the next row as an associative or numeric array, or false if there are no more rows.
    */
    public function fetch($type = 'assoc')
    {
        if ($this->iterator->valid()) {
            $current = $this->iterator->current();
            $current = $this->prepareData($current);
            $this->iterator->next();

            if ($type === 'assoc') {
                return (array) $current;
            }
        }
        return false;
    }

    /**
     * {@inheritdoc}
     * 
     * Fetches all rows from the result set.
     *
     * @param string $type The type of array to return: 'assoc' (default) for associative array, 'num' for numeric array.
     * @return array Returns all rows as an associative or numeric array.
     */
    public function fetchAll($type = 'assoc'): array
    {
        if ($type === 'assoc') {
            $results = [];
            foreach ($this->results as $row) {
                $results[] = $this->prepareData($row);
            }
            return $results;
        }
        return [];
    }

    /**
     * {@inheritdoc}
     * 
     * Closes the cursor, enabling the statement to be executed again.
     *
     * In the case of BigQuery, as the results are already fetched from the server,
     * this method does nothing.
     *
     * @return void
     */
    public function closeCursor(): void
    {
        // BigQuery results do not need to close cursor
    }

    /**
     * {@inheritdoc}
     * 
     * Returns the number of rows affected by the last SQL statement.
     *
     * In the case of BigQuery, as the results are already fetched from the server,
     * this method returns the count of rows in the result set.
     *
     * @return int Returns the number of rows affected by the last SQL statement.
     */
    public function rowCount(): int
    {
        return iterator_count($this->results->getIterator());
    }

    /**
     * {@inheritdoc}
     * 
     * Returns the number of columns in the result set.
     *
     * @return int Returns the number of columns in the result set.
     *
     * @throws \Exception If the schema information is not available.
     */
    public function columnCount(): int
    {
        return count($this->results->info()['schema']['fields']);
    }

    /**
     * {@inheritdoc}
     * 
     * Returns the SQLSTATE error code (if available) associated with the last operation on the statement.
     *
     * In the case of BigQuery, as we are not interacting directly with a database,
     * this method returns null.
     *
     * @return string|null Returns the SQLSTATE error code, or null if not available.
     */
    public function errorCode(): ?string
    {
        // Handle BigQuery error code if needed
        return null;
    }

    /**
     * {@inheritdoc}
     * 
     * Returns the error information associated with the last operation on the statement.
     *
     * In the case of BigQuery, as we are not interacting directly with a database,
     * this method returns an empty array.
     *
     * @return array Returns an array of error information, where the first element is the SQLSTATE error code,
     *               the second element is the driver-specific error code, and the third element is the driver-specific error message.
     *               If no error occurred, this method returns an empty array.
     */
    public function errorInfo(): array
    {
        // Handle BigQuery error info if needed
        return [];
    }

    /**
     * {@inheritdoc}
     * 
     * Binds a value to a parameter in the SQL statement.
     *
     * @param mixed $param Parameter identifier. For a prepared statement using named placeholders, this will be a parameter name of the form :name.
     *                      For a prepared statement using question mark placeholders, this will be the 1-indexed position of the parameter.
     * @param mixed $value The value to bind to the parameter.
     * @param string $type Explicit data type for the parameter. If omitted, PDO will attempt to determine the data type.
     *
     * @throws PDOException If the parameter cannot be bound.
     *
     * @return void
     */
    public function bindValue($param, $value, $type = 'string'): void
    {
        // Implement bind value logic if needed
    }

    /**
     * {@inheritdoc}
     * 
     * Fetches a single column from the next row of a result set and returns it as a string.
     *
     * @param int $columnIndex The 0-indexed number of the column you wish to retrieve from the row.
     *                         If no value is supplied, fetchColumn() will return the first column.
     *
     * @return mixed Returns the value of the specified column in the current row or false if there are no more rows.
     *
     * @throws Exception If the specified column does not exist.
     */
    public function fetchColumn($columnIndex = 0)
    {
        if ($this->iterator->valid()) {
            $current = (array) $this->iterator->current();
            $this->iterator->next();
            $keys = array_keys($current);
            if (isset($keys[$columnIndex])) {
                return $current[$keys[$columnIndex]];
            }
        }
        return false;
    }

    /**
     * {@inheritdoc}
     * 
     * Returns the number of rows in the result set.
     *
     * This method is a part of the IteratorAggregate interface, which allows
     * the object to be treated as an iterator to loop through the result set.
     *
     * @return int Returns the number of rows in the result set.
     *
     * @throws Exception If the schema information is not available.
     */
    public function count(): int
    {
        return iterator_count($this->results->getIterator());
    }

    /**
     * {@inheritdoc}
     * 
     * Binds a value to a parameter in the SQL statement.
     *
     * This method is part of the PDOStatement interface, which allows the object to be used as a prepared statement.
     *
     * @param mixed $param Parameter identifier. For a prepared statement using named placeholders, this will be a parameter name of the form :name.
     *                      For a prepared statement using question mark placeholders, this will be the 1-indexed position of the parameter.
     * @param mixed $variable The value to bind to the parameter.
     * @param string|null $type Explicit data type for the parameter. If omitted, PDO will attempt to determine the data type.
     * @param int|null $length The length of the data type. If omitted, PDO will use the default length.
     *
     * @throws PDOException If the parameter cannot be bound.
     *
     * @return void
     */
    public function bind($param, $variable, $type = null, $length = null): void
    {
        // Implement bind logic if needed
    }

    /**
     * {@inheritdoc}
     * 
     * Returns the ID of the last inserted row or sequence value.
     *
     * This method is part of the PDO interface, which allows the object to be used as a database connection.
     * In the case of BigQuery, as we are not interacting directly with a database,
     * this method always returns null.
     *
     * @param string|null $table The name of the table into which the row was inserted.
     *                            If not specified, the name of the table is inferred from the query.
     * @param string|null $column The name of the column that holds the auto-incremented ID.
     *                             If not specified, the column name is inferred from the query.
     *
     * @return string|null Returns the ID of the last inserted row or sequence value, or null if not available.
     */
    public function lastInsertId($table = null, $column = null)
    {
        return null;
    }

    /**
     * Prepares the data for the result set.
     *
     * This method iterates through each row in the result set and checks if any value is an instance of \DateTime.
     * If a \DateTime value is found, it is formatted as a string in the 'Y-m-d H:i:s' format.
     *
     * @param array $row The row to be prepared.
     *
     * @return array The prepared row with any \DateTime values formatted as strings.
     */
    protected function prepareData($row)
    {
        foreach ($row as $key => $value) {
            if ($value instanceof \DateTime) {
                $row[$key] = $value->format('Y-m-d H:i:s');
            }
        }
        return $row;
    }
}
