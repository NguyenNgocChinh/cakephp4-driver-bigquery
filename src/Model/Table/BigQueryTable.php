<?php

declare(strict_types=1);

namespace Chinh\BigQuery\Model\Table;

use Exception;
use ArrayObject;
use Cake\ORM\Table;
use ReflectionClass;
use RuntimeException;
use Cake\Datasource\EntityInterface;
use Cake\Database\Schema\TableSchema;
use Cake\Datasource\ConnectionManager;
use Chinh\BigQuery\BigQueryQuery;
use Google\Cloud\BigQuery\BigQueryClient;
use Cake\Database\Schema\TableSchemaInterface;
use Cake\ORM\Exception\RolledbackTransactionException;

class BigQueryTable extends Table
{
    protected $bigQueryClient;
    protected $dataset;
    protected $datasetName;
    protected $projectId;

    /**
     * {@inheritDoc}
     *
     * Initializes the table instance.
     *
     * Called after the constructor method.
     * Allows you to modify any other property or perform additional initialization.
     *
     * @param array $config The configuration for the Table.
     * @return void
     */
    public function initialize(array $config): void
    {
        parent::initialize($config);

        $this->setConnection(ConnectionManager::get('bigquery'));

        // Get BigQuery configuration
        $connection = $this->getConnection();
        $config = $connection->config();

        $this->projectId = $config['projectId'];
        $this->datasetName = $config['dataSet'];

        // Initialize BigQuery client
        $this->bigQueryClient = new BigQueryClient([
            'projectId' => $this->projectId,
            'keyFilePath' => $config['keyFilePath']
        ]);

        // Initialize dataset
        $this->dataset = $this->bigQueryClient->dataset($this->datasetName);
    }

    /**
     * {@inheritDoc}
     *
     * Creates a new Query instance for a table.
     *
     * @access public
     * @return BigQueryQuery
     */
    public function query(): BigQueryQuery
    {
        return new BigQueryQuery($this->bigQueryClient, $this->datasetName, $this->projectId, $this->getConnection(), $this);
    }

    /**
     * {@inheritDoc}
     *
     * Returns the schema for this table.
     *
     * @return \Cake\Database\Schema\TableSchemaInterface
     * @throws \RuntimeException If the entity class is not found for the table.
     */
    public function getSchema(): TableSchemaInterface
    {
        $tableName = $this->getTable();

        // Manually construct the schema based on the entity properties
        $schema = new TableSchema($tableName);

        $properties = $this->getEntityDataTypes();

        foreach ($properties as $field => $dataType) {
            $schema->addColumn($field, $dataType);
        }

        return $schema;
    }

    /**
     * {@inheritDoc}
     *
     * Save method
     *
     * @param EntityInterface $entity the entity to be saved
     * @param SaveOptionsBuilder|\ArrayAccess|array $options The options to use when saving.
     * @return EntityInterface|false
     * @throws RolledbackTransactionException If the transaction is aborted in the afterSave event.
     */
    public function save(EntityInterface $entity, $options = [])
    {
        $options = new ArrayObject((array)$options + [
            'atomic' => false, // Disable transactions
            'associated' => true,
            'checkRules' => true,
            'checkExisting' => true,
            '_primary' => true,
            '_cleanOnSuccess' => true,
        ]);

        if ($entity->hasErrors((bool)$options['associated'])) {
            return false;
        }

        if ($entity->isNew() === false && !$entity->isDirty()) {
            return $entity;
        }

        // Directly process the save without using transactions
        $success = $this->_processSave($entity, $options);

        if ($success && $options['_cleanOnSuccess']) {
            $entity->clean();
            $entity->setNew(false);
            $entity->setSource($this->getRegistryAlias());
        }

        return $success;
    }

    public function delete(EntityInterface $entity, $options = []): bool
    {
        $primaryKey = (array)$this->getPrimaryKey();
        $conditions = [];
        foreach ($primaryKey as $key) {
            $conditions[$key] = $entity->get($key);
        }

        $sql = sprintf(
            'DELETE FROM `%s.%s.%s` WHERE %s',
            $this->projectId,
            $this->datasetName,
            $this->getTable(),
            implode(' AND ', array_map(function ($key, $value) {
                return sprintf('%s = %s', $key, is_numeric($value) ? $value : "'$value'");
            }, array_keys($conditions), $conditions))
        );

        $queryJobConfig = $this->bigQueryClient->query($sql);

        $queryResults = $this->bigQueryClient->runQuery($queryJobConfig);

        return $queryResults->isComplete();
    }

    /**
     * {@inheritDoc}
     *
     * Processes the save operation for an entity.
     *
     * @param EntityInterface $entity The entity to be saved.
     * @param ArrayAccess|array $options The options to use when saving.
     * @return EntityInterface|false The saved entity or false on failure.
     * @throws RolledbackTransactionException If the transaction is aborted in the afterSave event.
     */
    protected function _processSave(EntityInterface $entity, ArrayObject $options)
    {
        $isNew = $entity->isNew();
        $this->dispatchEvent('Model.beforeSave', compact('entity', 'options'));

        $data = $entity->toArray();
        $table = $this->getTable();
        $connection = $this->getConnection();

        // Assuming the BigQuery client is available via the connection's driver
        $bigQueryClient = $connection->getDriver()->getConnection();
        $dataset = $bigQueryClient->dataset($this->datasetName);
        $bigQueryTable = $dataset->table($table);

        try {
            if ($isNew) {
                // Insert logic for BigQuery
                $insertResponse = $bigQueryTable->insertRows([['data' => $data]]);
                if (!$insertResponse->isSuccessful()) {
                    throw new \Exception('Insert failed: ' . json_encode($insertResponse->failedRows()));
                }
            } else {
                // Update logic for BigQuery
                $this->updateRows($bigQueryClient, $bigQueryTable, $entity->id, $this->formatData($data));
            }

            $this->dispatchEvent('Model.afterSave', compact('entity', 'options'));
            return $entity;
        } catch (\Exception $e) {
            $this->dispatchEvent('Model.saveError', compact('entity', 'options', 'e'));
            throw new RolledbackTransactionException('Save failed and transaction was rolled back.', null, $e);
        }
    }

    /**
     * @inheritdoc
     *
     * Updates rows in the BigQuery table.
     *
     * @param BigQueryClient $bigQueryClient The BigQuery client.
     * @param \Google\Cloud\BigQuery\Table $bigQueryTable The BigQuery table.
     * @param string $id The ID of the row to update.
     * @param array $data The data to update.
     * @return void
     * @throws Exception If the update operation fails.
     */
    protected function updateRows(BigQueryClient $bigQueryClient, $bigQueryTable, $id, $data)
    {
        $updateFields = [];
        foreach ($data as $column => $value) {
            if ($column !== 'id') {
                $valueUpdate = "'$value'";
                if (is_null($value)) {
                    $valueUpdate = 'NULL';
                } elseif (is_bool($value)) {
                    $valueUpdate = $value ? 'TRUE' : 'FALSE';
                } elseif (is_int($value) || is_float($value)) {
                    $valueUpdate = $value;
                } elseif (empty($value)) {
                    $value = " ";
                }

                $updateFields[] = "`$column` = " . $valueUpdate;
            }
        }
        $updateFieldsString = implode(', ', $updateFields);

        $qualifiedTableName = "`{$this->datasetName}.{$bigQueryTable->id()}`";

        $sql = "UPDATE $qualifiedTableName SET $updateFieldsString WHERE id = '$id'";

        $queryJobConfig = $bigQueryClient->query($sql);
        $job = $bigQueryClient->runQuery($queryJobConfig);

        if (!$job->isComplete()) {
            throw new Exception('Update failed');
        }
    }

    /**
     * Formats the data according to the entity data types.
     *
     * @param array $data The data to be formatted.
     * @return array The formatted data.
     * @throws RuntimeException If the entity class is not found for the table.
     */
    protected function formatData(array $data): array
    {
        $dataTypes = $this->getEntityDataTypes();

        foreach ($data as $key => $value) {
            if (!isset($dataTypes[$key])) {
                continue;
            }

            switch ($dataTypes[$key]['type']) {
                case 'integer':
                    $data[$key] = is_null($value) ? null : (int)$value;
                    break;
                case 'float':
                    $data[$key] = is_null($value) ? null : (float)$value;
                    break;
                case 'boolean':
                    $data[$key] = is_null($value) ? null : (bool)$value;
                    break;
                case 'datetime':
                    if ($value instanceof \Google\Cloud\BigQuery\Timestamp) {
                        $data[$key] = $value->formatAsString();
                    } elseif ($value instanceof \DateTime) {
                        $data[$key] = $value->format('Y-m-d\TH:i:s');
                    } else {
                        $data[$key] = date('Y-m-d\TH:i:s', strtotime((string)$value));
                    }
                    break;
                case 'text':
                case 'string':
                default:
                    $data[$key] = is_null($value) ? null : (string)$value;
                    break;
            }
        }

        return $data;
    }

    /**
     * Retrieves the data types of the entity properties.
     *
     * @return array An associative array where the keys are the property names and the values are the data types.
     * @throws RuntimeException If the entity class is not found for the table.
     */
    public function getEntityDataTypes(): array
    {
        $entityClass = $this->getEntityClass();

        $tableName = $this->getTable();

        if (!$entityClass) {
            throw new RuntimeException("Entity class not found for table $tableName");
        }

        $reflection = new ReflectionClass($entityClass);

        $properties = $reflection->getDefaultProperties()['_dataTypes'];

        return $properties;
    }

    /**
     * Returns the project ID associated with the BigQuery client.
     *
     * @return string The project ID.
     */
    public function getProjectId(): string
    {
        return $this->projectId;
    }

    /**
     * Returns the name of the dataset associated with the BigQuery client.
     *
     * @return string The name of the dataset.
     */
    public function getDatasetName(): string
    {
        return $this->datasetName;
    }

    /**
     * Returns the BigQuery client associated with the table.
     *
     * @return \Google\Cloud\BigQuery\BigQueryClient The BigQuery client.
     */
    public function getBigQueryClient(): BigQueryClient
    {
        return $this->bigQueryClient;
    }

    /**
     * Returns the dataset object associated with the BigQuery client.
     *
     * @return \Google\Cloud\BigQuery\Dataset The dataset object.
     */
    public function getDataset()
    {
        return $this->dataset;
    }

    /**
     * Maps CakePHP data types to BigQuery data types.
     *
     * @param string $cakeType The CakePHP data type to map.
     * @return string|null The corresponding BigQuery data type, or null if no mapping exists.
     */
    public function mapCakeTypeToBigQueryType(string $cakeType): ?string
    {
        // BigQuery does not have a TEXT type, use STRING instead
        if ($cakeType == 'text') {
            return 'STRING';
        }

        return strtoupper($cakeType);
    }
}
