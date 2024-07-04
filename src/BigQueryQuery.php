<?php

declare(strict_types=1);

namespace Chinh\BigQuery;

use Cake\ORM\Query as CakeQuery;
use Cake\Database\StatementInterface;
use Cake\Database\ValueBinder;
use Google\Cloud\BigQuery\BigQueryClient;

class BigQueryQuery extends CakeQuery
{
    protected $bigQueryClient;
    protected $datasetName;
    protected $projectId;
    protected $sqlKeywords = ['key', 'ignore', 'lock', 'index', 'unique', 'primary'];

    public function __construct(BigQueryClient $bigQueryClient, string $datasetName, string $projectId, $connection, $repository)
    {
        $this->bigQueryClient = $bigQueryClient;
        $this->datasetName = $datasetName;
        $this->projectId = $projectId;
        parent::__construct($connection, $repository);
    }

    /**
     * {@inheritdoc}
     * Executes the query and returns a StatementInterface object.
     *
     * @return StatementInterface The result set as a StatementInterface object.
     */
    public function execute(): StatementInterface
    {
        $binder = new ValueBinder();
        $sql = $this->sql($binder);

        // Automatically add backticks to SQL keywords
        $sql = $this->addBackticksToKeywords($sql);

        // Map the bindings for easy lookup
        $bindings = [];
        foreach ($binder->bindings() as $binding) {
            $bindings[':' . $binding['placeholder']] = [
                'value' => $binding['value'],
                'type' => $binding['type']
            ];
        }

        // Use preg_replace_callback to replace placeholders with actual values
        $sql = preg_replace_callback('/:([a-zA-Z0-9_]+)/', function ($matches) use ($bindings) {
            $placeholder = $matches[0];
            if (!isset($bindings[$placeholder])) {
                return $placeholder;
            }
            $binding = $bindings[$placeholder];
            $value = $binding['value'];
            $type = $binding['type'];

            // Handle different types appropriately
            switch ($type) {
                case 'string':
                    return "'" . addslashes((string) $value) . "'";
                case 'boolean':
                    return $value ? 'TRUE' : 'FALSE';
                case 'integer':
                case 'float':
                    return $value;
                default:
                    return "'" . addslashes((string) $value) . "'";
            }

            // Handle other types if necessary
            return $placeholder;
        }, $sql);

        $queryJob = $this->bigQueryClient->query($sql);
        $results = $this->bigQueryClient->runQuery($queryJob);

        // Wrap results in a CakePHP compatible statement object
        return new BigQueryStatement($results);
    }

    /**
     * {@inheritdoc}
     * Generates the SQL representation of the query.
     *
     * @param ValueBinder|null $binder The value binder to use for parameter binding.
     * @return string The SQL representation of the query.
     */
    public function sql(ValueBinder $binder = null): string
    {
        $sql = parent::sql($binder);

        // Replace table name with fully qualified table name
        $tableName = $this->getRepository()->getTable();
        $projectId = $this->projectId;
        $qualifiedTableName = "`$projectId.$this->datasetName.$tableName`";

        // Replace the table name in the SQL string
        $sql = preg_replace("/FROM\s+`?{$tableName}`?/i", "FROM {$qualifiedTableName}", $sql);
        $sql = preg_replace("/JOIN\s+`?{$tableName}`?/i", "JOIN {$qualifiedTableName}", $sql);

        return $sql;
    }

    /**
     * Adds backticks around SQL keywords in the given SQL string.
     *
     * @param string $sql The SQL string to process.
     * @return string The SQL string with backticks around SQL keywords.
     */
    private function addBackticksToKeywords(string $sql): string
    {
        $pattern = '/\b(' . implode('|', $this->sqlKeywords) . ')\b/i';
        return preg_replace_callback($pattern, function ($matches) {
            return '`' . $matches[1] . '`';
        }, $sql);
    }
}
