<?php
declare(strict_types=1);

namespace App\Database\BigQuery\Schema;

use Cake\Database\Schema\TableSchemaInterface;
use Cake\Database\Schema\TableSchema;
use Cake\Database\Schema\SchemaDialect;

class BigQuerySchemaDialect extends SchemaDialect
{
    public function listTablesSql($config): array
    {
        return []; // Not applicable for BigQuery
    }

    public function describeColumnSql($tableName, $config): array
    {
        return []; // Not applicable for BigQuery
    }

    public function describeIndexSql($tableName, $config): array
    {
        return []; // Not applicable for BigQuery
    }

    public function describeForeignKeySql($tableName, $config): array
    {
        return []; // Not applicable for BigQuery
    }

    public function convertColumnDescription(TableSchemaInterface $schema, array $row): void
    {
        $schema->addColumn($row['name'], [
            'type' => $this->_convertColumnType($row['type']),
            'null' => true
        ]);
    }

    public function convertIndexDescription(TableSchemaInterface $schema, array $row): void
    {
        // Implement conversion logic if needed
    }

    public function convertForeignKeyDescription(TableSchemaInterface $schema, array $row): void
    {
        // Implement conversion logic if needed
    }

    protected function _convertColumnType($bigQueryType)
    {
        switch ($bigQueryType) {
            case 'STRING':
                return 'string';
            case 'INT64':
                return 'integer';
            case 'FLOAT64':
                return 'float';
            case 'BOOL':
                return 'boolean';
            case 'DATE':
                return 'date';
            case 'DATETIME':
                return 'datetime';
            case 'TIMESTAMP':
                return 'timestamp';
            default:
                return 'text';
        }
    }

    public function createTableSql(
        TableSchema $schema,
        array $columns,
        array $constraints,
        array $indexes
    ): array {
        $tableName = $schema->name();
        $sql = "CREATE TABLE `{$tableName}` (";

        $cols = [];
        foreach ($columns as $column) {
            $cols[] = $column;
        }
        $sql .= implode(', ', $cols);

        if (!empty($constraints)) {
            foreach ($constraints as $constraint) {
                $sql .= ', ' . $constraint;
            }
        }

        $sql .= ')';

        return [$sql];
    }

    public function columnSql(TableSchemaInterface $schema, string $name): string
    {
        return '';
    }

    public function addConstraintSql(TableSchemaInterface $schema): array
    {
        return [];
    }

    public function dropConstraintSql(TableSchemaInterface $schema): array
    {
        return [];
    }

    public function constraintSql(TableSchemaInterface $schema, string $name): string
    {
        return '';
    }

    public function indexSql(TableSchemaInterface $schema, string $name): string
    {
        return '';
    }

    public function truncateTableSql(TableSchemaInterface $schema): array
    {
        return [];
    }
}
