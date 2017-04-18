<?php

namespace DreamFactory\Core\Couchbase\Database\Schema;

use DreamFactory\Core\Couchbase\Components\CouchbaseConnection;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Exceptions\InternalServerErrorException;

class Schema extends \DreamFactory\Core\Database\Components\Schema
{
    /** @var CouchbaseConnection */
    protected $connection;

    /**
     * @inheritdoc
     */
    protected function findColumns(TableSchema $table)
    {
        return [
            [
                'name'           => '_id',
                'db_type'        => 'string',
                'is_primary_key' => true,
                'auto_increment' => false,
            ]
        ];
    }

    /**
     * @inheritdoc
     */
    protected function findTableNames($schema = '')
    {
        $tables = [];
        $buckets = $this->connection->getCbClusterManager()->listBuckets();
        foreach ($buckets as $bucket) {
            $name = array_get($bucket, 'name');
            $tables[strtolower($name)] = new TableSchema(['name' => $name, 'native' => $bucket]);
        }

        return $tables;
    }

    /**
     * @inheritdoc
     */
    public function createTable($table, $options)
    {
        if (empty($tableName = array_get($table, 'name'))) {
            throw new \Exception("No valid name exist in the received table schema.");
        }
        $data = ['name' => $tableName];
        foreach (CouchbaseConnection::$editableBucketProperties as $prop) {
            if (null !== $option = array_get($table, $prop, array_get($table, 'native.' . $prop))) {
                $data[$prop] = $option;
            }
        }
        $result = $this->connection->getCbClusterManager()->createBucket($tableName, $data);
        if (isset($result['errors'])) {
            throw new InternalServerErrorException(null, null, null, $result);
        }

        return $result;
    }

    /**
     * @inheritdoc
     */
    protected function updateTable($tableSchema, $changes)
    {
        $data = ['name' => $tableSchema->quotedName];
        foreach (CouchbaseConnection::$editableBucketProperties as $prop) {
            if (null !== $option = array_get($changes, $prop, array_get($changes, 'native.' . $prop))) {
                $data[$prop] = $option;
            }
        }
        $this->connection->updateBucket($tableSchema->quotedName, $data);
    }

    /**
     * @inheritdoc
     */
    public function dropTable($table)
    {
        return $this->connection->getCbClusterManager()->removeBucket($table);
    }

    /**
     * @inheritdoc
     */
    protected function createFieldReferences($references)
    {
        // Do nothing here for now
    }

    /**
     * @inheritdoc
     */
    protected function createFieldIndexes($indexes)
    {
        // Do nothing here for now
    }
}