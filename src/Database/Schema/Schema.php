<?php

namespace DreamFactory\Core\Couchbase\Database\Schema;

use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Couchbase\Components\CouchbaseConnection;
use DreamFactory\Core\Exceptions\InternalServerErrorException;

class Schema extends \DreamFactory\Core\Database\Schema\Schema
{
    /** @var CouchbaseConnection */
    protected $connection;

    /**
     * @inheritdoc
     */
    protected function findColumns(TableSchema $table)
    {
        $table->native = $this->connection->getBucketInfo($table->name);
        $columns = [
            [
                'name'           => '_id',
                'db_type'        => 'string',
                'is_primary_key' => true,
                'auto_increment' => false,
            ]
        ];

        return $columns;
    }

    /**
     * @inheritdoc
     */
    protected function findTableNames($schema = '', $include_views = true)
    {
        $tables = [];
        $buckets = $this->connection->listBuckets();
        foreach ($buckets as $table) {
            $tables[strtolower($table)] = new TableSchema(['name' => $table]);
        }

        return $tables;
    }

    /**
     * @inheritdoc
     */
    public function createTable($table, $schema, $options = null)
    {
        $data = ['name' => $table];

        foreach (CouchbaseConnection::$editableBucketProperties as $prop) {
            if (isset($schema[$prop])) {
                $data[$prop] = $schema[$prop];
            }
        }
        $result = $this->connection->createBucket($table, $data);
        if (isset($result['errors'])) {
            throw new InternalServerErrorException(null, null, null, $result);
        }

        return $result;
    }

    /**
     * @inheritdoc
     */
    protected function updateTable($table, $schema)
    {
        $data = ['name' => $table];
        foreach (CouchbaseConnection::$editableBucketProperties as $prop) {
            if (isset($schema[$prop])) {
                $data[$prop] = $schema[$prop];
            }
        }
        $this->connection->updateBucket($table, $data);
    }

    /**
     * @inheritdoc
     */
    public function dropTable($table)
    {
        return $this->connection->deleteBucket($table);
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