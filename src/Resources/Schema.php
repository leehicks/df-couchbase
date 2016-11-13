<?php

namespace DreamFactory\Core\Couchbase\Resources;

use DreamFactory\Core\Couchbase\Components\CouchbaseConnection;
use DreamFactory\Core\Couchbase\Services\Couchbase;
use DreamFactory\Core\Resources\BaseNoSqlDbSchemaResource;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\BadRequestException;

class Schema extends BaseNoSqlDbSchemaResource
{
    /**
     * @var null|Couchbase
     */
    protected $parent = null;

    /**
     * @return null|Couchbase
     */
    public function getService()
    {
        return $this->parent;
    }

    public function describeTable($table, $refresh = false)
    {
        $name = (is_array($table)) ? array_get($table, 'name') : $table;

        try {
            $out = $this->parent->getConnection()->getBucketInfo($name);
            $out['name'] = $name;
            $out['access'] = $this->getPermissions($name);

            return $out;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException(
                "Failed to get table properties for table '$name'.\n{$ex->getMessage()}"
            );
        }
    }

    public function createTable($table, $properties = [], $check_exist = false, $return_schema = false)
    {
        if (empty($table)) {
            throw new BadRequestException("No 'name' field in data.");
        }

        try {
            $data = ['name' => $table];

            foreach (CouchbaseConnection::$editableBucketProperties as $prop){
                if(isset($properties[$prop])){
                    $data[$prop] = $properties[$prop];
                }
            }

            $this->parent->getConnection()->createBucket($table, $data);
            $this->refreshCachedTables();
            return ['name' => $table];
        } catch (\Exception $ex) {
            if($ex->getCode() >= 400 && $ex->getCode() < 500){
                throw $ex;
            }
            throw new InternalServerErrorException("Failed to create table '$table'.\n{$ex->getMessage()}");
        }
    }

    public function updateTable($table, $properties, $allow_delete_fields = false, $return_schema = false)
    {
        if (empty($table)) {
            throw new BadRequestException("No 'name' field in data.");
        }

        try {
            $data = ['name' => $table];

            foreach (CouchbaseConnection::$editableBucketProperties as $prop){
                if(isset($properties[$prop])){
                    $data[$prop] = $properties[$prop];
                }
            }

            $this->parent->getConnection()->updateBucket($table, $data);
            $this->refreshCachedTables();
            return ['name' => $table];
        } catch (\Exception $ex) {
            if($ex->getCode() >= 400 && $ex->getCode() < 500){
                throw $ex;
            }
            throw new InternalServerErrorException("Failed to update table '$table'.\n{$ex->getMessage()}");
        }
    }

    public function deleteTable($table, $check_empty = false)
    {
        $name = (is_array($table)) ? array_get($table, 'name') : $table;
        if (empty($name)) {
            throw new BadRequestException('Table name can not be empty.');
        }

        try {
            $this->parent->getConnection()->deleteBucket($name);
            $this->refreshCachedTables();

            return ['name' => $name];
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to delete table '$name'.\n{$ex->getMessage()}");
        }
    }
}