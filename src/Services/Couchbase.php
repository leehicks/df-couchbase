<?php

namespace DreamFactory\Core\Couchbase\Services;

use DreamFactory\Core\Couchbase\Components\CouchbaseConnection;
use DreamFactory\Core\Couchbase\Resources\Schema;
use DreamFactory\Core\Couchbase\Resources\Table;
use DreamFactory\Core\Database\Services\BaseDbService;
use DreamFactory\Core\Utility\Session;

class Couchbase extends BaseDbService
{
    /**
     * @var array
     */
    protected static $resources = [
        Schema::RESOURCE_NAME => [
            'name'       => Schema::RESOURCE_NAME,
            'class_name' => Schema::class,
            'label'      => 'Schema',
        ],
        Table::RESOURCE_NAME            => [
            'name'       => Table::RESOURCE_NAME,
            'class_name' => Table::class,
            'label'      => 'Table',
        ],
    ];

    protected function initializeConnection()
    {
        $host = array_get($this->config, 'host', '127.0.0.1');
        $port = array_get($this->config, 'port', 8091);
        $username = array_get($this->config, 'username');
        $password = array_get($this->config, 'password');

        $this->dbConn = new CouchbaseConnection($host, $port, $username, $password);
        /** @noinspection PhpParamsInspection */
        $this->schema = new \DreamFactory\Core\Couchbase\Database\Schema\Schema($this->dbConn);
        $this->schema->setCache($this);
        $this->schema->setExtraStore($this);
    }
}