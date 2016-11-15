<?php

namespace DreamFactory\Core\Couchbase\Services;

use DreamFactory\Core\Couchbase\Components\CouchbaseConnection;
use DreamFactory\Core\Couchbase\Database\Schema\Schema;
use DreamFactory\Core\Couchbase\Resources\Table;
use DreamFactory\Core\Resources\DbSchemaResource;
use DreamFactory\Core\Services\BaseDbService;
use DreamFactory\Core\Utility\Session;

class Couchbase extends BaseDbService
{
    /**
     * @var array
     */
    protected static $resources = [
        DbSchemaResource::RESOURCE_NAME => [
            'name'       => DbSchemaResource::RESOURCE_NAME,
            'class_name' => DbSchemaResource::class,
            'label'      => 'Schema',
        ],
        Table::RESOURCE_NAME            => [
            'name'       => Table::RESOURCE_NAME,
            'class_name' => Table::class,
            'label'      => 'Table',
        ],
    ];

    /** {@inheritdoc} */
    public function __construct(array $settings)
    {
        parent::__construct($settings);

        $config = (array)array_get($settings, 'config');
        Session::replaceLookups($config, true);

        $host = array_get($config, 'host', '127.0.0.1');
        $port = array_get($config, 'port', 8091);
        $username = array_get($config, 'username');
        $password = array_get($config, 'password');

        $this->dbConn = new CouchbaseConnection($host, $port, $username, $password);
        /** @noinspection PhpParamsInspection */
        $this->schema = new Schema($this->dbConn);
        $this->schema->setCache($this);
        $this->schema->setExtraStore($this);
    }
}