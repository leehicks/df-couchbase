<?php

namespace DreamFactory\Core\Couchbase\Services;

use DreamFactory\Core\Components\DbSchemaExtras;
use DreamFactory\Core\Couchbase\Components\CouchbaseConnection;
use DreamFactory\Core\Couchbase\Resources\Schema;
use DreamFactory\Core\Couchbase\Resources\Table;
use DreamFactory\Core\Services\BaseNoSqlDbService;
use DreamFactory\Core\Utility\Session;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Database\Schema\TableSchema;

class Couchbase extends BaseNoSqlDbService
{
    use DbSchemaExtras;

    /** @var null | CouchbaseConnection */
    protected $connection = null;

    /** @var array  */
    protected $tables = [];

    /**
     * @var array
     */
    protected static $resources = [
        Schema::RESOURCE_NAME => [
            'name'       => Schema::RESOURCE_NAME,
            'class_name' => Schema::class,
            'label'      => 'Schema',
        ],
        Table::RESOURCE_NAME  => [
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

        $this->connection = new CouchbaseConnection($host, $port, $username, $password);
    }

    /**
     * Destroys the connection
     */
    public function __destruct()
    {
        $this->connection = null;
    }

    public function getConnection()
    {
        if (empty($this->connection)) {
            throw new InternalServerErrorException('Database connection has not been initialized.');
        }

        return $this->connection;
    }

    /** {@inheritdoc} */
    public function getTableNames($schema = null, $refresh = false, $use_alias = false)
    {
        if($refresh || (empty($this->tables) && null === $this->tables = $this->getFromCache('table_names'))){
            $tables = [];
            $buckets = $this->connection->listBuckets();
            foreach ($buckets as $table){
                $tables[strtolower($table)] = new TableSchema(['name' => $table]);
            }
            // merge db extras
            if (!empty($extrasEntries = $this->getSchemaExtrasForTables($buckets))) {
                foreach ($extrasEntries as $extras) {
                    if (!empty($extraName = strtolower(strval($extras['table'])))) {
                        if (array_key_exists($extraName, $tables)) {
                            $tables[$extraName]->fill($extras);
                        }
                    }
                }
            }
            $this->tables = $tables;
            $this->addToCache('table_names', $this->tables, true);
        }

        return $this->tables;
    }

    /** {@inheritdoc} */
    public function refreshTableCache()
    {
        $this->removeFromCache('table_names');
        $this->tables = [];
    }
}