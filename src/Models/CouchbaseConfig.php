<?php

namespace DreamFactory\Core\Couchbase\Models;

use DreamFactory\Core\Database\Components\SupportsUpsertAndCache;
use DreamFactory\Core\Models\BaseServiceConfigModel;

class CouchbaseConfig extends BaseServiceConfigModel
{
    use SupportsUpsertAndCache;

    /** @var string */
    protected $table = 'couchbase_config';

    /** @var array */
    protected $fillable = ['service_id', 'host', 'port', 'username', 'password'];

    protected $casts = [
        'service_id' => 'integer',
        'port'       => 'integer',
    ];

    /** @var array */
    protected $encrypted = ['password'];

    /** @var array */
    protected $protected = ['password'];

    /** {@inheritdoc} */
    protected static function prepareConfigSchemaField(array &$schema)
    {
        parent::prepareConfigSchemaField($schema);

        switch ($schema['name']) {
            case 'host':
                $schema['label'] = 'Host';
                $schema['default'] = '127.0.0.1';
                $schema['description'] = 'IP Address/Hostname of your Couchbase Cluster.';
                break;
            case 'port':
                $schema['label'] = 'Port';
                $schema['default'] = 8091;
                $schema['description'] = 'Couchbase Port';
                break;
            case 'username':
                $schema['label'] = 'Username';
                $schema['description'] = 'Couchbase User';
                break;
            case 'password':
                $schema['label'] = 'Password';
                $schema['description'] = 'Couchbase Password';
                break;
        }
    }
}