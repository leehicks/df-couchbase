<?php

namespace DreamFactory\Core\Couchbase\Models;

use DreamFactory\Core\Models\BaseServiceConfigModel;

class CouchbaseConfig extends BaseServiceConfigModel
{
    /** @var string */
    protected $table = 'couchbase_config';

    /** @var array */
    protected $fillable = ['service_id', 'host', 'username', 'password'];

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
                $schema['description'] = 'Couchbase Admin User';
                break;
            case 'password':
                $schema['label'] = 'Password';
                $schema['description'] = 'Couchbase Admin Password';
                break;
        }
    }
}