<?php
namespace DreamFactory\Core\Couchbase\Resources;

use DreamFactory\Core\Database\Resources\DbSchemaResource;

class Schema extends DbSchemaResource
{
    /**
     * {@inheritdoc}
     */
    public function describeTable($name, $refresh = false)
    {
        $table = parent::describeTable($name, $refresh);
        if (isset($table['native']['saslPassword'])) {
            $table['native']['saslPassword'] = '********';
        }
        if (isset($table['native']['vBucketServerMap']['vBucketMap'])) {
            $table['native']['vBucketServerMap']['vBucketMap'] = '--HIDDEN-BY-DF--';
        }

        return $table;
    }
}