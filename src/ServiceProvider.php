<?php

namespace DreamFactory\Core\Couchbase;

use DreamFactory\Core\Couchbase\Models\CouchbaseConfig;
use DreamFactory\Core\Couchbase\Services\Couchbase;
use DreamFactory\Core\Enums\ServiceTypeGroups;
use DreamFactory\Core\Services\ServiceManager;
use DreamFactory\Core\Services\ServiceType;

class ServiceProvider extends \Illuminate\Support\ServiceProvider
{
    public function register()
    {
        // Add Couchbase service type
        $this->app->resolving('df.service', function (ServiceManager $df){
            $df->addType(
                new ServiceType([
                    'name'            => 'couchbase',
                    'label'           => 'Couchbase',
                    'description'     => 'Database service for Couchbase connections.',
                    'group'           => ServiceTypeGroups::DATABASE,
                    'config_handler'  => CouchbaseConfig::class,
                    'factory'         => function ($config){
                        return new Couchbase($config);
                    },
                ])
            );
        });
    }

    public function boot()
    {
        // add migrations
        $this->loadMigrationsFrom(__DIR__ . '/../database/migrations');
    }
}