<?php

namespace DreamFactory\Core\Couchbase\Components;

use CouchbaseCluster;
use DreamFactory\Core\Exceptions\InternalServerErrorException;

class CouchbaseConnection
{
    /** DSN prefix */
    const DSN_PREFIX = 'couchbase://';

    /** @var array */
    public static $editableBucketProperties = [
        'bucketType',
        'authType',
        'saslPassword',
        'proxyPort',
        'replicaIndex',
        'autoCompactionDefined',
        'replicaNumber',
        'evictionPolicy',
        'flushEnabled',
        'ramQuotaMB'
    ];

    /** @var null|string */
    private $host = null;

    /** @var int|integer */
    private $port = 8091;

    /** @var null|string */
    private $username = null;

    /** @var null|string */
    private $password = null;

    /** @var \CouchbaseCluster|null */
    protected $cbCluster = null;

    /** @var \CouchbaseClusterManager|null */
    protected $cbClusterManager = null;

    /**
     * CouchbaseConnection constructor.
     *
     * @param string $host
     * @param int    $port
     * @param string $username
     * @param string $password
     */
    public function __construct($host, $port = 8091, $username = '', $password = '')
    {
        $this->host = $host;
        $this->port = $port;
        $this->username = $username;
        $this->password = $password;
        $dsn = static::buildDsn($host, $port);
        $this->cbCluster = new CouchbaseCluster($dsn);
        $this->cbClusterManager = $this->cbCluster->manager($username, $password);
    }

    /**
     * @return CouchbaseCluster|null
     */
    public function getCbCluster()
    {
        return $this->cbCluster;
    }

    /**
     * @return \CouchbaseClusterManager|null
     */
    public function getCbClusterManager()
    {
        return $this->cbClusterManager;
    }

    /**
     * @param string  $host
     * @param integer $port
     *
     * @return string
     */
    protected static function buildDsn($host, $port)
    {
        $host = trim($host);
        $dsn = static::DSN_PREFIX . $host . ':' . $port . '?detailed_errcodes=true';

        return $dsn;
    }

    /*********************************
     * Bucket operations
     *********************************/

    /**
     * @param string $name
     * @param array  $options
     *
     * @return bool
     * @throws \DreamFactory\Core\Exceptions\InternalServerErrorException
     */
    public function updateBucket($name, array $options = [])
    {
        $buckets = $this->cbClusterManager->listBuckets();
        $bucketInfo = array_by_key_value($buckets, 'name', $name);
        $url = 'http://' . $this->host . array_get($bucketInfo, 'uri');
        $curlOptions = [
            CURLOPT_URL            => $url,
            CURLOPT_PORT           => $this->port,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_VERBOSE        => false,
            CURLOPT_POST           => true,
            CURLOPT_POSTFIELDS     => json_encode($options),
            CURLOPT_HTTPAUTH       => CURLAUTH_BASIC,
            CURLOPT_USERPWD        => $this->username . ":" . $this->password
        ];

        $ch = curl_init();
        curl_setopt_array($ch, $curlOptions);
        curl_exec($ch);
        $rs = curl_getinfo($ch);

        if (200 !== $code = array_get($rs, 'http_code')) {
            throw new InternalServerErrorException('Bucket update failed. Request received response code [' .
                $code .
                ']');
        }

        return true;
    }

    /**
     * @param string $name
     * @param string $password
     *
     * @return \CouchbaseBucket
     */
    public function openBucket($name, $password = '')
    {
        return $this->cbCluster->openBucket($name, $password);
    }
}