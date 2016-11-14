<?php

namespace DreamFactory\Core\Couchbase\Components;

use CouchbaseCluster;
use DreamFactory\Core\Couchbase\Resources\Table;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Library\Utility\ArrayUtils;

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
     * @param string  $host
     * @param integer $port
     *
     * @return string
     */
    protected static function buildDsn($host, $port)
    {
        $host = trim($host);
        $dsn = static::DSN_PREFIX . $host . ':' . $port;

        return $dsn;
    }

    /*********************************
     * Bucket operations
     *********************************/

    /**
     * @param bool $details
     *
     * @return array|mixed
     */
    public function listBuckets($details = false)
    {
        $buckets = $this->cbClusterManager->listBuckets();

        if (true === $details) {
            return $buckets;
        } else {
            $list = [];
            foreach ($buckets as $bucket) {
                $list[] = array_get($bucket, 'name');
            }

            return $list;
        }
    }

    /**
     * @param string $name
     *
     * @return null
     */
    public function getBucketInfo($name)
    {
        $buckets = $this->listBuckets(true);
        $bucket = ArrayUtils::findByKeyValue($buckets, 'name', $name);
        if (!empty(array_get($bucket, 'saslPassword'))) {
            array_set($bucket, 'saslPassword', '********');
        }
        if (null !== array_get($bucket, 'vBucketServerMap.vBucketMap')) {
            array_set($bucket, 'vBucketServerMap.vBucketMap', '--HIDDEN-BY-DF--');
        }

        return $bucket;
    }

    /**
     * @param string $name
     * @param array  $options
     *
     * @return mixed
     */
    public function createBucket($name, array $options = [])
    {
        $result = $this->cbClusterManager->createBucket($name, $options);

        return $result;
    }

    /**
     * @param string $name
     * @param array  $options
     *
     * @return bool
     * @throws \DreamFactory\Core\Exceptions\InternalServerErrorException
     */
    public function updateBucket($name, array $options = [])
    {
        $url = 'http://' . $this->host . '/pools/default/buckets/' . $name;
        $options = [
            CURLOPT_URL            => $url,
            CURLOPT_PORT           => $this->port,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_VERBOSE        => false,
            CURLOPT_POST           => true,
            CURLOPT_POSTFIELDS     => $options,
            CURLOPT_HTTPAUTH       => CURLAUTH_BASIC,
            CURLOPT_USERPWD        => $this->username . ":" . $this->password
        ];

        $ch = curl_init();
        curl_setopt_array($ch, $options);
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
     *
     * @return mixed
     */
    public function deleteBucket($name)
    {
        $result = $this->cbClusterManager->removeBucket($name);

        return $result;
    }

    /**
     * @param string $bucket
     * @param string $sql
     * @param array  $params
     *
     * @return array
     */
    public function query($bucket, $sql, $params = [])
    {
        $query = \CouchbaseN1qlQuery::fromString($sql);
        if (!empty($params)) {
            $query->namedParams($params);
        }
        $bucket = $this->cbCluster->openBucket($bucket);
        $result = $bucket->query($query);

        return (array)$result;
    }

    /*********************************
     * Document operations
     *********************************/

    /**
     * @param string $bucketName
     * @param mixed  $id
     *
     * @return array
     */
    public function getDocument($bucketName, $id)
    {
        $bucket = $this->cbCluster->openBucket($bucketName);
        $result = $bucket->get($id);
        $result = (array)$result->value;
        unset($result[Table::ID_FIELD]);
        $result = array_merge([Table::ID_FIELD => $id], $result);

        return $result;
    }

    /**
     * @param string $bucketName
     * @param mixed  $id
     * @param array  $record
     *
     * @return array
     */
    public function createDocument($bucketName, $id, $record)
    {
        $bucket = $this->cbCluster->openBucket($bucketName);
        $bucket->insert($id, $record);

        return [Table::ID_FIELD => $id];
    }

    /**
     * @param string $bucketName
     * @param mixed  $id
     * @param array  $record
     *
     * @return array
     */
    public function updateDocument($bucketName, $id, $record)
    {
        $bucket = $this->cbCluster->openBucket($bucketName);
        $bucket->upsert($id, $record);

        return [Table::ID_FIELD => $id];
    }

    /**
     * @param string $bucketName
     * @param mixed  $id
     * @param array  $record
     *
     * @return array
     */
    public function replaceDocument($bucketName, $id, $record)
    {
        $bucket = $this->cbCluster->openBucket($bucketName);
        $bucket->replace($id, $record);

        return [Table::ID_FIELD => $id];
    }

    /**
     * @param string $bucketName
     * @param mixed  $id
     *
     * @return array
     */
    public function deleteDocument($bucketName, $id)
    {
        $bucket = $this->cbCluster->openBucket($bucketName);
        $bucket->remove($id);

        return [Table::ID_FIELD => $id];
    }
}