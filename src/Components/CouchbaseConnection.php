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
        $dsn = static::DSN_PREFIX . $host . ':' . $port . '?detailed_errcodes=true';

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
        $bucketInfo = $this->getBucketInfo($name);
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
     *
     * @return mixed
     */
    public function deleteBucket($name)
    {
        $result = $this->cbClusterManager->removeBucket($name);

        return $result;
    }

    /**
     * @param string $bucketName
     * @param string $sql
     * @param array  $params
     *
     * @return array
     */
    public function query($bucketName, $sql, $params = [])
    {
        try {
            $query = \CouchbaseN1qlQuery::fromString($sql);
            if (!empty($params)) {
                $query->namedParams($params);
            }
            $bucket = $this->cbCluster->openBucket($bucketName);
            $result = $bucket->query($query);

            return (array)$result;
        } catch (\CouchbaseException $ce) {
            // Bucket with no primary index (possibly)
            // Create index and retry query.
            if ((59 === $ce->getCode() && strpos($ce->getMessage(), 'LCB_HTTP_ERROR') !== false) ||
                (4000 === $ce->getCode() && strpos($ce->getMessage(), 'No primary index on keyspace') !== false)) {
                $this->createPrimaryIndex($bucketName);

                return $this->query($bucketName, $sql, $params);
            }
        }
    }

    public function createPrimaryIndex($bucketName)
    {
        $bucket = $this->cbCluster->openBucket($bucketName);
        $manager = $bucket->manager();
        $manager->createN1qlPrimaryIndex('', true);

        return true;
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
        unset($record[Table::ID_FIELD]);
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
        unset($record[Table::ID_FIELD]);
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
        unset($record[Table::ID_FIELD]);
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