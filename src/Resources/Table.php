<?php

namespace DreamFactory\Core\Couchbase\Resources;

use DreamFactory\Core\Couchbase\Services\Couchbase;
use DreamFactory\Core\Resources\BaseNoSqlDbTableResource;
use DreamFactory\Core\Enums\ApiOptions;
use DreamFactory\Library\Utility\Scalar;
use DreamFactory\Core\Enums\DbLogicalOperators;
use DreamFactory\Core\Enums\DbComparisonOperators;
use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\ForbiddenException;
use DreamFactory\Library\Utility\Enums\Verbs;
use DreamFactory\Core\Utility\ResourcesWrapper;
use DreamFactory\Core\Exceptions\RestException;
use DreamFactory\Core\Exceptions\DfException;
use DreamFactory\Core\Exceptions\NotFoundException;
use Config;

class Table extends BaseNoSqlDbTableResource
{
    /** ID Field */
    const ID_FIELD = '_id';

    /**
     * @var null|Couchbase
     */
    protected $parent = null;

    /**
     * @var int An internal counter
     */
    private $i = 1;

    /** {@inheritdoc} */
    protected function getIdsInfo(
        $table,
        $fields_info = null,
        &$requested_fields = null,
        $requested_types = null
    ){
        $requested_fields = [static::ID_FIELD]; // can only be this
        $ids = [
            new ColumnSchema(['name' => static::ID_FIELD, 'type' => 'string', 'required' => true]),
        ];

        return $ids;
    }

    /** {@inheritdoc} */
    protected function addToTransaction(
        $record = null,
        $id = null,
        $extras = null,
        $rollback = false,
        $continue = false,
        $single = false
    ){
        $fields = array_get($extras, ApiOptions::FIELDS);
        $requireMore = array_get($extras, 'require_more');
        $updates = array_get($extras, 'updates');

        $out = [];
        try {
            switch ($this->getAction()) {
                case Verbs::POST:
                    if (empty($record)) {
                        throw new BadRequestException('No valid fields were found in record.');
                    }

                    if ($rollback) {
                        return parent::addToTransaction($record, $id);
                    }

                    $result = $this->parent->getConnection()->createDocument($this->transactionTable, $id, $record);

                    if ($requireMore) {
                        // for returning latest _rev
                        $result = array_merge($record, $result);
                    }

                    $out = static::cleanRecord($result, $fields, static::ID_FIELD);
                    break;

                case Verbs::PUT:
                    if (!empty($updates)) {
                        $record = $updates;
                        $record[static::ID_FIELD] = $id;
                    }

                    if (empty($record)) {
                        throw new BadRequestException('No valid fields were found in record.');
                    }

                    $old = null;
                    if ($rollback) {
                        $old = $this->parent->getConnection()->getDocument($this->transactionTable, $id);
                        $this->addToRollback($old);

                        return parent::addToTransaction($record, $id);
                    }

                    $result = $this->parent->getConnection()->replaceDocument($this->transactionTable, $id, $record);

                    if ($requireMore) {
                        $result = array_merge($record, $result);
                    }

                    $out = static::cleanRecord($result, $fields, static::ID_FIELD);
                    break;

                case Verbs::MERGE:
                case Verbs::PATCH:
                    if (!empty($updates)) {
                        $record = $updates;
                    }

                    // get all fields of record
                    $old = $this->parent->getConnection()->getDocument($this->transactionTable, $id);

                    // merge in changes from $record to $merge
                    $record = array_merge($old, $record);

                    // make sure record doesn't contain identifiers
                    if (empty($record)) {
                        throw new BadRequestException('No valid fields were found in record.');
                    }

                    // only update/patch by ids can use batching
                    if (!$single && !$continue && !$rollback) {
                        return parent::addToTransaction($record, $id);
                    }
                    // write back the changes
                    $result = $this->parent->getConnection()->updateDocument($this->transactionTable, $id, $record);
                    if ($rollback) {
                        $this->addToRollback($old);
                    }
                    if ($requireMore) {
                        $result = array_merge($record, $result);
                    }
                    $out = static::cleanRecord($result, $fields, static::ID_FIELD);
                    break;

                case Verbs::DELETE:
                    if (!$single && !$continue && !$rollback) {
                        return parent::addToTransaction(null, $id);
                    }
                    $old = $this->parent->getConnection()->getDocument($this->transactionTable, $id);
                    if ($rollback) {
                        $this->addToRollback($old);
                    }
                    $this->parent->getConnection()->deleteDocument($this->transactionTable, $id);
                    $out = static::cleanRecord($old, $fields, static::ID_FIELD);
                    break;

                case Verbs::GET:
                    if (!$single) {
                        return parent::addToTransaction(null, $id);
                    }

                    $result = $this->parent->getConnection()->getDocument($this->transactionTable, $id);
                    $out = static::cleanRecord($result, $fields, static::ID_FIELD);
                    break;
            }
        } catch (\couchException $ex) {
            throw new RestException($ex->getCode(), $ex->getMessage());
        }

        return $out;
    }

    /** {@inheritdoc} */
    protected function commitTransaction($extras = null)
    {
        if (empty($this->batchRecords) && empty($this->batchIds)) {
            return null;
        }
        $rollback = Scalar::boolval(array_get($extras, 'rollback'));
        $continue = Scalar::boolval(array_get($extras, 'continue'));
        $fields = array_get($extras, ApiOptions::FIELDS);
        $requireMore = array_get($extras, 'require_more');

        $out = [];
        switch ($this->getAction()) {
            case Verbs::POST:
                $result = [];
                foreach ($this->batchRecords as $record) {
                    $id = array_get($record, static::ID_FIELD);
                    unset($record[static::ID_FIELD]);
                    $rs = $this->parent->getConnection()->createDocument(
                        $this->transactionTable,
                        $id,
                        $record
                    );
                    if ($rollback) {
                        static::addToRollback($rs);
                    }
                    if ($requireMore) {
                        $rs = array_merge($record, $rs);
                    }
                    $result[] = $rs;
                }

                $out = static::cleanRecords($result, $fields, static::ID_FIELD);
                break;

            case Verbs::PUT:
            case Verbs::MERGE:
            case Verbs::PATCH:
                $result = [];
                $records = $this->batchRecords;

                foreach ($records as $record) {
                    $id = array_get($record, static::ID_FIELD);
                    unset($record[static::ID_FIELD]);
                    $rs = $this->parent->getConnection()->replaceDocument($this->transactionTable, $id, $record);
                    if ($requireMore) {
                        $rs = array_merge($record, $rs);
                    }
                    $result[] = $rs;
                }

                $out = static::cleanRecords($result, $fields, static::ID_FIELD);
                break;

            case Verbs::DELETE:
                $result = [];
                $errors = [];
                foreach ($this->batchIds as $id) {
                    $old = [];
                    if ($requireMore || $rollback) {
                        $old = $this->parent->getConnection()->getDocument($this->transactionTable, $id);
                        if ($rollback) {
                            static::addToRollback($old);
                        }
                    }

                    try {
                        $rs = $this->parent->getConnection()->deleteDocument($this->transactionTable, $id);
                        if ($requireMore) {
                            $rs = array_merge($old, $rs);
                        }
                        $result[] = $rs;
                    } catch (\Exception $e) {
                        if (false === $continue && false === $rollback) {
                            throw $e;
                        } else {
                            $result[] = $e->getMessage();
                            $errors[] = (!count($result)) ?: count($result) - 1;

                            if (true === $rollback) {
                                if ($e instanceof DfException) {
                                    $e->setContext(['error' => $errors, ResourcesWrapper::getWrapper() => $result]);
                                    $e->setMessage('Batch Error: Not all records were deleted.');
                                }
                                throw $e;
                            }
                        }
                    }
                }

                if (!empty($errors)) {
                    $context = ['error' => $errors, ResourcesWrapper::getWrapper() => $result];
                    throw new BadRequestException('Batch Error: Not all records were deleted.', null, null, $context);
                }

                $out = static::cleanRecords($result, $fields, static::ID_FIELD);
                break;

            case Verbs::GET:
                $result = [];
                $errors = [];
                foreach ($this->batchIds as $id) {
                    try {
                        $document = $this->parent->getConnection()->getDocument($this->transactionTable, $id);
                        $result[] = $document;
                    } catch (\Exception $e) {
                        if (strpos($e->getMessage(), 'LCB_KEY_ENOENT') !== false) {
                            if (count($this->batchIds) > 1) {
                                $result[] = "Record with identifier '" . $id . "' not found.";
                                $errors[] = (!count($result)) ?: count($result) - 1;
                            } else {
                                throw new NotFoundException("Record with identifier '" .
                                    $id .
                                    "' not found." .
                                    $e->getMessage());
                            }
                        } else {
                            throw $e;
                        }
                    }
                }

                if (!empty($errors)) {
                    $context = ['error' => $errors, ResourcesWrapper::getWrapper() => $result];
                    throw new NotFoundException('Batch Error: Not all records could be retrieved.', null, null,
                        $context);
                }

                $out = static::cleanRecords($result, $fields, static::ID_FIELD);
                break;

            default:
                break;
        }

        $this->batchIds = [];
        $this->batchRecords = [];

        return $out;
    }

    /** {@inheritdoc} */
    protected function rollbackTransaction()
    {
        if (!empty($this->rollbackRecords)) {
            switch ($this->getAction()) {
                case Verbs::POST:
                    foreach ($this->rollbackRecords as $rr) {
                        $id = array_get($rr, static::ID_FIELD);
                        if (!empty($id)) {
                            $this->parent->getConnection()->deleteDocument($this->transactionTable, $id);
                        }
                    }
                    break;
                case Verbs::PUT:
                case Verbs::PATCH:
                case Verbs::MERGE:
                    foreach ($this->rollbackRecords as $rr) {
                        $id = array_get($rr, static::ID_FIELD);
                        if (!empty($id)) {
                            $this->parent->getConnection()->replaceDocument($this->transactionTable, $id, $rr);
                        }
                    }
                    break;
                case Verbs::DELETE:
                    foreach ($this->rollbackRecords as $rr) {
                        $id = array_get($rr, static::ID_FIELD);
                        if (!empty($id)) {
                            $this->parent->getConnection()->createDocument($this->transactionTable, $id, $rr);
                        }
                    }
                    break;
                default:
                    // nothing to do here, rollback handled on bulk calls
                    break;
            }

            $this->rollbackRecords = [];
        }

        return true;
    }

    /**
     * Excluding _id field from field list
     *
     * @param string $fields
     *
     * @return string
     */
    protected static function cleanFields($fields)
    {
        $new = [];
        $fieldList = explode(',', $fields);
        foreach ($fieldList as $f) {
            if (static::ID_FIELD !== trim(strtolower($f))) {
                $new[] = $f;
            }
        }

        return implode(',', $new);
    }

    /** {@inheritdoc} */
    public function retrieveRecordsByFilter($table, $filter = null, $params = [], $extras = [])
    {
        $this->transactionTable = $table;
        $fields = array_get($extras, ApiOptions::FIELDS);
        $includeCounts = Scalar::boolval(array_get($extras, ApiOptions::INCLUDE_COUNT));
        $limit = array_get($extras, 'limit', Config::get('df.db.max_records_returned'));
        $offset = array_get($extras, 'offset');
        $orderBy = array_get($extras, 'order_by');
        $groupBy = array_get($extras, 'group_by');
        $fieldsCleaned = static::cleanFields($fields);
        $fieldsSql = $fieldsCleaned;
        if (empty($groupBy)) {
            $fieldsSql = $fieldsCleaned . ',meta().id as _id';
            if (empty($fieldsCleaned)) {
                $fieldsSql = 'meta().id as _id';
            }
        }

        $selectClause = "SELECT $fieldsSql FROM `$table` ";
        $whereClause = "";
        $groupByClause = "";
        $orderByClause = "";
        $limitClause = "LIMIT $limit ";
        $offsetClause = "";
        $params = [];

        if (!empty($groupBy)) {
            $groupByClause = "GROUP BY $groupBy ";
        }
        if (!empty($orderBy)) {
            $orderByClause = "ORDER BY $orderBy ";
        }
        if (!empty($offset)) {
            $offsetClause = "OFFSET $offset ";
        }
        if (!empty($filter)) {
            $filterString = $this->parseFilterString($table, $filter, $params);
            $whereClause = "WHERE $filterString ";
        }

        $sql = $selectClause . $whereClause . $groupByClause . $orderByClause . $limitClause . $offsetClause;
        $result = $this->parent->getConnection()->query($table, $sql, $params);
        $docs = $this->preCleanRecords(array_get($result, 'rows'));
        $idField = (empty($groupBy)) ? static::ID_FIELD : null;
        $out = static::cleanRecords($docs, $fields, $idField);

        if (true === $includeCounts) {
            $out['meta']['count'] = intval(array_get($result, 'metrics.resultCount'));
        }

        return $out;
    }

    /**
     * Cleaning Couchbase rows.
     *
     * @param array $records
     *
     * @return array
     */
    protected function preCleanRecords($records)
    {
        $new = [];
        foreach ($records as $record) {
            if (property_exists($record, $this->transactionTable)) {
                $cleaned = (array)$record->{$this->transactionTable};
                unset($cleaned[static::ID_FIELD]);
                if (property_exists($record, '_id')) {
                    $cleaned = array_merge([static::ID_FIELD => $record->_id], $cleaned);
                }
            } else {
                $cleaned = (array)$record;
            }
            $new[] = $cleaned;
        }

        return $new;
    }

    /** {@inheritdoc} */
    protected static function cleanRecord($record = [], $include = '*', $id_field = null)
    {
        if ('*' !== $include) {
            if (!empty($id_field) && !is_array($id_field)) {
                $id_field = array_map('trim', explode(',', trim($id_field, ',')));
            }
            $id_field = (array)$id_field;

            if (!empty($include) && !is_array($include)) {
                $include = array_map('trim', explode(',', trim($include, ',')));
            }
            $include = (array)$include;

            // make sure we always include identifier fields
            foreach ($id_field as $id) {
                if (false === array_search($id, $include)) {
                    $include[] = $id;
                }
            }

            // glean desired fields from record
            $out = [];
            $expCount = 1;
            foreach ($include as $key) {
                $recordKey = $key;
                if (false !== $alias = static::isExpression($key)) {
                    if (true === $alias) {
                        $recordKey = '$' . (string)$expCount;
                        $expCount++;
                    } else {
                        $recordKey = $alias;
                        $key = $alias;
                    }
                }
                $out[$key] = array_get($record, $recordKey);
            }

            return $out;
        }

        return $record;
    }

    /**
     * Checks to see if field is an expression
     *
     * @param string $field
     *
     * @return bool|string
     */
    protected static function isExpression($field)
    {
        $alias = explode(' as ', $field);
        $field = trim($alias[0]);
        if (preg_match('/\S+\(\S*\)/', $field) === 1) {
            if (isset($alias[1])) {
                return trim($alias[1]);
            }

            return true;
        }

        return false;
    }

    /**
     * @param string $table
     * @param string $filter
     * @param array  $out_params
     * @param array  $in_params
     *
     * @return string
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \Exception
     */
    protected function parseFilterString($table, $filter, array &$out_params, array $in_params = [])
    {
        if (empty($filter)) {
            return null;
        }

        $filter = trim($filter);
        // todo use smarter regex
        // handle logical operators first
        $logicalOperators = DbLogicalOperators::getDefinedConstants();
        foreach ($logicalOperators as $logicalOp) {
            if (DbLogicalOperators::NOT_STR === $logicalOp) {
                // NOT(a = 1)  or NOT (a = 1)format
                if ((0 === stripos($filter, $logicalOp . ' (')) || (0 === stripos($filter, $logicalOp . '('))) {
                    $parts = trim(substr($filter, 3));
                    $parts = $this->parseFilterString($table, $parts, $out_params, $in_params);

                    return static::localizeOperator($logicalOp) . $parts;
                }
            } else {
                // (a = 1) AND (b = 2) format or (a = 1)AND(b = 2) format
                $filter = str_ireplace(')' . $logicalOp . '(', ') ' . $logicalOp . ' (', $filter);
                $paddedOp = ') ' . $logicalOp . ' (';
                if (false !== $pos = stripos($filter, $paddedOp)) {
                    $left = trim(substr($filter, 0, $pos)) . ')'; // add back right )
                    $right = '(' . trim(substr($filter, $pos + strlen($paddedOp))); // adding back left (
                    $left = $this->parseFilterString($table, $left, $out_params, $in_params);
                    $right = $this->parseFilterString($table, $right, $out_params, $in_params);

                    return $left . ' ' . static::localizeOperator($logicalOp) . ' ' . $right;
                }
            }
        }

        $wrap = false;
        if ((0 === strpos($filter, '(')) && ((strlen($filter) - 1) === strrpos($filter, ')'))) {
            // remove unnecessary wrapping ()
            $filter = substr($filter, 1, -1);
            $wrap = true;
        }

        // Some scenarios leave extra parens dangling
        $pure = trim($filter, '()');
        $pieces = explode($pure, $filter);
        $leftParen = (!empty($pieces[0]) ? $pieces[0] : null);
        $rightParen = (!empty($pieces[1]) ? $pieces[1] : null);
        $filter = $pure;

        // the rest should be comparison operators
        // Note: order matters here!
        $sqlOperators = DbComparisonOperators::getParsingOrder();
        foreach ($sqlOperators as $sqlOp) {
            $paddedOp = static::padOperator($sqlOp);
            if (false !== $pos = stripos($filter, $paddedOp)) {
                $field = trim(substr($filter, 0, $pos));
                $negate = false;
                if (false !== strpos($field, ' ')) {
                    $parts = explode(' ', $field);
                    $partsCount = count($parts);
                    if (($partsCount > 1) &&
                        (0 === strcasecmp($parts[$partsCount - 1], trim(DbLogicalOperators::NOT_STR)))
                    ) {
                        // negation on left side of operator
                        array_pop($parts);
                        $field = implode(' ', $parts);
                        $negate = true;
                    }
                }
                /** @type ColumnSchema $info */
                if (null === $info = new ColumnSchema(['name' => strtolower($field)])) {
                    // This could be SQL injection attempt or bad field
                    throw new BadRequestException("Invalid or unparsable field in filter request: '$field'");
                }

                // make sure we haven't chopped off right side too much
                $value = trim(substr($filter, $pos + strlen($paddedOp)));
                if ((0 !== strpos($value, "'")) &&
                    (0 !== $lpc = substr_count($value, '(')) &&
                    ($lpc !== $rpc = substr_count($value, ')'))
                ) {
                    // add back to value from right
                    $parenPad = str_repeat(')', $lpc - $rpc);
                    $value .= $parenPad;
                    $rightParen = preg_replace('/\)/', '', $rightParen, $lpc - $rpc);
                }
                if (DbComparisonOperators::requiresValueList($sqlOp)) {
                    if ((0 === strpos($value, '(')) && ((strlen($value) - 1) === strrpos($value, ')'))) {
                        // remove wrapping ()
                        $value = substr($value, 1, -1);
                        $parsed = [];
                        foreach (explode(',', $value) as $each) {
                            $parsed[] = $this->parseFilterValue(trim($each), $info, $out_params, $in_params);
                        }
                        $value = '[' . implode(',', $parsed) . ']';
                    } else {
                        throw new BadRequestException('Filter value lists must be wrapped in parentheses.');
                    }
                } elseif (DbComparisonOperators::requiresNoValue($sqlOp)) {
                    $value = null;
                } else {
                    static::modifyValueByOperator($sqlOp, $value);
                    $value = $this->parseFilterValue($value, $info, $out_params, $in_params);
                }

                $sqlOp = static::localizeOperator($sqlOp);
                if ($negate) {
                    $sqlOp = DbLogicalOperators::NOT_STR . ' ' . $sqlOp;
                }

                $out = $info->name . " $sqlOp";
                $out .= (isset($value) ? " $value" : null);
                if ($leftParen) {
                    $out = $leftParen . $out;
                }
                if ($rightParen) {
                    $out .= $rightParen;
                }

                return ($wrap ? '(' . $out . ')' : $out);
            }
        }

        // This could be SQL injection attempt or unsupported filter arrangement
        throw new BadRequestException('Invalid or unparsable filter request.');
    }

    /**
     * @param mixed        $value
     * @param ColumnSchema $info
     * @param array        $out_params
     * @param array        $in_params
     *
     * @return int|null|string
     * @throws BadRequestException
     */
    protected function parseFilterValue($value, ColumnSchema $info, array &$out_params, array $in_params = [])
    {
        // if a named replacement parameter, un-name it because Laravel can't handle named parameters
        if (is_array($in_params) && (0 === strpos($value, ':'))) {
            if (array_key_exists($value, $in_params)) {
                $value = $in_params[$value];
            }
        }

        // remove quoting on strings if used, i.e. 1.x required them
        if (is_string($value)) {

            if ((0 === strpos($value, '(')) && ((strlen($value) - 1) === strrpos($value, ')'))) {
                // function call
                return $value;
            }
        }
        // if not already a replacement parameter, evaluate it
        try {
            $value = $this->parseValueForSet($value, $info);
        } catch (ForbiddenException $ex) {
            // need to prop this up?
        }

        if (is_numeric($value)) {
            $value = (int)$value;
        } elseif ('true' === strtolower($value)) {
            $value = true;
        } elseif ('false' === strtolower($value)) {
            $value = false;
        } elseif ((0 === strcmp("'" . trim($value, "'") . "'", $value)) ||
            (0 === strcmp('"' . trim($value, '"') . '"', $value))
        ) {
            $value = substr($value, 1, -1);
        }

        $key = $info->getName() . $this->i;
        $this->i++;
        $out_params[$key] = $value;
        $value = '$' . $key;

        return $value;
    }

    /**
     * @param $value
     * @param $field_info
     *
     * @return mixed
     */
    public function parseValueForSet($value, $field_info)
    {
        switch ($field_info->dbType) {
            case 'int':
                return intval($value);
            default:
                return $value;
        }
    }
}