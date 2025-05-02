/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv.hadoop.table;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.FaultException;
import oracle.kv.KVSecurityConstants;
import oracle.kv.KVSecurityException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.ParamConstant;
import oracle.kv.StatementResult;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldRange;
import oracle.kv.table.IndexKey;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Abstract parent class for RecordReader implementations used to read
 * table rows from an InputSplit. This class provides common, useful,
 * and/or convenient mechanisms that are intended to be shared by
 * subclasses of this class.
 *
 * @since 3.1
 */
abstract class TableRecordReaderBase<K, V> extends RecordReader<K, V> {

    private static final String THIS_CLASSNAME =
                                    TableRecordReaderBase.class.getName();
    private static final Logger LOG2 = LogManager.getLogger(THIS_CLASSNAME);

    private static final String FILE_SEP =
        System.getProperty("file.separator");
    /* Note: has protected scope to support testing. */
    protected static final String USER_SECURITY_DIR =
        System.getProperty("user.dir") + FILE_SEP +
                                         "TABLE_RECORD_READER_SECURITY_DIR";

    private static String[] requiredRangeNames = {"name", "start", "end"};

    private KVStore kvstore;
    private Table kvTable;
    private String tableName;

    /* Iterator input parameters, set in initialize() */
    private PrimaryKey primaryKey;
    private IndexKey indexKey;
    private MultiRowOptions rowOpts;
    private TableIteratorOptions itrOpts = null;
    private ExecuteOptions execOpts = null;

    /* List of remaining partitions to read from if partitionSet iteration. */
    private List<Set<Integer>> partitionSets;

    /* Initial number of partitionSets if using partitionSet iteration. */
    private int startNPartitionSets;

    /*
     * Fields related to how a query will be executed; index scan via IndexKey,
     * table scan via PrimaryKey, or NoSQL DB native query.
     */
    private int queryBy = TableInputSplit.QUERY_BY_PRIMARY_ALL_PARTITIONS;
    private String whereClause = null;
    private String nativeQuery = null;

    /* Set referencing ids of the shards (RepGroups) for index iteratation. */
    private Set<RepGroupId> shardSet;

    /* Initial number of shards when using index (shard) iteration. */
    private int startNShards;

    /*
     * The current iterator. Note that the elements of both IndexKey and
     * PrimaryKey based table iterators are instances of Row; whereas the
     * elements of the table iterator returned when executing a NoSQL DB
     * native query are RecordValue instances from which a Row must be
     * created.
     */
    private TableIterator<? extends RecordValue> iter;
    private StatementResult statementResult;

    protected Row current;

    /*
     * For test use only. Used to simulate a failure during iteration, to
     * verify the issue documented in SR25870 is handled correctly.
     */
    private TestHook<RecordValue> iterFailHook;

    /**
     * Called once at initialization.
     * @param split the split that defines the range of records to read
     * @param context the information about the task
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the operation is interrupted
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {

        final TableInputSplit tableInputSplit = (TableInputSplit) split;
        tableName = tableInputSplit.getTableName();

        LOG2.debug("split = " + tableInputSplit);

        final String kvStoreName = tableInputSplit.getKVStoreName();
        final String[] kvHelperHosts = tableInputSplit.getKVHelperHosts();

        if (kvstore == null) {

            final KVStoreConfig storeConfig =
                new KVStoreConfig(kvStoreName, kvHelperHosts);

            final String kvStoreSecurityFile =
                createLocalKVSecurity(tableInputSplit.getSecurityLogin(),
                                      tableInputSplit.getSecurityTrust());
            storeConfig.setSecurityProperties(
                KVStoreLogin.createSecurityProperties(kvStoreSecurityFile));

            /*
             * If multiple queries
             * connect to different KVStores where one store is non-secure and
             * the other(s) is secure, then since the security information is
             * stored in the state of the splits, if an attempt is made to
             * connect to the non-secure store and that security information
             * is non-null, then a FaultException will be thrown when
             * attempting to connect to the store. This is because that
             * non-null security information will cause the connection
             * mechanism to attempt a secure connection instead of a
             * non-secure connection. To address this, FaultException is
             * caught and the connection attempt is retried with no security
             * information.
             */
            RuntimeException cause = null;
            try {
                kvstore = KVStoreFactory.getStore(
                  storeConfig, tableInputSplit.getSecurityCredentials(), null);
            } catch (FaultException fe1) {
                cause = fe1;
            } catch (KVSecurityException kse1) {
                cause = kse1;
            }
            if (cause != null) {
                if (tableInputSplit.getSecurityCredentials() != null) {
                    final KVStoreConfig kvStoreConfigNonSecure =
                        new KVStoreConfig(kvStoreName, kvHelperHosts);
                    kvStoreConfigNonSecure.setSecurityProperties(
                        KVStoreLogin.createSecurityProperties(null));

                    /*
                     * Try one more time; quit if kvstore still cannot
                     * be retieved.
                     */
                    try {
                        kvstore = KVStoreFactory.getStore(
                                      kvStoreConfigNonSecure);
                    } catch (FaultException fe2) {
                        cause = fe2;
                    } catch (KVSecurityException kse2) {
                        cause = kse2;
                    }
                } else {
                    throw cause;
                }
            }
            /* kvstore still null, cannot proceed. */
            if (kvstore == null) {
                if (cause == null) {
                    throw new IllegalStateException(
                                  "null referenced in split");
                }
                throw cause;
            }
        }

        this.kvTable = kvstore.getTableAPI().getTable(tableName);
        if (kvTable == null) {
            final String msg =
                "Store does not contain table [name=" + tableName + "]";
            LOG2.warn(msg);
            throw new RuntimeException(msg);
        }
        primaryKey =
            getPrimaryKey(kvTable, tableInputSplit.getPrimaryKeyProperty());

        /* Construct MultiRowOptions */
        rowOpts =
            new MultiRowOptions(
                getFieldRange(kvTable,
                              tableInputSplit.getFieldRangeProperty()));

        /* Construct TableIteratorOptions */
        final Direction direction = tableInputSplit.getDirection();
        final Consistency consistency = tableInputSplit.getConsistency();
        final long timeout = tableInputSplit.getTimeout();
        final TimeUnit timeoutUnit = tableInputSplit.getTimeoutUnit();
        final int maxRequests = tableInputSplit.getMaxRequests();
        final int batchSize = tableInputSplit.getBatchSize();

        if ((timeout >= 0 && timeoutUnit != null) || maxRequests >= 0 ||
            batchSize >= 0 || direction != null) {

            execOpts = new ExecuteOptions();

            execOpts = execOpts.setConsistency(consistency);

            if (timeout >= 0 && timeoutUnit != null) {
                execOpts = execOpts.setTimeout(timeout, timeoutUnit);
            }

            if (maxRequests >= 0) {
                execOpts = execOpts.setMaxConcurrentRequests(maxRequests);
            }

            if (batchSize >= 0) {
                execOpts = execOpts.setResultsBatchSize(batchSize);
            }

            if (direction != null) {
                itrOpts = new TableIteratorOptions(
                    direction,
                    execOpts.getConsistency(),
                    execOpts.getTimeout(),
                    execOpts.getTimeoutUnit(),
                    execOpts.getMaxConcurrentRequests(),
                    execOpts.getResultsBatchSize());
            }
        }

        partitionSets = tableInputSplit.getPartitionSets();
        startNPartitionSets =
            (partitionSets == null ? 0 : partitionSets.size());

        queryBy = tableInputSplit.getQueryBy();
        whereClause = tableInputSplit.getWhereClause();
        nativeQuery = "SELECT * FROM " + tableName + " WHERE " + whereClause;

        shardSet = tableInputSplit.getShardSet();
        startNShards = (shardSet == null ? 0 : shardSet.size());

        /* Debug: log how the query will be satisfied. */
        if (LOG2.isDebugEnabled()) {

            if (queryBy == TableInputSplit.QUERY_BY_INDEX) {
                LOG2.debug("pushing index key (scanning ALL_SHARDS)");
            } else if (
                queryBy == TableInputSplit.QUERY_BY_PRIMARY_ALL_PARTITIONS) {
                LOG2.debug("pushing primary key (scanning ALL_PARTITIONS)");
            } else if (
                queryBy == TableInputSplit.QUERY_BY_PRIMARY_SINGLE_PARTITION) {
                LOG2.debug("pushing primary key (scanning SINGLE_PARTITION)");
            } else {
                if (queryBy == TableInputSplit.QUERY_BY_ONQL_ALL_PARTITIONS) {
                    LOG2.debug("pushing native query (scanning " +
                               "ALL_PARTITIONS) - [" + nativeQuery + "]");
                } else if (
                   queryBy == TableInputSplit.QUERY_BY_ONQL_SINGLE_PARTITION) {
                    LOG2.debug("pushing native query (scanning " +
                               "SINGLE_PARTITION) - [" + nativeQuery + "]");

                } else if (queryBy == TableInputSplit.QUERY_BY_ONQL_SHARDS) {
                    LOG2.debug("pushing native query (scanning " +
                               "ALL_SHARDS) - [" + nativeQuery + "]");
                } else {
                    LOG2.debug(
                        "pushing native query (using default, all " +
                        "partitions in 1 split) - [" + nativeQuery + "]");
                }
            }
        }
    }

    /**
     * Reads the next key/value pair as a <code>Row</code> and then processes
     * the contents of the row.
     *
     * @return <code>true</code> when a key/value pair was successfully read
     * and processed; otherwise, returns <code>false</code> when EOF is
     * encountered.
     *
     * @throws IOException when <code>IOException</code> occurs during
     * processing. When any other type of exception is encountered during
     * processing, throws that exception wrapped in <code>IOException</code>;
     * to be consistent with the Hadoop <code>RecordReader</code> interface
     * implemented by this class. See SR25870 for more details.
     */
    @Override
    public boolean nextKeyValue() throws IOException {

        try {
            if (iter == null) {
                iter = getNextIterator();
            }

            while (iter != null) {

                if (iter.hasNext()) {

                    final RecordValue nextRecord = iter.next();

                    /* When testing, simulate a failure during iteration. */
                    assert TestHookExecute.
                        doHookIfSet(iterFailHook, nextRecord);

                    if (nextRecord instanceof Row) {
                        current = (Row) nextRecord;
                    } else {
                        current = kvTable.createRow(nextRecord);
                    }
                    LOG2.trace("next row: " + current);
                    return true;
                }

                current = null;

                if (statementResult != null) {
                    statementResult.close();
                    statementResult = null;
                }

                iter = getNextIterator();
            }

            LOG2.trace("exit loop: return false");
            return false;

        } catch (Exception e) {
            LOG2.warn("Exception while iterating table [" + e + "]", e);
            /*
             * Throw the exception wrapped in an IOE to match the Hadoop
             * RecordReader interface for the next method. See SR25870.
             */
            throw new IOException(e);
        }
    }

    private TableIterator<? extends RecordValue> getNextIterator() {

        /*
         * 1. When the query is IndexKey based, each split is mapped to one
         *    shard. In this case, return a TableIterator that returns only
         *    the rows associated with the given IndexKey and corresponding
         *    shards.
         */
        if (queryBy == TableInputSplit.QUERY_BY_INDEX) {
            if (indexKey != null) {

                if (shardSet.isEmpty()) {
                    return null;
                }

                /*
                 * Each split mapped to one shard means one record reader per
                 * shard. As a result, the shardSet will contain only one
                 * element; the single shard from the associated split.
                 */
                final Set<RepGroupId> repGroupIds =
                    new HashSet<RepGroupId>(shardSet);

                shardSet.clear();

                return ((TableAPIImpl) kvstore.getTableAPI()).tableIterator(
                            indexKey, rowOpts, itrOpts, repGroupIds);
            }
        }

        /*
         * 2. When the query is PrimaryKey (or ShardKey) based, each split is
         *    mapped to a set of partitions. If PrimaryKey based, the union
         *    of all partition sets from each split will equal the set of all
         *    partitions from the store; and a TableIterator that returns
         *    only the rows associated with the given key and corresponding
         *    partitions will be returned. If the query is ShardKey based,
         *    then the associated set of partitions will contain only one
         *    element; the partition in which the ShardKey is located.
         */
        if (queryBy == TableInputSplit.QUERY_BY_PRIMARY_ALL_PARTITIONS ||
            queryBy == TableInputSplit.QUERY_BY_PRIMARY_SINGLE_PARTITION) {

            if (partitionSets.isEmpty()) {
                return null;
            }

            final Set<Integer> partitions = partitionSets.remove(0);
            assert partitions.size() > 0;

            return ((TableAPIImpl) kvstore.getTableAPI()).tableIterator(
                       primaryKey, rowOpts, itrOpts, partitions);
        }

        /*
         * 3. When the query will be satisfied via a partition based NATIVE
         *    NoSQL DB ONQL iterator.
         */
        if (queryBy == TableInputSplit.QUERY_BY_ONQL_ALL_PARTITIONS ||
            queryBy == TableInputSplit.QUERY_BY_ONQL_SINGLE_PARTITION) {

            return getNextOnqlIteratorPartitions(nativeQuery);
        }

        /*
         * 4. All possible values of queryBy have been handled except for
         *    TableInputSplit.QUERY_BY_ONQL_SHARDS; which means the query
         *    will be satisfied via a shard based NATIVE NoSQL DB ONQL
         *    iterator.
         */
        return getNextOnqlIteratorShards(nativeQuery);
    }

    private TableIterator<? extends RecordValue> getNextOnqlIteratorPartitions(
                                                     final String query) {

        if (partitionSets.isEmpty()) {
            return null;
        }

        final Set<Integer> partitions = partitionSets.remove(0);
        assert partitions.size() > 0;

        statementResult =
            ((KVStoreImpl) kvstore).executeSyncPartitions(
                                        query, execOpts, partitions);

        if (statementResult == null) {
            return null;
        }
        return statementResult.iterator();
    }

    private TableIterator<? extends RecordValue> getNextOnqlIteratorShards(
                                                     final String query) {
        if (shardSet.isEmpty()) {
            return null;
        }
        final Set<RepGroupId> repGroupIds = new HashSet<RepGroupId>(shardSet);
        shardSet.clear();

        statementResult =
            ((KVStoreImpl) kvstore).executeSyncShards(
                                        query, execOpts, repGroupIds);

        if (statementResult == null) {
            return null;
        }
        return statementResult.iterator();
    }

    /**
     * The current progress of the record reader through its data.
     *
     * @return a number between 0.0 and 1.0 that is the fraction of the data
     * read
     */
    @Override
    public float getProgress() {

        if (queryBy == TableInputSplit.QUERY_BY_INDEX) {
            return (shardSet == null || startNShards == 0) ? 0 :
                    (float) (startNShards - shardSet.size()) /
                                                   (float) startNShards;
        }
        return (partitionSets == null || startNPartitionSets == 0) ? 0 :
                    (float) (startNPartitionSets - partitionSets.size()) /
                                                   (float) startNPartitionSets;
    }

    /**
     * Close the record reader.
     */
    @Override
    public void close()
        throws IOException {
        LOG2.trace("close iterator and store");
        if (iter != null) {
            iter.close();
        }
        if (kvstore != null) {
            kvstore.close();
        }
    }

    /**
     * Constructs a PrimaryKey for iteration from the given String property.
     * This method assumes that the value of the given prop parameter is a
     * list of name:value pairs in JSON FORMAT like the following:
     * <code>
     *   -Doracle.kv.primaryKey="{\"name\":\"stringVal\",\"name\":floatVal}"
     * </code>
     * For example,
     * <code>
     *   -Doracle.kv.primaryKey="{\"make"\":\"ford\",\"price\":23450.23}"
     * </code>
     * where the list itself is enclosed in un-escaped double quotes and
     * corresponding curly brace; and each field name component -- as well
     * as each STRING type field value component -- is enclosed in ESCAPED
     * double quotes.
     *
     * Note that the double quotes that encapsulate the string values
     * referenced by each field name and each string type field value
     * MUST BE ESCAPED; otherwise, a parsing error will occur. This is
     * because the hadoop command interpreter strips off the double quotes
     * surrounding the name and value components before passing the system
     * property on to the Java VM in which the MapReduce job executes.
     * Escaping the double quotes in the way described above preserves
     * the double quotes so that the value of the system property is in
     * valid JSON format when parsed by this method below.
     */
    private PrimaryKey getPrimaryKey(final Table table, final String prop) {

        PrimaryKey retKey = table.createPrimaryKey();
        if (prop == null) {
            /* Send wildcard if the property is null. */
            return retKey;
        }

        final String warnStr =
            "Invalid JSON in property [" +
            ParamConstant.PRIMARY_KEY.getName() + "=" + prop + "]: " +
            "must be a list of name:value pairs in JSON format having " +
            "the form -D" + ParamConstant.PRIMARY_KEY.getName() + "=" +
            "\"{\\\"fieldName\\\":\\\"stringValue\\\"," +
            "\\\"fieldName\\\":floatValue, ... }\"; where the list " +
            "is enclosed in un-escaped double quotes and curly braces, " +
            "and each fieldName component and each STRING type fieldValue " +
            "component is enclosed in ESCAPED double quotes. ";

        final String proceedStr = "Proceeding with full PrimaryKey wildcard.";
        try {
            retKey = table.createPrimaryKeyFromJson(prop, false);
        } catch (IllegalArgumentException e) {
            LOG2.warn(e + ". " + warnStr + proceedStr);
        }
        return retKey;
    }

    /**
     * Constructs a FieldRange to use in a table iteration from the given
     * String property. This method assumes that the value of the given
     * rangeFieldProp parameter is a list of name:value pairs in JSON FORMAT
     * like the following:
     * <code>
     *   -Doracle.kv.fieldRange="{\"name\":\"fieldName\",
     *      \"start\":\"startVal\",[\"startInclusive\":true|false],
     *      \"end\"\"endVal\",[\"endInclusive\":true|false]}"
     * </code>
     * For example, if the PrimaryKey is specified using the fields,
     * 'primary-key -field type -field make -field model -field color', then
     * one might specify a field range with the following system properties:
     * <code>
     *   -Doracle.kv.primaryKey="{\"type\":\"truck\"}"
     *   -Doracle.kv.fieldRange="{\"name\":\"make\",
     *      \"start\":\"Chrysler\",\"startInclusive\":true,
     *      \"end\"\"GM\",\"endInclusive\":false}"
     * </code>
     * Note that the list itself is enclosed in un-escaped double quotes and
     * corresponding curly brace, but each name component -- as well as each
     * STRING type value component -- is enclosed in ESCAPED double quotes.
     *
     * Note also that the double quotes that encapsulate the string values
     * referenced by each name and each string type value MUST BE ESCAPED;
     * otherwise, a parsing error will occur. This is because the hadoop
     * command interpreter strips off the double quotes surrounding the name
     * and value components before passing the system property on to the
     * Java VM in which the MapReduce job executes. Escaping the double
     * quotes in the way described above preserves the double quotes so that
     * the value of the system property is in valid JSON format when parsed
     * by this method below.
     */
    private FieldRange getFieldRange(final Table table,
                                     final String rangeFieldProp) {

        FieldRange retRange = null;

        if (rangeFieldProp == null) {
            return retRange;
        }

        final String warnStr =
            "Invalid JSON in property [" +
            ParamConstant.FIELD_RANGE.getName() + "=" + rangeFieldProp +
            "]: must be a list of name:value pairs in JSON format having " +
            "the form -D" + ParamConstant.FIELD_RANGE.getName() + "=" +
            "\"{\\\"name\\\":\\\"fieldName\\\"," +
            "\\\"start\\\":\\\"stringStartVal\\\"|scalarStartVal," +
            "[\\\"startInclusive\\\":true|false]," +
            "\\\"end\\\":\\\"stringEndVal\\\"|scalarEndVal," +
            "[\\\"endInclusive\\\":true|false]}\"; where the list " +
            "is enclosed in un-escaped double quotes and curly braces, " +
            "and each name component and each STRING type value component " +
            "is enclosed in ESCAPED double quotes. ";

        final String proceedStr = "Proceeding with full range of values " +
              "for the PrimaryKey rather than a sub-range ";
        try {
            /* TODO: replace the call to the createFieldRange method from this
             *       class with Table.createFieldRange(rangeFieldProp) once
             *       that method is added to oracle.kv.table.Table; that is,
             *
             *       retRange = table.createFieldRange(rangeFieldProp);
             */
            retRange = createFieldRange(table, rangeFieldProp);
        } catch (IllegalArgumentException e) {
            LOG2.warn(e + ". " + warnStr + proceedStr);
        }
        return retRange;
    }

    /*
     * NOTE: this method is temporary. Once the createFieldRange method is
     *       added to oracle.kv.table.Table, the getFieldRange method above
     *       should be changed to invoke Table.createFieldRange rather than
     *       this method; and this method can be removed.
     */
    private FieldRange createFieldRange(final Table table,
                                        final String rangeFieldProp) {
        FieldRange retRange = null;

        if (rangeFieldProp == null) {
            return retRange;
        }

        final String missingLeftBraceStr =
                     "invalid JSON format: system property does not begin " +
                     "with left curly brace ['{']";
        final String missingRightBraceStr =
                     "invalid JSON format: system property does not end " +
                     "with right curly brace ['}']";
        if (!rangeFieldProp.startsWith("{")) {
            throw new IllegalArgumentException(missingLeftBraceStr);
        }
        if (!rangeFieldProp.endsWith("}")) {
            throw new IllegalArgumentException(missingRightBraceStr);
        }

        final String dq = "\"";
        final String colon = ":";
        final String missingNameBase = "missing required range field name ";

        /* Verify prop contains all required names in form, "requiredName": */
        for (String requiredName : requiredRangeNames) {
            final String missingName =
                      missingNameBase + "[" + requiredName + "]";

            final int indxOf = rangeFieldProp.indexOf(
                                   dq + requiredName + dq + colon);

            if (indxOf < 0) {
                throw new IllegalArgumentException(missingName);
            }
        }

        /* Build FieldRange from system property inside curly braces. */
        String nameVal = null;
        String startVal = null;
        boolean startInclusive = true;
        String endVal = null;
        boolean endInclusive = true;
        FieldDef.Type fieldType = null;

        final String invalidComponentBase =
                     "invalid JSON format: invalid \"name\":\"value\" pair " +
                     "in system property ";

        /* Strip off the enclosing curly braces from system property. */
        final String prop =
                     rangeFieldProp.substring(1, rangeFieldProp.length() - 1);

        final String[] propComponents = prop.split(",");
        for (String propComponent : propComponents) {

            final String invalidComponent =
                      invalidComponentBase + "[" + propComponent + "]";

            /* Each component must be "name":"value" pair; otherwise errorr. */
            final String[] rangeComponents = propComponent.split(":");
            if (rangeComponents.length != 2) {
                throw new IllegalArgumentException(invalidComponent);
            }

            /*
             * Each name must be encapsulated by double quotes (verified above)
             * and each value of type STRING must be encapsulated by double
             * quotes. Scalar values are not encapsulated by double quotes.
             * Strip off double quotes before constructing the FieldRange.
             */
            final String name = rangeComponents[0].substring(
                                          1, rangeComponents[0].length() - 1);

            /* For values, handle both scalars and strings appropriately. */
            final String val = rangeComponents[1];

            if ("name".equals(name.toLowerCase())) {
                /* The name of the field over which to range is a STRING. */
                if (!(val.startsWith(dq) && val.endsWith(dq))) {
                    throw new IllegalArgumentException(invalidComponent);
                }
                nameVal = val.substring(1, val.length() - 1);
                fieldType = table.getField(nameVal).getType();
            } else if ("start".equals(name.toLowerCase())) {
                /* Test for un-matched double quotes. */
                if (val.startsWith(dq) && !val.endsWith(dq)) {
                    throw new IllegalArgumentException(invalidComponent);
                } else if (!val.startsWith(dq) && val.endsWith(dq)) {
                    throw new IllegalArgumentException(invalidComponent);
                } else if (val.startsWith(dq) && val.endsWith(dq)) {
                    startVal = val.substring(1, val.length() - 1); /* string */
                } else {
                    startVal = val; /* scalar */
                }
            } else if ("startinclusive".equals(name.toLowerCase())) {
                startInclusive = Boolean.parseBoolean(val);
            } else if ("end".equals(name.toLowerCase())) {
                /* Test for un-matched double quotes. */
                if (val.startsWith(dq) && !val.endsWith(dq)) {
                    throw new IllegalArgumentException(invalidComponent);
                } else if (!val.startsWith(dq) && val.endsWith(dq)) {
                    throw new IllegalArgumentException(invalidComponent);
                } else if (val.startsWith(dq) && val.endsWith(dq)) {
                    endVal = val.substring(1, val.length() - 1); /* string */
                } else {
                    endVal = val; /* scalar */
                }
            } else if ("endinclusive".equals(name.toLowerCase())) {
                endInclusive = Boolean.parseBoolean(val);
            }
        }

        if (nameVal == null) {
            throw new IllegalArgumentException(invalidComponentBase);
        }
        if (startVal == null && fieldType == null) {
            throw new IllegalArgumentException(invalidComponentBase);
        }

        /* Verification complete. Construct the FieldRange return value. */

        retRange = table.createFieldRange(nameVal);

        if (FieldDef.Type.STRING.equals(fieldType)) {
            retRange.setStart(startVal, startInclusive);
            retRange.setEnd(endVal, endInclusive);
        } else {
            try {
                if (FieldDef.Type.INTEGER.equals(fieldType)) {
                    retRange.setStart(
                        Integer.parseInt(startVal), startInclusive);
                    retRange.setEnd(Integer.parseInt(endVal), endInclusive);
                } else if (FieldDef.Type.LONG.equals(fieldType)) {
                    retRange.setStart(
                        Long.parseLong(startVal), startInclusive);
                    retRange.setEnd(Long.parseLong(endVal), endInclusive);
                } else if (FieldDef.Type.FLOAT.equals(fieldType)) {
                    retRange.setStart(
                        Float.parseFloat(startVal), startInclusive);
                    retRange.setEnd(Float.parseFloat(endVal), endInclusive);
                } else if (FieldDef.Type.DOUBLE.equals(fieldType)) {
                    retRange.setStart(
                        Double.parseDouble(startVal), startInclusive);
                    retRange.setEnd(Double.parseDouble(endVal), endInclusive);
                } else {
                    throw new IllegalArgumentException(invalidComponentBase);
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                              new NumberFormatException(invalidComponentBase));
            }
        }
        return retRange;
    }

    /**
     * Convenience method that retrieves the login configuration and trust
     * credentials as resources from the classpath, and writes that
     * information to corresponding files on the user's local file system.
     * The login file and trust file that are created by this method can
     * then be used when attempting to interact with a secure KVStore.
     * <p>
     * This method returns a string containing the fully qualified path to
     * the login file that is created. The login file will reference the
     * name of the trust file that is created; which must be written to
     * the same directory in which the login file resides.
     */
    private static String createLocalKVSecurity(final String loginFlnm,
                                                final String trustFlnm)
                              throws IOException {

        if (loginFlnm == null) {
            return null;
        }

        if (trustFlnm == null) {
            return null;
        }

        /*
         * If loginFile and/or trustFile is a filename and not an absolute
         * path, then generate local versions of the file.
         */
        final File userSecurityDirFd = new File(USER_SECURITY_DIR);
        if (!userSecurityDirFd.exists()) {
            if (!userSecurityDirFd.mkdirs()) {
                throw new IOException("failed to create " + userSecurityDirFd);
            }
        }

        InputStream loginStream = null;
        InputStream trustStream = null;

        final ClassLoader cl = TableRecordReaderBase.class.getClassLoader();
        if (cl != null) {
            loginStream = cl.getResourceAsStream(loginFlnm);
            trustStream = cl.getResourceAsStream(trustFlnm);
        } else {
            loginStream = ClassLoader.getSystemResourceAsStream(loginFlnm);
            trustStream = ClassLoader.getSystemResourceAsStream(trustFlnm);
        }

        /*
         * Retrieve the login configuration as a resource from the classpath,
         * and write that information to the user's local file system.
         */
        final Properties loginProps = new Properties();
        if (loginStream != null) {
            loginProps.load(loginStream);
        }

        /* Strip off the path of the trust file. */
        final String trustProp =
            loginProps.getProperty(
                KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY);
        if (trustProp != null) {
            final File trustPropFd = new File(trustProp);
            if (!trustPropFd.exists()) {
                loginProps.setProperty(
                    KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                    trustPropFd.getName());
            }
        }

        final File loginFd =
            new File(USER_SECURITY_DIR + FILE_SEP + loginFlnm);
        final File loginParentFd = loginFd.getParentFile();
        if (!loginParentFd.exists()) {
            if (!loginParentFd.mkdirs()) {
                throw new IOException("failed to create " + loginParentFd);
            }
        }
        final FileOutputStream loginFos = new FileOutputStream(loginFd);
        loginProps.store(loginFos, null);
        loginFos.close();

        /*
         * Retrieve the trust credentials as a resource from the classpath,
         * and write that information to the user's local file system.
         */
        final File trustFd =
            new File(USER_SECURITY_DIR + FILE_SEP + trustFlnm);
        final File trustParentFd = trustFd.getParentFile();
        if (!trustParentFd.exists()) {
            if (!trustParentFd.mkdirs()) {
                throw new IOException("failed to create " + trustParentFd);
            }
        }
        final FileOutputStream trustFlnmFos =
            new FileOutputStream(trustFd);

        if (trustStream != null) {
            int nextByte = trustStream.read();
            while (nextByte != -1) {
                trustFlnmFos.write(nextByte);
                nextByte = trustStream.read();
            }
        }
        trustFlnmFos.close();

        return loginFd.toString();
    }

    public Table getKvTable() {
        return kvTable;
    }

    public void setIndexKey(final IndexKey key) {
        indexKey = key;
        primaryKey = null;
    }

    public void setPrimaryKey(final PrimaryKey key) {
        primaryKey = key;
        indexKey = null;
    }

    public void setMultiRowOptions(final FieldRange fieldRange) {
        rowOpts = new MultiRowOptions(fieldRange);
    }

    /**
     * For testing only; to support the use of a mocked store.
     */
    public void setStore(final KVStore testStore) {
        this.kvstore = testStore;
    }

    public void setQueryInfo(int newQueryBy, String newWhereClause) {
        this.queryBy = newQueryBy;
        this.whereClause = newWhereClause;
    }

    /**
     * Used only when testing. See SR25870.
     */
    public void setIterFailHook(TestHook<RecordValue> iterFailHook) {
        this.iterFailHook = iterFailHook;
    }
}
