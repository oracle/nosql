/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import java.io.IOException;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.map.HashKeyToPartitionMap;

import oracle.nosql.driver.Consistency;
import oracle.nosql.driver.Durability;
import oracle.nosql.driver.IndexNotFoundException;
import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.RequestTimeoutException;
import oracle.nosql.driver.RetryableException;
import oracle.nosql.driver.Version;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.kv.StoreAccessTokenProvider;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.GetIndexesRequest;
import oracle.nosql.driver.ops.GetIndexesResult;
import oracle.nosql.driver.ops.GetIndexesResult.IndexInfo;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.MultiDeleteRequest;
import oracle.nosql.driver.ops.MultiDeleteResult;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutRequest.Option;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryIterableResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.TableResult.State;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.ops.WriteMultipleResult.OperationResult;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.LongValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;

import standalone.datacheck.DataCheckMain.PrincipalType;
import standalone.datacheck.DataCheckMain.ProxyType;

/**
 * Implement API for data checking used by {@link DataCheckMain}.
 * This class is for checking Table entries.
 */
public class DataCheckTableProxy
        extends DataCheckTable<MapValue, MapValue, Request> {
    private static final int DELAY_MILLIS = 1000;
    private static final int DC_MD_STORAGE_GB = 1;
    private static final int DDL_RETRY_COUNT = 5;
    private static final String[] SHARD_KEY =
        {FLD_MAJORKEY1, FLD_MAJORKEY2};
    private static final String[] PRIMARY_KEY =
        {FLD_MAJORKEY1, FLD_MAJORKEY2, FLD_MINORKEY};
    private static final String[] FIELD_NAMES = {
        FLD_MAJORKEY1,
        FLD_MAJORKEY2,
        FLD_MINORKEY,
        FLD_OPERATION,
        FLD_FIRSTTHREAD,
        FLD_INDEX};
    private static final String[] MD_PRIMARY_KEY =
        {FLD_MDMAJORKEY1, FLD_MDMAJORKEY2, FLD_MDMAJORKEY3, FLD_MDMINORKEY};

    private static final String COMPLETE_KEY_MULTI_GET_STMT =
        "DECLARE $v1 STRING; " +
        "$v2 STRING; " +
        "$v3 STRING;\n" +
        "SELECT * FROM " + TAB_DATACHECK + " WHERE " +
        FLD_MAJORKEY1 + " = $v1 AND " +
        FLD_MAJORKEY2 + " = $v2 AND " +
        FLD_MINORKEY  + " = $v3 " +
        "ORDER BY " + FLD_MAJORKEY1 + ", " +
        FLD_MAJORKEY2 + ", " +
        FLD_MINORKEY;

    private static final String COMPLETE_SHARD_KEY_MULTI_GET_STMT =
        "DECLARE $v1 STRING; " +
        "$v2 STRING;\n" +
        "SELECT * FROM " + TAB_DATACHECK + " WHERE " +
        FLD_MAJORKEY1 + " = $v1 AND " +
        FLD_MAJORKEY2 + " = $v2 " +
        "ORDER BY " + FLD_MAJORKEY1 + ", " +
        FLD_MAJORKEY2 + ", " +
        FLD_MINORKEY;

    private static final String INDEX_ITERATOR_STMT =
        "DECLARE $v1 LONG;\n" +
        "SELECT * FROM " + TAB_DATACHECK + " " +
        "WHERE " + FLD_INDEX + " = $v1 " +
        "ORDER BY " + FLD_INDEX;

    private static final String INDEX_KEYS_ITERATOR_STMT =
        "DECLARE $v1 LONG;\n" +
        "SELECT " + FLD_MAJORKEY1 + ", " +
        FLD_MAJORKEY2 + ", " +
        FLD_MINORKEY + ", " +
        FLD_INDEX + " " +
        "FROM " + TAB_DATACHECK + " " +
        "WHERE " + FLD_INDEX + " = $v1 " +
        "ORDER BY " + FLD_INDEX;

    private static final String MULTI_GET_KEYS_TABLE_STMT =
        "DECLARE $v1 STRING; " +
        "$v2 STRING; " +
        "$v3 STRING;\n" +
        "SELECT " + FLD_MDMAJORKEY1 + ", " +
        FLD_MDMAJORKEY2 + ", " +
        FLD_MDMAJORKEY3 + ", " +
        FLD_MDMINORKEY + " " +
        "FROM " + TAB_DATACHECKMD + " WHERE " +
        FLD_MDMAJORKEY1 + " = $v1 AND " +
        FLD_MDMAJORKEY2 + " = $v2 AND " +
        FLD_MDMAJORKEY3 + " = $v3 " +
        "ORDER BY " + FLD_MDMAJORKEY1 + ", " +
        FLD_MDMAJORKEY2 + ", " +
        FLD_MDMAJORKEY3;

    /*
     * Used for dropping the DataCheck table before running on the cloud
     * to avoid duplicate data
     */
    private static final String DROP_TABLE_SQL =
        "DROP TABLE IF EXISTS " + TAB_DATACHECK;

    private static final String DROP_MDTABLE_SQL =
        "DROP TABLE IF EXISTS " + TAB_DATACHECKMD;

    /*
     * A list of UpdateOpTypes selected so that they will create no net
     * increase or decrease on the number of entries if chosen randomly.
     */
    {
        Collections.addAll(
            updateOpTypes,

            /* Pairs of operations with matching add and remove */
            PUT,
            DELETE,

            PUT_EXECUTE,
            DELETE_EXECUTE,

            PUT_IF_ABSENT,
            DELETE_IF_VERSION,

            PUT_IF_ABSENT_EXECUTE,
            DELETE_IF_VERSION_EXECUTE,

            /* Need pairs, so add PUT again */
            PUT,
            MULTI_DELETE);

        if (enableQueries) {
            Collections.addAll(
                updateOpTypes,

                QUERY_INSERT,
                QUERY_DELETE,

                /*
                 * There is only one query operation for deletes, so use it
                 * again to pair with UPSERT
                 */
                QUERY_UPSERT,
                QUERY_DELETE);
        }

        Collections.addAll(
            updateOpTypes,

            /*
             * The remaining operations have no effect on the number of
             * entries.
             */
            PUT_IF_PRESENT,
            PUT_IF_VERSION,
            PUT_IF_PRESENT_EXECUTE,
            PUT_IF_VERSION_EXECUTE);

        if (enableQueries) {
            Collections.addAll(
                updateOpTypes,

                /* Need an even number, so use this one twice */
                QUERY_UPDATE,
                QUERY_UPDATE);
        }
    }

    private final ProxyType proxyType;
    private final NoSQLHandle nosqlHdl;
    private MapValue emptyRow;
    private long dctableId;
    private final HashKeyToPartitionMap partitionMap;

    /*
     * cache of index names with the assumption that currently datacheck
     * neither alters the schema of existing indexes nor drops any of the
     * existing indexes
     */
    private final Set<String> indexNamesSet;

    private TableLimits dcTableLimits;
    /* if the table id is used to find the partition id */
    private final boolean useDcTableId;
    /* max bulk write operations on cloud is 50 unless manually increased */
    private final int writeMultipleMax;
    private final boolean dropCloudTableBefore;
    private final boolean dropCloudTableAfter;

    /* Cache prepared queries */
    private final Map<String, PreparedStatement> preparedStatements =
        Collections.synchronizedMap(new HashMap<>());

    /* Constructor */

    /**
     * Creates an instance of this class.  Use a new instance for each test
     * phase.
     *
     * @param proxyType the proxy type (ONPREM or CLOUD)
     * @param proxyProtocol the proxy protocol
     * @param proxyHostName the proxy host name
     * @param proxyPortNum the proxy port number
     * @param proxyUserName the proxy user name
     * @param proxyUserPass the proxy user password
     * @param principalType the type of authorization, like user principal,
     *        instance principal, or resource principal
     * @param tenantOcid need tenant ocid if using instance principal for
     *        authentication
     * @param ociConfigPath oci config file path
     * @param ociConfigProfile oci config profile
     * @param store the store
     * @param config the configuration used to create the store
     * @param reportingInterval the interval in milliseconds between reporting
     *        progress and status of the store
     * @param partitions limit operations to keys falling in the specified
     *        partitions, or null for no restriction
     * @param seed the random seed, or -1 to generate a seed randomly
     * @param verbose the verbose level, with increasing levels of verbosity
     *        for values beyond {@code 0}
     * @param err the error output stream
     * @param requestTimeout the maximum permitted timeout in milliseconds for
              store operations, including the network read timeout
     * @param blockTimeout the timeout in milliseconds for a set of operations
     *        on a block of entries
     * @param useParallelScan use parallel scan in exercise phase if specified.
     * @param scanMaxConcurrentRequests the max concurrent threads are allowed
     *        for a parallel scan.
     * @param scanMaxResultsBatches the max batches to temporally store the
     *        parallel scan results, each batch can store one scan results.
     * @param indexReadPercent index read percentage
     * @param maxThroughput max throughput for the client.
     * @param maxExecTime the max execution for the client workload.
     * @param useTTL whether to test the TTL feature
     * @param threads the number of threads to run each datacheck phase
     * @param numPartitions the number of kvstore partitions
     * @param dcTableLimits r/w/GB limits of table
     * @param writeMultipleMax the number of writes in bulk operations
     * @param useDcTableId is using debugging feature on kv24.2
     * @param dropCloudTableBefore drop DataCheck+DataCheckMD tables before run
     * @param dropCloudTableAfter drop DataCheck+DataCheckMD tables after run
     * @param isPopulatePhase if the current phase of DataCheck is Populate
     */
    public DataCheckTableProxy(ProxyType proxyType,
                               String proxyProtocol,
                               String proxyHostName,
                               String proxyPortNum,
                               String proxyUserName,
                               String proxyUserPass,
                               PrincipalType principalType,
                               String tenantOcid,
                               String ociConfigPath,
                               String ociConfigProfile,
                               KVStore store,
                               KVStoreConfig config,
                               long reportingInterval,
                               int[] partitions,
                               long seed,
                               int verbose,
                               PrintStream err,
                               long requestTimeout,
                               long blockTimeout,
                               boolean useParallelScan,
                               int scanMaxConcurrentRequests,
                               int scanMaxResultsBatches,
                               int indexReadPercent,
                               double maxThroughput,
                               int maxExecTime,
                               boolean useTTL,
                               int threads,
                               int numPartitions,
                               TableLimits dcTableLimits,
                               int writeMultipleMax,
                               boolean useDcTableId,
                               boolean dropCloudTableBefore,
                               boolean dropCloudTableAfter,
                               boolean isPopulatePhase) {

        super(store, config, reportingInterval, partitions,
              seed, verbose, err, requestTimeout, blockTimeout,
              useParallelScan, scanMaxConcurrentRequests, scanMaxResultsBatches,
              indexReadPercent, maxThroughput, maxExecTime, useTTL);

        this.proxyType = proxyType;
        this.dcTableLimits = dcTableLimits;
        this.writeMultipleMax = writeMultipleMax;
        this.useDcTableId = useDcTableId;
        this.dropCloudTableBefore = dropCloudTableBefore;
        this.dropCloudTableAfter = dropCloudTableAfter;
        partitionMap = new HashKeyToPartitionMap(numPartitions);

        NoSQLHandleConfig nosqlCfg;
        if (proxyType == ProxyType.ONPREM) {
            URL url;
            try {
                url = new URI(proxyProtocol + "://" +
                              proxyHostName + ":" +
                              Integer.parseInt(proxyPortNum) +
                              "/")
                    .toURL();
            } catch (MalformedURLException|URISyntaxException me) {
                throw new IllegalArgumentException(me.getMessage(), me);
            }

            nosqlCfg = new NoSQLHandleConfig(url);
        } else {
            /**
             * Enable rate limiting for CLOUD proxy type.
             * Since datacheck runs pairs of concurrent threads,
             * allow each to use 50% of the bandwidth.
             */
            nosqlCfg = new NoSQLHandleConfig(proxyHostName)
                .setRateLimitingEnabled(true);
            nosqlCfg.setDefaultRateLimitingPercentage(50.0);
        }
        nosqlCfg.setConnectionPoolMinSize(threads * 2);

        if (proxyProtocol.equals("https")) {
            if (proxyType == ProxyType.CLOUD) {
                // TODO: add support for PrincipalType.RESOURCE
                if (principalType == PrincipalType.USER) {
                    try {
                        nosqlCfg.setAuthorizationProvider(
                                new SignatureProvider(ociConfigPath,
                                                      ociConfigProfile));
                    } catch (IOException ioe) {
                        throw new IllegalArgumentException(
                                "Error in loading profile: " +
                                ociConfigProfile + " " +
                                "from OCI configuration file: " +
                                ociConfigPath);
                    }
                } else if (principalType == PrincipalType.INSTANCE) {
                    nosqlCfg.setDefaultCompartment(tenantOcid);
                    nosqlCfg.setAuthorizationProvider(
                            SignatureProvider.createWithInstancePrincipal());
                } else {
                    throw new IllegalArgumentException("Resource Principal " +
                                                       "type is not yet " +
                                                       "supported");
                }
            } else {
                nosqlCfg.setAuthorizationProvider(
                    new StoreAccessTokenProvider(proxyUserName,
                                                 proxyUserPass.toCharArray()));
            }
        } else {
            if (proxyType == ProxyType.CLOUD) {
                    throw new IllegalArgumentException(
                        "Proxy protocol cannot be http for CLOUD");
            }

            nosqlCfg.setAuthorizationProvider(new StoreAccessTokenProvider());
        }

        nosqlHdl = NoSQLHandleFactory.createNoSQLHandle(nosqlCfg);

        if (useTTL) {
            throw new IllegalArgumentException("TTL is not yet supported " +
                                               "in datacheck proxy mode");
        }

        if (dropCloudTableBefore && isPopulatePhase) {
            maybeDropDataCheckTables();
        }

        indexNamesSet = Collections.synchronizedSet(new HashSet<>());

        String newParams = "NEW PARAMS:" +
                           "\ndcTableConfig: " +
                           ((this.dcTableLimits == null) ? "null" :
                            this.dcTableLimits.toString()) +
                           "\nuseDcTableId: " +
                           this.useDcTableId +
                           "\ndropCloudTableBefore: " +
                           this.dropCloudTableBefore +
                           "\ndropCloudTableAfter: " +
                           this.dropCloudTableAfter;
        log(newParams);

        maybeInstallSchema();
        initializeRandom(seed);
    }

    @Override
    protected PhaseThread createPopulateThread(long start,
                                               long count,
                                               double targetThrptMs) {
        return new ProxyPopulateThread(start, count, targetThrptMs);
    }

    class ProxyPopulateThread extends PopulateThread {
        ProxyPopulateThread(long start, long count, double targetThrptMs) {
            super(start, count, targetThrptMs);
        }

        @Override
        protected void executeOps() {
            for (int i = 0; i < operations.size(); i += writeMultipleMax) {
                int end = Math.min(i + writeMultipleMax, operations.size());
                List<Request> batch = operations.subList(i, end);
                runOp(new Op.ExecuteOp<>(DataCheckTableProxy.this,
                                         batch));
                opsDone += batch.size();
                throttle();
            }
            operations.clear();
        }
    }

    @Override
    protected PhaseThread createCleanThread(long start,
                                            long count,
                                            double targetThrptMs) {
        return new ProxyCleanThread(start, count, targetThrptMs);
    }

    class ProxyCleanThread extends CleanThread {
        ProxyCleanThread(long start, long count, double targetThrptMs) {
            super(start, count, targetThrptMs);
        }

        @Override
        protected void executeOps() {
            for (int i = 0; i < operations.size(); i += writeMultipleMax) {
                int end = Math.min(i + writeMultipleMax, operations.size());
                List<Request> batch = operations.subList(i, end);
                runOp(new Op.ExecuteOp<>(DataCheckTableProxy.this,
                                         batch));
                opsDone += batch.size();
                throttle();
            }
            operations.clear();
        }
    }

    /**
     * Generate start and finish timestamp of a phase.
     * Format is ddHHmmss
     */
    @Override
    protected String getTimestamp() {
        return nowDashes()
            .substring(8, 19).replace("-", "").replace(":", "");
    }

    @Override
    public void populate(long start, long count, int threads) {
        try {
            super.populate(start, count, threads);
        } finally {
            nosqlHdl.close();
        }
    }

    /** Print information about whether queries are enabled */
    @Override
    public void exercise(long start,
                         long count,
                         int threads,
                         double readPercent,
                         boolean noCheck) {
        log("Queries " + (enableQueries ? "enabled" : "disabled"));
        try {
            super.exercise(start, count, threads, readPercent, noCheck);
        } finally {
            nosqlHdl.close();
        }
    }

    @Override
    public void check(long start, long count, int threads) {
        try {
            super.check(start, count, threads);
        } finally {
            nosqlHdl.close();
        }
    }

    @Override
    public void clean(long start, long count, int threads) {
        try {
            if (dropCloudTableAfter) {
                long startTime = System.currentTimeMillis();
                boolean passed = false;
                try {
                    maybeDropDataCheckTables();
                    passed = true;
                } catch (Throwable e) {
                    unexpectedExceptionWithContext("During clean phase to " +
                                                   "drop tables", e);
                }
                final long time = System.currentTimeMillis() - startTime;
                StringBuilder sb = new StringBuilder();
                Formatter fmt = new Formatter(sb);
                fmt.format("Result: %s", (passed ? "Passed" : "Failed"));
                fmt.format("\n  timeMillis=%d", time);
                fmt.format("\n  droppedTable=%s", TAB_DATACHECK);
                fmt.format("\n  droppedTable=%s", TAB_DATACHECKMD);
                fmt.close();
                String message = sb.toString();
                log(message);

                if (!passed) {
                    throw new TestFailedException("failed");
                }
            } else {
                super.clean(start, count, threads);
            }
        } finally {
            nosqlHdl.close();
        }
    }

    /**
     * drop table before installing schema on the cloud to avoid
     * excess data stored (like duplicate table entries)
     */
    private void maybeDropDataCheckTables() {
        maybeDropTable(DROP_TABLE_SQL);
        maybeDropTable(DROP_MDTABLE_SQL);
    }

    private void maybeDropTable(String statement) {
        boolean done = false;
        Exception ex = null;
        for (int i = 0; i < 10; i++) {
            try {
                executeDDL(statement);
                log("Executed ddl=" + statement);
                done = true;
                break;
            } catch (Exception e) {
                ex = e;
            }
        }
        if (!done) {
            throw new IllegalStateException(
                "Failed to drop table. Last exception: " + ex, ex);
        }
    }

    @Override
    void executeDDL(String stmt) {

        try {
            final TableRequest tableReq = new TableRequest()
                .setStatement(stmt)
                .setTimeout(DEFAULT_DDL_TIMEOUT_MS);

            State expTableState;
            if (stmt.contains("CREATE TABLE")) {
                expTableState = State.ACTIVE;
                if (proxyType == ProxyType.CLOUD) {
                    if (stmt.contains(TAB_DATACHECKMD)) {
                        tableReq.setTableLimits(
                                 getMdTableLimits(dcTableLimits));
                    } else {
                        tableReq.setTableLimits(dcTableLimits);
                    }
                }
            } else if (stmt.contains("DROP TABLE")) {
                expTableState = State.DROPPED;
            } else {
                expTableState = State.ACTIVE;
            }

            final TableResult tableRes = nosqlHdl.tableRequest(tableReq);
            tableRes.waitForCompletion(nosqlHdl,
                                       DEFAULT_DDL_TIMEOUT_MS,
                                       DELAY_MILLIS);
            final State actTableState = tableRes.getTableState();

            if (actTableState != expTableState) {
                throw new IllegalStateException(
                    "Execution failed, statement=" + stmt +
                    ", expected table state: " + expTableState +
                    ", actual table state: " + actTableState);
            }
        } catch (RequestTimeoutException rte) {
            final String msg = "Timeout in executing ddl=" + stmt +
                               ", timeoutMs=" + DEFAULT_DDL_TIMEOUT_MS;
            log(msg);
            throw new IllegalStateException(msg, rte);
        } catch (NoSQLException nse) {
            final String msg = "Failed to execute ddl=" + stmt +
                               ", error=" + nse;
            log(msg);
            throw new IllegalStateException(msg, nse);
        }
    }

    long getDataCheckTableId(TableResult tableRes) {
        /* prevent crash from "attribute not found" */
        if (!useDcTableId) {
            return 0;
        }
        MapValue tableSchema =
            (MapValue) FieldValue.createFromJson(tableRes.getSchema(), null);
        return tableSchema.getLong("id");
    }

    @Override
    boolean doesTableExist(String tName) {

        try {
            final GetTableRequest gettableReq = new GetTableRequest()
                .setTableName(tName);
            final TableResult tableRes = nosqlHdl.getTable(gettableReq);
            tableRes.waitForCompletion(nosqlHdl,
                                       DEFAULT_DDL_TIMEOUT_MS,
                                       DELAY_MILLIS);
            final State actTableState = tableRes.getTableState();
            final State expTableState = State.ACTIVE;

            if (tName.equalsIgnoreCase(TAB_DATACHECK) &&
                actTableState == State.ACTIVE) {
                dctableId = getDataCheckTableId(tableRes);
            }

            return actTableState == expTableState;
        } catch (NoSQLException nse) {
            return false;
        }
    }

    boolean doesIndexExistInCache(String iName) {
        return indexNamesSet.contains(iName.toLowerCase());
    }

    @Override
    boolean doesIndexExist(String iName) {

        if (doesIndexExistInCache(iName)) {
            return true;
        }

        int retryCnt = DDL_RETRY_COUNT;
        while (retryCnt > 0) {
            try {
                final GetIndexesRequest getindexesReq = new GetIndexesRequest()
                    .setTableName(tableName)
                    .setIndexName(iName);
                final GetIndexesResult getindexesRes =
                    nosqlHdl.getIndexes(getindexesReq);
                final IndexInfo[] indexesInfo = getindexesRes.getIndexes();
                if (indexesInfo.length != 1) {
                    throw new IllegalStateException(
                        "Unexpected number of indexes returned while getting " +
                        "index: " + iName + " of Table: " + tableName +
                        ", returned indexes: " + Arrays.toString(indexesInfo));
                }
                if (!indexesInfo[0].getIndexName().equalsIgnoreCase(iName)) {
                    throw new IllegalStateException(
                        "Unexpected index returned while getting " +
                        "index: " + iName + " of Table: " + tableName +
                        ", returned index name: " +
                        indexesInfo[0].getIndexName());
                }
                indexNamesSet.add(iName.toLowerCase());
                return true;
            } catch (IndexNotFoundException infe) {
                return false;
            } catch (NoSQLException nse) {
                if (nse.okToRetry()) {
                    retryCnt--;
                } else {
                    return false;
                }
            }
        }

        return false;
    }

    /** create empty row */
    private MapValue createRow() {
        return new MapValue();
    }

    /** create empty key */
    private static MapValue createKey() {
        return new MapValue();
    }

    /** create key from row */
    private static MapValue createKey(MapValue row) {
        MapValue key = createKey();
        for (String pkColName : PRIMARY_KEY) {
            key.put(pkColName, row.getString(pkColName));
        }
        return key;
    }

    /** create copy of the specified MapValue */
    private MapValue copyFrom(MapValue keyOrRow) {
        MapValue keyOrRowCopy = createRow();
        keyOrRowCopy.addAll(keyOrRow.iterator());
        return keyOrRowCopy;
    }

    /**
     * return true if the primary key passed as argument is a complete
     * primary key else return false
     */
    private boolean isCompletePrimaryKey(MapValue key) {
        return key.contains(FLD_MAJORKEY1) &&
            key.contains(FLD_MAJORKEY2) &&
            key.contains(FLD_MINORKEY);
    }

    /**
     * return true if the primary key passed as argument is a complete
     * shard primary key else return false
     */
    private boolean isCompleteShardPrimaryKey(MapValue key) {
        return key.contains(FLD_MAJORKEY1) &&
            key.contains(FLD_MAJORKEY2) &&
            !key.contains(FLD_MINORKEY);
    }

    /** return the field name at the specified field position */
    private String getFieldName(int fieldPos) {
        return FIELD_NAMES[fieldPos];
    }

    /** return all the field names as a list of strings */
    private List<String> getFieldNames() {
        return Arrays.asList(FIELD_NAMES);
    }

    /**
     * return the field name of the metadata table at the specified
     * field position
     */
    private String getMDFieldName(int fieldPos) {
        return MD_PRIMARY_KEY[fieldPos];
    }

    /** convert the direct java driver Durability to java sdk Durability */
    private Durability getDurability(Object durability) {

        if (durability == oracle.kv.Durability.COMMIT_SYNC) {
            return Durability.COMMIT_SYNC;
        }

        if (durability == oracle.kv.Durability.COMMIT_WRITE_NO_SYNC) {
            return Durability.COMMIT_WRITE_NO_SYNC;
        }

        return Durability.COMMIT_NO_SYNC;
    }

    /** convert the direct java driver Consistency to java sdk Consistency */
    private Consistency getConsistency(Object consistency) {

        if (consistency == oracle.kv.Consistency.ABSOLUTE) {
            return Consistency.ABSOLUTE;
        }

        return Consistency.EVENTUAL;
    }

    /** Return a row with default value. */
    @Override
    synchronized MapValue getEmptyValue() {
        if (emptyRow == null) {
            emptyRow = createRow();
            emptyRow.put(FLD_MAJORKEY1, "");
            emptyRow.put(FLD_MAJORKEY2, "");
            emptyRow.put(FLD_MINORKEY, "");
            emptyRow.put(FLD_OPERATION, OPERATION_POPULATE);
            emptyRow.put(FLD_FIRSTTHREAD, true);
            emptyRow.put(FLD_INDEX, 0L);
        }
        return emptyRow;
    }

    /**
     * Get the DataCheck MetaData table limits based off of the DataCheck
     * table limits. MetaData read and write units should be about 10% of
     * the original DataCheck table limits.
     */
    private TableLimits getMdTableLimits(TableLimits dcLimits) {
        return new TableLimits(dcLimits.getReadUnits() / 10,
                               dcLimits.getWriteUnits() / 10,
                               DC_MD_STORAGE_GB);
    }

    /**
     * Check if the given value equals to the expected value.
     */
    @Override
    boolean valuesEqual(MapValue value1, MapValue value2) {
        if (value2 == null) {
            return false;
        }
        return value1.equals(value2);
    }

    /* Exercise methods */

    /**
     * Execute a list of operations with the specified timeout, without
     * retrying if FaultException is thrown.
     *
     * @param operations the list of operations
     * @param timeoutMillis the timeout for all the operations
     * @return the operation results
     * @throws IllegalStateException if the operation fails
     */
    @Override
    List<OpResult<MapValue>> execute(List<Request> operations,
                                     long timeoutMillis) {
        final int numOps = operations.size();
        int numputIfAbsentOps = 0;
        boolean abortIfUnsuccessful;
        /*
         * Workaround for KVSTORE-1999: get the existing value using get api
         * since getExistingValue() of PutRequest / DeleteRequest does not
         * return the existing value if put / delete returns success
         */
        List<ValVer<MapValue>> existingValVers = new ArrayList<>();
        final WriteMultipleRequest writeMultipleReq =
            new WriteMultipleRequest().setTimeout((int) timeoutMillis);
        final List<OpResult<MapValue>> results = new ArrayList<>();

        for (Request r : operations) {
            MapValue key;
            if (r instanceof PutRequest) {
                PutRequest putReq = (PutRequest) r;
                if (putReq.getOption() != null) {
                    if (putReq.getOption().equals(Option.IfAbsent)) {
                        numputIfAbsentOps++;
                    }
                }
                key = createKey(putReq.getValue());
            } else {
                DeleteRequest delReq = (DeleteRequest) r;
                key = delReq.getKey();
            }
            ValVer<MapValue> existingValVer = doGet(key, null, timeoutMillis);
            existingValVers.add(existingValVer);
        }

        /*
         * All operations in the operations list are putIfAbsent ops
         * with abortIfUnsuccessful=true in the populate phase
         * and a mix of putIfAbsent, putIfPresent, putIfVersion, delete and
         * deleteIfVersion ops with abortIfUnsuccessful=false in the
         * exercise phase.
         * Unlike the direct java driver case there is no way to set this
         * abortIfUnsuccessful flag in the individual methods viz.
         * getPopulateOpInternal, createPutOp, createPutIfAbsentOp etc.
         * in the java sdk case.
         * So in the case of java sdk use this approach wherein if the
         * total number of ops in the operations list are equals to
         * putIfAbsent ops then this is guaranteed to be populate phase
         * and so set abortIfUnsuccessful to true else set it to false.
         */
        if (numOps == numputIfAbsentOps) {
            abortIfUnsuccessful = true;
        } else {
            abortIfUnsuccessful = false;
        }

        for (Request r : operations) {
            writeMultipleReq.add(r, abortIfUnsuccessful);
        }

        final WriteMultipleResult writeMultipleRes =
            nosqlHdl.writeMultiple(writeMultipleReq);

        int i = 0;
        for (OperationResult r : writeMultipleRes.getResults()) {
            results.add(new OpResultImpl(r, existingValVers.get(i++)));
        }

        return results;
    }

    private static class OpResultImpl implements OpResult<MapValue> {
        private final OperationResult r;
        private final ValVer<MapValue> existingValVer;
        OpResultImpl(OperationResult r, ValVer<MapValue> existingValVer) {
            this.r = r;
            this.existingValVer = existingValVer;
        }
        @Override
        public MapValue getPreviousValue() {
            return existingValVer.getValue();
        }
        @Override
        public boolean getSuccess() {
            return r.getSuccess();
        }
    }

    @Override
    MapValue getPopulateValueTableInternal(long index,
                                           long keynum,
                                           long ttlDays) {
        MapValue row = createRowFromKeynum(keynum);
        row.put(FLD_OPERATION, OPERATION_POPULATE);
        row.put(FLD_FIRSTTHREAD, false);
        row.put(FLD_INDEX, index);
        /*
         * TODO: How to set the row level ttl.
         * 1) In direct java driver the row level ttl is set in this method.
         * 2) In java sdk the row level ttl cannot be set in this method
         *    as the same need to be done in PutRequest.
         */
        return row;
    }

    @Override
    MapValue getExerciseValueTableInternal(long index,
                                           boolean firstThread,
                                           long keynum) {
        MapValue row = createRowFromKeynum(keynum);
        row.put(FLD_OPERATION, OPERATION_EXERCISE);
        row.put(FLD_FIRSTTHREAD, firstThread);
        row.put(FLD_INDEX, index);
        return row;
    }

    private MapValue createRowFromKeynum(long keynum) {
        return keynumToKey(keynum);
    }

    /** Get the index key for index idxIndex. */
    MapValue getIndexKey(long idx) {
        if (!doesIndexExist(IDX_INDEX)) {
            throw new IllegalStateException(
                "Index: " + IDX_INDEX + " of Table: " + tableName +
                " does not exist");
        }
        return createKey().put(FLD_INDEX, idx);
    }

    @Override
    MapValue keynumToKeyTableInternal(Key key) {
        return keyToPrimaryKey(key);
    }

    @Override
    MapValue keynumToMajorKeyTableInternal(Key key) {
        return keyToPrimaryKey(key);
    }

    @Override
    MapValue keynumToParentKeyTableInternal(Key key) {
        return keyToPrimaryKey(key);
    }

    /**Convert a primaryKey to a keynum, returning -1 if the key is not valid.*/
    @Override
    long keyToKeynum(MapValue pKey) {
        return keyToKeynumInternal(primaryKeyToKey(pKey));
    }

    /**
     * Checks if the key, along with it's associated keynum, is in the
     * partitions specified for this test.
     */
    @Override
    boolean keyInPartitions(MapValue pKey, long keynum) {
        final Key key = primaryKeyToInternalKey(pKey);
        int partitionId =
            partitionMap.getPartitionId(key.toByteArray()).getPartitionId();
        return isPartitionInPartitionSet(key, keynum, partitionId);
    }

    /** Returns the partition ID, if known, else -1. */
    @Override
    int getPartitionId(MapValue pKey) {
        final Key key = primaryKeyToInternalKey(pKey);
        return partitionMap.getPartitionId(key.toByteArray()).getPartitionId();
    }

    /** Convert primaryKey to internal KV key. */
    private Key primaryKeyToInternalKey(MapValue pKey) {
        final ArrayList<String> shardKeys = new ArrayList<String>();
        shardKeys.add(TableImpl.createIdString(dctableId));
        shardKeys.add(pKey.getString(FLD_MAJORKEY1));
        shardKeys.add(pKey.getString(FLD_MAJORKEY2));
        return Key.createKey(shardKeys);
    }

    /** Convert primaryKey to KV key. */
    private Key primaryKeyToKey(MapValue pKey) {
        List<String> majorPath = new ArrayList<>();
        List<String> minorPath = new ArrayList<>();
        int nMajor = SHARD_KEY.length;
        for (int i = 0; i < PRIMARY_KEY.length; i++) {
            final String path = pKey.getString(PRIMARY_KEY[i]);
            if (i < nMajor) {
                majorPath.add(path);
            } else {
                minorPath.add(path);
            }
        }
        return Key.createKey(majorPath, minorPath);
    }

    /** Convert KV key to primaryKey. */
    private MapValue keyToPrimaryKey(Key key) {
        MapValue pKey = createKey();
        int keySize = key.getFullPath().size();
        int idxKey = 0;
        if (PRIMARY_KEY.length < keySize) {
            throw new IllegalStateException(
                "The number of key's component is greater than " +
                "the number of fields of the primary key.");
        }
        int nPath = Math.min(PRIMARY_KEY.length, keySize);
        for(int i = 0; i < nPath; i++, idxKey++) {
            pKey.put(PRIMARY_KEY[i],
                     key.getFullPath().get(idxKey));
        }
        return pKey;
    }

    @Override
    String[] valueStringArray(MapValue row) {
        String[] allvaluesList = new String[row.size()];
        allvaluesList[0] = row.getString(getFieldName(0));
        allvaluesList[1] = row.getString(getFieldName(1));
        allvaluesList[2] = row.getString(getFieldName(2));
        allvaluesList[3] = row.getString(getFieldName(3));
        allvaluesList[4] = "" + row.getBoolean(getFieldName(4));
        allvaluesList[5] = "" + row.getLong(getFieldName(5));
        return allvaluesList;
    }

    @Override
    String[] keyStringArray(MapValue pKey) {
        String[] allkeysList = new String[pKey.size()];
        allkeysList[0] = pKey.getString(getFieldName(0));
        allkeysList[1] = pKey.getString(getFieldName(1));
        allkeysList[2] = pKey.getString(getFieldName(2));
        return allkeysList;
    }

    /** Get the length of the internal key for the given primary key.*/
    @Override
    int keyLength(MapValue pKey) {
        return pKey.getSerializedSize();
    }

    /** Get the length of the internal value for the given row.*/
    @Override
    int valueLength(MapValue row) {
        return row.getSerializedSize();
    }

    /* Threads */

    @Override
    Request getPopulateOpInternal(MapValue value) {
        return new PutRequest()
            .setTableName(TAB_DATACHECK)
            .setValue(value)
            .setReturnRow(false)
            .setOption(Option.IfAbsent);
    }

    @Override
    Op<MapValue, MapValue, Request> getCheckOp(long index) {
        return new Op.MultiGetIteratorCheckOp<>(this, index);
    }

    @Override
    Request getCleanUpOpInternal(MapValue key) {
        return new DeleteRequest()
            .setTableName(TAB_DATACHECK)
            .setKey(key)
            .setReturnRow(false);
    }

    /* Operations */

    @Override
    ValVer<MapValue> doGet(MapValue key,
                           Object consistency,
                           long timeoutMillis) {
        GetRequest getReq = new GetRequest()
            .setTableName(TAB_DATACHECK)
            .setKey(key)
            .setConsistency(getConsistency(consistency))
            .setTimeout((int) timeoutMillis);
        GetResult getRes = nosqlHdl.get(getReq);

        return new ValVerImpl(getRes.getValue(), getRes.getVersion());
    }

    private static class ValVerImpl implements ValVer<MapValue> {
        private final MapValue row;
        private final Version version;
        private final boolean result;
        ValVerImpl(MapValue row, Version version) {
            this(row, version, true);
        }
        ValVerImpl(MapValue row, Version version, boolean result) {
            this.row = row;
            this.version = version;
            this.result = result;
        }
        @Override
        public MapValue getValue() {
            if (row == null || row.size() == 0) {
                return null;
            }
            return row;
        }
        @Override
        public Object getVersion() {
            return (row != null) ? version : null;
        }
        @Override
        public boolean getSuccess() {
            return result;
        }
    }

    @Override
    Collection<MapValue> doMultiGet(MapValue key,
                                    Object consistency,
                                    long timeoutMillis) {
        try (final QueryRequest queryReq = new QueryRequest()) {
            queryReq
                .setPreparedStatement(getMultiGetPreparedStatement(key))
                .setConsistency(getConsistency(consistency))
                .setTimeout((int) timeoutMillis);
            final QueryIterableResult queryiterRes =
                nosqlHdl.queryIterable(queryReq);
            final ArrayList<MapValue> resultSet = new ArrayList<>();
            queryiterRes.forEach(resultSet::add);
            return resultSet;
        }
    }

    private PreparedStatement getMultiGetPreparedStatement(MapValue key) {
        if (isCompletePrimaryKey(key)) {
            return getQueryPreparedStatement(COMPLETE_KEY_MULTI_GET_STMT)
                .setVariable("$v1", stringValue(key, FLD_MAJORKEY1))
                .setVariable("$v2", stringValue(key, FLD_MAJORKEY2))
                .setVariable("$v3", stringValue(key, FLD_MINORKEY));
        } else if (isCompleteShardPrimaryKey(key)) {
            return getQueryPreparedStatement(COMPLETE_SHARD_KEY_MULTI_GET_STMT)
                .setVariable("$v1", stringValue(key, FLD_MAJORKEY1))
                .setVariable("$v2", stringValue(key, FLD_MAJORKEY2));
        } else {
            throw new IllegalArgumentException(
                "multiGet does not support incomplete shard primary key");
        }
    }

    @Override
    Collection<MapValue> doMultiGetKeys(MapValue key,
                                        Object consistency,
                                        long timeoutMillis) {
        throw new UnsupportedOperationException(
            "DataCheckTableProxy.doMultiGetKeys is not supported");
    }

    @Override
    Iterator<KeyVal<MapValue, MapValue>> doMultiGetIterator(
        MapValue key, Object consistency, long timeoutMillis)
    {
        return new KeyValIter(doMultiGet(key, consistency, timeoutMillis));
    }

    private static class KeyValIter
            implements Iterator<KeyVal<MapValue, MapValue>> {
        private final Collection<MapValue> collection;
        private final Iterator<MapValue> iter;
        KeyValIter(Collection<MapValue> collection) {
            this.collection = collection;
            iter = collection.iterator();
        }
        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }
        @Override
        public KeyVal<MapValue, MapValue> next() {
            return new KeyValImpl(iter.next());
        }
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
        /** Display the contents of the iteration, for debugging. */
        @Override
        public String toString() {
            return "KeyValIter{count=" + collection.size() +
                " items=" + collection + "}";
        }
    }

    private static class KeyValImpl implements KeyVal<MapValue, MapValue> {
        private final MapValue row;
        KeyValImpl(MapValue row) {
            this.row = row;
        }
        @Override
        public MapValue getKey() {
            return createKey(row);
        }
        @Override
        public MapValue getValue() {
            return row;
        }
    }

    @Override
    Iterator<MapValue> doMultiGetKeysIterator(MapValue key,
                                              Object consistency,
                                              long timeoutMillis) {
        throw new UnsupportedOperationException(
            "DataCheckTableProxy.doMultiGetKeysIterator is not supported");
    }

    @Override
    ValVer<MapValue> doPut(MapValue key,
                           MapValue value,
                           boolean requestPrevValue,
                           long timeoutMillis) {
        /*
         * Workaround for KVSTORE-1999: get the existing value using get api
         * since PutResult.getExistingValue() does not return the existing
         * value if put returns success
         */
        ValVer<MapValue> existingValVer = doGet(key, null, timeoutMillis);
        final PutRequest putReq = new PutRequest()
            .setTableName(TAB_DATACHECK)
            .setValue(value)
            .setReturnRow(requestPrevValue)
            .setTimeout((int) timeoutMillis);
        nosqlHdl.put(putReq);
        return new ValVerImpl(existingValVer.getValue(),
                              (Version) existingValVer.getVersion());
    }

    @Override
    Request createPutOp(MapValue key,
                        MapValue value,
                        boolean requestPrevValue) {
        return new PutRequest()
            .setTableName(TAB_DATACHECK)
            .setValue(value)
            .setReturnRow(requestPrevValue);
    }

    @Override
    ValVer<MapValue> doPutIfAbsent(MapValue key,
                                   MapValue value,
                                   boolean requestPrevValue,
                                   long timeoutMillis) {
        /*
         * Workaround for KVSTORE-1999: get the existing value using get api
         * since PutResult.getExistingValue() does not return the existing
         * value if put returns success
         */
        ValVer<MapValue> existingValVer = doGet(key, null, timeoutMillis);
        final PutRequest putReq = new PutRequest()
            .setTableName(TAB_DATACHECK)
            .setValue(value)
            .setReturnRow(requestPrevValue)
            .setOption(Option.IfAbsent)
            .setTimeout((int) timeoutMillis);
        final PutResult putRes = nosqlHdl.put(putReq);
        Version v = putRes.getVersion();
        return new ValVerImpl(existingValVer.getValue(),
                              (Version) existingValVer.getVersion(),
                              (v != null));
    }

    @Override
    Request createPutIfAbsentOp(MapValue key,
                                MapValue value,
                                boolean requestPrevValue) {
        return new PutRequest()
            .setTableName(TAB_DATACHECK)
            .setValue(value)
            .setReturnRow(requestPrevValue)
            .setOption(Option.IfAbsent);
    }

    @Override
    ValVer<MapValue> doPutIfPresent(MapValue key,
                                    MapValue value,
                                    boolean requestPrevValue,
                                    long timeoutMillis) {
        /*
         * Workaround for KVSTORE-1999: get the existing value using get api
         * since PutResult.getExistingValue() does not return the existing
         * value if put returns success
         */
        ValVer<MapValue> existingValVer = doGet(key, null, timeoutMillis);
        final PutRequest putReq = new PutRequest()
            .setTableName(TAB_DATACHECK)
            .setValue(value)
            .setReturnRow(requestPrevValue)
            .setOption(Option.IfPresent)
            .setTimeout((int) timeoutMillis);
        final PutResult putRes = nosqlHdl.put(putReq);
        final Version v = putRes.getVersion();
        return new ValVerImpl(existingValVer.getValue(),
                              (Version) existingValVer.getVersion(),
                              (v != null));
    }

    @Override
    Request createPutIfPresentOp(MapValue key,
                                 MapValue value,
                                 boolean requestPrevValue) {
        MapValue row;
        MapValue valuekey = createKey(value);
        if (!valuekey.equals(key)) {
            row = copyFrom(value);
            copyPrimaryKey(row, key);
        } else {
            row = value;
        }
        return new PutRequest()
            .setTableName(TAB_DATACHECK)
            .setValue(row)
            .setReturnRow(requestPrevValue)
            .setOption(Option.IfPresent);
    }

    @Override
    ValVer<MapValue> doPutIfVersion(MapValue key,
                                    MapValue value,
                                    Object version,
                                    boolean requestPrevValue,
                                    long timeoutMillis) {
        /*
         * Workaround for KVSTORE-1999: get the existing value using get api
         * since PutResult.getExistingValue() does not return the existing
         * value if put returns success
         */
        ValVer<MapValue> existingValVer = doGet(key, null, timeoutMillis);
        final PutRequest putReq = new PutRequest()
            .setTableName(TAB_DATACHECK)
            .setValue(value)
            .setReturnRow(requestPrevValue)
            .setOption(Option.IfVersion)
            .setMatchVersion((Version) version)
            .setTimeout((int) timeoutMillis);
        final PutResult putRes = nosqlHdl.put(putReq);
        final Version v = putRes.getVersion();
        return new ValVerImpl(existingValVer.getValue(),
                              (Version) existingValVer.getVersion(),
                              (v != null));
    }

    @Override
    Request createPutIfVersionOp(MapValue key,
                                 MapValue value,
                                 Object version,
                                 boolean requestPrevValue) {
        return new PutRequest()
            .setTableName(TAB_DATACHECK)
            .setValue(value)
            .setReturnRow(requestPrevValue)
            .setOption(Option.IfVersion)
            .setMatchVersion((Version) version);
    }

    @Override
    boolean doBulkPut(MapValue key, MapValue value, long timeoutMillis) {
        throw new IllegalStateException(
            "DataCheckTableProxy.doBulkPut is not supported");
    }

    @Override
    ValVer<MapValue> doDelete(MapValue key,
                              boolean requestPrevValue,
                              long timeoutMillis) {
        /*
         * Workaround for KVSTORE-1999: get the existing value using get api
         * since DeleteResult.getExistingValue() does not return the existing
         * value if put returns success
         */
        ValVer<MapValue> existingValVer = doGet(key, null, timeoutMillis);
        final DeleteRequest delReq = new DeleteRequest()
            .setTableName(TAB_DATACHECK)
            .setKey(key)
            .setReturnRow(requestPrevValue)
            .setTimeout((int) timeoutMillis);
        final DeleteResult delRes = nosqlHdl.delete(delReq);
        final boolean exist = delRes.getSuccess();
        return new ValVerImpl(existingValVer.getValue(),
                              (Version) existingValVer.getVersion(),
                              exist);
    }

    @Override
    Request createDeleteOp(MapValue key, boolean requestPrevValue) {
        return new DeleteRequest()
            .setTableName(TAB_DATACHECK)
            .setKey(key)
            .setReturnRow(requestPrevValue);
    }

    @Override
    int doMultiDelete(MapValue key, long timeoutMillis) {
        final MultiDeleteRequest multidelReq = new MultiDeleteRequest()
            .setTableName(TAB_DATACHECK)
            .setKey(key)
            .setTimeout((int) timeoutMillis);
        final MultiDeleteResult multidelRes = nosqlHdl.multiDelete(multidelReq);
        return multidelRes.getNumDeletions();
    }

    @Override
    ValVer<MapValue> doDeleteIfVersion(MapValue key,
                                       Object version,
                                       boolean requestPrevValue,
                                       long timeoutMillis) {
        /*
         * Workaround for KVSTORE-1999: get the existing value using get api
         * since DeleteResult.getExistingValue() does not return the existing
         * value if put returns success
         */
        ValVer<MapValue> existingValVer = doGet(key, null, timeoutMillis);
        final DeleteRequest delReq = new DeleteRequest()
            .setTableName(TAB_DATACHECK)
            .setKey(key)
            .setReturnRow(requestPrevValue)
            .setMatchVersion((Version) version)
            .setTimeout((int) timeoutMillis);
        nosqlHdl.delete(delReq);
        return new ValVerImpl(existingValVer.getValue(),
                              (Version) existingValVer.getVersion());
    }

    @Override
    Request createDeleteIfVersionOp(MapValue key,
                                    Object version,
                                    boolean requestPrevValue) {
        return new DeleteRequest()
            .setTableName(TAB_DATACHECK)
            .setKey(key)
            .setReturnRow(requestPrevValue)
            .setMatchVersion((Version) version);
    }

    @Override
    MapValue appendStringToKey(MapValue key, String x) {
        MapValue retKey = copyFrom(key);
        String fname = PRIMARY_KEY[PRIMARY_KEY.length - 1];
        String minorKey = key.getString(fname);
        retKey.put(fname, minorKey.concat(x));
        return retKey;
    }

    @Override
    void doIndexIteratorOp(ReadOp<MapValue, MapValue, Request> readOp,
                           long index,
                           long timeoutMillis) {
        final MapValue key = getIndexKey(index);
        for (final MapValue row : doIndexIterator(key, null, timeoutMillis)) {
            readOp.checkValue(createKey(row), row);
        }
    }

    /** Perform IndexIterator operation. */
    private List<MapValue> doIndexIterator(MapValue key,
                                           Consistency consistency,
                                           long timeoutMillis) {
        final PreparedStatement prepStmt =
            getQueryPreparedStatement(INDEX_ITERATOR_STMT)
            .setVariable("$v1", new LongValue(key.getLong(FLD_INDEX)));
        try (final QueryRequest queryReq = new QueryRequest()) {
            queryReq
                .setPreparedStatement(prepStmt)
                .setConsistency(consistency)
                .setTimeout((int) timeoutMillis);
            final ArrayList<MapValue> resultSet = new ArrayList<>();
            nosqlHdl.queryIterable(queryReq).forEach(resultSet::add);
            return resultSet;
        }
    }

    @Override
    void doIndexKeysIteratorOp(ReadOp<MapValue, MapValue, Request> readOp,
                               long index,
                               long timeoutMillis) {
        final MapValue key = getIndexKey(index);
        for (final MapValue row :
                 doIndexKeysIterator(key, null, timeoutMillis)) {
            readOp.checkKeyPresent(createKey(row));
        }
    }

    /** Perform IndexKeysIterator operation. */
    private List<MapValue> doIndexKeysIterator(MapValue key,
                                               Consistency consistency,
                                               long timeoutMillis) {
        final PreparedStatement prepStmt =
            getQueryPreparedStatement(INDEX_KEYS_ITERATOR_STMT)
            .setVariable("$v1", new LongValue(key.getLong(FLD_INDEX)));
        try (final QueryRequest queryReq = new QueryRequest()) {
            queryReq
                .setPreparedStatement(prepStmt)
                .setConsistency(consistency)
                .setTimeout((int) timeoutMillis);
            final ArrayList<MapValue> resultSet = new ArrayList<>();
            nosqlHdl.queryIterable(queryReq).forEach(resultSet::add);
            return resultSet;
        }
    }

    private StringValue stringValue(MapValue key, String fieldName) {
        return new StringValue(key.getString(fieldName));
    }

    @Override
    PreparedStatement getQueryPreparedStatement(String query) {
        return preparedStatements.computeIfAbsent(query,
                                                  this::getPreparedStatement)
            .copyStatement();
    }

    private PreparedStatement getPreparedStatement(String query) {
        final PrepareRequest request =
            new PrepareRequest().setStatement(query);
        return nosqlHdl.prepare(request).getPreparedStatement();
    }

    @Override
    void doCompletePrimaryKeyQueryOp(
        ReadOp<MapValue, MapValue, Request> readOp,
        long keynum,
        long timeoutMillis)
    {
        final MapValue key = keynumToKey(keynum);
        final PreparedStatement prepStmt =
            getQueryPreparedStatement(COMPLETE_PRIMARY_KEY_QUERY_STMT)
            .setVariable("$v0", stringValue(key, FLD_MAJORKEY1))
            .setVariable("$v1", stringValue(key, FLD_MAJORKEY2))
            .setVariable("$v2", stringValue(key, FLD_MINORKEY));
        try (final QueryRequest queryReq = new QueryRequest()) {
            queryReq
                .setPreparedStatement(prepStmt)
                .setTimeout((int) timeoutMillis);
            final Iterator<MapValue> iter =
                nosqlHdl.queryIterable(queryReq).iterator();
            final MapValue row = iter.hasNext() ? iter.next() : null;
            readOp.checkValue(key, row);
        }
    }

    @Override
    void doCompleteShardPrimaryKeyQueryOp(
        ReadOp<MapValue, MapValue, Request> readOp,
        long keynum,
        long timeoutMillis)
    {
        final MapValue completeKey = keynumToKey(keynum);
        final PreparedStatement prepStmt =
            getQueryPreparedStatement(COMPLETE_SHARD_PRIMARY_KEY_QUERY_STMT)
            .setVariable("$v0", stringValue(completeKey, FLD_MAJORKEY1))
            .setVariable("$v1", stringValue(completeKey, FLD_MAJORKEY2))
            .setVariable("$v2", stringValue(completeKey, FLD_MINORKEY));
        try (final QueryRequest queryReq = new QueryRequest()) {
            queryReq
                .setPreparedStatement(prepStmt)
                .setTimeout((int) timeoutMillis);
            MapValue foundRow = null;
            for (final MapValue recordValue :
                     nosqlHdl.queryIterable(queryReq)) {
                final MapValue key = createKey(recordValue);
                if (completeKey.equals(key)) {
                    foundRow = recordValue;
                    break;
                }
            }
            readOp.checkValue(completeKey, foundRow);
        }
    }

    @Override
    void doPartialShardPrimaryKeyQueryOp(
        ReadOp<MapValue, MapValue, Request> readOp,
        long keynum,
        long timeoutMillis)
    {
        final MapValue completeKey = keynumToKey(keynum);
        final PreparedStatement prepStmt =
            getQueryPreparedStatement(PARTIAL_SHARD_PRIMARY_KEY_QUERY_STMT)
            .setVariable("$v0", stringValue(completeKey, FLD_MAJORKEY1))
            .setVariable("$v1", stringValue(completeKey, FLD_MAJORKEY2))
            .setVariable("$v2", stringValue(completeKey, FLD_MINORKEY));
        try (final QueryRequest queryReq = new QueryRequest()) {
            queryReq
                .setPreparedStatement(prepStmt)
                .setTimeout((int) timeoutMillis);
            MapValue foundRow = null;
            for (final MapValue recordValue :
                     nosqlHdl.queryIterable(queryReq)) {
                final MapValue key = createKey(recordValue);
                if (completeKey.equals(key)) {
                    foundRow = recordValue;
                    break;
                }
            }
            readOp.checkValue(completeKey, foundRow);
        }
    }

    @Override
    void doSecondaryKeyQueryOp(SecondaryKeyQueryOp readOp,
                               long timeoutMillis) {
        final PreparedStatement prepared =
            getQueryPreparedStatement(SECONDARY_KEY_QUERY_STMT)
                .setVariable("$v0", new LongValue(readOp.index));
        try (final QueryRequest queryReq = new QueryRequest()) {
            queryReq
                .setPreparedStatement(prepared)
                .setTimeout((int) timeoutMillis);
            for (final MapValue recordValue :
                nosqlHdl.queryIterable(queryReq)) {
                readOp.checkValue(createKey(recordValue), recordValue);
            }
        }
    }

    /*
     * Query update operations
     */

    @Override
    MapValue doQueryOp(Object boundStmt,
                       long timeoutMillis,
                       boolean requestPrevValue,
                       QueryOp queryOp) {
        try (final QueryRequest queryReq = new QueryRequest()) {
            queryReq
                .setPreparedStatement((PreparedStatement) boundStmt)
                .setTimeout((int) timeoutMillis);
            final Iterator<MapValue> iter =
                nosqlHdl.queryIterable(queryReq).iterator();
            if (!iter.hasNext()) {
                return null;
            }
            final MapValue recordValue = iter.next();
            if (requestPrevValue) {
                return copyFrom(recordValue);
            } else if (queryOp.getReturnCount(recordValue) > 0) {
                return getEmptyValue();
            } else {
                return null;
            }
        }
    }

    @Override
    Object getBoundStatement(Object prepared, MapValue row) {
        for (int i = 0; i < row.size(); i++) {
            ((PreparedStatement) prepared)
                .setVariable("$v" + i, row.get(getFieldName(i)));
        }
        return prepared;
    }

    @Override
    Map<CreateReadOp<MapValue, MapValue, Request>, Integer>
        getReadOpCosts()
    {
        final Map<CreateReadOp<MapValue, MapValue, Request>, Integer>
            map = new HashMap<>();

        /* Make regular reads the cheapest */
        map.put(withDC(ReadOp.GetOp::new), 1);
        map.put(withDC(ReadOp.MultiGetOp::new), 1);

        if (enableQueries) {

            /*
             * Complete primary and shard key queries are cheap because they
             * are handled by a single RN
             */
            map.put(CompletePrimaryKeyQueryOp::new, 2);
            map.put(CompleteShardPrimaryKeyQueryOp::new, 2);

            /*
             * Secondary index queries are more expensive because they need to
             * consult each shard, although the actual cost depends on the
             * number of shards
             */
            map.put(SecondaryKeyQueryOp::new, 10);

            /* Partial primary key queries are very expensive */
            map.put(PartialShardPrimaryKeyQueryOp::new, 250);
        }

        return map;
    }

    @Override
    long getInsertReturnCount(Object recordValue) {
        return ((MapValue) recordValue).getInt("NumRowsInserted");
    }

    @Override
    int getRowSize(MapValue row) {
        return row.size();
    }

    @Override
    long getUpdateReturnCount(Object recordValue) {
        return ((MapValue) recordValue).getInt("NumRowsUpdated");
    }

    @Override
    long getDeleteReturnCount(Object recordValue) {
        return ((MapValue) recordValue).getLong("numRowsDeleted");
    }

    @Override
    List <String> getFieldNames(MapValue row) {
        return getFieldNames();
    }

    @Override
    String getFieldName(MapValue row, int fieldPos) {
        return getFieldName(fieldPos);
    }

    @Override
    protected void updateTTLTestHook(Phase phase, long start, long count) {
        /* TTL is not yet supported in datacheck proxy mode */
    }

    /*
     * Copy the field values from primary key to target row's fields
     */
    private void copyPrimaryKey(MapValue row, MapValue key) {
        for (String fieldName : getFieldNames()) {
            FieldValue val = key.get(fieldName);
            if (val != null) {
                row.put(fieldName, val);
            }
        }
    }

    @Override
    MapValue getRow(Key key, Value value) {
        final MapValue r = keyToMDRow(key);

        /*
         * Store the value if represents a random seed, and store nothing
         * if the operation represents EMPTY_VALUE.
         */
        if (value.getValue().length > 0) {
            final long seed = dataSerializer.valueToSeed(value);
            r.put(FLD_MDVALUE, seed);
        }

        return r;
    }

    @Override
    void runFaultingPutTable(MapValue row, Object durability,
        long timeoutMillis) {
        final PutRequest putReq = new PutRequest()
            .setTableName(TAB_DATACHECKMD)
            .setValue(row)
            .setDurability(getDurability(durability))
            .setTimeout((int) timeoutMillis);
        nosqlHdl.put(putReq);

        final MapValue pKey = createKey();
        final int npKeys = MD_PRIMARY_KEY.length;
        for (int i = 0; i < npKeys; i++) {
            pKey.put(getMDFieldName(i), row.get(getMDFieldName(i)));
        }
        tallyPut(pKey, row);
    }

    @Override
    Set<Key> callFaultingMultiGetKeysTable(MapValue pKey,
                                           long timeoutMillis) {
        final PreparedStatement prepStmt =
            getQueryPreparedStatement(MULTI_GET_KEYS_TABLE_STMT)
            .setVariable("$v1", stringValue(pKey, FLD_MDMAJORKEY1))
            .setVariable("$v2", stringValue(pKey, FLD_MDMAJORKEY2))
            .setVariable("$v3", stringValue(pKey, FLD_MDMAJORKEY3));
        try (final QueryRequest queryReq = new QueryRequest()) {
            queryReq
                .setPreparedStatement(prepStmt)
                .setConsistency(Consistency.ABSOLUTE)
                .setTimeout((int) timeoutMillis);
            final SortedSet<Key> keys = new TreeSet<>();
            for (final MapValue pk : nosqlHdl.queryIterable(queryReq)) {
                keys.add(mdPrimaryKeyToKey(pk));
            }
            return keys;
        }
    }

    @Override
    void runFaultingMultiDeleteTable(MapValue pKey,
                                     Object durability,
                                     long timeoutMillis) {
        final MultiDeleteRequest multidelReq = new MultiDeleteRequest()
            .setTableName(TAB_DATACHECKMD)
            .setKey(pKey)
            .setDurability(getDurability(durability))
            .setTimeout((int) timeoutMillis);
        nosqlHdl.multiDelete(multidelReq);
    }

    @Override
    ValueVersion callFaultingGetTable(MapValue pKey, long timeoutMillis) {
        final GetRequest getReq = new GetRequest()
            .setTableName(TAB_DATACHECKMD)
            .setKey(pKey)
            .setConsistency(Consistency.ABSOLUTE)
            .setTimeout((int) timeoutMillis);
        final GetResult getRes = nosqlHdl.get(getReq);
        final MapValue row = getRes.getValue();

        /* Return null if the entry was not found */
        if (row == null) {
            return null;
        }

        /*
         * Otherwise, treat the value as a long. The caller will only use
         * the value if it represents a random seed
         */
        final long seed = row.getLong(FLD_MDVALUE);
        return new ValueVersion(dataSerializer.seedToValue(seed), null);
    }

    MapValue keyToMDRow(Key key) {
        final MapValue row = createRow();
        addMDKeyFields(key, row, false);
        return row;
    }

    @Override
    MapValue keyToMDPrimaryKey(Key key, boolean skipMinor) {
        final MapValue pKey = createKey();
        addMDKeyFields(key, pKey, skipMinor);
        return pKey;
    }

    /**
     * Adds major and minor components from the key to the row, which may
     * actually be a primary key.
     * Skips the first major key component ("m") since that component is used
     * to mark all metadata entries.
     * Puts the remaining major key components, up to a maximum of 3, into the
     * three major key columns viz. mjkey1, mjkey2 and mjkey3 which also form
     * a composite shard key. If any major key component is not expected (see
     * below for the various key formats) then put an empty string for that
     * component as null values are not allowed by all api's expecting a
     * partial or full primary key.
     *
     * The key representing the seed /m/seed is stored as follows:
     * mjkey1      mjkey2    mjkey3    mikey                           value
     * =====================================================================
     * "seed"      ""        ""        ""                              seed
     *
     * The keys representing UPDATE_HISTORY /m/updates are stored as follows:
     * mjkey1      mjkey2    mjkey3    mikey                           value
     * =====================================================================
     * "updates"   ""        ""        ts-<phase>-<start>-<count>
     * "updates"   ""        ""        ts-<phase>-passed/failed-<start>-<count>
     *
     * The keys representing start of a phase block have the following
     * structure:
     * - /m/populateBlock/<parent>/-/<timestamp>
     * - /m/exerciseBlock/A/<parent>/-/<timestamp>
     * - /m/exerciseBlock/B/<parent>/-/<timestamp>
     * - /m/cleanBlock/<parent>/-/<timestamp>
     * - /m/cleaned-populateBlock/<parent>/-/<timestamp>
     * - /m/cleaned-exerciseBlock/A/<parent>/-/<timestamp>
     * - /m/cleaned-exerciseBlock/B/<parent>/-/<timestamp>
     * and are stored as follows:
     * mjkey1                   mjkey2    mjkey3    mikey
     * =====================================================================
     * "populateBlock"          <parent>  ""        <timestamp>
     * "exerciseBlock"          "A"       <parent>  <timestamp>
     * "exerciseBlock"          "B"       <parent>  <timestamp>
     * "cleanBlock"             <parent>  ""        <timestamp>
     * "cleaned-populateBlock"  <parent>  ""        <timestamp>
     * "cleaned-exerciseBlock"  "A"       <parent>  <timestamp>
     * "cleaned-exerciseBlock"  "B"       <parent>  <timestamp>
     *
     * If skipMinor is false then puts a single minor key component, if
     * present, into the minor key field, else puts an empty string. In other
     * words if skipMinor is true then a partial primary key is constructed
     * by populating values only in the composite shard key columns
     * (mjkey1, mjkey2, mjkey3). This is for multi key api's like multiGetKeys
     * and multiDelete. If skipMinor is false then a full primary key is
     * constructed by populating values in all primary key components
     * ((mjkey1, mjkey2, mjkey3), mikey). This is for single key api's like get.
     *
     * Throws an IllegalArgumentException if there are fewer than 2 or more
     * than 4 major key components, or if there is more than 1 minor key
     * component.
     */
    private void addMDKeyFields(Key key, MapValue row, boolean skipMinor) {
        final List<String> majorKeys =  key.getMajorPath();

        if (majorKeys.size() < 2 || majorKeys.size() > 4) {
            throw new IllegalArgumentException(
                "Incorrect number of major keys: " + majorKeys);
        }

        row.put(FLD_MDMAJORKEY1, majorKeys.get(1));
        row.put(FLD_MDMAJORKEY2,
                majorKeys.size() > 2 ? majorKeys.get(2) : "");
        row.put(FLD_MDMAJORKEY3,
                majorKeys.size() > 3 ? majorKeys.get(3) : "");

        if (!skipMinor) {
            final List<String> minorKeys =  key.getMinorPath();

            if (minorKeys.size() > 1) {
                throw new IllegalArgumentException(
                    "Incorrect number of minor keys: " + minorKeys);
            }

            row.put(FLD_MDMINORKEY,
                    minorKeys.size() > 0 ? minorKeys.get(0) : "");
        }
    }

    /**
     * converts a metadata table primary key to a Key by reversing the behavior
     * of addMDKeyFields.
     */
    Key mdPrimaryKeyToKey(MapValue pKey) {
        final int npKeys = MD_PRIMARY_KEY.length;
        final List<String> major = new ArrayList<>();

        /*
         * skip columns with empty strings to support keys of the following
         * format
         * - /m/populateBlock/<parent key>
         * - /m/cleanBlock/<parent key>
         * - /m/cleaned-populateBlock/<parent key>
         * See addMDKeyFields method's comments section for details on the
         * various metadata key formats
         */
        major.add("m");
        for (int i = 0; i < npKeys; i++) {
            final String majorVal = pKey.getString(getMDFieldName(i));
            if (majorVal.isEmpty()) {
                continue;
            }
            major.add(majorVal);
        }

        final String minorVal = pKey.getString(FLD_MDMINORKEY);

        return Key.createKey(major, minorVal);
    }

    /* Retry:
     * o RequestTimeoutException which can be signaled even if the
     *   requested amount of time has not elapsed
     * o RetryableException which may succeed on retry
     */
    @Override
    void handleStoreException(RuntimeException re) {
        if (!(re instanceof RequestTimeoutException) &&
            !(re instanceof RetryableException)) {
            throw re;
        }
    }
}
