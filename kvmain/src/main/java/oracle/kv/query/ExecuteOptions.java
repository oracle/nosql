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

package oracle.kv.query;

import static oracle.kv.impl.api.table.TableImpl.validateNamespace;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.GeometryUtils;
import oracle.kv.impl.api.table.Region;
import oracle.kv.impl.query.runtime.ResumeInfo.VirtualScan;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.util.contextlogger.LogContext;
import oracle.kv.table.TableIteratorOptions;

/**
 * Class contains several options for the execution of an SQL statement
 * (DDL or DML). SQL statements are executed via one of the
 * KVStore.execute() methods.
 *
 * @see KVStore#execute(String, ExecuteOptions)
 * @see KVStore#executeSync(String, ExecuteOptions)
 * @see KVStore#executeSync(Statement, ExecuteOptions)
 *
 * @since 4.0
 */
public class ExecuteOptions {

    public static short DRIVER_QUERY_V2 = 2;

    public static short DRIVER_QUERY_V3 = 3;

    public static long MAX_SERVER_MEMORY_CONSUMPTION = 10 * 1024 * 1024;

    private Consistency consistency;

    private Durability durability;

    private long timeout;

    private TimeUnit timeoutUnit;

    private int maxConcurrentRequests;

    private int resultsBatchSize;

    private byte traceLevel;

    private int batchCounter;

    private MathContext mathContext = MathContext.DECIMAL32;

    /* added in 4.4 */
    private String namespace;

    private PrepareCallback prepareCallback;

    /* added in 18.1 */
    private int maxReadKB;

    /* added in 19.1 */
    private int maxWriteKB;

    /*
     * This flag applies to proxy-based queries (for direct-driver queries it
     * is always false). If true, then the resultsBatchSize will be used as
     * a batch limit the same way the byte-based limit is used, that is, if
     * this limit is reached, no more query processing should take place and
     * driver method used to execute queries (e.g, NoSQLHandle.query() for
     * the java driver) must return to the application.
     *
     * Note: This flag is set to true if the application explicitly sets the
     * number-of-results limit. Otherwise, the flag will be false, unless
     * this is an on-prem proxy-based query and the version is >= v21.3.
     * That is, for on-prem proxy-based queries at or after v21.3, this
     * flag is always trur.
     */
    private boolean useBatchSizeAsLimit;

    private AuthContext authContext = null;

    private LogContext logContext = null;

    private boolean doPrefetching = true;

    private long maxClientMemoryConsumption = 100 * 1024 * 1024;

    private long maxServerMemoryConsumption = MAX_SERVER_MEMORY_CONSUMPTION;

    private int geoMaxCoveringCells = GeometryUtils.theMaxCoveringCellsForSearch;

    private int geoMinCoveringCells = GeometryUtils.theMinCoveringCellsForSearch;

    private int geoMaxRanges = GeometryUtils.theMaxRanges;

    private double geoSplitRatio = GeometryUtils.theSplitRatio;

    private int geoMaxSplits = GeometryUtils.theMaxSplits;

    /* For SQL DELETE, this is the max number of rows to delete per RN batch */
    private int deleteLimit = 1000;

    /* For SQL UPDATE, this is the max number of rows to update per RN batch */
    private int updateLimit = 1000;

    /* added in 18.3 */
    private boolean validateNamespace = true;

    /*
     * Should be set to true for proxy-based queries and false for queries
     * submitted via the direct driver (fat java client).
     */
    private boolean isProxyQuery;

    /*
     * If proxy-based query, this is the version of the driver-proxy protocol for
     * query
     */
    private int driverQueryVersion;

    /* For INSERT, the primary key size limit */
    private int maxPrimaryKeySize;

    /* For INSERT, the row size limit */
    private int maxRowSize;

    /**
     * The name of the system property that controls whether calls to
     * executeSync use a subscription-based iterator by default to execute DML
     * statements when using async.
     *
     * @hidden For internal use only
     */
    public static final String USE_SUBSCRIPTION_ITERATOR =
        "oracle.kv.execute.subscription.iterator";

    /**
     * Whether calls to executeSync use a subscription-based iterator by
     * default to execute DML statements when using async.
     *
     * @hidden For internal use only
     */
    public static final boolean DEFAULT_USE_SUBSCRIPTION_ITERATOR = true;

    /**
     * Whether calls to executeSync use a subscription-based iterator by
     * default to execute DML statements when using async.
     */
    private boolean useAsync = getDefaultUseSubIter();

    private boolean noCharge = false;

    private String queryName;

    private byte[] continuationKey;

    private int driverTopoSeqNum = -1;

    private VirtualScan virtualScan;

    private boolean doLogFileTracing = true;

    /*
     * For cloud.
     * The local region id, use for external region, used by update CRDT.
     */
    private int regionId = Region.NULL_REGION_ID;

    /*
     * For cloud.
     * Indicates whether put tombstone in delete ops, set true if target table
     * is external multi-region table.
     */
    private boolean doTombstone;

    /*
     * The cloud sets this field to true to allow CRDT fields to appear in any
     * table. For on-premises, the field should be false, meaning that CRDT
     * fields can only appear in tables created as multi-region tables. The
     * field is used when compiling create-table or alter-table DDL.
     */
    private boolean allowCRDT = false;

    private boolean inTestMode;

    public ExecuteOptions() {}

    /**
     * @hidden
     */
    public ExecuteOptions(TableIteratorOptions options) {

        if (options != null) {
            maxConcurrentRequests = options.getMaxConcurrentRequests();
            resultsBatchSize = options.getResultsBatchSize();
            maxReadKB = options.getMaxReadKB();
            consistency = options.getConsistency();
            timeout = options.getTimeout();
            timeoutUnit = options.getTimeoutUnit();
            logContext = options.getLogContext();
            authContext = options.getAuthContext();
            noCharge = options.getNoCharge();
        }
    }


    /**
     * Sets the execution consistency.
     */
    public ExecuteOptions setConsistency(Consistency consistency) {
        this.consistency = consistency;
        return this;
    }

    /**
     * Gets the last set execution consistency.
     */
    public Consistency getConsistency() {
        return consistency;
    }

    /**
     * Sets the execution durability.
     */
    public ExecuteOptions setDurability(Durability durability) {
        this.durability = durability;
        return this;
    }

    /**
     * Gets the last set execution durability.
     */
    public Durability getDurability() {
        return durability;
    }

    /**
     * The {@code timeout} parameter is an upper bound on the time interval for
     * processing one of the KVStore.execute(...) methods
     * ({@link KVStore#execute(String, ExecuteOptions)},
     * {@link KVStore#executeSync(String, ExecuteOptions)}, or
     * {@link KVStore#executeSync(Statement, ExecuteOptions)}) and any of the
     * subsequent next() and hasNext() invocations of
     * {@link oracle.kv.StatementResult#iterator()}. Also, in case of
     * asynchronous query execution (via
     * {@link KVStore#executeAsync(String, ExecuteOptions)} or
     * {@link KVStore#executeAsync(Statement, ExecuteOptions)}) is an upper
     * bound on the time interval between two successive callbacks to the
     * Subscriber instance used for the query execution.
     *  A best effort is made not to exceed the specified limit. If zero, the
     * {@link KVStoreConfig#getRequestTimeout default request timeout} is used.
     * <p>
     * If {@code timeout} is not 0, the {@code timeoutUnit} parameter must not
     * be {@code null}.
     *
     * @param timeout the timeout value to use
     * @param timeoutUnit the {@link TimeUnit} used by the
     * <code>timeout</code> parameter or null
     */
    public ExecuteOptions setTimeout(long timeout,
                                     TimeUnit timeoutUnit) {

        if (timeout < 0) {
            throw new IllegalArgumentException("timeout must be >= 0");
        }
        if ((timeout != 0) && (timeoutUnit == null)) {
            throw new IllegalArgumentException("A non-zero timeout requires " +
                "a non-null timeout unit");
        }

        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
        return this;
    }

    /**
     * Gets the timeout, which is an upper bound on the time interval for
     * processing the read or write operations.  A best effort is made not to
     * exceed the specified limit. If zero, the
     * {@link KVStoreConfig#getRequestTimeout default request timeout} is used.
     *
     * @return the timeout
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     * Gets the unit of the timeout parameter, and may
     * be {@code null} only if {@link #getTimeout} returns zero.
     *
     * @return the timeout unit or null
     */
    public TimeUnit getTimeoutUnit() {
        return timeoutUnit;
    }

    /**
     * Returns the maximum number of concurrent requests.
     */
    public int getMaxConcurrentRequests() {
        return maxConcurrentRequests;
    }

    /**
     * Sets the maximum number of concurrent requests.
     */
    public ExecuteOptions setMaxConcurrentRequests(int maxConcurrentRequests) {
        this.maxConcurrentRequests = maxConcurrentRequests;
        return this;
    }

    /**
     * Returns the number of results per request
     * (see {@link #setResultsBatchSize}).
     */
    public int getResultsBatchSize() {
        return (resultsBatchSize > 0 ?
                resultsBatchSize :
                getMaxReadKB() == 0 ?
                KVStoreImpl.DEFAULT_ITERATOR_BATCH_SIZE :
                KVStoreImpl.DEFAULT_BATCH_SIZE_FOR_PROXY_QUERIES);
    }

    /**
     * Sets the max number of query results to be generated during the execution
     * of the query over a partition or shard (which takes place at a replication
     * node (RN) that contains the target partition/shard). If this max is
     * reached, query execution at the RN stops and the generated batch of
     * results is returned to the driver. If more results may be available at
     * the same partition/shard, the driver will later send the query back to
     * an RN that contains the target partition/shard and request another batch
     * of results. The query will resume execution at the point where it got
     * suspended during the previous batch.
     */
    public ExecuteOptions setResultsBatchSize(int resultsBatchSize) {

        if (resultsBatchSize < 0) {
            throw new IllegalArgumentException(
                "The batch size can not be a negative value: " +
                resultsBatchSize);
        }
        this.resultsBatchSize = resultsBatchSize;
        return this;
    }

    /**
     * Returns the {@link MathContext} used for {@link BigDecimal} and
     * {@link BigInteger} operations. {@link MathContext#DECIMAL32} is used by
     * default.
     */
    public MathContext getMathContext() {
        return mathContext;
    }

    /**
     * Sets the {@link MathContext} used for {@link BigDecimal} and
     * {@link BigInteger} operations. {@link MathContext#DECIMAL32} is used by
     * default.
     */
    public ExecuteOptions setMathContext(MathContext mathContext) {

        if (mathContext != null) {
            this.mathContext = mathContext;
        }
        return this;
    }

    /**
     * Returns the trace level for a query
     * @hidden
     */
    public byte getTraceLevel() {
        return traceLevel;
    }

    /**
     * Sets the trace level for a query
     * @hidden
     */
    public ExecuteOptions setTraceLevel(byte level) {
        this.traceLevel = level;
        return this;
    }

    /**
     * @hidden
     */
    public ExecuteOptions setBatchCounter(int b) {
        batchCounter = b;
        return this;
    }

    /**
     * @hidden
     */
    public int getBatchCounter() {
        return batchCounter;
    }

    /**
     * Sets the namespace to use for the query. Query specified namespace
     * takes precedence, else this namespace value is used for unqualified
     * table names.
     *
     * @since 18.3
     */
    public ExecuteOptions setNamespace(String namespace) {
        return setNamespace(namespace, true);
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Sets the namespace to use for the query. This method is semantically
     * equivalent to {@link #setNamespace(String)} when validate flag is true.
     * The validate = false option should be used in the cloud implementation.
     *
     * @since 18.3
     */
    public ExecuteOptions setNamespace(String namespace, boolean validate) {
        if (namespace != null && validate) {
            validateNamespace(namespace);
        }

        this.namespace = namespace;
        this.validateNamespace = validate;

        return this;
    }

    /**
     * Returns the namespace to use for the query, null if not set.
     *
     * @since 18.3
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * @hidden
     * Sets the PrepareCallback to use for the query.
     *
     * @param prepareCallback an instance of PrepareCallback
     *
     * @since 18.1
     */
    public ExecuteOptions setPrepareCallback(PrepareCallback prepareCallback) {
        this.prepareCallback = prepareCallback;
        return this;
    }

    /**
     * @hidden
     * Returns the PrepareCallback set, or null if not set.
     *
     * @since 18.1
     */
    public PrepareCallback getPrepareCallback() {
        return prepareCallback;
    }

    /**
     * Used for http proxy only.
     * @hidden
     *
     * Sets the max number of KBytes read during a query.
     *
     * @since 18.1
     */
    public ExecuteOptions setMaxReadKB(int maxReadKB) {
        if (maxReadKB < 0) {
            throw new IllegalArgumentException("The max read KB can not " +
                "be a negative value: " + maxReadKB);
        }
        this.maxReadKB = maxReadKB;
        return this;
    }

    /**
     * Used for http proxy only.
     * @hidden
     *
     * Returns the max number of KBytes that may be read during a query.
     *
     * @since 19.2
     */
    public int getMaxReadKB() {
        return maxReadKB;
    }

    /**
     * Used for http proxy only.
     * @hidden
     *
     * Sets the max number of KBytes that may be written during a query.
     *
     * @since 19.2
     */
    public ExecuteOptions setMaxWriteKB(int maxWriteKB) {
        if (maxWriteKB < 0) {
            throw new IllegalArgumentException("The max read KB can not " +
                "be a negative value: " + maxWriteKB);
        }
        this.maxWriteKB = maxWriteKB;
        return this;
    }

    /**
     * Used for http proxy only.
     * @hidden
     *
     * Returns the max number of KBytes that may be written during a query.
     *
     * @since 18.1
     */
    public int getMaxWriteKB() {
        return maxWriteKB;
    }

    /**
     * Used for http proxy only.
     * @hidden
     *
     * Sets if use resultsBatchSize as the limit to the number of results
     * returned, the resultsBatchSize can be configured with
     * {@link #setResultsBatchSize}
     *
     * @since 18.1
     */
    public ExecuteOptions setUseBatchSizeAsLimit(boolean value) {
        useBatchSizeAsLimit = value;
        return this;
    }

    /**
     * Used for http proxy only.
     * @hidden
     *
     * Returns true if use resultsBatchSize as the limit to the number of
     * results returned.
     *
     * @since 18.1
     */
    public boolean getUseBatchSizeAsLimit() {
        return useBatchSizeAsLimit;
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Sets the LogContext to use for the query.
     *
     * @since 18.1
     */
    public ExecuteOptions setLogContext(LogContext lc) {
        this.logContext = lc;
        return this;
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Returns the LogContext to use for the query, null if not set.
     *
     * @since 18.1
     */
    public LogContext getLogContext() {
        return logContext;
    }

    /**
     * For internal use only.
     * @hidden
     */
    public void setDoPrefetching(boolean v) {
        doPrefetching = v;
    }

    /**
     * For internal use only.
     * @hidden
     */
    public boolean getDoPrefetching() {
        return doPrefetching;
    }

    /**
     * Set the maximum number of memory bytes that may be consumed by the
     * statement at the client for blocking operations, such as duplicate
     * elimination (which may be required due to the use of an index on an
     * array or map) and sorting (sorting by distance when a query contains
     * a geo_near() function). Such operations may consume a lot of memory
     * as they need to cache the full result set at the client memory.
     * The default value is 100MB.
     *
     * @since 18.3
     */
    public void setMaxMemoryConsumption(long v) {
        maxClientMemoryConsumption = v;
    }

    /**
     * Get the maximum number of memory bytes that may be consumed by the
     * statement at the client for blocking operations, such as duplicate
     * elimination (which may be required due to the use of an index on an
     * array or map) and sorting (sorting by distance when a query contains
     * a geo_near() function). Such operations may consume a lot of memory
     * as they need to cache the full result set at the client memory.
     * The default value is 100MB.
     */
    public long getMaxMemoryConsumption() {
        return maxClientMemoryConsumption;
    }

    /**
     * @hidden
     * Return the maximum number of memory bytes that may be consumed by the
     * query at a replication node.
     * @see #setMaxServerMemoryConsumption
     */
    public long getMaxServerMemoryConsumption() {
        return maxServerMemoryConsumption;
    }

    /**
     * @hidden
     * Sets the maximum number of memory bytes that may be consumed by the
     * query at a replication node. In general, queries do not consume
     * a lot of memory while executing at a replcation node and the value
     * of this parameter has no effect. Currently, the only exception are
     * queries that use the array_collect function. For such queries, if
     * the maximum amount of memory is exceeded, execution of the query
     * at the replication node will be terminated (without error) and the
     * set of query results that have been computed so far will be sent
     * to the driver. The driver will keep executing the query, sending
     * more requests to the replication nodes for additional results.
     * So, for queries that use array_collect, increasing the value of this
     * parameter will decrease the number of interactions between the driver
     * and the replication nodes at the expense of consuming more memory
     * at the nodes.
     * <p>
     * The default value is 10MB.
     *
     * @param maxBytes the value to use in bytes
     *
     * @return this
     */
    public ExecuteOptions setMaxServerMemoryConsumption(long maxBytes) {
        maxServerMemoryConsumption = maxBytes;
        return this;
    }

    /**
     * @hidden
     */
    public int getGeoMaxCoveringCells() {
        return geoMaxCoveringCells;
    }

    /**
     * @hidden
     */
    public void setGeoMaxCoveringCells(int v) {
        geoMaxCoveringCells = v;
    }

    /**
     * @hidden
     */
    public int getGeoMinCoveringCells() {
        return geoMinCoveringCells;
    }

    /**
     * @hidden
     */
    public void setGeoMinCoveringCells(int v) {
        geoMinCoveringCells = v;
    }

    /**
     * @hidden
     */
    public int getGeoMaxRanges() {
        return geoMaxRanges;
    }

    /**
     * @hidden
     */
    public void setGeoMaxRanges(int v) {
        geoMaxRanges = v;
    }

    /**
     * @hidden
     */
    public int getGeoMaxSplits() {
        return geoMaxSplits;
    }

    /**
     * @hidden
     */
    public void setGeoMaxSplits(int v) {
        geoMaxSplits = v;
    }

    /**
     * @hidden
     */
    public double getGeoSplitRatio() {
        return geoSplitRatio;
    }

    /**
     * @hidden
     */
    public void setGeoSplitRatio(double v) {
        geoSplitRatio = v;
    }

    /**
     * @hidden
     */
    public void setDeleteLimit(int v) {
        deleteLimit = v;
    }

    /**
     * @hidden
     */
    public int getDeleteLimit() {
        return deleteLimit;
    }

    /**
     * Sets the maximum number of records that can be updated in a single
     * update query. The affected records in a update query are processed
     * within a single transaction. If this limit is too large, it may impact
     * other operations that need to read or write the data involved in the
     * transaction. The default value is 1000.
     *
     * @since 25.1
     */
    public void setUpdateLimit(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException
                ("UpdateLimit may not be negative or zero");
        }
        updateLimit = limit;
    }

    /**
     * Returns the maximum number of records that can be updated in a single
     * update query. The default value is 1000.
     *
     * @since 25.1
     */
    public int getUpdateLimit() {
        return updateLimit;
    }

    /**
     * @hidden
     */
    public boolean isValidateNamespace() {
        return validateNamespace;
    }

    /**
     * @hidden
     */
    public ExecuteOptions setIsCloudQuery(boolean v) {
        isProxyQuery = v;
        return this;
    }

    /**
     * @hidden
     */
    public boolean isProxyQuery() {
        return isProxyQuery;
    }

    /**
     * @hidden
     */
    public ExecuteOptions setDriverQueryVersion(int v) {
        driverQueryVersion = v;
        return this;
    }

    /**
     * @hidden
     */
    public int getDriverQueryVersion() {
        return driverQueryVersion;
    }

    /**
     * @hidden
     */
    public AuthContext getAuthContext() {
        return authContext;
    }

    /**
     * @hidden
     */
    public ExecuteOptions setAuthContext(AuthContext authContext) {
        this.authContext = authContext;
        return this;
    }

    /**
     * @hidden
     */
    public ExecuteOptions setMaxPrimaryKeySize(int size) {
        maxPrimaryKeySize = size;
        return this;
    }

    /**
     * @hidden
     */
    public int getMaxPrimaryKeySize() {
        return maxPrimaryKeySize;
    }

    /**
     * @hidden
     */
    public ExecuteOptions setMaxRowSize(int size) {
        maxRowSize = size;
        return this;
    }

    /**
     * @hidden
     */
    public int getMaxRowSize() {
        return maxRowSize;
    }

    /**
     * Returns the default for whether calls to executeSync should use a
     * subscription-based iterator to execute DML statements when using async.
     *
     * @hidden For internal use only
     */
    public static boolean getDefaultUseSubIter() {
        return (System.getProperty(USE_SUBSCRIPTION_ITERATOR) == null) ?
            DEFAULT_USE_SUBSCRIPTION_ITERATOR :
            Boolean.getBoolean(USE_SUBSCRIPTION_ITERATOR);
    }

    /**
     * Specifies whether calls to executeSync should use a subscription-based
     * iterator to execute DML statements when using async.
     * @hidden
     */
    public ExecuteOptions setAsync(boolean v) {
        useAsync = v;
        return this;
    }

    /**
     * whether calls to executeSync should use a subscription-based
     * iterator to execute DML statements when using async.
     * @hidden
     */
    public boolean isAsync() {
        return useAsync;
    }

    /**
     * Returns the no-charge flag.
     *
     * @hidden
     */
    public boolean getNoCharge() {
        return noCharge;
    }

    /**
     * Sets the no-charge flag. If true, read or write throughput consumed by
     * the operation is not charged to the table. Defaults to false.
     *
     * @hidden
     */
    public void setNoCharge(boolean flag) {
        noCharge = flag;
    }

    public ExecuteOptions setQueryName(String n) {
        queryName = n;
        return this;
    }

    public String getQueryName() {
        return queryName;
    }

    /**
     * Used for http proxy only.
     * @hidden
     *
     * Sets the continuation Key from which the query resumed.
     *
     * @since 18.1
     */
    public ExecuteOptions setContinuationKey(byte[] continuationKey) {
        this.continuationKey = continuationKey;
        return this;
    }

    /**
     * Used for http proxy only.
     * @hidden
     *
     * Returns the continuation Key specified.
     *
     * @since 18.1
     */
    public byte[] getContinuationKey() {
        return continuationKey;
    }

    /**
     * @hidden
     */
    public int getDriverTopoSeqNum() {
        return driverTopoSeqNum;
    }

    /**
     * @hidden
     */
    public ExecuteOptions setDriverTopoSeqNum(int n) {
        driverTopoSeqNum = n;
        return this;
    }

    /**
     * @hidden
     */
    public VirtualScan getVirtualScan() {
        return virtualScan;
    }

    /**
     * @hidden
     */
    public ExecuteOptions setVirtualScan(VirtualScan vs) {
        virtualScan = vs;
        return this;
    }

    /**
     * @hidden
     */
    public boolean doLogFileTracing() {
        return doLogFileTracing;
    }

    /**
     * @hidden
     */
    public ExecuteOptions setDoLogFileTracing(boolean v) {
        doLogFileTracing = v;
        return this;
    }

    /**
     * Sets the external region id, used for table whose region is persisted in
     * external metadata only.
     *
     * @param regionId the region ID of the region performing the operation,
     * or NULL_REGION_ID if not performing a cloud operation
     *
     * @throws IllegalArgumentException if regionId is invalid
     *
     * @hidden
     */
    public ExecuteOptions setRegionId(int regionId) {
        if (regionId != Region.NULL_REGION_ID) {
            /* check it is a valid external region id */
            Region.checkId(regionId, true);
        }
        this.regionId = regionId;
        return this;
    }

    /**
     * For table which region is persist in external metadata only.
     * Returns the external region id or NULL_REGION_ID.
     *
     * @hidden
     */
    public int getRegionId() {
        return regionId;
    }

    /**
     * Returns whether put tombstone instead of deleting, used for delete
     * operation on external multi-region table.
     *
     * @hidden
     */
    public boolean doTombstone() {
        return doTombstone;
    }

    /**
     * Sets whether put tombstone instead of deleting, used for delete operation
     * on external multi-region table.
     *
     * @param doTombstone true to put tombstone instead of deleting
     *
     * @hidden
     */
    public ExecuteOptions setDoTombstone(boolean doTombstone) {
        this.doTombstone = doTombstone;
        return this;
    }

    /**
     * Sets whether to allow CRDT fields in all tables to support CRDTs in the
     * cloud.
     *
     * @param allowCRDT whether to allow CRDT fields in all tables
     *
     * @hidden
     */
    public ExecuteOptions setAllowCRDT(boolean allowCRDT) {
        this.allowCRDT = allowCRDT;
        return this;
    }

    /**
     * Returns whether to allow CRDT fields in all tables to support CRDTs in
     * the cloud.
     *
     * @hidden
     */
    public boolean allowCRDT() {
        return allowCRDT;
    }

    /**
     * @hidden
     */
    public void setInTestMode(boolean v) {
        inTestMode = v;
    }

    /**
     * @hidden
     */
    public boolean inTestMode() {
        return inTestMode;
    }
}
