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

package oracle.kv.impl.api;

import static oracle.kv.impl.async.FutureUtils.checkedComplete;
import static oracle.kv.impl.async.FutureUtils.checkedCompleteExceptionally;
import static oracle.kv.impl.async.FutureUtils.unwrapExceptionVoid;
import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.IOException;
import java.io.InputStream;
import java.lang.Thread.UncaughtExceptionHandler;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.logging.Logger;

import javax.net.ssl.SSLHandshakeException;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.BulkWriteOptions;
import oracle.kv.Consistency;
import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.EntryStream;
import oracle.kv.ExecutionFuture;
import oracle.kv.FaultException;
import oracle.kv.KVSecurityException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreException;
import oracle.kv.KerberosCredentials;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.KeyValue;
import oracle.kv.KeyValueVersion;
import oracle.kv.LoginCredentials;
import oracle.kv.MetadataNotFoundException;
import oracle.kv.Operation;
import oracle.kv.OperationExecutionException;
import oracle.kv.OperationResult;
import oracle.kv.ParallelScanIterator;
import oracle.kv.ReauthenticateHandler;
import oracle.kv.RequestTimeoutException;
import oracle.kv.ReturnValueVersion;
import oracle.kv.StatementResult;
import oracle.kv.StoreIteratorConfig;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.impl.api.bulk.BulkMultiGet;
import oracle.kv.impl.api.bulk.BulkPut;
import oracle.kv.impl.api.bulk.BulkPut.KVPair;
import oracle.kv.impl.api.lob.KVLargeObjectImpl;
import oracle.kv.impl.api.ops.Delete;
import oracle.kv.impl.api.ops.DeleteIfVersion;
import oracle.kv.impl.api.ops.Execute;
import oracle.kv.impl.api.ops.Execute.OperationFactoryImpl;
import oracle.kv.impl.api.ops.Execute.OperationImpl;
import oracle.kv.impl.api.ops.Get;
import oracle.kv.impl.api.ops.GetIdentityAttrsAndValues;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.ops.MultiDelete;
import oracle.kv.impl.api.ops.MultiGet;
import oracle.kv.impl.api.ops.MultiGetIterate;
import oracle.kv.impl.api.ops.MultiGetKeys;
import oracle.kv.impl.api.ops.MultiGetKeysIterate;
import oracle.kv.impl.api.ops.Put;
import oracle.kv.impl.api.ops.PutBatch;
import oracle.kv.impl.api.ops.PutIfAbsent;
import oracle.kv.impl.api.ops.PutIfPresent;
import oracle.kv.impl.api.ops.PutIfVersion;
import oracle.kv.impl.api.ops.PutResolve;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.ops.ResultKey;
import oracle.kv.impl.api.ops.ResultKeyValueVersion;
import oracle.kv.impl.api.ops.StoreIterate;
import oracle.kv.impl.api.ops.StoreKeysIterate;
import oracle.kv.impl.api.parallelscan.ParallelScan;
import oracle.kv.impl.api.parallelscan.ParallelScanHook;
import oracle.kv.impl.api.query.DmlFuture;
import oracle.kv.impl.api.query.InternalStatement;
import oracle.kv.impl.api.query.PreparedDdlStatementImpl;
import oracle.kv.impl.api.query.PreparedStatementImpl;
import oracle.kv.impl.api.query.QueryPublisher;
import oracle.kv.impl.api.query.QueryPublisher.QuerySubscription;
import oracle.kv.impl.api.rgstate.RepGroupStateTable;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.impl.api.table.SequenceImpl;
import oracle.kv.impl.api.table.SequenceImpl.SGAttributes;
import oracle.kv.impl.api.table.SequenceImpl.SGAttrsAndValues;
import oracle.kv.impl.api.table.SequenceImpl.SGIntegerValues;
import oracle.kv.impl.api.table.SequenceImpl.SGKey;
import oracle.kv.impl.api.table.SequenceImpl.SGLongValues;
import oracle.kv.impl.api.table.SequenceImpl.SGNumberValues;
import oracle.kv.impl.api.table.SequenceImpl.SGValues;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.api.table.TableMetadataHelper;
import oracle.kv.impl.api.table.ValueSerializer;
import oracle.kv.impl.api.table.ValueSerializer.RowSerializer;
import oracle.kv.impl.client.admin.DdlFuture;
import oracle.kv.impl.client.admin.DdlStatementExecutor;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.CompilerAPI;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.login.KerberosClientCreds;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.RepNodeLoginManager;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.systables.SGAttributesTableDesc;
import oracle.kv.impl.systables.SGAttributesTableDesc.SGType;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.WatcherNames;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.lob.InputStreamVersion;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.PrepareCallback;
import oracle.kv.query.PreparedStatement;
import oracle.kv.query.Statement;
import oracle.kv.stats.KVStats;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValueFactory;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TimeToLive;
import oracle.kv.table.WriteOptions;

import oracle.nosql.common.cache.Cache;
import oracle.nosql.common.cache.CacheBuilder;
import oracle.nosql.common.cache.CacheBuilder.CacheConfig;

import com.sleepycat.je.utilint.PropUtil;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class KVStoreImpl implements KVStore, Cloneable {

    /* TODO: Is this the correct default value? */
    private static int DEFAULT_TTL = 5;

    /* TODO: Is this the correct default value? */
    public static int DEFAULT_ITERATOR_BATCH_SIZE = 100;

    /* Query batch size used when a proxy-based driver does not explicitly
     * specify a batch size */
    public static int DEFAULT_BATCH_SIZE_FOR_PROXY_QUERIES = 2048;

    /*
     * An empty record that can be used to push into a queue of records to
     * notify waiters that there are no more records. All real return values
     * are non-empty, so callers will be able recognize this record as a
     * marker.
     */
    public static final RecordValue EMPTY_RECORD = new RecordValueImpl();

    /**
     * If set, called by the onNext method of the Subscriber that executeSync
     * passes to executeAsync. The hook is called with a boolean value that
     * represents whether the value being supplied to onNext is not the first
     * value. If the hook throws a RuntimeException, onNext will return without
     * supplying the new value.
     */
    private static TestHook<Boolean> subscriberOnNextHook;

    /** A request handler to use for making all requests.  */
    private final RequestDispatcher dispatcher;

    /**
     * Indicates whether the dispatcher is owned by this instance. If we own
     * the dispatcher, our LoginManager is used to gain access to Topology
     * maintenance API methods, and so login changes should be propagated to
     * the dispatcher.
     */
    private final boolean isDispatcherOwner;

    /** Default request timeout in millis, used when timeout param is zero. */
    private final int defaultRequestTimeoutMs;

    /** Socket read timeout for requests. */
    private final int readTimeoutMs;

    /** Default Consistency, used when Consistency param is null. */
    private final Consistency defaultConsistency;

    /** Default Durability, used when Durability param is null. */
    private final Durability defaultDurability;

    /** @see KVStoreConfig#getLOBTimeout(TimeUnit) */
    private final long defaultLOBTimeout;

    /** @see KVStoreConfig#getLOBSuffix() */
    private final String defaultLOBSuffix;

    /** @see KVStoreConfig#getLOBVerificationBytes() */
    private final long defaultLOBVerificationBytes;

    /** @see KVStoreConfig#getLOBChunksPerPartition() */
    private final int defaultChunksPerPartition;

    /** @see KVStoreConfig#getLOBChunkSize() */
    private final int defaultChunkSize;

    /** @see KVStoreConfig#getCheckInterval(TimeUnit) */
    private final long checkIntervalMillis;

    /** @see KVStoreConfig#getMaxCheckRetries */
    private final int maxCheckRetries;

    /** Implementation of OperationFactory. */
    private final OperationFactoryImpl operationFactory;

    /** Max partition ID.  This value is immutable for a store. */
    private final int nPartitions;

    /** Translates between byte arrays and Keys */
    private final KeySerializer keySerializer;

    /** The component implementing large object support. */
    final KVLargeObjectImpl largeObjectImpl;

    /** Debugging and unit test hook for Parallel Scan. */
    private ParallelScanHook parallelScanHook;

    /** LoginManager - may be null*/
    private volatile LoginManager loginMgr;

    /** Login/Logout locking handle */
    private final Object loginLock = new Object();

    /** Optional reauthentication handler */
    private final ReauthenticateHandler reauthHandler;

    /**
     * The KVStore handle for the associated external store if this is an
     * internal handle, otherwise null.
     */
    private KVStoreImpl external = null;

    private final SharedThreadPool sharedThreadPool;

    /*
     * Manages the execution of ddl statements.
     */
    final private DdlStatementExecutor statementExecutor;

    /* The default logger used on the client side. */
    private final Logger logger;

    /* TableAPI instance */
    private final TableAPIImpl tableAPI;

    /* Whether the store handle is closed */
    private volatile boolean isClosed = false;

    /* Cache values for sequence generators */

    private final Map<SequenceImpl.SGKey, SequenceImpl.SGValues<?>> sgValues;

    /* Timed cache of sequence generators attributes */

    private final Cache<SequenceImpl.SGKey, SGAttributes> sgAttributes;

    /* Whether to exclude tombstones for methods in KVStore. */
    private final boolean excludeTombstones;

    /* The KVStats monitor or null if not enabled */
    private final KVStatsMonitor kvStatsMonitor;

    /**
     * The KVStoreInternalFactory constructor
     */
    public KVStoreImpl(Logger logger,
                       RequestDispatcher dispatcher,
                       KVStoreConfig config,
                       LoginManager loginMgr) {

        this(logger, dispatcher, config, loginMgr,
             (ReauthenticateHandler) null,
             false /* isDispatcherOwner */);
    }

    /**
     * The KVStoreFactory constructor
     */
    public KVStoreImpl(Logger logger,
                       RequestDispatcher dispatcher,
                       KVStoreConfig config,
                       LoginManager loginMgr,
                       ReauthenticateHandler reauthHandler) {

        this(logger, dispatcher, config, loginMgr, reauthHandler,
             true /* isDispatcherOwner */);
    }

    private KVStoreImpl(Logger logger,
                        RequestDispatcher dispatcher,
                        KVStoreConfig config,
                        LoginManager loginMgr,
                        ReauthenticateHandler reauthHandler,
                        boolean isDispatcherOwner) {
        this.logger = logger;
        this.dispatcher = dispatcher;
        this.isDispatcherOwner = isDispatcherOwner;
        this.loginMgr = loginMgr;
        this.reauthHandler = reauthHandler;
        this.defaultRequestTimeoutMs =
            (int) config.getRequestTimeout(TimeUnit.MILLISECONDS);
        this.readTimeoutMs =
            (int) config.getSocketReadTimeout(TimeUnit.MILLISECONDS);
        this.defaultConsistency = config.getConsistency();
        this.defaultDurability = config.getDurability();
        this.checkIntervalMillis =
            config.getCheckInterval(TimeUnit.MILLISECONDS);
        this.maxCheckRetries = config.getMaxCheckRetries();
        this.keySerializer = KeySerializer.PROHIBIT_INTERNAL_KEYSPACE;
        this.operationFactory = new OperationFactoryImpl(keySerializer);
        this.nPartitions =
            dispatcher.getTopology().getPartitionMap().getNPartitions();
        this.defaultLOBTimeout = config.getLOBTimeout(TimeUnit.MILLISECONDS);
        this.defaultLOBSuffix = config.getLOBSuffix();
        this.defaultLOBVerificationBytes = config.getLOBVerificationBytes();
        this.defaultChunksPerPartition = config.getLOBChunksPerPartition();
        this.defaultChunkSize = config.getLOBChunkSize();
        this.largeObjectImpl = new KVLargeObjectImpl();

        this.sharedThreadPool = new SharedThreadPool(logger);
        this.tableAPI = new TableAPIImpl(this, config);

        /*
         * Only invoke this after all ivs have been initialized, since it
         * creates an internal handle.
         */
        largeObjectImpl.setKVSImpl(this);

        statementExecutor = new DdlStatementExecutor(this);

        sgValues = new ConcurrentHashMap<>();

        final CacheConfig cacheConfig =
            new CacheConfig().setCapacity(1000).
            setLifetime(config.getSGAttrsCacheTimeout());
        sgAttributes = CacheBuilder.build(cacheConfig);
        excludeTombstones = config.getExcludeTombstones();

        if (!config.getEnableStatsMonitor()) {
            kvStatsMonitor = null;
        } else {
            kvStatsMonitor = new KVStatsMonitor(
                this,
                dispatcher.getClientId().toString(),
                config.getStatsMonitorLogIntervalMillis(),
                config.getStatsMonitorCallback());
            kvStatsMonitor.start();
        }
    }

    public KVLargeObjectImpl getLargeObjectImpl() {
        return largeObjectImpl;
    }

    /**
     * Returns the Topology object.
     */
    public Topology getTopology() {
        return dispatcher.getTopology();
    }

    /**
     * Clones a handle for internal use, to provide access to the internal
     * keyspace (//) that is used for internal metadata.  This capability is
     * not exposed in published classes -- KVStoreConfig or KVStoreFactory --
     * in order to provide an extra safeguard against use of the internal
     * keyspace by user applications.  See KeySerializer for more information.
     * <p>
     * The new instance created by this method should never be explicitly
     * closed.  It will be discarded when the KVStoreImpl it is created from is
     * closed and discarded.
     * <p>
     * The new instance created by this method shares the KVStoreConfig
     * settings with the KVStoreImpl it is created from.  If specific values
     * are desired for consistency, durability or timeouts, these parameters
     * should be passed explicitly to the operation methods.
     */
    public static KVStore makeInternalHandle(KVStore other) {
        return new KVStoreImpl((KVStoreImpl) other,
                               true /*allowInternalKeyspace*/) {
            @Override
            public void close() {
                throw new UnsupportedOperationException();
            }
            @Override
            public void logout() {
                throw new UnsupportedOperationException();
            }
            @Override
            public void login(LoginCredentials creds) {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Returns the current LoginManager for a KVStore instance.
     */
    public static LoginManager getLoginManager(KVStore store) {
        final KVStoreImpl impl = (KVStoreImpl) store;
        synchronized (impl.loginLock) {
            return impl.loginMgr;
        }
    }

    /**
     * A predicate to determine whether this is an internal handle KVS handle.
     */
    private boolean isInternalHandle() {
        return keySerializer == KeySerializer.ALLOW_INTERNAL_KEYSPACE;
    }

    /**
     * Renew login manager. Only used for internal handle.
     */
    public void renewLoginManager(LoginManager loginManager) {
        if (isInternalHandle()) {
            synchronized (loginLock) {
                this.loginMgr = loginManager;
            }
        }
    }

    /**
     * Note that this copy constructor could be modified to allow overriding
     * KVStoreConfig settings, if multiple handles with difference settings are
     * needed in the future.
     */
    private KVStoreImpl(KVStoreImpl other, boolean allowInternalKeyspace) {
        this.logger = other.logger;
        this.loginMgr = getLoginManager(other);
        this.dispatcher = other.dispatcher;
        this.isDispatcherOwner = false;
        this.defaultRequestTimeoutMs = other.defaultRequestTimeoutMs;
        this.readTimeoutMs = other.readTimeoutMs;
        this.defaultConsistency = other.defaultConsistency;
        this.defaultDurability = other.defaultDurability;
        this.maxCheckRetries = other.maxCheckRetries;
        this.checkIntervalMillis = other.checkIntervalMillis;
        this.keySerializer = allowInternalKeyspace ?
            KeySerializer.ALLOW_INTERNAL_KEYSPACE :
            KeySerializer.PROHIBIT_INTERNAL_KEYSPACE;
        this.operationFactory = new OperationFactoryImpl(keySerializer);
        this.nPartitions = other.nPartitions;

        this.defaultLOBTimeout = other.defaultLOBTimeout;
        this.defaultLOBSuffix = other.defaultLOBSuffix;
        this.defaultLOBVerificationBytes = other.defaultLOBVerificationBytes;
        this.defaultChunksPerPartition = other.defaultChunksPerPartition;
        this.defaultChunkSize = other.defaultChunkSize;
        this.largeObjectImpl = other.largeObjectImpl;
        this.reauthHandler = other.reauthHandler;

        if (largeObjectImpl == null) {
            throw new IllegalStateException("null large object impl");
        }

        this.sharedThreadPool = new SharedThreadPool(logger);
        if (isInternalHandle()) {
            this.external = other;
        }

        statementExecutor = new DdlStatementExecutor(this);

        this.tableAPI = other.tableAPI;

        this.sgValues = other.sgValues;
        this.sgAttributes = other.sgAttributes;
        this.excludeTombstones = other.excludeTombstones;
        if (other.kvStatsMonitor == null) {
            kvStatsMonitor = null;
        } else {
            kvStatsMonitor = new KVStatsMonitor(
                this,
                dispatcher.getClientId().toString(),
                other.kvStatsMonitor.getLogIntervalMillis(),
                other.kvStatsMonitor.getCallback());
            kvStatsMonitor.start();
        }
    }

    public Logger getLogger() {
        return logger;
    }

    public UncaughtExceptionHandler getExceptionHandler() {
        return dispatcher.getExceptionHandler();
    }

    public KeySerializer getKeySerializer() {
        return keySerializer;
    }

    public int getNPartitions() {
        return nPartitions;
    }

    public RequestDispatcher getDispatcher() {
        return dispatcher;
    }

    public void setParallelScanHook(ParallelScanHook parallelScanHook) {
        this.parallelScanHook = parallelScanHook;
    }

    public ParallelScanHook getParallelScanHook() {
        return parallelScanHook;
    }

    public int getDefaultRequestTimeoutMs() {
        return defaultRequestTimeoutMs;
    }

    public int getReadTimeoutMs() {
        return readTimeoutMs;
    }

    public long getCheckIntervalMillis() {
        return checkIntervalMillis;
    }

    public int getMaxCheckRetries() {
        return maxCheckRetries;
    }

    @Override
    public ValueVersion get(Key key)
        throws FaultException {

        return get(key, null, 0, null);
    }

    @Override
    public ValueVersion get(Key key,
                            Consistency consistency,
                            long timeout,
                            TimeUnit timeoutUnit)
        throws FaultException {

        return getInternal(key, 0, consistency, timeout, timeoutUnit);
    }

    public ValueVersion getInternal(Key key,
                                    long tableId,
                                    Consistency consistency,
                                    long timeout,
                                    TimeUnit timeoutUnit)
        throws FaultException {

        final Request req = makeGetRequest(key, tableId, consistency, timeout,
                                           timeoutUnit, excludeTombstones);
        final Result result = executeRequest(req);
        return processGetResult(result);
    }

    public static ValueVersion processGetResult(Result result) {
        final Value value = result.getPreviousValue();
        if (value == null) {
            assert !result.getSuccess();
            return null;
        }
        assert result.getSuccess();
        final ValueVersion ret = new ValueVersion();
        ret.setValue(value);
        ret.setVersion(result.getPreviousVersion());
        return ret;
    }

    public Request makeGetRequest(Key key,
                                  long tableId,
                                  Consistency consistency,
                                  long timeout,
                                  TimeUnit timeoutUnit,
                                  boolean excludeTombstone) {
        final byte[] keyBytes = keySerializer.toByteArray(key);
        final PartitionId partitionId = dispatcher.getPartitionId(keyBytes);
        final Get get = new Get(keyBytes, tableId, excludeTombstone);
        return makeReadRequest(get, partitionId, consistency, timeout,
                               timeoutUnit);
    }

    @Override
    public SortedMap<Key, ValueVersion> multiGet(Key parentKey,
                                                 KeyRange subRange,
                                                 Depth depth)
        throws FaultException {

        return multiGet(parentKey, subRange, depth, null, 0, null);
    }

    @Override
    public SortedMap<Key, ValueVersion> multiGet(Key parentKey,
                                                 KeyRange subRange,
                                                 Depth depth,
                                                 Consistency consistency,
                                                 long timeout,
                                                 TimeUnit timeoutUnit)
        throws FaultException {

        if (depth == null) {
            depth = Depth.PARENT_AND_DESCENDANTS;
        }

        /* Execute request. */
        final byte[] parentKeyBytes = keySerializer.toByteArray(parentKey);
        final PartitionId partitionId =
            dispatcher.getPartitionId(parentKeyBytes);
        final MultiGet get = new MultiGet(parentKeyBytes, subRange, depth,
                                          excludeTombstones);
        final Request req = makeReadRequest(get, partitionId, consistency,
                                            timeout, timeoutUnit);
        final Result result = executeRequest(req);

        /* Convert byte[] keys to Key objects. */
        final List<ResultKeyValueVersion> byteKeyResults =
            result.getKeyValueVersionList();
        final SortedMap<Key, ValueVersion> stringKeyResults =
            new TreeMap<Key, ValueVersion>();
        for (ResultKeyValueVersion entry : byteKeyResults) {
            stringKeyResults.put
                (keySerializer.fromByteArray(entry.getKeyBytes()),
                 new ValueVersion(entry.getValue(), entry.getVersion()));
        }
        assert result.getSuccess() == (!stringKeyResults.isEmpty());
        return stringKeyResults;
    }

    @Override
    public SortedSet<Key> multiGetKeys(Key parentKey,
                                       KeyRange subRange,
                                       Depth depth)
        throws FaultException {

        return multiGetKeys(parentKey, subRange, depth, null, 0, null);
    }

    @Override
    public SortedSet<Key> multiGetKeys(Key parentKey,
                                       KeyRange subRange,
                                       Depth depth,
                                       Consistency consistency,
                                       long timeout,
                                       TimeUnit timeoutUnit)
        throws FaultException {

        if (depth == null) {
            depth = Depth.PARENT_AND_DESCENDANTS;
        }

        /* Execute request. */
        final byte[] parentKeyBytes = keySerializer.toByteArray(parentKey);
        final PartitionId partitionId =
            dispatcher.getPartitionId(parentKeyBytes);
        final MultiGetKeys get =
            new MultiGetKeys(parentKeyBytes, subRange, depth,
                             excludeTombstones);
        final Request req = makeReadRequest(get, partitionId, consistency,
                                            timeout, timeoutUnit);
        final Result result = executeRequest(req);

        /* Convert byte[] keys to Key objects. */
        final List<ResultKey> byteKeyResults = result.getKeyList();
        final SortedSet<Key> stringKeySet = new TreeSet<Key>();
        for (ResultKey entry : byteKeyResults) {
            stringKeySet.add(keySerializer.fromByteArray(entry.getKeyBytes()));
        }
        assert result.getSuccess() == (!stringKeySet.isEmpty());
        return stringKeySet;
    }

    @Override
    public Iterator<KeyValueVersion> multiGetIterator(Direction direction,
                                                      int batchSize,
                                                      Key parentKey,
                                                      KeyRange subRange,
                                                      Depth depth)
        throws FaultException {

        return multiGetIterator(direction, batchSize, parentKey, subRange,
                                depth, null, 0, null);
    }

    @Override
    public Iterator<KeyValueVersion>
        multiGetIterator(final Direction direction,
                         final int batchSize,
                         final Key parentKey,
                         final KeyRange subRange,
                         final Depth depth,
                         final Consistency consistency,
                         final long timeout,
                         final TimeUnit timeoutUnit)
        throws FaultException {

        if (direction != Direction.FORWARD &&
            direction != Direction.REVERSE) {
            throw new IllegalArgumentException
                ("Only Direction.FORWARD and REVERSE are supported, got: " +
                 direction);
        }

        final Depth useDepth =
            (depth != null) ? depth : Depth.PARENT_AND_DESCENDANTS;

        final int useBatchSize =
            (batchSize > 0) ? batchSize : DEFAULT_ITERATOR_BATCH_SIZE;

        final byte[] parentKeyBytes = keySerializer.toByteArray(parentKey);

        final PartitionId partitionId =
            dispatcher.getPartitionId(parentKeyBytes);

        return new ArrayIterator<KeyValueVersion>() {
            private boolean moreElements = true;
            private byte[] resumeKey = null;

            @Override
            KeyValueVersion[] getMoreElements() {

                /* Avoid round trip if we know there are no more elements. */
                if (!moreElements) {
                    return null;
                }

                /* Execute request. */
                final MultiGetIterate get = new MultiGetIterate
                    (parentKeyBytes, subRange, useDepth, direction,
                     useBatchSize, resumeKey, excludeTombstones);
                final Request req = makeReadRequest
                    (get, partitionId, consistency, timeout,
                     timeoutUnit);
                final Result result = executeRequest(req);

                /* Get results and save resume key. */
                moreElements = result.hasMoreElements();
                final List<ResultKeyValueVersion> byteKeyResults =
                    result.getKeyValueVersionList();
                if (byteKeyResults.size() == 0) {
                    assert (!moreElements);
                    return null;
                }
                resumeKey = byteKeyResults.get
                    (byteKeyResults.size() - 1).getKeyBytes();

                /* Convert byte[] keys to Key objects. */
                final KeyValueVersion[] stringKeyResults =
                    new KeyValueVersion[byteKeyResults.size()];
                for (int i = 0; i < stringKeyResults.length; i += 1) {
                    final ResultKeyValueVersion entry = byteKeyResults.get(i);
                    stringKeyResults[i] = new KeyValueVersion
                        (keySerializer.fromByteArray(entry.getKeyBytes()),
                         entry.getValue(), entry.getVersion(),
                         entry.getModificationTime());
                }
                return stringKeyResults;
            }
        };
    }

    @Override
    public Iterator<Key> multiGetKeysIterator(Direction direction,
                                              int batchSize,
                                              Key parentKey,
                                              KeyRange subRange,
                                              Depth depth)
        throws FaultException {

        return multiGetKeysIterator(direction, batchSize, parentKey, subRange,
                                    depth, null, 0, null);
    }

    @Override
    public Iterator<Key> multiGetKeysIterator(final Direction direction,
                                              final int batchSize,
                                              final Key parentKey,
                                              final KeyRange subRange,
                                              final Depth depth,
                                              final Consistency consistency,
                                              final long timeout,
                                              final TimeUnit timeoutUnit)
        throws FaultException {

        if (direction != Direction.FORWARD &&
            direction != Direction.REVERSE) {
            throw new IllegalArgumentException
                ("Only Direction.FORWARD and REVERSE are supported, got: " +
                 direction);
        }

        final Depth useDepth =
            (depth != null) ? depth : Depth.PARENT_AND_DESCENDANTS;

        final int useBatchSize =
            (batchSize > 0) ? batchSize : DEFAULT_ITERATOR_BATCH_SIZE;

        final byte[] parentKeyBytes = keySerializer.toByteArray(parentKey);

        final PartitionId partitionId =
            dispatcher.getPartitionId(parentKeyBytes);

        return new ArrayIterator<Key>() {
            private boolean moreElements = true;
            private byte[] resumeKey = null;

            @Override
            Key[] getMoreElements() {

                /* Avoid round trip if we know there are no more elements. */
                if (!moreElements) {
                    return null;
                }

                /* Execute request. */
                final MultiGetKeysIterate get = new MultiGetKeysIterate
                    (parentKeyBytes, subRange, useDepth, direction,
                     useBatchSize, resumeKey, excludeTombstones);
                final Request req = makeReadRequest
                    (get, partitionId, consistency,
                     timeout, timeoutUnit);
                final Result result = executeRequest(req);

                /* Get results and save resume key. */
                moreElements = result.hasMoreElements();
                final List<ResultKey> byteKeyResults = result.getKeyList();
                if (byteKeyResults.size() == 0) {
                    assert (!moreElements);
                    return null;
                }
                resumeKey = byteKeyResults.
                    get(byteKeyResults.size() - 1).getKeyBytes();

                /* Convert byte[] keys to Key objects. */
                final Key[] stringKeyResults = new Key[byteKeyResults.size()];
                for (int i = 0; i < stringKeyResults.length; i += 1) {
                    final byte[] entry = byteKeyResults.get(i).getKeyBytes();
                    stringKeyResults[i] = keySerializer.fromByteArray(entry);
                }
                return stringKeyResults;
            }
        };
    }

    @Override
    public Iterator<KeyValueVersion> storeIterator(Direction direction,
                                                   int batchSize)
        throws FaultException {

        return storeIterator(direction, batchSize, null, null, null,
                             null, 0, null);
    }

    @Override
    public Iterator<KeyValueVersion> storeIterator(Direction direction,
                                                   int batchSize,
                                                   Key parentKey,
                                                   KeyRange subRange,
                                                   Depth depth)
        throws FaultException {

        return storeIterator(direction, batchSize, parentKey, subRange, depth,
                             null, 0, null);
    }

    @Override
    public Iterator<KeyValueVersion>
        storeIterator(final Direction direction,
                      final int batchSize,
                      final Key parentKey,
                      final KeyRange subRange,
                      final Depth depth,
                      final Consistency consistency,
                      final long timeout,
                      final TimeUnit timeoutUnit)
        throws FaultException {

        return storeIterator(direction, batchSize, 1, nPartitions, parentKey,
                             subRange, depth, consistency, timeout,
                             timeoutUnit);
    }

    @Override
    public ParallelScanIterator<KeyValueVersion>
        storeIterator(final Direction direction,
                      final int batchSize,
                      final Key parentKey,
                      final KeyRange subRange,
                      final Depth depth,
                      final Consistency consistency,
                      final long timeout,
                      final TimeUnit timeoutUnit,
                      final StoreIteratorConfig storeIteratorConfig)
        throws FaultException {

        if (storeIteratorConfig == null) {
            throw new IllegalArgumentException
                ("The StoreIteratorConfig argument must be supplied.");
        }

        return ParallelScan.
            createParallelScan(this, direction, batchSize, parentKey, subRange,
                               depth, consistency, timeout, timeoutUnit,
                               storeIteratorConfig, excludeTombstones);
    }

    /**
     * Internal use only.  Iterates using the same rules as storeIterator,
     * but over the single, given partition.
     *
     * @param direction the direction may be {@link Direction#FORWARD} or
     * {@link Direction#UNORDERED} though keys are always returned in forward
     * order for the given partition.  In the future we may support a faster,
     * unordered iteration.
     */
    public Iterator<KeyValueVersion>
        partitionIterator(final Direction direction,
                          final int batchSize,
                          final int partition,
                          final Key parentKey,
                          final KeyRange subRange,
                          final Depth depth,
                          final Consistency consistency,
                          final long timeout,
                          final TimeUnit timeoutUnit)
        throws FaultException {

        if (direction != Direction.FORWARD &&
            direction != Direction.UNORDERED) {
            throw new IllegalArgumentException
                ("Only Direction.FORWARD or Direction.UNORDERED is currently " +
                 "supported, got: " + direction);
        }

        return storeIterator(Direction.UNORDERED, batchSize, partition,
                             partition, parentKey, subRange, depth,
                             consistency, timeout, timeoutUnit);
    }

    private Iterator<KeyValueVersion>
        storeIterator(final Direction direction,
                      final int batchSize,
                      final int firstPartition,
                      final int lastPartition,
                      final Key parentKey,
                      final KeyRange subRange,
                      final Depth depth,
                      final Consistency consistency,
                      final long timeout,
                      final TimeUnit timeoutUnit)
        throws FaultException {

        if (direction != Direction.UNORDERED) {
            throw new IllegalArgumentException
                ("Only Direction.UNORDERED is currently supported, got: " +
                 direction);
        }

        if ((parentKey != null) && (parentKey.getMinorPath().size()) > 0) {
            throw new IllegalArgumentException
                ("Minor path of parentKey must be empty");
        }

        final Depth useDepth =
            (depth != null) ? depth : Depth.PARENT_AND_DESCENDANTS;

        final int useBatchSize =
            (batchSize > 0) ? batchSize : DEFAULT_ITERATOR_BATCH_SIZE;

        final byte[] parentKeyBytes =
            (parentKey != null) ? keySerializer.toByteArray(parentKey) : null;

        /* Prohibit iteration of internal keyspace (//). */
        final KeyRange useRange =
            keySerializer.restrictRange(parentKey, subRange);

        return new ArrayIterator<KeyValueVersion>() {
            private boolean moreElements = true;
            private byte[] resumeKey = null;
            private PartitionId partitionId = new PartitionId(firstPartition);

            @Override
            KeyValueVersion[] getMoreElements() {

                while (true) {
                    /* If no more in one partition, move to the next. */
                    if ((!moreElements) &&
                        (partitionId.getPartitionId() < lastPartition)) {
                        partitionId =
                            new PartitionId(partitionId.getPartitionId() + 1);
                        moreElements = true;
                        resumeKey = null;
                    }

                    /* Avoid round trip when there are no more elements. */
                    if (!moreElements) {
                        return null;
                    }

                    /* Execute request. */
                    final StoreIterate get = new StoreIterate
                        (parentKeyBytes, useRange, useDepth, Direction.FORWARD,
                         useBatchSize, resumeKey, excludeTombstones);
                    final Request req = makeReadRequest
                        (get, partitionId, consistency,
                         timeout, timeoutUnit);
                    final Result result = executeRequest(req);

                    /* Get results and save resume key. */
                    moreElements = result.hasMoreElements();
                    final List<ResultKeyValueVersion> byteKeyResults =
                        result.getKeyValueVersionList();
                    if (byteKeyResults.size() == 0) {
                        assert (!moreElements);
                        continue;
                    }
                    resumeKey = byteKeyResults.get
                        (byteKeyResults.size() - 1).getKeyBytes();

                    /* Convert byte[] keys to Key objects. */
                    final KeyValueVersion[] stringKeyResults =
                        new KeyValueVersion[byteKeyResults.size()];
                    for (int i = 0; i < stringKeyResults.length; i += 1) {
                        final ResultKeyValueVersion entry =
                            byteKeyResults.get(i);
                        stringKeyResults[i] = createKeyValueVersion
                            (keySerializer.fromByteArray(entry.getKeyBytes()),
                             entry.getValue(), entry.getVersion(),
                             entry.getExpirationTime(),
                             entry.getModificationTime());
                    }
                    return stringKeyResults;
                }
            }
        };
    }

    @Override
    public Iterator<Key> storeKeysIterator(Direction direction,
                                           int batchSize)
        throws FaultException {

        return storeKeysIterator(direction, batchSize, null, null, null,
                                 null, 0, null);
    }

    @Override
    public Iterator<Key> storeKeysIterator(Direction direction,
                                           int batchSize,
                                           Key parentKey,
                                           KeyRange subRange,
                                           Depth depth)
        throws FaultException {

        return storeKeysIterator(direction, batchSize, parentKey, subRange,
                                 depth, null, 0, null);
    }

    @Override
    public Iterator<Key> storeKeysIterator(final Direction direction,
                                           final int batchSize,
                                           final Key parentKey,
                                           final KeyRange subRange,
                                           final Depth depth,
                                           final Consistency consistency,
                                           final long timeout,
                                           final TimeUnit timeoutUnit)
        throws FaultException {

        if (direction != Direction.UNORDERED) {
            throw new IllegalArgumentException
                ("Only Direction.UNORDERED is currently supported, got: " +
                 direction);
        }

        if ((parentKey != null) && (parentKey.getMinorPath().size()) > 0) {
            throw new IllegalArgumentException
                ("Minor path of parentKey must be empty");
        }

        final Depth useDepth =
            (depth != null) ? depth : Depth.PARENT_AND_DESCENDANTS;

        final int useBatchSize =
            (batchSize > 0) ? batchSize : DEFAULT_ITERATOR_BATCH_SIZE;

        final byte[] parentKeyBytes =
            (parentKey != null) ? keySerializer.toByteArray(parentKey) : null;

        /* Prohibit iteration of internal keyspace (//). */
        final KeyRange useRange =
            keySerializer.restrictRange(parentKey, subRange);

        return new ArrayIterator<Key>() {
            private boolean moreElements = true;
            private byte[] resumeKey = null;
            private PartitionId partitionId = new PartitionId(1);

            @Override
            Key[] getMoreElements() {

                while (true) {
                    /* If no more in one partition, move to the next. */
                    if ((!moreElements) &&
                        (partitionId.getPartitionId() < nPartitions)) {
                        partitionId =
                            new PartitionId(partitionId.getPartitionId() + 1);
                        moreElements = true;
                        resumeKey = null;
                    }

                    /* Avoid round trip when there are no more elements. */
                    if (!moreElements) {
                        return null;
                    }

                    /* Execute request. */
                    final StoreKeysIterate get = new StoreKeysIterate
                        (parentKeyBytes, useRange, useDepth, Direction.FORWARD,
                         useBatchSize, resumeKey, excludeTombstones);
                    final Request req = makeReadRequest
                        (get, partitionId, consistency,
                         timeout, timeoutUnit);
                    final Result result = executeRequest(req);

                    /* Get results and save resume key. */
                    moreElements = result.hasMoreElements();
                    final List<ResultKey> byteKeyResults = result.getKeyList();
                    if (byteKeyResults.size() == 0) {
                        assert (!moreElements);
                        continue;
                    }
                    resumeKey = byteKeyResults.
                        get(byteKeyResults.size() - 1).getKeyBytes();

                    /* Convert byte[] keys to Key objects. */
                    final Key[] stringKeyResults =
                        new Key[byteKeyResults.size()];
                    for (int i = 0; i < stringKeyResults.length; i += 1) {
                        final byte[] entry =
                            byteKeyResults.get(i).getKeyBytes();
                        stringKeyResults[i] =
                            keySerializer.fromByteArray(entry);
                    }
                    return stringKeyResults;
                }
            }
        };
    }

    @Override
    public ParallelScanIterator<Key>
        storeKeysIterator(final Direction direction,
                          final int batchSize,
                          final Key parentKey,
                          final KeyRange subRange,
                          final Depth depth,
                          final Consistency consistency,
                          final long timeout,
                          final TimeUnit timeoutUnit,
                          final StoreIteratorConfig storeIteratorConfig)
        throws FaultException {

        if (storeIteratorConfig == null) {
            throw new IllegalArgumentException
                ("The StoreIteratorConfig argument must be supplied.");
        }

        return ParallelScan.
            createParallelKeyScan(this, direction, batchSize,
                                  parentKey, subRange,
                                  depth, consistency, timeout, timeoutUnit,
                                  storeIteratorConfig, excludeTombstones);
    }

    @Override
    public ParallelScanIterator<KeyValueVersion>
        storeIterator(final Iterator<Key> parentKeyiterator,
                      final int batchSize,
                      final KeyRange subRange,
                      final Depth depth,
                      final Consistency consistency,
                      final long timeout,
                      final TimeUnit timeoutUnit,
                      final StoreIteratorConfig storeIteratorConfig)
        throws FaultException {

        if (parentKeyiterator == null) {
            throw new IllegalArgumentException("The parent key iterator " +
                                               "argument should not be null.");
        }

        final List<Iterator<Key>> parentKeyiterators =
            Arrays.asList(parentKeyiterator);

        return storeIterator(parentKeyiterators, batchSize, subRange,
                             depth, consistency, timeout, timeoutUnit,
                             storeIteratorConfig);
    }

    @Override
    public ParallelScanIterator<Key>
        storeKeysIterator(final Iterator<Key> parentKeyiterator,
                          final int batchSize,
                          final KeyRange subRange,
                          final Depth depth,
                          final Consistency consistency,
                          final long timeout,
                          final TimeUnit timeoutUnit,
                          final StoreIteratorConfig storeIteratorConfig)
        throws FaultException {

        if (parentKeyiterator == null) {
            throw new IllegalArgumentException("The parent key iterator " +
                                               "argument should not be null.");
        }

        final List<Iterator<Key>> parentKeyiterators =
            Arrays.asList(parentKeyiterator);

        return storeKeysIterator(parentKeyiterators, batchSize, subRange,
                                 depth, consistency, timeout, timeoutUnit,
                                 storeIteratorConfig);
    }

    @Override
    public ParallelScanIterator<KeyValueVersion>
        storeIterator(final List<Iterator<Key>> parentKeyIterators,
                      final int batchSize,
                      final KeyRange subRange,
                      final Depth depth,
                      final Consistency consistency,
                      final long timeout,
                      final TimeUnit timeoutUnit,
                      final StoreIteratorConfig storeIteratorConfig)
        throws FaultException {

        if (parentKeyIterators == null || parentKeyIterators.isEmpty()) {
            throw new IllegalArgumentException("The key iterator list cannot " +
                "be null or empty.");
        }

        if (parentKeyIterators.contains(null)) {
            throw new IllegalArgumentException("Elements of key iterator " +
                "list must not be null.");
        }

        return BulkMultiGet.createBulkMultiGetIterator(this, parentKeyIterators,
                                                       batchSize, subRange,
                                                       depth, consistency,
                                                       timeout, timeoutUnit,
                                                       storeIteratorConfig,
                                                       excludeTombstones);
    }

    @Override
    public ParallelScanIterator<Key>
        storeKeysIterator(final List<Iterator<Key>> parentKeyIterators,
                          final int batchSize,
                          final KeyRange subRange,
                          final Depth depth,
                          final Consistency consistency,
                          final long timeout,
                          final TimeUnit timeoutUnit,
                          final StoreIteratorConfig storeIteratorConfig)
        throws FaultException {

        if (parentKeyIterators == null || parentKeyIterators.isEmpty()) {
            throw new IllegalArgumentException("The key iterator list cannot " +
                "be null or empty.");
        }

        if (parentKeyIterators.contains(null)) {
            throw new IllegalArgumentException("Elements of key iterator " +
                "must not be null.");
        }

        return BulkMultiGet.createBulkMultiGetKeysIterator(this,
                                                           parentKeyIterators,
                                                           batchSize, subRange,
                                                           depth, consistency,
                                                           timeout, timeoutUnit,
                                                           storeIteratorConfig,
                                                           excludeTombstones);
    }

    public static TestHook<Integer> cacheTestHook;

    /**
     * Returns the next identity value.
     *
     * This is done using 2 cashes:
     *   - sgAttributes: it is a timed cache for the attributes. This keeps
     *   the attributes of the sequence generators for the set time.
     *   After this time the cache expires and the value in the cache will be
     *   null. In this case the next call to the server will ask for the
     *   attributes too.
     *   - sgValues: contains the next cached values from the server. The
     *   client driver will continue to return next sequence values without
     *   contacting the server if they are still available in sgValues.
     *
     * In the event that the attributes of a sequence are changed on the
     * server the driver will be notified the next time it contacts the server
     * with regard to the sequence. This is done using a version number of the
     * sequence options. The driver sends SGAttributes.version to the server
     * every time it needs values, if the version on the server is not the same
     * the response will also contain the new set of options.
     *
     * Note: The timeout for the sgAttributes cache has a default of 5
     * minutes, or it can be set using
     * {@link KVStoreConfig#setSGAttrsCacheTimeout(int)} when creating store.
     *
     * Note: The number of values cached on the client can be set using the
     * CACHE option when creating or altering a table or by using
     * {@link WriteOptions#setIdentityCacheSize(int)} when calling on of
     * the put methods. If the client doesn't ask for a certain cache size
     * the server size (set by CACHE option or 1000 if not specified) is used.
     */
    public FieldValueImpl getIdentityNextValue(TableImpl table,
        FieldDefImpl fieldDef,
        int clientIdentityCacheSize,
        ValueSerializer.FieldValueSerializer userValue,
        int fieldPos) {

        SequenceImpl.SGKey sKey = new SequenceImpl.SGKey(
            SGType.INTERNAL,
            SequenceImpl.getSgName(table, fieldPos),
            table.getId());

        SequenceImpl.SGValues<?> sValues = sgValues.get(sKey);
        if (sValues == null) {
            synchronized (sgValues) {
                sValues = sgValues.get(sKey);

                if (sValues == null) {
                    sValues = SGValues.newInstance(fieldDef.getType(), 0L);
                    sgValues.put(sKey, sValues);
                }
            }
        }

        /* if not set by user ignore the value */
        boolean isGenerateAlways = table.isIdentityGeneratedAlways();
        boolean isOnNull = table.isIdentityOnNull();

        /*
         * If onNull, no need to generate if userValue is not null and
         * userValue.isNull() is false.
         * If byDefault, no need to generate if userValue is not null.
         */
        if ((isOnNull && userValue != null && !userValue.isNull() &&
            !userValue.isEMPTY()) ||
            (!isOnNull && !isGenerateAlways && userValue != null &&
                !userValue.isEMPTY() )) {
            return null;
        }

        /*increment value*/
        synchronized (sValues) {
            SGAttributes identityAttrs = sgAttributes.get(sKey);

            /* If generateAlways and userValue is not null then it is error.
              Error is thrown in when Row.put() is called. */

            FieldValueImpl result;

            if (!fieldDef.getType().equals(sValues.getType())) {
                throw new IllegalStateException("The datatype stored in the " +
                    "cache does not match the datatype in the metadata.");
            }

            switch (fieldDef.getType()) {
            case INTEGER:
                result = integerIncrement(identityAttrs,
                    clientIdentityCacheSize,
                    sKey, (SGIntegerValues)sValues);
                break;

            case LONG:
                result = longIncrement(identityAttrs, clientIdentityCacheSize,
                    sKey, (SGLongValues)sValues);
                break;

            case NUMBER:
                result = numberIncrement(identityAttrs, clientIdentityCacheSize,
                    sKey, (SGNumberValues)sValues);
                break;

            default:
                throw new IllegalStateException(
                    "Unsupported type for identity sequence generator: " +
                        fieldDef.getType());
            }

            return result;
        }
    }

    private FieldValueImpl integerIncrement(SGAttributes identityAttrs,
                                        int clientIdentityCacheSize,
                                        SGKey sKey,
                                        SGIntegerValues sValues) {

        boolean needsAttributes = identityAttrs == null;
        SGAttrsAndValues result;

        if (sValues.currentValue == null) {
            /* First time we need values. Ask for values and attrs */
            result = getIdentityAttrsAndValues(sKey, -1,
                                               clientIdentityCacheSize,
                                               true, true);
        } else {
            /* have sgValues */
            boolean incPos = sValues.increment > 0;

            int nextValue = (sValues.currentValue + (int)sValues.increment);

            boolean overflow =
                (incPos && nextValue < sValues.currentValue) ||
                (!incPos && nextValue > sValues.currentValue);

            if (overflow ||
                (incPos && nextValue > sValues.lastValue ) ||
                (!incPos && nextValue < sValues.lastValue )) {

                /* ran out of sgValues */
                result = getIdentityAttrsAndValues(sKey, sValues.attrVersion,
                                                   clientIdentityCacheSize,
                                                   needsAttributes, true);
            } else {
                /* The cache still has numbers */
                sValues.currentValue = nextValue;

                if (identityAttrs == null) {
                    /* attributes have expired */
                    result = getIdentityAttrsAndValues(sKey,
                                                       sValues.attrVersion,
                                                       clientIdentityCacheSize,
                                                       true, false);

                    if (result != null && result.containsAttributes()) {
                        /* reset attribute cache */
                        sgAttributes.put(sKey, result.getAttributes());
                        sValues.attrVersion = result.getAttributes().getVersion();
                    }

                    if (result != null && !result.containsValues()) {
                        return (FieldValueImpl) FieldValueFactory.
                            createInteger(nextValue);
                    }
                } else {
                    return (FieldValueImpl) FieldValueFactory.
                        createInteger(nextValue);
                }
            }
        }

        if (result != null && result.containsAttributes()) {
            /* reset attribute cache */
            sgAttributes.put(sKey, result.getAttributes());
            sValues.attrVersion = result.getAttributes().getVersion();
        }

        if (result != null && result.containsValues()) {
            /* reset value cache */
            if (!(result.getValues() instanceof SGIntegerValues)) {
                throw new IllegalStateException("The cache values should " +
                    "be Integer.");
            }
            final SGIntegerValues newCache =
                    (SGIntegerValues)result.getValues();
            sValues.currentValue = newCache.currentValue;
            sValues.increment = newCache.increment;
            sValues.lastValue = newCache.lastValue;

            assert TestHookExecute.doHookIfSet(cacheTestHook,
                sValues.lastValue - sValues.currentValue);

            return (FieldValueImpl) FieldValueFactory.createInteger(sValues.currentValue);
        }

        return null;
    }

    private FieldValueImpl longIncrement(SGAttributes identityAttrs,
                                     int clientIdentityCacheSize,
                                     SGKey sKey,
                                     SGLongValues sValues) {

        boolean needsAttributes = identityAttrs == null;
        SGAttrsAndValues result;

        if (sValues.currentValue == null) {
            /* no sgValues, get some */
            result = getIdentityAttrsAndValues(sKey, -1,
                                               clientIdentityCacheSize,
                                               true, true);
        } else {
            /* have sgValues */
            boolean incPos = sValues.increment > 0;

            long nextValue = sValues.currentValue + sValues.increment;

            boolean overflow =
                (incPos && nextValue < sValues.currentValue) ||
                (!incPos && nextValue > sValues.currentValue);

            if (overflow ||
                (incPos && nextValue > sValues.lastValue ) ||
                (!incPos && nextValue < sValues.lastValue)) {

                /* ran out of sgValues */
                result = getIdentityAttrsAndValues(sKey, sValues.attrVersion,
                                                   clientIdentityCacheSize,
                                                   needsAttributes, true);
            } else {
                /* The cache still has numbers */
                sValues.currentValue = nextValue;

                if (identityAttrs == null) {
                    result = getIdentityAttrsAndValues(sKey,
                                                       sValues.attrVersion,
                                                       clientIdentityCacheSize,
                                                       true, false);

                    if (result != null && result.containsAttributes()) {
                        /* reset attribute cache */
                        sgAttributes.put(sKey, result.getAttributes());
                        sValues.attrVersion = result.getAttributes().getVersion();
                    }

                    if (result != null && !result.containsValues()) {
                        return (FieldValueImpl) FieldValueFactory.createLong(nextValue);
                    }
                } else {
                    return (FieldValueImpl) FieldValueFactory.createLong(nextValue);
                }
            }
        }

        if (result != null && result.containsAttributes()) {
            /* reset attribute cache */
            sgAttributes.put(sKey, result.getAttributes());
            sValues.attrVersion = result.getAttributes().getVersion();
        }

        if (result != null && result.containsValues()) {
            /* reset value cache */
            if (!(result.getValues() instanceof SGLongValues)) {
                throw new IllegalStateException("The cache values should " +
                    "be Long.");
            }
            final SGLongValues newCache = (SGLongValues)result.getValues();
            sValues.currentValue = newCache.currentValue;
            sValues.increment = newCache.increment;
            sValues.lastValue = newCache.lastValue;

            return (FieldValueImpl) FieldValueFactory.createLong(sValues.currentValue);
        }

        return null;
    }


    private FieldValueImpl numberIncrement(SGAttributes identityAttrs,
                                       int clientIdentityCacheSize,
                                       SGKey sKey,
                                       SGNumberValues sValues) {

        boolean needsAttributes = identityAttrs == null;
        SGAttrsAndValues result;

        if (sValues.currentValue == null) {
            /* no sgValues, get some */
            result = getIdentityAttrsAndValues(sKey, -1,
                                               clientIdentityCacheSize,
                                               true, true);
        } else {
            /* have sgValues */
            boolean incPos = sValues.increment > 0;

            BigDecimal nextValue =  sValues.currentValue.
                                    add(new BigDecimal(sValues.increment));

            if ((incPos &&
                 nextValue.compareTo(sValues.lastValue) == 1) ||
                (!incPos &&
                 nextValue.compareTo(sValues.lastValue) == -1)) {
                /* ran out of sgValues */
                result = getIdentityAttrsAndValues(sKey, sValues.attrVersion,
                                                   clientIdentityCacheSize,
                                                   needsAttributes,
                                                   true);
            } else {
                /* The cache still has numbers */
                sValues.currentValue = nextValue;

                if (identityAttrs == null) {
                    result = getIdentityAttrsAndValues(sKey,
                                                       sValues.attrVersion,
                                                       clientIdentityCacheSize,
                                                       true, false);

                    if (result != null && result.containsAttributes()) {
                        /* reset attribute cache */
                        sgAttributes.put(sKey, result.getAttributes());
                        sValues.attrVersion = result.getAttributes().getVersion();
                    }

                    if (result != null && !result.containsValues()) {
                        return (FieldValueImpl) FieldValueFactory.createNumber(nextValue);
                    }
                } else {
                    return (FieldValueImpl) FieldValueFactory.createNumber(nextValue);
                }
            }
        }

        if (result != null && result.containsAttributes()) {
            /* reset attribute cache */
            sgAttributes.put(sKey, result.getAttributes());
            sValues.attrVersion = result.getAttributes().getVersion();
        }

        if (result != null && result.containsValues()) {
            /* reset value cache */
            if (!(result.getValues() instanceof SGNumberValues)) {
                throw new IllegalStateException("The cache values should " +
                    "be Number.");
            }
            final SGNumberValues newCache = (SGNumberValues)result.getValues();
            sValues.currentValue = newCache.currentValue;
            sValues.increment = newCache.increment;
            sValues.lastValue = newCache.lastValue;

            return (FieldValueImpl) FieldValueFactory.
                createNumber(sValues.currentValue);
        }

        return null;
    }

    /* Contacts the server to get attributes and/or more sequence numbers. */
    public SGAttrsAndValues
        getIdentityAttrsAndValues(SGKey key,
                                  long curVersion,
                                  int clientIdentityCacheSize,
                                  boolean getAttributes,
                                  boolean getNextSequence) {
        Table sysTable = tableAPI.getTable(SGAttributesTableDesc.TABLE_NAME);
        PrimaryKey pk = sysTable.createPrimaryKey();
        pk.put(SGAttributesTableDesc.COL_NAME_SGTYPE, key.sgType.name());
        pk.put(SGAttributesTableDesc.COL_NAME_SGNAME, key.sgName);
        Key sysKey = ((TableImpl)sysTable).createKeyInternal((RowSerializer)pk,
                                                             false);
        Request request = makeGetIdentityRequest(sysKey, curVersion,
                                                 clientIdentityCacheSize,
                                                 getAttributes, getNextSequence,
                                                 key.sgName,
                                                 Durability.COMMIT_SYNC, 0,
                                                 null);
        Result result = executeRequestWithPrev(request, null);
        SGAttrsAndValues attsAndValues =  ((Result.GetIdentityResult)result).
            getSGAttrsAndValues();

        return attsAndValues;
    }

    private abstract class ArrayIterator<E> implements Iterator<E> {

        private E[] elements = null;
        private int nextElement = 0;

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() {
            if (elements != null && nextElement < elements.length) {
                return true;
            }
            elements = getMoreElements();
            if (elements == null) {
                return false;
            }
            assert (elements.length > 0);
            nextElement = 0;
            return true;
        }

        @Override
        public E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return elements[nextElement++];
        }

        /**
         * Returns more elements or null if there are none.  May not return a
         * zero length array.
         */
        abstract E[] getMoreElements();
    }

    public static Version getPutResult(Result result) {
        assert result.getSuccess() == (result.getNewVersion() != null);
        return result.getNewVersion();
    }

    /**
     * Returns the Choice associated with a ReturnValueVersion, returning NONE
     * if the argument is null.
     */
    public static ReturnValueVersion.Choice getReturnChoice(
        ReturnValueVersion prevValue)
    {
        return (prevValue != null) ?
            prevValue.getReturnChoice() :
            ReturnValueVersion.Choice.NONE;
    }

    public Request makePutRequest(InternalOperation.OpCode opCode,
                                  Key key,
                                  Value value,
                                  ReturnValueVersion.Choice returnChoice,
                                  long tableId,
                                  Durability durability,
                                  long timeout,
                                  TimeUnit timeoutUnit,
                                  TimeToLive ttl,
                                  boolean updateTTL,
                                  Version matchVersion) {

        final byte[] keyBytes = keySerializer.toByteArray(key);
        final PartitionId partitionId = dispatcher.getPartitionId(keyBytes);
        Put put = null;
        switch (opCode) {
        case PUT:
            put = new Put(keyBytes, value, returnChoice, tableId,
                          ttl, updateTTL, false /*isSQLUpdate*/);
            break;
        case PUT_IF_ABSENT:
            put = new PutIfAbsent(keyBytes, value, returnChoice, tableId,
                                  ttl, updateTTL);
            break;
        case PUT_IF_PRESENT:
            put = new PutIfPresent(keyBytes, value, returnChoice, tableId,
                                   ttl, updateTTL);
            break;
        case PUT_IF_VERSION:
            put = new PutIfVersion(keyBytes, value, returnChoice, matchVersion,
                                   tableId, ttl, updateTTL);
            break;
        default:
            throw new IllegalStateException(
                "Invalid operation for put: " + opCode);

        }
        return makeWriteRequest(put, partitionId, durability, timeout,
                                timeoutUnit);
    }

    /*
     * Handle all "local" or internal put request types that return
     * Version
     *
     * This is public for a test; otherwise it'd be private
     */
    public Version doPutInternal(Request putRequest,
                                 ReturnValueVersion prevValue) {
        Result result = executeRequestWithPrev(putRequest, prevValue);
        return getPutResult(result);
    }

    public Result executeRequestWithPrev(Request req,
                                         ReturnValueVersion prevValue)
        throws FaultException {

        final Result result = executeRequest(req);
        return resultSetPreviousValue(result, prevValue);
    }

    public static Result resultSetPreviousValue(Result result,
                                                ReturnValueVersion prevValue) {
        if (prevValue != null) {
            prevValue.setValue(result.getPreviousValue());
            prevValue.setVersion(result.getPreviousVersion());
        }
        return result;
    }

    @Override
    public Version put(Key key,
                       Value value)
        throws FaultException {

        return put(key, value, null, null, 0, null);
    }

    @Override
    public Version put(Key key,
                       Value value,
                       ReturnValueVersion prevValue,
                       Durability durability,
                       long timeout,
                       TimeUnit timeoutUnit)
        throws FaultException {

        return doPutInternal(
            makePutRequest(InternalOperation.OpCode.PUT,
                           key,
                           value,
                           getReturnChoice(prevValue),
                           0,
                           durability,
                           timeout,
                           timeoutUnit,
                           null,
                           false,
                           null),
            prevValue);
    }

    @Override
    public Version putIfAbsent(Key key,
                               Value value)
        throws FaultException {

        return putIfAbsent(key, value, null, null, 0, null);
    }

    @Override
    public Version putIfAbsent(Key key,
                               Value value,
                               ReturnValueVersion prevValue,
                               Durability durability,
                               long timeout,
                               TimeUnit timeoutUnit)
        throws FaultException {

        return doPutInternal(
            makePutRequest(InternalOperation.OpCode.PUT_IF_ABSENT,
                           key,
                           value,
                           getReturnChoice(prevValue),
                           0,
                           durability,
                           timeout,
                           timeoutUnit,
                           null,
                           false,
                           null),
            prevValue);
    }

    @Override
    public Version putIfPresent(Key key,
                                Value value)
        throws FaultException {

        return putIfPresent(key, value, null, null, 0, null);
    }

    @Override
    public Version putIfPresent(Key key,
                                Value value,
                                ReturnValueVersion prevValue,
                                Durability durability,
                                long timeout,
                                TimeUnit timeoutUnit)
        throws FaultException {

        return doPutInternal(
            makePutRequest(InternalOperation.OpCode.PUT_IF_PRESENT,
                           key,
                           value,
                           getReturnChoice(prevValue),
                           0,
                           durability,
                           timeout,
                           timeoutUnit,
                           null,
                           false,
                           null),
            prevValue);
    }

    @Override
    public Version putIfVersion(Key key,
                                Value value,
                                Version matchVersion)
        throws FaultException {

        return putIfVersion(key, value, matchVersion, null, null, 0, null);
    }

    @Override
    public Version putIfVersion(Key key,
                                Value value,
                                Version matchVersion,
                                ReturnValueVersion prevValue,
                                Durability durability,
                                long timeout,
                                TimeUnit timeoutUnit)
        throws FaultException {

        return doPutInternal(
            makePutRequest(InternalOperation.OpCode.PUT_IF_VERSION,
                           key,
                           value,
                           getReturnChoice(prevValue),
                           0,
                           durability,
                           timeout,
                           timeoutUnit,
                           null,
                           false,
                           matchVersion),
            prevValue);
    }

    public Request makePutResolveRequest(
        Key key,
        Value value,
        long tableId,
        ReturnValueVersion.Choice returnChoice,
        Durability durability,
        long timeout,
        TimeUnit timeoutUnit,
        long expTime,
        boolean updateTTL,
        boolean isTombstone,
        long timestamp,
        int regionId) {
        final byte[] keyBytes = keySerializer.toByteArray(key);
        final PartitionId partitionId = dispatcher.getPartitionId(keyBytes);
        final PutResolve putResolve = new PutResolve(keyBytes, value, tableId,
                                                     returnChoice,
                                                     expTime,
                                                     updateTTL,
                                                     isTombstone, timestamp,
                                                     regionId);

        return makeWriteRequest(putResolve, partitionId, durability, timeout,
                                timeoutUnit);
    }

    @Override
    public boolean delete(Key key)
        throws FaultException {

        return delete(key, null, null, 0, null);
    }

    @Override
    public boolean delete(Key key,
                          ReturnValueVersion prevValue,
                          Durability durability,
                          long timeout,
                          TimeUnit timeoutUnit)
        throws FaultException {

        return deleteInternal(key, prevValue, durability,
                              timeout, timeoutUnit, 0);
    }

    public boolean deleteInternal(Key key,
                                  ReturnValueVersion prevValue,
                                  Durability durability,
                                  long timeout,
                                  TimeUnit timeoutUnit,
                                  long tableId)
        throws FaultException {

        return deleteInternalResult(key,
                                    prevValue,
                                    durability,
                                    timeout,
                                    timeoutUnit,
                                    tableId).getSuccess();
    }

    public Result deleteInternalResult(Key key,
                                       ReturnValueVersion prevValue,
                                       Durability durability,
                                       long timeout,
                                       TimeUnit timeoutUnit,
                                       long tableId)
        throws FaultException {

        final Request req = makeDeleteRequest(key, getReturnChoice(prevValue),
                                              durability, timeout, timeoutUnit,
                                              tableId, false /* doTombstone */);
        return executeRequestWithPrev(req, prevValue);
    }

    public Request makeDeleteRequest(Key key,
                                     ReturnValueVersion.Choice returnChoice,
                                     Durability durability,
                                     long timeout,
                                     TimeUnit timeoutUnit,
                                     long tableId,
                                     boolean doTombstone) {
        final byte[] keyBytes = keySerializer.toByteArray(key);
        final PartitionId partitionId = dispatcher.getPartitionId(keyBytes);
        final Delete del = new Delete(keyBytes, returnChoice, tableId,
                                      doTombstone);
        return makeWriteRequest(del, partitionId, durability, timeout,
                                timeoutUnit);
    }

    @Override
    public boolean deleteIfVersion(Key key,
                                   Version matchVersion)
        throws FaultException {

        return deleteIfVersion(key, matchVersion, null, null, 0, null);
    }

    @Override
    public boolean deleteIfVersion(Key key,
                                   Version matchVersion,
                                   ReturnValueVersion prevValue,
                                   Durability durability,
                                   long timeout,
                                   TimeUnit timeoutUnit)
        throws FaultException {

        return deleteIfVersionInternal(key, matchVersion, prevValue,
                                       durability, timeout, timeoutUnit, 0);
    }

    public boolean deleteIfVersionInternal(Key key,
                                           Version matchVersion,
                                           ReturnValueVersion prevValue,
                                           Durability durability,
                                           long timeout,
                                           TimeUnit timeoutUnit,
                                           long tableId)
        throws FaultException {

        return deleteIfVersionInternalResult(key,
                                             matchVersion,
                                             prevValue,
                                             durability,
                                             timeout,
                                             timeoutUnit,
                                             tableId).getSuccess();
    }

    public Result deleteIfVersionInternalResult(Key key,
                                                Version matchVersion,
                                                ReturnValueVersion prevValue,
                                                Durability durability,
                                                long timeout,
                                                TimeUnit timeoutUnit,
                                                long tableId)
        throws FaultException {

        final Request req = makeDeleteIfVersionRequest(
            key, matchVersion, getReturnChoice(prevValue), durability,
            timeout, timeoutUnit, tableId, false /* doTombstone */);
        return executeRequestWithPrev(req, prevValue);
    }

    public Request makeDeleteIfVersionRequest(
        Key key,
        Version matchVersion,
        ReturnValueVersion.Choice returnChoice,
        Durability durability,
        long timeout,
        TimeUnit timeoutUnit,
        long tableId,
        boolean doTombstone)
    {
        final byte[] keyBytes = keySerializer.toByteArray(key);
        final PartitionId partitionId = dispatcher.getPartitionId(keyBytes);
        final Delete del = new DeleteIfVersion(keyBytes, returnChoice,
                                               matchVersion, tableId,
                                               doTombstone);
        return makeWriteRequest(del, partitionId, durability, timeout,
                                timeoutUnit);
    }

    @Override
    public int multiDelete(Key parentKey,
                           KeyRange subRange,
                           Depth depth)
        throws FaultException {

        return multiDelete(parentKey, subRange, depth, null, 0, null);
    }

    @Override
    public int multiDelete(Key parentKey,
                           KeyRange subRange,
                           Depth depth,
                           Durability durability,
                           long timeout,
                           TimeUnit timeoutUnit)
        throws FaultException {

        if (depth == null) {
            depth = Depth.PARENT_AND_DESCENDANTS;
        }

        final byte[] parentKeyBytes = keySerializer.toByteArray(parentKey);
        final PartitionId partitionId =
            dispatcher.getPartitionId(parentKeyBytes);
        final MultiDelete del =
            new MultiDelete(parentKeyBytes, subRange,
                            depth,
                            largeObjectImpl.getLOBSuffixBytes());
        final Request req = makeWriteRequest(del, partitionId, durability,
                                             timeout, timeoutUnit);
        final Result result = executeRequest(req);
        return result.getNDeletions();
    }

    @Override
    public List<OperationResult> execute(List<Operation> operations)
        throws OperationExecutionException,
               FaultException {

        return execute(operations, null, 0, null);
    }

    @Override
    public List<OperationResult> execute(List<Operation> operations,
                                         Durability durability,
                                         long timeout,
                                         TimeUnit timeoutUnit)
        throws OperationExecutionException,
               FaultException {

        Result result = executeInternal(operations, 0, durability,
                                        timeout, timeoutUnit);
        return result.getExecuteResult();
    }

    /**
     * Internal use only
     *
     * Public for use by cloud proxy
     */
    public Result executeInternal(List<Operation> operations,
                                  long tableId,
                                  Durability durability,
                                  long timeout,
                                  TimeUnit timeoutUnit)
        throws OperationExecutionException,
               FaultException {

        final Request req = makeExecuteRequest(operations, tableId, durability,
                                               timeout, timeoutUnit);
        return processExecuteResult(executeRequest(req), operations);
    }

    public Request makeExecuteRequest(List<Operation> operations,
                                      long tableId,
                                      Durability durability,
                                      long timeout,
                                      TimeUnit timeoutUnit) {
        /* Validate operations. */
        final List<OperationImpl> ops = OperationImpl.downcast(operations);
        if (ops == null || ops.size() == 0) {
            throw new IllegalArgumentException
                ("operations must be non-null and non-empty");
        }
        final OperationImpl firstOp = ops.get(0);
        final List<String> firstMajorPath = firstOp.getKey().getMajorPath();
        final Set<Key> keySet = new HashSet<Key>();
        keySet.add(firstOp.getKey());
        checkLOBKeySuffix(firstOp.getInternalOp());
        for (int i = 1; i < ops.size(); i += 1) {
            final OperationImpl op = ops.get(i);
            final Key opKey = op.getKey();
            if (!opKey.getMajorPath().equals(firstMajorPath) &&
                getNPartitions() != 1) {
                throw new IllegalArgumentException
                    ("Two operations have different major paths, first: " +
                     firstOp.getKey() + " other: " + opKey);
            }
            if (keySet.add(opKey) == false) {
                throw new IllegalArgumentException
                    ("More than one operation has the same Key: " + opKey);
            }
            checkLOBKeySuffix(op.getInternalOp());
        }

        /* Execute the execute. */
        final PartitionId partitionId =
            dispatcher.getPartitionId(firstOp.getInternalOp().getKeyBytes());
        final Execute exe = new Execute(ops, tableId);
        return makeWriteRequest(exe, partitionId, durability, timeout,
                                timeoutUnit);
    }

    public static Result processExecuteResult(Result result,
                                              List<Operation> operations)
        throws OperationExecutionException {

        final OperationExecutionException exception =
            result.getExecuteException(operations);
        if (exception != null) {
            throw exception;
        }
        return result;
    }

    public Request makeGetIdentityRequest(Key key,
                                          long curVersion,
                                          int clientIdentityCacheSize,
                                          boolean getAttributes,
                                          boolean getNextSequence,
                                          String sgName,
                                          Durability durability,
                                          long timeout,
                                          TimeUnit timeoutUnit) {
        final byte[] keyBytes = keySerializer.toByteArray(key);
        final PartitionId partitionId = dispatcher.getPartitionId(keyBytes);

        final GetIdentityAttrsAndValues attrsAndValues =
            new GetIdentityAttrsAndValues(keyBytes, curVersion,
                                          clientIdentityCacheSize,
                                          getAttributes, getNextSequence,
                                          sgName);
        return makeWriteRequest(attrsAndValues, partitionId, durability,
                                timeout, timeoutUnit);
    }

    @Override
    public void put(List<EntryStream<KeyValue>> kvStreams,
                    BulkWriteOptions writeOptions) {

        if (kvStreams == null || kvStreams.isEmpty()) {
            throw new IllegalArgumentException("The stream list cannot be " +
                "null or empty.");
        }

        if (kvStreams.contains(null)) {
            throw new IllegalArgumentException("Elements of stream list " +
                "must not be null.");
        }

        final BulkWriteOptions options =
            (writeOptions != null) ? writeOptions : new BulkWriteOptions();

        final BulkPut<KeyValue> bulkPut =
            new BulkPut<KeyValue>(this, options, kvStreams, getLogger()) {

                @Override
                public BulkPut<KeyValue>.StreamReader<KeyValue>
                    createReader(int streamId, EntryStream<KeyValue> stream) {

                    return new StreamReader<KeyValue>(streamId, stream) {

                        @Override
                        protected Key getKey(KeyValue kv) {
                            return kv.getKey();
                        }

                        @Override
                        protected Value getValue(KeyValue kv) {
                            return kv.getValue();
                        }
                    };
                }

                @Override
                protected KeyValue convertToEntry(Key key, Value value) {
                    return new KeyValue(key, value);
                }
        };

        try {
            bulkPut.execute();
        } catch (InterruptedException e) {
            throw new IllegalStateException("Unexpected interrupt during " +
                    "putBulk()", e);
        }
    }

    /*
     * Internal method to put KeyValueVersion, which may contain expiration
     * time, to be translated into a TTL. This is used by import.
     *
     * @param referenceTime the reference time to be subtracted from the
     * expiration time (if non-zero) on any KeyValueVersion to generate a TTL
     * duration. This value must be less than any expiration time to avoid
     * negative durations.
     */
    public void put(List<EntryStream<KeyValueVersion>> kvStreams,
                    final long referenceTime,
                    BulkWriteOptions writeOptions) {

        if (kvStreams == null || kvStreams.isEmpty()) {
            throw new IllegalArgumentException("The stream list cannot be " +
                "null or empty.");
        }

        if (kvStreams.contains(null)) {
            throw new IllegalArgumentException("Elements of stream list " +
                "must not be null.");
        }

        final BulkWriteOptions options =
            (writeOptions != null) ? writeOptions : new BulkWriteOptions();

        final BulkPut<KeyValueVersion> bulkPut =
            new BulkPut<KeyValueVersion>(this, options, kvStreams, getLogger()) {

                @Override
                public BulkPut<KeyValueVersion>.StreamReader<KeyValueVersion>
                    createReader(int streamId, EntryStream<KeyValueVersion> stream) {

                    return new StreamReader<KeyValueVersion>(streamId, stream) {

                        @Override
                        protected Key getKey(KeyValueVersion kv) {
                            return kv.getKey();
                        }

                        @Override
                        protected Value getValue(KeyValueVersion kv) {
                            return kv.getValue();
                        }

                        @Override
                        protected TimeToLive getTTL(KeyValueVersion kv) {
                            long expirationTime = kv.getExpirationTime();
                            if (expirationTime == 0) {
                                return null;
                            }
                            if (referenceTime > expirationTime) {
                                throw new IllegalArgumentException(
                                    "Reference time must be less than " +
                                    "expiration time");
                            }
                            return TimeToLive.fromExpirationTime(
                                kv.getExpirationTime(), referenceTime);
                        }
                    };
                }

                @Override
                protected KeyValueVersion convertToEntry(Key key, Value value) {
                    return new KeyValueVersion(key, value);
                }
        };

        try {
            bulkPut.execute();
        } catch (InterruptedException e) {
            throw new IllegalStateException("Unexpected interrupt during " +
                    "putBulk()", e);
        }
    }

    /**
     * TODO: Support to configure the checkpoint delay time.
     * Sam said that a large part of JE overhead (as much as 20% of throughput)
     * is in the resources of taking JE checkpoints at regular intervals. It
     * would boost load performance in certain circumstances, for example, when
     * a store was being populated initially (there were no other ongoing
     * operations), to delay taking a checkpoint on each RN until the very end
     * of the bulk load.
     *
     * Doing so requires support in both JE and JE HA, which does not exist
     * currently.
     *
     * NOTE: this method is only public so that it can be accessed from BulkPut
     */
    public List<Integer> putBatch(PartitionId partitionId,
                                  List<KVPair> le,
                                  long[] tableIds,
                                  boolean overwrite,
                                  boolean usePutResolve,
                                  int localRegionId,
                                  Durability durability,
                                  long timeout,
                                  TimeUnit timeoutUnit)
        throws FaultException {

        PutBatch pb = new PutBatch(le, tableIds, overwrite,
                                   usePutResolve, localRegionId);

        final Request req = makeWriteRequest(pb, partitionId, durability,
                                             timeout, timeoutUnit);
        final Result.PutBatchResult result =
            (Result.PutBatchResult)executeRequest(req);

        List<Integer> keysPresent = result.getKeysPresent() ;

        return keysPresent;
    }

    @Override
    public OperationFactoryImpl getOperationFactory() {
        return operationFactory;
    }

    @Override
    public void close() {
        synchronized(loginLock) {
            if (loginMgr != null) {
                logout();
            }
        }
        final ClientId clientId = dispatcher.getClientId();
        dispatcher.shutdown(null);
        if (kvStatsMonitor != null) {
            kvStatsMonitor.stop();
        }
        if (clientId != null) {
            RegistryUtils.clearRegistryCSF(clientId);
            ClientSocketFactory.clearConfiguration(clientId);
        }
        sharedThreadPool.shutdownNow();
        tableAPI.stop();
        sgAttributes.stop(false);
        /* Set the close flag after successfully close */
        isClosed = true;
    }

    /**
     * Perform any LOB key specific checks when using non-internal handles.
     *
     * @param op the operation whose key is to be checked
     */
    private void checkLOBKeySuffix(InternalOperation op) {

        if (isInternalHandle()) {
            return;
        }

        final byte[] keyBytes =
            op.checkLOBSuffix(largeObjectImpl.getLOBSuffixBytes());

        if (keyBytes == null) {
            return;
        }

        final String msg =
            "Operation: " + op.getOpCode() +
            " Illegal LOB key argument: " +
            Key.fromByteArray(keyBytes) +
            ". Use LOB-specific APIs to modify a LOB key/value pair.";

        throw new IllegalArgumentException(msg);
    }

    public Request makeWriteRequest(InternalOperation op,
                                     PartitionId partitionId,
                                     Durability durability,
                                     long timeout,
                                     TimeUnit timeoutUnit) {
        checkLOBKeySuffix(op);

        return makeRequest
            (op, partitionId, null /*repGroupId*/, true /*write*/,
             ((durability != null) ? durability : defaultDurability),
             null /*consistency*/,
             getTimeoutMs(timeout, timeoutUnit));
    }

    public Request makeWriteRequest(InternalOperation op,
                                     PartitionId partitionId,
                                     Durability durability,
                                     long timeoutMs) {
        checkLOBKeySuffix(op);

        return makeRequest
            (op, partitionId, null /*repGroupId*/, true /*write*/,
             ((durability != null) ? durability : defaultDurability),
             null /*consistency*/,
             timeoutMs);
    }

    public Request makeWriteRequest(InternalOperation op,
                                     RepGroupId repGroupId,
                                     Durability durability,
                                     long timeout,
                                     TimeUnit timeoutUnit) {
        return makeRequest
            (op, null /*partitionId*/, repGroupId, true /*write*/,
             ((durability != null) ? durability : defaultDurability),
             null /*consistency*/,
             getTimeoutMs(timeout, timeoutUnit));
    }

    public Request makeWriteRequest(InternalOperation op,
                                     RepGroupId repGroupId,
                                     Durability durability,
                                     long timeoutMs) {
        return makeRequest
            (op, null /*partitionId*/, repGroupId, true /*write*/,
             ((durability != null) ? durability : defaultDurability),
             null /*consistency*/,
             timeoutMs);
    }

    public Request makeReadRequest(InternalOperation op,
                                   PartitionId partitionId,
                                   Consistency consistency,
                                   long timeout,
                                   TimeUnit timeoutUnit) {

        return makeRequest
            (op, partitionId, null /*repGroupId*/,
             false /*write*/, null /*durability*/,
             ((consistency != null) ? consistency : defaultConsistency),
             getTimeoutMs(timeout, timeoutUnit));
    }

    public Request makeReadRequest(InternalOperation op,
                                   PartitionId partitionId,
                                   Consistency consistency,
                                   long timeoutMs) {

        return makeRequest
            (op, partitionId, null /*repGroupId*/,
             false /*write*/, null /*durability*/,
             ((consistency != null) ? consistency : defaultConsistency),
             timeoutMs);
    }

    public Request makeReadRequest(InternalOperation op,
                                   RepGroupId repGroupId,
                                   Consistency consistency,
                                   long timeout,
                                   TimeUnit timeoutUnit) {
        return makeRequest
            (op, null /*partitionId*/, repGroupId,
             false /*write*/, null /*durability*/,
             ((consistency != null) ? consistency : defaultConsistency),
             getTimeoutMs(timeout, timeoutUnit));
    }

    public Request makeReadRequest(InternalOperation op,
                                   RepGroupId repGroupId,
                                   Consistency consistency,
                                   long timeoutMs) {
        return makeRequest
            (op, null /*partitionId*/, repGroupId,
             false /*write*/, null /*durability*/,
             ((consistency != null) ? consistency : defaultConsistency),
             timeoutMs);
    }

    public Request makeRequest(InternalOperation op,
                                PartitionId partitionId,
                                RepGroupId repGroupId,
                                boolean write,
                                Durability durability,
                                Consistency consistency,
                                long timeoutMs) {

        final Topology topology = getTopology();
        final int topoSeqNumber =
            (topology == null) ? 0 : topology.getSequenceNumber();

        return (partitionId != null) ?
            new Request(op, partitionId, write, durability, consistency,
                        DEFAULT_TTL,
                        topoSeqNumber,
                        dispatcher.getDispatcherId(),
                        (int)timeoutMs,
                        !write ? dispatcher.getReadZoneIds() : null) :
            new Request(op, repGroupId, write, durability, consistency,
                        DEFAULT_TTL,
                        topoSeqNumber,
                        dispatcher.getDispatcherId(),
                        (int)timeoutMs,
                        !write ? dispatcher.getReadZoneIds() : null);
    }

    public int getTimeoutMs(long timeout, TimeUnit timeoutUnit) {

        int requestTimeoutMs = defaultRequestTimeoutMs;
        if (timeout > 0) {
            requestTimeoutMs = PropUtil.durationToMillis(timeout, timeoutUnit);
            if (!dispatcher.isAsync() && (requestTimeoutMs > readTimeoutMs)) {
                String format = "Request timeout parameter: %,d ms exceeds " +
                    "socket read timeout: %,d ms";
                throw new IllegalArgumentException
                    (String.format(format, requestTimeoutMs, readTimeoutMs));
            }
        }

        return requestTimeoutMs;
    }

    /**
     * Invokes a request through the request handler
     *
     * @param request the request to run
     * @return the result of the request
     * @see #executeRequestAsync(Request)
     */
    public Result executeRequest(Request request)
        throws FaultException {

        try {
            return getExecuteResult(request, executeRequestInternal(request));
        } catch (RuntimeException e) {
            if (handleExecuteRequestException(request, e)) {
                return getExecuteResult(
                    request, executeRequestInternal(request));
            }
            throw e;
        }
    }

    /**
     * Handle a RuntimeException thrown when executing a request. Updates the
     * table cache to remove the table associated with the operation if the
     * table metadata should be fetched again because of a security issue or
     * because table metadata was not found. Returns true if the operation
     * should be retried because it was associated with an SSL handshake
     * problem that was resolved.
     */
    private boolean handleExecuteRequestException(Request request,
                                                  RuntimeException re) {
        try {
            throw re;
        } catch (AuthenticationFailureException afe) {
            tableAPI.removeFromCache(request.getOperation().getTableId());
            if (afe.getCause() instanceof SSLHandshakeException) {

                tryResolveSSLHandshakeError();
                return true;
            }
        } catch (KVSecurityException se) {
            tableAPI.removeFromCache(request.getOperation().getTableId());
        } catch (MetadataNotFoundException mnfe) {
            tableAPI.removeFromCache(request.getOperation().getTableId());
            tableAPI.metadataNotification(mnfe.getTableMetadataSeqNum());
        } catch (RuntimeException e) {
        }
        return false;
    }

    private Result getExecuteResult(Request request, Response response) {
        assert TestHookExecute.doHookIfSet(beforeExecuteResultHook, response);
        final Result result = response.getResult();
        if (result.getMetadataSeqNum() > 0) {
            tableAPI.validateCache(request.getOperation().getTableId(),
                                   result.getMetadataSeqNum());
            tableAPI.metadataNotification(result.getMetadataSeqNum());
        }
        assert TestHookExecute.doHookIfSet(executeRequestHook, result);
        return result;
    }

    /**
     * A hook called by executeRequestInternal() after the execution of request,
     * the returning Result object is passed into the hook.
     */
    private TestHook<Result> executeRequestHook;

    /**
     * A hook called by getExecuteResult() after the execution of request,
     * the returning Response object is passed into the hook.
     */
    private TestHook<Response> beforeExecuteResultHook;

    private Response executeRequestInternal(Request request)
        throws FaultException {

        final LoginManager requestLoginMgr = this.loginMgr;
        final boolean isUserSuppliedAuth = request.getAuthContext() != null;
        try {
            return dispatcher.execute(
                request,
                /*
                 * If AuthContext is already supplied, which means externally
                 * caller will handle auth retry, in this case, pass null to
                 * the argument to avoid internal login manager auth retry.
                 * For other cases, use login manager to handle auth retry
                 * within request dispatcher.
                 */
                isUserSuppliedAuth ? null : loginMgr);
        } catch (AuthenticationRequiredException are) {
            /*
             * Try to reauthenticate, but if the AuthContext was provided by
             * the caller it needs to handle the  exception and reauthenticate.
             */
            if (!tryReauthenticate(requestLoginMgr, isUserSuppliedAuth)) {
                throw are;
            }

            /*
             * If the authentication completed, we assume we are ready to
             * retry the operation.  No retry on the authentication here.
             */
            return dispatcher.execute(request, loginMgr);
        }
    }

    /**
     * For testing.
     */
    public void setExecuteRequestHook(TestHook<Result> executeRequestHook) {
        this.executeRequestHook = executeRequestHook;
    }

    public void setBeforeExecuteResultHook(TestHook<Response> beforeExecuteResultHook) {
        this.beforeExecuteResultHook = beforeExecuteResultHook;
    }

    /**
     * Invokes a request through the request handler, returning the result
     * asynchronously.
     *
     * @param request the request to run
     * @return a future that returns the result
     * @see #executeRequest(Request)
     */
    public CompletableFuture<Result>
        executeRequestAsync(final Request request)
    {
        /*
         * Check at the beginning if the request has user-supplied
         * authentication information. When a request is retried, the request
         * will contain an AuthContext from the last attempt, but that doesn't
         * mean it is user supplied.
         */
        final boolean isUserSuppliedAuth = request.getAuthContext() != null;
        final CompletableFuture<Result> future = new CompletableFuture<>();
        class ExecuteRequestHandler
            implements BiConsumer<Response, Throwable> {

            /* Flags to make sure we do these things only once */
            private volatile boolean didReauth;
            private volatile boolean didResolveSSL;

            private volatile LoginManager requestLoginMgr;

            void executeInternal() {
                requestLoginMgr = loginMgr;
                dispatcher.executeAsync(
                    request, null,
                    /*
                     * If AuthContext is already supplied, which means
                     * externally caller will handle auth retry, in this case,
                     * pass null to the argument to avoid internal login
                     * manager auth retry. For other cases, use login manager
                     * to handle auth retry within request dispatcher.
                     */
                    isUserSuppliedAuth ? null : loginMgr)
                    .whenComplete(unwrapExceptionVoid(this));
            }

            @Override
            public void accept(Response response, Throwable e) {
                if (!didReauth &&
                    (e instanceof AuthenticationRequiredException)) {
                    didReauth = true;
                    class ReauthenticateAsync implements Runnable {
                        @Override
                        public void run() {
                            /*
                             * Try to reauthenticate, but if the AuthContext
                             * was provided by the caller it needs to handle
                             * the exception and reauthenticate.
                             */
                            if (!tryReauthenticate(requestLoginMgr,
                                                   isUserSuppliedAuth)) {
                                checkedCompleteExceptionally(future, e,
                                                             logger);
                            } else {
                                executeInternal();
                            }
                        }
                    }
                    /*
                     * Run this operation in the standard thread pool because
                     * it makes remote and other blocking calls
                     */
                    CompletableFuture.runAsync(new ReauthenticateAsync());
                    return;
                }
                if (!didResolveSSL && (e instanceof RuntimeException)) {
                    didResolveSSL = true;
                    if (handleExecuteRequestException(request,
                                                      (RuntimeException) e)) {
                        executeInternal();
                        return;
                    }
                }
                if (e != null) {
                    checkedCompleteExceptionally(future, e, logger);
                } else {
                    checkedComplete(future, getExecuteResult(request, response),
                                    logger);
                }
            }
        }
        try {
            new ExecuteRequestHandler().executeInternal();
        } catch (Throwable e) {
            checkedCompleteExceptionally(future, e, logger);
        }
        return future;
    }

    @Override
    public KVStats getStats(boolean clear) {
        return getStats(
            WatcherNames.getKVStoreGetStatsWatcherName(), clear);
    }

    @Override
    public KVStats getStats(String watcherName, boolean clear) {
        return new KVStats(watcherName, clear, dispatcher);
    }

    public KVStats getMonitorStats() {
        return getStats(WatcherNames.KVSTATS_MONITOR, true);
    }

    @Override
    public void login(LoginCredentials creds)
        throws RequestTimeoutException, AuthenticationFailureException,
               FaultException {

        if (creds == null) {
            throw new IllegalArgumentException("No credentials provided");
        }

        final LoginManager priorLoginMgr;
        synchronized (loginLock) {
            /*
             * If there is an existing login, the new creds must be for the
             * same username.
             */
            if (loginMgr != null) {
                if ((loginMgr.getUsername() == null &&
                     creds.getUsername() != null) ||
                    (loginMgr.getUsername() != null &&
                     !loginMgr.getUsername().equals(creds.getUsername()))) {
                    throw new AuthenticationFailureException(
                        "Logout required prior to logging in with new " +
                        "user identity.");
                }
            }

            final RepNodeLoginManager rnlm =
                new RepNodeLoginManager(creds.getUsername(), true,
                                        dispatcher.getClientId(),
                                        dispatcher.getProtocols(),
                                        logger);
            rnlm.setTopology(dispatcher.getTopologyManager());

            if (creds instanceof KerberosCredentials) {
                final KerberosClientCreds clientCreds = KVStoreLogin.
                    getKrbClientCredentials((KerberosCredentials) creds);
                rnlm.login(clientCreds);
            } else {
                rnlm.login(creds);
            }

            if (creds instanceof KerberosCredentials) {
                /*
                 * If login with Kerberos credentials, refresh cached
                 * Kerberos principals info based on topology.
                 */
                try {
                    rnlm.locateKrbPrincipals();
                } catch (KVStoreException e) {
                    throw new FaultException(e, false /*isRemote*/);
                }
            }

            /* login succeeded - establish new login */
            priorLoginMgr = loginMgr;
            if (isDispatcherOwner) {
                dispatcher.setRegUtilsLoginManager(rnlm);
            }
            this.loginMgr = rnlm;
            largeObjectImpl.renewLoginMgr(this.loginMgr);
            if (statementExecutor != null) {
                statementExecutor.renewLoginManager(this.loginMgr);
            }
        }

        if (priorLoginMgr != null) {
            Exception logException = null;
            try {
                priorLoginMgr.logout();
            } catch (SessionAccessException re) {
                /* ok */
                logException = re;
            } catch (AuthenticationRequiredException are) {
                /* ok */
                logException = are;
            }

            if (logException != null) {
                logger.info(logException.getMessage());
            }
        }
    }

    @Override
    public void logout()
        throws RequestTimeoutException, FaultException {

        synchronized(loginLock) {
            if (loginMgr == null) {
                throw new AuthenticationRequiredException(
                    "The KVStore handle has no associated login",
                    false /* isReturnSignal */);
            }

            try {
                loginMgr.logout();
            } catch (SessionAccessException sae) {
                logger.fine(sae.getMessage());
                /* ok */
            } finally {
                if (isDispatcherOwner) {
                    dispatcher.setRegUtilsLoginManager(null);
                }
            }
        }
    }

    public PartitionId getPartitionId(Key key) {
        final byte[] keyBytes = keySerializer.toByteArray(key);
        return dispatcher.getPartitionId(keyBytes);
    }

   public PartitionId getPartitionId(byte[] key) {
        return dispatcher.getPartitionId(key);
    }

    public long getDefaultLOBTimeout() {
        return defaultLOBTimeout;
    }

    public String getDefaultLOBSuffix() {
        return defaultLOBSuffix;
    }

    public long getDefaultLOBVerificationBytes() {
        return defaultLOBVerificationBytes;
    }

    public Consistency getDefaultConsistency() {
        return defaultConsistency;
    }

    public Durability getDefaultDurability() {
        return defaultDurability;
    }

    public int getDefaultChunksPerPartition() {
        return defaultChunksPerPartition;
    }

    public int getDefaultChunkSize() {
        return defaultChunkSize;
    }

    @Override
    public Version putLOB(Key lobKey,
                          InputStream lobStream,
                          Durability durability,
                          long lobTimeout,
                          TimeUnit timeoutUnit)
        throws IOException {

        return largeObjectImpl.putLOB(lobKey, lobStream,
                                      durability, lobTimeout, timeoutUnit);
    }

    @Override
    public boolean deleteLOB(Key lobKey,
                             Durability durability,
                             long lobTimeout,
                             TimeUnit timeoutUnit) {
        return largeObjectImpl.deleteLOB(lobKey, durability,
                                         lobTimeout, timeoutUnit);
    }

    @Override
    public InputStreamVersion getLOB(Key lobKey,
                                     Consistency consistency,
                                     long lobTimeout,
                                     TimeUnit timeoutUnit) {
        return largeObjectImpl.getLOB(lobKey, consistency,
                                      lobTimeout, timeoutUnit);
    }

    @Override
    public Version putLOBIfAbsent(Key lobKey,
                                  InputStream lobStream,
                                  Durability durability,
                                  long lobTimeout,
                                  TimeUnit timeoutUnit)
        throws IOException {

        return largeObjectImpl.
            putLOBIfAbsent(lobKey, lobStream,
                           durability, lobTimeout, timeoutUnit);
    }

    @Override
    public Version putLOBIfPresent(Key lobKey,
                                   InputStream lobStream,
                                   Durability durability,
                                   long lobTimeout,
                                   TimeUnit timeoutUnit)
        throws IOException {

        return largeObjectImpl.
            putLOBIfPresent(lobKey, lobStream,
                            durability, lobTimeout, timeoutUnit);
    }

    @Override
    public TableAPI getTableAPI() {
        return tableAPI;
    }

    public TableAPIImpl getTableAPIImpl() {
        return tableAPI;
    }

    @Override
    public Version appendLOB(Key lobKey,
                             InputStream lobAppendStream,
                             Durability durability,
                             long lobTimeout,
                             TimeUnit timeoutUnit)
        throws IOException {

        return largeObjectImpl.
            appendLOB(lobKey, lobAppendStream,
                      durability, lobTimeout, timeoutUnit);
    }

    /**
     * Attempt reauthentication, if possible, assuming that the request did not
     * have a user-supplied AuthContext.
     * @param requestLoginMgr the LoginManager in effect at the time of
     *   the request execution.
     * @return true if reauthentication has succeeded
     */
    public boolean tryReauthenticate(LoginManager requestLoginMgr)
        throws FaultException {

        return tryReauthenticate(requestLoginMgr,
                                 false /* isUserSuppliedAuth */);
    }

    /**
     * Attempt reauthentication, if possible.
     * @param requestLoginMgr the LoginManager in effect at the time of
     *   the request execution.
     * @param isUserSuppliedAuth if the request contained an AuthContext
     * supplied by the caller
     * @return true if reauthentication has succeeded
     */
    public boolean tryReauthenticate(LoginManager requestLoginMgr,
                                     boolean isUserSuppliedAuth)
        throws FaultException {

        /*
         * If the caller specified an AuthContext then they need to
         * reauthenticate
         */
        if (isUserSuppliedAuth) {
            return false;
        }

        if (reauthHandler == null) {
            return false;
        }

        synchronized (loginLock) {
            /*
             * If multiple threads are concurrently accessing the kvstore at
             * the time of an AuthenticationRequiredException, there is the
             * possibility of a flood of AuthenticationRequiredExceptions
             * occuring, with a flood of reauthentication attempts following.
             * Because of the synchronization on loginLock, only one thread
             * will be able to re-authenticate at a time, so by the time a
             * reauthentication request makes it here, another thread may
             * already have completed the authentication.
             */
            if (this.loginMgr == requestLoginMgr) {
                try {
                    if (isInternalHandle()) {
                        reauthHandler.reauthenticate(this.external);
                    } else {
                        reauthHandler.reauthenticate(this);
                    }
                } catch (KVSecurityException kvse) {
                    logger.fine(kvse.getMessage());
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Try to resolve the SSL handshake error in case it was caused by updated
     * SSL certificates, to allow an existing client to connect successfully
     * without reopening the store.
     */
    private void tryResolveSSLHandshakeError() {
        final String storeName =
            dispatcher.getTopologyManager().getTopology().getKVStoreName();

        /* Reread the truststore in case certificates have changed */
        RegistryUtils.resetRegistryCSF(storeName, dispatcher.getClientId());

        /*
         * Reset all request handlers in an attempt to get around SSL handshake
         * errors encountered by the previous resolution. If the store has
         * updated SSL certificates, resetting the request handlers for all
         * RepNodes will cause new connections to be created that can take
         * advantage of new certificates.
         */
        final RepGroupStateTable repGroupStateTable =
            dispatcher.getRepGroupStateTable();
        for (RepNodeState state : repGroupStateTable.getRepNodeStates()) {
            state.resetReqHandlerRef();
        }
    }

    /**
     * Gets a task executor which will run at most maxConcurrentTasks tasks
     * using the shared thread pool. Once maxConcurrentTasks have been
     * submitted to the shared thread pool for execution further tasks are
     * queued for later execution.
     *
     * Tasks submitted through the executor will compete with all other tasks
     * in the shared thread pool.
     *
     * @param maxConcurrentTasks the maximum number of tasks that the
     * returned executor will submit to the shared thread pool for execution.
     *
     * @return a task executor
     */
    public TaskExecutor getTaskExecutor(int maxConcurrentTasks) {
        return sharedThreadPool.getTaskExecutor(maxConcurrentTasks);
    }

    /**
     * The task executor for the shared thread pool.
     */
    public interface TaskExecutor {

        /**
         * Submits a Runnable task for execution and returns a Future
         * representing that task. The future's get method will return null
         * upon successful completion.
         *
         * @param task the task to submit
         * @return a Future representing pending completion of the task
         * @throws RejectedExecutionException if the executor or the shared
         * thread pool has been shutdown
         */
        Future<?> submit(Runnable task);

        /**
         * Attempts to stop all actively executing tasks, halts the processing
         * of waiting tasks, and returns a list of the tasks that were awaiting
         * execution. These tasks are drained (removed) from the task queue
         * upon return from this method. Only the tasks submitted through this
         * executor are affected.
         *
         * Tasks are canceled via Thread.interrupt(), so any task that fails
         * to respond to interrupts may never terminate.
         *
         * @return a list of tasks that never commenced execution
         */
        List<Runnable> shutdownNow();
    }

    @Override
    public ExecutionFuture execute(String statement)
        throws IllegalArgumentException, FaultException {

        return execute(statement, new ExecuteOptions());
    }

    @Override
    public ExecutionFuture execute(String statement, ExecuteOptions options)
        throws FaultException, IllegalArgumentException {

        checkClosed();

        if (options == null) {
            options = new ExecuteOptions();
        }

        PreparedStatement ps = prepare(statement, options);

        if (ps instanceof PreparedDdlStatementImpl) {

            return executeDdl((PreparedDdlStatementImpl) ps);
        }

        return executeDml((PreparedStatementImpl) ps, options);
    }

    @Override
    public ExecutionFuture execute(char[] statement, ExecuteOptions options)
        throws FaultException, IllegalArgumentException {

        checkClosed();

        if (options == null) {
            options = new ExecuteOptions();
        }

        PreparedStatement ps = prepare(statement, options);

        if (ps instanceof PreparedDdlStatementImpl) {
            return executeDdl((PreparedDdlStatementImpl) ps);
        }

        return executeDml((PreparedStatementImpl) ps, options);
    }

    /* Check if the store handle has been closed */
    private void checkClosed() {
        if (isClosed) {
            throw new IllegalArgumentException(
                "Cannot execute request on a closed store handle");
        }
    }

    @Override
    public Publisher<RecordValue> executeAsync(String statement,
                                               ExecuteOptions options) {
        checkNull("statement", statement);
        checkClosed();
        return executeAsync(prepare(statement, options), options);
    }

    @Override
    public Publisher<RecordValue> executeAsync(Statement statement,
                                               ExecuteOptions options) {
        return executeAsync(statement, options, null);
    }

    public Publisher<RecordValue> executeAsync(Statement statement,
                                               ExecuteOptions options,
                                               Set<RepGroupId> shards) {
        checkNull("statement", statement);
        return new QueryPublisher(this,
                                  options,
                                  (InternalStatement)statement,
                                  shards);
    }

    /**
     * Executes a DDL statement in an async fashion, returning an
     * ExecutionFuture.
     *
     * Because this method communicates directly with the admin it needs to
     * handle exceptions carefully.
     */
    private ExecutionFuture executeDdl(PreparedDdlStatementImpl statement)
        throws IllegalArgumentException, FaultException {
        final LoginManager requestLoginMgr = this.loginMgr;
        final ExecuteOptions options = statement.getExecuteOptions();

        try {
            return statementExecutor.executeDdl(statement.getQuery(),
                                                statement.getNamespace(),
                                                options,
                                                null, /* TableLimits */
                                                getLoginManager(this));
        } catch (AuthenticationRequiredException are) {
            /*
             * Try to reauthenticate, but if the AuthContext was provided by
             * the caller it needs to handle the  exception and reauthenticate.
             */
            if (!tryReauthenticate(requestLoginMgr,
                                   options.getAuthContext() != null)) {
                throw are;
            }

            /*
             * If the authentication completed, we assume we are ready to
             * retry the operation.  No retry on the authentication here.
             */
            return statementExecutor.executeDdl(statement.getQuery(),
                                                statement.getNamespace(),
                                                options,
                                                null, /* TableLimits */
                                                getLoginManager(this));
        }
    }

    /**
     * Executes a DML statement in an async fashion, returning an
     * ExecutionFuture. In this path there is no async protocol at this time,
     * so the query is executed and the results are always immediately
     * available.
     */
    private ExecutionFuture executeDml(PreparedStatementImpl statement,
                                       ExecuteOptions options)
        throws IllegalArgumentException, FaultException {

        try {
            final StatementResult result = executeSync(statement, options);
            return new DmlFuture(result);
        } catch (QueryException qe) {

            /* A QueryException thrown at the client; rethrow as IAE */
            throw qe.getIllegalArgument();
        }
    }

    /**
     * Executes a set table limits in an async fashion, returning an
     * ExecutionFuture.
     *
     * This is public for access directly from the cloud proxy.
     */
    public ExecutionFuture setTableLimits(String namespace,
                                          String tableName,
                                          TableLimits limits)
        throws IllegalArgumentException, FaultException {

        final LoginManager requestLoginMgr = this.loginMgr;
        try {
            return statementExecutor.setTableLimits(namespace,
                                                    tableName,
                                                    limits,
                                                    getLoginManager(this));
        } catch (AuthenticationRequiredException are) {
            if (!tryReauthenticate(requestLoginMgr,
                                   false /* isUserSuppliedAuth */)) {
                throw are;
            }

            /*
             * If the authentication completed, we assume we are ready to
             * retry the operation.  No retry on the authentication here.
             */
            return statementExecutor.setTableLimits(namespace,
                                                    tableName,
                                                    limits,
                                                    getLoginManager(this));
        }
    }

    @Override
    public StatementResult executeSync(String statement)
        throws FaultException {

        return executeSync(statement, null);
    }

    @Override
    public StatementResult executeSync(String statement,
                                       ExecuteOptions options)
        throws FaultException, IllegalArgumentException {
        ExecutionFuture f = execute(statement, options);
        return DdlStatementExecutor.waitExecutionResult(f);
    }

    @Override
    public StatementResult executeSync(char[] statement,
                                       ExecuteOptions options)
        throws FaultException, IllegalArgumentException {
            ExecutionFuture f = execute(statement, options);
            return DdlStatementExecutor.waitExecutionResult(f);
    }

    @Override
    public ExecutionFuture getFuture(byte[] futureBytes) {

        return new DdlFuture(futureBytes, statementExecutor);
    }

    @Override
    public PreparedStatement prepare(String query)
        throws FaultException, IllegalArgumentException {

        return prepare(query, null);
    }

    @Override
    public PreparedStatement prepare(String query, ExecuteOptions options)
        throws FaultException, IllegalArgumentException {

        return prepare(query.toCharArray(), options);
    }

    @Override
    public PreparedStatement prepare(char[] query, ExecuteOptions options)
        throws FaultException, IllegalArgumentException {

        if (options == null) {
            options = new ExecuteOptions();
        }

        /*
         * In order to keep the cache up-to-date as possible we provide a
         * callback object to record the table name parsed during the prepare.
         * If this is a DDL operation the table is removed from the cache. The
         * callback object will forward callback calls to the original callback
         * if present. The original callback is restored after the prepare.
         */
        final CallbackWrapper callback =
                new CallbackWrapper(options.getPrepareCallback());
        options.setPrepareCallback(callback);

        try {
            final PreparedStatement ps =
                    CompilerAPI.prepare(tableAPI, query, options);

            /* If DDL remove the table from the cache since it may be changing */
            if (ps instanceof PreparedDdlStatementImpl) {
                tableAPI.removeFromCache(callback.namespace, callback.table);
            }
            return ps;
        } catch (QueryException qe) {
            /* rethrow as IAE */
            throw qe.getIllegalArgument();
        } finally {
            /* Restore the options */
            options.setPrepareCallback(callback.wrapped);
        }
    }

    @Override
    public StatementResult executeSync(final Statement statement)
        throws FaultException {

        return executeSync(statement, null);
    }

    /* For testing */
    public static void setSubscriberOnNextHook(TestHook<Boolean> hook) {
        subscriberOnNextHook = hook;
    }

    @Override
    public StatementResult executeSync(Statement statement,
                                       ExecuteOptions options)
        throws FaultException {

        if (options == null) {
            options = new ExecuteOptions();
        }
        if (statement instanceof PreparedDdlStatementImpl ||
            !getDispatcher().isAsync() ||
            !options.isAsync()) {
            final LoginManager requestLoginMgr = this.loginMgr;
            try {
                return ((InternalStatement)statement).executeSync(this,
                                                                  options);
            } catch (AuthenticationRequiredException are) {
                /*
                 * Try to reauthenticate, but if the AuthContext was provided
                 * by the caller it needs to handle the  exception and
                 * reauthenticate.
                 */
                if (!tryReauthenticate(requestLoginMgr,
                            options.getAuthContext() != null)) {
                    throw are;
                }

                /*
                 * If the authentication completed, we assume we are ready to
                 * retry the operation.  No retry on the authentication here.
                 */
                return ((InternalStatement)statement).executeSync(this,
                                                                  options);
            }
        }

        /*
         * When possible, implement using executeAsync to reduce thread usage
         * [KVSTORE-1072]
         */
        final LinkedBlockingQueue<RecordValue> resultList
            = new LinkedBlockingQueue<>();
        final CompletableFuture<QuerySubscription> syncFuture
            = new CompletableFuture<>();
        final CompletableFuture<Boolean> iterFuture
            = new CompletableFuture<>();

        final long batchSize = options.getResultsBatchSize();
        final long timeoutMillis = (options.getTimeout() == 0) ?
            defaultRequestTimeoutMs :
            options.getTimeoutUnit().toMillis(options.getTimeout());

        executeAsync(statement, options).subscribe(

            new Subscriber<RecordValue>() {
                QuerySubscription currentSubscription = null;

                @Override
                public void onSubscribe(Subscription s) {
                    currentSubscription = (QuerySubscription)s;
                    s.request(batchSize);
                }
                @Override
                public void onNext(RecordValue record) {
                    /*
                     * Don't deliver this record if the hook throws an
                     * exception -- for testing
                     */
                    try {
                        assert TestHookExecute.doHookIfSet(
                            subscriberOnNextHook, syncFuture.isDone());
                    } catch (RuntimeException e) {
                        return;
                    }
                    resultList.offer(record);
                    if (resultList.size() >= 1 && !syncFuture.isDone()) {
                        syncFuture.complete(currentSubscription);
                    }
                }
                @Override
                public void onComplete() {
                    /*
                     * iterFuture.complete() and resultList.offer() ordering
                     * matters, any Order-Violation change may cause
                     * race condition in consumer or client thread.
                     */
                    iterFuture.complete(true);
                    resultList.offer(EMPTY_RECORD);
                    if (!syncFuture.isDone()) {
                        syncFuture.complete(currentSubscription);
                    }
                }
                @Override
                public void onError(Throwable exception) {
                    iterFuture.completeExceptionally(exception);
                    resultList.offer(EMPTY_RECORD);
                    if (!syncFuture.isDone()) {
                        syncFuture.completeExceptionally(exception);
                    }
                }
            });

        /*
         * Wait for the one result to be delivered before returning
         * the result so that we can deliver exceptions synchronously for any
         * initial connection problems
         */
        final QuerySubscription currentSubscription =
            AsyncRegistryUtils.getWithTimeout(syncFuture, "executeSync",
                                              timeoutMillis);
        return new SubscriptionStatementResult(resultList,
                                               currentSubscription,
                                               batchSize,
                                               iterFuture);
    }

    public StatementResult executeSyncShards(Statement statement,
                                             ExecuteOptions options,
                                             final Set<RepGroupId> shards)
        throws FaultException {

        if (options == null) {
            options = new ExecuteOptions();
        }

        final LoginManager requestLoginMgr = this.loginMgr;
        try {
            return ((InternalStatement)statement).
                   executeSyncShards(this, options, shards);
        } catch (AuthenticationRequiredException are) {
            /*
             * Try to reauthenticate, but if the AuthContext was provided by
             * the caller it needs to handle the  exception and reauthenticate.
             */
            if (!tryReauthenticate(requestLoginMgr,
                                   options.getAuthContext() != null)) {
                throw are;
            }

            /*
             * If the authentication completed, we assume we are ready to
             * retry the operation.  No retry on the authentication here.
             */
            return ((InternalStatement)statement).
                   executeSyncShards(this, options, shards);
        }
    }

    /**
     * Not part of the public KVStore interface available to external clients.
     * This method is employed when the Oracle NoSQL DB Hive/BigDataSQL
     * integration mechanism is used to process a query and disjoint partition
     * sets are specified for each split.
     */
    public StatementResult executeSyncPartitions(final String statement,
                                                 ExecuteOptions options,
                                                 final Set<Integer> partitions)
        throws FaultException, IllegalArgumentException {

        if (options == null) {
            options = new ExecuteOptions();
        }

        final PreparedStatement ps = prepare(statement, options);

        if (!(ps instanceof PreparedStatementImpl)) {
            throw new IllegalArgumentException("unsupported statement type [" +
                                               ps.getClass().getName() + "]");
        }

        try {
            final StatementResult result =
                ((PreparedStatementImpl) ps).executeSyncPartitions(
                                                    this, options, partitions);
            return DdlStatementExecutor.waitExecutionResult(
                                            new DmlFuture(result));
        } catch (QueryException qe) {
            throw qe.getIllegalArgument(); /* Rethrow QueryException as IAE */
        }
    }

    /**
     * Not part of the public KVStore interface available to external clients.
     * This method is employed when the Oracle NoSQL DB Hive/BigDataSQL
     * integration mechanism is used to process a query and disjoint shard
     * sets are specified for each split.
     */
    public StatementResult executeSyncShards(final String statement,
                                             ExecuteOptions options,
                                             final Set<RepGroupId> shards)
        throws FaultException, IllegalArgumentException {

        if (options == null) {
            options = new ExecuteOptions();
        }

        final PreparedStatement ps = prepare(statement, options);

        if (!(ps instanceof PreparedStatementImpl)) {
            throw new IllegalArgumentException("unsupported statement type [" +
                                               ps.getClass().getName() + "]");
        }

        try {
            final StatementResult result =
                ((PreparedStatementImpl) ps).executeSyncShards(
                                                        this, options, shards);
            return DdlStatementExecutor.waitExecutionResult(
                                            new DmlFuture(result));
        } catch (QueryException qe) {
            throw qe.getIllegalArgument(); /* Rethrow QueryException as IAE */
        }
    }

    /**
     * Internal use (but public) interface to create a table using
     * read/write/storage limits
     */
    public ExecutionFuture execute(char[] statement,
                                   ExecuteOptions options,
                                   TableLimits limits)
        throws FaultException, IllegalArgumentException {

        checkClosed();

        if (options == null) {
            options = new ExecuteOptions();
        }

        PreparedStatement ps = prepare(statement, options);

        if (ps instanceof PreparedDdlStatementImpl) {
            String namespace = null;
            if (options != null) {
                namespace = options.getNamespace();
            }
            return statementExecutor.executeDdl(statement,
                                                namespace,
                                                options,
                                                limits,
                                                getLoginManager(this));
        }
        throw new IllegalArgumentException(
            "Execute with TableLimits is restricted to DDL operations");
    }

    /**
     * Utility method to create a KeyValueVersion. If expiration time is
     * non-zero it creates KeyValueVersionInternal to hold it. This allows
     * space optimization for the more common case where there is no
     * expiration time.
     */
    public static KeyValueVersion createKeyValueVersion(
        final Key key,
        final Value value,
        final Version version,
        final long expirationTime,
        final long modificationTime) {

        if (expirationTime == 0) {
            return new KeyValueVersion(key, value, version,
                                       modificationTime);
        }
        return new KeyValueVersionInternal(key, value,
                                           version,
                                           expirationTime,
                                           modificationTime);
    }

    public DdlStatementExecutor getDdlStatementExecutor() {
        return statementExecutor;
    }

    /*
     * A PrepareCallback object which captures information during a
     * prepare operation. Callbacks are forwarded to the wrapped
     * PrepareCallback if needed.
     */
    private class CallbackWrapper implements PrepareCallback {
        final PrepareCallback wrapped;

        String namespace = null;
        String table = null;

        CallbackWrapper(PrepareCallback wrappedCallback) {
            wrapped = wrappedCallback;
        }

        @Override
        public void namespaceName(String namespaceName) {
            this.namespace = namespaceName;
            if (wrapped != null) {
                wrapped.namespaceName(namespaceName);
            }
        }
        @Override
        public void tableName(String tableName) {
            this.table = tableName;
            if (wrapped != null) {
                wrapped.tableName(tableName);
            }
        }

        @Override
        public boolean prepareNeeded() {
            return (wrapped != null) ? wrapped.prepareNeeded() : true;
        }

        @Override
        public void indexName(String indexName) {
            if (wrapped != null) {
                wrapped.indexName(indexName);
            }
        }

        @Override
        public void regionName(String regionName) {
        if (wrapped != null) {
                wrapped.regionName(regionName);
            }
        }

        @Override
        public void queryOperation(QueryOperation queryOperation) {
            if (wrapped != null) {
                wrapped.queryOperation(queryOperation);
            }
        }

        @Override
        public void ifNotExistsFound() {
            if (wrapped != null) {
                wrapped.ifNotExistsFound();
            }
        }

        @Override
        public void ifExistsFound() {
            if (wrapped != null) {
                wrapped.ifExistsFound();
            }
        }

        @Override
        public void isTextIndex() {
            if (wrapped != null) {
                wrapped.isTextIndex();
            }
        }

        @Override
        public void newTable(Table t) {
            if (wrapped != null) {
                wrapped.newTable(t);
            }
        }

        @Override
        public TableMetadataHelper getMetadataHelper() {
            return (wrapped != null) ? wrapped.getMetadataHelper() : null;
        }

        @Override
        public String mapNamespaceName(String namespaceName) {
            return (wrapped != null) ? wrapped.mapNamespaceName(namespaceName) :
                                       namespaceName;
        }

        @Override
        public String mapTableName(String tableName) {
            return (wrapped != null) ? wrapped.mapTableName(tableName) :
                                       tableName;
        }

        @Override
        public void indexFields(String[] fields) {
            if (wrapped != null) {
                wrapped.indexFields(fields);
            }
        }

        @Override
        public void indexFieldTypes(FieldDef.Type[] types) {
            if (wrapped != null) {
                wrapped.indexFieldTypes(types);
            }
        }
    }
}
