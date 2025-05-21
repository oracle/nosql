/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static oracle.nosql.proxy.ProxySerialization.getConsistency;
import static oracle.nosql.proxy.ProxySerialization.getDurability;
import static oracle.nosql.proxy.ProxySerialization.getContinuationKey;
import static oracle.nosql.proxy.ProxySerialization.getMultiRowOptions;
import static oracle.nosql.proxy.ProxySerialization.getReadKB;
import static oracle.nosql.proxy.ProxySerialization.raiseBadProtocolError;
import static oracle.nosql.proxy.ProxySerialization.readByteArray;
import static oracle.nosql.proxy.ProxySerialization.readFieldValue;
import static oracle.nosql.proxy.ProxySerialization.readInt;
import static oracle.nosql.proxy.ProxySerialization.readLong;
import static oracle.nosql.proxy.ProxySerialization.readMaxReadKB;
import static oracle.nosql.proxy.ProxySerialization.readMaxWriteKB;
import static oracle.nosql.proxy.ProxySerialization.readNonNullEmptyString;
import static oracle.nosql.proxy.ProxySerialization.readNumberLimit;
import static oracle.nosql.proxy.ProxySerialization.readNumVariables;
import static oracle.nosql.proxy.ProxySerialization.readOpCode;
import static oracle.nosql.proxy.ProxySerialization.readPrimaryKeySerializer;
import static oracle.nosql.proxy.ProxySerialization.readRowSerializer;
import static oracle.nosql.proxy.ProxySerialization.readShardId;
import static oracle.nosql.proxy.ProxySerialization.readString;
import static oracle.nosql.proxy.ProxySerialization.readTableLimits;
import static oracle.nosql.proxy.ProxySerialization.readTopologySeqNum;
import static oracle.nosql.proxy.ProxySerialization.readTTL;
import static oracle.nosql.proxy.ProxySerialization.readVersion;
import static oracle.nosql.proxy.ProxySerialization.writeByteArray;
import static oracle.nosql.proxy.ProxySerialization.writeConsumedCapacity;
import static oracle.nosql.proxy.ProxySerialization.writeExistingRow;
import static oracle.nosql.proxy.ProxySerialization.writeExpirationTime;
import static oracle.nosql.proxy.ProxySerialization.writeFieldValue;
import static oracle.nosql.proxy.ProxySerialization.writeGetTableResponse;
import static oracle.nosql.proxy.ProxySerialization.writeGetTableUsageResponse;
import static oracle.nosql.proxy.ProxySerialization.writeIndexInfo;
import static oracle.nosql.proxy.ProxySerialization.writeInt;
import static oracle.nosql.proxy.ProxySerialization.writeModificationTime;
import static oracle.nosql.proxy.ProxySerialization.writeString;
import static oracle.nosql.proxy.ProxySerialization.writeSuccess;
import static oracle.nosql.proxy.ProxySerialization.writeTableOperationResult;
import static oracle.nosql.proxy.ProxySerialization.writeVersion;
import static oracle.nosql.proxy.protocol.BinaryProtocol.mapDDLError;
import static oracle.nosql.proxy.protocol.BinaryProtocol.BAD_PROTOCOL_MESSAGE;
import static oracle.nosql.proxy.protocol.BinaryProtocol.BATCH_OP_NUMBER_LIMIT_EXCEEDED;
import static oracle.nosql.proxy.protocol.BinaryProtocol.ILLEGAL_ARGUMENT;
import static oracle.nosql.proxy.protocol.BinaryProtocol.ON_DEMAND;
import static oracle.nosql.proxy.protocol.BinaryProtocol.PROVISIONED;
import static oracle.nosql.proxy.protocol.BinaryProtocol.QUERY_V1;
import static oracle.nosql.proxy.protocol.BinaryProtocol.QUERY_V4;
import static oracle.nosql.proxy.protocol.BinaryProtocol.QUERY_V5;
import static oracle.nosql.proxy.protocol.BinaryProtocol.CURRENT_QUERY_VERSION;
import static oracle.nosql.proxy.protocol.BinaryProtocol.RECOMPILE_QUERY;
import static oracle.nosql.proxy.protocol.BinaryProtocol.REPLICA_STATS_LIMIT;
import static oracle.nosql.proxy.protocol.BinaryProtocol.REQUEST_SIZE_LIMIT_EXCEEDED;
import static oracle.nosql.proxy.protocol.BinaryProtocol.REQUEST_TIMEOUT;
import static oracle.nosql.proxy.protocol.BinaryProtocol.SECURITY_INFO_UNAVAILABLE;
import static oracle.nosql.proxy.protocol.BinaryProtocol.SERVER_ERROR;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TABLE_NOT_FOUND;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TABLE_USAGE_NUMBER_LIMIT;
import static oracle.nosql.proxy.protocol.BinaryProtocol.UNSUPPORTED_QUERY_VERSION;

// same as NSON types
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_BOOLEAN;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_DOUBLE;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_INTEGER;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_LONG;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_MAP;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_NUMBER;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_STRING;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_TIMESTAMP;

import static oracle.nosql.proxy.protocol.BinaryProtocol.UNKNOWN_ERROR;
import static oracle.nosql.proxy.protocol.BinaryProtocol.UNKNOWN_OPERATION;
import static oracle.nosql.proxy.protocol.BinaryProtocol.V1;
import static oracle.nosql.proxy.protocol.JsonProtocol.OPC_REQUEST_ID;
import static oracle.nosql.proxy.protocol.Protocol.OPERATION_NOT_SUPPORTED;
import static oracle.nosql.proxy.protocol.Protocol.READ_LIMIT_EXCEEDED;
import static oracle.nosql.proxy.protocol.Protocol.SERIAL_VERSION_STRING;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.ADD_REPLICA;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.DELETE;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.DELETE_IF_VERSION;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.DROP_TABLE;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.DROP_REPLICA;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.GET;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.GET_INDEXES;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.GET_REPLICA_STATS;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.GET_TABLE;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.GET_TABLE_USAGE;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.INTERNAL_DDL;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.INTERNAL_STATUS;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.LIST_TABLES;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.MULTI_DELETE;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.PREPARE;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.PUT;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.PUT_IF_ABSENT;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.PUT_IF_PRESENT;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.PUT_IF_VERSION;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.QUERY;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.SYSTEM_REQUEST;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.SYSTEM_STATUS_REQUEST;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.TABLE_REQUEST;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.WRITE_MULTIPLE;

import static oracle.nosql.common.http.Constants.KEEP_ALIVE;
import static oracle.nosql.common.http.Constants.CONTENT_LENGTH;
import static oracle.nosql.common.http.Constants.CONTENT_TYPE;

import static oracle.nosql.proxy.protocol.HttpConstants.NOSQL_DATA_PATH;
import static oracle.nosql.proxy.protocol.HttpConstants.PROXY_SERIAL_VERSION_HEADER;
import static oracle.nosql.proxy.protocol.HttpConstants.PROXY_VERSION_HEADER;
import static oracle.nosql.proxy.protocol.HttpConstants.REQUEST_ID_HEADER;
import static oracle.nosql.proxy.protocol.HttpConstants.X_FORWARDED_FOR_HEADER;
import static oracle.nosql.proxy.protocol.HttpConstants.X_REAL_IP_HEADER;
import static oracle.nosql.proxy.protocol.HttpConstants.pathInURIAllVersions;
import static oracle.nosql.proxy.protocol.NsonProtocol.*;
import static oracle.nosql.proxy.JsonCollSerializer.createValueFromNson;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.temporal.ChronoUnit;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.FaultException;
import oracle.kv.Key;
import oracle.kv.KVSecurityException;
import oracle.kv.MetadataNotFoundException;
import oracle.kv.PrepareQueryException;
import oracle.kv.ResourceLimitException;
import oracle.kv.Value;
import oracle.kv.Version;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.query.PreparedStatementImpl;
import oracle.kv.impl.api.table.PrimaryKeyImpl;
import oracle.kv.impl.api.query.QueryStatementResultImpl;
import oracle.kv.impl.api.query.QueryStatementResultImpl.QueryResultIterator;
import oracle.kv.impl.api.query.QueryPublisher.QuerySubscription;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableAPIImpl.GeneratedValueInfo;
import oracle.kv.impl.api.table.TableAPIImpl.OpFactory;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.ValueSerializer.RowSerializer;
import oracle.kv.impl.fault.InternalFaultException;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.runtime.ResumeInfo.TableResumeInfo;
import oracle.kv.impl.query.runtime.ResumeInfo.VirtualScan;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.PrepareCallback.QueryOperation;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FieldRange;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.RecordValue;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.Table;
import oracle.kv.table.TableOpExecutionException;
import oracle.kv.table.TableOperation;
import oracle.kv.table.TableOperationResult;
import oracle.kv.table.TimeToLive;
import oracle.kv.table.WriteOptions;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.http.LogControl;
import oracle.nosql.common.http.Service;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.kv.drl.LimiterManager;
import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.nson.Nson;
import oracle.nosql.nson.Nson.NsonSerializer;
import oracle.nosql.nson.util.NettyByteOutputStream;
import oracle.nosql.nson.values.MapWalker;
import oracle.nosql.nson.values.PathFinder;
import oracle.nosql.nson.values.TimestampValue;
import oracle.nosql.proxy.MonitorStats.OperationType;
import oracle.nosql.proxy.ProxySerialization.FieldValueWriterImpl;
import oracle.nosql.proxy.ProxySerialization.RowReaderImpl;
import oracle.nosql.proxy.ValueSerializer.RowSerializerImpl;
import oracle.nosql.proxy.audit.ProxyAuditManager;
import oracle.nosql.proxy.filter.FilterHandler;
import oracle.nosql.proxy.filter.FilterHandler.Filter;
import oracle.nosql.proxy.protocol.ByteInputStream;
import oracle.nosql.proxy.protocol.ByteOutputStream;
import oracle.nosql.proxy.protocol.JsonProtocol;
import oracle.nosql.proxy.protocol.Protocol;
import oracle.nosql.proxy.protocol.Protocol.OpCode;
import oracle.nosql.proxy.protocol.SerializationUtil;
import oracle.nosql.proxy.protocol.NsonProtocol;
import oracle.nosql.proxy.sc.GetTableResponse;
import oracle.nosql.proxy.sc.IndexResponse;
import oracle.nosql.proxy.sc.ListTableResponse;
import oracle.nosql.proxy.sc.TableUsageResponse;
import oracle.nosql.proxy.sc.TableUtils;
import oracle.nosql.proxy.sc.TableUtils.PrepareCB;
import oracle.nosql.proxy.sc.TableUtils.MapPrepareCB;
import oracle.nosql.proxy.sc.TenantManager;
import oracle.nosql.proxy.security.AccessContext;
import oracle.nosql.proxy.security.AccessContext.Type;
import oracle.nosql.proxy.util.ErrorManager;
import oracle.nosql.proxy.util.ProxyThreadPoolExecutor;
import oracle.nosql.proxy.util.TableCache.TableEntry;
import oracle.nosql.util.filter.Rule;
import oracle.nosql.util.tmi.IndexInfo;
import oracle.nosql.util.tmi.TableInfo;
import oracle.nosql.util.tmi.TableLimits;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * POST /V0/nosql/data
 */
public abstract class DataService extends DataServiceHandler implements Service {
    /*
     * Arbitrary max size for DDL operations not specific to a table
     * This is way too large but technically a create table might
     * be relatively large.
     */
    protected static final int DDL_REQUEST_SIZE_LIMIT = 1024 * 1024; // 1M

    /*
     * Keep-alive header parameters
     */
    private static final String KEEP_ALIVE_PARAMS = "timeout=60, max=1000000";

    private final String ADDITIONAL_ALLOWED_HEADERS =
        "x-nosql-request-id, x-nosql-compartment-id";


    /* true if we're allowing internal auth retries */
    private final boolean authRetriesEnabled;

    /* counter of total retries currently pending / active */
    private final AtomicInteger activeRetryCount;

    private final int maxActiveRetryCount;
    private final int maxRetriesPerRequest;
    private final int retryDelayMs;

    /*
     * If non-null use this executor for async completions for async KV
     * requests. This does not apply to all request types, only the simple
     * ones -- put, get, delete, multidelete, write multiple.
     * It is not used for query or prepare.
     */
    private final ProxyThreadPoolExecutor executor;

    /* create a default RC */
    private RequestContextFactory rcFactory = new
        RequestContextFactory() {
            @Override
            public RequestContext createRequestContext(
                FullHttpRequest request,
                ChannelHandlerContext ctx,
                LogContext lc,
                Object callerContext) {
                return new RequestContext(request, ctx, lc, callerContext);
            }
        };

    private final AtomicInteger activeWorkerThreads = new AtomicInteger(0);

    /*
     * A map used by calls to mark a failed operation, mapping OpCode to
     * OperationType for event logging.
     */
    private static final HashMap<OpCode, OperationType> opCodeMap =
        new HashMap<OpCode, OperationType>();

    static {
            opCodeMap.put(DELETE, OperationType.DELETE);
            opCodeMap.put(DELETE_IF_VERSION, OperationType.DELETE);
            opCodeMap.put(GET, OperationType.GET);
            opCodeMap.put(PUT, OperationType.PUT);
            opCodeMap.put(PUT_IF_ABSENT, OperationType.PUT);
            opCodeMap.put(PUT_IF_PRESENT, OperationType.PUT);
            opCodeMap.put(PUT_IF_VERSION, OperationType.PUT);
            opCodeMap.put(QUERY, OperationType.QUERY);
            opCodeMap.put(PREPARE, OperationType.PREPARE);
            opCodeMap.put(WRITE_MULTIPLE, OperationType.WRITE_MULTIPLE);
            opCodeMap.put(MULTI_DELETE, OperationType.MULTI_DELETE);
            opCodeMap.put(TABLE_REQUEST, OperationType.TABLE_REQUEST);
            opCodeMap.put(GET_TABLE, OperationType.GET_TABLE);
            opCodeMap.put(GET_TABLE_USAGE, OperationType.GET_TABLE_USAGE);
            opCodeMap.put(LIST_TABLES, OperationType.LIST_TABLES);
            opCodeMap.put(GET_INDEXES, OperationType.GET_INDEXES);
            opCodeMap.put(SYSTEM_REQUEST, OperationType.SYSTEM_REQUEST);
            opCodeMap.put(SYSTEM_STATUS_REQUEST,
                          OperationType.SYSTEM_STATUS_REQUEST);
            opCodeMap.put(ADD_REPLICA, OperationType.TABLE_REQUEST);
            opCodeMap.put(DROP_REPLICA, OperationType.TABLE_REQUEST);
            opCodeMap.put(INTERNAL_DDL, OperationType.TABLE_REQUEST);
            opCodeMap.put(INTERNAL_STATUS, OperationType.TABLE_REQUEST);
            opCodeMap.put(GET_REPLICA_STATS, OperationType.GET_TABLE_USAGE);
     }

    /*
     * The map of OpCode to handler methods used for dispatch of requests.
     * This can't be static because the methods in the map are not static.
     */
    private final HashMap<OpCode, ProxyOperation> operationMap =
        new HashMap<OpCode, ProxyOperation>();

    private void initOperation(OpCode op, ProxyOperation operation) {
        operationMap.put(op, operation);
    }

    private void initOperations() {

        initOperation(DELETE, this::handleDelete);
        initOperation(DELETE_IF_VERSION, this::handleDelete);
        initOperation(GET, this::handleGet);
        initOperation(PUT, this::handlePut);
        initOperation(PUT_IF_ABSENT, this::handlePut);
        initOperation(PUT_IF_PRESENT, this::handlePut);
        initOperation(PUT_IF_VERSION, this::handlePut);
        initOperation(WRITE_MULTIPLE, this::handleWriteMultiple);
        initOperation(MULTI_DELETE, this::handleMultiDelete);
        initOperation(QUERY, this::handleQuery);

        /* the following ops are not yet available async */
        initOperation(PREPARE, this::handlePrepare);
        initOperation(TABLE_REQUEST, this::handleTableOp);
        initOperation(GET_TABLE, this::handleGetTable);
        initOperation(GET_TABLE_USAGE, this::handleTableUsage);
        initOperation(LIST_TABLES, this::handleListTables);
        initOperation(GET_INDEXES, this::handleGetIndexes);
        initOperation(SYSTEM_REQUEST, this::handleSystemOp);
        initOperation(SYSTEM_STATUS_REQUEST, this::handleSystemStatus);

        /*
         * the following ops are for MR table
         *
         *   - ADD_REPLICA: add new replica on MR table
         *
         *   - DROP_REPLICA: drop replica on MR table.
         *
         *   - INTERNAL_DDL: execute cross region ddl issued from SC on home
         *     region where the ddl operation to MR table is initialized.
         *
         *   - INTERNAL_STATUS: execute cross region get-work-request or
         *     get-table issued from SC on remote region. This is used to get
         *     status of ddl execution and table on this region during cross
         *     region ddl execution.
         *
         *   - GET_REPLICA_STATS: get replication stats info of MR table.
         */
        initOperation(ADD_REPLICA, this::handleAddReplica);
        initOperation(DROP_REPLICA, this::handleDropReplica);
        initOperation(INTERNAL_DDL, this::handleInternalDdl);
        initOperation(INTERNAL_STATUS, this::handleInternalStatus);
        initOperation(GET_REPLICA_STATS, this::handleGetReplicaStats);
    }

    private void validateConfig(String conf, int val, int min, int max) {
        if (val >= min && val <= max) {
            return;
        }
        throw new IllegalArgumentException("Config value for field " + conf +
                      " (" + val + ") must be between " + min + " and " +
                      max + " inclusive.");
    }

    public DataService(SkLogger logger,
                       TenantManager tm,
                       MonitorStats stats,
                       ProxyAuditManager audit,
                       FilterHandler filter,
                       ErrorManager errorManager,
                       LimiterManager limiterManager,
                       Config config,
                       LogControl logControl) {

        super(logger, tm, stats, audit, filter,
              errorManager, limiterManager, config, logControl);
        this.authRetriesEnabled = config.getAuthRetriesEnabled();
        this.maxActiveRetryCount = config.getMaxActiveRetryCount();
        validateConfig(Config.MAX_ACTIVE_RETRY_COUNT.paramName,
                       maxActiveRetryCount, 1, 1000);
        this.maxRetriesPerRequest = config.getMaxRetriesPerRequest();
        validateConfig(Config.MAX_RETRIES_PER_REQUEST.paramName,
                       maxRetriesPerRequest, 1, 100);
        this.retryDelayMs = config.getRetryDelayMs();
        validateConfig(Config.RETRY_DELAY_MS.paramName,
                       retryDelayMs, 10, 100);
        this.activeRetryCount = new AtomicInteger(0);

        if (config.getKVThreadPoolSize() > 0) {
            /*
             * create an Executor to handle async completions from KV
             */
            executor = new ProxyThreadPoolExecutor(
                config.getKVThreadPoolSize(), "ProxyKVResponse");
        } else {
            executor = null;
        }

        initOperations();
    }

    /**
     * Allows the RequestContext to be created externally, by test code
     */
    public void setRequestContextFactory(RequestContextFactory factory) {
        this.rcFactory = factory;
    }

    /**
     * Handles a request.
     * Main entry point from common HttpServer to DataService.
     */

    @Override
    public FullHttpResponse handleRequest(FullHttpRequest request,
                                          ChannelHandlerContext ctx,
                                          LogContext lc) {
        return handleRequest(request, ctx, lc, null);
    }

    /**
     * Handles a request. This is an internal version that allows addition
     * of an option caller context object that is passed to the RequestContext.
     * This mostly facilitates calling the proxy without actually using a
     * Netty HTTP server.
     */
    protected FullHttpResponse handleRequest(FullHttpRequest request,
                                             ChannelHandlerContext ctx,
                                             LogContext lc,
                                             Object callerContext) {

        int threads = activeWorkerThreads.incrementAndGet();
        if (stats != null) {
            stats.markOpActiveWorkerThreads(threads);
        }
        /* get readonly/header fields from request, put in rc */
        RequestContext rc =
            rcFactory.createRequestContext(request, ctx, lc, callerContext);
        /* this service now always manages reference counting. */
        /* since returning from this method will release() the */
        /* message, retain() it here */
        request.retain();
        try {
            /* Block all requests if there is "big red button" rule */
            Rule rule = getBlockAll(lc);
            if (rule != null) {
                return filter.handleRequest(rc, rule);
            }

            /* Handle OPTIONS method for pre-flight request. */
            if (HttpMethod.OPTIONS.equals(rc.request.method())) {
                return handleOptions(rc.request, rc.lc);
            }
            /* read binary header. this may throw errors. */
            rc.readBinaryHeader(forceV3, forceV4);
            rc.opType = getOpType(rc.opCode);

            /* at this point the request has at least a valid binary header */
            handleRequest(rc);

            /* response sending managed by handleRequest() */
            return null;
        } catch (Exception e) {
            // e.printStackTrace();
            int code = BAD_PROTOCOL_MESSAGE;
            if (e instanceof RequestException) {
                code = ((RequestException)e).getErrorCode();
            }
            if (logger.isLoggable(Level.FINE, rc.lc)) {
                logger.fine("handleRequest() ERROR: code=" + code +
                            " msg=" + e.getMessage(), rc.lc);
            }
            FullHttpResponse resp =
                createErrorResponse(rc, e.getMessage(), code);
            finishOp(rc, resp);
            return null;
        } finally {
            activeWorkerThreads.decrementAndGet();
        }
    }

    /*
     * This method is split out because it may be called again in
     * cases of internal retries. It is definitely always called at least
     * once, by handleRequest() logic above
     */
    private void handleRequest(RequestContext rc) {
        if (logger.isLoggable(Level.FINE, rc.lc)) {
            logger.fine("handleRequest(), headers=" + rc.headers, rc.lc);
        }
        /* handle request */
        FullHttpResponse resp = handleRequestInternal(rc);
        /* finish op. this will send the response, if any. */
        finishOp(rc, resp);
    }

    /*
     * return true if response indicates an error that should
     * be part of error rate limiting.
     * Allow this to be overridden by subclasses.
     */
    protected boolean isErrorLimitingResponse(FullHttpResponse resp,
                                              ChannelHandlerContext ctx) {
        /* by default, onprem does not limit errors */
        return false;
    }

    /* Note: this method should never ever throw an exception */
    private FullHttpResponse handleRequestInternal(RequestContext rc) {
        /* Validate the input */
        final FullHttpResponse violation = validateHttpRequest(rc);
        if (violation != null) {
            return violation;
        }

        try {
            markOpStart(rc);
            return handleRequestWithContext(rc);
        } catch (Throwable e) {
            // e.printStackTrace();
            markOpFailed(rc, 1 /* serverFailure */);
            final String faultMsg = e.toString();
            if (logger.isLoggable(Level.INFO) &&
                limiter.isHandleable(faultMsg)) {
                /*
                 * include stack trace here because this path should be
                 * infrequent and may represent a bug in the proxy
                 */
                logger.logEvent("DataService" /* category */, Level.INFO,
                                "Request exception" /* subject */,
                                faultMsg /* message */,
                                e);
            }

            return createErrorResponse(rc, e.getMessage(),
                                       mapExceptionToErrorCode(e));
        }
    }

    /**
     * Manage retries for specific types of errors.
     * Currently this is only used for throttling errors and
     * SECURITY_INFO_UNAVAILABLE errors.
     */
    private boolean manageRetries(RequestContext rc,
                                  FullHttpResponse resp) {

        /* Only retry on an OK response with valid content */
        if (resp == null ||
            resp.status() != OK ||
            resp.content() == null ||
            resp.content().capacity() <= 0) {
            return false;
        }

        int delayMs = retryDelayMs;

        /* get error code from RequestContext */
        int code = rc.errorCode;
        if (code == SECURITY_INFO_UNAVAILABLE) {
            if (authRetriesEnabled == false) {
                return false;
            }
        } else {
            /* TODO: possibly retry other errors? */
            return false;
        }

        /* if we've retried more than N times already, don't retry */
        if (rc.numRetries >= maxRetriesPerRequest) {
            logger.fine("Too many retries for this request", rc.lc);
            return false;
        }

        return attemptRetry(rc, delayMs);
    }

    private boolean attemptRetry(RequestContext rc, int delayMs) {

        /* if response marked as excessive (possible DDoS), don't retry */
        if (rc.excessiveUse == true) {
            logger.fine("Excessive use: skipping retry", rc.lc);
            return false;
        }

        /* if we're within <retry delay> of the request timeout, don't retry */
        if ((System.nanoTime() + (delayMs * 1_000_000L))
                > rc.endTimeNs) {
            logger.fine("Almost at timeout: skipping retry", rc.lc);
            return false;
        }

        /*
         * NOTE: with use of ProxyThreadPoolExecutor for netty and/or KV
         * completions it's possible to get some queue size info
         *
         * TODO: get executor queue size. If above a threshold, don't
         * do internal retry. We currently can't do this, because netty's
         * NioEventExecutor doesn't expose the underlying queue.
         * So, instead we use a single atomic integer
         */
        int active = activeRetryCount.incrementAndGet();
        if (active > maxActiveRetryCount) {
            activeRetryCount.decrementAndGet();
            logger.fine("Too many active retries overall", rc.lc);
            return false;
        }

        /* increment count of retries */
        final int retries = rc.numRetries + 1;

        /* attempt to schedule this request for a retry */
        try {
            rc.ctx.executor().schedule(() -> {
                rc.numRetries = retries;
                try {
                    /* retry request */
                    handleRequest(rc);
                } finally {
                    /* decrement overall count of active retries */
                    activeRetryCount.decrementAndGet();
                }
            }, delayMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ree) {
            /* couldn't schedule: don't retry */
            logger.warning("retry got error: " + ree);
            activeRetryCount.decrementAndGet();
            return false;
        }

        logger.fine("Scheduled retry for " + delayMs +
                    "ms later. retries=" + retries, rc.lc);

        return true;
    }

    @Override
    protected String getAdditionalAllowedHeaders() {
        return ADDITIONAL_ALLOWED_HEADERS;
    }

    /*
     * Validate the request.
     * For cloud: does not allow AUTHORIZATION header to be null
     * For KV: null AUTHORIZATION header means accessing non-secure kvstore
     */
    protected FullHttpResponse validateHttpRequest(RequestContext rc) {

        /* basic validation already done in readBinaryHeaders() */
        return null;
    }

    /**
     * Returns true if the path indicates this service. Accept
     * /V0/nosql/data
     * /V2/nosql/data
     */
    @Override
    public boolean lookupService(String path) {
        return pathInURIAllVersions(path, NOSQL_DATA_PATH);
    }

    private FullHttpResponse handleRequestWithContext(RequestContext rc)
        throws IOException {

        boolean isAsync = false;

        rc.resetBuffers();
        rc.resetOpCode();
        rc.errorCode = 0;

        try {

            /* filters request based on opCode */
            filterRequest(rc.opCode, rc.lc);

            /*
             * table name is needed for the preamble call
             */
            if (!rc.isNson() && opHasTableName(rc.opCode)) {
                rc.origTableName =
                    readNonNullEmptyString(rc.bbis, "Table name");
                rc.handleV3TableNames();
            }

            /*
             * Do work common to all requests
             */
            doOperationPreamble(rc);

            /*
             * Get the operation method and call it
             */
            ProxyOperation operation = operationMap.get(rc.opCode);
            if (operation == null) {
                throw new RequestException(UNKNOWN_OPERATION,
                                           "Unknown op code: " + rc.opCode);
            }

            isAsync = operation.handle(rc);

            if (logger.isLoggable(Level.FINE, rc.lc)) {
                logger.fine("handleRequest, succeeded op=" +
                            opCodeToString(rc.opCode) + " namespace=" +
                            rc.actx.getNamespace() +
                            " table=" + rc.mappedTableName +
                            " async=" + isAsync, rc.lc);
            }
        } catch (Exception e) {
            // e.printStackTrace();
            return formulateErrorResponse(e, rc);
        }

        if (isAsync) {
            /*
             * async ops manage all headers/response writing.
             * returning null here will skip all callers' logic that
             * does the same.
             */
            return null;
        }

        return defaultHttpResponse(rc);
    }

    /*
     * Shared method to construct an HTTP response with required headers
     */
    private FullHttpResponse defaultHttpResponse(RequestContext rc) {
        FullHttpResponse resp =
            new DefaultFullHttpResponse(HTTP_1_1, OK, rc.bbos.buffer());

        resp.headers().set(CONTENT_TYPE, "application/octet-stream")
            .setInt(CONTENT_LENGTH, rc.bbos.buffer().readableBytes())
            .set(KEEP_ALIVE, KEEP_ALIVE_PARAMS)
            .set(PROXY_VERSION_HEADER, proxyVersionHeader)
            .set(PROXY_SERIAL_VERSION_HEADER, SERIAL_VERSION_STRING);
        if (rc.requestId != null) {
            resp.headers().set(REQUEST_ID_HEADER, rc.requestId);
        }

        if (rc.headers.contains(OPC_REQUEST_ID)) {
            /*
             * Intend to set header "Access-Control-Allow-Origin:*" for
             * console-only requests, the header "opc-request-id" is always
             * sent from console but not from other sources, just based on that
             * to identify the requests from console.
             */
            resp.headers().set(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
        }
        return resp;
    }

    /*
     * Returns a fully formed http response based on the
     * given error, or null if op should be retried.
     */
    private FullHttpResponse formulateErrorResponse(
            Throwable e,
            RequestContext rc) {
        try {
            try {
                if (e instanceof CompletionException) {
                    /*
                     * If we got a CompletionException, it needs to first be
                     * unwrapped. Eventually this should never happen, but
                     * for now we keep this in for safety.
                     * Don't unwrap (let fail) if running in tests.
                     */
                    if (!inTest()) {
                        e = e.getCause();
                    }
                }
                /* note purposeful potential double-unwrap */
                if (e instanceof ExcessiveUsageException) {
                    /*
                     * ExcessiveUsageException wraps the underlying reason, and
                     * indicates that this operation resulted in some resource
                     * being overused/abused (possible DDoS, etc).
                     * In this case, we set a flag in the request context so
                     * that upstream callers can act accordingly.
                     */
                    rc.excessiveUse = true;
                    /* base response on original exception */
                    e = e.getCause();
                }
                throw e;
            } catch (UncheckedIOException uc) {
                handleIOException(uc.getMessage(), uc.getCause());
            } catch (IOException ioe) {
                handleIOException(null, ioe);
            } catch (IllegalArgumentException iae) {
                /* common handling for IAE, not logged */
                throw new RequestException(ILLEGAL_ARGUMENT,
                                       (opCodeToString(rc.opCode) +
                                        ": Illegal Argument: " +
                                        iae.getMessage()));
            } catch (PrepareQueryException pqe) {
                int errorCode = (rc.queryVersion >= QUERY_V5) ?
                                RECOMPILE_QUERY : ILLEGAL_ARGUMENT;
                throw new RequestException(errorCode,
                                       (opCodeToString(rc.opCode) +
                                        ": " + pqe.getMessage()));
            } catch (ResourceLimitException rle) {
                /* logged below, if at all */
                handleLimitException(rle);
            } catch (KVSecurityException kvse) {
                /*
                 * For cloud, security exception is unexpected here, throw
                 * UNKNOWN_ERROR.
                 * For KV, this will throw proper request exception
                 * with error code for KV proxy driver to retry
                 * authentication.
                 */
                handleKVSecurityException(rc.opCode, kvse);
            } catch (FaultException fe) {
                /*
                 * Query preparation may fail on a getTable() call that is
                 * throttled. The problem is that the exception itself is
                 * buried in a RepNodeAdminFaultException, an instance of
                 * InternalFaultException. This should be fixed,
                 * but in the meantime, look at the underlying class name to
                 * know if it's a ResourceLimitException.
                 *
                 * Once the kv bug is fixed the exception will be a
                 * ResourceLimitException, handled above.
                 */
                Throwable cause = fe.getCause();
                if (cause != null) {
                    if (cause instanceof InternalFaultException) {
                        String faultName =
                            ((InternalFaultException)cause).getFaultClassName();
                        if (oracle.kv.ReadThroughputException.class.getName()
                            .equals(faultName)) {
                            /*
                             * KV appends the release in parens at the end of
                             * the exception, trim that off. Ideally we'd map
                             * the table name from OCID to real name,
                             * but that's trickier parsing.
                             */
                            String msg =
                                cause.getMessage().substring(
                                    0, cause.getMessage().indexOf('('));
                            throw new RequestException(
                                READ_LIMIT_EXCEEDED,
                                "Operation failed with limit exception: " +
                                msg);
                        }
                    }
                }
                /* TODO: map exception to more specific error codes */
                //fe.printStackTrace();
                throw new RequestException(SERVER_ERROR,
                                       opCodeToString(rc.opCode) +
                                               ": Server too busy: " +
                                       fe.getMessage());
            } catch (MetadataNotFoundException mnfe) {
                /* Handle a single internal retry for metadata not found */
                if (rc.numRetries == 0) {
                    if (rc.tableOpInfos != null) {
                        /*
                         * We don't know which table had an MNFE error.
                         * So we clear the table cache entries for all
                         * tables in the multi-table request. Ugh.
                         *
                         * Try to minimize disruption by just getting the
                         * unique entries to clear...
                         */
                        for (TableOperationInfo top : rc.uniqueOpInfos()) {
                            clearTableEntry(top.actx.getNamespace(),
                                            top.mappedTableName,
                                            top.origTableName,
                                            rc.lc);
                        }
                    } else if (rc.origTableName != null) {
                        clearTableEntry(rc.actx.getNamespace(),
                                        rc.mappedTableName,
                                        rc.origTableName,
                                        rc.lc);
                    } else {
                        /*
                         * TODO: remove this ridiculously expensive logic!
                         * Clear entire cache. Maybe add a way to flush just
                         * the portion used by the namespace. That requires
                         * change in TableCache.
                         */
                        logger.info("Proxy received metadata not found, " +
                                    "clearing entire table cache and retrying",
                                    rc.lc);
                        tableCache.clear();
                    }

                    resetIAMTableNameMapping(rc);
                    if (attemptRetry(rc, 0)) {
                        logger.fine("Retry succeeded after MNFE cache op");
                        return null;
                    }
                    /* if attempt to retry fails, carry on as normal, below */
                }
                /*
                 * Map to table not found
                 */
                String msg = "Table not found: " + mnfe.getMessage();
                if (rc.origTableName != null) {
                    msg += ", name: " + rc.origTableName;
                } else if (rc.tableOpInfos != null) {
                    msg += ", name: " +
                               Arrays.toString(rc.getUniqueTableNames());
                }
                throw new RequestException(TABLE_NOT_FOUND, msg);
            } catch (RequestException re) {
                /*
                 * Attempt a single retry after resetting if table is not
                 * found and using the cloud. There are additional conditions
                 * required as well.
                 */
                if (re.getErrorCode() != TABLE_NOT_FOUND || rc.numRetries > 0) {
                    throw re;
                }

                /*
                 * No need to retry/reset tenant manager ops or ops with no
                 * table(s)
                 */
                if (OpCode.isTMOperation(rc.opCode) ||
                    (rc.origTableName == null && rc.tableOpInfos == null)) {
                    throw re;
                }

                /*
                 * Don't do retries for multiple-table ops. The logic
                 * gets too complex with various edge cases.
                 */
                if (rc.origTableName == null) {
                    throw re;
                }

                if (resetIAMTableNameMapping(rc) == false) {
                    /* not IAM access context */
                    throw re;
                }

                logger.fine("Attempting retry for TABLE_NOT_FOUND", rc.lc);
                if (attemptRetry(rc, 0)) {
                    return null;
                }
                /* failure to retry falls through to throw */
                throw re;
            } catch (FilterRequestException fre) {
                /* this will currently always return null. Hmmm... */
                return handleFilterRequest(fre, rc);
            } catch (Throwable t) {
                /*
                 * This error may indicate a bug in the proxy. Make sure
                 * that it is not retryable.
                 */
                //t.printStackTrace();
                logger.info("Unexpected server side exception: " + t);
                throw new RequestException(UNKNOWN_ERROR,
                                           opCodeToString(rc.opCode) +
                                           ": Unexpected exception: " +
                                           t);
            }
        } catch (RequestException re) {
            logRequestException(re, rc.lc);
            markOpFailed(rc, getRequestExceptionFailure(re));

            String msg = re.getMessage();
            if (rc.mappedTableName != null) {
                msg = mapErrorMessage(rc.actx, rc.mappedTableName, msg);
            } else if (rc.tableOpInfos != null) {
                /*
                 * Sort AccessContexts in the reverse case-insensitive
                 * order of mapped table name, so that child table name
                 * is processed before its parent.
                 */
                Map<String, AccessContext> actxMap =
                    new TreeMap<>(String.CASE_INSENSITIVE_ORDER.reversed());
                for (TableOperationInfo top : rc.tableOpInfos) {
                    actxMap.put(top.mappedTableName, top.actx);
                }
                msg = mapErrorMessage(actxMap, msg);
            }
            return createErrorResponse(rc, msg, re.getErrorCode());
        }
        return null;
    }

    /* return true if using IAM access context(s) */
    private boolean resetIAMTableNameMapping(RequestContext rc) {
        boolean isIAM = false;
        /* do not reset multiple table ops */
        //if (rc.tableOpInfos != null) { ...

        if (rc.actx != null && rc.actx.getType() == Type.IAM) {
            rc.actx.resetTableNameMapping();
            isIAM = true;
        }
        return isIAM;
    }

    private void clearTableEntry(String namespace,
                                 String mappedTableName,
                                 String origTableName,
                                 LogContext lc) {
        /* remove the table from the cache */
        logger.info("Proxy received metadata not found, clearing table " +
                    "cache entry for table " + origTableName + " and " +
                    "retrying", lc);
        tableCache.flushEntry(namespace, mappedTableName);
    }

    /*
     * This method is for handling KV security exception of KV HTTP proxy.
     * In cloud, KVSecurityException should not be happened at this level, log
     * server error in this case.
     */
    protected void handleKVSecurityException(OpCode op,
                                             KVSecurityException kvse) {
        logger.warning("Unexpected server side security exception: " + kvse);
        throw new RequestException(
            UNKNOWN_ERROR,
            "Operation failed with server side security exception: " +
            kvse.getMessage());
    }

    /**
     * Send response back to client.
     * This performs the same logic as old upstream callers.
     *
     * This is typically called from async handlers in their
     * future lamda functions when they are finished processing.
     *
     * This includes:
     *  - adding http headers
     *  - calling "response done" trigger
     *  - write and flush response to channel context
     *
     * After this, no additional processing of this response is
     * necessary - in fact any more processing would be an error.
     */
    private void finishOp(RequestContext rc,
                          FullHttpResponse response) {
        if (response == null || rc == null) {
            return;
        }

        /*
         * test-mostly, allow rc to use state; it must not modify response
         */
        rc.finishOp(response);

        /* manage internal retries */
        if (manageRetries(rc, response)) {
            /* request will be retried a short time later */
            return;
        }

        /* at this point we will not retry the request. */
        /* we must now free it up. */
        /* note the _response_ may still be sent later */
        rc.releaseBuffers();

        if (isErrorLimitingResponse(response, rc.ctx)) {
            if (incrementErrorRate(response, rc)) {
                /* response sent later, or not at all */
                return;
            }
        }

        /*
         * Rate limiting logic: if the operation used read/write
         * units, apply them to the rate limiter(s) for the table.
         * This may result in the response being queued for later
         * delivery (response delayed).
         */
        if (rateLimitResponse(limiterManager, rc, response)) {
            /* response sent later, by different thread */
            return;
        }

        endAudit(response, rc);
        sendResponse(response, rc);
    }

    protected RowSerializer getPrimaryKeySerializer(ByteInputStream bbis,
                                                    Table table,
                                                    RequestLimits limits)
    throws IOException {
        return readPrimaryKeySerializer(
            bbis, table,
            (limits == null ? -1 : limits.getPrimaryKeySizeLimit()));
    }

    protected RowSerializer getRowSerializer(ByteInputStream bbis,
                                             Table table,
                                             boolean exact,
                                             RequestLimits limits)
    throws IOException {
        return readRowSerializer(
            bbis, table,
            (limits == null ? -1 : limits.getPrimaryKeySizeLimit()),
            (limits == null ? -1 : limits.getRowSizeLimit()), exact);
    }

    /**
     * Default - nothing to do, return the same name.
     */
    @Override
    protected String mapTableName(String tableName) {
        return tableName;
    }

    /**
     * Default - nothing to do, return the same name.
     */
    @Override
    protected String mapTableName(AccessContext actx, OpCode op, String name) {
        return name;
    }

    /**
     * Clean up resources on service shutdown
     */
    @Override
    public void shutDown() {
        tm.shutDown();
        if (executor != null) {
            executor.shutdown(true);
        }
    }

    /**
     * Default - nothing to do, return the original error message.
     */
    protected String mapErrorMessage(AccessContext actx,
                                     String tableName,
                                     String message) {
        return filterStackTraces(message);
    }

    /**
     * Default - nothing to do, return the original error message.
     *
     * Used by writeMultiple for multiple tables case
     */
    protected String mapErrorMessage(Map<String, AccessContext> tableActxs,
                                     String message) {
        return filterStackTraces(message);
    }

    /**
     * Get
     */
    private boolean handleGet(final RequestContext rc)
        throws IOException {

        final GetOpInfo info = new GetOpInfo();
        if (rc.isNson()) {
            getV4GetOpInfo(info, rc);
        } else {
            getGetOpInfo(info, rc.bbis);
        }
        rc.entry = getTableEntry(rc.actx, rc.mappedTableName, rc.lc);
        checkRequestSizeLimit(rc.bbis.available(), false,
                              rc.entry.getRequestLimits());
        Table table = rc.entry.getTable();
        TableAPIImpl tableApi = rc.entry.getTableAPI();

        /*
         * if DRL is enabled, and the request specifies to return a throttling
         * exception instead of slowing down due to rate limiting, check
         * here if the table is over its rate limit. If so, return a throttling
         * error.
         */
        if (requestWantsThrottlingErrors(limiterManager, rc)) {
            limiterManager.throwIfOverLimit(rc.origTableName,
                                            (TableImpl)table,
                                            rc.entry.getStoreName(),
                                            true, false);
        }

        /* rc.bbis must be positioned at the primary key */
        RowSerializer pkey =
            getPrimaryKeySerializer(rc.bbis, table, rc.entry.getRequestLimits());

        ReadOptions options = createReadOptions(info.consistency, rc.timeoutMs,
                                                rc.actx.getAuthString(), rc.lc);

        if (!useAsync()) {
            Result res = doGet(tableApi, pkey, options);
            handleGetResponse(res, null, rc, info, pkey);
            /* handleGetResponse sends the reply so pretend this is async */
            return true;
        } else {
            CompletableFuture<Result> future =
                future = tableApi.getAsyncInternal(pkey, options);
            future.whenComplete((result, e) -> {
                    if (executor != null) {
                        executor.execute(()-> {
                                handleGetResponse(result, e, rc, info, pkey);
                            });
                    } else {
                        handleGetResponse(result, e, rc, info, pkey);
                    }
                });
        }
        /* true == async */
        return true;
    }

    private void handleGetResponse(final Result result,
                                   final Throwable e,
                                   final RequestContext rc,
                                   final GetOpInfo info,
                                   final RowSerializer pkey) {

        Table table = rc.entry.getTable();
        TableAPIImpl tableApi = rc.entry.getTableAPI();
        FullHttpResponse resp = null;
        try {
            if (e != null) {
                resp = formulateErrorResponse(e, rc);
            } else {
                if (rc.isNson()) {
                    NsonProtocol.writeGetResponse(rc, this, result,
                                                  info.consistency,
                                                  tableApi,
                                                  (TableImpl)table,
                                                  pkey);
                } else {
                    writeSuccess(rc.bbos);
                    writeThroughput(rc.bbos,
                                    result.getReadKB(),
                                    result.getWriteKB(),
                                    isAbsolute(info.consistency));
                    if (result.getSuccess()) {
                        rc.bbos.writeBoolean(true);
                        /* Read row */
                        RowReaderImpl reader =
                            new RowReaderImpl(rc.bbos, table);
                        tableApi.createRowFromGetResult(
                            result, pkey, reader);
                        reader.done();
                        writeExpirationTime(rc.bbos,
                                            reader.getExpirationTime());
                        writeVersion(rc.bbos, reader.getVersion());
                        if (rc.serialVersion > Protocol.V2) {
                            writeModificationTime(rc.bbos,
                                                  reader.getModificationTime());
                        }
                    } else {
                        rc.bbos.writeBoolean(false);
                    }
                }
                markDataOpSucceeded(rc,
                                    result.getNumRecords(),
                                    result.getReadKB(),
                                    result.getWriteKB());
                rc.setThroughput(result);
                resp = defaultHttpResponse(rc);
            }
        } catch (Exception ue) {
            logger.info("Unexpected exception in getAsync response " +
                        "builder method: " + ue.getMessage(), rc.lc);
            resp = formulateErrorResponse(ue, rc);
        } finally {
            finishOp(rc, resp);
        }
    }

    /**
     * Put
     */
    private boolean handlePut(RequestContext rc)
        throws IOException {

        PutOpInfo info = new PutOpInfo();
        if (rc.isNson()) {
            getV4PutOpInfo(info, rc, false);
        } else {
            getPutOpInfo(info, rc);
        }

        rc.entry = getTableEntry(rc.actx, rc.mappedTableName, rc.lc);
        checkRequestSizeLimit(rc.bbis.available(), false,
                              rc.entry.getRequestLimits());
        TableImpl table = (TableImpl) rc.entry.getTable();
        TableAPIImpl tableApi = rc.entry.getTableAPI();

        /*
         * if DRL is enabled, and the request specifies to return a throttling
         * exception instead of slowing down due to rate limiting, check
         * here if the table is over its rate limit. If so, return a throttling
         * error.
         */
        if (requestWantsThrottlingErrors(limiterManager, rc)) {
            limiterManager.throwIfOverLimit(rc.origTableName,
                                            table,
                                            rc.entry.getStoreName(),
                                            false, true);
        }

        ReturnRow.Choice returnChoice =
            (info.returnInfo ? ReturnRow.Choice.ALL : null);

        RowSerializer row = table.isJsonCollection() ? null :
            getRowSerializer(rc.bbis, table, info.exactMatch,
                             rc.entry.getRequestLimits());

        if (!rc.isNson()) {
            info.updateTTL = rc.bbis.readBoolean();
            info.TTL = readTTL(rc.bbis);
        }
        if (info.TTL != null && row != null) {
            ((RowSerializerImpl)row).setTTL(info.TTL);
        }

        /*
         * ReturnRow is set from request.
         */
        final ReturnRow returnRow;
        if (returnChoice != null) {
            returnRow = table.createReturnRow(returnChoice);
        } else {
            returnRow = null;
        }

        if (!rc.isNson()) {
            info.matchVersion = (rc.opCode == PUT_IF_VERSION) ?
                readVersion(rc.bbis) : null;
        }

        WriteOptions options = createWriteOptions(rc.timeoutMs,
                                                  info.durability,
                                                  info.updateTTL,
                                                  info.identityCacheSize,
                                                  rc.actx.getAuthString(),
                                                  false /* doTombstone */,
                                                  rc.lc);

        /*
         * A put request requires 3 things:
         * 1. GeneratedValueInfo (for identity cols and UUID)
         * 2. Key -- primary key for row
         * 3. Value -- value
         */

        final GeneratedValueInfo genInfo =
            TableAPIImpl.makeGenInfo(table, options);

        Key kvKey = null;
        Value kvValue = null;
        KVStoreImpl store = tableApi.getStore();

        if (table.isJsonCollection()) {

            /*
             * Create Key and Value directly from NSON input, efficiently.
             * This call must also handle MR counters, if present, by
             *  1. ensuring if they are present that they have a path
             *  2. translating/replacing any value for the counter to the
             *  default (0)
             * Also, if the table is MR (without or without counters) the
             * appropriate format and region id need to be set in the Value
             * that is created.
             */
            PrimaryKeyImpl pkey = (PrimaryKeyImpl) table.createPrimaryKey();
            /* this is in case returnRow is not null and needs to be set */
            row = pkey;
            /* this method fills in PrimaryKey as well */
            kvValue = createValueFromNson(table, pkey, rc.bbis);
            kvKey = table.createKeyInternal(pkey, false, store, genInfo);
        } else {
            kvKey = table.createKeyInternal(row, false, store, genInfo);
            kvValue = table.createValueInternal(row, store, genInfo);
        }
        /*
         * This makes all variants of put request classes
         */
        Request rq = tableApi.makePutRequest(rc.getPutOpCode(),
                                             kvKey,
                                             kvValue,
                                             table,
                                             returnRow,
                                             info.TTL,
                                             options,
                                             info.matchVersion);

        /* this is needed to allow access from Lambda fcns below */
        final RowSerializer rowForReturnRow = row;
        if (!useAsync()) {
            Result res = tableApi.getStore().executeRequest(rq);
            handlePutResponse(res, null, rc,
                              genInfo, rowForReturnRow, returnRow);
            /* handlePutResponse sends the reply so pretend this is async */
            return true;
        } else {
            CompletableFuture<Result> future = store.executeRequestAsync(rq);
            future.whenComplete((result, e) -> {
                    if (executor != null) {
                        executor.execute(()-> {
                                handlePutResponse(result, e, rc,
                                          genInfo, rowForReturnRow, returnRow);
                            });
                    } else {
                        handlePutResponse(result, e, rc,
                                          genInfo, rowForReturnRow, returnRow);

                    }
                });
        }

        /* true == async */
        return true;
    }

    private void handlePutResponse(Result result, Throwable e,
                                   RequestContext rc,
                                   GeneratedValueInfo genInfo,
                                   RowSerializer rowForReturnRow,
                                   ReturnRow returnRow) {
        TableImpl table = (TableImpl) rc.entry.getTable();
        TableAPIImpl tableApi = rc.entry.getTableAPI();
        FullHttpResponse resp = null;
        try {
            if (e != null) {
                resp = formulateErrorResponse(e, rc);
            } else {
                if (genInfo != null) {
                    result.setGeneratedValue(genInfo.getGeneratedValue());
                }

                if (rc.isNson()) {
                    NsonProtocol.writePutResponse(rc, this, result,
                                                  tableApi,
                                                  rowForReturnRow,
                                                  returnRow);
                } else {
                    Version version = result.getNewVersion();

                    writeSuccess(rc.bbos);
                    writeThroughput(rc.bbos,
                                    result.getReadKB(),
                                    result.getWriteKB(),
                                    true); // absolute

                    /*
                     * version and previous info are independent. A
                     * response may have one, the other or both.
                     */
                    if (version != null) {
                        rc.bbos.writeBoolean(true);
                        writeVersion(rc.bbos, version);
                    } else {
                        rc.bbos.writeBoolean(false);
                    }

                    /* return row */
                    /* Note this is where the bbis/input row */
                    /* may be referenced... */
                    writeExistingRow(rc.bbos, (version != null),
                                     returnRow, tableApi,
                                     rowForReturnRow, result,
                                     rc.serialVersion);

                    /* generated value for identity column or uuid */
                    if (rc.serialVersion > V1) {
                        /* only return it if the operation succeeded */
                        if ((table.hasIdentityColumn() ||
                             table.hasUUIDcolumn())
                            && version != null) {
                            FieldValue generated =
                                result.getGeneratedValue();
                            if (generated != null) {
                                rc.bbos.writeBoolean(true);
                                writeFieldValue(rc.bbos, generated);
                            } else {
                                rc.bbos.writeBoolean(false);
                            }
                        } else {
                            rc.bbos.writeBoolean(false);
                        }
                    }
                }
                markDataOpSucceeded(rc,
                                    result.getNumRecords(),
                                    result.getReadKB(),
                                    result.getWriteKB());
                rc.setThroughput(result);
                resp = defaultHttpResponse(rc);
            }
        } catch (Exception ue) {
            logger.info("Unexpected exception in putAsync response " +
                        "builder method: " + ue.getMessage(), rc.lc);
            resp = formulateErrorResponse(ue, rc);
        } finally {
            finishOp(rc, resp);
        }
    }

    /**
     * Delete
     */
    private boolean handleDelete(RequestContext rc)
        throws IOException {

        DelOpInfo info = new DelOpInfo();
        if (rc.isNson()) {
            getV4DelOpInfo(info, rc, false);
        } else {
            getDelOpInfo(info, rc);
        }

        ReturnRow.Choice returnChoice =
            (info.returnInfo ? ReturnRow.Choice.ALL : null);

        rc.entry = getTableEntry(rc.actx, rc.mappedTableName, rc.lc);
        checkRequestSizeLimit(rc.bbis.available(), false,
                              rc.entry.getRequestLimits());
        Table table = rc.entry.getTable();
        TableAPIImpl tableApi = rc.entry.getTableAPI();

        /*
         * if DRL is enabled, and the request specifies to return a throttling
         * exception instead of slowing down due to rate limiting, check
         * here if the table is over its rate limit. If so, return a throttling
         * error.
         */
        if (requestWantsThrottlingErrors(limiterManager, rc)) {
            limiterManager.throwIfOverLimit(rc.origTableName,
                                            (TableImpl)table,
                                            rc.entry.getStoreName(),
                                            false, true);
        }

        RowSerializer pkey =
            getPrimaryKeySerializer(rc.bbis, table,
                                    rc.entry.getRequestLimits());
        /*
         * ReturnRow is set from request.
         */
        final ReturnRow returnRow;
        if (returnChoice != null) {
            returnRow = table.createReturnRow(returnChoice);
        } else {
            returnRow = null;
        }

        if (!rc.isNson()) {
            info.matchVersion = (rc.opCode == OpCode.DELETE_IF_VERSION) ?
                readVersion(rc.bbis) : null;
        }

        WriteOptions options = createWriteOptions(rc.timeoutMs,
                                                  info.durability,
                                                  rc.actx.getAuthString(),
                                                  doTombStone(rc.entry),
                                                  rc.lc);

        if (!useAsync()) {
            Result res = doDelete(tableApi, pkey, info.matchVersion,
                                  returnRow, options);
            handleDeleteResponse(res, null, rc, pkey, returnRow);
            /* handleDeleteResponse sends the reply so pretend this is async */
            return true;
        } else {
            CompletableFuture<Result> future;
            if (info.matchVersion == null) {
                future = tableApi.deleteAsyncInternal(pkey, returnRow, options);
            } else {
                future = tableApi.deleteIfVersionAsyncInternal(
                    pkey,
                    info.matchVersion,
                    returnRow,
                    options);
            }
            future.whenComplete((result, e) -> {
                    if (executor != null) {
                        executor.execute(()-> {
                                handleDeleteResponse(result, e, rc,
                                                     pkey, returnRow);
                            });
                    } else {
                        handleDeleteResponse(result, e, rc, pkey, returnRow);
                    }
                });
        }

        /* true == asynchronous */
        return true;
    }

    private void handleDeleteResponse(final Result result,
                                      final Throwable e,
                                      final RequestContext rc,
                                      final RowSerializer pkey,
                                      final ReturnRow returnRow) {
        FullHttpResponse resp = null;
        TableAPIImpl tableApi = rc.entry.getTableAPI();
        try {
            if (e != null) {
                resp = formulateErrorResponse(e, rc);
            } else {
                if (rc.isNson()) {
                    NsonProtocol.writeDeleteResponse(rc, this, result,
                                                     tableApi, pkey,
                                                     returnRow);
                } else {
                    writeSuccess(rc.bbos);
                    writeThroughput(rc.bbos,
                                    result.getReadKB(),
                                    result.getWriteKB(),
                                    true); // absolute

                    /* did the delete happen? */
                    rc.bbos.writeBoolean(result.getSuccess());

                    /* return row */
                    writeExistingRow(rc.bbos, result.getSuccess(),
                                     returnRow,
                                     tableApi, pkey, result,
                                     rc.serialVersion);
                }
                markDataOpSucceeded(rc,
                                    result.getNumRecords(),
                                    result.getReadKB(),
                                    result.getWriteKB());
                rc.setThroughput(result);
                resp = defaultHttpResponse(rc);
            }
        } catch (Exception ue) {
            logger.info("Unexpected exception in deleteAsync response " +
                        "builder method: " + ue.getMessage(), rc.lc);
            resp = formulateErrorResponse(ue, rc);
        } finally {
            finishOp(rc, resp);
        }
    }

    private String chooseNamespace(String actxNamespace, String queryNamespace) {
        if (isOnPrem()) {
            /* onprem, the namespace in the query takes priority */
            if (queryNamespace != null) {
                return queryNamespace;
            }
            return actxNamespace;
        }
        /* otherwise, context namespace takes priority */
        if (actxNamespace != null) {
            return actxNamespace;
        }
        return queryNamespace;
    }

    /**
     * Query
     *
     * Note that the use of a separate Executor and thread pool
     * to handle async responses from KV requests does not apply to queries
     */
    private boolean handleQuery(final RequestContext rc)
        throws IOException {

        int requestSize = rc.bbis.available();
        final QueryOpInfo info = new QueryOpInfo();
        if (rc.isNson()) {
            getV4QueryOpInfo(info, rc);
        } else {
            getQueryOpInfo(info, rc);
        }
        boolean doAsync = useAsync();

        //trace("driver topo seq num = " + rc.driverTopoSeqNum, rc.lc);

        /* if not tracing queries, ignore traceLevel */
        if (!getTraceQueries()) {
            info.traceLevel = 0;
        }

        /*
         * Note: if not explicitly set by the app, maxReadKB, maxWriteKB, and
         * limit are all 0 here, and will be stored as 0 in the new execOpts
         * instance. However, adjustMaxReadKB() and adjustMaxWriteKB() are
         * called below and, if this is a proxy for the cloud, set the
         * maxReadKB and maxWriteKB to 2MB, and the execOpts is updated as
         * well. The limit remains 0, but ExecuteOptions.getResultsBatchSize()
         * returns the appropriate default in this case.
         */
        ExecuteOptions execOpts =
            createExecuteOptions(rc.actx.getNamespace(),
                                 info.consistency,
                                 rc.timeoutMs,
                                 info.contKey,
                                 rc.driverTopoSeqNum,
                                 info.virtualScan,
                                 info.maxReadKB, info.maxWriteKB, info.limit,
                                 info.mathContext,
                                 info.traceLevel,
                                 info.logFileTracing,
                                 info.queryName,
                                 info.batchCounter,
                                 info.queryVersion,
                                 info.maxServerMemory,
                                 rc.actx.getAuthString(),
                                 rc.lc);
        /*
         * If not onprem, durability will be null. See mapDurability()
         */
        if (info.durability != null) {
            execOpts.setDurability(info.durability);
        }

        final int prepCost;
        boolean isUpdateOp = false;
        boolean isAbsolute;
        final PreparedStatementImpl prep;
        final TableUtils.PrepareCB cbInfo;
        KVStoreImpl store;
        Map<String, FieldValue> variables = null;
        AccessContextHelper actxHelper;
        Filter opFilter;
        final QueryOperation queryOp;

        if (info.isPrepared == false) {

            if (info.numOperations != 0 || info.operationNumber != 0) {
                throw new IllegalArgumentException(
                    "Parallel queries require a prepared query");
            }
            /* this method also enforces limit on query string length */
            cbInfo = TableUtils.getCallbackInfo(rc.actx, info.statement, tm);
            cbInfo.checkSupportedDml();
            queryOp = cbInfo.getOperation();

            OpCode opCode = TableUtils.getDMLOpCode(cbInfo.getOperation());

            /*
             * Determine the actual OpCode of query, run filter check with
             * operations-only rules to check if the op should be blocked or
             * not.
             */
            opFilter = getQueryFilter(cbInfo.getOperation());
            opFilter.filterRequest(opCode, null, null, null, rc.lc);

            isUpdateOp = cbInfo.isUpdateOp();
            isAbsolute =
                (info.consistency == Consistency.ABSOLUTE || isUpdateOp);

            String ns = chooseNamespace(rc.actx.getNamespace(),
                                        cbInfo.getNamespace());

            actxHelper = checkAccess(rc.request, ns, opCode,
                                     cbInfo.getTableName(),
                                     cbInfo.getNotTargetTables(),
                                     opFilter, rc.lc);

            if (info.traceLevel >= 5) {
                trace("preparing query for requestId=" + rc.requestId +
                    ": " + info.statement, rc.lc);
            }

            PrepareResult prepRet = doPrepare(rc, actxHelper,
                                              cbInfo, ns,
                                              opCode, execOpts, info.statement,
                                              info.queryVersion, rc.lc);
            prep = prepRet.getPreparedStatement();
            store = prepRet.getStore();

            prepCost = MIN_QUERY_COST;
            info.isSimpleQuery = prep.isSimpleQuery();
            rc.origTableName = cbInfo.getTableName();
            rc.mappedTableName = getMappedTableName(rc.origTableName,
                                                    rc.actx, opCode);
            rc.entry = prepRet.getEntry();
        } else {
            cbInfo = null;
            prepCost = 0;
            queryOp = info.psw.getOperation();
            opFilter = getQueryFilter(queryOp);
            isUpdateOp = info.psw.isUpdateOp();
            OpCode opCode = (isUpdateOp ? OpCode.PUT : OpCode.GET);

            /*
             * Determine the actual OpCode of query, run filter check with
             * operations-only rules to check if the op should be blocked or
             * not.
             */
            opFilter.filterRequest(opCode, null, null, null, rc.lc);

            prep = info.psw.statement;

            isAbsolute =
                (info.consistency == Consistency.ABSOLUTE || isUpdateOp);

            /*
             * Check access again since table name and query operation
             * are available until now.
             * This check is for cloud only
             */
            rc.origTableName = info.psw.getTableName();
            String ns = chooseNamespace(rc.actx.getNamespace(),
                                        info.psw.getNamespace());
            if (rc.actx.getType() == Type.IAM) {
                actxHelper = checkAccess(rc.request, ns, opCode,
                                         info.psw.getTableName(),
                                         info.psw.getNotTargetTables(),
                                         opFilter, rc.lc);
                rc.actx = actxHelper.get(info.psw.getTableName());
                rc.mappedTableName = mapTableName(rc.actx, opCode,
                                                  rc.origTableName);
                execOpts.setPrepareCallback(new MapPrepareCB(actxHelper));
                ns = rc.actx.getNamespace();
            } else {
                /*
                 * for onprem and cloudsim, update log context with namespace
                 * information
                 */
                updateLogContext(rc.lc, ns, rc.origTableName, opCode);
                rc.mappedTableName = rc.origTableName;
            }

            /* read external variables if any exist */
            if (rc.isNson()) {
                /* done already in V4 payload read */
                variables = info.bindVars;
            } else {
                variables = getQueryVariables(prep, rc);
            }

            /*
             * Get the table and check its version against the version from
             * the PreparedStatement
             */
            rc.entry = getTableAndCheckVersions(prep.getTableVersion(),
                                                ns, rc.mappedTableName, rc.lc);
            store = rc.entry.getStore();
        }

        /*
         * if DRL is enabled, and the request specifies to return a throttling
         * exception instead of slowing down due to rate limiting, check
         * here if the table is over its rate limit. If so, return a throttling
         * error.
         */
        if (requestWantsThrottlingErrors(limiterManager, rc)) {
            limiterManager.throwIfOverLimit(rc.origTableName,
                                            (TableImpl)rc.entry.getTable(),
                                            rc.entry.getStoreName(),
                                            (isUpdateOp==false), isUpdateOp);
        }

        RequestLimits limits = rc.entry.getRequestLimits();

        checkRequestSizeLimit(requestSize, false, limits);
        info.maxReadKB = adjustMaxReadKB(info.maxReadKB, isAbsolute, limits);
        info.maxWriteKB = adjustMaxWriteKB(info.maxWriteKB, limits);
        execOpts.setMaxWriteKB(info.maxWriteKB);
        execOpts.setMaxReadKB(info.maxReadKB);
        if (limits != null) {
            execOpts.setMaxPrimaryKeySize(limits.getPrimaryKeySizeLimit());
            execOpts.setMaxRowSize(limits.getRowSizeLimit());
        }
        execOpts.setDoTombstone(doTombStone(rc.entry));

        if (info.traceLevel >= 4) {
            trace("Driver maxReadKB = " + info.maxReadKB + " maxWriteKB = " +
                  info.maxWriteKB, rc.lc);
        }

        /*
         * Set ExecuteOptions.updateLimit for update query to limit the max
         * number of records can be updated in a single query:
         *  - In onprem, set it to the limit set by application.
         *  - In cloud, the max number of records that can be updated in a
         *    single query is limited by maxReadKB and maxWriteKB limits, so
         *    this update limit won't be used, set it to Integer.MAX_VALUE.
         */
        if (queryOp == QueryOperation.UPDATE) {
            int updateLimit = getUpdateLimit(info.limit);
            if (updateLimit > 0) {
                execOpts.setUpdateLimit(updateLimit);
            }
        }

        /* insert/update/upsert not allowed to be parallel */
        if (isUpdateOp &&
            (info.numOperations != 0 || info.operationNumber != 0)) {
            throw new IllegalArgumentException(
                "Cannot perform parallel query on inserts or updates");
        }

        /* FUTURE: use info.durability */

        NsonSerializer ns = null;
        if (rc.isNson()) {
            NettyByteOutputStream nos =
                new NettyByteOutputStream(rc.bbos.getByteBuf());
            ns = new NsonSerializer(nos);
            ns.startMap(0);
        }

        /*
         * if this is an "advanced" query that was not already prepared,
         * just send back a QueryResult with the prepared statement and
         * no actual query results. In this case there is nothing to be
         * done asynchronously.
         */
        if (info.isPrepared == false && prep.isSimpleQuery() == false &&
            info.queryVersion > QUERY_V1) {
            /* return empty result with prepared statement */
            if (rc.isNson()) {
                NsonProtocol.writeMapField(ns, ERROR_CODE, 0);
            } else {
                writeSuccess(rc.bbos);
                rc.bbos.writeInt(0); // numResults
                rc.bbos.writeBoolean(false); // not sort phase 1 result
            }
            finishQuery(info, isAbsolute, prep, cbInfo, prepCost,
                        null, // QueryStatementResultImpl
                        0,    // numResults
                        0,    // sizeOffset
                        store, rc, ns);
            return false; // sync
        }

        int sizeOffset = 0;
        if (ns != null) { // V4
            NsonProtocol.writeMapField(ns, ERROR_CODE, 0);
        } else {
            writeSuccess(rc.bbos);
            /*
             * Leave an integer-sized space in the stream for the number
             * of results. This is an uncompressed int value.
             */
            sizeOffset = rc.bbos.getOffset();
            rc.bbos.writeInt(0);
        }

        /*
         * this method validates the parameters and will throw if invalid.
         * It returns the total number of operations. If > 0 this is a parallel
         * query
         */
        int numberOfOperations =
            getParallelQueryOperations(info, prep, store.getTopology());

        /*
         * Compute synchronous query results. If this is a parallel query
         * the appropriate set of shards or partitions needs to be passed
         */
        Set<RepGroupId> shards = null;
        Set<Integer> partitions = null;

        if (info.shardId > 0) {
            /*
             * this is where the caller is explicitly handling a shard and
             * is never parallel
             */
            shards = new HashSet<>(1);
            shards.add(new RepGroupId(info.shardId));
        } else if (numberOfOperations > 1) {
            execOpts.setIsSimpleQuery(info.isSimpleQuery);
            if (prep.getDistributionKind().equals(
                    PreparedStatementImpl.DistributionKind.ALL_SHARDS)) {
                /* used shard-based split, even if all partition query */
                shards = computeParallelShards(info, store, rc);
            } else {
                /*
                 * there is no current async kv call to handle a set of
                 * partitions, so turn off async for this path.
                 * FUTURE: leave it async if KV supports it. See
                 * doAsyncQuery()
                 */
                partitions =
                    computeParallelPartitions(
                        info, store.getTopology().getNumPartitions());
                doAsync = false;
            }
        }

        if (doAsync) {

            if (info.traceLevel >= 5) {
                trace("Executing async query", rc.lc);
            }

            /* a few finals for lambda function */
            final int sOffset = sizeOffset;
            final NsonSerializer nser = ns;

            Publisher<RecordValue> qpub =
                doAsyncQuery(store, prep, variables, execOpts,
                             shards, partitions,
                             info.traceLevel, rc.lc);

            Subscriber<RecordValue> qsub = new Subscriber<RecordValue>() {

                final static int resultBatchSize = 10;

                Subscription subscription;

                QueryResultIterator queryIterator;

                int numResults;

                @SuppressWarnings("rawtypes")
                @Override
                public void onSubscribe(Subscription s) {

                    subscription = s;
                    queryIterator = ((QuerySubscription)subscription).
                                    getAsyncIterator();

                    try {
                        if (nser != null) { // V4
                            nser.startMapField(QUERY_RESULTS);
                            nser.startArray(0);
                        } else {
                            rc.bbos.writeBoolean(
                                queryIterator.hasSortPhase1Result());
                        }
                    } catch (IOException e) {
                        /* Rethrow as an UIOE. The onError method will be
                         * called with an ISE that wraps this UIOE */
                        throw new UncheckedIOException(
                            "Failed to write response into outut stream", e);
                    }

                    subscription.request(resultBatchSize);
                }

                @Override
                public void onNext(RecordValue result) {
                    /*
                     * In an ideal world there would be an interface
                     * that would accept the raw query result stream before
                     * it's deserialized into RecordValue, allowing direct
                     * re-serialization into the proxy's result stream.
                     *
                     * There is a lot of KV refactoring required for that to
                     * be possible.
                     */

                    try {
                        if (nser != null) { // V4
                            writeFieldValue(nser.getStream(), result);
                            nser.incrSize(1);
                        } else {
                            writeFieldValue(rc.bbos, result);
                        }
                    } catch (IOException e) {
                        /* Rethrow as an UIOE. The onError method will be called
                         * with an ISE that wraps this UIOE */
                        throw new UncheckedIOException(
                            "Failed to write response into outut stream", e);
                    }

                    ++numResults;

                    if (info.traceLevel >= 4) {
                        trace("Wrote result: " + result, rc.lc);
                    }

                    if (numResults % resultBatchSize == 0) {
                        subscription.request(resultBatchSize);
                    }
                }

                @Override
                public void onError(Throwable e) {
                    FullHttpResponse resp = formulateErrorResponse(e, rc);
                    finishOp(rc, resp);
                }

                @Override
                public void onComplete() {

                    QueryStatementResultImpl qres = queryIterator.
                                                    getQueryStatementResult();
                    FullHttpResponse resp = null;
                    try {
                        if (nser != null) {
                            nser.endArray(0);
                            nser.endMapField(QUERY_RESULTS);
                        }
                        finishQuery(info, isAbsolute,
                                    prep, cbInfo, prepCost, qres, numResults,
                                    sOffset, store, rc, nser);
                        if (qres != null) {
                            rc.setThroughput(qres.getReadKB(),
                                             qres.getWriteKB());
                        }
                        resp = defaultHttpResponse(rc);
                    } catch (Exception e) {
                        e.printStackTrace();
                        resp = formulateErrorResponse(e, rc);
                    } finally {
                        finishOp(rc, resp);
                    }
                }
            };

            qpub.subscribe(qsub);

            return true;
        }

        int numResults = 0;

        QueryStatementResultImpl qres =
            doQuery(store, prep, variables, execOpts,
                    shards, partitions,
                    info.traceLevel, rc.lc);

        if (ns == null && info.queryVersion > QUERY_V1) {
            rc.bbos.writeBoolean(qres.hasSortPhase1Result());
        }

        if (ns != null) { // V4
            ns.startMapField(QUERY_RESULTS);
            ns.startArray(0);
        }

        /* note this iterator may execute more remote calls */
        for (RecordValue val : qres) {
            if (ns != null) { // V4
                writeFieldValue(ns.getStream(), val);
                ns.incrSize(1);
            } else {
                writeFieldValue(rc.bbos, val);
            }
            ++numResults;
            if (info.traceLevel >= 4) {
                trace("Wrote result: " + val, rc.lc);
            }
        }

        if (ns != null) {
            ns.endArray(0);
            ns.endMapField(QUERY_RESULTS);
        }

        finishQuery(info, isAbsolute, prep, cbInfo, prepCost,
                    qres, numResults, sizeOffset, store, rc, ns);
        if (qres != null) {
            rc.setThroughput(qres.getReadKB(), qres.getWriteKB());
        }

        return false; // sync
    }

    /**
     * Validate parallel query operation parameters and return total number
     * of operations
     */
    private int getParallelQueryOperations(QueryOpInfo info,
                                           PreparedStatementImpl prep,
                                           Topology topo) {
        if (info.numOperations > 0) {
            if (info.operationNumber <= 0 || info.operationNumber >
                info.numOperations) {
                throw new IllegalArgumentException(
                    "Invalid parallel query parameters");
            }
            /*
             * cannot trust prep.isSimpleQuery() on an already-prepared
             * statement, use the info passed from the driver
             */
            if (!info.isSimpleQuery || prep.getDistributionKind().equals(
                    PreparedStatementImpl.DistributionKind.SINGLE_PARTITION)) {
                /* allow 1 but it's the same as if it were 0, not parallel */
                if (info.numOperations > 1) {
                    throw new IllegalArgumentException(
                        "Invalid number of operations for parallel query");
                }
                /* a single partition query is not parallel */
                return 0;
            }
            if (prep.getDistributionKind().equals(
                    PreparedStatementImpl.DistributionKind.ALL_SHARDS)) {
                if (info.numOperations > topo.getNumRepGroups()) {
                    throw new IllegalArgumentException(
                        "Invalid number of operations for parallel query, " +
                        "it must be less than or equal to " +
                        topo.getNumRepGroups());
                }
            } else if (info.numOperations > topo.getNumPartitions()) {
                throw new IllegalArgumentException(
                    "Invalid number of operations for parallel query, " +
                    "it must be less than or equal to " +
                    topo.getNumPartitions());
            }
            return info.numOperations;
        } else if (info.operationNumber != 0) {
            throw new IllegalArgumentException(
                "Invalid parallel query parameters");
        }
        return 0;
    }

    /*
     * These methods use a combination of the store topology, the total
     * number of parallel operations and the operation number to return sets
     * of items (shards/partitions) in a deterministic manner. The sets must be
     * the same/repeatable for any <topology, number of ops, op number>
     * combination in order to properly partition the data being
     * queried.
     *
     * It has already been verified that the number of operations is <=
     * number of shards or partitions in the topology. The simplest algorithm
     * is to walk the items assigning each to an operation number "bucket"
     * until all of the items have been assigned. If the items aren't evenly
     * divisible by the number of operations some buckets will have additional
     * items.
     *
     * For example if the number of items is 8 and number of operations is 3
     * then bucket 1 gets items 1, 4, 7, bucket 2 gets 2, 5, 8, and
     * bucket 3 gets 3, 6.
     *
     * These assignments are logically static for the duration of a query but
     * rather than round-trip them it's simpler to recalculate, which is not
     * deemed expensive.
     *
     * This calculation does the above. Items are 1-based. Starting at 1 and
     * going to the last item these items go in the target bucket (B)
     * B = bucket number
     * I = item number (start at 1)
     * N = number of operations
     * for (int I = 1; I <= numberOfItems; I++) {
     *     if ((I - B) % N == 0) {
     *       add to bucket B
     *     }
     */
    private Set<RepGroupId> computeParallelShards(QueryOpInfo info,
                                                  KVStoreImpl store,
                                                  RequestContext rc) {

        /*
         * Must use the driver's "base" topology for all queries
         */
        int numShards;
        try {
            numShards =
                store.getDispatcher().getTopologyManager().getTopology(
                    store, rc.driverTopoSeqNum, rc.timeoutMs).getNumRepGroups();
            /*
             * if the driver's notion of topology is different from the current
             * store topology, it means elasticity is happening and all-shard
             * parallel queries are not compatible with elasticity
             */
            if (store.getTopology().getNumRepGroups() != numShards) {
                /*
                 * use of RECOMPILE_QUERY is not very specific but can cause the
                 * caller to "start over" which is the behavior expected, because
                 * trying again will likely succeed
                 */
                throw new RequestException(
                    RECOMPILE_QUERY, "Parallel queries on indexes are not " +
                    "supported during certain points in an elasticity " +
                    "operation. Please retry the entire coordinated operation");
            }
        } catch (TimeoutException te) {
            throw new RequestException(
                REQUEST_TIMEOUT, "Failed to get server state required to " +
                "execute a query");
        }

        Set<RepGroupId> shards = new HashSet<>();
        int numOperations = info.numOperations;
        int bucket = info.operationNumber;
        for (int i = 1; i <= numShards; i++) {
            if ((i - bucket) % numOperations == 0) {
                shards.add(new RepGroupId(i));
            }
        }
        return shards;
    }

    /* see comment above. operation number is 1-based */
    private Set<Integer> computeParallelPartitions(QueryOpInfo info,
                                                   int numPartitions) {
        Set<Integer> partitions = new HashSet<>();
        int numOperations = info.numOperations;
        int bucket = info.operationNumber;
        for (int i = 1; i <= numPartitions; i++) {
            if ((i - bucket) % numOperations == 0) {
                partitions.add(i);
            }
        }
        return partitions;
    }

    private void finishQuery(
        QueryOpInfo qinfo,
        boolean isAbsolute,
        PreparedStatementImpl prep,
        TableUtils.PrepareCB cbInfo,
        int prepCost,
        QueryStatementResultImpl qres,
        int numResults,
        int sizeOffset,
        KVStoreImpl store,
        RequestContext rc,
        NsonSerializer ns)
        throws IOException {

        /* Note: qres is null if this is an advanced query that was not
         * already prepared. */
        byte[] retContdKey = (qres == null ? null : qres.getContinuationKey());

        int readUnits = (qres == null ? 0 : qres.getReadKB());
        int totReadUnits = prepCost + readUnits;
        /* if absolute, divide readUnits by 2 */
        int totReadKB = prepCost + getReadKB(readUnits, isAbsolute);
        int writeKB = (qres == null ? 0 : qres.getWriteKB());

        if (qinfo.traceLevel >= 4) {
            trace("readUnits = " + readUnits + " prepCost = " + prepCost +
                  " totReadKB = " + totReadKB + " writeKB = " + writeKB, rc.lc);
            trace("cont key = " +
                  (retContdKey != null ? "not null" : "null"), rc.lc);
        }

        if (ns != null) {
            NsonProtocol.writeQueryFinish(ns,
                                          this,
                                          qinfo.queryVersion,
                                          qres,
                                          prep,
                                          retContdKey,
                                          totReadUnits, totReadKB, writeKB,
                                          store.getTopology(),
                                          rc.driverTopoSeqNum,
                                          cbInfo,
                                          qinfo.isPrepared,
                                          false, // getQueryPlan
                                          false, // getQuerySchema
                                          qinfo.isSimpleQuery,
                                          qinfo.batchCounter,
                                          qinfo.batchName);
            if (qres != null) {
                qres.close();
            }
            ns.endMap(0);
            markDataOpSucceeded(rc, numResults, readUnits, writeKB);

            return;
        }

        if (qres != null && qres.hasSortPhase1Result()) {
            qres.writeSortPhase1Results(rc.bbos);
        }

        /* write the size at the saved offset */
        rc.bbos.writeIntAtOffset(sizeOffset, numResults);

        writeThroughput(rc.bbos, totReadUnits, totReadKB, writeKB);
        writeByteArray(rc.bbos, retContdKey);

        if (qinfo.queryVersion > QUERY_V1) {

            if (!qinfo.isPrepared) {
                /* Write the proxy-side query plan. */
                serializePreparedQuery(rc.bbos, cbInfo, prep);
                /* Write the driver-side query plan, if any. */
                FieldValueWriterImpl valWriter = new FieldValueWriterImpl();
                prep.serializeForDriver(rc.bbos, qinfo.queryVersion, valWriter);
            } else if (!qinfo.isSimpleQuery) {
                /* check for null qres is here only to eliminate warning. */
                if (qres != null) {
                    if (qinfo.traceLevel >= 4) {
                        trace("readchedLimit = " + qres.reachedLimit(), rc.lc);
                    }
                    rc.bbos.writeBoolean(qres.reachedLimit());
                }
            }

            /* Write topo info, if it is an advanced query */
            if (!qinfo.isSimpleQuery) {
                writeTopologyInfo(rc.bbos, store,
                                  rc.driverTopoSeqNum,
                                  qinfo.traceLevel, rc.lc);
            }
        }

        if (qres != null) {
            qres.close();
        }

        markDataOpSucceeded(rc, numResults, readUnits, writeKB);
    }

    /**
     * Prepare
     *
     * Note that the use of a separate Executor and thread pool
     * to handle async responses from KV requests does not apply to queries
     */
    private boolean handlePrepare(RequestContext rc)
        throws IOException {

        PrepareOpInfo info = new PrepareOpInfo();
        if (rc.isNson()) {
            getV4PrepareOpInfo(info, rc);
        } else {
            getPrepareOpInfo(info, rc);
        }

        PreparedStatementImpl prep;
        TableUtils.PrepareCB cbInfo;
        AccessContextHelper actxHelper;

        /* this method also enforces limit on query string length */
        cbInfo = TableUtils.getCallbackInfo(rc.actx, info.statement, tm);
        cbInfo.checkSupportedDml();

        String namespace = chooseNamespace(rc.actx.getNamespace(),
                                           cbInfo.getNamespace());

        actxHelper = checkAccess(rc.request, namespace, rc.opCode,
                                 cbInfo.getTableName(),
                                 cbInfo.getNotTargetTables(),
                                 this, rc.lc);

        PrepareResult prepRet = doPrepare(rc, actxHelper, cbInfo,
                                          namespace, rc.opCode,
                                          null /* ExecOpts */,
                                          info.statement, info.queryVersion,
                                          rc.lc);
        prep = prepRet.getPreparedStatement();

        final int readUnits = MIN_QUERY_COST;

        if (rc.isNson()) {
            NettyByteOutputStream nos =
                new NettyByteOutputStream(rc.bbos.getByteBuf());
            NsonSerializer ns = new NsonSerializer(nos);
            ns.startMap(0);
            NsonProtocol.writeQueryFinish(ns,
                                          this,
                                          info.queryVersion,
                                          null, // QueryStatementResultImpl
                                          prep,
                                          null, // contKey
                                          readUnits, readUnits, 0,
                                          prepRet.getStore().getTopology(),
                                          rc.driverTopoSeqNum,
                                          cbInfo,
                                          false, // isPrepared
                                          info.getQueryPlan,
                                          info.getQuerySchema,
                                          prep.isSimpleQuery(),
                                          0,/*batchCounter*/
                                          null);
            ns.endMap(0);
            markDataOpSucceeded(rc, 0, readUnits, 0);

            return false; /* synchronous */
        }

        writeSuccess(rc.bbos);
        writeThroughput(rc.bbos,
                        readUnits,
                        0, /* writeKB */
                        false /* absolute */);

        try {
            serializePreparedQuery(rc.bbos, cbInfo, prep);

            /* Write the driver-side query plan and the set of shard ids. */
            if (info.queryVersion > QUERY_V1) {

                if (info.getQueryPlan) {
                    writeString(rc.bbos, prep.toString());
                }

                FieldValueWriterImpl valWriter = new FieldValueWriterImpl();
                prep.serializeForDriver(rc.bbos, info.queryVersion, valWriter);

                if (!prep.isSimpleQuery()) {
                    writeTopologyInfo(rc.bbos, prepRet.getStore(),
                                      -1, // driverTopoSeqNum
                                      0, rc.lc);
                }
            }
        } catch (Exception e) {
            throw new IOException("Exception serializing query: " + e);
        }

        markDataOpSucceeded(rc,
                            0 /* result size */, readUnits /* read size */,
                            0 /* write size */);
        rc.setThroughput(readUnits, 0);

        /* false == synchronous */
        return false;
    }

    private void writeTopologyInfo(ByteOutputStream bbos,
                                   KVStoreImpl store,
                                   int driverTopoSeqNum,
                                   int traceLevel,
                                   LogContext lc)
        throws IOException {

        if (traceLevel >= 4) {
            trace("writeTopologyInfo: driverTopoSeqNum = " + driverTopoSeqNum,
                  lc);
        }

        Topology topo = store.getTopology();
        int proxyTopoSeqNum = topo.getSequenceNumber();

        if (proxyTopoSeqNum == driverTopoSeqNum) {
            if (traceLevel >= 4) {
                trace("writeTopologyInfo: nothing changed", lc);
            }
            writeInt(bbos, -1);
            return;
        }

        writeInt(bbos, proxyTopoSeqNum);

        List<RepGroupId> groupIds = topo.getSortedRepGroupIds();
        int[] shardIds = new int[groupIds.size()];
        int i = 0;
        for (RepGroupId id : groupIds) {
            shardIds[i++] = id.getGroupId();
        }

        if (traceLevel >= 2) {
            StringBuilder sb = new StringBuilder();
            sb.append("[ ");
            for (i = 0; i < shardIds.length; ++i) {
                sb.append(shardIds[i]).append(" ");
            }
            sb.append("]");
            if (traceLevel >= 4) {
                trace("writeTopologyInfo. seqNum =  " + proxyTopoSeqNum +
                      " shard ids = " + sb.toString(), lc);
            }
        }

        SerializationUtil.writePackedIntArray(bbos, shardIds);
    }

    /**
     * Serialize the prepared query. From the protocol view this is an
     * opaque byte array. In order to serialize and deserialize this
     * efficiently the ByteOutputStream is used directly. Because of
     * this the length is a full 4-byte int value so that it can be
     * written after the serialization.
     *
     * Serialized query is:
     *  length -- 4 bytes
     *  salted hash -- 32 bytes
     *  table access info (name, access type)
     *  prepared query itself
     */
    private void serializePreparedQuery(ByteOutputStream bbos,
                                        TableUtils.PrepareCB cbInfo,
                                        PreparedStatementImpl prep)
        throws IOException {

        /*
         * Leave space for the length
         */
        int startOffset = bbos.getOffset();
        bbos.writeInt(0);

        /* Writes the prepared query to bbos */
        int length = serializePreparedQuery(cbInfo, prep, bbos);

        /* Write length at reserved spot */
        bbos.writeIntAtOffset(startOffset, length);
    }

    /**
     * Deserialize the prepared query.
     */
    private PreparedStatementWrapper deserializePreparedQuery(
        ByteInputStream bbis,
        boolean skipHash) throws IOException {

        int totalLength = bbis.readInt();
        if (totalLength <= 0) {
            raiseBadProtocolError(
                "Bad length of prepared statement: " + totalLength, bbis);
        }

        return deserializePreparedQuery(bbis, totalLength, skipHash);
    }

    /**
     * Handle execute a batch of put/delete operations
     */
    protected boolean handleWriteMultiple(RequestContext rc)
        throws IOException {

        WriteMultipleOpInfo info = new WriteMultipleOpInfo();
        if (rc.isNson()) {
            getV4WriteMultipleOpInfo1(info, rc);
        } else {
            getWriteMultipleOpInfo1(info, rc);
        }

        /*
         * info has durability, numOps
         * call part 2 to construct the operations
         */
        info.tableOps = new ArrayList<TableOperation>(info.numOps);
        rc.tableOpInfos = new ArrayList<TableOperationInfo>(info.numOps);

        if (rc.isNson()) {
            getV4WriteMultipleOpInfo2(info, rc);
        } else {
            getWriteMultipleOpInfo2(info, rc);
        }

        /*
         * Verify that all tables given are in the same family
         */
        String parentTableName = null;
        for (TableOperationInfo opInfo : rc.tableOpInfos) {
            String parent = topTableName(opInfo.mappedTableName);
            if (parentTableName == null) {
                parentTableName = parent;
            } else {
               if (! parentTableName.equalsIgnoreCase(parent)) {
                   throw new RequestException(ILLEGAL_ARGUMENT,
                       "Table '" + opInfo.origTableName + "' is not related " +
                       "to table '" + rc.tableOpInfos.get(0).origTableName +
                       "': All sub requests must operate on the same " +
                       "table or descendant tables belonging to the same " +
                       "top level table.");
               }
            }
        }

        /*
         * if DRL is enabled, and the request specifies to return a throttling
         * exception instead of slowing down due to rate limiting, check
         * here if the table is over its rate limit. If so, return a throttling
         * error.
         */

// TODO: is it ok to just use any opInfo for rate limiting purposes?
        TableOperationInfo firstInfo = rc.tableOpInfos.get(0);
        if (requestWantsThrottlingErrors(limiterManager, rc)) {
            limiterManager.throwIfOverLimit(firstInfo.origTableName,
                                            (TableImpl)firstInfo.entry.getTable(),
                                            firstInfo.entry.getStoreName(),
                                            false, true);
        }

        WriteOptions options = createWriteOptions(rc.timeoutMs,
                                                  info.durability,
                                                  rc.actx.getAuthString(),
                                                  false, /* doTombstone */
                                                  rc.lc);

        CompletableFuture<Result> future;
        /*
         * use async unconditionally for now; otherwise need to figure out
         * how to share error handling
         */
        TableAPIImpl tableApi = firstInfo.entry.getTableAPI();
        future = tableApi.executeAsyncInternal(info.tableOps, options);

        future.whenComplete((result, e) -> {
                if (executor != null) {
                    executor.execute(()-> {
                            handleWriteMultipleResponse(result, e, rc,
                                                        tableApi, info);
                        });
                } else {
                    handleWriteMultipleResponse(result, e, rc,
                                                tableApi, info);
                }
            });

        /* true == asynchronous */
        return true;
    }

    private void handleWriteMultipleResponse(final Result result,
                                             Throwable e,
                                             final RequestContext rc,
                                             final TableAPIImpl tableApi,
                                             final WriteMultipleOpInfo info ) {
        FullHttpResponse resp = null;
        try {
            /*
             * If we got a CompletionException, it needs to first be
             * unwrapped. Eventually this should never happen, but for now
             * we keep this in for safety.
             */
            if (e != null && e instanceof CompletionException) {
                /* don't unwrap (let fail) if running in tests */
                if (!inTest()) {
                    e = e.getCause();
                }
            }
            if (e != null && e instanceof TableOpExecutionException) {
                TableOpExecutionException toee =
                    (TableOpExecutionException)e;
                if (rc.isNson()) {
                    NsonProtocol.writeWriteMultipleResponse(
                        rc,
                        this,
                        null, /* result comes from exception */
                        info,
                        tableApi,
                        toee);
                } else {
                    int failedOpIdx = toee.getFailedOperationIndex();
                    TableOperationResult failedOpResult =
                        toee.getFailedOperationResult();

                    /* serialize failed operation info */
                    writeSuccess(rc.bbos);
                    rc.bbos.writeBoolean(false);
                    writeThroughput(rc.bbos,
                                    toee.getReadKB(),
                                    toee.getWriteKB(),
                                    true); // absolute
                    rc.bbos.writeByte(failedOpIdx);
                    TableOperationInfo opInfo =
                        rc.tableOpInfos.get(failedOpIdx);
                    writeTableOperationResult(
                        rc.bbos,
                        rc.serialVersion,
                        info.tableOps.get(failedOpIdx).getType(),
                        opInfo.returnInfo,
                        opInfo.table,
                        failedOpResult,
                        null);
                }
                markOpFailed(rc, 1 /* isServerFailure */);
                resp = defaultHttpResponse(rc);
            } else if (e != null) {
                resp = formulateErrorResponse(e, rc);
            } else {
                if (rc.isNson()) {
                    NsonProtocol.writeWriteMultipleResponse(
                        rc,
                        this,
                        result,
                        info,
                        tableApi,
                        null);
                } else {
                    final List<TableOperationResult> results =
                        tableApi.createResultsFromExecuteResult(
                            result, info.tableOps);
                    writeSuccess(rc.bbos);
                    rc.bbos.writeBoolean(true);
                    writeThroughput(rc.bbos,
                                    result.getReadKB(),
                                    result.getWriteKB(),
                                    true); // absolute
                    writeInt(rc.bbos, results.size());
                    int idx = 0;
                    for (TableOperationResult opResult: results) {
                        TableOperationInfo opInfo =
                            rc.tableOpInfos.get(idx);
                        FieldValue generatedValue =
                            opInfo.genInfo != null ?
                            opInfo.genInfo.getGeneratedValue() : null;
                        writeTableOperationResult(
                            rc.bbos,
                            rc.serialVersion,
                            info.tableOps.get(idx).getType(),
                            opInfo.returnInfo,
                            opInfo.table,
                            opResult,
                            generatedValue);
                        idx++;
                    }
                }
                markDataOpSucceeded(rc,
                                    result.getNumRecords(), result.getReadKB(),
                                    result.getWriteKB());
                rc.setThroughput(result);
                resp = defaultHttpResponse(rc);
            }
        } catch (Exception ue) {
            logger.info("Unexpected exception in multiOp response " +
                        "builder method: " + ue.getMessage(), rc.lc);
            resp = formulateErrorResponse(ue, rc);
        } finally {
            finishOp(rc, resp);
        }
    }

    /**
     * Handle MultiDelete operation
     */
    protected boolean handleMultiDelete(RequestContext rc)
        throws IOException {

        MultiDelOpInfo info = new MultiDelOpInfo();
        if (rc.isNson()) {
            getV4MultiDelOpInfo(info, rc);
        } else {
            getMultiDelOpInfo(info, rc);
        }

        rc.entry = getTableEntry(rc.actx, rc.mappedTableName, rc.lc);
        checkRequestSizeLimit(rc.bbis.available(), false,
                              rc.entry.getRequestLimits());
        Table table = rc.entry.getTable();
        TableAPIImpl tableApi = rc.entry.getTableAPI();

        /*
         * if DRL is enabled, and the request specifies to return a throttling
         * exception instead of slowing down due to rate limiting, check
         * here if the table is over its rate limit. If so, return a throttling
         * error.
         */
        if (requestWantsThrottlingErrors(limiterManager, rc)) {
            limiterManager.throwIfOverLimit(rc.origTableName,
                                            (TableImpl)table,
                                            rc.entry.getStoreName(),
                                            false, true);
        }

        /* PrimaryKey */
        RowSerializer pKey =
            getPrimaryKeySerializer(rc.bbis, table,
                                    rc.entry.getRequestLimits());

        /*
         * Read FieldRange and create MultiRowOptions. This requires the
         * Table to create the FieldRange instance
         */
        MultiRowOptions mro = null;
        if (rc.isNson()) {
            FieldRange range = readFieldRange(rc.bbis, info, table);
            if (range != null) {
                mro = range.createMultiRowOptions();
            }
        } else {
            mro = getMultiRowOptions(rc.bbis, table);
        }

        /* maxWriteKB */
        if (!rc.isNson()) {
            info.maxWriteKB = readMaxWriteKB(rc.bbis);
        }
        info.maxWriteKB =
            adjustMaxWriteKB(info.maxWriteKB, rc.entry.getRequestLimits());

        /* continuationKey */
        if (!rc.isNson()) {
            info.continuationKey = readByteArray(rc.bbis);
        }

        WriteOptions options = createWriteOptions(rc.timeoutMs,
                                                  info.durability,
                                                  rc.actx.getAuthString(),
                                                  doTombStone(rc.entry),
                                                  rc.lc)
                                    .setMaxWriteKB(info.maxWriteKB);

        if (!useAsync()) {
            Result res = tableApi.multiDeleteInternal(pKey,
                                                      info.continuationKey,
                                                      mro, options);
            handleMultiDeleteResponse(res, null, rc);
            /* handleMultiDeleteResponse sends reply, pretend this is async */
            return true;
        } else {
            CompletableFuture<Result> future =
                tableApi.multiDeleteAsyncInternal(pKey, info.continuationKey,
                                                  mro, options);
            future.whenComplete((result, e) -> {
                    if (executor != null) {
                        executor.execute(()-> {
                                handleMultiDeleteResponse(result, e, rc);
                            });
                    } else {
                        handleMultiDeleteResponse(result, e, rc);
                    }
                });
        }

        /* true == asynchronous */
        return true;
    }

    private void handleMultiDeleteResponse(final Result result,
                                           final Throwable e,
                                           final RequestContext rc) {
        FullHttpResponse resp = null;
        TableAPIImpl tableApi = rc.entry.getTableAPI();

        try {
            if (e != null) {
                resp = formulateErrorResponse(e, rc);
            } else {
                if (rc.isNson()) {
                    NsonProtocol.writeMultiDeleteResponse(rc, this, result,
                                                          tableApi);
                } else {
                    writeSuccess(rc.bbos);
                    writeThroughput(rc.bbos,
                                    result.getReadKB(),
                                    result.getWriteKB(),
                                    true); // absolute
                    writeInt(rc.bbos, result.getNDeletions());
                    writeByteArray(rc.bbos, result.getPrimaryResumeKey());
                }

                markDataOpSucceeded(rc,
                                    result.getNumRecords(), result.getReadKB(),
                                    result.getWriteKB());
                rc.setThroughput(result);

                resp = defaultHttpResponse(rc);
            }
        } catch (Exception ue) {
            logger.info("Unexpected exception in multiDelete response " +
                        "builder method: " + ue.getMessage(), rc.lc);
            resp = formulateErrorResponse(ue, rc);
        } finally {
            finishOp(rc, resp);
        }
    }

    /**
     * Handle a get table operation
     */
    protected boolean handleGetTable(RequestContext rc)
        throws IOException {

        GetTableInfo info = new GetTableInfo();

        if (rc.isNson()) {
            getV4GetTableInfo(info, rc);
        } else {
            getGetTableInfo(info, rc.bbis);
        }

        GetTableResponse response = getTable(rc.actx,
                                             rc.mappedTableName,
                                             info.operationId,
                                             tm,
                                             rc.lc);

        if (!response.getSuccess()) {
            throw new RequestException(response.getErrorCode(),
                                       response.getErrorString());
        }

        if (logger.isLoggable(Level.FINE, rc.lc)) {
            logger.fine("GetTable: namespace " + rc.actx.getNamespace() +
                        ", table " + rc.mappedTableName + ", state " +
                        response.getTableInfo().getStateEnum(), rc.lc);
        }

        if (rc.isNson()) {
            String[] tags =
                rc.actx.getExistingTags(response.getTableInfo().getTags());
            NsonProtocol.writeGetTableResponse(rc,
                                               response.getTableInfo(),
                                               tags,
                                               rc.actx.getNamespace(),
                                               tm);
        } else {
            writeSuccess(rc.bbos);
            writeGetTableResponse(rc.bbos, response, rc.serialVersion);
        }

        markTMOpSucceeded(rc, 1 /* TM operation count*/);

        /* false == synchronous */
        return false;
    }

    protected GetTableResponse getTable(AccessContext actx,
                                        String tableName,
                                        String operationId,
                                        TenantManager tm2,
                                        LogContext lc) {
        return TableUtils.getTable(actx,
                                   tableName,
                                   operationId,
                                   tm,
                                   false /* internal */,
                                   lc);
    }

    /**
     * Handle a get table usage operation
     */
    protected boolean handleTableUsage(RequestContext rc)
        throws IOException {

        checkRequestSizeLimit(rc.bbis.available());
        TableUsageOpInfo info = new TableUsageOpInfo();
        if (rc.isNson()) {
            getV4TableUsageOpInfo(info, rc);
        } else {
            getTableUsageOpInfo(info, rc.bbis);
        }
        info.limit = validateTableUsageLimit(info.limit);
        validateUsageTimeRange(info.startRange, info.endRange,
                               "startTime", "endTime");

        TableUsageResponse response =
            TableUtils.getTableUsage(rc.actx,
                                     rc.mappedTableName,
                                     info.startRange,
                                     info.endRange,
                                     0 /* startIndex */,
                                     info.limit,
                                     tm,
                                     rc.lc);

        if (!response.getSuccess()) {
            throw new RequestException(response.getErrorCode(),
                                       response.getErrorString());
        }

        if (rc.isNson()) {
            NsonProtocol.writeTableUsageResponse(rc, response,
                                                 rc.mappedTableName);
        } else {
            writeSuccess(rc.bbos);
            writeGetTableUsageResponse(rc.bbos, response,
                                       rc.actx.getTenantId(),
                                       rc.mappedTableName);
        }

        markTMOpSucceeded(rc, 1 /* TM operation count*/);

        /* false == synchronous */
        return false;
    }

    /**
     * List tables
     */
    @SuppressWarnings("unused")
    private boolean handleListTables(RequestContext rc)
        throws IOException {

        checkRequestSizeLimit(rc.bbis.available());
        ListTablesOpInfo info = new ListTablesOpInfo();
        if (rc.isNson()) {
            getV4ListTablesOpInfo(info, rc);
        } else {
            getListTablesOpInfo(info, rc);
        }

        /* if onprem, and no namespace in request, use default from actx */
        if (isOnPrem() && info.namespace == null) {
            info.namespace = rc.actx.getNamespace();
        }

        if (rc.actx.getNamespace() == null) {
            rc.actx.setNamespace(info.namespace);
            if (isOnPrem() && info.namespace != null) {
                /* for onprem, update log context with namespace info */
                updateLogContext(rc.lc, info.namespace,
                                 null, OpCode.LIST_TABLES);
            }
        }

        /* validate */
        if (info.startIndex < 0) {
            raiseBadProtocolError(
                "Invalid start index, it must be non-negative", null);
        }
        if (info.numTables < 0) {
            raiseBadProtocolError(
                "Invalid numTables, it must be non-negative", null);
        }

        ListTableResponse response =
            TableUtils.listTables(rc.actx,
                                  info.startIndex,
                                  info.numTables,
                                  tm,
                                  rc.lc);
        if (!response.getSuccess()) {
            throw new RequestException(response.getErrorCode(),
                                       response.getErrorString());
        }

        if (rc.isNson()) {
            NsonProtocol.writeListTableResponse(rc, response);
        } else {
            writeSuccess(rc.bbos);
            String[] tables = response.getTables();
            writeInt(rc.bbos, tables.length);
            for (String table : tables) {
                writeString(rc.bbos, table);
            }
            writeInt(rc.bbos, response.getLastIndexReturned());
        }
        markTMOpSucceeded(rc, 1 /* TM operation count*/);

        /* false == synchronous */
        return false;
    }

    /**
     * Get Indexes
     */
    private boolean handleGetIndexes(RequestContext rc)
        throws IOException {

        final String pathToIndexName = PAYLOAD + "." + INDEX;
        checkRequestSizeLimit(rc.bbis.available());

        final String indexName = rc.isNson() ?
            readV4String(rc, pathToIndexName, false) :
            rc.bbis.readBoolean() ?
            readNonNullEmptyString(rc.bbis, "Index name") :
            null;
        IndexResponse response = TableUtils.getIndexInfo(rc.actx,
                                                         rc.mappedTableName,
                                                         indexName,
                                                         tm,
                                                         rc.lc);
        if (!response.getSuccess()) {
            throw new RequestException(response.getErrorCode(),
                                       response.getErrorString());
        }

        if (rc.isNson()) {
            NsonProtocol.writeGetIndexesResponse(rc, response);
        } else {
            writeSuccess(rc.bbos);
            IndexInfo[] indexes = response.getIndexInfo();
            writeInt(rc.bbos, indexes.length);
            for (IndexInfo index : indexes) {
                writeIndexInfo(rc.bbos, index);
            }
        }

        markTMOpSucceeded(rc, 1 /* TM operation count*/);

        /* false == synchronous */
        return false;
    }

    /**
     * Handle a table DDL operation
     * NOTE: errors in the DDL operation, syntax or otherwise, are thrown
     * past this method as RequestException from TableUtils.
     */
    private boolean handleTableOp(RequestContext rc)
        throws IOException {

        checkRequestSizeLimit(rc.bbis.available());
        TableOpInfo opInfo = new TableOpInfo();
        if (rc.isNson()) {
            getV4TableOpInfo(opInfo, rc);
            if (opInfo.freeFormTags != null || opInfo.definedTags != null) {
                rc.actx.setNewTags(
                    JsonProtocol.parseFreeFormTags(opInfo.freeFormTags),
                    JsonProtocol.parseDefinedTags(opInfo.definedTags),
                    null);
            }
        } else {
            getTableOpInfo(opInfo, rc.bbis, rc);
        }
        boolean hasLimits = opInfo.limits != null;
        if (hasLimits) {
            validateTableLimits(opInfo.limits);
        }

        GetTableResponse response = null;
        int errorCode = Protocol.NO_ERROR;

        try {
            if (opInfo.isAlter) {
                rc.opCode = OpCode.ALTER_TABLE;
                if (opInfo.limits != null) {
                    response = handleTableLimits(rc, opInfo.limits,
                                                 opInfo.matchETag);
                } else {
                    response = handleTableTags(rc, opInfo.matchETag);
                }
            } else {
                if (opInfo.statement == null) {
                    throw new IllegalArgumentException(
                        "Table Request must have either statement or " +
                        "limits plus table name");
                }
                logger.fine("TableOperation, statement: " + opInfo.statement +
                            ", " + opInfo.limits, rc.lc);
                PrepareCB callback = TableUtils.getCallbackInfo(
                    rc.actx,
                    opInfo.statement,
                    tm);
                rc.opCode = TableUtils.getOpCode(callback.getOperation());
                rc.origTableName = callback.getTableName();
                rc.mappedTableName = rc.origTableName;

                /* for testing SECURITY_INFO_UNAVAILABLE retries */
                if (inTest() && rc.numRetries == 0 &&
                    authRetriesEnabled == true && rc.opCode == DROP_TABLE &&
                    Boolean.getBoolean("test.simulateSIU")) {
                    throw new RequestException(SECURITY_INFO_UNAVAILABLE,
                              "simulated submitting auth request to IAM");
                }

                /*
                 * For Cloud, use TableUtils directly.
                 * For KV table DDL, need to set AuthContext in the options
                 */
                response = handleTableDdl(rc, opInfo, callback);
            }
        } catch (RequestException re) {
            errorCode = re.getErrorCode();
            throw re;
        } catch (FilterRequestException fre) {
            throw fre;
        } catch (Throwable t) {
            errorCode = UNKNOWN_ERROR;
            if (t instanceof FaultException) {
                /* TODO: TBD: does something better */
            }
            if (t instanceof IllegalArgumentException ||
                t instanceof QueryException) {
                /*
                 * For now, map query/ddl problems to ILLEGAL_ARGUMENT.
                 * At some point a query or ddl-specific error code may
                 * be needed.
                 *
                 * Try to tease out table/index not found exceptions.
                 * May need additional information from query API. E.g.
                 * a table/index not found exception that extends
                 * QueryException. The information is available.
                 */
                errorCode = mapDDLError(t.getMessage());
                if (errorCode == 0) {
                    errorCode = ILLEGAL_ARGUMENT;
                }
            }
            /* this will be logged in caller */
            throw new RequestException(errorCode, t.getMessage());
        } finally {
            TableInfo info = (response == null) ? null :
                response.getTableInfo();
            auditOperation(rc, info, errorCode);
        }

        if (!response.getSuccess()) {
            throw new RequestException(response.getErrorCode(),
                                       response.getErrorString());
        }

        /*
         * Flush the local Table cache. DDL Operations nearly always invalidate
         * the current state
         */
        TableInfo info = response.getTableInfo();
        if (info != null) {
            if (rc.actx.getType() == Type.IAM) {
                String mappedName = rc.actx.getMapTableName(rc.origTableName);
                if (mappedName != null) {
                    tableCache.flushEntry(null, mappedName);
                }
            } else {
                tableCache.flushEntry(info.getTenantId(), rc.origTableName);
            }
        }

        if (rc.isNson()) {
            String[] tags =
                rc.actx.getExistingTags(response.getTableInfo().getTags());
            NsonProtocol.writeGetTableResponse(rc,
                                               response.getTableInfo(),
                                               tags,
                                               rc.actx.getNamespace(),
                                               tm);
        } else {
            writeSuccess(rc.bbos);
            writeGetTableResponse(rc.bbos, response, rc.serialVersion);
        }

        markTMOpSucceeded(rc, 1 /* TM operation count*/);

        /* false == synchronous */
        return false;
    }

    /**
     * Handle a generic DDL (System) operation. This is not supported in the
     * cloud. Operations are those related to security and namespaces.
     */
    protected boolean handleSystemOp(RequestContext rc)
        throws IOException {

        raiseBadProtocolError(
            "Cloud service cannot accept generic system operations", rc.bbis);

        /* false == synchronous */
        return false;
    }

    /**
     * Handle generic system operation status. This is not supported in the
     * cloud. Operations are those related to security and namespaces.
     */
    protected boolean handleSystemStatus(RequestContext rc)
        throws IOException {

        raiseBadProtocolError(
            "Cloud service cannot accept generic system status operations",
            rc.bbis);

        /* false == synchronous */
        return false;
    }

    /*
     * Handle table DDL operation
     */
    protected abstract GetTableResponse handleTableDdl(RequestContext rc,
                                                       TableOpInfo opInfo,
                                                       PrepareCB callback);

    /**
     * Check the number of operations in WriteMultiple
     */
    protected void checkOperationLimit(int numOperations,
                                       RequestLimits limits) {
        if (limits == null) {
            return;
        }

        int limit = limits.getBatchOpNumberLimit();
        if (numOperations > limit) {
            throw new RequestException(
                BATCH_OP_NUMBER_LIMIT_EXCEEDED,
                "The number of operations exceeds the max number of " +
                limit + ": " + numOperations);
        }
    }

    /*
     * Handle table limit operation
     */
    protected abstract GetTableResponse handleTableLimits(RequestContext rc,
                                                          TableLimits limits,
                                                          byte[] matchETag);

    /**
     * Handle table tags operation. Default is no-op (on-prem)
     */
    protected abstract GetTableResponse
        handleTableTags(RequestContext rc, byte[] matchETag);

    protected boolean handleAddReplica(RequestContext rc)
        throws IOException {

        throw new RequestException(OPERATION_NOT_SUPPORTED,
                                   "AddReplica operation is not supported");

    }

    protected boolean handleDropReplica(RequestContext rc)
        throws IOException {

        throw new RequestException(OPERATION_NOT_SUPPORTED,
                                   "DropReplica operation is not supported");
    }

    protected boolean handleInternalDdl(RequestContext rc)
        throws IOException {

        throw new RequestException(OPERATION_NOT_SUPPORTED,
                                   "InternalDDL operation is not supported");

    }

    protected boolean handleInternalStatus(RequestContext rc)
        throws IOException {

        throw new RequestException(OPERATION_NOT_SUPPORTED,
                                   "InternalStatus operation is not supported");
    }

    protected boolean handleGetReplicaStats(RequestContext rc)
        throws IOException {

        throw new RequestException(OPERATION_NOT_SUPPORTED,
                                   "GetReplicaStats operation is not supported");
    }

    protected void writeThroughput(ByteOutputStream out,
                                   int readKB,
                                   int writeKB,
                                   boolean isAbsolute) throws IOException {
        writeConsumedCapacity(out, readKB, writeKB, isAbsolute);
    }

    protected void writeThroughput(ByteOutputStream out,
                                   int readUnits,
                                   int readKB,
                                   int writeKB)  throws IOException {
        writeConsumedCapacity(out, readUnits, readKB, writeKB);
    }

    private OperationType getOpType(OpCode op) {
        if (op == null) {
            return OperationType.UNKNOWN;
        }
        final OperationType type = opCodeMap.get(op);
        if (type == null) {
            return OperationType.UNKNOWN;
        }
        return type;
    }

    /*
     * Internal utilities
     */

    /**
     * There is something in the request itself that is invalid.
     * This may be a purposeful attack, so handle it separately from
     * generic errors. Note that many requests with unrecognized URL
     * patterns will be detected by the caller and never get to this module.
     *
     * Note: the ErrorLimiting (DDoS) logic now handles these types of
     *       errors and manages delaying and/or not responding.
     */
    protected FullHttpResponse invalidRequest(RequestContext rc,
                                              String msg,
                                              int errorCode) {
        if (logger.isLoggable(Level.FINE, rc.lc)) {
            final CharSequence realIp = rc.headers.get(X_REAL_IP_HEADER);
            final CharSequence forwardedFor =
                rc.headers.get(X_FORWARDED_FOR_HEADER);
            final String remoteAddr =
                rc.ctx.channel().remoteAddress().toString();

            StringBuilder sb = new StringBuilder();
            sb.append(msg);
            sb.append(", remote address=").append(remoteAddr);
            if (realIp != null) {
                sb.append(", ")
                .append(X_REAL_IP_HEADER)
                .append("=")
                .append(realIp);
            }
            if (forwardedFor != null) {
                sb.append(", ")
                .append(X_FORWARDED_FOR_HEADER)
                .append("=")
                .append(forwardedFor);
            }

            logger.fine(sb.toString(), rc.lc);
        }

        return createErrorResponse(rc, msg, errorCode);
    }

    /**
     * Creates an error response based on the input parameters
     */
    private FullHttpResponse createErrorResponse(RequestContext rc,
                                                 String msg,
                                                 int errorCode) {
        /* logging, if done, is in caller */
        try {
            /*
             * set error code in RC so it can be used to manage internal
             * retries if needed
             */
            rc.errorCode = errorCode;
            rc.resetBuffers();
            if (rc.isNson()) {
                /*
                 * Is there any way to get consumed capacity?
                 * Retry hints?
                 * TODO: if done they'll probably need to be pushed
                 * into the rc
                 */
                NsonProtocol.writeErrorResponse(rc,
                                                msg,
                                                errorCode);
            } else {
                rc.bbos.writeByte((byte) errorCode);
                writeString(rc.bbos, msg);
            }
        } catch (IOException ioe) {
        }

        return defaultHttpResponse(rc);
    }

    private int mapExceptionToErrorCode(Throwable e) {
        /* TODO -- implement */
        return UNKNOWN_ERROR;
    }

    /**
     * Return true if the op has a table name in the payload. Most do except for
     * queries, DDL operations and administrative operations that cross table
     * boundaries such as list tables
     */
    private static boolean opHasTableName(OpCode op) {
        switch (op) {
        case PREPARE:
        case QUERY:
        case LIST_TABLES:
        case TABLE_REQUEST:
        case SYSTEM_REQUEST:
        case SYSTEM_STATUS_REQUEST:
            return false;
        default:
            return true;
        }
    }

    /**
     * Allow these methods, which might enforce limits, to be overridden.
     */
    protected int adjustMaxWriteKB(int maxWriteKB,
                                   RequestLimits limits) throws IOException {
        if (limits == null) {
            return maxWriteKB;
        }

        int limit = limits.getRequestWriteKBLimit();
        if (maxWriteKB > limit) {
            raiseBadProtocolError("Invalid maxWriteKB, it can not exceed " +
                                  limit + ": " + maxWriteKB, null);
        }
        /* Return the system-defined write size limit if maxWriteKB is 0. */
        return (maxWriteKB != 0) ? maxWriteKB : limit;
    }

    private int validateTableUsageLimit(int limit) throws IOException {
        if (limit < 0) {
            throw new RequestException(ILLEGAL_ARGUMENT,
                "The limit must not be negative value: " + limit);
        }
        if (limit > TABLE_USAGE_NUMBER_LIMIT) {
            raiseBadProtocolError("Invalid limit, it can not exceed " +
                                  TABLE_USAGE_NUMBER_LIMIT + ": " + limit,
                                  null);
        }

        /*
         * Return the number limit of table usage records per request if limit
         * is 0.
         */
        return (limit != 0) ? limit : TABLE_USAGE_NUMBER_LIMIT;
    }

    protected int validateReplicaStatsLimit(int limit) throws IOException {
        if (limit < 0) {
            throw new RequestException(ILLEGAL_ARGUMENT,
                "The limit must not be negative value: " + limit);
        }
        if (limit > REPLICA_STATS_LIMIT) {
            raiseBadProtocolError("Invalid limit, it can not exceed " +
                                  REPLICA_STATS_LIMIT + ": " + limit,
                                  null);
        }
        return (limit != 0) ? limit : REPLICA_STATS_LIMIT;
    }

    /*
     * Checks if the request size exceeds the limit.
     */
    protected void checkRequestSizeLimit(int requestSize,
                                         boolean isWriteMultiple,
                                         RequestLimits limits) {
        if (limits == null) {
            return;
        }
        final int requestSizeLimit = (isWriteMultiple) ?
            limits.getBatchRequestSizeLimit() :
            limits.getRequestSizeLimit();
        if (requestSize > requestSizeLimit) {
            throw new RequestException(REQUEST_SIZE_LIMIT_EXCEEDED,
                "The size of request exceeded the limit of " +
                requestSizeLimit + ": " + requestSize);
        }
    }

    /*
     * Checks if a non-data operation exceeds the request size limit.
     * These are operations like getTable(), listTables(), etc.
     * This uses a somewhat arbitrary static value
     */
    protected void checkRequestSizeLimit(int requestSize) {
        if (requestSize > DDL_REQUEST_SIZE_LIMIT) {
            throw new RequestException(REQUEST_SIZE_LIMIT_EXCEEDED,
                "The size of request exceeded the limit of " +
                DDL_REQUEST_SIZE_LIMIT + ": " + requestSize);
        }
    }

    private static boolean isPutOp(OpCode op) {
        return (op == PUT || op == PUT_IF_ABSENT ||
                op == PUT_IF_PRESENT || op == PUT_IF_VERSION);
    }

    /*
     * Interface for handling operation requests.
     */
    @FunctionalInterface
    private interface ProxyOperation {
        /*
         * return true if handler manages all writing/flushing of
         * response/headers/etc (typically for async operations).
         *
         * return false for synchronous ops that do not manage all
         * headers/response/flushing/etc.
         */
        boolean handle(RequestContext rc)
            throws IOException;
    }

    /**
     * A common method called at the beginning of each operation once it
     * has a table name (assuming it has one). It does these things:
     *  1. access checking (which includes table-based filtering)
     *  2. starts auditing if enabled
     *  3. updates LogControl and logs request start if enabled
     *  4. table name mapping if required (e.g. changing cloud table
     *  format to avoid "." or stripping namespace for on-prem)
     */
    private void doOperationPreamble(RequestContext rc) {
        /* note this will do main authN */
        rc.actx = checkAccess(rc.request, null, rc.opCode,
                              rc.origTableName, this, rc.lc);
        startAudit(rc, rc.origTableName, rc.actx);
        updateLogContext(rc.lc, rc.actx, rc.opCode);
        if (logger.isLoggable(Level.FINE, rc.lc)) {
            logger.fine("handleRequest, op " + opCodeToString(rc.opCode) +
                        ", namespace " + rc.actx.getNamespace() +
                        ", table " + rc.origTableName, rc.lc);
        }
        if (rc.origTableName == null) {
            return;
        }
        rc.mappedTableName = getMappedTableName(rc.origTableName,
                                                rc.actx, rc.opCode);
    }

    protected String getMappedTableName(String origTableName,
                                        AccessContext actx,
                                        OpCode opCode) {
        if (actx == null || origTableName == null) {
            return origTableName;
        }
        if (actx.getNamespace() != null) {
            /*
             * On-prem: strip namespace prefix if needed
             */
            return mapTableName(origTableName);
        } else if (actx.getType() == Type.IAM) {
            return mapTableName(actx, opCode, origTableName);
        } else {
            return origTableName;
        }
    }

    /*
     * From here down are structures and methods to handle protocol and
     * serialization differences
     */

    /* get info */
    private class GetOpInfo {
        Consistency consistency;
    }

    /* delete info */
    private class DelOpInfo {
        Durability durability;
        boolean returnInfo;
        Version matchVersion;
        boolean abortIfUnsuccessful; // only used by WriteMultiple
    }

    /* multi-delete info */
    private class MultiDelOpInfo {
        Durability durability;
        /* offset into the stream to read the field range */
        int fieldRangeOffset;
        int maxWriteKB;
        byte[] continuationKey;
    }

    /* put info TODO: can this extend DelOpInfo? */
    private class PutOpInfo {
        Durability durability;
        boolean returnInfo;
        boolean exactMatch;
        int identityCacheSize;
        boolean updateTTL;
        TimeToLive TTL;
        Version matchVersion;
        boolean abortIfUnsuccessful; // only used by WriteMultiple
    }

    /* public for access from NsonProtocol */
    public class WriteMultipleOpInfo {
        int numOps;
        Durability durability;
        public ArrayList<TableOperation> tableOps;
     }

    private class PrepareOpInfo {
        String statement;
        short queryVersion;
        boolean getQueryPlan;
        boolean getQuerySchema;
    }

    private static class QueryOpInfo {
        String tableName; //???
        Consistency consistency;
        Durability durability;
        int limit;
        int maxReadKB;
        int maxWriteKB;
        int shardId;
        byte[] contKey;
        VirtualScan virtualScan;
        short queryVersion;
        long maxServerMemory;
        MathContext mathContext;
        boolean isSimpleQuery;
        byte traceLevel;
        boolean logFileTracing;
        int batchCounter;
        boolean isPrepared;
        String statement; /* valid if !isPrepared */
        PreparedStatementWrapper psw; /* valid if isPrepared */
        Map<String, FieldValue> bindVars;
        String queryName;
        String batchName;
        int numOperations;
        int operationNumber;
    }

    /*
     * Public for access by subclasses of DataService
     */
    static public class TableOpInfo {
        public String statement;
        public TableLimits limits;
        public boolean isAlter;
        public String definedTags;
        public String freeFormTags;
        public byte[] matchETag;
    }

    private class GetTableInfo {
        String operationId;
    }

    private class ListTablesOpInfo {
        int startIndex;
        int numTables;
        String namespace;
    }

    private class TableUsageOpInfo {
        long startRange;
        long endRange;
        int limit;
        int startIndex;
    }

    public static class AddReplicaOpInfo {
        public String region;
        public int readUnits;
        public int writeUnits;
        public byte[] matchETag;
    }

    public static class DropReplicaOpInfo {
        public String region;
        public byte[] matchETag;
    }

    public static class InternalDdlOpInfo {
        public String operation;
        public String payload;
        public boolean isSystem;
    }

    public static class InternalStatusOpInfo {
        public String resource;
        public String ocid;
        public String name;

        public enum Type {
            WORKREQUEST,
            TABLE
        }

        public Type getResourceType() {
            if (resource != null) {
                return Type.valueOf(resource.toUpperCase());
            }
            return null;
        }
    }

    public static class GetReplicaStatsOpInfo {
        public String replicaName;
        public long startTime;
        public int limit;
    }

    private void getGetOpInfo(GetOpInfo info, ByteInputStream bis)
        throws IOException {

        info.consistency = getConsistency(bis);
    }

    private void getV4GetOpInfo(GetOpInfo info, RequestContext rc)
        throws IOException {

        int keyOffset = 0;
        /* reset the bbis on the payload map */
        rc.resetInputBuffer();
        ByteInputStream bis = rc.bbis;
        boolean found = seekToPayload(bis);
        if (!found) {
            badProtocol("No payload in message");
        }

        MapWalker walker = new MapWalker(bis);
        while (walker.hasNext()) {
            walker.next();
            String name = walker.getCurrentName();
            if (name.equals(CONSISTENCY)) {
                info.consistency = readConsistency(bis);
            } else if (name.equals(KEY)) {
                keyOffset = bis.getOffset();
            } else {
                skipUnknownField(walker, name);
            }
        }
        /* even though key should be last, be explicit about offset */
        bis.setOffset(keyOffset);
    }

    private void getDelOpInfo(DelOpInfo info, RequestContext rc)
        throws IOException {

        info.returnInfo = rc.bbis.readBoolean();
        if (rc.serialVersion > Protocol.V2) {
            info.durability = mapDurability(rc.bbis);
        }
    }

    /*
     * If skipSeek is true don't look for PAYLOAD. The stream is positioned
     * at the payload (for WriteMultiple deserialization)
     *
     * Returns the offset at the end of reading the map
     */
    private int getV4DelOpInfo(DelOpInfo info,
                               RequestContext rc,
                               boolean skipSeek)
        throws IOException {


        if (!skipSeek) {
            rc.resetInputBuffer();
            boolean found = seekToPayload(rc.bbis);
            if (!found) {
                badProtocol("No payload in message");
            }
        }

        ByteInputStream bis = rc.bbis;
        int keyOffset = 0;
        MapWalker walker = new MapWalker(bis);
        while (walker.hasNext()) {
            walker.next();
            String name = walker.getCurrentName();
            if (name.equals(DURABILITY)) {
                info.durability = mapDurability(Nson.readNsonInt(bis));
            } else if (name.equals(KEY)) {
                /* save offset, skip field; it's read by caller */
                keyOffset = bis.getOffset();
                walker.skip();
            } else if (name.equals(RETURN_ROW)) {
                info.returnInfo = Nson.readNsonBoolean(bis);
            } else if (name.equals(ROW_VERSION)) {
                info.matchVersion =
                    Version.fromByteArray(Nson.readNsonBinary(bis));
            } else if (name.equals(ABORT_ON_FAIL)) {
                /* Write Multiple only */
                info.abortIfUnsuccessful = Nson.readNsonBoolean(bis);
            } else {
                skipUnknownField(walker, name);
            }
        }

        int retOff = bis.getOffset();
        /* set the offset to the key when leaving */
        bis.setOffset(keyOffset);
        return retOff;
    }

    private void getMultiDelOpInfo(MultiDelOpInfo info, RequestContext rc)
        throws IOException {

        if (rc.serialVersion > Protocol.V2) {
            info.durability = mapDurability(rc.bbis);
        }
    }

    /*
     * Returns the offset at the end of reading the map, leaving
     * the offset of the input stream pointing at the key
     */
    private int getV4MultiDelOpInfo(MultiDelOpInfo info,
                                    RequestContext rc)
        throws IOException {

        int keyOffset = 0;
        rc.resetInputBuffer();
        ByteInputStream bis = rc.bbis;
        /* set a default range offset, this may be nonzero */
        info.fieldRangeOffset = bis.getOffset();
        boolean found = seekToPayload(bis);
        if (!found) {
            badProtocol("No payload in message");
        }

        MapWalker walker = new MapWalker(bis);
        while (walker.hasNext()) {
            walker.next();
            String name = walker.getCurrentName();
            if (name.equals(DURABILITY)) {
                info.durability = mapDurability(Nson.readNsonInt(bis));
            } else if (name.equals(KEY)) {
                /* save offset, skip field; it's read by caller */
                keyOffset = bis.getOffset();
                walker.skip();
            } else if (name.equals(RANGE)) {
                info.fieldRangeOffset = bis.getOffset();
                walker.skip();
            } else if (name.equals(CONTINUATION_KEY)) {
                info.continuationKey = Nson.readNsonBinary(bis);
            } else if (name.equals(MAX_WRITE_KB)) {
                info.maxWriteKB = Nson.readNsonInt(bis);
            } else {
                skipUnknownField(walker, name);
            }
        }

        int retOff = bis.getOffset();
        /* set the offset to the key when leaving */
        bis.setOffset(keyOffset);
        return retOff;
    }

    /*
     * FieldRange is complex because it requires the Table to construct
     * and then the start/end are optional. The initial walk of the
     * payload has stored the offset into the stream for the range object
     * if present. Scan it here and create the actual range.
     * After this the processing is done with the payload.
     * Format:
     * "range": {
     *   "path": path to field (string)
     *   "start" {
     *      "value": {FieldValue}
     *      "inclusive": bool
     *   }
     *   "end" {
     *      "value": {FieldValue}
     *      "inclusive": bool
     *   }
     *
     */
    private FieldRange readFieldRange(ByteInputStream bis, MultiDelOpInfo info,
                                      Table table)
        throws IOException {

        String path = null;
        int startValueOffset = 0;
        int endValueOffset = 0;
        /* default value of inclusive is false */
        boolean startInclusive = false;
        boolean endInclusive = false;
        bis.setOffset(info.fieldRangeOffset);
        MapWalker walker = new MapWalker(bis);
        while (walker.hasNext()) {
            walker.next();
            String name = walker.getCurrentName();
            if (name.equals(RANGE_PATH)) {
                path = Nson.readNsonString(bis);
            } else if (name.equals(START)) {
                MapWalker rwalker = new MapWalker(bis);
                while (rwalker.hasNext()) {
                    rwalker.next();
                    name = rwalker.getCurrentName();
                    if (name.equals(INCLUSIVE)) {
                        startInclusive = Nson.readNsonBoolean(bis);
                    } else if (name.equals(VALUE)) {
                        startValueOffset = bis.getOffset();
                        rwalker.skip();
                    } else {
                        skipUnknownField(rwalker, name);
                    }
                }
            } else if (name.equals(END)) {
                MapWalker rwalker = new MapWalker(bis);
                while (rwalker.hasNext()) {
                    rwalker.next();
                    name = rwalker.getCurrentName();
                    if (name.equals(INCLUSIVE)) {
                        endInclusive = Nson.readNsonBoolean(bis);
                    } else if (name.equals(VALUE)) {
                        endValueOffset = bis.getOffset();
                        rwalker.skip();
                    } else {
                        skipUnknownField(rwalker, name);
                    }
                }
            } else {
                skipUnknownField(walker, name);
            }
        }
        if (path != null) {
            FieldRange range = table.createFieldRange(path);
            if (startValueOffset != 0) {
                bis.setOffset(startValueOffset);
                range.setStart(readFieldValue(bis, range.getDefinition(),
                                              true, false), startInclusive);
            }
            if (endValueOffset != 0) {
                bis.setOffset(endValueOffset);
                range.setEnd(readFieldValue(bis, range.getDefinition(),
                                            true, false), endInclusive);
            }
            return range;
        }
        return null;
    }

    private void getListTablesOpInfo(ListTablesOpInfo info,
                                     RequestContext rc)
        throws IOException {

        info.startIndex = rc.bbis.readInt();
        info.numTables = rc.bbis.readInt();
        if (rc.serialVersion > V1) {
            info.namespace = readString(rc.bbis);
        }
    }

    private void getV4ListTablesOpInfo(ListTablesOpInfo info,
                                       RequestContext rc)
        throws IOException {

        ByteInputStream bis = rc.bbis;
        int keyOffset = 0;
        boolean found = seekToPayload(bis);
        if (!found) {
            badProtocol("No payload in message");
        }

        MapWalker walker = new MapWalker(bis);
        while (walker.hasNext()) {
            walker.next();
            String name = walker.getCurrentName();
            if (name.equals(LIST_START_INDEX)) {
                info.startIndex = Nson.readNsonInt(bis);
            } else if (name.equals(LIST_MAX_TO_READ)) {
                info.numTables = Nson.readNsonInt(bis);
            } else if (name.equals(NAMESPACE)) {
                info.namespace = Nson.readNsonString(bis);
            } else {
                skipUnknownField(walker, name);
            }
        }
    }

    /**
     * Read an Nson integer and check its range.
     * @param bis the input stream
     * @param min minimum value, inclusive
     * @param max maximum value, inclusive
     * @param fieldName the human-readable field name,
     *                  like "topology sequence number"
     * @return the value if within range
     * @throws IOException if value is out of range
     */
    private static int readNsonInt(ByteInputStream bis, int min, int max,
                                   String fieldName)  throws IOException {
        int val = Nson.readNsonInt(bis);
        if (val < min) {
            throw new IOException("Invalid " + fieldName + " (" +
                                  val + "): must be >= " + min);
        }
        if (val > max) {
            throw new IOException("Invalid " + fieldName + " (" +
                                  val + "): must be <= " + max);
        }
        return val;
    }

    /**
     * Convert a code to MathContext
     */
    public static MathContext codeToMathContext(byte code,
                                                int precision,
                                                int roundingMode)
        throws IOException {

        switch (code) {
        case 0:
            return null;
        case 1:
            return MathContext.DECIMAL32;
        case 2:
            return MathContext.DECIMAL64;
        case 3:
            return MathContext.DECIMAL128;
        case 4:
            return MathContext.UNLIMITED;
        case 5:
            return
                new MathContext(precision, RoundingMode.valueOf(roundingMode));
        default:
            throw new IOException("Unknown MathContext code (" + code + ")");
        }
    }

    private void getV4PrepareOpInfo(PrepareOpInfo info, RequestContext rc)
        throws IOException {

        ByteInputStream bis = rc.bbis;
        /* reset the bbis on the payload map */
        boolean found = seekToPayload(bis);
        if (!found) {
            badProtocol("No payload in message");
        }

        MapWalker walker = new MapWalker(bis);
        while (walker.hasNext()) {
            walker.next();
            String name = walker.getCurrentName();
            if (name.equals(QUERY_VERSION)) {
                info.queryVersion = (short)Nson.readNsonInt(bis);
                if (info.queryVersion < QUERY_V1 ||
                    info.queryVersion > CURRENT_QUERY_VERSION) {
                    throw new RequestException(UNSUPPORTED_QUERY_VERSION,
                        "Unsupported query version " + info.queryVersion);
                }
            } else if (name.equals(STATEMENT)) {
                info.statement = Nson.readNsonString(bis);
            } else if (name.equals(GET_QUERY_PLAN)) {
                info.getQueryPlan = Nson.readNsonBoolean(bis);
            } else if (name.equals(GET_QUERY_SCHEMA)) {
                info.getQuerySchema = Nson.readNsonBoolean(bis);
            } else {
                skipUnknownField(walker, name);
            }
        }
    }

    private void getPrepareOpInfo(PrepareOpInfo info, RequestContext rc)
        throws IOException {
        info.statement = readNonNullEmptyString(rc.bbis, "statement");
        info.queryVersion = QUERY_V1;
        info.getQueryPlan = false;
        info.getQuerySchema = false;
        if (rc.serialVersion > V1) {
            info.queryVersion = rc.bbis.readShort();
            if (info.queryVersion > QUERY_V1) {
                info.getQueryPlan = rc.bbis.readBoolean();
            }
        }
        /*
         * V3 (binary) protocol only supports query versions 1 thru 3
         * Higher query versions are only supported by V4 (nson) protocol
         */
        if (info.queryVersion >= QUERY_V4) {
            throw new RequestException(UNSUPPORTED_QUERY_VERSION,
                "Unsupported query version " + info.queryVersion);
        }
    }

    // TODO: move to NsonProtocol
    private void getV4QueryOpInfo(QueryOpInfo info, RequestContext rc)
        throws IOException {

        ByteInputStream bis = rc.bbis;
        /* reset the bbis on the payload map */
        boolean found = seekToPayload(bis);
        if (!found) {
            badProtocol("No payload in message");
        }

        info.maxWriteKB = 0;
        info.maxReadKB = 0;
        info.mathContext = null;
        info.shardId = -1;
        info.isSimpleQuery = true;
        info.traceLevel = 0;
        info.logFileTracing = true;
        info.batchCounter = 0;
        info.psw = null;
        info.bindVars = null;
        info.virtualScan = null;

        /*
         * per-tenancy limits are not available at this point in a query.
         * If applying per-tenancy limits is desired the adjustment must
         * happen later in the query processing, after the table cache has
         * been read
         */
        info.maxServerMemory = isOnPrem() ?
            ExecuteOptions.MAX_SERVER_MEMORY_CONSUMPTION:
            RequestLimits.defaultLimits().getRequestReadKBLimit();

        /* for math context */
        byte mcCode = 0;
        int mcPrecision = 0;
        int mcRoundingMode = 0;

        MapWalker walker = new MapWalker(bis);
        while (walker.hasNext()) {
            walker.next();
            String name = walker.getCurrentName();
            if (name.equals(CONSISTENCY)) {
                info.consistency = readConsistency(bis);
            } else if (name.equals(NUMBER_LIMIT)) {
                info.limit = Nson.readNsonInt(bis);
            } else if (name.equals(DURABILITY)) {
                info.durability = mapDurability(Nson.readNsonInt(bis));
            } else if (name.equals(MAX_READ_KB)) {
                info.maxReadKB = Nson.readNsonInt(bis);
            } else if (name.equals(IS_PREPARED)) {
                info.isPrepared = Nson.readNsonBoolean(bis);
            } else if (name.equals(QUERY_VERSION)) {
                info.queryVersion = (short)Nson.readNsonInt(bis);
                if (info.queryVersion < QUERY_V1 ||
                    info.queryVersion > CURRENT_QUERY_VERSION) {
                    throw new RequestException(UNSUPPORTED_QUERY_VERSION,
                        "Unsupported query version " + info.queryVersion);
                }
                rc.queryVersion = info.queryVersion;
            } else if (name.equals(CONTINUATION_KEY)) {
                info.contKey = Nson.readNsonBinary(bis);
            } else if (name.equals(STATEMENT)) {
                info.statement = Nson.readNsonString(bis);
            } else if (name.equals(TRACE_LEVEL)) {
                info.traceLevel = (byte)readNsonInt(bis, 0, 127, "trace level");
            } else if (name.equals(TRACE_AT_LOG_FILES)) {
                info.logFileTracing = Nson.readNsonBoolean(bis);
            } else if (name.equals(BATCH_COUNTER)) {
                info.batchCounter = Nson.readNsonInt(bis);
            } else if (name.equals(MAX_WRITE_KB)) {
                info.maxWriteKB = Nson.readNsonInt(bis);
            } else if (name.equals(PREPARED_QUERY)) {
                info.psw = deserializeNsonPreparedQuery(
                               Nson.readNsonBinary(bis));
            } else if (name.equals(BIND_VARIABLES)) {
                getV4BindVariables(info, bis);
            } else if (name.equals(MATH_CONTEXT_CODE)) {
                mcCode = (byte)readNsonInt(bis, 0, 5, "math context code");
            } else if (name.equals(MATH_CONTEXT_PRECISION)) {
                mcPrecision = Nson.readNsonInt(bis);
            } else if (name.equals(MATH_CONTEXT_ROUNDING_MODE)) {
                mcRoundingMode = Nson.readNsonInt(bis);
            } else if (name.equals(SHARD_ID)) {
                info.shardId = readNsonInt(bis, -1, Integer.MAX_VALUE,
                                           "shard id");
            } else if (name.equals(IS_SIMPLE_QUERY)) {
                info.isSimpleQuery = Nson.readNsonBoolean(bis);
            } else if (name.equals(QUERY_NAME)) {
                info.queryName = Nson.readNsonString(bis);
            } else if (name.equals(NUM_QUERY_OPERATIONS)) {
                info.numOperations = Nson.readNsonInt(bis);
            } else if (name.equals(QUERY_OPERATION_NUM)) {
                info.operationNumber = Nson.readNsonInt(bis);
            } else if (name.equals(VIRTUAL_SCAN)) {
                info.virtualScan = readVirtualScan(bis);
            } else if (name.equals(SERVER_MEMORY_CONSUMPTION)) {
                long maxMem = Nson.readNsonLong(bis);
                /* ignore vs throw on attempt to increase server memory */
                if (!(tm.isSecureStore() || maxMem < info.maxServerMemory)) {
                    info.maxServerMemory = maxMem;
                }
            } else {
                skipUnknownField(walker, name);
            }
        }

        info.mathContext = codeToMathContext(mcCode, mcPrecision,
                                             mcRoundingMode);

        if (info.traceLevel > 0) {
            info.batchName =
                (Clock.systemUTC().instant().truncatedTo(ChronoUnit.MILLIS) +
                 " PROXY B-" + info.batchCounter);
        }
    }

    private VirtualScan readVirtualScan(ByteInputStream bis)
        throws IOException {

        int sid = -1;
        int pid = -1;
        byte[] primKey = null;
        byte[] secKey = null;
        boolean moveAfter = true;
        byte[] descResumeKey = null;
        int[] joinPathTables = null;
        byte[] joinPathKey = null;
        byte[] joinPathSecKey = null;
        boolean joinPathMatched = false;
        int currentIndexRange = 0;
        int numTables = 1;
        int currTable = 0;
        TableResumeInfo[] tableRIs = new TableResumeInfo[1];

        MapWalker walker = new MapWalker(bis);

        while (walker.hasNext()) {
            walker.next();
            String name = walker.getCurrentName();
            if (name.equals(VIRTUAL_SCAN_SID)) {
                sid = readNsonInt(bis, 1, Integer.MAX_VALUE, "virtual scan sid");
            } else if (name.equals(VIRTUAL_SCAN_PID)) {
                pid = readNsonInt(bis, 1, Integer.MAX_VALUE, "virtual scan pid");
            } else if (name.equals(VIRTUAL_SCAN_NUM_TABLES)) {
                numTables = readNsonInt(bis, 1, Integer.MAX_VALUE, "num tables");
                tableRIs = new TableResumeInfo[numTables];
            } else if (name.equals(VIRTUAL_SCAN_CURRENT_INDEX_RANGE)) {
                currentIndexRange = readNsonInt(bis, 0, Integer.MAX_VALUE,
                                                "current index range");
            } else if (name.equals(VIRTUAL_SCAN_PRIM_KEY)) {
                primKey = Nson.readNsonBinary(bis);
            } else if (name.equals(VIRTUAL_SCAN_SEC_KEY)) {
                secKey = Nson.readNsonBinary(bis);
            } else if (name.equals(VIRTUAL_SCAN_MOVE_AFTER)) {
                moveAfter = Nson.readNsonBoolean(bis);
            } else if (name.equals(VIRTUAL_SCAN_JOIN_DESC_RESUME_KEY)) {
                descResumeKey = Nson.readNsonBinary(bis);
            } else if (name.equals(VIRTUAL_SCAN_JOIN_PATH_TABLES)) {
                joinPathTables = Nson.readIntArray(bis);
            } else if (name.equals(VIRTUAL_SCAN_JOIN_PATH_KEY)) {
                joinPathKey = Nson.readNsonBinary(bis);
            } else if (name.equals(VIRTUAL_SCAN_JOIN_PATH_SEC_KEY)) {
                joinPathSecKey = Nson.readNsonBinary(bis);
            } else if (name.equals(VIRTUAL_SCAN_JOIN_PATH_MATCHED)) {
                joinPathMatched = Nson.readNsonBoolean(bis);
                tableRIs[currTable] = new TableResumeInfo(currentIndexRange,
                                                          primKey, secKey,
                                                          moveAfter,
                                                          descResumeKey,
                                                          joinPathTables,
                                                          joinPathKey,
                                                          joinPathSecKey,
                                                          joinPathMatched);
                ++currTable;
            } else {
                raiseBadProtocolError("Unknown virtual scan field: " + name,
                                      bis);
            }
        }

        return new VirtualScan(pid, sid, tableRIs);
    }

    /* TODO: avoid new ByteInputStream here */
    /* TODO: full nson-ized PreparedStatementWrapper */
    private PreparedStatementWrapper deserializeNsonPreparedQuery(
        byte[] bytes) throws IOException {
        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        ByteInputStream bis = new ByteInputStream(buf);
        try {
            return deserializePreparedQuery(bis, bytes.length, true);
        } catch (Exception e) {
            /* this might have been prepared by v2/3 */
            bis.setOffset(0);
            return deserializePreparedQuery(bis, bytes.length, false);
        }
        /* no need to release wrapped buffer */
    }

    private void getQueryOpInfo(QueryOpInfo info, RequestContext rc)
        throws IOException {

        info.consistency = getConsistency(rc.bbis);
        info.limit = readNumberLimit(rc.bbis);
        info.maxReadKB = readMaxReadKB(rc.bbis);
        info.contKey = getContinuationKey(rc.bbis);
        info.isPrepared = rc.bbis.readBoolean();

        info.maxWriteKB = 0;
        info.mathContext = null;
        info.shardId = -1;
        info.isSimpleQuery = true;
        info.traceLevel = 0;
        info.logFileTracing = true;
        info.batchCounter = 0;
        info.virtualScan = null;
        info.maxServerMemory = 0;

        if (rc.serialVersion > V1) {
            info.queryVersion = rc.bbis.readShort();

            if (info.queryVersion > QUERY_V1) {
                info.traceLevel = rc.bbis.readByte();
                info.maxWriteKB = readMaxWriteKB(rc.bbis);
                info.mathContext = SerializationUtil.readMathContext(rc.bbis);
                rc.driverTopoSeqNum = readTopologySeqNum(rc.bbis);
                info.shardId = readShardId(rc.bbis);
                info.isSimpleQuery = rc.bbis.readBoolean();
            }
        } else {
            info.queryVersion = QUERY_V1;
        }

        /*
         * V3 (binary) protocol only supports query versions 1 thru 3
         * Higher query versions are only supported by V4 (nson) protocol
         */
        if (info.queryVersion >= QUERY_V4) {
            throw new RequestException(UNSUPPORTED_QUERY_VERSION,
                "Unsupported query version " + info.queryVersion);
        }

        if (info.isPrepared == false) {
            info.statement = readNonNullEmptyString(rc.bbis, "statement");
        } else {
            int offset = rc.bbis.getOffset();
            try {
                info.psw = deserializePreparedQuery(rc.bbis, false);
            } catch (Exception e) {
                /* this may have been prepared by v4 (nson) protocol */
                rc.bbis.setOffset(offset);
                info.psw = deserializePreparedQuery(rc.bbis, true);
            }
        }
    }

    private void getV4BindVariables(QueryOpInfo info, ByteInputStream bis)
        throws IOException {
        int t = bis.readByte();
        if (t != Nson.TYPE_ARRAY) {
            throw new IllegalArgumentException("Bad type in bind " +
                        "variables: " + Nson.typeString(t) +
                        ", should be ARRAY");
        }
        int totalLength = bis.readInt(); /* length of array in bytes */
        int numElements = bis.readInt(); /* number of array elements */
        if (numElements == 0) {
            return;
        }
        if (info.psw == null || info.psw.statement == null) {
            throw new IllegalArgumentException("Bind variables supplied with " +
                                               "no prepared statement");
        }
        info.bindVars = new HashMap<String, FieldValue>(numElements);

        for (int i = 0; i < numElements; i++) {
            String fieldName = null;
            MapWalker walker = new MapWalker(bis);
            while (walker.hasNext()) {
                walker.next();
                String name = walker.getCurrentName();
                if (name.equals(NAME)) {
                    fieldName = Nson.readNsonString(bis);
                    if (fieldName.charAt(0) == '#') {
                        String posStr = fieldName.substring(1);
                        int pos = Integer.parseInt(posStr);
                        fieldName = info.psw.statement.varPosToName(pos);
                    }
                } else if (name.equals(VALUE)) {
                    /* fieldName must be before fieldValue */
                    if (fieldName == null) {
                        throw new RequestException(
                            ILLEGAL_ARGUMENT,
                            "Invalid order of fieldname and fieldvalue");
                    }
                    FieldDef def = info.psw.statement.getExternalVarType(fieldName);
                    if (def == null) {
                        throw new RequestException(
                            ILLEGAL_ARGUMENT,
                            "The query doesn't contain the variable: " + fieldName);
                    }
                    try {
                        FieldValue value = readFieldValue(bis, def, true, false);
                        info.bindVars.put(fieldName, value);
                    } catch (IllegalArgumentException iae) {
                        throw new RequestException(
                            ILLEGAL_ARGUMENT,
                            "Failed to create Field Value for variable '" +
                            fieldName + "': " + iae.getMessage());
                    }
                } else {
                    skipUnknownField(walker, name);
                }
            }
        }
    }

    private HashMap<String, FieldValue> getQueryVariables(
                                            PreparedStatementImpl prep,
                                            RequestContext rc)
        throws IOException {
        int numVars = readNumVariables(rc.bbis);

        if (numVars <= 0) {
            return null;
        }
        return readBindVariables(prep, rc.bbis, numVars);
    }

    private HashMap<String, FieldValue> readBindVariables(
                                            PreparedStatementImpl prep,
                                            ByteInputStream bis,
                                            int numVars)
        throws IOException {
        HashMap<String, FieldValue> variables =
            new HashMap<String, FieldValue>(numVars);

        for (int i = 0; i < numVars; i++) {
            String name = readNonNullEmptyString(bis, "Variable name");

            if (name.charAt(0) == '#') {
                String posStr = name.substring(1);
                int pos = Integer.parseInt(posStr);
                name = prep.varPosToName(pos);
            }

            FieldDef def = prep.getExternalVarType(name);
            if (def == null) {
                throw new RequestException(
                    ILLEGAL_ARGUMENT,
                    "The query doesn't contain the variable: " + name);
            }

            FieldValue value = null;
            try {
                value = readFieldValue(bis, def, true, false);
            } catch (IllegalArgumentException iae) {
                throw new RequestException(
                    ILLEGAL_ARGUMENT,
                    "Failed to create Field Value for variable '" +
                    name + "': " + iae.getMessage());
            }
            variables.put(name, value);
        }
        return variables;
    }

    private void getPutOpInfo(PutOpInfo info, RequestContext rc)
        throws IOException {

        info.returnInfo = rc.bbis.readBoolean();
        if (rc.serialVersion > Protocol.V2) {
            info.durability = mapDurability(rc.bbis);
        }
        if (rc.serialVersion > V1) {
            info.exactMatch = rc.bbis.readBoolean();
            info.identityCacheSize = readInt(rc.bbis);
        }
    }

    /*
     * If skipSeek is true don't look for PAYLOAD. The stream is positioned
     * at the payload (for WriteMultiple deserialization)
     *
     * Returns the offset at the end of reading the map
     */
    private int getV4PutOpInfo(PutOpInfo info,
                               RequestContext rc,
                               boolean skipSeek)
        throws IOException {

        int valueOffset = 0;
        if (!skipSeek) {
            rc.resetInputBuffer();
            boolean found = seekToPayload(rc.bbis);
            if (!found) {
                badProtocol("No payload in message");
            }
        }

        ByteInputStream bis = rc.bbis;
        MapWalker walker = new MapWalker(bis);
        while (walker.hasNext()) {
            walker.next();
            String name = walker.getCurrentName();
            if (name.equals(DURABILITY)) {
                info.durability = mapDurability(Nson.readNsonInt(bis));
            } else if (name.equals(VALUE)) {
                valueOffset = bis.getOffset();
                walker.skip();
            } else if (name.equals(RETURN_ROW)) {
                info.returnInfo = Nson.readNsonBoolean(bis);
            } else if (name.equals(EXACT_MATCH)) {
                info.exactMatch = Nson.readNsonBoolean(bis);
            } else if (name.equals(UPDATE_TTL)) {
                info.updateTTL = Nson.readNsonBoolean(bis);
            } else if (name.equals(TTL)) {
                info.TTL = parseTTL(Nson.readNsonString(bis));
            } else if (name.equals(IDENTITY_CACHE_SIZE)) {
                info.identityCacheSize = Nson.readNsonInt(bis);
            } else if (name.equals(ROW_VERSION)) {
                info.matchVersion =
                    Version.fromByteArray(Nson.readNsonBinary(bis));
            } else if (name.equals(ABORT_ON_FAIL)) {
                /* Write Multiple only */
                info.abortIfUnsuccessful = Nson.readNsonBoolean(bis);
            } else {
                skipUnknownField(walker, name);
            }
        }
        int retOff = bis.getOffset();
        /* set the offset to the value when leaving */
        bis.setOffset(valueOffset);
        return retOff;
    }

    private void getTableOpInfo(TableOpInfo info, ByteInputStream bis,
                                RequestContext rc) throws IOException {
        info.statement = readString(bis);
        boolean hasLimits = bis.readBoolean();
        if (hasLimits) {
            TableLimits limits = readTableLimits(bis, rc.serialVersion);
            validateTableLimits(limits);
            info.limits = limits;
            if (bis.readBoolean()) {
                info.isAlter = true;
                rc.origTableName = readNonNullEmptyString(bis, "Table name");
                rc.mappedTableName = rc.origTableName;
            }
        }
    }

    private void getV4TableOpInfo(TableOpInfo info, RequestContext rc)
        throws IOException {

        /* reset the bbis on the payload map */
        rc.resetInputBuffer();
        ByteInputStream bis = rc.bbis;
        boolean found = seekToPayload(bis);
        if (!found) {
            badProtocol("No payload in message");
        }

        boolean hasTags = false;
        MapWalker walker = new MapWalker(bis);
        while (walker.hasNext()) {
            walker.next();
            String name = walker.getCurrentName();
            if (name.equals(STATEMENT)) {
                info.statement = Nson.readNsonString(bis);
            } else if (name.equals(LIMITS)) {
                int mode = -1;
                int readKB = -1;
                int writeKB = -1;
                int storageGB = -1;
                MapWalker lwalker = new MapWalker(bis);
                while (lwalker.hasNext()) {
                    lwalker.next();
                    name = lwalker.getCurrentName();
                    if (name.equals(READ_UNITS)) {
                        readKB = Nson.readNsonInt(bis);
                    } else if (name.equals(WRITE_UNITS)) {
                        writeKB = Nson.readNsonInt(bis);
                    } else if (name.equals(STORAGE_GB)) {
                        storageGB = Nson.readNsonInt(bis);
                    } else if (name.equals(LIMITS_MODE)) {
                        mode = Nson.readNsonInt(bis);
                    } else {
                        skipUnknownField(lwalker, name);
                    }
                }
                switch (mode) {
                case PROVISIONED:
                    info.limits =
                        new TableLimits(readKB, writeKB, storageGB);
                    break;
                case ON_DEMAND:
                    info.limits =
                        new TableLimits(storageGB);
                    break;
                default:
                    raiseBadProtocolError(("Unknown TableLimits mode: " + mode),
                                          bis);
                }
                validateTableLimits(info.limits);
            } else if (name.equals(DEFINED_TAGS)) {
                info.definedTags = Nson.readNsonString(bis);
                hasTags = true;
            } else if (name.equals(FREE_FORM_TAGS)) {
                info.freeFormTags = Nson.readNsonString(bis);
                hasTags = true;
            } else if (name.equals(ETAG)) {
                /* the protocol uses a Base64-encoded string vs binary */
                String etag = Nson.readNsonString(bis);
                info.matchETag =
                    oracle.nosql.common.json.JsonUtils.decodeBase64(etag);
            } else {
                skipUnknownField(walker, name);
            }
            if (info.statement == null) {
                info.isAlter = (info.limits != null || hasTags);
                if (info.limits != null && hasTags) {
                    throw new IllegalArgumentException(
                        "A single alter table operation cannot specify " +
                        "both table limits and tags");
                }
            }
        }
    }

    private void getGetTableInfo(GetTableInfo info, ByteInputStream bis)
        throws IOException {
        info.operationId = readString(bis);
    }

    private void getV4GetTableInfo(GetTableInfo info, RequestContext rc)
        throws IOException {

        /* reset the bbis on the payload map */
        rc.resetInputBuffer();
        ByteInputStream bis = rc.bbis;
        boolean found = seekToPayload(bis);
        if (!found) {
            badProtocol("No payload in message");
        }

        MapWalker walker = new MapWalker(bis);
        while (walker.hasNext()) {
            walker.next();
            String name = walker.getCurrentName();
            if (name.equals(OPERATION_ID)) {
                info.operationId = Nson.readNsonString(bis);
            } else {
                skipUnknownField(walker, name);
            }
        }
    }

    /*
     * Write multiple info is obtained in 2 parts because part 2 requires
     * the Table instance.
     *  1. generic info -- table name, durability, num ops
     *  <get Table>
     *  2. operations themselves
     */
    /*
     * part 1
     */
    private void getWriteMultipleOpInfo1(WriteMultipleOpInfo info,
                                         RequestContext rc)
        throws IOException {

        final ByteInputStream bbis = rc.bbis;
        info.numOps = readInt(bbis);
        if (info.numOps <= 0) {
            raiseBadProtocolError(
                "The number of operations must be a positive value", bbis);
        }

        if (rc.serialVersion > Protocol.V2) {
            info.durability = mapDurability(bbis);
        }
    }

    /*
     * Part 2 - read the operations
     */
    private void getWriteMultipleOpInfo2(WriteMultipleOpInfo info,
                                         RequestContext rc)
        throws IOException {

        /*
         * V3 compatibility:
         * If multiple (parent/child) tables are given,
         * rc.V3TableNames will have the full comma-separated
         * list of table names, one per operation.
         */
        final String[] tnames;
        if (rc.V3TableNames != null) {
            tnames = rc.V3TableNames.split(",");
            if (tnames.length != info.numOps) {
                raiseBadProtocolError("Invalid table names, the number of " +
                    "names should be one or same as operations number " +
                    info.numOps + ", but got " + tnames.length + " names",
                    rc.bbis);
            }
        } else {
            tnames = null;
        }


        /* loop on operations */
        for (int i = 0; i < info.numOps; i++) {

            final String tableName;
            if (tnames == null) {
                tableName = rc.origTableName;
            } else {
                tableName = tnames[i];
            }
            boolean exact = false;
            int startOffset = rc.bbis.getOffset();
            boolean abortIfUnsuccessful = rc.bbis.readBoolean();
            /* opCode */
            OpCode op = OpCode.getOP(readOpCode(rc.bbis));

            TableOperationInfo opInfo = new TableOperationInfo();
            setupTableOpInfo(rc, opInfo, op, tableName);

            RequestLimits limits = opInfo.entry.getRequestLimits();

            checkOperationLimit(info.numOps, limits);

            int keySizeLimit = (limits == null ? -1 :
                                limits.getPrimaryKeySizeLimit());
            int valueSizeLimit = (limits == null ? -1 :
                                  limits.getRowSizeLimit());


            /* returnChoice */
            boolean returnInfo = rc.bbis.readBoolean() &&
                (op != OpCode.PUT && op != OpCode.PUT_IF_PRESENT &&
                 op != OpCode.DELETE);
            ReturnRow.Choice returnChoice =
                (returnInfo ? ReturnRow.Choice.ALL : ReturnRow.Choice.NONE);
            opInfo.returnInfo = returnInfo;
            RowSerializer row = null;
            boolean updateTTL = false;

            if (isPutOp(op)) {
                if (rc.serialVersion > V1) {
                    exact = rc.bbis.readBoolean();
                    int cacheSize = readInt(rc.bbis);
                    if (opInfo.genInfo != null) {
                        opInfo.genInfo.setCacheSize(cacheSize);
                    }
                }

                row = readRowSerializer(rc.bbis, opInfo.table, keySizeLimit,
                                        valueSizeLimit, exact);
                updateTTL = rc.bbis.readBoolean();
                TimeToLive ttl = readTTL(rc.bbis);
                if (ttl != null) {
                    ((RowSerializerImpl)row).setTTL(ttl);
                }
            } else {
                /* Primary key */
                row = readPrimaryKeySerializer(rc.bbis, opInfo.table,
                                               keySizeLimit);
            }

            /* Read matched version for putIfVersion and deleteIfVersion op */
            Version matchVersion =
                (op == PUT_IF_VERSION || op == DELETE_IF_VERSION) ?
                readVersion(rc.bbis) : null;

            /*
             * Saves the current offset after reading all information for the
             * current operation from InputStream. The offset of InputStream
             * will be set back to this position after creating operation.
             */
            int savedOffset = rc.bbis.getOffset();

            /* Check sub request size limit */
            checkRequestSizeLimit(savedOffset-startOffset, false, limits);

            final TableAPIImpl tableApi = opInfo.entry.getTableAPI();
            final KVStoreImpl store = tableApi.getStore();
            final TableImpl table = (TableImpl)row.getTable();
            final OpFactory factory =
                (OpFactory) tableApi.getTableOperationFactory();
            final boolean doTombstone = doTombStone(opInfo.entry);

            TableOperation tableOp;
            switch(op) {
            case DELETE:
                tableOp = factory.createDeleteInternal(
                                      row, returnChoice,
                                      doTombstone,
                                      abortIfUnsuccessful);
                break;
            case DELETE_IF_VERSION: {
                tableOp = factory.createDeleteIfVersionInternal(
                                      row, matchVersion,
                                      returnChoice,
                                      doTombstone,
                                      abortIfUnsuccessful);
                break;
            }
            case PUT:
            case PUT_IF_ABSENT:
            case PUT_IF_PRESENT:
            case PUT_IF_VERSION: {
                Key kvKey =
                    table.createKeyInternal(row, false, store, opInfo.genInfo);
                Value kvValue =
                    table.createValueInternal(row, store, opInfo.genInfo);
                PrimaryKeyImpl pkey =
                    table.createPrimaryKeyFromKeyBytes(kvKey.toByteArray());
                tableOp = factory.createPutInternal(
                    mapPutOpCode(op),
                    kvKey,
                    kvValue,
                    pkey,
                    table,
                    row.getTTL(),
                    returnChoice,
                    abortIfUnsuccessful,
                    matchVersion);

                tableOp.setUpdateTTL(updateTTL);
                break;
            }
            default:
                throw new IllegalStateException("Unknown operation for " +
                                                "WriteMultipleRequest: " + op);
            }

            /*
             * Set the offset of InputStream to the reserved offset which
             * points to the position after reading the current operation.
             */
            rc.bbis.setOffset(savedOffset);
            info.tableOps.add(tableOp);
            rc.tableOpInfos.add(opInfo);
        }
    }

    private void setupTableOpInfo(RequestContext rc,
                                  TableOperationInfo opInfo,
                                  OpCode op,
                                  String tableName)
        throws IOException {

        /* currently disallow anything but PUTs and DELETEs */
        opInfo.mappedOp = mapWriteOp(op, rc, true);

        /* scan for previous opinfo with same tablename and op */
        TableOperationInfo existingOpInfo =
            findTableOpInfo(rc, tableName, opInfo.mappedOp);
        if (existingOpInfo != null) {
            opInfo.copyFrom(existingOpInfo);
            return;
        }

        opInfo.actx = checkAccess(rc.request, null, opInfo.mappedOp,
                                  tableName, this, rc.lc);
        opInfo.origTableName = tableName;
        opInfo.mappedTableName = getMappedTableName(tableName, opInfo.actx, op);

        opInfo.entry = getTableEntry(opInfo.actx,
                                     opInfo.mappedTableName, rc.lc);
        opInfo.table = (TableImpl) opInfo.entry.getTable();
        if (opInfo.table.hasIdentityColumn() ||
            opInfo.table.hasUUIDcolumn()) {
            opInfo.genInfo = new GeneratedValueInfo();
        }
    }

    /* return PUT or DELETE, original op, or throw an error */
    private static OpCode mapWriteOp(OpCode op,
                                     RequestContext rc,
                                     boolean throwOnError)
    throws IOException {
        switch(op) {
            case PUT:
            case PUT_IF_ABSENT:
            case PUT_IF_PRESENT:
            case PUT_IF_VERSION:
                return OpCode.PUT;
            case DELETE:
            case DELETE_IF_VERSION:
                return OpCode.DELETE;
        }
        if (throwOnError) {
            raiseBadProtocolError(
                "Invalid operation for WriteMultipleRequest: " + op,
                rc.bbis);
        }
        return op;
    }

    private TableOperationInfo findTableOpInfo(RequestContext rc,
                                               String tableName,
                                               OpCode mappedOp) {
        if (rc.tableOpInfos == null) {
            return null;
        }
        for (TableOperationInfo opInfo : rc.tableOpInfos) {
            if (opInfo.mappedOp == mappedOp &&
                (opInfo.origTableName.equalsIgnoreCase(tableName) ||
                 opInfo.mappedTableName.equalsIgnoreCase(tableName))) {
                return opInfo;
            }
        }
        return null;
    }

    /*
     * See comment above about part1 vs part2.
     */
    private void getV4WriteMultipleOpInfo1(WriteMultipleOpInfo info,
                                           RequestContext rc)
        throws IOException {

        ByteInputStream bis = rc.bbis;

        /* reset the bbis on the payload map */
        boolean found = seekToPayload(bis);
        if (!found) {
            badProtocol("No payload in message");
        }

        MapWalker walker = new MapWalker(bis);
        while (walker.hasNext()) {
            walker.next();
            String name = walker.getCurrentName();
            if (name.equals(DURABILITY)) {
                info.durability = mapDurability(Nson.readNsonInt(bis));
            } else if (name.equals(NUM_OPERATIONS)) {
                info.numOps = Nson.readNsonInt(bis);
            } else {
                skipUnknownField(walker, name);
            }
        }
    }

    /*
     * Part 2 - read the operations
     */
    private void getV4WriteMultipleOpInfo2(WriteMultipleOpInfo info,
                                           RequestContext rc)
        throws IOException {

        final String path = PAYLOAD + "." + OPERATIONS;

        /* reset the bbis on the payload map */
        rc.resetInputBuffer();
        ByteInputStream bis = rc.bbis;
        boolean found = PathFinder.seek(path, bis);
        if (!found) {
            badProtocol("No operations found in writemultiple at path " + path);
        }

        /*
         * OPERATIONS is an array
         *
         * There is no way to walk an NSON array using something like a
         * MapWalker. The array format is:
         *  length of entire array in bytes
         *  number of array elements
         *  the elements, which in this case are maps where each map has
         * the operation information
         */
        int t = bis.readByte();
        if (t != Nson.TYPE_ARRAY) {
            badProtocol("Operations: bad type in writemultiple: " +
                        Nson.typeString(t) + ", should be ARRAY");
        }
        bis.readInt(); /* length of entire array in bytes */
        int numElements = bis.readInt(); /* number of array elements */
        if (numElements != info.numOps) {
            badProtocol("WriteMultiple: number of operation elements (" +
                        numElements + ") does not equal expected number " +
                        "of operations (" + info.numOps + ")");
        }
        for (int i = 0; i < numElements; i++) {
            createV4TableOperation(info, rc);
        }
    }

    /**
     * create a table operation from an NSON operation. Each operation
     * is a map in the array of operations and may have these fields:
     * op code (variant of put and delete)
     * abortIfUnsuccessful
     * return_row (true/false)
     * operation-specific fields (put vs delete)
     *
     * If using multiple table names, TABLENAME must be first in the map.
     * The op code MUST be next in the map, given the current
     * algorithm.
     */
    private void createV4TableOperation(WriteMultipleOpInfo info,
                                        RequestContext rc)
        throws IOException {

        /* save offset to reset for the operation walker */
        ByteInputStream bis = rc.bbis;
        int mapOffset = bis.getOffset();
        int op; /* use numeric value to check range */
        String tableName = null;
        MapWalker walker = new MapWalker(bis);
        while (walker.hasNext()) {
            walker.next();
            String name = walker.getCurrentName();
            /* only allow single table name in header OR names in each op */
            if (name.equals(TABLE_NAME)) {
                tableName = Nson.readNsonString(bis);
                if (rc.origTableName != null &&
                    ! rc.origTableName.equalsIgnoreCase(tableName)) {
                    badProtocol("Table name must be in header OR " +
                                "in each operation");
                }
            } else if (name.equals(OP_CODE)) {
                if (tableName == null) {
                    tableName = rc.origTableName;
                }
                if (tableName == null) {
                    badProtocol("Table name is required before op code");
                }
                op = Nson.readNsonInt(bis);
                OpCode opCode = OpCode.getOP(op);
                /*
                 * resetting the offset allows the create* calls to
                 * use MapWalker
                 */
                bis.setOffset(mapOffset);
                TableOperationInfo opInfo = new TableOperationInfo();
                setupTableOpInfo(rc, opInfo, opCode, tableName);

                RequestLimits limits = opInfo.entry.getRequestLimits();
                checkOperationLimit(info.numOps, limits);

                if (Protocol.OpCode.isPut(op)) {
                    createPutTableOp(opCode, info, rc, opInfo);
                } else if (Protocol.OpCode.isDel(op)) {
                    createDelTableOp(opCode, info, rc, opInfo);
                } else {
                    badProtocol("Operations: bad op type: " +
                                Nson.typeString(op));
                }
                /*
                 * the create* operations have entirely consumed this map
                 * and the stream offset should be at the next array
                 * element (or the end of the array) so return
                 */
                return;
            } else {
                throw new IllegalArgumentException(
                    "The op field was not first in the operation object, was " +
                                                   name);
            }
        }
    }

    private void createPutTableOp(OpCode opCode,
                                  WriteMultipleOpInfo info,
                                  RequestContext rc,
                                  TableOperationInfo opInfo)
        throws IOException {

        RequestLimits limits = opInfo.entry.getRequestLimits();
        int keySizeLimit = (limits == null ? -1 :
                            limits.getPrimaryKeySizeLimit());
        int valueSizeLimit = (limits == null ? -1 : limits.getRowSizeLimit());

        PutOpInfo pinfo = new PutOpInfo();
        /* returns the offset at the end of the operation info */
        int endOffset = getV4PutOpInfo(pinfo, rc, true);
        /* at this point the offset should be set to the row value */
        ByteInputStream bis = rc.bbis;
        int startOffset = bis.getOffset();
        ReturnRow.Choice returnChoice =
            pinfo.returnInfo ? ReturnRow.Choice.ALL :
            ReturnRow.Choice.NONE;
        opInfo.returnInfo = pinfo.returnInfo;
        if (opInfo.genInfo != null) {
            /* if no cache size was passed it is 0 */
            opInfo.genInfo.setCacheSize(pinfo.identityCacheSize);
        }

        TableImpl table = opInfo.table;

        /* stream should be positioned at the value */
        RowSerializerImpl row = table.isJsonCollection() ? null :
            (RowSerializerImpl) readRowSerializer(bis, table,
                                                  keySizeLimit,
                                                  valueSizeLimit,
                                                  pinfo.exactMatch);

        if (pinfo.TTL != null) {
            row.setTTL(pinfo.TTL);
        }

        /* Check sub request size limit */
        checkRequestSizeLimit(bis.getOffset()-startOffset, false, limits);

        final TableAPIImpl tableApi = opInfo.entry.getTableAPI();
        final OpFactory factory =
            (OpFactory) tableApi.getTableOperationFactory();
        KVStoreImpl store = tableApi.getStore();
        Key kvKey = null;
        Value kvValue = null;
        PrimaryKeyImpl pkey = null;
        if (table.isJsonCollection()) {
            pkey = (PrimaryKeyImpl) table.createPrimaryKey();
            /* this method fills in PrimaryKey as well */
            kvValue = createValueFromNson(table, pkey, rc.bbis);
            kvKey = table.createKeyInternal(pkey, false, store, opInfo.genInfo);
        } else {
            kvKey = table.createKeyInternal(row, false, store, opInfo.genInfo);
            kvValue = table.createValueInternal(row, store, opInfo.genInfo);
            pkey = table.createPrimaryKeyFromKeyBytes(kvKey.toByteArray());
        }
        TableOperation top = factory.createPutInternal(
            mapPutOpCode(opCode),
            kvKey,
            kvValue,
            pkey,
            table,
            pinfo.TTL,
            returnChoice,
            pinfo.abortIfUnsuccessful,
            pinfo.matchVersion);

        /*
         * Set the offset of InputStream to the reserved offset which
         * points to the position after reading the current operation.
         */
        bis.setOffset(endOffset);
        top.setUpdateTTL(pinfo.updateTTL);
        info.tableOps.add(top);
        rc.tableOpInfos.add(opInfo);
    }


    private void createDelTableOp(OpCode opCode,
                                  WriteMultipleOpInfo info,
                                  RequestContext rc,
                                  TableOperationInfo opInfo)
        throws IOException {

        RequestLimits limits = opInfo.entry.getRequestLimits();
        int keySizeLimit = (limits == null ? -1 :
                            limits.getPrimaryKeySizeLimit());

        DelOpInfo dinfo = new DelOpInfo();
        /* returns the offset at the end of the operation info */
        int endOffset = getV4DelOpInfo(dinfo, rc, true);
        /* at this point the offset should be set to the key value */
        ByteInputStream bis = rc.bbis;
        int startOffset = bis.getOffset();

        ReturnRow.Choice returnChoice =
            dinfo.returnInfo ? ReturnRow.Choice.ALL :
            ReturnRow.Choice.NONE;
        opInfo.returnInfo = dinfo.returnInfo;

        /* stream should be positioned at the value */
        RowSerializer key = readPrimaryKeySerializer(rc.bbis,
                                                     opInfo.table,
                                                     keySizeLimit);
        /* Check sub request size limit */
        checkRequestSizeLimit(bis.getOffset()-startOffset, false, limits);

        final TableAPIImpl tableApi = opInfo.entry.getTableAPI();
        final OpFactory factory =
            (OpFactory) tableApi.getTableOperationFactory();
        final boolean doTombstone = doTombStone(opInfo.entry);

        TableOperation top = null;
        switch (opCode) {
        case DELETE:
            top = factory.createDeleteInternal(key, returnChoice,
                                               doTombstone,
                                               dinfo.abortIfUnsuccessful);
            break;
        case DELETE_IF_VERSION:
            top = factory.createDeleteIfVersionInternal(
                    key, dinfo.matchVersion,
                    returnChoice,
                    doTombstone,
                    dinfo.abortIfUnsuccessful);
            break;
        default:
                throw new IllegalStateException("Unknown operation for " +
                                                "WriteMultipleRequest: " +
                                                opCode);
        }
        /*
         * Set the offset of InputStream to the reserved offset which
         * points to the position after reading the current operation.
         */
        rc.bbis.setOffset(endOffset);
        info.tableOps.add(top);
        rc.tableOpInfos.add(opInfo);
    }

    private void getTableUsageOpInfo(TableUsageOpInfo info,
                                     ByteInputStream bis)
        throws IOException {

        info.startRange = readLong(bis);
        info.endRange = readLong(bis);
        info.limit = readInt(bis);
    }

    private void getV4TableUsageOpInfo(TableUsageOpInfo info,
                                       RequestContext rc)
        throws IOException {

        /* reset the bbis on the payload map */
        rc.resetInputBuffer();
        ByteInputStream bis = rc.bbis;
        boolean found = seekToPayload(bis);
        if (!found) {
            badProtocol("No payload in message");
        }

        MapWalker walker = new MapWalker(bis);
        while (walker.hasNext()) {
            walker.next();
            String name = walker.getCurrentName();
            if (name.equals(LIST_START_INDEX)) {
                info.startIndex = Nson.readNsonInt(bis);
            } else if (name.equals(START)) {
                info.startRange = timeToLong(Nson.readNsonString(bis));
            } else if (name.equals(END)) {
                info.endRange = timeToLong(Nson.readNsonString(bis));
            } else if (name.equals(LIST_MAX_TO_READ)) {
                info.limit = Nson.readNsonInt(bis);
            } else {
                skipUnknownField(walker, name);
            }
        }
    }

    protected void getV4AddReplicaOpInfo(AddReplicaOpInfo info,
                                         RequestContext rc)
        throws IOException {

        /* reset the bbis on the payload map */
        rc.resetInputBuffer();
        ByteInputStream bis = rc.bbis;
        boolean found = PathFinder.seek(PAYLOAD, bis);
        if (!found) {
            badProtocol("No payload in message");
        }

        MapWalker walker = new MapWalker(bis);
        while (walker.hasNext()) {
            walker.next();
            String name = walker.getCurrentName();
            if (name.equals(REGION)) {
                info.region = Nson.readNsonString(bis);
            } else if (name.equals(READ_UNITS)) {
                info.readUnits = Nson.readNsonInt(bis);
            } else if (name.equals(WRITE_UNITS)) {
                info.writeUnits = Nson.readNsonInt(bis);
            } else if (name.equals(ETAG)) {
                String etag = Nson.readNsonString(bis);
                info.matchETag = JsonUtils.decodeBase64(etag);
            } else {
                skipUnknownField(walker, name);
            }
        }
    }

    protected void getV4DropReplicaOpInfo(DropReplicaOpInfo info,
                                          RequestContext rc)
        throws IOException {

        /* reset the bbis on the payload map */
        rc.resetInputBuffer();
        ByteInputStream bis = rc.bbis;
        boolean found = PathFinder.seek(PAYLOAD, bis);
        if (!found) {
            badProtocol("No payload in message");
        }

        MapWalker walker = new MapWalker(bis);
        while (walker.hasNext()) {
            walker.next();
            String name = walker.getCurrentName();
            if (name.equals(REGION)) {
                info.region = Nson.readNsonString(bis);
            } else if (name.equals(ETAG)) {
                String etag = Nson.readNsonString(bis);
                info.matchETag = JsonUtils.decodeBase64(etag);
            } else {
                skipUnknownField(walker, name);
            }
        }
    }

    protected void getV4InternalDdlOpInfo(InternalDdlOpInfo info,
                                          RequestContext rc)
        throws IOException {

        /* reset the bbis on the payload map */
        rc.resetInputBuffer();
        ByteInputStream bis = rc.bbis;
        boolean found = PathFinder.seek(PAYLOAD, bis);
        if (!found) {
            badProtocol("No payload in message");
        }

        MapWalker walker = new MapWalker(bis);
        while (walker.hasNext()) {
            walker.next();
            String name = walker.getCurrentName();
            if (name.equals(OPERATION)) {
                info.operation = Nson.readNsonString(bis);
            } else if (name.equals(PAYLOAD)) {
                info.payload = Nson.readNsonString(bis);
            } else if (name.equals(SYSTEM)) {
                info.isSystem = Nson.readNsonBoolean(bis);
            } else {
                skipUnknownField(walker, name);
            }
        }
    }

    protected void getV4InternalStatusOpInfo(InternalStatusOpInfo info,
                                             RequestContext rc)
        throws IOException {

        /* reset the bbis on the payload map */
        rc.resetInputBuffer();
        ByteInputStream bis = rc.bbis;
        boolean found = PathFinder.seek(PAYLOAD, bis);
        if (!found) {
            badProtocol("No payload in message");
        }

        MapWalker walker = new MapWalker(bis);
        while (walker.hasNext()) {
            walker.next();
            String name = walker.getCurrentName();
            if (name.equals(RESOURCE)) {
                info.resource = Nson.readNsonString(bis);
            } else if (name.equals(RESOURCE_ID)) {
                info.ocid = Nson.readNsonString(bis);
            } else {
                skipUnknownField(walker, name);
            }
        }
    }

    protected void getV4GetReplicaStatsOpInfo(GetReplicaStatsOpInfo info,
                                              RequestContext rc)
        throws IOException {

        /* reset the bbis on the payload map */
        rc.resetInputBuffer();
        ByteInputStream bis = rc.bbis;
        boolean found = PathFinder.seek(PAYLOAD, bis);
        if (!found) {
            badProtocol("No payload in message");
        }

        MapWalker walker = new MapWalker(bis);
        while (walker.hasNext()) {
            walker.next();
            String name = walker.getCurrentName();
            if (name.equals(REGION)) {
                info.replicaName = Nson.readNsonString(bis);
            } else if (name.equals(START)) {
                info.startTime = timeToLong(Nson.readNsonString(bis));
            } else if (name.equals(LIST_MAX_TO_READ)) {
                info.limit = Nson.readNsonInt(bis);
            } else {
                skipUnknownField(walker, name);
            }
        }
    }

    /* use NSON for this */
    private static long timeToLong(String timestamp) {
        return new oracle.nosql.nson.values.TimestampValue(timestamp).getLong();
    }

    /**
     * turns "5 DAYS" into TTL
     */
    private static TimeToLive parseTTL(String ttl) {
        String[] ttlArray = ttl.split(" ");
        if (ttlArray.length != 2) {
            throw new IllegalArgumentException("Bad TTL format: " + ttl);
        }
        long value = Long.parseLong(ttlArray[0]);
        TimeUnit tu = TimeUnit.valueOf(ttlArray[1]);
        return TimeToLive.createTimeToLive(value, tu);
    }

    protected static void badProtocol(String msg) {
        throw new IllegalArgumentException("Bad protocol: " + msg);
    }

    private Consistency readConsistency(ByteInputStream bis)
        throws IOException {
        MapWalker walker = new MapWalker(bis);
        int ctype = -1;
        while (walker.hasNext()) {
            walker.next();
            String name = walker.getCurrentName();
            if (name.equals(TYPE)) {
                ctype = Nson.readNsonInt(bis);
            } else {
                skipUnknownField(walker, name);
            }
        }
        if (ctype < 0 || ctype > 1) {
            throw new IllegalArgumentException("Unknown or missing " +
                                               "consistency type: " + ctype);
        }
        return getConsistency(ctype);
    }

    private Durability mapDurability(ByteInputStream bis)
        throws IOException {

        Durability durability = getDurability(bis);
        /* ignore durability if not on-prem */
        return isOnPrem() ? durability : null;
    }

    private Durability mapDurability(int value)
        throws IOException {

        Durability durability = getDurability(value);
        /* ignore durability if not on-prem */
        return isOnPrem() ? durability : null;
    }

    protected void skipUnknownField(MapWalker walker,
                                    String name)
        throws IOException {
        // TODO log/warn?
        walker.skip();
    }

    /**
     * Find string in payload based on path
     */
    protected String readV4String(RequestContext rc,
                                  String path,
                                  boolean mustExist) throws IOException {
        rc.resetInputBuffer();
        ByteInputStream bis = rc.bbis;
        boolean found = PathFinder.seek(path, bis);
        if (!found) {
            if (mustExist) {
                badProtocol("No item at path " + path);
            }
            return null;
        }
        return Nson.readNsonString(bis);
    }

    protected static boolean seekToPayload(ByteInputStream bis)
        throws IOException {

        return PathFinder.seek(PAYLOAD, bis);
    }
}
