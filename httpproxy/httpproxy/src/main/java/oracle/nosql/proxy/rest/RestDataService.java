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

package oracle.nosql.proxy.rest;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static oracle.nosql.common.http.Constants.CONTENT_LENGTH;
import static oracle.nosql.common.http.Constants.CONTENT_TYPE;
import static oracle.nosql.proxy.ProxySerialization.decodeBase64;
import static oracle.nosql.proxy.ProxySerialization.encodeBase64;
import static oracle.nosql.proxy.protocol.HttpConstants.APPLICATION_JSON;
import static oracle.nosql.proxy.protocol.HttpConstants.APPLICATION_JSON_NOCHARSET;
import static oracle.nosql.proxy.protocol.HttpConstants.X_FORWARDED_FOR_HEADER;
import static oracle.nosql.proxy.protocol.HttpConstants.X_REAL_IP_HEADER;
import static oracle.nosql.proxy.protocol.JsonProtocol.*;
import static oracle.nosql.proxy.protocol.Protocol.ILLEGAL_ARGUMENT;
import static oracle.nosql.proxy.protocol.Protocol.NO_ERROR;
import static oracle.nosql.proxy.protocol.Protocol.OPERATION_NOT_SUPPORTED;
import static oracle.nosql.proxy.protocol.Protocol.SERVER_ERROR;
import static oracle.nosql.proxy.protocol.Protocol.TABLE_NOT_FOUND;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.ADD_REPLICA;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.ALTER_TABLE;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.CANCEL_WORKREQUEST;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.CHANGE_COMPARTMENT;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.CREATE_INDEX;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.CREATE_TABLE;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.DELETE;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.DROP_INDEX;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.DROP_REPLICA;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.DROP_TABLE;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.GET;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.GET_CONFIGURATION;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.GET_INDEX;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.GET_INDEXES;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.GET_TABLE;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.GET_TABLE_USAGE;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.GET_WORKREQUEST;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.LIST_TABLES;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.LIST_WORKREQUESTS;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.GET_WORKREQUEST_ERRORS;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.GET_WORKREQUEST_LOGS;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.PREPARE;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.PUT;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.QUERY;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.REMOVE_CONFIG_KMS_KEY;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.SUMMARIZE;
import static oracle.nosql.proxy.protocol.Protocol.OpCode.UPDATE_CONFIGURATION;
import static oracle.nosql.proxy.security.AccessContext.EXTERNAL_OCID_PREFIX;

import java.io.IOException;
import java.io.StringReader;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.logging.Level;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import oracle.kv.Consistency;
import oracle.kv.FaultException;
import oracle.kv.Key;
import oracle.kv.MetadataNotFoundException;
import oracle.kv.ResourceLimitException;
import oracle.kv.Value;
import oracle.kv.Version;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.query.PreparedStatementImpl;
import oracle.kv.impl.api.query.QueryStatementResultImpl;
import oracle.kv.impl.api.table.NullJsonValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.NumberDefImpl;
import oracle.kv.impl.api.table.PrimaryKeyImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableAPIImpl.GeneratedValueInfo;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableKey;
import oracle.kv.impl.query.QueryException;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.PrepareCallback.QueryOperation;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FieldValueFactory;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.RecordDef;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.TimeToLive;
import oracle.kv.table.WriteOptions;
import oracle.nosql.common.JsonBuilder;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.http.LogControl;
import oracle.nosql.common.http.Service;
import oracle.nosql.common.kv.drl.LimiterManager;
import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.proxy.Config;
import oracle.nosql.proxy.DataServiceHandler;
import oracle.nosql.proxy.ExcessiveUsageException;
import oracle.nosql.proxy.MonitorStats;
import oracle.nosql.proxy.RequestException;
import oracle.nosql.proxy.RequestLimits;
import oracle.nosql.proxy.MonitorStats.OperationType;
import oracle.nosql.proxy.audit.ProxyAuditManager;
import oracle.nosql.proxy.filter.FilterHandler;
import oracle.nosql.proxy.filter.FilterHandler.Filter;
import oracle.nosql.proxy.protocol.ByteInputStream;
import oracle.nosql.proxy.protocol.ByteOutputStream;
import oracle.nosql.proxy.protocol.JsonProtocol.JsonArray;
import oracle.nosql.proxy.protocol.JsonProtocol.JsonObject;
import oracle.nosql.proxy.protocol.JsonProtocol.JsonPayload;
import oracle.nosql.proxy.protocol.Protocol.OpCode;
import oracle.nosql.proxy.sc.GetTableResponse;
import oracle.nosql.proxy.sc.GetWorkRequestResponse;
import oracle.nosql.proxy.sc.IndexResponse;
import oracle.nosql.proxy.sc.ListTableInfoResponse;
import oracle.nosql.proxy.sc.ListWorkRequestResponse;
import oracle.nosql.proxy.sc.TableUsageResponse;
import oracle.nosql.proxy.sc.TableUtils;
import oracle.nosql.proxy.sc.TableUtils.MapPrepareCB;
import oracle.nosql.proxy.sc.TenantManager;
import oracle.nosql.proxy.sc.TableUtils.PrepareCB;
import oracle.nosql.proxy.security.AccessChecker;
import oracle.nosql.proxy.security.AccessContext;
import oracle.nosql.proxy.security.AccessContext.Type;
import oracle.nosql.proxy.util.ErrorManager;
import oracle.nosql.proxy.util.TableCache.TableEntry;
import oracle.nosql.util.filter.Rule;
import oracle.nosql.util.tmi.IndexInfo;
import oracle.nosql.util.tmi.TableInfo;
import oracle.nosql.util.tmi.TableLimits;
import oracle.nosql.util.tmi.WorkRequest;

/**
 * Rest service that handles the following requests:
 *
 *  GET     /<API_VESRION>/tables
 *  POST    /<API_VESRION>/tables
 *  PUT     /<API_VESRION>/tables/{tableNameOrId}
 *  DELETE  /<API_VESRION>/tables/{tableNameOrId}
 *  GET     /<API_VESRION>/tables/{tableNameOrId}
 *  GET     /<API_VESRION>/tables/{tableNameOrId}/usage
 *  POST    /<API_VESRION>/tables/{tableNameOrId}/actions/changeCompartment
 *
 *  POST    /<API_VESRION>/tables/{tableNameOrId}/replicas
 *  DELETE  /<API_VESRION>/tables/{tableNameOrId}/replicas/{region}
 *
 *  GET     /<API_VESRION>/tables/{tableNameOrId}/indexes
 *  POST    /<API_VESRION>/tables/{tableNameOrId}/indexes
 *  GET     /<API_VESRION>/tables/{tableNameOrId}/indexes/{indexName}
 *  DELETE  /<API_VESRION>/tables/{tableNameOrId}/indexes/{indexName}
 *
 *  PUT     /<API_VESRION>/tables/{tableNameOrId}/rows
 *  GET     /<API_VESRION>/tables/{tableNameOrId}/rows
 *  DELETE  /<API_VESRION>/tables/{tableNameOrId}/rows
 *
 *  POST    /<API_VESRION>/query
 *  GET     /<API_VESRION>/query/prepare
 *  GET     /<API_VESRION>/query/summarize
 *
 *  GET     /<API_VESRION>/workRequests/{workRequestId}
 *  GET     /<API_VESRION>/workRequests
 *  GET     /<API_VESRION>/workRequests/{workRequestId}/errors
 *  GET     /<API_VESRION>/workRequests/{workRequestId}/logs
 *  DELETE  /<API_VESRION>/workRequests/{workRequestId}
 *
 *  GET     /<API_VESRION>/configuration/
 *  PUT     /<API_VESRION>/configuration/
 *  POST    /<API_VESRION>/configuration/actions/unassignkmskey
 *
 *  OPTIONS /<API_VERSION>/*
 */
/*
 * TODO:
 * Configuration for rest service:
 *   1. there is only one port for all services
 *   2. the resources such as threads, etc, are shared among all of the
 *      services created (basically the binary and REST).
 *   This is reasonable, and simple for the time being but it's possible we
 *   might be forced to use a different port and/or http server for REST.
 */
public abstract class RestDataService extends DataServiceHandler
    implements Service {

    /*
     * Now rest interface uses QUERY_V2 but actually limited to support QUERY_V1
     *
     * It is to be compatible with that KV 20.3 supports QUERY_V2 or later
     * version and QUERY_V1 is no longer supported, but oci driver doesn't
     * support advanced query, so add additional check in doPrepare() method
     * of this class to limit the query supported only be "simple" ones
     * supported by QUERY_V1.
     */
    private final static short queryVersion = QUERY_V2;

    private final static int DEFAULT_TRACE_LEVEL = 0;
    private final static boolean DEFAULT_IF_EXISTS = false;
    private final static boolean DEFAULT_IF_NOT_EXISTS = false;
    private final static int DEFAULT_PAGE_SIZE = 1000;
    private final static int DEFAULT_TIMEOUT_MS = 5000;

    /* The UTC zone */
    private final static ZoneId UTCZone = ZoneId.of(ZoneOffset.UTC.getId());
    /* DataTimeFormatter -- use the static ISO_D */
    private final static DateTimeFormatter timestampFormatter =
        DateTimeFormatter.ISO_DATE_TIME.withZone(UTCZone);
    /*
     * RetryToken must contain alphanumerics plus underscore ("_") and dash(-)
     * and length <= 64
     */
    private static final String VALID_RETRYTOKEN_REGEX = "^[a-zA-Z0-9_-]{1,64}$";


    private static final String ADDITIONAL_ALLOWED_HEADERS =
        "opc-client-info, opc-client-retries, opc-idempotency-token, " +
        "opc-retry-token, x-cross-tenancy-request";

    private static final String EXPOSED_HEADERS =
        "Content-Length, opc-request-id, opc-next-page, opc-work-request-id, " +
        "ETag, retry-after, opc-client-info, Location";

    private final static Map<Integer, ErrorCode> errorMap = createErrorCodeMap();

    private final Map<HttpMethod, Map<UrlInfo, OpCode>> methodMap =
                                                        new HashMap<>();
    private final boolean isCmekEnabled;

    private void initMethods() {
        /* Table resource */
        initMethod(HttpMethod.POST, "/tables",
                   CREATE_TABLE);
        initMethod(HttpMethod.POST,
                   "/tables/{tableNameOrId}/actions/changeCompartment",
                   CHANGE_COMPARTMENT);
        initMethod(HttpMethod.PUT, "/tables/{tableNameOrId}",
                   ALTER_TABLE);
        initMethod(HttpMethod.DELETE, "/tables/{tableNameOrId}",
                   DROP_TABLE);
        initMethod(HttpMethod.GET, "/tables",
                   LIST_TABLES);
        initMethod(HttpMethod.GET, "/tables/{tableNameOrId}",
                   GET_TABLE);
        initMethod(HttpMethod.GET, "/tables/{tableNameOrId}/usage",
                   GET_TABLE_USAGE);

        /* Replica resource */
        initMethod(HttpMethod.POST, "/tables/{tableNameOrId}/replicas",
                   ADD_REPLICA);
        initMethod(HttpMethod.DELETE, "/tables/{tableNameOrId}/replicas/{region}",
                   DROP_REPLICA);

        /* Index resource */
        initMethod(HttpMethod.POST, "/tables/{tableNameOrId}/indexes",
                   CREATE_INDEX);
        initMethod(HttpMethod.DELETE,
                   "/tables/{tableNameOrId}/indexes/{indexName}",
                   DROP_INDEX);
        initMethod(HttpMethod.GET,
                   "/tables/{tableNameOrId}/indexes/{indexName}",
                   GET_INDEX);
        initMethod(HttpMethod.GET, "/tables/{tableNameOrId}/indexes",
                   GET_INDEXES);

        /* Row resource */
        initMethod(HttpMethod.PUT, "/tables/{tableNameOrId}/rows", PUT);
        initMethod(HttpMethod.DELETE, "/tables/{tableNameOrId}/rows", DELETE);
        initMethod(HttpMethod.GET, "/tables/{tableNameOrId}/rows", GET);

        /* Query */
        initMethod(HttpMethod.POST, "/query", QUERY);
        initMethod(HttpMethod.GET, "/query/prepare", PREPARE);
        initMethod(HttpMethod.GET, "/query/summarize", SUMMARIZE);

        /* WorkRequests */
        initMethod(HttpMethod.GET, "/workRequests/{workRequestId}",
                   GET_WORKREQUEST);
        initMethod(HttpMethod.GET, "/workRequests",
                   LIST_WORKREQUESTS);
        initMethod(HttpMethod.GET, "/workRequests/{workRequestId}/errors",
                   GET_WORKREQUEST_ERRORS);
        initMethod(HttpMethod.GET, "/workRequests/{workRequestId}/logs",
                   GET_WORKREQUEST_LOGS);
        initMethod(HttpMethod.DELETE, "/workRequests/{workRequestId}",
                   CANCEL_WORKREQUEST);

        /* Configuration */
        initMethod(HttpMethod.GET, "/configuration", GET_CONFIGURATION);
        initMethod(HttpMethod.PUT, "/configuration", UPDATE_CONFIGURATION);
        initMethod(HttpMethod.POST, "/configuration/actions/unassignkmskey",
                   REMOVE_CONFIG_KMS_KEY);
    }

    private final Map<OpCode, ProxyOperation> operationMap =
        new HashMap<OpCode, ProxyOperation>();

    private void initOperations() {
        /* Table resource */
        initOperation(CREATE_TABLE, this::handleCreateTable);
        initOperation(CHANGE_COMPARTMENT, this::handleChangeCompartment);
        initOperation(ALTER_TABLE, this::handleUpdateTable);
        initOperation(DROP_TABLE, this::handleDropTable);
        initOperation(GET_TABLE, this::handleGetTable);
        initOperation(LIST_TABLES, this::handleListTables);
        initOperation(GET_TABLE_USAGE, this::handleGetTableUsage);

        initOperation(ADD_REPLICA, this::handleAddReplica);
        initOperation(DROP_REPLICA, this::handleDropReplica);

        /* Index resource */
        initOperation(CREATE_INDEX, this::handleCreateIndex);
        initOperation(DROP_INDEX, this::handleDropIndex);
        initOperation(GET_INDEX, this::handleGetIndex);
        initOperation(GET_INDEXES, this::handleGetIndexes);

        /* Row resource */
        initOperation(PUT, this::handlePut);
        initOperation(DELETE, this::handleDelete);
        initOperation(GET, this::handleGet);

        /* Query resource */
        initOperation(QUERY, this::handleQuery);
        initOperation(PREPARE, this::handlePrepare);
        initOperation(SUMMARIZE, this::handleSummarize);

        /* WorkRequests resource */
        initOperation(GET_WORKREQUEST, this::handleGetWorkRequest);
        initOperation(LIST_WORKREQUESTS, this::handleListWorkRequests);
        initOperation(GET_WORKREQUEST_ERRORS,
                      this::handleListWorkRequestErrors);
        initOperation(GET_WORKREQUEST_LOGS, this::handleListWorkRequestLogs);
        initOperation(CANCEL_WORKREQUEST, this::handleCancelWorkRequest);

        /* Configuration */
        initOperation(GET_CONFIGURATION, this::handleGetConfiguration);
        initOperation(UPDATE_CONFIGURATION, this::handleUpdateConfiguration);
        initOperation(REMOVE_CONFIG_KMS_KEY, this::handleRemoveKmsKey);
    }

    /*
     * A map used by calls to mark a failed operation, mapping OpCode to
     * OperationType for event logging.
     */
    private static final HashMap<OpCode, OperationType> opCodeMap =
        new HashMap<OpCode, OperationType>();

    static {
        opCodeMap.put(PUT, OperationType.PUT);
        opCodeMap.put(GET, OperationType.GET);
        opCodeMap.put(DELETE, OperationType.DELETE);

        opCodeMap.put(QUERY, OperationType.QUERY);
        opCodeMap.put(PREPARE, OperationType.PREPARE);
        opCodeMap.put(SUMMARIZE, OperationType.PREPARE);

        opCodeMap.put(CREATE_TABLE, OperationType.TABLE_REQUEST);
        opCodeMap.put(ALTER_TABLE, OperationType.TABLE_REQUEST);
        opCodeMap.put(DROP_TABLE, OperationType.TABLE_REQUEST);
        opCodeMap.put(CHANGE_COMPARTMENT, OperationType.TABLE_REQUEST);
        opCodeMap.put(GET_TABLE, OperationType.GET_TABLE);
        opCodeMap.put(GET_TABLE_USAGE, OperationType.GET_TABLE_USAGE);
        opCodeMap.put(LIST_TABLES, OperationType.LIST_TABLES);

        opCodeMap.put(ADD_REPLICA, OperationType.TABLE_REQUEST);
        opCodeMap.put(DROP_REPLICA, OperationType.TABLE_REQUEST);

        opCodeMap.put(CREATE_INDEX, OperationType.TABLE_REQUEST);
        opCodeMap.put(DROP_INDEX, OperationType.TABLE_REQUEST);
        opCodeMap.put(GET_INDEXES, OperationType.GET_INDEXES);
        opCodeMap.put(GET_INDEX, OperationType.GET_INDEXES);

        opCodeMap.put(GET_WORKREQUEST, OperationType.GET_WORKREQUEST);
        opCodeMap.put(LIST_WORKREQUESTS, OperationType.LIST_WORKREQUESTS);
        opCodeMap.put(GET_WORKREQUEST_ERRORS,
                      OperationType.LIST_WORKREQUEST_ERRORS);
        opCodeMap.put(GET_WORKREQUEST_LOGS,
                      OperationType.LIST_WORKREQUEST_LOGS);
        opCodeMap.put(CANCEL_WORKREQUEST, OperationType.CANCEL_WORKREQUEST);
    }

    public RestDataService(SkLogger logger,
                           TenantManager tm,
                           AccessChecker ac,
                           MonitorStats stats,
                           ProxyAuditManager audit,
                           FilterHandler filter,
                           ErrorManager errorManager,
                           LimiterManager limiterManager,
                           Config config,
                           LogControl logControl) {
        super(logger, tm, stats, audit,
              filter, errorManager, limiterManager,
              config, logControl);
        initMethods();
        initOperations();
        isCmekEnabled = config.isCmekEnabled();
    }

    private void initOperation(OpCode op, ProxyOperation operation) {
        operationMap.put(op, operation);
    }

    private void initMethod(HttpMethod method, String path, OpCode opCode) {
        Map<UrlInfo, OpCode> urlOps = methodMap.get(method);
        if (urlOps == null) {
            urlOps = new HashMap<>();
            methodMap.put(method, urlOps);
        }
        urlOps.put(new UrlInfo(path), opCode);
    }

    /*
     * Handle table DDL operation
     */
    protected abstract GetTableResponse handleTableDdl(
            FullHttpRequest request,
            AccessContext actx,
            String tableName,
            String statement,
            TableLimits limits,
            boolean isAutoReclaimable,
            Map<String, String> freeformTags,
            Map<String, Map<String, Object>> definedTags,
            byte[] matchETag,
            String retryToken,
            OpCode op,
            PrepareCB callback,
            LogContext lc,
            String apiVersion);

    /*
     * Handle table limit operation
     */
    protected abstract GetTableResponse handleTableLimits(
                                            FullHttpRequest request,
                                            AccessContext actx,
                                            String tableName,
                                            TableLimits limits,
                                            byte[] matchETag,
                                            LogContext lc,
                                            String apiVersion);

    /*
     * Handle table tags operation
     */
    protected abstract GetTableResponse handleTableTags(
            FullHttpRequest request,
            AccessContext actx,
            String tableName,
            Map<String, String> freeformTags,
            Map<String, Map<String, Object>> predefinedTags,
            byte[] matchETag,
            LogContext lc,
            String apiVersion);

    /*
     * Handle change compartment operation
     */
    protected abstract GetTableResponse handleChangeCompartment(
            FullHttpRequest request,
            AccessContext actx,
            String tableName,
            byte[] matchETag,
            String retryToken,
            LogContext lc,
            String apiVersion);

    @Override
    public FullHttpResponse handleRequest(FullHttpRequest request,
                                          ChannelHandlerContext ctx,
                                          LogContext lc) {
        final RequestContext rc = new RequestContext(request, ctx, lc, null);

        /* set up REST specific information */
        rc.readREST();

        /* handle request */
        FullHttpResponse resp = handleRequestInternal(rc);

        /* Close the output stream, the response may still be sent later */
        rc.releaseBuffers();

        /* check for excessive errors */
        if (isErrorLimitingResponse(resp, rc.ctx)) {
            if (incrementErrorRate(resp, rc)) {
                /* response sent later, or not at all */
                return null;
            }
        }

        /*
         * Rate limiting logic: if the operation used read/write
         * units, apply them to the rate limiter(s) for the table.
         * This may result in the response being queued for later
         * delivery (response delayed).
         */
        if (rateLimitResponse(limiterManager, rc, resp)) {
            /* response sent later, by different thread */
            return null;
        }

        endAudit(resp, rc);
        sendResponse(resp, rc);
        return null;
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

    private FullHttpResponse handleRequestInternal(RequestContext rc) {

        logger.fine("Proxy data service request on channel: " + rc.ctx, rc.lc);

        final HttpMethod method = rc.request.method();

        try {
            /* Block all requests if there is "big red button" rule */
            Rule rule = getBlockAll(rc.lc);
            if (rule != null) {
                return filter.handleRequest(rc, rule);
            }

            /* Handle OPTIONS used for pre-flight request. */
            if (HttpMethod.OPTIONS.equals(method)) {
                return handleOptions(rc.request, rc.lc);
            }

            /* Validate the input */
            FullHttpResponse violation = validateHttpRequest(rc);
            if (violation != null) {
                return violation;
            }
            markOpStart(rc);

            rc.restParams = new RequestParams(rc.request);
            return handleRequest(rc);
        } catch (Throwable e) {
            markOpFailed(rc, 1 /* serverFailure */);

            /*
             * TODO: are there exceptions that should result in closing the
             * channel?
             */
            final String faultMsg = e.toString();
            if (logger.isLoggable(Level.INFO) &&
                limiter.isHandleable(faultMsg)) {
                /*
                 * include stack trace here because this path should be
                 * infrequent and may represent a bug in the proxy
                 */
                logger.logEvent("DataService" /* category */, Level.INFO,
                                "Request exception" /* subject */,
                                faultMsg /* message */, e);
            }
            return createErrorResponse(rc, e.getMessage(),
                                       mapExceptionToErrorCode(e));
        }
    }

    /**
     * Clean up resources on service shutdown
     */
    @Override
    public void shutDown() {
        tm.shutDown();
    }

    @Override
    protected String getAdditionalAllowedHeaders() {
        return ADDITIONAL_ALLOWED_HEADERS;
    }

    /*
     * Validate the request.
     */
    protected FullHttpResponse validateHttpRequest(RequestContext rc) {
        return null;
    }

    /* Handle unknown field when parses payload, just skip it */
    protected void handleUnknownField(String title, JsonPayload pl)
        throws IOException {

        pl.skipValue();
    }

    private FullHttpResponse handleRequest(RequestContext rc) {

        boolean doRetry = true;
        final String serialVersion = rc.restParams.getRoot();

        rc.opCode = findOpCode(rc.request.method(), rc.restParams);
        rc.opType = getOpType(rc.opCode);

        /* TODO: change this to match attmptRetry() logic in DataService */
        while (true) {

            rc.origTableName = null;
            rc.mappedTableName = null;
            rc.actx = null;
            FullHttpResponse resp = null;

            try {
                try {
                    try {

                    /* Check whether the configuration operation is supported */
                    checkConfigurationOperation(rc.opCode);

                    /* Filters request based on the OpCode */
                    filterRequest(rc.opCode, rc.lc);

                    /* Get the operation handler */
                    ProxyOperation operation = operationMap.get(rc.opCode);
                    if (operation == null) {
                        throw new RequestException(UNKNOWN_OPERATION,
                                "Unknown op code: " + rc.opCode);
                    }

                    /* Perform access check */
                    checkAccess(rc);

                    startAudit(rc, rc.origTableName, rc.actx);

                    if (logger.isLoggable(Level.FINE, rc.lc)) {
                        logger.fine("handleRequest, op " +
                                    opCodeToRestString(rc.opCode) +
                                    ", namespace " + getLogNamespace(rc.actx) +
                                    ", table " + rc.origTableName, rc.lc);
                    }

                    /* Create response */
                    resp = createResponse(rc.bbos.getByteBuf(), rc.requestId);

                    /* Execute operation */
                    operation.handle(resp, rc, rc.actx, serialVersion);
                    if (logger.isLoggable(Level.FINE, rc.lc)) {
                        logger.fine("handleRequest, succeeded op=" +
                                    opCodeToRestString(rc.opCode) +
                                    " namespace=" + getLogNamespace(rc.actx) +
                                    " table=" + rc.origTableName, rc.lc);
                    }
                    } catch (Exception e) {
                        if (e instanceof ExcessiveUsageException) {
                            /*
                             * ExcessiveUsageException wraps the underlying
                             * reason, and indicates that this operation
                             * resulted in some resource being overused or
                             * abused (possible DDoS, etc).
                             * In this case, we set a flag in the channel
                             * context so that upstream callers can act
                             * accordingly.
                             */
                            rc.excessiveUse = true;
                            throw e.getCause();
                        } else {
                            throw e;
                        }
                    }
                } catch (IllegalArgumentException iae) {
                    /* common handling for IAE, not logged */
                    throw new RequestException(ILLEGAL_ARGUMENT,
                                               (opCodeToRestString(rc.opCode) +
                                                ": Illegal Argument: " +
                                                iae.getMessage()));
                } catch (ResourceLimitException rle) {
                    /* logged below, if at all */
                    handleLimitException(rle);
                } catch (FaultException fe) {
                    /* TODO: map exception to more specific error codes */
                    throw new RequestException(SERVER_ERROR,
                                               opCodeToRestString(rc.opCode) +
                                               ": Server too busy: " +
                                               fe.getMessage());
                } catch (MetadataNotFoundException mnfe) {
                    if (doRetry) {

                        /* reset response buffer for retry */
                        rc.resetOutputBuffer();

                        if (rc.mappedTableName != null) {
                            /* remove the table from the cache */
                            logger.info("Proxy received metadata not found, " +
                                        "clearing table cache entry for " +
                                        "table " + rc.mappedTableName +
                                        " and retrying", rc.lc);
                            tableCache.flushEntry(rc.actx.getNamespace(),
                                                  rc.mappedTableName);
                        } else {
                            /*
                             * clear entire cache. Maybe add a way to flush just
                             * the portion used by the namespace. That requires
                             * change in TableCache.
                             */
                            logger.info("Proxy received metadata not found, " +
                                        "clearing entire table cache and " +
                                        "retrying", rc.lc);
                            tableCache.clear();
                        }

                        if (rc.actx != null && rc.actx.getType() == Type.IAM) {
                            rc.actx.resetTableNameMapping();
                        }
                        doRetry = false;
                        continue;
                        /* TODO: MNFE retry internal */
                    }
                    /*
                     * Map to table not found
                     */
                    String msg = "Table not found: " + mnfe.getMessage();
                    if (rc.origTableName != null) {
                        msg += ", name: " + rc.origTableName;
                    }
                    throw new RequestException(TABLE_NOT_FOUND, msg);
                } catch (RequestException re) {
                    /*
                     * Allow a single retry after resetting mappings on
                     * table not found
                     */
                    if (re.getErrorCode() != TABLE_NOT_FOUND ||
                        !doRetry) {
                        throw re;
                    }

                    if (OpCode.isTMOperation(rc.opCode) ||
                        OpCode.isWorkRequestOp(rc.opCode) ||
                        rc.opCode == OpCode.PUT ||
                        rc.origTableName == null ||
                        rc.origTableName.startsWith(EXTERNAL_OCID_PREFIX)) {
                        throw re;
                    }
                    /*
                     * retry on TABLE_NOT_FOUND in case access context
                     * holds obsolete table name mapping
                     */
                    if (rc.actx != null) {
                        rc.actx.resetTableNameMapping();
                    }

                    /* rest response buffer for retry */
                    rc.resetOutputBuffer();
                    doRetry = false;
                    continue;
                } catch (FilterRequestException bre) {
                    return handleFilterRequest(bre, rc);
                } catch (Throwable e) {
                    throw new RequestException(SERVER_ERROR,
                                               "Unknown Exception" + e);

                }
            } catch (RequestException re) {
                logRequestException(re, rc.lc);

                /* Mark failure */
                markOpFailed(rc, getRequestExceptionFailure(re));

                /* Build error response */
                resp = createErrorResponse(rc,
                                           mapErrorMessage(rc.actx,
                                                           rc.origTableName,
                                                           re.getMessage()),
                                           mapExceptionToErrorCode(re));
                if (logger.isLoggable(Level.FINE, rc.lc)) {
                    logger.fine("handleRequest, failed op=" +
                                opCodeToRestString(rc.opCode) +
                                ", error: " + re, rc.lc);
                }
            }
            return resp;
        }
    }

    private void checkConfigurationOperation(OpCode opCode) {
        if (!isCmekEnabled && OpCode.isUpdateConfigurationRequestOp(opCode)) {
            throw new RequestException(ILLEGAL_ARGUMENT,
                "The configuration operation is not enabled: " + opCode);
        }
    }

    /*
     * Performs access check for 3 types of operations:
     *   1. Work request
     *   2. Configuration
     *   3. DDL and DML
     */
    private void checkAccess(RequestContext rc) {
        String compartmentId = null;

        if (opHasCompartmentIdInUrl(rc.opCode)) {
            compartmentId = readCompartmentId(rc.restParams);
        }

        if (OpCode.isWorkRequestOp(rc.opCode)) {
            String workRequestId = null;
            if (opHasWorkRequestIdInUrl(rc.opCode)) {
                workRequestId = readWorkRequestId(rc.restParams);
            }
            OpCode[] authorizeOps = null;
            boolean shouldAuthorizeAllOps = true;
            if (rc.opCode == OpCode.LIST_WORKREQUESTS) {
                /*
                 * The list-work-requests returns all work requests for
                 * operations based on the work request mechanism. Currently,
                 * we have DDL and CMEK operations.
                 *
                 * List-work-requests does not require all sub-operations must
                 * be authorized, it will only return the work requests for
                 * those operations that are authorized for this request.
                 *
                 * e.g. If user only has permission to read DDL work request,
                 * list-work-requests will only return DDL work requests. If
                 * user has permissions to read work requests for both DDL and
                 * CMEK operations, work requests for both operations will be
                 * returned.
                 *
                 * The authorizeOps contains all the operations to be authorized.
                 * The shouldAuthorizeAllOps flag is set to false, indicating
                 * that not all operations need to be authorized. After
                 * permission check, the authorized operations are returned in
                 * AccessContext.AuthorizedOps.
                 */
                authorizeOps = OpCode.getListWorkRequestSubOps(isCmekEnabled);
                shouldAuthorizeAllOps = false;
            }
            rc.actx = checkWorkRequestAccess(rc.request,
                                             rc.opCode,
                                             authorizeOps,
                                             shouldAuthorizeAllOps,
                                             compartmentId,
                                             workRequestId,
                                             rc.lc);
        } else if (OpCode.isConfigurationRequestOp(rc.opCode)) {
            OpCode[] authorizeOpCodes = null;
            if (rc.opCode == OpCode.GET_CONFIGURATION) {
                /*
                 * Get-configuration returns the service level configurations,
                 * we only have kms-key configuration so far, more configuration
                 * could be added in future.
                 *
                 * Get-configuration requires user has the permission to read
                 * all the sub configurations. If the user does not have
                 * permission to read any sub configuration, the user does not
                 * have permission to execute get-configuration.
                 */
                authorizeOpCodes = OpCode.getGetConfigurationSubOps();
            }
            rc.actx = checkConfigurationAccess(rc.request,
                                               rc.opCode,
                                               authorizeOpCodes,
                                               compartmentId,
                                               rc.lc);
        } else {
            if (opHasTableNameOrIdInUrl(rc.opCode)) {
                rc.origTableName = readTableNameOrId(rc.restParams);
            }
            rc.actx = checkAccess(rc.request, compartmentId, rc.opCode,
                                  rc.origTableName, this, rc.lc);
            rc.mappedTableName = getMapTableName(rc.actx, rc.opCode,
                                                 rc.origTableName);
        }
    }

    /**
     * Check authorization for WorkRequest operation
     */
    protected abstract AccessContext
        checkWorkRequestAccess(FullHttpRequest request,
                               OpCode opCode,
                               OpCode[] authorizeOpCodes,
                               boolean shouldAuthorizeAllOps,
                               String compartmentId,
                               String workRequestId,
                               LogContext lc);
    /**
     * Check authorization for Configuration operation
     */
    protected abstract AccessContext
        checkConfigurationAccess(FullHttpRequest request,
                                 OpCode opCode,
                                 OpCode[] authorizeOps,
                                 String compartmentId,
                                 LogContext lc);

    /**
     * Default - nothing to do, return the original error message.
     */
    protected String mapErrorMessage(AccessContext actx,
                                     String tableName,
                                     String message) {
        return filterStackTraces(message);
    }

    /**
     * Finds the http method to handle the given request.
     *
     * It compares the http method URL path with the given request's URL in
     * case sensitive manner.
     */
    private OpCode findOpCode(HttpMethod method, RequestParams request) {
        final Map<UrlInfo, OpCode> urlOps = methodMap.get(method);
        if (urlOps != null) {
            for (Map.Entry<UrlInfo, OpCode> info : urlOps.entrySet()) {
                UrlInfo url = info.getKey();
                if (url.match(request)) {
                    /*
                     * Found the method, parse the parameters from request path
                     * and store to RequestParams.pathParam.
                     */
                    for (int index : url.getIndexParams()) {
                        request.addPathParam(url.getParamName(index), index);
                    }
                    return info.getValue();
                }
            }
        }
        throw new RequestException(ILLEGAL_ARGUMENT,
            "Unsupported request " + request.getUri());
    }

    private String opCodeToRestString(OpCode code) {
        switch (code) {
        case LIST_TABLES:
            return "LIST_TABLES";
        case CREATE_TABLE:
            return "CREATE_TABLE";
        case GET_TABLE:
            return "GET_TABLE";
        case ALTER_TABLE:
            return "ALTER_TABLE";
        case DROP_TABLE:
            return "DELETE_TABLE";
        case GET_TABLE_USAGE:
            return "GET_TABLE_USAGE";
        case CHANGE_COMPARTMENT:
            return "CHANGE_COMPARTMENT";
        case GET_INDEXES:
            return "GET_INDEXES";
        case GET_INDEX:
            return "GET_INDEX";
        case CREATE_INDEX:
            return "CREATE_INDEX";
        case DROP_INDEX:
            return "DROP_INDEX";
        case PUT:
            return "PUT";
        case GET:
            return "GET";
        case DELETE:
            return "DELETE";
        case QUERY:
            return "QUERY";
        case PREPARE:
            return "PREPARE";
        case SUMMARIZE:
            return "SUMMARIZE";
        case LIST_WORKREQUESTS:
            return "LIST_WORKREQUESTS";
        case GET_WORKREQUEST:
            return "GET_WORKREQUEST";
        case CANCEL_WORKREQUEST:
            return "CANCEL_WORKREQUEST";
        case GET_WORKREQUEST_ERRORS:
            return "LIST_WORKREQUEST_ERRORS";
        case GET_WORKREQUEST_LOGS:
            return "LIST_WORKREQUEST_LOGS";
        case ADD_REPLICA:
            return "ADD_REPLICA";
        case DROP_REPLICA:
            return "DROP_REPLICA";
        default:
            return "UNKNOWN OPERATION";
        }
    }

    /**
     * Return true if the op has a table name in URL path.
     */
    private static boolean opHasTableNameOrIdInUrl(OpCode op) {
        switch (op) {
        case GET_TABLE:
        case GET_TABLE_USAGE:
        case CHANGE_COMPARTMENT:
        case GET_INDEX:
        case GET_INDEXES:
        case PUT:
        case DELETE:
        case GET:
        case CREATE_INDEX:
        case ALTER_TABLE:
        case DROP_INDEX:
        case DROP_TABLE:
        case DROP_REPLICA:
            return true;
        default:
            return false;
        }
    }

    /**
     * Return true if the op has a compartmentId in URL path.
     */
    private static boolean opHasCompartmentIdInUrl(OpCode op) {
        switch (op) {
        case LIST_TABLES:
        case GET_TABLE:
        case GET_TABLE_USAGE:
        case GET_INDEX:
        case GET_INDEXES:
        case DELETE:
        case GET:
        case PREPARE:
        case SUMMARIZE:
        case LIST_WORKREQUESTS:
        case GET_CONFIGURATION:
        case UPDATE_CONFIGURATION:
        case REMOVE_CONFIG_KMS_KEY:
            return true;
        default:
            return false;
        }
    }

    /**
     * Return true if the op has a workRequestId in URL path.
     */
    private static boolean opHasWorkRequestIdInUrl(OpCode op) {
        switch(op) {
        case GET_WORKREQUEST:
        case GET_WORKREQUEST_LOGS:
        case GET_WORKREQUEST_ERRORS:
        case CANCEL_WORKREQUEST:
            return true;
        default:
            return false;
        }
    }

    /**
     * Create table
     */
    private void handleCreateTable(FullHttpResponse response,
                                   RequestContext rc,
                                   AccessContext actx,
                                   String apiVersion) {

        /* Header parameter */
        String retryToken = readOpcRetryToken(rc.restParams);

        /* Payload */
        String name = null;
        String compartmentId = null;
        String ddlStatement = null;
        TableLimits tableLimits = null;
        boolean isAutoReclaimable = false;
        Map<String, String> freeformTags = null;
        Map<String, Map<String, Object>> definedTags = null;

        checkNotNull(CREATE_TABLE_DETAILS, rc.restParams.getPayload());

        JsonPayload pl = null;
        try {
            pl = rc.restParams.parsePayload();
            while (pl.hasNext()) {
                if (pl.isField(NAME)) {
                    name = pl.readString();
                    checkNotEmpty(NAME, name);
                } else if (pl.isField(COMPARTMENT_ID)) {
                    compartmentId = pl.readString();
                    checkNotEmpty(COMPARTMENT_ID, compartmentId);
                } else if (pl.isField(DDL_STATEMENT)) {
                    ddlStatement = pl.readString();
                    checkNotEmpty(DDL_STATEMENT, ddlStatement);
                } else if (pl.isField(TABLE_LIMITS)) {
                    JsonObject jo = pl.readObject();
                    if (jo != null) {
                        tableLimits = parseTableLimits(jo);
                    }
                } else if (pl.isField(IS_AUTO_RECLAIMABLE))  {
                    isAutoReclaimable = pl.readBool();
                } else if (pl.isField(FREE_FORM_TAGS)) {
                    JsonObject jo = pl.readObject();
                    if (jo != null) {
                        freeformTags = parseFreeformTags(jo);
                    }
                } else if (pl.isField(DEFINED_TAGS)) {
                    JsonObject jo = pl.readObject();
                    if (jo != null) {
                        definedTags = parseDefinedTags(jo);
                    }
                } else {
                    handleUnknownField(CREATE_TABLE_DETAILS, pl);
                }
            }

            checkNotNull(NAME, name);
            checkNotNull(COMPARTMENT_ID, compartmentId);
            checkNotNull(DDL_STATEMENT, ddlStatement);
            if (TableUtils.isChildTable(name)) {
                if (tableLimits != null) {
                    throw new IllegalArgumentException(
                        "Can't set table limits on a child table");
                }
            } else {
                checkNotNull(TABLE_LIMITS, tableLimits);
            }
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Invalid payload for put request: " + ioe.getMessage());
        } finally {
            if (pl != null) {
                pl.close();
            }
        }

        actx.setCompartmentId(compartmentId);

        handleTableOp(rc, response, actx, name,
                      ddlStatement, tableLimits, isAutoReclaimable,
                      freeformTags, definedTags, null /* matchTag */,
                      retryToken, 0 /* timeOutMs*/,
                      apiVersion);
    }

    private TableLimits parseTableLimits(JsonObject jo) throws IOException {
        Integer maxReadUnits = 0;
        Integer maxWriteUnits = 0;
        Integer maxStorageInGBs = 0;
        String modeStr = null;

        while (jo.hasNext()) {
            if (jo.isField(MAX_READ_UNITS)) {
                maxReadUnits = jo.readInt();
            } else if (jo.isField(MAX_WRITE_UNITS)) {
                maxWriteUnits = jo.readInt();
            } else if (jo.isField(MAX_STORAGE_IN_GBS)) {
                maxStorageInGBs = jo.readInt();
            } else if (jo.isField(CAPACITY_MODE)) {
                modeStr = jo.readString();
            } else {
                throw new IllegalArgumentException(
                    "Unexpected field of TableLimits: " +
                    jo.getCurrentField());
            }
        }
        TableLimits tableLimits = null;
        if (modeStr == null || modeStr.equalsIgnoreCase(PROVISIONED)) {
            tableLimits = new TableLimits(maxReadUnits,
                                          maxWriteUnits,
                                          maxStorageInGBs);
        } else if (modeStr.equalsIgnoreCase(ON_DEMAND)) {
            tableLimits = new TableLimits(maxStorageInGBs);
        } else {
            throw new IllegalArgumentException(
                "Invalid TableLimits, " + modeStr +
                " is unknown capacity mode");
        }

        validateTableLimits(tableLimits);
        return tableLimits;
    }

    /**
     * Alter table
     */
    private void handleUpdateTable(FullHttpResponse response,
                                   RequestContext rc,
                                   AccessContext actx,
                                   String apiVersion) {

        /* Header parameter */
        byte[] matchETag = readIfMatch(rc.restParams);
        /* Path parameter */
        String tableNameOrId = readTableNameOrId(rc.restParams);

        /* Payload */
        String compartmentId = null;
        String ddlStatement = null;
        TableLimits tableLimits = null;
        Map<String, String> freeformTags = null;
        Map<String, Map<String, Object>> definedTags = null;

        checkNotNull(UPDATE_TABLE_DETAILS, rc.restParams.getPayload());

        JsonPayload pl = null;
        try {
            pl = rc.restParams.parsePayload();
            while (pl.hasNext()) {
                if (pl.isField(COMPARTMENT_ID)) {
                    compartmentId = pl.readString();
                    checkNotEmpty(COMPARTMENT_ID, compartmentId);
                } else if (pl.isField(DDL_STATEMENT)) {
                    ddlStatement = pl.readString();
                    checkNotEmpty(DDL_STATEMENT, ddlStatement);
                } else if (pl.isField(TABLE_LIMITS)) {
                    JsonObject jo = pl.readObject();
                    if (jo != null) {
                        tableLimits = parseTableLimits(jo);
                    }
                } else if (pl.isField(FREE_FORM_TAGS)) {
                    JsonObject jo = pl.readObject();
                    if (jo != null) {
                        freeformTags = parseFreeformTags(jo);
                    }
                } else if (pl.isField(DEFINED_TAGS)) {
                    JsonObject jo = pl.readObject();
                    if (jo != null) {
                        definedTags = parseDefinedTags(jo);
                    }
                } else {
                    handleUnknownField(UPDATE_TABLE_DETAILS, pl);
                }
            }

            boolean hasTags = (freeformTags != null || definedTags != null);
            if (ddlStatement != null) {
                if (tableLimits != null) {
                    throw new IllegalArgumentException("Only one of " +
                        "ddlStatement or tableLimits can be specified");
                } else {
                    if (hasTags) {
                        throw new IllegalArgumentException("Only one of " +
                            "ddlStatement or tags can be specified");
                    }
                }
            } else {
                if (tableLimits != null) {
                    if (hasTags) {
                        throw new IllegalArgumentException("Only one of " +
                            "tableLimits or tags can be specified");
                    }
                } else {
                    if (!hasTags) {
                        throw new IllegalArgumentException(
                            "The ddlStatement or tableLimits or tags should " +
                            "not be null");
                    }
                }
            }
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Invalid payload for put request: " + ioe.getMessage());
        } finally {
            if (pl != null) {
                pl.close();
            }
        }

        if (compartmentId != null) {
            actx.setCompartmentId(compartmentId);
        }
        handleTableOp(rc, response, actx, tableNameOrId,
                      ddlStatement, tableLimits, false /* isAutoReclaimable */,
                      freeformTags, definedTags, matchETag,
                      null /* retryToken */, 0 /* timeoutMs */,
                      apiVersion);
    }

    /**
     * Drop table
     */
    private void handleDropTable(FullHttpResponse response,
                                 RequestContext rc,
                                 AccessContext actx,
                                 String apiVersion) {

        /* Header parameter */
        byte[] matchETag = readIfMatch(rc.restParams);
        /* Path parameter */
        String tableNameOrId = readTableNameOrId(rc.restParams);
        /* Query parameter */
        String compartmentId = readCompartmentId(rc.restParams);
        boolean isIfExists = readIfExists(rc.restParams);

        /* Build drop table ddl */
        String ddl = buildDropTableDdl(actx, tableNameOrId, isIfExists);

        if (compartmentId != null) {
            actx.setCompartmentId(compartmentId);
        }
        handleTableOp(rc, response, actx, tableNameOrId,
                      ddl, null /* tableLimits */,
                      false /* isAutoReclaimable */,null /* freeformTags */,
                      null /* predefinedTags */, matchETag,
                      null /* retryToken */, 0 /* timeoutMs */,
                      apiVersion);
    }

    /**
     * Changes compartment
     */
    private void handleChangeCompartment(FullHttpResponse response,
                                         RequestContext rc,
                                         AccessContext actx,
                                         String apiVersion) {
        /* Header parameter */
        byte[] matchETag = readIfMatch(rc.restParams);
        String retryToken = readOpcRetryToken(rc.restParams);

        /* Path parameter */
        String tableNameOrId = readTableNameOrId(rc.restParams);

        /* Parse payload */
        String compartmentId = null;
        String newCompartmentId = null;

        checkNotNull(CHANGE_TABLE_COMPARTMENT_DETAILS,
                     rc.restParams.getPayload());

        JsonPayload pl = null;
        try {
            pl = rc.restParams.parsePayload();
            while (pl.hasNext()) {
                if (pl.isField(FROM_COMPARTMENT_ID)) {
                    compartmentId = pl.readString();
                    checkNotEmpty(FROM_COMPARTMENT_ID, compartmentId);
                } else if (pl.isField(TO_COMPARTMENT_ID)) {
                    newCompartmentId = pl.readString();
                    checkNotEmpty(TO_COMPARTMENT_ID, newCompartmentId);
                } else {
                    handleUnknownField(CHANGE_TABLE_COMPARTMENT_DETAILS, pl);
                }
            }
            checkNotNull(TO_COMPARTMENT_ID, newCompartmentId);
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Invalid payload for change table compartment request: " +
                ioe.getMessage());
        } finally {
            if (pl != null) {
                pl.close();
            }
        }

        /* Set compartmentId and dest compartmentId to AccessContext */
        if (compartmentId != null) {
            actx.setCompartmentId(compartmentId);
        }
        actx.moveCompartment(newCompartmentId);

        /* Execute moving compartment */
        GetTableResponse resp = null;
        int errorCode = NO_ERROR;
        try {
            resp = handleChangeCompartment(rc.request,
                                           actx,
                                           tableNameOrId,
                                           matchETag,
                                           retryToken,
                                           rc.lc,
                                           apiVersion);
        } catch (RequestException re) {
            errorCode = re.getErrorCode();
            throw re;
        } catch (Throwable t) {
            errorCode = UNKNOWN_ERROR;
            throw t;
        } finally {
            TableInfo info = (resp == null) ? null : resp.getTableInfo();
            auditOperation(rc, info, errorCode);
        }

        if (!resp.getSuccess()) {
            throw new RequestException(resp.getErrorCode(),
                                       resp.getErrorString());
        }

        String workRequestId = null;
        TableInfo info = resp.getTableInfo();
        if (info != null) {
            workRequestId = tm.getWorkRequestId(info, rc.opCode);
        }

        /* build response */
        buildWorkRequestIdResponse(response, workRequestId);
        markTMOpSucceeded(rc, 1 /* TM operation count*/);
    }

    /**
     * Get table
     */
    private void handleGetTable(FullHttpResponse response,
                                RequestContext rc,
                                AccessContext actx,
                                String apiVersion) {

        /* Path parameter */
        String tableNameOrId = readTableNameOrId(rc.restParams);

        /* Get table */
        String tableName = actx.getOriginalTableName(tableNameOrId);
        GetTableResponse resp = TableUtils.getTable(actx, tableName,
                                                    null /* operationId */,
                                                    tm,
                                                    false /* internal */,
                                                    rc.lc);
        if (!resp.getSuccess()) {
            throw new RequestException(resp.getErrorCode(),
                                       resp.getErrorString());
        }

        TableInfo info = resp.getTableInfo();
        if (logger.isLoggable(Level.FINE, rc.lc)) {
            logger.fine("GetTable: namespace " + actx.getNamespace() +
                        ", table " + tableNameOrId + ", state " +
                        info.getStateEnum(), rc.lc);
        }

        String[] tagJsons = null;
        if (info.getTags() != null) {
            tagJsons = actx.getExistingTags(info.getTags());
        }

        String tableInfo = buildTable(info, tagJsons, tm);
        buildTaggedResponse(response, tableInfo, info.getETag());

        markTMOpSucceeded(rc, 1 /* TM operation count*/);
    }

    /**
     * List tables
     */
    private void handleListTables(FullHttpResponse response,
                                  RequestContext rc,
                                  AccessContext actx,
                                  String apiVersion) {

        /* Query parameter */
        String name = readName(rc.restParams);
        int limit = readLimit(rc.restParams);
        String page = readPage(rc.restParams);
        int startIndex = (page == null) ? 0 : parseStartIndexPageToken(page);
        SortBy sortBy = readSortBy(rc.restParams);
        SortOrder sortOrder = readSortOrder(rc.restParams);
        LifecycleState state = readlifeCycleState(rc.restParams);

        /* List TableInfos */
        limit = getLimit(limit);
        ListTableInfoResponse resp =
            TableUtils.listTableInfos(actx, name,
                                      ((state != null)? state.name() : null),
                                      ((sortBy != null)? sortBy.name() : null),
                                      (sortOrder == SortOrder.ASC),
                                      startIndex,
                                      limit,
                                      tm,
                                      rc.lc);

        if (!resp.getSuccess()) {
            throw new RequestException(resp.getErrorCode(),
                                       resp.getErrorString());
        }

        /* build response */
        String tc = buildTableCollection(resp, actx, tm, logger);
        boolean hasMore = (resp.getTableInfos().length == limit);
        String nextPageToken = hasMore?
            generateLastIndexPageToken(resp.getLastIndexReturned()) : null;
        buildPaginatedResponse(response, tc, nextPageToken);

        markTMOpSucceeded(rc, 1 /* TM operation count*/);
    }

    /**
     * Get table usage
     */
    private void handleGetTableUsage(FullHttpResponse response,
                                     RequestContext rc,
                                     AccessContext actx,
                                     String apiVersion) {

        /* Path parameter */
        String tableNameOrId = readTableNameOrId(rc.restParams);
        /* Query parameter */
        long startRange = readTimeStart(rc.restParams);
        long endRange = readTimeEnd(rc.restParams);
        validateUsageTimeRange(startRange, endRange, TIME_START, TIME_END);

        /* Use default limit if not specified */
        int limit = getLimit(readLimit(rc.restParams));
        String page = readPage(rc.restParams);
        int startIndex = (page != null)? parseStartIndexPageToken(page) : 0;

        /* Get table usage */
        TableUsageResponse resp = TableUtils.getTableUsage(actx, tableNameOrId,
                                    startRange, endRange, startIndex,
                                    limit, tm, rc.lc);
        if (!resp.getSuccess()) {
            throw new RequestException(resp.getErrorCode(),
                                       resp.getErrorString());
        }

        /* Build response */
        String info = buildTableUsageCollection(resp.getTableUsage());
        boolean hasMore = (resp.getTableUsage().length == limit);

        String nextPageToken = hasMore?
            generateLastIndexPageToken(resp.getLastIndexReturned()) : null;
        buildPaginatedResponse(response, info, nextPageToken);
        markTMOpSucceeded(rc, 1 /* TM operation count*/);
    }

    /**
     * Parses the timestamp in string format to milliseconds since epoch.
     */
    static long parseTimestamp(String timestampStr) {
        TemporalAccessor ta;
        try {
            ta = timestampFormatter.parse(timestampStr);
        } catch (DateTimeParseException dtpe) {
            String msg = "Date is not in RFC 3339 format: " + dtpe.getMessage();
            throw new RequestException(ILLEGAL_ARGUMENT, msg);
        }
        Instant instant;
        if (ta.isSupported(ChronoField.HOUR_OF_DAY)) {
            instant = Instant.from(ta);
        } else {
            instant = LocalDate.from(ta).atStartOfDay(UTCZone).toInstant();
        }
        return instant.toEpochMilli();
    }

    /**
     * Create index
     */
    private void handleCreateIndex(FullHttpResponse response,
                                   RequestContext rc,
                                   AccessContext actx,
                                   String apiVersion) {

        /* Path parameter */
        String tableNameOrId = readTableNameOrId(rc.restParams);
        /* Header parameter */
        String retryToken = readOpcRetryToken(rc.restParams);

        /* Payload */
        String indexName = null;
        String compartmentId = null;
        List<String> indexKeys = null;
        boolean isIfNotExists = DEFAULT_IF_NOT_EXISTS;

        checkNotNull(CREATE_INDEX_DETAILS, rc.restParams.getPayload());

        JsonPayload pl = null;
        try {
            pl = rc.restParams.parsePayload();
            while (pl.hasNext()) {
                if (pl.isField(NAME)) {
                    indexName = pl.readString();
                    checkNotEmpty(NAME, indexName);
                } else if (pl.isField(COMPARTMENT_ID)) {
                    compartmentId = pl.readString();
                    checkNotEmpty(COMPARTMENT_ID, compartmentId);
                } else if (pl.isField(KEYS)) {
                    JsonArray ja = pl.readArray();
                    if (ja != null) {
                        indexKeys = parseIndexKeys(ja);
                    }
                } else if (pl.isField(IS_IF_NOT_EXISTS)) {
                    isIfNotExists = pl.readBool();
                } else {
                    handleUnknownField(CREATE_INDEX_DETAILS, pl);
                }
            }

            checkNotNull(NAME, indexName);
            checkNotNull(KEYS, indexKeys);
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Invalid payload for put request: " + ioe.getMessage());
        } finally {
            if (pl != null) {
                pl.close();
            }
        }

        /*
         * Build create index ddl.
         *
         * The 2nd access check is in following handleTableOp(), the original
         * table name is not available in AccessContext at this time. So if
         * tableNameOrId is table OCID, use its internal format when compose
         * the index ddl, because the OCID may contain invalid characters like
         * "-"  table name which may result in error when pre-compile the DDL
         * using PrepareCB in handleTableOp().
         *
         * The table name in create index ddl will be replaced with actual KV
         * table name in SC, it is OK to use internal format OCID in ddl.
         *
         * AccessContext.mapToInternalId() only converts external OCID to
         * internal format, do nothing and return tableNameOrId if it is not
         * external OCID.
         */
        String tableName = actx.mapToInternalId(tableNameOrId);
        String ddl = buildCreateIndexDdl(tableName, indexName,
                                         indexKeys, isIfNotExists);
        if (compartmentId != null) {
            actx.setCompartmentId(compartmentId);
        }
        handleTableOp(rc, response, actx, tableNameOrId,
                      ddl, null /* limits */, false /* isAutoReclaimable */,
                      null /* freeformTags */, null /* definedTag */,
                      null /* matchETag */, retryToken, 0 /* timeoutMs */,
                      apiVersion);
    }

    private List<String> parseIndexKeys(JsonArray keys) throws IOException {
        List<String> indexKeys = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();
        while (keys.hasNext()) {
            JsonObject jo = keys.readObject();
            String columnName = null;
            String jsonPath = null;
            String jsonFieldType = null;
            while (jo.hasNext()) {
                if (jo.isField(COLUMN_NAME)) {
                    columnName = jo.readString();
                } else if (jo.isField(JSON_PATH)) {
                    jsonPath = jo.readString();
                } else if (jo.isField(JSON_FIELD_TYPE)) {
                    jsonFieldType = jo.readString();
                } else {
                    throw new IllegalArgumentException(
                        "Unknown field of IndexKey: " + jo.getCurrentField());
                }
            }

            checkNotNull("IndexKey." + COLUMN_NAME, columnName);
            if (jsonPath != null) {
                if (jsonFieldType == null) {
                    throw new IllegalArgumentException("Invalid IndexKey, " +
                        "JsonFieldType should not be null if JsonPath is " +
                        "specified: " + columnName);
                }
            } else {
                if (jsonFieldType != null) {
                    throw new IllegalArgumentException("Invalid IndexKey, " +
                       "JsonFieldType should not be specified if JsonPath is " +
                       "not specified: " + columnName);
                }
            }

            sb.setLength(0);
            sb.append(columnName);
            if (jsonPath != null) {
                sb.append(".").append(jsonPath)
                  .append(" as ").append(jsonFieldType);
            }

            indexKeys.add(sb.toString());
        }

        if (indexKeys.isEmpty()) {
            throw new IllegalArgumentException("Invalid " + KEYS +
                ", it should not be empty");
        }
        return indexKeys;
    }

    /**
     * Drop index
     */
    private void handleDropIndex(FullHttpResponse response,
                                 RequestContext rc,
                                 AccessContext actx,
                                 String apiVersion) {

        /* Header parameter */
        byte[] matchETag = readIfMatch(rc.restParams);
        /* Path parameter */
        String tableNameOrId = readTableNameOrId(rc.restParams);
        String indexName = readIndexName(rc.restParams);
        /* Query parameter */
        String compartmentId = readCompartmentId(rc.restParams);
        boolean isIfExists = readIfExists(rc.restParams);

        /*
         * Build drop index ddl
         *
         * The 2nd access check is in following handleTableOp(), the original
         * table name is not available in AccessContext at this time. So if
         * tableNameOrId is table OCID, use its internal format when compose
         * the index ddl, because the OCID may contain invalid characters like
         * "-"  table name which may result in error when pre-compile the DDL
         * using PrepareCB in handleTableOp().
         *
         * The table name in drop index ddl will be replaced with actual KV
         * table name in SC, it is OK to use internal format OCID in ddl.
         *
         * AccessContext.mapToInternalId() only converts external OCID to
         * internal format, do nothing and return tableNameOrId if it is not
         * external OCID.
         */
        String tableName = actx.mapToInternalId(tableNameOrId);
        String ddl = buildDropIndexDdl(tableName, indexName, isIfExists);

        if (compartmentId != null) {
            actx.setCompartmentId(compartmentId);
        }
        handleTableOp(rc, response, actx, tableNameOrId,
                      ddl, null /* limits */, false /* isAutoReclaimable */,
                      null /* freeformTags */, null /* definedTags */,
                      matchETag, null /* retryToken */, 0 /* timeoutMs */,
                      apiVersion);
    }

    /**
     * Get index
     */
    private void handleGetIndex(FullHttpResponse response,
                                RequestContext rc,
                                AccessContext actx,
                                String apiVersion) {

        /* Path parameter */
        String tableNameOrId = readTableNameOrId(rc.restParams);
        String indexName = readIndexName(rc.restParams);

        /* Get index info */
        IndexResponse resp = TableUtils.getIndexInfo(actx, tableNameOrId,
                                                     indexName, null, null,
                                                     null, true, 0, 0,
                                                     tm, rc.lc);
        if (!resp.getSuccess()) {
            throw new RequestException(resp.getErrorCode(),
                                       resp.getErrorString());
        }

        IndexInfo indexInfo = resp.getIndexInfo()[0];
        /*
         * TODO: If IndexKey in rest spec is simplified to remove "jsonPath",
         * then below code to get KV Table can be removed, details see below:
         *
         * In current rest spec, the IndexKey returned
         * {
         *    "columnName": <field-path> or <json-field-path> for JSON field
         *    "jsonPath": null or <internal-json-path> for JSON field
         *    "jsonFieldType": null or <field-as-type> for JSON field.
         * }
         *
         * The IndexInfo.IndexField.path returned from TM is full index field
         * path, that is <json-field-path>.<internal-json-path>, so to extract
         * <internal-json-path> from the full index path, the KV Table is used
         * for this.
         *
         * See JsonProtocol.addIndexWithJsonField() on how to extract
         * <json-field-path> and <internal-json-path> from full index field
         * path.
         */
        TableImpl table = null;
        if (isIndexHasJsonField(indexInfo)) {
            String tableName = getMapTableName(actx, rc.opCode, tableNameOrId);
            TableEntry te = getTableEntry(actx, tableName, rc.lc);
            table = (TableImpl)te.getTable();
            checkRequestSizeLimit(rc.request, te);
        }

        String info = buildIndex(actx.getCompartmentId(),
                                 actx.getOriginalTableName(tableNameOrId),
                                 actx.getTableIdExternal(tableNameOrId),
                                 indexInfo,
                                 table);

        buildTaggedResponse(response, info, indexInfo.getETag());

        markTMOpSucceeded(rc, 1 /* TM operation count*/);
    }

    /**
     * Get indexes
     */
    private void handleGetIndexes(FullHttpResponse response,
                                  RequestContext rc,
                                  AccessContext actx,
                                  String apiVersion) {

        /* Path parameter */
        String tableNameOrId = readTableNameOrId(rc.restParams);
        /* Query parameter */
        String namePattern = readName(rc.restParams);
        int limit = readLimit(rc.restParams);
        String page = readPage(rc.restParams);
        SortOrder sortOrder = readSortOrder(rc.restParams);
        SortBy sortBy = readSortBy(rc.restParams);
        LifecycleState state = readlifeCycleState(rc.restParams);

        int startIndex = (page == null) ? 0 : parseStartIndexPageToken(page);
        limit = getLimit(limit);

        IndexResponse resp;
        resp = TableUtils.getIndexInfo(actx, tableNameOrId,
                                       null /* indexName */, namePattern,
                                       ((state != null)? state.name() : null),
                                       ((sortBy != null)? sortBy.name() : null),
                                       (sortOrder == SortOrder.ASC),
                                       startIndex,
                                       limit,
                                       tm, rc.lc);
        if (!resp.getSuccess()) {
            throw new RequestException(resp.getErrorCode(),
                                       resp.getErrorString());
        }

        IndexInfo[] indexes = resp.getIndexInfo();
        /*
         * TODO: If IndexKey in rest spec is simplified to remove "jsonPath",
         * then below code to get KV Table can be removed, details see below:
         *
         * In current rest spec, the IndexKey returned
         * {
         *    "columnName": <field-path> or <json-field-path> for JSON field
         *    "jsonPath": null or <internal-json-path> for JSON field
         *    "jsonFieldType": null or <field-as-type> for JSON field.
         * }
         *
         * The IndexInfo.IndexField.path returned from TM is full index field
         * path, that is <json-field-path>.<internal-json-path>, so to extract
         * <internal-json-path> from the full index path, the KV Table is used
         * for this.
         *
         * See JsonProtocol.addIndexWithJsonField() on how to extract
         * <json-field-path> and <internal-json-path> from full index field
         * path.
         */
        TableImpl table = null;
        if (hasJsonField(indexes)) {
            String tableName = getMapTableName(actx, rc.opCode, tableNameOrId);
            TableEntry te = getTableEntry(actx, tableName, rc.lc);
            table = (TableImpl)te.getTable();
            checkRequestSizeLimit(rc.request, te);
        }

        String info = buildIndexCollection(indexes, table);
        boolean hasMore = (resp.getIndexInfo().length == limit);
        String nextPageToken = hasMore?
            generateLastIndexPageToken(resp.getLastIndexReturned()) : null;
        buildPaginatedResponse(response, info, nextPageToken);

        markTMOpSucceeded(rc, 1 /* TM operation count*/);
    }

    private boolean hasJsonField(IndexInfo[] indexInfos) {
        for (IndexInfo indexInfo : indexInfos) {
            if (isIndexHasJsonField(indexInfo)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Put
     */
    private void handlePut(FullHttpResponse response,
                           RequestContext rc,
                           AccessContext actx,
                           String apiVersion) {

        /* Header parameter */
        byte[] ifMatch = readIfMatch(rc.restParams);
        /* Path parameter */
        String tableNameOrId = readTableNameOrId(rc.restParams);

        /* Payload */
        String compartmentId = null;
        int identityCacheSize = 0;
        boolean isExactMatch = false;
        boolean isGetReturnRow = false;
        boolean isTtlUseTableDefault = false;
        PutOption option = null;
        int timeoutInMs = 0;
        int ttlInDays = 0;
        String value = null;

        /* Parse payload */
        checkNotNull(UPDATE_ROW_DETAILS, rc.restParams.getPayload());

        JsonPayload pl = null;
        try {
            pl = rc.restParams.parsePayload();
            while (pl.hasNext()) {
                if (pl.isField(COMPARTMENT_ID)) {
                    compartmentId = pl.readString();
                    checkNotEmpty(COMPARTMENT_ID, compartmentId);
                } else if (pl.isField(IDENTITY_CACHE_SIZE)) {
                    identityCacheSize = pl.readInt();
                    checkNonNegativeInt(IDENTITY_CACHE_SIZE, identityCacheSize);
                } else if (pl.isField(IS_EXACT_MATCH)) {
                    isExactMatch = pl.readBool();
                } else if (pl.isField(IS_GET_RETURN_ROW)) {
                    isGetReturnRow = pl.readBool();
                } else if (pl.isField(IS_TTL_USE_TABLE_DEFAULT)) {
                    isTtlUseTableDefault = pl.readBool();
                } else if (pl.isField(OPTION)) {
                    String str = pl.readString();
                    option = PutOption.valueOf(str.toUpperCase());
                } else if (pl.isField(TIMEOUT_IN_MS)) {
                    timeoutInMs = pl.readInt();
                    checkNonNegativeInt(TIMEOUT_IN_MS, timeoutInMs);
                } else if (pl.isField(TTL)) {
                    ttlInDays = pl.readInt();
                    checkNonNegativeInt(TTL, ttlInDays);
                } else if (pl.isField(VALUE)) {
                    value = pl.readValueAsJson();
                } else {
                    handleUnknownField(UPDATE_ROW_DETAILS, pl);
                }
            }
            validateValue(value);
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Invalid payload for put request: " + ioe.getMessage());
        } finally {
            if (pl != null) {
                pl.close();
            }
        }

        /*
         * Check access again since compartmentId is available until now
         */
        if (compartmentId != null) {
            actx.setCompartmentId(compartmentId);
        }
        actx = checkAccess(rc.request, rc.opCode, actx, tableNameOrId,
                           this, rc.lc);
        String tableName = getMapTableName(actx, rc.opCode, tableNameOrId);

        TableEntry te = null;
        try {
            te = getTableEntry(actx, tableName, rc.lc);
        } catch (RequestException re) {

            /* try again after reset of mappings if error is TABLE_NOT_FOUND */
            if (re.getErrorCode() != TABLE_NOT_FOUND) {
                throw re;
            }
            actx.resetTableNameMapping();
            actx = checkAccess(rc.request, rc.opCode, actx,
                               tableNameOrId, this, rc.lc);
            tableName = getMapTableName(actx, rc.opCode, tableNameOrId);
            te = getTableEntry(actx, tableName, rc.lc);
        }
        checkRequestSizeLimit(rc.request, te);
        TableImpl table = (TableImpl)te.getTable();

        Version matchVersion = null;
        OpCode putOp = null;
        if (ifMatch != null) {
            matchVersion = Version.fromByteArray(ifMatch);
            putOp = OpCode.PUT_IF_VERSION;
        } else {
            if (option != null) {
                switch (option) {
                case IF_ABSENT:
                    putOp = OpCode.PUT_IF_ABSENT;
                    break;
                case IF_PRESENT:
                    putOp = OpCode.PUT_IF_PRESENT;
                    break;
                }
            } else {
                putOp = OpCode.PUT;
            }
        }

        /* create row */
        RestRow row = new RestRow(table,
                                  te.getRequestLimits().getPrimaryKeySizeLimit(),
                                  te.getRequestLimits().getRowSizeLimit());

        RestRow.createFromJson(row, new StringReader(value),
                               isExactMatch, false /* addMissingFields */);

        /* set ttl */
        TimeToLive ttl = isTtlUseTableDefault ? table.getDefaultTTL() :
                                                getTimeToLive(ttlInDays);
        if (ttl != null) {
            row.setTTL(ttl);
        }
        boolean updateTTL = (row.getTTL() != null);

        /* create ReturnRow */
        ReturnRow returnRow = null;
        if (isGetReturnRow && putOp != OpCode.PUT &&
            putOp != OpCode.PUT_IF_PRESENT) {
            returnRow = table.createReturnRow(ReturnRow.Choice.ALL);
        }

        TableAPIImpl tableApi = te.getTableAPI();
        WriteOptions options = createWriteOptions(getTimeoutMs(timeoutInMs),
                                                  null, /* TODO: Durability */
                                                  updateTTL,
                                                  identityCacheSize,
                                                  actx.getAuthString(),
                                                  false /* doTombstone */,
                                                  rc.lc);

        /*
         * Some of this could be shared with code in DataService.java but
         * not at this time
         */
        final GeneratedValueInfo genInfo =
            TableAPIImpl.makeGenInfo(table, options);
        KVStoreImpl store = tableApi.getStore();
        Key kvKey = table.createKeyInternal(row, false, store, genInfo);
        Value kvValue = table.createValueInternal(row, store, genInfo);
        Request rq = tableApi.makePutRequest(mapPutOpCode(putOp),
                                             kvKey,
                                             kvValue,
                                             table,
                                             returnRow,
                                             row.getTTL(),
                                             options,
                                             matchVersion);
        Result result = store.executeRequest(rq);
        if (genInfo != null) {
            result.setGeneratedValue(genInfo.getGeneratedValue());
        }

        if (matchVersion != null && result.getNewVersion() == null) {
            throw new RequestException(ETAG_MISMATCH, "The version does not " +
                "match the existing row or the row is not present.");
        }

        if (returnRow != null && result.getPreviousValue() != null) {
            tableApi.initReturnRowFromResult(returnRow, row, result, null);
        }

        /* Build response */
        String updRet = buildUpdateRowResult(result, returnRow);
        byte[] etag = (result.getNewVersion() != null) ?
                       result.getNewVersion().toByteArray() : null;
        buildTaggedResponse(response, updRet, etag);

        rc.setThroughput(result);
        markDataOpSucceeded(rc, result.getNumRecords(),
                            result.getReadKB(), result.getWriteKB());
    }

    /**
     * Get
     */
    private void handleGet(FullHttpResponse response,
                           RequestContext rc,
                           AccessContext actx,
                           String apiVersion) {

        /* Path parameter */
        String tableNameOrId = readTableNameOrId(rc.restParams);
        /* Query string */
        List<String> key = readKey(rc.restParams);
        Consistency consistency = getConsistency(rc.restParams);
        int timeoutMs = readTimeoutInMs(rc.restParams);

        String tableName = getMapTableName(actx, rc.opCode, tableNameOrId);
        TableEntry te = getTableEntry(actx, tableName, rc.lc);
        checkRequestSizeLimit(rc.request, te);
        TableImpl table = (TableImpl)te.getTable();
        TableAPIImpl tableApi = te.getTableAPI();

        PrimaryKeyImpl pkey = createPrimaryKey(table, key);
        ReadOptions options = createReadOptions(consistency,
                                                getTimeoutMs(timeoutMs),
                                                actx.getAuthString(), rc.lc);

        Result result = doGet(tableApi, pkey, options);

        /* Read row */
        oracle.kv.table.Row row = tableApi.processGetResult(result, pkey);

        /* Build response */
        String info = null;
        byte[] etag = null;
        info = buildRow(result, row);
        if (row != null) {
            etag = row.getVersion().toByteArray();
        }
        buildTaggedResponse(response, info, etag);

        rc.setThroughput(result);
        markDataOpSucceeded(rc,
                            result.getNumRecords(), result.getReadKB(),
                            result.getWriteKB());
    }

    /**
     * Delete
     */
    private void handleDelete(FullHttpResponse response,
                              RequestContext rc,
                              AccessContext actx,
                              String apiVersion) {

        /* Header parameter */
        byte[] ifMatch = readIfMatch(rc.restParams);
        /* Path parameter */
        String tableNameOrId = readTableNameOrId(rc.restParams);
        /* Query parameter */
        List<String> key = readKey(rc.restParams);
        boolean isGetRetRow = readIsGetReturnRow(rc.restParams);
        int timeoutMs = readTimeoutInMs(rc.restParams);

        String tableName = getMapTableName(actx, rc.opCode, tableNameOrId);
        TableEntry te = getTableEntry(actx, tableName, rc.lc);
        checkRequestSizeLimit(rc.request, te);
        TableImpl table = (TableImpl)te.getTable();
        TableAPIImpl tableApi = te.getTableAPI();

        /* matchVersion */
        Version matchVersion = null;
        if (ifMatch != null) {
            matchVersion = Version.fromByteArray(ifMatch);
        }

        /*
         * ReturnRow is only set if the operation is conditional
         * and it is requested.
         */
        ReturnRow returnRow = null;
        if (isGetRetRow && matchVersion != null) {
            returnRow = table.createReturnRow(ReturnRow.Choice.ALL);
        }

        /* create primary key */
        PrimaryKeyImpl pkey = createPrimaryKey(table, key);
        WriteOptions options = createWriteOptions(timeoutMs,
                                                  null, /* TODO: Durability */
                                                  actx.getAuthString(),
                                                  doTombStone(te),
                                                  rc.lc);

        Result result = doDelete(tableApi, pkey,
                                 matchVersion, returnRow, options);
        if (matchVersion != null && !result.getSuccess()) {
            throw new RequestException(ETAG_MISMATCH, "The version does not " +
                "match the existing row or the row is not present.");
        }

        if (returnRow != null && result.getPreviousValue() != null) {
            tableApi.initReturnRowFromResult(returnRow, pkey, result, null);
        }

        /* Build response */
        String info = buildDeleteRowResult(result, returnRow);
        buildResponse(response, info);

        rc.setThroughput(result);
        markDataOpSucceeded(rc,
                            result.getNumRecords(), result.getReadKB(),
                            result.getWriteKB());
    }

    /**
     * Prepare
     */
    private void handlePrepare(FullHttpResponse response,
                               RequestContext rc,
                               AccessContext actx,
                               String apiVersion) {

        /* Query parameter */
        String compartmentId = readCompartmentId(rc.restParams,
                                                 true /* required */);
        String statement = readStatement(rc.restParams);
        boolean isGetQueryPlan = readIsGetQueryPlan(rc.restParams);

        PrepareCB cbInfo = getStatementInfo(actx, statement, true);

        String ns = (actx.getNamespace() != null ?
                     actx.getNamespace() : cbInfo.getNamespace());

        /*
         * Check access again since table name is available until now.
         */
        AccessContextHelper actxHelper;
        PrepareResult prepRet;

        actx.setCompartmentId(compartmentId);
        actxHelper = checkAccess(rc.request, rc.opCode, actx,
                                 cbInfo.getTableName(),
                                 cbInfo.getNotTargetTables(),
                                 this, rc.lc);

        prepRet = doPrepare(rc, actxHelper, cbInfo, ns, rc.opCode,
                            null /* ExecuteOptions */, statement, queryVersion,
                            rc.lc);

        String stmt;
        ByteBuf buf = Unpooled.buffer();
        try {
            ByteOutputStream bos = new ByteOutputStream(buf);
            serializePreparedQuery(cbInfo, prepRet.getPreparedStatement(), bos);

            byte[] bytes = new byte[buf.readableBytes()];
            System.arraycopy(buf.array(), 0, bytes, 0, buf.readableBytes());
            stmt = encodeBase64(bytes);
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Serialize prepared statement failed: " + ioe);
        } finally {
            buf.release(buf.refCnt());
        }

        String queryPlan = isGetQueryPlan ?
                prepRet.getPreparedStatement().getQueryPlan().display() : null;

        /* Build Response */
        int prepCost = MIN_QUERY_COST;
        String info = buildPreparedStatement(stmt, queryPlan, prepCost);
        buildResponse(response, info);

        rc.setThroughput(prepCost, 0);
        markDataOpSucceeded(rc,
                            0 /* result size */, prepCost /* read size */,
                            0 /* write size */);
    }

    /**
     * Summarize
     */
    private void handleSummarize(FullHttpResponse response,
                                 RequestContext rc,
                                 AccessContext actx,
                                 String apiVersion) {

        /* Query parameter */
        String compartmentId = readCompartmentId(rc.restParams,
                                                 true /* required */);
        String statement = readStatement(rc.restParams);

        TableUtils.PrepareCB cbInfo = null;
        String syntaxError = null;
        try {
            cbInfo = getStatementInfo(actx, statement, false);
        } catch (IllegalArgumentException iae) {
            syntaxError = iae.getMessage();
        }

        /*
         * Check access again since table name is available until now.
         * This check is for cloud only.
         */
        actx.setCompartmentId(compartmentId);
        String tableName = (cbInfo != null ? cbInfo.getTableName() : null);
        actx = checkAccess(rc.request, rc.opCode, actx, tableName, this, rc.lc);

        /* build response */
        String info = buildStatementSummary(cbInfo, syntaxError);
        buildResponse(response, info);

        markDataOpSucceeded(rc,
                            0 /* result size */,
                            0 /* read size */,
                            0 /* write size */);
    }

    /**
     * Query
     */
    private void handleQuery(FullHttpResponse response,
                             RequestContext rc,
                             AccessContext actx,
                             String apiVersion) {

        /* Query parameter */
        int limit = readLimit(rc.restParams);
        String page = readPage(rc.restParams);
        byte[] continuationKey = parseQueryPageToken(page);

        /* Payload */
        String compartmentId = null;
        String statement = null;
        boolean isPrepared = false;
        Consistency consistency = null;
        int maxReadKB = 0;
        String variables = null;
        int timeoutMs = 0;

        checkNotNull(QUERY_DETAILS, rc.restParams.getPayload());

        JsonPayload pl = null;
        try {
            pl = rc.restParams.parsePayload();
            while (pl.hasNext()) {
                if (pl.isField(COMPARTMENT_ID)) {
                    compartmentId = pl.readString();
                    checkNotEmpty(COMPARTMENT_ID, compartmentId);
                } else if (pl.isField(STATEMENT)) {
                    statement = pl.readString();
                    checkNotEmpty(STATEMENT, statement);
                } else if (pl.isField(IS_PREPARED)) {
                    isPrepared = pl.readBool();
                } else if (pl.isField(CONSISTENCY)) {
                    String str = pl.readString();
                    consistency = mapToKVConsistency(str);
                } else if (pl.isField(MAX_READ_IN_KBS)) {
                    maxReadKB = pl.readInt();
                    checkNonNegativeInt(MAX_READ_IN_KBS, maxReadKB);
                } else if (pl.isField(VARIABLES)) {
                    variables = pl.readValueAsJson();
                } else if (pl.isField(TIMEOUT_IN_MS)) {
                    timeoutMs = pl.readInt();
                    checkNonNegativeInt(TIMEOUT_IN_MS, timeoutMs);
                } else {
                    handleUnknownField(QUERY_DETAILS, pl);
                }
            }

            checkNotNull(COMPARTMENT_ID, compartmentId);
            checkNotNull(STATEMENT, statement);

        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Invalid payload for query request: " + ioe.getMessage());
        } finally {
            if (pl != null) {
                pl.close();
            }
        }

        /*
         * Check access again since table name and query operation
         * are available until now.
         */
        actx.setCompartmentId(compartmentId);

        QueryStatementResultImpl qres;
        qres = doQuery(rc, actx, statement,
                       isPrepared, getConsistency(consistency), maxReadKB,
                       variables, getTimeoutMs(timeoutMs), continuationKey,
                       getLimit(limit), rc.lc);

        /* build response */
        final JsonBuilder jb = JsonBuilder.create();
        int prepCost = (isPrepared ?  0 : MIN_QUERY_COST);
        int count = buildQueryResult(jb, qres, prepCost);
        String nextPage = generateQueryPageToken(qres.getContinuationKey());
        buildPaginatedResponse(response, jb.toString(), nextPage);

        rc.setThroughput(qres.getReadKB() + prepCost, qres.getWriteKB());
        markDataOpSucceeded(rc, count,
                            qres.getReadKB() + prepCost,
                            qres.getWriteKB());
    }

    @Override
    protected PreparedStatementImpl doPrepare(KVStoreImpl store,
                                              String statement,
                                              ExecuteOptions execOpts) {

        PreparedStatementImpl prep;
        prep = super.doPrepare(store, statement, execOpts);
        /* Limits to support simple query only (QUERY_V1) */
        if (!prep.isSimpleQuery()) {
            throw new IllegalArgumentException(
                "The driver or SDK being used does not support complex query.");
        }
        return prep;
    }

    private QueryStatementResultImpl doQuery(RequestContext rc,
                                             AccessContext actx,
                                             String statement,
                                             boolean isPrepared,
                                             Consistency consistency,
                                             int maxReadKB,
                                             String variablesJson,
                                             int timeoutMs,
                                             byte[] continuationKey,
                                             int limit,
                                             LogContext lc) {
        final byte traceLevel = DEFAULT_TRACE_LEVEL;

        TableEntry te = null;
        RequestLimits limits = null;
        PreparedStatementImpl prep;
        ExecuteOptions execOpts;
        KVStoreImpl store;
        Filter opFilter;
        AccessContextHelper actxHelper;

        execOpts = createExecuteOptions(actx.getNamespace(),
                                        consistency,
                                        timeoutMs,
                                        continuationKey,
                                        -1, // driverTopoSeqNum ????
                                        null, // virtualScan ????
                                        maxReadKB,
                                        0 /* maxWriteKB */,
                                        limit,
                                        null /* mathContext */,
                                        traceLevel,
                                        true, // logFileTracing
                                        null, // queryName
                                        0, // batchCounter
                                        queryVersion,
                                        0, /* maxServerMemory*/
                                        actx.getAuthString(),
                                        lc,
                                        null /* rowMetadata */);
        if (!isPrepared) {
            PrepareCB cbInfo = getStatementInfo(actx, statement, true);
            String ns = (actx.getNamespace() != null ?
                         actx.getNamespace() : cbInfo.getNamespace());
            OpCode opCode = TableUtils.getDMLOpCode(cbInfo.getOperation());
            /*
             * Determine the actual OpCode of query, run filter check with
             * operations-only rules to check if the op should be blocked or
             * not.
             */
            opFilter = getQueryFilter(cbInfo.getOperation());
            opFilter.filterRequest(opCode, null, null, null, lc);

            boolean isAbsolute = (consistency == Consistency.ABSOLUTE ||
                                  cbInfo.isUpdateOp());
            actxHelper = checkAccess(rc.request, opCode, actx,
                                     cbInfo.getTableName(),
                                     cbInfo.getNotTargetTables(),
                                     opFilter, lc);

            PrepareResult prepRet = doPrepare(rc, actxHelper, cbInfo, ns,
                                              opCode, execOpts, statement,
                                              queryVersion, lc);
            prep = prepRet.getPreparedStatement();
            store = prepRet.getStore();
            te = prepRet.getEntry();
            limits = te.getRequestLimits();
            checkRequestSizeLimit(rc.request, te);
            maxReadKB = adjustMaxReadKB(maxReadKB, isAbsolute, limits);
        } else  {

            PreparedStatementWrapper psw;
            byte[] prepStmt;
            try {
                prepStmt = decodeBase64(statement);
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException(
                    "Invalid serialized prepared statement: " +
                    ex.getMessage());
            }
            try {
                psw = deserializePreparedQuery(prepStmt);
            } catch (IOException ioe) {
                throw new IllegalArgumentException(
                    "Deserialize prep query failed: " + ioe.getMessage());
            }

            prep = psw.getPreparedStatement();
            OpCode opCode = (psw.isUpdateOp() ? OpCode.PUT : OpCode.GET);
            /*
             * Determine the actual OpCode of query, run filter check with
             * operations-only rules to check if the op should be blocked or
             * not.
             */
            opFilter = getQueryFilter(psw.getOperation());
            opFilter.filterRequest(opCode, null, null, null, lc);

            String tableName = psw.getTableName();
            String ns = (actx.getNamespace() != null ?
                         actx.getNamespace() : psw.getNamespace());

            actxHelper = checkAccess(rc.request, opCode, actx,
                                     psw.getTableName(),
                                     psw.getNotTargetTables(), opFilter, lc);

            actx = actxHelper.get(psw.getTableName());
            tableName = getMapTableName(actx, opCode, tableName);
            if (actx.getType() == Type.IAM) {
                execOpts.setPrepareCallback(new MapPrepareCB(actxHelper));
                ns = actx.getNamespace();
            }

            /*
             * Get the table and check its version against the version from
             * the PreparedStatement
             */
            te = getTableAndCheckVersions(prep.getTableVersion(),
                                          ns, tableName, lc);

            checkRequestSizeLimit(rc.request, te);

            store = te.getStore();
            limits = te.getRequestLimits();


            /* Adjust MaxReadKB in ExecuteOptions */
            maxReadKB = adjustMaxReadKB(maxReadKB,
                                        (consistency == Consistency.ABSOLUTE ||
                                         psw.isUpdateOp()),
                                        limits);
        }

        /*
         * More configuration to ExecuteOptions
         */
        execOpts.setMaxReadKB(maxReadKB);
        if (limits != null) {
            execOpts.setMaxPrimaryKeySize(limits.getPrimaryKeySizeLimit());
            execOpts.setMaxRowSize(limits.getRowSizeLimit());
        }
        execOpts.setDoTombstone(doTombStone(te));

        Map<String, FieldValue> varValues = null;
        if (variablesJson != null) {
            varValues = createExternalVaraibles(prep, variablesJson);
        }

        return doQuery(store, prep, varValues, execOpts, traceLevel, lc);
    }

    private HashMap<String, FieldValue>
        createExternalVaraibles(PreparedStatementImpl prep,
                                String variablesJson)  {

        final HashMap<String, FieldValue> variables = new HashMap<>();
        try (JsonPayload pl = new JsonPayload(variablesJson)) {
            while (pl.hasNext()) {
                String name = pl.getCurrentField();
                String val = pl.readValueAsJson();

                FieldDef def = prep.getExternalVarType(name);
                if (def == null) {
                    throw new RequestException(
                        ILLEGAL_ARGUMENT,
                        "The query doesn't contain the variable: " + name);
                }
                FieldValue value = null;
                try {
                    value = createFieldValue(def, val);
                } catch (IllegalArgumentException iae) {
                    throw new RequestException(
                        ILLEGAL_ARGUMENT,
                        "Failed to create Field Value for variable '" +
                        name + "': " + iae.getMessage());
                }
                variables.put(name, value);
            }
            return variables;
        } catch (IOException ioe) {
            throw new RequestException(ILLEGAL_ARGUMENT,
                "Deserialize variables JSON string failed: " +
                ioe.getMessage());
        }
    }

    private FieldValue createFieldValue(FieldDef def, String json) {
        /* Distinguished from the string "null", the JSON NULL has no quotes */
        if (json.equals("null")) {
            if (def.isJson()) {
                return NullJsonValueImpl.getInstance();
            } else {
                return NullValueImpl.getInstance();
            }
        }
        return FieldValueFactory.createValueFromJson(def, json);
    }

    /**
     * GetWorkRequest
     */
    private void handleGetWorkRequest(FullHttpResponse response,
                                      RequestContext rc,
                                      AccessContext actx,
                                      String apiVersion) {
        /* Path parameter */
        String workRequestId = readWorkRequestId(rc.restParams);

        GetWorkRequestResponse res = TableUtils.getWorkRequest(actx,
                                                               workRequestId,
                                                               tm, rc.lc);
        if (!res.getSuccess()) {
            throw new RequestException(res.getErrorCode(),
                                       res.getErrorString());
        }

        /* Build response */
        String workRequest = buildWorkRequest(res.getWorkRequest());
        buildResponse(response, workRequest);

        markTMOpSucceeded(rc, 1 /* TM operation count*/);
    }

    /**
     * List work requests
     */
    private void handleListWorkRequests(FullHttpResponse response,
                                        RequestContext rc,
                                        AccessContext actx,
                                        String apiVersion) {
        /* Query parameter */
        int limit = readLimit(rc.restParams);
        String page = readPage(rc.restParams);

        /* List WorkRequests */
        ListWorkRequestResponse resp = TableUtils.listWorkRequests(actx,
                                                                   page,
                                                                   limit,
                                                                   tm,
                                                                   rc.lc);
        if (!resp.getSuccess()) {
            throw new RequestException(resp.getErrorCode(),
                                       resp.getErrorString());
        }

        /* build response */
        WorkRequest[] requests = resp.getWorkRequests();
        buildPaginatedResponse(response,
                               buildWorkRequestCollection(requests),
                               resp.getNextPageToken());

        markTMOpSucceeded(rc, 1 /* TM operation count*/);
    }

    /**
     * List work request errors
     */
    private void handleListWorkRequestErrors(FullHttpResponse response,
                                             RequestContext rc,
                                             AccessContext actx,
                                             String apiVersion) {
        /* Path parameter */
        String workRequestId = readWorkRequestId(rc.restParams);
        GetWorkRequestResponse res = TableUtils.getWorkRequest(actx,
                                                               workRequestId,
                                                               tm, rc.lc);
        if (!res.getSuccess()) {
            throw new RequestException(res.getErrorCode(),
                                       res.getErrorString());
        }

        /* Build response */
        String info = buildWorkRequestErrors(res.getWorkRequest());
        buildResponse(response, info);
        markTMOpSucceeded(rc, 1 /* TM operation count*/);
    }

    /**
     * List work request logs
     */
    private void handleListWorkRequestLogs(FullHttpResponse response,
                                           RequestContext rc,
                                           AccessContext actx,
                                           String apiVersion) {

        /* Path parameter */
        String workRequestId = readWorkRequestId(rc.restParams);
        GetWorkRequestResponse res = TableUtils.getWorkRequest(actx,
                                                               workRequestId,
                                                               tm, rc.lc);
        if (!res.getSuccess()) {
            throw new RequestException(res.getErrorCode(),
                                       res.getErrorString());
        }

        /* Build response */
        String info = buildWorkRequestLogs(res.getWorkRequest());
        buildResponse(response, info);

        markTMOpSucceeded(rc, 1 /* TM operation count*/);
    }

    /**
     * Cancel a work request
     */
    private void handleCancelWorkRequest(FullHttpResponse response,
                                         RequestContext rc,
                                         AccessContext actx,
                                         String apiVersion) {

        /* Path parameter */
        String workRequestId = readWorkRequestId(rc.restParams);

        /*
         * TODO: Cancel-workRequest is not yet implemented, always return
         * CANNOT_CANCEL_WORK_REQUEST for now.
         */
        throw new RequestException(CANNOT_CANCEL_WORK_REQUEST,
            "The work request can not be cancelled: " + workRequestId);
    }

    /**
     * Add replica
     */
    protected void handleAddReplica(FullHttpResponse response,
                                    RequestContext rc,
                                    AccessContext actx,
                                    String apiVersion) {

        throw new RequestException(OPERATION_NOT_SUPPORTED,
                                   "AddReplica operation is not supported");
    }

    /**
     * Drop replica
     */
    protected void handleDropReplica(FullHttpResponse response,
                                     RequestContext rc,
                                     AccessContext actx,
                                     String apiVersion) {

        throw new RequestException(OPERATION_NOT_SUPPORTED,
                                   "DropReplica operation is not supported");
    }

    /**
     * Get configuration
     */
    protected void handleGetConfiguration(FullHttpResponse response,
                                          RequestContext rc,
                                          AccessContext actx,
                                          String apiVersion) {
        throw new RequestException(OPERATION_NOT_SUPPORTED,
                                   "GetConfiguration is not supported");
    }

    /**
     * UpdateConfigration
     */
    protected void handleUpdateConfiguration(FullHttpResponse response,
                                             RequestContext rc,
                                             AccessContext actx,
                                             String apiVersion) {
        throw new RequestException(OPERATION_NOT_SUPPORTED,
                                   "UpdateConfiguration is not supported");
    }

    /**
     * RemoveCmek
     */
    protected void handleRemoveKmsKey(FullHttpResponse response,
                                      RequestContext rc,
                                      AccessContext actx,
                                      String apiVersion) {
        throw new RequestException(OPERATION_NOT_SUPPORTED,
                                   "RemoveKmsKey is not supported");
    }

    private static byte[] parseQueryPageToken(String pageToken) {
        if (pageToken != null) {
            try {
                return decodeBase64(pageToken);
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("Invalid page: " +
                    ex.getMessage());
            }
        }
        return null;
    }

    private static String generateQueryPageToken(byte[] continuationKey) {
        if (continuationKey != null) {
            return encodeBase64(continuationKey);
        }
        return null;
    }

    private OperationType getOpType(OpCode opCode) {
        return opCodeMap.get(opCode);
    }

    private void handleTableOp(RequestContext rc,
                               FullHttpResponse response,
                               AccessContext actx,
                               String tableName,
                               String statement,
                               TableLimits limits,
                               boolean isAutoReclaimable,
                               Map<String, String> freeformTags,
                               Map<String, Map<String, Object>> definedTags,
                               byte[] matchETag,
                               String retryToken,
                               int timeoutMs,
                               String apiVersion) {

        handleTableOp(rc, response, actx, () -> {
            GetTableResponse resp = null;
            if (rc.opCode == OpCode.ALTER_TABLE) {
                if (limits != null) {
                    /* Update table limits */
                    resp = handleTableLimits(rc.request, actx, tableName, limits,
                                             matchETag, rc.lc, apiVersion);
                } else if (freeformTags != null || definedTags != null) {
                    /* Update table tags */
                    resp = handleTableTags(rc.request, actx, tableName,
                                           freeformTags, definedTags,
                                           matchETag, rc.lc, apiVersion);
                }
            }

            String tableOcid = null;
            /* Execute ddl statement: create table or alter table */
            if (statement != null) {
                logger.fine("TableOperation, statement: " + statement +
                            ", " + limits, rc.lc);
                PrepareCB callback = TableUtils.getCallbackInfo(actx,
                                                                statement,
                                                                tm);

                /* Check if the ddl statement is valid for the operation */
                if (!checkDdlOperation(rc.opCode, callback.getOperation())) {
                    throw new RequestException(ILLEGAL_ARGUMENT,
                        "Invalid ddl statement for operation " +
                        rc.opCode + ": " + statement);
                }

                /*
                 * Check if the given table name matches the name of the table
                 * in ddl statement.
                 *
                 * For ALTER_TABLE, if the table name specified is ocid, it will
                 * be passed down to TableUtils.tableDdlOperation() to validate
                 * after access check and the ocid of the table in ddl will be
                 * resolved then.
                 */
                if (rc.opCode == OpCode.CREATE_TABLE) {
                    if (!tableName.equalsIgnoreCase(callback.getTableName())) {
                        throw new RequestException(ILLEGAL_ARGUMENT,
                            "The table name '" + tableName +
                            "' does not match the table name in statement:" +
                            statement);
                    }
                } else if (rc.opCode == OpCode.ALTER_TABLE) {
                    if (tableName.startsWith(EXTERNAL_OCID_PREFIX)) {
                        tableOcid = tableName;
                    } else {
                        if (!tableName
                                .equalsIgnoreCase(callback.getTableName())) {
                            throw new RequestException(ILLEGAL_ARGUMENT,
                                "The table name '" + tableName +
                                "' does not match the table name in statement:" +
                                statement);
                        }
                    }
                }

                /*
                 * For Cloud, use TableUtils directly.
                 * For KV table DDL, need to set AuthContext in the options
                 */
                resp = handleTableDdl(rc.request, actx, tableOcid, statement,
                                      limits, isAutoReclaimable,
                                      freeformTags, definedTags, matchETag,
                                      retryToken, rc.opCode, callback,
                                      rc.lc, apiVersion);
            }
            return resp;
        });
    }

    protected void handleTableOp(RequestContext rc,
                                 FullHttpResponse response,
                                 AccessContext actx,
                                 Supplier<GetTableResponse> executor) {

        GetTableResponse resp = null;
        int errorCode = NO_ERROR;
        try {
            resp = executor.get();
        } catch (RequestException re) {
            errorCode = re.getErrorCode();
            throw re;
        } catch (FilterRequestException fre) {
            throw fre;
        } catch (Throwable t) {
            errorCode = UNKNOWN_ERROR;
            if (t instanceof FaultException) {
                /* TBD: does something better */
            }
            if (t instanceof IllegalArgumentException ||
                t instanceof QueryException) {
                /*
                 * For now, map query/ddl problems to ILLEGAL_ARGUMENT. At some
                 * point a query or ddl-specific error code may be needed.
                 *
                 * TODO: try to tease out table/index not found exceptions. May
                 * need additional information from query API. E.g. a
                 * table/index not found exception that extends QueryException.
                 * The information is available.
                 */
                errorCode = ILLEGAL_ARGUMENT;
            }
            /* this will be logged in caller */
            throw new RequestException(errorCode, t.getMessage());
        } finally {
            TableInfo info = (resp == null) ? null : resp.getTableInfo();
            auditOperation(rc, info, errorCode);
        }

        if (!resp.getSuccess()) {
            throw new RequestException(resp.getErrorCode(),
                                       resp.getErrorString());
        }

        String workRequestId = null;
        TableInfo info = resp.getTableInfo();
        if (info != null) {
            workRequestId = tm.getWorkRequestId(info, rc.opCode);
            /*
             * Flush the local Table cache. DDL Operations nearly always
             * invalidate the current state
             */
            if (actx.getType() == Type.IAM) {
                if (info.getKVTableName() != null) {
                    tableCache.flushEntry(null, info.getKVTableName());
                }
            } else {
                tableCache.flushEntry(info.getTenantId(), info.getTableName());
            }
        }

        /* build response */
        buildWorkRequestIdResponse(response, workRequestId);

        markTMOpSucceeded(rc, 1 /* TM operation count*/);
    }

    private boolean checkDdlOperation(OpCode op, QueryOperation qop) {
        if (qop == QueryOperation.CREATE_TABLE) {
            /* Create table ddl can be used for CREATE_TABLE or ALTER_TABLE op */
            return op == OpCode.CREATE_TABLE || op == OpCode.ALTER_TABLE;
        }
        return op == TableUtils.getOpCode(qop);
    }

    protected String getMapTableName(AccessContext actx,
                                     OpCode op,
                                     String tableName) {
        if (actx.getType() == Type.IAM) {
            return mapTableName(actx, op, tableName);
        }
        return tableName;
    }

    /* Generates DROP TABLE statement */
    private static String buildDropTableDdl(AccessContext actx,
                                            String tableNameOrId,
                                            boolean isIfExists) {
        StringBuilder sb = new StringBuilder("DROP TABLE ");
        if (isIfExists) {
            sb.append("IF EXISTS ");
        }

        sb.append(actx.mapToInternalId(tableNameOrId));
        return sb.toString();
    }

    /* Generates CREATE INDEX statement */
    private static String buildCreateIndexDdl(String tableName,
                                              String indexName,
                                              List<String> indexKeys,
                                              boolean isIfNotExists) {
        StringBuilder sb = new StringBuilder("CREATE INDEX ");
        if (isIfNotExists) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(indexName);
        sb.append(" ON ");
        sb.append(tableName);
        sb.append("(");

        boolean first = true;
        for (String indexKey : indexKeys) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append(indexKey);
        }
        sb.append(")");
        return sb.toString();
    }

    /* Generates DROP INDEX statement */
    private static String buildDropIndexDdl(String tableName,
                                            String indexName,
                                            boolean isIfExists) {
        StringBuilder sb = new StringBuilder("DROP INDEX ");
        if (isIfExists) {
            sb.append(" IF EXISTS ");
        }
        sb.append(indexName);
        sb.append(" ON ");
        sb.append(tableName);
        return sb.toString();
    }

    /**
     * Get statement information, this method also enforces limit on query
     * string length.
     */
    private PrepareCB getStatementInfo(AccessContext actx,
                                       String statement,
                                       boolean checkDml) {

        PrepareCB cbInfo = TableUtils.getCallbackInfo(actx, statement, tm);
        if (checkDml) {
            cbInfo.checkSupportedDml();
        }
        return cbInfo;
    }

    private PreparedStatementWrapper deserializePreparedQuery(byte[] bytes)
        throws IOException {

        int totalLength = bytes.length;
        if (totalLength <= 0) {
            throw new IllegalArgumentException(
                "Bad length of prepared statement: " + totalLength);
        }

        ByteBuf buf = Unpooled.copiedBuffer(bytes);
        try {
            ByteInputStream bis = new ByteInputStream(buf);
            return deserializePreparedQuery(bis, totalLength, false);
        } finally {
            buf.release(buf.refCnt());
        }
    }

    private static TimeToLive getTimeToLive(int ttlInDays) {
        if (ttlInDays > 0) {
            return TimeToLive.ofDays(ttlInDays);
        }
        return null;
    }

    private static int getTimeoutMs(int timeoutMs) {
        return (timeoutMs == 0) ? DEFAULT_TIMEOUT_MS : timeoutMs;
    }

    private static Consistency getConsistency(Consistency consistency) {
        return consistency != null ? consistency : Consistency.NONE_REQUIRED;
    }

    private static int getLimit(int limit) {
        return (limit == 0) ? DEFAULT_PAGE_SIZE : limit;
    }

    /**
     * Create RowSerializer object for the given key which is an array of
     * strings, each of the format "column-name:value"
     */
    private static PrimaryKeyImpl createPrimaryKey(TableImpl table,
                                                   List<String> key) {

        final PrimaryKeyImpl pkey = table.createPrimaryKey();
        final RecordDef recDef = pkey.getDefinition();

        for (String comp : key) {
            int pos = comp.indexOf(':');
            if (pos <= 0) {
                throw new IllegalArgumentException("Invalid key, " +
                    "key is not in format of 'column-name:value': " + comp);
            }
            String name = comp.substring(0, pos);
            String value = comp.substring(pos + 1);
            FieldDef fdef = recDef.getFieldDef(name);
            if (fdef == null) {
                throw new IllegalArgumentException(
                    "There is no field with name '" + name + "'");
            }
            pkey.put(name, createKeyFieldValue(name, fdef, value));
        }
        return pkey;
    }

    private static FieldValue createKeyFieldValue(String name,
                                                  FieldDef def,
                                                  String value) {
        try {
            switch (def.getType()) {
            case INTEGER:
                return def.asInteger().createInteger(Integer.parseInt(value));
            case LONG:
                return def.asLong().createLong(Long.parseLong(value));
            case BOOLEAN:
                return def.asBoolean().createBoolean(Boolean.valueOf(value));
            case TIMESTAMP:
                return def.asTimestamp().fromString(value);
            case STRING:
                return def.asString().createString(value);
            case ENUM:
                return def.asEnum().createEnum(value);
            case DOUBLE:
                return def.asFloat().createDouble(Double.parseDouble(value));
            case FLOAT:
                return def.asFloat().createFloat(Float.parseFloat(value));
            case NUMBER:
                return ((NumberDefImpl)def.asNumber()).createNumber(value);
            default:
                throw new IllegalStateException(
                    "Unexpected primary key field type: " + def.getType());
            }
        } catch (RuntimeException ex) {
            throw new IllegalArgumentException("Invalid value for field '" +
                name + "': " + ex.getMessage());
        }
    }

    protected static void buildWorkRequestIdResponse(FullHttpResponse resp,
                                                     String workRequestId) {
        resp.setStatus(HttpResponseStatus.ACCEPTED);
        resp.headers().set(OPC_WORK_REQUEST_ID, workRequestId);
    }

    private static void buildResponse(FullHttpResponse resp, String info) {
        ByteBuf payload = resp.content();
        if (info != null) {
            payload.writeCharSequence(info, UTF_8);
        }
        resp.headers().setInt(CONTENT_LENGTH, payload.readableBytes());
    }

    protected static void buildTaggedResponse(FullHttpResponse resp,
                                              String info,
                                              byte[] etag) {
        buildResponse(resp, info);
        if (etag != null) {
            resp.headers().set(ETAG, encodeBase64(etag));
        }
    }

    /* Parses page token that represents a integer resume index */
    private static int parseStartIndexPageToken(String pageToken) {
        try {
            return Integer.parseInt(pageToken);
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException("Invalid Page  '" +
                    pageToken + "': " + nfe.getMessage());
        }
    }

    /* Generates page token that represents a integer resume index */
    private static String generateLastIndexPageToken(int lastIndex) {
        return String.valueOf(lastIndex);
    }

    private static void buildPaginatedResponse(FullHttpResponse resp,
                                               String info,
                                               String nextPageToken) {
        ByteBuf payload = resp.content();
        if (info != null) {
            payload.writeCharSequence(info, UTF_8);
        }
        resp.headers().setInt(CONTENT_LENGTH, payload.readableBytes());

        if (nextPageToken != null) {
            resp.headers().set(OPC_NEXT_PAGE, nextPageToken);
        }
    }

    private static FullHttpResponse createResponse(ByteBuf payload,
                                                   String requestId) {

        FullHttpResponse resp = new DefaultFullHttpResponse(HTTP_1_1,
                HttpResponseStatus.OK, payload);
        /*
         * It's necessary to add the header below to allow script-based
         * access from browsers.
         */
        resp.headers().set(CONTENT_TYPE, APPLICATION_JSON)
            .setInt(CONTENT_LENGTH, 0)
            .set(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .set(ACCESS_CONTROL_EXPOSE_HEADERS, EXPOSED_HEADERS)
            .set(OPC_REQUEST_ID, requestId);

        return resp;
    }

    private static ErrorCode mapExceptionToErrorCode(Throwable ex) {

        if (ex instanceof IllegalArgumentException) {
            return ErrorCode.INVALID_PARAMETER;
        }

        ErrorCode code;
        if (ex instanceof RequestException) {
            code = getErrorCode(((RequestException)ex).getErrorCode());
            if (code != null) {
                return code;
            }
        }
        return ErrorCode.INTERNAL_SERVER_ERROR;
    }

    private static ErrorCode getErrorCode(int errorCode) {
        if (errorMap.containsKey(errorCode)) {
            return errorMap.get(errorCode);
        }
        return ErrorCode.UNKNOWN_ERROR;
    }

    /*
     * The map of error code between RequestException and RenderableException
     */
    private static Map<Integer, ErrorCode> createErrorCodeMap() {
        return new HashMap<Integer, ErrorCode>() {
            private static final long serialVersionUID = 1L;
            {
                /*
                 * Error codes for user-generated errors, range from 1 to
                 * 50(exclusive).
                 * These include illegal arguments, exceeding size limits for
                 * some objects, resource not found, etc.
                 */
                put(UNKNOWN_OPERATION, ErrorCode.NOT_FOUND);
                put(TABLE_NOT_FOUND, ErrorCode.TABLE_NOT_FOUND);
                put(INDEX_NOT_FOUND, ErrorCode.INDEX_NOT_FOUND);
                put(ILLEGAL_ARGUMENT, ErrorCode.INVALID_PARAMETER);
                put(ROW_SIZE_LIMIT_EXCEEDED,
                    ErrorCode.ROW_SIZE_LIMITED_EXCEEDED);
                put(KEY_SIZE_LIMIT_EXCEEDED,
                    ErrorCode.KEY_SIZE_LIMITED_EXCEEDED);
                put(BATCH_OP_NUMBER_LIMIT_EXCEEDED,
                    ErrorCode.BATCH_OP_NUMBER_LIMIT_EXCEEDED);
                put(REQUEST_SIZE_LIMIT_EXCEEDED,
                    ErrorCode.REQUEST_SIZE_LIMIT_EXCEEDED);
                put(TABLE_EXISTS, ErrorCode.TABLE_ALREADY_EXISTS);
                put(INDEX_EXISTS, ErrorCode.INDEX_ALREADY_EXISTS);
                put(INVALID_AUTHORIZATION, ErrorCode.INVALID_AUTHORIZED);
                put(INSUFFICIENT_PERMISSION, ErrorCode.INSUFFICIENT_PERMISSION);
                put(RESOURCE_EXISTS, ErrorCode.RESOURCE_ALREADY_EXISTS);
                put(RESOURCE_NOT_FOUND, ErrorCode.RESOURCE_NOT_FOUND);
                put(ETAG_MISMATCH, ErrorCode.ETAG_MISMATCH);
                put(INVALID_RETRY_TOKEN, ErrorCode.INVALID_RETRY_TOKEN);
                put(TABLE_LIMIT_EXCEEDED, ErrorCode.TABLE_LIMIT_EXCEEDED);
                put(INDEX_LIMIT_EXCEEDED, ErrorCode.INDEX_LIMIT_EXCEEDED);
                put(BAD_PROTOCOL_MESSAGE, ErrorCode.CANNOT_PARSE_REQUEST);
                put(EVOLUTION_LIMIT_EXCEEDED,
                    ErrorCode.EVOLUTION_LIMIT_EXCEEDED);
                put(TABLE_DEPLOYMENT_LIMIT_EXCEEDED,
                    ErrorCode.TABLE_DEPLOYMENT_LIMIT_EXCEEDED);
                put(TENANT_DEPLOYMENT_LIMIT_EXCEEDED,
                    ErrorCode.TENANT_DEPLOYMENT_LIMIT_EXCEEDED);
                put(OPERATION_NOT_SUPPORTED, ErrorCode.METHOD_NOT_IMPLEMENTED);
                put(CANNOT_CANCEL_WORK_REQUEST,
                    ErrorCode.CANNOT_CANCEL_WORK_REQUEST);

                /*
                 * Error codes for user throttling, range from 50 to
                 * 100(exclusive).
                 */
                put(READ_LIMIT_EXCEEDED, ErrorCode.TOO_MANY_REQUESTS);
                put(WRITE_LIMIT_EXCEEDED, ErrorCode.TOO_MANY_REQUESTS);
                put(SIZE_LIMIT_EXCEEDED, ErrorCode.SIZE_LIMIT_EXCEEDED);
                put(OPERATION_LIMIT_EXCEEDED, ErrorCode.TOO_MANY_REQUESTS);

                /*
                 * Retry-able server issues, range from 100 to 125(exclusive).
                 * These are internal problems, presumably temporary, and need
                 * to be sent back to the application for retry.
                 */
                put(REQUEST_TIMEOUT, ErrorCode.REQUEST_TIMEOUT);
                put(SERVER_ERROR, ErrorCode.INTERNAL_SERVER_ERROR);
                put(SERVICE_UNAVAILABLE, ErrorCode.SERVICE_UNAVAILABLE);
                put(SECURITY_INFO_UNAVAILABLE,
                    ErrorCode.SECURITY_INFO_UNAVAILABLE);
                put(RETRY_AUTHENTICATION, ErrorCode.RETRY_AUTHENTICATION);

                /*
                 * Other server issues, begin from 125.
                 * These include server illegal state, unknown server error, etc.
                 * They might be retry-able, or not.
                 */
                put(UNKNOWN_ERROR, ErrorCode.UNKNOWN_ERROR);
                put(ILLEGAL_STATE, ErrorCode.ILLEGAL_STATE);
            }
        };
    }

    /**
     * There is something in the request itself that is invalid. This may be a
     * purposeful attack, so handle it separately from generic errors.
     * Note that many requests with unrecognized URL patterns will be detected
     * by the caller and never get to this module.
     *
     * TODO: perhaps do a hard-close on the connection in this path or take
     * other action if an attack is suspected.
     */
    protected FullHttpResponse invalidRequest(RequestContext rc,
                                              String message,
                                              int errorCode) {

        if (logger.isLoggable(Level.FINE, rc.lc)) {
            CharSequence realIp = rc.headers.get(X_REAL_IP_HEADER);
            CharSequence forwardedFor = rc.headers.get(X_FORWARDED_FOR_HEADER);
            String remoteAddr = rc.ctx.channel().remoteAddress().toString();

            StringBuilder sb = new StringBuilder();
            sb.append(message);
            sb.append(", remote address=").append(remoteAddr);
            if (realIp != null) {
                sb.append(", ").append(X_REAL_IP_HEADER).append("=").append(realIp);
            }
            if (forwardedFor != null) {
                sb.append(", ").append(X_FORWARDED_FOR_HEADER).append("=")
                    .append(forwardedFor);
            }

            logger.fine(sb.toString(), rc.lc);
        }

        return createErrorResponse(rc, message, getErrorCode(errorCode));
    }

    private static FullHttpResponse createErrorResponse(RequestContext rc,
                                                        String errorMessage,
                                                        ErrorCode errorCode) {

        rc.resetOutputBuffer();

        FullHttpResponse resp =
            new DefaultFullHttpResponse(HTTP_1_1,
                                        errorCode.getHttpStatusCode(),
                                        rc.bbos.getByteBuf());

        String body = JsonBuilder.create()
                        .append("code", errorCode.getErrorCode())
                        .append("message", errorMessage)
                        .toString();
        rc.bbos.getByteBuf().writeCharSequence(body, UTF_8);

        HttpHeaders headers = resp.headers();
        headers.set(CONTENT_TYPE, APPLICATION_JSON_NOCHARSET)
               .setInt(CONTENT_LENGTH, rc.bbos.getByteBuf().readableBytes())
               .set(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
               .set(OPC_REQUEST_ID, rc.requestId);

        return resp;
    }

    protected static String readTableNameOrId(RequestParams request) {
        String tableNameOrId = request.getPathParam(TABLE_NAME_OR_ID);
        checkNotNullEmpty(TABLE_NAME_OR_ID, tableNameOrId);
        return tableNameOrId;
    }

    private static String readIndexName(RequestParams request) {
        String idxName = request.getPathParam(INDEX_NAME);
        checkNotNullEmpty(INDEX_NAME, idxName);
        return idxName;
    }

    protected static String readCompartmentId(RequestParams request) {
        return readCompartmentId(request, false /* required */);
    }

    protected static String readCompartmentId(RequestParams request,
                                              boolean required) {
        String compartmentId = request.getQueryParamAsString(COMPARTMENT_ID);
        if (required) {
            checkNotNullEmpty(COMPARTMENT_ID, compartmentId);
        } else {
            checkNotEmpty(COMPARTMENT_ID, compartmentId);
        }
        return compartmentId;
    }

    private static boolean readIfExists(RequestParams request) {
        return request.getQueryParamAsBoolean(IS_IF_EXISTS, DEFAULT_IF_EXISTS);
    }

    protected static byte[] readIfMatch(RequestParams request) {
        String ifMatch = request.getHeaderAsString(IF_MATCH);
        checkNotEmpty(IF_MATCH, ifMatch);
        try {
            return decodeBase64(ifMatch);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Invalid " + IF_MATCH + ": " +
                                               ex.getMessage());
        }
    }

    protected static String readOpcRetryToken(RequestParams request) {
        String token = request.getHeaderAsString(OPC_RETRY_TOKEN);
        if (token != null && !token.matches(VALID_RETRYTOKEN_REGEX)) {
            throw new IllegalArgumentException("Invalid retry token, it must " +
                "contain alphanumerics plus underscore (\"_\") and dash(-) " +
                " and its length must be less than 64 characters: " + token);
        }
        return token;
    }

    private static List<String> readKey(RequestParams request) {
        List<String> key = request.getQueryParam(KEY);
        validateKey(key);
        return key;
    }

    private static boolean readIsGetReturnRow(RequestParams request) {
        return request.getQueryParamAsBoolean(IS_GET_RETURN_ROW, false);
    }

    private static String readPage(RequestParams request) {
        String page = request.getQueryParamAsString(PAGE);
        checkNotEmpty(PAGE, page);
        return page;
    }

    private static int readLimit(RequestParams request) {
        int limit = request.getQueryParamAsInt(LIMIT);
        checkNonNegativeInt(LIMIT, limit);
        return limit;
    }

    private static long readTimeStart(RequestParams request) {
        String timeStart = request.getQueryParamAsString(TIME_START);
        checkNotEmpty(TIME_START, timeStart);
        if (timeStart != null) {
            return parseTimestamp(timeStart);
        }
        return 0;
    }

    private static long readTimeEnd(RequestParams request) {
        String timeEnd = request.getQueryParamAsString(TIME_END);
        checkNotEmpty(TIME_END, timeEnd);
        if (timeEnd != null) {
            return parseTimestamp(timeEnd);
        }
        return 0;
    }

    private static SortBy readSortBy(RequestParams request) {
        String sortBy = request.getQueryParamAsString(SORT_BY);
        checkNotEmpty(SORT_BY, sortBy);
        if (sortBy != null) {
            try {
                return SortBy.valueOf(sortBy.toUpperCase());
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException("Invalid " +
                    SORT_BY + "\"" + sortBy + "\"");
            }
        }
        return null;
    }

    private static SortOrder readSortOrder(RequestParams request) {
        String sortOrder = request.getQueryParamAsString(SORT_ORDER);
        checkNotEmpty(SORT_ORDER, sortOrder);
        if (sortOrder != null) {
            try {
                return SortOrder.valueOf(sortOrder.toUpperCase());
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException("Invalid " +
                    SORT_ORDER + " \"" + sortOrder + "\"");
            }
        }
        return null;
    }

    private static String readName(RequestParams request) {
        String name = request.getQueryParamAsString(NAME);
        checkNotEmpty(NAME, name);
        return name;
    }

    private static LifecycleState readlifeCycleState(RequestParams request) {
        String lifecycleState = request.getQueryParamAsString(LIFE_CYCLE_STATE);
        checkNotEmpty(LIFE_CYCLE_STATE, lifecycleState);
        if (lifecycleState != null) {
            try {
                return LifecycleState.valueOf(lifecycleState.toUpperCase());
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("Invalid " +
                    LIFE_CYCLE_STATE + "\"" + lifecycleState + "\"");
            }
        }
        return null;
    }

    private static String readStatement(RequestParams request) {
        String statement = request.getQueryParamAsString(STATEMENT);
        checkNotNullEmpty(STATEMENT, statement);
        return statement;
    }

    private static boolean readIsGetQueryPlan(RequestParams request) {
        return request.getQueryParamAsBoolean(IS_GET_QUERY_PLAN,
                                              false /* default */);
    }

    private static int readTimeoutInMs(RequestParams request) {
        int timeoutMs = request.getQueryParamAsInt(TIMEOUT_IN_MS);
        checkNonNegativeInt(TIMEOUT_IN_MS, timeoutMs);
        return timeoutMs;
    }

    private static String readWorkRequestId(RequestParams request) {
        String workRequestId = request.getPathParam(WORK_REQUEST_ID);
        checkNotNullEmpty(WORK_REQUEST_ID, workRequestId);
        return workRequestId;
    }

    protected static boolean readDryRun(RequestParams request) {
        return Boolean.valueOf(request.getHeaderAsString(IS_OPC_DRY_RUN));
    }

    private Consistency getConsistency(RequestParams request) {
        String consistency = request.getQueryParamAsString(CONSISTENCY);
        checkNotEmpty(CONSISTENCY, consistency);
        return mapToKVConsistency(consistency);
    }

    private static void validateKey(List<String> key) {
        checkNotNull(KEY, key);
        int index = 0;
        for (String comp : key) {
            String name = "key[" + index++ + "]";
            checkNotNullEmpty(name, comp);
        }
    }

    private static void validateValue(String value) {
        checkNotNull(VALUE, value);
        if (value.length() == 0) {
            throw new IllegalArgumentException(
                "Invalid value, it should not be empty: " + value);
        }
    }

    void checkRequestSizeLimit(FullHttpRequest request, TableEntry entry) {
        /*
         * Get the request size, reset the read index to get full request size.
         */
        ByteBuf buf = request.content();
        int markIndex = buf.readerIndex();
        int requestSize = buf.readerIndex(0).readableBytes();
        buf.readerIndex(markIndex);

        int limit = entry.getRequestLimits().getRequestSizeLimit();

        if (requestSize > limit) {
            throw new RequestException(REQUEST_SIZE_LIMIT_EXCEEDED,
                "The size of request exceeded the limit of " +
                limit + ": " + requestSize);
        }
    }


    /*
     * Interface for handling operation requests.
     */
    @FunctionalInterface
    private interface ProxyOperation {
        void handle(FullHttpResponse response,
                    RequestContext rc,
                    AccessContext actx,
                    String apiVersion);
    }

    /* TODO: support flexible type-casting */
    protected static class RestRow extends RowImpl {
        private static final long serialVersionUID = 1L;

        private int keySizeLimit;
        private int valueSizeLimit;

        public RestRow(TableImpl table, int keySizeLimit, int valueSizeLimit) {
            super(table.createRow());
            this.keySizeLimit = keySizeLimit;
            this.valueSizeLimit = valueSizeLimit;
        }

        @Override
        public void validateKey(TableKey key) {
            checkKeySize(key, keySizeLimit);
        }

        @Override
        public void validateValue(Value value) {
            checkValueSize(value, valueSizeLimit);
        }
    }
}
