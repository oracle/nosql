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
package oracle.nosql.proxy.sc;

import static oracle.nosql.proxy.protocol.HttpConstants.ACTIONS;
import static oracle.nosql.proxy.protocol.HttpConstants.CHANGE_COMPARTMENT;
import static oracle.nosql.proxy.protocol.HttpConstants.COMPARTMENT_ID;
import static oracle.nosql.proxy.protocol.HttpConstants.CROSS_REGION_DDL;
import static oracle.nosql.proxy.protocol.HttpConstants.DML_MS;
import static oracle.nosql.proxy.protocol.HttpConstants.DRY_RUN;
import static oracle.nosql.proxy.protocol.HttpConstants.END_TIMESTAMP;
import static oracle.nosql.proxy.protocol.HttpConstants.GENERAL;
import static oracle.nosql.proxy.protocol.HttpConstants.IF_MATCH;
import static oracle.nosql.proxy.protocol.HttpConstants.KEY_ID;
import static oracle.nosql.proxy.protocol.HttpConstants.LIMIT;
import static oracle.nosql.proxy.protocol.HttpConstants.NAME_ONLY;
import static oracle.nosql.proxy.protocol.HttpConstants.NAME_PATTERN;
import static oracle.nosql.proxy.protocol.HttpConstants.OPERATION_ID;
import static oracle.nosql.proxy.protocol.HttpConstants.SET_ACTIVITY;
import static oracle.nosql.proxy.protocol.HttpConstants.REPLICA;
import static oracle.nosql.proxy.protocol.HttpConstants.REPLICA_STATS;
import static oracle.nosql.proxy.protocol.HttpConstants.RETURN_COLLECTION;
import static oracle.nosql.proxy.protocol.HttpConstants.SORT_BY;
import static oracle.nosql.proxy.protocol.HttpConstants.SORT_ORDER_ASC;
import static oracle.nosql.proxy.protocol.HttpConstants.START_INDEX;
import static oracle.nosql.proxy.protocol.HttpConstants.START_TIMESTAMP;
import static oracle.nosql.proxy.protocol.HttpConstants.STATE;
import static oracle.nosql.proxy.protocol.HttpConstants.TABLE_INDEXES;
import static oracle.nosql.proxy.protocol.HttpConstants.TABLE_STOREINFO;
import static oracle.nosql.proxy.protocol.HttpConstants.TABLE_USAGE;
import static oracle.nosql.proxy.protocol.HttpConstants.TENANT_ID;
import static oracle.nosql.proxy.protocol.HttpConstants.VAULT_ID;
import static oracle.nosql.proxy.security.AccessContext.INTERNAL_OCID_PREFIX;
import static oracle.nosql.util.http.HttpConstants.REQUEST_ORIGIN_HEADER;
import static oracle.nosql.util.http.HttpConstants.REQUEST_ORIGIN_PROXY;
import static oracle.nosql.util.http.HttpConstants.WORK_REQUEST_TYPE;
import static oracle.nosql.util.http.HttpConstants.WORK_REQUEST_DDL;
import static oracle.nosql.util.http.HttpConstants.WORK_REQUEST_KMSKEY;

import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.gson.reflect.TypeToken;

import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.cache.Cache;
import oracle.nosql.common.cache.CacheBuilder;
import oracle.nosql.common.cache.CacheBuilder.CacheConfig;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.proxy.Config;
import oracle.nosql.proxy.MonitorStats;
import oracle.nosql.proxy.protocol.Protocol.OpCode;
import oracle.nosql.proxy.sc.TableUtils.PrepareCB;
import oracle.nosql.proxy.security.AccessContext;
import oracle.nosql.proxy.util.CloudServiceTableCache;
import oracle.nosql.proxy.util.TableCache;
import oracle.nosql.util.ConnectivityTrack;
import oracle.nosql.util.HostPort;
import oracle.nosql.util.HttpRequest;
import oracle.nosql.util.HttpRequest.ConnectionHandler;
import oracle.nosql.util.HttpRequest.HttpMethod;
import oracle.nosql.util.HttpResponse;
import oracle.nosql.util.SCResolver;
import oracle.nosql.util.ServiceDirectory;
import oracle.nosql.util.fault.ErrorCode;
import oracle.nosql.util.fault.ErrorResponse;
import oracle.nosql.util.fault.RequestFaultException;
import oracle.nosql.util.filter.Rule;
import oracle.nosql.util.ph.HealthStatus;
import oracle.nosql.util.ssl.SSLConfig;
import oracle.nosql.util.ssl.SSLConnectionHandler;
import oracle.nosql.util.tmi.DdlHistoryEntry;
import oracle.nosql.util.tmi.DropInputs;
import oracle.nosql.util.tmi.IndexInfo;
import oracle.nosql.util.tmi.IndexInfo.IndexState;
import oracle.nosql.util.tmi.KmsKeyInfo;
import oracle.nosql.util.tmi.ListWorkRequestsResult;
import oracle.nosql.util.tmi.ReplicaStats;
import oracle.nosql.util.tmi.StoreInfo;
import oracle.nosql.util.tmi.TableCollection;
import oracle.nosql.util.tmi.TableDDLInputs;
import oracle.nosql.util.tmi.TableInfo;
import oracle.nosql.util.tmi.TableInfo.TableState;
import oracle.nosql.util.tmi.TableLimits;
import oracle.nosql.util.tmi.TableUsage;
import oracle.nosql.util.tmi.TenantLimits;
import oracle.nosql.util.tmi.WorkRequest;

/**
 * Implementation of TenantManager that talks over an HTTP/1 connection to
 * an actual Service Controller instance.
 *
 * This is a very simple-minded implementation that creates and destroys a
 * connection for each call. The SCTenantManager is reentrant, but
 * connections are not cached.
 */
public class SCTenantManager implements TenantManager {

    /*
     * this needs to be set externally post-construction, because it's shared
     * with the proxy.
     */
    private SkLogger logger;
    private final HttpRequest scRequest;
    private ConnectionHandler scSSLHandler;
    private final ConnectivityTrack connectTrack;

    /* SC connectivity failure tolerance */
    private static final int FAILURE_TOLERANCE = 3;
    private final static int DEFAULT_REQ_CONNECT_TIMEOUT = 5000;
    private final static int DEFAULT_REQ_READ_TIMEOUT = 5000;

    /* threshold for logging SC latencies at INFO level */
    private final static int DEFAULT_LATENCY_INFO_THRESHOLD_MS = 1000;

    /* Query parameter internal */
    private final static String INTERNAL_QUERY = "internal=true";

    /*
     * The urlBase contains the SC hostname address. It's set upon demand
     * whenever the SCTenantManager receives its first call.
     *
     * In a situation where there is a loadbalancer, the LB provides discovery
     * services for the SC. Where there is no LB, the urlBase may need to be
     * reset if the connections to the SC fails, and the SC needs to be
     * rediscovered. Since the SCTenantManager must be reentrant, the
     * urlBase must be protected with a mutex.
     */
    private String tmTablesBase; /* /V0/tm/tables */
    private String tmUrlBase; /* /V0/tm/ */
    private String tmWorkRequestsBase;  /* /V0/tm/workRequests */
    private String filterRequestsBase;  /* /V0/filters */
    private String cmekUrlBase;  /* /V0/cmek/kmsKey */
    private List<String> pingUrls;
    private final String tmAPIVersion;
    private final Cache<String, TenantLimits> limitsCache;

    /* timeouts for HTTP requests to SC, defaults above */
    private final int connectTimeoutMs;
    private final int readTimeoutMs;

    /* threshold for SC latencies to be logged at INFO level */
    private final int latencyInfoThresholdMs;

    private TableCache tableCache;

    private final boolean isChildTableEnabled;
    private final ServiceDirectory serviceDirectory;

    public SCTenantManager(String tmAPIVersion,
                           int connectTimeoutMs,
                           int readTimeoutMs,
                           boolean isChildTableEnabled,
                           int latencyInfoThresholdMs,
                           ServiceDirectory serviceDirectory) {
        /* TODO: how to negotiate the version of the API to use? */
        this.tmAPIVersion = tmAPIVersion;
        /* use min of 1s for timeouts */
        this.connectTimeoutMs = connectTimeoutMs > 1000 ? connectTimeoutMs :
            DEFAULT_REQ_CONNECT_TIMEOUT;
        this.readTimeoutMs = readTimeoutMs > 1000 ? readTimeoutMs :
            DEFAULT_REQ_READ_TIMEOUT;
        this.latencyInfoThresholdMs =
            latencyInfoThresholdMs > 0 ? latencyInfoThresholdMs :
            DEFAULT_LATENCY_INFO_THRESHOLD_MS;
        this.serviceDirectory = serviceDirectory;

        /*
         * cache TenantLimits objects. Configuration calls are
         * self-explanatory. The cache has an expiration time on entries.
         * Don't use a built-in loader.
         *
         * Expire after 10 minutes in the cache.
         *
         * There are other configuration options for this object, such as stats.
         * They are not used at this time.
         */
        limitsCache = CacheBuilder.build(
            new CacheConfig().setLifetime(10 * 60 * 1000)
                             .setName("TenantLimitsCache"));

        scRequest = new HttpRequest().disableRetry()
            .setTimeout(this.connectTimeoutMs, this.readTimeoutMs)
            .addStandingHeader(REQUEST_ORIGIN_HEADER, REQUEST_ORIGIN_PROXY);

        connectTrack = new ConnectivityTrack("SC", FAILURE_TOLERANCE);

        this.isChildTableEnabled = isChildTableEnabled;
    }

    @Override
    public void setLogger(final SkLogger logger) {
        this.logger = logger;
    }

    /**
     * This method may do a remote call to the SC to retrieve this information.
     * Technically it could be cached for a while, probably LRU and
     * time-based.
     */
    @Override
    public TenantLimits getTenantLimits(String tenantId, LogContext lc) {
        if (tenantId == null) {
            return null;
        }
        TenantLimits limits = limitsCache.get(tenantId);
        if (limits != null) {
            return limits;
        }

        try {
            StringBuilder sb = new StringBuilder().append(getTMUrlBase())
                .append("tenant").append("/").append(tenantId).append("/")
                .append("limits");

            final String url = sb.toString();

            logTrace(lc, "getTenantLimits: " + url);
            final HttpResponse response =
                doHttpRequest(scRequest, HttpMethod.GET, url,
                              null /* payload */,
                              scSSLHandler, lc);

            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                ErrorResponse er = getErrorResponse(response, "getTenantLimits");
                logError(response, lc, "getTenantLimits error: " + er);
                throw new IllegalArgumentException(
                    "Can't get tenant limits for " + tenantId + ": " + er);
            }

            limits = JsonUtils.readValue(response.getOutput(),
                                         TenantLimits.class);
            logTrace(lc, "getTenantLimits response: " + limits);
            limitsCache.put(tenantId, limits);
            return limits;
        } catch (Exception e) {
            logException("GetTenantLimits", e, lc);
            throw new IllegalArgumentException(
                "Can't get tenant limits for " + tenantId +
                ": " + e.getMessage());
        }
    }

    /**
     * No-op close
     */
    @Override
    public void close() {}

    @Override
    public GetTableResponse getTable(AccessContext actx,
                                     String tableName,
                                     String operationId,
                                     boolean internal,
                                     LogContext lc) {
        try {
            StringBuilder sb = new StringBuilder().append(getTMTablesBase())
                .append(mapTableName(tableName));
            boolean firstParam = true;
            if (actx != null) {
                addQueryParam(sb, actx, firstParam);
                firstParam = (actx.getTenantId() == null &&
                              actx.getCompartmentId() == null);
            }
            if (operationId != null) {
                addQueryParam(sb, OPERATION_ID, operationId, firstParam);
                firstParam = false;
            }
            if (internal) {
                addInternalQueryParam(sb, firstParam);
            }

            final String url = sb.toString();

            logTrace(lc, "getTable: " + url);
            final HttpResponse response =
                doHttpRequest(scRequest, HttpMethod.GET, url,
                              null /* payload */,
                              scSSLHandler, lc);

            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                ErrorResponse er = getErrorResponse(response, "getTable");
                logError(response, lc, "getTable error: " + er);
                return new GetTableResponse(er);
            }

            TableInfo responseInfo = deserializePojo(response.getOutput(),
                                                     TableInfo.class);
            logTrace(lc, "getTable response: " + responseInfo);
            return new GetTableResponse(response.getStatusCode(), responseInfo);
        } catch (Exception e) {
            logException("GetTable", e, lc);
            return getTableError(e);
        }
    }

    @Override
    public GetTableResponse createTable(AccessContext actx,
                                        String tableName,
                                        String statement,
                                        boolean ifNotExists,
                                        TableLimits limits,
                                        boolean isAutoReclaimable,
                                        String retryToken,
                                        LogContext lc) {

        /* Construct the json payload for create table */
        String jsonPayload = null;
        try {
            validateTableName(tableName);
            jsonPayload = serializePojo(
                new TableDDLInputs(statement,
                                   actx.getTenantId(),
                                   actx.getCompartmentId(),
                                   null, /* matchETag */
                                   ifNotExists,
                                   limits,
                                   actx.getAuthorizedTags(),
                                   isAutoReclaimable,
                                   retryToken));

            StringBuilder sb = new StringBuilder().append(getTMTablesBase())
                .append(mapTableName(tableName));

            /* Execute the api: POST tm/tables/{tablename} */
            String createTableUri = sb.toString();
            logTrace(lc, "createTable: " + createTableUri);
            final HttpResponse response =
                doHttpRequest(scRequest, HttpMethod.POST,
                              createTableUri, jsonPayload,
                              scSSLHandler, lc);

            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                ErrorResponse er = getErrorResponse(response, "createTable");
                logError(response, lc, "createTable error: " + er);
                return new GetTableResponse(er);
            }
            TableInfo responseInfo = deserializePojo(response.getOutput(),
                                                      TableInfo.class);
            logTrace(lc, "createTable: response from TM: " + responseInfo);
            return new GetTableResponse(response.getStatusCode(), responseInfo);
        } catch (Exception e) {
            logException("CreateTable", e, lc);
            return getTableError(e);
        }
    }

    /**
     * The differences between alterTable and createTable are:
     * o use of tableName in TableDDLInputs
     * o use of ifNotExists in TableDDLInputs
     * o PUT (alter) vs POST (create)
     */
    @Override
    public synchronized GetTableResponse alterTable(AccessContext actx,
                                                    String tableName,
                                                    String statement,
                                                    TableLimits limits,
                                                    byte[] matchETag,
                                                    LogContext lc) {
        String jsonPayload = null;
        try {
            TableDDLInputs inputs =
                new TableDDLInputs(statement,
                                   actx.getTenantId(),
                                   actx.getCompartmentId(),
                                   matchETag,
                                   false,
                                   limits,
                                   actx.getAuthorizedTags(),
                                   false /* autoReclaimable */,
                                   null  /* retryToken */);
            if (actx.getOboToken() != null) {
                inputs.setOboToken(actx.getOboToken());
            }

            jsonPayload = serializePojo(inputs);

            /* Execute the api: PUT tm/tables/{tablename} */
            StringBuilder sb = new StringBuilder().append(getTMTablesBase())
                .append(actx.getOriginalTableName(tableName));
            String alterTableUri = sb.toString();
            logTrace(lc, "alterTable: " + alterTableUri);
            final HttpResponse response =
                doHttpRequest(scRequest, HttpMethod.PUT,
                              alterTableUri, jsonPayload,
                              scSSLHandler, lc);

            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                ErrorResponse er = getErrorResponse(response, "alterTable");
                logError(response, lc, "alterTable error: " + er);
                return new GetTableResponse(er);
            }
            TableInfo responseInfo = deserializePojo(response.getOutput(),
                                                     TableInfo.class);
            logTrace(lc, "alterTable: response from TM: " + responseInfo);
            return new GetTableResponse(response.getStatusCode(), responseInfo);
        } catch (Exception e) {
            logException("AlterTable", e, lc);
            return getTableError(e);
        }
    }

    /**
     * Hmm, almost exactly like the create.
     */
    @Override
    public GetTableResponse dropTable(AccessContext actx,
                                      String tableName,
                                      boolean ifExists,
                                      byte[] matchETag,
                                      LogContext lc) {

        String jsonPayload;
        try {
            jsonPayload = serializePojo(
                new DropInputs(ifExists,
                               actx.getTenantId(),
                               actx.getCompartmentId(),
                               matchETag));

            StringBuilder sb = new StringBuilder().append(getTMTablesBase())
                .append(actx.getOriginalTableName(tableName));
            String dropTableUri = sb.toString();
            logTrace(lc, "dropTable: " + dropTableUri);
            final HttpResponse response =
                doHttpRequest(scRequest, HttpMethod.DELETE,
                              dropTableUri, jsonPayload,
                              scSSLHandler, lc);
            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                ErrorResponse er = getErrorResponse(response, "dropTable");
                logError(response, lc, "dropTable error: " + er);
                return new GetTableResponse(er);
            }

            TableInfo responseInfo = deserializePojo(response.getOutput(),
                                                     TableInfo.class);
            logTrace(lc, "dropTable: response from TM: " + responseInfo);
            return new GetTableResponse(response.getStatusCode(), responseInfo);
        } catch (Exception e) {
            logException("DropTable", e, lc);
            return getTableError(e);
        }
    }

    @Override
    public GetTableResponse createIndex(AccessContext actx,
                                        String tableName,
                                        String indexName,
                                        String statement,
                                        boolean ifNotExists,
                                        String retryToken,
                                        LogContext lc) {
        /* Construct the json payload for create index */
        String jsonPayload = null;
        try {

            validateIndexName(indexName);

            TableDDLInputs inputs = new TableDDLInputs(statement,
                    actx.getTenantId(),
                    actx.getCompartmentId(),
                    null /* matchETag */,
                    ifNotExists,
                    null /* tableLimits */,
                    null /* tags */,
                    false /* autoReclaimable */,
                    retryToken);
            if (actx.getOboToken() != null) {
                inputs.setOboToken(actx.getOboToken());
            }

            jsonPayload = serializePojo(inputs);

            /* Execute the api: PUT tm/tables/{tablename}/indexes */
            StringBuilder sb = new StringBuilder().append(getTMTablesBase())
                .append(actx.getOriginalTableName(tableName)).append("/")
                .append(TABLE_INDEXES).append("/").append(indexName);
            String createIndexUri = sb.toString();
            logTrace(lc, "createIndex: " + createIndexUri);
            final HttpResponse response =
                doHttpRequest(scRequest, HttpMethod.POST,
                              createIndexUri, jsonPayload,
                              scSSLHandler, lc);

            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                ErrorResponse er = getErrorResponse(response, "createIndex");
                logError(response, lc, "createIndex error: " + er);
                return new GetTableResponse(er);
            }
            TableInfo responseInfo = deserializePojo(response.getOutput(),
                                                     TableInfo.class);
            logTrace(lc, "createIndex: response from TM: " + responseInfo);
            return new GetTableResponse(response.getStatusCode(), responseInfo);
        } catch (Exception e) {
            logException("CreateIndex", e, lc);
            return getTableError(e);
        }
    }

    @Override
    public GetTableResponse dropIndex(AccessContext actx,
                                      String tableName,
                                      String indexName,
                                      boolean ifExists,
                                      byte[] matchETag,
                                      LogContext lc) {
        String jsonPayload;
        try {
            DropInputs inputs = new DropInputs(ifExists,
                                               actx.getTenantId(),
                                               actx.getCompartmentId(),
                                               matchETag);
            if (actx.getOboToken() != null) {
                inputs.setOboToken(actx.getOboToken());
            }

            jsonPayload = serializePojo(inputs);

            StringBuilder sb = new StringBuilder().append(getTMTablesBase())
                .append(actx.getOriginalTableName(tableName)).append("/")
                .append(TABLE_INDEXES).append("/").append(indexName);
            String dropIndexUri = sb.toString();
            logTrace(lc, "dropIndex: " + dropIndexUri);
            final HttpResponse response =
                doHttpRequest(scRequest, HttpMethod.DELETE,
                              dropIndexUri, jsonPayload,
                              scSSLHandler, lc);
            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                ErrorResponse er = getErrorResponse(response, "dropIndex");
                logError(response, lc, "dropIndex error: " + er);
                return new GetTableResponse(er);
            }

            TableInfo responseInfo = deserializePojo(response.getOutput(),
                                                     TableInfo.class);
            logTrace(lc, "dropIndex: response from TM: " + responseInfo);
            return new GetTableResponse(response.getStatusCode(), responseInfo);
        } catch (Exception e) {
            logException("DropIndex", e, lc);
            return getTableError(e);
        }
    }

    @Override
    public GetTableResponse changeCompartment(AccessContext actx,
                                              String tableName,
                                              byte[] matchETag,
                                              String retryToken,
                                              LogContext lc) {
        String jsonPayload = null;
        try {
            jsonPayload = serializePojo(
                new TableDDLInputs(actx.getDestCompartmentId(),
                                   actx.getTenantId(),
                                   actx.getCompartmentId(),
                                   matchETag,
                                   false, /* IfNotExist */
                                   null,  /* limits */
                                   null   /* tags */,
                                   false  /* autoReclaimable */,
                                   retryToken));

            /* Execute the api:
             * PUT /tables/{tablename}/actions/changeCompartment
             */
            StringBuilder sb = new StringBuilder().append(getTMTablesBase())
                .append(actx.getOriginalTableName(tableName))
                .append("/").append(ACTIONS)
                .append("/").append(CHANGE_COMPARTMENT);

            String moveCompartmentUri = sb.toString();
            logTrace(lc, "changeCompartment: " + moveCompartmentUri);
            final HttpResponse response =
                doHttpRequest(scRequest, HttpMethod.PUT,
                              moveCompartmentUri, jsonPayload,
                              scSSLHandler, lc);

            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                ErrorResponse er = getErrorResponse(response,
                                                    "changeCompartment");
                logError(response, lc, "changeCompartment error: " + er);
                return new GetTableResponse(er);
            }
            TableInfo respInfo = deserializePojo(response.getOutput(),
                                                 TableInfo.class);
            logTrace(lc, "changeCompartment: response from TM: " + respInfo);
            return new GetTableResponse(response.getStatusCode(), respInfo);
        } catch (Exception e) {
            logException("changeCompartment", e, lc);
            return getTableError(e);
        }
    }

    @Override
    public ListTableResponse listTables(AccessContext actx,
                                        int startIndex,
                                        int numTables,
                                        LogContext lc) {

        try {
            StringBuilder sb = new StringBuilder().append(getTMTablesBase());
            addQueryParam(sb, actx, true);
            if (startIndex >= 0) {
                addQueryParam(sb, START_INDEX,
                              String.valueOf(startIndex), false);
            }
            if (numTables > 0) {
                addQueryParam(sb, LIMIT, String.valueOf(numTables), false);
            }

            final String url = sb.toString();

            logTrace(lc, "listTables: " + url);
            final HttpResponse response =
                doHttpRequest(scRequest, HttpMethod.GET,
                              url, null /* payload */,
                              scSSLHandler, lc);

            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                ErrorResponse er = getErrorResponse(response, "listTables");
                logError(response, lc, "listTables error: " + er);
                return new ListTableResponse(er);
            }

            String[] responseInfo = deserializePojo(response.getOutput(),
                                                    String[].class);
            int lastIndex = startIndex + responseInfo.length;
            return new ListTableResponse(response.getStatusCode(),
                                         responseInfo,
                                         lastIndex);
        } catch (Exception e) {
            logException("ListTable", e, lc);
            return new ListTableResponse(handleError(e));
        }
    }

    @Override
    public ListTableInfoResponse listTableInfo(AccessContext actx,
                                               String namePattern,
                                               String state,
                                               String sortBy,
                                               boolean isSortOrderAsc,
                                               int startIndex,
                                               int numTables,
                                               LogContext lc) {
        try {
            StringBuilder sb = new StringBuilder().append(getTMTablesBase());
            addQueryParam(sb, actx, true);
            addQueryParam(sb, NAME_ONLY, "false", false);
            if (namePattern != null) {
                String pattern = URLEncoder.encode(namePattern,
                                    StandardCharsets.UTF_8.name());
                addQueryParam(sb, NAME_PATTERN, pattern, false);
            }

            String tstate = mapToTableStateName(state);
            if (tstate != null) {
                addQueryParam(sb, STATE, tstate, false);
            }

            if (sortBy != null) {
                if (!sortBy.equalsIgnoreCase("name") &&
                    !sortBy.equalsIgnoreCase("timeCreated")) {
                    throw new IllegalArgumentException("Unsupport sortBy '" +
                        sortBy + "' for list tableInfo");
                }
                addQueryParam(sb, SORT_BY, sortBy, false);
                addQueryParam(sb, SORT_ORDER_ASC,
                              Boolean.toString(isSortOrderAsc), false);
            }
            if (startIndex >= 0) {
                addQueryParam(sb, START_INDEX,
                              String.valueOf(startIndex), false);
            }
            if (numTables > 0) {
                addQueryParam(sb, LIMIT, String.valueOf(numTables), false);
            }
            addQueryParam(sb, RETURN_COLLECTION, Boolean.TRUE.toString(), false);

            final String url = sb.toString();

            logTrace(lc, "listTableInfo: " + url);
            HttpResponse response =
                doHttpRequest(scRequest, HttpMethod.GET,
                              url, null /* payload */,
                              scSSLHandler, lc);

            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                ErrorResponse er = getErrorResponse(response, "ListTableInfo");
                logError(response, lc, "ListTableInfo error: " + er);
                return new ListTableInfoResponse(er);
            }

            TableCollection tc = deserializePojo(response.getOutput(),
                                                 TableCollection.class);
            List<TableInfo> tables = tc.getTableInfos();
            int lastIndex = startIndex + tables.size();

            String[] replicas = null;
            if (tc.getAvailableReplicationRegions() != null) {
                int num = tc.getAvailableReplicationRegions().size();
                replicas = tc.getAvailableReplicationRegions()
                             .toArray(new String[num]);
            }

            return new ListTableInfoResponse(200,
                  tables.toArray(new TableInfo[tables.size()]),
                  tc.getMaxAutoReclaimableTables(),
                  tc.getAutoReclaimableTables(),
                  tc.getMaxAutoScalingTables(),
                  tc.getAutoScalingTables(),
                  replicas,
                  lastIndex);
        } catch (Exception e) {
            logException("ListTableInfo", e, lc);
            return new ListTableInfoResponse(handleError(e));
        }
    }

    /* Maps the given state to TableState */
    private static String mapToTableStateName(String state) {
        if (state == null || state.equalsIgnoreCase("ALL")) {
            return null;
        }

        TableState tstate;
        if (state.equalsIgnoreCase("ACTIVE")) {
            tstate = TableState.ACTIVE;
        } else if (state.equalsIgnoreCase("CREATING")) {
            tstate = TableState.CREATING;
        } else if (state.equalsIgnoreCase("UPDATING")) {
            tstate = TableState.UPDATING;
        } else if (state.equalsIgnoreCase("DELETING")) {
            tstate = TableState.DROPPING;
        } else if (state.equalsIgnoreCase("DELETED")) {
            tstate = TableState.DROPPED;
        } else {
            throw new IllegalArgumentException("Unsupport state '" + state +
                                               "' for list tableInfo");
        }
        return tstate.name();
    }

    @Override
    public GetStoreResponse getStoreInfo(String tenantId,
                                         String tableName,
                                         LogContext lc) {
        try {
            StringBuilder sb = new StringBuilder().append(getTMTablesBase())
                .append(mapTableName(tableName))
                .append("/").append(TABLE_STOREINFO);
            if (tenantId != null) {
                addQueryParam(sb, TENANT_ID, tenantId, true);
            }
            String uri = sb.toString();

            logTrace(lc, "getStoreInfo: " + uri);
            final HttpResponse response =
                doHttpRequest(scRequest, HttpMethod.GET,
                              uri, null /* payload */,
                              scSSLHandler, lc);

            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                ErrorResponse er = getErrorResponse(response, "getStoreInfo");
                logError(response, lc, "getStoreInfo error: " + er);
                return new GetStoreResponse(er);
            }

            StoreInfo storeInfo = deserializePojo(response.getOutput(),
                                                  StoreInfo.class);

            return new GetStoreResponse(response.getStatusCode(),
                                        storeInfo.getDatastoreName(),
                                        storeInfo.getHelperhosts(),
                                        storeInfo.getTableRequestLimits(),
                                        storeInfo.isMultiRegion(),
                                        storeInfo.isInitialized());
        } catch (Exception e) {
            logException("GetStore", e, lc);
            return new GetStoreResponse(handleError(e));
        }
    }

    @Override
    public TableUsageResponse getTableUsage(AccessContext actx,
                                            String tableName,
                                            long startTimestamp,
                                            long endTimestamp,
                                            int startIndex,
                                            int limit,
                                            LogContext lc) {

        try {
            StringBuilder sb = new StringBuilder().append(getTMTablesBase())
                .append(actx.getOriginalTableName(tableName))
                .append("/").append(TABLE_USAGE);
            addQueryParam(sb, actx, true);

            if (startTimestamp != 0) {
                addQueryParam(sb,
                              START_TIMESTAMP,
                              Long.toString(startTimestamp),
                              false);
            }

            if (endTimestamp != 0) {
                addQueryParam(sb,
                              END_TIMESTAMP,
                              Long.toString(endTimestamp),
                              false);
            }

            if (startIndex != 0) {
                addQueryParam(sb,
                              START_INDEX,
                              Integer.toString(startIndex),
                              false);
            }

            if (limit != 0) {
                addQueryParam(sb,
                              LIMIT,
                              Integer.toString(limit),
                              false);
            }

            String url = sb.toString();

            logTrace(lc, "getTableUsage: " + url);
            final HttpResponse response =
                doHttpRequest(scRequest, HttpMethod.GET,
                              url, null /* payload */,
                              scSSLHandler, lc);

            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                ErrorResponse er = getErrorResponse(response, "getTableUsage");
                logError(response, lc, "getTableUsage error: " + er);
                return new TableUsageResponse(er);
            }
            TableUsage[] responseInfo = deserializePojo(response.getOutput(),
                                                        TableUsage[].class);
            int lastIndex = startIndex + responseInfo.length;
            return new TableUsageResponse(response.getStatusCode(),
                                          responseInfo, lastIndex);
        } catch (Exception e) {
            logException("TableUsage", e, lc);
            return new TableUsageResponse(handleError(e));
        }
    }

    @Override
    public IndexResponse getIndexInfo(AccessContext actx,
                                      String tableName,
                                      String indexName,
                                      String namePattern,
                                      String state,
                                      String sortBy,
                                      boolean isSortOrderAsc,
                                      int startIndex,
                                      int limit,
                                      LogContext lc) {

        try {
            StringBuilder sb = new StringBuilder().append(getTMTablesBase())
                .append(actx.getOriginalTableName(tableName))
                .append("/").append(TABLE_INDEXES);
            if (indexName != null) {
                sb.append("/").append(indexName);
                addQueryParam(sb, actx, true);
            } else {
                addQueryParam(sb, actx, true);
                if (namePattern != null) {
                    String pattern = URLEncoder.encode(namePattern,
                                        StandardCharsets.UTF_8.name());
                    addQueryParam(sb, NAME_PATTERN, pattern, false);
                }

                String lfstate = mapToIndexStateName(state);
                if (lfstate != null) {
                    addQueryParam(sb, STATE, lfstate, false);
                }
                if (sortBy != null) {
                    if (!sortBy.equalsIgnoreCase("name") &&
                        !sortBy.equalsIgnoreCase("timeCreated")) {
                        throw new IllegalArgumentException("Unsupport sortBy '" +
                            sortBy + "' for list tableInfo");
                    }
                    addQueryParam(sb, SORT_BY, sortBy, false);
                    addQueryParam(sb, SORT_ORDER_ASC,
                                  Boolean.toString(isSortOrderAsc), false);
                }
                if (startIndex >= 0) {
                    addQueryParam(sb, START_INDEX,
                                  String.valueOf(startIndex), false);
                }
                if (limit > 0) {
                    addQueryParam(sb, LIMIT, String.valueOf(limit), false);
                }
            }
            String url = sb.toString();

            logTrace(lc, "getIndexInfo: " + url);
            final HttpResponse response =
                doHttpRequest(scRequest, HttpMethod.GET,
                              url, null /* payload */,
                              scSSLHandler, lc);

            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                ErrorResponse er = getErrorResponse(response, "getIndexInfo");
                logError(response, lc, "getIndexInfo error: " + er);
                return new IndexResponse(er);
            }

            IndexInfo[] indexes = deserializePojo(response.getOutput(),
                                                  IndexInfo[].class);
            int lastIndex = startIndex + indexes.length;
            return new IndexResponse(response.getStatusCode(),
                                     indexes, lastIndex);
        } catch (Exception e) {
            logException("getIndexInfo", e, lc);
            return new IndexResponse(handleError(e));
        }
    }

    /* Maps the given state to name of IndexState */
    private static String mapToIndexStateName(String state) {
        if (state == null || state.equalsIgnoreCase("ALL")) {
            return null;
        }

        IndexState indexState;
        if (state.equalsIgnoreCase("ACTIVE")) {
            indexState = IndexState.ACTIVE;
        } else if (state.equalsIgnoreCase("CREATING")) {
            indexState = IndexState.CREATING;
        } else if (state.equalsIgnoreCase("DELETING")) {
            indexState = IndexState.DROPPING;
        } else if (state.equalsIgnoreCase("DELETED")) {
            indexState = IndexState.DROPPED;
        } else {
            throw new IllegalArgumentException("Unsupport state '" + state +
                                               "' for get indexes");
        }
        return indexState.name();
    }

    @Override
    public String getWorkRequestId(TableInfo tableInfo, OpCode opCode) {
        return tableInfo.getOperationId();
    }

    @Override
    public GetDdlWorkRequestResponse getDdlWorkRequest(AccessContext actx,
                                                       String workRequestId,
                                                       boolean internal,
                                                       LogContext lc)  {
        try {
            final StringBuilder sb = new StringBuilder()
                .append(getTMWorkRequestsBase())
                .append(workRequestId);
            boolean firstParam = true;
            if (actx != null && actx.getTenantId() != null) {
                addQueryParam(sb, TENANT_ID, actx.getTenantId(), firstParam);
                firstParam = false;
            }
            if (internal) {
                addInternalQueryParam(sb, firstParam);
            }

            final String url = sb.toString();
            logTrace(lc, "GetDdlWorkRequest: " + url);
            final HttpResponse response =
                doHttpRequest(scRequest, HttpMethod.GET, url,
                              null /* payload */,
                              scSSLHandler, lc);

            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                ErrorResponse er = getErrorResponse(response,
                                                    "GetDdlWorkRequest");
                logError(response, lc, "GetDdlWorkRequest error: " + er);
                return new GetDdlWorkRequestResponse(er);
            }

            DdlHistoryEntry ddlEntry = deserializePojo(response.getOutput(),
                                                       DdlHistoryEntry.class);
            return new GetDdlWorkRequestResponse(response.getStatusCode(),
                                                 ddlEntry);
        } catch (Exception e) {
            logException("GetWorkRequest", e, lc);
            return new GetDdlWorkRequestResponse(handleError(e));
        }
    }

    @Override
    public GetWorkRequestResponse getWorkRequest(AccessContext actx,
                                                 String workRequestId,
                                                 boolean internal,
                                                 LogContext lc) {

        try {
            final StringBuilder sb = new StringBuilder()
                .append(getTMWorkRequestsBase())
                .append(workRequestId);
            addQueryParam(sb, GENERAL, "true", true);
            if (actx != null && actx.getTenantId() != null) {
                addQueryParam(sb, TENANT_ID, actx.getTenantId(), false);
            }
            if (internal) {
                addInternalQueryParam(sb, false);
            }

            final String url = sb.toString();

            logTrace(lc, "GetWorkRequest: " + url);
            final HttpResponse resp =
                doHttpRequest(scRequest, HttpMethod.GET, url,
                              null /* payload */,
                              scSSLHandler, lc);

            if (resp.getStatusCode() != HttpURLConnection.HTTP_OK) {
                ErrorResponse er =
                    getErrorResponse(resp, "GetWorkRequest");
                logError(resp, lc, "GetWorkRequest error: " + er);
                return new GetWorkRequestResponse(er);
            }

            WorkRequest workRequest = deserializePojo(resp.getOutput(),
                                                      WorkRequest.class);
            return new GetWorkRequestResponse(resp.getStatusCode(),
                                              workRequest);
        } catch (Exception e) {
            logException("GetWorkRequest", e, lc);
            return new GetWorkRequestResponse(handleError(e));
        }
    }

    @Override
    public ListWorkRequestResponse listWorkRequests(AccessContext actx,
                                                    String nextPageToken,
                                                    int limit,
                                                    LogContext lc) {
        try {
            StringBuilder sb = new StringBuilder()
                .append(getTMWorkRequestsBase());
            addQueryParam(sb, actx, true);
            if (nextPageToken != null) {
                addQueryParam(sb, START_INDEX, nextPageToken, false);
            }
            if (limit > 0) {
                addQueryParam(sb, LIMIT, String.valueOf(limit), false);
            }

            if (actx.getAuthorizedOps() != null) {
                for (OpCode op : actx.getAuthorizedOps()) {
                    addQueryParam(sb, WORK_REQUEST_TYPE,
                                  mapWorkRequestType(op), false);
                }
            }

            final String url = sb.toString();

            logTrace(lc, "ListWorkRequests: " + url);
            final HttpResponse response =
                doHttpRequest(scRequest, HttpMethod.GET, url,
                              null /* payload */,
                              scSSLHandler, lc);

            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                ErrorResponse er = getErrorResponse(response,
                                                    "ListWorkRequests");
                logError(response, lc, "ListWorkRequests error: " + er);
                return new ListWorkRequestResponse(er);
            }

            ListWorkRequestsResult result =
                deserializePojo(response.getOutput(),
                                ListWorkRequestsResult.class);
            return new ListWorkRequestResponse(response.getStatusCode(),
                                               result.getWorkRequests(),
                                               result.getNextPageToken());
        } catch (Exception e) {
            logException("ListWorkRequests", e, lc);
            return new ListWorkRequestResponse(handleError(e));
        }
    }

    private String mapWorkRequestType(OpCode op) {
        switch(op) {
        case GET_TABLE:
            return WORK_REQUEST_DDL;
        case GET_CONFIG_KMS_KEY:
            return WORK_REQUEST_KMSKEY;
        default:
            throw new IllegalArgumentException(
                "Invalid sub operation for list-workrequest: " + op);
        }
    }

    @Override
    public ListRuleResponse listRules(LogContext lc) {

        try {
            /* Execute the api: GET /filters */
            String listRuleUrl = getFilterUrlBase();
            logTrace(lc, "listRules: " + listRuleUrl);
            final HttpResponse response =
                doHttpRequest(scRequest, HttpMethod.GET,
                              listRuleUrl, null /* payload */,
                              scSSLHandler, lc);

            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                ErrorResponse er = getErrorResponse(response, "listRules");
                logError(response, lc, "listRules error: " + er);
                return new ListRuleResponse(er);
            }

            Rule[] rules = Rule.getGson().fromJson(response.getOutput(),
                                                   Rule[].class);
            logTrace(lc, "listRules: " + rules.length + " rules returned");
            return new ListRuleResponse(response.getStatusCode(), rules);
        } catch (Exception e) {
            logException("getRule", e, lc);
            return new ListRuleResponse(handleError(e));
        }
    }

    @Override
    public GetTableResponse setTableActive(AccessContext actx,
                                           String tableName,
                                           LogContext lc) {
        try {
            /*
             * PUT
             * /tables/{tableid}/actions/setActivity?tenantid=<ocid>&dmlms=<ms>
             */
            StringBuilder sb = new StringBuilder()
                    .append(getTMTablesBase())
                    .append(actx.getTableId(tableName))
                    .append("/").append(ACTIONS)
                    .append("/").append(SET_ACTIVITY);
            addQueryParam(sb, actx, true);
            addQueryParam(sb, DML_MS,
                          String.valueOf(System.currentTimeMillis()),
                          false);

            final String setActivityUrl = sb.toString();
            logTrace(lc, "setTableActivity: " + setActivityUrl);

            final HttpResponse resp =
                doHttpRequest(scRequest, HttpMethod.PUT,
                              setActivityUrl, null /* payload */,
                              scSSLHandler, lc);

            if (resp.getStatusCode() != HttpURLConnection.HTTP_OK) {
                ErrorResponse er = getErrorResponse(resp, "setTableActivity");
                logError(resp, lc, "setTableActivity error: " + er);
                return new GetTableResponse(er);
            }

            TableInfo info = deserializePojo(resp.getOutput(), TableInfo.class);
            logTrace(lc, "setTableActivity response: " + info);
            return new GetTableResponse(resp.getStatusCode(), info);
        } catch (Exception e) {
            logException("setTableActivity", e, lc);
            return new GetTableResponse(handleError(e));
        }
    }

    @Override
    public GetTableResponse addReplica(AccessContext actx,
                                       String tableNameOrId,
                                       String newReplicaServiceName,
                                       int readLimit,
                                       int writeLimit,
                                       byte[] matchETag,
                                       String retryToken,
                                       LogContext lc) {
        final String op = "AddReplica";
        try {
            TableDDLInputs inputs = new TableDDLInputs(
                    null, /* statement */
                    actx.getTenantId(),
                    actx.getCompartmentId(),
                    matchETag,
                    false /* ifNotExists */,
                    new TableLimits(readLimit, writeLimit, 0),
                    null  /* tags */,
                    false /* autoReclaimable */,
                    retryToken);
            /* obo token */
            inputs.setOboToken(actx.getOboToken());

            String payload = serializePojo(inputs);

            /* Execute api: POST tm/tables/{tablename}/replica/{region} */
            StringBuilder sb = new StringBuilder()
                    .append(getTMTablesBase())
                    .append(actx.getOriginalTableName(tableNameOrId))
                    .append("/").append(REPLICA)
                    .append("/").append(newReplicaServiceName);
            String url = sb.toString();

            return executeTableRequest(HttpMethod.POST, url, payload, op, lc);
        } catch (Exception e) {
            logException(op, e, lc);
            return getTableError(e);
        }
    }

    @Override
    public GetTableResponse dropReplica(AccessContext actx,
                                        String tableNameOrId,
                                        String replicaToRemoveServiceName,
                                        byte[] matchETag,
                                        LogContext lc) {

        final String op = "DropReplica";
        try {
            DropInputs inputs = new DropInputs(false,  /* ifNotExists */
                                               actx.getTenantId(),
                                               actx.getCompartmentId(),
                                               matchETag);
            /* obo token */
            inputs.setOboToken(actx.getOboToken());

            String payload = serializePojo(inputs);

            /* Execute api: DELETE tm/tables/{tablename}/replica/{region} */
            StringBuilder sb = new StringBuilder()
                    .append(getTMTablesBase())
                    .append(actx.getOriginalTableName(tableNameOrId))
                    .append("/").append(REPLICA)
                    .append("/").append(replicaToRemoveServiceName);
            String url = sb.toString();
            return executeTableRequest(HttpMethod.DELETE, url, payload, op, lc);
        } catch (Exception e) {
            logException(op, e, lc);
            return getTableError(e);
        }
    }

    @Override
    public WorkRequestIdResponse doInternalDdl(AccessContext actx,
                                               String tableNameOrId,
                                               String operation,
                                               String payload,
                                               LogContext lc) {
        try {
            StringBuilder sb = new StringBuilder()
                    .append(getTMTablesBase())
                    .append(actx.getOriginalTableName(tableNameOrId))
                    .append("/").append(CROSS_REGION_DDL).append("/")
                    .append(operation);
            if (actx.getCompartmentId() != null) {
                addQueryParam(sb, COMPARTMENT_ID, actx.getCompartmentId(),
                              true /* first */);
            }
            String url = sb.toString();
            return executeInternalDdl(url, payload, operation, lc);
        } catch (Exception ex) {
            logException(operation, ex, lc);
            return new WorkRequestIdResponse(handleError(ex));
        }
    }

    @Override
    public ReplicaStatsResponse getReplicaStats(AccessContext actx,
                                                String tableNameOrId,
                                                String replicaName,
                                                long startTime,
                                                int limit,
                                                LogContext lc) {
        try {
            StringBuilder sb = new StringBuilder().append(getTMTablesBase())
                .append(actx.getOriginalTableName(tableNameOrId))
                .append("/").append(REPLICA_STATS);
            addQueryParam(sb, actx, true);

            if (startTime != 0) {
                addQueryParam(sb, START_TIMESTAMP, Long.toString(startTime),
                              false);
            }
            if (replicaName != null) {
                addQueryParam(sb, REPLICA, replicaName, false);
            }
            if (limit != 0) {
                addQueryParam(sb, LIMIT, Integer.toString(limit), false);
            }

            String url = sb.toString();

            logTrace(lc, "getReplicaStats: " + url);
            final HttpResponse response =
                doHttpRequest(scRequest, HttpMethod.GET,
                              url, null /* payload */,
                              scSSLHandler, lc);

            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                ErrorResponse er = getErrorResponse(response, "getTableUsage");
                logError(response, lc, "getTableUsage error: " + er);
                return new ReplicaStatsResponse(er);
            }

            Map<String, List<ReplicaStats>> result =
                deserializePojo(response.getOutput(),
                    new TypeToken<Map<String, List<ReplicaStats>>>(){}.getType());

            return new ReplicaStatsResponse(response.getStatusCode(),
                                            result, startTime);
        } catch (Exception e) {
            logException("ReplicaStats", e, lc);
            return new ReplicaStatsResponse(handleError(e));
        }
    }

    /*
     * GET V0/cmek/kmskey
     */
    @Override
    public GetKmsKeyInfoResponse getKmsKey(AccessContext actx,
                                           boolean internal,
                                           LogContext lc) {
        final String op = "getKmsKey";
        try {
            String url = getCmekUrlBase();
            if (internal) {
                url += "?" + INTERNAL_QUERY;
            }
            logTrace(lc, op + ": " + url);

            final HttpResponse res = doHttpRequest(scRequest, HttpMethod.GET,
                                                   url, null /* payload */,
                                                   scSSLHandler, lc);
            if (res.getStatusCode() != HttpURLConnection.HTTP_OK) {
                ErrorResponse err = getErrorResponse(res, op);
                logError(res, lc, op + " error: " + err);
                return new GetKmsKeyInfoResponse(err);
            }

            KmsKeyInfo keyInfo = deserializePojo(res.getOutput(),
                                                 KmsKeyInfo.class);
            return new GetKmsKeyInfoResponse(keyInfo, res.getStatusCode());
        } catch (Exception ex) {
            logException(op, ex, lc);
            return new GetKmsKeyInfoResponse(handleError(ex));
        }
    }

    /*
     *   PUT V0/cmek/kmskey?tenantid=<tenant_id>&&keyid=<key_id>
     *                      [&&vaultid=<vault_id>]
     *                      [&&dryrun=<boolean>]
     *                      [&&ifmatch=<etag>]
     */
    @Override
    public WorkRequestIdResponse updateKmsKey(AccessContext actx,
                                              String kmsKeyId,
                                              String kmsVaultId,
                                              byte[] matchETag,
                                              boolean dryRun,
                                              LogContext lc) {
        final String op = "updateKmsKey";
        try {
            StringBuilder sb = new StringBuilder(getCmekUrlBase());
            addQueryParam(sb, TENANT_ID, actx.getTenantId(), true);
            addQueryParam(sb, KEY_ID, kmsKeyId, false);
            if (kmsVaultId != null) {
                addQueryParam(sb, VAULT_ID, kmsVaultId, false);
            }
            addQueryParam(sb, DRY_RUN, String.valueOf(dryRun), false);
            if (matchETag != null) {
                addQueryParam(sb, IF_MATCH, JsonUtils.encodeBase64(matchETag),
                              false);
            }
            final String url = sb.toString();

            logTrace(lc, op + ": " + url);

            final HttpResponse res = doHttpRequest(scRequest, HttpMethod.PUT,
                                                   url, null /* payload */,
                                                   scSSLHandler, lc);
            if (res.getStatusCode() != HttpURLConnection.HTTP_OK) {
                ErrorResponse er = getErrorResponse(res, op);
                logError(res, lc, op + " error: " + er);
                return new WorkRequestIdResponse(er);
            }

            String workRequestId = res.getOutput();
            logTrace(lc, op + ": response from TM: " + workRequestId);

            return new WorkRequestIdResponse(res.getStatusCode(), workRequestId);

        } catch (Exception ex) {
            logException(op, ex, lc);
            return new WorkRequestIdResponse(handleError(ex));
        }
    }

    /*
     * DELETE V0/cmek/kmskey?tenantid=<tenant_id>
     *                       [&&dryrun=<boolean>]
     *                       [&&ifmatch=<etag>]
     */
    @Override
    public WorkRequestIdResponse removeKmsKey(AccessContext actx,
                                              byte[] matchETag,
                                              boolean dryRun,
                                              LogContext lc) {

        final String op = "removeKmsKey";
        try {
            StringBuilder sb = new StringBuilder(getCmekUrlBase());
            addQueryParam(sb, TENANT_ID, actx.getTenantId(), true);
            addQueryParam(sb, DRY_RUN, String.valueOf(dryRun), false);
            if (matchETag != null) {
                addQueryParam(sb, IF_MATCH, JsonUtils.encodeBase64(matchETag),
                              false);
            }
            final String url = sb.toString();

            logTrace(lc, op + ": " + url);

            final HttpResponse res = doHttpRequest(scRequest, HttpMethod.DELETE,
                                                   url, null /* payload */,
                                                   scSSLHandler, lc);
            if (res.getStatusCode() != HttpURLConnection.HTTP_OK) {
                ErrorResponse er = getErrorResponse(res, op);
                logError(res, lc, op + " error: " + er);
                return new WorkRequestIdResponse(er);
            }

            String workRequestId = res.getOutput();
            logTrace(lc, op + ": response from TM: " + workRequestId);

            return new WorkRequestIdResponse(res.getStatusCode(), workRequestId);

        } catch (Exception ex) {
            logException(op, ex, lc);
            return new WorkRequestIdResponse(handleError(ex));
        }
    }

    private GetTableResponse executeTableRequest(HttpMethod method,
                                                 String url,
                                                 String payload,
                                                 String op,
                                                 LogContext lc) {

        logTrace(lc, op + ": url=" + url + ": payload=" + payload);

        final HttpResponse response =
            doHttpRequest(scRequest, method, url, payload,
                          scSSLHandler, lc);

        if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
            ErrorResponse er = getErrorResponse(response, op);
            logError(response, lc, op + " error: " + er);
            return new GetTableResponse(er);
        }

        TableInfo info = deserializePojo(response.getOutput(), TableInfo.class);
        logTrace(lc, op + ": response from TM: " + info);

        return new GetTableResponse(response.getStatusCode(), info);
    }

    private WorkRequestIdResponse executeInternalDdl(String url,
                                                     String payload,
                                                     String op,
                                                     LogContext lc) {

        logTrace(lc, op + ": " + url);

        final HttpResponse res =
            doHttpRequest(scRequest, HttpMethod.POST, url, payload,
                          scSSLHandler, lc);

        if (res.getStatusCode() != HttpURLConnection.HTTP_OK) {
            ErrorResponse er = getErrorResponse(res, op);
            logError(res, lc, op + " error: " + er);
            return new WorkRequestIdResponse(er);
        }

        String workRequestId = res.getOutput();
        logTrace(lc, op + ": response from TM: " + workRequestId);

        return new WorkRequestIdResponse(res.getStatusCode(), workRequestId);
    }

    /**
     * Check Proxy -> SC connectivity.
     */
    public HealthStatus checkSCConnectivity(List<String> errors) {
        return connectTrack.checkConnectivity(scRequest, scSSLHandler,
                                              getPingUrls(), errors, logger);
    }

    /**
     * Access to the base URL must be protected in case the SC is reset.
     */
    synchronized private String getTMTablesBase() {
        if (tmTablesBase == null) {
            establishURLBase();
        }
        return tmTablesBase;
    }

    /**
     * Access to the base URL must be protected in case the SC is reset.
     */
    synchronized private String getTMUrlBase() {
        if (tmUrlBase == null) {
            establishURLBase();
        }
        return tmUrlBase;
    }


    /**
     * Access to the base URL must be protected in case the SC is reset.
     */
    synchronized private String getCmekUrlBase() {
        if (cmekUrlBase == null) {
            establishURLBase();
        }
        return cmekUrlBase;
    }

    /**
     * Access to SC ping URL must be protected in case the SC is reset.
     */
    synchronized private List<String> getPingUrls() {
        if (pingUrls == null) {
            establishURLBase();
        }
        return pingUrls;
    }

    /**
     * Access to the base URL must be protected in case the SC is reset.
     */
    synchronized private String getTMWorkRequestsBase() {
        if (tmWorkRequestsBase == null) {
            establishURLBase();
        }
        return tmWorkRequestsBase;
    }

    /**
     * Access to the base URL must be protected in case the SC is reset.
     */
    synchronized private String getFilterUrlBase() {
        if (filterRequestsBase == null) {
            establishURLBase();
        }
        return filterRequestsBase;
    }

    /**
     * Establishing the base URL requires discovering the SC. This method is
     * called when synchronized. Right now it assumes that the API version for
     * the SCService is the same as for TMService.
     * TODO: separate these if needed.
     */
    private void establishURLBase() {
        /*
         * Finding the host port requires making http calls to ping
         * the potential hosts.
         */
        SCResolver scresolver = new SCResolver();
        HostPort hp = scresolver.getSCHostPort(logger);
        if (hp == null) {
            throw new RuntimeException("SCTenantManager can't connect to an SC");
        }

        boolean useSSL = SSLConfig.isInternalSSLEnabled();
        establishURLBase(hp.toUrl(useSSL), false /* reset */);

        /* Initialize SSLHandler used by HttpRequest to access SC */
        if (useSSL && scSSLHandler == null) {
            scRequest.disableHostVerification();
            scSSLHandler = SSLConnectionHandler.getOCICertHandler(logger);
        }
    }

    /**
     * For unit test.
     */
    public void establishURLBase(String scUrl, boolean reset) {
        final String scAPIBase = scUrl + "/" + tmAPIVersion;

        if (tmUrlBase == null || reset) {
            /*
             * tm
             */
            tmUrlBase = scAPIBase + "/tm/";
        }
        if (tmTablesBase == null || reset) {
            /*
             * All internal use assumes .../tm/tables/ so hardwire it.
             */
            tmTablesBase = tmUrlBase + "tables/";
        }
        if (tmWorkRequestsBase == null || reset) {
            /* workRequest url base */
            tmWorkRequestsBase = tmUrlBase + "workRequests/";
        }
        if (pingUrls == null || reset) {
            pingUrls = new ArrayList<>();
            pingUrls.add(scAPIBase + "/health");
        }
        if (filterRequestsBase == null || reset) {
            /* filters url base */
            filterRequestsBase = scAPIBase + "/filters";
        }
        if (cmekUrlBase == null || reset) {
            cmekUrlBase = scAPIBase + "/cmek/kmskey";
        }
    }

    /**
     * Serialize the object into json and standardize the error handling
     * if there's a problem with it.
     * @throws IllegalArgumentException
     */
    private String serializePojo(Object obj) {
        return JsonUtils.print(obj);
    }

    /**
     * Deserialize object from the json and standardize the error handling
     * if there's a problem with it. INCORRECT_STATE maps to ILLEGAL_STATE
     * in the protocol/driver.
     *
     * @throws RequestFaultException
     */
    private <T> T deserializePojo(String jsonString, Type type) {
        try {
            return JsonUtils.readValue(jsonString, type);
        } catch (Exception e) {
            throw new RequestFaultException(
                "Unable to deserialize object from String to " +
                type.getTypeName() + ", error: " + e.getMessage(),
                e, ErrorCode.INCORRECT_STATE);
        }
    }

    private void addQueryParam(StringBuilder sb,
                               AccessContext actx,
                               boolean first) {
        if (actx.getTenantId() == null && actx.getCompartmentId() == null) {
            return;
        }
        if (actx.getCompartmentId() != null) {
            addQueryParam(sb, COMPARTMENT_ID, actx.getCompartmentId(), first);
            first = false;
        }
        if (actx.getTenantId() != null) {
            addQueryParam(sb, TENANT_ID, actx.getTenantId(), first);
        }
    }

    /**
     * Adds a query parameter via StringBuilder to the URL being created.
     *
     * Assumes that the parameter does NOT require URLEncoding.
     */
    private void addQueryParam(StringBuilder sb,
                               String paramName,
                               String paramValue,
                               boolean first) {
        if (first) {
            sb.append("?");
        } else {
            sb.append("&");
        }
        sb.append(paramName).append("=").append(paramValue);
    }

    private void addInternalQueryParam(StringBuilder sb, boolean first) {
        if (first) {
            sb.append("?");
        } else {
            sb.append("&");
        }
        sb.append(INTERNAL_QUERY);
    }

    private String mapTableName(String tableName) {
        if (tableName.contains(":")) {
            return tableName.split(":")[1];
        }
        return tableName;
    }

    private static void validateTableName(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Table name must not be null");
        }
        String lowercaseName = name.toLowerCase();
        if (lowercaseName.contains(INTERNAL_OCID_PREFIX) ||
            lowercaseName.equals("oci") ||
            lowercaseName.startsWith("ocid")) {
            throw new IllegalArgumentException(
                "Invalid table name " + name + ", having reserved keyword " +
                INTERNAL_OCID_PREFIX + ", oci and ocid");
        }
    }

    private static void validateIndexName(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Index name must not be null");
        }
        String lowercaseName = name.toLowerCase();
        if (lowercaseName.contains(INTERNAL_OCID_PREFIX) ||
            lowercaseName.equals("oci") ||
            lowercaseName.startsWith("ocid")) {
            throw new IllegalArgumentException(
                "Invalid index name " + name + ", having reserved keyword " +
                INTERNAL_OCID_PREFIX + ", oci and ocid");
        }
    }

    private ErrorResponse getErrorResponse(HttpResponse response,
                                           String operation) {
        final String payload = response.getOutput();
        ErrorResponse errResp;
        try {
            errResp = JsonUtils.readValue(payload, ErrorResponse.class);
            if (errResp.getStackTrace() != null) {
                errResp.stripDebuggingInfo();
            }
        } catch (Exception e) {
            /*
             * The payload is not a valid JSON string for ErrorResponse, build
             * ErrorResponse using statusCode, type and payload message.
             *
             * The statusCode of 502-504 should be retry-able as they indicate
             * an internal system issue, use TYPE_SERVICE_UNAVAILABLE as type,
             * otherwise set type to TYPE_UNKNOWN_ERROR.
             */
            int statusCode = response.getStatusCode();
            /* this maps 502-504 to 503 */
            ErrorCode code = (statusCode >= 502 && statusCode <= 504) ?
                ErrorCode.SERVICE_UNAVAILABLE :
                ErrorCode.UNKNOWN_ERROR;
            errResp = ErrorResponse.build(code, payload);
            logger.info(operation + " " + errResp.toString());
        }
        return errResp;
    }

    /*
     * Wrap http request handling to check latency and log if
     * greater than a threshold.
     */
    private HttpResponse doHttpRequest(HttpRequest request,
                                       HttpMethod method,
                                       String url,
                                       String payload,
                                       ConnectionHandler handler,
                                       LogContext lc)
        throws RequestFaultException {

        /* if no logger, no need to check timing */
        if (logger == null) {
            return connectTrack.doHttpRequest(request, method, url,
                                              payload, handler, lc);
        }

        long startMs = System.currentTimeMillis();
        try {
            HttpResponse resp =
                connectTrack.doHttpRequest(request, method, url,
                                           payload, handler, lc);
            long latencyMs = System.currentTimeMillis() - startMs;
            if (latencyMs >= latencyInfoThresholdMs) {
                logger.info("SCTenantManager: request " +
                            resp.getStatusCode() + " latency " + latencyMs +
                            "ms for url " + url);
            }
            return resp;
        } catch (Exception e) {
            long latencyMs = System.currentTimeMillis() - startMs;
            if (latencyMs >= latencyInfoThresholdMs) {
                logger.info("SCTenantManager: request exception latency " +
                            latencyMs + "ms for url " + url);
            }
            throw e;
        }
    }


    private void logException(String op, Exception e, LogContext lc) {
        if (logger != null) {
            logger.info("SCTenantManager, unexpected exception during " +
                        op + ": " + e, lc);
        }
    }

    private void logError(HttpResponse resp, LogContext lc, String msg) {
        if (logger != null) {
            if (resp.getStatusCode() >= HttpURLConnection.HTTP_INTERNAL_ERROR) {
                logger.info(msg, lc);
            } else {
                logger.fine(msg, lc);
            }
        }
    }

    private void logTrace(LogContext lc, String msg) {
        if (logger != null) {
            logger.fine(msg, lc);
        }
    }

    private static GetTableResponse getTableError(Exception e) {
        return new GetTableResponse(handleError(e));
    }

    private static ErrorResponse handleError(Exception ex) {
        try {
            throw ex;
        } catch (IllegalArgumentException iae) {
            return ErrorResponse.build(ErrorCode.ILLEGAL_ARGUMENT,
                                       iae.getMessage());
        } catch (RequestFaultException rfe) {
            return ErrorResponse.build(rfe.getError(), rfe.getMessage());
        } catch (Exception e) {
            return ErrorResponse.build(ErrorCode.UNKNOWN_ERROR,
                                       "Unexpected error: " + e.getMessage());
        }
    }

    /**
     * create a table cache that handles multiple stores and uses the SC
     */
    @Override
    public void createTableCache(Config config,
                                 MonitorStats stats,
                                 SkLogger logger) {
        tableCache =
            new CloudServiceTableCache(this,
                                       config.getTemplateKVStoreConfig(),
                                       stats, logger,
                                       config.getTableCacheExpirationSec(),
                                       config.getTableCacheRefreshSec(),
                                       config.getTableCacheCheckIntervalSec());
    }

    @Override
    public TableCache getTableCache() {
        return tableCache;
    }

    @Override
    synchronized public void shutDown() {
        if (tableCache != null) {
            tableCache.shutDown();
            tableCache = null;
        }
    }

    @Override
    public PrepareCB createPrepareCB(String namespace) {
        return new PrepareCB(namespace, false, isChildTableEnabled);
    }

	@Override
	public int getLocalRegionId() {
		return serviceDirectory.getLocalServiceInteger();
	}

	@Override
	public String translateToRegionName(String serviceName) {
		return serviceDirectory.translateToRegionName(serviceName);
	}

	@Override
	public String validateRemoteReplica(String remoteRegionName) {
		return serviceDirectory.validateRemoteReplica(remoteRegionName);
	}

	@Override
	public String getLocalServiceName() {
		return serviceDirectory.getLocalServiceName();
	}
}
