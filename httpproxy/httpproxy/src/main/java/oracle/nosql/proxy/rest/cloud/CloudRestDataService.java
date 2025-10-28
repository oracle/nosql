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

package oracle.nosql.proxy.rest.cloud;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static oracle.nosql.proxy.protocol.HttpConstants.AUTHORIZATION;
import static oracle.nosql.proxy.protocol.JsonProtocol.COMPARTMENT_ID;
import static oracle.nosql.proxy.protocol.JsonProtocol.CREATE_REPLICA_DETAILS;
import static oracle.nosql.proxy.protocol.JsonProtocol.ENVIRONMENT;
import static oracle.nosql.proxy.protocol.JsonProtocol.FREE_TIER_SYS_TAGS;
import static oracle.nosql.proxy.protocol.JsonProtocol.HOSTED_ENVIRONMENT;
import static oracle.nosql.proxy.protocol.JsonProtocol.ID;
import static oracle.nosql.proxy.protocol.JsonProtocol.KMS_KEY;
import static oracle.nosql.proxy.protocol.JsonProtocol.KMS_KEY_STATE;
import static oracle.nosql.proxy.protocol.JsonProtocol.KMS_VAULT_ID;
import static oracle.nosql.proxy.protocol.JsonProtocol.MAX_READ_UNITS;
import static oracle.nosql.proxy.protocol.JsonProtocol.MAX_WRITE_UNITS;
import static oracle.nosql.proxy.protocol.JsonProtocol.REGION;
import static oracle.nosql.proxy.protocol.JsonProtocol.REST_CURRENT_VERSION;
import static oracle.nosql.proxy.protocol.JsonProtocol.TIME_CREATED;
import static oracle.nosql.proxy.protocol.JsonProtocol.TIME_UPDATED;
import static oracle.nosql.proxy.protocol.JsonProtocol.UPDATE_CONFIGURATION_DETAILS;
import static oracle.nosql.proxy.protocol.JsonProtocol.buildConfiguration;
import static oracle.nosql.proxy.protocol.JsonProtocol.checkNonNegativeInt;
import static oracle.nosql.proxy.protocol.JsonProtocol.checkNotEmpty;
import static oracle.nosql.proxy.protocol.JsonProtocol.checkNotNull;
import static oracle.nosql.proxy.protocol.JsonProtocol.checkNotNullEmpty;
import static oracle.nosql.proxy.protocol.Protocol.INVALID_AUTHORIZATION;
import static oracle.nosql.proxy.protocol.Protocol.SERIAL_VERSION;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.http.LogControl;
import oracle.nosql.common.kv.drl.LimiterManager;
import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.proxy.Config;
import oracle.nosql.proxy.MonitorStats;
import oracle.nosql.proxy.RequestException;
import oracle.nosql.proxy.audit.ProxyAuditManager;
import oracle.nosql.proxy.cloud.CloudDataService;
import oracle.nosql.proxy.filter.FilterHandler;
import oracle.nosql.proxy.filter.FilterHandler.Filter;
import oracle.nosql.proxy.protocol.JsonProtocol.JsonObject;
import oracle.nosql.proxy.protocol.JsonProtocol.JsonPayload;
import oracle.nosql.proxy.protocol.Protocol.OpCode;
import oracle.nosql.proxy.rest.RequestParams;
import oracle.nosql.proxy.rest.RestDataService;
import oracle.nosql.proxy.sc.GetKmsKeyInfoResponse;
import oracle.nosql.proxy.sc.GetTableResponse;
import oracle.nosql.proxy.sc.TableUtils;
import oracle.nosql.proxy.sc.TableUtils.PrepareCB;
import oracle.nosql.proxy.sc.TenantManager;
import oracle.nosql.proxy.sc.WorkRequestIdResponse;
import oracle.nosql.proxy.security.AccessChecker;
import oracle.nosql.proxy.security.AccessContext;
import oracle.nosql.proxy.util.ErrorManager;
import oracle.nosql.util.tmi.KmsKeyInfo;
import oracle.nosql.util.tmi.TableLimits;

public class CloudRestDataService extends RestDataService {

    private final static Map<String, Short> RestAPIVersions =
        new HashMap<String, Short>();
    static {
        RestAPIVersions.put(REST_CURRENT_VERSION, SERIAL_VERSION);
    }

    private final AccessChecker ac;

    public CloudRestDataService(SkLogger logger,
                                TenantManager tm,
                                AccessChecker ac,
                                MonitorStats stats,
                                ProxyAuditManager audit,
                                FilterHandler filter,
                                ErrorManager errorManager,
                                LimiterManager limiterManager,
                                Config config,
                                LogControl logControl) {
        super(logger, tm, ac, stats, audit, filter,
              errorManager, limiterManager, config, logControl);
        this.ac = ac;
    }

    public void flushTableCache(String tableName) {
        tableCache.flushEntry(null, tableName);
    }

    /*
     * Validate the request.
     */
    @Override
    protected FullHttpResponse validateHttpRequest(RequestContext rc) {
        final HttpHeaders headers = rc.headers;
        final CharSequence auth = headers.get(AUTHORIZATION);
        if (auth == null) {
            return invalidRequest(rc,
                                  "Authorization header is missing",
                                  INVALID_AUTHORIZATION);
        }
        return super.validateHttpRequest(rc);
    }

    @Override
    protected AccessContext checkAccess(FullHttpRequest request,
                                        OpCode opCode,
                                        AccessContext callerActx,
                                        String tableName,
                                        Filter filter,
                                        LogContext lc) {

        AccessContext actx = ac.checkAccess(request.method(),
                                            request.uri(),
                                            request.headers(),
                                            opCode,
                                            callerActx.getCompartmentId(),
                                            tableName,
                                            TableUtils.getPayload(request),
                                            callerActx,
                                            this,
                                            lc);

        updateLogContext(lc, actx, opCode);

        /*
         * Activate the table in IDLE state when performs a data access
         * operation.
         */
        if (actx.isTableInactive() && OpCode.isDataOp(opCode)) {
            actx.resetTableInactive();
            activateTable(actx, tableName, lc);
        }
        return actx;
    }

    @Override
    protected AccessContext checkAccess(FullHttpRequest request,
                                        String compartmentId,
                                        OpCode opCode,
                                        String tableName,
                                        Filter filter,
                                        LogContext lc) {

        AccessContext actx = ac.checkAccess(request.method(),
                                            request.uri(),
                                            request.headers(),
                                            opCode,
                                            compartmentId,
                                            tableName,
                                            TableUtils.getPayload(request),
                                            null,
                                            this,
                                            lc);

        updateLogContext(lc, actx, opCode);

        /*
         * Activate the table in IDLE state when performs a data access
         * operation.
         */
        if (actx.isTableInactive() && OpCode.isDataOp(opCode)) {
            actx.resetTableInactive();
            activateTable(actx, tableName, lc);
        }
        return actx;
    }

    @Override
    protected AccessContext checkWorkRequestAccess(FullHttpRequest request,
                                                   OpCode opCode,
                                                   OpCode[] authorizeOps,
                                                   boolean shouldAuthorizeAllOps,
                                                   String compartmentId,
                                                   String workRequestId,
                                                   LogContext lc) {
        AccessContext actx =
            ac.checkWorkRequestAccess(request.method(),
                                      request.uri(),
                                      request.headers(),
                                      opCode,
                                      authorizeOps,
                                      shouldAuthorizeAllOps,
                                      compartmentId,
                                      workRequestId,
                                      TableUtils.getPayload(request),
                                      this,
                                      lc);

        updateLogContext(lc, actx, opCode);
        return actx;
    }

    @Override
    protected
    AccessContext checkConfigurationAccess(FullHttpRequest request,
                                           OpCode opCode,
                                           OpCode[] authorizedOps,
                                           String compartmentId,
                                           LogContext lc) {
        AccessContext actx =
            ac.checkConfigurationAccess(request.method(),
                                        request.uri(),
                                        request.headers(),
                                        opCode,
                                        authorizedOps,
                                        compartmentId,
                                        TableUtils.getPayload(request),
                                        this,
                                        lc);
        updateLogContext(lc, actx, opCode);
        return actx;
    }

    @Override
    protected String mapTableName(AccessContext actx, OpCode op, String name) {
        String mapTableName = actx.getMapTableName(name);
        if (mapTableName != null) {
            return mapTableName;
        }
        return name;
    }

    @Override
    protected String mapErrorMessage(AccessContext actx,
                                     String tableName,
                                     String message) {

        return CloudDataService.mapMessageInternal(actx, tableName, message);
    }

    @Override
    public boolean lookupService(String uri) {
        int pos = uri.indexOf("/");
        if (pos > 0) {
            String root = uri.substring(0, pos);
            return RestAPIVersions.containsKey(root);
        }
        return false;
    }

    @Override
    protected GetTableResponse handleTableDdl(
            FullHttpRequest request,
            AccessContext actx,
            String tableOcid,
            String statement,
            TableLimits limits,
            boolean isAutoReclaimable,
            Map<String, String> freeformTags,
            Map<String, Map<String, Object>> predefinedTags,
            byte[] matchETag,
            String retryToken,
            OpCode op,
            PrepareCB callback,
            LogContext lc,
            String apiVersion) {

        short serialVersion = RestAPIVersions.get(apiVersion);
        if (freeformTags != null || predefinedTags != null ||
            isAutoReclaimable) {
            actx.setNewTags(freeformTags, predefinedTags,
                            (isAutoReclaimable ? FREE_TIER_SYS_TAGS : null));
        }
        return TableUtils.tableDdlOperation(actx,
                                            tableOcid,
                                            statement,
                                            limits,
                                            isAutoReclaimable,
                                            matchETag,
                                            retryToken,
                                            op,
                                            tm,
                                            lc,
                                            request,
                                            ac,
                                            this,
                                            callback,
                                            serialVersion,
                                            this::updateLogContext);
    }

    @Override
    protected GetTableResponse handleTableLimits(FullHttpRequest request,
                                                 AccessContext actx,
                                                 String tableName,
                                                 TableLimits limits,
                                                 byte[] matchETag,
                                                 LogContext lc,
                                                 String apiVersion) {

        logger.fine("Alter table limits for table " + tableName + ": " +
                     limits, lc);
        return TableUtils.alterTable(actx,
                                     tableName,
                                     null, // statement
                                     limits,
                                     matchETag,
                                     tm,
                                     lc,
                                     request,
                                     ac,
                                     this,
                                     this::updateLogContext);
    }

    @Override
    protected GetTableResponse handleTableTags(
            FullHttpRequest request,
            AccessContext actx,
            String tableName,
            Map<String, String> freeformTags,
            Map<String, Map<String, Object>> definedTags,
            byte[] matchETag,
            LogContext lc,
            String apiVersion) {

        logger.fine("Alter table tags for table " + tableName +
                    ", freeformTags: " + freeformTags +
                    " definedTags: " + definedTags , lc);

        /* Set tags */
        actx.setNewTags(freeformTags, definedTags, null);
        return TableUtils.updateTableTags(actx,
                                          tableName,
                                          matchETag,
                                          tm,
                                          lc,
                                          request,
                                          ac,
                                          this);
    }

    @Override
    protected GetTableResponse handleChangeCompartment(FullHttpRequest request,
                                                       AccessContext actx,
                                                       String tableName,
                                                       byte[] matchETag,
                                                       String retryToken,
                                                       LogContext lc,
                                                       String apiVersion) {

        logger.fine("Change compartment for table " + tableName +
                    ", current compartmentId: " + actx.getCompartmentId() +
                    " new compartmentId: " + actx.getDestCompartmentId() , lc);

        return TableUtils.changeCompartment(actx,
                                            tableName,
                                            matchETag,
                                            retryToken,
                                            tm,
                                            lc,
                                            request,
                                            ac,
                                            this);
    }

    /**
     * Add replica
     */
    @Override
    protected void handleAddReplica(FullHttpResponse response,
                                    RequestContext rc,
                                    AccessContext actx,
                                    String apiVersion) {

        /* Path parameter */
        final String tableNameOrId = readTableNameOrId(rc.restParams);
        /* Header parameter */
        final String retryToken = readOpcRetryToken(rc.restParams);
        /* Header parameter */
        final byte[] matchETag = readIfMatch(rc.restParams);

        String compartmentId = null;
        String remoteRegionName = null;
        int readUnits = 0;
        int writeUnits = 0;

        /* Parse payload */
        checkNotNull(CREATE_REPLICA_DETAILS, rc.restParams.getPayload());

        JsonPayload pl = null;
        try {
            pl = rc.restParams.parsePayload();
            while (pl.hasNext()) {
                if (pl.isField(COMPARTMENT_ID)) {
                    compartmentId = pl.readString();
                    checkNotEmpty(COMPARTMENT_ID, compartmentId);
                }  else if (pl.isField(REGION)) {
                    remoteRegionName = pl.readString();
                    checkNotEmpty(REGION, remoteRegionName);
                } else if (pl.isField(MAX_READ_UNITS)) {
                    readUnits = pl.readInt();
                    checkNonNegativeInt(MAX_READ_UNITS, readUnits);
                } else if (pl.isField(MAX_WRITE_UNITS)) {
                    writeUnits = pl.readInt();
                    checkNonNegativeInt(MAX_WRITE_UNITS, writeUnits);
                } else {
                    handleUnknownField(CREATE_REPLICA_DETAILS, pl);
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

        checkNotNull(REGION, remoteRegionName);
        String remoteServiceName =
        		tm.validateRemoteReplica(remoteRegionName);

        /*
         * Check access again since compartmentId is available until now
         */
        if (compartmentId != null) {
            actx.setCompartmentId(compartmentId);
        }

        int readLimit = readUnits;
        int writeLimit = writeUnits;
        handleTableOp(rc, response, rc.actx, () -> {
            GetTableResponse res =
                TableUtils.addReplica(rc.actx, tableNameOrId, remoteServiceName,
                                      readLimit, writeLimit, matchETag,
                                      retryToken, tm, rc.lc, rc.request, ac,
                                      this, this::updateLogContext);

            if (logger.isLoggable(Level.FINE, rc.lc)) {
                logger.fine("[Rest]AddReplica: " + res, rc.lc);
            }

            return res;
        });
    }

    /**
     * Drop replica
     */
    @Override
    protected void handleDropReplica(FullHttpResponse response,
                                     RequestContext rc,
                                     AccessContext actx,
                                     String apiVersion) {
        /* Header parameter */
        byte[] matchETag = readIfMatch(rc.restParams);

        /* Path parameter */
        String tableNameOrId = readTableNameOrId(rc.restParams);
        String remoteRegionName = readRegion(rc.restParams);
        /* Query parameter */
        String compartmentId = readCompartmentId(rc.restParams);

        checkNotNull(REGION, remoteRegionName);
        String remoteServiceName =
        		tm.validateRemoteReplica(remoteRegionName);

        if (compartmentId != null) {
            actx.setCompartmentId(compartmentId);
        }

        handleTableOp(rc, response, rc.actx, () -> {
            GetTableResponse res =
                TableUtils.dropReplica(rc.actx,  tableNameOrId,
                                       remoteServiceName, matchETag,
                                       tm, rc.lc, rc.request,
                                       ac, this, this::updateLogContext);
            if (logger.isLoggable(Level.FINE, rc.lc)) {
                logger.fine("[Rest]AddReplica: " + res, rc.lc);
            }
            return res;
        });
    }

    private static String readRegion(RequestParams request) {
        String idxName = request.getPathParam(REGION);
        checkNotNullEmpty(REGION, idxName);
        return idxName;
    }

    /**
     * Get configuration
     *
     * GET <api_version>/configuration?compartmentId=<dedicated-tenant-id>
     */
    @Override
    protected void handleGetConfiguration(FullHttpResponse response,
                                          RequestContext rc,
                                          AccessContext actx,
                                          String apiVersion) {

        GetKmsKeyInfoResponse resp = TableUtils.getKmsKeyInfo(actx, tm, rc.lc);
        if (!resp.getSuccess()) {
            throw new RequestException(resp.getErrorCode(),
                                       resp.getErrorString());
        }

        KmsKeyInfo key = resp.getKeyInfo();
        if (logger.isLoggable(Level.FINE, rc.lc)) {
            logger.fine("GetConfiguration: tenant=" + actx.getTenantId() +
                        ", kmsKeyInfo=" + key.toString(), rc.lc);
        }

        String keyInfo = buildConfiguration(key);
        buildTaggedResponse(response, keyInfo, key.getETag());

        markTMOpSucceeded(rc, 1 /* TM operation count*/);
    }

    /**
     * Update Configuration
     *
     * Currently, only updating kms key is supported.
     *
     * URL:
     *   PUT <api_version>/configuration?compartmentId=<dedicated-tenant-id>
     *
     * Header parameters:
     *   - if-match
     *   - is-opc-dry-run
     *
     * Payload:
     * {
     *   "environment":<HOSTED>,
     *   "kmsKey": {
     *     "id": <key_ocid>,
     *     "kmsVaultId": <vault_ocid>
     *    }
     * }
     */
    @Override
    protected void handleUpdateConfiguration(FullHttpResponse response,
                                             RequestContext rc,
                                             AccessContext actx,
                                             String apiVersion) {
        /* Query parameter */
        String compartmentId = readCompartmentId(rc.restParams, true);

        /* Header parameters */
        byte[] ifMatch = readIfMatch(rc.restParams);
        boolean dryRun = readDryRun(rc.restParams);

        checkNotNull(UPDATE_CONFIGURATION_DETAILS, rc.restParams.getPayload());

        boolean isHostedEnv = false;
        String kmsKeyId = null;
        String kmsVaultId = null;

        JsonPayload pl = null;
        try {
            pl = rc.restParams.parsePayload();
            while (pl.hasNext()) {
                if (pl.isField(ENVIRONMENT)) {
                    isHostedEnv = HOSTED_ENVIRONMENT.equals(pl.readString());
                } else if (pl.isField(KMS_KEY)) {
                    JsonObject jo = pl.readObject();
                    if (jo != null) {
                        while (jo.hasNext()) {
                            if (jo.isField(ID)) {
                                kmsKeyId = jo.readString();
                            } else if (jo.isField(KMS_VAULT_ID)) {
                                kmsVaultId = jo.readString();
                            } else if (jo.isField(KMS_KEY_STATE) ||
                                       jo.isField(TIME_CREATED) ||
                                       jo.isField(TIME_UPDATED)) {
                                jo.readString();
                            } else {
                                throw new IllegalArgumentException(
                                    "Unexpected field of KmsKey: " +
                                    jo.getCurrentField());
                            }
                        }
                    }
                } else {
                    handleUnknownField(UPDATE_CONFIGURATION_DETAILS, pl);
                }
            }
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Invalid payload for UpdateConfiguration request: " +
                ioe.getMessage());
        } finally {
            if (pl != null) {
                pl.close();
            }
        }

        if (!isHostedEnv) {
            throw new IllegalArgumentException(
                "Configuration for non Hosted Environment is not supported");
        }
        if (kmsKeyId == null || kmsKeyId.isBlank()) {
            throw new IllegalArgumentException(
               "The kms key Id must not be null or empty");
        }

        /*
         * Only one kind of configuration can be updated in a request. This is
         * because changing configurations can be handled by different sub
         * services, handling a mix of successes and failures adds complexity.
         *
         * TODO: If multiple configurations are supported in future, check only
         * one configuration can be updated at a time.
         */
        WorkRequestIdResponse resp =
            TableUtils.updateKmsKey(actx, tm, compartmentId, kmsKeyId,
                                    kmsVaultId, ifMatch, dryRun, rc.lc,
                                    rc.request, ac, this,
                                    this::updateLogContext);
        if (!resp.getSuccess()) {
            throw new RequestException(resp.getErrorCode(),
                                       resp.getErrorString());
        }

        if (logger.isLoggable(Level.FINE, rc.lc)) {
            logger.fine("handleUpdateConfiguration: workRequestId: " +
                        resp.getWorkRequestId(), rc.lc);
        }

        /* build response */
        buildWorkRequestIdResponse(response, resp.getWorkRequestId());

        markTMOpSucceeded(rc, 1 /* TM operation count*/);
    }

    /**
     * Remove kms key
     *
     * URL:
     *   POST <api_version>/configuration/actions/unassignkmskey?
     *        compartmentId=<dedicated-tenant-id>
     *
     * Header parameters:
     *   - if-match
     *   - is-opc-dry-run
     */
    @Override
    protected void handleRemoveKmsKey(FullHttpResponse response,
                                      RequestContext rc,
                                      AccessContext actx,
                                      String apiVersion) {

        /* Header parameters */
        byte[] ifMatch = readIfMatch(rc.restParams);
        boolean dryRun = readDryRun(rc.restParams);

        WorkRequestIdResponse resp = TableUtils.removeKmsKey(actx, tm, ifMatch,
                                                             dryRun, rc.lc);
        if (!resp.getSuccess()) {
            throw new RequestException(resp.getErrorCode(),
                                       resp.getErrorString());
        }

        if (logger.isLoggable(Level.FINE, rc.lc)) {
            logger.fine("handleRemoveKmsKey: workRequestId " +
                        resp.getWorkRequestId(), rc.lc);
        }

        /* build response */
        buildWorkRequestIdResponse(response, resp.getWorkRequestId());

        markTMOpSucceeded(rc, 1 /* TM operation count*/);
    }

    /*
     * return true if response indicates an error that should
     * be part of error rate limiting.
     * Allow this to be overridden by subclasses.
     */
    @Override
    protected boolean isErrorLimitingResponse(FullHttpResponse resp,
                                              ChannelHandlerContext ctx) {
        if (resp == null) {
            return false;
        }
        /*
         * The REST logic uses the same error codes for several
         * different kinds of errors. So this may do limiting on
         * more types of errors than we'd really want, but the
         * alternative is either a) parsing the json payload or
         * b) code refactoring. So going with this for now.
         */
        if (resp.status() == NOT_FOUND ||
            resp.status() == UNAUTHORIZED ||
            resp.status() == BAD_REQUEST) {
                return true;
        }
        return false;
    }

    /*
     * In cloud, the maximum number of records that can be updated in a query
     * is limited by maxReadKB and maxWriteKB, return Integer.MAX_VALUE to not
     * use this limit.
     */
    @Override
    protected int getUpdateLimit(int value) {
        return Integer.MAX_VALUE;
    }
}
