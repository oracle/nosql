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

package oracle.nosql.proxy.cloud;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static oracle.nosql.proxy.protocol.BinaryProtocol.BAD_PROTOCOL_MESSAGE;
import static oracle.nosql.proxy.protocol.BinaryProtocol.ILLEGAL_ARGUMENT;
import static oracle.nosql.proxy.protocol.BinaryProtocol.INVALID_AUTHORIZATION;
import static oracle.nosql.proxy.protocol.BinaryProtocol.UNKNOWN_OPERATION;
import static oracle.nosql.proxy.protocol.HttpConstants.AUTHORIZATION;
import static oracle.nosql.proxy.protocol.Protocol.UNKNOWN_ERROR;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.logging.Level;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.http.LogControl;
import oracle.nosql.common.kv.drl.LimiterManager;
import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.proxy.Config;
import oracle.nosql.proxy.DataService;
import oracle.nosql.proxy.MonitorStats;
import oracle.nosql.proxy.RequestException;
import oracle.nosql.proxy.audit.ProxyAuditManager;
import oracle.nosql.proxy.filter.FilterHandler;
import oracle.nosql.proxy.filter.FilterHandler.Filter;
import oracle.nosql.proxy.protocol.NsonProtocol;
import oracle.nosql.proxy.protocol.Protocol;
import oracle.nosql.proxy.protocol.Protocol.OpCode;
import oracle.nosql.proxy.sc.CommonResponse;
import oracle.nosql.proxy.sc.GetTableResponse;
import oracle.nosql.proxy.sc.ReplicaStatsResponse;
import oracle.nosql.proxy.sc.SCTenantManager;
import oracle.nosql.proxy.sc.TableUtils;
import oracle.nosql.proxy.sc.TableUtils.PrepareCB;
import oracle.nosql.proxy.sc.TenantManager;
import oracle.nosql.proxy.sc.WorkRequestIdResponse;
import oracle.nosql.proxy.security.AccessChecker;
import oracle.nosql.proxy.security.AccessContext;
import oracle.nosql.proxy.security.AccessContext.Type;
import oracle.nosql.proxy.util.CloudServiceTableCache;
import oracle.nosql.proxy.util.ErrorManager;
import oracle.nosql.util.ph.HealthStatus;
import oracle.nosql.util.tmi.TableLimits;
import oracle.nosql.util.tmi.TableInfo;

public class CloudDataService extends DataService {

    private final AccessChecker ac;

    public CloudDataService(SkLogger logger,
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
        this.ac = ac;
    }

    @Override
    protected FullHttpResponse validateHttpRequest(RequestContext rc) {
        final CharSequence auth = rc.headers.get(AUTHORIZATION);
        /* TODO: check for empty, missing "Signature: " (IAM) */
        if (auth == null || auth.length() == 0) {
            return invalidRequest(rc,
                                  "Authorization header is missing",
                                  INVALID_AUTHORIZATION);
        }
        return super.validateHttpRequest(rc);
    }

    @Override
    protected AccessContext checkAccess(FullHttpRequest request,
                                        OpCode opCode,
                                        AccessContext actx,
                                        String tableName,
                                        Filter filter,
                                        LogContext lc) {
        return checkAccess(request,
                           actx.getCompartmentId(),
                           opCode,
                           actx.getOriginalTableName(tableName),
                           filter,
                           lc);
    }

    @Override
    protected String mapTableName(AccessContext actx, OpCode op, String name) {
        if (op == OpCode.GET_TABLE ||
            op == OpCode.GET_TABLE_USAGE ||
            op == OpCode.GET_INDEXES) {

            return name;
        }
        if (actx.getMapTableName(name) == null) {
            return name;
        }
        return actx.getMapTableName(name);
    }

    @Override
    protected AccessContext checkAccess(FullHttpRequest request,
                                        String compartmentId,
                                        OpCode opCode,
                                        String tableName,
                                        Filter filter,
                                        LogContext lc) {
        /*
         * Permission check of PREPARE, QUERY, TABLE_REQUEST and MULTI_WRITE
         * operations would be skipped, since table name or operation code are
         * method, must call access check again.
         */
        AccessContext actx = ac.checkAccess(POST, request.uri(),
                                            request.headers(), opCode,
                                            compartmentId, tableName,
                                            null, filter, lc);

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

    /*
     * Used to verify the request is from NoSQL Service, see javadoc of
     * AccessChecker.checkInternalAccess().
     */
    private void checkInternalAccess(FullHttpRequest req,
                                     OpCode op,
                                     LogContext lc) {
        ac.checkInternalAccess(POST, req.uri(), req.headers(), op, lc);
    }

    @Override
    protected String mapErrorMessage(AccessContext actx,
                                     String tableName,
                                     String message) {
        return mapMessageInternal(actx, tableName, message);
    }

    /**
     * Map internal table OCID(s) to its original table name or to external ocid
     * format in error message.
     */
    @Override
    protected String mapErrorMessage(Map<String, AccessContext> tableActxs,
                                     String message) {
        message = filterStackTraces(message);

        if (message == null || tableActxs == null) {
            return message;
        }

        AccessContext actx = null;
        for (Map.Entry<String, AccessContext> e : tableActxs.entrySet()) {
            actx = e.getValue();
            /* not IAM access, no need to map error message */
            if (actx.getType() != Type.IAM) {
                return message;
            }
            message = replaceTableName(actx, e.getKey(), message);
        }

        if (actx != null &&
            message.contains(AccessContext.INTERNAL_OCID_PREFIX)) {
            return actx.mapToExternalId(message);
        }
        return message;
    }

    public void flushTableCache(String tableName) {
        tableCache.flushEntry(null, tableName);
    }

    /**
     * Map internal table OCID to original table name in error message. This
     * method is also shared by REST Data service cloud implementation.
     */
    public static String mapMessageInternal(AccessContext actx,
                                            String tableName,
                                            String message) {

        message = filterStackTraces(message);

        /* not IAM access, no need to map error message */
        if (message == null || actx == null || actx.getType() != Type.IAM) {
            return message;
        }

        message = replaceTableName(actx, tableName, message);

        if (message.contains(AccessContext.INTERNAL_OCID_PREFIX)) {
            return actx.mapToExternalId(message);
        }
        return message;
    }

    /* replace internal table name to original one if present in message */
    private static String replaceTableName(AccessContext actx,
                                           String tableName,
                                           String message) {

        /* replace internal table name to original one if present in message */
        if (actx.getMapTableName(tableName) != null &&
            actx.getOriginalTableName(tableName) != null &&
            message.contains(actx.getMapTableName(tableName))) {

            return message.replace(actx.getMapTableName(tableName),
                                   actx.getOriginalTableName(tableName));
        }
        return message;
    }

    @Override
    protected GetTableResponse handleTableDdl(RequestContext rc,
                                              TableOpInfo opInfo,
                                              PrepareCB callback) {
        return TableUtils.tableDdlOperation(rc.actx,
                                            null, /* tableOcid */
                                            opInfo.statement,
                                            opInfo.limits,
                                            false, /* isAutoReclaimable */
                                            opInfo.matchETag,
                                            null,  /* retryToken */
                                            null,  /* opCode */
                                            tm,
                                            rc.lc,
                                            rc.request,
                                            ac,
                                            this,
                                            callback,
                                            rc.serialVersion,
                                            this::updateLogContext);
    }

    @Override
    protected GetTableResponse handleTableLimits(RequestContext rc,
                                                 TableLimits limits,
                                                 byte[] matchETag) {
        logger.fine("Alter table limits for table " + rc.mappedTableName + ": " +
                    limits, rc.lc);
        return TableUtils.alterTable(rc.actx,
                                     rc.mappedTableName,
                                     null, // statement
                                     limits,
                                     matchETag,
                                     tm,
                                     rc.lc,
                                     rc.request,
                                     ac,
                                     this,
                                     this::updateLogContext);
    }

    /*
     * NOTE: the tags have already been set in the rc.actx by caller
     */
    @Override
    protected GetTableResponse handleTableTags(RequestContext rc,
                                               byte[] matchETag) {
        logger.fine(("Alter tags for table " + rc.mappedTableName), rc.lc);
        return TableUtils.updateTableTags(rc.actx,
                                          rc.mappedTableName,
                                          matchETag,
                                          tm,
                                          rc.lc,
                                          rc.request,
                                          ac,
                                          this);
    }

    /*
     * return true if response indicates an error that should
     * be part of error rate limiting.
     */
    @Override
    protected boolean isErrorLimitingResponse(FullHttpResponse resp,
                                              ChannelHandlerContext ctx) {
        if (resp == null) {
            return false;
        }
        if (resp.status() != OK) {
            /* TODO: serious error here? Hmmm. */
        } else if (resp.content() != null && resp.content().capacity() > 0) {
            /* peek at the first byte of response to determine error code */
            int code = resp.content().getByte(0);
            switch (code) {
                case UNKNOWN_OPERATION:
                case ILLEGAL_ARGUMENT:
                case INVALID_AUTHORIZATION:
                case BAD_PROTOCOL_MESSAGE:
                    return true;
                /*
                 * The following are purposefully not counted as errors that
                 * should be limited:
                 * RETRY_AUTHENTICATION: only happens in on-prem
                 * UNKNOWN_ERROR: could happen for a variety of reasons, we
                 *                might limit for something that shouldn't
                 *                be limited.
                 * SECURITY_INFO_UNAVAILABLE: See NOSQL-376. This may be
                 *                            retried inside proxy.
                 */
            }
        }
        return false;
    }

    @Override
    protected boolean handleAddReplica(RequestContext rc)
        throws IOException {

        checkRequestSizeLimit(rc.bbis.available());

        AddReplicaOpInfo info = new AddReplicaOpInfo();

        if (rc.isNson()) {
            getV4AddReplicaOpInfo(info, rc);
        } else {
            throw new IllegalArgumentException(
                "AddReplica operation is not supported with serial version: " +
                rc.serialVersion);
        }

        /*
         * Validate the customer region name specified by the API and convert
         * to a service name;
         */
        String remoteReplicaServiceName =
                tm.validateRemoteReplica(info.region);

        GetTableResponse res = executeTableOp(rc,
                () -> TableUtils.addReplica(rc.actx, rc.mappedTableName,
                                            remoteReplicaServiceName,
                                            info.readUnits, info.writeUnits,
                                            info.matchETag,
                                            null /* retryToken */,
                                            tm, rc.lc, rc.request, ac, this,
                                            this::updateLogContext));

        if (!res.getSuccess()) {
            throw new RequestException(res.getErrorCode(),
                                       res.getErrorString());
        }

        if (logger.isLoggable(Level.FINE, rc.lc)) {
            logger.fine("AddReplica: " + res, rc.lc);
        }

        String[] tags = rc.actx.getExistingTags(res.getTableInfo().getTags());
        NsonProtocol.writeGetTableResponse(rc,
                                           res.getTableInfo(),
                                           tags,
                                           rc.actx.getNamespace(),
                                           tm);

        markTMOpSucceeded(rc, 1 /* TM operation count*/);

        /* false == synchronous */
        return false;
    }

    @Override
    protected boolean handleDropReplica(RequestContext rc)
        throws IOException {

        checkRequestSizeLimit(rc.bbis.available());

        DropReplicaOpInfo info = new DropReplicaOpInfo();
        if (rc.isNson()) {
            getV4DropReplicaOpInfo(info, rc);
        } else {
            throw new IllegalArgumentException(
                "DropReplica operation is not supported with serial version: " +
                rc.serialVersion);
        }

        String remoteServiceName = tm.validateRemoteReplica(info.region);

        GetTableResponse res = executeTableOp(rc,
                () -> TableUtils.dropReplica(rc.actx,
                                             rc.mappedTableName,
                                             remoteServiceName,
                                             info.matchETag,
                                             tm,
                                             rc.lc,
                                             rc.request,
                                             ac,
                                             this,
                                             this::updateLogContext));

        if (!res.getSuccess()) {
            throw new RequestException(res.getErrorCode(),
                                       res.getErrorString());
        }

        if (logger.isLoggable(Level.FINE, rc.lc)) {
            logger.fine("DropReplica: " + res, rc.lc);
        }

        String[] tags = rc.actx.getExistingTags(res.getTableInfo().getTags());
        NsonProtocol.writeGetTableResponse(rc,
                                           res.getTableInfo(),
                                           tags,
                                           rc.actx.getNamespace(),
                                           tm);

        markTMOpSucceeded(rc, 1 /* TM operation count*/);

        /* false == synchronous */
        return false;
    }

    @Override
    protected boolean handleInternalDdl(RequestContext rc)
        throws IOException {

        checkRequestSizeLimit(rc.bbis.available());

        InternalDdlOpInfo info = new InternalDdlOpInfo();
        if (rc.isNson()) {
            getV4InternalDdlOpInfo(info, rc);
        } else {
            throw new IllegalArgumentException(
                "InternalDdl operation is not supported with serial version: " +
                rc.serialVersion);
        }

        checkInternalAccess(rc.request, rc.opCode, rc.lc);

        WorkRequestIdResponse res = TableUtils.doInternalDdl(
                rc.actx, rc.mappedTableName, info.operation, info.payload,
                info.isSystem, tm, rc.lc, rc.request, ac, this,
                this::updateLogContext);

        if (!res.getSuccess()) {
            throw new RequestException(res.getErrorCode(),
                                       res.getErrorString());
        }

        if (logger.isLoggable(Level.FINE, rc.lc)) {
            logger.fine("handleInternalDdl: " + res, rc.lc);
        }

        NsonProtocol.writeInternalOpResponse(rc.bbos, res.getWorkRequestId());

        markTMOpSucceeded(rc, 1 /* TM operation count*/);

        /* false == synchronous */
        return false;
    }

    @Override
    protected boolean handleInternalStatus(RequestContext rc)
        throws IOException {

        checkRequestSizeLimit(rc.bbis.available());

        InternalStatusOpInfo info = new InternalStatusOpInfo();
        if (rc.isNson()) {
            getV4InternalStatusOpInfo(info, rc);
        } else {
            throw new IllegalArgumentException(
                "InternalDdl operation is not supported with serial version: " +
                rc.serialVersion);
        }

        /* virtual resource access check */
        checkInternalAccess(rc.request, rc.opCode, rc.lc);

        CommonResponse res;
        switch (info.getResourceType()) {
        case WORKREQUEST:
            /*
             * By design, CMEK related work requests are only visible through
             * the REST API, so the binary API specifically only fetches DDL
             * related requests. This is used by the GAT DDL in other region to
             * check the DDL request information in current region.
             */
            res = TableUtils.getDdlWorkRequest(null /* AccessContext */,
                                               info.ocid, tm,
                                               true /* internal */, rc.lc);
            break;
        case TABLE:
            res = TableUtils.getTable(null /* AccessContext */, info.ocid,
                                      null /* operationId */, tm,
                                      true /* internal */, rc.lc);
            break;
        default:
            throw new IllegalArgumentException("Unsupported resource type of " +
                    rc.opCode + ": " + info.getResourceType());
        }

        if (!res.getSuccess()) {
            throw new RequestException(res.getErrorCode(),
                                       res.getErrorString());
        }

        String payload = res.successPayload();

        if (logger.isLoggable(Level.FINE, rc.lc)) {
            logger.fine("InternalStatus [type=" + info.resource +
                        ", ocid=" + info.ocid +
                        (info.name != null ?
                            ", name=" + info.name + "]: " : "]: ") +
                        payload, rc.lc);
        }

        NsonProtocol.writeInternalOpResponse(rc.bbos, payload);

        markTMOpSucceeded(rc, 1 /* TM operation count*/);

        /* false == synchronous */
        return false;
    }

    @Override
    protected boolean handleGetReplicaStats(RequestContext rc)
        throws IOException {

        checkRequestSizeLimit(rc.bbis.available());

        GetReplicaStatsOpInfo info = new GetReplicaStatsOpInfo();
        if (rc.isNson()) {
            getV4GetReplicaStatsOpInfo(info, rc);
        } else {
            throw new IllegalArgumentException("GetReplicaStats operation is " +
                "not supported with serial version: " + rc.serialVersion);
        }

        String replicaServiceName = null;
        if (info.replicaName != null) {
            replicaServiceName =
                    tm.validateRemoteReplica(info.replicaName);
        }
        info.limit = validateReplicaStatsLimit(info.limit);

        ReplicaStatsResponse response =
            TableUtils.getReplicaStats(rc.actx,
                                       rc.mappedTableName,
                                       replicaServiceName,
                                       info.startTime,
                                       info.limit,
                                       tm,
                                       rc.lc);
        if (!response.getSuccess()) {
            throw new RequestException(response.getErrorCode(),
                                       response.getErrorString());
        }

        NsonProtocol.writeReplicaStatsResponse(rc.bbos, response,
                                               rc.origTableName);

        markTMOpSucceeded(rc, 1 /* TM operation count*/);
        /* false == synchronous */
        return false;
    }

    /**
     * Check health status.
     *
     * NOTE: this method will not generally be called unless running in the
     * context of the cloud service (vs cloud simulator) but it should work
     * either way and if called in the cloud simulator it will always return
     * GREEN.
     */
    @Override
    public HealthStatus checkStatus(List<String> errors) {
        HealthStatus result = HealthStatus.GREEN;
        if (tableCache instanceof CloudServiceTableCache) {
            result = ((CloudServiceTableCache)tableCache).
                checkStoreConnectivity(errors);
        }

        if (tm instanceof SCTenantManager) {
            HealthStatus scStatus =
                ((SCTenantManager) tm).checkSCConnectivity(errors);
            if (scStatus.ordinal() > result.ordinal()) {
                result = scStatus;
            }
        }

        /*
         * Reports warning if transient rule existed, it indicates the system
         * is in maintenance which is unhealthy state.
         */
        if (filter != null && filter.hasTransientRule()) {
            HealthStatus status = HealthStatus.YELLOW;
            errors.add("Transient filter rule existed");
            if (status.ordinal() > result.ordinal()) {
                result = status;
            }
        }

        /*
         * Reports warning if enabled tombstones for tables due to its cached
         * table entry not being refreshed as scheduled.
         */
        if (!tableTombstoneEnabled.isEmpty()) {
            HealthStatus status = HealthStatus.YELLOW;
            String msg = "Enabling tombstones for " +
                         tableTombstoneEnabled.size() + " tables: " +
                         tableTombstoneEnabled;
            errors.add(msg);
            logger.warning(msg);
            if (status.ordinal() > result.ordinal()) {
                result = status;
            }
            tableTombstoneEnabled.clear();
        }
        return result;
    }

    private GetTableResponse executeTableOp(RequestContext rc,
                                            Supplier<GetTableResponse> execOp) {

        int errorCode = Protocol.NO_ERROR;
        GetTableResponse res = null;
        try {
            res = execOp.get();
        } catch (RequestException re) {
            errorCode = re.getErrorCode();
            throw re;
        } catch (Throwable t) {
            errorCode = UNKNOWN_ERROR;
            throw t;
        } finally {
            TableInfo tableInfo = (res != null) ? res.getTableInfo() : null;
            auditOperation(rc, tableInfo, errorCode);
        }
        return res;
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
