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

import static oracle.nosql.proxy.protocol.BinaryProtocol.V1;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TABLE_NOT_FOUND;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpRequest;
import oracle.kv.impl.api.table.TableMetadataHelper;
import oracle.kv.impl.query.compiler.CompilerAPI;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.PrepareCallback;
import oracle.kv.query.PrepareCallback.QueryOperation;
import oracle.kv.table.Table;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.proxy.security.AccessChecker;
import oracle.nosql.proxy.security.AccessContext;
import oracle.nosql.proxy.security.AccessContext.Type;
import oracle.nosql.proxy.DataServiceHandler.AccessContextHelper;
import oracle.nosql.proxy.DataServiceHandler.UpdateLogContext;
import oracle.nosql.proxy.RequestException;
import oracle.nosql.proxy.filter.FilterHandler.Filter;
import oracle.nosql.proxy.protocol.Protocol.OpCode;
import oracle.nosql.util.tmi.DdlHistoryEntry.DdlOp;
import oracle.nosql.util.tmi.TableLimits;

/**
 * TableUtils is some utility methods used for table DDL operations. For now,
 * it is a static wrapper around access to the TenantManager interface. It
 * will expand.
 */
public class TableUtils {

    /* used in checkForIdentity */
    private static Pattern pattern = Pattern.compile("as\\s+identity");
    private static String MISMATCH_INPUT = "mismatched input";
    private static String EXTRA_INPUT = "extraneous input";

    /**
     * Creates a table based on the DDL string passed.
     *
     * @param statement a DDL create table statement
     * @param tm an instance of TenantManager to use
     *
     * @return an instance of GetTableResponse
     */
    private static GetTableResponse createTable(AccessContext actx,
                                                String tableName,
                                                String statement,
                                                boolean ifNotExists,
                                                TableLimits limits,
                                                boolean isAutoReclaimable,
                                                String retryToken,
                                                TenantManager tm,
                                                LogContext lc) {

        if (isChildTable(tableName)) {
            if (limits != null) {
                throw new IllegalArgumentException(
                    "Cannot set limits on child table: " + tableName);
            }
        } else {
            if (limits == null) {
                throw new IllegalArgumentException(
                    "Create table requires throughput and capacity");
            }
        }

        /*
         * Call the TenantManager to do the operation workflow
         */
        return tm.createTable(actx,
                              tableName,
                              statement,
                              ifNotExists,
                              limits,
                              isAutoReclaimable,
                              retryToken,
                              lc);
    }

    /**
     * Alters a table based on the DDL string passed OR the
     * table limits, but not both.
     *
     * @param actx the AccessContext instance
     * @param tableName the target table name
     * @param statement a DDL alter table statement
     * @param limits table limits
     * @param matchETag the table's ETag to be matched
     * @param tm an instance of TenantManager to use
     * @param lc the LogContext instance
     * @param request the http request
     * @param ac an instance of AccessChecker to use
     *
     * @return an instance of GetTableResponse
     */
    public static GetTableResponse alterTable(AccessContext actx,
                                              String tableName,
                                              String statement,
                                              TableLimits limits,
                                              byte[] matchETag,
                                              TenantManager tm,
                                              LogContext lc,
                                              FullHttpRequest request,
                                              AccessChecker ac,
                                              Filter filter,
                                              UpdateLogContext updateLc) {

        /* callers ensure this */
        assert(limits != null || statement != null);

        if (limits != null) {
            if (statement != null) {
                throw new IllegalArgumentException(
                    "Alter table accepts either a statement or limits, but " +
                    "not both");
            }
        }

        if (ac != null && limits != null) {
            ac.checkAccess(request.method(),
                           request.uri(),
                           request.headers(),
                           OpCode.ALTER_TABLE,
                           actx.getCompartmentId(),
                           tableName,
                           getPayload(request),
                           actx,
                           filter,
                           lc);
            if (updateLc != null) {
                updateLc.update(lc, actx, OpCode.ALTER_TABLE);
            }
        }

        if (limits != null) {
            /*
             * For cloud, table name would only be available after the
             * second call of checkAccess if using table OCID.
             */
            if (isChildTable(actx.getOriginalTableName(tableName))) {
                throw new IllegalArgumentException(
                    "Cannot set limits on child table: " + tableName);
            }
        }

        /*
         * Call the TenantManager to do the operation workflow
         */
        return tm.alterTable(actx,
                             tableName,
                             statement,
                             limits,
                             matchETag,
                             lc);
    }

    /**
     * Update the tags of table, the tags are set to AccessContext by caller.
     *
     * @param actx the AccessContext instance
     * @param tableName the target table name
     * @param matchETag the table's ETag to be matched
     * @param tm an instance of TenantManager to use
     * @param lc the LogContext instance
     * @param request the http request
     * @param ac an instance of AccessChecker to use
     *
     * @return an instance of GetTableResponse
     */
    public static GetTableResponse updateTableTags(AccessContext actx,
                                                   String tableName,
                                                   byte[] matchETag,
                                                   TenantManager tm,
                                                   LogContext lc,
                                                   FullHttpRequest request,
                                                   AccessChecker ac,
                                                   Filter filter) {

        if (ac != null) {
            ac.checkAccess(request.method(),
                           request.uri(),
                           request.headers(),
                           OpCode.ALTER_TABLE,
                           actx.getCompartmentId(),
                           tableName,
                           getPayload(request),
                           actx,
                           filter,
                           lc);
        }

        /*
         * Call the TenantManager to do the operation workflow
         */
        return tm.alterTable(actx,
                             tableName,
                             null, /* statement */
                             null, /* limits */
                             matchETag,
                             lc);
    }

    /**
     * Changes the table's compartment, the destination compartmentId is set to
     * AccessContext by caller.
     *
     * @param actx the AccessContext instance
     * @param tableName the target table name
     * @param matchETag the table's ETag to be matched
     * @param tm an instance of TenantManager to use
     * @param lc the LogContext instance
     * @param request the http request
     * @param ac an instance of AccessChecker to use
     *
     * @return an instance of GetTableResponse
     */
    public static GetTableResponse changeCompartment(AccessContext actx,
                                                     String tableName,
                                                     byte[] matchETag,
                                                     String retryToken,
                                                     TenantManager tm,
                                                     LogContext lc,
                                                     FullHttpRequest request,
                                                     AccessChecker ac,
                                                     Filter filter) {

        if (ac != null) {
            ac.checkAccess(request.method(),
                           request.uri(),
                           request.headers(),
                           OpCode.CHANGE_COMPARTMENT,
                           actx.getCompartmentId(),
                           tableName,
                           getPayload(request),
                           actx,
                           filter,
                           lc);
        }

        /*
         * Call the TenantManager to do the operation workflow
         */
        return tm.changeCompartment(actx,
                                    actx.getOriginalTableName(tableName),
                                    matchETag,
                                    retryToken,
                                    lc);
    }

    /**
     * Simple get table request
     */
    public static GetTableResponse getTable(AccessContext actx,
                                            String tableName,
                                            String operationId,
                                            TenantManager tm,
                                            boolean internal,
                                            LogContext lc) {
        return tm.getTable(actx, tableName, operationId, internal, lc);
    }

    /**
     * List tables
     */
    public static ListTableResponse listTables(AccessContext actx,
                                               int startIndex,
                                               int numTables,
                                               TenantManager tm,
                                               LogContext lc) {
        return tm.listTables(actx, startIndex, numTables, lc);
    }

    /**
     * List tables with detailed information returned
     */
    public static ListTableInfoResponse listTableInfos(AccessContext actx,
                                                       String namePattern,
                                                       String state,
                                                       String sortBy,
                                                       boolean isAscSort,
                                                       int startIndex,
                                                       int numTables,
                                                       TenantManager tm,
                                                       LogContext lc) {
        return tm.listTableInfo(actx, namePattern, state, sortBy, isAscSort,
                                startIndex, numTables, lc);
    }

    /**
     * Performs a table DDL operation, returning a GetTableResponse
     *
     * @param actx the AccessContext instance
     * @param statement a DDL statement
     * @param limits table limits
     * @param matchETag the ETag of the affected table or index to be matched
     * @param tm an instance of TenantManager to use
     * @param lc the LogContext instance
     * @param request the http request
     * @param ac an instance of AccessChecker to use
     * @param serialVersion the protocol version
     *
     * @return an instance of GetTableResponse
     */
    public static GetTableResponse tableDdlOperation(AccessContext actx,
                                                     String tableOcid,
                                                     String statement,
                                                     TableLimits limits,
                                                     boolean isAutoReclaimable,
                                                     byte[] matchETag,
                                                     String retryToken,
                                                     OpCode opCode,
                                                     TenantManager tm,
                                                     LogContext lc,
                                                     FullHttpRequest request,
                                                     AccessChecker ac,
                                                     Filter filter,
                                                     PrepareCB callback,
                                                     short serialVersion,
                                                     UpdateLogContext updateLc) {

        GetTableResponse response = null;

        /*
         * getCallbackInfo won'be able to check maximum column of given table if
         * tenant id is unavailable, need to invoke getCallbackInfo again for
         * maximum column check.
         */
        boolean needMaxColumnCheck = (actx.getType() == Type.IAM) &&
                                     (actx.getTenantId() == null);

        if (callback.getIsTextIndex()) {
            /*
             * Text index is not supported in cloud, don't allow to create
             * fulltext index.
             */
            throw new IllegalArgumentException(
                "Full text indexes are not supported");
        }

        if (opCode == null) {
            opCode = getOpCode(callback.getOperation());
        }

        if (actx.getType() != Type.KV) {
            checkAccess(tm, actx, lc, callback, opCode, request, ac, filter);
            if (updateLc != null) {
                updateLc.update(lc, actx, opCode);
            }
        }

        if (needMaxColumnCheck &&
            (callback.getOperation() == QueryOperation.CREATE_TABLE ||
             callback.getOperation() == QueryOperation.ALTER_TABLE)) {
            callback = getCallbackInfo(actx, statement, tm);
        }

        String tableName = callback.getTableName();

        /*
         * Perform the operation
         */
        QueryOperation qop = callback.getOperation();
        if (qop == QueryOperation.CREATE_TABLE && opCode == OpCode.ALTER_TABLE) {
            /*
             * Create table ddl can be used for alter table operation, alter
             * table ddl will be generated based on new create table ddl in
             * alterTable(), so consider this case as QueryOperation.ALTER_TABLE.
             */
            qop = QueryOperation.ALTER_TABLE;
        }

        switch (qop) {
        case CREATE_TABLE:
            if (serialVersion <= V1) {
                /* identity columns not supported in V1 */
                checkForIdentity(statement);
            }

            response = createTable(actx,
                                   tableName,
                                   statement,
                                   callback.getIfNotExists(),
                                   limits,
                                   isAutoReclaimable,
                                   retryToken,
                                   tm,
                                   lc);
            break;
        case DROP_TABLE:
            response = tm.dropTable(actx,
                                    tableName,
                                    callback.getIfExists(),
                                    matchETag,
                                    lc);
            break;
        case CREATE_INDEX:
            response = tm.createIndex(actx,
                                      tableName,
                                      callback.getIndexName(),
                                      statement,
                                      callback.getIfNotExists(),
                                      retryToken,
                                      lc);
            break;
        case ALTER_TABLE:
            /* look for identity columns, not supported at this time */
            if (serialVersion <= V1) {
                /* identity columns not supported in V1 */
                checkForIdentity(statement);
            }

            /*
             * REST API user may issue a request with table OCID but
             * specifying incorrect table name in the statement
             */
            if (tableOcid != null &&
                !callback.getTableName().equalsIgnoreCase(
                    actx.getOriginalTableName(null))) {
                throw new IllegalArgumentException("The table name of '" +
                    tableOcid +
                    "' does not match the table name in ddl statement: " +
                    callback.getTableName());
            }

            /*
             * No need to pass FilterHelper to alterTable because the check on
             * filtering request has been done in above checkAccess().
             */
            response = alterTable(actx,
                                  tableName,
                                  statement,
                                  limits,
                                  matchETag,
                                  tm,
                                  lc,
                                  request,
                                  ac,
                                  null /* filter */,
                                  null /* UpdateLogContext */);
            break;
        case DROP_INDEX:
            response = tm.dropIndex(actx,
                                    tableName,
                                    callback.getIndexName(),
                                    callback.getIfExists(),
                                    matchETag,
                                    lc);
            break;
        case SELECT:
        case UPDATE:
            throw new IllegalArgumentException("TableRequest can not be used " +
                "for " + callback.getOperation() +
                " statement, use QueryRequest instead");
        case CREATE_USER:
        case ALTER_USER:
        case DROP_USER:
        case CREATE_ROLE:
        case DROP_ROLE:
        case GRANT:
        case REVOKE:
        case DESCRIBE:
        case SHOW:
        default:
            throw new IllegalArgumentException(
                callback.getOperation().name().replace('_', ' ') +
                " statement is not supported");
        }
        return response;
    }

    /**
     * Table usage
     */
    public static TableUsageResponse getTableUsage(AccessContext actx,
                                                   String tableName,
                                                   long startTimestamp,
                                                   long endTimestamp,
                                                   int startIndex,
                                                   int limit,
                                                   TenantManager tm,
                                                   LogContext lc) {
        if (isChildTable(actx.getOriginalTableName(tableName))) {
            throw new IllegalArgumentException(
                "No usage information for child table: " + tableName);
        }
        return tm.getTableUsage(actx, tableName, startTimestamp, endTimestamp,
                                startIndex, limit, lc);
    }

    /**
     * Get index infos
     *
     * tenantId and tableName are required. Index name is optional. If indexName
     * is present, just information on that index is returned; otherwise info on
     * all indexes is returned. If the indexName is present and the index
     * doesn't exist an error is returned (index not found).
     */
    public static IndexResponse getIndexInfo(AccessContext actx,
                                             String tableName,
                                             String indexName,
                                             TenantManager tm,
                                             LogContext lc) {
        return getIndexInfo(actx, tableName, indexName,
                            null /* namePattern */,
                            null /* state */,
                            null /* sortBy */,
                            true /* isAscSort*/,
                            0 /* startIndex */,
                            0 /* limit */,
                            tm, lc);
    }

    public static IndexResponse getIndexInfo(AccessContext actx,
                                             String tableName,
                                             String indexName,
                                             String namePattern,
                                             String state,
                                             String sortBy,
                                             boolean isAscSort,
                                             int startIndex,
                                             int limit,
                                             TenantManager tm,
                                             LogContext lc) {
        return tm.getIndexInfo(actx, tableName, indexName, namePattern, state,
                               sortBy, isAscSort, startIndex, limit, lc);
    }


    /**
     * Returns information about the query
     */
    public static PrepareCB getCallbackInfo(AccessContext actx,
                                            String statement,
                                            TenantManager tm) {

        /*
         * Extract information from the query String. The namespace provided
         * comes from the access context. In the case of the cloud service it
         * may be a compartment name because they are allowed as a namespace
         * prefix in queries in the cloud. On-premise it will be null because
         * namespaces are allowed. In the cloudsim path it's an artificial
         * non-null string which allows the code that checks if namespaces are
         * allowed to throw an exception.
         */
        PrepareCB callback = tm.createPrepareCB(actx.getNamespace());

        ExecuteOptions options =
            new ExecuteOptions().setPrepareCallback(callback)
            .setAllowCRDT(true)
            .setNamespace(actx.getNamespace(),
                          false); /* don't validate namespace */

        /*
         * Extracts information from statement and validates it.
         *
         * NOTE: if/when child tables are supported it won't work to set null
         * as the TableMetadataHelper. Validation of the query requires the
         * parent's TableImpl. It may be necessary to provide a
         * TableMetadataHelper locally that does the job using the table cache.
         * TODO.
         */
        try {
            CompilerAPI.prepare(null, statement.toCharArray(), options);
        } catch (IllegalArgumentException iae) {
            String msg = iae.getMessage();
            if (msg.contains(MISMATCH_INPUT) || msg.contains(EXTRA_INPUT)) {
                throw new IllegalArgumentException(
                    "Table, index and unquoted field names may contain only " +
                    "alphanumeric values plus the character \"_\". " + msg);
            }
            throw iae;
        }

        return callback;
    }

    /**
     * Gets DDL workRequest information
     *
     * This is used by cross region internal-status request, for GAT DDL op.
     */
    public static GetDdlWorkRequestResponse getDdlWorkRequest(
            AccessContext actx,
            String workReqId,
            TenantManager tm,
            boolean internal,
            LogContext lc) {
        return tm.getDdlWorkRequest(actx, workReqId, internal, lc);
    }

    /**
     * Gets workRequest information
     */
    public static GetWorkRequestResponse getWorkRequest(AccessContext actx,
                                                        String workReqId,
                                                        TenantManager tm,
                                                        LogContext lc) {
        return tm.getWorkRequest(actx, workReqId, false /* internal */, lc);
    }

    /**
     * Lists workRequests
     */
    public static ListWorkRequestResponse listWorkRequests(AccessContext actx,
                                                           String startIndex,
                                                           int limit,
                                                           TenantManager tm,
                                                           LogContext lc) {
        return tm.listWorkRequests(actx, startIndex, limit, lc);
    }

    private static void checkAccess(TenantManager tm,
                                    AccessContext actx,
                                    LogContext lc,
                                    PrepareCB callback,
                                    OpCode opCode,
                                    FullHttpRequest request,
                                    AccessChecker ac,
                                    Filter filter) {

        if (ac == null) {
            return;
        }

        String compartmentId = actx.getCompartmentId();
        if (callback.getNamespace() != null) {
            /* For binary protocol, namespace here might be compartment path */
            compartmentId = callback.getNamespace();
        }

        ac.checkAccess(
            request.method(),
            request.uri(),
            request.headers(),
            opCode,
            compartmentId,
            callback.getTableName(),
            getPayload(request),
            actx,
            filter,
            lc);

        /* fail index operations if table doesn't exist */
        if ((opCode == OpCode.CREATE_INDEX) ||
            (opCode == OpCode.DROP_INDEX)) {
            GetTableResponse gtr = tm.getTable(actx, callback.getTableName(),
                                               null, true /* internal */, lc);
            if (!gtr.getSuccess() && gtr.getErrorCode() == TABLE_NOT_FOUND) {
                throw new RequestException(gtr.getErrorCode(),
                                           gtr.getErrorString());
            }

        }
    }

    public static OpCode getOpCode(QueryOperation op) {
        switch(op) {
        case CREATE_TABLE:
            return OpCode.CREATE_TABLE;
        case ALTER_TABLE:
            return OpCode.ALTER_TABLE;
        case DROP_TABLE:
            return OpCode.DROP_TABLE;
        case CREATE_INDEX:
            return OpCode.CREATE_INDEX;
        case DROP_INDEX:
            return OpCode.DROP_INDEX;
        default:
            throw new IllegalArgumentException(
                "Unsupported query operation " + op);
        }
    }

    private static void checkForIdentity(String statement) {
        Matcher regexMatcher = pattern.matcher(statement.toLowerCase());
        if (regexMatcher.find()) {
            throw new IllegalArgumentException(
                "Identity columns are not supported");
        }
    }

    /**
     * Return mapping OpCode of QueryOperation.
     *
     * SELETE - GET
     * UPDATE, INSERT - PUT
     * DELETE - DELETE
     *
     * @param op query operation
     * @return OpCode
     */
    public static OpCode getDMLOpCode(QueryOperation op) {

        if (op != null) {
            switch (op) {
            case SELECT:
                return OpCode.GET;
            case UPDATE:
            case INSERT:
                return OpCode.PUT;
            case DELETE:
                return OpCode.DELETE;
            default:
                break;
            }
        }

        throw new IllegalArgumentException(
            op + " is not a valid DML operation");
    }

    /**
     * Returns a payload string of Http Request, this is used by acess check.
     */
    public static byte[] getPayload(FullHttpRequest request) {
        ByteBuf buf = request.content();
        byte[] bytes;
        if (buf.hasArray()) {
            bytes = buf.array();
            return Arrays.copyOfRange(bytes, buf.readerIndex(),
                                      buf.readerIndex() + buf.readableBytes());
        }

        int size = buf.resetReaderIndex().readableBytes();
        bytes = new byte[size];
        if (size > 0) {
            buf.readBytes(bytes);
        }
        return bytes;
    }

    /**
     * Sets table activity state to ACTIVE
     */
    public static GetTableResponse setTableActive(AccessContext actx,
                                                  String tableNameOrId,
                                                  TenantManager tm,
                                                  LogContext lc) {
        return tm.setTableActive(actx, tableNameOrId, lc);
    }

    /**
     * Performs a add-replica DDL operation, returning a GetTableResponse
     *
     * @param actx the AccessContext instance
     * @param tableNameOrId the target table name or OCID
     * @param newReplicaServiceName the service name of new replica
     * @param readUnits the max read units of new replica table
     * @param writeUnits the max write units of new replica table
     * @param matchETag the ETag of the affected table
     * @param retryToken
     * @param tm an instance of TenantManager to use
     * @param lc the LogContext instance
     * @param request the http request
     * @param ac an instance of AccessChecker to use
     * @param filter the handler to filter request
     * @param updateLc the handler to update log context
     * @param serialVersion the protocol version
     *
     * @return an instance of GetTableResponse
     */
    public static GetTableResponse addReplica(AccessContext actx,
                                              String tableNameOrId,
                                              String newReplicaServiceName,
                                              int readUnits,
                                              int writeUnits,
                                              byte[] matchETag,
                                              String retryToken,
                                              TenantManager tm,
                                              LogContext lc,
                                              FullHttpRequest request,
                                              AccessChecker ac,
                                              Filter filter,
                                              UpdateLogContext updateLc) {
        if (ac != null) {
            ac.checkAccess(request.method(),
                           request.uri(),
                           request.headers(),
                           OpCode.ADD_REPLICA,
                           actx.getCompartmentId(),
                           tableNameOrId,
                           getPayload(request),
                           actx,
                           filter,
                           lc);
            if (updateLc != null) {
                updateLc.update(lc, actx, OpCode.ADD_REPLICA);
            }
        }
        return tm.addReplica(actx, tableNameOrId, newReplicaServiceName,
                             readUnits, writeUnits, matchETag, retryToken, lc);
    }

    /**
     * Performs a drop-replica DDL operation, returning a GetTableResponse
     *
     * @param actx the AccessContext instance
     * @param tableNameOrId the target table name or OCID
     * @param replicaToRemoveServiceName service name of replica to be dropped
     * @param matchETag the ETag of the affected table
     * @param tm an instance of TenantManager to use
     * @param lc the LogContext instance
     * @param request the http request
     * @param ac an instance of AccessChecker to use
     * @param filter the handler to filter request
     * @param updateLc the handler to update log context
     *
     * @return an instance of GetTableResponse
     */
    public static GetTableResponse dropReplica(AccessContext actx,
                                               String tableNameOrId,
                                               String replicaToRemoveServiceName,
                                               byte[] matchETag,
                                               TenantManager tm,
                                               LogContext lc,
                                               FullHttpRequest request,
                                               AccessChecker ac,
                                               Filter filter,
                                               UpdateLogContext updateLc) {

        if (ac != null) {
            ac.checkAccess(request.method(),
                           request.uri(),
                           request.headers(),
                           OpCode.DROP_REPLICA,
                           actx.getCompartmentId(),
                           tableNameOrId,
                           getPayload(request),
                           actx,
                           filter,
                           lc);
            if (updateLc != null) {
                updateLc.update(lc, actx, OpCode.DROP_REPLICA);
            }
        }

        return tm.dropReplica(actx, tableNameOrId, replicaToRemoveServiceName,
                              matchETag, lc);
    }

    /**
     * Performs a internal DDL operation, returning a WorkRequestIdResponse
     *
     * @param actx the AccessContext instance
     * @param tableNameOrId the target table name or OCID
     * @param operation the operation type
     * @param payload the payload of request
     * @param tm an instance of TenantManager to use
     * @param lc the LogContext instance
     * @param request the http request
     * @param ac an instance of AccessChecker to use
     * @param filter the handler to filter request
     * @param updateLc the handler to update log context
     *
     * @return an instance of WorkRequestIdResponse
     */
    public static WorkRequestIdResponse doInternalDdl(
            AccessContext actx,
            String tableNameOrId,
            String operation,
            String payload,
            boolean isSystem,
            TenantManager tm,
            LogContext lc,
            FullHttpRequest request,
            AccessChecker ac,
            Filter filter,
            UpdateLogContext updateLc) {

        if (!isSystem && ac != null) {
            OpCode opCode = mapInternalDdlOp(operation);
            actx.setIsInternalDdl(true);
            ac.checkAccess(request.method(),
                           request.uri(),
                           request.headers(),
                           opCode,
                           null, /* compartmentId */
                           tableNameOrId,
                           null, /* payload */
                           actx,
                           filter,
                           lc);
            if (updateLc != null) {
                updateLc.update(lc, actx, opCode);
            }
        }
        return tm.doInternalDdl(actx, tableNameOrId, operation, payload, lc);
    }

    /**
     * Get table replicas stats information
     *
     * @param actx the AccessContext instance
     * @param tableNameOrId the target table name or OCID
     * @param replicaName the replica name to query the stats
     * @param startTime the start time to query the stats, it is milliseconds
     *        since the Epoch in UTC time
     * @param limit the max number of stats records to return
     * @param tm an instance of TenantManager to use
     * @param lc the LogContext instance
     *
     * @return an instance of ReplicaStatsResponse
     */
    public static ReplicaStatsResponse getReplicaStats(AccessContext actx,
                                                       String tableNameOrId,
                                                       String replicaName,
                                                       long startTime,
                                                       int limit,
                                                       TenantManager tm,
                                                       LogContext lc) {
        return tm.getReplicaStats(actx, tableNameOrId, replicaName, startTime,
                                  limit, lc);
    }

    /**
     * Get the service level kms key information
     *
     * @param actx the AccessContext instance
     * @param tm an instance of TenantManager to use
     * @param lc the LogContext instance
     *
     * @return an instance of GetKmsKeyInfoResponse that represents the
     * service level configuration information
     */
    public static GetKmsKeyInfoResponse getKmsKeyInfo(AccessContext actx,
                                                      TenantManager tm,
                                                      LogContext lc) {
        return tm.getKmsKey(actx, false /* internal */, lc);
    }

    /**
     * Updates the service level kms key
     *
     * @param actx the AccessContext instance
     * @param tm the TenantManager instance
     * @param configuration the new configuration
     * @param matchETag the index ETag to be matched
     * @param dryRun set true if test update configuration without actually
     * executing it
     * @param lc the LogContext instance
     * @param request the http request
     * @param ac an instance of AccessChecker to use
     * @param filter the handler to filter request
     * @param updateLc the handler to update log context
     *
     * @return an instance of WorkRequestIdResponse
     */
    public static WorkRequestIdResponse updateKmsKey(AccessContext actx,
                                                     TenantManager tm,
                                                     String tenantId,
                                                     String kmsKeyId,
                                                     String kmsVaultId,
                                                     byte[] matchEtag,
                                                     boolean dryRun,
                                                     LogContext lc,
                                                     FullHttpRequest request,
                                                     AccessChecker ac,
                                                     Filter filter,
                                                     UpdateLogContext updateLc){

        if (ac != null) {
            actx = ac.checkConfigurationAccess(
                        request.method(),
                        request.uri(),
                        request.headers(),
                        OpCode.UPDATE_CONFIG_KMS_KEY,
                        null /* authorizeOps */,
                        tenantId,
                        getPayload(request),
                        filter,
                        lc);
            if (updateLc != null) {
                updateLc.update(lc, actx, OpCode.UPDATE_CONFIGURATION);
            }
        }
        return tm.updateKmsKey(actx, kmsKeyId, kmsVaultId, matchEtag,
                               dryRun, lc);
    }

    /**
     * Removes the kms key used by the service
     *
     * @param actx the AccessContext instance
     * @param tm the TenantManager instance
     * @param matchETag the index ETag to be matched
     * @param dryRun set true if test update configuration without actually
     * executing it
     * @param lc the LogContext instance
     *
     * @return an instance of WorkRequestIdResponse
     */
    public static WorkRequestIdResponse removeKmsKey(AccessContext actx,
                                                     TenantManager tm,
                                                     byte[] matchEtag,
                                                     boolean dryRun,
                                                     LogContext lc) {
        return tm.removeKmsKey(actx, matchEtag, dryRun, lc);
    }

    /*
     * Map cross region ddl operation to ddl op for permission check
     */
    private static OpCode mapInternalDdlOp(String operation) {
        DdlOp ddlOp = DdlOp.valueOf(operation);
        switch(ddlOp) {
        case parentAddReplica:
            return OpCode.ALTER_TABLE;
        case parentAddReplicaTable:
            return OpCode.CREATE_TABLE;
        case parentDropReplica:
            return OpCode.ALTER_TABLE;
        case parentDropReplicaTable:
            return OpCode.DROP_TABLE;
        case parentCreateIndex:
            return OpCode.CREATE_INDEX;
        case parentDropIndex:
            return OpCode.DROP_INDEX;
        case parentAlterTable:
        case parentUpdateTable:
            return OpCode.ALTER_TABLE;
        default:
            throw new IllegalArgumentException("Unexpected internal ddl: " +
                                               ddlOp);
        }
    }

    public static boolean isChildTable(String tableName) {
        return tableName.contains(".");
    }

    /**
     * An instance of PrepareCallback used to get information from
     * a table DDL operation string.
     */
    public static class PrepareCB implements PrepareCallback {
        private String targetTable;
        /* The name of non-target tables, it is non-null for join query */
        private List<String> notTargetTables;
        private String indexName;
        private boolean ifNotExists;
        private boolean ifExists;
        private boolean isTextIndex;
        private QueryOperation op;
        private String namespace;
        private boolean allowNamespaces;
        private boolean allowChildTable;
        private Table newTable;

        public PrepareCB(String namespace, boolean allowNamespaces) {
            this(namespace, allowNamespaces, true /* allowChildTable */);
        }

        /**
         * @param namespace if set any string returned in the namespaceName
         * callback must match.
         */
        public PrepareCB(String namespace,
                         boolean allowNamespaces,
                         boolean allowChildTable) {
            this.namespace = namespace;
            this.allowNamespaces = allowNamespaces;
            this.allowChildTable = allowChildTable;
        }

        @Override
        public void tableName(String name) {
            if (!allowChildTable && isChildTable(name)) {
                /* child tables are not supported */
                throw new IllegalArgumentException(
                    "Child tables are not supported in the cloud and " +
                    "non-child table names cannot include \".\": " + name);
            }
            if (targetTable == null) {
                targetTable = name;
            } else {
                if (notTargetTables == null) {
                    notTargetTables = new ArrayList<>();
                }
                notTargetTables.add(name);
            }
        }

        @Override
        public void indexName(String name) {
            this.indexName = name;
        }

        @Override
        public void queryOperation(QueryOperation queryOperation) {
            this.op = queryOperation;
        }

        @Override
        public void ifNotExistsFound() {
            ifNotExists = true;
        }

        @Override
        public void ifExistsFound() {
            ifExists = true;
        }

        @Override
        public void isTextIndex() {
            isTextIndex = true;
        }

        @Override
        public boolean prepareNeeded() {
            return false;
        }

        @Override
        public void newTable(Table table) {
            this.newTable = table;
        }

        @Override
        public TableMetadataHelper getMetadataHelper() {
            return null;
        }

        /*
         * This code is a bit confusing. There are 2 parts:
         * 1) set the namespace if there's one in the query
         * 2) validate that namespaces are allowed, or, in the case of
         * the cloud, that the namespace is == an expected compartment name.
         * The cloud allows namespaces in syntax but they *must* be an OCI
         * compartment name. In that case the compartment name is passed to the
         * constructor of this class.
         */
        @Override
        public void namespaceName(String name) {
            if (!allowNamespaces && namespace != null &&
                !namespace.equals(name)) {
                throw new IllegalArgumentException(
                    "Namespaces are not supported");
            }
            namespace = name;
        }

        public String getNamespace() {
            return namespace;
        }

        public String getTableName() {
            return targetTable;
        }

        public String[] getNotTargetTables() {
            if (notTargetTables != null) {
                return notTargetTables.toArray(
                        new String[notTargetTables.size()]);
            }
            return null;
        }

        public String getIndexName() {
            return indexName;
        }

        public QueryOperation getOperation() {
            return op;
        }

        public boolean getIfNotExists() {
            return ifNotExists;
        }

        public boolean getIfExists() {
            return ifExists;
        }

        public boolean getIsTextIndex() {
            return isTextIndex;
        }

        public boolean isInsertOp() {
            return (op == QueryOperation.UPDATE ||
                    op == QueryOperation.INSERT);
        }

        public boolean isUpdateOp() {
            return (op == QueryOperation.UPDATE ||
                    op == QueryOperation.INSERT ||
                    op == QueryOperation.DELETE);
        }

        private boolean isDdlOp() {
            return (op != QueryOperation.SELECT &&
                    op != QueryOperation.DELETE &&
                    op != QueryOperation.INSERT &&
                    op != QueryOperation.UPDATE);
        }

        /**
         * Throws an exception if the operation is not supported
         * TODO: when INSERT and DELETE are supported, add them here.
         */
        public void checkSupportedDml() {
            switch (op) {
            case SELECT:
            case UPDATE:
            case INSERT:
            case DELETE:
                return;
            default:
                break;
            }

            if (isDdlOp()) {
                throw new IllegalArgumentException(
                    "The query and prepare methods can not be used for DDL " +
                    "statements, use TableRequest instead");
            }

            throw new IllegalArgumentException(
                "The query statement, " + op + ", is not supported");
        }

        public Table getNewTable() {
            return newTable;
        }
    }

    /**
     * A PrepareCallback instance that implements mapTableName() and
     * mapNamespaceName. The table name is only mapped if it matches
     * the original (if the original is not null).
     */
    public static class MapPrepareCB extends PrepareCB {

        private final AccessContextHelper actxHelper;

        public MapPrepareCB(AccessContextHelper actxHelper) {
            super(null, false);
            this.actxHelper = actxHelper;
        }

        @Override
        public boolean prepareNeeded() {
            return true;
        }

        @Override
        public String mapTableName(String tableName) {
            AccessContext actx = actxHelper.get(tableName);
            if (actx != null) {
                return actx.getMapTableName(tableName);
            }
            return tableName;
        }

        @Override
        public String mapNamespaceName(String ns) {
            /* always return null namespace */
            return null;
        }
    }
}
