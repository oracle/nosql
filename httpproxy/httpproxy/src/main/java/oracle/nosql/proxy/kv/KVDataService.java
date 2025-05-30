/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy.kv;

import static oracle.nosql.proxy.ProxySerialization.UTF8ToCharArray;
import static oracle.nosql.proxy.ProxySerialization.readString;
import static oracle.nosql.proxy.ProxySerialization.readUTF8ToCharArray;
import static oracle.nosql.proxy.ProxySerialization.writeString;
import static oracle.nosql.proxy.ProxySerialization.writeSuccess;
import static oracle.nosql.proxy.protocol.BinaryProtocol.mapDDLError;
import static oracle.nosql.proxy.protocol.HttpConstants.AUTHORIZATION;
import static oracle.nosql.proxy.protocol.HttpConstants.DEFAULT_NAMESPACE;
import static oracle.nosql.proxy.protocol.Protocol.INSUFFICIENT_PERMISSION;
import static oracle.nosql.proxy.protocol.Protocol.INVALID_AUTHORIZATION;
import static oracle.nosql.proxy.protocol.Protocol.OPERATION_NOT_SUPPORTED;
import static oracle.nosql.proxy.protocol.Protocol.RETRY_AUTHENTICATION;
import static oracle.nosql.proxy.protocol.Protocol.UNKNOWN_ERROR;
import static oracle.nosql.proxy.protocol.NsonProtocol.OPERATION_ID;
import static oracle.nosql.proxy.protocol.NsonProtocol.PAYLOAD;
import static oracle.nosql.proxy.protocol.NsonProtocol.STATEMENT;

import java.io.IOException;

import io.netty.handler.codec.http.FullHttpRequest;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.ExecutionFuture;
import oracle.kv.KVSecurityException;
import oracle.kv.StatementResult;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.client.admin.DdlFuture;
import oracle.kv.impl.client.admin.DdlStatementExecutor;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.query.ExecuteOptions;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.http.LogControl;
import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.nson.Nson;
import oracle.nosql.nson.values.PathFinder;
import oracle.nosql.proxy.Config;
import oracle.nosql.proxy.DataService;
import oracle.nosql.proxy.MonitorStats;
import oracle.nosql.proxy.RequestException;
import oracle.nosql.proxy.RequestLimits;
import oracle.nosql.proxy.audit.ProxyAuditManager;
import oracle.nosql.proxy.filter.FilterHandler.Filter;
import oracle.nosql.proxy.protocol.ByteInputStream;
import oracle.nosql.proxy.protocol.ByteOutputStream;
import oracle.nosql.proxy.protocol.NsonProtocol;
import oracle.nosql.proxy.protocol.Protocol.OpCode;
import oracle.nosql.proxy.sc.GetTableResponse;
import oracle.nosql.proxy.sc.TableUtils;
import oracle.nosql.proxy.sc.TableUtils.PrepareCB;
import oracle.nosql.proxy.sc.TenantManager;
import oracle.nosql.proxy.security.AccessContext;
import oracle.nosql.util.tmi.TableLimits;

public class KVDataService extends DataService {

    private final boolean isSecure;

    public KVDataService(SkLogger logger,
                         TenantManager tm,
                         MonitorStats stats,
                         ProxyAuditManager audit,
                         Config config,
                         LogControl logControl) {
        super(logger, tm, stats, audit, null, null, null, config, logControl);
        this.isSecure = config.useSSL();
    }

    @Override
    protected AccessContext checkAccess(FullHttpRequest request,
                                        String compartmentId,
                                        OpCode opCode,
                                        String tableName,
                                        Filter filter,
                                        LogContext lc) {
        CharSequence authHeader = request.headers().get(AUTHORIZATION);

        /* authHeader can be null for KV proxy, parse here to avoid NPE */
        String authString = authHeader == null ? null : authHeader.toString();

        if (isSecure && authString == null) {
            /* do not change this message. it's checked in drivers. */
            throw new IllegalArgumentException(
                "Missing authentication information");
        }

        String ns = compartmentId;

        /* namespace in tablename always overrides any other namespace */
        if (tableName != null && tableName.contains(":")) {
            String[] splitName = tableName.split(":");
            if (splitName.length != 2) {
                throw new IllegalArgumentException(
                    "Illegal table name: " + tableName);
            }
            ns = splitName[0];
            tableName = splitName[1];
        }

        /* if no namespace given, check for default in header */
        if (ns == null || ns.isEmpty()) {
            /* look for default namespace in header */
            String defNamespace = request.headers().get(DEFAULT_NAMESPACE);
            if (defNamespace != null && !defNamespace.isEmpty()) {
                ns = defNamespace;
            }
        }

        if (ns == null && authString == null &&
            (opCode != OpCode.TABLE_REQUEST && opCode != OpCode.LIST_TABLES)) {
            /* TABLE_REQUEST and LIST_TABLES need to set ns later */
            return AccessContext.NULL_KV_CTX;
        }

        if (ns != null) {
            /*
             * Update log context with namespace information for a specified
             * log level
             */
            updateLogContext(lc, ns, tableName, opCode);
        }

        return new KVAccessContext(ns, authString);
    }

    @Override
    protected String mapTableName(String tableName) {
        if (tableName.contains(":")) {
            return tableName.split(":")[1];
        }
        return tableName;
    }

    @Override
    protected void handleKVSecurityException(OpCode op,
                                             KVSecurityException kvse) {
        if (kvse instanceof AuthenticationRequiredException) {
            /*
             * Session expired need to retry
             */
            throw new RequestException(RETRY_AUTHENTICATION,
                                       (opCodeToString(op) +
                                        ": Authentication required: " +
                                        kvse.getMessage()));
        } else if (kvse instanceof AuthenticationFailureException) {
            /*
             * Token is not correct
             */
            throw new RequestException(INVALID_AUTHORIZATION,
                                       (opCodeToString(op) +
                                        ": Authentication failed: " +
                                        kvse.getMessage()));
        } else if (kvse instanceof UnauthorizedException) {
            /*
             * Does not have enough privileges, back to client
             */
            throw new RequestException(INSUFFICIENT_PERMISSION,
                                       (opCodeToString(op) +
                                        ": Insufficient privilege: " +
                                        kvse.getMessage()));
        }

        throw new RequestException(UNKNOWN_ERROR,
                                   (opCodeToString(op) +
                                    ": unexpected security exception: " +
                                    kvse.getMessage()));
    }

    @Override
    protected GetTableResponse handleTableDdl(RequestContext rc,
                                              TableOpInfo opInfo,
                                              PrepareCB callback) {
        if (callback.getNamespace() != null) {
            rc.actx.setNamespace(callback.getNamespace());
            /* update log context with namespace for a specified log level */
            updateLogContext(rc.lc,
                             callback.getNamespace(),
                             callback.getTableName(),
                             TableUtils.getOpCode(callback.getOperation()));
        }

        try {
            switch (callback.getOperation()) {
            case CREATE_TABLE:
                return tm.createTable(rc.actx,
                                      callback.getTableName(),
                                      opInfo.statement,
                                      callback.getIfNotExists(),
                                      null,  /* tableLimits */
                                      false, /* isAutoReclaimable */
                                      null,  /* retryToken */
                                      rc.lc);
            case DROP_TABLE:
                return tm.dropTable(rc.actx,
                                    callback.getTableName(),
                                    callback.getIfExists(),
                                    null, /* opInfo.matchETag */
                                    rc.lc);
            case CREATE_INDEX:
                return tm.createIndex(rc.actx,
                                      callback.getTableName(),
                                      callback.getIndexName(),
                                      opInfo.statement,
                                      callback.getIfNotExists(),
                                      null,  /* retryToken */
                                      rc.lc);
            case ALTER_TABLE:
                return tm.alterTable(rc.actx,
                                     callback.getTableName(),
                                     opInfo.statement,
                                     null, /* limits */
                                     null, /* opInfo.matchETag */
                                     rc.lc);
            case DROP_INDEX:
                return tm.dropIndex(rc.actx,
                                    callback.getTableName(),
                                    callback.getIndexName(),
                                    callback.getIfExists(),
                                    null, /* opInfo.matchETag */
                                    rc.lc);
            case SELECT:
            case UPDATE:
                throw new IllegalArgumentException(
                    "TableRequest can not be used for " +
                    callback.getOperation() +
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
        } catch (KVSecurityException kvse) {
            handleKVSecurityException(OpCode.TABLE_REQUEST, kvse);
            return null;
        }
    }

    /**
     * In: SystemRequest
     *     statement (char[])
     * Out: SystemResult
     *      completion state
     *      operationid
     *      original statement
     *
     *  Error handling.
     *   o errors must be thrown as exceptions
     *   o resource (table, index, user, role) exists, not exists -- error
     *   messages and discrimination
     *   o bad statements -- these already generate IAE
     *   o plan failures
     */
    @Override
    protected boolean handleSystemOp(RequestContext rc)
        throws IOException {

        char[] statement = rc.isNson() ? readV4SystemOp(rc.bbis) :
            readUTF8ToCharArray(rc.bbis);
        ExecuteOptions execOpts =
            createExecuteOptions(rc.actx.getNamespace(),
                                 rc.actx.getAuthString(), rc.lc);
        ExecutionFuture future = null;

        try {
            future =
                ((KVTenantManager)tm).getStore().execute(statement, execOpts,
                                                         null /* limits */);
        } catch (IllegalArgumentException iae) {
            mapSystemException(iae);
        }

        if (!(future instanceof DdlFuture)) {
            throw new IllegalStateException(
                "ExecutionFuture must be a DdlFuture: " + future.getClass());
        }
        StatementResult res = future.updateStatus();

        int planId = ((DdlFuture)future).getPlanId();
        String operationId = (planId > 0 ? Integer.toString(planId) : null);


        if (res.isDone() && !res.isSuccessful()) {
                /*
                 * Ideally we'd know more about the error, but in the
                 * absence of that information, throw IAE
                 */
                throw new IllegalArgumentException(
                    "System Operation failed: " + res.getErrorMessage());
        }

        if (rc.isNson()) {
            NsonProtocol.writeSystemResponse(rc,
                                             res,
                                             operationId,
                                             redactPasswd(statement));
            return false; /* not async */
        }
        /* <= v3 */
        writeSuccess(rc.bbos);
        if (res.isDone()) {
            rc.bbos.writeByte(0); // COMPLETE
        } else {
            rc.bbos.writeByte(1); // WORKING
        }
        writeString(rc.bbos, operationId);
        writeString(rc.bbos, redactPasswd(statement));
        writeString(rc.bbos, res.getResult());

        /* false == synchronous */
        return false;
    }

    /**
     * In: SystemStatusRequest
     *     operationId (string)
     *     statement (string) (optional, may be null)
     * Out: SystemResult
     *      completion state
     *      operationid
     *      original statement
     * Error handling:
     *   o this method has no way to return a serialized error condition,
     *   they need to be thrown
     *   o many errors are thrown by KV as exceptions
     *   o check status of StatementResult
     */
    @Override
    protected boolean handleSystemStatus(RequestContext rc)
        throws IOException {

        final String pathToOp = PAYLOAD + "." + OPERATION_ID;
        final String pathToStatement = PAYLOAD + "." + STATEMENT;
        /* operationid, statement, statement is optional */
        /* NOTE: rather than walk the v4 map, just do 2 independent seeks */
        String operationId = rc.isNson() ?
            readV4String(rc, pathToOp, true) :
            readString(rc.bbis);
        String statement = rc.isNson() ?
            readV4String(rc, pathToStatement, false) :
            readString(rc.bbis);
        int planId = Integer.parseInt(operationId);
        DdlStatementExecutor executor = ((KVTenantManager)tm).getExecutor();
        DdlFuture future = new DdlFuture(
            planId,
            executor,
            KVTenantManager.parseAuthString(rc.actx.getAuthString()));
        StatementResult res = future.updateStatus();

        if (res.isDone() && !res.isSuccessful()) {
                /*
                 * Ideally we'd know more about the error, but in the
                 * absence of that information, throw IAE
                 */
                throw new IllegalArgumentException(
                    "System Operation failed: " + res.getErrorMessage());
        }
        if (rc.isNson()) {
            NsonProtocol.writeSystemResponse(rc,
                                             res,
                                             operationId,
                                             statement);
            return false; /* not async */
        }

        writeSuccess(rc.bbos);
        if (res.isDone()) {
            rc.bbos.writeByte(0);
        } else {
            rc.bbos.writeByte(1);
        }
        writeString(rc.bbos, operationId);
        writeString(rc.bbos, statement);
        writeString(rc.bbos, null); /* no result string in this path */

        /* false == synchronous */
        return false;
    }

    /**
     * Remove/blank out password, if present. This requires creating a
     * string. Look for "identified by <password>"
     */
    private String redactPasswd(char[] statement) {
        final String identified = "identified by ";
        final char redactChar = '*';
        /* normalize white space */
        String clean = new String(statement).trim().replaceAll("\\s+", " ");
        int idx = clean.toLowerCase().indexOf(identified);
        if (idx > 0) {
            int startPass = idx + identified.length();
            int endPass = startPass;

            /*
             * Create char[] for replacement of remainder of statement.
             * Because the password can create quote and white space it's
             * difficult to identify it alone, so redact the entire statement.
             */
            char[] replace = clean.toCharArray();
            while (endPass < replace.length) {
                /* && !Character.isWhitespace(replace[endPass])) { */
                replace[endPass++] = redactChar;
            }
            return new String(replace);
        }
        return clean;
    }

    @Override
    protected GetTableResponse handleTableLimits(RequestContext rc,
                                                 TableLimits limits,
                                                 byte[] matchETag) {
        throw new RequestException(OPERATION_NOT_SUPPORTED,
                                   "Table limit operation is not supported");
    }

    @Override
    protected GetTableResponse handleTableTags(RequestContext rc,
                                               byte[] matchETag) {

        /* think about faking tags in the cloud simulator */
        throw new RequestException(OPERATION_NOT_SUPPORTED,
                                   "Table tag operations are not supported");
    }

    @Override
    protected AuthContext createAuthContext(String authString) {
        return KVTenantManager.parseAuthString(authString);
    }

    @Override
    protected void writeThroughput(ByteOutputStream out,
                                   int readKB,
                                   int writeKB,
                                   boolean isAbsolute) throws IOException {
        writeThroughput(out, 0, 0, 0);
    }

    @Override
    protected void writeThroughput(ByteOutputStream out,
                                   int readUnits,
                                   int readKB,
                                   int writeKB) throws IOException {
        oracle.nosql.proxy.ProxySerialization.writeInt(out, 0); // read units
        oracle.nosql.proxy.ProxySerialization.writeInt(out, 0); // read KB
        oracle.nosql.proxy.ProxySerialization.writeInt(out, 0); // write KB
    }

    @Override
    protected int adjustMaxWriteKB(int maxWriteKB,
                                   RequestLimits limits) throws IOException {
        return maxWriteKB;
    }

    @Override
    protected int adjustMaxReadKB(int maxReadKB, boolean isAbsolute,
                                  RequestLimits limits) {
        return maxReadKB;
    }

    @Override
    protected GetTableResponse getTable(AccessContext actx,
                                        String tableName,
                                        String operationId,
                                        TenantManager tm2,
                                        LogContext lc) {
        return tm.getTable(actx, tableName, operationId,
                           false /* internal */, lc);
    }

    @Override
    public boolean isOnPrem() {
        return true;
    }

    /**
     * Find statement in SystemOperation request, which looks like this:
     * ...
     * "payload": {
     *  "statement": ...
     */
    private char[] readV4SystemOp(ByteInputStream bis) throws IOException {
        final String pathToStatement = PAYLOAD + "." + STATEMENT;
        boolean found = PathFinder.seek(pathToStatement, bis);
        if (!found) {
            badProtocol("No statement in message");
        }
        final byte[] bytes = Nson.readNsonBinary(bis);
        return UTF8ToCharArray(bytes);
    }

    /*
     * If we could get better discrimination of exceptions from System
     * queries this wouldn't be necessary. TBD -- do that.
     * Infer the appropriate exception based on the error message.
     *  - exists (index or table)
     *  - not found (index or table)
     */
    private static void mapSystemException(IllegalArgumentException iae) {
        int code = mapDDLError(iae.getMessage());
        if (code == 0) {
            throw iae;
        }
        throw new RequestException(code, iae.getMessage());
    }

    private static class KVAccessContext implements AccessContext {
        private String ns;
        private final String authString;

        KVAccessContext(String ns, String authString) {
            this.ns = ns;
            this.authString = authString;
        }

        @Override
        public String getTenantId() {
            return null;
        }

        @Override
        public void setNamespace(String ns) {
            this.ns = ns;
        }

        @Override
        public String getNamespace() {
            return ns;
        }

        @Override
        public String getPrincipalId() {
            return null;
        }

        @Override
        public String getAuthString() {
            return authString;
        }

        @Override
        public Type getType() {
            return Type.KV;
        }
    }
}
