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

package oracle.nosql.proxy;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_MAX_AGE;
import static io.netty.handler.codec.http.HttpHeaderNames.ALLOW;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static oracle.nosql.common.http.Constants.CONNECTION;
import static oracle.nosql.common.http.Constants.CONTENT_LENGTH;
import static oracle.nosql.common.http.Constants.KEEP_ALIVE;

import static oracle.nosql.proxy.ProxySerialization.readString;
import static oracle.nosql.proxy.ProxySerialization.readDriverVersion;
import static oracle.nosql.proxy.ProxySerialization.readOpCode;
import static oracle.nosql.proxy.ProxySerialization.readQueryOperation;
import static oracle.nosql.proxy.ProxySerialization.readTimeout;
import static oracle.nosql.proxy.ProxySerialization.writeTableAccessInfo;
import static oracle.nosql.proxy.protocol.HttpConstants.REQUEST_ID_HEADER;
import static oracle.nosql.proxy.protocol.HttpConstants.USER_AGENT;
import static oracle.nosql.proxy.protocol.HttpConstants.X_RATELIMIT_DELAY;
import static oracle.nosql.proxy.protocol.HttpConstants.X_NOSQL_US;
import static oracle.nosql.proxy.protocol.JsonProtocol.OPC_REQUEST_ID;
import static oracle.nosql.proxy.protocol.Protocol.BAD_PROTOCOL_MESSAGE;
import static oracle.nosql.proxy.protocol.Protocol.ILLEGAL_ARGUMENT;
import static oracle.nosql.proxy.protocol.Protocol.KEY_SIZE_LIMIT_EXCEEDED;
import static oracle.nosql.proxy.protocol.Protocol.QUERY_V1;
import static oracle.nosql.proxy.protocol.Protocol.READ_LIMIT_EXCEEDED;
import static oracle.nosql.proxy.protocol.Protocol.ROW_SIZE_LIMIT_EXCEEDED;
import static oracle.nosql.proxy.protocol.Protocol.SERVER_ERROR;
import static oracle.nosql.proxy.protocol.Protocol.SIZE_LIMIT_EXCEEDED;
import static oracle.nosql.proxy.protocol.Protocol.TABLE_NOT_FOUND;
import static oracle.nosql.proxy.protocol.Protocol.TABLE_NOT_READY;
import static oracle.nosql.proxy.protocol.Protocol.UNKNOWN_ERROR;
import static oracle.nosql.proxy.protocol.Protocol.UNKNOWN_OPERATION;
import static oracle.nosql.proxy.protocol.Protocol.UNSUPPORTED_PROTOCOL;
import static oracle.nosql.proxy.protocol.Protocol.V3;
import static oracle.nosql.proxy.protocol.Protocol.V4;
import static oracle.nosql.proxy.protocol.Protocol.WRITE_LIMIT_EXCEEDED;
import static oracle.nosql.proxy.protocol.NsonProtocol.*;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.math.MathContext;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.KVVersion;
import oracle.kv.KeySizeLimitException;
import oracle.kv.ReadThroughputException;
import oracle.kv.ResourceLimitException;
import oracle.kv.ServerResourceLimitException;
import oracle.kv.TableSizeLimitException;
import oracle.kv.ValueSizeLimitException;
import oracle.kv.Version;
import oracle.kv.WriteThroughputException;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.query.PreparedStatementImpl;
import oracle.kv.impl.api.query.PreparedStatementImpl.DistributionKind;
import oracle.kv.impl.api.query.QueryStatementResultImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableAPIImpl.GeneratedValueInfo;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.ValueSerializer.RowSerializer;
import oracle.kv.impl.query.compiler.QueryControlBlock;
import oracle.kv.impl.query.runtime.ResumeInfo.VirtualScan;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.PasswordStore;
import oracle.kv.impl.security.wallet.WalletManager;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.util.RateLimiting;
import oracle.kv.query.BoundStatement;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.PrepareCallback;
import oracle.kv.query.PreparedStatement;
import oracle.kv.query.PrepareCallback.QueryOperation;
import oracle.kv.table.FieldValue;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.RecordValue;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.WriteOptions;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.http.LogControl;
import oracle.nosql.common.http.ServiceRequestHandler;
import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.common.kv.drl.LimiterManager;
import oracle.nosql.nson.Nson;
import oracle.nosql.nson.values.MapWalker;
import oracle.nosql.nson.values.PathFinder;
import oracle.nosql.nson.values.JsonUtils;
import oracle.nosql.proxy.Config.ProxyType;
import oracle.nosql.proxy.MonitorStats.DriverLang;
import oracle.nosql.proxy.MonitorStats.DriverProto;
import oracle.nosql.proxy.MonitorStats.OperationType;
import oracle.nosql.proxy.audit.ProxyAuditContextBuilder;
import oracle.nosql.proxy.audit.ProxyAuditManager;
import oracle.nosql.proxy.filter.FilterHandler;
import oracle.nosql.proxy.filter.FilterHandler.Action;
import oracle.nosql.proxy.filter.FilterHandler.Filter;
import oracle.nosql.proxy.protocol.ByteInputStream;
import oracle.nosql.proxy.protocol.ByteOutputStream;
import oracle.nosql.proxy.protocol.Protocol;
import oracle.nosql.proxy.protocol.Protocol.OpCode;
import oracle.nosql.proxy.rest.RequestParams;
import oracle.nosql.proxy.sc.GetTableResponse;
import oracle.nosql.proxy.sc.TableUtils;
import oracle.nosql.proxy.sc.TableUtils.MapPrepareCB;
import oracle.nosql.proxy.sc.TableUtils.PrepareCB;
import oracle.nosql.proxy.sc.TenantManager;
import oracle.nosql.proxy.security.AccessContext;
import oracle.nosql.proxy.security.AccessContext.Type;
import oracle.nosql.proxy.util.ErrorManager;
import oracle.nosql.proxy.util.ErrorManager.ErrorAction;
import oracle.nosql.proxy.util.TableCache;
import oracle.nosql.proxy.util.TableCache.TableEntry;
import oracle.nosql.util.filter.Rule;
import oracle.nosql.util.ph.HealthStatus;
import oracle.nosql.util.tmi.TableInfo;
import oracle.nosql.util.tmi.TableLimits;

import org.reactivestreams.Publisher;

/**
 * The base class of Data Service
 */
public abstract class DataServiceHandler implements Filter {

    static {
        /*
         * Don't use SubscriptionIter because proxy needs more information
         * from QueryStatementResultImpl like read/write cost, continuation
         * key etc.
         */
        System.setProperty("oracle.kv.execute.subscription.iterator", "false");
    }

    /*
     * Minimal query cost.
     */
    protected static final int MIN_QUERY_COST = 2;

    /**
     * The max number of faults to track for the purposes of log rate limiting.
     */
    private static final int LIMIT_FAULTS = 20;

    /**
     * Faults be logged at most once within the configured time period in ms.
     */
    private static final int LIMIT_SAMPLE_PERIOD = 60_000;

    /**
     * The extra wait time to enable tombstone for row deletion after the cached
     * table entry was not refreshed as scheduled. The motivation for extra wait
     * time is to avoid enabling tombstones too aggressively.
     *
     * While the proxy will refresh the table cache in a routine way, the SC
     * also asks the proxies to invalidate the cached table entry when the table
     * is turned to GAT or back to singleton during the execution of add or drop
     * replica ddl.
     *
     * If this request to flush the table from proxy caches fails, SC will keep
     * retrying until times out, the time out value is based on the table entry
     * refresh time + this extra wait time.
     *
     * A GAT table must run with tombstones for row deletion enabled, but since
     * tombstones will be enabled if table is still not refreshed after refresh
     * interval plus this period, add/drop replica ddl can safely continue its
     * execution instead of being blocked even if the SC is unable to invalidate
     * the table in any one of proxies. See NOSQL-689
     */
    private static final int EXTRA_WAIT_TIME_SET_TOMBSTONE = 120_000;
    /*
     * The size of the hash for the salted hash
     */
    private static final int hashSize = 32;

    private static final String saltAlias = "current-proxy-salt";
    private static final String oldSaltAlias = "old-proxy-salt";

    /**
     * Used by OPTIONS
     */
    private static final String ALLOWED_METHODS =
        (HttpMethod.GET + ", " +
         HttpMethod.POST + ", " +
         HttpMethod.DELETE + ", " +
         HttpMethod.PUT + ", " +
         HttpMethod.OPTIONS);

    /**
     * Used by OPTIONS
     */
    private static final String ACCESS_CONTROL_METHODS =
        (HttpMethod.GET + ", " +
         HttpMethod.POST + ", " +
         HttpMethod.DELETE + ", " +
         HttpMethod.PUT);

    /**
     * Used by OPTIONS.
     */
    private static final String ALLOWED_HEADERS =
        "Accept, Authorization, Cache-Control, Connection, Content-Length, " +
        "Content-Type, Date, keep-alive, host, Origin, Pragma, User-Agent, " +
        "X-Content-SHA256, X-Date, X-Requested-With, opc-request-id";

    protected final TableCache tableCache;
    protected final TenantManager tm;
    protected final ErrorManager errorManager;
    protected final LimiterManager limiterManager;
    protected final boolean drlOptInRequired;
    protected final MonitorStats stats;
    protected final ProxyAuditManager audit;
    protected final FilterHandler filter;
    protected final SkLogger logger;
    private final boolean traceQueries;
    private final boolean useAsync;
    private final boolean inTest;
    protected final boolean forceV3;
    protected final boolean forceV4;

    /*
     * The proxy version header string. Format is:
     *  "proxy-version=<version> kv-version=<version>"
     */
    protected String proxyVersionHeader;

    /*
     * "salt" to use for hashing prepared queries.
     */
    private byte[] salt;
    /* if available, used for a transition to replace the salt */
    @SuppressWarnings("unused")
    private byte[] oldSalt;

    /*
     * It is used to limit redundant logging within the configured time period.
     */
    protected final RateLimiting<String> limiter;

    private final Filter updateQueryFilter;

    /*
     * The ocid of tables with tombstones enabled due to its cached table entry
     * not being refreshed as scheduled.
     */
    protected Set<String> tableTombstoneEnabled;

    private final LogControl logControl;

    public DataServiceHandler(SkLogger logger,
                              TenantManager tm,
                              MonitorStats stats,
                              ProxyAuditManager audit,
                              FilterHandler filter,
                              ErrorManager errorManager,
                              LimiterManager limiterManager,
                              Config config,
                              LogControl logControl) {
        this.tableCache = tm.getTableCache();
        assert(tableCache != null);
        this.logger = logger;
        this.tm = tm;
        this.errorManager = errorManager;
        this.limiterManager = limiterManager;
        this.drlOptInRequired = config.getDRLOptInRequired();
        this.stats = stats;
        this.audit = audit;
        this.filter = filter;
        this.logControl = logControl;
        this.traceQueries = config.getQueryTracing();
        this.useAsync = config.getAsync();
        this.inTest = ProxyType.CLOUDTEST.equals(config.getProxyType());

        initSalt(config.getSaltWallet());

        limiter = new RateLimiting<String>(LIMIT_SAMPLE_PERIOD, LIMIT_FAULTS);
        updateQueryFilter = new UpdateQueryFilter();
        tableTombstoneEnabled = ConcurrentHashMap.newKeySet();
        setProxyVersionHeader();

        /* backward comp for tests */
        this.forceV3 = config.getForceV3() || Boolean.getBoolean("forceV3");
        this.forceV4 = config.getForceV4() || Boolean.getBoolean("forceV4");
        if (forceV3 && forceV4) {
            throw new IllegalArgumentException("forceV3 and forceV4 options " +
                          "are mutually exclusive");
        }
        if (forceV3 || forceV4) {
            logger.info("Proxy starting in force " + (forceV3?"V3":"V4") +
                        " mode.");
        }
    }

    /**
     * Check health status. For non-cloud service environments this is a
     * no-op. The Cloud Service DataService instances must override
     */
    public HealthStatus checkStatus(List<String> errors) {
        return HealthStatus.GREEN;
    }

    /*
     * Checks if there is a block-all rule and return its action handler if
     * exists.
     */
    protected Action checkBlockAll(LogContext lc) {
        if (filter != null) {
            Rule rule = filter.getFilterAllRule();
            if (rule != null) {
                return filter.getAction(rule);
            }
        }
        return null;
    }

    /*
     * Filters request based on the given OpCode.
     */
    protected void filterRequest(OpCode op, LogContext lc) {
        filterRequest(op, null, null, null, lc);
    }

    /*
     * Filters request based on OpCode, principal tenantId, principalId and
     * table ocid.
     */
    @Override
    public void filterRequest(OpCode op,
                              String principalTenantId,
                              String principalId,
                              String tableOcid,
                              LogContext lc) {
        if (filter == null) {
            return;
        }

        Rule rule = filter.matchRule(op,
                                     principalTenantId,
                                     principalId,
                                     tableOcid);
        if (rule != null) {
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("Block request [op=" + op.name() +
                            ", principalTenantId=" + principalTenantId +
                            ", principalId=" + principalId +
                            ", tableId=" + tableOcid + "]: " +
                            rule.toJson(), lc);
            }
            throw new FilterRequestException(rule);
        }
    }

    /*
     * Returns the Filter implementation used to filter the specific type of
     * query request, it is called by handleQuery().
     */
    protected Filter getQueryFilter(QueryOperation op) {
        if (op == QueryOperation.UPDATE || op == QueryOperation.DELETE) {
            return updateQueryFilter;
        }
        return this;
    }

    /*
     * Handles request by invoking the action of the filter rule recorded
     * in the given FilterRequestException.
     */
    protected FullHttpResponse handleFilterRequest(FilterRequestException fre,
                                                   String requestId,
                                                   LogContext lc) {
        if (filter != null) {
            final Rule rule = fre.getRule();
            return filter.getAction(rule).handleRequest(null,
                                                        requestId,
                                                        lc);
        }
        return null;
    }

    protected boolean getTraceQueries() {
        return traceQueries;
    }

    protected boolean useAsync() {
        return useAsync;
    }

    protected boolean inTest() {
        return inTest;
    }

    /**
     * Performs get operation.
     */
    protected Result doGet(TableAPIImpl tableApi,
                           RowSerializer key,
                           ReadOptions options) {
        return tableApi.getInternal(key, options);
    }

    /**
     * Performs delete operation.
     */
    protected Result doDelete(TableAPIImpl tableApi,
                              RowSerializer key,
                              Version matchVersion,
                              ReturnRow returnRow,
                              WriteOptions options) {

        if (matchVersion == null) {
            return tableApi.deleteInternal(key, returnRow, options);
        }
        return tableApi.deleteIfVersionInternal(key, matchVersion,
                                                returnRow, options);
    }

    /**
     * Performs prepare operation, this method also checks if the query
     * statement is supported.
     */
    protected PreparedStatementImpl doPrepare(KVStoreImpl store,
                                              String namespace,
                                              String statement,
                                              short queryVersion,
                                              String authString,
                                              PrepareCallback cb,
                                              RequestLimits limits,
                                              LogContext lc) {

        ExecuteOptions execOpts;
        execOpts = createExecuteOptions(namespace, authString, lc);
        execOpts.setIsCloudQuery(queryVersion > QUERY_V1 ? true : false)
                .setDriverQueryVersion(queryVersion)
                .setPrepareCallback(cb);
        if (limits != null) {
            execOpts.setMaxPrimaryKeySize(limits.getPrimaryKeySizeLimit())
                    .setMaxRowSize(limits.getRowSizeLimit());
        }

        return doPrepare(store, statement, execOpts);
    }

    protected PreparedStatementImpl doPrepare(KVStoreImpl store,
                                              String statement,
                                              ExecuteOptions execOpts) {

        PreparedStatementImpl prep;
        prep = (PreparedStatementImpl) store.prepare(statement, execOpts);
        return prep;
    }

    /**
     * Performs query operation.
     */
    protected QueryStatementResultImpl doQuery(KVStoreImpl store,
                                               PreparedStatement pstmt,
                                               Map<String, FieldValue> variables,
                                               ExecuteOptions execOpts,
                                               byte traceLevel,
                                               LogContext lc) {

        return doQuery(store, pstmt, variables, execOpts, -1 /* shardId */,
                       traceLevel, lc);
    }


    private BoundStatement bindVariables(PreparedStatement pstmt,
                                            Map<String, FieldValue> variables,
                                            byte traceLevel,
                                            LogContext lc) {

        BoundStatement bs = pstmt.createBoundStatement();
        for (Map.Entry<String, FieldValue> e : variables.entrySet()) {
            String name = e.getKey();
            FieldValue value = e.getValue();
            bs.setVariable(name, value);
            if (traceLevel >= 5) {
                trace("Bound variable " + name + " to value " + value,
                      lc);
            }
        }

        return bs;
    }

    protected QueryStatementResultImpl doQuery(KVStoreImpl store,
                                               PreparedStatement pstmt,
                                               Map<String, FieldValue> variables,
                                               ExecuteOptions execOpts,
                                               int shardId,
                                               byte traceLevel,
                                               LogContext lc) {

        QueryStatementResultImpl qres = null;

        if (variables != null) {
            pstmt = bindVariables(pstmt, variables, traceLevel, lc);
        }

        if (shardId == -1) {
            qres = (QueryStatementResultImpl)store.executeSync(pstmt, execOpts);
        } else {
            /* Execute the query only at the specified shard */
            RepGroupId rgid = new RepGroupId(shardId);
            Set<RepGroupId> shards = new HashSet<RepGroupId>(1);
            shards.add(rgid);
            if (traceLevel >= 2) {
                trace("Executing query on shard " + shardId, lc);
            }
            qres = (QueryStatementResultImpl)
                   store.executeSyncShards(pstmt, execOpts, shards);
        }

        return qres;
    }

    protected Publisher<RecordValue> doAsyncQuery(
        KVStoreImpl store,
        PreparedStatement pstmt,
        Map<String, FieldValue> variables,
        ExecuteOptions execOpts,
        int shardId,
        byte traceLevel,
        LogContext lc) {

        /* FUTURE: Set<Integer> partitions = null; */
        Set<RepGroupId> shards = null;
        if (shardId >= 0) { // TODO: is zero valid?
            shards = new HashSet<RepGroupId>(1);
            shards.add(new RepGroupId(shardId));
        }

        try {
            if (variables != null) {
                BoundStatement bs = bindVariables(pstmt, variables,
                                                  traceLevel, lc);

                return store.executeAsync(bs, execOpts, shards);
            }

            return store.executeAsync(pstmt, execOpts, shards);

        } catch (Throwable e) {
            if (traceLevel >= 1) {
                trace("Can't start async query: " + e, lc);
            }
            throw e;
        }
    }

    /**
     * Serialize the prepared query, returns the length of serialized bytes.
     *
     * Serialized query is:
     *  salted hash -- 32 bytes
     *  table access info (name, access type)
     *  prepared query itself
     */
    protected int serializePreparedQuery(PrepareCB cbInfo,
                                         PreparedStatementImpl prep,
                                         ByteOutputStream bbos)
        throws IOException {

        int startOffset = bbos.getOffset();
        /*
         * Skip hash, it is calculated and written below.
         */
        bbos.skipBytes(hashSize);
        int hashOffset = bbos.getOffset();

        /* table access info, this gets hashed along with the prepared query */
        writeTableAccessInfo(bbos, cbInfo);

        /* the prepared query */
        prep.serializeForProxy(bbos);

        /* now go back and write the actual length and hash */

        /*
         * length of the opaque byte[] as seen by the driver,
         * measured from after the length itself.
         */
        int totalLength = bbos.getOffset() - startOffset;

        /*
         * calculate the SHA-256 hash. Not all ByteBuf instances allow direct
         * access to the backing array. The current one used in this path
         * does not.
         */
        if (getSalt() != null) {
            ByteBuf buf = bbos.buffer();
            byte[] hashArray = null;
            int bytesToHash = totalLength - hashSize;
            if (buf.hasArray()) {
                /*
                 * NOTE: this generally will NOT happen given today's netty
                 * code
                 */
                hashArray = calculateHash(bbos.buffer().array(),
                                          hashOffset,
                                          bytesToHash);
            } else {
                byte[] backingArray = new byte[bytesToHash];
                buf.getBytes(hashOffset,
                             backingArray,
                             0, /* offset in backing array */
                             bytesToHash);
                hashArray = calculateHash(backingArray, 0, bytesToHash);
            }

            /* write the hash */
            bbos.writeArrayAtOffset(startOffset, hashArray);
        }

        /*
         * length of the opaque byte[] as seen by the driver,
         * measured from after the length itself.
         */
        return totalLength;
    }

    /**
     * Deserializes the prepared query
     */
    protected PreparedStatementWrapper deserializePreparedQuery(
        ByteInputStream in,
        int length,
        boolean skipHash) throws IOException {

        /*
         * Read the hash and compare it to the bytes in the payload. It is not,
         * unfortunately, possible to do this without making a copy of the
         * bytes to be hashed because there is no direct access to the
         * backing array in the payload's ByteBuf. TODO: consider how to
         * do this.
         */
        if (skipHash == false) {
            final byte[] hashFromClient = new byte[hashSize];
            in.readFully(hashFromClient);

            if (getSalt() != null) {
                /* now calculate the hash from the payload */
                int bytesToHash = length - hashSize;
                byte[] backingArray = new byte[bytesToHash];
                ByteBuf buf = in.buffer();
                buf.getBytes(in.getOffset(),
                             backingArray,
                             0, /* offset in backing array */
                             bytesToHash);
                byte[] hash =  calculateHash(backingArray, 0, bytesToHash);
                assert(hash != null);

                /* equal? */
                if (!Arrays.equals(hash, hashFromClient)) {
                    throw new IOException(
                    "Hash provided for prepared query does not match payload");
                }
            }
        }

        /* table access */
        int numTables = in.readByte();
        if (numTables < 1) {
            throw new IOException(
                "Expected 1 or more tables in prepared query, got: " +
                numTables);
        }

        PreparedStatementWrapper psw = new PreparedStatementWrapper();
        psw.namespace = readString(in);
        psw.targetTable = readString(in);
        if (numTables > 1) {
            String[] notTargetTables = new String[numTables - 1];
            for (int i = 0; i < notTargetTables.length; i++) {
                notTargetTables[i] = readString(in);
            }
            psw.notTargetTables = notTargetTables;
        }
        psw.operation = readQueryOperation(in);
        psw.statement = new PreparedStatementImpl(in);

        return psw;
    }

    /**
     * Creates WriteOptions
     */
    protected WriteOptions createWriteOptions(int timeoutMs,
                                              Durability durability,
                                              String authString,
                                              boolean doTombstone,
                                              LogContext lc) {
        return createWriteOptions(timeoutMs, durability, false /* updateTTL */,
                                  0, authString, doTombstone, lc);
    }

    protected WriteOptions createWriteOptions(int timeoutMs,
                                              Durability durability,
                                              boolean updateTTL,
                                              int identityCacheSize,
                                              String authString,
                                              boolean doTombstone,
                                              LogContext lc) {
        return new WriteOptions()
            .setTimeout(timeoutMs, TimeUnit.MILLISECONDS)
            .setUpdateTTL(updateTTL)
            .setDurability(durability)
            .setIdentityCacheSize(identityCacheSize)
            .setLogContext(new oracle.kv.impl.util.contextlogger.LogContext(lc))
            .setAuthContext(createAuthContext(authString))
            .setDoTombstone(doTombstone);
    }

    /**
     * Creates ReadOptions
     */
    protected ReadOptions createReadOptions(Consistency consistency,
                                            int timeoutMs,
                                            String authString,
                                            LogContext lc) {
        return new ReadOptions(consistency, timeoutMs, TimeUnit.MILLISECONDS)
            .setLogContext(new oracle.kv.impl.util.contextlogger.LogContext(lc))
            .setAuthContext(createAuthContext(authString));
    }

    /**
     * Creates ExecutionOptions for query compilation
     */
    protected ExecuteOptions createExecuteOptions(String namespace,
                                                  String authString,
                                                  LogContext lc) {
        /*
         * The integer version of the service id is used in the execute
         * options as it will be saved within the persisted data.
         */
        return new ExecuteOptions().setNamespace(namespace, false)
            .setLogContext(
                new oracle.kv.impl.util.contextlogger.LogContext(lc)).
                    setAuthContext(createAuthContext(authString))
            .setRegionId(tm.getLocalRegionId());
    }

    /**
     * Creates ExecutionOptions for query execution
     */
    protected ExecuteOptions createExecuteOptions(String namespace,
                                                  Consistency consistency,
                                                  int timeoutMs,
                                                  byte[] contKey,
                                                  int driverTopoSeqNum,
                                                  VirtualScan virtualScan,
                                                  int maxReadKB,
                                                  int maxWriteKB,
                                                  int limit,
                                                  MathContext mathContext,
                                                  byte traceLevel,
                                                  boolean logFileTracing,
                                                  String queryName,
                                                  int batchCounter,
                                                  short queryVersion,
                                                  long maxServerMemory,
                                                  String authString,
                                                  LogContext lc) {
        ExecuteOptions opts = createExecuteOptions(namespace, authString, lc);
        opts.setIsCloudQuery(queryVersion > QUERY_V1 ? true : false)
            .setDriverQueryVersion(queryVersion)
            .setConsistency(consistency)
            .setTimeout(timeoutMs, TimeUnit.MILLISECONDS)
            .setContinuationKey(contKey)
            .setDriverTopoSeqNum(driverTopoSeqNum)
            .setVirtualScan(virtualScan)
            .setMaxReadKB(maxReadKB)
            .setMaxWriteKB(maxWriteKB)
            .setMathContext(mathContext)
            .setTraceLevel(traceLevel)
            .setDoLogFileTracing(logFileTracing)
            .setQueryName(queryName)
            .setBatchCounter(batchCounter);

        if (maxServerMemory > 0) {
            opts.setMaxServerMemoryConsumption(maxServerMemory);
        }

        if (limit > 0) {
            opts.setResultsBatchSize(limit).setUseBatchSizeAsLimit(true);
        } else if (isOnPrem()) {
            opts.setUseBatchSizeAsLimit(true);
            /* The actual limit that will be used in this case is determined
             * by the ExecuteOptions.getResultsBatchSize() method */
        }
        return opts;
    }

    /**
     * Creates AuthContext
     */
    protected AuthContext createAuthContext(String authString) {
        return null;
    }

    /**
     * Adjusts the maxReadKB and return.
     * Note: this method is overwritten by KVDataService to simply return
     * the given maxReadKB
     */
    protected int adjustMaxReadKB(int maxReadKB, boolean isAbsolute,
                                  RequestLimits limits) {
        /* Return the system-defined read size limit if maxReadKB is 0 */
        int limit = limits.getRequestReadKBLimit();
        if (maxReadKB > limit) {
            throw new IllegalArgumentException(
                "Invalid maxReadKB, it can not exceed " +
                limit + ": " + maxReadKB, null);
        }

        if (maxReadKB == 0)  {
            maxReadKB = limit;
        }

        /*
         * KV will interpret the maxReadKB as read units because absolute
         * reads will consume 2x the size, so if absolute, double this
         * value.
         */
        if (isAbsolute) {
            maxReadKB <<= 1;
        }
        return maxReadKB;
    }

    /**
     * Return the update limit used for update query, the value passed in the
     * limit set by application.
     *
     * Note: this method is overridden by CloudDataService and
     * CloudRestDataService to return Integer.MAX_VALUE to not use this limit,
     * since the max number of records that can be updated in a single query is
     * limited by maxReadKB and maxWriteKB limits in cloud.
     */
    protected int getUpdateLimit(int value) {
        return value;
    }

    /**
     * Validates query. At this time just checks the length of
     * the statement size. It is limited if not an update. If it's an
     * update (insert, update) it needs to be long enough to handle
     * row size.
     *
     * NOTE: this only applies to insert/update/upsert. A query with a
     * predicate that attempts to compare very large field values (e.g. over 10k
     * bytes) will still fail in the cloud.
     */
    private void validateQuery(String statement, boolean isInsert,
                               RequestLimits limits) {

        if (limits == null) {
            return;
        }

        int sizeLimit = limits.getQueryStringSizeLimit();
        if (isInsert) {
            sizeLimit = limits.getQueryUpdateSizeLimit();
        }
        if (statement.length() > sizeLimit) {
            throw new IllegalArgumentException(
                "Query statement too long: " + statement.length() +
                ", maximum length is " + sizeLimit);
        }
    }

    /**
     * Validates the table version in PreparedStatement deserialized
     */
    protected TableEntry getTableAndCheckVersions(
        int prepVersion,
        String namespace,
        String tableName,
        LogContext lc) {

        int numRetries = 0; /* shouldn't be needed but loops are bad */
        TableEntry entry = null;
        while (true) {
            entry = getTableEntry(namespace, tableName, lc);
            int tableVersion = entry.getTable().getTableVersion();
            if (prepVersion > tableVersion && numRetries < 2) {
                /* there's a newer table out there, flush cache and retry */
                tableCache.flushEntry(namespace, tableName);
                ++numRetries;
                continue;
            }

            break;
        }
        return entry;
    }

    /**
     * Validates Table Limits
     */
    protected void validateTableLimits(TableLimits limits) {
        if (limits.getTableSize() <= 0) {
            throw new IllegalArgumentException("Invalid TableLimits, the " +
                "table storage size limit must be a value greater than 0");
        }
        if (limits.modeIsProvisioned()) {
            if (limits.getReadUnits() <= 0) {
                throw new IllegalArgumentException("Invalid TableLimits, the " +
                    "read throughput limit must be a value greater than 0");
            }
            if (limits.getWriteUnits() <= 0) {
                throw new IllegalArgumentException("Invalid TableLimits, the " +
                    "write throughput limit must be a value greater than 0");
            }
        } else if (limits.modeIsAutoScaling()) {
            if (limits.getReadUnits() > 0) {
                throw new IllegalArgumentException("Invalid TableLimits, " +
                    "cannot set read throughput limit for auto scaling table");
            }
            if (limits.getWriteUnits() > 0) {
                throw new IllegalArgumentException("Invalid TableLimits, " +
                    "cannot set write throughput limit for auto scaling table");
            }
        } else {
            throw new IllegalArgumentException("Invalid TableLimits, " +
                    "unknown mode");
        }
    }

    protected void validateUsageTimeRange(long startRange,
                                          long endRange,
                                          String startTitle,
                                          String endTitle) {

        if (startRange < 0 || endRange < 0) {
            throw new RequestException(ILLEGAL_ARGUMENT,
                "The " + ((startRange < 0) ? startTitle : endTitle) +
                " must not be negative value: " +
                ((startRange < 0) ? startRange : endRange));
        }

        if (startRange > 0 && endRange > 0 && endRange < startRange) {
            throw new RequestException(ILLEGAL_ARGUMENT,
                "The " + endTitle + " must be greater than " + startTitle);
        }
    }

    protected void trace(String msg, LogContext lc) {
        /*
         * TODO: log at level INFO. Might it be possible to use LogControl
         * to handle this, or even force the tracing level for a given tenant
         * rather than rely on driver calls? E.g. using LogControl to:
         *   set traceLevel to N for tenant T in all calls and change the
         *   log level so that query traces are logged.
         */
        logger.info(msg, lc);
        System.out.println(System.currentTimeMillis() + " PROXY: " + msg);
    }

    protected boolean isAbsolute(ReadOptions options) {
        return isAbsolute(options.getConsistency());
    }

    protected boolean isAbsolute(Consistency consistency) {
        return consistency == Consistency.ABSOLUTE;
    }

    /**
     * Initialize the salt value used for hashing prepared queries. If it
     * cannot be found the queries will not be hashed or validated. This
     * should not happen in the cloud-based service itself.
     */
    private void initSalt(File walletDir) {
        if ((walletDir == null) ||(!walletDir.exists())){
            logger.info("No wallet at " + walletDir + ", there will be " +
                        "no hash on prepared queries");
            return;
        }
        logger.info("Using wallet at " + walletDir);
        char[] csalt = getSecret(walletDir.toString(),
                                 saltAlias);
        char[] osalt = getSecret(walletDir.toString(),
                                 oldSaltAlias);
        if (csalt != null) {
            /* this is temporary */
            logger.info("Found salt value in wallet");

            salt = getBytes(csalt);
        } else {
            logger.info("Unable to find salt for alias " + saltAlias +
                    " in wallet");
        }
        if (osalt != null) {
            oldSalt = getBytes(osalt);
            logger.info("Found old salt value in wallet");
        }
    }

    /**
     * Calculate a salted SHA-256 hash on the array specified by the
     * parameters plus additional state. The "salt" comes from local
     * configuration.
     *
     * Components of this opaque array (see serializePreparedQuery,
     * which writes the table info just before the prepared query):
     *  1. hash (salted hash for validation on return)
     *  2. table access info [tableName, access type]
     *  3. the prepared query itself
     */
    private byte[] calculateHash(byte[] backingArray, int offset, int length) {
        if (getSalt() != null) {
            try {
                MessageDigest md = MessageDigest.getInstance("SHA-256");
                md.update(backingArray, offset, length);
                return md.digest();
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("Bad hash algorithm: SHA-256");
            }
        }
        return null;
    }

    private byte[] getSalt() {
        return salt;
    }

    private byte[] getBytes(char[] chars) {
        byte[] bytes = new byte[chars.length];
        for(int i = 0; i < chars.length; i++) {
            bytes[i] = (byte) chars[i];
        }
        return bytes;
    }

    /**
     * Handle throughput/limit exceptions.
     * TODO: specify and handle operation throttling exceptions that
     * are generated locally (vs by the store) and indicate excessive
     * non-data operations. These are OPERATION_LIMIT_EXCEEDED errors.
     *
     * NOTE: there are a number of subclasses of ResourceLimitException and
     * not all are retry-able. This means they (1) need to be examined
     * individually and (2) this code is fragile to the addition of new
     * subclasses because there isn't a single "correct" error. For this
     * reason exceptions that aren't explicitly caught will result in
     * UKNOWN_ERROR, which isn't ideal but there is no other reasonable
     * alternative. Many of the subclasses will never be thrown to the
     * proxy at all.
     */
    protected void handleLimitException(ResourceLimitException rle) {
        int code;
        String msg = null;
        /*
         * Don't show the actual rate. It is often not precise in this context
         * and can be misleading. The information may eventually be dropped
         * from the ResourceLimitException.
         */
        final String fmt = "%s throughput rate exceeded for table " +
            rle.getTableName() + ". Limit: %,d Units/Sec";
        try {
            throw rle;
        } catch (ReadThroughputException rte) {
            msg = String.format(fmt, "Read", rte.getReadRateLimit());
            code = READ_LIMIT_EXCEEDED;
        } catch (WriteThroughputException wte) {
            msg = String.format(fmt, "Write", wte.getWriteRateLimit());
            code = WRITE_LIMIT_EXCEEDED;
        } catch (TableSizeLimitException tsle) {
            code = SIZE_LIMIT_EXCEEDED;
        } catch (KeySizeLimitException ksle) {
            code = KEY_SIZE_LIMIT_EXCEEDED;
            msg = ksle.getMessage() + ": ";
            if (ksle.getIndexName() != null) {
                msg += ksle.getIndexName() + " on "  + ksle.getTableName();
            } else {
                msg += ksle.getTableName();
            }
        } catch (ValueSizeLimitException vsle) {
            code = ROW_SIZE_LIMIT_EXCEEDED;
            msg = vsle.getMessage() + ": " + vsle.getTableName();
        } catch (ServerResourceLimitException vsle) {
            /* this is retry-able if ever thrown here */
            code = SERVER_ERROR;
        } catch (Exception ex) {
            code = UNKNOWN_ERROR;
        }

        throw new RequestException(code,
                                   "Operation failed with limit exception: " +
                                   ((msg != null) ? msg : rle.getMessage()));
    }

    protected void handleIOException(String message, IOException ioe) {
        StringBuilder sb = new StringBuilder();
        if (message != null) {
            sb.append(message);
            sb.append(": ");
        }
        sb.append((ioe instanceof EOFException && ioe.getMessage() == null) ?
                  "Reached end of input stream" : ioe.getMessage());
        throw new RequestException(BAD_PROTOCOL_MESSAGE, sb.toString());
    }

    protected int getRequestExceptionFailure(RequestException re) {
        final int errorCode = re.getErrorCode();
        if (Protocol.isUserThrottling(errorCode)) {
            return 2;
        }
        if (Protocol.isUserFailure(errorCode)) {
            return 3;
        }
        return 1; /* server failure and unknown failure */
    }

    /**
     * RequestException is usually related to an application issue, so only
     * log if the exception is unknown or a throttling exception. Others, such
     * as IllegalArgumentException, will pass through, unlogged.
     *
     * NOTE: maybe log throttling exceptions at FINE vs INFO
     */
    protected void logRequestException(RequestException re, LogContext lc) {
        switch(re.getErrorCode()) {
        case UNKNOWN_ERROR:
        case UNKNOWN_OPERATION:
            logger.warning("Unknown error code or operation: " +
                           re.getErrorCode() + ": " + re.toString(),lc);
            break;
        case SIZE_LIMIT_EXCEEDED:
        case READ_LIMIT_EXCEEDED:
        case WRITE_LIMIT_EXCEEDED:
            if (logger.isLoggable(Level.FINE, lc)) {
                logger.fine("Table limit exception: " + re.getMessage(), lc);
            }
            break;
        case BAD_PROTOCOL_MESSAGE:
            logger.warning("Bad protocol message: " + re.getMessage(), lc);
            break;
        }
    }

    /*
     * Monitor stats related methods
     */

    protected void markOpStart(RequestContext rc) {
        if (rc.statsMarked) {
            return;
        }
        rc.statsMarked = true;
        if (stats != null) {
            stats.markOpStart();
        }
    }

    protected void markDataOpSucceeded(RequestContext rc,
                                       int dataOpCount,
                                       int readKB,
                                       int writeKB) {
        if (!rc.statsMarked) {
            return;
        }
        rc.statsMarked = false;
        if (stats != null) {
            stats.markDataOpSucceeded(rc.startTimeNs, checkOpType(rc),
                                      dataOpCount,
                                      readKB, writeKB,
                                      rc.driverLang, rc.driverProto,
                                      rc.numRetries);
        }
    }

    private OperationType checkOpType(RequestContext rc) {
        if (rc != null && rc.opType != null) {
            return rc.opType;
        }
        return OperationType.UNKNOWN;
    }

    protected void markOpFailed(RequestContext rc,
                                int failureType) {
        if (!rc.statsMarked) {
            return;
        }
        rc.statsMarked = false;
        if (stats != null) {
            stats.markOpFailed(rc.startTimeNs,
                               checkOpType(rc), failureType,
                               rc.driverLang, rc.driverProto,
                               rc.numRetries);
        }
    }

    protected void markTMOpSucceeded(RequestContext rc,
                                     int tmOpCount) {
        if (!rc.statsMarked) {
            return;
        }
        rc.statsMarked = false;
        if (stats != null) {
            stats.markTMOpSucceeded(rc.startTimeNs, checkOpType(rc),
                                    rc.driverLang, rc.driverProto,
                                    tmOpCount, rc.numRetries);
        }
    }

    protected void startAudit(RequestContext rc,
                              String tableName,
                              AccessContext actx) {
        if (audit == null || !audit.isAllowed(rc.opCode)) {
            return;
        }
        rc.auditBuilder = audit.startAudit();
        rc.auditBuilder.setRequest(rc.request, rc.ctx,
                                   rc.requestId, tableName, actx);
    }

    protected void endAudit(FullHttpResponse response,
                            RequestContext rc) {
        if (audit == null || response == null ||
            rc.auditBuilder == null) {
            return;
        }
        rc.auditBuilder.setResponse(response);
        audit.endAudit(rc.auditBuilder);
    }

    protected void auditOperation(RequestContext rc,
                                  TableInfo tableInfo,
                                  int errorCode) {
        if (audit == null || rc.auditBuilder == null ||
            rc.opCode == null) {
            return;
        }
        if (errorCode == Protocol.OPERATION_LIMIT_EXCEEDED) {
            /*
             * Don't audit rate limited operations, to avoid a DOS attack on
             * the audit system.
             */
            return;
        }
        rc.auditBuilder.setOperation(rc.opCode, tableInfo);
    }


    /**
     * Send response back to client.
     * This includes:
     *  - adding http headers
     *  - write and flush response to channel context
     *
     * After this, no additional processing of this response is
     * necessary - in fact any more processing would be an error.
     */
    protected void sendResponse(FullHttpResponse response,
                                RequestContext rc) {

        if (rc == null || response == null || rc.ctx == null) {
            return;
        }

        ServiceRequestHandler.addRequiredHeaders(response);

        if (rc.keepAlive) {
            response.headers().set(CONNECTION, KEEP_ALIVE);
        }

        /*
         * set the time spent into a header. This can be unconditional as
         * it doesn't affect clients not looking for the header
         */
        long diffUs = (System.nanoTime() - rc.startTimeNs) / 1000;
        response.headers().set(X_NOSQL_US, "" + diffUs);

// uncomment to print NSON response output
// TODO: FINEST
//try {
//ByteInputStream bbis = new ByteInputStream(response.content());
//System.out.println(printNson(bbis,true));
//bbis.close();
//} catch (Exception e) {}

        /* Note: writeAndFlush will release the response */
        final ChannelFuture f = rc.ctx.writeAndFlush(response);

        if (rc.keepAlive == false) {
            f.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) {
                    rc.ctx.close();
                }
            });
        }
    }

    protected AccessContext checkAccess(FullHttpRequest request,
                                        OpCode opCode,
                                        AccessContext actx,
                                        String tableName,
                                        Filter filter,
                                        LogContext lc) {
        return actx;
    }

    /*
     * Check authorization for this operation
     */
    protected abstract AccessContext checkAccess(FullHttpRequest request,
                                                 String compartmentId,
                                                 OpCode opCode,
                                                 String tableName,
                                                 Filter filter,
                                                 LogContext lc);

    /*
     * Check authorization for the operation accessing multiple tables, used
     * by prepare or query operation.
     */
    protected AccessContextHelper checkAccess(FullHttpRequest request,
                                              String compartmentId,
                                              OpCode opCode,
                                              String targetTable,
                                              String[] notTargetTables,
                                              Filter filter,
                                              LogContext lc) {
        assert(targetTable != null);

        AccessContext actx = checkAccess(request, compartmentId, opCode,
                                         targetTable, filter, lc);
        if (notTargetTables == null) {
            return new SingleAccessContextHelper(actx);
        }

        /* The key of tableActxMap is table name, it is case insensitive */
        Map<String, AccessContext> actxMap =
            new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        actxMap.put(targetTable, actx);

        for (String name : notTargetTables) {
            actx = checkAccess(request, compartmentId, opCode, name, filter, lc);
            actxMap.put(name, actx);
        }
        return new MultipleAccessContextHelper(actxMap);
    }

    protected AccessContextHelper checkAccess(FullHttpRequest request,
                                              OpCode opCode,
                                              AccessContext callerActx,
                                              String targetTable,
                                              String[] notTargetTables,
                                              Filter filter,
                                              LogContext lc) {
        assert(targetTable != null);

        AccessContext actx = checkAccess(request, opCode, callerActx,
                                         targetTable, filter, lc);
        if (notTargetTables == null) {
            return new SingleAccessContextHelper(actx);
        }

        /* The key of tableActxMap is table name, it is case insensitive */
        Map<String, AccessContext> actxMap =
            new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        actxMap.put(targetTable, actx);

        for (String name : notTargetTables) {
            actx = checkAccess(request, opCode, callerActx, name, filter, lc);
            actxMap.put(name, actx);
        }
        return new MultipleAccessContextHelper(actxMap);
    }

    /**
     * Default - nothing to do, return the same name.
     */
    protected String mapTableName(String tableName) {
        return tableName;
    }

    private static char[] getSecret(String walletPath, String alias) {
        try {
            PasswordStore ws = new WalletManager().
                getStoreHandle(new File(walletPath));
            ws.open(null);
            return ws.getSecret(alias);
        } catch (Exception e) {
            throw new IllegalStateException("Wallet secret cannot be read " +
                                            e.getMessage());
        }
    }

    /**
     * Default - nothing to do, return the same name.
     */
    protected String mapTableName(AccessContext actx, OpCode op, String name) {
        return name;
    }

    protected PrepareResult doPrepare(RequestContext rc,
                                      AccessContextHelper actxHelper,
                                      PrepareCB pcb,
                                      String ns,
                                      OpCode op,
                                      ExecuteOptions opts,
                                      String statement,
                                      short queryVersion,
                                      LogContext lc) {

        PrepareCB cb = null;
        String tableName = null;
        boolean retry = true;
        TableEntry entry = null;

        /* Get AccessContext of target table */
        final String targetTable = pcb.getTableName();
        rc.origTableName = targetTable;
        AccessContext actx = actxHelper.get(targetTable);
        while (true) {
            /*
             * Check access again since table name and query operation
             * are available until now.
             * This check is for cloud only.
             */
            if (actx.getType() == Type.IAM) {
                if (ns == null) {
                    ns = actx.getCompartmentId();
                }

                if (!retry) {
                    actxHelper = checkAccess(rc.request, ns, op, targetTable,
                                             pcb.getNotTargetTables(),
                                             this, lc);
                    actx = actxHelper.get(targetTable);
                }

                tableName = mapTableName(actx, op, targetTable);
                cb = new MapPrepareCB(actxHelper);
                ns = actx.getNamespace();

                if (opts != null) {
                    opts.setPrepareCallback(cb);
                }
            } else {
                tableName = targetTable;
            }
            rc.mappedTableName = tableName;

            try {
                entry = getTableEntry(ns, tableName, lc);

                KVStoreImpl store = entry.getStore();
                validateQuery(statement,
                              pcb.isInsertOp(),
                              entry.getRequestLimits());

                RequestLimits lmt = entry.getRequestLimits();

                PreparedStatementImpl pstmt;
                if (opts != null) {
                    if (lmt != null) {
                        opts.setMaxPrimaryKeySize(lmt.getPrimaryKeySizeLimit());
                        opts.setMaxRowSize(lmt.getRowSizeLimit());
                    }
                    pstmt = doPrepare(store, statement, opts);
                } else {
                    pstmt = doPrepare(store, ns, statement, queryVersion,
                                      actx.getAuthString(), cb, lmt, lc);
                }
                return new PrepareResult(entry, pstmt);
            } catch (RequestException | IllegalArgumentException e) {
                handlePrepareTableNotFound(actx, e, entry, tableName, retry);

                /* reset table name mapping and retry */
                retry = false;
                actx.resetTableNameMapping();
                continue;
            }
        }
    }

    private void handlePrepareTableNotFound(AccessContext actx,
                                            RuntimeException e,
                                            TableEntry te,
                                            String tableName,
                                            boolean retry) {

        /*
         * Proxy may have obsolete table name mapping. If table ocid
         * doesn't exist in the table cache, a RequestException will
         * be thrown. In addition, it's possible for table ocid in the
         * table cache, but not in the KVStore, query compilation fails
         * with an IllegalArgumentException.
         *
         * Reset the table name mapping and retry prepare.
         */
        /* handle this case for cloud only */
        if (!retry || actx.getType() != Type.IAM) {
            throw e;
        }

        /* table not found in table cache */
        if (e instanceof RequestException &&
            ((RequestException)e).getErrorCode() != TABLE_NOT_FOUND) {
            throw e;
        }

        /*
         * If drop table has been processed on different proxy, it's possible
         * that the table with old ocid has cached, prepare will fail when
         * translator parses the query statement and IllegalArgumentException
         * in this case. Check its message and verify if table exists.
         */
        if (e instanceof IllegalArgumentException) {
            if (te == null  ||
                !e.getMessage().contains("does not exist") ||
                te.getTableAPI().getTable(tableName) != null) {
                throw e;
            }
        }
    }

    static String getProxyVersion() {
        try {
            Enumeration<URL> resources = ProxyMain.class.getClassLoader()
                .getResources("META-INF/MANIFEST.MF");
            while (resources.hasMoreElements()) {
                Manifest manifest =
                    new Manifest(resources.nextElement().openStream());
                Attributes attrs = manifest.getMainAttributes();
                String main =
                    attrs.getValue(Attributes.Name.MAIN_CLASS);
                if (main != null &&
                    main.toLowerCase().contains("proxymain")) {
                    return attrs.getValue(
                        Attributes.Name.IMPLEMENTATION_VERSION);
                }
            }
        } catch (IOException e) {
            /* ignore */
        }
        return null;
    }

    /*
     * Useful for debugging
     */
    public static String printNson(ByteInputStream bis, boolean pretty) {
        int off = bis.getOffset();
        String ret;
        try {
            ret = JsonUtils.fromNson(bis, pretty);
        } catch (Exception e) {
            ret = "Error parsing nson: " + e.getMessage();
        }
        bis.setOffset(off);
        return ret;
    }

    /*
     * Get proxyVersion, acquired from MANIFEST in jar, get KV version
     * from client jar.
     */
    private void setProxyVersionHeader() {
        StringBuilder sb = new StringBuilder();
        sb.append("proxy=");
        sb.append(getProxyVersion());
        sb.append(" ");
        sb.append(" kv=");
        sb.append(KVVersion.CURRENT_VERSION.getNumericVersionString());
        proxyVersionHeader = sb.toString();
    }

    /*
     * Changes table activity state to ACTIVE
     *
     * Just log warning if activate failed, the table will reactivated in
     * next operation if it is still inactive
     */
    protected void activateTable(AccessContext actx,
                                 String tableNameOrId,
                                 LogContext lc) {
        String tableOcid = actx.getTableIdExternal(tableNameOrId);
        try {
            GetTableResponse res =
                TableUtils.setTableActive(actx, tableNameOrId, tm, lc);
            if (!res.getSuccess()) {
                logger.warning("Fail to activate table " + tableOcid +
                    ": "+ res.getErrorString() + ", error code " +
                    res.getErrorCode());
            }
            logger.info("Table was activated: " + tableOcid);
        } catch (Throwable t) {
            logger.warning("Fail to activate table " + tableOcid + ": "+
                           t.getMessage());
        }
    }

    protected TableEntry getTableEntry(AccessContext actx,
                                       String tableName,
                                       LogContext lc) {
        return getTableEntry(actx.getNamespace(), tableName, lc);
    }

    protected TableEntry getTableEntry(String namespace,
                                       String tableName,
                                       LogContext lc) {
        TableEntry entry = tableCache.getTableEntry(namespace,
                                                    tableName,
                                                    lc);
        if (!entry.isInitialized()) {
            throw new RequestException(TABLE_NOT_READY,
                "Table is not ready for DML operation");
        }
        return entry;
    }

    /*
     * This is for cloud only.
     *
     * Enabling tombstone for row deletion if the table is GAT or the table
     * entry in cache is considered stale.
     *
     * If table entry was not refreshed within the time period of cache refresh
     * time + additional wait time(2 minutes), the table entry is considered
     * stale. It is possible that the table has been converted to GAT but table
     * entry in cache was not refreshed. In order to ensure data correctness,
     * enable tombstone for this case. Note that enabling tombstones for a table
     * is never incorrect, but it is wasteful for a singleton table.
     */
    protected boolean doTombStone(TableEntry entry) {
        if (isOnPrem()) {
            return false;
        }

        if (entry.isMultiRegion()) {
            return true;
        }

        if (tableCache.getEntryRefreshMs() > 0) {
            boolean ret = (System.currentTimeMillis() - entry.getLastRefresh()) >
                          tableCache.getEntryRefreshMs() +
                          EXTRA_WAIT_TIME_SET_TOMBSTONE;
            if (ret) {
                tableTombstoneEnabled.add(entry.getTable().getName());
            }
            return ret;
        }
        return false;
    }

    /*
     * This is a heuristic to filter out stack traces if they appear
     * in an error message, probably from kv. Java's
     * Throwable.printStackTrace does this
     *   println("\tat " + traceElement)
     *
     * This implies that searching for "\tat " is a reasonable way to
     * find the first stack frame. There is usually (always?) a newline before
     * the first stack frame, so look for a preceding "\n" and eliminate that as
     * well.
     *
     * There is another case where error filtering is handy. If an index
     * creation operation fails because of an existing row that has an
     * entry that exceeds the key size limit the error is a failed kv "plan"
     * which has a large amount of confusing information that is intended
     * for KV admins to help debugging. While not a stack trace the information
     * is confusing and not useful, so detect that case and extract the actual
     * message.
     *  o look for WaitForAddIndex in the message
     *  o if found, extract the portion that includes something like
     *   java.lang.IllegalArgumentException: KeySizeLimitException index
     *       key of 68 bytes exceeded limit of 64 (22.4.15) on
     *       [2023-07-20 20:37:24.230 UTC] (22.4.15)
     */
    protected static String filterStackTraces(String message) {
        final String stackElementStart = "\tat ";
        final String indexPlan = "WaitForAddIndex";
        final String keySizeString = "KeySizeLimitException";

        if (message == null) {
            return message;
        }

        int index = message.indexOf(stackElementStart);
        if (index != -1) {
            /* back up over newline if present */
            if (message.charAt(index -1) == '\n') {
                index -= 1;
            }
            return message.substring(0, index);
        }

        /*
         * look for index key size error. Ideally if this is found the
         * exception/error could be mapped to KEY_SIZE_LIMIT_EXCEEDED but
         * we are too far into error processing to change the error here.
         * It'd need to be done sooner. This will be IAE, which is reasonable
         */
        index = message.indexOf(indexPlan);
        if (index != -1) {
            int excIndex = message.indexOf(keySizeString, index);
            if (excIndex != -1) {
                /* found exception, search to end of line, return substring */
                int endIndex = message.indexOf("\n", excIndex);
                if (endIndex != -1) {
                    return message.substring(excIndex, endIndex);
                } else {
                    return message.substring(excIndex);
                }
            }
        }
        return message;
    }

    /*
     * Small wrapper class to return multiple pieces of information from
     * a serialized prepared query.
     */
    protected static class PreparedStatementWrapper {
        PreparedStatementImpl statement;
        String namespace;
        String targetTable;
        String[] notTargetTables;
        QueryOperation operation;

        public PreparedStatementImpl getPreparedStatement() {
            return statement;
        }

        public String getNamespace() {
            return namespace;
        }

        public String getTableName() {
            return targetTable;
        }

        public String[] getNotTargetTables() {
            return notTargetTables;
        }

        public boolean isUpdateOp() {
            return (operation == QueryOperation.INSERT ||
                    operation == QueryOperation.UPDATE ||
                    operation == QueryOperation.DELETE);
        }

        public QueryOperation getOperation() {
            return operation;
        }

        @Override
        public String toString() {
            return statement.toString();
        }
    }

    /*
     * The exception thrown when the request matches a filter rule, the matched
     * rule is recorded into the exception.
     */
    public static class FilterRequestException extends RuntimeException {

        private static final long serialVersionUID = 1L;
        private final Rule rule;

        FilterRequestException(Rule rule) {
            this.rule = rule;
        }

        public Rule getRule() {
            return rule;
        }
    }

    /**
     * The filter for update query.
     *
     * Besides the given OpCode, it will check additional OpCode.GET operation,
     * since the update query internally performs read and write.
     */
    protected class UpdateQueryFilter implements Filter {
        private final OpCode internalOp = OpCode.GET;

        @Override
        public void filterRequest(OpCode op,
                                  String principalTenantId,
                                  String principalId,
                                  String tableName,
                                  LogContext lc) {

            DataServiceHandler.this.filterRequest(internalOp,
                                                  principalTenantId,
                                                  principalId,
                                                  tableName,
                                                  lc);
            DataServiceHandler.this.filterRequest(op,
                                                  principalTenantId,
                                                  principalId,
                                                  tableName,
                                                  lc);
        }
    }

    /**
     * Increment error rate for a client.
     * @return true if the response will be handled later (or not at all).
     */
    protected boolean incrementErrorRate(FullHttpResponse response,
                                         RequestContext rc) {
        if (errorManager == null) {
            return false;
        }
        ErrorAction act = errorManager.incrementErrorRate(response,
                      rc.clientIP,
                      rc.excessiveUse,
                      rc.ctx,
                      () -> {
                          endAudit(response, rc);
                          sendResponse(response, rc);
                      });
        switch (act) {
            case RESPOND_NORMALLY:
                return false;
            case RESPONSE_DELAYED:
                return true;
            case DO_NOT_RESPOND:
                /* don't audit DDoS ops: see auditOperation() */
                /* endAudit(response, ctx); */
                /* we must release response buffer here */
                response.content().release();
                return true;
        }
        return false;
    }

    /*
     * Return true if the request specifies it wants to get throttling
     * errors instead of being slowed down due to distributed rate
     * limiting. This also checks for DRL opt-in.
     */
    protected boolean requestWantsThrottlingErrors(
                                        LimiterManager limiterManager,
                                        RequestContext rc) {
        if (limiterManager == null || rc.preferThrottlingErrors == false) {
            return false;
        }
        /*
         * If DRL opt-in is required, only do DRL if the request specifies
         * it has opted in to DRL.
         */
        if (drlOptInRequired == true && rc.drlOptedIn == false) {
            return false;
        }
        return true;
    }

    /*
     * Rate limiting logic: apply the used read and write units
     * to the rate limiter(s) for the table.
     * This may result in the response being queued for later
     * delivery (response delayed).
     *
     * return true if the response will be delayed and delivered
     * later (by a different thread).
     */
    protected boolean rateLimitResponse(LimiterManager limiterManager,
                                        RequestContext rc,
                                        FullHttpResponse response) {
        if (limiterManager == null || rc == null ||
            response == null ||
            (rc.usedReadKB == 0 && rc.usedWriteKB == 0)) {
            return false;
        }

        /*
         * If DRL opt-in is required, only do DRL if the request specifies
         * it has opted in to DRL.
         */
        if (drlOptInRequired == true && rc.drlOptedIn == false) {
            return false;
        }

        TableEntry entry = rc.entry;
        if (entry == null) {
            /*
             * operations on multiple tables will have non-null tableOpInfos.
             * use the first one: the logic in LimiterManager will get the
             * parent table and operate on that.
             */
            if (rc.tableOpInfos == null || rc.tableOpInfos.size() == 0) {
                return false;
            }
            entry = rc.tableOpInfos.get(0).entry;
            if (entry == null) {
                return false;
            }
        }
        /* reduce timeout by time already spent */
        int timeoutMs =
            (int)((rc.endTimeNs - System.nanoTime()) / 1_000_000L);
        if (timeoutMs < 0 || rc.preferThrottlingErrors) {
            /* timeoutMs==0 will update limiters and immediately return */
            timeoutMs = 0;
        }
        int delayMs = limiterManager.delayResponse(
                          (TableImpl)entry.getTable(),
                          entry.getStoreName(),
                          entry.getStore(),
                          rc.usedReadKB, rc.usedWriteKB, timeoutMs, rc.lc,
                          () -> {
                              endAudit(response, rc);
                              sendResponse(response, rc);
                          });
        if (delayMs > 0) {
            /* response will be sent later by a different thread */
            /* set delayed time in response header */
            response.headers().set(X_RATELIMIT_DELAY, delayMs);
            return true;
        }
        return false;
    }

    /**
     * The return class of doPrepare() that encapsulates the store handle and
     * the prepared statement.
     */
    protected class PrepareResult {
        private final TableEntry entry;
        private final PreparedStatementImpl pstmt;

        PrepareResult(TableEntry entry, PreparedStatementImpl pstmt) {
            this.entry = entry;
            this.pstmt = pstmt;
        }

        public TableEntry getEntry() {
            return entry;
        }

        public KVStoreImpl getStore() {
            return entry.getStore();
        }

        public PreparedStatementImpl getPreparedStatement() {
            return pstmt;
        }
    }

    /**
     * Is this handler for On-Prem use?
     * Public for access from NsonProtocol
     */
    public boolean isOnPrem() {
        return false;
    }

    /**
     * OPTIONS /<API_VERSION>/*
     *
     * HTTP OPTIONS used for pre-flight request.
     */
    protected FullHttpResponse handleOptions(FullHttpRequest request,
                                             LogContext lc) {

        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK);

        String allowedHeaders = ALLOWED_HEADERS;
        String moreHeaders = getAdditionalAllowedHeaders();
        if (moreHeaders != null) {
            allowedHeaders += ", " + moreHeaders;
        }

        response.headers()
            .set(ALLOW, ALLOWED_METHODS)
            .set(ACCESS_CONTROL_ALLOW_HEADERS, allowedHeaders)
            .set(ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_METHODS)
            .set(ACCESS_CONTROL_MAX_AGE, 86400)  /* one day */
            .set(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .setInt(CONTENT_LENGTH, 0);
        return response;
    }

    protected String getAdditionalAllowedHeaders() {
        return null;
    }

    protected static String topTableName(String tableName) {
        int idx = tableName.indexOf(".");
        if (idx > 0) {
            return tableName.substring(0, idx);
        }
        return tableName;
    }

    /**
     * A helper class to get the AccessContext object associated with the
     * specified table.
     */
    public static interface AccessContextHelper {
        AccessContext get(String tableName);
    }

    /**
     * Represents for single AccessContext object
     */
    private static class SingleAccessContextHelper
        implements AccessContextHelper {

        private final AccessContext actx;

        SingleAccessContextHelper(AccessContext actx) {
            this.actx = actx;
        }

        @Override
        public AccessContext get(String tableName) {
            if (tableName.equalsIgnoreCase(
                    actx.getOriginalTableName(tableName))) {
                return actx;
            }
            return null;
        }
    }

    /**
     * Represents for multiple AccessContext objects
     */
    private static class MultipleAccessContextHelper
        implements AccessContextHelper {

        private final Map<String, AccessContext> actxMap;

        MultipleAccessContextHelper(Map<String, AccessContext> actxMap) {
            this.actxMap = actxMap;
        }

        @Override
        public AccessContext get(String tableName) {
            return actxMap.get(tableName);
        }
    }

    /**
     * An interface used to create a RequestContext instance. Using a factory
     * pattern allows tests and other programs to change the
     * default behavior of RequestContext
     */
    public interface RequestContextFactory {
        RequestContext createRequestContext(FullHttpRequest request,
                                            ChannelHandlerContext ctx,
                                            LogContext lc,
                                            Object callerContext);
    }

    protected String getLogNamespace(AccessContext actx) {
        return (actx.getType() == Type.IAM ?
                actx.getTenantId(): actx.getNamespace());
    }

    protected void updateLogContext(LogContext lc,
                                    AccessContext actx,
                                    OpCode op) {
        updateLogContext(lc, getLogNamespace(actx),
                         actx.getOriginalTableName(null), op);
    }

    protected void updateLogContext(LogContext lc,
                                    String namespace,
                                    String tableName,
                                    OpCode op) {
        if (namespace != null) {
            logControl.updateLogContext(lc, namespace);
            if (logger.isLoggable(Level.FINE, lc)) {
                logger.fine("[updateLogContext] handleRequest, op " + op +
                            ", namespace " + namespace +
                            ", table " + tableName, lc);
            }
        }
    }

    protected static String opCodeToString(OpCode code) {
        switch (code) {
        case DELETE:
        case DELETE_IF_VERSION:
            return "DELETE";
        case GET:
            return "GET";
        case PUT:
        case PUT_IF_ABSENT:
        case PUT_IF_PRESENT:
        case PUT_IF_VERSION:
            return "PUT";
        case QUERY:
            return "QUERY";
        case PREPARE:
            return "PREPARE";
        case SCAN:
            return "SCAN";
        case INDEX_SCAN:
            return "INDEX_SCAN";
        case WRITE_MULTIPLE:
            return "WRITE_MULTIPLE";
        case MULTI_DELETE:
            return "MULTI_DELETE";
        case TABLE_REQUEST:
            return "TABLE_REQUEST";
        case CREATE_TABLE:
            return "CREATE_TABLE";
        case ALTER_TABLE:
            return "ALTER_TABLE";
        case DROP_TABLE:
            return "DROP_TABLE";
        case CREATE_INDEX:
            return "CREATE_INDEX";
        case DROP_INDEX:
            return "DROP_INDEX";
        case GET_TABLE:
            return "GET_TABLE";
        case GET_TABLE_USAGE:
            return "GET_TABLE_USAGE";
        case LIST_TABLES:
            return "LIST_TABLES";
        case GET_INDEXES:
            return "GET_INDEXES";
        case SYSTEM_REQUEST:
            return "SYSTEM_REQUEST";
        case SYSTEM_STATUS_REQUEST:
            return "SYSTEM_STATUS_REQUEST";
        case ADD_REPLICA:
            return "ADD_REPLICA";
        case DROP_REPLICA:
            return "DROP_REPLICA";
        case INTERNAL_DDL:
            return "INTERNAL_DDL";
        case INTERNAL_STATUS:
            return "INTERNAL_STATUS";
        default:
            return "UNKNOWN OPERATION: code=" + code.ordinal();
        }
    }

    /*
     * returns true if store is secure. In the cloud this will always
     * return true
     */
    protected boolean isSecureStore() {
        return tm.isSecureStore();
    }

    /*
     * Interface for updating log context with tenantId or namespace
     * information.
     */
    @FunctionalInterface
    public interface UpdateLogContext {
        void update(LogContext lc, AccessContext actx, OpCode op);
    }

    /*
     * Encapsulates the additional information for each sub operation
     */
    /* public for access from NsonProtocol */
    public static class TableOperationInfo {
        public String origTableName;
        public String mappedTableName;
        public OpCode mappedOp; /* currently either PUT or DELETE */
        public AccessContext actx;
        public TableEntry entry;
        public TableImpl table;
        public boolean returnInfo;
        public GeneratedValueInfo genInfo;

        @Override
        public String toString() {
            return "OperationInfo [" +
                   "returnInfo=" + returnInfo +
                   ", genInfo=" + genInfo +
                   ", mappedOp=" + opCodeToString(mappedOp) +
                   ", origTableName=" + origTableName +
                   ", mappedTableName=" + mappedTableName + "]";
        }

        public void copyFrom(TableOperationInfo other) {
            this.origTableName = other.origTableName;
            this.mappedTableName = other.mappedTableName;
            this.mappedOp = other.mappedOp;
            this.actx = other.actx;
            this.entry = other.entry;
            this.table = other.table;
            this.returnInfo = other.returnInfo;
            /* do not use same reference to genInfo: create a new one */
            if (other.genInfo == null) {
                this.genInfo = null;
            } else {
                this.genInfo = new GeneratedValueInfo();
            }
        }
    }

    /**
     * Context for a specific request.
     * This may be used once or multiple times, in request retries.
     * It contains the request, channel context, a few values that never
     * change in the request (regardless of retries), input/output
     * buffers, and a count of retries done on this request.
     *
     * This is created at the first call to handleRequest(). It lives until
     * the request is fully processed - this may include retries and/or
     * delays (DDoS, rate limiting) done via executor scheduling.
     *
     * On retries, the resetBuffers() method should be called to reset the
     * input/output buffer indexes.
     *
     * When the request is finally done, the releaseBuffers() method
     * must be called to free up any held resources.
     *
     * The callerContext Object has been added along with a factory
     * construction mechanism to allow entities other than Netty to control
     * the request/response handling by extending this class.
     * In particular this is helpful for testing that does not use Netty as
     * a server.
     *
     * Note: this is currently only used for binary requests. REST
     * requests do not use this. We should subclass this for REST vs.
     * binary (vs. V3) protocol requests in the near future. TODO
     */
    static public class RequestContext {
        /*
         * If a request doesn't specify a timeout, default to 5 seconds
         * (same as REST default). Note this shouldn't happen, since all
         * binary requests should specify timeout in payload, and
         * RequestContext is only used for binary protocol requests.
         */
        private static final int DEFAULT_REQUEST_TIMEOUT_MS = 5_000;

        /*
         * Response buffer size, minimum. Consider adjusting this per-request,
         * or other mechanism to reduce the frequency of resizing the buffer.
         */
        private static final int RESPONSE_BUFFER_SIZE = 1024;

        /* values that do not change while processing a request */
        public final ChannelHandlerContext ctx;
        public final FullHttpRequest request;
        public final LogContext lc;
        public final Object callerContext;
        public final String clientIP;
        public final HttpHeaders headers;
        public final boolean keepAlive;
        public DriverLang driverLang;
        public DriverProto driverProto;
        public String requestId;
        public long startTimeNs;
        public long endTimeNs; /* startTimeNs + request timeout */
        public int timeoutMs;

        /* If the driver is new enough, it will send to the proxy a topo seq num
         * that will be >= -1. So, by initializing the driverTopoSeqNum to -2,
         * we can detect old drivers that do not send topo seq num and do not
         * expect to receive topology info; the proxy should not send topology
         * info to such old drivers (see NsonProtocol.writeTopologyInfo()). */
        public int driverTopoSeqNum = -2;

        public OpCode originalOpCode; /* to allow resetting */
        public OperationType opType;
        public ProxyAuditContextBuilder auditBuilder;
        public boolean isMultiOp; /* request has sub-operations */

        /*
         * If this is set, and a request comes in for a table that is
         * currently over its rate limit, immediately return a
         * throttling exception. This overrides the default behavior, which
         * is to slow down the request by doing the operation and then delaying
         * its response back to the client.
         */
        public boolean preferThrottlingErrors;

        /*
         * If this is set, the request is explicitly opting in to using
         * Distributed Rate Limiting (DRL). Note that eventually this will
         * become obsolete when DRL is enabled globally.
         */
        public boolean drlOptedIn;

        /* values that may change during processing */

        /*
         * Access context for top-level operation. This may not have
         * table information, depending on op (see below). Or it may
         * be replaced with table-specific auth after "deferred table"
         * ops determine their table name.
         */
        public AccessContext actx;

        /*
         * These will be null for non-table ops (ex: LIST_TABLES)
         * AND for multi-operation requests (ex: WRITE_MULTIPLE) that
         * use different tables.
         * They will be set at the start of processing for "regular"
         * table-based ops, like PUT / GET.
         * They may be filled in later for "deferred table" ops, for
         * example QUERY, PREPARE.
         *
         * Specifically, if origTableName is set, this request always
         * operates on one and only one table.
         */
        /* the original (as sent by client) table name */
        public String origTableName;
        /* mapped table name - may be internal OCID(s). */
        public String mappedTableName;
        /* TableCache entry for table from mapped name */
        public TableEntry entry;

        /*
         * isMultiOp requests will have a list of opInfos, with a 1:1
         * mapping of opInfos to sub-operations.
         */
        public ArrayList<TableOperationInfo> tableOpInfos;

        public OpCode opCode; /* may become more specific */
        public boolean excessiveUse;
        public int numRetries;
        public int usedReadKB;
        public int usedWriteKB;
        public int errorCode;
        public volatile boolean statsMarked;

        /* binary-specific fields */
        public short serialVersion;
        public ByteInputStream bbis;
        protected int inputOffset; /* start of op-specific data */
        public ByteOutputStream bbos;
        /* V3 backward compatibility for WriteMultiple */
        public String V3TableNames;
        public short queryVersion;

        /* REST-specific fields */
        public RequestParams restParams;

        public RequestContext(FullHttpRequest request,
                              ChannelHandlerContext ctx,
                              LogContext lc,
                              Object callerContext) {
            this.request = request;
            this.ctx = ctx;
            this.lc = lc;
            this.callerContext = callerContext;
            clientIP = ErrorManager.getClientIP(request);
            headers = request.headers();
            keepAlive = HttpUtil.isKeepAlive(request);
            driverLang = DriverLang.parse(headers.get(USER_AGENT));

            numRetries = 0;
            startTimeNs = System.nanoTime();

            /* this will be correctly set when the payload is deserialized */
            setTimeoutMs(DEFAULT_REQUEST_TIMEOUT_MS);
        }

        /**
         * Read common fields from a REST request.
         */
        public void readREST() {
            /* Request id */
            requestId = headers.get(OPC_REQUEST_ID);
            if (requestId == null || requestId.length() == 0) {
                requestId =
                    UUID.randomUUID().toString().replace("-", "").toUpperCase();
            }
            lc.setId(requestId);
            driverProto = DriverProto.REST;

            /* TODO: timeout, buffers, preferThrottling */
        }

        /**
         * Read common fields from a binary payload.
         * This will throw an error if any of the fields are malformed.
         * On successful completion, the requestId, serialVersion,
         * offset will be marked so that subsequent resetBuffers() calls will
         * point the input stream at the start of op-specific data.
         *
         * TODO: move this into a subclass BinaryRequestContext
         */
        public void readBinaryHeader(boolean forceV3, boolean forceV4)
            throws IOException, RequestException {
            /* check requestId here, it should always exist for binary data */
            requestId = headers.get(REQUEST_ID_HEADER);
            if (requestId == null || requestId.isEmpty()) {
                throw new IOException("missing requestId in header");
            }
            /* all binary requests must have nonempty payload */
            if (request.content() == null) {
                throw new IOException("empty request content");
            }
            if (request.content().readableBytes() <= 0) {
                throw new IOException("empty request content");
            }
            lc.setId(requestId);
            bbis = new ByteInputStream(request.content());

            /*
             * Even with V4, the version must be the first two bytes of
             * the payload.
             */
            serialVersion = readDriverVersion(bbis);
            if (serialVersion >= V4) {
                if (forceV3) {
                    throw new RequestException(UNSUPPORTED_PROTOCOL,
                              "Requests must use V3 protocol");
                }
                /* there is not yet a driver proto > V4 */
                driverProto = DriverProto.V4;
// uncomment to print NSON request input
// TODO: FINEST
//System.out.println(printNson(bbis,true));
                /* mark bytebuf location where op-specific data starts */
                inputOffset = bbis.buffer().readerIndex();
                readV4Header();
            } else {
                if (forceV4) {
                    throw new RequestException(UNSUPPORTED_PROTOCOL,
                              "Requests must use V4 protocol");
                }
                if (serialVersion == V3) {
                    driverProto = DriverProto.V3;
                } else {
                    driverProto = DriverProto.V2;
                }
                opCode = OpCode.getOP(readOpCode(bbis));
                setTimeoutMs(readTimeout(bbis));
                /* mark bytebuf location where op-specific data starts */
                inputOffset = bbis.buffer().readerIndex();
            }
            originalOpCode = opCode; /* to allow resetting on retries */

            /*
             * Currently, only WriteMultiple allows multiple sub-operations.
             * This may change with new APIs in the future.
             */
            isMultiOp = (opCode == OpCode.WRITE_MULTIPLE);

            resetBuffers();
        }

        public boolean isNson() {
            return (serialVersion >= V4);
        }

        /**
         * Deserialize a V4 header, which is NSON
         */
        private void readV4Header() throws IOException {
            bbis.setOffset(inputOffset);
            boolean found = PathFinder.seek(HEADER, bbis);
            if (!found) {
                throw new RequestException(UNSUPPORTED_PROTOCOL, "Bad header");
            }
            /* read the header fields */
            walkHeader(bbis);
            /* a null table name is valid, but an blank name is not */
            if (origTableName != null && origTableName.isBlank()) {
                throw new RequestException(ILLEGAL_ARGUMENT,
                          "Table name must be non-empty");
            }
         }

        /**
         * Walk the V4 header, assigning RC state
         */
        private void walkHeader(ByteInputStream bbis) throws IOException {
            MapWalker walker = new MapWalker(bbis);

            while (walker.hasNext()) {
                walker.next();
                String name = walker.getCurrentName();
                if (name.equals(OP_CODE)) {
                    opCode = OpCode.getOP(Nson.readNsonInt(bbis));
                } else if (name.equals(TOPO_SEQ_NUM)) {
                    driverTopoSeqNum = Nson.readNsonInt(bbis);
                } else if (name.equals(TABLE_NAME)) {
                    origTableName = Nson.readNsonString(bbis);
                } else if (name.equals(TIMEOUT)) {
                    setTimeoutMs(Nson.readNsonInt(bbis));
                } else if (name.equals(PREFER_THROTTLING)) {
                    preferThrottlingErrors = Nson.readNsonBoolean(bbis);
                } else if (name.equals(DRL_OPTIN)) {
                    drlOptedIn = Nson.readNsonBoolean(bbis);
                } else if (name.equals(VERSION)) {
                    /* version came from header... ignore for now */
                    walker.skip();
                } else {
                    /* log unknown field? */
                    walker.skip();
                }
            }
        }

        /* V3 backward compatibility */
        public void handleV3TableNames() {
            if (origTableName == null) {
                return;
            }
            /* if table name has commas, it has multiple names */
            int idx = origTableName.indexOf(",");
            if (idx <= 0) {
                return;
            }
            /* only allow multiOp requests to have multiple table names */
            if (!isMultiOp) {
                throw new IllegalArgumentException("Commas are not allowed " +
                              "in table names: " + origTableName);
            }
            V3TableNames = origTableName;
            /* set table name to null: checks will be done on individual ops */
            origTableName = null;
        }

        /* return an array of unique table names in this request */
        /* Note the table names are as supplied by the client */
        public String[] getUniqueTableNames() {
            if (tableOpInfos == null) {
                return new String[]{origTableName};
            }
            /* unique the list into new array */
            HashSet<String> names = new HashSet<String>();
            for (TableOperationInfo opInfo : tableOpInfos) {
                names.add(opInfo.origTableName.toLowerCase());
            }
            String[] unames = new String[names.size()];
            int i=0;
            for (String name: names) {
                unames[i++] = name;
            }
            return unames;
        }

        /* return a list of unique operation infos, based on table names */
        public ArrayList<TableOperationInfo> uniqueOpInfos() {
            if (tableOpInfos == null) {
                return null;
            }
            String[] names = getUniqueTableNames();
            ArrayList<TableOperationInfo> uniqueOps =
                new ArrayList<TableOperationInfo>(names.length);
            for (int i=0; i<names.length; i++) {
                /* find the first opInfo that matches */
                for (TableOperationInfo opInfo : tableOpInfos) {
                    if (opInfo.origTableName.equalsIgnoreCase(names[i])) {
                        uniqueOps.add(opInfo);
                        break;
                    }
                }
            }
            return uniqueOps;
        }

        public void setTimeoutMs(int timeoutMs) {
            this.timeoutMs = timeoutMs;
            endTimeNs = startTimeNs + (timeoutMs * 1_000_000l);
        }

        /**
         * Reset input/output buffers to where they were at the
         * start of request processing.
         * Typically used during internal retries.
         */
        public void resetBuffers() {
            resetInputBuffer();
            if (bbos == null) {
                ByteBuf resp = ctx.alloc().directBuffer(RESPONSE_BUFFER_SIZE);
                bbos = new ByteOutputStream(resp);
            }
            bbos.buffer().writerIndex(0);
        }

        /**
         * Reset input buffer.
         */
        public void resetInputBuffer() {
            bbis.buffer().readerIndex(inputOffset);
        }

        /**
         * reset opCode to original.
         * This is because some operations may internally change the
         * opCode for access checks. Example: TABLE_OPERATION may be
         * changed to DROP_TABLE.
         */
        public void resetOpCode() {
            opCode = originalOpCode;
        }

        /**
         * This exists for implementing classes to know when an operation
         * is ending, with the response. It is used by test code.
         */
        public void finishOp(FullHttpResponse response) {
        }

        /**
         * Release input/output buffers. If an attempt to reference the
         * buffers is made after this call, it will result in a NPE.
         */
        public void releaseBuffers() {
            if (bbis != null) {
                bbis.buffer().release();
                bbis.close();
                bbis = null;
            }
            if (bbos != null) {
                /* do not release output buffer, see below */
                bbos.close();
                bbos = null;
            }
            /*
             * Note: output buffer will normally be released by netty when
             * the message is written to the wire. If we know for sure the
             * message will not be written, it should be explicitly
             * released at the point where this is determined. (example:
             * DDoS DO_NOT_RESPOND logic above)
             */
        }

        public void setThroughput(int readKB, int writeKB) {
            this.usedReadKB = readKB;
            this.usedWriteKB = writeKB;
        }

        public void setThroughput(Result res) {
            if (res != null) {
                setThroughput(res.getReadKB(), res.getWriteKB());
            }
        }

        public InternalOperation.OpCode getPutOpCode() {
            return mapPutOpCode(opCode);
        }
    }

    protected static InternalOperation.OpCode mapPutOpCode(OpCode op) {
        switch(op) {
        case PUT:
            return InternalOperation.OpCode.PUT;
        case PUT_IF_ABSENT:
            return InternalOperation.OpCode.PUT_IF_ABSENT;
        case PUT_IF_PRESENT:
            return InternalOperation.OpCode.PUT_IF_PRESENT;
        case PUT_IF_VERSION:
            return InternalOperation.OpCode.PUT_IF_VERSION;
        default:
            throw new IllegalStateException(
                "Invalid put op code: " + op);
        }
    }

}
