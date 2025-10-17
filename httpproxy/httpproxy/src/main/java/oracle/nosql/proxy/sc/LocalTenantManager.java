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

import static oracle.nosql.proxy.protocol.Protocol.INDEX_EXISTS;
import static oracle.nosql.proxy.protocol.Protocol.INDEX_NOT_FOUND;
import static oracle.nosql.proxy.protocol.Protocol.RESOURCE_NOT_FOUND;
import static oracle.nosql.proxy.protocol.Protocol.RESOURCE_EXISTS;
import static oracle.nosql.proxy.protocol.Protocol.TABLE_EXISTS;
import static oracle.nosql.proxy.protocol.Protocol.TABLE_NOT_FOUND;
import static oracle.nosql.proxy.protocol.BinaryProtocol.mapDDLError;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.core.JsonParser;

import oracle.kv.FaultException;
import oracle.kv.ExecutionFuture;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.KVSecurityException;
import oracle.kv.KVVersion;
import oracle.kv.StatementResult;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.DDLGenerator;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.JsonDefImpl;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadataHelper;
import oracle.kv.impl.client.admin.DdlFuture;
import oracle.kv.impl.client.admin.DdlStatementExecutor;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.table.FieldDef;
import oracle.kv.table.Index;
import oracle.kv.table.Table;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.proxy.Config;
import oracle.nosql.proxy.MonitorStats;
import oracle.nosql.proxy.RequestLimits;
import oracle.nosql.proxy.protocol.JsonProtocol;
import oracle.nosql.proxy.protocol.Protocol.OpCode;
import oracle.nosql.proxy.sc.TableUtils.PrepareCB;
import oracle.nosql.proxy.security.AccessContext;
import oracle.nosql.proxy.security.AccessContext.Type;
import oracle.nosql.proxy.util.PassThroughTableCache;
import oracle.nosql.proxy.util.TableCache;
import oracle.nosql.proxy.util.TableCache.TableEntry;
import oracle.nosql.util.fault.ErrorCode;
import oracle.nosql.util.fault.ErrorResponse;
import oracle.nosql.util.filter.Rule;
import oracle.nosql.util.tmi.DdlHistoryEntry;
import oracle.nosql.util.tmi.IndexInfo;
import oracle.nosql.util.tmi.IndexInfo.IndexField;
import oracle.nosql.util.tmi.IndexInfo.IndexState;
import oracle.nosql.util.tmi.ReplicaStats;
import oracle.nosql.util.tmi.TableInfo;
import oracle.nosql.util.tmi.TableInfo.ActivityPhase;
import oracle.nosql.util.tmi.TableInfo.TableState;
import oracle.nosql.util.tmi.WorkRequest.EntityType;
import oracle.nosql.util.tmi.WorkRequest.OperationType;
import oracle.nosql.util.tmi.TableLimits;
import oracle.nosql.util.tmi.TableUsage;
import oracle.nosql.util.tmi.TenantLimits;
import oracle.nosql.util.tmi.WorkRequest;

/*
 * A TenantManager instance that creates and uses a KVStore instance
 * directly vs using the cloud infrastructure. This is used for testing
 * as well as the Cloud Simulator.
 *
 * This instance *will* handle limits on:
 *  o row and key sizes
 *  o throughput (read/write units) as expressed in TableLimits
 *
 * This instance *will not* track tables or enforce limits on tenant and table
 * metadata such as those implemented in the TenantLimits object:
 *  o tables per tenant
 *  o indexes per table
 *  o columns per table
 *  o # schema evolutions
 *
 * Essentially this instance won't track anything that isn't readily available
 * in table metadata (TableImpl).
 *
 * The cloud life cycle states are emulated using plan state. See
 * checkTableOperation() for details.
 */
public class LocalTenantManager implements TenantManager {

    protected final KVStoreImpl store;
    private final String storeName;
    private final String[] helperHosts;
    private final boolean noLimits;
    protected final DdlStatementExecutor executor;
    protected final TableAPIImpl tableAPI;
    /*
     * table full namespace name -> limit mode
     * Transient save table limit mode which is cloud specific metadata.
     * This is useful for testing.
     */
    private Map<String, String> limitModes;
    protected TableCache tableCache; // not final, created post-construction

    /*
     * this needs to be set externally post-construction, because it's shared
     * with the entity that creates this instance.
     */
    protected SkLogger logger;

    protected LocalTenantManager(KVStore store,
                                 String storeName,
                                 boolean noLimits,
                                 String[] helperHosts) {
        this.store = (KVStoreImpl) store;
        this.storeName = storeName;
        this.noLimits = noLimits;
        this.helperHosts = helperHosts;
        executor = this.store.getDdlStatementExecutor();
        this.tableAPI = (TableAPIImpl) store.getTableAPI();
        this.limitModes = new ConcurrentHashMap<>();
    }

    public static LocalTenantManager createTenantManager(Config config) {

        final KVStoreConfig storeConfig = config.getTemplateKVStoreConfig();
        storeConfig.setStoreName(config.getStoreName());
        storeConfig.setHelperHosts(config.getHelperHosts());

        return new LocalTenantManager(connectKVStore(storeConfig),
                                      config.getStoreName(),
                                      config.getNoLimits(),
                                      config.getHelperHosts());
    }

    /* test only */
    public static LocalTenantManager createTenantManager(
        String storeName,
        String... helperHosts) {

        KVStoreConfig config = new KVStoreConfig(storeName, helperHosts);
        return new LocalTenantManager(connectKVStore(config),
                                      storeName,
                                      false,
                                      helperHosts);
    }

    protected static KVStore connectKVStore(KVStoreConfig config) {

        KVStore store = null;

        /*
         * socketReadTimeout is used to detect network issues, even if kv
         * is using the async protocol. Increase it from the default 30s to
         * 60s to give users more room if the KV version is < 21, which
         * allows the request timeout to exceed the socket timeout.
         *
         * When it's unlikely to be using a kv client < 21, remove this
         * code altogether.
         */
        if (KVVersion.CURRENT_VERSION.getMajor() < 21) {
            config.setSocketReadTimeout(60, TimeUnit.SECONDS);
        }

        /*
         * Use a retry loop to give the store a chance to start
         */
        int retryCount = 0;
        Throwable cachedError = null;
        do {
            try {
                store = KVStoreFactory.getStore(config);
            } catch (Throwable e) {
                if (e instanceof KVSecurityException) {
                    throw (KVSecurityException) e;
                }
                cachedError = e;
                try {
                    Thread.sleep(1000);
                } catch (Exception f){
                    /* Just go around the loop again. */
                }
            }
            if (++retryCount > 10) {
                if (cachedError != null &&
                    cachedError instanceof NoClassDefFoundError) {
                    throw new IllegalStateException(
                        "Cannot find required class, please make sure you " +
                        "are using EE package",
                        cachedError);
                }
                throw new IllegalArgumentException(
                    "Can't connect to store " + config.getStoreName() +
                    " at " + Arrays.toString(config.getHelperHosts()),
                    cachedError);
            }
        } while (store == null);

        /*
         * set the table cache to 0, which is unlimited.
         * Idle tables are removed after 30s by default
         */
        ((TableAPIImpl) store.getTableAPI()).setCacheCapacity(0);
        return store;
    }

    @Override
    public void setLogger(final SkLogger logger) {
        this.logger = logger;
    }

    protected GetTableResponse handleGetTableException(String operationId,
                                                       Exception e,
                                                       LogContext lc) {
        logger.warning("Cannot reach store to check operation, id: " +
                operationId + ", error: " + e.getMessage(), lc);
        return new GetTableResponse(ErrorResponse.build(
            ErrorCode.ILLEGAL_ARGUMENT,
            "Unable to get status of operation id " +
            operationId + ": " + e.getMessage()));
    }

    @Override
    public GetTableResponse getTable(AccessContext actx,
                                     String tableName,
                                     String operationId,
                                     boolean internal,
                                     LogContext lc) {

        TableState state = TableState.ACTIVE; // default state
        if (operationId != null) {
            OperationInfo opInfo = null;
            try {
                opInfo = checkTableOperation(operationId,
                                             actx.getAuthString(),
                                             lc);
            } catch (Exception e) {
                return handleGetTableException(operationId, e, lc);
            }
            /*
             * We have a non-null opInfo
             * Check this operation is on the specified table.
             */
            if (!opInfo.isOnTable(getNamespace(actx), tableName)) {
                return new GetTableResponse(
                    ErrorResponse.build(ErrorCode.ILLEGAL_ARGUMENT,
                        "operation id: " + operationId + " is not for table " +
                        tableName + " (namespace=" + getNamespace(actx) + ")"));
            }
            if (opInfo.isDone() && !opInfo.isSuccessful()) {
                /*
                 * Should this get its own error? Plan failures are rare...
                 */
                return new GetTableResponse(
                    ErrorResponse.build(ErrorCode.ILLEGAL_ARGUMENT,
                                        opInfo.getErrorString()));
            }
            state = mapTableState(opInfo.isDone(), opInfo.getOpType());
        }
        GetTableResponse gtr =
            makeGetTableResponse(getNamespace(actx), actx, tableName,
                                 state, operationId, null, "GetTable",
                                 lc);
        if (state == TableState.ACTIVE ||
            state == TableState.UPDATING) {
            gtr = addTableMetadata(gtr, lc);
        }
        return gtr;
    }

    /**
     * Add metadata -- limits and schema.
     * This has a side effect (desired) of returning table not found.
     */
    private GetTableResponse addTableMetadata(GetTableResponse gtr,
                                              LogContext lc) {
        /* don't do this if there's an error */
        if (gtr.getHttpResponse() != 200) {
            return gtr;
        }
        TableInfo info = gtr.getTableInfo();
        Table table = getTable(info.getTenantId(),
                               info.getTableName());
        if (table == null) {
            return tableNotFound(info.getTenantId(),
                                 info.getTableName(),
                                 logger,
                                 lc);
        }

        if (table.getParent() == null) {
            info.setTableLimits(getTableLimits((TableImpl) table));
        }

        info.setSchema(((TableImpl) table).toJsonString(true));

        try {
            info.setDdl(getDDLGenerator(table, true).getDDL());
        } catch (RuntimeException ex) {
            /*
             * TODO: fix 2 bugs in DDLGenerator found in unit tests, for now
             * ignore errors if generate ddls failed.
             *
             * 1. KVProxyTest.testCRDT():
             *    - error "Only multi-region tables support MR_counters"
             *    - table ddl
             *      create table mrtable(id integer,
             *                           count integer as mr_counter,
             *                           primary key(id))
             *      in regions localRegion
             *
             * 2. QueryTest.testQueryCompat():
             *    - error "Invalid type for JSON index field: POINT"
             *    - table ddls
             *      create table points(id integer, info json, primary key(id));
             *      create index idx_ptn on points(info.point as point);
             */
        }
        return gtr;
    }

    /**
     * There is only one store.
     */
    @Override
    public GetStoreResponse getStoreInfo(String namespace,
                                         String tableName,
                                         LogContext lc) {

        return new GetStoreResponse(200,
                                    storeName,
                                    helperHosts);
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
        if (ifNotExists) {
            String namespace = getNamespace(actx);
            Table table = getTable(namespace, tableName);
            if (table != null) {
                GetTableResponse gtr =
                    makeGetTableResponse(namespace, actx, tableName,
                                         TableState.ACTIVE, null, null,
                                         "TableOperation", lc);
                if (table.getParent() == null) {
                    gtr.getTableInfo().
                        setTableLimits(getTableLimits((TableImpl) table));
                }
                gtr.getTableInfo().
                    setSchema(((TableImpl) table).toJsonString(true));
                return gtr;
            }
        }
        try {
            return executeTableDDL(statement,
                                   actx,
                                   tableName,
                                   TableState.ACTIVE,
                                   TableState.CREATING,
                                   limits,
                                   lc);
        } catch (IllegalArgumentException iae) {
            logger.fine("Create table exception: " + iae);
            /*
             * IAE is unfortunately used for "table exists". Try to
             * figure that out and rethrow a better exception.
             */
            String msg = iae.getMessage().toLowerCase();
            if (msg.contains("already exists")) {
                return new GetTableResponse(
                    ErrorResponse.build(ErrorCode.TABLE_ALREADY_EXISTS,
                                        msg));
            }
            throw iae;
        }

    }

    @Override
    public GetTableResponse dropTable(AccessContext actx,
                                      String tableName,
                                      boolean ifExists,
                                      byte[] matchETag,
                                      LogContext lc) {

        /*
         * Construct the drop table statement
         */
        StringBuilder sb = new StringBuilder();
        sb.append("drop table ");
        if (ifExists) {
            sb.append("if exists ");
        }
        sb.append(tableName);

        try {
            return executeTableDDL(sb.toString(),
                                   actx,
                                   tableName,
                                   TableState.DROPPED,
                                   TableState.DROPPING,
                                   null,
                                   lc);
        } catch (IllegalArgumentException iae) {
            logger.fine("Drop table exception: " + iae);
            /*
             * IAE is unfortunately used for "table not found". Try to
             * figure that out and rethrow a better exception.
             */
            String msg = iae.getMessage().toLowerCase();
            if (msg.contains("does not exist")) {
                return new GetTableResponse(
                    ErrorResponse.build(ErrorCode.TABLE_NOT_FOUND,
                                        msg));
            }
            throw iae;
        }
    }

    @Override
    public GetTableResponse dropIndex(AccessContext actx,
                                      String tableName,
                                      String indexName,
                                      boolean ifExists,
                                      byte[] matchETag,
                                      LogContext lc) {
        /*
         * Construct the drop index statement
         */
        StringBuilder sb = new StringBuilder();
        sb.append("drop index ");
        if (ifExists) {
            sb.append("if exists ");
        }
        sb.append(indexName).append(" on ").append(tableName);

        GetTableResponse gtr = executeTableDDL(sb.toString(),
                                               actx,
                                               tableName,
                                               TableState.ACTIVE,
                                               TableState.UPDATING,
                                               null,
                                               lc);
        return addTableMetadata(gtr, lc);
    }

    @Override
    public GetTableResponse alterTable(AccessContext actx,
                                       String tableName,
                                       String statement,
                                       TableLimits limits,
                                       byte[] matchETag,
                                       LogContext lc) {
        if (limits == null && statement == null) {
            if (actx.getType() == Type.IAM) {
                /* tagging unit test, do nothing just return the table */
                return getTable(actx, tableName, null, false, lc);
            }
            return new GetTableResponse(
                ErrorResponse.build(
                    ErrorCode.ILLEGAL_ARGUMENT,
                    "alter table must include a statement or limits"));
        }
        if (limits != null && statement != null) {
            return new GetTableResponse(
                ErrorResponse.build(
                    ErrorCode.ILLEGAL_ARGUMENT,
                    "alter table cannot include both a statement and limits"));
        }

        if (isCreateTableDdl(statement)) {
            statement = getAlterDdl(actx, tableName, statement, lc);
            if (statement == null) {
                /*
                 * The target table is semantically equivalent to original
                 * table, no change to the table.
                 */
                return getTable(actx, tableName, null, true /*internal*/, lc);
            }
            logger.info("Generate ALTER table ddl: " + statement);
        }

        GetTableResponse gtr = executeTableDDL(statement,
                                               actx,
                                               tableName,
                                               TableState.ACTIVE,
                                               TableState.UPDATING,
                                               limits,
                                               lc);
        return addTableMetadata(gtr, lc);
    }

    private boolean isCreateTableDdl(String statement) {
        return statement != null &&
               statement.toLowerCase().startsWith("create table");
    }

    private String getAlterDdl(AccessContext actx,
                               String tableName,
                               String newTableDdl,
                               LogContext lc) {

        TableEntry te = tableCache.getTableEntry(getNamespace(actx),
                                                 tableName, lc);
        PrepareCB cb = TableUtils.getCallbackInfo(actx, newTableDdl, this);
        String[] ddls = getDDLGenerator(te.getTable(), false)
                            .genAlterDdl(cb.getNewTable());

        if (ddls.length > 1) {
            throw new IllegalArgumentException("Multiple alter table " +
                "operations are needed to evolve to target table, but only " +
                "single alter table operation per request is supported " +
                "at this time: " +
                NameUtils.makeQualifiedName(getNamespace(actx), tableName));
        }

        return (ddls.length == 0) ? null : ddls[0];
    }

    @Override
    public GetTableResponse createIndex(AccessContext actx,
                                        String tableName,
                                        String indexName,
                                        String statement,
                                        boolean ifNotExists,
                                        String retryToken,
                                        LogContext lc) {

        GetTableResponse gtr = executeTableDDL(statement,
                                               actx,
                                               tableName,
                                               TableState.ACTIVE,
                                               TableState.UPDATING,
                                               null,
                                               lc);
        return addTableMetadata(gtr, lc);
    }

    /*
     * Note: Parameters "namePattern", "state", "sortBy" and "isAscSort" are
     * not used or acknowledged in this method.
     */
    @Override
    public IndexResponse getIndexInfo(AccessContext actx,
                                      String tableName,
                                      String indexName,
                                      String namePattern,
                                      String state,
                                      String sortBy,
                                      boolean isAscSort,
                                      int startIndex,
                                      int limit,
                                      LogContext lc) {

        String namespace = getNamespace(actx);
        Table table = getTable(namespace, tableName);
        if (table == null) {
            return new IndexResponse(
                ErrorResponse.build(ErrorCode.TABLE_NOT_FOUND,
                                    "Table not found: " +
                                    makeNamespaceName(namespace, tableName)));
        }

        final Map<String, Index> allIndexes = table.getIndexes();
        /* Get specified index */
        if (indexName != null) {
            Index index = allIndexes.get(indexName);
            if (index == null) {
                return new IndexResponse(
                    ErrorResponse.build(
                        ErrorCode.INDEX_NOT_FOUND,
                        "Index, " + indexName +
                        ", not found in table " +
                        table.getFullNamespaceName()));
            }
            IndexInfo indexInfo = makeIndexInfo(index);
            return new IndexResponse(200, new IndexInfo[]{indexInfo}, 1);
        }

        /* Get indexes */
        List<IndexInfo> indexes = new ArrayList<IndexInfo>();
        int current = 0;
        for (Index index : allIndexes.values()) {
            if (current++ < startIndex) {
                continue;
            }
            indexes.add(makeIndexInfo(index));
            /* if limit is 0, this will never be true, that's OK */
            if (indexes.size() == limit) {
                break;
            }
        }
        IndexInfo[] indexInfos = indexes.toArray(new IndexInfo[indexes.size()]);
        return new IndexResponse(200, indexInfos, current);
    }

    @Override
    public ListTableResponse listTables(AccessContext actx,
                                        int startIndex,
                                        int numTables,
                                        LogContext lc) {

        Set<String> tables = getTableNames(getNamespace(actx));

        ArrayList<String> tableNames = new ArrayList<String>(tables.size());
        int current = 0;
        int num = 0;
        for (String s : tables) {
            if (current++ < startIndex) {
                continue;
            }
            tableNames.add(s);
            /* if numTables is 0, this will never be true, that's OK */
            if (++num == numTables) {
                break;
            }
        }
        return new ListTableResponse(200,
                                     tableNames.toArray(new String[num]),
                                     current);
    }

    protected void addTablesToSet(Set<String> results, String namespace) {
        Map<String, Table> nsTables = getTables(namespace);
        for (Map.Entry<String, Table> entry : nsTables.entrySet()) {
            addTableToSet(results, entry.getValue());
        }
    }

    /**
     * Recursively add table and any child tables. Add child tables in reverse
     * order so that if this list is being used for dropping tables they will
     * be done in order. E.g. if the child table tree is this:
     *  parent
     *    child1
     *      child1a
     *    child2
     *      child2a
     * the order will be child1a, child1, child2a, child2 parent
     */
    private void addTableToSet(Set<String> results, Table table) {
        Map<String, Table> childTables = table.getChildTables();
        for (Map.Entry<String, Table> entry : childTables.entrySet()) {
            addTableToSet(results, entry.getValue());
        }
        addTableNameToSet(results, table);
    }

    protected void addTableNameToSet(Set<String> results, Table table) {
        results.add(table.getFullName());
    }

    @Override
    public ListTableInfoResponse listTableInfo(AccessContext actx,
                                               String namePattern,
                                               String state,
                                               String sortBy,
                                               boolean isAscSort,
                                               int startIndex,
                                               int numTables,
                                               LogContext lc) {

        String namespace = actx.getNamespace();

        /* repeatable order */
        Map<String, Table> tables = new LinkedHashMap<>();
        for (Entry<String, Table> e : tableAPI.getTables(namespace).entrySet()) {
            TableImpl table = (TableImpl)e.getValue();
            if (table.hasChildren()) {
                addChildTables(tables, table);
            }
            tables.put(table.getFullName(), table);
        }

        List<TableInfo> tableInfos = new ArrayList<TableInfo>();
        int current = 0;
        int num = 0;
        for (Map.Entry<String, Table> e : tables.entrySet()) {
            TableImpl table = (TableImpl)e.getValue();
            if (current++ < startIndex) {
                continue;
            }
            TableInfo ti = makeTableInfo(table.getFullName(),
                                         namespace,
                                         actx,
                                         TableInfo.TableState.ACTIVE,
                                         getTableLimits(table));
            ti.setSchema(table.toJsonString(true));

            tableInfos.add(ti);
            /* if numTables is 0, this will never be true, that's OK */
            if (++num == numTables) {
                break;
            }
        }

        /**
         * TODO: Support filtering by namePattern/state, and sorting by
         * timedCreated and name
         */

        return new ListTableInfoResponse(200,
                    tableInfos.toArray(new TableInfo[tableInfos.size()]),
                    0 /* maxAutoReclaimableTables */,
                    0 /* autoReclaimableTables */,
                    0 /* maxAutoScalingTables */,
                    0 /* autoScalingTables */,
                    null /* avaiableReplicas */,
                    current);
    }

    private void addChildTables(Map<String, Table> tables, Table parent) {
        Map<String, Table> childTables = parent.getChildTables();
        for (Entry<String, Table> e : childTables.entrySet()) {
            TableImpl table = (TableImpl) e.getValue();
            if (table.hasChildren()) {
                addChildTables(tables, table);
            }
            tables.put(table.getFullName(), table);
        }
    }

    protected Set<String> getTableNames(String namespace) {
        /* repeatable order */
        Set<String> tables = new LinkedHashSet<String>();
        addTablesToSet(tables, namespace);
        return tables;
    }

    @Override
    public TableUsageResponse getTableUsage(AccessContext actx,
                                            String tableName,
                                            long startTimestamp,
                                            long endTimestamp,
                                            int startIndex,
                                            int limit,
                                            LogContext lc) {

        final TableUsage[] fakeUsage = new TableUsage[] {
            new TableUsage(System.currentTimeMillis() - 3000L,
                           2,    // interval in secs
                           45,   // read KB
                           20,   // write KB
                           6,    // storage GB
                           0,    // maxPartitionUsage
                           0, 0, 0) // read, write, storage throttle counts
        };

        /* will throw if table doesn't exist */
        getTable(actx, tableName, null, false, lc);

        /*
         * This TenantManager doesn't have access to usage information at
         * this time, if ever. Use a fake
         */
        return new TableUsageResponse(200, fakeUsage, 1);
    }

    @Override
    public TenantLimits getTenantLimits(String namespace, LogContext lc) {
        return null;
    }

    @Override
    public ListRuleResponse listRules(LogContext lc) {
        /*
         * Persistent rule is not supported in local mode, return a fake result
         */
        return new ListRuleResponse(200, new Rule[0]);
    }

    @Override
    public void close() {
        if (store != null) {
            store.close();
        }
    }

    public KVStoreImpl getStore() {
        return store;
    }

    public DdlStatementExecutor getExecutor() {
        return executor;
    }

    private String getNamespace(AccessContext actx) {
        if (actx.getType() == Type.IAM) {
            return actx.getCompartmentId();
        }
        return actx.getNamespace();
    }

    private static IndexInfo makeIndexInfo(Index index) {
        IndexImpl indexImpl = ((IndexImpl)index);
        IndexField[] fields = new IndexField[indexImpl.getFields().size()];

        int i = 0;
        IndexField field;
        String path;
        FieldDef tdef;
        for (oracle.kv.impl.api.table.IndexImpl.IndexField idxFld :
             indexImpl.getIndexFields()) {
            path = idxFld.getPathName();
            tdef = idxFld.getDeclaredType();
            if (tdef == null) {
                field = new IndexField(path);
            } else {
                field = new IndexField(path, tdef.getType().name());
            }
            fields[i++] = field;
        }

        /*
         * Because there is no createdTime for Index, just use current system
         * time as createdTime of the index.
         */
        return new IndexInfo(index.getName(), fields, IndexState.ACTIVE,
                             System.currentTimeMillis());
    }

    private OperationInfo checkTableOperation(String operationId,
                                              String authString,
                                              LogContext lc) {
        int planId = Integer.parseInt(operationId);
        logger.fine("Checking plan id: " + planId);
        DdlFuture future = createDdlFuture(planId, authString);
        StatementResult res = future.updateStatus();
        return new OperationInfo(res);
    }

    protected DdlFuture createDdlFuture(int planId, String authString) {
        return new DdlFuture(planId, executor);
    }

    private static GetTableResponse tableNotFound(String namespace,
                                                  String tableName,
                                                  SkLogger logger,
                                                  LogContext lc) {
        logger.fine("Table not found: " +
                    makeNamespaceName(namespace,tableName), lc);
        return new GetTableResponse(
            ErrorResponse.build(ErrorCode.TABLE_NOT_FOUND,
                                "Table not found: " +
                                makeNamespaceName(namespace, tableName)));
    }

    /**
     * Allow this to be overridden so that the namespace and tableName
     * can be modified if needed.
     */
    protected TableInfo makeTableInfo(String tableName,
                                      String namespace,
                                      AccessContext actx,
                                      TableState state,
                                      TableLimits limits) {
        TableInfo info = new TableInfo(tableName,
                                       namespace,
                                       actx.getCompartmentId(),
                                       state,
                                       limits,
                                       0 /* createTime */,
                                       0 /* updateTime */,
                                       null /* work request id */,
                                       false /* isAutoReclaimable */,
                                       ActivityPhase.ACTIVE.name(),
                                       0 /* timeOfExpiration */);

        if (actx.getCompartmentId() != null) {
            info.setCompartmentId(actx.getCompartmentId());
        }

        /* Set table ocid to compartmentId:tableName */
        info.setTableOcid(NameUtils.makeQualifiedName(
                              ((actx.getNamespace() != null) ?
                               actx.getNamespace() : actx.getCompartmentId()),
                              info.getTableName()));

        return info;
    }

    private GetTableResponse makeGetTableResponse(String namespace,
                                                  AccessContext actx,
                                                  String tableName,
                                                  TableState state,
                                                  String opId,
                                                  TableLimits limits,
                                                  String caller,
                                                  LogContext lc) {
        TableInfo info = makeTableInfo(tableName,
                                       namespace,
                                       actx,
                                       state,
                                       limits);

        info.setOperationId(opId);
        logger.fine("Get table response from " + caller + ": " + info, lc);
        return new GetTableResponse(200, info);
    }

    /**
     * Shared code to execute DDL requests that return GetTableResponse:
     *   o create/drop/alter table
     */
    private GetTableResponse executeTableDDL(String statement,
                                             AccessContext actx,
                                             String tableName,
                                             TableState successState,
                                             TableState workingState,
                                             TableLimits limits,
                                             LogContext lc) {

        try {
            logger.fine("Executing table DDL, statement, limits: " +
                        statement + ", " + limits, lc);
            String namespace = getNamespace(actx);
            ExecuteOptions options = createExecuteOptions(namespace,
                                                          actx.getAuthString(),
                                                          lc);

            ExecutionFuture future = null;
            if (limits != null && statement == null) {
                future = setTableLimits(namespace, tableName,
                                        makeKVTableLimits(limits));
            } else {
                future = store.execute(statement.toCharArray(),
                                       options,
                                       makeKVTableLimits(limits));
            }
            StatementResult res = future.updateStatus();
            String operationId = null;
            int planId = ((DdlFuture) future).getPlanId();
            if (planId > 0) {
                operationId =
                    Integer.toString(((DdlFuture) future).getPlanId());
            }

            /*
             * Transient save table limit mode after we can submit plan
             * successfully
             */
            String fullNamespaceName = makeNamespaceName(namespace,
                                                         tableName);
            fullNamespaceName = fullNamespaceName.toLowerCase();
            if (limits != null) {
                /* It is create table or update table limits DDL */
                limitModes.put(fullNamespaceName, limits.getMode());
            } else if (successState == TableState.DROPPED) {
                /* It is drop table DDL */
                limitModes.remove(fullNamespaceName);
            }

            return makeGetTableResponse(
                namespace,
                actx,
                tableName,
                (res.isSuccessful() ? successState : workingState),
                operationId,
                limits,
                "TableOperation",
                lc);
        } catch (IllegalArgumentException iae) {
            return mapDDLException(iae);
        }
    }

    /*
     * If we could get better discrimination of exceptions from DDL
     * queries this wouldn't be necessary. TBD -- do that.
     * Infer the appropriate exception based on the error message.
     *  - exists (index or table)
     *  - not found (index or table)
     */
    private GetTableResponse mapDDLException(IllegalArgumentException iae) {
        int code = mapDDLError(iae.getMessage());
        if (code == 0) {
            throw iae;
        }
        ErrorCode ecode = null;
        switch (code) {
        case INDEX_EXISTS:
            ecode = ErrorCode.INDEX_ALREADY_EXISTS;
            break;
        case RESOURCE_EXISTS:
            ecode = ErrorCode.RESOURCE_ALREADY_EXISTS;
            break;
        case TABLE_EXISTS:
            ecode = ErrorCode.TABLE_ALREADY_EXISTS;
            break;
        case INDEX_NOT_FOUND:
            ecode = ErrorCode.INDEX_NOT_FOUND;
            break;
        case RESOURCE_NOT_FOUND:
            ecode = ErrorCode.RESOURCE_NOT_FOUND;
            break;
        case TABLE_NOT_FOUND:
            ecode = ErrorCode.TABLE_NOT_FOUND;
            break;
        default:
            throw iae;
        }
        return buildErrorResponse(ecode, iae.getMessage());
    }

    private GetTableResponse buildErrorResponse(ErrorCode code, String msg) {
        return new GetTableResponse(ErrorResponse.build(code, msg));
    }

    protected ExecuteOptions createExecuteOptions(String namespace,
                                                  String authString,
                                                  LogContext lc) {
        return new ExecuteOptions().
            setNamespace(namespace, false).
            setLogContext(new oracle.kv.impl.util.contextlogger.LogContext(lc)).
            setAllowCRDT(true);
    }

    protected static String makeNamespaceName(String namespace,
                                            String tableName) {
        if (namespace == null) {
            return tableName;
        }
        final StringBuilder sb = new StringBuilder();
        sb.append(namespace).append(":").append(tableName);
        return sb.toString();
    }

    /*
     * Figure out correct TableState from OperationInfo.OpState
     * Operation not done:
     *  CREATE => CREATING
     *  ALTER => UPDATING
     *  DROP => DROPPING
     * Operation done:
     *  DROP => DROPPED
     *  anything else => ACTIVE
     */
    private TableState mapTableState(boolean isDone,
                                     OperationInfo.OpType type) {
        if (isDone) {
            if (type == OperationInfo.OpType.DROP) {
                return TableState.DROPPED;
            }
            return TableState.ACTIVE;
        }
        switch (type) {
        case CREATE:
            return TableState.CREATING;
        case ALTER:
            return TableState.UPDATING;
        case DROP:
            return TableState.DROPPING;
        default:
            throw new IllegalArgumentException("Unknown op type " + type);
        }
    }

    static class OperationInfo {

        final boolean isDone;
        final boolean successful;
        OpType opType;
        OpState opState;
        String errorString;
        String name; /* internal plan name, debugging */

        /**
         * Break types into 3 classes:
         *  CREATE -- create a table, index, user, etc
         *  DROP -- drop  a table, index, user, etc
         *  ALTER -- change an existing resource
         * This is sufficient to inform the caller as to the expected resource
         * state.
         */
        enum OpType {
            CREATE, DROP, ALTER, UNKNOWN
        }

        /**
         * Operation state. This is a subset of KV plan states based on
         * what is possible in this path:
         *  RUNNING -- plan is still running
         *  SUCCEEDED -- plan is complete and succeeded
         *  ERROR -- plan is complete and in error state
         */
        enum OpState {
            RUNNING, SUCCEEDED, ERROR
        }

        OperationInfo(StatementResult result) {
            this.successful = result.isSuccessful();
            this.isDone = result.isDone();
            if (isDone) {
                if (!successful) {
                    errorString = result.getErrorMessage();
                    opState = OpState.ERROR;
                } else {
                    opState = OpState.SUCCEEDED;
                }
            } else {
                opState = OpState.RUNNING;
            }
            /*
             * Need to parse JSON for the operation type and to validate
             * the existing state.
             */
            String jsonInfo = result.getInfoAsJson();
            if (jsonInfo == null) {
                /*
                 * shouldn't happen, but just in case. UNKNOWN exists for this
                 */
                opType = OpType.UNKNOWN;
            } else {
                FieldValueImpl fv = null;
                try {
                    StringReader reader = new StringReader(jsonInfo);
                    JsonParser jp = JsonProtocol.createJsonParser(reader);
                    fv = (FieldValueImpl) JsonDefImpl.createFromJson(jp, true);
                    name = fv.asMap().get("planInfo").asMap()
                        .get("name").asString().get();
                    setOpType(name.toLowerCase());
                } catch (IOException ioe) {
                    opType = OpType.UNKNOWN;
                    name = null;
                }
            }
        }

        /*
         * Figure out correct TableState based on the name of the plan. This is
         * a bit clunky but it's all we have. The strings below are sensitive
         * to the plan names generated by kv.
         */
        private void setOpType(String type) {
            final String CREATE_TABLE = "createtable";
            final String DROP_TABLE = "droptable";
            final String INDEX = "index";
            final String ALTER_TABLE = "alter";
            final String LIMITS = "tablelimits";

            if (type.contains(CREATE_TABLE)) {
                opType = OpType.CREATE;
            } else if (type.contains(DROP_TABLE)) {
                opType = OpType.DROP;
            } else if (type.contains(INDEX) ||
                       type.contains(ALTER_TABLE) ||
                       type.contains(LIMITS)) {
                opType = OpType.ALTER;
            } else {
                throw new IllegalArgumentException(
                    "Unknown table operation: " + type);
            }
        }

        boolean isSuccessful() {
            return successful;
        }

        boolean isDone() {
            return isDone;
        }

        String getName() {
            return name;
        }

        String getErrorString() {
            return errorString;
        }

        OpState getState() {
            return opState;
        }

        OpType getOpType() {
            return opType;
        }

        /*
         * Simple verify if this operation is on the specified table.
         */
        boolean isOnTable(String namespace, String tableName) {
            if (name == null) {
                return false;
            }
            String fullNamespaceName = makeNamespaceName(namespace,
                                                         tableName);
            fullNamespaceName = fullNamespaceName.toLowerCase();
            return name.toLowerCase().contains(fullNamespaceName);
        }
    }


    /*
     * Utilities
     */
    protected ExecutionFuture setTableLimits(
        String namespace,
        String tableName,
        oracle.kv.impl.api.table.TableLimits limits) {

        if (noLimits) {
            throw new IllegalArgumentException(
                "Table limit operation is not supported in no limits mode");
        }

        try {
            return store.setTableLimits(namespace, tableName, limits);
        } catch (FaultException fe) {
            /*
             * this is almost certainly table not found. Caller will map.
             */
            throw new IllegalArgumentException(fe.getMessage());
        }
    }

    protected DDLGenerator getDDLGenerator(Table table,
                                           boolean withIfNotExists) {
        return new DDLGenerator(table, withIfNotExists,
                                tableAPI.getRegionMapper());
    }

    protected TableLimits getTableLimits(TableImpl table) {
        oracle.kv.impl.api.table.TableLimits limits =
            table.getTableLimits();
        if (limits != null) {
            String fullNamespaceName = table.getFullNamespaceName();
            String mode = limitModes.get(fullNamespaceName.toLowerCase());
            return new TableLimits(limits.getReadLimit(),
                                   limits.getWriteLimit(),
                                   limits.getSizeLimit(),
                                   mode);
        }
        return TableLimits.getNoLimits();
    }

    protected oracle.kv.impl.api.table.TableLimits makeKVTableLimits(
        TableLimits limits) {
        if (limits == null) {
            return null;
        }
        if (noLimits) {
            /* ignore limits set by the user */
            return new
                oracle.kv.impl.api.table.TableLimits(
                    Integer.MAX_VALUE - 1,
                    Integer.MAX_VALUE - 1,
                    limits.getTableSize(),
                    -1, -1,
                    Integer.MAX_VALUE - 1); // Index key size
        }
        if (limits.modeIsAutoScaling()) {
            return new
                oracle.kv.impl.api.table.TableLimits(
                    Integer.MAX_VALUE - 1,
                    Integer.MAX_VALUE - 1,
                    limits.getTableSize(),
                    /* hard-code index key size limit (64) for cloudsim */
                    -1, -1, 64);
        } else if (limits.modeIsProvisioned()) {
            return new
                oracle.kv.impl.api.table.TableLimits(
                    limits.getReadUnits(),
                    limits.getWriteUnits(),
                    limits.getTableSize(),
                    /* hard-code index key size limit (64) for cloudsim */
                    -1, -1, 64);
        } else {
            throw new IllegalArgumentException("Invalid TableLimits, " +
                "unknown mode");
        }
    }

    /*
     * Metadata methods are encapsulated here for modularity.
     */

    protected Table getTable(String namespace,
                             String tableName) {
        return tableAPI.getTable(namespace, tableName, true);
    }

    protected Map<String, Table> getTables(String namespace) {
        return tableAPI.getTables(namespace);
    }

    protected Set<String> listNamespaces() {
        return tableAPI.listNamespaces();
    }

    /**
     * create a simple table cache
     */
    @Override
    public void createTableCache(Config config,
                                 MonitorStats stats,
                                 SkLogger logger) {
        this.tableCache = new PassThroughTableCache(this, logger);
    }

    @Override
    public TableCache getTableCache() {
        return tableCache;
    }

    /*
     * For tests and cloudsim. Wait for the KV system metadata table to
     * be initialized.

     * NOTE, TODO: this will fail/timeout if a table has not yet been
     * created. In the near future there should be a mechanism to trigger
     * initialization without creating a table.
     */
    @Override
    public void waitForStoreInit(int timeoutSecs) throws TimeoutException {
        long timeoutMs = timeoutSecs * 1000;
        long startMs = System.currentTimeMillis();

        while ((System.currentTimeMillis() - startMs) < timeoutMs) {
            if (tableAPI.getTableMDSysTable() != null) {
                return;
            }
            try {
                Thread.sleep(500);
            } catch (Exception e) {
            }
        }
        throw new TimeoutException(
            "System metadata table is not initialized after waiting for " +
            timeoutSecs + " seconds. Ensure a table has been created first");
    }

    /* local stores are not usually secure -- Cloudsim */
    @Override
    public boolean isSecureStore() {
        return (KVStoreImpl.getLoginManager(getStore()) != null);
    }

    @Override
    synchronized public void shutDown() {
        if (store != null) {
            store.close();
        }
    }

    @Override
    public PrepareCB createPrepareCB(String namespace) {
        /*
         * Do not allow namespaces for cloudsim.
         */
        return new PrepareCB(namespace, false) {
            @Override
            public TableMetadataHelper getMetadataHelper() {
                return tableAPI.getTableMetadataHelper();
            }
        };
    }

    /**
     * By default return a "default" set of limits. This can be overridden
     * on-premise
     */
    public RequestLimits getRequestLimits() {
        if (noLimits) {
            return RequestLimits.defaultNoLimits();
        }
        return RequestLimits.defaultLimits();
    }

    @Override
    public String getWorkRequestId(TableInfo ti, OpCode opCode) {
        return new WorkRequestId(ti.getCompartmentId(),
                                 ti.getTableName(),
                                 ti.getOperationId(),
                                 opCode,
                                 ti.getCreateTime()).encodeString();
    }

    @Override
    public GetDdlWorkRequestResponse getDdlWorkRequest(AccessContext actx,
                                                       String workRequestId,
                                                       boolean internal,
                                                       LogContext lc) {
        /* Path parameter */
        WorkRequestId workReqId = WorkRequestId.fromString(workRequestId);

        /*
         * Check access again since compartmentId and tableName are
         * extracted from workRequestId until now.
         */
        DdlHistoryEntry.Status status = DdlHistoryEntry.Status.SUCCEEDED;
        String tenantId = null;
        int planId = 0;
        Timestamp createTime = null;
        String errorCode = null;
        String errorMsg = null;
        if (actx != null) {
            actx.setCompartmentId(workReqId.getCompartmentId());

            GetTableResponse res = getTable(actx,
                                            workReqId.getTableName(),
                                            workReqId.getOperationId(),
                                            false,
                                            lc);
            if (res.getSuccess()) {
                /* Build DdlHistoryEntry from TableInfo and WorkRequestId */
                final TableInfo ti = res.getTableInfo();
                TableState tstat = ti.getStateEnum();
                status = (tstat == TableState.ACTIVE ||
                          tstat == TableState.DROPPED)?
                             DdlHistoryEntry.Status.SUCCEEDED :
                             DdlHistoryEntry.Status.INPROGRESS;

                planId = (ti.getOperationId() != null) ?
                         Integer.valueOf(ti.getOperationId()) : 0;
            } else {
                if (workReqId.getOpCode() == OpCode.DROP_TABLE &&
                    res.getErrorCode() == TABLE_NOT_FOUND) {
                    status = DdlHistoryEntry.Status.SUCCEEDED;
                } else {
                    status = DdlHistoryEntry.Status.FAILED;
                    errorCode = String.valueOf(res.getErrorCode());
                    errorMsg = res.getErrorString();
                }
                planId = 0;
            }
            createTime = new Timestamp(workReqId.getAcceptedTime());
            tenantId = actx.getTenantId();
        }
        DdlHistoryEntry ddlEntry =
            new DdlHistoryEntry(workRequestId, tenantId,
                                workReqId.getCompartmentId(),
                                workReqId.getTableName(),
                                mapDdlOp(workReqId.getOpCode()).name(),
                                status.getCode(),
                                createTime,
                                null /* updateTime */,
                                null /* indexName */,
                                null /* matchETag */,
                                false /* existFlag */,
                                null /* tags */,
                                null /* ddl */,
                                errorMsg /* resultMsg */,
                                errorCode /* errorCode */,
                                planId,
                                UUID.randomUUID().toString(),
                                false /* autoReclaimable */,
                                null /* limits */,
                                null /* retryToken */);
        return new GetDdlWorkRequestResponse(200, ddlEntry);
    }

    private DdlHistoryEntry.DdlOp mapDdlOp(OpCode op) {
        switch(op) {
        case CREATE_TABLE:
            return DdlHistoryEntry.DdlOp.createTable;
        case DROP_TABLE:
            return DdlHistoryEntry.DdlOp.dropTable;
        case ALTER_TABLE:
            return DdlHistoryEntry.DdlOp.alter;
        case CREATE_INDEX:
            return DdlHistoryEntry.DdlOp.createIndex;
        case DROP_INDEX:
            return DdlHistoryEntry.DdlOp.dropIndex;
        default:
            throw new IllegalArgumentException(
                "Unexpected OpCode for mapDdlOp: " + op);
        }
    }

    @Override
    public GetWorkRequestResponse getWorkRequest(AccessContext actx,
                                                 String workRequestId,
                                                 boolean internal,
                                                 LogContext lc) {

        /* Path parameter */
        WorkRequestId workReqId = WorkRequestId.fromString(workRequestId);

        /*
         * Check access again since compartmentId and tableName are
         * extracted from workRequestId until now.
         */
        WorkRequest.ActionType actionType = WorkRequest.ActionType.IN_PROGRESS;
        WorkRequest.Status status = WorkRequest.Status.IN_PROGRESS;
        Timestamp createTime = new Timestamp(workReqId.getAcceptedTime());

        ErrorCode errorCode = null;
        String errorMsg = null;
        if (actx != null) {
            actx.setCompartmentId(workReqId.getCompartmentId());
            GetTableResponse res = getTable(actx,
                                            workReqId.getTableName(),
                                            workReqId.getOperationId(),
                                            false,
                                            lc);
            if (res.getSuccess()) {
                final TableInfo ti = res.getTableInfo();
                status = (ti.getStateEnum() == TableState.ACTIVE ||
                          ti.getStateEnum() == TableState.DROPPED)?
                              WorkRequest.Status.SUCCEEDED :
                              WorkRequest.Status.IN_PROGRESS;
                if (workReqId.getOpCode() == OpCode.CREATE_TABLE) {
                    actionType = WorkRequest.ActionType.CREATED;
                } else if (workReqId.getOpCode() == OpCode.DROP_TABLE) {
                    actionType = WorkRequest.ActionType.DELETED;
                }
            } else {
                if (workReqId.getOpCode() == OpCode.DROP_TABLE &&
                    res.getErrorCode() == TABLE_NOT_FOUND) {
                    status = WorkRequest.Status.SUCCEEDED;
                    actionType = WorkRequest.ActionType.DELETED;
                } else {
                    status = WorkRequest.Status.FAILED;
                    errorCode = ErrorCode.values()[res.getErrorCode()];
                    errorMsg = res.getErrorString();
                }
            }
        }

        String identifier = NameUtils.makeQualifiedName(
                                workReqId.getCompartmentId(),
                                workReqId.getTableName());
        WorkRequest workRequest =
            new WorkRequest(workRequestId,
                            mapOperationType(workReqId.getOpCode()),
                            status,
                            workReqId.getCompartmentId(),
                            identifier,
                            workReqId.getTableName(),
                            EntityType.TABLE,
                            null /* tags */,
                            actionType,
                            createTime.getTime(),
                            0 /* timeStarted */,
                            0 /* timeUpdated */,
                            errorCode,
                            errorMsg);
        return new GetWorkRequestResponse(200, workRequest);
    }

    private OperationType mapOperationType(OpCode op) {
        switch(op) {
        case CREATE_TABLE:
            return OperationType.CREATE_TABLE;
        case DROP_TABLE:
            return OperationType.DELETE_TABLE;
        case ALTER_TABLE:
        case CREATE_INDEX:
        case DROP_INDEX:
            return OperationType.UPDATE_TABLE;
        default:
            throw new IllegalArgumentException(
                "Unexpected OpCode for mapOperationType: " + op);
        }
    }

    @Override
    public ReplicaStatsResponse getReplicaStats(AccessContext actx,
                                                String tableName,
                                                String replicaName,
                                                long startTime,
                                                int limit,
                                                LogContext lc) {

        final ReplicaStats fakeStats = new ReplicaStats(startTime, 1000);

        /* will throw if table doesn't exist */
        getTable(actx, tableName, null, false, lc);

        /*
         * This TenantManager doesn't have access to replicaStats information.
         */
        return new ReplicaStatsResponse(200,
                Map.of("replica", Arrays.asList(fakeStats)), startTime);
    }

    /**
     * The WorkRequestId represents the work request id.
     *
     * Its string format returned with WorkRequestId.encodeString() is the
     * returned response of ddl operations, and it is the input of
     * getWorkRequest() to check the status of ddl's execution.
     *
     * It contains below informations:
     *  o compartmentId, tableName, operationId: the required input for
     *    getTable().
     *  o acceptedTime, opCode: the needed information for response of
     *    getWorkRequest()
     */
    private static class WorkRequestId {

        private String compartmentId;
        private String tableName;
        private String operationId;
        private long acceptedTime;
        private OpCode opCode;

        public WorkRequestId(String compartmentId,
                             String tableName,
                             String operationId,
                             OpCode opCode,
                             long acceptedTime) {
            this.compartmentId = compartmentId;
            this.tableName = tableName;
            this.operationId = operationId;
            this.opCode = opCode;
            this.acceptedTime = acceptedTime;
            validate();
        }

        public String getCompartmentId() {
            return compartmentId;
        }

        public String getTableName() {
            return tableName;
        }

        public String getOperationId() {
            return operationId;
        }

        public long getAcceptedTime() {
            return acceptedTime;
        }

        public OpCode getOpCode() {
            return opCode;
        }

        public String encodeString() {

            try (ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
                 DataOutputStream out = new DataOutputStream(baos)) {

                out.writeUTF(compartmentId);
                out.writeUTF(tableName);
                if (operationId != null) {
                    out.writeUTF(operationId);
                } else {
                    out.writeUTF("");
                }
                out.writeByte((byte)(opCode.ordinal()));
                out.writeLong(acceptedTime);

                return JsonProtocol.urlEncodeBase64(baos.toByteArray());
            } catch (IOException ioe) {
                throw new IllegalArgumentException(
                    "Encode WorkRequestId to String failed: "  + ioe);
            }
        }

        public static WorkRequestId fromString(String str) {
            byte[] bytes = JsonProtocol.urlDecodeBase64(str);

            try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                 DataInputStream in = new DataInputStream(bais)) {

                String compartmentId = in.readUTF();
                String tableName = in.readUTF();
                String operationId = in.readUTF();
                if (operationId.isEmpty()) {
                    operationId = null;
                }
                int index = in.readByte();
                OpCode opCode = OpCode.values()[index];
                long acceptedTime = in.readLong();

                return new WorkRequestId(compartmentId, tableName, operationId,
                                         opCode, acceptedTime);
            } catch (IOException ioe) {
                throw new IllegalArgumentException(
                    "Decode WorkRequestId from String failed: "  + ioe);
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("compartmentId=").append(compartmentId);
            sb.append("; tableName=").append(tableName);
            sb.append("; operationId=").append(operationId);
            sb.append("; opCode=").append(opCode.name());
            return sb.toString();
        }

        private void validate() {
            /* TODO: validation */
        }
    }
}
