/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy.rest;

import static oracle.nosql.proxy.protocol.HttpConstants.FILTERS_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import oracle.nosql.util.HttpRequest;
import oracle.nosql.util.HttpResponse;
import oracle.nosql.util.filter.Rule;
import oracle.nosql.util.filter.Rule.Action;

import com.google.gson.reflect.TypeToken;
import com.oracle.bmc.ClientConfiguration.ClientConfigurationBuilder;
import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.nosql.model.ChangeTableCompartmentDetails;
import com.oracle.bmc.nosql.model.CreateIndexDetails;
import com.oracle.bmc.nosql.model.IndexKey;
import com.oracle.bmc.nosql.model.KmsKey;
import com.oracle.bmc.nosql.model.PreparedStatement;
import com.oracle.bmc.nosql.model.QueryDetails;
import com.oracle.bmc.nosql.model.TableLimits;
import com.oracle.bmc.nosql.model.UpdateHostedConfigurationDetails;
import com.oracle.bmc.nosql.model.UpdateRowDetails;
import com.oracle.bmc.nosql.model.UpdateTableDetails;
import com.oracle.bmc.nosql.requests.ChangeTableCompartmentRequest;
import com.oracle.bmc.nosql.requests.CreateIndexRequest;
import com.oracle.bmc.nosql.requests.DeleteIndexRequest;
import com.oracle.bmc.nosql.requests.DeleteRowRequest;
import com.oracle.bmc.nosql.requests.DeleteTableRequest;
import com.oracle.bmc.nosql.requests.GetConfigurationRequest;
//import com.oracle.bmc.nosql.requests.GetConfigurationRequest;
import com.oracle.bmc.nosql.requests.GetIndexRequest;
import com.oracle.bmc.nosql.requests.GetRowRequest;
import com.oracle.bmc.nosql.requests.GetWorkRequestRequest;
import com.oracle.bmc.nosql.requests.ListIndexesRequest;
import com.oracle.bmc.nosql.requests.ListTableUsageRequest;
import com.oracle.bmc.nosql.requests.ListTablesRequest;
import com.oracle.bmc.nosql.requests.ListWorkRequestsRequest;
import com.oracle.bmc.nosql.requests.PrepareStatementRequest;
import com.oracle.bmc.nosql.requests.QueryRequest;
import com.oracle.bmc.nosql.requests.SummarizeStatementRequest;
import com.oracle.bmc.nosql.requests.UnassignKmsKeyRequest;
import com.oracle.bmc.nosql.requests.UpdateConfigurationRequest;
import com.oracle.bmc.nosql.requests.UpdateRowRequest;
import com.oracle.bmc.nosql.requests.UpdateTableRequest;
import com.oracle.bmc.nosql.responses.PrepareStatementResponse;
import com.oracle.bmc.nosql.responses.UnassignKmsKeyResponse;
import com.oracle.bmc.nosql.responses.UpdateConfigurationResponse;
import com.oracle.bmc.retrier.RetryConfiguration;
import com.oracle.bmc.util.CircuitBreakerUtils;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;

/*
 * This test suite is only for miniCloud test.
 *
 * Tests filtering request based on rules.
 *
 * The 2 methods blockOps() and executeOps() are used to execute request:
 *   o blockOps() expects to get specified status code due to being blocked.
 *   o executeOps() expect the request executed successfully.
 *
 * Basically, the test add rule and then run operations using above 2 methods
 * to verify that the operations matching the rule will be blocked, those that
 * don't match the rule can execute successfully
 */
public class FilterTest extends RestAPITestBase {

    private final int REQUEST_WAIT_MS = 3000;

    private final String proxyFilterUrl =
        getProxyEndpoint() + "/V0/" + FILTERS_PATH;
    private String scFilterUrl;
    private final HttpRequest httpRequest = new HttpRequest().disableRetry();

    private final String tableName = "restFilterTest";
    private final String indexName = "idxName";

    private final String createTableDdl = "create table if not exists " +
            tableName + "(id integer, name string, primary key(id))";
    private final String alterTableDdl = "alter table " + tableName +
            "(add i1 integer)";

    private final TableLimits limits = TableLimits.builder()
            .maxReadUnits(100)
            .maxWriteUnits(20)
            .maxStorageInGBs(1)
            .build();

    private static final Map<String, Map<String, Object>> tags =
            new HashMap<>();
    static {
        Map<String, Object> props = new HashMap<>();
        props.put("type", "backup");
        props.put("purpose", "WebTier");
        tags.put("Operations", props);
    }

    private static final Rule.Action dropRequest = Rule.DROP_REQUEST;
    private static final Rule.Action returnError =
        new Rule.ReturnErrorAction(102 /* SERVICE_UNAVAILABLE*/,
                                   "server is undergoing maintenance");
    /* 503 Service Unavailable */
    private static final int returnErrorRespCode = 503;

    private final String selectStmt = "select * from " + tableName;
    private final String insertStmt = "insert into " + tableName +
                                      " values(3, 'abc')";
    private final String deleteStmt = "delete from " + tableName +
                                      " where id = 3";
    private final String updateStmt = "update " + tableName +
                                      " set name=\"name_upd\" where id = 3";
    private String workRequestId = null;

    private String selectPrepStmt;
    private String insertPrepStmt;
    private String updatePrepStmt;
    private String deletePrepStmt;
    private String tableOcid;

    /*
     * Operations
     */
    private final OpWrapper createTable = new OpWrapper("createTable") {
        @Override
        void execOp(String tableNameOrid, boolean isTableId) {
            createTable(tableNameOrid, createTableDdl, limits, false /* wait */);
        }
    };

    private final OpWrapper alterTable = new OpWrapper("alterTable") {
        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            updateTable(tableNameOrId, isTableId, alterTableDdl, null, null);
        }
    };

    private final OpWrapper updateTableLimits = new OpWrapper("updateLimits") {
        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            updateTable(tableNameOrId, isTableId, null, null, limits);
        }
    };

    private final OpWrapper updateTableTags = new OpWrapper("updateTableTags") {
        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            updateTable(tableNameOrId, isTableId, null, tags, null);
        }
    };

    private final OpWrapper dropTable = new OpWrapper("dropTable") {
        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            deleteTable(tableNameOrId, isTableId);
        }
    };

    private final OpWrapper getTable = new OpWrapper("getTable") {
        @Override
        void execOp(String tableNameOrid, boolean isTableId) {
            getTable((isTableId ? null : getCompartmentId()), tableNameOrid);
        }
    };

    private final OpWrapper listTables = new OpWrapper("listTables") {
        @Override
        void execOp(String tableNameOrid, boolean isTableId) {
            ListTablesRequest req = ListTablesRequest.builder()
                    .compartmentId(getCompartmentId())
                    .build();
            client.listTables(req);
        }
    };

    private final OpWrapper createIndex = new OpWrapper("createIndex") {
        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            createIndex(tableNameOrId, isTableId);
        }
    };

    private final OpWrapper dropIndex = new OpWrapper("dropIndex") {
        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            deleteIndex(tableNameOrId, isTableId);
        }
    };

    private final OpWrapper listIndexes = new OpWrapper("listIndexes") {
        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            listIndexes(tableNameOrId, isTableId);
        }
    };

    private final OpWrapper getIndex = new OpWrapper("getIndex") {
        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            getIndex(tableNameOrId, isTableId);
        }
    };

    private final OpWrapper changeCompartment = new OpWrapper("changeCompt") {
        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            changeCompartment(tableNameOrId, isTableId);
        }
    };

    private final OpWrapper listWorkRequests = new OpWrapper("listWorkReqs") {

        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            ListWorkRequestsRequest req = ListWorkRequestsRequest.builder()
                    .compartmentId(getCompartmentId())
                    .build();
            client.listWorkRequests(req);
        }
    };

    private final OpWrapper getWorkRequest = new OpWrapper("getWorkRequest") {

        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            GetWorkRequestRequest req = GetWorkRequestRequest.builder()
                    .workRequestId(workRequestId)
                    .build();
            client.getWorkRequest(req);
        }
    };

    private final OpWrapper getTableUsage = new OpWrapper("getTableUsage") {
        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            listTableUsage(tableNameOrId, isTableId);
        }
    };

    private final OpWrapper put = new OpWrapper("put") {
        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            put(tableNameOrId, isTableId);
        }
    };

    private final OpWrapper get = new OpWrapper("get") {
        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            get(tableNameOrId, isTableId);
        }
    };

    private final OpWrapper delete = new OpWrapper("delete") {
        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            delete(tableNameOrId, isTableId);
        }
    };

    private final OpWrapper prepare = new OpWrapper("prepare") {
        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            PrepareStatementRequest req = PrepareStatementRequest.builder()
                    .compartmentId(getCompartmentId())
                    .statement(selectStmt)
                    .build();
            client.prepareStatement(req);
        }
    };

    private final OpWrapper selectQuery = new OpWrapper("selectQuery") {
        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            runQuery(selectStmt);
        }
    };

    private final OpWrapper selectPrepQuery = new OpWrapper("selectPrepQuery") {
        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            runPreparedQuery(selectPrepStmt);
        }
    };

    private final OpWrapper insertQuery = new OpWrapper("insertQuery") {
        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            runQuery(insertStmt);
        }
    };

    private final OpWrapper insertPrepQuery = new OpWrapper("insertPrepQuery") {
        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            runPreparedQuery(insertPrepStmt);
        }
    };

    private final OpWrapper deleteQuery = new OpWrapper("deleteQuery") {
        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            runQuery(deleteStmt);
        }
    };

    private final OpWrapper deletePrepQuery = new OpWrapper("deletePrepQuery") {
        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            runPreparedQuery(deletePrepStmt);
        }
    };

    private final OpWrapper updateQuery = new OpWrapper("updateQuery") {
        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            runQuery(updateStmt);
        }
    };

    private final OpWrapper updatePrepQuery = new OpWrapper("updatePrepQuery") {
        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            runPreparedQuery(updatePrepStmt);
        }
    };

    private final OpWrapper summarize = new OpWrapper("summarize") {
        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            SummarizeStatementRequest req = SummarizeStatementRequest.builder()
                    .compartmentId(getCompartmentId())
                    .statement(selectStmt)
                    .build();
            client.summarizeStatement(req);
        }
    };

    private final OpWrapper getConfiguration =
            new OpWrapper("getConfiguration") {
        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            GetConfigurationRequest req = GetConfigurationRequest.builder()
                    .compartmentId(TENANT_NOSQL_DEV)
                    .build();
            client.getConfiguration(req);
        }
    };

    private final OpWrapper updateConfiguration =
            new OpWrapper("updateConfiguration") {
        private final String testKeyId =
            "ocid1.key.oc1.ca-montreal-1.gbtt5qeuaaa2c.ab4xkljr2eqgqifrmpxqddjdtic2lfj4owmln4dyfu4hhifw677hanrk5pna";

        @Override
        void execOp(String tableNameOrId, boolean isTableId) {
            KmsKey.Builder key = KmsKey.builder().id(testKeyId);

            UpdateHostedConfigurationDetails details =
                UpdateHostedConfigurationDetails.builder()
                    .kmsKey(key.build())
                    .build();

            UpdateConfigurationRequest req =
                 UpdateConfigurationRequest.builder()
                     .compartmentId(TENANT_NOSQL_DEV)
                     .updateConfigurationDetails(details)
                     .build();

            client.updateConfiguration(req);
        }
    };

    private final OpWrapper unassignKmsKey = new OpWrapper("unassignKmsKey") {
         @Override
        void execOp(String tableNameOrId, boolean isTableId) {
             UnassignKmsKeyRequest req = UnassignKmsKeyRequest
                     .builder()
                     .compartmentId(TENANT_NOSQL_DEV)
                     .build();
             client.unassignKmsKey(req);
        }
    };

    private OpWrapper[] ddlOps = new OpWrapper[] {
        createTable,
        alterTable,
        updateTableLimits,
        updateTableTags,
        createIndex,
        dropIndex,
        changeCompartment,
        dropTable
    };

    private OpWrapper[] writeOps = new OpWrapper[] {
        put,
        delete,
        insertQuery,
        insertPrepQuery,
        deleteQuery,
        deletePrepQuery,
        updateQuery,
        updatePrepQuery,
    };

    private OpWrapper[] readOps = new OpWrapper[] {
        get,
        summarize,
        prepare,
        selectQuery,
        selectPrepQuery,

        getTable,
        getIndex,
        listTables,
        listIndexes,

        getTableUsage,
        getWorkRequest,
        listWorkRequests
    };

    @BeforeClass
    public static void staticSetUp() throws Exception {
        Assume.assumeTrue(
            "Skipping FilterTest if not minicloud test",
            Boolean.getBoolean(USEMC_PROP));

        RestAPITestBase.staticSetUp();
    }

    @Override
    protected void configClient(ClientConfigurationBuilder builder) {
        /*
         * Disable circuit breaker(enabled by default) in this test, the circuit
         * breaker is in client, it will block requests once the number of
         * failed requests reaches threshold. The filter test is intended to
         * test requests will be blocked by proxy, so disable the circuit
         * breaker.
         */
        super.configClient(builder);
        builder.readTimeoutMillis(REQUEST_WAIT_MS)
               .retryConfiguration(RetryConfiguration.NO_RETRY_CONFIGURATION)
               .circuitBreakerConfiguration(
                       CircuitBreakerUtils.getNoCircuitBreakerConfiguration());
    }

    @Override
    public void setUp() throws Exception {
        /*
         * Configures Netty logging to suppress the logging output when
         * try to instantiate slf4j logger firstly.
         */
        InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
        removeAllRules();
        super.setUp();
        initTableAndPreparedStmts();
    }

    private void initTableAndPreparedStmts() {
        workRequestId = createTable(tableName, createTableDdl, limits);
        createIndex(tableName, indexName, new String[]{"name"});
        tableOcid = getTableId(tableName);

        selectPrepStmt = prepare(selectStmt);
        insertPrepStmt = prepare(insertStmt);
        updatePrepStmt = prepare(updateStmt);
        deletePrepStmt = prepare(deleteStmt);
    }

    @Override
    public void tearDown() throws Exception {
        removeAllRules();
        super.tearDown();
    }

    /*
     * Filter requests by the rule containing operations only
     */
    @Test
    public void testOpRule() {
        /*
         * all operations should be blocked by the rule:
         *  {"name":"block_ops", "operations":["ALL"]}
         */
        Rule rule = addRule("block_ops",
                            dropRequest,
                            new String[]{"all"},
                            false /* persist */);
        blockOps(rule, ddlOps);
        blockOps(rule, writeOps);
        blockOps(rule, readOps);
        deleteRule("block_ops", false /* persist */);

        /*
         * ddl and write operations should be blocked by the rule:
         *  {"name":"block_ops", "operations":["DDL", "WRITE"]}
         */
        rule = addRule("block_ops",
                       returnError,
                       new String[] {"ddl", "write"},
                       false /* persist */);
        blockOps(rule, ddlOps);
        blockOps(rule, writeOps);
        executeOps(readOps);
    }

    /*
     * Test filtering requests by the rule with principal tenant and/or user
     * information
     */
    @Test
    public void testPrincipalRule() {
        String principalTenantId = getTenantId();
        String principalId = getUserId();

        /*
         * Add rule to block all operations from the user of specified tenant:
         *  {
         *    "name": "block_tenant",
         *    "tenant": <tenant_id>,
         *    "operations":["ddl"]
         *  }
         */
        Rule rule = addRule("block_tenant",
                            dropRequest,
                            principalTenantId,
                            null, /* user */
                            null, /* table */
                            new String[] {"ddl"},
                            false /* persist */);
        blockOps(rule, ddlOps);
        executeOps(put, getTable);
        assertTrue(deleteRule("block_tenant", false));

        /*
         * Add rule to block ddl and write requests from the specified user.
         * {
         *   "name": "block_user",
         *   "user": <principal-id>,
         *   "table": <table-ocid>,
         *   "operations":[write]
         * }
         */
        rule = addRule("block_user",
                       returnError,
                       null, /* tenant */
                       principalId,
                       null, /* table */
                       new String[]{"write"},
                       false /* persist */);
        blockOps(rule, writeOps);
        executeOps(get, createTable);
    }

    /*
     * Filter requests by the rule containing table ocid.
     */
    @Test
    public void testTableRule() {
        OpWrapper[] table_ddl_ops = new OpWrapper[] {
            alterTable,
            updateTableLimits,
            updateTableTags,
            createIndex,
            dropIndex,
            dropTable,
            changeCompartment,
        };

        OpWrapper[] table_write_ops = new OpWrapper[] {
            put,
            delete,
            insertQuery,
            deleteQuery
        };

        OpWrapper[] table_read_ops = new OpWrapper[] {
            getTable,
            listIndexes,
            getTableUsage,

            prepare,
            selectQuery,
            get
        };

        /*
         * Add rule to block all requests to the specified target table:
         * {
         *   "name": "block_table",
         *   "table": <table-ocid>,
         *   "operations": ["ALL]
         * }
         */
        Rule rule = addRule("block_table",
                            returnError,
                            null,       /* tenant */
                            null,       /* user */
                            tableOcid,  /* table */
                            new String[] {"all"}, /* operations */
                            false);     /* persist */
        blockOps(rule, table_ddl_ops);
        blockOps(rule, table_write_ops);
        blockOps(rule, table_read_ops);
        executeOps(listTables, listWorkRequests, getWorkRequest);
        deleteRule("block_table", false /* persist */);

        /*
         * Update the rule to block ddl requests to the specified target table:
         * {
         *   "name": "block_table",
         *   "table": <table-ocid>,
         *   "operations": ["dll"]
         * }
         */
        rule = addRule("block_table",
                       dropRequest,
                       null,    /* tenant */
                       null,    /* user */
                       tableOcid,
                       new String[]{"ddl"},
                       false);  /* persist */
        blockOps(rule, table_ddl_ops);
        blockOpsWithTableOcid(rule, tableOcid, table_ddl_ops);

        executeOps(table_write_ops);
        executeOps(table_read_ops);
        executeOpsWithTableOcid(tableOcid, listIndexes, getTable,
                                getTableUsage, put, get, delete);
    }

    /*
     * Test filtering query request.
     *
     * Query can be a read or write operation, the actual operation is deferred
     * to determine after parse the statement in handleQuery().
     *
     * This test is to verify query operation can be blocked as expected by the
     * rule that blocks "read" or "write" operation.
     */
    @Test
    public void testQuery() {
        /*
         * Add rule to block read requests to the specified target table:
         *  {
         *    "name":"block_query",
         *    "table": tableOcid,
         *    "operations": ["READ"]
         *  }
         *
         * The prepare, summarize and select query should be blocked, the
         * insert/delete query should execute successfully.
         */
        Rule rule = addRule("block_query",
                            returnError,
                            null,   /* tenant */
                            null,   /* user */
                            null,   /* table */
                            new String[]{"read"},
                            false); /* persist */
        blockOps(rule, prepare, summarize, selectQuery, selectPrepQuery,
                 updateQuery, updatePrepQuery, deleteQuery, deletePrepQuery);
        executeOps(insertQuery, insertPrepQuery);
        deleteRule("block_query", false /* persist */);

        /*
         * Update rule to block write requests to the specified target table:
         *  {
         *    "name":"block_query",
         *    "table": tableOcid,
         *    "operations": ["WRITE"]
         *  }
         *
         * The delete/insert query should be blocked, and prepare, summarize and
         * select query should execute successfully.
         */
        rule = addRule("block_query",
                       dropRequest,
                       null,    /* tenant */
                       null,    /* user */
                       tableOcid,
                       new String[]{"write"},
                       false);  /* persist */
        blockOps(rule, insertQuery, insertPrepQuery, deleteQuery,
                 deletePrepQuery, updateQuery, updatePrepQuery);
        executeOps(prepare, summarize, selectQuery, selectPrepQuery);
    }

    /* Test persistent rule */
    @Test
    public void testPersistentRule() {
        String principalTenantId = getTenantId();
        String principalId = getUserId();

        /*
         * Add rule to block all operations from the user of specified tenant:
         *  {
         *    "name": "block_tenant",
         *    "tenant": <tenant_id>,
         *    "operations":["ddl"]
         *  }
         */
        Rule rule = addRule("block_tenant",
                            dropRequest,
                            principalTenantId,
                            null,   /* user */
                            null,   /* table */
                            new String[] {"ddl"},
                            true);  /* persist */
        reloadPersistentRules();
        blockOps(rule, ddlOps);
        executeOps(put, getTable);

        assertTrue(deleteRule("block_tenant", true /* persist */));
        reloadPersistentRules();
        executeOps(updateTableLimits, createIndex, dropIndex);

        /*
         * Add rule to block ddl and write requests from the specified user.
         * {
         *   "name": "block_user",
         *   "user": <principal-id>,
         *   "table": <table-ocid>,
         *   "operations":[write]
         * }
         */
        rule = addRule("block_user",
                       returnError,
                       null,    /* user */
                       principalId,
                       tableOcid,
                       new String[]{"write"},
                       true);   /* persist */
        reloadPersistentRules();
        blockOps(rule, writeOps);
        executeOps(get, createTable);
    }

    @Test
    public void testConfigurationOps() {
        try {
            setDedicatedTenantId(TENANT_NOSQL_DEV);

            /*
             * all configuration operations should be blocked by the rule:
             *  {"name":"block_ops", "operations":["ALL"]}
             */
            Rule rule = addRule("block_ops",
                                dropRequest,
                                new String[]{"config_read", "config_update"},
                                false /* persist */);
            blockOps(rule,
                     getConfiguration,
                     updateConfiguration,
                     unassignKmsKey);

            /*
             * delete the block_ops, the configuration operations can be
             * executed
             */
            deleteRule("block_ops", false /* persist */);
            executeOps(getConfiguration, updateConfiguration, unassignKmsKey);

            /*
             * The updateConfiguration and unassignKmsKey should be blocked by
             * the rule:
             * {"name":"block_config_update", "operations":["config_update"]}
             */
            rule = addRule("block_config_update",
                           returnError,
                           new String[]{"CONFIG_UPDATE"},
                           false /* persist */);
            blockOps(rule, updateConfiguration, unassignKmsKey);
            executeOps(getConfiguration, getTable);

            /*
             * delete the block_config_update, the configuration operations can
             * be executed.
             */
            deleteRule("block_config_update", false /* persist */);
            executeOps(getConfiguration, updateConfiguration, unassignKmsKey);
        } finally {
            setDedicatedTenantId(null);
        }
    }

    private void executeOps(OpWrapper... ops) {
        for (OpWrapper op : ops) {
            op.exec();
        }
    }

    private void blockOps(Rule rule, OpWrapper... ops) {
        int expCode = getExpectedStatusCode(rule);
        for (OpWrapper op : ops) {
            op.exec(expCode);
        }
    }

    private void executeOpsWithTableOcid(String tableId, OpWrapper... ops) {
        for (OpWrapper op : ops) {
            op.execWithOcid(tableId);
        }
    }

    private void blockOpsWithTableOcid(Rule rule,
                                       String tableId,
                                       OpWrapper... ops) {
        int expCode = getExpectedStatusCode(rule);
        for (OpWrapper op : ops) {
            op.execWithOcid(tableId, expCode);
        }
    }

    private int getExpectedStatusCode(Rule rule) {
        switch (rule.getActionType()) {
        case DROP_REQUEST:
            return -1;
        case RETURN_ERROR:
            return returnErrorRespCode;
        default:
            fail("Unexpceted action type: " + rule.getAction());
        }
        return 0;
    }

    private void put(String tableNameOrId, boolean isTableId) {
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("id", 1);
        row.put("name", "name1");
        UpdateRowDetails info = UpdateRowDetails.builder()
                .compartmentId((isTableId ? null : getCompartmentId()))
                .value(row)
                .build();
        UpdateRowRequest req = UpdateRowRequest.builder()
                .tableNameOrId(tableNameOrId)
                .updateRowDetails(info)
                .build();
        client.updateRow(req);
    }

    private void get(String tableNameOrId, boolean isTableId) {
        List<String> key = new ArrayList<String>();
        key.add("id:1");
        GetRowRequest req = GetRowRequest.builder()
                .tableNameOrId(tableNameOrId)
                .compartmentId((isTableId ? null : getCompartmentId()))
                .key(key)
                .build();
        client.getRow(req);
    }

    private void delete(String tableNameOrId, boolean isTableId) {
        List<String> key = new ArrayList<String>();
        key.add("id:1");
        DeleteRowRequest req = DeleteRowRequest.builder()
                .tableNameOrId(tableNameOrId)
                .compartmentId((isTableId ? null : getCompartmentId()))
                .key(key)
                .build();
        client.deleteRow(req);
    }

    private void updateTable(String tableNameOrId,
                             boolean isTableId,
                             String ddl,
                             Map<String, Map<String, Object>> defTags,
                             TableLimits tableLimits) {

        UpdateTableDetails.Builder builder = UpdateTableDetails.builder();
        if (!isTableId) {
            builder.compartmentId(getCompartmentId());
        }
        if (ddl != null) {
            builder.ddlStatement(ddl);
        } else if (defTags != null) {
            builder.definedTags(defTags);
        } else if (tableLimits != null) {
            builder.tableLimits(tableLimits);
        } else {
            fail("One of ddl, definedTags and tableLimits should be specified");
        }

        UpdateTableDetails info = builder.build();
        UpdateTableRequest req = UpdateTableRequest.builder()
                .tableNameOrId(tableNameOrId)
                .updateTableDetails(info)
                .build();
        client.updateTable(req);
    }

    private void createIndex(String tableNameOrId, boolean isTableId) {
        /* Create Index */
        List<IndexKey> keys = new ArrayList<IndexKey>();
        keys.add(IndexKey.builder().columnName("name").build());

        CreateIndexDetails info = CreateIndexDetails.builder()
                .name(indexName)
                .compartmentId((isTableId ?  null : getCompartmentId()))
                .keys(keys)
                .build();
        CreateIndexRequest req = CreateIndexRequest.builder()
                .tableNameOrId(tableNameOrId)
                .createIndexDetails(info)
                .build();
        client.createIndex(req);
    }

    private void deleteIndex(String tableNameOrId, boolean isTableId) {
        /* Delete Index */
        DeleteIndexRequest req = DeleteIndexRequest.builder()
                .tableNameOrId(tableNameOrId)
                .compartmentId((isTableId ? null : getCompartmentId()))
                .indexName(indexName)
                .build();
        client.deleteIndex(req);
    }

    private void listIndexes(String tableNameOrId, boolean isTableId) {
        /* List Indexes */
        ListIndexesRequest req = ListIndexesRequest.builder()
                .tableNameOrId(tableNameOrId)
                .compartmentId((isTableId ? null : getCompartmentId()))
                .build();
        client.listIndexes(req);
    }

    private void getIndex(String tableNameOrId, boolean isTableId) {
        /* Get Index */
        GetIndexRequest req = GetIndexRequest.builder()
                .tableNameOrId(tableNameOrId)
                .compartmentId((isTableId ? null : getCompartmentId()))
                .indexName(indexName)
                .build();
        client.getIndex(req);
    }

    private void deleteTable(String tableNameOrId, boolean isTableId) {
        DeleteTableRequest req = DeleteTableRequest.builder()
                .compartmentId((isTableId ? null : getCompartmentId()))
                .tableNameOrId(tableNameOrId)
                .isIfExists(true)
                .build();
        client.deleteTable(req);
    }

    private void changeCompartment(String tableNameOrId, boolean isTableId) {
        String toCompId =
            "ocid1.compartment.oc1..aaaaaaaahy6aozjru5grkp2dhrhqfdwh4hihd6fpeafqdxvlfb6scf7hotnq";
        ChangeTableCompartmentDetails info =
            ChangeTableCompartmentDetails.builder()
                .fromCompartmentId((isTableId ? null : getCompartmentId()))
                .toCompartmentId(toCompId)
                .build();
        ChangeTableCompartmentRequest req =
            ChangeTableCompartmentRequest.builder()
                .tableNameOrId(tableNameOrId)
                .changeTableCompartmentDetails(info)
                .build();
        client.changeTableCompartment(req);
    }

    private void listTableUsage(String tableNameOrId, boolean isTableId) {
        ListTableUsageRequest req = ListTableUsageRequest.builder()
                .tableNameOrId(tableNameOrId)
                .compartmentId((isTableId ? null : getCompartmentId()))
                .limit(1)
                .build();
        client.listTableUsage(req);
    }

    private Rule addRule(String name,
                         Action action,
                         String[] operations,
                         boolean persist) {
        return addRule(name, action, null /* tenant */, null /* user */,
                       null /* table */, operations, persist);
    }

    private Rule addRule(String name,
                         Action action,
                         String testTenantId,
                         String testUserId,
                         String tableId,
                         String[] operations,
                         boolean persist) {

        Rule rule = Rule.createRule(name, action, testTenantId,
                                    testUserId, tableId, operations);
        addRule(rule.toJson(), persist);

        rule = getRule(name, persist);
        assertNotNull(rule);
        return rule;
    }

    private void addRule(String payload, boolean persist) {
        addRule(payload, HttpResponseStatus.OK.code(), persist);
    }

    private void addRule(String payload, int statusCode, boolean persist) {
        String url = getUrl(null, persist);
        HttpResponse resp = httpRequest.doHttpPost(url, payload);
        assertEquals(statusCode, resp.getStatusCode());
    }

    private Rule getRule(String name, boolean persist) {
        return getRule(name, HttpResponseStatus.OK.code(), persist);
    }

    private Rule getRule(String name, int statusCode, boolean persist) {
        String url = getUrl(name, persist);
        HttpResponse resp = httpRequest.doHttpGet(url);
        assertEquals(statusCode, resp.getStatusCode());
        if (statusCode == HttpResponseStatus.OK.code()) {
            return parseRuleFromResponse(resp);
        }
        return null;
    }

    private boolean deleteRule(String name, boolean persist) {
        return deleteRule(name, HttpResponseStatus.OK.code(), persist);
    }

    private boolean deleteRule(String name, int statusCode, boolean persist) {
        String url = getUrl(name, persist);
        HttpResponse resp = httpRequest.doHttpDelete(url, null);
        assertEquals(statusCode, resp.getStatusCode());
        if (statusCode == HttpResponseStatus.OK.code()) {
            return resp.getOutput().contains("deleted");
        }
        return false;
    }

    private List<Rule> listRules(boolean persist) {
        String url = getUrl(null, persist);
        HttpResponse resp = httpRequest.doHttpGet(url);
        assertEquals(HttpResponseStatus.OK.code(), resp.getStatusCode());
        return parseRulesFromResponse(resp);
    }

    private void reloadPersistentRules() {
        String url = getUrl("reload", false);
        HttpResponse resp = httpRequest.doHttpPut(url, null);
        assertEquals(HttpResponseStatus.OK.code(), resp.getStatusCode());
    }

    private String getUrl(String append, boolean persist) {
        String url = persist ? getSCFilterUrl() : proxyFilterUrl;
        if (url == null) {
            fail("Filter url should not be null");
        }
        if (append != null) {
            url += "/" + append;
        }
        return url;
    }

    private String getSCFilterUrl() {
        if (cloudRunning) {
            if (scFilterUrl == null && scHost != null && scPort != null) {
                scFilterUrl = "http://" + scHost + ":" + scPort + "/V0/filters";
            }
            return scFilterUrl;
        }
        return null;
    }

    private void removeAllRules() {
        removeAllRules(false);
        removeAllRules(true);
        reloadPersistentRules();
    }

    private void removeAllRules(boolean persist) {
        List<Rule> rules = listRules(persist);
        for (Rule rule : rules) {
            assertTrue(deleteRule(rule.getName(), persist));
        }

        assertTrue(listRules(persist).isEmpty());
    }

    private List<Rule> parseRulesFromResponse(HttpResponse resp) {
        String output = resp.getOutput().trim();
        if (output.isEmpty()) {
            return null;
        }

        Type type = new TypeToken<List<Rule>>(){}.getType();
        return Rule.getGson().fromJson(output, type);
    }

    private Rule parseRuleFromResponse(HttpResponse resp) {
        String output = resp.getOutput().trim();
        if (output.isEmpty()) {
            return null;
        }

        return Rule.fromJson(output);
    }

    private void runQuery(String sql) {
        QueryDetails info = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(sql)
                .build();
        QueryRequest req = QueryRequest.builder()
                .queryDetails(info)
                .build();
        client.query(req);
    }

    private void runPreparedQuery(String preparedStmt) {
        QueryDetails info = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(preparedStmt)
                .isPrepared(true)
                .build();
        QueryRequest req = QueryRequest.builder()
                .queryDetails(info)
                .build();
        client.query(req);
    }

    private String prepare(String query) {
        PrepareStatementRequest prepReq = PrepareStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .build();
        PrepareStatementResponse prepRet = client.prepareStatement(prepReq);
        PreparedStatement prepStmt = prepRet.getPreparedStatement();
        assertNotNull(prepStmt);
        return prepStmt.getStatement();
    }

    /**
     * Run a code snippet and expect a specified status code.
     */
    private abstract class OpWrapper {
        private final String name;

        OpWrapper(String name) {
            this.name = name;
        }

        void exec() {
            exec(tableName, false /* tableName */, 200);
        }

        void exec(int statusCode) {
            exec(tableName, false /* tableName */, statusCode);
        }

        void execWithOcid(String tableId) {
            execWithOcid(tableId, 200);
        }

        void execWithOcid(String tableId, int statusCode) {
            exec(tableId, true, statusCode);
        }

        private void exec(String tableNameOrId,
                          boolean isTableId,
                          int statusCode) {
            try {
                execOp(tableNameOrId, isTableId);
                if (statusCode != 200) {
                    fail("Expected get " + statusCode + ": " + name);
                }
            } catch (BmcException ex) {
                if (ex.getStatusCode() != statusCode) {
                    fail("Didn't expect " + ex + ": " + name);
                }
            }
        }

        abstract void execOp(String tableNameOrId, boolean isTableId);
    }
}
