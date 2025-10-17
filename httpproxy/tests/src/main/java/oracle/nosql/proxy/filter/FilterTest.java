/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2018 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.proxy.filter;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import org.junit.Test;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.RequestTimeoutException;
import oracle.nosql.driver.SystemException;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.GetIndexesRequest;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.ListTablesRequest;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.TableUsageRequest;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.util.HttpRequest;
import oracle.nosql.util.HttpResponse;
import oracle.nosql.util.filter.Rule;

/*
 * Tests filtering request based on rules.
 *
 * The 2 methods blockOps() and executeOps() are used to execute request:
 *   o blockOps() expects to catch the specified exception due to being blocked.
 *   o executeOps() expect the request executed successfully.
 *
 * Basically, the test add rule and then run operations using above 2 methods
 * to verify that the operations matching the rule will be blocked, those that
 * don't match the rule can execute successfully
 */
public class FilterTest extends FilterTestBase {

    private final int REQUEST_WAIT_MS = 3000;

    private final String tenantId = getTenantId();
    private final String userId = (isSecure() ? "testuser" : null);

    private final String tableName = "filterTest";
    private final String indexName = "idxName";

    private final TableLimits limits = new TableLimits(10, 10, 1);

    private final String createTableDDL = "create table if not exists " +
            tableName + "(sid integer, id integer, name string, " +
                         "primary key(shard(sid), id))";

    private final String alterTableDdl =
        "alter table " + tableName + "(add i1 integer)";

    private final String createIndexDdl =
        "create index if not exists " + indexName + " on " +
        tableName + "(name)";

    private final String dropIndexDdl =
        "drop index if exists " + indexName + " on " + tableName;

    private final String selectStmt = "select * from " + tableName;

    private final String insertStmt =
        "insert into " + tableName + " values (3, 1, 'abc')";

    private final String deleteStmt = "delete from " + tableName +
                                      " where sid = 1 and id = 1";

    private final String updateStmt = "update " + tableName +
            " set name = \"name_upd\" where sid = 1 and id = 1";

    private final MapValue row = (MapValue)MapValue.createFromJson(
            "{\"sid\":1, \"id\":1, \"name\":\"a\"}", null);
    private final MapValue key = (MapValue)MapValue.createFromJson(
            "{\"sid\":1, \"id\":2}", null);

    private String tableOcid;
    private PreparedStatement selectPrepStmt;
    private PreparedStatement insertPrepStmt;
    private PreparedStatement updatePrepStmt;
    private PreparedStatement deletePrepStmt;

    private static final Rule.Action dropRequest = Rule.DROP_REQUEST;
    private static final Rule.Action returnError =
        new Rule.ReturnErrorAction(102 /* SERVICE_UNAVAILABLE*/,
                                   "server is undergoing maintenance");
    private static final Class<?> returnErrorException = SystemException.class;

    /*
     * Operations
     */
    private final OpWrapper createTable = new OpWrapper("createTable") {
        @Override
        void execOp() {
            tableOperation(handle, createTableDDL, limits, null);
        }
    };

    private final OpWrapper alterTable = new OpWrapper("alterTable") {
        @Override
        void execOp() {
            tableOperation(handle, alterTableDdl, null, null);
        }
    };

    private final OpWrapper updateTableLimits = new OpWrapper("updateLimits") {
        @Override
        void execOp() {
            tableOperation(handle, null, limits, tableName);
        }
    };

    private final OpWrapper dropTable = new OpWrapper("dropTable") {
        @Override
        void execOp() {
            String ddl = "drop table if exists " + tableName;
            tableOperation(handle, ddl, null, null);
        }
    };

    private final OpWrapper createIndex = new OpWrapper("createIndex") {
        @Override
        void execOp() {
            tableOperation(handle, createIndexDdl, null, null);
        }
    };

    private final OpWrapper dropIndex = new OpWrapper("dropIndex") {
        @Override
        void execOp() {
            tableOperation(handle, dropIndexDdl, null, null);
        }
    };

    private final OpWrapper getTable = new OpWrapper("getTable") {
        @Override
        void execOp() {
            GetTableRequest req = new GetTableRequest()
                    .setTableName(tableName);
            handle.getTable(req);
        }
    };

    private final OpWrapper getIndexes = new OpWrapper("getIndexes") {
        @Override
        void execOp() {
            GetIndexesRequest req = new GetIndexesRequest()
                    .setTableName(tableName);
            handle.getIndexes(req);
        }
    };

    private final OpWrapper listTables = new OpWrapper("listTables") {
        @Override
        void execOp() {
            ListTablesRequest req = new ListTablesRequest()
                .setLimit(1);
            handle.listTables(req);
        }
    };

    private final OpWrapper put = new OpWrapper("put") {
        @Override
        void execOp() {
            PutRequest putReq = new PutRequest()
                    .setTableName(tableName)
                    .setValue(row);
            handle.put(putReq);
        }
    };

    private final OpWrapper delete = new OpWrapper("delete") {
        @Override
        void execOp() {
            DeleteRequest deleteReq = new DeleteRequest()
                    .setTableName(tableName)
                    .setKey(key);
            handle.delete(deleteReq);
        }
    };

    private final OpWrapper get = new OpWrapper("get") {
        @Override
        void execOp() {
            GetRequest req = new GetRequest()
                    .setTableName(tableName)
                    .setKey(key);
            handle.get(req);
        }
    };

    private final OpWrapper prepare = new OpWrapper("prepare") {
        @Override
        void execOp() {
            PrepareRequest req = new PrepareRequest().setStatement(selectStmt);
            handle.prepare(req);
        }
    };

    private final OpWrapper selectQuery = new OpWrapper("selectQuery") {
        @Override
        void execOp() {
            QueryRequest req = new QueryRequest().setStatement(selectStmt);
            handle.query(req);
        }
    };

    private final OpWrapper selectPrepQuery = new OpWrapper("selectPrepQuery") {
        @Override
        void execOp() {
            QueryRequest req = new QueryRequest()
                .setPreparedStatement(selectPrepStmt);
            handle.query(req);
        }
    };

    private final OpWrapper deleteQuery = new OpWrapper("deleteQuery") {
        @Override
        void execOp() {
            QueryRequest req = new QueryRequest().setStatement(deleteStmt);
            handle.query(req);
        }
    };

    private final OpWrapper deletePrepQuery = new OpWrapper("deletePrepQuery") {
        @Override
        void execOp() {
            QueryRequest req = new QueryRequest()
                .setPreparedStatement(deletePrepStmt);
            handle.query(req);
        }
    };

    private final OpWrapper insertQuery = new OpWrapper("insertQuery") {
        @Override
        void execOp() {
            QueryRequest req = new QueryRequest().setStatement(insertStmt);
            handle.query(req);
        }
    };

    private final OpWrapper insertPrepQuery = new OpWrapper("insertPrepQuery") {
        @Override
        void execOp() {
            QueryRequest req = new QueryRequest()
                .setPreparedStatement(insertPrepStmt);
            handle.query(req);
        }
    };

    private final OpWrapper updateQuery = new OpWrapper("updateQuery") {
        @Override
        void execOp() {
            QueryRequest req = new QueryRequest().setStatement(updateStmt);
            handle.query(req);
        }
    };

    private final OpWrapper updatePrepQuery = new OpWrapper("updatePrepQuery") {
        @Override
        void execOp() {
            QueryRequest req = new QueryRequest()
                .setPreparedStatement(updatePrepStmt);
            handle.query(req);
        }
    };

    private final OpWrapper writeMultiple = new OpWrapper("writeMultiple") {
        @Override
        void execOp() {
            WriteMultipleRequest req = new WriteMultipleRequest()
                .add(new PutRequest()
                        .setTableName(tableName)
                        .setValue(row),
                     false)
                .add(new DeleteRequest()
                        .setTableName(tableName)
                        .setKey(key),
                     false);
            handle.writeMultiple(req);
        }
    };

    private final OpWrapper getTableUsage = new OpWrapper("getTableUsage") {
        @Override
        void execOp() {
            TableUsageRequest req = new TableUsageRequest()
                    .setTableName(tableName)
                    .setLimit(1);
            handle.getTableUsage(req);
        }
    };

    private final OpWrapper[] ddlOps = new OpWrapper[] {
        createTable,
        alterTable,
        updateTableLimits,
        createIndex,
        dropIndex,
        dropTable
    };

    private final OpWrapper[] ddlOps_existing_table = new OpWrapper[] {
        alterTable,
        updateTableLimits,
        createIndex,
        dropIndex,
        dropTable
    };

    private final OpWrapper[] readOps = new OpWrapper[] {
        getTable,
        getIndexes,
        getTableUsage,
        listTables,
        get,
        prepare,
        selectQuery
    };

    private final OpWrapper[] writeOps = new OpWrapper[] {
        put,
        delete,
        insertQuery,
        deleteQuery,
        writeMultiple
    };

    @Override
    public void setUp() throws Exception {
        super.setUp();
        initTableAndPrepStmts();
    }

    private void initTableAndPrepStmts() {
        tableOperation(handle, createTableDDL, limits, 60000);
        tableOcid = getTableOcid(tableName);
        selectPrepStmt = prepare(selectStmt);
        insertPrepStmt = prepare(insertStmt);
        updatePrepStmt = prepare(updateStmt);
        deletePrepStmt = prepare(deleteStmt);
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
        Rule rule = addRule("block_ops", dropRequest, new String[] {"all"});
        blockOps(rule, ddlOps);
        blockOps(rule, readOps);
        blockOps(rule, writeOps);

        deleteRule("block_ops", false);

        /*
         * ddl and write operations should be blocked by the rule:
         *  {"name":"block_ops", "operations":["DDL", "WRITE"]}
         */
        rule = addRule("block_ops", returnError, new String[] {"ddl", "write"});
        blockOps(rule, createTable);
        blockOps(rule, writeOps);
        executeOps(readOps);
    }

    /*
     * Test filtering requests by the rule with principal tenant and/or user
     * information
     */
    @Test
    public void testPrincipalRule() {
        /* This test only runs with security enabled and minicloud */
        assumeTrue(useMiniCloud);

        /*
         * Add rule to block all operations from the user of specified tenant:
         *  {
         *    "name":"block_test_tenant",
         *    "tenant": tenantId,
         *    "operations": ["ALL"]
         *  }
         */
        Rule rule = addRule("block_test_tenant",
                            dropRequest,
                            tenantId,
                            null /* userId */,
                            null /* tableId */,
                            new String[] {"all"},
                            false);
        blockOps(rule, createTable, getTable, put, get);

        /* remove above rule "block_test_tenant", operations can proceed */
        deleteRule("block_test_tenant", false);
        reloadPersistentRules();
        executeOps(createTable, getTable, put, get);

        /*
         * Add rule to block ddl and write requests from the specified user.
         *  {
         *    "name":"block_test_user",
         *    "user": userId,
         *    "operations": ["DDL", "WRITE"]
         *  }
         */
        rule = addRule("block_test_user",
                       dropRequest,
                       null,
                       userId,
                       null,
                       new String[] {"ddl", "write"},
                       false);
        blockOps(rule, createTable, put);
        /* not block read operations. */
        executeOps(getTable, get);

        /* remove the above rule "block_test_user", operations can proceed */
        deleteRule("block_test_user", false);
        reloadPersistentRules();
        executeOps(createTable, getTable, put, get);
    }

    /*
     * Filter requests by the rule containing table ocid.
     */
    @Test
    public void testTableRule() {
        /*
         * Add rule to block all requests to the specified target table from
         * a user from the specified tenant:
         *  {
         *    "name":"block_table_xxx",
         *    "tenant": tenantId,
         *    "table": tableOcid,
         *    "operations": ["ALL"]
         *  }
         */
        String ruleName = "block_table_" + tableName;
        Rule rule = addRule(ruleName,
                            returnError,
                            (cloudRunning) ? tenantId : null,
                            null,
                            tableOcid,
                            new String[] {"all"},
                            false);
        blockOps(rule, ddlOps_existing_table);
        blockOps(rule, writeOps);
        deleteRule(ruleName, false);

        /*
         * Update rule to block write requests to the specified target table
         * from the specified user
         *  {
         *    "name":"block_table_xxx",
         *    "user": userId,
         *    "table": tableOcid,
         *    "operations": ["WRITE"]
         *  }
         */
        rule = addRule(ruleName,
                       returnError,
                       null,
                       (cloudRunning) ? userId : null,
                       tableOcid,
                       new String[] {"write"},
                       false);
        blockOps(rule, writeOps);
        executeOps(ddlOps);
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
         * Add rule to block write requests to the specified target table:
         *  {
         *    "name":"block_query",
         *    "table": tableOcid,
         *    "operations": ["WRITE"]
         *  }
         *
         * The insert/delete/update query should be blocked, and prepare and
         * select query should be executed successfully.
         */
        Rule rule = addRule("block_query",
                            dropRequest,
                            null,
                            null,
                            tableOcid,
                            new String[] {"write"},
                            false);
        blockOps(rule, insertQuery, deleteQuery, updateQuery);
        if (cloudRunning) {
            blockOps(rule, insertPrepQuery, deletePrepQuery, updatePrepQuery);
        }
        executeOps(prepare, selectQuery, selectPrepQuery);
        deleteRule("block_query", false);

        /*
         * Update rule to block read requests to the specified target table:
         *  {
         *    "name":"block_query",
         *    "table": tableOcid,
         *    "operations": ["READ"]
         *  }
         *
         * The prepare and select query should be blocked, and
         * insert/update/delete query should be executed successfully.
         */
        rule = addRule("block_query",
                       dropRequest,
                       null,
                       null,
                       tableOcid,
                       new String[] {"read"},
                       false);
        blockOps(rule, prepare, selectQuery, updateQuery, deleteQuery);
        if (cloudRunning) {
            blockOps(rule, selectPrepQuery, updatePrepQuery, deletePrepQuery);
        }
        executeOps(insertQuery, insertPrepQuery);
    }

    @Test
    public void testPersistentRule() {
        assumeTrue(useMiniCloud);
        /*
         * Add rule to block all operations from the user of specified tenant:
         *  {
         *    "name":"block_test_tenant",
         *    "tenant": tenantId,
         *    "operations": ["ALL"]
         *  }
         */
        Rule rule = addRule("block_test_tenant",
                            dropRequest,
                            tenantId,
                            null /* userId */,
                            null /* tableId */,
                            new String[] {"all"},
                            true);
        reloadPersistentRules();
        blockOps(rule, createTable, getTable, put, get);

        /* remove above rule "block_test_tenant", operations can proceed */
        deleteRule("block_test_tenant", true);
        reloadPersistentRules();
        executeOps(createTable, getTable, put, get);

        /*
         * Add rule to block ddl and write requests from the specified user.
         *  {
         *    "name":"block_test_user",
         *    "user": userId,
         *    "table": tableOcid
         *    "operations": ["DDL", "WRITE"]
         *  }
         */
        rule = addRule("block_test_user_table",
                       returnError,
                       null,
                       userId,
                       tableOcid,
                       new String[] {"ddl", "write"},
                       true);
        reloadPersistentRules();
        blockOps(rule, alterTable, put);
        /* not block read operations. */
        executeOps(getTable, get);

        /*
         * remove the above rule "block_test_user_table",
         * operations can proceed
         */
        deleteRule("block_test_user_table", true);
        reloadPersistentRules();
        executeOps(createTable, getTable, put, get);
    }

    @Override
    protected void perTestHandleConfig(NoSQLHandleConfig hconfig) {
        hconfig.configureDefaultRetryHandler(0, 0);
        hconfig.setRequestTimeout(REQUEST_WAIT_MS);
        hconfig.setTableRequestTimeout(REQUEST_WAIT_MS);
    }

    private TableResult tableOperation(NoSQLHandle handle,
                                       String ddl,
                                       TableLimits limits,
                                       String tableName) {
        TableRequest req = new TableRequest();
        if (ddl != null) {
            req.setStatement(ddl);
        }
        if (limits != null) {
            req.setTableLimits(limits);
        }
        if (tableName != null) {
            req.setTableName(tableName);
        }
        return handle.tableRequest(req);
    }

    private void executeOps(OpWrapper... ops) {
        for (OpWrapper op : ops) {
            op.exec();
        }
    }

    private void blockOps(Rule rule, OpWrapper... ops) {
        Class<?> expEx = getExpectedException(rule);
        for (OpWrapper op : ops) {
            op.exec(expEx);
        }
    }

    private Class<?> getExpectedException(Rule rule) {
        switch (rule.getActionType()) {
        case DROP_REQUEST:
            return RequestTimeoutException.class;
        case RETURN_ERROR:
            return returnErrorException;
        default:
            fail("Unexpected action type: " + rule.getAction());
        }
        return null;
    }

    private String getTableOcid(String tableName) {
        if (!cloudRunning) {
            return tableName;
        }

        String url = tmUrlBase + "tables/" + tableName +
                "?compartmentid=" + tenantId +
                "&tenantid=" + tenantId;
        HttpRequest httpRequest = new HttpRequest().disableRetry();
        HttpResponse response = httpRequest.doHttpGet(url);
        if (200 != response.getStatusCode()) {
            fail("getTble failed: " + response);
        }

        /* Extract tableOcid from response */
        String output = response.getOutput();
        String field = "\"ocid\":";
        int pos = output.indexOf(field);
        assertTrue(pos > 0);
        pos += field.length() + 1;
        assertTrue (pos < output.length());
        int to = output.indexOf("\"", pos);
        String tableOcid = output.substring(pos, to);
        return tableOcid.replace("_", ".");
    }

    private PreparedStatement prepare(String query) {
        PrepareRequest prep = new PrepareRequest().setStatement(query);
        PrepareResult prepRet = handle.prepare(prep);
        return prepRet.getPreparedStatement();
    }

    /**
     * Run a code snippet and expect a specified error.
     */
    private abstract class OpWrapper {

        private final String name;

        OpWrapper(String name) {
            this.name = name;
        }

        void exec() {
            exec(null);
        }

        void exec(Class<?> expectedException) {
            try {
                execOp();
                if (expectedException != null) {
                    fail("Expected " + expectedException.getSimpleName() +
                         " on operation: " + name);
                }
            } catch (Exception e) {
                if (expectedException == null) {
                    fail("Expected no exception, got " + e + ": " + name);
                } else if (!expectedException.isInstance(e)) {
                    fail("Expected " + expectedException.getSimpleName() +
                         " but got " + e + ": " + name);
                }
            }
        }

        abstract void execOp() throws Exception;
    }
}
