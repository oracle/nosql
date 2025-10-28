/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.nosql.model.PreparedStatement;
import com.oracle.bmc.nosql.model.QueryDetails;
import com.oracle.bmc.nosql.model.QueryResultCollection;
import com.oracle.bmc.nosql.model.RequestUsage;
import com.oracle.bmc.nosql.model.StatementSummary;
import com.oracle.bmc.nosql.model.TableLimits;
import com.oracle.bmc.nosql.model.StatementSummary.Operation;
import com.oracle.bmc.nosql.requests.PrepareStatementRequest;
import com.oracle.bmc.nosql.requests.QueryRequest;
import com.oracle.bmc.nosql.requests.SummarizeStatementRequest;
import com.oracle.bmc.nosql.responses.PrepareStatementResponse;
import com.oracle.bmc.nosql.responses.QueryResponse;
import com.oracle.bmc.nosql.responses.SummarizeStatementResponse;

/**
 * Test query APIs:
 *  o prepare
 *  o query
 *  o summarize
 */
public class QueryTest extends RestAPITestBase {

    @Test
    public void testQuery() {
        final String tableName = "foo";
        createTestTable(tableName);

        final int numRows = 31;
        for (int i = 0; i < numRows; i++) {
            Map<String, Object> value = createValue(i);
            putRow(tableName, value);
        }

        String query = "select * from foo";

        PrepareStatementRequest prepReq = PrepareStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .build();
        PrepareStatementResponse prepRes = client.prepareStatement(prepReq);
        assertNotNull(prepRes.getPreparedStatement());
        String prepStmt = prepRes.getPreparedStatement().getStatement();
        assertNotNull(prepStmt);

        /* Query with prepred statment */
        QueryDetails infoPrep = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(prepStmt)
                .isPrepared(true)
                .build();

        /* Query */
        QueryDetails info = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .build();

        /* Run query */
        int totalCost1 = runQuery(info, numRows, this::validateSimpleRow);
        int totalCost2 = runQuery(infoPrep, numRows, this::validateSimpleRow);
        assertEquals(totalCost1, totalCost2 + 2);

        /* Run query with limit */
        int limit = 10;
        totalCost1 = runQueryWithLimit(info, limit, numRows,
                                       this::validateSimpleRow);
        totalCost2 = runQueryWithLimit(infoPrep, limit, numRows,
                                       this::validateSimpleRow);
        int batches = (numRows + (limit - 1))/ limit;
        assertEquals(totalCost1, totalCost2 + batches * 2);

        /* Run query with maxReadKB */
        int maxReadKB = 15;
        info = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .maxReadInKBs(maxReadKB)
                .build();

        infoPrep = QueryDetails.builder()
                    .compartmentId(getCompartmentId())
                    .statement(prepStmt)
                    .isPrepared(true)
                    .maxReadInKBs(maxReadKB)
                    .build();

        totalCost1 = runQueryWithMaxReadKB(info, maxReadKB, numRows,
                                           this::validateSimpleRow);
        totalCost2 = runQueryWithMaxReadKB(infoPrep, maxReadKB, numRows,
                                           this::validateSimpleRow);
        assertTrue(totalCost1 > totalCost2);

        /* 0 row returned */
        query = "select * from foo where id = -1";
        info = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .build();
        runQuery(info, 0, null);
    }

    @Test
    public void testQueryBadRequest() {
        QueryDetails info;
        QueryRequest req;

        String query = "select * from foo";
        info = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .build();

        /* Invalid limit, it should not be negative value */
        req = QueryRequest.builder()
                .limit(-1)
                .queryDetails(info)
                .build();
        runQueryFail(req, 400 /* bad request */);

        /* Invalid page: page should not be empty or contain white space only */
        req = QueryRequest.builder()
                .page("")
                .queryDetails(info)
                .build();
        runQueryFail(req, 400 /* bad request */);

        /* Invalid page, Cannot deserialize value of type `byte[]` */
        req = QueryRequest.builder()
                .page("invalid")
                .queryDetails(info)
                .build();
        runQueryFail(req, 400 /* bad request */);

        /* Invalid compartmentId: compartmentId should not be null */
        info = QueryDetails.builder()
                .statement(query)
                .build();
        req = QueryRequest.builder()
                .queryDetails(info)
                .build();
        runQueryFail(req, 400 /* bad request */);

        /*
         * Invalid compartmentId: compartmentId should not be empty or contain
         * white space only
         */
        info = QueryDetails.builder()
                .compartmentId(" ")
                .statement(query)
                .build();
        req = QueryRequest.builder()
                .queryDetails(info)
                .build();
        runQueryFail(req, 400 /* bad request */);

        /*
         * Invalid statement: statement should not be null
         */
        info = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .build();
        req = QueryRequest.builder()
                .queryDetails(info)
                .build();
        runQueryFail(req, 400 /* bad request */);

        /*
         * Invalid statement: statement should not be empty or contain white
         * space only
         */
        info = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(" ")
                .build();
        req = QueryRequest.builder()
                .queryDetails(info)
                .build();
        runQueryFail(req, 400 /* bad request */);

        /*
         * Invalid maxReadInKBs, it should not be negative value: -1
         */
        info = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .maxReadInKBs(-1)
                .build();
        req = QueryRequest.builder()
                .queryDetails(info)
                .build();
        runQueryFail(req, 400 /* bad request */);

        /*
         * Invalid timeoutInMs, it should not be negative value: -1
         */
        info = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .timeoutInMs(-1)
                .build();
        req = QueryRequest.builder()
                .queryDetails(info)
                .build();
        runQueryFail(req, 400 /* bad request */);

        /*
         * Table not found
         */
        info = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .build();
        req = QueryRequest.builder()
                .queryDetails(info)
                .build();
        runQueryFail(req, 404 /* Table not found */);

        /*
         * Invalid serialized prepared statement
         */
        info = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .isPrepared(true)
                .build();
        req = QueryRequest.builder()
                .queryDetails(info)
                .build();
        runQueryFail(req, 400 /* bad request */);

        /* Complex query is not supported */
        createTestTable("foo");
        query = "select * from foo order by name";
        info = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .build();
        req = QueryRequest.builder()
                .queryDetails(info)
                .build();
        runQueryFail(req, 400);
    }

    private void runQueryFail(QueryRequest req, int expCode) {
        try {
            client.query(req);
            fail("expect to fail but not");
        } catch (BmcException ex) {
            assertEquals(expCode , ex.getStatusCode());
        }
    }

    /* TODO: fix below commented type, add test for complex types */
    @Test
    public void testBindVariables() {
        String tableName = "foo";
        String ddl = "create table if not exists " + tableName + "(" +
                "id integer, s String, i integer, l long, f float, d double, " +
                "bl boolean, n number, t timestamp(3), bi binary, " +
                "primary key(id))";

        /* Create table */
        createTable(tableName, ddl);

        /* Put single row */
        int id = 1;
        String s = "abc";
        int i = Integer.MIN_VALUE;
        long l = Long.MIN_VALUE;
        float f = Float.MIN_VALUE;
        double d = Double.MIN_VALUE;
        boolean bl = false;
        //BigDecimal n = BigDecimal.valueOf(Long.MAX_VALUE, 10);
        long n = Long.MAX_VALUE;
        String dt = "2019-08-20T12:12:39.123Z";
        String bi = "AAECAw==";
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("id", id);
        row.put("s", s);
        row.put("i", i);
        row.put("l", l);
        row.put("f", f);
        row.put("d", d);
        row.put("bl", bl);
        row.put("n", n);
        row.put("t", dt);
        row.put("bi", bi);
        putRow(tableName, row);

        PrepareStatementRequest prepReq;
        PrepareStatementResponse prepRes;
        QueryDetails qinfo;
        Map<String, Object> variables = new HashMap<String, Object>();

        String query = "declare $i integer; $l long; $f float; $d double; " +
                       "$bl boolean; $n number;" +
                       "select * from " + tableName +
                       " where i = $i and l = $l and f = $f and d = $d" +
                       " and bl = $bl and n = $n";

        prepReq = PrepareStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .build();
        prepRes = client.prepareStatement(prepReq);
        String prepStmt = prepRes.getPreparedStatement().getStatement();
        assertNotNull(prepStmt);

        variables.clear();
        variables.put("$i", i);
        variables.put("$l", l);
        variables.put("$f", f);
        variables.put("$d", d);
        variables.put("$bl", bl);
        variables.put("$n", n);
        //variables.put("$t", dt);
        //variables.put("$bi", bi);
        qinfo = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(prepStmt)
                .variables(variables)
                .isPrepared(true)
                .build();
        runQuery(qinfo, 1,
                 new RowValidator() {
                    @Override
                    public void check(Map<String, Object> value) {
                        assertEquals(row.size(), value.size());
                        for (String key : row.keySet()) {
                            Object exp = row.get(key);
                            Object val = value.get(key);

                            if (key.equals("f")) {
                                assertTrue(val instanceof Double);
                                assertTrue(Float.compare(((Float)exp),
                                              ((Double)val).floatValue()) == 0);
                            } else {
                                assertEquals(exp, val);
                            }
                        }
                    }
                });
    }

    @Test
    public void testPrepare() {
        final String tableName = "foo";
        createTestTable(tableName);

        PrepareStatementRequest req;
        PrepareStatementResponse res;
        PreparedStatement pstmt;

        String query = "select * from foo";
        req = PrepareStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .build();
        res = client.prepareStatement(req);
        pstmt = res.getPreparedStatement();
        assertNotNull(pstmt);
        assertNotNull(pstmt.getStatement());
        assertNull(pstmt.getQueryPlan());
        RequestUsage usage = pstmt.getUsage();
        assertNotNull(usage);
        assertTrue(usage.getReadUnitsConsumed() == 2);
        assertTrue(usage.getWriteUnitsConsumed() == 0);

        query = "select * from foo where id > 10 and contains(name, \"abc\")";
        req = PrepareStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .isGetQueryPlan(true)
                .build();
        res = client.prepareStatement(req);
        pstmt = res.getPreparedStatement();
        assertNotNull(pstmt.getQueryPlan());
    }

    @Test
    public void testPrepareBadRequest() {

        PrepareStatementRequest req;

        /*
         * CompartmentId should not be null or empty
         */
        req = PrepareStatementRequest.builder()
                .compartmentId("")
                .statement("select * from foo")
                .build();
        runPrepareStatementFail(req, 400 /* bad request */);

        /*
         * Statement should not be empty
         */
        req = PrepareStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement("")
                .build();
        runPrepareStatementFail(req, 400 /* bad request */);

        /*
         * Table not found
         */
        req = PrepareStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement("select * from notExists")
                .build();
        runPrepareStatementFail(req, 404 /* Table not found */);

        /*
         * The query and prepare methods can not be used for DDL statements
         */
        req = PrepareStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement("create table foo(id integer, name string, " +
                            "primary key(id))")
                .build();
        runPrepareStatementFail(req, 400 /* bad request */);

        /*
         * Unsupported query: queries with order by expressions
         */
        createTestTable("test");
        req = PrepareStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement("select * from test order by id")
                .build();
        runPrepareStatementFail(req, 400 /* bad request */);
    }

    private void runPrepareStatementFail(PrepareStatementRequest req,
                                         int expCode) {
        try {
            client.prepareStatement(req);
            fail("expect to fail but not");
        } catch (BmcException ex) {
            assertEquals(expCode , ex.getStatusCode());
            checkErrorMessage(ex);
        }
    }

    @Test
    public void testSummarize() {
        String tableName = "foo";
        String tableDdl = "create table if not exists foo (" +
                "id integer, name string, primary key(id))";

        SummarizeStatementRequest req;
        SummarizeStatementResponse res;
        StatementSummary info;

        /* Create table */
        req = SummarizeStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement(tableDdl)
                .build();
        res = client.summarizeStatement(req);
        assertNotNull(res.getStatementSummary());
        info = res.getStatementSummary();
        assertEquals(Operation.CreateTable, info.getOperation());
        assertEquals(tableName, info.getTableName());
        assertNull(info.getIndexName());
        assertTrue(info.getIsIfNotExists());
        assertNull(info.getIsIfExists());
        assertNull(info.getSyntaxError());

        /* Create index */
        String ddl = "create index idxName on foo(name)";
        req = SummarizeStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement(ddl)
                .build();
        res = client.summarizeStatement(req);
        assertNotNull(res.getStatementSummary());
        info = res.getStatementSummary();
        assertEquals(Operation.CreateIndex, info.getOperation());
        assertEquals(tableName, info.getTableName());
        assertEquals("idxName", info.getIndexName());
        assertFalse(info.getIsIfNotExists());
        assertNull(info.getIsIfExists());
        assertNull(info.getSyntaxError());

        /* Drop table */
        ddl = "drop table if exists foo";
        req = SummarizeStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement(ddl)
                .build();
        res = client.summarizeStatement(req);
        assertNotNull(res.getStatementSummary());
        info = res.getStatementSummary();
        assertEquals(Operation.DropTable, info.getOperation());
        assertEquals(tableName, info.getTableName());
        assertNull(info.getIndexName());
        assertTrue(info.getIsIfExists());
        assertNull(info.getIsIfNotExists());
        assertNull(info.getSyntaxError());

        /* Drop index */
        ddl = "drop index idxName on foo";
        req = SummarizeStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement(ddl)
                .build();
        res = client.summarizeStatement(req);
        assertNotNull(res.getStatementSummary());
        info = res.getStatementSummary();
        assertEquals(Operation.DropIndex, info.getOperation());
        assertEquals(tableName, info.getTableName());
        assertEquals("idxName", info.getIndexName());
        assertFalse(info.getIsIfExists());
        assertNull(info.getIsIfNotExists());
        assertNull(info.getSyntaxError());

        /*
         * Create the table before alter table if run locally, this is to
         * satisfy LocalTenantManager.createPrepareCB() that provides
         * TableMetadataHelper to check the existence of the table.
         */
        if (!cloudRunning) {
            createTable(tableName, tableDdl);
        }

        /* Alter table */
        ddl = "alter table foo (add address string)";
        req = SummarizeStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement(ddl)
                .build();
        res = client.summarizeStatement(req);
        assertNotNull(res.getStatementSummary());
        info = res.getStatementSummary();
        assertEquals(Operation.AlterTable, info.getOperation());
        assertEquals(tableName, info.getTableName());
        assertNull(info.getIndexName());
        assertNull(info.getIsIfNotExists());
        assertNull(info.getIsIfExists());
        assertNull(info.getSyntaxError());

        /* Query */
        String query = "select * from foo where id = 1";
        req = SummarizeStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .build();
        res = client.summarizeStatement(req);
        assertNotNull(res.getStatementSummary());
        info = res.getStatementSummary();
        assertEquals(Operation.Select, info.getOperation());
        assertEquals(tableName, info.getTableName());
        assertNull(info.getIndexName());
        assertNull(info.getIsIfNotExists());
        assertNull(info.getIsIfExists());
        assertNull(info.getSyntaxError());

        /* Insert */
        query = "insert into foo values(1, 'test')";
        req = SummarizeStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .build();
        res = client.summarizeStatement(req);
        assertNotNull(res.getStatementSummary());
        info = res.getStatementSummary();
        assertEquals(Operation.Insert, info.getOperation());
        assertEquals(tableName, info.getTableName());
        assertNull(info.getIndexName());
        assertNull(info.getIsIfNotExists());
        assertNull(info.getIsIfExists());
        assertNull(info.getSyntaxError());

        /* Update */
        query = "update foo set name = 'test' where id = 1";
        req = SummarizeStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .build();
        res = client.summarizeStatement(req);
        assertNotNull(res.getStatementSummary());
        info = res.getStatementSummary();
        assertEquals(Operation.Update, info.getOperation());
        assertEquals(tableName, info.getTableName());
        assertNull(info.getIndexName());
        assertNull(info.getIsIfNotExists());
        assertNull(info.getIsIfExists());
        assertNull(info.getSyntaxError());

        /* Delete */
        query = "delete from foo where id = 1";
        req = SummarizeStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .build();
        res = client.summarizeStatement(req);
        assertNotNull(res.getStatementSummary());
        info = res.getStatementSummary();
        assertEquals(Operation.Delete, info.getOperation());
        assertEquals(tableName, info.getTableName());
        assertNull(info.getIndexName());
        assertNull(info.getIsIfNotExists());
        assertNull(info.getIsIfExists());
        assertNull(info.getSyntaxError());

        /* Syntax error */
        query = "create table foo (id integer, name string)";
        req = SummarizeStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .build();
        res = client.summarizeStatement(req);
        assertNotNull(res.getStatementSummary());
        info = res.getStatementSummary();
        assertNotNull(info.getSyntaxError());
    }

    @Test
    public void testSummarizeBadRequest() {

        SummarizeStatementRequest req;

        /*
         * CompartmentId should not be null or empty
         */
        req = SummarizeStatementRequest.builder()
                .compartmentId("")
                .statement("select * from foo")
                .build();
        runSummarizeStatementFail(req, 400);

        /*
         * Statement should not be empty
         */
        req = SummarizeStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement("")
                .build();
        runSummarizeStatementFail(req, 400);
    }

    @Test
    public void testQueryTableNameMapping()
        throws Exception {

        /*
         * Run this test for minicloud only
         *
         * This test directly calls SC API to create table to test proxy cache,
         * it can only be run in minicloud.
         */
        assumeTrue("Skipping testQueryTableNameMapping() if not minicloud test",
                   useMiniCloud);

        String tableName = "testQueryTableNameMapping";
        String ddl = "create table " + tableName + "(" +
                     "id integer, name String, age integer, " +
                     "primary key(id))";
        String ddl2 = "create table " + tableName + "(" +
                     "id1 integer, name String, age integer, " +
                     "primary key(id1))";

        /* drop non-existing table */
        dropTable(tableName);

        /* re-create table */
        scRecreateTable(getTenantId(), getCompartmentId(), tableName, ddl);

        String query = "select * from " + tableName + " where id = '1'";
        String query2 = "select * from " + tableName + " where id1 = '1'";

        /* Prepared query to cache mapping */
        PrepareStatementRequest prepReq = PrepareStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .build();
        PrepareStatementResponse prepRes = client.prepareStatement(prepReq);
        assertNotNull(prepRes.getPreparedStatement());
        String prepStmt = prepRes.getPreparedStatement().getStatement();
        assertNotNull(prepStmt);

        QueryDetails infoPrep = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(prepStmt)
                .isPrepared(true)
                .build();

        /* Run query */
        QueryRequest qryReq = QueryRequest.builder()
                .queryDetails(infoPrep)
                .build();
        client.query(qryReq);
        client.query(qryReq);

        /* re-create table */
        scRecreateTable(getTenantId(), getCompartmentId(), tableName, ddl2);

        prepReq = PrepareStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement(query2)
                .build();
        prepRes = client.prepareStatement(prepReq);
        assertNotNull(prepRes.getPreparedStatement());
        prepStmt = prepRes.getPreparedStatement().getStatement();
        assertNotNull(prepStmt);

        /* Query with prepred statment */
        infoPrep = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(prepStmt)
                .isPrepared(true)
                .build();

        /* Run query */
        qryReq = QueryRequest.builder()
                .queryDetails(infoPrep)
                .build();
        client.query(qryReq);

        /* Query to cache mapping */
        QueryDetails info = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(query2)
                .build();
        qryReq = QueryRequest.builder()
                .queryDetails(info)
                .build();
        client.query(qryReq);
        client.query(qryReq);

        /* re-create table */
        scRecreateTable(getTenantId(), getCompartmentId(), tableName, ddl);

        /* Query */
        info = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .build();
        qryReq = QueryRequest.builder()
                .queryDetails(info)
                .build();
        client.query(qryReq);
        client.query(qryReq);

        /* re-create table */
        scRecreateTable(getTenantId(), getCompartmentId(), tableName, ddl2);

        info = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(query2)
                .build();
        qryReq = QueryRequest.builder()
                .queryDetails(info)
                .build();
        client.query(qryReq);

        /* Summarize to cache mapping */
        SummarizeStatementRequest req = SummarizeStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .build();
        SummarizeStatementResponse res = client.summarizeStatement(req);
        assertNotNull(res.getStatementSummary());

        /* re-create table */
        scRecreateTable(getTenantId(), getCompartmentId(), tableName, ddl);

        req = SummarizeStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .build();
        res = client.summarizeStatement(req);
        assertNotNull(res.getStatementSummary());
    }

    @Test
    public void testQueryChildTable() {
        final String createTDdl =
            "create table t(id integer, s string, primary key(id))";
        final String createTADdl =
            "create table t.a(ida integer, s string, primary key(ida))";
        final String createTABDdl =
            "create table t.a.b(idb integer, s string, primary key(idb))";
        final TableLimits limits = TableLimits.builder()
                                       .maxReadUnits(1000)
                                       .maxWriteUnits(200)
                                       .maxStorageInGBs(1)
                                       .build();

        createTable("t", createTDdl, limits);
        createTable("t.a", createTADdl, null /* limits */);
        createTable("t.a.b", createTABDdl, null /* limits */);

        int numId = 3;
        int numIdaPerId = 3;
        int numIdbPerIda = 3;
        for (int i = 0; i < numId; i++) {
            putRow("t", makeTRow(i));
            for (int j = 0; j < numIdaPerId; j++) {
                putRow("t.a", makeTARow(i, j));
                for (int k = 0; k < numIdbPerIda; k++) {
                    putRow("t.a.b", makeTABRow(i, j, k));
                }
            }
        }

        String stmt;
        QueryDetails qryInfo;
        int maxReadKB;

        stmt = "select * from t.a";
        qryInfo = QueryDetails.builder()
                    .compartmentId(getCompartmentId())
                    .statement(stmt)
                    .build();
        runQuery(qryInfo, 9, null);

        stmt = "select * from t.a.b";
        qryInfo = QueryDetails.builder()
                    .compartmentId(getCompartmentId())
                    .statement(stmt)
                    .build();
        runQuery(qryInfo, 27, null);
        runQueryWithLimit(qryInfo, 10, 27, null);

        maxReadKB = 15;
        qryInfo = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(stmt)
                .maxReadInKBs(maxReadKB)
                .build();
        runQueryWithMaxReadKB(qryInfo, maxReadKB, 27, null);

        stmt = "select * from nested tables(t descendants(t.a a, t.a.b b))";
        qryInfo = QueryDetails.builder()
                    .compartmentId(getCompartmentId())
                    .statement(stmt)
                    .build();
        runQuery(qryInfo, 27, null);
        runQueryWithLimit(qryInfo, 10, 27, null);

        maxReadKB = 15;
        qryInfo = QueryDetails.builder()
                    .compartmentId(getCompartmentId())
                    .statement(stmt)
                    .maxReadInKBs(maxReadKB)
                    .build();
        runQueryWithMaxReadKB(qryInfo, maxReadKB, 27, null);
    }

    private Map<String, Object> makeTRow(int id) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("s", "s" + id);
        return row;
    }

    private Map<String, Object> makeTARow(int id, int ida) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("ida", ida);
        row.put("s", "s" + id + "_" + ida);
        return row;
    }

    private Map<String, Object> makeTABRow(int id, int ida, int idb) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("ida", ida);
        row.put("idb", idb);
        row.put("s", "s" + id + "_" + ida + "_" + idb);
        return row;
    }

    private void runSummarizeStatementFail(SummarizeStatementRequest req,
                                           int expCode) {
        try {
            client.summarizeStatement(req);
            fail("expect to fail but not");
        } catch (BmcException ex) {
            assertEquals(expCode , ex.getStatusCode());
            checkErrorMessage(ex);
        }
    }

    private int runQuery(QueryDetails info,
                         int expCount,
                         RowValidator validator) {
        return runQueryWithLimit(info, 0, expCount, validator);
    }

    private int runQueryWithLimit(QueryDetails info,
                                  int limit,
                                  int expCount,
                                  RowValidator rowValidator) {
        int total = 0;
        QueryResponse res = null;
        QueryRequest qryReq;
        String nextPage = null;
        int count = 0;
        int totalKB = 0;
        do {
            qryReq = QueryRequest.builder()
                    .queryDetails(info)
                    .page(nextPage)
                    .limit(limit)
                    .build();

            res = client.query(qryReq);

            QueryResultCollection qrc = res.getQueryResultCollection();
            if (rowValidator != null) {
                for (Map<String, Object> e : qrc.getItems()) {
                    rowValidator.check(e);
                }
            }
            count = qrc.getItems().size();
            total += count;
            totalKB += qrc.getUsage().getReadUnitsConsumed();

            if (count > 0) {
                assertTrue(qrc.getUsage().getReadUnitsConsumed() > 0);
            }
            assertTrue(qrc.getUsage().getWriteUnitsConsumed() == 0);

            nextPage = res.getOpcNextPage();
            if (limit > 0) {
                if (nextPage != null) {
                    assertEquals(count, limit);
                } else {
                    assertTrue(count <= limit);
                }
            }
        } while (nextPage!= null);

        assertEquals(expCount, total);
        return totalKB;
    }

    private int runQueryWithMaxReadKB(QueryDetails info,
                                      int maxReadKB,
                                      int expCount,
                                      RowValidator rowValidator) {
        final int prepCost = 2;
        boolean isPrepared = info.getIsPrepared() != null && info.getIsPrepared();

        int total = 0;
        QueryResponse res = null;
        QueryRequest qryReq;
        String nextPage = null;

        int readKB = 0;
        int totalKB = 0;
        do {
            qryReq = QueryRequest.builder()
                    .queryDetails(info)
                    .page(nextPage)
                    .build();
            res = client.query(qryReq);

            QueryResultCollection qrc = res.getQueryResultCollection();
            if (rowValidator != null) {
                for (Map<String, Object> e : qrc.getItems()) {
                    rowValidator.check(e);
                }
            }

            total += qrc.getItems().size();
            readKB = qrc.getUsage().getReadUnitsConsumed();
            totalKB += readKB;

            if (maxReadKB > 0) {
                if (isPrepared) {
                    assertTrue(readKB <= maxReadKB + 2);
                } else {
                    assertTrue(readKB - prepCost <= maxReadKB + 2);
                }
            }
            assertTrue(qrc.getUsage().getWriteUnitsConsumed() == 0);

            nextPage = res.getOpcNextPage();
        } while (nextPage!= null);

        assertEquals(expCount, total);
        return totalKB;
    }

    private void createTestTable(String tableName) {
        String ddl = "create table if not exists " + tableName + "(" +
                        "id integer, name String, age integer, " +
                        "primary key(id))";

        TableLimits limits = TableLimits.builder()
                .maxReadUnits(2000)
                .maxWriteUnits(1000)
                .maxStorageInGBs(1)
                .build();

        createTable(tableName, ddl, limits);
    }

    private Map<String, Object> createValue(int i) {
        Map<String, Object> value = new HashMap<String, Object>();
        value.put("id", i);
        value.put("name", "name" + i);
        value.put("age", 20 + i % 40);
        return value;
    }

    private void validateSimpleRow(Map<String, Object> value) {
        assertNotNull(value.get("id"));
        Integer id = (Integer)value.get("id");
        Map<String, Object> exp = createValue(id.intValue());
        assertEquals(exp, value);
    }

    @FunctionalInterface
    private interface RowValidator {
        void check(Map<String, Object> value);
    }
}
