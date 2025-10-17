/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.nosql.model.PreparedStatement;
import com.oracle.bmc.nosql.model.QueryDetails;
import com.oracle.bmc.nosql.model.TableLimits;
import com.oracle.bmc.nosql.model.TableLimits.CapacityMode;
import com.oracle.bmc.nosql.model.UpdateRowDetails;
import com.oracle.bmc.nosql.requests.CreateIndexRequest;
import com.oracle.bmc.nosql.requests.CreateTableRequest;
import com.oracle.bmc.nosql.requests.GetRowRequest;
import com.oracle.bmc.nosql.requests.GetWorkRequestRequest;
import com.oracle.bmc.nosql.requests.ListWorkRequestsRequest;
import com.oracle.bmc.nosql.requests.PrepareStatementRequest;
import com.oracle.bmc.nosql.requests.QueryRequest;
import com.oracle.bmc.nosql.requests.UpdateRowRequest;
import com.oracle.bmc.nosql.requests.UpdateTableRequest;
import com.oracle.bmc.nosql.responses.GetRowResponse;
import com.oracle.bmc.nosql.responses.PrepareStatementResponse;
import com.oracle.bmc.nosql.responses.QueryResponse;
import com.oracle.bmc.nosql.responses.UpdateRowResponse;
import com.oracle.bmc.retrier.DefaultRetryCondition;
import com.oracle.bmc.retrier.RetryConfiguration;

import oracle.nosql.proxy.ProxyTestBase;

import org.junit.Test;

/**
 * Throttling and limits test:
 *   o Read/write throttling
 *   o Size limit
 *      1. Key/value size
 *      2. Query size
 *      3. Request size
 *   o Ddl limits
 *      1. number of columns
 *      2. number of tables
 *      3. number of indexes
 *      4. number of evolution
 *      5. table provisioning limits
 *      6. operation limits (TODO)
 */
public class ThrottleLimitTest extends RestAPITestBase {

    /* Test read/write throttling */
    @Test
    public void testReadWriteThrottling() throws Exception {

        final RetryConfiguration retryConfiguration =
                RetryConfiguration.builder()
                    .retryCondition(new TestRetryCondition())
                    .build();

        /* Create a table with small throughput */
        final String tableName = "testThrottle";
        String ddl = "create table if not exists testThrottle (" +
                         "id integer, name String, age integer, " +
                         "primary key(id))";

        TableLimits limits = TableLimits.builder()
                .maxReadUnits(1)
                .maxWriteUnits(1)
                .maxStorageInGBs(50)
                .build();

        createTable(tableName, ddl, limits);

        /* PUT */
        Map<String, Object> value = new HashMap<String, Object>();
        value.put("id", 1);
        value.put("name", "jack");
        value.put("age", 21);
        UpdateRowDetails row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .value(value)
                .build();
        UpdateRowRequest putReq = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .retryConfiguration(retryConfiguration)
                .updateRowDetails(row)
                .build();
        int num = 0;
        try {
            while (true) {
                client.updateRow(putReq);
                num++;
                if (num > 1000) {
                    fail("Throttling exception should have been thrown");
                }
            }
        } catch (BmcException ex) {
            /* success */
            assertError(ex, 429, "TooManyRequests");
            checkErrorMessage(ex);
        }

        /* GET */
        List<String> key = new ArrayList<String>();
        key.add("id:1");
        num = 0;
        try {
            while (true) {
                GetRowRequest getReq = GetRowRequest.builder()
                    .compartmentId(getCompartmentId())
                    .tableNameOrId(tableName)
                    .retryConfiguration(retryConfiguration)
                    .key(key)
                    .build();
                GetRowResponse getRes = client.getRow(getReq);
                assertNotNull(getRes.getRow());
                num++;
                if (num > 1000) {
                    fail("Throttling exception should have been thrown");
                }
            }
        } catch (BmcException ex) {
            /* success */
            assertError(ex, 429, "TooManyRequests");
        }

        Thread.sleep(2000); /* try to avoid previous throttling */

        /* Query based on single partition scanning */
        String query = "select * from testThrottle where id = 1";
        PrepareStatementRequest prepReq =
            PrepareStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .build();
        PrepareStatementResponse prepRes = client.prepareStatement(prepReq);
        assertTrue("Prepare statement failed",
                   prepRes.getPreparedStatement() != null);

        /* Query with size limit */
        QueryDetails queryDetails = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(prepRes.getPreparedStatement().getStatement())
                .isPrepared(true)
                .maxReadInKBs(3)
                .build();
        QueryRequest queryReq = QueryRequest.builder()
                .queryDetails(queryDetails)
                .retryConfiguration(retryConfiguration)
                .build();
        num = 0;
        try {
            while (true) {
                /* Query */
                QueryResponse queryRes = client.query(queryReq);
                assertEquals(1, queryRes.getQueryResultCollection()
                                        .getItems().size());
                num++;
                if (num > 1000) {
                    fail("Throttling exception should have been thrown");
                }
            }
        } catch (BmcException ex) {
            /* success */
            assertError(ex, 429, "TooManyRequests");
            checkErrorMessage(ex);
        }

        /* Alter table limit to increase write limit */
        limits = TableLimits.builder()
                .maxReadUnits(1)
                .maxWriteUnits(200)
                .maxStorageInGBs(50)
                .build();
        updateTable(tableName, limits);

        /* Put 200 rows */
        UpdateRowResponse putRes;
        for (int i = 0; i < 200; i++) {
            value.put("id", 100 + i);
            row = UpdateRowDetails.builder()
                    .compartmentId(getCompartmentId())
                    .value(value)
                    .build();
            putReq = UpdateRowRequest.builder()
                    .updateRowDetails(row)
                    .tableNameOrId(tableName)
                    .build();
            putRes = client.updateRow(putReq);
            assertNotNull(putRes.getUpdateRowResult().getVersion());
        }

        /* Query based on all partitions scanning */
        Thread.sleep(2000); /* try to avoid previous throttling */
        query = "select * from testThrottle";
        prepReq = PrepareStatementRequest.builder()
                    .statement(query)
                    .compartmentId(getCompartmentId())
                    .build();
        prepRes = client.prepareStatement(prepReq);
        assertTrue("Prepare statement failed",
                   prepRes.getPreparedStatement() != null);

        queryDetails = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(prepRes.getPreparedStatement().getStatement())
                .isPrepared(true)
                .maxReadInKBs(20)
                .build();
        queryReq = QueryRequest.builder()
                .queryDetails(queryDetails)
                .retryConfiguration(retryConfiguration)
                .build();
        QueryResponse queryRes;
        num = 0;
        try {
            while (true) {
                queryRes = client.query(queryReq);
                num++;
                if (queryRes.getOpcNextPage() == null) {
                    fail("Throttling exception should have been thrown");
                    break;
                }
                queryReq = QueryRequest.builder()
                            .queryDetails(queryDetails)
                            .page(queryRes.getOpcNextPage())
                            .build();
            }
        } catch (BmcException ex) {
            /* success */
            assertError(ex, 429, "TooManyRequests");
            checkErrorMessage(ex);
        }
        assertTrue(num > 0);

        /* Query without limits */
        Thread.sleep(1000);
        num = 0;
        queryDetails = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(prepRes.getPreparedStatement().getStatement())
                .isPrepared(true)
                .build();
        queryReq = QueryRequest.builder()
                .queryDetails(queryDetails)
                .retryConfiguration(retryConfiguration)
                .build();
        try {
            while (true) {
                /* Query */
                queryRes = client.query(queryReq);
                assertTrue(queryRes.getQueryResultCollection()
                               .getItems().size() > 0);
                num++;
                if (num > 1000) {
                    fail("Throttling exception should have been thrown");
                }
            }
        } catch (BmcException ex) {
            /* success */
            assertError(ex, 429, "TooManyRequests");
            checkErrorMessage(ex);
        }
    }

    @Test
    public void testKeyValueSizeLimit() {
        final int keySizeLimit = 64;
        final int valueSizeLimit = 512 * 1024;

        final String tableName = "foo";
        final String ddl = "create table if not exists " + tableName + "(" +
                            "id string, name String, " +
                            "primary key(id))";
        createTable(tableName, ddl);

        Map<String, Object> row = new HashMap<>();
        row.put("id", genString(keySizeLimit));
        row.put("name", genString(valueSizeLimit - 5));/* 5 bytes for overhead */
        putRow("foo", row);

        row.put("id", genString(keySizeLimit + 1));
        try {
            putRow("foo", row);
            fail("Expect to get 400(KeySizeLimitExceeded) but not");
        } catch (BmcException ex) {
            assertError(ex, 400, "KeySizeLimitExceeded");
            checkErrorMessage(ex);
        }

        row.put("id", genString(1));
        row.put("name", genString(valueSizeLimit - 4));
        try {
            putRow("foo", row);
            fail("Expect to get 400(RowSizeLimitExceeded) but not");
        } catch (BmcException ex) {
            assertError(ex, 400, "RowSizeLimitExceeded");
            checkErrorMessage(ex);
        }
    }

    @Test
    public void testInsertKeyValueSize() {
        final String tableName = "testSize";
        String ddl = "create table if not exists " + tableName + "(" +
                     "sid string, id string, s String, " +
                     "primary key(shard(sid), id))";
        createTable(tableName, ddl,
                    TableLimits.builder()
                        .maxReadUnits(100)
                        .maxWriteUnits(10000)
                        .maxStorageInGBs(1)
                        .capacityMode(CapacityMode.Provisioned)
                        .build());

        PrepareStatementRequest prep;
        PrepareStatementResponse pret;
        PreparedStatement pstmt;

        QueryDetails qd;
        QueryRequest query;
        QueryResponse qret;

        final String s32 = genString(32);
        final String s512K = genString(512 * 1024 - 5); /* 5 - overhead */

        String fmt = "insert into " + tableName +
                     " (sid, id, s) values(\"%s\", \"%s\", \"%s\")";
        String insert = String.format(fmt, s32, s32, s32);

        prep = PrepareStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement(insert)
                .build();
        pret = client.prepareStatement(prep);

        qd = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(insert)
                .build();
        query = QueryRequest.builder()
                .queryDetails(qd)
                .build();
        qret = client.query(query);
        assertEquals(1, qret.getQueryResultCollection().getItems().size());

        /* Key size exceeded limit */
        insert = String.format(fmt, s32 + "a", s32, s32);
        prep = PrepareStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement(insert)
                .build();
        try {
            pret = client.prepareStatement(prep);
        } catch (BmcException ex) {
            assertError(ex, 400, "KeySizeLimitExceeded");
            checkErrorMessage(ex);
        }

        insert = String.format(fmt, s32, s32 + "a", s32);
        qd = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(insert)
                .build();
        query = QueryRequest.builder()
                .queryDetails(qd)
                .build();
        try {
            qret = client.query(query);
        } catch (BmcException ex) {
            assertError(ex, 400, "KeySizeLimitExceeded");
            checkErrorMessage(ex);
        }

        insert = "declare $sid string; $id string; $s string; insert into " +
                 tableName + " values($sid, $id, $s)";
        prep = PrepareStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement(insert)
                .build();
        pret = client.prepareStatement(prep);
        pstmt = pret.getPreparedStatement();
        assertNotNull(pstmt.getStatement());

        /* Key size exceeded limit */
        Map<String, Object> values = new HashMap<>();
        values.put("$sid", s32);
        values.put("$id", s32);
        values.put("$s", s512K);

        qd = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .isPrepared(true)
                .statement(pstmt.getStatement())
                .variables(values)
                .build();
        query = QueryRequest.builder()
                .queryDetails(qd)
                .build();
        qret = client.query(query);
        assertEquals(1, qret.getQueryResultCollection().getItems().size());

        values.clear();
        values.put("$sid", s32 + "a");
        values.put("$id", s32);
        values.put("$s", s512K);

        /* Key size exceeded limit */
        try {
            qret = client.query(query);
        } catch (BmcException ex) {
            assertError(ex, 400, "KeySizeLimitExceeded");
            checkErrorMessage(ex);
        }

        values.clear();
        values.put("$sid", s32);
        values.put("$id", s32);
        values.put("$s", s512K + "a");
        /* Value size exceeded limit */
        try {
            qret = client.query(query);
        } catch (BmcException ex) {
            assertError(ex, 400, "RowSizeLimitExceeded");
            checkErrorMessage(ex);
        }
    }

    @Test
    public void testQuerySize() {
        final int querySizeLimit = 10 * 1024;
        final String tableName = "foo";
        final String ddl = "create table if not exists " + tableName + "(" +
                            "id string, name String, " +
                            "primary key(id))";
        createTable(tableName, ddl);

        /* Test edge size: query string length = 10240 */
        StringBuilder sb = new StringBuilder(querySizeLimit);
        sb.append("select * from foo where name = \"");
        int maxStrLen = (querySizeLimit - sb.length() - 1);
        sb.append(genString(maxStrLen));
        sb.append("\"");
        String query = sb.toString();
        QueryDetails info = QueryDetails.builder()
            .compartmentId(getCompartmentId())
            .statement(query)
            .build();
        QueryRequest queryReq = QueryRequest.builder()
            .queryDetails(info)
            .build();
        client.query(queryReq);

        /* query string length = 10241 */
        sb.append("select * from foo where name = \"");
        sb.append(genString(maxStrLen + 1));
        sb.append("\"");
        query = sb.toString();
        info = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .build();
        queryReq = QueryRequest.builder()
                .queryDetails(info)
                .build();
        try {
            client.query(queryReq);
            fail("Expect to get 400 (InvalidParameter, " +
                 "Query statement too long) but not");
        } catch (BmcException ex) {
            assertError(ex, 400, "InvalidParameter");
            checkErrorMessage(ex);
        }
    }

    @Test
    public void testRequestSize() {
        assumeTrue("Skipping testLimitTables if not minicloud or cloud test" +
                   " or tenantLimits is not provided",
                   cloudRunning && tenantLimits != null);

        final int requestSizeLimit = tenantLimits.getStandardTableLimits().
                                                  getRequestSizeLimit();
        final String tableName = "testRequestSize";

        String ddl = "create table " + tableName +
                      "(id integer, name string, primary key(id))";
        createTable(tableName, ddl);

        String query = "declare $id integer; $name string;" +
                       "insert into " + tableName + " values($id, $name)";

        PrepareStatementRequest prepReq = PrepareStatementRequest.builder()
                .compartmentId(getCompartmentId())
                .statement(query)
                .build();
        PrepareStatementResponse prepRes = client.prepareStatement(prepReq);
        PreparedStatement pstmt = prepRes.getPreparedStatement();

        Map<String, Object> variables = new HashMap<>();
        variables.put("$id", 1);
        variables.put("$name", genString(requestSizeLimit));

        QueryDetails info = QueryDetails.builder()
                .compartmentId(getCompartmentId())
                .statement(pstmt.getStatement())
                .isPrepared(true)
                .variables(variables)
                .build();

        QueryRequest qryReq;
        try {
            qryReq = QueryRequest.builder()
                    .queryDetails(info)
                    .build();

            client.query(qryReq);
            fail("Expect to get 400 (RequestSizeLimitExceeded, " +
                    "Query statement too long) but not");
        } catch (BmcException ex) {
            assertError(ex, 400, "RequestSizeLimitExceeded");
            checkErrorMessage(ex);
        }
    }

    @Test
    public void testColumnNumLimit() {
        assumeTrue("Skipping testLimitTables if not minicloud or cloud test" +
                   " or tenantLimits is not provided",
                   cloudRunning && tenantLimits != null);

        final int numFields = tenantLimits.getStandardTableLimits().
                                           getColumnsPerTable();

        String tableName = "testColumnNumLimit";
        String ddl = makeCreateTableDdl(tableName, numFields);
        createTable(tableName, ddl);

        ddl = "alter table " + tableName + "(add a1 integer)";
        UpdateTableRequest utReq = buildUpdateTableRequest(tableName, ddl);
        executeDdlFail(utReq, "IllegalArgument");

        tableName = "testColumnLimitBad";
        ddl = makeCreateTableDdl(tableName, numFields + 1);
        createTableFail(tableName, ddl, defaultLimits, "IllegalArgument");
    }

    @Test
    public void testTableNumLimit() {
        assumeTrue("Skipping testTableNumLimit if not minicloud or cloud test" +
                   " or tenantLimits is not provided",
                   cloudRunning && tenantLimits != null);


        final int numTables = tenantLimits.getNumTables();
        if (numTables > NUM_TABLES) {
            /*
             * To prevent this test from running too long, skip the test if the
             * table number limit > ProxyTestBase.NUM_TABLES
             */
            return;
        }

        for (int i = 0; i < numTables; i++) {
            String tableName = "testTableNumLimit" + i;
            String ddl = makeCreateTableDdl(tableName, 1);
            createTable(tableName, ddl);
        }

        String tableName = "testTableNumLimit" + numTables;
        String ddl = makeCreateTableDdl(tableName, 1);
        createTableFail(tableName, ddl, defaultLimits, "TableLimitExceeded");
    }

    @Test
    public void testIndexNumLimit() {
        assumeTrue("Skipping testIndexNumLimit if not minicloud or cloud test" +
                   " or tenantLimits is not provided",
                   cloudRunning && tenantLimits != null);

        final String tableName = "testIndexNumLimit";
        final int numIndexesPerTable = tenantLimits.getStandardTableLimits().
                                                    getIndexesPerTable();

        final String ddl = makeCreateTableDdl(tableName, numIndexesPerTable + 1);
        createTable(tableName, ddl);

        for (int i = 0; i < numIndexesPerTable; i++) {
            String indexName = "idx" + i;
            createIndex(tableName, indexName, new String[]{makeFieldName(i)});
        }

        createIndexFail(tableName, "idxBad", makeFieldName(numIndexesPerTable),
                        "IndexLimitExceeded");
    }

    @Test
    public void testEvolutionNumLimit() {
        assumeTrue("Skipping testEvolutionNumLimit if not minicloud or cloud test " +
                   "tenantLimits is not provided",
                   cloudRunning && tenantLimits != null);

        final int numEvolutions = tenantLimits.getStandardTableLimits().
                                               getSchemaEvolutions();
        if (numEvolutions > ProxyTestBase.NUM_SCHEMA_EVOLUTIONS) {
            /*
             * To prevent this test from running too long, skip the test if the
             * table evolution times limit > ProxyTestBase.NUM_SCHEMA_EVOLUTIONS
             */
            return;
        }

        final String tableName = "testEvolutionNumLimit";
        String ddl = makeCreateTableDdl(tableName, 0);
        createTable(tableName, ddl);

        for (int i = 0; i < numEvolutions; i++) {
            ddl = makeAlterTableDdl(tableName, i);
            alterTable(tableName, ddl);
        }

        ddl = makeAlterTableDdl(tableName, numEvolutions);
        alterTableFail(tableName, ddl, "EvolutionLimitExceeded");
    }

    /**
     * Tests limits on total size and throughput allowed per-table and
     * per-tenant.
     */
    @Test
    public void testTableProvisioningLimits() {
        /*
         * This test aims to create tables exceeds the tenant capacity, it is
         * not applicable in cloud test
         */
        assumeTrue("Skipping testTableProvisioningLimits if not minicloud test",
                   useMiniCloud);

        final int maxRead = tenantLimits.getStandardTableLimits().
                                         getTableReadUnits();
        final int maxWrite = tenantLimits.getStandardTableLimits().
                                          getTableWriteUnits();
        final int maxSize = tenantLimits.getStandardTableLimits().
                                         getTableSize();

        /* TODO: when per-tenant limits are available get them */
        final int maxTenantRead = tenantLimits.getTenantReadUnits();
        final int maxTenantWrite = tenantLimits.getTenantWriteUnits();
        final int maxTenantSize = tenantLimits.getTenantSize();

        String tableName = "testLimits";
        String ddl = makeCreateTableDdl(tableName, 1);

        /* ReadUnits > maxTableReadUnits */
        TableLimits limits = TableLimits.builder()
                .maxReadUnits(maxRead + 1)
                .maxWriteUnits(1)
                .maxStorageInGBs(1)
                .build();
        createTableFail(tableName, ddl, limits, "TableDeploymentLimitExceeded");

        /* WriteUnits > maxTableWriteUnits */
        limits = TableLimits.builder()
                .maxReadUnits(1)
                .maxWriteUnits(maxWrite + 1)
                .maxStorageInGBs(1)
                .build();

        createTableFail(tableName, ddl, limits, "TableDeploymentLimitExceeded");

        /* tableSize > maxTableSize */
        limits = TableLimits.builder()
                .maxReadUnits(1)
                .maxWriteUnits(1)
                .maxStorageInGBs(maxSize + 1)
                .build();
        createTableFail(tableName, ddl, limits, "TableDeploymentLimitExceeded");

        /* make a table and try to evolve it past read limit */
        limits = TableLimits.builder()
                .maxReadUnits(maxRead)
                .maxWriteUnits(maxWrite)
                .maxStorageInGBs(maxSize)
                .build();
        createTable(tableName, ddl, limits);

        limits = TableLimits.builder()
                .maxReadUnits(maxRead + 1)
                .maxWriteUnits(maxWrite)
                .maxStorageInGBs(maxSize)
                .build();
        updateTableLimitsFail(tableName, limits, "TableDeploymentLimitExceeded");

        /*
         * Test per-tenant limits by trying to create another table. If it's one
         * table this only works if the per-table limit is >= 1/2 of the
         * tenant limit. See ProxyTestBase's TenantLimits.
         */
        tableName = "testLimits1";
        ddl = makeCreateTableDdl(tableName, 1);
        limits = TableLimits.builder()
                .maxReadUnits(maxTenantRead - maxRead + 1)
                .maxWriteUnits(1)
                .maxStorageInGBs(1)
                .build();
        createTableFail(tableName, ddl, limits, "TableDeploymentLimitExceeded");

        limits = TableLimits.builder()
                .maxReadUnits(1)
                .maxWriteUnits(maxTenantWrite - maxWrite + 1)
                .maxStorageInGBs(1)
                .build();
        createTableFail(tableName, ddl, limits, "TableDeploymentLimitExceeded");

        limits = TableLimits.builder()
                .maxReadUnits(1)
                .maxWriteUnits(1)
                .maxStorageInGBs(maxTenantSize - maxSize + 1)
                .build();
        createTableFail(tableName, ddl, limits, "TableDeploymentLimitExceeded");
    }

    @Test
    public void testOpThrottling() {
        /* This test adjusts op throttling rate, it is for minicloud only */
        assumeTrue("Skipping testOpThrottling if not minicloud test",
                   useMiniCloud);

        final String tableName = "testOpRate";
        String workReqId;

        String ddl = "create table if not exists " + tableName +
                     "(id integer, name string, primary key(id))";
        TableLimits limits = TableLimits.builder()
                .maxReadUnits(100)
                .maxWriteUnits(100)
                .maxStorageInGBs(1)
                .build();

        setOpThrottling(getTenantId(), DEFAULT_OP_THROTTLE);

        try {
            /* create table */
            workReqId = createTable(tableName, ddl, limits, false);

            GetWorkRequestRequest req = GetWorkRequestRequest.builder()
                    .workRequestId(workReqId)
                    .build();
            int num = 0;
            try {
                while (true) {
                    client.getWorkRequest(req);
                    num++;
                    if (num > 100) {
                        fail("Op Throttling exception should have been thrown");
                    }
                }
            } catch (BmcException ex) {
                /* success */
                assertError(ex, 429, "TooManyRequests");
                checkErrorMessage(ex);
            }

            ListWorkRequestsRequest lwReq = ListWorkRequestsRequest.builder()
                    .compartmentId(getCompartmentId())
                    .build();
            num = 0;
            try {
                while (true) {
                    client.listWorkRequests(lwReq);
                    num++;
                    if (num > 100) {
                        fail("Op Throttling exception should have been thrown");
                    }
                }
            } catch (BmcException ex) {
                /* success */
                assertError(ex, 429, "TooManyRequests");
                checkErrorMessage(ex);
            }

        } finally {
            setOpThrottling(getTenantId(), NO_OP_THROTTLE);
        }
    }

    void createTableFail(String tableName,
                         String ddl,
                         int expHttpStatusCode,
                         String expErrorCode) {
        CreateTableRequest req = buildCreateTableRequest(getCompartmentId(),
                                                         tableName, ddl,
                                                         defaultLimits);
        executeDdlFail(req, expHttpStatusCode, expErrorCode);
    }

    void createTableFail(String tableName,
                         String ddl,
                         TableLimits limits,
                         String expErrorCode) {
        CreateTableRequest req = buildCreateTableRequest(getCompartmentId(),
                                                         tableName,
                                                         ddl,
                                                         limits);
        executeDdlFail(req, expErrorCode);
    }

    void alterTableFail(String tableName, String ddl, String expErrorCode) {
        UpdateTableRequest req = buildUpdateTableRequest(tableName, ddl);
        executeDdlFail(req, expErrorCode);
    }

    void createIndexFail(String tableName,
                         String indexName,
                         String field,
                         String expErrorCode) {
        CreateIndexRequest req = buildCreateIndexRequest(tableName,
                                                         indexName,
                                                         new String[]{field},
                                                         false /* ifNotExists*/);
        executeDdlFail(req, expErrorCode);
    }

    void updateTableLimitsFail(String tableName,
                               TableLimits limits,
                               String expErrorCode) {
        UpdateTableRequest req = buildUpdateTableRequest(tableName, limits);
        executeDdlFail(req, expErrorCode);
    }

    private void assertError(BmcException ex,
                             int statusCode,
                             String serviceCode) {

        assertEquals(statusCode, ex.getStatusCode());
        assertEquals(serviceCode, ex.getServiceCode());
    }

    private String makeCreateTableDdl(String name, int numFields) {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        sb.append(name);
        sb.append("(id INTEGER, ");
        for (int i = 0; i < numFields - 1; i++) {
            sb.append(makeFieldName(i));
            sb.append(" STRING, ");
        }
        sb.append("PRIMARY KEY(id))");
        return sb.toString();
    }

    private String makeAlterTableDdl(String tableName, int idxField) {
        final StringBuilder sb = new StringBuilder("ALTER TABLE ");
        sb.append(tableName);
        sb.append(" (ADD ");
        sb.append(makeFieldName(idxField));
        sb.append(" STRING)");
        return sb.toString();
    }

    private String makeFieldName(int idxField) {
        return "c" + idxField;
    }

    private static String genString(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append((char)('A' + i % 10));
        }
        return sb.toString();
    }

    private static class TestRetryCondition extends DefaultRetryCondition {
        TestRetryCondition() {
            super();
        }

        @Override
        public boolean shouldBeRetried(final BmcException ex) {
            boolean shouldBeRetried = super.shouldBeRetried(ex);
            if (ex.getStatusCode() == 429) {
                assertTrue(shouldBeRetried);
            }
            return shouldBeRetried;
        }
    }
}
