/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;

import static oracle.nosql.driver.ops.TableLimits.CapacityMode.ON_DEMAND;
import static oracle.nosql.driver.ops.TableLimits.CapacityMode.PROVISIONED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.UUID;

import org.junit.Test;

import oracle.kv.impl.topo.RepNodeId;
import oracle.nosql.driver.DeploymentException;
import oracle.nosql.driver.EvolutionLimitException;
import oracle.nosql.driver.IndexLimitException;
import oracle.nosql.driver.KeySizeLimitException;
import oracle.nosql.driver.OperationThrottlingException;
import oracle.nosql.driver.RequestSizeLimitException;
import oracle.nosql.driver.RowSizeLimitException;
import oracle.nosql.driver.TableLimitException;
import oracle.nosql.driver.TableNotFoundException;
import oracle.nosql.driver.Version;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutRequest.Option;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.TableResult.State;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.util.tmi.TableRequestLimits;
import oracle.nosql.util.tmi.TenantLimits;

/**
 * Test various limits:
 * DDL limits (minicloud only)
 *  o num indexes
 *  o num tables
 *  o num schema evolutions
 * Data limits (minicloud and not)
 *  o key size
 *  o index key size
 */
public class LimitsTest extends ProxyTestBase {

    final static int INDEX_KEY_SIZE_LIMIT = 64;
    final static int KEY_SIZE_LIMIT = rlimits.getPrimaryKeySizeLimit();
    final static int QUERY_SIZE_LIMIT = rlimits.getQueryStringSizeLimit();
    final static int REQUEST_SIZE_LIMIT = rlimits.getRequestSizeLimit();
    final static int ROW_SIZE_LIMIT = rlimits.getRowSizeLimit();
    final static int BATCH_REQUEST_SIZE_LIMIT =
        rlimits.getBatchRequestSizeLimit();

    final static String tableName = "limitTable";

    /* Create a table */
    final static String createTableDDL =
        "CREATE TABLE IF NOT EXISTS limitTable (" +
        "sid INTEGER, " +
        "id INTEGER, " +
        "name STRING, " +
        "json JSON, " +
        "PRIMARY KEY(SHARD(sid), id))";

    /* Create a table used for key limits */
    final static String createKeyTable1DDL =
        "CREATE TABLE IF NOT EXISTS keyLimitTable1 (" +
        "name STRING, " +
        "city STRING, " +
        "PRIMARY KEY(name))";

    /* Create a table used for key limits */
    final static String createKeyTable2DDL =
        "CREATE TABLE IF NOT EXISTS keyLimitTable2 (" +
        "name STRING, " +
        "city STRING, " +
        "address STRING, " +
        "PRIMARY KEY(shard(name), city))";

    /* Create an index for key limits */
    final static String createKeyTableIndexDDL =
        "CREATE INDEX CityIndex on keyLimitTable1 (city)";

    /**
     * Test limit on number of indexes
     */
    @Test
    public void testIndexLimit() throws Exception {
        assumeTrue("Skip the test if not minicloud or cloud test or " +
                   "tenantLimits is not provided",
                   cloudRunning && tenantLimits != null);

        TableRequestLimits limits = tenantLimits.getStandardTableLimits();
        int indexLimit = limits.getIndexesPerTable();

        /* create a table with a bunch of fields */
        StringBuilder sb = new StringBuilder();
        sb.append("create table limitTable(id integer, ");
        for (int i = 0; i < indexLimit + 1; i++) {
            sb.append("name").append(i).append(" string,");
        }
        sb.append("primary key(id))");

        tableOperation(handle, sb.toString(),
                       new TableLimits(20000, 20000, 50),
                       TableResult.State.ACTIVE, 20000);

        for (int i = 0; i < indexLimit + 1; i++) {
            sb = new StringBuilder();
            sb.append("create index idx").append(i).
                append(" on limitTable(name").append(i).append(")");
            final String statement = sb.toString();
            if (i == indexLimit) {
                try {
                    tableOperation(handle, statement, null,
                                   TableResult.State.ACTIVE, 20000);
                    fail("Adding index should have failed");
                } catch (IndexLimitException ile) {}
            } else {
                tableOperation(handle, statement, null,
                               TableResult.State.ACTIVE, 20000);
            }
        }

        /* listIndexes is a test-only method right now */
        String[] indexes = listIndexes(handle, tableName);
        assertEquals("Unexpected number of indexes", indexLimit, indexes.length);
    }

    /**
     * Test limit on index size
     */
    @Test
    public void testIndexSizeLimit() throws Exception {
        assumeTrue(onprem == false); /* not for onprem */

        /* create a table add some long-ish fields */
        final String tableDDL = "create table limitTable(id integer, " +
            "data string, primary key(id))";

        tableOperation(handle, tableDDL,
                       new TableLimits(20000, 20000, 50),
                       TableResult.State.ACTIVE, 20000);

        /* add some rows with data that exceeds default 64 bytes */
        String data = makeString(400);
        for (int i = 0; i < 20; i++) {
            PutRequest prq = new PutRequest().setTableName("limitTable")
                .setValue(new MapValue().put("id", i).put("data", data));
            PutResult prs = handle.put(prq);
            assertNotNull(prs.getVersion());
        }

        /*
         * Create an index that should fail because of key size limit
         */
        try {
            final String statement = "create index idx on limitTable(data)";
            tableOperation(handle, statement, null,
                           TableResult.State.ACTIVE, 20000);
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("KeySizeLimitException"));
            /* expected */
        }
    }

    /**
     * Test limit on number of schema evolutions
     */
    @Test
    public void testEvolutionLimit() throws Exception {
        assumeTrue("Skip the test if not minicloud or cloud test or " +
                   "tenantLimits is not provided",
                   cloudRunning && tenantLimits != null);

        TableRequestLimits limits = tenantLimits.getStandardTableLimits();
        int evoLimit = limits.getSchemaEvolutions();
        if (evoLimit > NUM_SCHEMA_EVOLUTIONS) {
            /*
             * To prevent this test from running too long, skip the test if the
             * table evolution times limit > ProxyTestBase.NUM_SCHEMA_EVOLUTIONS
             */
            return;
        }

        createTable();

        for (int i = 0; i < evoLimit + 1; i++) {
            StringBuilder sb = new StringBuilder();
            sb.append("alter table limitTable(add name").
                append(i).append(" string)");
            final String statement = sb.toString();
            if (i == evoLimit) {
                try {
                    tableOperation(handle, statement, null,
                                   TableResult.State.ACTIVE, 20000);
                    fail("Alter table should have failed, num alter table: "
                         + i + ", limit: " + evoLimit);
                } catch (EvolutionLimitException ele) {}
            } else {
                tableOperation(handle, statement, null,
                               TableResult.State.ACTIVE, 20000);
            }
        }
    }

    /**
     * Test limit on number of tables
     */
    @Test
    public void testNumTablesLimit() throws Exception {
        assumeTrue("Skip the test if not minicloud or cloud test or " +
                   "tenantLimits is not provided",
                   cloudRunning && tenantLimits != null);

        TenantLimits limits = tenantLimits;
        int tableLimit = limits.getNumTables();
        if (tableLimit > NUM_TABLES) {
            /*
             * To prevent this test from running too long, skip the test if the
             * table number limit > ProxyTestBase.NUM_TABLES
             */
            return;
        }

        for (int i = 0; i < tableLimit + 1; i++) {
            StringBuilder sb = new StringBuilder();
            sb.append("create table t").append(i).append("(id integer, ").
                append("primary key(id))");
            final String statement = sb.toString();
            if (i == tableLimit) {
                /*
                 * Make the limits on these small so they don't trip
                 * the size/throughput per-tenant limits before hitting the
                 * number of tables limits
                 */
                try {
                    tableOperation(handle, statement,
                                   new TableLimits(500, 500, 10),
                                   TableResult.State.ACTIVE, 20000);
                    fail("create table should have failed, num create table: "
                         + i + ", limit: " + tableLimit);
                } catch (TableLimitException iae) {}
            } else {
                tableOperation(handle, statement,
                               new TableLimits(500, 500, 10),
                               TableResult.State.ACTIVE, 20000);
            }
        }
    }

    /*
     * Not strictly a limit, but test the case where an index is created on
     * existing JSON and there is a type mismatch.
     */
    @Test
    public void testBadIndexType() {
        final String jsonRow =
            "{\"sid\": 1, \"id\": 1, \"name\": \"joe\", \"json\":" +
            "{\"age\":5}"+
            "}";
        final String conflictingIndex =
            "create index idx on limitTable(json.age as string)";

        createTable();

        /* put the JSON */
        PutRequest prq = new PutRequest().setValueFromJson(jsonRow, null).
            setTableName(tableName);
        PutResult prs = handle.put(prq);
        assertNotNull(prs.getVersion());

        /* create a conflicting index, string index on integer field */
        try {
            tableOperation(handle, conflictingIndex, null,
                           TableResult.State.ACTIVE, 20000);
            fail("Attempt to add a conflicting index should have failed");
        } catch (IllegalArgumentException iae) {
            // success
        }
    }

    /**
     * The query size limit is artificially 200. See ProxyTestBase
     */
    @Test
    public void testQuerySizeLimit() {
        assumeTrue(onprem == false);
        createTable();
        final StringBuilder sb = new StringBuilder();
        sb.append("select aaaaa,bbbbb,ccccccccccc, dddddddd,")
            .append("eeeeeee,ffffff ")
            .append("from limitTable ")
            .append("where xxxxxxxxxxxxxxxx = yyyyyyyyyyyyyyyyyyy");
        while (sb.toString().length() < QUERY_SIZE_LIMIT) {
            sb.append(" and xxxxxxxxxxxxxxx = yyyyyyyyyyyyyyyyyyy");
        }

        final String longQuery = sb.toString();

        createTable();

        QueryRequest qr = new QueryRequest().setStatement(longQuery);
        PrepareRequest pr = new PrepareRequest().setStatement(longQuery);

        try {
            handle.query(qr);
            fail("Query should have failed");
        } catch (IllegalArgumentException iae) {
            // success
        }

        try {
            handle.prepare(pr);
            fail("Prepare should have failed");
        } catch (IllegalArgumentException iae) {
            // success
        }
    }

    /**
     * Test key size limits (primary, index) and value size limit
     */
    @Test
    public void testKeyValueSizeLimit() {
        assumeTrue("Skip the test if onprem test or tenantLimits is not provided",
                   !onprem && tenantLimits != null);

        final oracle.kv.Version kvver =
            new oracle.kv.Version(UUID.randomUUID(), 1, new RepNodeId(1, 1), 1);
        final Version dummyVersion = Version.createVersion(kvver.toByteArray());

        createKeyTable();

        /*
         * PrimaryKey size limit
         */
        final int keySizeLimit = tenantLimits.getStandardTableLimits()
                                             .getPrimaryKeySizeLimit();
        PutRequest putReq;
        WriteMultipleRequest umReq = new WriteMultipleRequest();
        int expFailIndex = keySizeLimit + 1;
        for (int i = 32; i <= keySizeLimit + 1; i++) {
            String name = makeName(i);
            /* Put */
            putReq = new PutRequest()
                .setTableName("keyLimitTable1")
                .setValue(new MapValue()
                          .put("name", name)
                          .put("city", "Omaha"));
            try {
                handle.put(putReq);
                if (i > keySizeLimit) {
                    fail("Put should have failed");
                }
            } catch (KeySizeLimitException ex) {
                assertEquals(expFailIndex, i);
            }

            /* WriteMultipleRequest(Put) */
            umReq.clear();
            umReq.add(putReq, true);
            try {
                handle.writeMultiple(umReq);
                if (i > keySizeLimit) {
                    fail("Put should have failed");
                }
            } catch (KeySizeLimitException ex) {
                assertEquals(expFailIndex, i);
            }

            /* PutIfAbsent */
            putReq.setOption(Option.IfAbsent);
            try {
                handle.put(putReq);
                if (i > keySizeLimit) {
                    fail("Put should have failed");
                }
            } catch (KeySizeLimitException ex) {
                assertEquals(expFailIndex, i);
            }

            /* WriteMultipleRequest(PutIfAbsent) */
            umReq.clear();
            umReq.add(putReq, true);
            try {
                handle.writeMultiple(umReq);
                if (i > keySizeLimit) {
                    fail("Put should have failed");
                }
            } catch (KeySizeLimitException ex) {
                assertEquals(expFailIndex, i);
            }

            /* PutIfPresent */
            putReq.setOption(Option.IfPresent);
            try {
                handle.put(putReq);
                if (i > keySizeLimit) {
                    fail("Put should have failed");
                }
            } catch (KeySizeLimitException ex) {
                assertEquals(expFailIndex, i);
            }

            /* WriteMultipleRequest(PutIfPresent) */
            umReq.clear();
            umReq.add(putReq, true);
            try {
                handle.writeMultiple(umReq);
                if (i > keySizeLimit) {
                    fail("Put should have failed");
                }
            } catch (KeySizeLimitException ex) {
                assertEquals(expFailIndex, i);
            }

            /* PutIfVersion */
            putReq.setOption(Option.IfVersion)
                .setMatchVersion(dummyVersion);
            try {
                handle.put(putReq);
                if (i > keySizeLimit) {
                    fail("Put should have failed");
                }
            } catch (KeySizeLimitException ex) {
                assertEquals(expFailIndex, i);
            }

            /* WriteMultipleRequest(PutIfVersion) */
            umReq.clear();
            umReq.add(putReq, true);
            try {
                handle.writeMultiple(umReq);
                if (i > keySizeLimit) {
                    fail("Put should have failed");
                }
            } catch (KeySizeLimitException ex) {
                assertEquals(expFailIndex, i);
            }
        }

        /* Primary key contains 2 fields: 1 shard key + 1 minor key */
        String name = makeName(32);
        expFailIndex = keySizeLimit - name.length() + 1;
        for (int i = 0; i <= keySizeLimit - name.length() + 1; i++) {
            String city = makeName(i);
            /* Put */
            putReq = new PutRequest()
                .setTableName("keyLimitTable2")
                .setValue(new MapValue().put("name", name)
                          .put("city", city));

            try {
                handle.put(putReq);
                if (i + name.length() > keySizeLimit) {
                    fail("Put should have failed");
                }
            } catch (KeySizeLimitException ex) {
                assertEquals(expFailIndex, i);
            }

            /* PutIfAbsent */
            putReq.setOption(Option.IfAbsent);
            try {
                handle.put(putReq);
                if (i > keySizeLimit) {
                    fail("Put should have failed");
                }
            } catch (KeySizeLimitException ex) {
                assertEquals(expFailIndex, i);
            }

            /* PutIfPresent */
            putReq.setOption(Option.IfPresent);
            try {
                handle.put(putReq);
                if (i > keySizeLimit) {
                    fail("Put should have failed");
                }
            } catch (KeySizeLimitException ex) {
                assertEquals(expFailIndex, i);
            }

            /* PutIfVersion */
            putReq.setOption(Option.IfVersion)
                .setMatchVersion(dummyVersion);
            try {
                handle.put(putReq);
                if (i > keySizeLimit) {
                    fail("Put should have failed");
                }
            } catch (KeySizeLimitException ex) {
                assertEquals(expFailIndex, i);
            }
        }

        /*
         * IndexKey size limit
         */
        final int indexKeyLimit = INDEX_KEY_SIZE_LIMIT;

        /*
         * Expect index size limit exception
         */
        name = makeName(62);
        for (int i = 1; i < indexKeyLimit + 1; i++) {
            String city = makeName(i);
            try {
                putReq = new PutRequest()
                    .setTableName("keyLimitTable1")
                    .setValue(new MapValue().put("name", name)
                              .put("city", city));
                handle.put(putReq);
                if (i > indexKeyLimit) {
                    fail("Put should have failed");
                }
            } catch (Exception e) {
                assertEquals(indexKeyLimit, i);
            }
        }

        /*
         * Value size limit
         */
        final int rowSizeLimit = ROW_SIZE_LIMIT;
        String address = makeName(rowSizeLimit);
        try {
            putReq = new PutRequest()
                .setTableName("keyLimitTable2")
                .setValue(new MapValue().put("name", "aaaa")
                          .put("city", "Omaha")
                          .put("address", address));
            handle.put(putReq);
            fail("Put should have failed");
        } catch (RowSizeLimitException ex) {
        }

        /*
         * Value size limit check on WriteMultiple sub request.
         */
        address = makeName(ROW_SIZE_LIMIT);
        umReq = new WriteMultipleRequest();
        umReq.add(new PutRequest()
                      .setTableName("keyLimitTable2")
                      .setValue(new MapValue().put("name", "aaaa")
                                .put("city", "Omaha")
                                .put("address", address)),
                  false);
        try {
            handle.writeMultiple(umReq);
            fail("WriteMultiple should have failed");
        } catch (RowSizeLimitException ex) {
        }
    }

    @Test
    public void testInsertKeyValueSize() {
        assumeTrue("Skip testKeyValueSizeInsert() if run against on-prem",
                   onprem == false);
        assumeKVVersion("testInsertKeyValueSize", 21, 3, 5);

        final int maxKeySize = rlimits.getPrimaryKeySizeLimit();
        final int maxRowSize = rlimits.getRowSizeLimit();

        tableOperation(handle, createKeyTable1DDL,
                       new TableLimits(1000, 1000, 5),
                       TableResult.State.ACTIVE, 10000);

        String ddl = "CREATE TABLE IF NOT EXISTS testId(" +
                     "id INTEGER GENERATED ALWAYS AS IDENTITY, pk STRING, " +
                     "s STRING, PRIMARY KEY(pk, id))";
        tableOperation(handle, ddl, new TableLimits(1000, 1000, 5),
                       TableResult.State.ACTIVE, 10000);


        ddl = "CREATE TABLE IF NOT EXISTS test2pk(" +
              "sk STRING, pk STRING, s STRING, PRIMARY KEY(shard(sk), pk))";
        tableOperation(handle, ddl, new TableLimits(1000, 1000, 5),
               TableResult.State.ACTIVE, 10000);

        /* Test insert query */
        PrepareRequest preq;
        PrepareResult pret;

        PreparedStatement pstmt;
        QueryRequest qreq;
        QueryResult qret;
        String insert;

        String fmt = "insert into keyLimitTable1 values('%s', '%s')";
        String name64 = makeName(maxKeySize);
        String city512K = makeName(maxRowSize - 5); /* 5 - overhead */

        insert = String.format(fmt, name64, city512K);
        preq = new PrepareRequest().setStatement(insert);
        pret = handle.prepare(preq);
        qreq = new QueryRequest().setPreparedStatement(pret);
        qret = handle.query(qreq);
        assertEquals(1, qret.getResults().size());

        insert = String.format(fmt, name64 + "a", city512K);
        preq = new PrepareRequest().setStatement(insert);
        try {
            handle.prepare(preq);
            fail("Prepare should fail: key size exceeded");
        } catch (KeySizeLimitException ex) {
            /* expected */
            checkErrorMessage(ex);
        }

        qreq = new QueryRequest().setStatement(insert);
        try {
            handle.query(qreq);
            fail("Query should fail: key size exceeded");
        } catch (KeySizeLimitException ex) {
            /* expected */
            checkErrorMessage(ex);
        }

        insert = String.format(fmt, name64, city512K + "a");
        preq = new PrepareRequest().setStatement(insert);
        try {
            handle.prepare(preq);
            fail("Prepare should fail: value size exceeded");
        } catch (RowSizeLimitException ex)  {
            /* expected */
            checkErrorMessage(ex);
        }

        qreq = new QueryRequest().setStatement(insert);
        try {
            handle.query(qreq);
            fail("Query should fail: value size exceeded");
        } catch (RowSizeLimitException ex) {
            /* expected */
            checkErrorMessage(ex);
        }

        insert = "declare $name string; $city string; " +
                 "insert into keyLimitTable1(name, city) values($name, $city)";
        preq = new PrepareRequest().setStatement(insert);
        pret = handle.prepare(preq);
        pstmt = pret.getPreparedStatement();

        pstmt.setVariable("$name", new StringValue(name64));
        pstmt.setVariable("$city", new StringValue(city512K));
        qreq = new QueryRequest().setPreparedStatement(pstmt);
        qret = handle.query(qreq);
        assertEquals(1, qret.getResults().size());

        pstmt.clearVariables();
        pstmt.setVariable("$name", new StringValue(name64 + "a"));
        pstmt.setVariable("$city", new StringValue(city512K));
        qreq = new QueryRequest().setPreparedStatement(pstmt);
        try {
            qret = handle.query(qreq);
            fail("Query should fail: key size exceeded");
        } catch (KeySizeLimitException ex)  {
            /* expected */
            checkErrorMessage(ex);
        }

        pstmt.clearVariables();
        pstmt.setVariable("$name", new StringValue(name64));
        pstmt.setVariable("$city", new StringValue(city512K + "a"));
        qreq = new QueryRequest().setPreparedStatement(pstmt);
        try {
            qret = handle.query(qreq);
            fail("Query should fail: value size exceeded");
        } catch (RowSizeLimitException ex)  {
            /* expected */
            checkErrorMessage(ex);
        }

        fmt = "insert into testId(pk, s) values('%s', '%s')";
        insert = String.format(fmt, name64, city512K);
        preq = new PrepareRequest().setStatement(insert);
        pret = handle.prepare(preq);

        qreq = new QueryRequest().setPreparedStatement(pret);
        try {
            qret = handle.query(qreq);
            fail("Query should fail: key size exceeded");
        } catch (KeySizeLimitException ex)  {
            /* expected */
            checkErrorMessage(ex);
        }

        /* PK has 2 components */
        String s32 = makeName(32);
        String s33 = makeName(33);
        fmt = "insert into test2pk values('%s', '%s', 'a')";
        insert = String.format(fmt, s32, s32);
        preq = new PrepareRequest().setStatement(insert);
        pret = handle.prepare(preq);
        qreq = new QueryRequest().setPreparedStatement(pret);
        qret = handle.query(qreq);
        assertEquals(1, qret.getResults().size());

        /* Key size exceeded, sk: 33, pk: 32 */
        insert = String.format(fmt, s33, s32);
        preq = new PrepareRequest().setStatement(insert);
        try {
            handle.prepare(preq);
            fail("Prepare should fail: key size exceeded");
        } catch (KeySizeLimitException ex)  {
            /* expected */
            checkErrorMessage(ex);
        }

        /* Key size exceeded, sk: 32, pk: 33 */
        insert = String.format(fmt, s32, s33);
        preq = new PrepareRequest().setStatement(insert);
        try {
            handle.prepare(preq);
            fail("Prepare should fail: key size exceeded");
        } catch (KeySizeLimitException ex)  {
            /* expected */
            checkErrorMessage(ex);
        }

        /* Query with variables */
        insert = "declare $sk string; $pk string; " +
                 "upsert into test2pk(sk, pk, s) values($sk, $pk, 'a')";
        preq = new PrepareRequest().setStatement(insert);
        pret = handle.prepare(preq);
        pstmt = pret.getPreparedStatement();

        pstmt.setVariable("$sk", new StringValue(s32));
        pstmt.setVariable("$pk", new StringValue(s32));
        qreq = new QueryRequest().setPreparedStatement(pstmt);
        qret = handle.query(qreq);
        assertEquals(1, qret.getResults().size());

        /* Key size exceeded, sk: 33, pk: 32 */
        pstmt.clearVariables();
        pstmt.setVariable("$sk", new StringValue(s33));
        pstmt.setVariable("$pk", new StringValue(s32));
        qreq = new QueryRequest().setPreparedStatement(pstmt);
        try {
            qret = handle.query(qreq);
            fail("Query should fail: key size exceeded");
        } catch (KeySizeLimitException ex)  {
            /* expected */
            checkErrorMessage(ex);
        }

        /* Key size exceeded, sk: 32, pk: 33 */
        pstmt.clearVariables();
        pstmt.setVariable("$sk", new StringValue(s32));
        pstmt.setVariable("$pk", new StringValue(s33));
        qreq = new QueryRequest().setPreparedStatement(pstmt);
        try {
            qret = handle.query(qreq);
            fail("Query should fail: key size exceeded");
        } catch (KeySizeLimitException ex)  {
            /* expected */
            checkErrorMessage(ex);
        }
    }

    /**
     * Test index key size by populating a table before creating the
     * index. Index creation should fail.
     */
    @Test
    public void testCreateIndexFail() {
        assumeTrue("Skip the test if onprem test or tenantLimits is not provided",
                   !onprem && tenantLimits != null);

        final int indexKeyLimit = tenantLimits.getStandardTableLimits()
                                              .getIndexKeySizeLimit();
        createTable();

        /*
         * Populate with a bunch of values that will fail an index key size
         * limit check. Use the name field.
         */
        MapValue value = new MapValue()
            .put("id", 1)
            .put("name", makeName(indexKeyLimit+1))
            .putFromJson("json", "{\"a\": \"boo\"}", null);
        PutRequest putReq = new PutRequest()
            .setTableName("limitTable")
            .setValue(value);

        for (int i = 0; i < 500; i++) {
            value.put("sid", i);
            handle.put(putReq);
        }

        final String statementSize = "create index name on limitTable(name)";
        final String statementType =
            "create index name on limitTable(json.a as integer)";
        try {
            tableOperation(handle, statementSize, null,
                           TableResult.State.ACTIVE, 20000);
            fail("Adding index should have failed");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Try adding an index on JSON with the wrong type. This will
         * also fail. The rows have a string in that field.
         */
        try {
            tableOperation(handle, statementType, null,
                           TableResult.State.ACTIVE, 20000);
            fail("Adding index should have failed");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void testRequestSizeLimit() {
        assumeTrue("Skip the test if onprem test or tenantLimits is not provided",
                   !onprem && tenantLimits != null);

        final TableRequestLimits limits = tenantLimits.getStandardTableLimits();
        final int reqSizeLimit = limits.getRequestSizeLimit();
        final int batchReqSizeLimit = limits.getBatchRequestSizeLimit();

        createTable();

        MapValue value = new MapValue()
            .put("sid", 0)
            .put("id", 1)
            .put("name", "jack.smith")
            .put("json", makeName(reqSizeLimit));
        PutRequest putReq = new PutRequest()
            .setTableName("limitTable")
            .setValue(value);
        try {
            handle.put(putReq);
            fail("Put should have failed");
        } catch (RequestSizeLimitException ex) {
        }

        /*
         * WriteMultipleRequest with max number of operations and data size,
         * it is expected to succeed.
         */
        final int numOps = 50;
        int dataSizePerOp = ROW_SIZE_LIMIT - 1024;
        WriteMultipleRequest umReq = new WriteMultipleRequest();
        for (int i = 0; i < numOps; i++) {
            value = new MapValue()
                .put("sid", 0)
                .put("id", i)
                .put("name", "jack.smith")
                .put("json", makeName(dataSizePerOp));
            umReq.add(new PutRequest()
                          .setTableName("limitTable")
                          .setValue(value),
                      false);
        }
        try {
            handle.writeMultiple(umReq);
        } catch (Exception ex) {
            fail("WriteMultiple failed: " + ex.getMessage());
        }

        /*
         * WriteMultipleRequest's request size exceeded batchReqSizeLimit,
         * it is expected to fail.
         */
        dataSizePerOp = batchReqSizeLimit/numOps;
        umReq = new WriteMultipleRequest();
        for (int i = 0; i < numOps; i++) {
            value = new MapValue()
                .put("sid", 0)
                .put("id", i)
                .put("name", "jack.smith")
                .put("json", makeName(dataSizePerOp));
            umReq.add(new PutRequest()
                          .setTableName("limitTable")
                          .setValue(value),
                      false);
        }
        try {
            handle.writeMultiple(umReq);
            fail("WriteMultiple should have failed");
        } catch (RequestSizeLimitException ex) {
        }

        /*
         * Each sub request size should not exceed REQUEST_SIZE_LIMIT.
         */
        dataSizePerOp = REQUEST_SIZE_LIMIT;
        umReq = new WriteMultipleRequest();
        value = new MapValue()
            .put("sid", 0)
            .put("id", 0)
            .put("name", "jack.smith")
            .put("json", makeName(dataSizePerOp));
        umReq.add(new PutRequest()
                      .setTableName("limitTable")
                      .setValue(value),
                  false);
        try {
            handle.writeMultiple(umReq);
            fail("WriteMultiple should have failed");
        } catch (RequestSizeLimitException ex) {
        }
    }

    /**
     * Test the number of column limit per table.
     */
    @Test
    public void testColumnLimit() {
        assumeTrue("Skip the test if not minicloud or cloud test or " +
                   "tenantLimits is not provided",
                   cloudRunning && tenantLimits != null);

        TableRequestLimits requestLimits =
            tenantLimits.getStandardTableLimits();
        final int numFields = requestLimits.getColumnsPerTable();
        final TableLimits tableLimits = new TableLimits(1000, 1000, 1);

        String ddl = makeCreateTableDDL("columnLimitOK", numFields);
        tableOperation(handle, ddl, tableLimits, State.ACTIVE, 20_000);

        /*
         * Create a table with column number exceeded limit, it is expected
         * to fail.
         */
        ddl = makeCreateTableDDL("bad", numFields + 1);
        try {
            tableOperation(handle, ddl, tableLimits, State.ACTIVE, 20_000);
            fail("Creating table with the number of columns that exceeded " +
                 "the limit should have fail");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Enforcing the # of columns limit via alter table is only
         * supported by the "real" cloud -- minicloud or level 3 tests
         */
        if (cloudRunning) {
            /*
             * Add one more field to a table which already has max number of
             * column, it is expected to fail.
             */
            ddl = "ALTER TABLE columnLimitOK(ADD nc1 INTEGER)";
            try {
                tableOperation(handle, ddl, null, State.ACTIVE, 20_000);
                fail("Adding an new field to a table with max number of " +
                     "columns should have failed.");
            } catch (IllegalArgumentException iae) {
            }

            /* Drop a column, then add an new column. */
            ddl = "ALTER TABLE columnLimitOK(drop c0)";
            tableOperation(handle, ddl, null, State.ACTIVE, 20_000);
            ddl = "ALTER TABLE columnLimitOK(ADD nc1 INTEGER)";
            tableOperation(handle, ddl, null, State.ACTIVE, 20_000);
        }
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
        assumeTrue(useMiniCloud);

        TableRequestLimits requestLimits =
            tenantLimits.getStandardTableLimits();
        final int maxRead = requestLimits.getTableReadUnits();
        final int maxWrite = requestLimits.getTableWriteUnits();
        final int maxSize = requestLimits.getTableSize();

        /* TODO: when per-tenant limits are available get them */
        final int maxTenantRead = tenantLimits.getTenantReadUnits();
        final int maxTenantWrite = tenantLimits.getTenantWriteUnits();
        final int maxTenantSize = tenantLimits.getTenantSize();

        TableLimits limits = new TableLimits(maxRead + 1, 1, 1);
        String ddl = makeCreateTableDDL("limits", 2);

        assertDeploymentException(ddl, limits, null, "read");

        limits = new TableLimits(1, maxWrite + 1, 1);
        assertDeploymentException(ddl, limits, null, "write");

        limits = new TableLimits(1, 1, maxSize + 1);
        assertDeploymentException(ddl, limits, null, "size");

        /* make a table and try to evolve it past read limit */
        limits = new TableLimits(maxRead, maxWrite, maxSize);
        tableOperation(handle, ddl, limits, State.ACTIVE, 20_000);

        limits = new TableLimits(maxRead+1, maxWrite, maxSize);
        assertDeploymentException(null, limits, "limits", "read");

        /*
         * Test per-tenant limits by trying to create another table. If it's one
         * table this only works if the per-table limit is >= 1/2 of the
         * tenant limit. See ProxyTestBase's TenantLimits.
         */
        limits = new TableLimits(maxTenantRead - maxRead + 1, 1, 1);
        ddl = makeCreateTableDDL("limits1", 2);
        assertDeploymentException(ddl, limits, null,
                                  new String[] {"read", "tenant"});

        limits = new TableLimits(1, maxTenantWrite - maxWrite + 1, 1);
        assertDeploymentException(ddl, limits, null,
                                  new String[] {"write", "tenant"});

        limits = new TableLimits(1, 1, maxTenantSize - maxSize + 1);
        assertDeploymentException(ddl, limits, null,
                                  new String[] {"size", "tenant"});
    }

    /**
     * Use a special tier and tenant for this test. Otherwise the
     * operations that happened in other tests get involved in this
     * test case.
     */
    @Test
    public void testOperationLimits() {
        /*
         * This test needs adjust the rate of ddl execution and table limits
         * reduction, it is not applicable in cloud test
         */
        assumeTrue(useMiniCloud);

        /*
         * In order to isolate this test from others as well as allowing it
         * to run more than once/day, use a timestamp on the test tier to
         * make it unique.
         */
        String suffix = Long.toString(System.currentTimeMillis());
        final String limitsTenant = "LimitsTenant." + suffix;
        final int ddlRate = 4;
        final int redRate = 2;

        /*
         * Throttling exceptions are retry-able so don't retry to get the
         * right exception (vs timeout)
         */
        handle = configNoRetryHandle(limitsTenant);

        /* run few operations ahead to warm up security cache */
        if (isSecure()) {
            try {
                createTable(limitsTenant);
                handle.getTable(new GetTableRequest()
                    .setTableName(tableName));
            } catch (TableNotFoundException e) {
            }
        }

        int origDDLRate = tenantLimits.getDdlRequestsRate();
        int origReductionRate = tenantLimits.getTableLimitReductionsRate();
        tenantLimits.setDdlRequestsRate(ddlRate);
        tenantLimits.setTableLimitReductionsRate(redRate);
        addTier(limitsTenant, tenantLimits);
        try {
            for (int i = 0; i < 10; i++) {
                try {
                    createTable(limitsTenant);
                    if (i == 5) {
                        fail("DDL operation should have failed");
                    }
                } catch (OperationThrottlingException e) {
                    // success
                    break;
                }
            }
            /* reset DDL rate to avoid failure for that reason */
            tenantLimits.setDdlRequestsRate(origDDLRate);
            addTier(limitsTenant, tenantLimits);

            /*
             * 2 reductions are allowed. The 3rd should throw
             */
            TableLimits limits = new TableLimits(10000, 20000, 50);
            tableOperation(handle, null, limits, limitsTenant, tableName,
                           null /* matchETag */, State.ACTIVE, 20_000);
            limits = new TableLimits(20000, 10000, 50);
            tableOperation(handle, null, limits, limitsTenant, tableName,
                           null /* matchETag */, State.ACTIVE, 20_000);
            limits = new TableLimits(19000, 10000, 50);
            failLimitsChange(limits, limitsTenant);

            /* read */
            limits = new TableLimits(10000, 10000, 50);
            failLimitsChange(limits, limitsTenant);

            /* write */
            limits = new TableLimits(20000, 1000, 50);
            failLimitsChange(limits, limitsTenant);

            /* size */
            limits = new TableLimits(20000, 10000, 30);
            failLimitsChange(limits, limitsTenant);
        } finally {
            /* cleanup */
            tenantLimits.setTableLimitReductionsRate(origDDLRate);
            tenantLimits.setTableLimitReductionsRate(origReductionRate);
            handle = configHandle(getProxyURL());
            deleteTier(limitsTenant);
        }
    }

    /**
     * Test tenant max auto scaling table count and limits mode max change per
     * day.
     */
    @Test
    public void testAutoScalingTableLimits() {
        assumeTrue("Skip the test if not minicloud or cloud test or " +
                   "tenantLimits is not provided",
                   cloudRunning && tenantLimits != null);

        /*
         * Create 3 auto scaling tables.
         */
        final String CREATE_TABLEX = "create table if not exists testusersX(" +
            "id integer, name string, primary key(id))";
        final String CREATE_TABLE1 = "create table if not exists testusers1(" +
            "id integer, name string, primary key(id))";
        final String CREATE_TABLE2 = "create table if not exists testusers2(" +
            "id integer, name string, primary key(id))";
        TableResult tres = tableOperation(handle,
                                          CREATE_TABLEX,
                                          new TableLimits(20),
                                          20000);
        verifyAutoScalingResult(tres, 20);
        tres = tableOperation(handle,
                              CREATE_TABLE1,
                              new TableLimits(30),
                              20000);
        verifyAutoScalingResult(tres, 30);
        tres = tableOperation(handle,
                              CREATE_TABLE2,
                              new TableLimits(40),
                              20000);
        verifyAutoScalingResult(tres, 40);

        /*
         * Cannot create more than 3 auto scaling tables.
         */
        final String CREATE_TABLE3 = "create table if not exists testusers3(" +
            "id integer, name string, primary key(id))";
        tableOperation(handle,
                       CREATE_TABLE3,
                       new TableLimits(50),
                       null,
                       TableResult.State.ACTIVE,
                       TableLimitException.class);

        /*
         * Alter the table limits mode from AUTO_SCALING to PROVISIONED
         */
        tres = tableOperation(handle,
                              null,
                              new TableLimits(30, 40, 50),
                              "testusersX",
                              TableResult.State.ACTIVE,
                              20000);
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());
        assertEquals(30, tres.getTableLimits().getReadUnits());
        assertEquals(40, tres.getTableLimits().getWriteUnits());
        assertEquals(50, tres.getTableLimits().getStorageGB());
        assertEquals(PROVISIONED, tres.getTableLimits().getMode());

        if (tenantLimits.getBillingModeChangeRate() == 1) {
            /*
             * Cannot change the limits mode any more after reaching mode max
             * allowed changes per day.
             */
            tableOperation(handle,
                           null,
                           new TableLimits(10),
                           "testusersX",
                           TableResult.State.ACTIVE,
                           OperationThrottlingException.class);
        } else {
            /*
             * Alter the table limits mode from PROVISIONED to AUTO_SCALING
             */
            tres = tableOperation(handle,
                                  null,
                                  new TableLimits(10),
                                  "testusersX",
                                  TableResult.State.ACTIVE,
                                  20000);
            verifyAutoScalingResult(tres, 10);

            /*
             * Cannot change the limits mode any more after reaching mode max
             * allowed changes per day.
             */
            tableOperation(handle,
                           null,
                           new TableLimits(300, 400, 500),
                           "testusersX",
                           TableResult.State.ACTIVE,
                           OperationThrottlingException.class);
        }
    }

    private void verifyAutoScalingResult(TableResult tres, int tableSize) {
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());
        assertEquals(tenantLimits.getAutoScalingTableReadUnits(),
                     tres.getTableLimits().getReadUnits());
        assertEquals(tenantLimits.getAutoScalingTableWriteUnits(),
                     tres.getTableLimits().getWriteUnits());
        assertEquals(tableSize, tres.getTableLimits().getStorageGB());
        assertEquals(ON_DEMAND, tres.getTableLimits().getMode());
    }

    private void failLimitsChange(TableLimits limits, String compartmentId) {
        try {
            tableOperation(handle, null, limits, compartmentId, tableName,
                           null /* matchETag */, State.ACTIVE, 20_000);
            fail("Attempt at reduction should have failed");
        } catch (IllegalArgumentException | OperationThrottlingException iae) {
            // success
        }
    }

    private void assertDeploymentException(String statement,
                                           TableLimits limits,
                                           String name,
                                           String ... contains) {
        try {
            tableOperation(handle, statement, limits,
                           name, State.ACTIVE, 20_000);
            fail("Operation should have thrown");
        } catch (DeploymentException de) {
            for (String s : contains) {
                assertTrue(de.getMessage(), de.getMessage().contains(s));
            }
        }
    }

    private String makeCreateTableDDL(String name, int numFields) {
        final StringBuilder sb = new StringBuilder("CREATE TABLE ");
        sb.append(name);
        sb.append("(id INTEGER, ");
        for (int i = 0; i < numFields - 1; i++) {
            sb.append("c");
            sb.append(i);
            sb.append(" STRING, ");
        }
        sb.append("PRIMARY KEY(id))");
        return sb.toString();
    }

    private String makeName(int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            sb.append("a");
        }
        return sb.toString();
    }

    /* shared by test cases */
    private void createTable() {
        tableOperation(handle, createTableDDL,
                       new TableLimits(20000, 20000, 50),
                       TableResult.State.ACTIVE, 10000);
    }

    /* shared by test cases */
    private void createTable(String compartmentId) {
        tableOperation(handle, createTableDDL,
                       new TableLimits(20000, 20000, 50),
                       compartmentId, null /* tableName */,
                       null /* matchETag */, TableResult.State.ACTIVE, 10000);
    }

    /* shared by test cases */
    private void createKeyTable() {
        tableOperation(handle, createKeyTable1DDL,
                       new TableLimits(20000, 20000, 50),
                       TableResult.State.ACTIVE, 10000);
        tableOperation(handle, createKeyTableIndexDDL,
                       null,
                       TableResult.State.ACTIVE, 10000);
        tableOperation(handle, createKeyTable2DDL,
                       new TableLimits(20000, 20000, 50),
                       TableResult.State.ACTIVE, 10000);
    }
}
