/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;

import static oracle.nosql.driver.ops.TableLimits.CapacityMode.ON_DEMAND;
import static oracle.nosql.proxy.protocol.Protocol.TABLE_USAGE_NUMBER_LIMIT;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.net.URL;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import oracle.nosql.driver.Consistency;
import oracle.nosql.driver.DefinedTags;
import oracle.nosql.driver.Durability;
import oracle.nosql.driver.Durability.SyncPolicy;
import oracle.nosql.driver.FreeFormTags;
import oracle.nosql.driver.Durability.ReplicaAckPolicy;
import oracle.nosql.driver.IndexExistsException;
import oracle.nosql.driver.IndexNotFoundException;
import oracle.nosql.driver.KeySizeLimitException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.OperationThrottlingException;
import oracle.nosql.driver.ReadThrottlingException;
import oracle.nosql.driver.RowSizeLimitException;
import oracle.nosql.driver.TableExistsException;
import oracle.nosql.driver.TableNotFoundException;
import oracle.nosql.driver.TimeToLive;
import oracle.nosql.driver.Version;
import oracle.nosql.driver.WriteThrottlingException;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.GetIndexesRequest;
import oracle.nosql.driver.ops.GetIndexesResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.ListTablesRequest;
import oracle.nosql.driver.ops.ListTablesResult;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutRequest.Option;
import oracle.nosql.driver.ops.TableUsageResult.TableUsage;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.TableUsageRequest;
import oracle.nosql.driver.ops.TableUsageResult;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.ops.WriteMultipleResult.OperationResult;
import oracle.nosql.driver.ops.WriteRequest;
import oracle.nosql.driver.ops.WriteResult;
import oracle.nosql.driver.values.BinaryValue;
import oracle.nosql.driver.values.BooleanValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.JsonNullValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.NullValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.driver.values.TimestampValue;
import oracle.nosql.util.HttpResponse;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/*
 * The tests are ordered so that the zzz* test goes last so it picks up
 * DDL history reliably.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ProxyTest extends ProxyTestBase {
    /*
     * The time stamp string pattern used to parse start/end range parameter
     * of table usage.
     */
    private final static String TimestampPattern =
        "yyyy-MM-dd['T'HH:mm:ss[.SSS]]";
    private final static ZoneId UTCZone = ZoneId.of(ZoneOffset.UTC.getId());
    private final static DateTimeFormatter timestampFormatter =
        DateTimeFormatter.ofPattern(TimestampPattern).withZone(UTCZone);

    private final static int USAGE_TIME_SLICE_MS = 60 * 1000;
    final static int KEY_SIZE_LIMIT = rlimits.getPrimaryKeySizeLimit();
    final static int ROW_SIZE_LIMIT = rlimits.getRowSizeLimit();

    @Test
    public void smokeTest() {

        try {

            MapValue key = new MapValue().put("id", 10);

            MapValue value = new MapValue().put("id", 10).put("name", "jane");

            /* drop a table */
            TableResult tres = tableOperation(handle,
                                              "drop table if exists testusers",
                                              null, TableResult.State.DROPPED,
                                              20000);
            assertNotNull(tres.getTableName());
            assertTrue(tres.getTableState() == TableResult.State.DROPPED);
            assertNull(tres.getTableLimits());

            /* Create a table */
            tres = tableOperation(
                handle,
                "create table if not exists testusers(id integer, " +
                "name string, primary key(id))",
                new TableLimits(500, 500, 50),
                20000);
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());

            /* Create an index */
            tres = tableOperation(
                handle,
                "create index if not exists Name on testusers(name)",
                null,
                TableResult.State.ACTIVE,
                20000);
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());

            /* GetTableRequest for table that doesn't exist */
            try {
                GetTableRequest getTable =
                    new GetTableRequest()
                    .setTableName("not_a_table");
                tres = handle.getTable(getTable);
                fail("Table should not be found");
            } catch (TableNotFoundException tnfe) {}

            /* list tables */
            ListTablesRequest listTables =
                new ListTablesRequest();
            ListTablesResult lres = handle.listTables(listTables);
            /*
             * the test cases don't yet clean up so there may be additional
             * tables present, be flexible in this assertion.
             */
            assertTrue(lres.getTables().length >= 1);
            assertNotNull(lres.toString());

            /* getTableUsage. It won't return much in test mode */
            if (!onprem) {
                TableUsageRequest gtu = new TableUsageRequest()
                    .setTableName("testusers").setLimit(2)
                    .setEndTime(System.currentTimeMillis());
                TableUsageResult gtuRes = handle.getTableUsage(gtu);
                assertNotNull(gtuRes);
                assertNotNull(gtuRes.getUsageRecords());
            }

            /* PUT */
            PutRequest putRequest = new PutRequest()
                .setValue(value)
                .setTableName("testusers");

            PutResult res = handle.put(putRequest);
            assertNotNull("Put failed", res.getVersion());
            assertWriteKB(res);
            /* put a few more. set TTL to test that path */
            putRequest.setTTL(TimeToLive.ofHours(2));
            for (int i = 20; i < 30; i++) {
                value.put("id", i);
                handle.put(putRequest);
            }

            /*
             * Test ReturnRow for simple put of a row that exists. 2 cases:
             * 1. unconditional (will return info)
             * 2. if absent (will return info)
             */
            value.put("id", 20);
            putRequest.setReturnRow(true);
            PutResult pr = handle.put(putRequest);
            assertNotNull(pr.getVersion()); /* success */
            assertNotNull(pr.getExistingVersion());
            assertNotNull(pr.getExistingValue());
            assertTrue(pr.getExistingModificationTime() != 0);
            assertReadKB(pr);
            assertWriteKB(pr);

            putRequest.setOption(Option.IfAbsent);
            pr = handle.put(putRequest);
            assertNull(pr.getVersion()); /* failure */
            assertNotNull(pr.getExistingVersion());
            assertNotNull(pr.getExistingValue());
            assertTrue(pr.getExistingModificationTime() != 0);
            assertReadKB(pr);

            /* clean up */
            putRequest.setReturnRow(false);
            putRequest.setOption(null);

            /* GET */
            GetRequest getRequest = new GetRequest()
                .setKey(key)
                .setTableName("testusers");

            GetResult res1 = handle.get(getRequest);
            assertNotNull("Get failed", res1.getJsonValue());
            assertReadKB(res1);

            /* DELETE */
            DeleteRequest delRequest = new DeleteRequest()
                .setKey(key)
                .setTableName("testusers");

            DeleteResult del = handle.delete(delRequest);
            assertTrue("Delete failed", del.getSuccess());
            assertWriteKB(del);

            /* GET -- no row, it was removed above */
            getRequest.setTableName("testusers");
            res1 = handle.get(getRequest);
            assertNull(res1.getValue());
            assertReadKB(res1);

            /* GET -- no table */
            try {
                getRequest.setTableName("not_a_table");
                res1 = handle.get(getRequest);
                fail("Attempt to access missing table should have thrown");
            } catch (TableNotFoundException nse) {
                /* success */
            }

            /* PUT -- invalid row -- this will throw */
            try {
                value.remove("id");
                value.put("not_a_field", 1);
                res = handle.put(putRequest);
                fail("Attempt to put invalid row should have thrown");
            } catch (IllegalArgumentException iae) {
                /* success */
            }
        } catch (Exception e) {
            checkErrorMessage(e);
            e.printStackTrace();
            fail("Exception in test");
        }
    }

    @Test
    public void testCaseSensitivity()
        throws Exception {

        assumeKVVersion("testCaseSensitivity", 23, 3, 0);

        String ddl =
            "create table foo(id integer, S string, " +
            "primary key(Id, s))";
        tableOperation(
                handle,
                ddl,
                new TableLimits(500, 500, 50),
                TableResult.State.ACTIVE,
                20000);

        MapValue val = new MapValue().put("id", 1).put("s", "xyz");
        PutRequest putReq = new PutRequest()
            .setTableName("foo")
            .setValue(val);
        handle.put(putReq);

        GetRequest getRequest = new GetRequest()
            .setKey(new MapValue().put("id", 1).put("s", "xyz"))
            .setTableName("foo");
        GetResult res = handle.get(getRequest);
        /*
         * "Id" in pkey should have been turned into "id" and "s" to "S"
         */
        assertTrue(res.getValue().contains("id"));
        assertFalse(res.getValue().contains("Id"));
        assertTrue(res.getValue().contains("S"));
        assertFalse(res.getValue().contains("s"));
    }

    @Test
    public void testSimpleThroughput() throws Exception {

        assumeTrue(onprem == false);

        final String create = "create table testusersTp(id integer," +
            "name string, primary key(id))";

        /* Create a table */
        TableResult tres = tableOperation(
            handle,
            create,
            new TableLimits(500, 500, 50),
            20000);
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        MapValue value = new MapValue().put("id", 10).put("name", "jane");

        /*
         * Handle some Put cases
         */
        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName("testusersTp");


        PutResult res = handle.put(putRequest);
        assertNotNull("Put failed", res.getVersion());
        int origRead = res.getReadKB();
        int origWrite = res.getWriteKB();
        assertEquals(1, origWrite);
        assertEquals(0, origRead);

        /*
         * do a second put. Read should still be 0, write will increase
         * because it's an update, which counts the "delete"
         */
        res = handle.put(putRequest);
        int newRead = res.getReadKB();
        int newWrite = res.getWriteKB();
        assertEquals(2*origWrite, newWrite);
        assertEquals(0, newRead);

        /* set return row and expect read unit is 1 */
        putRequest.setReturnRow(true);
        res = handle.put(putRequest);
        newRead = res.getReadKB();
        newWrite = res.getWriteKB();
        assertEquals(2*origWrite, newWrite);
        assertEquals(1, newRead);

        /* make it ifAbsent and verify read and write consumption */
        putRequest.setOption(PutRequest.Option.IfAbsent);
        res = handle.put(putRequest);
        assertNull("Put should have failed", res.getVersion());
        /* use read units because in a write, readKB != readUnits */
        newRead = res.getReadUnits();
        newWrite = res.getWriteKB();
        /*
         * no write, but read is min read + record size, former for the version
         * and the latter for the value
         */
        assertEquals(0, newWrite);
        assertEquals(1 + origWrite, newRead);
    }

    /**
     * Test bad urls.
     */
    @Test
    public void testBadURL() throws Exception {
        /* bad port */
        tryURL(new URL("http", getProxyHost(), getProxyPort() + 7, "/"));
        /* bad host */
        tryURL(new URL("http", "nohost", getProxyPort(), "/"));
    }

    private void tryURL(URL url) {
        try {
            NoSQLHandleConfig config = new NoSQLHandleConfig(url);
            setHandleConfig(config);
            NoSQLHandle myhandle = getHandle(config);
            myhandle.close();
            fail("Connection should have failed");
        } catch (Exception e) {
            /* TODO: check for specific exception */
            /* success */
        }
    }

    /**
     * Test that throttling happens. This requires its own table and
     * handle.
     */
    @Test
    public void throttleTest() throws Exception {

        assumeTrue(onprem == false);

        /* this test is invalid with proxy-level rate limiting */
        assumeTrue(Boolean.getBoolean(PROXY_DRL_ENABLED_PROP) == false);

        /*
         * Create a new handle configured with no retries
         */
        NoSQLHandleConfig config = new NoSQLHandleConfig(getProxyEndpoint());
        setHandleConfig(config);

        /*
         * no retries
         */
        config.configureDefaultRetryHandler(0, 0);

        /*
         * Open the handle
         */
        NoSQLHandle myhandle = getHandle(config);

        MapValue key = new MapValue().put("id", 10);
        MapValue value = new MapValue().put("id", 10).put("name", "jane");

        /* Create a table with small throughput */
        TableResult tres = tableOperation(
            myhandle,
            "create table testusersThrottle(id integer, " +
            "name string, primary key(id))",
            new TableLimits(1, 1, 50),
            20000);
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());


        int num = 0;
        try {
            while (true) {
                /* PUT */
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName("testusersThrottle");
                myhandle.put(putRequest);
                num++;
                if (num > 1000) {
                    fail("Throttling exception should have been thrown");
                }
            }
        } catch (WriteThrottlingException wte) {
            checkErrorMessage(wte);
            /* success */
        }
        num = 0;
        try {
            while (true) {
                /* GET */
                GetRequest getRequest = new GetRequest()
                    .setKey(key)
                    .setTableName("testusersThrottle");
                GetResult gres = myhandle.get(getRequest);
                assertNotNull(gres.getValue());
                num++;
                if (num > 1000) {
                    fail("Throttling exception should have been thrown");
                }
            }
        } catch (ReadThrottlingException wte) {
            checkErrorMessage(wte);
            /* success */
        }
        /* Query based on single partition scanning */
        String query = "select * from testusersThrottle where id = 10";
        PrepareRequest prepReq = new PrepareRequest().setStatement(query);
        PrepareResult prepRes = handle.prepare(prepReq);
        assertTrue("Prepare statement failed",
                   prepRes.getPreparedStatement() != null);

        /* Query with size limit */
        num = 0;
        try {
            while (true) {
                QueryRequest queryReq = new QueryRequest()
                    .setPreparedStatement(prepRes)
                    .setMaxReadKB(3);
                /* Query */
                QueryResult res = myhandle.query(queryReq);
                assertTrue(res.getResults().size() == 1);
                num++;
                if (num > 1000) {
                    fail("Throttling exception should have been thrown");
                }
            }
        } catch (ReadThrottlingException rte) {
            checkErrorMessage(rte);
            /* success */
        }
        /* Alter table limit to increase read limit */
        tres = tableOperation(
            myhandle,
            null,
            new TableLimits(10, 200, 50),
            "testusersThrottle",
            TableResult.State.ACTIVE,
            20000);
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName("testusersThrottle");
        for (int i = 0; i < 200; i++) {
            value.put("id", 100 + i);
            myhandle.put(putRequest);
        }

        /* prepare should get throttled */
        try {
            for (int i = 0; i < 1000; i++) {
                query = "select * from testusersThrottle where name = \"jane\"";
                prepReq = new PrepareRequest().setStatement(query);
                prepRes = myhandle.prepare(prepReq);
            }
            fail("Throttling exception should have been thrown");
        } catch (Exception rte) {
            checkErrorMessage(rte);
        }

        /* Query based on all partitions scanning */
        /* Use the original handle to get throttling retries */
        query = "select * from testusersThrottle where name = \"jane\"";
        prepReq = new PrepareRequest().setStatement(query);
        prepRes = handle.prepare(prepReq);
        assertTrue("Prepare statement failed",
                   prepRes.getPreparedStatement() != null);

        /* Query with size limit */
        Thread.sleep(2000); /* try to avoid previous throttling */
        num = 0;
        try {
            QueryRequest queryReq = new QueryRequest()
                .setPreparedStatement(prepRes)
                .setMaxReadKB(20);
            do {
                /* Query */
                QueryResult res = myhandle.query(queryReq);

                /* it's possible to get 0 results and continuation key */
                num += res.getResults().size();
                if (num > 1000) {
                    fail("Throttling exception should have been thrown");
                }
            } while (!queryReq.isDone());
        } catch (ReadThrottlingException rte) {
            /* success */
            checkErrorMessage(rte);
        }
        assertTrue(num > 0);

        /* Query without limits */
        Thread.sleep(1000);
        num = 0;
        try {
            while (true) {
                QueryRequest queryReq = new QueryRequest()
                    .setPreparedStatement(prepRes);
                /* Query */
                QueryResult res = myhandle.query(queryReq);
                assertTrue(res.getResults().size() > 0);
                num++;
                if (num > 1000) {
                    fail("Throttling exception should have been thrown");
                }
            }
        } catch (ReadThrottlingException rte) {
            /* success */
            checkErrorMessage(rte);
        }
    }

    @Test
    public void droppedTableTest() throws Exception {
        assumeTrue("Skipping droppedTableTest for minicloud test",
                   !cloudRunning);

        final String CREATE_TABLE = "create table if not exists testDropped(" +
            "id integer, name string, primary key(id))";

        /* create a table */
        TableResult tres;
        tres = tableOperation(handle,
                              CREATE_TABLE,
                              new TableLimits(500, 500, 5),
                              20000);
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        /* start a background thread to drop the table after 5 seconds */
        Thread bg = new Thread(()-> {
            try {
                Thread.sleep(5_000);
            } catch (Exception e) {}
            tableOperation(handle, "drop table testDropped",
                           null, 20000);
        });
        bg.start();

        /*
         * Run gets/puts for 10 seconds. After about 5 seconds they should
         * start failing and consistently fail thereafter.
         * Note: this test is mainly designed to exercise the
         *       MetadataNotFoundException retry logic in the proxy
         */
        MapValue key = new MapValue().put("id", 10);
        MapValue value = new MapValue().put("id", 10).put("name", "jane");
        long endTimeMs = System.currentTimeMillis() + 10_000;
        while (true) {
            /* PUT */
            PutRequest putRequest = new PutRequest()
                .setValue(value)
                .setTimeout(1000)
                .setTableName("testDropped");

            try {
                handle.put(putRequest);
            } catch (TableNotFoundException tnfe) {
                /* expected */
            }

            /* GET */
            GetRequest getRequest = new GetRequest()
                .setKey(key)
                .setTimeout(2000)
                .setTableName("testDropped");

            try {
                handle.get(getRequest);
            } catch (TableNotFoundException tnfe) {
                /* expected */
            }

            if (System.currentTimeMillis() > endTimeMs) {
                break;
            }

            try {
                Thread.sleep(50);
            } catch (Exception e) {
                break;
            }
        }
        bg.join(10000);
    }

    @Test
    public void ddlTest() throws Exception {
        final String CREATE_TABLE = "create table if not exists testusersX(" +
            "id integer, name string, primary key(id))";
        final String CREATE_TABLE_NO_IFNOTEXISTS = "create table testusersX(" +
            "id integer, name string, primary key(id))";
        final String CREATE_TABLE_SCHEMA_DIFF =
            "create table if not exists testusersX(" +
            "id integer, name string, age integer, primary key(id))";

        final String BAD_DDL = "create tab x(id integer, " +
            "name string, primary key(id))";
        final String ALTER_DDL = "alter table testusersX(add name1 string)";
        final String BAD_ADD_INDEX =
            "create index idx on testusers_not_here(name)";
        final String BAD_ADD_TEXT_INDEX =
            "create fulltext index idxText on testusersX(name)";
        final String ADD_INDEX = "create index idx on testusersX(name)";
        final String DROP_INDEX = "drop index idx on testusersX";
        final String DROP_INDEX_IFX = "drop index if exists idx on testusersX";
        final String DROP_DDL = "drop table testusersX";

        TableResult tres;

        /*
         * Bad syntax
         */
        try {
            tres = tableOperation(handle,
                                  BAD_DDL,
                                  null,
                                  20000);
            fail("Expected IAE");
        } catch (IllegalArgumentException iae) {
            checkErrorMessage(iae);
        }

        /*
         * Table doesn't exist
         */
        tres = tableOperation(handle,
                              BAD_ADD_INDEX,
                              TableResult.State.ACTIVE,
                              TableNotFoundException.class);

        /*
         * create the table to alter it
         */
        tres = tableOperation(handle,
                              CREATE_TABLE,
                              new TableLimits(5000, 5000, 50),
                              20000);

        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        /*
         * Table already exists.
         */
        tres = tableOperation(handle,
                              CREATE_TABLE_NO_IFNOTEXISTS,
                              new TableLimits(5000, 5000, 50),
                              null, TableResult.State.ACTIVE,
                              TableExistsException.class);

        /*
         * "create table if not exists" should not check schema for existing
         * table.
         */
        tres = tableOperation(handle,
                              CREATE_TABLE_SCHEMA_DIFF,
                              new TableLimits(5000, 5000, 50),
                              20000);

        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        /*
         * Add index
         */
        tres = tableOperation(handle,
                              ADD_INDEX,
                              null,
                              20000);

        /*
         * Index already exists.
         */
        tres = tableOperation(handle,
                              ADD_INDEX,
                              TableResult.State.ACTIVE,
                              IndexExistsException.class);

        /*
         * FullText index is not allowed.
         */
        tres = tableOperation(handle,
                              BAD_ADD_TEXT_INDEX,
                              TableResult.State.ACTIVE,
                              IllegalArgumentException.class);

        /*
         * Drop index
         */
        tres = tableOperation(handle,
                              DROP_INDEX,
                              null,
                              20000);

        /*
         * Drop index again, using if exists
         */
        tres = tableOperation(handle,
                              DROP_INDEX_IFX,
                              null,
                              20000);

        /*
         * Alter the table
         */
        tres = tableOperation(handle,
                              ALTER_DDL,
                              null,
                              20000);
        /*
         * Alter the table limits
         */
        if (!onprem) {
            tres = tableOperation(handle,
                                  null,
                                  new TableLimits(50, 50, 10),
                                  "testusersX",
                                  TableResult.State.ACTIVE,
                                  20000);
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());
            assertEquals(50, tres.getTableLimits().getReadUnits());
        }

        /*
         * drop the table
         * NOTE: this sequence may, or may not work with the real Tenant
         * Manager. The local/test version only updates the state of its
         * cached tables on demand. I.e. if a table is dropped and never
         * "gotten" again, it could live in the cache in DROPPING state for
         * a very long time. TODO: time out cache entries.
         */
        tres = tableOperation(handle,
                              DROP_DDL,
                              null,
                              20000);

        /*
         * the table should be gone now
         */
        try {
            GetTableRequest getTable =
                new GetTableRequest().setTableName("testusersX");
            tres = handle.getTable(getTable);
            fail("Table should not be found");
        } catch (TableNotFoundException tnfe) {
            checkErrorMessage(tnfe);
        }

        /*
         * Unsupported ddl operations
         */

        /* CRATE USER */
        tres = tableOperation(handle,
                              "CREATE USER guest IDENTIFIED BY \"welcome\"",
                              TableResult.State.ACTIVE,
                              IllegalArgumentException.class);
        /* ALTER USER */
        tres = tableOperation(handle,
                              "ALTER USER guest ACCOUNT LOCK",
                              TableResult.State.ACTIVE,
                              IllegalArgumentException.class);

        /* DROP USER */
        tres = tableOperation(handle,
                              "DROP USER guest",
                              TableResult.State.ACTIVE,
                              IllegalArgumentException.class);

        /* CREATE ROLE */
        tres = tableOperation(handle,
                              "CREATE ROLE employee",
                              TableResult.State.ACTIVE,
                              IllegalArgumentException.class);

        /* DROP ROLE */
        tres = tableOperation(handle,
                              "DROP ROLE employee",
                              TableResult.State.ACTIVE,
                              IllegalArgumentException.class);

        /* GRANT */
        tres = tableOperation(handle,
                              "GRANT readwrite TO USER guest",
                              TableResult.State.ACTIVE,
                              IllegalArgumentException.class);

        /* REVOKE */
        tres = tableOperation(handle,
                              "REVOKE readwrite FROM USER guest",
                              TableResult.State.ACTIVE,
                              IllegalArgumentException.class);

        /* SHOW */
        tres = tableOperation(handle,
                              "SHOW TABLES",
                              TableResult.State.ACTIVE,
                              IllegalArgumentException.class);

        /* DESCRIBE */
        tres = tableOperation(handle,
                              "DESCRIBE TABLE testusersX",
                              TableResult.State.ACTIVE,
                              IllegalArgumentException.class);

        /*
         * DML operation with TableRequest
         */
        tres = tableOperation(handle,
                              "SELECT * FROM testusersX",
                              TableResult.State.ACTIVE,
                              IllegalArgumentException.class);

        tres = tableOperation(handle,
                              "UPDATE testusersX SET name = \"test\" " +
                              "where id = 1",
                              TableResult.State.ACTIVE,
                              IllegalArgumentException.class);
    }

    @Test
    public void testGetProvisionedTable() throws Exception {
        TableLimits tableLimits = new TableLimits(10, 20, 1);
        testGetTable(tableLimits, tableLimits);
    }

    @Test
    public void testGetAutoScalingTable() throws Exception {
        if (cloudRunning && tenantLimits == null) {
            /* Skip this test if tenantLimits is not provided */
            return;
        }

        TableLimits tableLimits = new TableLimits(1);
        TableLimits expectedLimits;
        if (cloudRunning) {
            expectedLimits = new TableLimits(
                tenantLimits.getAutoScalingTableReadUnits(),
                tenantLimits.getAutoScalingTableWriteUnits(),
                tableLimits.getStorageGB(),
                ON_DEMAND);
        } else {
            expectedLimits = new TableLimits(
                Integer.MAX_VALUE - 1,
                Integer.MAX_VALUE - 1,
                tableLimits.getStorageGB(),
                ON_DEMAND);
        }
        testGetTable(tableLimits, expectedLimits);
    }

    private void testGetTable(TableLimits tableLimits,
                              TableLimits expectedLimits) throws Exception {
        final String tableName = "getTableTest";
        final String statement = "create table if not exists " + tableName +
                                 "(id integer, name string, primary key(id))";

        TableRequest tableRequest = new TableRequest()
                .setStatement(statement)
                .setTableLimits(tableLimits)
                .setTimeout(15000);

        TableResult tres = handle.tableRequest(tableRequest);
        TableLimits resultLimits = tres.getTableLimits();
        if (resultLimits != null) {
            assertEquals(expectedLimits.getStorageGB(),
                         resultLimits.getStorageGB());
            assertEquals(expectedLimits.getMode(),
                         resultLimits.getMode());
        }

        /*
         * Get table with operation id, the table name is invalid, expect to
         * get TableNotFoundException.
         */
        GetTableRequest getReq = new GetTableRequest()
                .setTableName("invalid")
                .setOperationId(tres.getOperationId());
        try {
            tres = handle.getTable(getReq);
            fail("Expect to get TableNotFoundException but not");
        } catch (IllegalArgumentException ex) {
            /* expected */
        }
        tres.waitForCompletion(handle, 20000, 1000);

        /*
         * Get table, check the schema text contains the table name.
         */
        getReq = new GetTableRequest().setTableName(tableName);
        tres = handle.getTable(getReq);
        assertTableOcid(tres.getTableId());
        assertNotNull(tres.getSchema());
        resultLimits = tres.getTableLimits();
        if (resultLimits != null) {
            assertEquals(expectedLimits.getReadUnits(),
                         resultLimits.getReadUnits());
            assertEquals(expectedLimits.getWriteUnits(),
                         resultLimits.getWriteUnits());
            assertEquals(expectedLimits.getStorageGB(),
                         resultLimits.getStorageGB());
            assertEquals(expectedLimits.getMode(),
                         resultLimits.getMode());
        }
        assertTrue(tres.getSchema().contains(tableName));
    }

    @Test
    public void testListTables() {
        final int numTables = 8;
        final String ddlFmt =
            "create table %s (id integer, name string, primary key(id))";
        final TableLimits tableLimits = new TableLimits(10, 10, 1);
        final String[] namePrefix = new String[] {"USERB", "userA", "userC"};

        if (onprem) {
            handle.doSystemRequest("create namespace NS001", 20000, 1000);
        }

        /*
         * create tables
         */
        TableResult tres;
        Set<String> nameSorted = new TreeSet<>();
        for (int i = 0; i < numTables; i++) {
            /* if onprem, create a mix of tables, some with namespaces */
            String tableName;
            if (onprem && (i % 2) == 1) {
                tableName = "NS001:" + namePrefix[i % namePrefix.length] + i;
            } else {
                tableName = namePrefix[i % namePrefix.length] + i;
            }
            tres = tableOperation(handle,
                                  String.format(ddlFmt, tableName),
                                  tableLimits,
                                  20000);
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());
            nameSorted.add(tableName);
        }
        List<String> nameSortedList = new ArrayList<String>(nameSorted);

        /*
         * List all tables
         */
        ListTablesRequest req = new ListTablesRequest();
        ListTablesResult res = handle.listTables(req);
        List<String> returnedTableNames = Arrays.asList(res.getTables());
        if (cloudRunning) {
            /* verify tables sorted by name */
            assertEquals(nameSortedList, returnedTableNames);
        } else {
            /* verify all added tables are in list */
            for (String name : nameSorted) {
                assertTrue("Table " + name + " missing from listTables",
                           returnedTableNames.contains(name));
            }
        }

        /*
         * List all tables with limit
         */
        int[] values = new int[] {0, 6, 2, 1};
        List<String> tables;
        for (int limit : values) {
            tables = doListTables(limit);
            if (cloudRunning) {
                /* verify tables sorted by name */
                assertEquals(nameSortedList, tables);
            }
        }
    }

    /* Run list tables with limit specified */
    private List<String> doListTables(int limit) {
        List<String> tables = new ArrayList<>();

        ListTablesRequest req = new ListTablesRequest();
        req.setLimit(limit);
        ListTablesResult res;
        while(true) {
            res = handle.listTables(req);
            if (res.getTables().length > 0) {
                tables.addAll(Arrays.asList(res.getTables()));
            }

            if (limit == 0 || res.getTables().length < limit) {
                break;
            }
            assertEquals(limit, res.getTables().length);
            req.setStartIndex(res.getLastReturnedIndex());
        }
        return tables;
    }

    /**
     * Tests serialization of types, including some coercion to schema
     * types in the proxy.
     */
    @Test
    public void typeTest() throws Exception {

        final String TABLE_CREATE =
            "create table if not exists Types( " +
            "id integer, " +
            "primary key(id), " +
            "longField long, " +
            "doubleField double, " +
            "stringField string, " +
            "numberField number, " +
            "enumField enum(a,b,c)" +
            ")";

        final String jsonString =
            "{" +
            "\"id\":1, " +
            "\"longField\": 123 ," + // int => long
            "\"doubleField\":4 ," + // int => double
            "\"stringField\":\"abc\" ," + // no coercion
            "\"numberField\":4.5 ," + // double => number
            "\"enumField\":\"b\"" + // string => enum
            "}";
        TableResult tres;

        tres = tableOperation(handle,
                              TABLE_CREATE,
                              new TableLimits(50, 50, 50),
                              TableResult.State.ACTIVE,
                              20000);

        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        PutRequest pr = new PutRequest().setValueFromJson(jsonString, null).
            setTableName("Types");
        PutResult pres = handle.put(pr);
        assertNotNull(pres.getVersion());
    }

    @Test
    public void recreateTest() throws Exception {
        final String CREATE_TABLE =
            "create table recreate( " +
            "id integer, " +
            "primary key(id), " +
            "name string)";
        final String DROP_TABLE = "drop table recreate";
        TableResult tres = tableOperation(handle,
                                          CREATE_TABLE,
                                          new TableLimits(50, 50, 50),
                                          20000);

        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        PutRequest pr = new PutRequest()
            .setTableName("recreate")
            .setValue(new MapValue().put("id", 1).put("name", "joe"));
        PutResult pres = handle.put(pr);
        assertNotNull(pres.getVersion());

        tres = tableOperation(handle,
                              DROP_TABLE,
                              null,
                              20000);

        tres = tableOperation(handle,
                              CREATE_TABLE,
                              new TableLimits(50, 50, 50),
                              20000);
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        pres = handle.put(pr);
        assertNotNull(pres.getVersion());
    }


    /**
     * This test does a lot of simple operations in a loop in multiple threads,
     * looking for HTTP transport problems. This is probably temporary.
     */
    @Test
    public void httpTest() {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        Collection<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
        for (int i = 0; i < 6; i++) {
            tasks.add(new Callable<Void>() {
                    @Override
                    public Void call() {
                        doHttpTest();
                        return null;
                    }
                });
        }
        try {
            List<Future<Void>> futures = executor.invokeAll(tasks);
            for(Future<Void> f : futures) {
                f.get();
            }
        } catch (Exception e) {
            fail("Exception: " + e);
        }
    }

    private void doHttpTest() {
        try {

            MapValue key = new MapValue().put("id", 10);
            MapValue value = new MapValue().put("id", 10).put("name", "jane");

            for (int i = 0; i < 10; i++) {
                try {
                    /* Create a table */
                    TableResult tres = tableOperation(
                        handle,
                        "create table if not exists testusers(id integer, " +
                        "name string, primary key(id))",
                        new TableLimits(500, 500, 50),
                        20000);
                    assertEquals(TableResult.State.ACTIVE, tres.getTableState());
                } catch (Exception e) {
                    System.out.println("httpTest: exception in Thread " +
                                       Thread.currentThread().getId() +
                                       " on attempt " + i + ": " +
                                       e);
                }
            }

            for (int i = 0; i < 100; i++) {
                /* PUT */
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName("testusers");

                PutResult res = handle.put(putRequest);
                assertNotNull("Put failed", res.getVersion());
                assertWriteKB(res);

                /* GET */
                GetRequest getRequest = new GetRequest()
                    .setKey(key)
                    .setTableName("testusers");

                GetResult res1 = handle.get(getRequest);
                assertNotNull("Get failed", res1.getJsonValue());
                assertReadKB(res1);
            }
        } catch (Exception e) {
            fail("Internal Exception: " + e);
        }
    }

    @Test
    public void testPutGetDelete() {

        final String tableName = "testusers";
        final int recordKB = 2;

        /* Create a table */
        TableResult tres = tableOperation(
            handle,
            "create table if not exists testusers(id integer, " +
            "name string, primary key(id))",
            new TableLimits(500, 500, 50),
            20000);
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        final String name = genString((recordKB - 1) * 1024);
        MapValue value = new MapValue().put("id", 10).put("name", name);
        MapValue newValue = new MapValue().put("id", 11).put("name", name);
        MapValue newValue1 = new MapValue().put("id", 12).put("name", name);
        MapValue newValue2 = new MapValue().put("id", 13).put("name", name);

        /* Durability will be ignored unless run with -Donprem=true */
        Durability dur = new Durability(SyncPolicy.WRITE_NO_SYNC,
                                        SyncPolicy.NO_SYNC,
                                        ReplicaAckPolicy.NONE);

        /* Put a row with empty table name: should get illegal argument */
        PutRequest putReq = new PutRequest()
            .setValue(value)
            .setDurability(dur)
            .setTableName("");
        try {
            handle.put(putReq);
            fail("expected illegal argument exception on empty table name");
        } catch (IllegalArgumentException iae) {
            /* success */
        }

        /* Put a row */
        putReq = new PutRequest()
            .setValue(value)
            .setDurability(dur)
            .setTableName(tableName);
        PutResult putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       true  /* shouldSucceed */,
                       false /* rowPresent */,
                       null  /* expPrevValue */,
                       null  /* expPrevVersion */,
                       false, /* modtime should be zero */
                       recordKB,
                       false /* put overWrite */ );

        /* Put a row again with SetReturnRow(false).
         * expect no row returned
         */
        putReq.setReturnRow(false);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       true  /* shouldSucceed */,
                       false /* rowPresent */,
                       null  /* expPrevValue */,
                       null  /* expPrevVersion */,
                       false, /* modtime should be zero */
                       recordKB,
                       true /* put over write */);
       Version oldVersion = putRes.getVersion();

        /*
         * Put row again with SetReturnRow(true),
         * expect existing row returned.
         */
        putReq.setReturnRow(true);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       true /* shouldSucceed */,
                       true /* rowPresent */,
                       value /* expPrevValue */,
                       oldVersion /* expPrevVersion */,
                       true, /* modtime should be zero */
                       recordKB,
                       true /* put overWrite */);
        oldVersion = putRes.getVersion();

        /*
         * Put a new row with SetReturnRow(true),
         * expect no existing row returned.
         */
        putReq = new PutRequest()
            .setValue(newValue)
            .setDurability(dur)
            .setTableName(tableName)
            .setReturnRow(true);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       true /* shouldSucceed */,
                       false /* rowPresent */,
                       null /* expPrevValue */,
                       null /* expPrevVersion */,
                       false, /* modtime should be zero */
                       recordKB,
                       false /* put overWrite */);

        /* PutIfAbsent an existing row, it should fail */
        putReq = new PutRequest()
            .setOption(Option.IfAbsent)
            .setValue(value)
            .setDurability(dur)
            .setTableName(tableName);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       false /* shouldSucceed */,
                       false  /* rowPresent */,
                       null  /* expPrevValue */,
                       null  /* expPrevVersion */,
                       false, /* modtime should be zero */
                       recordKB,
                       false /* put overWrite */);
        /*
         * PutIfAbsent fails + SetReturnRow(true),
         * return existing value and version
         */
        putReq.setReturnRow(true);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       false /* shouldSucceed */,
                       true  /* rowPresent */,
                       value /* expPrevValue */,
                       oldVersion /* expPrevVersion */,
                       true, /* modtime should be recent */
                       recordKB,
                       false /* put overWrite */);

        /* PutIfPresent an existing row, it should succeed */
        putReq = new PutRequest()
            .setOption(Option.IfPresent)
            .setValue(value)
            .setDurability(dur)
            .setTableName(tableName);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       true /* shouldSucceed */,
                       false /* rowPresent */,
                       null /* expPrevValue */,
                       null /* expPrevVersion */,
                       false, /* modtime should be zero */
                       recordKB,
                       false /* put overWrite */);
        oldVersion = putRes.getVersion();

        /*
         * PutIfPresent succeed + SetReturnRow(true),
         * expect existing row returned.
         */
        putReq.setReturnRow(true);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       true /* shouldSucceed */,
                       true /* rowPresent */,
                       value /* expPrevValue */,
                       oldVersion /* expPrevVersion */,
                       true, /* modtime should be zero */
                       recordKB,
                       false /* put overWrite */);
        Version ifVersion = putRes.getVersion();

        /* PutIfPresent an new row, it should fail */
        putReq = new PutRequest()
            .setOption(Option.IfPresent)
            .setValue(newValue1)
            .setDurability(dur)
            .setTableName(tableName);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       false /* shouldSucceed */,
                       false /* rowPresent */,
                       null  /* expPrevValue */,
                       null  /* expPrevVersion */,
                       false, /* modtime should be zero */
                       recordKB,
                       false /* put overWrite */);
        /*
         * PutIfPresent fail + SetReturnRow(true),
         * expect no existing row returned.
         */
        putReq.setReturnRow(true);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       false /* shouldSucceed */,
                       false /* rowPresent */,
                       null  /* expPrevValue */,
                       null  /* expPrevVersion */,
                       false, /* modtime should be zero */
                       recordKB,
                       false /* put overWrite */);

        /* PutIfAbsent an new row, it should succeed */
        putReq = new PutRequest()
            .setOption(Option.IfAbsent)
            .setValue(newValue1)
            .setDurability(dur)
            .setTableName(tableName);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       true  /* shouldSucceed */,
                       false /* rowPresent */,
                       null  /* expPrevValue */,
                       null  /* expPrevVersion */,
                       false, /* modtime should be zero */
                       recordKB,
                       false /* put overWrite */);

        /* PutIfAbsent success + SetReturnRow(true) */
        putReq.setValue(newValue2).setReturnRow(true);
        putRes =  handle.put(putReq);
        checkPutResult(putReq, putRes,
                       true  /* shouldSucceed */,
                       false /* rowPresent */,
                       null  /* expPrevValue */,
                       null  /* expPrevVersion */,
                       false, /* modtime should be zero */
                       recordKB,
                       false /* put overWrite */);

        /*
         * PutIfVersion an existing row with unmatched version, it should fail.
         */
        putReq = new PutRequest()
            .setOption(Option.IfVersion)
            .setMatchVersion(oldVersion)
            .setValue(value)
            .setDurability(dur)
            .setTableName(tableName);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       false /* shouldSucceed */,
                       false  /* rowPresent */,
                       null  /* expPrevValue */,
                       null  /* expPrevVersion */,
                       false, /* modtime should be zero */
                       recordKB,
                       false /* put overWrite */);
        /*
         * PutIfVersion fails + SetReturnRow(true),
         * expect existing row returned.
         */
        putReq.setReturnRow(true);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       false /* shouldSucceed */,
                       true  /* rowPresent */,
                       value /* expPrevValue */,
                       ifVersion /* expPrevVersion */,
                       true, /* modtime should be recent */
                       recordKB,
                       false /* put overWrite */);

        /*
         * Put an existing row with matching version, it should succeed.
         */
        putReq = new PutRequest()
            .setOption(Option.IfVersion)
            .setMatchVersion(ifVersion)
            .setValue(value)
            .setDurability(dur)
            .setTableName(tableName);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       true /* shouldSucceed */,
                       false /* rowPresent */,
                       null /* expPrevValue */,
                       null /* expPrevVersion */,
                       false, /* modtime should be zero */
                       recordKB,
                       false /* put overWrite */);
        ifVersion = putRes.getVersion();
        /*
         * PutIfVersion succeed + SetReturnRow(true),
         * expect no existing row returned.
         */
        putReq.setMatchVersion(ifVersion).setReturnRow(true);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       true /* shouldSucceed */,
                       false /* rowPresent */,
                       null /* expPrevValue */,
                       null /* expPrevVersion */,
                       false, /* modtime should be zero */
                       recordKB,
                       false /* put overWrite */);
        Version newVersion = putRes.getVersion();

        /*
         * Put with IfVersion but no matched version is specified, put should
         * fail.
         */
        putReq = new PutRequest()
            .setOption(Option.IfVersion)
            .setValue(value)
            .setDurability(dur)
            .setTableName(tableName);
        try {
            putRes = handle.put(putReq);
            fail("Put with IfVersion should fail");
        } catch (IllegalArgumentException iae) {
            checkErrorMessage(iae);
        }

        /*
         * Get
         */

        /* Get a row with empty table name: should get illegal argument */
        MapValue key = new MapValue().put("id", 10);
        GetRequest getReq = new GetRequest()
            .setKey(key)
            .setTableName("");
        try {
            handle.get(getReq);
            fail("expected illegal argument exception on empty table name");
        } catch (IllegalArgumentException iae) {
            /* success */
        }

        /* Get a row */
        getReq = new GetRequest()
            .setKey(key)
            .setTableName(tableName);
        GetResult getRes = handle.get(getReq);
        checkGetResult(getReq, getRes,
                       true /* rowPresent*/,
                       value,
                       null, /* Don't check version if Consistency.EVENTUAL */
                       true, /* modtime should be recent */
                       recordKB);

        /* Get a row with ABSOLUTE consistency */
        getReq.setConsistency(Consistency.ABSOLUTE);
        getRes = handle.get(getReq);
        checkGetResult(getReq, getRes,
                       true /* rowPresent*/,
                       value,
                       newVersion,
                       true, /* modtime should be recent */
                       recordKB);

        /* Get non-existing row */
        key = new MapValue().put("id", 100);
        getReq = new GetRequest()
            .setKey(key)
            .setTableName(tableName);
        getRes = handle.get(getReq);
        checkGetResult(getReq, getRes,
                       false /* rowPresent*/,
                       null  /* expValue */,
                       null  /* expVersion */,
                       false, /* modtime should be zero */
                       recordKB);

        /* Get a row with ABSOLUTE consistency */
        getReq.setConsistency(Consistency.ABSOLUTE);
        getRes = handle.get(getReq);
        checkGetResult(getReq, getRes,
                       false /* rowPresent*/,
                       null  /* expValue */,
                       null  /* expVersion */,
                       false, /* modtime should be zero */
                       recordKB);

        /* Delete a row with empty table name: should get illegal argument */
        key = new MapValue().put("id", 10);
        DeleteRequest delReq = new DeleteRequest()
            .setKey(key)
            .setTableName("");
        try {
            handle.delete(delReq);
            fail("expected illegal argument exception on empty table name");
        } catch (IllegalArgumentException iae) {
            /* success */
        }

        /* Delete a row */
        delReq = new DeleteRequest()
            .setKey(key)
            .setTableName(tableName);
        DeleteResult delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
                          true  /* shouldSucceed */,
                          false  /* rowPresent */,
                          null  /* expPrevValue */,
                          null  /* expPrevVersion */,
                          false, /* modtime should be zero */
                          recordKB);

        /* Put the row back to store */
        putReq = new PutRequest().setValue(value).setTableName(tableName);
        putRes = handle.put(putReq);
        oldVersion = putRes.getVersion();
        assertNotNull(oldVersion);

        /* Delete succeed + setReturnRow(true), existing row returned. */
        delReq.setReturnRow(true);
        delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
                          true /* shouldSucceed */,
                          true /* rowPresent */,
                          value /* expPrevValue */,
                          oldVersion /* expPrevVersion */,
                          true, /* modtime should be zero */
                          recordKB);

        /* Delete fail + setReturnRow(true), no existing row returned. */
        delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
                          false /* shouldSucceed */,
                          false /* rowPresent */,
                          null  /* expPrevValue */,
                          null  /* expPrevVersion */,
                          false, /* modtime should be zero */
                          recordKB);

        /* Put the row back to store */
        putReq = new PutRequest().setValue(value).setTableName(tableName);
        putRes = handle.put(putReq);
        ifVersion = putRes.getVersion();

        /* DeleteIfVersion with unmatched version, it should fail */
        delReq = new DeleteRequest()
            .setMatchVersion(oldVersion)
            .setKey(key)
            .setTableName(tableName);
        delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
                          false /* shouldSucceed */,
                          false  /* rowPresent */,
                          null  /* expPrevValue */,
                          null  /* expPrevVersion */,
                          false, /* modtime should be zero */
                          recordKB);

        /*
         * DeleteIfVersion with unmatched version + setReturnRow(true),
         * the existing row returned.
         */
        delReq.setReturnRow(true);
        delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
                          false /* shouldSucceed */,
                          true  /* rowPresent */,
                          value /* expPrevValue */,
                          ifVersion /* expPrevVersion */,
                          true, /* modtime should be recent */
                          recordKB);

        /* DeleteIfVersion with matched version, it should succeed. */
        delReq = new DeleteRequest()
            .setMatchVersion(ifVersion)
            .setKey(key)
            .setTableName(tableName);
        delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
                          true  /* shouldSucceed */,
                          false  /* rowPresent */,
                          null  /* expPrevValue */,
                          null  /* expPrevVersion */,
                          false, /* modtime should be zero */
                          recordKB);

        /* Put the row back to store */
        putReq = new PutRequest().setValue(value).setTableName(tableName);
        putRes = handle.put(putReq);
        ifVersion = putRes.getVersion();

        /*
         * DeleteIfVersion with matched version + setReturnRow(true),
         * it should succeed but no existing row returned.
         */
        delReq.setMatchVersion(ifVersion).setReturnRow(true);
        delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
                          true  /* shouldSucceed */,
                          false  /* returnRow */,
                          null  /* expPrevValue */,
                          null  /* expPrevVersion */,
                          false, /* modtime should be zero */
                          recordKB);

        /* DeleteIfVersion with a key not existing, it should fail. */
        delReq = new DeleteRequest()
            .setMatchVersion(ifVersion)
            .setKey(key)
            .setTableName(tableName);
        delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
                          false /* shouldSucceed */,
                          false /* returnRow */,
                          null  /* expPrevValue */,
                          null  /* expPrevVersion */,
                          false, /* modtime should be zero */
                          recordKB);
        /*
         * DeleteIfVersion with a key not existing + setReturnRow(true),
         * it should fail and no existing row returned.
         */
        delReq.setReturnRow(true);
        delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
                          false /* shouldSucceed */,
                          false /* returnRow */,
                          null  /* expPrevValue */,
                          null  /* expPrevVersion */,
                          false, /* modtime should be zero */
                          recordKB);
    }

    /*
     * Test GetIndexesRequest.
     */
    @Test
    public void testGetIndexes() {

        /* Request to get all indexes */
        final GetIndexesRequest getAllIndexes = new GetIndexesRequest()
            .setTableName("testusers");

        /* Request to get index idxName */
        final GetIndexesRequest getIndexName = new GetIndexesRequest()
            .setTableName("testusers")
            .setIndexName("idxName");

        GetIndexesResult giRes;

        /* Table does not exist, expects to get TableNotFoundException */
        try {
            giRes = handle.getIndexes(getAllIndexes);
            fail("Expected to catch TableNotFoundException");
        } catch (TableNotFoundException tnfe) {
            /* Succeed */
            checkErrorMessage(tnfe);
        }

        /* Create table */
        TableResult tres = tableOperation(
            handle,
            "create table if not exists testusers(id integer, " +
            "name string, age integer, primary key(id))",
            new TableLimits(500, 500, 50),
            TableResult.State.ACTIVE,
            20000);
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        /* Get indexes, 0 index returned */
        giRes = handle.getIndexes(getAllIndexes);
        assertTrue(giRes.getIndexes().length == 0);

        /* Get index idxName, expects to get IndexNotFoundException */
        try {
            giRes = handle.getIndexes(getIndexName);
            fail("Expected to caught IndexNotFoundException but not");
        } catch (IndexNotFoundException infe) {
            /* Succeed */
            checkErrorMessage(infe);
        }

        /* Create indexes */
        tres = tableOperation(
            handle,
            "create index if not exists idxName on testusers(name)",
            null,
            TableResult.State.ACTIVE,
            20000);
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        tres = tableOperation(
            handle,
            "create index if not exists idxAgeName on testusers(age, name)",
            null,
            TableResult.State.ACTIVE,
            20000);
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        /* Get indexes, 2 indexes returned */
        giRes = handle.getIndexes(getAllIndexes);
        assertTrue(giRes.getIndexes().length == 2);

        /* Get idxName, 1 index returned */
        giRes = handle.getIndexes(getIndexName);
        assertTrue(giRes.getIndexes().length == 1);

        /* Invalid argument - miss table name */
        try {
            GetIndexesRequest badReq = new GetIndexesRequest();
            handle.getIndexes(badReq);
            fail("Expected to caught IllegalArgumentException " +
                 "because of missing table name");
        } catch (IllegalArgumentException iae) {
            /* Succeed */
            checkErrorMessage(iae);
        }
    }

    private void checkModTime(long modTime, boolean modTimeRecent) {
        if (modTimeRecent) {
            if (modTime < (System.currentTimeMillis() - 2000)) {
                fail("Expected modtime to be recent, got " + modTime);
            }
        } else {
            if (modTime != 0) {
                fail("Expected modtime to be zero, got " + modTime);
            }
        }
    }

    private void checkPutResult(PutRequest request,
                                PutResult result,
                                boolean shouldSucceed,
                                boolean rowPresent,
                                MapValue expPrevValue,
                                Version expPrevVersion,
                                boolean modTimeRecent,
                                int recordKB,
                                boolean putOverWrite) {
        if (shouldSucceed) {
            assertNotNull("Put should succeed", result.getVersion());
        } else {
            assertNull("Put should fail", result.getVersion());
        }
        checkExistingValueVersion(request, result, shouldSucceed, rowPresent,
                                  expPrevValue, expPrevVersion);

        checkModTime(result.getExistingModificationTime(), modTimeRecent);

        int[] expCosts = getPutReadWriteCost(request,
                                             shouldSucceed,
                                             rowPresent,
                                             recordKB,
                                             putOverWrite);

        if (onprem == false) {
            assertReadKB(result, expCosts[0], true /* isAbsolute */);
            assertWriteKB(result, expCosts[1]);
        }
    }

    private void checkDeleteResult(DeleteRequest request,
                                   DeleteResult result,
                                   boolean shouldSucceed,
                                   boolean rowPresent,
                                   MapValue expPrevValue,
                                   Version expPrevVersion,
                                   boolean modTimeRecent,
                                   int recordKB) {

        assertEquals("Delete should " + (shouldSucceed ? "succeed" : " fail"),
                     shouldSucceed, result.getSuccess());
        checkExistingValueVersion(request, result, shouldSucceed, rowPresent,
                                  expPrevValue, expPrevVersion);

        checkModTime(result.getExistingModificationTime(), modTimeRecent);

        int[] expCosts = getDeleteReadWriteCost(request,
                                                shouldSucceed,
                                                rowPresent,
                                                recordKB);

        if (onprem == false) {
            assertReadKB(result, expCosts[0], true /* isAbsolute */);
            assertWriteKB(result, expCosts[1]);
        }
    }

    private void checkGetResult(GetRequest request,
                                GetResult result,
                                boolean rowPresent,
                                MapValue expValue,
                                Version expVersion,
                                boolean modTimeRecent,
                                int recordKB) {


        if (rowPresent) {
            if (expValue != null) {
                assertEquals("Unexpected value", expValue, result.getValue());
            } else {
                assertNotNull("Unexpected value", expValue);
            }
            if (expVersion != null) {
                assertArrayEquals("Unexpected version",
                                  expVersion.getBytes(),
                                  result.getVersion().getBytes());
            } else {
                assertNotNull("Unexpected version", result.getVersion());
            }
        } else {
            assertNull("Unexpected value", expValue);
            assertNull("Unexpected version", result.getVersion());
        }

        checkModTime(result.getModificationTime(), modTimeRecent);

        final int minRead = getMinRead();
        int expReadKB = rowPresent ? recordKB : minRead;

        if (onprem == false) {
            assertReadKB(result, expReadKB,
                     (request.getConsistencyInternal() == Consistency.ABSOLUTE));
            assertWriteKB(result, 0);
        }
    }

    private void checkExistingValueVersion(WriteRequest request,
                                           WriteResult result,
                                           boolean shouldSucceed,
                                           boolean rowPresent,
                                           MapValue expPrevValue,
                                           Version expPrevVersion) {

        boolean hasReturnRow = rowPresent;
        if (hasReturnRow) {
            assertNotNull("PrevValue should be non-null",
                          result.getExistingValueInternal());
            if (expPrevValue != null) {
                assertEquals("Unexpected PrevValue",
                    expPrevValue, result.getExistingValueInternal());
            }
            assertNotNull("PrevVersion should be non-null",
                          result.getExistingVersionInternal());
            if (expPrevVersion != null) {
                assertNotNull(result.getExistingVersionInternal());
                assertArrayEquals("Unexpected PrevVersion",
                          expPrevVersion.getBytes(),
                          result.getExistingVersionInternal().getBytes());
            }
        } else {
            assertNull("PrevValue should be null",
                       result.getExistingValueInternal());
            assertNull("PrevVersion should be null",
                       result.getExistingVersionInternal());
        }
    }

    @Test
    public void testDataSizeLimit() {

        assumeTrue(onprem == false);

        final String tableName = "dataSizeTest";
        final String createTableDdl = "create table if not exists dataSizeTest" +
            "(sk String, data String, pk String, primary key(shard(sk), pk))";

        /* Create a table */
        TableResult tres = tableOperation(
            handle,
            createTableDdl,
            new TableLimits(500, 500, 50),
            TableResult.State.ACTIVE,
            20000);
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        /* Key size exceeds the limit */
        MapValue row = new MapValue();
        row.put("sk", genString(KEY_SIZE_LIMIT))
           .put("pk", "pk")
           .put("data", "");
        PutRequest putReq = new PutRequest()
            .setTableName(tableName)
            .setValue(row);
        try {
            handle.put(putReq);
            fail("Key size exceeds the limit, expect to fail");
        } catch (KeySizeLimitException e) {
            checkErrorMessage(e);
        }

        row = new MapValue();
        row.put("data", "data")
           .put("sk", "sk")
           .put("pk", genString(KEY_SIZE_LIMIT));
        putReq = new PutRequest().setTableName(tableName).setValue(row);
        try {
            handle.put(putReq);
            fail("Key size exceeds the limit, expect to fail");
        } catch (KeySizeLimitException e) {
            checkErrorMessage(e);
        }

        /* Data size exceeds the limit */
        row = new MapValue();
        row.put("sk", "sk")
           .put("pk", "pk")
           .put("data", genString(ROW_SIZE_LIMIT));
        putReq = new PutRequest().setTableName(tableName).setValue(row);
        try {
            handle.put(putReq);
            fail("Data size exceeds the limit, expect to fail");
        } catch (RowSizeLimitException e) {
            checkErrorMessage(e);
        }
    }

    /*
     * Test on put values(compatible or incompatible) to KV table non-numeric
     * primitive data types:
     *  o BOOLEAN
     *  o STRING
     *  o ENUM
     *  o TIMESTAMP
     *  o BINARY
     *  o FIXED_BINARY
     *  o JSON
     */
    @Test
    public void testNonNumericDataTypes() {
        final String tableName = "DataTypes";
        final String createTableDdl =
            "CREATE TABLE IF NOT EXISTS " + tableName + "(" +
                "id INTEGER, " +
                "bl BOOLEAN, " +
                "s STRING, " +
                "e ENUM(red, yellow, blue), " +
                "ts TIMESTAMP(9), " +
                "bi BINARY, " +
                "fbi BINARY(10), " +
                "json JSON," +
                "PRIMARY KEY(id)" +
            ")";

        final FieldValue intVal = new IntegerValue(1);
        final FieldValue boolVal = BooleanValue.trueInstance();
        final FieldValue strVal = new StringValue("oracle nosql");
        final FieldValue enumStrVal = new StringValue("red");

        final Timestamp ts = Timestamp.valueOf("2018-05-02 10:23:42.123");
        final FieldValue tsVal = new TimestampValue(ts);
        final FieldValue tsStrVal = new StringValue("2018-05-02T10:23:42.123");

        byte[] byte10 = genBytes(10);
        byte[] byte20 = genBytes(20);
        final FieldValue bi10Val = new BinaryValue(byte10);
        final FieldValue bi20Val = new BinaryValue(byte20);
        final FieldValue strByte10 =
            new StringValue(ProxySerialization.encodeBase64(byte10));
        final FieldValue strByte20 =
            new StringValue(ProxySerialization.encodeBase64(byte20));

        /* Create a table */
        TableResult tres = tableOperation(
            handle,
            createTableDdl,
            new TableLimits(500, 500, 50),
            TableResult.State.ACTIVE,
            20000);
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        FieldValue[] invalidValues;
        FieldValue[] validValues;
        String targetField;

        /* Boolean type */
        targetField = "bl";
        invalidValues = new FieldValue[] {intVal, tsVal, bi10Val};
        validValues = new FieldValue[] {boolVal, strVal};
        runPut(tableName, targetField, invalidValues, false);
        runPut(tableName, targetField, validValues, true);

        /* String type */
        targetField = "s";
        invalidValues = new FieldValue[] {intVal, boolVal, tsVal, bi10Val};
        validValues = new FieldValue[] {strVal};
        runPut(tableName, targetField, invalidValues, false);
        runPut(tableName, targetField, validValues, true);

        /* Emum type */
        targetField = "e";
        invalidValues =
            new FieldValue[] {intVal, boolVal, strVal, tsVal, bi10Val};
        validValues = new FieldValue[] {enumStrVal};
        runPut(tableName, targetField, invalidValues, false);
        runPut(tableName, targetField, validValues, true);

        /* Timestamp type */
        targetField = "ts";
        invalidValues = new FieldValue[] {intVal, boolVal, strVal, bi10Val};
        validValues = new FieldValue[] {tsVal, tsStrVal};
        runPut(tableName, targetField, invalidValues, false);
        runPut(tableName, targetField, validValues, true);

        /* Binary type */
        targetField = "bi";
        invalidValues = new FieldValue[] {intVal, boolVal, strVal, tsVal};
        validValues = new FieldValue[] {bi10Val, bi20Val, strByte10, strByte20};
        runPut(tableName, targetField, invalidValues, false);
        runPut(tableName, targetField, validValues, true);

        /* Fixed binary type */
        targetField = "fbi";
        invalidValues = new FieldValue[] {intVal, boolVal, strVal, tsVal,
                                          bi20Val, strByte20};
        validValues = new FieldValue[] {bi10Val, strByte10};
        runPut(tableName, targetField, invalidValues, false);
        runPut(tableName, targetField, validValues, true);

        /* JSON type */
        targetField = "json";
        invalidValues = new FieldValue[] {intVal, boolVal, strVal, tsVal,
                                          bi10Val};
        runPut(tableName, targetField, validValues, true);
    }

    /**
     * Test case-insensitivity of table names
     */
    @Test
    public void testCase() {
        final String create1 = "create table foo(i integer, primary key(i))";
        final String create2 = "create table Foo(i integer, primary key(i))";
        final String alter1 = "alter table FoO(add name string)";
        final String drop = "drop table fOo";

        final TableLimits limits = new TableLimits(500, 500, 50);
        tableOperation(handle, create1, limits, null,
                       TableResult.State.ACTIVE, null);
        tableOperation(handle, create2, limits, null,
                       TableResult.State.ACTIVE, TableExistsException.class);

        /* get with different case */
        GetTableRequest getTable =
            new GetTableRequest().setTableName("FoO");
        /* this will throw if the table isn't found */
        handle.getTable(getTable);

        /* alter with different case */
        tableOperation(handle, alter1, null, null,
                       TableResult.State.ACTIVE, null);

        tableOperation(handle, drop, null, null,
                       TableResult.State.DROPPED, null);
    }

    @Test
    public void testNullJsonNull() {
        final String createTable1 =
            "create table tjson(id integer, info json, primary key(id))";
        final String createTable2 =
            "create table trecord(id integer, " +
                                  "info record(name string, age integer), " +
                                  "primary key(id))";

        tableOperation(handle, createTable1, new TableLimits(10, 10, 1),
                       null, TableResult.State.ACTIVE, null);
        tableOperation(handle, createTable2, new TableLimits(10, 10, 1),
                       null, TableResult.State.ACTIVE, null);

        MapValue rowNull = new MapValue()
            .put("id", 0)
            .put("info",
                 new MapValue()
                     .put("name", NullValue.getInstance())
                     .put("age", 20));
        MapValue rowJsonNull = new MapValue()
            .put("id", 0)
            .put("info",
                 new MapValue()
                     .put("name", JsonNullValue.getInstance())
                     .put("age", 20));

        MapValue[] rows = new MapValue[] {rowNull, rowJsonNull};
        Map<String, MapValue> tableExpRows = new HashMap<String, MapValue>();
        tableExpRows.put("tjson", rowJsonNull);
        tableExpRows.put("trecord", rowNull);

        /*
         * Put rows with NullValue or JsonNullValue, they should be converted
         * to the right value for the target type.
         */
        for (Map.Entry<String, MapValue> e : tableExpRows.entrySet()) {
            String table = e.getKey();
            MapValue expRow = e.getValue();

            for (MapValue row : rows) {
                PutRequest putReq = new PutRequest()
                    .setTableName(table)
                    .setValue(row);
                PutResult putRet = handle.put(putReq);
                Version pVersion = putRet.getVersion();
                assertNotNull(pVersion);

                MapValue key = new MapValue().put("id", row.get("id"));
                GetRequest getReq = new GetRequest()
                    .setTableName(table)
                    .setConsistency(Consistency.ABSOLUTE)
                    .setKey(key);
                GetResult getRet = handle.get(getReq);
                assertEquals(expRow, getRet.getValue());
                assertNotNull(getRet.getVersion());
                assertTrue(Arrays.equals(pVersion.getBytes(),
                                         getRet.getVersion().getBytes()));
            }
        }

        /*
         * Query with variable for json field and set NullValue or
         * JsonNullValue to variable, the NullValue is expected to be converted
         * to JsonNullValue.
         */
        String query = "declare $name json;" +
            "select * from tjson t where t.info.name = $name";
        PrepareRequest prepReq = new PrepareRequest().setStatement(query);
        PrepareResult prepRet = handle.prepare(prepReq);
        PreparedStatement prepStmt = prepRet.getPreparedStatement();

        prepStmt.setVariable("$name", JsonNullValue.getInstance());
        QueryRequest queryReq = new QueryRequest()
            .setPreparedStatement(prepStmt);
        QueryResult queryRet = handle.query(queryReq);
        assertEquals(1, queryRet.getResults().size());
        assertEquals(rowJsonNull, queryRet.getResults().get(0));

        prepStmt.setVariable("$name", NullValue.getInstance());
        queryRet = handle.query(queryReq);
        assertEquals(0, queryRet.getResults().size());
    }

    /**
     * Tests that read-only operation rate throttling happens in the SC, in the
     * cloud.
     * Operations:
     *  getTable()
     *  getIndexes()
     *  getTableUsage()
     *  listTables()
     * These two use direct REST calls to the SC as they are not available in
     * the driver API at this time:
     *  getDdlHistory()
     *  getPeakUsage()
     */
    @Test
    public void testOpThrottling() {
        /*
         * This test need adjust the op rate using SC api, it is for minicloud
         * test only
         */
        assumeTrue("Skip this test if not minicloud test", useMiniCloud);

        final String create = "create table testOpThrottle(id integer," +
            "name string, primary key(id))";
        try {
            setOpThrottling(getTenantId(), DEFAULT_OP_THROTTLE);

            /*
             * create a table to use for further operations
             */
            tableOperation(handle, create,
                           new TableLimits(500, 500, 50),
                           TableResult.State.ACTIVE,
                           20000);

            /* getTable */
            try {
                GetTableRequest req =
                    new GetTableRequest().setTableName("testOpThrottle");
                for (int i = 0; i < 100; i++) {
                    handle.getTable(req);
                }
                fail("getTable should have been throttled");
            } catch (OperationThrottlingException e) {
                /* success */
                checkErrorMessage(e);
            }

            /* getIndexes */
            try {
                GetIndexesRequest req =
                    new GetIndexesRequest().setTableName("testOpThrottle");
                for (int i = 0; i < 100; i++) {
                    handle.getIndexes(req);
                }
                fail("getIndexes should have been throttled");
            } catch (OperationThrottlingException e) {
                /* success */
                checkErrorMessage(e);
            }

            /* getTableUsage */
            try {
                TableUsageRequest req =
                    new TableUsageRequest().setTableName("testOpThrottle");
                for (int i = 0; i < 100; i++) {
                    handle.getTableUsage(req);
                }
                fail("getTableUsage should have been throttled");
            } catch (OperationThrottlingException e) {
                /* success */
                checkErrorMessage(e);
            }

            /* listTables */
            try {
                ListTablesRequest req = new ListTablesRequest();
                for (int i = 0; i < 100; i++) {
                    handle.listTables(req);
                }
                fail("listTables should have been throttled");
            } catch (OperationThrottlingException e) {
                /* success */
                checkErrorMessage(e);
            }

            /* Peak usage (via direct REST to SC) */
            try {
                for (int i = 0; i < 100; i++) {
                    HttpResponse response = getPeakUsage(getTenantId(),
                                                         "testOpThrottle",
                                                         0, 0);
                    /* method returns error in response, not exception */
                    if (response.getStatusCode() != 200) {
                        assertEquals(429, response.getStatusCode());
                        assertTrue(response.getOutput()
                                   .contains("OperationRateLimitExceeded"));
                        throw new OperationThrottlingException("ignored");
                    }
                }
                fail("getPeakUsage should have been throttled");
            } catch (OperationThrottlingException e) {
                /* success */
                checkErrorMessage(e);
            }
        } finally {
            setOpThrottling(getTenantId(), NO_OP_THROTTLE);
        }
    }
    @Test
    public void testExactMatch() {
        final String tableName = "tMatch";
        final String createTable =
            "create table tMatch(id integer, name string, " +
            "age integer, primary key(id))";

        tableOperation(handle, createTable, new TableLimits(10, 10, 1),
                       null, TableResult.State.ACTIVE, null);

        /* use extra values, not exact match */
        MapValue value = new MapValue()
            .put("id", 1)
            .put("name", "myname")
            .put("age", 5)
            .put("extra", "foo");

        PutRequest putReq = new PutRequest()
            .setTableName(tableName)
            .setValue(value);
        PutResult putRet = handle.put(putReq);
        assertNotNull(putRet.getVersion());

        /* set exact match to true, this shoudl fail */
        putReq.setExactMatch(true);
        try {
            putRet = handle.put(putReq);
            fail("Put should have thrown IAE");
        } catch (Exception e) {
            /* success */
            checkErrorMessage(e);
        }

        /* test via query insert */
        String insertQ =
            "insert into tMatch(id, name, age) values(5, 'fred', 6)";
        QueryRequest qReq = new QueryRequest().setStatement(insertQ);
        QueryResult qRes = handle.query(qReq);
        for (MapValue res : qRes.getResults()) {
            assertEquals(1, res.get("NumRowsInserted").getInt());
        }

        /* try using prepared query */
        insertQ =
            "insert into tMatch(id, name, age) values(6, 'jack', 6)";
        PrepareRequest prepReq = new PrepareRequest().setStatement(insertQ);
        PrepareResult prepRet = handle.prepare(prepReq);
        PreparedStatement prepStmt = prepRet.getPreparedStatement();
        qReq = new QueryRequest()
            .setPreparedStatement(prepStmt);
        qRes = handle.query(qReq);
        for (MapValue res : qRes.getResults()) {
            assertEquals(1, res.get("NumRowsInserted").getInt());
        }
    }

    @Test
    public void testIdentityColumn() {
        final String tableName = "tIdentity";
        final String createTable1 =
            "create table tIdentity(id integer, id1 long generated always " +
            "as identity, name string, primary key(shard(id), id1))";

        tableOperation(handle, createTable1, new TableLimits(10, 10, 1),
                       null, TableResult.State.ACTIVE, null);

        MapValue value = new MapValue()
            .put("id", 1)
            .put("name", "myname");

        /* test single put */
        PutRequest putReq = new PutRequest()
            .setTableName(tableName)
            .setValue(value)
            .setIdentityCacheSize(5);
        PutResult putRet = handle.put(putReq);
        assertNotNull(putRet.getVersion());
        assertNotNull(putRet.getGeneratedValue());

        /* test WriteMultiple */
        WriteMultipleRequest wmReq = new WriteMultipleRequest();
        for (int i = 0; i < 10; i++) {
            PutRequest putRequest = new PutRequest()
                .setValue(value)
                .setIdentityCacheSize(i)
                .setTableName(tableName);
            /* cause last operation to fail and not return a generated value */
            if (i == 9) {
                putRequest.setOption(PutRequest.Option.IfPresent);
            }
            wmReq.add(putRequest, false);
        }

        WriteMultipleResult wmRes = handle.writeMultiple(wmReq);
        assertEquals(10, wmRes.getResults().size());
        int i = 0;
        int lastIdVal = -1;
        for (OperationResult result : wmRes.getResults()) {
            if (i++ == 9) {
                assertNull(result.getGeneratedValue());
            } else {
                assertNotNull(result.getGeneratedValue());
                if (lastIdVal < 0) {
                    lastIdVal = result.getGeneratedValue().getInt();
                } else {
                    assertTrue(result.getGeneratedValue().getInt() > lastIdVal);
                    lastIdVal = result.getGeneratedValue().getInt();
                }
            }
        }

        /*
         * Verify that a failed operation (without an exception) will not
         * return a generated value. The system may have generated one, but
         * it is not relevant in this case.
         */
        putReq.setOption(PutRequest.Option.IfPresent);
        putRet = handle.put(putReq);
        assertNull(putRet.getGeneratedValue());


        /* try an invalid case, use value from above, plus the id col */
        putReq.setValue(value.put("id1", 1));
        try {
            putRet = handle.put(putReq);
            fail("Exception should have been thrown on put");
        } catch (Exception e) {
            /* success */
            checkErrorMessage(e);
        }

        /* try an insert query */
        String insertQ = "insert into tIdentity(id, name) values(5, 'fred')";
        QueryRequest qReq = new QueryRequest().setStatement(insertQ);
        QueryResult qRes = handle.query(qReq);
        for (MapValue res : qRes.getResults()) {
            assertEquals(1, res.get("NumRowsInserted").getInt());
        }

        insertQ = "insert into tIdentity(id, name) values(5, 'jack')";

        PrepareRequest prepReq = new PrepareRequest().setStatement(insertQ);
        PrepareResult prepRet = handle.prepare(prepReq);
        PreparedStatement prepStmt = prepRet.getPreparedStatement();
        qReq = new QueryRequest()
            .setPreparedStatement(prepStmt);
        qRes = handle.query(qReq);
        for (MapValue res : qRes.getResults()) {
            assertEquals(1, res.get("NumRowsInserted").getInt());
        }
    }

    @Test
    public void testNameValidations() {
        assumeTrue(cloudRunning);

        String ddl = "create table if not exists " +
                     "%s(id integer, primary key(id))";
        TableLimits limits = new TableLimits(500, 500, 50);
        try {
            tableOperation(handle, String.format(ddl, "ocid_nosqltable_1"),
                           limits, 20000);
            fail("expect to fail");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("reserved keyword"));
        }

        try {
            tableOperation(handle, String.format(ddl, "ocid.nosqltable.1"),
                           limits, 20000);
            fail("expect to fail");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("alphanumeric values"));
        }

        try {
            tableOperation(handle, String.format(ddl, "oci"), limits, 20000);
            fail("expect to fail");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("reserved keyword"));
        }

        try {
            tableOperation(handle, String.format(ddl, "OCID"), limits, 20000);
            fail("expect to fail");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("reserved keyword"));
        }

        try {
            tableOperation(handle, String.format(ddl, "Foo-ta"), limits, 20000);
            fail("expect to fail");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("alphanumeric values"));
        }

        try {
            tableOperation(handle, String.format(ddl, "7oo"), limits, 20000);
            fail("expect to fail");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains(
                       "Identifiers must start with a letter"));
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 257; i++) {
            sb.append("o");
        }
        String longName = sb.toString();
        try {
            tableOperation(handle, String.format(ddl, longName), limits, 20000);
            fail("expect to fail");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("256 characters"));
        }

        /* Create a table */
        TableResult tres = tableOperation(
            handle,
            "create table if not exists user(id integer, " +
            "name string, primary key(id))",
            new TableLimits(500, 500, 50),
            20000);
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        ddl = "create index if not exists %s on user(name)";
        try {
            tableOperation(handle, String.format(ddl, "ocid_nosqltable_1"),
                           null, TableResult.State.ACTIVE, 20000);
            fail("expect to fail");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("reserved keyword"));
        }
        try {
            tableOperation(handle, String.format(ddl, "ocid.nosqltable.1"),
                           null, TableResult.State.ACTIVE, 20000);
            fail("expect to fail");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("alphanumeric values"));
        }
        try {
            tableOperation(handle, String.format(ddl, "oci"), null,
                           TableResult.State.ACTIVE, 20000);
            fail("expect to fail");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("reserved keyword"));
        }
        try {
            tableOperation(handle, String.format(ddl, "OCID"), null,
                           TableResult.State.ACTIVE, 20000);
            fail("expect to fail");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("reserved keyword"));
        }
        try {
            tableOperation(handle, String.format(ddl, "foo-index"), null,
                           TableResult.State.ACTIVE, 20000);
            fail("expect to fail");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("alphanumeric values"));
        }
        try {
            tableOperation(handle, String.format(ddl, "foo.index"), null,
                           TableResult.State.ACTIVE, 20000);
            fail("expect to fail");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("alphanumeric values"));
        }
        try {
            tableOperation(handle, String.format(ddl, "7oo"), null,
                           TableResult.State.ACTIVE, 20000);
            fail("expect to fail");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains(
                "Identifiers must start with a letter"));
        }
        sb = new StringBuilder();
        for (int i = 0; i < 65; i++) {
            sb.append("o");
        }
        longName = sb.toString();
        try {
            tableOperation(handle, String.format(ddl, longName), null,
                           TableResult.State.ACTIVE, 20000);
            fail("expect to fail");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("64 char"));
        }
    }

    @Test
    public void testGetTableUsage() {
        assumeTrue(cloudRunning);

        final String tableName = "testGetTableUsage";
        final String createTableDDL = "create table " + tableName +
                "(id integer, name string, primary key(id))";
        tableOperation(handle, createTableDDL,
                       new TableLimits(100, 100, 1),
                       TableResult.State.ACTIVE, 10000);

        final int numUsagesPerDay = (24 * 60 * 60 * 1000) / USAGE_TIME_SLICE_MS;
        final int maxNumUsagesPerRequest = TABLE_USAGE_NUMBER_LIMIT;

        String startDate = "2019-01-16";
        String endDate = "2019-01-16T23:59:00";
        int limit = 0;
        TableUsage[] usages;

        /* limit = 0, expect to get all the records of the day of 2019-01-16 */
        usages = runGetTableUsage(tableName, startDate, endDate, limit);
        /* the number of records per day = 24 * 60 */
        verifyTableUsages(usages, startDate, null, numUsagesPerDay);

        /* limit = 100, return the first 100  */
        usages = runGetTableUsage(tableName, startDate, endDate, 100);
        verifyTableUsages(usages, startDate, null, 100);

        /*
         * limit = 0, expect to get all records from startDate to
         * endDate(inclusively)
         */
        endDate = "2019-01-25T23:59:00";
        limit = 0;
        usages = runGetTableUsage(tableName, startDate, endDate, limit);
        verifyTableUsages(usages, startDate, null, numUsagesPerDay * 10);

        /*
         * limit = 5001, expect to get all records from startDate up
         * to 5001
         */
        limit = 5001;
        usages = runGetTableUsage(tableName, startDate, null, limit);
        verifyTableUsages(usages, startDate, null, limit);

        /*
         * limit = 0, expect to get all records from startDate
         */
        usages = runGetTableUsage(tableName, startDate, null, 0 /* limit */);
        verifyTableUsages(usages, startDate, null, maxNumUsagesPerRequest);

        endDate = "2019-01-26";
        limit = 100;
        /*
         * limit = 100, expect to get all records to 2020-01-26T00:00:00 up
         * to 100
         */
        usages = runGetTableUsage(tableName, null, endDate, limit);
        verifyTableUsages(usages, null, endDate, limit);

        /*
         * limit = 0, expect to get all records to endDate
         */
        usages = runGetTableUsage(tableName, null, endDate, 0 /* limit */);
        verifyTableUsages(usages, null, endDate, maxNumUsagesPerRequest);

        /*
         * startDate == null, endDate == null, limit = 0, expect to return
         * single record for the penultimate period
         */
        usages = runGetTableUsage(tableName, null /* startDate */,
                                  null /* endDate */, 0 /* limit */);
        assertEquals(1, usages.length);

        /*
         * Expect to get IAE if the specified limit exceeds the
         * TABLE_USAGE_NUMBER_LIMIT.
         */
        try {
            runGetTableUsage(tableName, startDate, null,
                             TABLE_USAGE_NUMBER_LIMIT + 1);
            fail("Expect to get IAE but not");
        } catch (IllegalArgumentException iae) {
            /* succeed */
        }

        /*
         * Invalid arguments: startTime, endTime and limit must not be a
         * negative value.
         */
        TableUsageRequest req = new TableUsageRequest()
                .setTableName(tableName);
        try {
            req.setStartTime(-1);
            handle.getTableUsage(req);
        } catch (IllegalArgumentException iae) {
            /* expected */
        }
        req.setStartTime(0);

        try {
            req.setEndTime(-1);
            handle.getTableUsage(req);
        } catch (IllegalArgumentException iae) {
            /* expected */
        }
        req.setEndTime(0);

        try {
            req.setLimit(-1);
            handle.getTableUsage(req);
        } catch (IllegalArgumentException iae) {
            /* expected */
        }
        req.setLimit(0);
    }

    @Test
    public void testLargeRow() {
        try {
            doLargeRow(handle, false);
        } catch (Exception e) {
            /* success */
        }
        try {
            doLargeRow(handle, true);
        } catch (Exception e) {
            /* success */
        }
    }

    /*
     * Test case to ensure that a small table with throughput of 1
     * can work. This was prompted by a proxy issue with throughput
     * of 1.
     */
    @Test
    public void testLowThroughput() throws Exception {
        final String createTable =
            "create table Users(id integer, name string, primary key(id))";

        /*
         * Read throughput 1
         */
        tableOperation(handle, createTable, new TableLimits(1, 1, 1),
                       null, TableResult.State.ACTIVE, null);

        /*
         * Put 3 rows
         */
        MapValue value = new MapValue()
            .put("id", 1)
            .put("name", "name1");
        PutRequest putRequest =
            new PutRequest().setValue(value).setTableName("Users");
        PutResult pres = handle.put(putRequest);
        assertNotNull(pres.getVersion());
        value.put("id", 2).put("name", "name2");
        pres = handle.put(putRequest);
        assertNotNull(pres.getVersion());
        value.put("id", 3).put("name", "name3");
        pres = handle.put(putRequest);
        assertNotNull(pres.getVersion());

        /*
         * Loop on queries. Success means completion without exceptions.
         */
        try {
            QueryRequest qReq =
                new QueryRequest().setStatement("select * from Users");
            for (int i = 0; i < 10; i++) {
                Thread.sleep(200);
                runQuery(qReq);
            }
        } catch (Exception e) {
            fail("test failed with exception " + e);
        }
    }

    /*
     * Tests support for flexible casting of types in the proxy where there is
     * no data loss, e.g.:
     *  String "1" to Integer (or other numeric)
     *  String "true" or "false" to Boolean
     *  Valid timestamp mappings
     */
    @Test
    public void testFlexibleMapping() throws Exception {
        final String createTable =
            "create table flex(id integer, primary key(id), " +
            "str string, " +
            "bool boolean, " +
            "int integer, " +
            "long long, " +
            "doub double, " +
            "num number, " +
            "ts timestamp(3))";

        /* JSON with various valid mappings */

        /* string value for numeric fields */
        final String strToNum = "{" +
            "\"id\": 1, \"str\": \"str\", \"bool\": true, \"int\": \"5\", " +
            "\"long\": \"456\", \"doub\":\"5.6\", \"num\":\"12345678910\", " +
            "\"ts\": \"2017-08-21T13:34:35.123\"" +
            "}";

        /* int timestamp */
        final String intToTs = "{" +
            "\"id\": 1, \"str\": \"str\", \"bool\": true, \"int\": 5, " +
            "\"long\": 456, \"doub\":5.6, \"num\":12345678910, " +
            "\"ts\": 12" +
            "}";

        /* long timestamp */
        final String longToTs = "{" +
            "\"id\": 1, \"str\": \"str\", \"bool\": true, \"int\": 5, " +
            "\"long\": 456, \"doub\":5.6, \"num\":12345678910, " +
            "\"ts\": 1234567891011" +
            "}";

        /* string boolean */
        final String strToBool = "{" +
            "\"id\": 1, \"str\": \"str\", \"bool\": \"true\", \"int\": 5, " +
            "\"long\": 456, \"doub\":5.6, \"num\":12345678910, " +
            "\"ts\": 1234567891011" +
            "}";

        final String[] mappings = {strToNum, intToTs, longToTs, strToBool};

        tableOperation(handle, createTable, new TableLimits(100, 100, 1),
                       null, TableResult.State.ACTIVE, null);

        for (String s : mappings) {
            PutRequest pr = new PutRequest().setValueFromJson(s, null).
                setTableName("flex");
            PutResult pres = handle.put(pr);
            assertNotNull(pres.getVersion());
        }
    }

    @Test
    public void testDropTable() {

        String tableName = "testDropTable";
        String createTable = "create table " + tableName +
                "(id integer, primary key(id))";
        TableLimits limits = new TableLimits(10, 10, 1);
        String dropTable = "drop table " + tableName;
        String dropTableIfExists = "drop table if exists " + tableName;

        /*
         * drop table that doesn't exist, should get TableNotFoundException
         */
        tableOperation(handle, dropTable, TableResult.State.DROPPED,
                        TableNotFoundException.class);

        /* drop table not existing with "if exists", should succeed */
        tableOperation(handle, dropTableIfExists, TableResult.State.DROPPED,
                       null);

        /* drop an existing table, should succeed */
        tableOperation(handle, createTable, limits, null,
                       TableResult.State.ACTIVE, null);
        tableOperation(handle, dropTable, TableResult.State.DROPPED, null);

        /* drop an existing table with "if exists", should succeed */
        tableOperation(handle, createTable, limits, null,
                       TableResult.State.ACTIVE, null);
        tableOperation(handle, dropTableIfExists, TableResult.State.DROPPED,
                       null);
    }

    @Test
    public void testTableTags() {
        assumeTrue(cloudRunning);

        final int waitMs = 20000;
        final int delayMs = 300;

        /*
         * Create table with definedTags and freeFormTags
         */
        String tableName = "testTableTags";
        String ddl = "create table " + tableName +
                     "(id integer, primary key(id))";
        TableLimits limits = new TableLimits(10, 10, 1);

        DefinedTags dtags = new DefinedTags();
        dtags.addTag(DEFINED_TAG_NAMESPACE, DEFINED_TAG_PROP, "v0");

        FreeFormTags ftags = new FreeFormTags();
        ftags.addTag("scope", "test");
        ftags.addTag("test", "function");

        TableRequest req = new TableRequest()
                .setStatement(ddl)
                .setTableLimits(limits)
                .setDefinedTags(dtags)
                .setFreeFormTags(ftags);

        TableResult tr = handle.tableRequest(req);
        tr.waitForCompletion(handle, waitMs, delayMs);

        tr = getTable(tableName, handle);
        assertNotNull(tr);
        assertTagsEquals(dtags, tr.getDefinedTags());
        assertTagsEquals(ftags, tr.getFreeFormTags());

        /*
         * Update tags
         */

        dtags.addTag(DEFINED_TAG_NAMESPACE, DEFINED_TAG_PROP, "v1");

        ftags = new FreeFormTags();
        ftags.addTag("scope", "cloudtest");
        ftags.addTag("test", "stress");

        req = new TableRequest()
                .setTableName(tableName)
                .setFreeFormTags(ftags)
                .setDefinedTags(dtags);
        tr = handle.tableRequest(req);
        tr.waitForCompletion(handle, waitMs, delayMs);

        tr = getTable(tableName, handle);
        assertNotNull(tr);
        assertTagsEquals(dtags, tr.getDefinedTags());
        assertTagsEquals(ftags, tr.getFreeFormTags());

        /*
         * Clear tags
         */
        dtags = new DefinedTags();
        ftags = new FreeFormTags();
        req = new TableRequest()
                .setTableName(tableName)
                .setFreeFormTags(ftags)
                .setDefinedTags(dtags);
        tr = handle.tableRequest(req);
        tr.waitForCompletion(handle, waitMs, delayMs);

        tr = getTable(tableName, handle);
        assertNotNull(tr);
        assertTagsEquals(dtags, tr.getDefinedTags());
        assertTagsEquals(ftags, tr.getFreeFormTags());
    }

    /* Test ddls ops using matchETag */
    @Test
    public void testMatchETag() {
        assumeTrue(cloudRunning);

        String comptId = getCompartmentId();
        String tableName = "testMatchETag";
        String ddl = "create table " + tableName +
                     "(id integer, primary key(id))";
        int waitMs = 20000;

        TableResult ret;
        String etag;
        String oldETag;

        /* create table */
        ret = tableOperation(handle, ddl, new TableLimits(10, 10, 1),
                             TableResult.State.ACTIVE, waitMs);
        etag = ret.getMatchETag();

        /* alter table using valid ETag */
        ddl = "alter table " + tableName + "(add info json)";
        ret = tableOperation(handle, ddl, null /* limits */, comptId,
                             null /* tableName */, etag,
                             TableResult.State.ACTIVE, waitMs);
        oldETag = etag;
        etag = ret.getMatchETag();

        /* alter table using invalid ETag */
        try {
            ddl = "alter table " + tableName + "(drop info)";
            tableOperation(handle, ddl, null /* limits */, comptId,
                           null /* tableName */, oldETag,
                           TableResult.State.ACTIVE, waitMs);
            fail("expect to fail");
        } catch (IllegalArgumentException ex) {
        }

        /* update table limits using invalid ETag */
        TableLimits newLimits = new TableLimits(20, 20, 2);
        try {
            tableOperation(handle, null /* ddl */, newLimits, comptId,
                           tableName, oldETag, TableResult.State.ACTIVE,
                           waitMs);
            fail("expect to fail");
        } catch (IllegalArgumentException ex) {
        }

        /* update table limits using valid ETag */
        ret = tableOperation(handle, null /* ddl */, newLimits, comptId,
                             tableName, etag, TableResult.State.ACTIVE,
                             waitMs);
        assertEquals(newLimits.getReadUnits(),
                     ret.getTableLimits().getReadUnits());
        assertEquals(newLimits.getWriteUnits(),
                     ret.getTableLimits().getWriteUnits());
        assertEquals(newLimits.getStorageGB(),
                     ret.getTableLimits().getStorageGB());
        assertEquals(newLimits.getMode(),
                     ret.getTableLimits().getMode());
        oldETag = etag;
        etag = ret.getMatchETag();

        /* update tags using invalid ETag */
        FreeFormTags ftags = new FreeFormTags();
        ftags.addTag("scope", "test");

        TableRequest req = new TableRequest()
                .setTableName(tableName)
                .setMatchEtag(oldETag)
                .setFreeFormTags(ftags);
        ret = handle.tableRequest(req);
        try {
            ret.waitForCompletion(handle, waitMs, 500);
            fail("expect to fail");
        } catch (IllegalArgumentException ex) {
        }

        /* update tags using valid ETag */
        req.setMatchEtag(etag);
        ret = handle.tableRequest(req);
        ret.waitForCompletion(handle, waitMs, 500);
        ret = getTable(tableName, handle);
        assertTagsEquals(ftags, ret.getFreeFormTags());
        oldETag = etag;
        etag = ret.getMatchETag();

        /* drop table using invalid ETag */
        ddl = "drop table " + tableName;
        try {
            tableOperation(handle, ddl, null /* limits */, comptId,
                           null /* tableName */, oldETag,
                           TableResult.State.ACTIVE, waitMs);
            fail("Expect to fail");
        } catch (IllegalArgumentException ex) {
        }

        /* drop table using valid ETag */
        tableOperation(handle, ddl, null /* limits */, comptId,
                       null /* tableName */, etag,
                       TableResult.State.DROPPED, waitMs);
    }

    private void runQuery(QueryRequest req) {
        do {
            QueryResult res = handle.query(req);
            res.getResults();
        } while (!req.isDone());
    }

    private TableUsage[] runGetTableUsage(String tableName,
                                          String startTime,
                                          String endTime,
                                          int limit) {

        TableUsageRequest req = new TableUsageRequest()
                .setTableName(tableName);
        if (startTime != null) {
            req.setStartTime(startTime);
        }
        if (endTime != null) {
            req.setEndTime(endTime);
        }
        if (limit > 0) {
            req.setLimit(limit);
        }
        TableUsageResult res = handle.getTableUsage(req);
        return res.getUsageRecords();
    }

    private void verifyTableUsages(TableUsage[] usages,
                                   String startDate,
                                   String endDate,
                                   int expNum) {

        assertEquals(expNum, usages.length);

        final long delta = USAGE_TIME_SLICE_MS;;
        long startTime = 0;
        if (startDate != null) {
            startTime = parseTimestamp(startDate);
        } else if (endDate != null) {
            startTime = parseTimestamp(endDate) - (expNum - 1) * delta;
        }

        if (startTime > 0) {
            for (TableUsage usage : usages) {
                assertEquals(startTime, usage.getStartTime());
                startTime += delta;
            }
        }
    }

    /**
     * Parses the timestamp in string format to milliseconds since epoch.
     */
    private static long parseTimestamp(String timestampStr) {
        TemporalAccessor ta;
        try {
            ta = timestampFormatter.parse(timestampStr);
        } catch (DateTimeParseException dtpe) {
            throw new RuntimeException("Fail to parse timestamp string: " +
                                        dtpe.getMessage());
        }
        Instant instant;
        if (ta.isSupported(ChronoField.HOUR_OF_DAY)) {
            instant = Instant.from(ta);
        } else {
            instant = LocalDate.from(ta).atStartOfDay(UTCZone).toInstant();
        }
        return instant.toEpochMilli();
    }

    private void runPut(String tableName,
                        String targetField,
                        FieldValue[] values,
                        boolean expSucceed) {
        MapValue row = new MapValue().put("id", 1);
        for (FieldValue value : values) {
            row.put(targetField, value);

            PutRequest putReq = new PutRequest()
                .setTableName(tableName)
                .setValue(row);
            try {
                handle.put(putReq);
                if (!expSucceed) {
                    fail("Expect to fail but succeed");
                }
            } catch (Throwable ex) {
                if (expSucceed) {
                    fail("Expect to succeed but fail");
                }
            }
        }
    }

    private String genString(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append((char)('A' + i % 26));
        }
        return sb.toString();
    }

    private byte[] genBytes(int size) {
        byte[] bytes = new byte[size];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte)(i % 256);
        }
        return bytes;
    }

    private void assertTagsEquals(DefinedTags exp, DefinedTags tags) {
        MapValue v0 = (MapValue)MapValue.createFromJson(exp.toString(), null);
        MapValue v1 = (MapValue)MapValue.createFromJson(tags.toString(), null);
        if (useCloudService) {
            /*
             * Ignore the default defined tag added by cloud service
             * automatically
             */
            v1.remove(DEFAULT_DEFINED_TAG_NAMESPACE);
        }
        assertEquals(v0, v1);
    }

    private void assertTagsEquals(FreeFormTags exp, FreeFormTags tags) {
        assertEquals(MapValue.createFromJson(exp.toString(), null),
                     MapValue.createFromJson(tags.toString(), null));
    }
}
