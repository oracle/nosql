/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import oracle.kv.BulkWriteOptions;
import oracle.kv.EntryStream;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TimeToLive;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test for TableAPI.putBulk() API:
 *
 *  public void putBulk(BulkWriteOptions bulkWriteOptions,
 *                      List<EntryStream<Row>> streams)
 *      throws DurabilityException,
 *             RequestTimeoutException,
 *             FaultException;
 */
public class BulkTableTest extends TableTestBase {

    private Table userTable = null;
    private Table emailTable = null;
    private boolean setTTL = false;

    @BeforeClass
    public static void staticSetUp() throws Exception {
        //TODO: remove this after MRTable supports TTL.
        Assume.assumeFalse("Test should not run in MR table mode", mrTableMode);
        TableTestBase.staticSetUp(1, 1, 2);
    }

    @Test
    public void testBasic() {

        TestStream userStream;
        TestStream emailStream;

        final int userCount = 1000;
        final int emailCount = 1500;

        final BulkWriteOptions writeOptions =
            new BulkWriteOptions(null, 0, null);

        createTables();

        /* Run bulk put using single stream */
        userStream = new TestStream(userTable, userCount);
        runBulkPut(writeOptions, userStream);
        verifyStreamResult(userStream, false, 0, true);
        verifyTable(userTable, userCount);

        /* Run bulk put with 2 entry streams */
        deleteTable(userTable);
        userStream = new TestStream(userTable, userCount);
        emailStream = new TestStream(emailTable, emailCount);
        writeOptions.setStreamParallelism(2);
        runBulkPut(writeOptions, userStream, emailStream);
        verifyStreamResult(userStream, false, 0, true);
        verifyStreamResult(emailStream, false, 0, true);
        verifyTable(userTable, userCount);
        verifyTable(emailTable, emailCount);

        /* RowCount = 1 */
        deleteTable(userTable);
        deleteTable(emailTable);
        userStream = new TestStream(userTable, 1);
        emailStream = new TestStream(emailTable, 1);
        runBulkPut(writeOptions, userStream, emailStream);
        verifyStreamResult(userStream, false, 0, true);
        verifyStreamResult(emailStream, false, 0, true);
        verifyTable(userTable, 1);
        verifyTable(emailTable, 1);

        /* RowCount = 0 */
        deleteTable(userTable);
        deleteTable(emailTable);
        userStream = new TestStream(userTable, 0);
        emailStream = new TestStream(emailTable, 0);
        runBulkPut(writeOptions, userStream, emailStream);
        verifyStreamResult(userStream, false, 0, true);
        verifyStreamResult(emailStream, false, 0, true);
        verifyTable(userTable, 0);
        verifyTable(emailTable, 0);
    }

    @Test
    public void testInvalidArgument() {

        try {
            tableImpl.put(null, null);
            fail("Expected to catch IllegalArgumentException but not");
        } catch (IllegalArgumentException iae) {
        }

        try {
            tableImpl.put(new ArrayList<EntryStream<Row>>(), null);
            fail("Expected to catch IllegalArgumentException but not");
        } catch (IllegalArgumentException iae) {
        }

        try {
            List<EntryStream<Row>> streams = new ArrayList<EntryStream<Row>>();
            streams.add(null);
            tableImpl.put(streams, null);
            fail("Expected to catch IllegalArgumentException but not");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void testKeyExists() {
        int userCount = 500;
        int emailCount = 500;

        createTables();

        TestStream userStream = new TestStream(userTable, userCount);
        TestStream emailStream = new TestStream(emailTable, emailCount);
        final BulkWriteOptions writeOptions =
            new BulkWriteOptions(null, 0, null);
        writeOptions.setStreamParallelism(2);
        runBulkPut(writeOptions, userStream, emailStream);
        verifyStreamResult(userStream, false, 0, true);
        verifyStreamResult(emailStream, false, 0, true);

        userStream = new TestStream(userTable, 2 * userCount);
        emailStream = new TestStream(emailTable, 2 * emailCount);
        runBulkPut(writeOptions, userStream, emailStream);
        verifyStreamResult(userStream, false, userCount, true);
        verifyStreamResult(emailStream, false, emailCount, true);
        verifyTable(userTable, 2 * userCount);
        verifyTable(emailTable, 2 * emailCount);
    }

    @Test
    public void testBulkWriteOptions() {
        int userCount = 1000;
        int emailCount = 500;
        TestStream userStream;
        TestStream emailStream;

        createTables();

        /* Use defaults of BulkWriteOptions */
        userStream = new TestStream(userTable, userCount);
        runBulkPut(userStream);
        verifyStreamResult(userStream, false, 0, true);
        verifyTable(userTable, userCount);
        deleteTable(userTable);

        /* BulkHeapPercent: 50, PerShardParallelism: 10, StreamParallelism: 2 */
        BulkWriteOptions options = new BulkWriteOptions(null, 0, null);
        options.setBulkHeapPercent(50);
        assertEquals(50, options.getBulkHeapPercent());
        options.setPerShardParallelism(10);
        assertEquals(10, options.getPerShardParallelism());
        options.setStreamParallelism(2);
        assertEquals(2, options.getStreamParallelism());

        userStream = new TestStream(userTable, userCount);
        emailStream = new TestStream(emailTable, emailCount);
        runBulkPut(options, userStream, emailStream);
        verifyStreamResult(userStream, false, 0, true);
        verifyStreamResult(emailStream, false, 0, true);
        verifyTable(userTable, userCount);
        verifyTable(emailTable, emailCount);

        /* BulkHeapPercent: 1, PerShardParallelism: 1, StreamParallelism: 1 */
        options = new BulkWriteOptions(null, 0, null);
        options.setBulkHeapPercent(1);
        assertEquals(1, options.getBulkHeapPercent());
        options.setPerShardParallelism(1);
        assertEquals(1, options.getPerShardParallelism());
        options.setStreamParallelism(1);
        assertEquals(1, options.getStreamParallelism());

        userStream = new TestStream(userTable, userCount);
        emailStream = new TestStream(emailTable, emailCount);
        runBulkPut(options, userStream, emailStream);
        verifyStreamResult(userStream, false, userCount, true);
        verifyStreamResult(emailStream, false, emailCount, true);
    }

    @Test
    public void testCatchException() throws Exception {
        final int userCount = 1000;
        final int emailCount = 1000;

        createTables();

        /* ReaderStream caught exception, the entire bulk put is terminated. */
        TestStream stream1 = new TestStream(emailTable, emailCount,
                                            emailCount - 1);
        TestStream stream2 = new TestStream(userTable, userCount);
        BulkWriteOptions options = new BulkWriteOptions(null, 0, null);
        options.setStreamParallelism(1);
        runBulkPut(options, true, stream1, stream2);
        verifyStreamResult(stream1, false, 0, false);
        verifyStreamResult(stream2, false, 0, false);

        /*
         * An exception is thrown during store.putBatch(), the exception handler
         * of stream1 is invoked and return normally, the insertion of entries
         * supplied by stream2 succeeds.
         */
        deleteTable(userTable);
        deleteTable(emailTable);
        List<Row> list = new ArrayList<Row>();
        for (int i = 0; i < 10; i++) {
            final Row row = createRow(emailTable, i, 10);
            list.add(row);
        }
        stream1 = new TestStream(list);
        stream2 = new TestStream(userTable, userCount);
        removeTable(null, "user.email", true);
        emailTable = null;
        runBulkPut(options, stream1, stream2);

        /*
         * An exception is thrown during store.putBatch(), the exception
         * handler of stream1 is invoked and throw an exception, the entire
         * bulk put operation is terminated.
         */
        deleteTable(userTable);
        stream1 = new TestStream(list);
        stream2 = new TestStream(userTable, userCount);
        stream1.setThrowException(true);
        runBulkPut(options, true, stream1, stream2);
    }

    @Test
    public void testUseTTL() throws Exception {
        setTTL = true;
        createTables();

        int userCount = 100;
        int emailCount = 200;
        TestStream userStream = new TestStream(userTable, userCount);
        TestStream emailStream = new TestStream(emailTable, emailCount);

        final BulkWriteOptions writeOptions =
            new BulkWriteOptions(null, 0, null);
        writeOptions.setStreamParallelism(2);

        runBulkPut(writeOptions, userStream, emailStream);

        verifyStreamResult(userStream, false, 0, true);
        verifyStreamResult(emailStream, false, 0, true);
        verifyTable(userTable, userCount);
        verifyTable(emailTable, emailCount);
    }

    /**
     * Bulk load to tables with identity column
     */
    @Test
    public void testIdentityType() throws Exception {
        String nameKeyId = "tableKeyId";
        String ddl0 = "create table " + nameKeyId +
                      "(id integer generated always as identity, " +
                      " name string, " +
                      " primary key(id))";

        String nameValueId = "tableValueId";
        String ddl1 = "create table " + nameValueId +
                      "(id integer, " +
                      " value integer generated always as identity, " +
                      " primary key(id))";

        createTable(ddl0, null);
        createTable(ddl1, null);

        Table table0 = tableImpl.getTable(nameKeyId);
        assertNotNull(table0);
        Table table1 = tableImpl.getTable(nameValueId);
        assertNotNull(table1);

        int num = 100;
        List<Row> rows0 = new ArrayList<Row>();
        List<Row> rows1 = new ArrayList<Row>();
        for (int i = 0; i < num; i++) {
            switch (i % 4) {
            case 0:
                rows0.add(table0.createRow().put("name", "name" + i).asRow());
                break;
            case 1:
                rows0.add(table1.createRow().put("id", i).asRow());
                break;
            case 2:
                rows1.add(table0.createRow().put("name", "name" + i).asRow());
                break;
            case 3:
                rows1.add(table1.createRow().put("id", i).asRow());
                break;
            }
        }

        TestStream stream0 = new TestStream(rows0);
        TestStream stream1 = new TestStream(rows1);

        BulkWriteOptions options = new BulkWriteOptions();
        options.setIdentityCacheSize(10);
        runBulkPut(options, stream0, stream1);

        verifyStreamResult(stream0, false, 0, true);
        assertEquals(num/2, countTableRows(table0.createPrimaryKey(), table0));

        verifyStreamResult(stream1, false, 0, true);
        assertEquals(num/2, countTableRows(table1.createPrimaryKey(), table1));
    }

    /**
     * Bulk load to tables with namespace
     */
    @Test
    public void testNamespace() throws Exception {
        String tableName = "testns";
        final String ddl = "create table " + tableName +
                           "(id integer, name string, primary key(id))";

        final String namespace = "myns";
        executeDdl(ddl);
        executeDdl(ddl, namespace);

        Table table0 = tableImpl.getTable(tableName);
        assertNotNull(table0);

        tableName = NameUtils.makeQualifiedName(namespace, tableName);
        Table table1 = tableImpl.getTable(tableName);
        assertNotNull(table1);

        int num = 100;
        List<Row> rows0 = new ArrayList<Row>();
        List<Row> rows1 = new ArrayList<Row>();
        for (int i = 0; i < num; i++) {
            switch (i % 4) {
            case 0:
                rows0.add(table0.createRow().put("id", i)
                                            .put("name", "name" + i)
                                            .asRow());
                break;
            case 1:
                rows0.add(table1.createRow().put("id", i)
                                            .put("name", "name" + i)
                                            .asRow());
                break;
            case 2:
                rows1.add(table0.createRow().put("id", i)
                                            .put("name", "name" + i)
                                            .asRow());
                break;
            case 3:
                rows1.add(table1.createRow().put("id", i)
                                            .put("name", "name" + i)
                                            .asRow());
                break;
            }
        }

        TestStream stream0 = new TestStream(rows0);
        TestStream stream1 = new TestStream(rows1);
        runBulkPut(stream0, stream1);

        verifyStreamResult(stream0, false, 0, true);
        assertEquals(num/2, countTableRows(table0.createPrimaryKey(), table0));

        verifyStreamResult(stream1, false, 0, true);
        assertEquals(num/2, countTableRows(table1.createPrimaryKey(), table1));
    }

    @Test
    public void testUsePutResolve() {

        final int rowCount = 20;
        final int numTombstones = 6;
        final String tableName = "bulktable";
        final String ddl = "create table " + tableName +
                           "(id integer, name string, primary key(id))";
        final BulkWriteOptions writeOptions =
            new BulkWriteOptions(null, 0, null);
        writeOptions.setUsePutResolve(true);
        List<Row> rows = new ArrayList<Row>();

        /*
         * create and populate the table using normal bulk put, but set an
         * explicit mod time, as restore or putresolve might do
         */
        /* save current time for later */
        long t0 = System.currentTimeMillis();

        executeDdl(ddl);
        Table table = tableImpl.getTable(tableName);
        for (int i = 0; i < rowCount; i++) {
            Row row = table.createRow();
            row.put("id", i).put("name", ("name"+i));
            ((RowImpl)row).setModificationTime(t0);
            rows.add(row);
        }

        /* add some tombstones */
        for (int i = rowCount; i < rowCount + numTombstones; i++) {
            PrimaryKey key = table.createPrimaryKey();
            key.put("id", i);
            ((RowImpl)key).setModificationTime(t0);
            // see if region works
            ((RowImpl)key).setRegionId(7);
            rows.add(key);
        }

        TestStream stream = new TestStream(rows);
        runBulkPut(writeOptions, stream);
        verifyStreamResult(stream, false, 0, true);
        assertEquals(rowCount, countTableRows(table.createPrimaryKey(), table));

        /*
         * for 1/2 of the rows set mod time back, to just before t0,
         * for the other half use current time. The first half
         * should fail the resolve and not be put, the second half
         * should succeed
         */
        long t1 = System.currentTimeMillis() + 1;
        for (int i = 0; i < rowCount; i++) {
            Row row = rows.get(i);
            ((RowImpl)row).setModificationTime(i < rowCount/2 ? t0-1L : t1);
            row.put("name", ("name_new" + i));
        }

        /* same for tombstones, except use new rows vs tombstones */
        for (int i = 0; i < numTombstones; i++) {
            Row row = table.createRow();
            row.put("id", (rowCount + i));
            row.put("name", ("ts" + (i)));
            ((RowImpl)row).setModificationTime(i < numTombstones/2 ? t0-1L :
                                               t1);
            /* overwrite in list */
            rows.set(rowCount + i, row);
        }

        /* do bulk put again */
        stream = new TestStream(rows);
        runBulkPut(writeOptions, stream);

        /*
         * Can't currently scan for tombstones, assert that expected
         * rows and mod times exist after scan
         */
        PrimaryKey key = table.createPrimaryKey();
        TableIterator<Row> iter = tableImpl.tableIterator(key, null, null);
        int t0count = 0; /* original rows */
        int t1count = 0; /* updated rows, includes overwritten tombstones */
        while (iter.hasNext()) {
            final Row row = iter.next();
            if (row.getLastModificationTime() == t0) {
                ++t0count;
            } else if (row.getLastModificationTime() == t1) {
                ++t1count;
            }
        }
        /* num rows is original rows plus half of the overwritten tombstones */
        assertEquals(rowCount + numTombstones/2, t0count + t1count);
        /* original rows should be 10 (half of original rows) */
        assertEquals(rowCount/2, t0count);
        /* new rows should be half of original rows plus half of tombstones */
        assertEquals(rowCount/2 + numTombstones/2, t1count);
    }

    private void runBulkPut(TestStream... streams) {
        runBulkPut(null, streams);
    }

    private void runBulkPut(final BulkWriteOptions options,
                            TestStream... streams) {
        runBulkPut(options, false, streams);
    }

    private void runBulkPut(final BulkWriteOptions options,
                            final boolean shouldFail,
                            TestStream... streams) {

        final List<EntryStream<Row>> list;
        if (streams.length == 1) {
            list = Collections.singletonList((EntryStream<Row>)streams[0]);
        } else {
            list = new ArrayList<EntryStream<Row>>();
            for (TestStream stream : streams) {
                list.add(stream);
            }
        }
        try {
            tableImpl.put(list, options);
            if (shouldFail) {
                fail("Expected to be failed but actually not");
            }
        } catch (RuntimeException re) {
            if (!shouldFail) {
                fail("Failed to execute putBulk operation: " + re.getMessage());
            }
        }
    }

    private void verifyStreamResult(final TestStream stream,
                                    boolean caughtException,
                                    int numKeyExists,
                                    boolean isCompleted) {

        if (caughtException) {
            assertNotNull(stream.exception);
        } else {
            assertNull(stream.exception);
        }
        assertEquals(numKeyExists, stream.getNumExists());
        assertTrue(stream.isCompleted == isCompleted);
    }

    /* Delete rows from the table */
    private void deleteTable(Table table) {
        PrimaryKey key = table.createPrimaryKey();
        TableIterator<PrimaryKey> iter =
            tableImpl.tableKeysIterator(key, null, countOpts);
        while (iter.hasNext()) {
            key = iter.next();
            tableImpl.delete(key, null, null);
        }
        iter.close();
    }

    /* Verify the rows in the table */
    private void verifyTable(Table table, int expRowCount) {
        PrimaryKey key = table.createPrimaryKey();
        TableIterator<Row> iter = tableImpl.tableIterator(key, null, countOpts);
        int rowCount = 0;
        while (iter.hasNext()) {
            final Row row = iter.next();
            final int index = row.get("index").asInteger().get();
            final Row expRow = createRow(table, index, expRowCount);
            assertTrue("expected to get row: " + expRow.toJsonString(false) +
                       " but actually get " + row.toJsonString(false),
                       expRow.equals(row));
            if (setTTL) {
                verifyRowTTL(table, index, row);
            }
            ++rowCount;
        }
        iter.close();
        assertEquals(expRowCount, rowCount);
    }

    private void verifyRowTTL(Table table, int index, Row row) {
        TimeToLive ttl = getExpectedTTL(table, index);
        assertTimeToLive(ttl, row);
    }

    private void createTables() {
        if (setTTL) {
            userTable = buildUserTable(TimeToLive.ofDays(10));
            emailTable = buildEmailTable(userTable, TimeToLive.ofHours(50));
        } else {
            userTable = buildUserTable(null);
            emailTable = buildEmailTable(userTable, null);
        }
    }

    private TableImpl buildUserTable(TimeToLive ttl) {

        TableImpl table = getTable("user");
        if (table != null) {
            return table;
        }

        TableBuilder tb = (TableBuilder)TableBuilder.createTableBuilder("User")
            .addString("firstName")
            .addString("lastName")
            .addInteger("age")
            .addString("address")
            .addBinary("binary", null)
            .addInteger("index")
            .primaryKey("lastName", "firstName")
            .shardKey("lastName");

        if (ttl != null) {
            tb.setDefaultTTL(ttl);
        }

        table = tb.buildTable();
        try {
            table = addTable(table, true);
        } catch (Exception e) {
            fail("Failed to create user table: " + e.getMessage());
        }
        return table;
    }

    private TableImpl buildEmailTable(Table parent, TimeToLive ttl) {

        final String name = parent.getFullName() + ".email";
        TableImpl table = getTable(name);
        if (table != null) {
            return table;
        }

        TableBuilder tb =
            (TableBuilder)TableBuilder.createTableBuilder(null,
                                                          "email",
                                                          "child of user",
                                                          parent,
                                                          null)
            .addInteger("eid")
            .addString("email")
            .addEnum("type", new String[]{"POP", "IMAP", "EXCHANGE"}, null)
            .addInteger("index")
            .primaryKey("eid");

        if (ttl != null) {
            tb.setDefaultTTL(ttl);
        }
        table = tb.buildTable();

        try {
            table = addTable(table, true);
        } catch (Exception e) {
            fail("Failed to create user.email table: " + e.getMessage());
        }
        return table;
    }

    private Row createRow(Table table, int index, int total) {
        final String name = table.getFullName();
        Row row;
        if (name.equalsIgnoreCase("user")) {
            row = createUserRow(table, index, total);
        } else if (name.equalsIgnoreCase("user.email")) {
            row =  createEmailRow(table, index, total);
        } else {
            throw new IllegalArgumentException("Failed to create row for " +
            		"table: " + name);
        }
        if (setTTL) {
            TimeToLive ttl = getRowTTL(index);
            row.setTTL(ttl);
        }
        return row;
    }

    private Row creatUserPrimaryKey(Table table, int index, int total) {
        final Row row = table.createRow();
        final int numFirstName = 10;
        final int numLastName = (total + 9) /10;
        row.put("firstName", "firstName_" + (index % numFirstName));
        row.put("lastName", "lastName_" + ((index * numLastName) / total));
        return row;
    }

    private Row createUserRow(Table table, int index, int total) {
        final Row row = creatUserPrimaryKey(table, index, total);
        row.put("index", index);
        if (useNullValue(index)) {
            row.putNull("age");
            row.putNull("address");
            row.putNull("binary");
        } else {
            row.put("age", 20 + (index % 40));
            row.put("address", "address_" + index);
            row.put("binary", getBinary(index % 30 + 1));
        }
        return row;
    }

    private Row createEmailRow(Table table, int index, int total) {
        final Row key = creatUserPrimaryKey(table.getParent(), index, total);
        final Row row = table.createRowFromJson(key.toJsonString(false), false);
        row.put("eid", index);
        row.put("index", index);
        if (useNullValue(index)) {
            row.putNull("email");
            row.putNull("type");
        } else {
            row.put("email", "email" + index + "@abc.com");
            final String[] values = table.getField("type").asEnum().getValues();
            row.putEnum("type", values[index % values.length]);
        }
        return row;
    }

    private byte[] getBinary(int len) {
        if (len == 0) {
            return null;
        }
        final byte[] buf = new byte[len];
        for (int i = 0; i < len; i++) {
            buf[i] = (byte)('A' + i % 26);
        }
        return buf;
    }

    private boolean useNullValue(int index) {
        return (index % 20 == 19);
    }

    private TimeToLive getRowTTL(int index) {
        if (index % 10 == 0) {
            return null;
        } else if (index % 10 == 1) {
            return TimeToLive.DO_NOT_EXPIRE;
        } else {
            if (index % 2 == 0) {
                return TimeToLive.ofHours(index);
            }
            return TimeToLive.ofDays(index);
        }
    }

    private TimeToLive getExpectedTTL(Table table, int index) {
        TimeToLive ttl = getRowTTL(index);
        if (ttl == null) {
            ttl = table.getDefaultTTL();
        }
        return ttl;
    }

    /**
     * Asserts given TTL duration and time-to-live duration of given row differs
     * not more than a day. The approximate comparison is to account for
     * rounding error and clock skew.
     */
    private void assertTimeToLive(TimeToLive ttl, Row row) {
        final long DAY_IN_MILLIS = 24 * 60 * 60 * 1000;
        long actual =  row.getExpirationTime();
        long expected = ttl.toExpirationTime(System.currentTimeMillis());
        assertTrue("Actual TTL duration " + actual + "ms differs by "
                + "more than a day from expected duration of " + expected +"ms",
                Math.abs(actual - expected) < DAY_IN_MILLIS);
    }

    private class TestStream implements EntryStream<Row> {

        private final Table table;
        private final int rowCount;
        private final int failedIndex;
        private final Iterator<Row> iterator;

        private AtomicInteger rowExists = new AtomicInteger();
        private AtomicInteger catchExceptions = new AtomicInteger();
        private int i = 0;
        private RuntimeException exception;
        private boolean isCompleted = false;
        boolean throwException = false;

        TestStream(Table table, int rowCount) {
            this(table, rowCount, -1);
        }

        TestStream(Table table, int rowCount, int failedIndex) {
            super();
            this.table = table;
            this.rowCount = rowCount;
            iterator = null;
            this.failedIndex = failedIndex;
        }

        TestStream(List<Row> rows) {
            super();
            table = null;
            rowCount = rows.size();
            iterator = rows.iterator();
            failedIndex = -1;
        }

        @Override
        public String name() {
            return "TestRowStream";
        }

        @Override
        public Row getNext() {

            if (iterator != null) {
                if (iterator.hasNext()) {
                    return iterator.next();
                }
                return null;
            }

            if ( i++ >= rowCount) {
                return null;
            }

            if (i == failedIndex) {
                throw new IllegalArgumentException("Dummy exception");
            }
            final Row row = createRow(table, i, rowCount);
            return row;
        }

        @Override
        public void completed() {
            isCompleted = true;
        }

        @Override
        public void keyExists(Row entry) {
            if (table != null) {
                assertTrue(entry.getTable().equals(table));
            }
            rowExists.incrementAndGet();
        }

        public int getNumExists() {
            return rowExists.get();
        }

        public void setThrowException(boolean throwException) {
            this.throwException = throwException;
        }

        @Override
        public void catchException(RuntimeException runtimeException,
                                   Row entry) {
            exception = runtimeException;
            catchExceptions.incrementAndGet();
            if (throwException) {
                throw runtimeException;
            }
        }
    }
}
