/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.table;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static oracle.kv.util.TestUtils.checkCause;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import oracle.kv.BulkWriteOptions;
import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.EntryStream;
import oracle.kv.FaultException;
import oracle.kv.KVStoreConfig;
import oracle.kv.RequestTimeoutException;
import oracle.kv.StatementResult;
import oracle.kv.StoreIteratorException;
import oracle.kv.TestBase;
import oracle.kv.Version;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.RequestDispatcherImpl;
import oracle.kv.impl.api.ops.ClientTestBase;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.table.ArrayBuilder;
import oracle.kv.impl.api.table.MapBuilder;
import oracle.kv.impl.api.table.PrimaryKeyImpl;
import oracle.kv.impl.api.table.RecordBuilder;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.async.exception.DialogUnknownException;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.FilterableParameterized;
import oracle.kv.impl.util.registry.AsyncControl;
import oracle.kv.query.ExecuteOptions;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Test Basic table APIs using only the kvclient jar.
 */
@RunWith(FilterableParameterized.class)
public class ClientTableAPITest extends ClientTestBase {

    private static final ReadOptions readOptions =
        new ReadOptions(Consistency.ABSOLUTE, 1, MILLISECONDS);

    private static final WriteOptions writeOptions =
        new WriteOptions(Durability.COMMIT_SYNC, 1, MILLISECONDS);

    private static final TableIteratorOptions iteratorOptions =
        new TableIteratorOptions(Direction.FORWARD, null, 5000, MILLISECONDS);

    private static final String useAsyncProp =
        System.getProperty(KVStoreConfig.USE_ASYNC);
    private static final String serverUseAsyncProp =
        System.getProperty(AsyncControl.SERVER_USE_ASYNC);
    private static final boolean initialServerUseAsync =
        AsyncControl.serverUseAsync;
    private static final String EXTRAARGS = "oracle.kv.jvm.extraargs";
    private static final String extraargs = System.getProperty(EXTRAARGS);

    private static final int groups = 10;
    private static final int users = 10;

    public ClientTableAPITest(boolean secure) {
        super(secure);
    }

    @Override
    public void setUp()
        throws Exception {

        checkRMIRegistryLookup();
        startServices = false;
        super.setUp();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();

        /* Revert any changes to async configuration */
        if (useAsyncProp == null) {
            System.clearProperty(KVStoreConfig.USE_ASYNC);
        } else {
            System.setProperty(KVStoreConfig.USE_ASYNC, useAsyncProp);
        }
        if (serverUseAsyncProp == null) {
            System.clearProperty(AsyncControl.SERVER_USE_ASYNC);
        } else {
            System.setProperty(AsyncControl.SERVER_USE_ASYNC,
                               serverUseAsyncProp);
        }
        AsyncControl.serverUseAsync = initialServerUseAsync;
        if (extraargs == null) {
            System.clearProperty(EXTRAARGS);
        } else {
            System.setProperty(EXTRAARGS, extraargs);
        }

    }

    @Test
    public void testFieldDefAndValue() {
        TableBuilder tb = TableBuilder.createTableBuilder("tab1");
        tb.addBinary("bi")
          .addBoolean("bl")
          .addDouble("d")
          .addEnum("e", new String[]{"e1", "e2", "e3"}, null)
          .addFixedBinary("fb", 5)
          .addFloat("fl")
          .addInteger("i")
          .addLong("l")
          .addString("s")
          .addTimestamp("ts", 3);

        ArrayBuilder ab = TableBuilder.createArrayBuilder();
        ab.addString();
        tb.addField("a", ab.build());

        MapBuilder mb = TableBuilder.createMapBuilder("map");
        mb.addLong();
        tb.addField("m", mb.build());

        RecordBuilder rb = TableBuilder.createRecordBuilder("r");
        rb.addInteger("ri");
        rb.addString("rs");
        tb.addField("r", rb.build());

        tb.primaryKey("i", "s");
        tb.shardKey("i");

        assertTrue("Wrong type of field 'bi'", tb.getField("bi").isBinary());
        assertTrue("Wrong type of field 'bl'", tb.getField("bl").isBoolean());
        assertTrue("Wrong type of field 'd'", tb.getField("d").isDouble());
        assertTrue("Wrong type of field 'e'", tb.getField("e").isEnum());
        assertTrue("Wrong type of field 'fb'",
                   tb.getField("fb").isFixedBinary());
        assertTrue("Wrong type of field 'fl'", tb.getField("fl").isFloat());
        assertTrue("Wrong type of field 'i'", tb.getField("i").isInteger());
        assertTrue("Wrong type of field 'l'", tb.getField("l").isLong());
        assertTrue("Wrong type of field 's'", tb.getField("s").isString());
        assertTrue("Wrong type of field 'ts'", tb.getField("ts").isTimestamp());
        assertTrue("Wrong type of field 'a'", tb.getField("a").isArray());
        assertTrue("Wrong type of field 'm'", tb.getField("m").isMap());
        assertTrue("Wrong type of field 'r'", tb.getField("r").isRecord());
        assertEquals("Wrong number of primary key fields",
                     tb.getPrimaryKey().size(), 2);
        assertEquals("Wrong number of shard key fields",
                     tb.getShardKey().size(), 1);

        TableImpl table = tb.buildTable();
        FieldValue fv;

        /* Binary value */
        byte[] bi = new byte[] {0x00, 0x01, 0x02, 0x00};
        fv = table.getField("bi").createBinary(bi);
        assertTrue("Wrong value type of field 'bi'", fv.isBinary());
        assertTrue("Wrong value of 'bi'",
                    Arrays.equals(fv.asBinary().get(), bi));

        /* Boolean value */
        boolean bl = false;
        fv = table.getField("bl").createBoolean(bl);
        assertTrue("Wrong value type of field 'bl'", fv.isBoolean());
        assertEquals("Wrong value of 'bl'", fv.asBoolean().get(), bl);

        /* Double value*/
        double d = 0.000000000001;
        fv = table.getField("d").createDouble(d);
        assertTrue("Wrong value type of field 'd'", fv.isDouble());
        assertTrue("Wrong value of 'd'",
                   Double.compare(fv.asDouble().get(), d) == 0);

        /* Enum value*/
        String e = "e1";
        fv = table.getField("e").createEnum(e);
        /* Bug? */
        assertTrue("Wrong value type of field 'e'", fv.isEnum());
        assertTrue("Wrong value of 'e'", fv.asEnum().get().compareTo(e) == 0);

        /* Float value*/
        float fl = 0.000123f;
        fv = table.getField("fl").createFloat(fl);
        assertTrue("Wrong value type of field 'fl'", fv.isFloat());
        assertTrue("Wrong value of 'fl'",
                    Float.compare(fv.asFloat().get(), fl) == 0);

        /* Long value */
        long l = 10000L;
        fv = table.getField("l").createLong(l);
        assertTrue("Wrong value type of field 'l'", fv.isLong());
        assertEquals("Wrong value of 'l'", fv.asLong().get(), l);

        /* Integer value */
        int i = 100;
        fv = table.getField("i").createInteger(i);
        assertTrue("Wrong value type of field 'i'", fv.isInteger());
        assertEquals("Wrong value of 'i'", fv.asInteger().get(), i);

        /* String value */
        String s = "string value";
        fv = table.getField("s").createString(s);
        assertTrue("Wrong value type of field 's'", fv.isString());
        assertTrue("Wrong value of 's'", fv.asString().get().compareTo(s) == 0);

        /* Timestamp value */
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        fv = table.getField("ts").createTimestamp(ts);
        assertTrue("Wrong value type of field 'ts'", fv.isTimestamp());
        assertTrue("Wrong value of 'ts'",
                   fv.asTimestamp().get().compareTo(ts) == 0);

        /* Array value */
        fv = table.getField("a").createArray();
        fv.asArray().add("av string1").add("av string2");
        assertTrue("Wrong value type of field 'a'", fv.isArray());
        assertTrue("Wrong value size of field 'a'", fv.asArray().size() == 2);

        /* Map value */
        fv = table.getField("m").createMap();
        fv.asMap().put("key1", 1L).put("key2", 2L);
        assertTrue("Wrong value type of field 'm'", fv.isMap());
        assertTrue("Wrong value type of field 'm.key1'",
                    fv.asMap().get("key1").isLong());
        assertEquals("Wrong value of 'm.key1'",
                     fv.asMap().get("key1").asLong().get(), 1L);
        assertTrue("Wrong size of field value 'm'",
                   fv.asMap().getFields().size() == 2);

        /* Record value */
        fv = table.getField("r").createRecord();
        fv.asRecord().put("ri", 1).put("rs", "record string1");
        assertTrue("Wrong type of field value 'r'", fv.isRecord());
        assertEquals("Wrong value of 'r.ri'",
                     fv.asRecord().get("ri").asInteger().get(), 1);
        assertTrue("Wrong value type of 'r.rs'",
                   fv.asRecord().get("rs").isString());
    }

    @Test
    public void testBasicAPI() throws Exception {

        /*
         * Test the sync APIs by default, but not if async is explicitly
         * enabled
         */
        assumeTrue("Async is explicitly enabled",
                   ((useAsyncProp == null) ||
                    !Boolean.valueOf(useAsyncProp)) &&
                   ((serverUseAsyncProp == null) ||
                    !Boolean.valueOf(serverUseAsyncProp)));

        open();
        final Table table = createTableAndIndex();
        final TableAPI tableImpl = store.getTableAPI();

        /* Get a row */
        PrimaryKey key = table.createPrimaryKey();
        key.put("gid", 0).put("uid", 0);
        Row row1 = tableImpl.get(key, null);
        assertTrue("Get wrong row",
                    row1.compareTo(genUsersRow(table, 0, 0)) == 0);

        /* Delete a row */
        boolean deleted = tableImpl.delete(key, null, null);
        assertTrue("Failed to delete the row", deleted);

        /* Get rows using MultiGet() */
        key = table.createPrimaryKey();
        key.put("gid", 1);
        List<Row> rs = tableImpl.multiGet(key, null, null);
        assertTrue("Get wrong number of returned row using MultiGet()",
                   rs.size() == 10);

        /* Get rows using tableIterator() with primary key*/
        key = table.createPrimaryKey();
        FieldRange fr = table.createFieldRange("gid")
                             .setStart(1, true)
                             .setEnd(2, true);
        TableIterator<Row> itr =
            tableImpl.tableIterator(key, fr.createMultiRowOptions(), null);
        int cnt = 0;
        while(itr.hasNext()) {
            itr.next();
            cnt++;
        }
        assertTrue("Get wrong number of returned row using tableIterator()",
                   cnt == 20);

        /* Get keys using tableKeyIterator() with index key */
        Index index = table.getIndex("idx1");
        IndexKey ikey = index.createIndexKey();
        ikey.put("age", 21);
        TableIterator<KeyPair> itrKeys =
            tableImpl.tableKeysIterator(ikey, null, null);
        cnt = 0;
        while(itrKeys.hasNext()) {
            KeyPair kp = itrKeys.next();
            assertTrue("Wrong index key in resultset of " +
                       "tableKeysIterator(indexKey)",
                       kp.getIndexKey().compareTo(ikey) == 0);
            cnt++;
        }
        assertTrue("Get wrong number of key pair " +
                    "using tableKeysIterator(indexKey)",
                    cnt == 10);

        /* Get rows using tableIterator(Iterator<PrimaryKey> iterator) */
        key = table.createPrimaryKey();
        key.put("gid", 1);
        itr = tableImpl.tableIterator(Arrays.asList(key).iterator(),
                                      null, null);
        assertTrue("Get wrong number of returned row using " +
                   "tableIterator(Iterator<PrimaryKey>)",
                   rs.size() == 10);

        /* Execute operations in bulk mode */
        TableOperationFactory factory = tableImpl.getTableOperationFactory();
        List<TableOperation> ops = new ArrayList<TableOperation>();
        key = table.createPrimaryKey();
        key.put("gid", 1);
        itr = tableImpl.tableIterator(key, null, null);
        while(itr.hasNext()) {
            Row row = itr.next();
            row.put("age", row.get("age").asInteger().get() + 1);
            ops.add(factory.createPutIfPresent(row, null, true));
        }

        List<TableOperationResult> results = tableImpl.execute(ops, null);
        assertEquals("Wrong number of TableOperationResult",
                     results.size(), ops.size());
        for (int i = 0; i < results.size(); i++) {
            TableOperationResult result = results.get(i);
            assertTrue("The operation result should be success",
                       result.getSuccess());
        }

        /* PutBulk operation */
        final int nStreams = 3;
        final BulkWriteOptions options = new BulkWriteOptions(null, 0, null);
        options.setPerShardParallelism(10);
        options.setStreamParallelism(nStreams);

        final List<EntryStream<Row>> streams =
            new ArrayList<EntryStream<Row>>(nStreams);
        for (int i = 0; i < nStreams; i++) {
            streams.add(new TestStream(table, groups + i, users));
        }

        tableImpl.put(streams, options);
        for (EntryStream<Row> stream : streams) {
            assertTrue("The stream should be completed",
                       ((TestStream)stream).isCompleted);
        }

        /* Drop index and table */
        dropIndexAndTable();
        close();
    }

    private Table createTableAndIndex() {
        String createTableDDL =
            "create table users (gid integer, " +
                                "uid integer, " +
                                "firstName string, " +
                                "lastName string, " +
                                "age integer, " +
                                "primary key(shard(gid), uid))";
        createTableDDL = TestBase.addRegionsForMRTable(createTableDDL);
        final String createIndexDDL = "create index idx1 on users(age)";

        /* Create table users and idx1 on users(age) */
        StatementResult sr = store.executeSync(createTableDDL);
        assertTrue("Failed to create table", sr.isDone() && sr.isSuccessful());
        sr = store.executeSync(createIndexDDL);
        assertTrue("Failed to create index", sr.isDone() && sr.isSuccessful());

        TableAPI tableImpl = store.getTableAPI();
        Table table = tableImpl.getTable("users");
        for (int gid = 0; gid < groups; gid++) {
            for (int uid = 0; uid < users; uid++) {
                Row row = genUsersRow(table, gid, uid);
                /* Put a row */
                Version v = tableImpl.put(row, null, null);
                assertTrue("Failed to put a row to users", v != null);
            }
        }
        return table;
    }

    private void dropIndexAndTable() {
        final String dropIndexDDL = "drop index idx1 on users";
        final String dropTableDDL = "drop table users";

        StatementResult sr = store.executeSync(dropIndexDDL);
        assertTrue("Failed to drop index", sr.isDone() && sr.isSuccessful());

        sr = store.executeSync(dropTableDDL);
        assertTrue("Failed to drop table", sr.isDone() && sr.isSuccessful());
    }

    @Test
    public void testAsyncAPI() throws Exception {

        /* Test async by default, but not if it is explicitly disabled */
        assumeTrue("Async is explicitly disabled",
                   ((useAsyncProp == null) ||
                    Boolean.valueOf(useAsyncProp)) &&
                   ((serverUseAsyncProp == null) ||
                    Boolean.valueOf(serverUseAsyncProp)));

        /* Make sure async is enabled */
        System.setProperty(KVStoreConfig.USE_ASYNC, "true");
        System.setProperty(AsyncControl.SERVER_USE_ASYNC, "true");
        AsyncControl.serverUseAsync = true;
        System.setProperty(EXTRAARGS,
                           (extraargs == null ? "" : extraargs + ";") +
                           "-D" + AsyncControl.SERVER_USE_ASYNC + "=true");

        open();
        final Table table = createTableAndIndex();
        final TableAPI tableImpl = store.getTableAPI();

        testGetAsync(tableImpl, table);
        testGetAsyncInternal(tableImpl, table);
        testMultiGetAsync(tableImpl, table);
        testMultiGetKeysAsync(tableImpl, table);
        testTableIteratorAsync(tableImpl, table);
        testTableKeysIteratorAsync(tableImpl, table);
        testTableIteratorAsyncIndexKey(tableImpl, table);
        testTableKeysIteratorAsyncIndexKey(tableImpl, table);
        testTableIteratorAsyncIterator(tableImpl, table);
        testTableKeysIteratorAsyncIterator(tableImpl, table);
        testTableIteratorAsyncList(tableImpl, table);
        testTableKeysIteratorAsyncList(tableImpl, table);
        testExecuteAsyncQueryString(table);
        testExecuteAsyncQueryStatement(table);

        testMultiGetContinuationAsync((TableAPIImpl) tableImpl, table);
        testMultiGetKeysContinuationAsync((TableAPIImpl) tableImpl, table);
        testMultiGetContinuationAsyncIndexKey((TableAPIImpl) tableImpl, table);
        testMultiGetKeysContinuationAsyncIndexKey((TableAPIImpl) tableImpl,
                                                  table);

        /*
         * Put operations that may do deletes after iterations, to make
         * counting the expected results easier
         */
        testPutAsync(tableImpl, table);
        testPutAsyncInternal(tableImpl, table);
        testPutIfAbsentAsync(tableImpl, table);
        testPutIfAbsentAsyncInternal(tableImpl, table);
        testPutIfPresentAsync(tableImpl, table);
        testPutIfPresentAsyncInternal(tableImpl, table);
        testPutIfVersionAsync(tableImpl, table);
        testPutIfVersionAsyncInternal(tableImpl, table);
        testDeleteAsync(tableImpl, table);
        testDeleteAsyncInternal(tableImpl, table);
        testDeleteIfVersionAsync(tableImpl, table);
        testDeleteIfVersionAsyncInternal(tableImpl, table);
        testMultiDeleteAsync(tableImpl, table);
        testMultiDeleteAsyncInternal(tableImpl, table);
        testExecuteAsync(tableImpl, table);
        testExecuteAsyncInternal(tableImpl, table);

        dropIndexAndTable();
        close();
    }

    private void testGetAsync(TableAPI tableImpl, Table table)
        throws Exception {

        CompletableFuture<Row> future = tableImpl.getAsync(null, readOptions);
        assertException(IllegalArgumentException.class, "key must not be null",
                        future, 0);

        PrimaryKey key = table.createPrimaryKey();
        key.put("gid", 0);
        future = tableImpl.getAsync(key, readOptions);
        assertException(IllegalArgumentException.class, "primary key",
                        future, 0);

        key = table.createPrimaryKey();
        key.put("gid", 0).put("uid", 0);
        future = tableImpl.getAsync(key, null);
        Row row = future.get(5000, MILLISECONDS);
        assertEquals(genUsersRow(table, 0, 0), row);

        /*
         * Test that dialog layer exception is thrown as FaultException, not
         * wrapped in CompletionException.
         */
        setDispatcherException(createDialogUnknownException());
        try {
            assertException(FaultException.class, null,
                            tableImpl.getAsync(key, null),
                            5000);
        } finally {
            setDispatcherException(null);
        }
    }

    private void testGetAsyncInternal(TableAPI tableImpl, Table table)
        throws Exception {

        PrimaryKey key = table.createPrimaryKey();
        key.put("gid", 0).put("uid", 0);
        CompletableFuture<Result> future =
            ((TableAPIImpl) tableImpl).getAsyncInternal(
                (PrimaryKeyImpl) key, readOptions);
        future.get(5000, MILLISECONDS);

        /* Test dialog layer exception is thrown as FaultException */
        setDispatcherException(createDialogUnknownException());
        try {
            assertException(FaultException.class, null,
                            ((TableAPIImpl) tableImpl).getAsyncInternal(
                                (PrimaryKeyImpl) key, null),
                            5000);
        } finally {
            setDispatcherException(null);
        }
    }

    private static void assertException(
        Class<? extends Throwable> exceptionClass,
        String message,
        CompletableFuture<?> future,
        long timeoutMillis) throws Exception
    {
        final Semaphore done = new Semaphore(0);
        final AtomicReference<Throwable> exception = new AtomicReference<>();
        future.whenComplete((ignore, ex) -> {
                exception.set(ex);
                done.release();
            });
        if (!done.tryAcquire(timeoutMillis, MILLISECONDS)) {
            throw new Exception("Future failed to complete within " +
                                timeoutMillis + " milliseconds");
        }
        Throwable ex = exception.get();
        if (ex == null) {
            fail("Expected " + exceptionClass.getName());
        }
        if (!exceptionClass.isInstance(ex)) {
            throw new AssertionError("Exception should be " +
                                     exceptionClass.getName() +
                                     ", found " + ex,
                                     ex);
        }
        if (message != null) {
            assertExceptionMessage(message, ex);
        }
    }

    private static void assertExceptionMessage(String message, Throwable ex) {
        if (!ex.getMessage().contains(message)) {
            throw new AssertionError(
                "Exception message should contain '" + message +
                "', found: " + ex.getMessage(),
                ex);
        }
    }

    /**
     * Creates an exception that should be wrapped in FaultException and
     * should not be retried.
     */
    private DialogUnknownException createDialogUnknownException() {
        return new DialogUnknownException(false /* hasSideEffect */,
                                          false /* fromRemote */,
                                          "injected",
                                          null /* cause */);
    }

    /**
     * Causes the specified exception to be thrown when the dispatcher executes
     * an operation, or clears the exception if null is specified.
     */
    private void setDispatcherException(Exception e) {
        final RequestDispatcherImpl dispatcher =
            (RequestDispatcherImpl) ((KVStoreImpl) store).getDispatcher();
        dispatcher.setPreExecuteHook((e != null) ? r -> { throw e; } : null);
    }

    private void testMultiGetAsync(TableAPI tableImpl, Table table)
        throws Exception {

        testMultiGetAsync(
            tableImpl, table, new MultiGetAsyncOp<Row>() {
                @Override
                CompletableFuture<List<Row>> multiGetAsync(TableAPI ta,
                                                           PrimaryKey pk,
                                                           MultiRowOptions mro,
                                                           ReadOptions ro) {
                    return ta.multiGetAsync(pk, mro, ro);
                }
            });
    }

    private void testMultiGetKeysAsync(TableAPI tableImpl, Table table)
        throws Exception {

        testMultiGetAsync(
            tableImpl, table, new MultiGetAsyncOp<PrimaryKey>() {
                @Override
                CompletableFuture<List<PrimaryKey>>
                    multiGetAsync(TableAPI ta,
                                  PrimaryKey pk,
                                  MultiRowOptions mro, ReadOptions ro) {
                    return ta.multiGetKeysAsync(pk, mro, ro);
                }
            });
    }

    abstract class MultiGetAsyncOp<T extends Row> {
        abstract CompletableFuture<List<T>> multiGetAsync(TableAPI ta,
                                                          PrimaryKey pk,
                                                          MultiRowOptions mro,
                                                          ReadOptions ro);
    }

    private <T extends Row> void testMultiGetAsync(TableAPI tableImpl,
                                                   Table table,
                                                   MultiGetAsyncOp<T> op)
        throws Exception {
        MultiRowOptions getOptions =
            new MultiRowOptions(table.createFieldRange("gid"));
        CompletableFuture<List<T>> future =
            op.multiGetAsync(tableImpl, null, getOptions, readOptions);
        assertException(IllegalArgumentException.class, "key must not be null",
                        future, 0);

        PrimaryKey key = table.createPrimaryKey();
        future = op.multiGetAsync(tableImpl, key, getOptions, readOptions);
        assertException(IllegalArgumentException.class, "primary key",
                        future, 0);

        key = table.createPrimaryKey();
        key.put("gid", 1);
        future = op.multiGetAsync(tableImpl, key, null, null);
        List<T> results = future.get(5000, MILLISECONDS);
        assertEquals("Results", 10, results.size());

        /* Test dialog layer exception is thrown as FaultException */
        setDispatcherException(createDialogUnknownException());
        try {
            assertException(FaultException.class, null,
                            op.multiGetAsync(tableImpl, key, null, null),
                            5000);
        } finally {
            setDispatcherException(null);
        }
    }

    private void testTableIteratorAsync(TableAPI tableImpl, Table table)
        throws Exception {

        testTableIteratorAsync(
            tableImpl, table, new TableIteratorOp<Row>() {
                @Override
                Publisher<Row> tableIteratorAsync(
                    TableAPI ta, PrimaryKey pk, MultiRowOptions mro,
                    TableIteratorOptions tio) {
                    return ta.tableIteratorAsync(pk, mro, tio);
                }
            });
    }

    private void testTableKeysIteratorAsync(TableAPI tableImpl, Table table)
        throws Exception {

        testTableIteratorAsync(
            tableImpl, table,
            new TableIteratorOp<PrimaryKey>() {
                @Override
                Publisher<PrimaryKey> tableIteratorAsync(
                    TableAPI ta, PrimaryKey pk, MultiRowOptions mro,
                    TableIteratorOptions tio) {
                    return ta.tableKeysIteratorAsync(pk, mro, tio);
                }
            });
    }

    abstract class TableIteratorOp<T extends Row> {
        abstract Publisher<T> tableIteratorAsync(
            TableAPI ta, PrimaryKey pk, MultiRowOptions mro,
            TableIteratorOptions tio);
    }

    private <T extends Row> void testTableIteratorAsync(
        TableAPI tableImpl, Table table, TableIteratorOp<T> op)
        throws Exception {

        /* Exception for null key */
        FieldRange fieldRange = table.createFieldRange("gid")
            .setStart(1, true)
            .setEnd(2, true);
        MultiRowOptions getOptions = fieldRange.createMultiRowOptions();
        Publisher<T> publisher = op.tableIteratorAsync(
            tableImpl, (PrimaryKey) null, getOptions, null);
        CheckingSubscriber<Object> checkingSubscriber =
            new CheckingSubscriber<>(1);
        publisher.subscribe(checkingSubscriber);
        checkingSubscriber.assertException(IllegalArgumentException.class,
                                           "key must not be null", 1000);

        /* Exception for invalid key */
        PrimaryKey key = table.createPrimaryKey();
        key.put("gid", 0).put("uid", 0);
        publisher = op.tableIteratorAsync(tableImpl, key, getOptions, null);
        checkingSubscriber = new CheckingSubscriber<Object>(1);
        publisher.subscribe(checkingSubscriber);
        checkingSubscriber.assertException(
            IllegalArgumentException.class,
            "FieldRange with a complete primary key", 1000);

        /* Exception for null subscriber */
        key = table.createPrimaryKey();
        publisher = op.tableIteratorAsync(tableImpl, key, getOptions, null);
        try {
            publisher.subscribe(null);
            fail("Expected NullPointerException");
        } catch (NullPointerException e) {
            assertExceptionMessage("subscribe", e);
        }

        /* OK to supply real subscriber after null one */
        checkingSubscriber = new CheckingSubscriber<>(2);
        publisher.subscribe(checkingSubscriber);
        checkingSubscriber.getSubscription(5000).request(2);
        checkingSubscriber.await(5000, 2);
        checkingSubscriber.getSubscription(5000).cancel();

        /* Exception for multiple subscribers */
        publisher = op.tableIteratorAsync(tableImpl, key, getOptions, null);
        checkingSubscriber = new CheckingSubscriber<>(1);
        publisher.subscribe(checkingSubscriber);
        CheckingSubscriber<Object> checkingSubscriber2 =
            new CheckingSubscriber<>(0);
        publisher.subscribe(checkingSubscriber2);
        checkingSubscriber2.assertException(IllegalStateException.class,
                                            "subscribe multiple times", 1000);

        /* OK to use real subscriber after second one */
        checkingSubscriber.getSubscription(5000).request(1);
        checkingSubscriber.await(5000, 1);
        checkingSubscriber.getSubscription(5000).cancel();

        /* Exception for repeating the same subscriber */
        publisher = op.tableIteratorAsync(tableImpl, key, getOptions, null);
        checkingSubscriber = new CheckingSubscriber<>(1);
        publisher.subscribe(checkingSubscriber);
        publisher.subscribe(checkingSubscriber);
        checkingSubscriber.assertException(IllegalStateException.class,
                                           "subscribe multiple times", 1000);
        checkingSubscriber.getSubscription(5000).cancel();

        /* Exception thrown by onSubscribe */
        publisher = op.tableIteratorAsync(tableImpl, key, getOptions, null);
        CountingSubscriber<Object> subscriber = new BlockingSubscriber() {
            @Override
            public synchronized void onSubscribe(Subscription s) {
                super.onSubscribe(s);
                throw new RuntimeException("onSubscribe throws an exception");
            }
        };
        publisher.subscribe(subscriber);
        subscriber.assertException(IllegalStateException.class,
                                   "Unexpected exception calling onSubscribe",
                                   1000);
        subscriber.assertTerminated();

        /* Error thrown by onSubscribe */
        publisher = op.tableIteratorAsync(tableImpl, key, getOptions, null);
        subscriber = new BlockingSubscriber() {
            @Override
            public synchronized void onSubscribe(Subscription s) {
                super.onSubscribe(s);
                throw new Error("inject error in onSubscribe");
            }
        };
        publisher.subscribe(subscriber);
        subscriber.assertException(Error.class, "inject error in onSubscribe",
                                   1000);
        subscriber.assertTerminated();

        /* Exception for 0 request argument */
        publisher = op.tableIteratorAsync(tableImpl, key, getOptions, null);
        subscriber = new BlockingSubscriber();
        publisher.subscribe(subscriber);
        subscriber.getSubscription(5000).request(0);
        subscriber.assertException(IllegalArgumentException.class,
                                   "greater than zero", 5000);
        subscriber.assertTerminated();

        /* Exception for negative request argument */
        publisher = op.tableIteratorAsync(tableImpl, key, getOptions, null);
        subscriber = new BlockingSubscriber();
        publisher.subscribe(subscriber);
        subscriber.getSubscription(5000).request(-24);
        subscriber.assertException(IllegalArgumentException.class,
                                   "greater than zero", 5000);
        subscriber.assertTerminated();

        /* Terminate for exception in onError */
        publisher = op.tableIteratorAsync(tableImpl, key, getOptions, null);
        subscriber = new BlockingSubscriber() {
            @Override
            public synchronized void onError(Throwable exception) {
                super.onError(exception);
                throw new RuntimeException("onError throws an exception");
            }
        };
        publisher.subscribe(subscriber);
        subscriber.getSubscription(5000).request(0);
        /* Should get the original exception, not the one thrown by onError */
        subscriber.assertException(IllegalArgumentException.class,
                                   "greater than zero", 5000);
        subscriber.assertTerminated();

        /* Request, cancel, no-ops after cancel */
        publisher = op.tableIteratorAsync(tableImpl, key, getOptions, null);
        subscriber = new CountingSubscriber<Object>(3);
        publisher.subscribe(subscriber);
        subscriber.getSubscription(5000).request(3);
        subscriber.await(5000);
        Subscription subscription = subscriber.getSubscription(5000);
        subscription.cancel();
        /* Make sure request and cancel are no-ops after cancel */
        subscription.request(1);
        subscription.cancel();

        /* Multiple requests */
        publisher = op.tableIteratorAsync(tableImpl, key, getOptions, null);
        subscriber = new CountingSubscriber<Object>(Long.MAX_VALUE);
        publisher.subscribe(subscriber);
        subscription = subscriber.getSubscription(5000);
        subscription.request(2);
        /* Make sure integer overflow produces a positive request number */
        subscription.request(Long.MAX_VALUE - 1);
        subscriber.await(5000, 3);
        subscriber.getSubscription(5000).cancel();

        /* Test recursive calls to request */
        publisher = op.tableIteratorAsync(tableImpl, key, getOptions, null);
        subscriber = new CountingSubscriber<Object>(3) {
            private Subscription sub;
            private int count;
            @Override
            public synchronized void onSubscribe(Subscription s) {
                super.onSubscribe(s);
                sub = s;
                s.request(1);
                count++;
            }
            @Override
            public synchronized void onNext(Object result) {
                super.onNext(result);
                if (count < 3) {
                    sub.request(1);
                    count++;
                }
            }
        };
        publisher.subscribe(subscriber);
        subscriber.await(5000);
        subscriber.getSubscription(5000).cancel();

        /* Exception in onNext */
        publisher = op.tableIteratorAsync(tableImpl, key, getOptions, null);
        subscriber = new BlockingSubscriber() {
            @Override
            public synchronized void onNext(Object result) {
                throw new SpecialException("Inject failure 1");
            }
        };
        publisher.subscribe(subscriber);
        subscriber.getSubscription(5000).request(1);
        subscriber.assertException(IllegalStateException.class,
                                   "Unexpected exception calling onNext",
                                   5000);
        subscriber.assertTerminated();

        /* Error in onNext */
        publisher = op.tableIteratorAsync(tableImpl, key, getOptions, null);
        subscriber = new BlockingSubscriber() {
            @Override
            public synchronized void onNext(Object result) {
                throw new SpecialError("Inject failure 3");
            }
        };
        publisher.subscribe(subscriber);
        subscriber.getSubscription(5000).request(1);
        subscriber.assertException(SpecialError.class,
                                   "Inject failure 3", 5000);
        subscriber.assertTerminated();

        /* Exception in onComplete */
        publisher = op.tableIteratorAsync(tableImpl, key, getOptions, null);
        subscriber = new CheckingSubscriber<Object>(20) {
            @Override
            public synchronized void onComplete() {
                super.onComplete();
                throw new RuntimeException("onComplete throws an exception");
            }
        };
        publisher.subscribe(subscriber);
        subscriber.getSubscription(5000).request(21);
        subscriber.await(5000);
        subscriber.assertTerminated();

        /* Repeated subscriptions */
        for (int i = 0; i < 1000; i++) {
            publisher = op.tableIteratorAsync(tableImpl, key, getOptions,
                                              null);
            checkingSubscriber = new CheckingSubscriber<>(20);
            publisher.subscribe(checkingSubscriber);
            subscription = checkingSubscriber.getSubscription(5000);
            subscription.request(21);
            checkingSubscriber.await(5000);
            subscription.cancel();
        }

        /* Test dialog layer exception is thrown as FaultException */
        setDispatcherException(createDialogUnknownException());
        try {
            publisher = op.tableIteratorAsync(tableImpl, key, getOptions,
                                              null);
            final CountingSubscriber<Object> sub =
                new CountingSubscriber<>(20);
            publisher.subscribe(sub);
            subscription = sub.getSubscription(5000);
            subscription.request(21);
            checkCause(checkException(() -> sub.await(5000),
                                      StoreIteratorException.class),
                       FaultException.class, null);
            subscription.cancel();
        } finally {
            setDispatcherException(null);
        }
    }

    private static class SpecialException extends RuntimeException {
        private static final long serialVersionUID = 0;
        SpecialException(String msg) {
            super(msg);
        }
    }

    private static class SpecialError extends Error {
        private static final long serialVersionUID = 0;
        SpecialError(String msg) {
            super(msg);
        }
    }

    private void testTableIteratorAsyncIndexKey(TableAPI tableImpl,
                                                Table table)
        throws Exception {

        testTableIteratorAsyncIndexKey(
            tableImpl, table,
            new TableIteratorIndexKeyOp<Row>() {
                @Override
                Publisher<Row> tableIteratorAsync(
                    TableAPI ta, IndexKey ik, MultiRowOptions mro,
                    TableIteratorOptions tio) {
                    return ta.tableIteratorAsync(ik, mro, tio);
                }
                @Override
                FieldValue getAgeField(Row row) {
                    return row.get("age");
                }
            });
    }

    private void testTableKeysIteratorAsyncIndexKey(TableAPI tableImpl,
                                                    Table table)
        throws Exception {

        testTableIteratorAsyncIndexKey(
            tableImpl, table,
            new TableIteratorIndexKeyOp<KeyPair>() {
                @Override
                Publisher<KeyPair> tableIteratorAsync(
                    TableAPI ta, IndexKey ik, MultiRowOptions mro,
                    TableIteratorOptions tio) {
                    return ta.tableKeysIteratorAsync(ik, mro, tio);
                }
                @Override
                FieldValue getAgeField(KeyPair keyPair) {
                    return keyPair.getIndexKey().get("age");
                }
            });
    }

    abstract class TableIteratorIndexKeyOp<T> {
        abstract Publisher<T> tableIteratorAsync(
            TableAPI ta, IndexKey ik, MultiRowOptions mro,
            TableIteratorOptions tio);
        abstract FieldValue getAgeField(T value);
    }

    private <T> void testTableIteratorAsyncIndexKey(
        TableAPI tableImpl, Table table, final TableIteratorIndexKeyOp<T> op)
        throws Exception {

        MultiRowOptions getOptions =
            new MultiRowOptions(table.createFieldRange("gid"));
        Publisher<T> publisher = op.tableIteratorAsync(
            tableImpl, (IndexKey) null, getOptions, iteratorOptions);
        CheckingSubscriber<T> checkingSubscriber =
            new CheckingSubscriber<T>(1);
        publisher.subscribe(checkingSubscriber);
        checkingSubscriber.assertException(IllegalArgumentException.class,
                                           "key must not be null", 1000);

        Index index = table.getIndex("idx1");
        IndexKey ikey = index.createIndexKey();
        ikey.put("age", 21);
        publisher = op.tableIteratorAsync(tableImpl, ikey, null, null);
        try {
            publisher.subscribe(null);
            fail("Expected NullPointerException");
        } catch (NullPointerException e) {
            assertExceptionMessage("subscribe", e);
        }

        publisher = op.tableIteratorAsync(tableImpl, ikey, null, null);
        checkingSubscriber =
            new CheckingSubscriber<T>(10) {
                @Override
                public synchronized void onNext(T result) {
                    super.onNext(result);
                    int age = op.getAgeField(result).asInteger().get();
                    if (age != 21) {
                        onError(
                            new RuntimeException(
                                "Expected age=21, got " + age));
                    }
                }
            };
        publisher.subscribe(checkingSubscriber);
        Subscription subscription = checkingSubscriber.getSubscription(5000);
        subscription.request(11);
        checkingSubscriber.await(5000);
        subscription.cancel();

        /* Test dialog layer exception is thrown as FaultException */
        setDispatcherException(createDialogUnknownException());
        try {
            publisher = op.tableIteratorAsync(tableImpl, ikey, null, null);
            final CountingSubscriber<Object> sub =
                new CountingSubscriber<>(10);
            publisher.subscribe(sub);
            subscription = sub.getSubscription(5000);
            subscription.request(11);
            checkCause(checkException(() -> sub.await(5000),
                                      StoreIteratorException.class),
                       FaultException.class, null);
            subscription.cancel();
        } finally {
            setDispatcherException(null);
        }
    }

    private void testTableIteratorAsyncIterator(TableAPI tableImpl,
                                                Table table)
        throws Exception {

        testTableIteratorAsyncIterator(
            tableImpl, table,
            new TableIteratorIteratorOp<Row>() {
                @Override
                Publisher<Row> tableIteratorAsync(
                    TableAPI ta, Iterator<PrimaryKey> i, MultiRowOptions mro,
                    TableIteratorOptions tio) {
                    return ta.tableIteratorAsync(i, mro, tio);
                }
            });
    }

    private void testTableKeysIteratorAsyncIterator(TableAPI tableImpl,
                                                    Table table)
        throws Exception {

        testTableIteratorAsyncIterator(
            tableImpl, table,
            new TableIteratorIteratorOp<PrimaryKey>() {
                @Override
                Publisher<PrimaryKey> tableIteratorAsync(
                    TableAPI ta, Iterator<PrimaryKey> i,
                    MultiRowOptions mro, TableIteratorOptions tio) {
                    return ta.tableKeysIteratorAsync(i, mro, tio);
                }
            });
    }

    abstract class TableIteratorIteratorOp<T extends Row> {
        abstract Publisher<T> tableIteratorAsync(
            TableAPI ta, Iterator<PrimaryKey> i, MultiRowOptions mro,
            TableIteratorOptions tio);
    }

    private <T extends Row> void testTableIteratorAsyncIterator(
        TableAPI tableImpl, Table table, TableIteratorIteratorOp<T> op)
        throws Exception {

        FieldRange fieldRange = table.createFieldRange("gid")
            .setStart(1, true)
            .setEnd(2, true);
        MultiRowOptions getOptions = fieldRange.createMultiRowOptions();
        Publisher<T> publisher =
            op.tableIteratorAsync(tableImpl, (Iterator<PrimaryKey>) null,
                                  getOptions, iteratorOptions);
        CheckingSubscriber<Object> checkingSubscriber =
            new CheckingSubscriber<>(1);
        publisher.subscribe(checkingSubscriber);
        checkingSubscriber.assertException(
            IllegalArgumentException.class,
            "Primary key iterator should not be null", 1000);

        PrimaryKey key = table.createPrimaryKey();
        publisher = op.tableIteratorAsync(
            tableImpl, Collections.singleton(key).iterator(), null,
            iteratorOptions);
        checkingSubscriber = new CheckingSubscriber<Object>(1);
        publisher.subscribe(checkingSubscriber);
        checkingSubscriber.assertException(
            IllegalArgumentException.class,
            "Direction must be Direction.UNORDERED", 1000);

        key = table.createPrimaryKey();
        key.put("gid", 1);
        publisher = op.tableIteratorAsync(
            tableImpl, Collections.singleton(key).iterator(), null, null);
        try {
            publisher.subscribe(null);
            fail("Expected NullPointerException");
        } catch (NullPointerException e) {
            assertExceptionMessage("subscribe", e);
        }

        /*
         * Subscribing to table iteration that uses iterators to supply the
         * keys blocks if the iteration blocks, but shouldn't. [#27557]
         */
        final Semaphore awaitNextCall = new Semaphore(0);
        final Semaphore allowIteration = new Semaphore(0);
        final PrimaryKey k = key;
        final Iterator<PrimaryKey> iterator = new Iterator<PrimaryKey>() {
            private volatile boolean done;
            @Override
            public boolean hasNext() {
                return !done;
            }
            @Override
            public PrimaryKey next() {
                if (done) {
                    throw new NoSuchElementException();
                }
                awaitNextCall.release(1);
                done = true;
                try {
                    assertTrue(allowIteration.tryAcquire(1, 10000,
                                                         MILLISECONDS));
                } catch (InterruptedException e) {
                    throw new RuntimeException("Unexpected exception", e);
                }
                return k;
            }
        };
        final Publisher<T> p = op.tableIteratorAsync(tableImpl, iterator, null,
                                                     null);
        final CheckingSubscriber<Object> cs = new CheckingSubscriber<>(1);
        final Thread t = new Thread(() -> p.subscribe(cs));
        final AtomicReference<Throwable> exception = new AtomicReference<>();
        t.setUncaughtExceptionHandler((th, e) -> exception.set(e));
        t.start();
        assertTrue(awaitNextCall.tryAcquire(1, 10000, MILLISECONDS));
        /*
         * TODO: The subscribe call currently blocks due to [#27557]
         * TableAPI.tableIteratorAsync can block if key iterator blocks.
         * Uncomment these lines when the problem is fixed.
         *
         * t.join(5000);
         * assertFalse("Thread should be done", t.isAlive());
         */
        allowIteration.release(1);
        t.join(5000);
        assertFalse("Thread should be done", t.isAlive());
        cs.getSubscription(5000).cancel();
        assertNull(exception.get());

        for (int i = 0; i < 1000; i++) {
            publisher = op.tableIteratorAsync(
                tableImpl, Collections.singleton(key).iterator(), null, null);
            checkingSubscriber = new CheckingSubscriber<>(10);
            publisher.subscribe(checkingSubscriber);
            Subscription subscription =
                checkingSubscriber.getSubscription(5000);
            subscription.request(11);
            checkingSubscriber.await(5000);
            subscription.cancel();
        }

        /* Test dialog layer exception is thrown as FaultException */
        setDispatcherException(createDialogUnknownException());
        try {
            publisher = op.tableIteratorAsync(
                tableImpl, Collections.singleton(key).iterator(), null, null);
            final CountingSubscriber<Object> sub =
                new CountingSubscriber<>(10);
            publisher.subscribe(sub);
            Subscription subscription = sub.getSubscription(5000);
            subscription.request(11);
            checkCause(checkException(() -> sub.await(5000),
                                      StoreIteratorException.class),
                       FaultException.class, null);
            subscription.cancel();
        } finally {
            setDispatcherException(null);
        }
    }

    private void testTableIteratorAsyncList(TableAPI tableImpl, Table table)
        throws Exception {

        testTableIteratorAsyncList(
            tableImpl, table,
            new TableIteratorListOp<Row>() {
                @Override
                Publisher<Row> tableIteratorAsync(
                    TableAPI ta,
                    List<Iterator<PrimaryKey>> l,
                    MultiRowOptions mro,
                    TableIteratorOptions tio) {
                    return ta.tableIteratorAsync(l, mro, tio);
                }
            });
    }

    private void testTableKeysIteratorAsyncList(TableAPI tableImpl, Table table)
        throws Exception {

        testTableIteratorAsyncList(
            tableImpl, table,
            new TableIteratorListOp<PrimaryKey>() {
                @Override
                Publisher<PrimaryKey> tableIteratorAsync(
                    TableAPI ta,
                    List<Iterator<PrimaryKey>> l,
                    MultiRowOptions mro,
                    TableIteratorOptions tio) {
                    return ta.tableKeysIteratorAsync(l, mro, tio);
                }
            });
    }

    abstract class TableIteratorListOp<T extends Row> {
        abstract Publisher<T> tableIteratorAsync(
            TableAPI ta,
            List<Iterator<PrimaryKey>> l,
            MultiRowOptions mro,
            TableIteratorOptions tio);
    }

    private <T extends Row> void testTableIteratorAsyncList(
        TableAPI tableImpl, Table table, TableIteratorListOp<T> op)
        throws Exception {

        FieldRange fieldRange = table.createFieldRange("gid")
            .setStart(1, true)
            .setEnd(2, true);
        MultiRowOptions getOptions = fieldRange.createMultiRowOptions();
        Publisher<T> publisher =
            op.tableIteratorAsync(tableImpl, (List<Iterator<PrimaryKey>>) null,
                                  getOptions, iteratorOptions);
        CheckingSubscriber<T> checkingSubscriber =
            new CheckingSubscriber<>(1);
        publisher.subscribe(checkingSubscriber);
        checkingSubscriber.assertException(
            IllegalArgumentException.class,
            "key iterator list cannot be null", 5000);

        PrimaryKey key = table.createPrimaryKey();
        key.put("gid", 1);
        List<Iterator<PrimaryKey>> list =
            Collections.singletonList(Collections.singleton(key).iterator());
        publisher = op.tableIteratorAsync(tableImpl, list, null, null);
        try {
            publisher.subscribe(null);
            fail("Expected NullPointerException");
        } catch (NullPointerException e) {
            assertExceptionMessage("subscribe", e);
        }

        publisher = op.tableIteratorAsync(tableImpl, list, null, null);
        checkingSubscriber = new CheckingSubscriber<>(10);
        publisher.subscribe(checkingSubscriber);
        Subscription subscription =
            checkingSubscriber.getSubscription(5000);
        subscription.request(11);
        checkingSubscriber.await(5000);
        subscription.cancel();

        /* Test dialog layer exception is thrown as FaultException */
        setDispatcherException(createDialogUnknownException());
        try {
            list = Collections.singletonList(
                Collections.singleton(key).iterator());
            publisher = op.tableIteratorAsync(tableImpl, list, null, null);
            final CountingSubscriber<Object> sub =
                new CountingSubscriber<>(10);
            publisher.subscribe(sub);
            subscription = sub.getSubscription(5000);
            subscription.request(11);
            checkCause(checkException(() -> sub.await(5000),
                                      StoreIteratorException.class),
                       FaultException.class, null);
            subscription.cancel();
        } finally {
            setDispatcherException(null);
        }
    }

    private void testPutAsync(TableAPI tableImpl, Table table)
        throws Exception {

        ReturnRow returnRow = table.createReturnRow(ReturnRow.Choice.ALL);
        CompletableFuture<Version> future =
            tableImpl.putAsync(null, returnRow, null);
        assertException(IllegalArgumentException.class, "row must not be null",
                        future, 0);

        Row row = table.createRow();
        tableImpl.putAsync(row, returnRow, null);
        assertException(IllegalArgumentException.class, "row must not be null",
                        future, 0);

        PrimaryKey key = table.createPrimaryKey();
        key.put("gid", 0).put("uid", 0);
        row = tableImpl.getAsync(key, null).get(5000, MILLISECONDS);

        Row putRow = row.clone();
        putRow.put("age", row.get("age").asInteger().get() + 1);
        Version version =
            tableImpl.putAsync(putRow, returnRow, null).get(5000, MILLISECONDS);
        assertNotEquals("New row version", row.getVersion(), version);
        assertEquals("Old row", 0, row.compareTo(returnRow));

        /* Test dialog layer exception is thrown as FaultException */
        setDispatcherException(createDialogUnknownException());
        try {
            assertException(FaultException.class, null,
                            tableImpl.putAsync(putRow, returnRow, null),
                            5000);
        } finally {
            setDispatcherException(null);
        }
    }

    @SuppressWarnings("deprecation")
    private void testPutAsyncInternal(TableAPI tableImpl, Table table)
        throws Exception {

        Row row = genUsersRow(table, 0, 0);
        row.put("age", row.get("age").asInteger().get() + 1);
        CompletableFuture<Result> future =
            ((TableAPIImpl) tableImpl).putAsyncInternal(
                (RowImpl) row, null, null);
        future.get(5000, MILLISECONDS);

        /* Test dialog layer exception is thrown as FaultException */
        setDispatcherException(createDialogUnknownException());
        try {
            assertException(FaultException.class, null,
                            ((TableAPIImpl) tableImpl).putAsyncInternal(
                                (RowImpl) row, null, null),
                            5000);
        } finally {
            setDispatcherException(null);
        }
    }

    private void testPutIfAbsentAsync(TableAPI tableImpl, Table table)
        throws Exception {

        PrimaryKey key = table.createPrimaryKey();
        key.put("gid", 0).put("uid", 0);
        tableImpl.deleteAsync(key, null, null).get(5000, MILLISECONDS);

        ReturnRow returnRow = table.createReturnRow(ReturnRow.Choice.ALL);
        Row putRow = genUsersRow(table, 0, 0);
        Version version = tableImpl.putIfAbsentAsync(putRow, returnRow, null)
            .get(5000, MILLISECONDS);
        assertNotNull("New row version", version);
        assertEquals("Old row version", null, returnRow.getVersion());

        /* Test dialog layer exception is thrown as FaultException */
        setDispatcherException(createDialogUnknownException());
        try {
            assertException(FaultException.class, null,
                            tableImpl.putIfAbsentAsync(putRow, returnRow, null),
                            5000);
        } finally {
            setDispatcherException(null);
        }
    }

    @SuppressWarnings("deprecation")
    private void testPutIfAbsentAsyncInternal(TableAPI tableImpl, Table table)
        throws Exception {

        Row row = genUsersRow(table, 0, 0);
        row.put("age", row.get("age").asInteger().get() + 1);
        CompletableFuture<Result> future =
            ((TableAPIImpl) tableImpl).putIfAbsentAsyncInternal(
                (RowImpl) row, null, null);
        future.get(5000, MILLISECONDS);

        /* Test dialog layer exception is thrown as FaultException */
        setDispatcherException(createDialogUnknownException());
        try {
            assertException(
                FaultException.class, null,
                ((TableAPIImpl) tableImpl).putIfAbsentAsyncInternal(
                    (RowImpl) row, null, null),
                5000);
        } finally {
            setDispatcherException(null);
        }
    }

    private void testPutIfPresentAsync(TableAPI tableImpl, Table table)
        throws Exception {

        PrimaryKey key = table.createPrimaryKey();
        key.put("gid", 0).put("uid", 0);
        Row row = tableImpl.getAsync(key, null).get(5000, MILLISECONDS);

        ReturnRow returnRow = table.createReturnRow(ReturnRow.Choice.ALL);
        CompletableFuture<Version> future =
            tableImpl.putIfPresentAsync(null, returnRow, writeOptions);
        assertException(IllegalArgumentException.class, "row must not be null",
                        future, 0);

        Row putRow = row.clone();
        putRow.put("age", row.get("age").asInteger().get() + 1);
        Version version = tableImpl.putIfPresentAsync(putRow, returnRow, null)
            .get(5000, MILLISECONDS);
        assertNotNull("New row version", version);
        assertNotEquals("New row version", row.getVersion(), version);
        assertEquals("Old row version", 0, row.compareTo(returnRow));

        /* Test dialog layer exception is thrown as FaultException */
        setDispatcherException(createDialogUnknownException());
        try {
            assertException(FaultException.class, null,
                            tableImpl.putIfPresentAsync(putRow, returnRow,
                                                        null),
                            5000);
        } finally {
            setDispatcherException(null);
        }
    }

    @SuppressWarnings("deprecation")
    private void testPutIfPresentAsyncInternal(TableAPI tableImpl, Table table)
        throws Exception {

        Row row = genUsersRow(table, 0, 0);
        row.put("age", row.get("age").asInteger().get() + 1);
        CompletableFuture<Result> future =
            ((TableAPIImpl) tableImpl).putIfPresentAsyncInternal(
                (RowImpl) row, null, null);
        future.get(5000, MILLISECONDS);

        /* Test dialog layer exception is thrown as FaultException */
        setDispatcherException(createDialogUnknownException());
        try {
            assertException(
                FaultException.class, null,
                ((TableAPIImpl) tableImpl).putIfPresentAsyncInternal(
                    (RowImpl) row, null, null),
                5000);
        } finally {
            setDispatcherException(null);
        }
    }

    private void testPutIfVersionAsync(TableAPI tableImpl, Table table)
        throws Exception {

        PrimaryKey key = table.createPrimaryKey();
        key.put("gid", 0).put("uid", 0);

        /*
         * Get the current value using absolute consistency to make sure that
         * the version is the one on the master
         */
        Row row = tableImpl.getAsync(key,
                                     new ReadOptions(Consistency.ABSOLUTE,
                                                     5000, MILLISECONDS))
            .get(5000, MILLISECONDS);

        Version version = row.getVersion();
        ReturnRow returnRow = table.createReturnRow(ReturnRow.Choice.ALL);
        CompletableFuture<Version> future =
            tableImpl.putIfVersionAsync(null, version, returnRow,
                                        writeOptions);
        assertException(IllegalArgumentException.class, "row must not be null",
                        future, 0);

        Row putRow = row.clone();
        putRow.put("age", row.get("age").asInteger().get() + 1);
        Version newVersion =
            tableImpl.putIfVersionAsync(putRow, version, null, null)
            .get(5000, MILLISECONDS);
        assertNotNull("Operation should succeed and return non-null" +
                      " because the version should match",
                      newVersion);
        assertNotEquals("New row version", version, newVersion);

        /* Test dialog layer exception is thrown as FaultException */
        setDispatcherException(createDialogUnknownException());
        try {
            assertException(FaultException.class, null,
                            tableImpl.putIfVersionAsync(putRow, version,
                                                        null, null),
                            5000);
        } finally {
            setDispatcherException(null);
        }
    }

    @SuppressWarnings("deprecation")
    private void testPutIfVersionAsyncInternal(TableAPI tableImpl, Table table)
        throws Exception {

        PrimaryKey key = table.createPrimaryKey();
        key.put("gid", 0).put("uid", 0);
        Row row = tableImpl.getAsync(key, null).get(5000, MILLISECONDS);
        Version version = row.getVersion();
        row = row.clone();
        row.put("age", row.get("age").asInteger().get() + 1);
        CompletableFuture<Result> future =
            ((TableAPIImpl) tableImpl).putIfVersionAsyncInternal(
                (RowImpl) row, version, null, null);
        future.get(5000, MILLISECONDS);

        /* Test dialog layer exception is thrown as FaultException */
        setDispatcherException(createDialogUnknownException());
        try {
            assertException(
                FaultException.class, null,
                ((TableAPIImpl) tableImpl).putIfVersionAsyncInternal(
                    (RowImpl) row, version, null, null),
                5000);
        } finally {
            setDispatcherException(null);
        }
    }

    private void testDeleteAsync(TableAPI tableImpl, Table table)
        throws Exception {

        Row row = genUsersRow(table, 0, 0);
        tableImpl.putAsync(row, null, null).get(5000, MILLISECONDS);

        ReturnRow returnRow = table.createReturnRow(ReturnRow.Choice.ALL);
        CompletableFuture<Boolean> future =
            tableImpl.deleteAsync(null, returnRow, writeOptions);
        assertException(IllegalArgumentException.class, "key must not be null",
                        future, 0);

        PrimaryKey key = table.createPrimaryKey();
        key.put("gid", 0).put("uid", 0);
        boolean deleted =
            tableImpl.deleteAsync(key, null, null).get(5000, MILLISECONDS);
        assertEquals("Deleted result", true, deleted);

        /* Test dialog layer exception is thrown as FaultException */
        setDispatcherException(createDialogUnknownException());
        try {
            assertException(FaultException.class, null,
                            tableImpl.deleteAsync(key, null, null),
                            5000);
        } finally {
            setDispatcherException(null);
        }
    }

    private void testDeleteAsyncInternal(TableAPI tableImpl, Table table)
        throws Exception {

        PrimaryKey key = table.createPrimaryKey();
        key.put("gid", 0).put("uid", 0);
        CompletableFuture<Result> future =
            ((TableAPIImpl) tableImpl).deleteAsyncInternal(
                (PrimaryKeyImpl) key, null, null);
        future.get(5000, MILLISECONDS);

        /* Test dialog layer exception is thrown as FaultException */
        setDispatcherException(createDialogUnknownException());
        try {
            assertException(
                FaultException.class, null,
                ((TableAPIImpl) tableImpl).deleteAsyncInternal(
                    (PrimaryKeyImpl) key, null, null),
                5000);
        } finally {
            setDispatcherException(null);
        }
    }

    private void testDeleteIfVersionAsync(TableAPI tableImpl, Table table)
        throws Exception {

        Row row = genUsersRow(table, 0, 0);
        Version version =
            tableImpl.putAsync(row, null, null).get(5000, MILLISECONDS);

        ReturnRow returnRow = table.createReturnRow(ReturnRow.Choice.ALL);
        CompletableFuture<Boolean> future =
            tableImpl.deleteIfVersionAsync(null, version, returnRow,
                                           writeOptions);
        assertException(IllegalArgumentException.class, "key must not be null",
                        future, 0);

        PrimaryKey key = table.createPrimaryKey();
        key.put("gid", 0).put("uid", 0);
        boolean deleted =
            tableImpl.deleteIfVersionAsync(key, version, null, null)
            .get(5000, MILLISECONDS);
        assertEquals("Deleted result", true, deleted);

        /* Test dialog layer exception is thrown as FaultException */
        setDispatcherException(createDialogUnknownException());
        try {
            assertException(FaultException.class, null,
                            tableImpl.deleteIfVersionAsync(key, version,
                                                           null, null),
                            5000);
        } finally {
            setDispatcherException(null);
        }
    }

    private void testDeleteIfVersionAsyncInternal(TableAPI tableImpl,
                                                  Table table)
        throws Exception {

        Row row = genUsersRow(table, 0, 0);
        Version version =
            tableImpl.putAsync(row, null, null).get(5000, MILLISECONDS);
        PrimaryKey key = table.createPrimaryKey();
        key.put("gid", 0).put("uid", 0);
        CompletableFuture<Result> future =
            ((TableAPIImpl) tableImpl).deleteIfVersionAsyncInternal(
                (PrimaryKeyImpl) key, version, null, null);
        future.get(5000, MILLISECONDS);

        /* Test dialog layer exception is thrown as FaultException */
        setDispatcherException(createDialogUnknownException());
        try {
            assertException(
                FaultException.class, null,
                ((TableAPIImpl) tableImpl).deleteIfVersionAsyncInternal(
                    (PrimaryKeyImpl) key, version, null, null),
                5000);
        } finally {
            setDispatcherException(null);
        }
    }

    private void testMultiDeleteAsync(TableAPI tableImpl, Table table)
        throws Exception {

        MultiRowOptions getOptions =
            new MultiRowOptions(table.createFieldRange("gid"));
        CompletableFuture<Integer> future =
            tableImpl.multiDeleteAsync(null, getOptions, writeOptions);
        assertException(IllegalArgumentException.class, "key must not be null",
                        future, 0);

        PrimaryKey key = table.createPrimaryKey();
        key.put("gid", 1);
        int count =
            tableImpl.multiDeleteAsync(key, null, null).get(5000, MILLISECONDS);
        assertEquals("MultiDeleteAsync count", 10, count);

        /* Test dialog layer exception is thrown as FaultException */
        setDispatcherException(createDialogUnknownException());
        try {
            assertException(FaultException.class, null,
                            tableImpl.multiDeleteAsync(key, null, null),
                            5000);
        } finally {
            setDispatcherException(null);
        }
    }

    private void testMultiDeleteAsyncInternal(TableAPI tableImpl, Table table)
        throws Exception {

        PrimaryKey key = table.createPrimaryKey();
        key.put("gid", 1);
        CompletableFuture<Result> future =
            ((TableAPIImpl) tableImpl).multiDeleteAsyncInternal(
                (PrimaryKeyImpl) key, null, null, null);
        future.get(5000, MILLISECONDS);

        /* Test dialog layer exception is thrown as FaultException */
        setDispatcherException(createDialogUnknownException());
        try {
            assertException(
                FaultException.class, null,
                ((TableAPIImpl) tableImpl).multiDeleteAsyncInternal(
                    (PrimaryKeyImpl) key, null, null, null),
                5000);
        } finally {
            setDispatcherException(null);
        }
    }

    private void testExecuteAsync(TableAPI tableImpl, Table table)
        throws Exception {

        CompletableFuture<List<TableOperationResult>> future =
            tableImpl.executeAsync(null, writeOptions);
        assertException(IllegalArgumentException.class,
                        "operations must not be null", future, 0);

        TableOperationFactory factory = tableImpl.getTableOperationFactory();
        List<TableOperation> ops = new ArrayList<TableOperation>();
        PrimaryKey key = table.createPrimaryKey();
        key.put("gid", 2);
        TableIterator<Row> itr = tableImpl.tableIterator(key, null, null);
        while (itr.hasNext()) {
            Row row = itr.next();
            row.put("age", row.get("age").asInteger().get() + 1);
            ops.add(factory.createPutIfPresent(row, null, true));
        }
        List<TableOperationResult> results = tableImpl.executeAsync(ops, null)
            .get(5000, MILLISECONDS);
        assertEquals("Wrong number of TableOperationResult",
                     results.size(), ops.size());
        for (int i = 0; i < results.size(); i++) {
            TableOperationResult result = results.get(i);
            assertTrue("The operation result should be success",
                       result.getSuccess());
        }

        /* Test dialog layer exception is thrown as FaultException */
        setDispatcherException(createDialogUnknownException());
        try {
            assertException(FaultException.class, null,
                            tableImpl.executeAsync(ops, null),
                            5000);
        } finally {
            setDispatcherException(null);
        }

        /* Test an exception thrown by the operation */
        ops.clear();
        itr = tableImpl.tableIterator(key, null, null);
        Row row = itr.next();
        row.put("age", row.get("age").asInteger().get() + 1);
        ops.add(factory.createPutIfAbsent(row, null, true));
        assertException(TableOpExecutionException.class, null,
                        tableImpl.executeAsync(ops, null),
                        5000);
    }

    private void testExecuteAsyncInternal(TableAPI tableImpl, Table table)
        throws Exception {

        TableOperationFactory factory = tableImpl.getTableOperationFactory();
        List<TableOperation> ops = new ArrayList<TableOperation>();
        PrimaryKey key = table.createPrimaryKey();
        key.put("gid", 2);
        TableIterator<Row> itr = tableImpl.tableIterator(key, null, null);
        while (itr.hasNext()) {
            Row row = itr.next();
            row.put("age", row.get("age").asInteger().get() + 1);
            ops.add(factory.createPutIfPresent(row, null, true));
        }
        CompletableFuture<Result> future =
            ((TableAPIImpl) tableImpl).executeAsyncInternal(ops, null);
        future.get(5000, MILLISECONDS);

        /* Test dialog layer exception is thrown as FaultException */
        setDispatcherException(createDialogUnknownException());
        try {
            assertException(
                FaultException.class, null,
                ((TableAPIImpl) tableImpl).executeAsyncInternal(ops, null),
                5000);
        } finally {
            setDispatcherException(null);
        }

        /* Test an exception thrown by the operation */
        ops.clear();
        itr = tableImpl.tableIterator(key, null, null);
        Row row = itr.next();
        row.put("age", row.get("age").asInteger().get() + 1);
        ops.add(factory.createPutIfAbsent(row, null, true));
        assertException(
            TableOpExecutionException.class, null,
            ((TableAPIImpl) tableImpl).executeAsyncInternal(ops, null),
            5000);
    }

    private void testMultiGetContinuationAsync(TableAPIImpl tableImpl,
                                               Table table)
        throws Exception {

        testMultiGetContinuationAsync(
            tableImpl,
            table,
            new MultiGetContinuationOp<Row>() {
                @Override
                CompletableFuture<MultiGetResult<Row>>
                    multiGetAsync(TableAPIImpl tai, PrimaryKey pk,
                                  byte[] ck, MultiRowOptions mro,
                                  TableIteratorOptions tio) {
                    return tai.multiGetAsync(pk, ck, mro, tio);
                }
            });
    }

    private void testMultiGetKeysContinuationAsync(TableAPIImpl tableImpl,
                                                   Table table)
        throws Exception {

        testMultiGetContinuationAsync(
            tableImpl,
            table,
            new MultiGetContinuationOp<PrimaryKey>() {
                @Override
                CompletableFuture<MultiGetResult<PrimaryKey>>
                    multiGetAsync(TableAPIImpl tai, PrimaryKey pk,
                                  byte[] ck, MultiRowOptions mro,
                                  TableIteratorOptions tio)
                {
                    return tai.multiGetKeysAsync(pk, ck, mro, tio);
                }
            });
    }

    abstract class MultiGetContinuationOp<T extends Row> {
        abstract CompletableFuture<MultiGetResult<T>>
            multiGetAsync(TableAPIImpl tai, PrimaryKey pk, byte[] ck,
                          MultiRowOptions mro, TableIteratorOptions tio);
    }

    private <T extends Row> void testMultiGetContinuationAsync(
        TableAPIImpl tableImpl, Table table, MultiGetContinuationOp<T> op)
        throws Exception {

        MultiRowOptions getOptions =
            new MultiRowOptions(table.createFieldRange("gid"));
        CompletableFuture<MultiGetResult<T>> future =
            op.multiGetAsync(tableImpl, (PrimaryKey) null, (byte[]) null,
                             getOptions, iteratorOptions);
        assertException(IllegalArgumentException.class,
                        "rowKey must not be null", future, 0);

        PrimaryKey key = table.createPrimaryKey();
        key.put("gid", 1);
        TableIteratorOptions iterOptions =
            new TableIteratorOptions(Direction.UNORDERED, null,
                                     5000, MILLISECONDS,
                                     0, 1 /* batchResultsSize */);
        byte[] continuation = null;
        int count = 0;
        for (int i = 0; i < 11; i++) {
            MultiGetResult<T> getResult =
                op.multiGetAsync(tableImpl, key, continuation, null,
                                 iterOptions)
                .get(5000, MILLISECONDS);
            List<T> result = getResult.getResult();
            assertTrue("Expect one plus batch size or fewer: " + result,
                       result.size() <= 2);
            count += result.size();
            continuation = getResult.getContinuationKey();
            if (continuation == null) {
                break;
            }
        }
        assertEquals(10, count);

        /* Test dialog layer exception is thrown as FaultException */
        setDispatcherException(createDialogUnknownException());
        try {
            assertException(FaultException.class, null,
                            op.multiGetAsync(tableImpl, key, null, null,
                                             iterOptions),
                            5000);
        } finally {
            setDispatcherException(null);
        }
    }

    private void testMultiGetContinuationAsyncIndexKey(TableAPIImpl tableImpl,
                                                       Table table)
        throws Exception {

        testMultiGetContinuationAsyncIndexKey(
            tableImpl,
            table,
            new MultiGetContinuationOpIndexKey<Row>() {
                @Override
                CompletableFuture<MultiGetResult<Row>>
                    multiGetAsync(TableAPIImpl tai, IndexKey ik,
                                  byte[] ck, MultiRowOptions mro,
                                  TableIteratorOptions tio) {
                    return tai.multiGetAsync(ik, ck, mro, tio);
                }
            });
    }

    private void testMultiGetKeysContinuationAsyncIndexKey(
        TableAPIImpl tableImpl, Table table)
        throws Exception {

        testMultiGetContinuationAsyncIndexKey(
            tableImpl,
            table,
            new MultiGetContinuationOpIndexKey<KeyPair>() {
                @Override
                CompletableFuture<MultiGetResult<KeyPair>>
                    multiGetAsync(TableAPIImpl tai, IndexKey ik,
                                  byte[] ck, MultiRowOptions mro,
                                  TableIteratorOptions tio)
                {
                    return tai.multiGetKeysAsync(ik, ck, mro, tio);
                }
            });
    }

    abstract class MultiGetContinuationOpIndexKey<T> {
        abstract CompletableFuture<MultiGetResult<T>>
            multiGetAsync(TableAPIImpl tai, IndexKey ik, byte[] ck,
                          MultiRowOptions mro, TableIteratorOptions tio);
    }

    private <T> void testMultiGetContinuationAsyncIndexKey(
        TableAPIImpl tableImpl, Table table,
        MultiGetContinuationOpIndexKey<T> op)
        throws Exception {

        MultiRowOptions getOptions =
            new MultiRowOptions(table.createFieldRange("gid"));
        CompletableFuture<MultiGetResult<T>> future =
            op.multiGetAsync(tableImpl, (IndexKey) null, (byte[]) null,
                             getOptions, iteratorOptions);
        assertException(IllegalArgumentException.class,
                        "indexKeyArg must not be null", future, 0);

        Index index = table.getIndex("idx1");
        IndexKey ikey = index.createIndexKey();
        ikey.put("age", 21);
        TableIteratorOptions iterOptions =
            new TableIteratorOptions(Direction.UNORDERED, null,
                                     5000, MILLISECONDS,
                                     0, 1 /* batchResultsSize */);
        byte[] continuation = null;
        int count = 0;
        for (int i = 0; i < 11; i++) {
            MultiGetResult<T> getResult =
                op.multiGetAsync(tableImpl, ikey, continuation, null,
                                 iterOptions)
                .get(5000, MILLISECONDS);
            List<T> result = getResult.getResult();
            assertTrue("Expect one plus batch size or fewer: " + result,
                       result.size() <= 2);
            count += result.size();
            continuation = getResult.getContinuationKey();
            if (continuation == null) {
                break;
            }
        }
        assertEquals(10, count);

        /* Test dialog layer exception is thrown as FaultException */
        setDispatcherException(createDialogUnknownException());
        try {
            assertException(FaultException.class, null,
                            op.multiGetAsync(tableImpl, ikey, null, null,
                                             iterOptions),
                            5000);
        } finally {
            setDispatcherException(null);
        }
    }

    private void testExecuteAsyncQueryString(Table table)
        throws Exception {

        testExecuteAsyncQuery(
            table,
            new ExecuteOp() {
                @Override
                Publisher<RecordValue> executeAsync(String query,
                                                    ExecuteOptions options) {
                    return store.executeAsync(query, options);
                }
            });
    }

    private void testExecuteAsyncQueryStatement(Table table)
        throws Exception {

        testExecuteAsyncQuery(
            table,
            new ExecuteOp() {
                @Override
                Publisher<RecordValue> executeAsync(String query,
                                                    ExecuteOptions options) {
                    return store.executeAsync(
                        (query == null) ? null : store.prepare(query),
                        options);
                }
            });
    }

    private abstract class ExecuteOp {
        abstract Publisher<RecordValue> executeAsync(String query,
                                                     ExecuteOptions options);
    }

    private void testExecuteAsyncQuery(Table table, ExecuteOp op)
        throws Exception {

        ExecuteOptions executeOptions = new ExecuteOptions();
        try {
            op.executeAsync((String) null, executeOptions);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }

        final String query = "SELECT * FROM " + table.getName();
        Publisher<RecordValue> publisher = op.executeAsync(query, null);
        BlockingSubscriber subscriber = new BlockingSubscriber();
        publisher.subscribe(subscriber);
        subscriber.assertNotComplete(2);
        publisher = op.executeAsync(query, null);
        publisher.subscribe(subscriber);
        subscriber.assertException(AssertionError.class,
                                   "onSubscribe must only be called once",
                                   1000);
        subscriber.assertTerminated();

        /* Async DDL queries should fail */
        final String ddlQuery = "CREATE TABLE t (i INTEGER, PRIMARY KEY (i))";
        try {
            publisher = op.executeAsync(ddlQuery, null);
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
        }

        /* ALL_SHARDS */
        doExecuteAsync(query, op, 100);

        /* SINGLE_PARTITION */
        doExecuteAsync(query + " WHERE gid = 3 AND uid = 4", op, 1);

        /* ALL_PARTITIONS */
        doExecuteAsync(query + " WHERE firstName = 'first_2_5'", op, 1);
    }

    private void doExecuteAsync(String query, ExecuteOp op, int expectedCount)
        throws InterruptedException {

        Publisher<RecordValue> publisher = op.executeAsync(query, null);
        final CheckingSubscriber<RecordValue> subscriber =
            new CheckingSubscriber<>(expectedCount);
        publisher.subscribe(subscriber);
        Subscription subscription = subscriber.getSubscription(5000);
        subscription.request(expectedCount+1);
        subscriber.await(5000);
        subscription.cancel();

        /* Test dialog layer exception is thrown as FaultException */
        setDispatcherException(createDialogUnknownException());
        try {
            publisher = op.executeAsync(query, null);
            final CountingSubscriber<Object> sub =
                new CountingSubscriber<>(expectedCount);
            publisher.subscribe(sub);
            final Subscription s = sub.getSubscription(5000);
            s.request(expectedCount+1);
            checkException(() -> sub.await(5000), FaultException.class);
            s.cancel();
        } finally {
            setDispatcherException(null);
        }
    }

    private Row genUsersRow(Table table, int gid, int uid) {
        Row row = table.createRow();
        row.put("gid", gid);
        row.put("uid", uid);
        row.put("firstName", "first_" + gid + "_" + uid);
        row.put("lastName", "last_" + gid + "_" + uid);
        row.put("age", 20 + uid);
        return row;
    }

    private class TestStream implements EntryStream<Row> {

        private boolean isCompleted = false;
        private final int groupId;
        private final int nUsers;
        private final Table table;
        private int i;

        TestStream(Table table, int gid, int nUsers) {
            super();
            this.table = table;
            this.groupId = gid;
            this.nUsers = nUsers;
            i = 0;
        }

        @Override
        public String name() {
            return "TestRowStream";
        }

        @Override
        public Row getNext() {
            if (i == nUsers) {
                return null;
            }
            return genUsersRow(table, groupId, i++);
        }

        @Override
        public void completed() {
            isCompleted = true;
        }

        @Override
        public void keyExists(Row entry) {
        }

        @Override
        public void catchException(RuntimeException runtimeException,
                                   Row entry) {
            throw runtimeException;
        }
    }

    private static void maybeRethrowException(Throwable e) {
        if (e == null) {
            return;
        }
        if (e instanceof Error) {
            CommonLoggerUtils.appendCurrentStack(e);
            throw (Error) e;
        }
        if (e instanceof RuntimeException) {
            CommonLoggerUtils.appendCurrentStack(e);
            throw (RuntimeException) e;
        }
        throw new RuntimeException("Unexpected exception: " + e, e);
    }

    private static class CountingSubscriber<R> implements Subscriber<R> {
        final long maxCount;
        private Subscription subscription;
        private long count;
        private Throwable exception;
        private boolean complete;
        CountingSubscriber(long maxCount) {
            this.maxCount = maxCount;
        }
        @Override
        public synchronized void onSubscribe(Subscription s) {
            assertNull("onSubscribe must only be called once", subscription);
            assertNotNull("Parameter to onSubscribe must not be null", s);
            subscription = s;
        }
        @Override
        public synchronized void onNext(R result) {
            assertOnSubscribeCalled();
            assertNotTerminated();
            if (count >= maxCount) {
                throw new RuntimeException(
                    "Subscriber.onNext called after max count: " + this);
            }
            count++;
        }
        @Override
        public synchronized void onError(Throwable e) {
            assertOnSubscribeCalled();
            assertNotTerminated();
            assertNotNull("Parameter to onError must not be null", e);
            exception = e;
        }
        @Override
        public synchronized void onComplete() {
            assertOnSubscribeCalled();
            assertNotTerminated();
            complete = true;
        }
        private void assertOnSubscribeCalled() {
            assertNotNull("onSubscribe should have been called",
                          subscription);
        }
        private void assertNotTerminated() {
            assertFalse("onComplete should not have been called", complete);
            assertNull("onError should not have been called", exception);
        }
        synchronized void assertTerminated() {
            assertTrue("onComplete or onError should have been called:" +
                       this,
                       complete || (exception != null));
        }
        @Override
        public synchronized String toString() {
            return super.toString() + "[" +
                "maxCount=" + maxCount +
                " subscription=" + subscription +
                " count=" + count +
                " exception=" + exception +
                " complete=" + complete + "]";
        }
        synchronized Subscription getSubscription(long timeoutMillis)
            throws InterruptedException
        {
            if (timeoutMillis < 0) {
                throw new IllegalArgumentException(
                    "Timeout must be 0 or greater");
            }
            final long stop = System.currentTimeMillis() + timeoutMillis;
            while (subscription == null) {
                final long wait = stop - System.currentTimeMillis();
                if (wait <= 0) {
                    break;
                }
                wait(wait);
            }
            assertNotNull("subscription", subscription);
            return subscription;
        }
        void await(long timeoutMillis) throws InterruptedException {
            await(timeoutMillis, maxCount);
        }
        synchronized void await(long timeoutMillis, long awaitCount)
            throws InterruptedException {

            if (timeoutMillis < 0) {
                throw new IllegalArgumentException(
                    "Timeout must be 0 or greater");
            }
            final long stop = System.currentTimeMillis() + timeoutMillis;
            while (!isDone(awaitCount)) {
                final long wait = stop - System.currentTimeMillis();
                if (wait <= 0) {
                    break;
                }
                wait(wait);
            }
            maybeRethrowException(getException());
            if (awaitCount > count) {
                throw new RequestTimeoutException(
                    (int) timeoutMillis,
                    "Request timed out with awaitCount=" + awaitCount +
                    " count=" + count,
                    null, false);
            }
        }
        synchronized boolean isDone(long awaitCount) {
            return (count >= awaitCount) || (exception != null);
        }
        synchronized Throwable getException() {
            return exception;
        }
        void assertException(Class<? extends Throwable> exceptionClass,
                             String message,
                             long timeout) {
            try {
                await(timeout);
            } catch (Throwable ex) {
                if (!exceptionClass.isInstance(ex)) {
                    throw new AssertionError("Exception should be " +
                                             exceptionClass.getName() +
                                             ", found " + ex,
                                             ex);
                }
                if (message != null) {
                    assertExceptionMessage(message, ex);
                }
                return;
            }
            fail("Expected " + exceptionClass.getName());
        }
    }

    private static class CheckingSubscriber<R> extends CountingSubscriber<R> {
        private Throwable exception;
        private boolean completed;
        CheckingSubscriber(long maxCount) {
            super(maxCount);
        }
        @Override
        public synchronized void onComplete() {
            super.onComplete();
            if (!super.isDone(maxCount)) {
                exception = new RuntimeException(
                    "onComplete called before Subscriber is done: " + this);
            } else if (completed) {
                exception = new RuntimeException(
                    "onComplete called more than once: " + this);
            }
            completed = true;
            notifyAll();
        }
        @Override
        public synchronized String toString() {
            return super.toString() + "[" +
                "completed=" + completed + " exception=" + exception + "]";
        }
        @Override
        synchronized void await(long timeoutMillis)
            throws InterruptedException {

            super.await(timeoutMillis);
            if (completed) {
                return;
            } else if (exception instanceof RuntimeException) {
                throw (RuntimeException) exception;
            } else if (exception instanceof Error) {
                throw (Error) exception;
            } else if (exception != null) {
                throw new RuntimeException(
                    "Unexpected exception: " + exception, exception);
            } else {
                throw new RequestTimeoutException(
                    (int) timeoutMillis, "Request not completed: " + this,
                    null, false);
            }
        }
        @Override
        synchronized boolean isDone(long awaitCount) {
            return completed && super.isDone(awaitCount);
        }
        @Override
        synchronized Throwable getException() {
            if (exception != null) {
                return exception;
            }
            return super.getException();
        }
        synchronized void assertNotComplete(long timeout)
            throws InterruptedException {

            try {
                await(timeout);
            } catch (RequestTimeoutException e) {
            }
            assertFalse("Subscriber should not be completed: " + this,
                        completed);
        }
    }

    private static class BlockingSubscriber
            extends CheckingSubscriber<Object> {
        BlockingSubscriber() {
            super(0);
        }
    }
}
