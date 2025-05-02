/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.pubsub;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import oracle.kv.KVStoreFactory;
import oracle.kv.StatementResult;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.pubsub.StreamDelEvent;
import oracle.kv.impl.pubsub.StreamPutEvent;
import oracle.kv.impl.pubsub.StreamSequenceId;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;

import org.junit.After;
import org.junit.Before;

/**
 * Shared methods for dynamic filter related unit tests
 */
public class StreamDynamicFilterTestBase extends PubSubTestBase {

    final static int INS_ROWS = 1024;
    final static int UPD_ROWS = INS_ROWS / 2;
    final static int DEL_ROWS = UPD_ROWS / 2;

    private final static String USER_TABLE_NS = "UserNS";
    private final static String USER_TABLE = "User";
    private final static String TEST_TABLE_NS = "TestNS";
    protected final static String TEST_TABLE = "Test";
    final static String NOT_EXIST_TABLE = "NonExistTable";

    final String userTableName =
        NameUtils.makeQualifiedName(USER_TABLE_NS, USER_TABLE);
    final String testTableName =
        NameUtils.makeQualifiedName(TEST_TABLE_NS, TEST_TABLE);

    protected NoSQLPublisher publisher;

    @Before
    @Override
    public void setUp() throws Exception {

        super.setUp();

        kvstoreName = "mystore";
        /* 1 x 1 store **/
        repFactor = 1;
        numStorageNodes = 1;
        numPartitions = 12;
        useFeederFilter = true;

        startStore();

        kvStoreConfig = createKVConfig(createStore);
        store = KVStoreFactory.getStore(kvStoreConfig);
        createDefaultTestNameSpace();
        tableAPI = store.getTableAPI();

        traceOnScreen = false;
        publisher = getTestPublisher();
    }

    @After
    @Override
    public void tearDown() throws Exception {

        if (publisher != null && !publisher.isClosed()) {
            publisher.close(true);
        }

        if (store != null) {
            store.close();
        }

        if (createStore != null) {
            createStore.shutdown();
        }

        super.tearDown();
    }

    private NoSQLPublisher getTestPublisher() {
        /* create a publisher */
        final NoSQLPublisherConfig pconf =
            new NoSQLPublisherConfig.Builder(kvStoreConfig, testPath)
                .build();
        return NoSQLPublisher.get(pconf, logger);
    }

    void initAllTables() {

        createNamespace(USER_TABLE_NS);
        createNamespace(TEST_TABLE_NS);

        createTable(userTableName);
        trace("Table " + userTableName + " created with " + INS_ROWS +
              " rows");

        createTable(testTableName);
        trace("Table " + testTableName + " created with " + INS_ROWS +
              " rows");

    }

    protected void createTable(String tableName) {

        /* create test tables */
        final String ddl = "CREATE TABLE " + tableName + " " +
                           "(id INTEGER, tableName STRING, name STRING, " +
                           "PRIMARY KEY (id, tableName))";

        store.executeSync(ddl);
        assertNotNull("table " + tableName + " not created",
                      tableAPI.getTable(tableName));

        writeTable(tableName, INS_ROWS);
        trace(INS_ROWS + " rows updated or inserted into table " + tableName);
    }

    void writeTable(final String tableName, final int num) {
        for (int i = 0; i < num; i++) {
            final Row row = tableAPI.getTable(tableName).createRow();
            row.put("id", i);
            row.put("tableName", tableName);
            row.put("name", "name-" + random.nextInt(Integer.MAX_VALUE));
            tableAPI.put(row, null, null);
        }
        trace(num + " rows updated to table " + tableName);
    }

    void deleteTable(final String tableName) {
        for (int i = 0; i < DEL_ROWS; i++) {
            final PrimaryKey pkey =
                tableAPI.getTable(tableName).createPrimaryKey();
            pkey.put("id", i);
            pkey.put("tableName", tableName);
            tableAPI.delete(pkey, null, null);
        }
        trace(DEL_ROWS + " rows deleted from table " + tableName);
    }

    protected void dropTable(final String tableName) {
        /* delete test tables */
        final String ddl = "DROP TABLE " + tableName;
        final StatementResult result = store.executeSync(ddl);
        assertTrue(result.isSuccessful());
    }

    void waitForEvents(final TestNoSQLSubscriber subscriber,
                       final int puts, final int dels)
        throws Exception {
        waitFor(new PollCondition(TEST_POLL_INTERVAL_MS,
                                  TEST_POLL_TIMEOUT_MS) {
            @Override
            protected boolean condition() {
                trace("Expect put " + puts + ", del " + dels +
                      ", get put: " + subscriber.getReceivedPuts().size() +
                      ", del: " + subscriber.getReceivedDels().size()
                );
                dumpCountsPerTable(subscriber);
                return subscriber.getReceivedPuts().size() == puts &&
                       subscriber.getReceivedDels().size() == dels;
            }
        });

        assertTrue("Expect no errors in test but get " +
                   Arrays.toString(subscriber.getRecvErrors().toArray()),
                   subscriber.getRecvErrors().isEmpty());
    }

    private void dumpCountsPerTable(final TestNoSQLSubscriber subscriber) {

        final Map<String, List<Row>> putByTble = subscriber.getPutsByTable();
        putByTble.keySet().forEach(
            table -> trace("#PUTs of " + table + ": " +
                           (putByTble.get(table) == null ? 0 :
                               putByTble.get(table).size())));


        final Map<String, List<Row>> delByTble = subscriber.getDelsByTable();
        putByTble.keySet().forEach(
            table -> trace("#DELs of " + table + ": " +
                           (delByTble.get(table) == null ? 0 :
                               delByTble.get(table).size())));
    }

    /*
     * Subscribe updates from a source table, converting each update to a put
     * or a delete of target table and apply it to the target table
     */
    class TestNoSQLSubscriber extends TestNoSQLSubscriberBase {

        final List<Row> receivedPuts;
        final List<PrimaryKey> receivedDels;
        final Map<String, List<Row>> putsByTable;
        final Map<String, List<Row>> delsByTable;

        volatile StreamPosition effectivePos;
        volatile Throwable exception;

        TestNoSQLSubscriber(NoSQLSubscriptionConfig config) {

            super(config);
            receivedPuts = new ArrayList<>();
            receivedDels = new ArrayList<>();
            putsByTable = new HashMap<>();
            delsByTable = new HashMap<>();
            effectivePos = null;
        }

        @Override
        public synchronized void onNext(StreamOperation t) {

            if (t instanceof StreamPutEvent) {
                final Row row = t.asPut().getRow();
                receivedPuts.add(row);
                final String tableName = row.get("tableName").asString().get();
                putsByTable.computeIfAbsent(tableName,
                                            r -> new ArrayList<>())
                           .add(row);
            } else if (t instanceof StreamDelEvent) {
                final PrimaryKey pkey = t.asDelete().getPrimaryKey();
                receivedDels.add(pkey);
                final String tableName = pkey.get("tableName").asString().get();
                delsByTable.computeIfAbsent(tableName,
                                            r -> new ArrayList<>())
                           .add(pkey);
            } else {
                throw new IllegalStateException("Receive unsupported stream " +
                                                "operation from shard " +
                                                t.getRepGroupId() +
                                                ", seq: " +
                                                ((StreamSequenceId)
                                                    t.getSequenceId())
                                                    .getSequence());
            }
        }

        @Override
        public void onCheckpointComplete(StreamPosition streamPosition,
                                         Throwable cause) {

            trace("Checkpoint completed: " +
                  ((cause == null) ? "succ" : "fail"));
        }

        @Override
        public void onChangeResult(StreamPosition pos, Throwable exp) {
            effectivePos = pos;
            exception = exp;
        }

        StreamPosition getEffectivePos() {
            return effectivePos;
        }

        Throwable getChangeResultException() {
            return exception;
        }

        void clearChangeResultException() {
            exception = null;
        }

        Map<String, List<Row>> getPutsByTable() {
            return putsByTable;
        }

        List<Row> getPutsByTable(String tableName) {
            return putsByTable.get(tableName);
        }

        Map<String, List<Row>> getDelsByTable() {
            return delsByTable;
        }

        List<Row> getDelsByTable(String tableName) {
            return delsByTable.get(tableName);
        }

        List<Row> getReceivedPuts() {
            return receivedPuts;
        }

        List<PrimaryKey> getReceivedDels() {
            return receivedDels;
        }

        /* forget all we have received */
        synchronized void clear() {
            receivedPuts.clear();
            receivedDels.clear();
            putsByTable.keySet().forEach(k -> putsByTable.get(k).clear());
            putsByTable.clear();
            exception = null;
            effectivePos = null;
        }
    }
}
