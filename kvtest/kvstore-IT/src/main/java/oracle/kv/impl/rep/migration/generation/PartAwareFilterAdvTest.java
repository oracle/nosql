/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep.migration.generation;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.logging.Level.FINE;
import static java.util.logging.Level.FINER;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import oracle.kv.Durability;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.KeySerializer;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableTestBase;
import oracle.kv.impl.pubsub.StreamDelEvent;
import oracle.kv.impl.pubsub.StreamPutEvent;
import oracle.kv.impl.pubsub.StreamSequenceId;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.PartitionMap;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.pubsub.NoSQLPublisher;
import oracle.kv.pubsub.NoSQLPublisherConfig;
import oracle.kv.pubsub.NoSQLSubscriber;
import oracle.kv.pubsub.NoSQLSubscription;
import oracle.kv.pubsub.NoSQLSubscriptionConfig;
import oracle.kv.pubsub.PublisherFailureException;
import oracle.kv.pubsub.StreamOperation;
import oracle.kv.pubsub.StreamPosition;
import oracle.kv.pubsub.SubscriptionFailureException;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.WriteOptions;
import oracle.kv.util.CreateStore;
import oracle.nosql.common.contextlogger.LogFormatter;

import org.junit.BeforeClass;
import org.junit.Test;
import org.reactivestreams.Subscription;

/**
 * Unit tests that verify the partition-aware feeder filter. The tests in
 * this testcase uses createStore to set the test env.
 */
public class PartAwareFilterAdvTest extends TableTestBase {

    private static final int TEST_POLL_INTERVAL_MS = 1000;
    private static final int TEST_POLL_TIMEOUT_MS = 60000;
    private static final WriteOptions WRITE_OPTIONS =
        new WriteOptions(Durability.COMMIT_NO_SYNC, 10000, MILLISECONDS);

    private static final boolean traceOnScreen = false;

    private static final Random random = new Random(1234567890);
    private static final RepGroupId rg1 = new RepGroupId(1);
    private static final RepGroupId rg2 = new RepGroupId(2);
    private static final PartitionId p1 = new PartitionId(1);
    private static final PartitionId p2 = new PartitionId(2);
    private static final PartitionId p3 = new PartitionId(3);
    private static final PartitionId p4 = new PartitionId(4);
    private static final PartitionId p5 = new PartitionId(5);
    private static final String ckptTableName = "CheckpointTable";

    private final String testPath = TestUtils.getTestDir().getAbsolutePath();
    private final String tableName = "User";
    private final Collector<CharSequence, ?, String> coll =
        Collectors.joining(",", "[", "]");

    private static final int NUM_INIT_ROWS = 1024 * 10;
    private static final int NUM_MORE_ROWS = 1024;
    private static final int TOTAL = NUM_INIT_ROWS + NUM_MORE_ROWS;

    @BeforeClass
    public static void staticSetUp() throws Exception {
        /*
         * Store will be created in SetUp(), so no need to start a store
         * here.
         */
    }

    @Override
    public void setUp() throws Exception {
        kvstoreName = "mystore";
        TestUtils.clearTestDirectory();
        TestStatus.setManyRNs(true);
        createStore = new CreateStore(kvstoreName,
                                      startPort,
                                      2, /* n SNs */
                                      1, /* rf */
                                      10, /* n partitions */
                                      1, /* capacity */
                                      CreateStore.MB_PER_SN,
                                      true,
                                      null);
        createStore.setPoolSize(1); /* reserve one SN for later. */
        createStore.start();
        store = KVStoreFactory.getStore(createKVConfig(createStore));
        TestBase.mrTableSetUp(store);
        addLoggerFileHandler();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        TestBase.mrTableTearDown();
    }


    /**
     * Test as follows
     *
     * 1. Starts with a single shard rg1 with 10 partitions
     * 2. Load some initial rows
     * 3. Expands the store to two shards, rg1 and rg2, each with 5 partitions.
     * 4. Load more rows that would go to rg2 only
     * 5. Create a stream client to stream all writes
     *
     * Expect
     * 1. All init rows in step 2 should be streamed from rg1 since they are
     * loaded before migration and belong to generation 0
     * 2. Rows in step 4 should be streamed from rg2 since they belong to
     * generation 1 and are loaded after migration.
     */
    @Test(timeout=1200000)
    public void testStoreExpansion() throws Exception {

        /*
         * Partitions on rg1:[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
         */
        final CommandServiceAPI cs = createStore.getAdmin();
        trace("dump topology before migration");
        trace(dumpTopo(cs.getTopology()));

        /* create table and load some data */
        final TableAPI tableAPI = store.getTableAPI();
        executeDdl("CREATE TABLE " + tableName + " " +
                   "(id INTEGER, firstName STRING, lastName STRING," +
                   "age INTEGER, PRIMARY KEY (id))");
        final Table tbl = tableAPI.getTable(tableName);
        final List<Row> initRows = insertRowsIntoTable(tbl);
        trace(NUM_INIT_ROWS + " rows loaded into table " + tbl.getFullName());

        /* Now deploy a new topology that includes the spare SN. */
        final String topo = "newTopo";
        cs.copyCurrentTopology(topo);
        cs.redistributeTopology(topo, "AllStorageNodes");

        trace("Expand store to two shards");
        final int planId = cs.createDeployTopologyPlan("deploy-new-topo", topo,
                                                       null);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);

        /*
         * After migration:
         * Partitions on rg1:[6, 7, 8, 9, 10]
         * Partitions on rg2:[1, 2, 3, 4, 5]
         */
        final Set<PartitionId> partsOnRG2 =
            Stream.of(p1, p2, p3, p4, p5).collect(Collectors.toSet());
        /* refresh handle after expansion */
        trace("dump topology after migration");
        store = KVStoreFactory.getStore(createKVConfig(createStore));
        tableImpl = (TableAPIImpl) store.getTableAPI();
        trace(dumpTopo(((KVStoreImpl)store).getTopology()));

        /* # rows that would go to rg2 */
        final List<Row> updates = moreWrites(tbl, partsOnRG2);
        trace("Done write " + NUM_MORE_ROWS + " rows of table " +
              tbl.getFullName() + " to partitions on rg2:");

        /*
         * Now we create stream from vlsn=1, we expect that all init rows are
         * streamed from rg1 because they are written before the migration
         * (previous generation),  and all updates to rg2 are only streamed
         * from rg2 (new generation).
         */
        final TestNoSQLSubscriber sub = createStream();
        sub.getSubscription().request(TOTAL);
        waitForTestDone(sub);

        /* all init rows should stream from rg1 */
        verifyRows(sub.getRowsByShard(rg1), initRows);
        /* all updated rows should stream from rg2 */
        verifyRows(sub.getRowsByShard(rg2), updates);
    }

    private List<Row> moreWrites(Table tbl, Set<PartitionId> pids) {
        final List<Row> rowsWritten = new ArrayList<>();
        while (rowsWritten.size() < NUM_MORE_ROWS) {
            final RowImpl row = updateToPart(tbl, pids);
            rowsWritten.add(row);
        }
        return rowsWritten;
    }

    private RowImpl updateToPart(Table t, Set<PartitionId> pids) {
        final TableAPI apiImpl = store.getTableAPI();
        /* repeat till getting a key from given partitions */
        while(true) {
            final RowImpl row = makeRandomRow((TableImpl)t, random.nextInt());
            if (isFromPartition(row, pids)) {
                apiImpl.put(row, null, WRITE_OPTIONS);
                return row;
            }
        }
    }

    /**
     * Returns true if the row belongs to given partition set
     */
    private boolean isFromPartition(Row row, Set<PartitionId> pids) {
        final KeySerializer serializer =
            ((KVStoreImpl)store).getKeySerializer();
        final Topology topology = ((KVStoreImpl) store).getTopology();
        final Key pk = ((RowImpl)row).getPrimaryKey(false);
        final byte[] keyBytes = serializer.toByteArray(pk);
        final PartitionId pid = topology.getPartitionId(keyBytes);
        return pids.contains(pid);
    }

    private List<Row> insertRowsIntoTable(Table tbl) {
        final TableAPI tableAPI = store.getTableAPI();
        final ArrayList<Row> rows = new ArrayList<>();
        for (int i = 0; i < NUM_INIT_ROWS; i++) {
            final RowImpl row = makeRandomRow((TableImpl)tbl, i);
            tableAPI.put(row, null, WRITE_OPTIONS);
            rows.add(row);
        }

        return rows;
    }

    private RowImpl makeRandomRow(TableImpl table, int which) {
        final RowImpl row = table.createRow();
        row.put("id", which);
        row.put("firstName",
                "FirstName-" + ThreadLocalRandom.current().nextInt(1, 1000));
        row.put("lastName",
                "lastName-" + ThreadLocalRandom.current().nextInt(1, 1000));
        row.put("age", ThreadLocalRandom.current().nextInt(20, 80));
        return row;
    }

    protected void trace(String message) {
        trace(INFO, message);
    }

    protected void trace(Level level, String message) {
        if (logger == null || traceOnScreen ) {
            System.err.println(message);
        } else {
            if (level.equals(INFO)) {
                logger.info(message);
            } else if (level.equals(FINE)) {
                logger.fine(message);
            } else if (level.equals(FINER)) {
                logger.finer(message);
            } else if (level.equals(FINEST)) {
                logger.finest(message);
            } else if (level.equals(WARNING)) {
                logger.warning(message);
            } else if (level.equals(SEVERE)) {
                logger.severe(message);
            }
        }
    }

    private String dumpTopo(Topology topology) {
        final StringBuilder builder = new StringBuilder();
        builder.append("====== start topology dump ======");

        topology.getRepGroupIds()
                .forEach(id -> builder.append("\n").append(id).append(":")
                                      .append(topology.get(id)
                                                      .getRepNodes()
                                                      .stream()
                                                      .map(RepNode::toString)
                                                      .collect(coll)));

        /* dump partitions on each shard */
        builder.append("\n");
        final PartitionMap pm = topology.getPartitionMap();
        final Map<RepGroupId, List<PartitionId>>
            result = pm.getAllIds().stream()
                       .collect(Collectors.groupingBy(pm::getRepGroupId));
        result.keySet()
              .forEach(gid -> builder.append("Partitions on ")
                                     .append(gid).append(":")
                                     .append(result.get(gid).stream()
                                                   .map(pid -> String.valueOf(
                                                       pid.getPartitionId()))
                                                   .collect(coll))
                                     .append("\n"));
        builder.append("\n===== done topology dump ======\n");
        return builder.toString();
    }

    /* Test subscriber which merely remembers anything it receives */
    private class TestNoSQLSubscriber implements NoSQLSubscriber {

        private final NoSQLSubscriptionConfig conf;
        private NoSQLSubscription subscription;
        private boolean isSubscribeSucc;

        private final Map<Integer, List<Row>> recvOps;
        Throwable causeOfFailure;

        TestNoSQLSubscriber(NoSQLSubscriptionConfig conf,
                            int numShards) {
            this.conf = conf;
            recvOps = new HashMap<>();
            for (int i = 1; i <= numShards; i++) {
                recvOps.put(i, new ArrayList<>());
            }
        }

        @Override
        public void onSubscribe(Subscription s) {
            subscription = (NoSQLSubscription) s;
            isSubscribeSucc = true;
            trace("Publisher called onSubscribe for " + conf.getSubscriberId());
        }

        @Override
        public void onNext(StreamOperation t) {
            if (t instanceof StreamPutEvent || t instanceof StreamDelEvent) {
                recvOps.get(t.getRepGroupId()).add(t.asPut().getRow());
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
        public void onError(Throwable t) {
            if (t instanceof SubscriptionFailureException) {
                causeOfFailure = t.getCause();
            } else if (t instanceof PublisherFailureException) {
                causeOfFailure = t.getCause();
            } else {
                causeOfFailure = t;
            }

            isSubscribeSucc = false;
            trace(Level.INFO,
                  "Subscriber " + conf.getSubscriberId() +
                  " receives an error: " + t.getMessage());
        }

        @Override
        public void onComplete() {

        }

        @Override
        public NoSQLSubscriptionConfig getSubscriptionConfig() {
            return conf;
        }

        @Override
        public void onWarn(Throwable t) {
            trace(Level.INFO,
                  "Subscriber " + conf.getSubscriberId() +
                  " receives a warning: " + t.getMessage());
        }

        @Override
        public void onCheckpointComplete(StreamPosition streamPosition,
                                         Throwable failureCause) {

        }

        long getNumOps() {
            return recvOps.values().stream().mapToInt(List::size).sum();
        }

        Map<Integer, Long> getNumOpsByShard() {
            final Map<Integer, Long> ret = new HashMap<>();
            recvOps.forEach((key, value) -> ret.put(key, (long) value.size()));
            return ret;
        }

        List<Row> getRowsByShard(RepGroupId id) {
            return recvOps.get(id.getGroupId());
        }

        public boolean isSubscriptionSucc() {
            return isSubscribeSucc;
        }

        public String getCauseOfFailure() {
            if (causeOfFailure == null) {
                return "no failure";
            }
            return causeOfFailure.getMessage();
        }

        public NoSQLSubscription getSubscription() {
            return subscription;
        }
    }

    /* wait for test done, by checking subscription callback */
    private void waitForTestDone(TestNoSQLSubscriber sub)
        throws TimeoutException {

        boolean success =
            new PollCondition(TEST_POLL_INTERVAL_MS, TEST_POLL_TIMEOUT_MS) {
                @Override
                protected boolean condition() {
                    final long count = sub.getNumOps();
                    trace("# received=" + count + ", # expected=" + TOTAL +
                          ", # received by shard=" + sub.getNumOpsByShard());
                    return count == TOTAL;
                }
            }.await();

        /* if timeout */
        if (!success) {
            throw new TimeoutException("timeout in polling test ");
        }
    }

    private TestNoSQLSubscriber createStream() {

        final int numShards = 2;
        /* create a publisher */
        final NoSQLPublisherConfig pubConf =
            new NoSQLPublisherConfig.Builder(createStore.createKVConfig(),
                                             testPath).build();
        final NoSQLPublisher publisher = NoSQLPublisher.get(pubConf, logger);
        /* create test subscriber */
        final NoSQLSubscriptionConfig conf =
            new NoSQLSubscriptionConfig.Builder(ckptTableName)
                .setSubscribedTables(tableName)
                .build();

        final TestNoSQLSubscriber subscriber =
            new TestNoSQLSubscriber(conf, numShards);
        publisher.subscribe(subscriber);
        assertTrue("Subscription failed, reason " +
                   subscriber.getCauseOfFailure(),
                   subscriber.isSubscriptionSucc());
        return subscriber;
    }

    private void verifyRows(List<Row> act, List<Row> exp) {
        assertEquals(exp.size(), act.size());
        assertTrue(act.containsAll(exp));
        assertTrue(exp.containsAll(act));
    }

    private void addLoggerFileHandler() throws IOException {
        final String fileName = "testlog";
        final String path = TestUtils.getTestDir().getAbsolutePath();
        final File loggerFile = new File(new File(path), fileName);
        final FileHandler handler =
            new FileHandler(loggerFile.getAbsolutePath(), false);
        handler.setFormatter(new LogFormatter(null));
        tearDowns.add(() -> logger.removeHandler(handler));
        logger.addHandler(handler);
        logger.info("Add test log file handler: path=" + path +
                    ", log file name=" + fileName +
                    ", file exits?=" + loggerFile.exists());
    }
}
