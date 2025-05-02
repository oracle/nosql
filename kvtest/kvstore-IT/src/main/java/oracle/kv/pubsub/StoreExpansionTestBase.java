/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.pubsub;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.logging.Level.INFO;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import oracle.kv.Durability;
import oracle.kv.FaultException;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.Version;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.KeySerializer;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableTestBase;
import oracle.kv.impl.pubsub.NoSQLSubscriptionImpl;
import oracle.kv.impl.pubsub.StreamPutEvent;
import oracle.kv.impl.pubsub.StreamSequenceId;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.PartitionMap;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.Pair;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.WriteOptions;
import oracle.kv.util.CreateStore;
import oracle.nosql.common.contextlogger.LogFormatter;

import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.je.utilint.VLSN;

import org.junit.BeforeClass;
import org.reactivestreams.Subscription;

public class StoreExpansionTestBase extends TableTestBase {

    static boolean traceOnScreen = false;

    static final String TEST_PATH = TestUtils.getTestDir().getAbsolutePath();
    static final int TEST_POLL_INTERVAL_MS = 1000;
    static final int TEST_POLL_TIMEOUT_MS = 60000;
    static final RepGroupId SRC_SHARD = new RepGroupId(1);
    static final RepGroupId TGT_SHARD = new RepGroupId(2);
    private static final WriteOptions WRITE_OPTIONS =
        new WriteOptions(Durability.COMMIT_SYNC, 10000, MILLISECONDS);
    static final String CKPT_TABLE_NAME = "CheckpointTable";
    /* source table */
    static final String SRC_TBL_NAME = "User";
    private static final String DDL_SRC_TBL =
        "CREATE TABLE " + SRC_TBL_NAME + " " +
        "(id Long, name STRING, PRIMARY KEY (id))";

    /* Collector to build trace for list */
    static final Collector<CharSequence, ?, String> LIST_COLL =
        Collectors.joining(",", "[", "]");

    /* total # partitions in store */
    static final int NUM_PARTS = 2;
    static final Set<PartitionId> PART_IDS = new HashSet<>();
    static {
        IntStream.range(1, NUM_PARTS + 1)
                 .forEach(i -> PART_IDS.add(new PartitionId(i)));
    }
    static final int MIGRATED_PID = 1;
    static final int NON_MIGRATED_PID = 2;

    /* # of shards after expansion */
    static final int MAX_NUM_SHARDS = 2;
    static final int NEW_SHARD_ID = 2;

    /* sleep time in ms */
    static final int TBL_UPDE_TIME_MS = 1000 * 10;

    private volatile NoSQLPublisher publisher;

    @BeforeClass
    public static void staticSetUp() {
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
                                      NUM_PARTS, /* n partitions */
                                      1, /* capacity */
                                      CreateStore.MB_PER_SN,
                                      true,
                                      null);
        createStore.setPoolSize(1); /* reserve one SN for later. */
        createStore.start();
        store = KVStoreFactory.getStore(createKVConfig(createStore));
        logger.setLevel(Level.INFO);
        addLoggerFileHandler();
        mrTableSetUp(store);
        trace("Store topology:\n" +
              dumpTopo(createStore.getAdmin().getTopology()));
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        mrTableTearDown();
        if (publisher != null && !publisher.isClosed()) {
            publisher.close(true);
        }
    }

    void trace(String message) {
        trace(INFO, lm(message));
    }

    private String lm(String msg) {
        return "[TEST] " + msg;
    }

    void trace(Level level, String message) {

        if (traceOnScreen) {
            if (logger.isLoggable(level)) {
                System.out.println(message);
            }
        }

        logger.log(level, message);
    }

    void waitFor(final PollCondition pollCondition)
        throws TimeoutException {

        boolean success = pollCondition.await();
        /* if timeout */
        if (!success) {
            throw new TimeoutException("timeout in polling test ");
        }
    }

    private void addLoggerFileHandler() throws IOException {
        final String fileName = "testlog";
        final String testPath = TestUtils.getTestDir().getAbsolutePath();
        final File loggerFile = new File(new File(testPath), fileName);
        final FileHandler handler =
            new FileHandler(loggerFile.getAbsolutePath(), false);
        handler.setFormatter(new LogFormatter(null));
        tearDowns.add(() -> logger.removeHandler(handler));
        logger.addHandler(handler);
        logger.info("Add test log file handler: path=" + testPath +
                    ", log file name=" + fileName +
                    ", level=" + logger.getLevel() +
                    ", file exits?=" + loggerFile.exists());
    }

    String logPair(Pair<Long, Long> vlsn) {
        return "[" + vlsn.first() + ", " + vlsn.second() + "]";
    }

    void createTestTable() {
        final TableAPI tableAPI = store.getTableAPI();
        executeDdl(DDL_SRC_TBL);
        final Table tbl = tableAPI.getTable(SRC_TBL_NAME);
        assertNotNull(tbl);
        trace("Created table=" + tbl.getFullName());
    }

    static RowImpl makeRandomRow(TableImpl table, long which) {
        final RowImpl row = table.createRow();
        row.put("id", which);
        row.put("name", "name" + UUID.randomUUID());
        return row;
    }

    static String dumpTopo(Topology topology) {
        final StringBuilder builder = new StringBuilder();
        topology.getRepGroupIds()
                .forEach(id -> builder.append(id).append(":")
                                      .append(topology.get(id)
                                                      .getRepNodes()
                                                      .stream()
                                                      .map(RepNode::toString)
                                                      .collect(LIST_COLL))
                                      .append("\n"));

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
                                                   .collect(LIST_COLL))
                                     .append("\n"));
        return builder.toString();
    }

    boolean updateTable(TableImpl t,
                        long id,
                        Set<PartitionId> pids,
                        ConcurrentMap<Integer, Map<Integer, AtomicLong>> counts,
                        ConcurrentMap<Integer,
                            Map<Integer, Pair<AtomicLong, AtomicLong>>> vlsns) {
        final TableAPI apiImpl = store.getTableAPI();

        /* repeat till getting a key from given partitions */
        final RowImpl r = makeRandomRow(t, id);
        final PartitionId pid = fromPartition(r);
        if (pids.contains(pid)) {
            final Version ver = putWithRetry(r, pid, apiImpl);
            updateStats(ver.getRepNodeId().getGroupId(),
                        pid.getPartitionId(),
                        ver.getVLSN(),
                        counts,
                        vlsns);
            return true;
        }
        return false;
    }

    Version putWithRetry(Row row, PartitionId pid, TableAPI tapi) {
        while (true) {
            try {
                final Version ver = tapi.put(row, null, WRITE_OPTIONS);
                trace(Level.FINE,
                      "Put key=" + row.get("id").asLong().get() +
                      " to partition=" + pid);
                return ver;
            } catch (FaultException fe) {
                trace("Fail to put key=" + row.get("id").asLong().get() +
                      " to partition=" + pid +
                      ", error=" + fe +
                      ", will retry");
            }
        }
    }

    void updateStats(int gid, int pid, long vlsn,
                     Map<Integer, Map<Integer, AtomicLong>> partCount,
                     Map<Integer, Map<Integer, Pair<AtomicLong,
                         AtomicLong>>> vlsns) {
        partCount.get(gid).get(pid).incrementAndGet();
        final Pair<AtomicLong, AtomicLong> range = vlsns.get(gid).get(pid);
        if (range.first().get() == VLSN.NULL_VLSN) {
            /* first ops */
            range.first().set(vlsn);
            range.second().set(vlsn);
            return;
        }
        range.second().set(vlsn);
    }

    static PartitionId fromPartition(Row row) {
        final KeySerializer serializer =
            ((KVStoreImpl) store).getKeySerializer();
        final Topology topology = ((KVStoreImpl) store).getTopology();
        final Key pk = ((RowImpl) row).getPrimaryKey(false);
        final byte[] keyBytes = serializer.toByteArray(pk);
        return topology.getPartitionId(keyBytes);
    }

    void logStats(
        long total,
        Map<Integer, Map<Integer, AtomicLong>> partCount,
        Map<Integer, Map<Integer, Pair<AtomicLong, AtomicLong>>> vlsn) {

        trace("Total #ops=" + total);
        for (int gid = 1; gid <= MAX_NUM_SHARDS; gid ++) {
            trace("On RepGroup Id=" + gid + ":");
            for (int pid = 1; pid <= NUM_PARTS; pid ++) {
                trace("\tPartition id=" + pid + ":");
                trace("\t\t#ops=" + partCount.get(gid).get(pid).get());
                final Pair<AtomicLong, AtomicLong> range = vlsn.get(gid)
                                                               .get(pid);
                trace("\t\tvlsn range=" +
                      logPair(new Pair<>(range.first().get(),
                                         range.second().get())));
            }
        }
    }

    void initStats(
        Map<Integer, Map<Integer, AtomicLong>> partCount,
        Map<Integer, Map<Integer, Pair<AtomicLong, AtomicLong>>> vlsns) {
        IntStream.range(1, MAX_NUM_SHARDS + 1).forEach(i -> {
            final Map<Integer, AtomicLong> cmap = new HashMap<>();
            final Map<Integer, Pair<AtomicLong, AtomicLong>> vmap =
                new HashMap<>();
            IntStream.range(1, NUM_PARTS + 1).forEach(j -> {
                cmap.put(j, new AtomicLong(0));
                vmap.put(j, new Pair<>(new AtomicLong(VLSN.NULL_VLSN),
                                       new AtomicLong(VLSN.NULL_VLSN)));
            });
            partCount.put(i, cmap);
            vlsns.put(i, vmap);
        });
    }

    void expandStore() throws Exception {

        final CommandServiceAPI cs = createStore.getAdmin();
        final String topo = "newTopo";
        cs.copyCurrentTopology(topo);
        cs.redistributeTopology(topo, "AllStorageNodes");

        trace(Level.FINE, "Expand store to two shards");
        final int planId =
            cs.createDeployTopologyPlan("deploy-new-topo", topo, null);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);

        /* refresh handle after expansion */
        store = KVStoreFactory.getStore(createKVConfig(createStore));
        tableImpl = (TableAPIImpl) store.getTableAPI();
    }

    TestNoSQLSubscriber createStream() {
        return createStream(NoSQLSubscriptionConfig.getSingletonSubscriberId(),
                            false, TestNoSQLSubscriber::new);
    }

    <T extends TestNoSQLSubscriber> T
    createStream(NoSQLSubscriberId sid,
                 Function<NoSQLSubscriptionConfig, T> supplier) {
        return createStream(sid,
                            false/* internal checkpoint for elastic ops */,
                            supplier);
    }

    <T extends TestNoSQLSubscriber> T
    createStream(NoSQLSubscriberId sid,
                 boolean useExternalCkpt,
                 Function<NoSQLSubscriptionConfig, T> supplier) {

        /* create a publisher */
        final NoSQLPublisherConfig pubConf =
            new NoSQLPublisherConfig.Builder(createStore.createKVConfig(),
                                             TEST_PATH).build();
        publisher = NoSQLPublisher.get(pubConf, logger);
        /* create test subscriber */
        final NoSQLSubscriptionConfig.Builder confBuilder =
            new NoSQLSubscriptionConfig.Builder(
                id -> PubSubTestBase.buildCkptTableName(id, CKPT_TABLE_NAME))
                .setSubscribedTables(SRC_TBL_NAME)
                .setSubscriberId(sid);
        if (useExternalCkpt) {
            confBuilder.setUseExternalCheckpointForElasticity();
        }
        final NoSQLSubscriptionConfig conf = confBuilder.build();
        final T subscriber = supplier.apply(conf);
        publisher.subscribe(subscriber);
        try {
            waitFor(new PollCondition(TEST_POLL_INTERVAL_MS,
                                      TEST_POLL_TIMEOUT_MS) {
                @Override
                protected boolean condition() {
                    return subscriber.isSubscriptionSucc();
                }
            });
        } catch (TimeoutException e) {
            fail("Fail to create stream");
        }
        trace("Stream created with sid=" + sid +
              ", cpt table=" + conf.getCkptTableName() +
              ", ckpt table map" + conf.getCkptTableNameMap() +
              ", external ckpt for elasticity=" +
              conf.getUseExtCkptForElasticity());
        return subscriber;
    }

    /* Test subscriber which merely remembers anything it receives */
    class TestNoSQLSubscriber implements NoSQLSubscriber {

        final NoSQLSubscriptionConfig conf;

        private volatile Throwable error;
        private volatile NoSQLSubscription subscription;
        private volatile boolean isSubscribeSucc;
        private final AtomicLong total;

        /* nested map from shard id -> map (partition id -> count) */
        private final ConcurrentMap<Integer, Map<Integer, AtomicLong>> counts;

        /* start and end vlsn of each partition on each shard */
        private final ConcurrentMap<Integer,
            Map<Integer, Pair<AtomicLong, AtomicLong>>> vlsns;

        TestNoSQLSubscriber(NoSQLSubscriptionConfig conf) {
            this.conf = conf;
            total = new AtomicLong(0);
            counts = new ConcurrentHashMap<>();
            vlsns = new ConcurrentHashMap<>();
            initStats(counts, vlsns);
        }

        @Override
        public void onSubscribe(Subscription s) {
            subscription = (NoSQLSubscription) s;
            isSubscribeSucc = true;
            trace(Level.FINE,
                  "Publisher called onSubscribe for " +
                  conf.getSubscriberId());
        }

        @Override
        public void onNext(StreamOperation t) {
            if (t instanceof StreamPutEvent) {
                total.incrementAndGet();
                final Row r = t.asPut().getRow();
                final int gid = t.getRepGroupId();
                final StreamSequenceId seq =
                    (StreamSequenceId) t.getSequenceId();
                final long vlsn = seq.getSequence();
                final int pid = fromPartition(r).getPartitionId();
                updateStats(gid, pid, vlsn, counts, vlsns);
                trace(Level.FINE,
                      "From gid=" + t.getRepGroupId() + ", vlsn= " + vlsn +
                      ", pid=" + pid + ": " + r.toJsonString(false));
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
            isSubscribeSucc = false;
            error = t;
            final String msg = "Subscriber=" + conf.getSubscriberId() +
                               " receives an error=" + t +
                               "\n" + LoggerUtils.getStackTrace(t);
            trace(msg);
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
            trace(Level.WARNING, "Subscriber " + conf.getSubscriberId() +
                                 " receives a warning: " + t.getMessage());
        }

        @Override
        public void onCheckpointComplete(StreamPosition streamPosition,
                                         Throwable failureCause) {

        }

        public Throwable getError() {
            return error;
        }

        public NoSQLSubscriberId getSubscriberId() {
            return conf.getSubscriberId();
        }

        public Set<Integer> getShards() {
            final NoSQLSubscriptionImpl stream =
                (NoSQLSubscriptionImpl) getSubscription();
            return stream.getCoveredShards().stream()
                         .map(RepGroupId::getGroupId)
                         .collect(Collectors.toSet());
        }

        public long getTotal() {
            return total.get();
        }

        void dumpStats() {
            trace("======= Dump stats for subscriber =======");
            logStats(total.get(), counts, vlsns);
        }

        public boolean isSubscriptionSucc() {
            return isSubscribeSucc;
        }

        public NoSQLSubscription getSubscription() {
            return subscription;
        }

        long getCount(int shardId, int partId) {
            return counts.get(shardId).get(partId).get();
        }

        Pair<Long, Long> getVLSNRange(int shardId, int partId) {
            final Pair<AtomicLong, AtomicLong> range =
                vlsns.get(shardId).get(partId);
            return new Pair<>(range.first().get(), range.second().get());
        }
    }

    class UpdateThread extends StoppableThread {

        final TableImpl tbl;

        /* allowed partition ids */
        final Set<PartitionId> pids;

        /* nested map from shard id -> map (partition id -> count) */
        final ConcurrentMap<Integer, Map<Integer, AtomicLong>> counts;

        /* start and end vlsn of each partition on each shard */
        final ConcurrentMap<Integer,
            Map<Integer, Pair<AtomicLong, AtomicLong>>> vlsns;


        /* # of updates made */
        final AtomicLong total;

        volatile boolean shutdownRequested = false;

        UpdateThread(Set<PartitionId> pids) {
            super("Update thread");
            this.pids = pids;
            tbl = (TableImpl) store.getTableAPI().getTable(SRC_TBL_NAME);
            total = new AtomicLong();
            counts = new ConcurrentHashMap<>();
            vlsns = new ConcurrentHashMap<>();
            initStats(counts, vlsns);
        }

        @Override
        public void run() {
            trace("Starting update thread for table=" + SRC_TBL_NAME +
                  " update to partition=" + pids);
            while (!shutdownRequested) {
                writeToTable();
                total.incrementAndGet();
            }
        }

        @Override
        protected Logger getLogger() {
            return logger;
        }

        @Override
        protected int initiateSoftShutdown() {
            shutdownRequested = true;
            return 0; /* let current write finish */
        }

        boolean writeToTable() {
            return updateTable(tbl, total.get(), PART_IDS, counts, vlsns);
        }

        long getTotal() {
            return total.get();
        }

        long getCount(int shardId, int partId) {
            return counts.get(shardId).get(partId).get();
        }

        long getCount(int shardId) {
            final Map<Integer, AtomicLong> countShard = counts.get(shardId);
            if (countShard == null) {
                return 0;
            }
            return countShard.values().stream()
                             .mapToLong(AtomicLong::get)
                             .sum();
        }

        Pair<Long, Long> getVLSNRange(int shardId, int partId) {
            final Pair<AtomicLong, AtomicLong> range =
                vlsns.get(shardId).get(partId);
            return new Pair<>(range.first().get(), range.second().get());
        }

        void dumpStats() {
            trace("======= Dump stats for update thread =======");
            logStats(total.get(), counts, vlsns);
        }
    }
}
