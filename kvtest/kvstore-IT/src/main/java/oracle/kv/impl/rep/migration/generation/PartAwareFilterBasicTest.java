/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep.migration.generation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import oracle.kv.KVStoreConfig;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.pubsub.NoSQLStreamFeederFilter;
import oracle.kv.impl.pubsub.NoSQLSubscriptionImpl;
import oracle.kv.impl.pubsub.PublishingUnit;
import oracle.kv.impl.pubsub.ReplicationStreamConsumer;
import oracle.kv.impl.pubsub.ShardMasterInfo;
import oracle.kv.impl.pubsub.StreamDelEvent;
import oracle.kv.impl.pubsub.StreamPutEvent;
import oracle.kv.impl.pubsub.StreamSequenceId;
import oracle.kv.impl.rep.IncorrectRoutingException;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.admin.RepNodeAdmin;
import oracle.kv.impl.rep.migration.PartitionMigrationStatus;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.util.HostPort;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.pubsub.NoSQLPublisher;
import oracle.kv.pubsub.NoSQLPublisherConfig;
import oracle.kv.pubsub.NoSQLStreamMode;
import oracle.kv.pubsub.NoSQLSubscription;
import oracle.kv.pubsub.NoSQLSubscriptionConfig;
import oracle.kv.pubsub.PubSubTestBase;
import oracle.kv.pubsub.StreamOperation;
import oracle.kv.pubsub.StreamPosition;
import oracle.kv.table.Row;

import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.impl.node.Feeder;
import com.sleepycat.je.utilint.StoppableThread;

import org.junit.Test;

/**
 * Unit tests that verify the partition-aware feeder filter. The tests in
 * this testcase all use the KVRepTestConfig to create test env, which is
 * easier to access the internal state of RN and verify the feeder filter
 * state.
 *
 * Please note because there is no admin in the test store, the partition
 * db of migrated is not dropped and the partition remains open in source
 * shard after migration is over.
 */
public class PartAwareFilterBasicTest extends PubSubTestBase {

    private static final int NUM_KEYS_USER_TABLE = 1024 * 10;
    private static final PartitionId P1 = new PartitionId(1);
    private static final PartitionId P2 = new PartitionId(2);
    private static final PartitionId P3 = new PartitionId(3);
    private static final PartitionId P4 = new PartitionId(4);
    private static final PartitionId P5 = new PartitionId(5);
    private static final PartitionId P6 = new PartitionId(6);
    private static final PartitionId P7 = new PartitionId(7);
    private static final PartitionId P8 = new PartitionId(8);
    private static final PartitionId P9 = new PartitionId(9);
    private static final PartitionId P10 = new PartitionId(10);

    private static final RepGroupId RG_1 = new RepGroupId(1);
    private static final RepGroupId RG_2 = new RepGroupId(2);
    private static final RepNodeId SOURCE_ID = new RepNodeId(1, 1);
    private static final RepNodeId TARGET_ID = new RepNodeId(2, 1);
    private static final RepNodeId RG_1_MASTER = new RepNodeId(1, 1);
    private static final RepNodeId RG_2_MASTER = new RepNodeId(2, 1);

    private static final int POLL_INTV_MS = 1000;
    private static final int POLL_TIMEOUT_MS = 15000;

    private PublishingUnit publishingUnit1 = null;
    private PublishingUnit publishingUnit2 = null;
    private NoSQLPublisher publisher = null;

    @Override
    public void setUp() throws Exception {

        super.setUp();
        /* each test should create its own config */
        config = null;
        traceOnScreen = false;
        ensureTestDir(testPath);
    }

    @Override
    public void tearDown() throws Exception {
        config.stopRepNodeServices();
        config = null;

        if (publisher != null) {
            publisher.close(true);
        }
        super.tearDown();
    }

    /**
     * Tests without migration, the feeder filter allows updates to
     * all partitions on each shard to pass
     */
    @Test
    public void testFeederFilter() throws Exception {

        /* 2-shard store */
        config = createTwoShardStoreConfig();
        prepareTestEnv(false);
        addTableToMetadata(userTable);
        trace("Test environment created successfully," +
              "\ntopology:\n" + PubSubTestBase.topo2String(config));

        /* load data */
        final Map<String, Row> allExpectedRows =
            insertRowsIntoTable(userTable, NUM_KEYS_USER_TABLE);
        trace("load data done, # of rows: " + allExpectedRows.size());

        final Set<TableImpl> tables =
            Stream.of(userTable).collect(Collectors.toSet());

        /* test a small # of keys, enough for either shard */
        /* since no migration owned part shall be null */
        testRepGroup(RG_1_MASTER, tables);
        testRepGroup(RG_2_MASTER, tables);
    }

    /**
     * Tests that in a two-shard store, when p1 migrates from rg1 to rg2:
     *
     * Before migration is over, the filter on rg2 should block all writes
     * from migration of p1, while the filter on rg1 should continue to allow
     * writes to p1 to pass;
     *
     * After the migration is over, the filter at rg2 should adjust itself to
     * allow all new writes to p1 on rg2 to pass;
     *
     * After the migration is over, the filter at rg1 should adjust itself to
     * remove p1 from its allowed partitions.
     */
    @Test
    public void testMigration() throws Exception {

        /* 2-shard store */
        config = createTwoShardStoreConfig();
        final Set<PartitionId> partsOnRG1 = Stream.of(P1, P3, P5, P7, P9)
                                                  .collect(Collectors.toSet());
        final Set<PartitionId> partsOnRG2 = Stream.of(P2, P4, P6, P8, P10)
                                                  .collect(Collectors.toSet());
        prepareTestEnv(false);
        addTableToMetadata(userTable);
        trace("Test environment created successfully," +
              "\ntopology:\n" + PubSubTestBase.topo2String(config));

        /* load data */
        final Map<String, Row> allExpectedRows =
            insertRowsIntoTable(userTable, NUM_KEYS_USER_TABLE);
        trace("load data done, # rows=" + allExpectedRows.size());

        /* create stream */
        final TestNoSQLSubscriber s = createStream(userTable.getFullName(), 1);
        /* stream all pre-loaded data from two shards */
        s.getSubscription().request(NUM_KEYS_USER_TABLE);
        waitForTestDone(s, NUM_KEYS_USER_TABLE);
        trace("All pre-loaded ops streamed=" + NUM_KEYS_USER_TABLE);

        final int numOpsRG2 = s.getNumOpsFromRG2();
        trace("Remember # ops=" + numOpsRG2 +
              " streamed on rg2 before migration");
        /* trigger migration of p1 from source to target shard */
        migratePartition();
        trace("Migration of P1 done");
        partsOnRG2.add(P1);

        /* verify partition generation table on rg1 */
        final PartitionGenerationTable pgt1 =
        config.getRN(RG_1_MASTER).getPartitionManager().getPartGenTable();
        /* partitions on rg1 still open */
        partsOnRG1.forEach(pid -> assertTrue("Expect open gen for " + pid,
                                             pgt1.getOpenGen(pid).isOpen()));
        trace("PGT on rg1 verified");

        /* verify partition generation table on rg2 */
        final PartitionGenerationTable pgt2 =
            config.getRN(RG_2_MASTER).getPartitionManager().getPartGenTable();
        assertTrue("RG2 should have p1", partsOnRG2.contains(P1));
        /* every pid on rg2 should be open */
        partsOnRG2.forEach(pid -> assertTrue("Expect open gen for " + pid,
                                             pgt2.getOpenGen(pid).isOpen()));
        /* p1 on rg2 should have generation 1 */
        final PartitionGeneration pg = pgt2.getLastGen(P1);
        assertEquals("Expect incremented gen for p1 ",
                     1, pg.getGenNum().getNumber());
        assertEquals("Expect p1 is from rg1 ",
                     1, pg.getPrevGenRepGroup().getGroupId());
        assertTrue("Expect last vlsn on prev gen is set",
                   pg.getPrevGenEndVLSN() < Long.MAX_VALUE);
        trace("PGT on rg2 verified");

        /* verify no ops of p1 is received at rg2 before migration done */
        trace("Remember # ops streamed on rg2 after migration: " +
              s.getNumOpsFromRG2());
        assertEquals("Expect no new ops from target shard ",
                     numOpsRG2, s.getNumOpsFromRG2());

        /* migration is over, verify feeder filter on rg1 and rg2 */
        final String f1 = publishingUnit1.getConsumer(RG_1).getConsumerId();
        final String f2 = publishingUnit1.getConsumer(RG_2).getConsumerId();
        verifyFeederFilter(RG_1_MASTER, f1, partsOnRG1);
        trace("feeder filter on " + RG_1 + " verified");
        verifyFeederFilter(RG_2_MASTER, f2, partsOnRG2);
        trace("feeder filter on " + RG_2 + " verified");

        /* clean up */
        s.getSubscription().cancel();
    }

    @Test
    public void testReinitialization() throws Exception {
        /* 2-shard store */
        config = createTwoShardStoreConfig();
        final Set<PartitionId> partsOnRG1 = Stream.of(P1, P3, P5, P7, P9)
                                                  .collect(Collectors.toSet());
        final Set<PartitionId> partsOnRG2 = Stream.of(P2, P4, P6, P8, P10)
                                                  .collect(Collectors.toSet());
        prepareTestEnv(false);
        addTableToMetadata(userTable);
        trace("Test environment created successfully," +
              "\ntopology:\n" + PubSubTestBase.topo2String(config));

        /* load data */
        final int numKeysUserTable = 1024 * 10;
        final Map<String, Row> allExpectedRows =
            insertRowsIntoTable(userTable, numKeysUserTable);
        trace("load data done, # of rows: " + allExpectedRows.size());

        final PartitionGenerationTable rg1Pgt =
            config.getRN(RG_1_MASTER).getPartitionManager().getPartGenTable();
        final PartitionGenerationTable rg2Pgt =
            config.getRN(RG_2_MASTER).getPartitionManager().getPartGenTable();

        /* fake incomplete initialization of generations */
        Set<PartitionId> rg1InitSet = Stream.of(P1, P3)
                                            .collect(Collectors.toSet());
        for (PartitionId id : rg1InitSet) {
            PartitionGenerationTestBase.dbPutInTxn(
                rg1Pgt.getDbManager(), id, new PartitionGeneration(id),
                this::trace);
        }
        Set<PartitionId> rg2InitSet = Stream.of(P2, P4)
                                            .collect(Collectors.toSet());
        for (PartitionId id : rg2InitSet) {
            PartitionGenerationTestBase.dbPutInTxn(
                rg2Pgt.getDbManager(), id, new PartitionGeneration(id),
                this::trace);
        }

        /* create with publisher so stream can find the new master */
        final TestNoSQLSubscriber s = createStream(userTable.getFullName(), 1);

        /* verify feeder filter on original masters of rg1 and rg2 */
        String feeder1 = publishingUnit1.getConsumer(RG_1).getConsumerId();
        String feeder2 = publishingUnit1.getConsumer(RG_2).getConsumerId();
        verifyFeederFilter(RG_1_MASTER, feeder1, rg1InitSet);
        verifyFeederFilter(RG_2_MASTER, feeder2, rg2InitSet);

        /* stop original masters to trigger master transfer */
        config.stopRepNodeServicesSubset(true, RG_1_MASTER, RG_2_MASTER);

        /*
         * wait for generation table to be ready on new master,
         * excluding the original masters while finding the new ones
         */
        waitGenerationTableReady(RG_1, RG_1_MASTER /* excludeRN */);
        waitGenerationTableReady(RG_2, RG_2_MASTER /* excludeRN */);

        /*
         * find the new master of rg1 and rg2, excluding the original
         * masters while finding the new ones
         */
        RepNode rg1NewMaster = getMaster(RG_1, RG_1_MASTER  /* excludeRN */);
        if (rg1NewMaster == null) {
            fail("New master of rg1 is not available");
            return;
        }
        RepNode rg2NewMaster = getMaster(RG_2, RG_2_MASTER /* excludeRN */);
        if (rg2NewMaster == null) {
            fail("New master of rg2 is not available");
            return;
        }
        waitForFeeder(rg1NewMaster, feeder1);
        waitForFeeder(rg2NewMaster, feeder2);

        verifyFeederFilter(rg1NewMaster.getRepNodeId(), feeder1, partsOnRG1);
        trace("feeder filter on " + RG_1 + " verified");
        verifyFeederFilter(rg2NewMaster.getRepNodeId(), feeder2, partsOnRG2);
        trace("feeder filter on " + RG_2 + " verified");

        s.getSubscription().request(numKeysUserTable);
        waitForTestDone(s, numKeysUserTable);
        trace("All pre-loaded ops streamed: " + numKeysUserTable);
    }

    /**
     * Test that the owned partition of two streams from the same shard are
     * distinct instances of objects
     */
    @Test
    public void testFeederForTwoSubscriptions() throws Exception {
        /* 2-shard store */
        config = createTwoShardStoreConfig();
        prepareTestEnv(false);
        addTableToMetadata(userTable);
        trace("Test environment created successfully," +
              "\ntopology:\n" + PubSubTestBase.topo2String(config));
        final Set<PartitionId> partsOnRG1 = Stream.of(P1, P3, P5, P7, P9)
            .collect(Collectors.toSet());

        /* load data */
        final int numKeysUserTable = 1024 * 10;
        final Map<String, Row> allExpectedRows =
            insertRowsIntoTable(userTable, numKeysUserTable);
        trace("load data done, # of rows: " + allExpectedRows.size());

        /* create stream */
        final TestNoSQLSubscriber s1 = createStream(userTable.getFullName(), 1);
        final TestNoSQLSubscriber s2 = createStream(userTable.getFullName(), 2);
        /* stream all pre-loaded data from two shards */
        s1.getSubscription().request(numKeysUserTable);
        s2.getSubscription().request(numKeysUserTable);
        waitForTestDone(s1, numKeysUserTable);
        waitForTestDone(s2, numKeysUserTable);
        trace("All pre-loaded ops streamed: " + numKeysUserTable);

        /* trigger migration of p1 from source to target shard */
        migratePartition();
        trace("Migration of P1 done");

        final String feeder1 = publishingUnit1.getConsumer(RG_1).getConsumerId();
        final String feeder2 = publishingUnit2.getConsumer(RG_1).getConsumerId();

        Set<PartitionId> part1 = getOwnedParts(RG_1_MASTER, feeder1);
        Set<PartitionId> part2 = getOwnedParts(RG_1_MASTER, feeder2);

        /*
         * Verify feeder filter on rg1 for both subscriptions
         * and also verify the two feeders are independent.
         */
        verifyFeederFilter(RG_1_MASTER, feeder1, partsOnRG1);
        verifyFeederFilter(RG_1_MASTER, feeder2, partsOnRG1);

        /* verify two sets are separate, although the content is equal */
        trace("part1=" + part1);
        trace("part2=" + part2);

        /* same content */
        assertEquals(part1, part2);

        /* no admin, p1 is still in both */
        assertTrue(part1.contains(P1));
        assertTrue(part2.contains(P1));

        /* add a new partition to part1 and the contents are not equal */
        part1.add(new PartitionId(Integer.MAX_VALUE));
        assertNotEquals(part1, part2);
    }

    private TestNoSQLSubscriber createStream(String tables, int id) {
        /* create test subscriber */
        final NoSQLSubscriptionConfig subscriptionConfig =
            new NoSQLSubscriptionConfig.Builder(ckptTableName + id)
                .setSubscribedTables(tables)
                .build();

        /* no admin, cannot create checkpoint table in stream */
        subscriptionConfig.disableCheckpoint();

        final TestNoSQLSubscriber testNoSQLSubscriber =
            new TestNoSQLSubscriber(subscriptionConfig);
        trace("subscriber created");

        /* create a publisher */
        final StorageNode sn = config.getTopology().getStorageNodeMap()
                                     .getAll().iterator().next();
        final String helpHost = sn.getHostname() + ":" + sn.getRegistryPort();
        KVStoreConfig ksConfig = new KVStoreConfig(kvstoreName, helpHost);

        final NoSQLPublisherConfig conf1 =
            new NoSQLPublisherConfig.Builder(ksConfig, testPath).build();
        publisher = NoSQLPublisher.get(conf1);

        store = NoSQLPublisher.getKVStore(kvstoreName, helpHost);
        /* get a pu without checkpoint */
        if (id == 1) {
            publishingUnit1 =
                    new PublishingUnit(publisher, ((KVStoreImpl)store),
                                       Long.MAX_VALUE, null, testPath, true,
                                       logger);
            trace("publishing unit 1 created");

            /* hook the subscriber with publishing unit */
            publishingUnit1.subscribe(testNoSQLSubscriber);
        } else {
            publishingUnit2 =
                new PublishingUnit(publisher, ((KVStoreImpl)store),
                                   Long.MAX_VALUE, null, testPath, true,
                                   logger);
            trace("publishing unit 2 created");

            publishingUnit2.subscribe(testNoSQLSubscriber);
        }
        try {
            waitFor(new PollCondition(100, 10 * 1000) {
                @Override
                protected boolean condition() {
                    return testNoSQLSubscriber.isSubscriptionSucc();
                }
            });
        } catch (TimeoutException e) {
            fail("timeout in waiting");
        }
        assertTrue("Subscription failed for " +
                   testNoSQLSubscriber.getSubscriptionConfig()
                                      .getSubscriberId() +
                   ", reason " + testNoSQLSubscriber.getCauseOfFailure(),
                   testNoSQLSubscriber.isSubscriptionSucc());
        trace("subscription created");

        final NoSQLSubscription subscription =
            testNoSQLSubscriber.getSubscription();

        /* ensure subscription handle from publisher is expected */
        verifySubscription(subscription, testNoSQLSubscriber);
        trace("subscription verified");

        return testNoSQLSubscriber;

    }

    private void testRepGroup(RepNodeId master, Set<TableImpl> tables)
        throws Exception {

        final int numKeys =  NUM_KEYS_USER_TABLE / 10;
        final NoSQLStreamFeederFilter filter = buildFilter(tables);
        trace("filter built: "  + filter);

        final PublishingUnit.BoundedOutputQueue outputQueue  =
            new PublishingUnit.BoundedOutputQueue(SID,
                                                  "TestCkptTable",
                                                  Integer.MAX_VALUE,
                                                  logger);
        final ReplicationStreamConsumer rscRG1 =
            getRSC(master, tables, filter, outputQueue);
        /* verify on client the allowed list is not initialized */
        assertNull(filter.getOwnedParts());
        rscRG1.start();
        trace("Stream created w/ id=" + rscRG1.getConsumerId());

        waitForStream(master, rscRG1);

        /* verify filter on feeder */
        verifyFeederFilter(master, rscRG1.getConsumerId(), null);

        final MockEntryConsumerThread
            mockEntryConsumer =
            new MockEntryConsumerThread("MockSubscription", numKeys,
                                        outputQueue.getBoundedQueue());
        mockEntryConsumer.start();
        waitForTestDone(mockEntryConsumer);
        trace("Has received " + numKeys + " keys");

        /* verify all rows received from given shard */
        verify(mockEntryConsumer.getRecvOps(), master.getGroupId());
        trace("All received #ops=" + numKeys +
              " verified from " + master.getGroupId());
    }

    private void waitForStream(RepNodeId master,
                               ReplicationStreamConsumer rsc) {

        final String fid = rsc.getConsumerId();
        final boolean succ = new PollCondition(POLL_INTV_MS,
                                               2 * POLL_TIMEOUT_MS) {

            @Override
            protected boolean condition() {
                final NoSQLStreamFeederFilter filter =
                    getFeederFilter(master, fid);
                trace("Feeder filter on " + master + ":" + filter);
                if (filter == null) {
                    trace("Null filter, id="+ fid);
                    return false;
                }
                final int maxOpen = filter.getMaxNumOpenTxn();
                trace("Max open txn=" + maxOpen);
                return maxOpen > 0;
            }
        }.await();
        if (!succ) {
            fail("Timeout in waiting");
        }

        trace("Streaming data started");
    }

    /**
     * Verify all operations in the given list are from give group id
     */
    private void verify(List<StreamOperation> ops, int gid) {
        final long numOps = ops.stream()
                               .map(StreamOperation::getRepGroupId)
                               .filter(id -> {
                                   if (gid == id) {
                                       return true;
                                   }
                                   System.out.println(
                                       "See operation from " + id + " " +
                                       "while expecting " + gid);
                                   return false;
                               })
                               .count();
        assertEquals("Expect all ops from shard " + gid, ops.size(), numOps);
    }

    private void verifyFeederFilter(RepNodeId master,
                                    String fid,
                                    Set<PartitionId> expParts) {
        final Set<PartitionId> parts = getOwnedParts(master, fid);
        assertEquals(expParts, parts);

        final NoSQLStreamFeederFilter filter = getFeederFilter(master, fid);
        trace("Feeder filter on " + master + ":" + filter);
        assertNotNull("Filter must exist", filter);
        final long maxOpenTxn = filter.getMaxNumOpenTxn();
        trace("max # open txn in feeder filter=" + maxOpenTxn);
        assertTrue("Max number of open txn must be positive", maxOpenTxn > 0);
    }

    private Set<PartitionId> getOwnedParts(RepNodeId master, String feederId) {
        final NoSQLStreamFeederFilter filter = getFeederFilter(master, feederId);
        trace("Feeder filter on " + master + ":" + filter);
        return filter.getOwnedParts();
    }

    private ReplicationStreamConsumer getRSC(
        RepNodeId rnId,
        Set<TableImpl> tables,
        NoSQLStreamFeederFilter filter,
        PublishingUnit.BoundedOutputQueue q) throws Exception {

         /* get a feeder filter passing selected tables */
        final RepNode sourceRN = config.getRN(rnId);
        final ReplicatedEnvironment sourceNodeEnv = sourceRN.getEnv(60000);
        if (sourceNodeEnv == null) {
            throw new RNUnavailableException("Source node environment " +
                                             "unavailable while initializing " +
                                             " source rep node");
        }
        final String sourceNodeName = sourceNodeEnv.getNodeName();
        final ReplicationNode node = sourceNodeEnv.getGroup()
                                                  .getMember(sourceNodeName);
        final HostPort hostPort = new HostPort(node.getHostName(),
                                               node.getPort());
        final String feederHostPort = hostPort.toString();
        final RepGroupId gid = new RepGroupId(rnId.getGroupId());
        final ShardMasterInfo masterInfo =
            new ShardMasterInfo(gid, rnId, feederHostPort,
                                System.currentTimeMillis());
        return new ReplicationStreamConsumer(null,
                                             masterInfo,
                                             gid,
                                             q,
                                             tables,
                                             TestUtils.getTestDir()
                                                      .getAbsolutePath(),
                                             filter,
                                             NoSQLStreamMode
                                                 .FROM_STREAM_POSITION,
                                             10,
                                             null, /* deserializer */
                                             null,  /* non-secure store */
                                             logger);
    }

    private void waitForTestDone(final MockEntryConsumerThread t)
        throws TimeoutException {

        boolean success =
            new PollCondition(TEST_POLL_INTERVAL_MS, TEST_POLL_TIMEOUT_MS) {
                @Override
                protected boolean condition() {
                    return t.isTestDone();
                }
            }.await();

        /* if timeout */
        if (!success) {
            throw new TimeoutException("timeout in polling test ");
        }
    }

    /* wait for test done, by checking subscription callback */
    private void waitForTestDone(final TestNoSQLSubscriber testNoSQLSubscriber,
                                 final int expected)
        throws TimeoutException {

        boolean success =
            new PollCondition(TEST_POLL_INTERVAL_MS, TEST_POLL_TIMEOUT_MS) {
                @Override
                protected boolean condition() {
                    trace("# received=" + testNoSQLSubscriber.getNumPuts() +
                          ", #expected=" + expected);
                    return testNoSQLSubscriber.getNumPuts() == expected;
                }
            }.await();

        /* if timeout */
        if (!success) {
            throw new TimeoutException("timeout in polling test ");
        }
    }

    private KVRepTestConfig createTwoShardStoreConfig() throws Exception {

        /*
         * This will create two RGs.
         * RG1 will start with partitions 1,3,5,7,9
         * RG2 will start with partitions 2,4,6,8,10
         */
        return new KVRepTestConfig(this,
                                   1, /* nDC */
                                   2, /* nSN */
                                   3, /* repFactor */
                                   10 /* nPartitions */);


    }

    /* Test subscriber which merely remembers anything it receives */
    private class TestNoSQLSubscriber extends TestNoSQLSubscriberBase {

        final int[] numOpsPerShard = new int[2];

        TestNoSQLSubscriber(NoSQLSubscriptionConfig config) {
            super(config);
        }

        @Override
        public void onNext(StreamOperation t) {
            if (t instanceof StreamPutEvent) {
                numPuts.incrementAndGet();
                recvPutOps.add(t);
                numOpsPerShard[t.getRepGroupId()-1]++;
            } else if (t instanceof StreamDelEvent) {
                numDels.incrementAndGet();
                recvDelOps.add(t);
                numOpsPerShard[t.getRepGroupId()-1]++;
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

        int getNumOpsFromRG2() {
            final int shard = RG_2.getGroupId();
            if (shard <= 0 || shard > numOpsPerShard.length) {
                throw new IllegalStateException("invalid shard id " + shard);
            }
            return numOpsPerShard[shard-1];
        }
    }

    /* txn processor used in unit test  */
    private class MockEntryConsumerThread extends StoppableThread {

        private final BlockingQueue<StreamOperation> queue;
        private final int numExpectedOps;
        private final List<StreamOperation> recvOps;

        volatile boolean testDone;
        MockEntryConsumerThread(String threadName, int numExpectedOps,
                                BlockingQueue<StreamOperation> queue) {
            super(threadName);
            this.queue = queue;
            this.numExpectedOps = numExpectedOps;
            recvOps = new ArrayList<>();
            testDone = false;
        }

        /**
         * The loop which processes state change requests.
         */
        @Override
        public void run() {
            try {
                dequeueLoop(numExpectedOps);
            } catch (Exception e) {
                fail("Unable to receive expected stream operations, # ops " +
                     "received: " + recvOps.size() +
                     ", error: " + e.getMessage());
            }
        }

        /**
         * @return a logger to use when logging uncaught exceptions.
         */
        @Override
        protected Logger getLogger() {
            return logger;
        }

        boolean isTestDone() {
            return testDone;
        }

        List<StreamOperation> getRecvOps() {
            return recvOps;
        }

        /* Loop to dequeue and consume requested entries */
        private void dequeueLoop(long numEntries)
            throws InterruptedException, IllegalStateException {
            long counter = 0;
            while (counter < numEntries) {
                final StreamOperation operation =
                    queue.poll(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
                recvOps.add(operation);
                counter++;
            }

            testDone = true;
        }
    }

    private void verifySubscription(NoSQLSubscription subscription,
                                    TestNoSQLSubscriber subscriber) {
        NoSQLSubscriptionImpl noSQLSubscription =
            (NoSQLSubscriptionImpl) subscription;

        StreamPosition expStreamPosition =
            subscriber.getSubscriptionConfig().getInitialPosition();

        if (expStreamPosition == null) {
            expStreamPosition =
                StreamPosition.getInitStreamPos(
                    kvstoreName, ((KVStoreImpl)store).getTopology().getId(),
                    ((KVStoreImpl)store).getTopology().getRepGroupIds());
        }
        assertTrue("Mismatched init stream pos, expected " +
                   expStreamPosition + ", while actual " +
                   noSQLSubscription.getInitPos(),
                   expStreamPosition.match(noSQLSubscription.getInitPos()));

        TestNoSQLSubscriber actualSubscriber =
            (TestNoSQLSubscriber) noSQLSubscription.getSubscriber();

        assertEquals("Different subscriber", subscriber, actualSubscriber);
    }

    private void migratePartition() {

        final PartitionId pid = P1;
        final RepNodeId srcId = SOURCE_ID;
        final RepNodeId tgtId = TARGET_ID;
        trace("Start migrate " + pid + " from " + srcId + " to " + tgtId);

        final RepGroupId srcGroupId = new RepGroupId(srcId.getGroupId());

        final RepNode source = config.getRN(srcId);
        final RepNode target = config.getRN(tgtId);

        assertEquals(RepNodeAdmin.PartitionMigrationState.PENDING,
                     target.migratePartition(pid, srcGroupId).
                         getPartitionMigrationState());
        waitForMigrationState(target, pid);

        /* The source and target should have changed their partition map */
        waitForPartition(target, pid, true);
        waitForPartition(source, pid, false);

        /* Should be able to call again and get success */
        assertEquals(RepNodeAdmin.PartitionMigrationState.SUCCEEDED,
                     target.getMigrationState(pid).
                         getPartitionMigrationState());

        trace("Done migrating" + pid + " from " + srcId + " to " + tgtId);
        trace("=====================================\n");
        trace("PGT on node " + srcId + ":\n");
        PartitionGenerationTable generationTable =
            source.getPartitionManager().getPartGenTable();
        trace(generationTable.dumpTable());
        trace("=====================================\n");
        trace("PGT on node " + tgtId + ":\n");
        generationTable = target.getPartitionManager().getPartGenTable();
        trace(generationTable.dumpTable());
        trace("=====================================\n");
    }

    private void waitForMigrationState(RepNode rn, PartitionId pId) {
        final RepNodeAdmin.PartitionMigrationState st =
            RepNodeAdmin.PartitionMigrationState.SUCCEEDED;
        boolean success = new PollCondition(POLL_INTV_MS, POLL_TIMEOUT_MS) {

            @Override
            protected boolean condition() {
                final PartitionMigrationStatus status =
                    rn.getMigrationStatus(pId);
                return status != null && status.getState().equals(st);
            }
        }.await();

        assert (success);
    }

    private void waitForPartition(RepNode rn, PartitionId pId,
                                  boolean present) {
        boolean success = new PollCondition(500, 15000) {

            @Override
            protected boolean condition() {
                try {
                    rn.getPartitionDB(pId);
                    return present;
                } catch (IncorrectRoutingException ire) {
                    return !present;
                }
            }
        }.await();
        assert (success);
    }

    private void waitGenerationTableReady(RepGroupId rgId,
                                          RepNodeId excludeRN) {

        boolean success = new PollCondition(POLL_INTV_MS, POLL_TIMEOUT_MS) {

            @Override
            protected boolean condition() {
                RepNode master = getMaster(rgId, excludeRN);
                if (master == null) {
                    return false;
                }
                return master.getPartitionManager().getPartGenTable().isReady();
            }
        }.await();

        assertTrue(success);
    }

    private RepNode getMaster(RepGroupId rgId, RepNodeId excludeRN) {
        for (RepNode rn : config.getRNs()) {
            RepNodeId rnId = rn.getRepNodeId();
            if (rnId.equals(excludeRN) ||
                rnId.getGroupId() != rgId.getGroupId()) {
                continue;
            }

            if (rn.getEnv(1).getState().isMaster()) {
                return rn;
            }
        }
        return null;
    }

    private void waitForFeeder(RepNode master, String feederId) {
        boolean success = new PollCondition(1000, 15000) {

            @Override
            protected boolean condition() {
                ReplicatedEnvironment env = master.getEnv(0);
                Feeder feeder = RepInternal.getRepImpl(env)
                                           .getRepNode()
                                           .feederManager()
                                           .getFeeder(feederId);
                return feeder != null;
            }
        }.await();

        assertTrue(success);
    }

    private void ensureTestDir(String testpath) throws IOException {
        final File dir = new File(testpath);
        if (dir.exists()) {
            try {
                Files.delete(dir.toPath());
            } catch (IOException e) {
                trace("Fail to delete the test path=" + testpath);
            }
        }
        Files.createDirectories(dir.toPath());
        assertTrue(dir.exists());
    }
}
