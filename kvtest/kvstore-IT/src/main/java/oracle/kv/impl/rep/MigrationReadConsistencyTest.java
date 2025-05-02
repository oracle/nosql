/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.rmi.RemoteException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.RequestHandlerImpl;
import oracle.kv.impl.api.Response;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.rep.admin.RepNodeAdmin.PartitionMigrationState;
import oracle.kv.impl.rep.migration.PartitionMigrationStatus;
import oracle.kv.impl.rep.migration.generation.PartitionGenDBManager;
import oracle.kv.impl.rep.migration.generation.PartitionGenNum;
import oracle.kv.impl.rep.migration.generation.PartitionGenerationTable;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.TopologyTest;
import oracle.kv.impl.util.KeyGenerator;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.util.TestUtils;

import org.junit.Test;

/**
 * A collection of unit tests that test read operation with consistency
 * during the partition migration.
 */
public class MigrationReadConsistencyTest extends PartitionMigrationTestBase {
    private static CountDownLatch waitLatch;
    private final RepNodeId laggingRNId = new RepNodeId(2, 2);
    private RepNode source;
    private RepNode target;
    private RepNode laggingRN;
    private CyclicBarrier readBlocked;
    private CyclicBarrier readContinue;

    @Override
    public void tearDown()
        throws Exception {

        if (waitLatch != null) {
            waitLatch.countDown();
        }
        if (readBlocked != null) {
            readBlocked.reset();
        }
        if (readContinue != null) {
            readContinue.reset();
        }
        super.tearDown();
    }

    /**
     * Test read operation with time consistency after a partition migration
     * on a lagging replica that hasn't received all data from migrating
     * partition but has PGT ready and partition database open.
     */
    @Test
    public void testTimeConsistencyLaggingMigration()
        throws Exception {

        /* Prepare test, put a key value, find its partition */
        final Key k1 = prepareTest();
        final PartitionId k1p = source.getPartitionId(k1.toByteArray());

        /*
         * Set a filter to block the replication stream once
         * master detects the data from k1p partition.
         */
        waitLatch = new CountDownLatch(1);
        blockFeeder(target, laggingRN,
                    impl -> impl.getName().equals(k1p.getPartitionName()),
                    waitLatch);

        /* Migrate partition */
        migratePartition(k1p);

        /* Update lagging RN topology so that new partition can be opened */
        laggingRN.getTopologyManager()
            .update(source.getLocalTopology().getCopy());
        waitForPartition(laggingRN, k1p, true);

        /*
         * Partition generation is not opened on lagging RN
         * request should be forwarded and processed by other RN
         */
        Request getRequest = makeTimeConsistencyGetRequest(k1);
        Response res = config.getRH(laggingRNId).execute(getRequest);
        ValueVersion vv = KVStoreImpl.processGetResult(res.getResult());
        assertNotNull(vv);
        assertNotEquals(vv.getVersion().getRepNodeId(), laggingRNId);

        /* Other nodes in the rg can process the request */
        exerciseTimeConsistencyGetRequest(k1, laggingRNId.getGroupId(),
                                          Collections.singleton(laggingRNId));

        /* Unblock the stream and wait for partition generation to open */
        waitLatch.countDown();
        waitForPartitionGenOpen(laggingRN, k1p);

        /* The lagging RN now is able to process */
        exerciseTimeConsistencyGetRequest(k1, laggingRNId.getGroupId(),
                                          Collections.emptySet());
    }

    /**
     * Test read operation with time consistency after a partition migration
     * on a lagging replica that doesn't have partition generation table ready,
     * because it hasn't receive the txn of PGT initialization.
     */
    @Test
    public void testTimeConsistencyPGTNotReady()
        throws Exception {

        /* Prepare test, put a key value, find its partition */
        final Key k1 = prepareTest();
        final PartitionId k1p = source.getPartitionId(k1.toByteArray());

        /* Update with current topology so lagging RN can read PGT from db */
        laggingRN.getTopologyManager()
            .update(source.getLocalTopology().getCopy());

        /*
         * Set a filter to block the replication stream once master
         * detects the data from partition generation database so that
         * the PGT won't be ready on lagging RN.
         */
        waitLatch = new CountDownLatch(1);
        blockFeeder(target, laggingRN,
                    impl ->impl.getName().equals(
                        PartitionGenDBManager.getDBName()),
                    waitLatch);

        /* Migrate partition */
        migratePartition(k1p);

        /*
         * Partition database is not opened on lagging RN even if update
         * topology is invoked, because the replication stream is blocked
         * since the PGT initialization, the txn of opening migrated database
         * wasn't seen by lagging RN, expect RNUnavailableException.
         */
        Request getRequest = makeTimeConsistencyGetRequest(k1);
        laggingRN.getTopologyManager()
            .update(source.getLocalTopology().getCopy());
        try {
            config.getRH(laggingRN.getRepNodeId()).execute(getRequest);
            fail("expect RNUnavailableException");
        } catch (RNUnavailableException rue) {
            TestUtils.checkException(rue, RNUnavailableException.class,
                "database is missing");
        }

        /* Unblock the stream and wait for partition generation to open */
        waitLatch.countDown();
        waitForPartitionGenOpen(laggingRN, k1p);
        waitForPartition(laggingRN, k1p, true);

        /* The lagging RN now is able to process */
        exerciseTimeConsistencyGetRequest(k1, laggingRNId.getGroupId(),
                                          Collections.emptySet());
    }

    /**
     * Test time consistency read post-check when partition generation has
     * changed over the actual read execution.
     */
    @Test
    public void testTimeConsistencyPostCheck()
        throws Exception {

        /* Prepare test, put a key value, find its partition */
        final Key k1 = prepareTest();
        final PartitionId k1p = source.getPartitionId(k1.toByteArray());
        final RepGroupId targetRGId = new RepGroupId(2);

        readBlocked = new CyclicBarrier(2);
        readContinue = new CyclicBarrier(2);

        /* Migrate p1 to target node to trigger PGT init */
        PartitionId anotherPid = null;
        for (PartitionId pid : source.getPartitions()) {
            if (!pid.equals(k1p)) {
                anotherPid = pid;
                break;
            }
        }
        migratePartition(anotherPid);
        assertTrue(source.getPartGenTable().isReady());

        /*
         * Test the case where partition generation is changed from open to
         * close over the read execution, expect the read to be forwarded to
         * different shard.
         */
        final RequestHandlerImpl sourceRH = config.getRH(source.getRepNodeId());
        sourceRH.setPreResponseTestHook(r -> {
            try {
                readBlocked.await();
                readContinue.await();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        SingleGetThread readThread = new SingleGetThread(k1, sourceRH);
        readThread.start();

        /* Wait the read to be blocked */
        readBlocked.await();

        /* Migrate k1p out so generation closes */
        migratePartition(k1p);

        /* clean the previous source record */
        source.getMigrationManager().canceled(k1p, targetRGId);
        assertNull(source.getPartGenTable().getOpenGen(k1p));

        /* Let read continues */
        readContinue.await();
        readThread.join();

        /* Expect the read forward to different shard */
        ValueVersion vv = KVStoreImpl.processGetResult(readThread.getResult());
        assertNotNull(vv);
        assertEquals(vv.getVersion().getRepNodeId().getGroupId(),
                     targetRGId.getGroupId());

        /*
         * Test the case where partition generation is still open after read
         * execution but generation number has changed, expect read to forward.
         */
        readBlocked = new CyclicBarrier(2);
        readContinue = new CyclicBarrier(2);
        final RepNodeId targetReplicaId = new RepNodeId(2, 2);
        final RequestHandlerImpl targetRH = config.getRH(targetReplicaId);
        targetRH.setPreResponseTestHook(r -> {
            try {
                readBlocked.await();
                readContinue.await();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        readThread = new SingleGetThread(k1, targetRH);
        readThread.start();

        /* Wait the read to be blocked */
        readBlocked.await();

        /* Migrate k1p back to source */
        migratePartition(k1p, target, source);

        /* Migrate k1p to target again */
        migratePartition(k1p, source, target);

        /*
         * Release the hook, the read continues but should be forwarded because
         * partition generation number has changed.
         */
        readContinue.await();
        readThread.join();
        vv = KVStoreImpl.processGetResult(readThread.getResult());
        assertNotNull(vv);
        assertNotEquals(vv.getVersion().getRepNodeId(), targetReplicaId);
    }

    /**
     * Test time consistency read post-check when partition generation has
     * changed over the read execution for the cases where partition
     * generation table wasn't initialized at the time of the pre-check.
     */
    @Test
    public void testTimeConsistencyPostCheckUninitPGT()
        throws Exception {

        config.startRepNodeServices();
        source = config.getRN(sourceId);
        target = config.getRN(targetId);
        kvs = KVStoreFactory.getStore(config.getKVSConfig());

        final RepNode srcReplica = config.getRN(new RepNodeId(1, 2));
        final RepNode tarReplica = config.getRN(new RepNodeId(2, 2));

        /* Choose partition id used to test */
        final Iterator<PartitionId> srcIter = source.getPartitions().iterator();
        final PartitionId p1 = srcIter.next();
        final PartitionId p2 = target.getPartitions().iterator().next();
        final PartitionId p3 = srcIter.next();

        /* Insert two keys to p1 and p2 */
        final Key k1 = new KeyGenerator(source).getKeys(p1, 1)[0];
        final Key k2 = new KeyGenerator(target).getKeys(p2, 1)[0];
        final Value v = Value.createValue("Initial value".getBytes());
        assertNotNull(kvs.put(k1, v));
        assertNotNull(kvs.put(k2, v));

        readBlocked = new CyclicBarrier(3);
        readContinue = new CyclicBarrier(3);

        /* Start a thread to read k1 in p1 against srcReplica */
        final RequestHandlerImpl srcRH = config.getRH(srcReplica.getRepNodeId());
        srcRH.setPreResponseTestHook(r -> {
            try {
                readBlocked.await();
                readContinue.await();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        final SingleGetThread srcThread = new SingleGetThread(k1, srcRH);
        srcThread.start();

        /* Start a thread to read k2 in p2 against tarReplica */
        final RequestHandlerImpl tarRH = config.getRH(tarReplica.getRepNodeId());
        tarRH.setPreResponseTestHook(r -> {
            try {
                readBlocked.await();
                readContinue.await();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        final SingleGetThread tarThread = new SingleGetThread(k2, tarRH);
        tarThread.start();

        /* Wait two reads are blocked */
        readBlocked.await();

        /* PGT is not initialized on either srcReplica or tarReplica */
        assertFalse(srcReplica.getPartGenTable().isReady());
        assertFalse(tarReplica.getPartGenTable().isReady());

        /* Migrate another p3 to trigger PGT init */
        migratePartition(p3);
        assertTrue(srcReplica.getPartGenTable().isReady());
        assertTrue(tarReplica.getPartGenTable().isReady());

        /* Migrate p2 out from target shard and back */
        migratePartition(p2, target, source);
        migratePartition(p2, source, target);

        /* PGT is ready, now p1 gen seq number is 0 */
        assertEquals(srcReplica.getPartGenTable().getOpenGen(p1).getGenNum(),
                     PartitionGenNum.generationZero());

        /* p2 gen seq number is 2 after two migrations */
        assertEquals(tarReplica.getPartGenTable().getOpenGen(p2).getGenNum(),
                     new PartitionGenNum(2));

        /* Let the read continues */
        readContinue.await();
        srcThread.join();
        tarThread.join();

        /* Read succeed on source replica without forward */
        ValueVersion vv = KVStoreImpl.processGetResult(srcThread.getResult());
        assertNotNull(vv);
        assertEquals(vv.getVersion().getRepNodeId(), srcReplica.getRepNodeId());

        /* Read is forwarded since generation isn't the zero generation */
        vv = KVStoreImpl.processGetResult(tarThread.getResult());
        assertNotNull(vv);
        assertNotEquals(vv.getVersion().getRepNodeId(),
                        tarReplica.getRepNodeId());
    }

    /*
     * Create RNs and put a generated key value, return the key.
     */
    private Key prepareTest() {
        config.startRepNodeServices();

        source = config.getRN(sourceId);
        target = config.getRN(targetId);
        laggingRN = config.getRN(laggingRNId);

        /* Generate a key which will be on the source node. */
        final Key k1 = new KeyGenerator(source).getKeys(1)[0];
        kvs = KVStoreFactory.getStore(config.getKVSConfig());
        TopologyTest.assertTopoEquals(source.getLocalTopology(),
                                      source.getTopology());
        /* Write an initial value. */
        final Value v1 = Value.createValue("Initial value".getBytes());
        assertNotNull(kvs.put(k1, v1));
        return k1;
    }

    private void migratePartition(PartitionId pid) {
        migratePartition(pid, source, target);
    }

    private void migratePartition(PartitionId pid,
                                  RepNode sourceRN,
                                  RepNode targetRN) {
        final RepGroupId sourceRGId =
            new RepGroupId(sourceRN.getRepNodeId().getGroupId());
        final RepGroupId targetRGId =
            new RepGroupId(targetRN.getRepNodeId().getGroupId());
        assertEquals(PartitionMigrationState.PENDING,
                     targetRN.migratePartition(pid, sourceRGId).
                         getPartitionMigrationState());
        waitForMigrationState(targetRN, pid, PartitionMigrationState.SUCCEEDED);
        PartitionMigrationStatus status = targetRN.getMigrationStatus(pid);
        assertNotNull(status);
        assertEquals(PartitionMigrationState.SUCCEEDED, status.getState());
        assertTrue(status.forTarget());
        status = sourceRN.getMigrationStatus(pid);
        assert status != null;
        assertNull(status.getState());
        assertTrue(status.forSource());
        checkPartition(sourceRN, pid, false);
        checkPartition(targetRN, pid, true);

        /* Update topology to clean up completed migration records */
        final Topology topology = sourceRN.getTopology().getCopy();
        topology.updatePartition(pid, targetRGId);
        sourceRN.updateMetadata(topology);
        targetRN.updateMetadata(topology);

        /*
         * Call canceled() to clean up previous completed migration record
         * in case it still exists, which may cause problem for some tests
         * moving partition between two shards multiple times.
         */
        sourceRN.canceled(pid, targetRGId);
    }

    private void exerciseTimeConsistencyGetRequest(Key k1,
                                                   int rgId,
                                                   Set<RepNodeId> excludeRNs)
        throws Exception {

        for (RepNode rn : config.getRNs()) {
            RepNodeId rnId = rn.getRepNodeId();
            if (rnId.getGroupId() == rgId && !excludeRNs.contains(rnId)) {
                Request getRequest = makeTimeConsistencyGetRequest(k1);
                Response res = config.getRH(rnId).execute(getRequest);
                ValueVersion vv = KVStoreImpl.processGetResult(res.getResult());
                assertNotNull(vv);
                assertEquals(vv.getVersion().getRepNodeId(), rnId);
            }
        }
    }

    private Request makeTimeConsistencyGetRequest(Key key) {
        KVStoreImpl kvsImpl = (KVStoreImpl) kvs;
        return kvsImpl.makeGetRequest(
            key, 0,
            new Consistency.Time(20, TimeUnit.SECONDS, 1, TimeUnit.SECONDS),
            3, TimeUnit.SECONDS, true /* excludeTombstone */);
    }

    /**
     * Waits for a partition generation is opened on this node.
     */
    private void waitForPartitionGenOpen(final RepNode rn,
                                         final PartitionId pid) {
        boolean success = new PollCondition(500, 15000) {

            @Override
            protected boolean condition() {
                PartitionGenerationTable pgt = rn.getPartGenTable();
                if (!pgt.isReady()) {
                    return false;
                }
                return pgt.isPartitionOpen(pid);
            }
        }.await();
        assertTrue(success);
    }

    private class SingleGetThread extends Thread {
        Set<Response> results = new HashSet<>();
        Key key;
        RequestHandlerImpl rh;
        SingleGetThread(Key k, RequestHandlerImpl rh) {
            this.key = k;
            this.rh = rh;
        }

        @Override
        public void run() {
            final Request getRequest = makeTimeConsistencyGetRequest(key);
            try {
                results.add(rh.execute(getRequest));
            } catch (RemoteException e) {
                fail("unexpected error " + e);
            }
        }

        Result getResult() {
            return results.iterator().next().getResult();
        }
    }
}
