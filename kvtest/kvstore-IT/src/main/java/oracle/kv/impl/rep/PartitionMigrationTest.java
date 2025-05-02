/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep;

import static java.util.concurrent.TimeUnit.SECONDS;
import static oracle.kv.impl.api.table.TableTestBase.makeIndexList;
import static oracle.kv.impl.rep.TableTest.addRows;
import static oracle.kv.impl.rep.TableTest.updateMetadata;
import static oracle.kv.impl.rep.TableTest.waitForPopulate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.FaultException;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.Operation;
import oracle.kv.OperationFactory;
import oracle.kv.RequestTimeoutException;
import oracle.kv.StoreIteratorException;
import oracle.kv.Value;
import oracle.kv.Value.Format;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.impl.api.AggregateThroughputTracker;
import oracle.kv.impl.api.table.Region;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableKey;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.admin.RepNodeAdmin.PartitionMigrationState;
import oracle.kv.impl.rep.migration.PartitionMigrationStatus;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.TopologyTest;
import oracle.kv.impl.util.KeyGenerator;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.registry.AsyncControl;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.KeyPair;
import oracle.kv.table.Row;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.Response;

import org.junit.Test;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 */
public class PartitionMigrationTest extends PartitionMigrationTestBase {
    private static final PartitionId p1 = new PartitionId(1);
    private static final PartitionId p2 = new PartitionId(2);
    private static final PartitionId p3 = new PartitionId(3);
    private static final RepGroupId rg1 = new RepGroupId(1);
    private static final RepGroupId rg2 = new RepGroupId(2);

    /**
     * Tests to make sure that the local topology is the same as the current
     * topology.
     */
    @Test
    public void testLocalTopology() {
        config.startRepNodeServices();

        final RepNode repNode = config.getRN(sourceId);

        assertNotNull(repNode.getLocalTopology());
        assertNotNull(repNode.getTopology());
        TopologyTest.assertTopoEquals(repNode.getLocalTopology(),
                                      repNode.getTopology());

        Topology newTopo = config.getTopology().getCopy();
        newTopo.updatePartition(new PartitionId(1),
                                new RepGroupId(
                                         repNode.getRepNodeId().getGroupId()));

        /* Update the topology and check if it is set correctly */
        repNode.updateMetadata(newTopo);
        TopologyTest.assertTopoEquals(newTopo, repNode.getTopology());
        TopologyTest.assertTopoEquals(newTopo, repNode.getLocalTopology());
    }

    /**
     * Tests for correct responses from good and bad requests.
     */
    @Test
    public void testMigrationAPIs() {
        final PartitionId p999 = new PartitionId(999);
        final RepNodeId sourceMasterId = sourceId;
        final RepNodeId targetMasterId = targetId;
        final RepNodeId sourceReplicaId = new RepNodeId(1, 2);
        final RepNodeId targetReplicaId = new RepNodeId(2, 2);

        config.getRepNodeParams(targetId).getMap().
                setParameter(ParameterState.RN_PM_WAIT_AFTER_ERROR, "100 ms");
        config.startRepNodeServices();

        final RepNode sourceMaster = config.getRN(sourceMasterId);
        final RepNode targetMaster = config.getRN(targetMasterId);
        final RepNode sourceReplica = config.getRN(sourceReplicaId);
        final RepNode targetReplica = config.getRN(targetReplicaId);

        /* -- Target API calls to masters -- */

        /*
         * Check the state of a migration for a partition that
         * is not on the node.
         */
        assertEquals(PartitionMigrationState.UNKNOWN,
                     targetMaster.getMigrationState(p1).
                         getPartitionMigrationState());

        /*
         * Check state of a migration for a partition that is already on
         * the node. This should succeed as the partition may have moved there
         * and the admin is just catching up.
         */
        assertEquals(PartitionMigrationState.SUCCEEDED,
                     sourceMaster.getMigrationState(p1).
                         getPartitionMigrationState());

        /*
         * Request a migration from the wrong (existing) source. This fails
         * immediately.
         */
        assertEquals(PartitionMigrationState.ERROR,
                     targetMaster.migratePartition(p1, rg2).
                         getPartitionMigrationState());

        /*
         * Request a migration from the wrong (bogus) source. This test needs
         * a different partition than the previous test because the failed
         * record for partition 1 will still be present.
         */
        assertEquals(PartitionMigrationState.PENDING,
                     targetMaster.migratePartition(p3, new RepGroupId(999)).
                         getPartitionMigrationState());
        /* Will eventually fail after multiple retries */
        waitForMigrationState(targetMaster, p3, PartitionMigrationState.ERROR);

        /*
         * The ERROR state will remain, so we need to call getMigrationState
         * for it to be removed.
         */
        assertEquals(PartitionMigrationState.ERROR,
                     targetMaster.getMigrationState(p3).
                         getPartitionMigrationState());

        /*
         * Request a migration for a bogus partition. This fails
         * immediately
         */
        assertEquals(PartitionMigrationState.ERROR,
                     targetMaster.migratePartition(p999, rg2).
                         getPartitionMigrationState());

        /*
         * Request to cancel a migration should return null unless there is
         * a migration in progress. (If the return is non-null then SUCCEEDED
         * means the operation could not be canceled)
         *
         * Cancel request, valid partition
         */
        assertNull(sourceMaster.canCancel(p2));

        /* Cancel request. bad partition  */
        assertNull(sourceMaster.canCancel(p1));

        /*
         * There should be one status record indicating the migration ended
         * in the ERROR state.
         */
        assertEquals(1, targetMaster.getMigrationStatus().length);
        assertEquals(PartitionMigrationState.ERROR,
                     targetMaster.getMigrationStatus()[0].getState());

        /* -- Target API calls to replicas -- */

        /*
         * Check the state of a migration for a partition that
         * is not on the node.
         */
        assertEquals(PartitionMigrationState.UNKNOWN,
                     targetMaster.getMigrationState(p1).
                         getPartitionMigrationState());

        assertEquals(PartitionMigrationState.UNKNOWN,
                     targetReplica.getMigrationState(p1).
                         getPartitionMigrationState());

        /*
         * Check state of a migration for a partition that is already on
         * the node. This should succeed as the partition may have moved there
         * and the admin is just catching up. (Master/replica does not matter)
         */
        assertEquals(PartitionMigrationState.SUCCEEDED,
                     sourceReplica.getMigrationState(p1).
                         getPartitionMigrationState());

        /* Cancel request to a replica, bad partition  */
        assertEquals(PartitionMigrationState.UNKNOWN,
                     targetReplica.canCancel(p1).
                         getPartitionMigrationState());

        /* Cancel request to a replica, valid partition  */
        assertEquals(PartitionMigrationState.UNKNOWN,
                     sourceReplica.canCancel(p1).
                         getPartitionMigrationState());

        /* -- Source API calls -- */

        /* Real partition and group */
        assertTrue(sourceMaster.canceled(p1, rg1));

        /* Valid, but not present partition */
        assertTrue(sourceMaster.canceled(p2, rg1));    // TODO - should fail?

        /* Bogus partition */
        assertTrue(sourceMaster.canceled(p999, rg1));    // TODO - should fail?

        /* Good partition, wrong group */
        assertTrue(sourceMaster.canceled(p1, rg2));    // TODO - should fail?

        /* Replica should always fail */
        assertFalse(sourceReplica.canceled(p1, rg1));
    }

    /**
     * Tests a simple migration case. Moves one (empty) partition.
     */
    @Test
    public void testEmptyMigration() {
        config.startRepNodeServices();

        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);

        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(p1, rg1).
                         getPartitionMigrationState());
        waitForMigrationState(target, p1, PartitionMigrationState.SUCCEEDED);

        /* The source and target should have changed their partition map */
        waitForPartition(target, p1, true);
        waitForPartition(source, p1, false);

        /* Should be able to call again and get success */
        assertEquals(PartitionMigrationState.SUCCEEDED,
                     target.getMigrationState(p1).
                         getPartitionMigrationState());

        final Topology newTopo = config.getTopology().getCopy();
        newTopo.updatePartition(p1, new RepGroupId(
                                         target.getRepNodeId().getGroupId()));

        /* Should match the local copies */
        TopologyTest.assertTopoEquals(
            source.getLocalTopology(), newTopo, true);
        TopologyTest.assertTopoEquals(
            target.getLocalTopology(), newTopo, true);

        /* Update the topology on the source and target and check */
        source.updateMetadata(newTopo);
        TopologyTest.assertTopoEquals(newTopo, source.getTopology());
        TopologyTest.assertTopoEquals(source.getLocalTopology(),
                                      source.getTopology());

        /*
         * Check to see if the partition DB was actually removed by trying
         * to open it. We use the repNode configuration but change the
         * allow to create to false.
         */
        try {
            source.getEnv(1).openDatabase(null, p1.getPartitionName(),
                           source.getPartitionDbConfig().setAllowCreate(false));
            throw new AssertionError("source partition DB not removed");
        } catch (DatabaseNotFoundException dnfe) {
            /* success */
        }

        target.updateMetadata(newTopo);
        TopologyTest.assertTopoEquals(newTopo, target.getTopology());
        TopologyTest.assertTopoEquals(target.getLocalTopology(),
                                      target.getTopology());

        /* Check to see if the target DB was created */
        Database db = target.getEnv(1).openDatabase(null, p1.getPartitionName(),
                           source.getPartitionDbConfig().setAllowCreate(false));
        db.close();

        /*
         * Should be able to check state even after ToC completed and get
         * success
         */
        assertEquals(PartitionMigrationState.SUCCEEDED,
                     target.getMigrationState(p1).
                         getPartitionMigrationState());
    }

    /**
     * Tests a simple migration case. Moves one record and checks that it made
     * it to the new partition. Also reads at different stages in the
     * migration.
     */
    @Test
    public void testSimpleMigration() {
        config.startRepNodeServices();

        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);

        /* Generate a key which will be on the source node. */
        final Key k1 = new KeyGenerator(source).getKeys(1)[0];

        /* Find its partition */
        final PartitionId k1p = source.getPartitionId(k1.toByteArray());

        kvs = KVStoreFactory.getStore(config.getKVSConfig());

        TopologyTest.assertTopoEquals(source.getLocalTopology(),
                                      source.getTopology());
        /* Write an initial value. */
        final Value v1 = Value.createValue("Initial value".getBytes());
        final Version version1 = kvs.put(k1, v1);

        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(k1p, rg1).
                         getPartitionMigrationState());
        waitForMigrationState(target, k1p, PartitionMigrationState.SUCCEEDED);

        /* Check the target status */
        PartitionMigrationStatus status = target.getMigrationStatus(k1p);
        assert status != null;
        assertEquals(PartitionMigrationState.SUCCEEDED, status.getState());
        assertTrue(status.forTarget());

        /* Check the source status */
        status = source.getMigrationStatus(k1p);
        assert status != null;
        assertNull(status.getState());
        assertTrue(status.forSource());
        assertEquals(status.getRecordsSent(), 1);

        /* The source and target should have changed their partition map */
        checkPartition(source, k1p, false);
        checkPartition(target, k1p, true);

        ValueVersion ver = kvs.get(k1, Consistency.ABSOLUTE, 0, null);
        assertEquals(v1, ver.getValue());
        assertNotEquals(version1, ver.getVersion());

        /* Write a value to the partition. */
        final Value v2 = Value.createValue("Second value".getBytes());
        kvs.put(k1, v2, null, Durability.COMMIT_SYNC, 0, null);

        /* Update the official topo to reflect the migration */
        final Topology newTopo = config.getTopology().getCopy();
        newTopo.updatePartition(
            k1p, new RepGroupId(target.getRepNodeId().getGroupId()));

        /* Should match the local copies */
        TopologyTest.assertTopoEquals(
            source.getLocalTopology(), newTopo, true);
        TopologyTest.assertTopoEquals(
            target.getLocalTopology(), newTopo, true);

        source.updateMetadata(newTopo);
        target.updateMetadata(newTopo);

        /* With the topology updated, try another read. */
        ver = kvs.get(k1, Consistency.ABSOLUTE, 0, null);
        assertEquals(v2, ver.getValue());

        /* And another write */
        final Value v3 = Value.createValue("Second value".getBytes());
        kvs.put(k1, v3, null, Durability.COMMIT_SYNC, 0, null);
        ver = kvs.get(k1, Consistency.ABSOLUTE, 0, null);
        assertEquals(v3, ver.getValue());
    }

    /**
     * Test that putIfVersion checks that the shard UUID matches, not just the
     * VLSN [#28006]
     */
    @Test
    public void testPutIfVersionAfterMigrate() throws Exception {
        config.startRepNodeServices();

        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);

        /* Generate a key which will be on the source node. */
        final Key k1 = new KeyGenerator(source).getKeys(1)[0];

        /* Find its partition */
        final PartitionId k1p = source.getPartitionId(k1.toByteArray());

        kvs = KVStoreFactory.getStore(config.getKVSConfig());

        TopologyTest.assertTopoEquals(source.getLocalTopology(),
                                      source.getTopology());
        /* Write an initial value. */
        final Value v1 = Value.createValue("Initial value".getBytes());

        /* Increase the VLSN so we can match it in the other partition */
        for (int i = 0; i < 25; i++) {
            kvs.put(k1, v1);
        }

        final Version version1 = kvs.put(k1, v1);

        /* Migrate partition */
        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(k1p, rg1).
                         getPartitionMigrationState());
        waitForMigrationState(target, k1p, PartitionMigrationState.SUCCEEDED);
        PartitionMigrationStatus status = target.getMigrationStatus(k1p);
        assert status != null;
        assertEquals(PartitionMigrationState.SUCCEEDED, status.getState());
        assertTrue(status.forTarget());
        status = source.getMigrationStatus(k1p);
        assert status != null;
        assertNull(status.getState());
        assertTrue(status.forSource());
        checkPartition(source, k1p, false);
        checkPartition(target, k1p, true);

        /* Check migrated value */
        final ValueVersion vv2 = kvs.get(k1, Consistency.ABSOLUTE, 0, null);
        assertEquals(v1, vv2.getValue());
        final Version version2 = vv2.getVersion();
        assertNotEquals(version1, version2);

        /*
         * Perform operations to advance the VLSN in the new shard so that we
         * can update the value of k1 and have the update's VLSN matches the
         * one in the old shard
         */
        final long vlsn1 = version1.getVLSN();
        final long vlsn2 = kvs.put(k1, v1).getVLSN();

        /*
         * Compute how many operations we need to perform in order to match the
         * VLSN. Subtract 1 VLSN for the txn commit for the execute, 1 VLSN for
         * the final put, and 1 for the txn associated with the final put.
         */
        final long opsCount = vlsn1 - vlsn2 - 3;
        assertTrue("opsCount (" + opsCount + ") > 0", opsCount > 0);
        final OperationFactory opFactory = kvs.getOperationFactory();
        final List<Operation> ops = new ArrayList<>();
        for (int i = 0; i < opsCount; i++) {
            final Key k = Key.createKey(k1.getMajorPath(), "minor" + i);
            ops.add(opFactory.createPut(k, v1));
        }
        kvs.execute(ops);

        /* Update the value for k1 */
        final Value v2 = Value.createValue("Second value".getBytes());
        kvs.put(k1, v2);
        final ValueVersion vv3 = kvs.get(k1, Consistency.ABSOLUTE, 0, null);
        assertEquals(v2, vv3.getValue());
        final Version version3 = vv3.getVersion();
        assertNotEquals(version1, version3);
        assertEquals(version1.getVLSN(), version3.getVLSN());

        /*
         * Make sure that putIfVersion doesn't find a matching version even
         * though VLSN is the same
         */
        assertEquals(null, kvs.putIfVersion(k1, v2, version1));
    }

    /**
     * Tests canceling a migration.
     */
    @Test
    public void testCancel() {
        config.startRepNodeServices();

        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(new RepNodeId(2, 1));

        final Topology originalTopo = source.getLocalTopology();

        /*
         * This will cause the source to wait before sending
         * any data, keeping the migration from completing.
         */
        ReadHook readHook = new ReadHook();
        source.getMigrationManager().setReadHook(readHook);

        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(p1, rg1).
                         getPartitionMigrationState());

        waitForMigrationState(target, p1, PartitionMigrationState.RUNNING);

        /* Cancel should work */
        assertEquals(PartitionMigrationState.ERROR,
                     target.canCancel(p1).getPartitionMigrationState());

        /* When canceled the state should report an error */
        waitForMigrationState(target, p1, PartitionMigrationState.ERROR);

        /* Let the source thread continue */
        source.getMigrationManager().setReadHook(null);
        readHook.releaseAll();

        /* Make sure the topologies were unaffected */
        TopologyTest.assertTopoEquals(originalTopo, source.getTopology());
        TopologyTest.assertTopoEquals(originalTopo, source.getLocalTopology());

        TopologyTest.assertTopoEquals(originalTopo, target.getTopology());
        TopologyTest.assertTopoEquals(originalTopo, target.getLocalTopology());

        /* Make sure the partition DB is still there. */
        Database db = source.getEnv(1).openDatabase(null, p1.getPartitionName(),
                           source.getPartitionDbConfig().setAllowCreate(false));
        db.close();
    }

    /**
     * Tests that you can not cancel a migration once the transfer is complete.
     */
    @Test
    public void testNoCancel() {
        config.startRepNodeServices();

        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);

        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(p1, rg1).
                         getPartitionMigrationState());
        waitForMigrationState(target, p1, PartitionMigrationState.SUCCEEDED);

        /* Cancel should fail and the state should remian SUCCEEDED */
        assertEquals(PartitionMigrationState.SUCCEEDED,
                     target.canCancel(p1).getPartitionMigrationState());

        final Topology newTopo = config.getTopology().getCopy();
        newTopo.updatePartition(p1, new RepGroupId(
                                         target.getRepNodeId().getGroupId()));
        source.updateMetadata(newTopo);
        target.updateMetadata(newTopo);

        /* Should now report null */
        assertNull(target.canCancel(p1));
        assertNull(source.canCancel(p1));
    }

    /**
     * Tests migrating multiple records.
     */
    @Test
    public void testMultiple() {
        final int N_RECORDS = 1000;

        config.startRepNodeServices();

        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);

        /* Check the throughput consumed by migration */
        final AggregateThroughputTracker sourceTC =
            (AggregateThroughputTracker)source.getAggregateThroughputTracker();
        final AggregateThroughputTracker targetTC =
            (AggregateThroughputTracker)target.getAggregateThroughputTracker();

        kvs = KVStoreFactory.getStore(config.getKVSConfig());

        /* Generate keys which are in the partition on the source node. */
        final Key[] keys = new KeyGenerator(source).getKeys(p1, N_RECORDS);
        final Value[] values = new Value[keys.length];

        for (int i = 0; i < keys.length; i++) {
            values[i] = Value.createValue(("Value " + i).getBytes());
            kvs.put(keys[i], values[i], null, Durability.COMMIT_SYNC, 0, null);
        }

        /* Reads and writes will be rounded up to 1KB in the tracker */
        assertEquals(0,         sourceTC.getReadKB());
        assertEquals(N_RECORDS, sourceTC.getWriteKB());

        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(p1, new RepGroupId(1)).
                         getPartitionMigrationState());
        waitForMigrationState(target, p1, PartitionMigrationState.SUCCEEDED);

        final Topology newTopo = config.getTopology();
        newTopo.updatePartition(p1, new RepGroupId(
                                         target.getRepNodeId().getGroupId()));
        source.updateMetadata(newTopo);
        target.updateMetadata(newTopo);

        /* Check to make sure the DBs are correct */
        checkPartition(source, p1, false);
        checkPartition(target, p1, true);

        /**
         * Migration will add read KBs to the source and
         * write KBs to the target
         */
        assertEquals(N_RECORDS, sourceTC.getReadKB());
        assertEquals(N_RECORDS, sourceTC.getWriteKB());
        assertEquals(0,         targetTC.getReadKB());
        assertEquals(N_RECORDS, targetTC.getWriteKB());

        /* The topology has been updated so verify data. */
        for (int i = 0; i < keys.length; i++) {
            ValueVersion ver = kvs.get(keys[i], Consistency.ABSOLUTE, 0, null);
            assertEquals(values[i], ver.getValue());
        }
    }

    /**
     * Tests inserting client operations into the migration stream.
     * This test will single step the migration and once started inject
     * update/insert/delete client operations into the stream both before
     * and after the cursor location.
     */
    @Test
    public void testClientOp() {
        config.startRepNodeServices();

        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);

        kvs = KVStoreFactory.getStore(config.getKVSConfig());

        /* Generate sorted keys which are in the partition on the source node. */
        final Key[] keys = new KeyGenerator(source).getSortedKeys(p1, 10);

        final Value[] values = new Value[keys.length];

        for (int i = 0; i < keys.length; i++) {
            values[i] = Value.createValue(("Value " + i).getBytes());
        }

        /* Initialize the store with k/v 1, 3, 5, 7, and 9 */
        kvs.put(keys[1], values[1], null, Durability.COMMIT_SYNC, 0, null);
        kvs.put(keys[3], values[3], null, Durability.COMMIT_SYNC, 0, null);
        kvs.put(keys[5], values[5], null, Durability.COMMIT_SYNC, 0, null);
        kvs.put(keys[7], values[7], null, Durability.COMMIT_SYNC, 0, null);
        kvs.put(keys[9], values[9], null, Durability.COMMIT_SYNC, 0, null);

        /*
         * This will cause the migration source to wait before each read. The
         * initial wait will be before the first read by the cursor.
         */
        ReadHook readHook = new ReadHook();
        source.getMigrationManager().setReadHook(readHook);

        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(p1, rg1).
                         getPartitionMigrationState());

        /* The migration target should be waiting in the RUNNING state */
        waitForMigrationState(target, p1, PartitionMigrationState.RUNNING);

        /* Likewise the source is waiting */
        readHook.waitForHook();

        /* Release the hook to send a single k/v to the target */
        readHook.releaseHook();     // key[1] sent
        readHook.waitForHook();
        readHook.releaseHook();     // key[3] sent
        readHook.waitForHook();
        readHook.releaseHook();     // key[5] sent

        /* Insert k/v 0, 2, and 4 that are behind the current key */
        kvs.put(keys[0], values[0], null, Durability.COMMIT_SYNC, 0, null);
        kvs.put(keys[2], values[2], null, Durability.COMMIT_SYNC, 0, null);
        kvs.put(keys[4], values[4], null, Durability.COMMIT_SYNC, 0, null);

        /* Insert k/v 6, and 8 which are in front of the current key */
        kvs.put(keys[6], values[6], null, Durability.COMMIT_SYNC, 0, null);
        kvs.put(keys[8], values[8], null, Durability.COMMIT_SYNC, 0, null);

        /* Delete k 1 (old) and 2 (new) behind the current key */
        kvs.delete(keys[1], null, Durability.COMMIT_SYNC, 0, null);
        kvs.delete(keys[2], null, Durability.COMMIT_SYNC, 0, null);

        /* Delete k 8 (new) and 9 (old) in front of the current key */
        kvs.delete(keys[8], null, Durability.COMMIT_SYNC, 0, null);
        kvs.delete(keys[9], null, Durability.COMMIT_SYNC, 0, null);

        /* Update k/v 3 which is behind the current key */
        values[3] = Value.createValue("UpdatedValue ".getBytes());
        kvs.put(keys[3], values[3], null, Durability.COMMIT_SYNC, 0, null);

        /* Let the read from disk finish */
        readHook.waitForHook();
        source.getMigrationManager().setReadHook(null);
        readHook.releaseHook();
        waitForMigrationState(target, p1, PartitionMigrationState.SUCCEEDED);

        /*
         * Partitions are moved, but only the local topologies have been
         * updated. Try to read the objects while in this state.
         */

        /*
         * k/vs 3, 4, 5, 6, and 7 should still be in the store Note that
         * k/v 3 was an update.
         */
        ValueVersion ver;
        ver = kvs.get(keys[0], Consistency.ABSOLUTE, 0, null);
        assertEquals(values[0], ver.getValue());
        ver = kvs.get(keys[3], Consistency.ABSOLUTE, 0, null);
        assertEquals(values[3], ver.getValue());
        ver = kvs.get(keys[4], Consistency.ABSOLUTE, 0, null);
        assertEquals(values[4], ver.getValue());
        ver = kvs.get(keys[5], Consistency.ABSOLUTE, 0, null);
        assertEquals(values[5], ver.getValue());
        ver = kvs.get(keys[6], Consistency.ABSOLUTE, 0, null);
        assertEquals(values[6], ver.getValue());
        ver = kvs.get(keys[7], Consistency.ABSOLUTE, 0, null);
        assertEquals(values[7], ver.getValue());

        /* k/vs 1, 2, 8 and 9 should be gone */
        assertNull(kvs.get(keys[1], Consistency.ABSOLUTE, 0, null));
        assertNull(kvs.get(keys[2], Consistency.ABSOLUTE, 0, null));
        assertNull(kvs.get(keys[8], Consistency.ABSOLUTE, 0, null));
        assertNull(kvs.get(keys[9], Consistency.ABSOLUTE, 0, null));

        /* Migration done, update topo. */
        final Topology topo = config.getTopology();
        topo.updatePartition(p1, new RepGroupId(
                                      target.getRepNodeId().getGroupId()));
        source.updateMetadata(topo);
        target.updateMetadata(topo);

        /* The topology has been updated so verify data again. */

        ver = kvs.get(keys[0], Consistency.ABSOLUTE, 0, null);
        assertEquals(values[0], ver.getValue());
        ver = kvs.get(keys[3], Consistency.ABSOLUTE, 0, null);
        assertEquals(values[3], ver.getValue());
        ver = kvs.get(keys[4], Consistency.ABSOLUTE, 0, null);
        assertEquals(values[4], ver.getValue());
        ver = kvs.get(keys[5], Consistency.ABSOLUTE, 0, null);
        assertEquals(values[5], ver.getValue());
        ver = kvs.get(keys[6], Consistency.ABSOLUTE, 0, null);
        assertEquals(values[6], ver.getValue());
        ver = kvs.get(keys[7], Consistency.ABSOLUTE, 0, null);
        assertEquals(values[7], ver.getValue());

        assertNull(kvs.get(keys[1], Consistency.ABSOLUTE, 0, null));
        assertNull(kvs.get(keys[2], Consistency.ABSOLUTE, 0, null));
        assertNull(kvs.get(keys[8], Consistency.ABSOLUTE, 0, null));
        assertNull(kvs.get(keys[9], Consistency.ABSOLUTE, 0, null));
    }

    @Test
    public void testConflict() {
        /* Pausing the source may cause the sream to timeout on some systems */
        config.getRepNodeParams(targetId).getMap().
               setParameter(ParameterState.RN_PM_SO_READ_WRITE_TIMEOUT, "15 s");

        config.startRepNodeServices();

        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);

        kvs = KVStoreFactory.getStore(config.getKVSConfig());

        /* Generate sorted keys which are in the partition on the source node. */
        final Key[] keys = new KeyGenerator(source).getSortedKeys(p1, 3);

        final Value[] values = new Value[keys.length];

        for (int i = 0; i < keys.length; i++) {
            values[i] = Value.createValue(("Value " + i).getBytes());
        }
        /* Initialize the store with k/v 1, 3, 5, 7, and 9 */
        kvs.put(keys[1], values[1], null, Durability.COMMIT_SYNC, 0, null);

        /*
         * This will cause the migration source to wait before each read. The
         * initial wait will be before the first read by the cursor.
         */
        ReadHook readHook = new ReadHook();
        source.getMigrationManager().setReadHook(readHook);

        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(p1, rg1).
                         getPartitionMigrationState());

        /* The migration target should be waiting in the RUNNING state */
        waitForMigrationState(target, p1, PartitionMigrationState.RUNNING);

        /* Likewise the source is waiting */
        readHook.waitForHook();

        /* Release the hook to send a single k/v to the target */
        readHook.releaseHook();     // key[1] sent

        /* Write complete, key[1] is locked, waiting to read next */
        readHook.waitForHook();

        /* Writing before and after should be OK */
        kvs.put(keys[0], values[0], null, Durability.COMMIT_SYNC, 0, null);
        kvs.put(keys[2], values[2], null, Durability.COMMIT_SYNC, 0, null);

        /* Attempt to write to the same key */
        try {
            kvs.put(keys[1], values[1], null, Durability.COMMIT_SYNC, 0, null);
            fail("write to same key should have failed");
        } catch (RequestTimeoutException rte) {
            /* Should happen */
        }

        /* Let the read from disk finish */
        source.getMigrationManager().setReadHook(null);
        readHook.releaseHook();
        waitForMigrationState(target, p1, PartitionMigrationState.SUCCEEDED);

        /*
         * Put should work fine now (eventually). It may take some time
         * before the replicas are on page with the competition.
         */
        boolean success = new PollCondition(100, 10000) {

            @Override
            protected boolean condition() {
                try {
                    kvs.put(keys[1], values[0], null,
                            Durability.COMMIT_SYNC, 0, null);
                    return true;
                } catch (FaultException e) {
                    return false;
                }
            }
        }.await();
        assert(success);
    }

    @Test
    public void testSourceRestart() {

        /*
         * Reduce the wait after an error response to reduce test runtime.
         * The wait parameters are read when the target is started.
         */
        config.getRepNodeParams(targetId).getMap().
                    setParameter(ParameterState.RN_PM_WAIT_AFTER_ERROR, "1 s");

        config.startRepNodeServices();

        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);

        /* Start and hold the migration. */
        ReadHook readHook = new ReadHook();
        source.getMigrationManager().setReadHook(readHook);

        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(p1, rg1).
                         getPartitionMigrationState());

        waitForMigrationState(target, p1, PartitionMigrationState.RUNNING);

        readHook.waitForHook();

        /*
         * This will free the source thread (so we can shutdown) and will
         * cause the migration to fail (which is OK).
         */
        readHook.throwException(
                    new RuntimeException("Exception injected by unit test"));

        /* Kill the source */
        config.stopRepNodeServicesSubset(true, source.getRepNodeId());

        /* After the source fails, the target should return to PENDING state */
        waitForMigrationState(target, p1, PartitionMigrationState.PENDING);

        config.startRepNodeSubset(sourceId);

        /* Eventually the migration should resume and complete */
        waitForMigrationState(target, p1, PartitionMigrationState.SUCCEEDED);
    }

    /*
     * Tests when the migration target fails after starting a migration and
     * one of the replicas takes over as master. The new master should restart
     * and complete the migration.
     */
    @Test
    public void testTargetFailover() {
        final RepNodeId target2Id = new RepNodeId(2, 2);
        final RepNodeId target3Id = new RepNodeId(2, 3);

        /**
         * After failover RN2 or 3 will become the master and restart the
         * migration. The restart has to wait for the target monitor thread to
         * notice the aborted migration the new target retry up to 10 times,
         * waiting in between. The waits should not be so short as to timeout
         * but not the default (2 min!).
         */
        config.getRepNodeParams(target2Id).getMap().
                    setParameter(ParameterState.RN_PM_WAIT_AFTER_BUSY, "2 s");
        config.getRepNodeParams(target2Id).getMap().
                    setParameter(ParameterState.RN_PM_WAIT_AFTER_ERROR, "2 s");
        config.getRepNodeParams(target3Id).getMap().
                    setParameter(ParameterState.RN_PM_WAIT_AFTER_BUSY, "2 s");
        config.getRepNodeParams(target3Id).getMap().
                    setParameter(ParameterState.RN_PM_WAIT_AFTER_ERROR, "2 s");

        config.startRepNodeServices();

        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);

        /* Start and hold the migration. */
        ReadHook readHook = new ReadHook();
        source.getMigrationManager().setReadHook(readHook);

        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(p1, rg1).
                         getPartitionMigrationState());

        readHook.waitForHook();

        waitForMigrationState(target, p1, PartitionMigrationState.RUNNING);

        /* Kill the target node while waiting for source */
        config.stopRepNodeServicesSubset(true, target.getRepNodeId());

        /* Clear the hook to allow the source to work normally */
        readHook.releaseHook();
        source.getMigrationManager().setReadHook(null);

        /* One of the replicas will become the master. */
        final RepNode newTarget = waitForMaster(config.getRN(target2Id),
                                                config.getRN(target3Id));

        /*
         * The source will note that the target has failed and will reset.
         * Eventually the target request will be granted the the migration
         * restart and complete.
         */
        waitForMigrationState(newTarget, p1, PartitionMigrationState.SUCCEEDED);
    }

    /**
     * Tests multiple migration requests causing the source to be busy.
     */
    @Test
    public void testSourceBusy() {

        /* Reduce the wait after a busy response to reduce test runtime. */
        config.getRepNodeParams(targetId).getMap().
                    setParameter(ParameterState.RN_PM_WAIT_AFTER_BUSY, "1 s");

        config.startRepNodeServices();

        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);

        /*
         * This will cause the source to wait before sending
         * any data, keeping the migration from completing.
         */
        ReadHook readHook = new ReadHook();
        source.getMigrationManager().setReadHook(readHook);

        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(p1, rg1).
                         getPartitionMigrationState());

        waitForMigrationState(target, p1, PartitionMigrationState.RUNNING);

        /* Request for the same in-progress migration should return RUNNING */
        assertEquals(PartitionMigrationState.RUNNING,
                     target.migratePartition(p1, rg1).
                         getPartitionMigrationState());

        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(p3, rg1).
                         getPartitionMigrationState());

        readHook.waitForHook();

        /* Let the source thread continue for p1 */
        readHook.releaseHook();

        /* Once p1 is done, moving p3 should start */
        waitForMigrationState(target, p1, PartitionMigrationState.SUCCEEDED);
        waitForMigrationState(target, p3, PartitionMigrationState.RUNNING);

        /* Let the source thread continue for p3 */
        source.getMigrationManager().setReadHook(null);
        readHook.releaseAll();

        waitForMigrationState(target, p3, PartitionMigrationState.SUCCEEDED);

        /* Check that requests to move them again should return SUCCEEDED */
        assertEquals(PartitionMigrationState.SUCCEEDED,
                     target.migratePartition(p1, rg1).
                         getPartitionMigrationState());

        assertEquals(PartitionMigrationState.SUCCEEDED,
                     target.migratePartition(p3, rg1).
                         getPartitionMigrationState());
    }

    /**
     * Tests multiple migration requests causing the target to be busy.
     */
    @Test
    public void testTargetBusy() {
        final PartitionId p5 = new PartitionId(5);

        config.getRepNodeParams(targetId).getMap().
                setParameter(ParameterState.RN_PM_WAIT_AFTER_BUSY, "1 s");
        config.getRepNodeParams(sourceId).getMap().
                setParameter(ParameterState.RN_PM_CONCURRENT_SOURCE_LIMIT, "3");
        config.getRepNodeParams(targetId).getMap().
                setParameter(ParameterState.RN_PM_CONCURRENT_TARGET_LIMIT, "2");
        config.getRepNodeParams(targetId).getMap().
                setParameter(ParameterState.RN_PM_SO_READ_WRITE_TIMEOUT, "20 s");

        config.startRepNodeServices();

        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);

        /*
         * This will cause the source to wait before sending
         * any data, keeping the migration from completing.
         */
        final ReadHook readHook = new ReadHook();
        source.getMigrationManager().setReadHook(readHook);

        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(p1, rg1).
                         getPartitionMigrationState());
        waitForMigrationState(target, p1, PartitionMigrationState.RUNNING);

        /* A second request should enter the running state */
        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(p3, rg1).
                         getPartitionMigrationState());
        waitForMigrationState(target, p3, PartitionMigrationState.RUNNING);

        /* The third request should remain paused */
        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(p5, rg1).
                         getPartitionMigrationState());

        readHook.waitForHook();

        waitForMigrationState(target, p5, PartitionMigrationState.PENDING);

        /* At this point there should be three targets */
        assertEquals(3, target.getMigrationStatus().length);

        /* Replicas should always report nothing */
        final RepNode targetReplica = config.getRN(new RepNodeId(2, 2));
        assertEquals(0, targetReplica.getMigrationStatus().length);

        /* Let the source thread(s) continue */
        readHook.releaseHook();

        /* Once at least one is done, migrating p5 should start */
        waitForMigrationState(target, p5, PartitionMigrationState.RUNNING);

        /* Let the source thread continue */
        source.getMigrationManager().setReadHook(null);
        readHook.releaseAll();

        waitForMigrationState(target, p1, PartitionMigrationState.SUCCEEDED);
        waitForMigrationState(target, p3, PartitionMigrationState.SUCCEEDED);
        waitForMigrationState(target, p5, PartitionMigrationState.SUCCEEDED);
    }

    /**
     * Tests the migration store topo sequence # which is meant to prevent
     * an out-of-date topology to be used in the case of a node restart.
     */
    @Test
    public void testTopoMismatch() {
        config.startRepNodeServices();

        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);
        final RepNodeId sourceReplicaId = new RepNodeId(1, 2);

        RepNode sourceReplica = config.getRN(sourceReplicaId);

        /* Stop one of the replicas before the migration happens. */
        config.stopRepNodeServicesSubset(true, sourceReplica.getRepNodeId());
        try {
            Thread.sleep(100);
        } catch (InterruptedException ignore) {}

        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(p1, rg1).
                         getPartitionMigrationState());
        waitForMigrationState(target, p1, PartitionMigrationState.SUCCEEDED);

        /*
         * Update the official topo to reflect the migration. This will cause
         * the migration record for the completed migration to be removed and
         * the topo seq # in the migration store to set to the latest topo.
         */
        final Topology newTopo = config.getTopology().getCopy();
        newTopo.updatePartition(p1, new RepGroupId(
                                         target.getRepNodeId().getGroupId()));

        source.updateMetadata(newTopo);
        target.updateMetadata(newTopo);

        /*
         * Restart the replica. When it comes up its stored topo will be an
         * older version than the # stored in the migration store (once the
         * replication stream is processed). When this is detected it will
         * send a NOP to the master to get a new topo.
         */
        config.startRepNodeSubset(sourceReplicaId);

        /* Refresh the ref to restarted node */
        sourceReplica = config.getRN(sourceReplicaId);

        /*
         * Eventually the replica will catch up nd the moved partition will be
         * gone.
         */
        waitForPartition(sourceReplica, p1, false);
    }

    /*
     * Tests the target's reaction to the possible migration request responses
     */
    @Test
    public void testSourceResponse() {
        config.getRepNodeParams(targetId).getMap().
                    setParameter(ParameterState.RN_PM_WAIT_AFTER_BUSY, "1 s");
        config.getRepNodeParams(targetId).getMap().
                    setParameter(ParameterState.RN_PM_WAIT_AFTER_ERROR, "1 s");

        config.startRepNodeServices();

        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);

        final ResponseHook responseHook = new ResponseHook();
        source.getMigrationManager().setResponseHook(responseHook);

        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(p1, rg1).
                         getPartitionMigrationState());

        /* BUSY and UNKNOWN_SERVICE will be retried */
        responseHook.waitForHook();
        responseHook.sendResponse(Response.BUSY);
        waitForMigrationState(target, p1, PartitionMigrationState.PENDING);

        responseHook.waitForHook();
        responseHook.sendResponse(Response.UNKNOWN_SERVICE);
        waitForMigrationState(target, p1, PartitionMigrationState.PENDING);

        /* FORMAT_ERROR is fatal */
        responseHook.waitForHook();
        responseHook.sendResponse(Response.FORMAT_ERROR);
        waitForMigrationState(target, p1, PartitionMigrationState.ERROR);
    }

    /**
     * Tests that an aborted partition migration is restarted and completes
     * correctly when there is a secondary database present.
     *
     * 1. Create an index and populate the store
     * 2. Start a partition migration, moving some records to the target
     * 3. Cause the migration to fail
     * 4. Wait for a migration restart and success
     *
     * If the interlock does not work, when the migration restarts it will
     * fail with SecondaryIntegrityExceptions when the duplicate secondary
     * records are detected because either the records were not cleaned, or
     * not cleaned in time.
     */
    @Test
    public void testTableInterlock() {
        config.getRepNodeParams(targetId).getMap().
                    setParameter(ParameterState.RN_PM_WAIT_AFTER_BUSY, "1 s");
        config.getRepNodeParams(targetId).getMap().
                    setParameter(ParameterState.RN_PM_WAIT_AFTER_ERROR, "1 s");

        config.startRepNodeServices();

        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);

        kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI apiImpl = kvs.getTableAPI();

        TableMetadata md = new TableMetadata(false);

        final TableImpl userTable = TableBuilder.createTableBuilder("User")
                                                .addInteger("id")
                                                .addString("firstName")
                                                .addString("lastName")
                                                .addInteger("age")
                                                .primaryKey("id")
                                                .shardKey("id")
                                                .buildTable();

        TableImpl table = md.addTable(userTable.getInternalNamespace(),
                                      userTable.getName(),
                                      userTable.getParentName(),
                                      userTable.getPrimaryKey(),
                                      null,
                                      userTable.getShardKey(),
                                      userTable.getFieldMap(),
                                      null, null,
                                      false, 0,
                                      null,
                                      null /* owner */);

        updateMetadata(source, md);
        updateMetadata(target, md);

        addRows(table, 200, apiImpl);
        md.addIndex(null, "FirstName", table.getFullName(),
                    makeIndexList("firstName"), null, true, false,
                    null);
        updateMetadata(source, md);
        updateMetadata(target, md);

        waitForPopulate(source, "FirstName", table);
        waitForPopulate(target, "FirstName", table);

        final ReadHook readHook = new ReadHook();
        source.getMigrationManager().setReadHook(readHook);

        /* Start a migration, wait for it to run, and then cause an error */
        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(p1, rg1).
                         getPartitionMigrationState());

        waitForMigrationState(target, p1, PartitionMigrationState.RUNNING);

        /* Send some records */
        for (int i = 0; i < 10; i++) {
            readHook.waitForHook();
            readHook.releaseHook();
        }
        readHook.waitForHook();

        /* This will cause the migration to fail  */
        readHook.throwException(
                    new RuntimeException("Exception injected by unit test"));

        /* Clear the hook so that a restart runs. */
        source.getMigrationManager().setReadHook(null);

        /*
         * Once the secondary is cleaned the migration will resume and
         * eventually complete
         */
        waitForMigrationState(target, p1, PartitionMigrationState.SUCCEEDED);
    }

    /**
     * Tests the function of the awaitIdle() method. Basically the method
     * should return false unless there is no migration activity on either
     * the source or target nodes.
     */
    @Test
    public void testAwaitIdle() throws InterruptedException {

        /*
         * Increase the partition migration read/write timeout to cover the
         * amount of time needed to update the metadata since we hold up writes
         * by the source during that time.
         */
        config.getRepNodeParams(targetId).getMap().
               setParameter(ParameterState.RN_PM_SO_READ_WRITE_TIMEOUT,
                            "60 s");

        config.startRepNodeServices();

        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);

        kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI apiImpl = kvs.getTableAPI();

        TableMetadata md = new TableMetadata(false);

        final TableImpl userTable = TableBuilder.createTableBuilder("User")
                                                .addInteger("id")
                                                .addString("firstName")
                                                .addString("lastName")
                                                .addInteger("age")
                                                .primaryKey("id")
                                                .shardKey("id")
                                                .buildTable();

        TableImpl table = md.addTable(userTable.getInternalNamespace(),
                                      userTable.getName(),
                                      userTable.getParentName(),
                                      userTable.getPrimaryKey(),
                                      null,
                                      userTable.getShardKey(),
                                      userTable.getFieldMap(),
                                      null, null,
                                      false, 0,
                                      null,
                                      null /* owner */);

        updateMetadata(source, md);
        updateMetadata(target, md);

        addRows(table, 2000, apiImpl);
        md.addIndex(null, "FirstName", table.getFullName(),
                    makeIndexList("firstName"), null, true, false,
                    null);
        updateMetadata(source, md);
        updateMetadata(target, md);

        waitForPopulate(source, "FirstName", table);
        waitForPopulate(target, "FirstName", table);

        /* At this point nothing should be running */
        assertTrue(source.getMigrationManager().isIdle());
        assertTrue(target.getMigrationManager().awaitTargetIdle(null));

        final ReadHook readHook = new ReadHook();
        source.getMigrationManager().setReadHook(readHook);

        /* Start a migration, wait for it to run, and then cause an error */
        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(p1, rg1).
                         getPartitionMigrationState());

        waitForMigrationState(target, p1, PartitionMigrationState.RUNNING);

        /* Send some records */
        for (int i = 0; i < 10; i++) {
            readHook.waitForHook();
            readHook.releaseHook();
        }
        readHook.waitForHook();

        /* Now the source and target should be busy */
        assertFalse(source.getMigrationManager().isIdle());
        assertFalse(target.getMigrationManager().awaitTargetIdle(null));

        /* Clear the hook so that the migration completes */
        source.getMigrationManager().setReadHook(null);
        readHook.releaseAll();
        waitForMigrationState(target, p1, PartitionMigrationState.SUCCEEDED);

        /* The source should still be busy but the target should be clear */
        assertFalse(source.getMigrationManager().isIdle());
        assertTrue(target.getMigrationManager().awaitTargetIdle(null));

        final Topology newTopo = config.getTopology().getCopy();
        newTopo.updatePartition(p1, new RepGroupId(
                                         target.getRepNodeId().getGroupId()));

        source.updateMetadata(newTopo);
        target.updateMetadata(newTopo);

        /* The source should now be clear */
        assertTrue(source.getMigrationManager().isIdle());
    }

    @Test
    public void testIndexCreate() throws InterruptedException {

        /*
         * Increase the partition migration read/write timeout to cover the
         * amount of time needed to update the metadata since we hold up writes
         * by the source during that time.
         */
        config.getRepNodeParams(targetId).getMap().
               setParameter(ParameterState.RN_PM_SO_READ_WRITE_TIMEOUT,
                            "60 s");
        config.startRepNodeServices();

        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);

        kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI apiImpl = kvs.getTableAPI();

        TableMetadata md = new TableMetadata(false);

        final TableImpl userTable = TableBuilder.createTableBuilder("User")
                                                .addInteger("id")
                                                .addString("firstName")
                                                .addString("lastName")
                                                .addInteger("age")
                                                .primaryKey("id")
                                                .shardKey("id")
                                                .buildTable();

        TableImpl table = md.addTable(userTable.getInternalNamespace(),
                                      userTable.getName(),
                                      userTable.getParentName(),
                                      userTable.getPrimaryKey(),
                                      null,
                                      userTable.getShardKey(),
                                      userTable.getFieldMap(),
                                      null, null,
                                      false, 0,
                                      null,
                                      null /* owner */);

        updateMetadata(source, md);
        updateMetadata(target, md);

        addRows(table, 5000, apiImpl);

        /* At this point nothing should be running */
        assertTrue(source.getMigrationManager().isIdle());
        assertTrue(target.getMigrationManager().awaitTargetIdle(null));

        final ReadHook readHook = new ReadHook();
        source.getMigrationManager().setReadHook(readHook);

        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(p1, rg1).
                         getPartitionMigrationState());

        waitForMigrationState(target, p1, PartitionMigrationState.RUNNING);

        /* Send some records */
        for (int i = 0; i < 500; i++) {
            readHook.waitForHook();
            readHook.releaseHook();
        }
        readHook.waitForHook();

        /* Now the source and target should be busy */
        assertFalse(source.getMigrationManager().isIdle());
        assertFalse(target.getMigrationManager().awaitTargetIdle(null));

        /* Create an index */
        md.addIndex(null, "FirstName", table.getFullName(),
                    makeIndexList("firstName"), null, true, false,
                    null);
        updateMetadata(source, md);
        updateMetadata(target, md);

        /* Clear the hook so that the migration completes */
        source.getMigrationManager().setReadHook(null);
        readHook.releaseAll();
        waitForMigrationState(target, p1, PartitionMigrationState.SUCCEEDED);

        /* The source should still be busy but the target should be clear */
        assertFalse(source.getMigrationManager().isIdle());
        assertTrue(target.getMigrationManager().awaitTargetIdle(null));

        final Topology newTopo = config.getTopology().getCopy();
        newTopo.updatePartition(p1, new RepGroupId(
                                         target.getRepNodeId().getGroupId()));

        source.updateMetadata(newTopo);
        target.updateMetadata(newTopo);

        /* The source should now be clear */
        assertTrue(source.getMigrationManager().isIdle());

        waitForPopulate(source, "FirstName", table);
        waitForPopulate(target, "FirstName", table);

        /**
         * The table metadata is only changed once. Creating the iterator
         * again should get the new topo. So really only needs to be 1.
         */
        int retry = 2;
        while (retry > 0) {
            try {
                /**
                 * Count the records via the index to make sure they are
                 * all there
                 */
                final Index index = md.getIndex(null, table.getFullName(),
                                                "FirstName");
                final IndexKey ikey = index.createIndexKey();
                final TableIterator<KeyPair> iter =
                        apiImpl.tableKeysIterator(ikey, null, null);
                int count = 0;
                while (iter.hasNext()) {
                    iter.next();
                    ++count;
                }
                assertEquals("Unexpected count", 5000, count);
                iter.close();
            } catch (StoreIteratorException sie) {
                /* May get this due to the topology changing - retry */
                retry--;
            }
            return;
        }
        fail("Unable to get count after retries");
    }

    /**
     * Tests #28160. The test cancels a migration and then attempts to
     * delete a table. The cancel left transient state in the migration manager
     * (necessary for cleanup) but the bug was that the canceled partition
     * was given to the table maintenance thread for cleaning. The partition
     * did not exist and the cleaning would loop forever.
     */
    @Test
    public void testTableDelete() {
        config.startRepNodeServices();

        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);

        kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI tableAPI = kvs.getTableAPI();

        TableMetadata md = new TableMetadata(false);

        final TableImpl userTable = TableBuilder.createTableBuilder("User")
                                                .addInteger("id")
                                                .addString("firstName")
                                                .addString("lastName")
                                                .addInteger("age")
                                                .primaryKey("id")
                                                .shardKey("id")
                                                .buildTable();

        final TableImpl table = md.addTable(userTable.getInternalNamespace(),
                                            userTable.getName(),
                                            userTable.getParentName(),
                                            userTable.getPrimaryKey(),
                                            null,
                                            userTable.getShardKey(),
                                            userTable.getFieldMap(),
                                            null, null,
                                            false, 0,
                                            null,
                                            null /* owner */);
        updateMetadata(source, md);
        updateMetadata(target, md);

        addRows(table, 2000, tableAPI);

        /*
         * This will cause the source to wait before sending
         * any data, keeping the migration from completing.
         */
        ReadHook readHook = new ReadHook();
        source.getMigrationManager().setReadHook(readHook);

        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(p1, rg1).
                         getPartitionMigrationState());

        waitForMigrationState(target, p1, PartitionMigrationState.RUNNING);

        /* Cancel should work */
        assertEquals(PartitionMigrationState.ERROR,
                     target.canCancel(p1).getPartitionMigrationState());

        /* When canceled the state should report an error */
        waitForMigrationState(target, p1, PartitionMigrationState.ERROR);

        /* Let the source thread continue */
        source.getMigrationManager().setReadHook(null);
        readHook.releaseAll();

        /* Mark the table for delete, this will start the partition cleaning */
        md.dropTable(null, table.getName(), true /*markForDelete*/);
        updateMetadata(source, md);
        updateMetadata(target, md);

        /* Make sure the table cleaning finishes */
        boolean success = new PollCondition(500, 10000) {
            @Override
            protected boolean condition() {
                return target.removeTableDataComplete(null, table.getName());
            }
        }.await();
        assertTrue("wait failed for table remove on target", success);
    }

    @Test
    public void testMultiRegionTable() throws InterruptedException {

        /*
         * Increase the partition migration read/write timeout to cover the
         * amount of time needed to update the metadata since we hold up writes
         * by the source during that time.
         */
        config.getRepNodeParams(targetId).getMap().
               setParameter(ParameterState.RN_PM_SO_READ_WRITE_TIMEOUT,
                            "60 s");
        config.startRepNodeServices();

        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);

        /* Enable reading tombstones */
        kvs = KVStoreFactory.getStore(config.getKVSConfig().
                                                setExcludeTombstones(false));
        final TableAPI tableAPI = kvs.getTableAPI();

        TableMetadata md = new TableMetadata(false);
        md.setLocalRegionName("LocalRegion");
        final String regionName = "Foo";
        md.createRegion(regionName);
        final int regionId = md.getRegionMapper().getRegionId(regionName);

        final TableImpl userTable = TableBuilder.createTableBuilder("User")
                                                .addInteger("id")
                                                .addString("firstName")
                                                .addString("lastName")
                                                .addInteger("age")
                                                .primaryKey("id")
                                                .shardKey("id")
                                                .buildTable();

        TableImpl table = md.addTable(userTable.getInternalNamespace(),
                                      userTable.getName(),
                                      userTable.getParentName(),
                                      userTable.getPrimaryKey(),
                                      null,
                                      userTable.getShardKey(),
                                      userTable.getFieldMap(),
                                      null, null,
                                      false, 0,
                                      null,
                                      null /* owner */,
                                      false /*sysTable*/,
                                      null /*identityColumnInfo*/,
                                      Collections.singleton(regionId),
                                      false, /* json collection */
                                      null   /* mr counters for json coll */);

        updateMetadata(source, md);
        updateMetadata(target, md);

        /*
         * Put and delete a bunch of rows which will create tombstones. There
         * should be enough that some are in the source partition.
         */
        final Key[] keys = new Key[1000];
        for (int i = 0; i < keys.length; i++) {
            final Row row = table.createRow();
            row.put("id", i+100);
            row.put("firstName", "record" + i);
            /* Save K/V key */
            keys[i] = TableKey.createKey(table, row, false).getKey();
            tableAPI.put(row, null, null);

            /* Delete second half now */
            if  (i >= keys.length/2) {
                tableAPI.delete(row.createPrimaryKey(), null, null);
            }
        }

        /* Tombstones for the second half will be there */
        for (int i = keys.length/2; i < keys.length; i++) {
            assertTrue(isTombstone(kvs.get(keys[i], Consistency.ABSOLUTE,
                                           0, null)));
        }

        /* At this point nothing should be running */
        assertTrue(source.getMigrationManager().isIdle());
        assertTrue(target.getMigrationManager().awaitTargetIdle(null));

        final ReadHook readHook = new ReadHook();
        source.getMigrationManager().setReadHook(readHook);

        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(p1, rg1).
                         getPartitionMigrationState());

        waitForMigrationState(target, p1, PartitionMigrationState.RUNNING);

        /* Send some records */
        for (int i = 0; i < 100; i++) {
            readHook.waitForHook();
            readHook.releaseHook();
        }
        readHook.waitForHook();

        /* Now the source and target should be busy */
        assertFalse(source.getMigrationManager().isIdle());
        assertFalse(target.getMigrationManager().awaitTargetIdle(null));

        /*
         * Delete the remaining records. Note that this segment is sensitive to
         * the test parameters. That is because the migration will have a
         * record locked while it is paused in the hook. If the record is one
         * being deleted, the operation will fail with a timeout.
         */
        for (int i = 0; i < keys.length/2; i++) {
            final Row row = table.createRow();
            row.put("id", i+100);
            row.put("firstName", "record" + i);
            tableAPI.delete(row.createPrimaryKey(), null, null);
        }
        /* Clear the hook so that the migration completes */
        source.getMigrationManager().setReadHook(null);
        readHook.releaseAll();
        waitForMigrationState(target, p1, PartitionMigrationState.SUCCEEDED);

        /* The source should still be busy but the target should be clear */
        assertFalse(source.getMigrationManager().isIdle());
        assertTrue(target.getMigrationManager().awaitTargetIdle(null));

        final Topology newTopo = config.getTopology().getCopy();
        newTopo.updatePartition(p1, new RepGroupId(
                                         target.getRepNodeId().getGroupId()));

        source.updateMetadata(newTopo);
        target.updateMetadata(newTopo);

        /* The source should now be clear */
        assertTrue(source.getMigrationManager().isIdle());

        /* Tombstones should be there for all of the deleted records */
        int missing = 0;
        for (int i = 0; i < keys.length; i++) {
            if (!isTombstone(kvs.get(keys[i], Consistency.ABSOLUTE, 0, null))) {
                missing++;
            }
        }
        assertEquals("Missing " + missing + " tombstones", 0, missing);
    }

    /*
     * Returns true if the value is a tombstone. The check assumes that the
     * non-tombstone records have a non-zero length.
     */
    private boolean isTombstone(ValueVersion vv) {
        if (vv == null) {
            return false;
        }
        final Value value = vv.getValue();
        return value.getFormat() == Format.MULTI_REGION_TABLE &&
               Region.isMultiRegionId(value.getRegionId()) &&
               value.getValue().length == 0;
    }

    /**
     * Test that a table iteration using the async API notices when a partition
     * migration happens during the iteration and responds quickly rather than
     * waiting for the operation to timeout. Unfortunately, although this case
     * seems similar to the one in the bug report, it doesn't fail prior to the
     * associated fixes. Leave it in because it provides some coverage for the
     * issue. [#27912]
     */
    @Test
    public void testAsyncShardIteration() throws Exception {
        assumeTrue("Only when testing async", AsyncControl.serverUseAsync);

        config.startRepNodeServices();

        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);

        kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI apiImpl = kvs.getTableAPI();

        TableMetadata md = new TableMetadata(false);

        final TableImpl userTable = TableBuilder.createTableBuilder("User")
                                                .addInteger("id")
                                                .addString("firstName")
                                                .addString("lastName")
                                                .addInteger("age")
                                                .primaryKey("id")
                                                .shardKey("id")
                                                .buildTable();

        TableImpl table = md.addTable(userTable.getInternalNamespace(),
                                      userTable.getName(),
                                      userTable.getParentName(),
                                      userTable.getPrimaryKey(),
                                      null,
                                      userTable.getShardKey(),
                                      userTable.getFieldMap(),
                                      null, null,
                                      false, 0,
                                      null,
                                      null /* owner */);

        /* Create an index */
        md.addIndex(null, "FirstName", table.getFullName(),
                    makeIndexList("firstName"), null, true, false,
                    null);

        updateMetadata(source, md);
        updateMetadata(target, md);

        /*
         * Note that we are assuming that entries 1000-5000 will contain items
         * in migrated partition, but seems like it is sufficiently likely
         */
        addRows(table, 5000, apiImpl);

        final Index index =
            md.getIndex(null, table.getFullName(), "FirstName");
        final IndexKey ikey = index.createIndexKey();

        final Publisher<Row> publisher = apiImpl.tableIteratorAsync(
            ikey, null,
            new TableIteratorOptions(Direction.FORWARD, null,
                                     30, SECONDS,       /* timeout */
                                     1,                 /* max concurrency */
                                     10));              /* batch size */
        final AtomicInteger iterCount = new AtomicInteger();
        final CompletableFuture<Boolean> done = new CompletableFuture<>();

        publisher.subscribe(new Subscriber<Row>() {
            Subscription subscription;
            @Override
            public void onSubscribe(Subscription s) {
                subscription = s;
                subscription.request(1001);
            }
            @Override
            public void onNext(Row row) {
                final int count = iterCount.incrementAndGet();
                if (count == 1000) {
                    new Thread() {
                        {
                            setUncaughtExceptionHandler(
                                PartitionMigrationTest.this);
                        }
                        @Override
                        public void run() {
                            assertEquals(PartitionMigrationState.PENDING,
                                         target.migratePartition(p1, rg1).
                                         getPartitionMigrationState());
                            waitForMigrationState(
                                target, p1, PartitionMigrationState.SUCCEEDED);
                            subscription.request(4001);
                        }
                    }.start();
                } else if (count == 5000) {
                    done.complete(true);
                }
            }
            @Override
            public void onComplete() {
                done.completeExceptionally(
                    new RuntimeException("Unexpected onComplete call"));
            }
            @Override
            public void onError(Throwable t) {
                logger.log(Level.INFO, "Got exception: " + t, t);
                done.completeExceptionally(t);
            }
        });
        final long startGet = System.currentTimeMillis();
        try {
            done.get(30, SECONDS);
            fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (!(cause instanceof StoreIteratorException)) {
                throw e;
            }
            final long waitTime =
                (System.currentTimeMillis() - startGet) / 1000;
            final Throwable cause2 = cause.getCause();
            assertTrue("StoreIteratorException cause should be" +
                       " UnsupportedOperationException or" +
                       " IllegalStateException, found: " +
                       cause2,
                       ((cause2 instanceof UnsupportedOperationException) ||
                        (cause2 instanceof IllegalStateException)));
            assertTrue("Wait time should be no more than 5 seconds, found: " +
                       waitTime,
                       waitTime <= 5);
        }
    }

    /**
     * Base class for a hook which waits in the doHook() method, effectively
     * implementing a breakpoint.
     *
     * @param <T>
     */
    public static abstract class BaseHook<T> implements TestHook<T> {
        private boolean waiting = false;

        /**
         * Waits for a thread to wait on the hook. Note that this may not
         * operate properly when multiple threads wait on the same hook.
         */
        void waitForHook() {
            if (waiting) {
                return;
            }

            boolean success = new PollCondition(10, 20000) {

                @Override
                protected boolean condition() {
                    return waiting;
                }
            }.await();
            assert(success);
        }

        /**
         * Releases a single thread waiting on the hook. This method checks
         * to make sure someone is waiting and throws an assert if not.
         */
        synchronized void releaseHook() {
            assert waiting;
            waiting = false;
            notify();
        }

        /**
         * Releases everyone waiting on the hook.
         */
        synchronized void releaseAll() {
            notifyAll();
        }

        /*
         * Will wait until release*() is called. Also releases anyone waiting
         * in waitForHook().
         */
        synchronized void waitForRelease() {
            waiting = true;
            try {
                wait();
            } catch (InterruptedException ex) {
                throw new AssertionError("hook wait interripted");
            }
        }
    }

    /**
     * Hook for allowing the test pause the source and to single step
     * sending DB records to the target.
     */
    static class ReadHook extends BaseHook<DatabaseEntry> {
        private DatabaseEntry lastKey = null;
        private RuntimeException exception = null;

        /**
         * Causes the hook to release from wait and immediately throws
         * the specified exception.
         *
         * @param re runtime exception to throw
         */
        void throwException(RuntimeException re) {
            exception = re;
            releaseHook();
        }

        /* -- From TestHook -- */

        @Override
        public void doHook(DatabaseEntry key) {
            waitForRelease();
            lastKey = key;

            if (exception != null) {
                throw exception;
            }
        }

        @Override
        public String toString() {
            return "ReadHook[" +
                   ((lastKey == null) ? "null" :
                                        new String(lastKey.getData())) + "]";
        }
    }

    /**
     * Hook to inject error responses from the source to the target.
     */
    private static class ResponseHook
                                extends BaseHook<AtomicReference<Response>> {
        private Response response;

        /**
         * Causes the hook to be released and will set the specified response.
         *
         * @param resp
         */
        void sendResponse(Response resp) {
            this.response = resp;
            releaseHook();
        }

        @Override
        public void doHook(AtomicReference<Response> arg) {
            waitForRelease();
            if (response != null) {
                arg.set(response);
            }
        }
    }

    /**
     * Waits for one of the specified nodes to become a master and returns it.
     */
    private RepNode waitForMaster(final RepNode ... nodes) {
        final List<RepNode> master = new ArrayList<>();

        boolean success = new PollCondition(500, 20000) {

            @Override
            protected boolean condition() {

                for (RepNode node : nodes) {
                    if (node.getEnv(1).getState().isMaster()) {
                        master.add(node);
                        return true;
                    }
                }
                return false;
            }
        }.await();
        assert(success);
        return master.get(0);
    }

    /**
     * Tests that the wait past method for partition update works correctly.
     */
    @Test
    public void testPartitionUpdateWait() throws Exception {
        config.startRepNodeServices();
        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);
        final Key k1 = new KeyGenerator(source).getKeys(1)[0];
        final PartitionId k1p = source.getPartitionId(k1.toByteArray());
        logger.info(String.format("target: %s", target.getRepNodeId()));
        /* Installs the hook so that the update will be delayed. */
        tearDowns.add(() -> PartitionManager.updateTopoTestHook = null);
        final CountDownLatch latch = new CountDownLatch(1);
        PartitionManager.updateTopoTestHook = (obj) -> {
            logger.info(String.format(
                "Test hook: %s will be blocked", obj));
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            logger.info(String.format(
                "Test hook: %s will proceed", obj));
        };
        /* Migrate the partition. */
        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(k1p, rg1).
                         getPartitionMigrationState());
        waitForMigrationState(target, k1p, PartitionMigrationState.SUCCEEDED);
        /* Try wait past the localized topology, but should timeout. */
        final PartitionManager targetPM = target.getPartitionManager();
        final Topology localizedTopo = target.getLocalTopology();
        logger.info(
            String.format(
                "Localized topology: %s",
                localizedTopo.getOrderNumberTuple()));
        Topology targetUpdatedTopo;
        targetUpdatedTopo = targetPM.awaitUpdate(localizedTopo, 1000);
        assertTrue(targetUpdatedTopo == null);
        /* Count down the latch and try again. */
        latch.countDown();
        targetUpdatedTopo = targetPM.awaitUpdate(localizedTopo, 1000);
        assertTopoNumbersEqualOrAfter(localizedTopo, targetUpdatedTopo);
    }

    private void assertTopoNumbersEqualOrAfter(Topology expected,
                                               Topology actual) {
        assertTrue(expected.getSequenceNumber() <= actual.getSequenceNumber());
        if (expected.getSequenceNumber() == actual.getSequenceNumber()) {
            assertTrue(expected.getLocalizationNumber()
                       <= actual.getLocalizationNumber());
        }
    }

    /**
     * Tests that the partition update cannot happen out-of-order. Simulates
     * that even if an upper layer bug happened that
     * PartitionManager#updateDbHandles is called out-of-order will not result
     * in partition data structures being updated out-of-order.
     */
    @Test
    public void testPartitionUpdateOrder() throws Exception {
        config.startRepNodeServices();
        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);
        final Key k1 = new KeyGenerator(source).getKeys(1)[0];
        final PartitionId k1p = source.getPartitionId(k1.toByteArray());
        logger.info(String.format("target: %s", target.getRepNodeId()));
        /* Migrate the partition. */
        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(k1p, rg1).
                         getPartitionMigrationState());
        waitForMigrationState(target, k1p, PartitionMigrationState.SUCCEEDED);
        Topology targetUpdatedTopo;
        final PartitionManager targetPM = target.getPartitionManager();
        final Topology localizedTopo = target.getLocalTopology();
        logger.info(
            String.format(
                "Localized topology: %s",
                localizedTopo.getOrderNumberTuple()));
        targetUpdatedTopo = targetPM.awaitUpdate(localizedTopo, 1000);
        assertTopoNumbersEqualOrAfter(localizedTopo, targetUpdatedTopo);
        /*
         * Call the updateDbHandles method to invoke a update of lower
         * localization number topo.
         */
        final Topology oldTopo = config.getTopology().getCopy();
        targetPM.updateDbHandles(oldTopo);
        targetUpdatedTopo =
            targetPM.awaitUpdate(oldTopo, 1000);
        assertTopoNumbersEqualOrAfter(localizedTopo, targetUpdatedTopo);
        /*
         * Update the topo to the official new topo and call the
         * updateDbHandles again with the previous localized one to invoke a
         * update of lower sequence number topo.
         */
        final Topology newTopo = config.getTopology().getCopy();
        newTopo.updatePartition(
            k1p, new RepGroupId(target.getRepNodeId().getGroupId()));
        target.updateMetadata(newTopo);
        targetPM.updateDbHandles(localizedTopo);
        targetUpdatedTopo =
            targetPM.awaitUpdate(localizedTopo, 1000);
        assertTopoNumbersEqualOrAfter(newTopo, targetUpdatedTopo);
    }

    /**
     * Tests the interaction between the partition update and the lagging
     * replica.
     */
    @Test
    public void testPartitionUpdateWithLaggingReplica() throws Exception {
        config.startRepNodeServices();
        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);
        final Key k1 = new KeyGenerator(source).getKeys(1)[0];
        final PartitionId k1p = source.getPartitionId(k1.toByteArray());
        /* Writes an initial value. */
        final Value v1 = Value.createValue("Initial value".getBytes());
        kvs = KVStoreFactory.getStore(config.getKVSConfig());
        assertNotNull(kvs.put(k1, v1));
        logger.info(String.format("target: %s", target.getRepNodeId()));
        /* Blocks the replication before migration. */
        final CountDownLatch latch = new CountDownLatch(1);
        final RepNodeId laggingRNId = new RepNodeId(2, 2);
        final RepNode laggingRN = config.getRN(laggingRNId);
        blockFeeder(target, laggingRN,
                    impl -> impl.getName().equals(k1p.getPartitionName()),
                    latch);
        /* Do migration. */
        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(k1p, rg1).
                         getPartitionMigrationState());
        waitForMigrationState(target, k1p, PartitionMigrationState.SUCCEEDED);
        /* Update lagging RN topology so that new partition can be opened */
        final Topology newTopo = config.getTopology().getCopy();
        newTopo.updatePartition(
            k1p, new RepGroupId(target.getRepNodeId().getGroupId()));
        laggingRN.getTopologyManager().update(newTopo);
        waitForPartition(laggingRN, k1p, true);
        /* Try wait past the localized topology, but should timeout. */
        final PartitionManager lagginPM = laggingRN.getPartitionManager();
        final Topology localizedTopo = laggingRN.getLocalTopology();
        logger.info(
            String.format(
                "Localized topology: %s",
                localizedTopo.getOrderNumberTuple()));
        Topology targetUpdatedTopo;
        targetUpdatedTopo = lagginPM.awaitUpdate(localizedTopo, 1000);
        assertTrue(targetUpdatedTopo == null);
        /* Count down the latch and try again. */
        logger.info(String.format("Unblock %s replication", laggingRNId));
        latch.countDown();
        targetUpdatedTopo = lagginPM.awaitUpdate(
            localizedTopo,
            /*
             * Wait timeout set to larger than
             * PartitionManager.DB_UPDATE_RETRY_MS.
             */
            2000);
        assertTopoNumbersEqualOrAfter(localizedTopo, targetUpdatedTopo);
    }
}
