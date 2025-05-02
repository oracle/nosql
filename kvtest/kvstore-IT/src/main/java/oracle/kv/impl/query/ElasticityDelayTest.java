/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.MathContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import oracle.kv.Consistency;
import oracle.kv.KVStoreFactory;
import oracle.kv.StatementResult;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.ops.TableQuery;
import oracle.kv.impl.api.query.PreparedStatementImpl.DistributionKind;
import oracle.kv.impl.api.table.FieldDefFactory;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.StringValueImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.fault.WrappedClientException;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.query.compiler.ExprConst;
import oracle.kv.impl.query.compiler.QueryControlBlock;
import oracle.kv.impl.query.runtime.ConstIter;
import oracle.kv.impl.query.runtime.ResumeInfo;
import oracle.kv.impl.query.runtime.RuntimeControlBlock;
import oracle.kv.impl.rep.MigrationReadConsistencyTestBase;
import oracle.kv.impl.rep.PartitionManager;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.admin.RepNodeAdmin.PartitionMigrationState;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

import org.junit.Test;

/**
 * Tests the correctness of query scans under scenarios where certain routines
 * got delayed.
 *
 * [KVSTORE-1518]
 */
public class ElasticityDelayTest extends MigrationReadConsistencyTestBase {

    private static final String INDEX = "Index";
    private static final String USER = "User";
    private static final String USER_ID = "Id";
    private static final String USER_NAME = "Name";
    private static final String USER_AGE = "Age";
    private static final TableImpl USER_TABLE =
        TableBuilder.createTableBuilder(USER)
        .addInteger(USER_ID)
        .addString(USER_NAME)
        .addInteger(USER_AGE)
        .primaryKey(USER_ID)
        .shardKey(USER_ID)
        .buildTable();
    private static final String NAME_QUERY =
        String.format("select * from %s where Name = \"name1\"", USER);
    private static final String AGE_QUERY =
        String.format("select * from %s where Age >= 50", USER);

    /**
     * Tests normal scan with time consistency after a partition migration
     * on a lagging replica that hasn't received all data from migrating
     * partition but has PGT ready and partition database open.
     */
    @Test
    public void testNormalTimeConsistencyWithLaggingMigration()
        throws Exception {

        prepareScanTest();
        final PartitionId p1 = new PartitionId(1);

        /*
         * Set a filter to block the replication stream once
         * master detects the data from k1p partition.
         */
        waitLatch = new CountDownLatch(1);
        blockFeeder(target, laggingRN,
                    impl -> impl.getName().equals(p1.getPartitionName()));

        /* Migrate partition */
        migratePartition(p1);

        /* Update lagging RN topology so that new partition can be opened */
        final Topology newTopo = config.getTopology().getCopy();
        newTopo.updatePartition(
            p1, new RepGroupId(target.getRepNodeId().getGroupId()));
        laggingRN.getTopologyManager().update(newTopo);
        waitForPartition(laggingRN, p1, true);

        /*
         * Obtains the laggingRN topology so that we can set that as the base.
         */
        final int baseTopoNum = laggingRN.getTopology().getSequenceNumber();

        /*
         * Partition generation is not opened on lagging RN, the request should
         * time out
         */
        try {
            final Request scanRequest = makeNormalScanRequest(baseTopoNum);
            config.getRH(laggingRNId).execute(scanRequest);
            fail("Request should fail due to timeout");
        } catch (RNUnavailableException e) {
            assertTrue(
                e.getMessage(),
                e.getMessage()
                .contains("Wait timeout at replica for server state updated"));
        }

        /* Unblock the stream and wait for partition generation to open */
        waitLatch.countDown();
        waitForPartitionGenOpen(laggingRN, p1);

        /* The lagging RN now is able to process */
        final Request scanRequest = makeNormalScanRequest(baseTopoNum);
        config.getRH(laggingRNId).execute(scanRequest);
    }

    /**
     * Prepares for a scan test. Create table and indices and put rows.
     */
    private void prepareScanTest() {
        config.startRepNodeServices();

        source = config.getRN(sourceId);
        target = config.getRN(targetId);
        laggingRN = config.getRN(laggingRNId);

        kvs = KVStoreFactory.getStore(config.getKVSConfig());

        createTableAndIndex();
        populateRows(100);
    }

    private void createTableAndIndex() {
        final TableMetadata md =
            (TableMetadata) source.getMetadata(MetadataType.TABLE).getCopy();
        md.addTable(USER_TABLE.getInternalNamespace(),
                    USER_TABLE.getName(),
                    USER_TABLE.getParentName(),
                    USER_TABLE.getPrimaryKey(),
                    null, // primaryKeySizes
                    USER_TABLE.getShardKey(),
                    USER_TABLE.getFieldMap(),
                    null, // TTL
                    null, // limits
                    false, 0,
                    null, null);
        md.addIndex(null, INDEX,
                    USER_TABLE.getFullName(),
                    new ArrayList<>(Arrays.asList(USER_NAME, USER_AGE)),
                    null, true, false, null);
        boolean success;
        success = source.updateMetadata(md);
        assertTrue(success);
        success = target.updateMetadata(md);
        assertTrue(success);
        success = PollCondition.await(
            1000 /* check interval */,
            5000 /* timeout */,
            () ->
            source.addIndexComplete(null, INDEX, USER_TABLE.getFullName()));
        assertTrue(success);
        success = PollCondition.await(
            1000 /* check interval */,
            5000 /* timeout */,
            () ->
            target.addIndexComplete(null, INDEX, USER_TABLE.getFullName()));
        assertTrue(success);
    }

    private void populateRows(int numRows) {
        final TableAPI tableAPI = kvs.getTableAPI();
        final Table table = tableAPI.getTable(USER_TABLE.getFullName());
        for (int i = 0; i < numRows; ++i) {
            final Row row = table.createRow();
            row.put(USER_ID, i);
            row.put(USER_NAME, "name" + i);
            row.put(USER_AGE, i);
            tableAPI.put(row, null, null);
        }
    }

    private Request makeNormalScanRequest(int baseTopoNum) {
        return makeScanRequest(baseTopoNum, -1);
    }

    private Request makeScanRequest(int baseTopoNum,
                                    int virtualScanPid) {
        final ExecuteOptions options = new ExecuteOptions();
        final Consistency consistency =
            new Consistency.Time(20, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);
        options.setConsistency(consistency);
        final QueryControlBlock qcb =
            new QueryControlBlock(
                (TableAPIImpl) kvs.getTableAPI(),
                options,
                NAME_QUERY.toCharArray(),
                null, /* sctx */
                null, /* namespace */
                null /* prepareCallback */);
        qcb.compile();
        final ResumeInfo resumeInfo =
            new ResumeInfo((RuntimeControlBlock) null);
        resumeInfo.setBaseTopoNum(baseTopoNum);
        if (virtualScanPid > 0) {
            resumeInfo.setVirtualScanPid(virtualScanPid);
        }
        final TableQuery op = new TableQuery(
            "query",
            DistributionKind.ALL_SHARDS,
            FieldDefFactory.createStringDef(),
            true /* may return null */,
            new ConstIter(
                new ExprConst(
                    qcb,
                    null, /* sctx */
                    new QueryException.Location(
                        1, /* startLine */
                        2, /* startColumn */
                        3, /* endLine */
                        4 /* endColumn */),
                    new StringValueImpl("name1")),
                1, /* resultReg */
                new StringValueImpl("name1"),
                false /* forCloud */),
            new FieldValueImpl[] { new StringValueImpl("name1") },
            10, /* numIterators */
            10, /* numRegisters */
            1, /* tableId */
            MathContext.UNLIMITED,
            (byte) 5, /* traceLevel */
            true, /* doLogFileTracing*/
            100, /* batchSize */
            10, /* maxReadKB */
            10, /* currentMaxReadKB */
            10, /* currentMaxWriteKB */
            resumeInfo,
            10, /* emptyReadFactor */
            11, /* deleteLimit */
            10, /* updateLimit */
            0, /* localRegionId */
            true, /* localRegionId*/
            10000 /*maxServerMemoryConsumption*/,
            false /* performsWrite*/);

        return ((KVStoreImpl)kvs)
            .makeReadRequest(op, PartitionId.NULL_ID, consistency, 5000);
    }

    /**
     * Tests virtual scan with time consistency after a partition migration on
     * a lagging replica that hasn't received all data from migrating partition
     * but has PGT ready and partition database open.
     */
    @Test
    public void testVirtualTimeConsistencyWithLaggingMigration()
        throws Exception {

        prepareScanTest();
        final PartitionId p1 = new PartitionId(1);

        /*
         * Set a filter to block the replication stream once
         * master detects the data from k1p partition.
         */
        waitLatch = new CountDownLatch(1);
        blockFeeder(target, laggingRN,
                    impl -> impl.getName().equals(p1.getPartitionName()));

        /* Migrate partition */
        migratePartition(p1);

        /* Update lagging RN topology so that new partition can be opened */
        final Topology newTopo = config.getTopology().getCopy();
        newTopo.updatePartition(
            p1, new RepGroupId(target.getRepNodeId().getGroupId()));
        laggingRN.getTopologyManager().update(newTopo);
        waitForPartition(laggingRN, p1, true);

        /*
         * Obtains the laggingRN topology so that we can set that as the base.
         */
        final int baseTopoNum = laggingRN.getTopology().getSequenceNumber();

        /*
         * Partition generation is not opened on lagging RN, the request should
         * time out
         */
        try {
            final Request scanRequest =
                makeScanRequest(baseTopoNum, p1.getPartitionId());
            config.getRH(laggingRNId).execute(scanRequest);
            fail("Request should fail due to timeout");
        } catch (RNUnavailableException e) {
            assertTrue(
                e.getMessage(),
                e.getMessage()
                .contains("PARTITION-1 was supposed to have migrated"));
        }

        /* Unblock the stream and wait for partition generation to open */
        waitLatch.countDown();
        waitForPartitionGenOpen(laggingRN, p1);

        /* The lagging RN now is able to process */
        final Request scanRequest =
            makeScanRequest(baseTopoNum, p1.getPartitionId());
        config.getRH(laggingRNId).execute(scanRequest);
    }

    /**
     * Tests secondary scan with the new base sequence number of the migration
     * will not proceed until update of the new official topology is successful
     * on the target.
     */
    @Test
    public void testDelayedOfficialTopologyUpdate() throws Exception {
        prepareScanTest();
        final PartitionId p1 = new PartitionId(1);

        /*
         * Block the update of target with the localized topology of the
         * partition migration.
         */
        final Topology topo = target.getTopology().getCopy();
        topo.updatePartition(p1, new RepGroupId(targetId.getGroupId()));
        final int baseTopoNum = topo.getSequenceNumber();
        waitLatch = new CountDownLatch(1);
        blockUpdate(targetId, baseTopoNum);

        /* Migrate partition */
        migratePartition(p1);

        /* Broadcast the new topology to the target. */
        target.updateMetadata(topo);

        /*
         * The request should fail.
         */
        try {
            final Request scanRequest = makeNormalScanRequest(baseTopoNum);
            config.getRH(targetId).execute(scanRequest);
            fail("Request should fail due to timeout");
        } catch (WrappedClientException e) {
            assertTrue(
                e.getMessage(),
                e.getMessage()
                .contains("Wait timeout at master for server state updated"));
        }

        /* Unblock the update and the next request should succeed. */
        waitLatch.countDown();
        final Request scanRequest = makeNormalScanRequest(baseTopoNum);
        config.getRH(targetId).execute(scanRequest);
    }

    private void blockUpdate(RepNodeId expectedId, int seqNum) {
        PartitionManager.updateTopoTestHook = (obj) -> {
            final RepNode repNode = obj.repNode;
            final Topology topo = obj.topology;
            final long localizationNum = topo.getLocalizationNumber();
            repNode.getLogger()
                .info(
                    () -> String.format(
                        "Updating for topology seqNo=%s, localizationNum=%s",
                        topo.getSequenceNumber(),
                        localizationNum));
            if (!(repNode.getRepNodeId().equals(expectedId)
                  && (topo.getSequenceNumber() == seqNum))) {
                return;
            }
            repNode.getLogger()
                .info(
                    () -> String.format(
                        "Blocking partition manager update of "
                        + "topology seqNo=%s, localizationNum=%s",
                        seqNum, localizationNum));
            try {
                waitLatch.await();
            } catch (InterruptedException e) {

            }
            repNode.getLogger()
                .info(
                    () -> String.format(
                        "Unblocked partition manager update of "
                        + "topology seqNo=%s, localizationNum=%s",
                        seqNum, localizationNum));
        };
    }

    /**
     * Tests secondary scan will not proceed until update of the new localized
     * topology is successful on the target.
     */
    @Test
    public void testDelayedLocalTopologyUpdate() throws Exception {
        prepareScanTest();
        final PartitionId p1 = new PartitionId(1);

        /*
         * Block the update of target with the localized topology of the
         * partition migration.
         */
        final Topology topo = target.getTopology().getCopy();
        final int baseTopoNum = topo.getSequenceNumber();
        waitLatch = new CountDownLatch(1);
        logger.info(
            String.format(
                "Set to block localized topology with seqNo=%s",
                baseTopoNum));
        blockUpdate(targetId, baseTopoNum);

        /* Migrate partition */
        migratePartition(p1, false /* do not check partition */);

        /*
         * The request should fail.
         */
        try {
            final Request scanRequest =
                makeScanRequest(baseTopoNum, p1.getPartitionId());
            config.getRH(targetId).execute(scanRequest);
            fail("Request should fail due to timeout");
        } catch (WrappedClientException e) {
            assertTrue(
                e.getMessage(),
                e.getMessage()
                .contains("PARTITION-1 was supposed to have migrated"));
        }

        /* Unblock the update and the next request should succeed. */
        waitLatch.countDown();
        final Request scanRequest =
            makeScanRequest(baseTopoNum, p1.getPartitionId());
        config.getRH(targetId).execute(scanRequest);
        checkPartition(source, p1, false);
        checkPartition(target, p1, true);
    }

    /**
     * Tests query during RN shutting down.
     */
    @Test
    public void testQueryDuringRNShuttingDown() throws Exception {
        prepareScanTest();
        final Topology topo = config.getTopology();
        final int sourceGid = source.getRepNodeId().getGroupId();
        final int targetGid = target.getRepNodeId().getGroupId();
        for (PartitionId pid : topo.getPartitionsInShard(sourceGid, null)) {
            target.migratePartition(pid, new RepGroupId(sourceGid));
            waitForMigrationState(target, pid,
                PartitionMigrationState.SUCCEEDED);
        }
        for (int i = 1; i <= 3; ++i) {
            config.getRH(new RepNodeId(sourceGid, i)).stop();
        }
        final Topology newTopo = topo.getCopy();
        for (int i = 1; i <= 3; ++i) {
            newTopo.remove(new RepNodeId(sourceGid, i));
        }
        for (PartitionId pid : topo.getPartitionsInShard(sourceGid, null)) {
            newTopo.updatePartition(pid, new RepGroupId(targetGid));
        }
        for (int i = 1; i <= 3; ++i) {
            config.getRN(new RepNodeId(targetGid, i)).getTopologyManager()
                .update(newTopo);
        }
        final StatementResult result = kvs.executeSync(AGE_QUERY);
        final Iterator<RecordValue> iter = result.iterator();
        final AtomicInteger numRows = new AtomicInteger(0);
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final Thread iteratingThread = new Thread(() -> {
            try {
                while (iter.hasNext()) {
                    iter.next();
                    numRows.getAndIncrement();
                }
            } catch (Throwable t) {
                error.set(t);
            }
        });
        iteratingThread.start();
        /*
         * Sleep a bit so that the iteration has issued requests for each shard
         * stream of the BaseParallelScanIteratorImpl, then updates the
         * topology.
         */
        Thread.sleep(1000);
        ((KVStoreImpl) kvs).getDispatcher().getTopologyManager()
            .update(newTopo);
        iteratingThread.join(1000);
        assertEquals(null, error.get());
        assertEquals(50, numRows.get());
    }
}
