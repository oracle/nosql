/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static oracle.kv.impl.rep.table.ResourceCollector.CAP_TIME_SEC;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.rmi.RemoteException;

import oracle.kv.Consistency;
import oracle.kv.ReadThroughputException;
import oracle.kv.ThroughputLimitException;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.WriteOptions;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;

import oracle.nosql.common.ratelimit.SimpleRateLimiter;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class ThrottlingTest extends TableTestBase {

    private static final String TABLE_DDL = "create table users (" +
                                            "id integer, " +
                                            "name String, " +
                                            "friends array(String), " +
                                            "relatives map(String), " +
                                            "primary key(id))";
    private static final int RF = 1;
    private static final int N_NODES = 2;

    private static final ReadOptions ABS_CONST =
                            new ReadOptions(Consistency.ABSOLUTE, 0, null);
    private static final ReadOptions NO_CONST =
                            new ReadOptions(Consistency.NONE_REQUIRED, 0, null);

    /*
     * Row 1 is in shard 1 while row 2 is in shard 2.
     */
    private Row row1 = null;
    private Row row2 = null;

    @BeforeClass
    public static void staticSetUp() throws Exception {
        /* Create a 2x1 store */
        //TODO: remove this after MRTable is put on cloud.
        Assume.assumeFalse("Test should not run in MR table mode", mrTableMode);
        TableTestBase.staticSetUp(2 /* nSNs*/, RF, 1 /*capacity*/);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        createStore.stopAggregationService();
    }

    /**
     * Tests static throttling behavior. Tests throttling of a node when the
     * Aggregation Service is not present. Also tests switching from static
     * to dynamic and back.
     */
    @Test
    public void testStaticThrottling() throws Exception {

        final int readLimit = 200;
        final int writeLimit = 200;
        final TableLimits limits = new TableLimits(readLimit, writeLimit,
                                                   TableLimits.NO_LIMIT);
        createTable(TABLE_DDL, limits);

        final Table table = tableImpl.getTable("users");
        row1 = generateRowForShard(table, 1);
        row2 = generateRowForShard(table, 2);

        /* Without the AS we should be able to get the table limit on each node */
        doMultiNodeReads(table, (readLimit-10) * N_NODES, 4, false /*fail*/);

        doMultiNodeWrites(table, (writeLimit-10) * N_NODES, 4, false /*fail*/);

        /* Going over at a single node should fail */
        doReads(table, (readLimit+10), 3,false /*noCharge*/,  true /*fail*/);

        doWrites(table, (writeLimit+10), 3,false /*noCharge*/,  true /*fail*/);

        /* Setting noCharge should bypass tracking & checks */
        doReads(table, (readLimit+10), 3, true /*noCharge*/,  false /*fail*/);

        doWrites(table, (writeLimit+10), 3, true /*noCharge*/,  false /*fail*/);

        /* Bank seconds must be >= 2x poll period */
        final int bankSec = 8;
        final int pollPeriod = 3;
        createStore.startAggregationService(bankSec, pollPeriod, true /*wait*/);

        /*
         * The AS starts with full banks. Once consumed we can't maintain
         * the high rate.
         */
        doMultiNodeReads(table, (readLimit-10) * N_NODES,
                         bankSec + pollPeriod, true /*fail*/);

        doMultiNodeWrites(table, (writeLimit-10) * N_NODES,
                          bankSec + pollPeriod, true /*fail*/);
        Thread.sleep(1000);  /* Clear the failure */

        createStore.stopAggregationService();

        /* Generate some activity to allow the collector to ramp up. */
        doReadWrites(table, 10, CAP_TIME_SEC * 4, false /*fail*/);

        /* Should be able to return to full limits */
        doMultiNodeReads(table, (readLimit-10) * N_NODES, 4, false /*fail*/);

        doMultiNodeWrites(table, (writeLimit-10) * N_NODES, 4, false /*fail*/);
    }

    @Test
    public void testDynamicThrottling() throws Exception {

        /* Bank seconds must be >= 2x poll period */
        final int bankSec = 8;
        final int pollPeriod = 3;
        createStore.startAggregationService(bankSec, pollPeriod, true /*wait*/);

        final int readLimit = 200;
        final int writeLimit = 200;
        final TableLimits limits = new TableLimits(readLimit, writeLimit,
                                                   TableLimits.NO_LIMIT);
        createTable(TABLE_DDL, limits);
        final Table table = tableImpl.getTable("users");
        row1 = generateRowForShard(table, 1);
        row2 = generateRowForShard(table, 2);

        /* --- reads --- */

        /* Consume bank */
        doMultiNodeReads(table, (readLimit-10) * 2, bankSec / 2, false /*fail*/);

        /* Bank gone, should fail */
        doMultiNodeReads(table, (readLimit-10) * 2, bankSec, true /*fail*/);
        Thread.sleep(1000);  /* Clear the failure */

        /* Generate some activity to allow the collector to ramp up. */
        doMultiNodeReads(table, 10, CAP_TIME_SEC * 4, false /*fail*/);

        /*
         * Should be able to consume close to the limit. Note that very little
         * will be leftover to bank.
         */
        doReads(table, (readLimit-10), pollPeriod * 3,
                false /*noCharge*/, false /*fail*/);

        /* There shouldn't be much bank */
        doMultiNodeReads(table, (readLimit+10), pollPeriod * 3, true /*fail*/);

        /* --- writes --- */

        /* Consume bank */
        doMultiNodeWrites(table, (writeLimit-10) * 2,
                          bankSec / 2, false /*fail*/);

        /* Bank gone, should now fail */
        doMultiNodeWrites(table, (writeLimit-10) * 2, bankSec, true /*fail*/);
        Thread.sleep(1000);  /* Clear the failure */

        /* Generate some activity to allow the collector to ramp up. */
        doMultiNodeWrites(table, 10, CAP_TIME_SEC * 4, false /*fail*/);

        /*
         * Should be able to consume the limit.
         */
        doWrites(table, (writeLimit-10), CAP_TIME_SEC * 2,
                 false /*noCharge*/, false /*fail*/);

        /* There shouldn't be much bank */
        doMultiNodeWrites(table, (writeLimit+10),
                          CAP_TIME_SEC * 2, true /*fail*/);
    }

    /*
     * Test that prepare operations are throttled
     */
    @Test
    public void testPrepareThrottling() throws Exception {

        final String query = "select * from users";
        final int readLimit = 10;
        final int writeLimit = 20;
        final TableLimits limits = new TableLimits(readLimit, writeLimit,
                                                   TableLimits.NO_LIMIT);
        createTable(TABLE_DDL, limits);

        /*
         * ReadThroughputException should be thrown
         */
        for (int i = 0; i < 5000; i++) {
            try {
                prepare(query);
            } catch (ReadThroughputException rte) {
                return;
            }
        }
        fail("Operation should have been throttled");
    }


    /* Methods to generates reads and writes at the specified KBs per second. */

    /* Performs equal reads and writes to a multiple nodes */
    private void doReadWrites(Table table, int KBSec,
                              int seconds, boolean fail) {
        doOps(table, KBSec, seconds, true, true, true, false, fail);
    }

    /* Performs reads to a single node */
    private void doReads(Table table, int KBSec, int seconds,
                         boolean noCharge, boolean fail) {
        doOps(table, KBSec, seconds, true, false, false, noCharge, fail);
    }

    /* Performs writes to a single node */
    private void doWrites(Table table, int KBSec, int seconds,
                          boolean noCharge, boolean fail) {
        doOps(table, KBSec, seconds, false, true, false, noCharge, fail);
    }

    /* Performs reads equally across both nodes */
    private void doMultiNodeReads(Table table, int KBSec,
                                  int seconds, boolean fail) {
        doOps(table, KBSec, seconds, true, false, true, false, fail);
    }

    /* Performs writes equally across both nodes */
    private void doMultiNodeWrites(Table table, int KBSec,
                                   int seconds, boolean fail) {
        doOps(table, KBSec, seconds, false, true, true, false, fail);
    }

    private void doOps(Table table,
                       int KBSec, int seconds,
                       boolean doReads,
                       boolean doWrites,
                       boolean multiNode,
                       boolean noCharge,
                       boolean fail) {
        assert doReads || doWrites;
        assert row1 != null;
        assert row2 != null;

        /* The writes are 2KB each so set the limiter to 1/2 the desired rate */
        final SimpleRateLimiter rateLimiter =
            new SimpleRateLimiter(KBSec/2);

        int totalReadKB = 0;
        int totalWriteKB = 0;

        final long startNanos = System.nanoTime();
        long endNanos = startNanos + SECONDS.toNanos(seconds);

        final ReadOptions ro = multiNode ? NO_CONST : ABS_CONST;
        ro.setNoCharge(noCharge);

        final WriteOptions wo;
        if (noCharge) {
            wo = new WriteOptions();
            wo.setNoCharge(true);
        } else {
            wo = null;
        }

        int i = 0;
        try {
            while (endNanos > System.nanoTime()) {
                rateLimiter.consumeUnits(1);

                final Row row;
                if (multiNode) {
                    row = (i++ & 0x1) == 0 ? row1 : row2;
                } else {
                    row = row1;
                }

                Result result;

                if (doWrites) {
                    result = tableImpl.putInternal((RowImpl)row, null, wo);
                    assertNotNull(result.getNewVersion());
                    totalWriteKB += result.getWriteKB();
                }

                if (doReads) {
                    final PrimaryKey pk = table.createPrimaryKey(row);

                    result = tableImpl.getInternal((PrimaryKeyImpl)pk, ro);
                    totalReadKB += result.getReadKB();

                    /*
                     * In multi node we use no consistency so the reads are
                     * only 1K. Therfore do two to equal the write KB.
                     */
                    if (multiNode) {
                        result = tableImpl.getInternal((PrimaryKeyImpl)pk, ro);
                        totalReadKB += result.getReadKB();
                    }
                }

            }
        } catch (ThroughputLimitException tle) {
            if (fail) {
                return;
            }
            final long afterSeconds =
                        NANOSECONDS.toSeconds(System.nanoTime() - startNanos);
            if (afterSeconds < 1) {
                fail("Unexpected throughput exception after generating " +
                     totalReadKB + " read KB and " +
                     totalWriteKB + " write KB: " + tle.getMessage());
            } else {
                fail("Unexpected throughput exception after generating " +
                     totalReadKB/afterSeconds + " read KB/sec and " +
                     totalWriteKB/afterSeconds + " write KB/sec after " +
                     afterSeconds + " seconds: " + tle.getMessage());
            }
        }
        if (fail) {
            fail("Expected throughput exception after generating " +
                 totalReadKB/seconds + " read KB/sec and " +
                 totalWriteKB/seconds + " write KB/sec for " +
                 seconds + " seconds");
        }
    }

    /*
     * Generates a row for the specified shard.
     * Note that the shard that the row is assigned is sensitive to table ID.
     * The ID may change due to system tables, or change in the test.
     */
    private Row generateRowForShard(Table table, int shard) {
        final Topology topo;
        try {
            topo = createStore.getAdmin().getTopology();
        } catch (RemoteException re) {
            fail("Exception getting topology: " + re.getMessage());
            return null;
        }
        /* Generate keys until we find one belonging to the target shard */
        for (int i = 0; i < 100; i++) {
            Row row = table.createRowFromJson(
                                         "{\"id\":" + i +
                                         ", \"name\": \"joe\"," +
                                         "\"friends\":[], \"relatives\":{}}",
                                         false);
            PrimaryKeyImpl pki = (PrimaryKeyImpl)table.createPrimaryKey(row);
            PartitionId pid = pki.getPartitionId((KVStoreImpl)store);
            RepGroupId rgId = topo.getRepGroupId(pid);
            if (rgId.getGroupId() == shard) {
                return row;
            }
        }
        fail("Unable to generate row for shard " + shard + "??");
        return null;
    }
}
