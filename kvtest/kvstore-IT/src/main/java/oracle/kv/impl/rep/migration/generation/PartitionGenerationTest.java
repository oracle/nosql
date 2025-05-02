/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep.migration.generation;

import static com.sleepycat.je.utilint.VLSN.FIRST_VLSN;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import oracle.kv.Durability;
import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.admin.RepNodeAdmin;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.KeyGenerator;
import oracle.kv.impl.util.PollCondition;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.utilint.VLSN;

import org.junit.Test;

/**
 * A collection of unit tests that test the partition generation table
 */
public class PartitionGenerationTest extends PartitionGenerationTestBase {

    @Override
    public void setUp() throws Exception {
        trace_on_screen = false;
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Test the serialization and de-serialization code of partition
     * generation which are used to read and write to JE database
     */
    @Test
    public void testPartGenSerialization() {

        final PartitionId pid = new PartitionId(100);
        final PartitionGenNum pgn = new PartitionGenNum(200);
        DatabaseEntry entry = PartitionGenDBManager.buildKey(pid, pgn);
        assertEquals("Mismatch partition id", pid,
                     PartitionGenDBManager
                         .readPartIdFromKey(entry.getData()));
        assertEquals("Mismatch partition generation number", pgn,
                     PartitionGenDBManager
                         .readGenNumFromKey(entry.getData()));

        final long startVLSN = 1024;
        final RepGroupId prevGroup = new RepGroupId(1);
        final long preVLSN = 2048;
        final PartitionGeneration pg = new PartitionGeneration(pid, pgn,
                                                               startVLSN,
                                                               prevGroup,
                                                               preVLSN);
        entry = PartitionGenDBManager.buildValue(pg);
        assertEquals("Mismatch partition generation", pg,
                     PartitionGenDBManager
                         .readPartGenFromVal(entry.getData()));

        /* test zero generation without previous group info */
        final PartitionGeneration pg1 = new PartitionGeneration(pid);
        entry = PartitionGenDBManager.buildValue(pg1);
        assertEquals("Mismatch partition generation", pg1,
                     PartitionGenDBManager
                         .readPartGenFromVal(entry.getData()));
    }

    /**
     * Tests to make sure that after store is created, no generation table is
     * ready since no migration
     */
    @Test
    public void testInitialization() {

        config.startRepNodeServices();

        RepNode master = config.getRN(rg1Master);
        final PartitionGenerationTable pgtRG1 =
            master.getPartitionManager().getPartGenTable();
        /* no migration, no PGT */
        assertFalse("Expect no generation table ", pgtRG1.isReady());

        master = config.getRN(rg2Master);
        final PartitionGenerationTable pgtRG2 =
            master.getPartitionManager().getPartGenTable();
        /* no migration, no PGT */
        assertFalse("Expect no generation table ", pgtRG2.isReady());

        /* no migration, no PGT */
        for (RepNode rn : config.getRNs()) {
            if (!rn.getRepNodeId().equals(rg1Master) &&
                !rn.getRepNodeId().equals(rg2Master)) {
                final PartitionGenerationTable pgt =
                    rn.getPartitionManager().getPartGenTable();

                assertFalse("Expect no generation table " + rn.getRepNodeId(),
                            pgt.isReady());
            }
        }
    }

    /**
     * Tests to migrate a single partition from rg1 to rg2
     */
    @Test
    public void testMoveSinglePart() {
        config.startRepNodeServices();

        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);

        migratePartition(p1, sourceId, targetId);

        /* verify generation table on source */
        trace("now verify generation table on the source");
        PartitionGenerationTable generationTable =
            source.getPartitionManager().getPartGenTable();
        assertTrue("Table must be available on src",
                   generationTable.isReady());
        trace(generationTable.dumpTable());

        /* check migration history of p1 */
        assertTrue("Table must contain P1", generationTable.hasPartition(p1));
        /* no admin P1 still open on source */
        assertTrue("Table should not have an open generation for P1",
                   generationTable.getLastGen(p1).isOpen());
        Collection<PartitionGeneration> hist = generationTable.getHistory(p1)
                                                              .values();
        assertEquals("Expect one generation", 1, hist.size());
        PartitionGeneration pg = hist.iterator().next();
        trace("Generation for=" + p1 + " is=" + pg);
        assertTrue("Expect open generation", pg.isOpen());
        assertEquals("Expect id match", p1, pg.getPartId());
        assertEquals("Start vlsn should be null", NULL_VLSN, pg.getStartVLSN());
        assertEquals("End vlsn should be less than max vlsn",
                     PartitionGeneration.OPEN_GEN_END_VLSN, pg.getEndVLSN());
        int[] pids = new int[]{1, 3, 5, 7, 9};
        for (int id : pids) {
            if (id != p1.getPartitionId()) {
                verifyInitPartGen(new PartitionId(id), generationTable);
            }
        }

        /* verify PG table on the target */
        trace("now verify generation table on the target");
        generationTable = target.getPartitionManager().getPartGenTable();
        assertTrue("Table must be available on target",
                   generationTable.isReady());
        /* check migration history of p1 */
        assertTrue("Table must contain P1", generationTable.hasPartition(p1));
        assertNotNull("Table should have an open generation for P1 after " +
                      "migration", generationTable.getOpenGen(p1));
        hist = generationTable.getHistory(p1).values();
        assertEquals("Expect one generation", 1, hist.size());
        pg = hist.iterator().next();

        assertTrue("Expect open generation", pg.isOpen());
        assertEquals("Expect id match", p1, pg.getPartId());
        assertEquals("Expect incremented generation #",
                     PartitionGenNum.generationZero().getNumber() + 1,
                     pg.getGenNum().getNumber());
        assertTrue("Start vlsn should be greater than 1",
                   pg.getStartVLSN() > FIRST_VLSN);
        assertEquals("End vlsn should be max vlsn since it is open",
                     PartitionGeneration.OPEN_GEN_END_VLSN, pg.getEndVLSN());
        /* verify prev generation info */
        assertEquals("Expect partition migrated from shard 1", 1,
                     pg.getPrevGenRepGroup().getGroupId());
        assertTrue("Expect a valid prev generation last vlsn",
                   pg.getPrevGenEndVLSN() <
                       PartitionGeneration.OPEN_GEN_END_VLSN);

        /* other partition on target should not be changed */
        pids = new int[]{2, 4, 6, 8, 10};
        for (int id : pids) {
            verifyInitPartGen(new PartitionId(id), generationTable);
        }

        /* verify PG table on target shard replicas */
        waitForPartGenOpen(targetId.getGroupId(), p1);
        assertPGTEquals(targetId.getGroupId(), generationTable);
    }

    /**
     * Tests to migrate p1 from rg1 to rg2 and migrate p2 from rg2 to rg1
     */
    @SuppressWarnings("hiding")
    @Test
    public void testSwitchTwoParts() {

        config.startRepNodeServices();

        final RepNode rg1Master = config.getRN(sourceId);
        final RepNode rg2Master = config.getRN(targetId);

        /* move p1 from rg1 to rg2 */
        migratePartition(p1, sourceId, targetId);

        /* move p2 from rg2 to rg1 */
        migratePartition(p2, targetId, sourceId);


        /* check P1 and P2 in the generation table */
        final PartitionGenerationTable rg1PGT =
            rg1Master.getPartitionManager().getPartGenTable();
        assertTrue("Table must be available on " + rg1Master.getRepNodeId(),
                   rg1PGT.isReady());
        final PartitionGenerationTable rg2PGT =
            rg2Master.getPartitionManager().getPartGenTable();
        assertTrue("Table must be available on " + rg2Master.getRepNodeId(),
                   rg2PGT.isReady());

        /* P1 should be open on rg2 */
        PartitionGeneration openGen = rg2PGT.getOpenGen(p1);
        assertNotNull("Expect generation at " +
                      rg2Master.getRepNodeId(), openGen);
        assertTrue("Expect open gen", openGen.isOpen());
        assertEquals("Expect PGN=1", 1, openGen.getGenNum().getNumber());
        assertTrue("Expect >1 start VLSN",
                   openGen.getStartVLSN() > FIRST_VLSN);
        assertEquals("Expect previous gen", 1,
                     openGen.getPrevGenRepGroup().getGroupId());
        assertTrue("Expect valid end vlsn of previous gen",
                   openGen.getPrevGenEndVLSN() <
                       PartitionGeneration.OPEN_GEN_END_VLSN);
        /* P1 should be closed on rg1 */
        Collection<PartitionGeneration> history = rg1PGT.getHistory(p1)
                                                        .values();
        assertEquals("Expect one generation at " +
                     rg1Master.getRepNodeId(), 1, history.size());
        PartitionGeneration closedGen = history.iterator().next();
        /* no admin, generation still open on source */
        assertTrue("Expect close gen at " + rg1Master.getRepNodeId(),
                   closedGen.isOpen());
        assertEquals("Expect valid end vlsn at " +
                     rg1Master.getRepNodeId(),
                     PartitionGeneration.OPEN_GEN_END_VLSN,
                     closedGen.getEndVLSN());

        /* P2 should be on rg1 */

        /* P2 should be open on rg1 */
        openGen = rg1PGT.getOpenGen(p2);
        assertNotNull("Expect generation at " +
                      rg1Master.getRepNodeId(), openGen);
        assertTrue("Expect open gen", openGen.isOpen());
        assertEquals("Expect PGN=1", 1, openGen.getGenNum().getNumber());
        assertTrue("Expect >1 start VLSN",
                   openGen.getStartVLSN() > FIRST_VLSN);
        assertEquals("Expect previous gen", 2,
                     openGen.getPrevGenRepGroup().getGroupId());
        assertTrue("Expect valid end vlsn of previous gen",
                   openGen.getPrevGenEndVLSN() <
                       PartitionGeneration.OPEN_GEN_END_VLSN);

        /* verify PG table on source shard replicas */
        waitForPartGenOpen(sourceId.getGroupId(), p2);

        /* no admin, no partition db drop, P2 is still open on rg2 */
        history = rg2PGT.getHistory(p2).values();
        assertEquals("Expect one generation at " +
                     rg2Master.getRepNodeId(), 1, history.size());
        closedGen = history.iterator().next();
        assertTrue("Expect open gen at " + rg2Master.getRepNodeId(),
                   closedGen.isOpen());
        assertEquals("Expect valid end vlsn at " +
                     rg2Master.getRepNodeId(),
                     PartitionGeneration.OPEN_GEN_END_VLSN,
                     closedGen.getEndVLSN());

        /* wait PG table is updated on replicas */
        waitForPartGenOpen(sourceId.getGroupId(), p2);
        waitForPartGenOpen(targetId.getGroupId(), p1);

        /* verify PG table on source shard replicas */
        assertPGTEquals(sourceId.getGroupId(), rg1PGT);
        assertPGTEquals(targetId.getGroupId(), rg2PGT);
    }

    /**
     * Tests to migrate multiple partitions from rg1 to rg2
     */
    @Test
    public void testMoveMultiPart() {
        config.startRepNodeServices();

        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);

        /* migrate multiple partitions */
        final PartitionId[] pids = new PartitionId[]{p1, p3, p5};
        for (PartitionId pid : pids) {
            migratePartition(pid, sourceId, targetId);
        }

        /* verify generation table on source */
        trace("now verify generation table on the source");
        PartitionGenerationTable pgt =
            source.getPartitionManager().getPartGenTable();
        assertTrue("Table must be available on src",
                   pgt.isReady());
        trace(pgt.dumpTable());
        /*
         * since no admin in the test env, the partition db would not be
         * removed and thus the generation remains open
         */
        for (PartitionId pid : pids) {
            assertTrue(pgt.getLastGen(pid).isOpen());
        }

        /* verify generation table on target */
        trace("now verify generation table on the target");
        pgt = target.getPartitionManager().getPartGenTable();
        trace(pgt.dumpTable());
        for (PartitionId pid : pids) {
            final PartitionGeneration pg = pgt.getOpenGen(pid);
            assertNotNull("Expect migrated partition open on target", pg);
            assertEquals("Expect PGN=1 but get " + pg.getGenNum().getNumber(),
                         1, pg.getGenNum().getNumber());
            assertTrue("Expect open generation", pg.isOpen());
            assertTrue("Expect >1 start vlsn",
                       pg.getStartVLSN() > FIRST_VLSN);
            assertFalse("Expect non-null prev generation shard",
                        pg.getPrevGenRepGroup().isNull());
            assertTrue("Expect valid prev generation end vlsn",
                       !VLSN.isNull(pg.getPrevGenEndVLSN()) &&
                       pg.getPrevGenEndVLSN() <
                           PartitionGeneration.OPEN_GEN_END_VLSN);

            /* verify PG table on target shard replicas */
            waitForPartGenOpen(targetId.getGroupId(), pid);
            assertPGTEquals(targetId.getGroupId(), pgt);
        }
    }

    /**
     * Tests canceling an ongoing migration, and ensure the generation number
     * of each partition does not change.
     */
    @Test
    public void testCancel() {
        config.startRepNodeServices();

        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);

        /*
         * This will cause the source to wait before sending
         * any data, keeping the migration from completing.
         */
        final ReadHook readHook = new ReadHook();
        source.getMigrationManager().setReadHook(readHook);
        assertEquals(RepNodeAdmin.PartitionMigrationState.PENDING,
                     target.migratePartition(p1, rg1).
                         getPartitionMigrationState());
        trace("migration waiting to start");
        waitForMigrationState(target, p1,
                              RepNodeAdmin.PartitionMigrationState.RUNNING);
        trace("migration running");

        waitForPGTReady(source);
        trace("PGT ready on " + source.getRepNodeId());
        waitForPGTReady(target);
        trace("PGT ready on " + target.getRepNodeId());

        /* Cancel should work */
        assertEquals(RepNodeAdmin.PartitionMigrationState.ERROR,
                     target.canCancel(p1).getPartitionMigrationState());
        /* When canceled the state should report an error */
        waitForMigrationState(target, p1,
                              RepNodeAdmin.PartitionMigrationState.ERROR);
        /* Let the source thread continue */
        source.getMigrationManager().setReadHook(null);
        readHook.releaseAll();
        trace("hook released");
        /* Make sure the partition DB is still there. */
        final Database db = source.getEnv(1).openDatabase(
            null, p1.getPartitionName(),
            source.getPartitionDbConfig().setAllowCreate(false));
        db.close();

        /* verify on source, nothing changed */
        PartitionGenerationTable pgt =
            source.getPartitionManager().getPartGenTable();
        assertTrue("Table must be available on master, table: " +
                   pgt.dumpTable(), pgt.isReady());
        trace(pgt.dumpTable());
        int[] pids = new int[]{1, 3, 5, 7, 9};
        for (int id : pids) {
            verifyInitPartGen(new PartitionId(id), pgt);
        }

        /* verify PG table on source shard replicas */
        assertPGTEquals(sourceId.getGroupId(), pgt);

        /* verify on target, nothing changed */
        pgt = target.getPartitionManager().getPartGenTable();
        assertTrue("Table must be available on master, table: " +
                   pgt.dumpTable(), pgt.isReady());
        trace(pgt.dumpTable());
        pids = new int[]{2, 4, 6, 8, 10};
        for (int id : pids) {
            verifyInitPartGen(new PartitionId(id), pgt);
        }

        /* verify PG table on target shard replicas */
        assertPGTEquals(targetId.getGroupId(), pgt);
    }

    /**
     * Tests that after source restart, the migration of P1 will continue and
     * generation table is correct after migration is done.
     */
    @SuppressWarnings("null")
    @Test
    public void testSourceFailover() {
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
        final ReadHook readHook = new ReadHook();
        source.getMigrationManager().setReadHook(readHook);
        assertEquals(RepNodeAdmin.PartitionMigrationState.PENDING,
                     target.migratePartition(p1, rg1).
                         getPartitionMigrationState());
        waitForMigrationState(target, p1,
                              RepNodeAdmin.PartitionMigrationState.RUNNING);
        readHook.waitForHook();
        trace("migration held");

        readHook.throwException(
            new RuntimeException("Exception injected by unit test"));
        trace("Set a bomb at source");

        /* After the source fails, the target should return to PENDING state */
        waitForMigrationState(target, p1,
                              RepNodeAdmin.PartitionMigrationState.PENDING);

        readHook.releaseAll();
        trace("Let bomb blow up to close the channel from source");
        source.getMigrationManager().setReadHook(null);
        trace("Bomb removed, target should retry migration and succeed");

        /* Eventually the migration should resume and complete */
        waitForMigrationState(target, p1,
                              RepNodeAdmin.PartitionMigrationState.SUCCEEDED);

        trace("migration done");

        /* verify target PGT */
        RepNode master = config.getRN(rg2Master);
        PartitionGenerationTable pgt = master.getPartitionManager()
                                             .getPartGenTable();
        assertTrue("Table must be available on master, table: " +
                   pgt.dumpTable(), pgt.isReady());
        trace(pgt.dumpTable());
        /* check migration record of p1 */
        assertTrue("Table must contain P1", pgt.hasPartition(p1));
        assertNotNull("Table should have an open generation for P1 after " +
                      "migration", pgt.getOpenGen(p1));
        Collection<PartitionGeneration> hist = pgt.getHistory(p1).values();
        assertEquals("Expect one generation", 1, hist.size());
        PartitionGeneration pg = hist.iterator().next();
        assertTrue("Expect open generation", pg.isOpen());
        assertEquals("Expect id match", p1, pg.getPartId());
        assertEquals("Expect incremented generation #",
                     PartitionGenNum.generationZero().getNumber() + 1,
                     pg.getGenNum().getNumber());
        assertTrue("Start vlsn should be greater than 1",
                   pg.getStartVLSN() > FIRST_VLSN);
        assertEquals("End vlsn should be max vlsn since it is open",
                     PartitionGeneration.OPEN_GEN_END_VLSN, pg.getEndVLSN());
        /* verify prev generation info */
        assertEquals("Expect partition migrated from shard 1", 1,
                     pg.getPrevGenRepGroup().getGroupId());
        assertTrue("Expect a valid prev generation last vlsn",
                   pg.getPrevGenEndVLSN() <
                       PartitionGeneration.OPEN_GEN_END_VLSN);

        /* verify PG table on target shard replicas */
        waitForPartGenOpen(rg2Master.getGroupId(), p1);
        assertPGTEquals(rg2Master.getGroupId(), pgt);

        /* verify source PGT */

        /* find master of source rg1 */
        final int srcGrgp = 1;
        master = null;
        for (int i = 1; i <= 3; i++) {
            final RepNodeId id = new RepNodeId(srcGrgp, i);
            final RepNode rn = config.getRN(id);
            if (rn.getEnvImpl(0).getState().isMaster()) {
                master = rn;
                break;
            }
        }
        if (master == null) {
            fail("Cannot find master for rg1");
        }
        pgt = master.getPartitionManager().getPartGenTable();
        trace("master on rg1 " + master.getRepNodeId());
        trace(pgt.dumpTable());
        assertTrue("Table must be available on master, table: " +
                   pgt.dumpTable(), pgt.isReady());

        /* no admin in test env, the generation should be open on source */
        /* verify P1 is still open on rg1 */
        assertTrue("Table must contain P1", pgt.hasPartition(p1));
        assertTrue(pgt.getLastGen(p1).isOpen());
        assertEquals("Expect id match", p1, pg.getPartId());
    }

    /**
     * Tests that during the migration, target fails and a new master is
     * elected as master in target shard, the new master should restart
     * and complete the migration, and the generation table is correctly
     * updated on the new master.
     */
    @Test
    public void testTargetFailover() {

        final RepNodeId target2Id = new RepNodeId(2, 2);
        final RepNodeId target3Id = new RepNodeId(2, 3);

        /* reduce test time */
        config.getRepNodeParams(target2Id).getMap().
            setParameter(ParameterState.RN_PM_WAIT_AFTER_BUSY, "1 s");
        config.getRepNodeParams(target3Id).getMap().
            setParameter(ParameterState.RN_PM_WAIT_AFTER_BUSY, "1 s");
        config.getRepNodeParams(target3Id).getMap().
            setParameter(ParameterState.RN_PM_WAIT_AFTER_ERROR, "1 s");

        config.startRepNodeServices();

        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);

        /* Start and hold the migration. */
        final ReadHook readHook = new ReadHook();
        source.getMigrationManager().setReadHook(readHook);

        assertEquals(RepNodeAdmin.PartitionMigrationState.PENDING,
                     target.migratePartition(p1, rg1).
                         getPartitionMigrationState());

        readHook.waitForHook();

        waitForMigrationState(target, p1,
                              RepNodeAdmin.PartitionMigrationState.RUNNING);

        /* Kill the target node while waiting for source */
        config.stopRepNodeServicesSubset(true, target.getRepNodeId());

        /* Clear the hook to allow the source to work normally */
        readHook.releaseHook();
        source.getMigrationManager().setReadHook(null);

        /* a new master would elected in target shard, and restart migration */
        RepNode master = waitForMaster(config.getRN(target2Id),
                                       config.getRN(target3Id));
        waitForMigrationState(master, p1,
                              RepNodeAdmin.PartitionMigrationState.SUCCEEDED);


        /* verify target PGT */
        PartitionGenerationTable pgt = master.getPartitionManager()
                                             .getPartGenTable();
        assertTrue("Table must be available on master, table: " +
                   pgt.dumpTable(), pgt.isReady());
        trace(pgt.dumpTable());
        /* check migration record of p1 */
        assertTrue("Table must contain P1", pgt.hasPartition(p1));
        assertNotNull("Table should have an open generation for P1 after " +
                      "migration", pgt.getOpenGen(p1));
        Collection<PartitionGeneration> hist = pgt.getHistory(p1).values();
        assertEquals("Expect one generation", 1, hist.size());
        PartitionGeneration pg = hist.iterator().next();
        assertTrue("Expect open generation", pg.isOpen());
        assertEquals("Expect id match", p1, pg.getPartId());
        assertEquals("Expect incremented generation #",
                     pg.getGenNum().getNumber(),
                     PartitionGenNum.generationZero().getNumber() + 1);
        assertTrue("Start vlsn should be greater than 1",
                   pg.getStartVLSN() > FIRST_VLSN);
        assertEquals("End vlsn should be max vlsn since it is open",
                     PartitionGeneration.OPEN_GEN_END_VLSN, pg.getEndVLSN());
        /* verify prev generation info */
        assertEquals("Expect partition migrated from shard 1", 1,
                     pg.getPrevGenRepGroup().getGroupId());
        assertTrue("Expect a valid prev generation last vlsn",
                   pg.getPrevGenEndVLSN() <
                       PartitionGeneration.OPEN_GEN_END_VLSN);

        /* verify PG table on target shard replicas */
        waitForPartGenOpen(master.getRepNodeId().getGroupId(), p1,
                           Collections.singleton(target.getRepNodeId()));
        assertPGTEquals(master.getRepNodeId().getGroupId(), pgt,
                        Collections.singleton(target.getRepNodeId()));

        /* verify source PGT */
        master = config.getRN(sourceId);
        pgt = master.getPartitionManager().getPartGenTable();
        trace("master on rg1 " + master.getRepNodeId());
        trace(pgt.dumpTable());
        assertTrue("Table must be available on master, table: " +
                   pgt.dumpTable(), pgt.isReady());
        /* no admin, P1 is still open on rg1 */
        assertTrue("Table must contain P1", pgt.hasPartition(p1));
        assertTrue("Table should have an open generation for P1",
                   pgt.getLastGen(p1).isOpen());
        hist = pgt.getHistory(p1).values();
        assertEquals("Expect one generation", 1, hist.size());
        pg = hist.iterator().next();
        /* no admin P1 still open on source */
        trace("For partition=" + p1 + ", generation=" + pg);
        assertTrue("Expect open generation", pg.isOpen());
        assertEquals("Expect id match", p1, pg.getPartId());
        assertEquals("Start vlsn should be null", NULL_VLSN, pg.getStartVLSN());
        assertEquals("End vlsn should be less than max vlsn",
                     PartitionGeneration.OPEN_GEN_END_VLSN, pg.getEndVLSN());

        /* verify PG table on source shard replicas */
        assertPGTEquals(master.getRepNodeId().getGroupId(), pgt);
    }

    /**
     * Tests that generation table should return partitions that covers a
     * given VLSN
     */
    @Test
    public void testGenWithVLSN() {
        config.startRepNodeServices();
        final RepNode source = config.getRN(sourceId);
        final RepNode target = config.getRN(targetId);

        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());

        /* write some data to source shard */
        final Key[] keys = new KeyGenerator(source).getSortedKeys(p1, 10);
        final Value[] values = new Value[keys.length];
        for (int i = 0; i < keys.length; i++) {
            values[i] = Value.createValue(("Value " + i).getBytes());
            kvs.put(keys[i], values[i], null, Durability.COMMIT_SYNC, 0, null);
        }

        final PartitionGenerationTable pgtRG1 = source.getPartitionManager()
                                                      .getPartGenTable();
        /* since no migration, the generation is not ready */
        assertFalse("Expect table not ready on master", pgtRG1.isReady());

        /* move one partition to make PGT initialized on both ends */
        migratePartition(p1, sourceId, targetId);
        trace(p1 + " moved to " + targetId + " from " + sourceId);
        assertTrue("Expect table ready on master", pgtRG1.isReady());

        /* get all generations covering vlsn=1 */
        Set<PartitionGeneration> generations =
            pgtRG1.getGensWithVLSN(1);
        Set<Integer> pids =
            generations.stream()
                       .map(pg -> pg.getPartId().getPartitionId())
                       .collect(Collectors.toSet());
        /* expect all partition covers vlsn=1 */
        int[] exp = new int[]{1, 3, 5, 7, 9};
        for (int i : exp) {
            assertTrue("expect include partition " + i, pids.contains(i));
        }
        assertEquals("expect same size", exp.length, pids.size());

        /* write more data to source shard */
        for (int i = 0; i < keys.length; i++) {
            values[i] = Value.createValue(("New Value " + i).getBytes());
            kvs.put(keys[i], values[i], null, Durability.COMMIT_SYNC, 0, null);
        }

        /* verify PG table on source shard replicas */
        assertPGTEquals(sourceId.getGroupId(), pgtRG1);

        /* get P1 from target */
        final PartitionGenerationTable pgtRG2 = target.getPartitionManager()
                                                      .getPartGenTable();
        assertEquals("Expect p1 has generation 1 on target",
                     1, pgtRG2.getOpenGen(p1).getGenNum().getNumber());
        assertEquals("Expect p1's prev group is source",
                     source.getRepNodeId().getGroupId(),
                     pgtRG2.getOpenGen(p1).getPrevGenRepGroup().getGroupId());
        final long prevEndVSLN = pgtRG2.getOpenGen(p1).getPrevGenEndVLSN();

        /* for vlsn after this one, covering partition */
        generations = pgtRG1.getGensWithVLSN(VLSN.getNext(prevEndVSLN));
        pids = generations.stream()
                          .map(pg -> pg.getPartId().getPartitionId())
                          .collect(Collectors.toSet());

        /* all partitions other than p1 should  cover vlsn=1 */
        exp = new int[]{1, 3, 5, 7, 9};
        for (int i : exp) {
            assertTrue("expect include partition " + i, pids.contains(i));
        }
        assertEquals("expect same size", exp.length, pids.size());


        /* verify target */
        final long startVLSN = pgtRG2.getOpenGen(p1).getStartVLSN();
        generations = pgtRG2.getGensWithVLSN(startVLSN);
        pids = generations.stream()
                          .map(pg -> pg.getPartId().getPartitionId())
                          .collect(Collectors.toSet());

        /* all partitions including p1 should cover start vlsn */
        exp = new int[]{1, 2, 4, 6, 8, 10};
        for (int i : exp) {
            assertTrue("expect include partition " + i, pids.contains(i));
        }
        assertEquals("expect same size", exp.length, pids.size());

        /* verify PG table on target shard replicas */
        waitForPartGenOpen(targetId.getGroupId(), p1);
        assertPGTEquals(targetId.getGroupId(), pgtRG2);
    }

    /**
     * Tests the database manager which backs up the generation table
     */
    @Test
    public void testDBManager() {

        config.startRepNodeServices();
        final RepNode source = config.getRN(sourceId);
        final PartitionGenDBManager dbManager =
            source.getPartitionManager().getPartGenTable().getDbManager();

        assert source.getMigrationManager().getGenerationDB() != null;
        trace("generation db created and opened");

        /* put some open generations */
        for (int i = 1; i < 10; i++) {
            final PartitionId pid = new PartitionId(i);
            final PartitionGeneration pg = new PartitionGeneration(pid);
            dbPutInTxn(dbManager, pid, pg, this::trace);
        }

        /* read, verify, and close open generations */
        final long closeVLSN = 1024;
        for (int i = 1; i < 10; i++) {
            final PartitionId pid = new PartitionId(i);
            final PartitionGenNum pgn = PartitionGenNum.generationZero();
            PartitionGeneration pg = dbManager.get(pid, pgn);
            assertEquals(pid, pg.getPartId());
            assertEquals(pgn, pg.getGenNum());
            assertEquals(NULL_VLSN, pg.getStartVLSN());
            assertEquals(RepGroupId.NULL_ID, pg.getPrevGenRepGroup());
            assertEquals(NULL_VLSN, pg.getPrevGenEndVLSN());

            /* write closed pg to je db */
            pg.close(closeVLSN);
            dbPutInTxn(dbManager, pid, pg, this::trace);
        }

        /* verify closed generations */
        for (int i = 1; i < 10; i++) {
            final PartitionId pid = new PartitionId(i);
            final PartitionGenNum pgn = PartitionGenNum.generationZero();
            final PartitionGeneration pg = dbManager.get(pid, pgn);
            assertEquals(pid, pg.getPartId());
            assertEquals(closeVLSN, pg.getEndVLSN());
        }

        /* verify read history for a partition */
        final PartitionId pid = new PartitionId(1);
        final TestFilter filter = new TestFilter(pid);
        final List<PartitionGeneration> history = dbManager.scan(filter);
        assertEquals(1, history.size());
        final PartitionGeneration pg = history.get(0);
        final PartitionGenNum pgn = PartitionGenNum.generationZero();
        assertEquals(pid, pg.getPartId());
        assertEquals(pgn, pg.getGenNum());
        assertEquals(closeVLSN, pg.getEndVLSN());
    }

    /**
     * Base class for a hook which waits in the doHook() method, effectively
     * implementing a breakpoint.
     *
     * @param <T>
     */
    private static abstract class BaseHook<T> implements TestHook<T> {
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
            assert (success);
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
    private static class ReadHook extends BaseHook<DatabaseEntry> {
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

    /* verify initial partition generation */
    private void verifyInitPartGen(PartitionId pid,
                                   PartitionGenerationTable pgt) {

        assertTrue("Generation for partition " + pid + " must exist",
                   pgt.hasPartition(pid));

        /* verify the open generation */
        final PartitionGeneration pg = pgt.getOpenGen(pid);
        assertEquals("Mismatch partition id, expect " + pid + " while " +
                     "actual " + pg.getPartId(),
                     pid, pg.getPartId());

        assertEquals("Unexpected start VLSN, expect null vlsn while " +
                     "actual " + pg.getStartVLSN(), NULL_VLSN,
                     pg.getStartVLSN());

        assertEquals("Unexpected end VLSN, expect vlsn= " +
                     " while actual " + pg.getEndVLSN(),
                     PartitionGeneration.OPEN_GEN_END_VLSN, pg.getEndVLSN());

        assertTrue("Expect null prev owner shard, while actual " +
                   pg.getPrevGenRepGroup(),
                   pg.getPrevGenRepGroup().isNull());

        assertEquals("Expect null vlsn from prev generation, while actual " +
                     pg.getPrevGenEndVLSN(), NULL_VLSN, pg.getPrevGenEndVLSN());

        assertTrue("Generation for partition " + pid + " must be open",
                   pg.isOpen());

        assertEquals("Expect first PGN for " + pid + " while actual " +
                     pg.getGenNum(),
                     PartitionGenNum.generationZero(),
                     pg.getGenNum());

        final PartitionGeneration firstPG =
            pgt.getGen(pid, PartitionGenNum.generationZero());
        assertEquals("Without migration, expect very first generation",
                     firstPG.getGenNum(), pg.getGenNum());

        final Collection<PartitionGeneration> history = pgt.getHistory(pid)
                                                           .values();
        assertEquals("Expect only one generation ", 1, history.size());
        assertEquals("Expect only generation is open",
                     pg, history.iterator().next());
    }

    /**
     * Waits for one of the specified nodes to become a master and returns it.
     */
    private RepNode waitForMaster(final RepNode... nodes) {
        final List<RepNode> master = new ArrayList<>();

        boolean success = new PollCondition(500, 15000) {

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
        assert (success);

        return master.get(0);
    }

    private void waitForPGTReady(RepNode rn) {
        boolean success = new PollCondition(500, 15000) {
            @Override
            protected boolean condition() {
                final PartitionGenerationTable pgt =
                    rn.getPartitionManager().getPartGenTable();
                return pgt.isReady();
            }
        }.await();
        assert (success);
    }

    /*
     * Wait for generation of given partition to be open on
     * all rn of specified rg.
     */
    private void waitForPartGenOpen(final int rgId, final PartitionId pid) {
        waitForPartGenOpen(rgId, pid, Collections.emptySet());
    }

    /*
     * Wait for generation of given partition to be open on
     * all rn of specified rg, skip rn in excludeRNs.
     */
    private void waitForPartGenOpen(final int rgId,
                                    final PartitionId pid,
                                    final Set<RepNodeId> excludeRNs) {
        boolean success = new PollCondition(500, 15000) {

            @Override
            protected boolean condition() {

                for (RepNode rn : config.getRNs()) {
                    if (excludeRNs.contains(rn.getRepNodeId())) {
                        continue;
                    }
                    PartitionGenerationTable pgt = rn.getPartGenTable();
                    if (rn.getRepNodeId().getGroupId() == rgId) {
                        if (!pgt.isReady()) {
                            return false;
                        }
                        if (!pgt.isPartitionOpen(pid)) {
                            return false;
                        }
                    }
                }
                return true;
            }
        }.await();
        assert(success);
    }

    /*
     * Assert partition generation table of all rn in given rg has the
     * same generations as expected table.
     */
    private void assertPGTEquals(final int rgId,
                                 final PartitionGenerationTable expectPGT) {
        assertPGTEquals(rgId, expectPGT, Collections.emptySet());
    }

    /*
     * Assert partition generation table of all rn in given rg has the
     * same generations as expected table,  skip rn in excludeRNs.
     */
    private void assertPGTEquals(final int rgId,
                                 final PartitionGenerationTable expectPGT,
                                 final Set<RepNodeId> excludeRNs) {

        for (RepNode rn : config.getRNs()) {
            if (excludeRNs.contains(rn.getRepNodeId())) {
                continue;
            }
            if (rn.getRepNodeId().getGroupId() == rgId) {
                assertEquals(expectPGT.getGenTable(),
                             rn.getPartGenTable().getGenTable());
            }
        }
    }

    /* test filter to scan the je db */
    static class TestFilter implements PartitionGenDBManager.MetadataFilter {

        final PartitionId pid;

        TestFilter(PartitionId pid) {
            this.pid = pid;
        }

        @Override
        public boolean filter(DatabaseEntry key, DatabaseEntry val) {
            final byte[] data = key.getData();
            if (data == null) {
                return false;
            }
            final PartitionId id =
                PartitionGenDBManager.readPartIdFromKey(key.getData());
            return pid.equals(id);
        }
    }
}
