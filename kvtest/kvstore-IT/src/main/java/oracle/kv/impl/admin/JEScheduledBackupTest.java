/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

import oracle.kv.TestBase;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;

import com.sleepycat.je.BackupArchiveLocation;
import com.sleepycat.je.BackupFSArchiveCopy;
import com.sleepycat.je.BackupFileCopy;
import com.sleepycat.je.BackupFileLocation;
import com.sleepycat.je.dbi.BackupManager;
import com.sleepycat.je.dbi.SnapshotManifest;
import com.sleepycat.je.util.CheckSnapshot;

import org.junit.Test;

/**
 * Test enabling and using the JE automatic backup facilities.
 */
public class JEScheduledBackupTest extends TestBase {

    private static final int START_PORT = 5000;
    private static final long TIME_MULTIPLIER = 3600;

    /** Store archive here if non-null, for debugging */
    private static final String saveArchive = null;

    private CreateStore createStore;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        TestUtils.clearTestDirectory();
        TestStatus.setManyRNs(true);

        /* Each hour takes a second */
        BackupManager.timeMultiplier = TIME_MULTIPLIER;
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        if (createStore != null) {
            createStore.shutdown();
        }
        LoggerUtils.closeAllHandlers();

        BackupManager.timeMultiplier = 0;
    }

    @Test
    public void testBasic()
        throws Exception {

        /* Create store */
        createStore = new CreateStore(kvstoreName, START_PORT,
                                      3 /* numStorageNodes */,
                                      3 /* replicationFactor */,
                                      10 /* numPartitions */,
                                      1 /* capacity */);
        createStore.setAdminLocations(1, 2, 3);
        createStore.start();

        /* Create backup configuration */
        Path copyConfig = Files.createTempFile("copy", "properties");
        Files.write(copyConfig,
                    singleton(BackupFSArchiveCopy.CHECKSUM_KEY + "=SHA1"));
        Path archive;
        if (saveArchive != null) {
            archive = Paths.get(saveArchive);
            if (Files.exists(archive)) {
                try (Stream<Path> s = Files.walk(archive)) {
                    s.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
                }
            }
            Files.createDirectory(archive);
        } else {
            archive = Files.createTempDirectory(
                Paths.get(System.getProperty("java.io.tmpdir")), "archive");
        }
        assertEquals("Archive should be empty",
                     0, Files.list(archive).count());

        String backupParams =
            "je.env.runBackup=true;" +
            "je.backup.schedule=0 * * * *;" +
            "je.backup.copyConfig=" + copyConfig + ";" +
            "je.backup.locationConfig=" + archive + ";";
        String timeMultiplierParams =
            "-Dcom.sleepycat.je.test.timeMultiplier=" + TIME_MULTIPLIER;

        /* Enable backups on RNs */
        CommandServiceAPI cs = createStore.getAdmin();
        ParameterMap map = new ParameterMap();
        map.setType(ParameterState.REPNODE_TYPE);
        map.setParameter(ParameterState.JE_MISC, backupParams);
        map.setParameter(ParameterState.JVM_MISC, timeMultiplierParams);
        int planId =
            cs.createChangeAllParamsPlan("enable backups on RNs", null, map);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        /* Enable backups on Admins */
        map = new ParameterMap();
        map.setType(ParameterState.ADMIN_TYPE);
        map.setParameter(ParameterState.JE_MISC, backupParams);
        map.setParameter(ParameterState.JVM_MISC, timeMultiplierParams);
        int adminPlanId = cs.createChangeAllAdminsPlan(
            "enable backups on Admins", null, map);
        cs.approvePlan(adminPlanId);
        cs.executePlan(adminPlanId, false);
        assertTrue(
            "Await plan completion after admin restart",
            PollCondition.await(
                1000, 120000,
                () -> {
                    try {
                        CommandServiceAPI master =
                            createStore.getAdminMaster();
                        master.awaitPlan(adminPlanId, 0, null);
                        master.assertSuccess(adminPlanId);
                        return true;
                    } catch (Exception e) {
                        return false;
                    }
                }));

        /* Wait for three backup cycles */
        BackupManager.sleepMs(3 * 60 * 60 * 1000);

        /* Stop store to stop backups */
        createStore.shutdown();

        /*
         * Verify the first 3 backups. Later backups might not be complete
         * since we may be attempting to verify them while they are being
         * created.
         */
        BackupFileCopy copy = new BackupFSArchiveCopy();
        copy.initialize(copyConfig.toFile());
        String[] nodes = {"1", "2", "3", "rg1-rn1", "rg1-rn2", "rg1-rn3"};
        for (String node : nodes) {
            Path nodePath = archive.resolve(node);
            int count = 0;
            for (Path p : Files.list(nodePath)
                     .sorted()
                     .limit(3)
                     .toArray(Path[]::new)) {
                SnapshotManifest manifest = CheckSnapshot.readManifest(
                    p.resolve(BackupManager.SNAPSHOT_MANIFEST));
                BackupArchiveLocation location = new BackupFileLocation();
                location.initialize(node.toString(), archive.toFile());
                new CheckSnapshot(manifest, copy, location).check();
                count++;
            }
            assertTrue("Count for node " + node +
                       " should be at least 3, found " + count,
                       count >= 3);
        }
    }
}
