/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.backup;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.StoreUtils;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.server.JENotifyHooks.LogRewriteListener;

import org.junit.Test;

/**
 * Tests the basic snapshot function
 */
public class SnapshotTest extends BackupTestBase {

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
    }

    /**
     * Create the "backup" store and test out some basic snapshot operations.
     */
    @Test
    public void testBasicSnapshot()
        throws Exception {

        createBackupStore();
        StoreUtils su = createBackupStoreUtils();
        su.load(100);
        String snap = createBackupSnapshot("basic");

        /**
         * Some assertions about the snapshot.
         */
        File adminDir = new File(getBackupAdminSnapshotDir(), snap);
        File rnDir = new File(getBackupRepNodeSnapshotDir(), snap);
        assertTrue(adminDir.isDirectory());
        assertTrue(rnDir.isDirectory());
        String[] snaps = listBackupSnapshots();
        assertTrue(snaps.length == 1);
        assertTrue(snaps[0].equals(snap));

        /* snapshot of config */
        File snapshotConfigRoot =
            FileNames.getSnapshotNamedDir(backupRoot, snap);
        File snapshotConfigStoreDir =
            new File(snapshotConfigRoot, backupStorename);
        File snapshtConfigSNDir =
            new File(snapshotConfigStoreDir,
                 backupStore.getSNA().getStorageNodeAgent().getServiceName());
        assertTrue(snapshotConfigRoot.exists());
        assertTrue(snapshotConfigStoreDir.exists());
        assertTrue(snapshtConfigSNDir.exists());

        /**
         * Remove the snapshot and assert the state is as expected.
         */
        assertTrue(removeBackupSnapshot(snap));

        assertTrue(listBackupSnapshots().length == 0);
        assertFalse(adminDir.exists());
        assertFalse(rnDir.exists());

        /* snapshot config dirs */
        assertFalse(snapshotConfigRoot.exists());
        assertFalse(snapshotConfigStoreDir.exists());
        assertFalse(snapshtConfigSNDir.exists());
    }

    /**
     * Create the "backup" store and test out some basic snapshot operations
     * in a zone.
     */
    @Test
    public void testBasicSnapshotInZone()
        throws Exception {

        createBackupStore();
        DatacenterId dcId = DatacenterId.parse("zn1");
        StoreUtils su = createBackupStoreUtils();
        su.load(100);
        String snap = createBackupSnapshot("basic" ,dcId);

        /**
         * Some assertions about the snapshot.
         */
        File adminDir = new File(getBackupAdminSnapshotDir(), snap);
        File rnDir = new File(getBackupRepNodeSnapshotDir(), snap);
        assertTrue(adminDir.isDirectory());
        assertTrue(rnDir.isDirectory());
        String[] snaps = listBackupSnapshots();
        assertTrue(snaps.length == 1);
        assertTrue(snaps[0].equals(snap));

        /* snapshot of config */
        File snapshotConfigRoot =
            FileNames.getSnapshotNamedDir(backupRoot, snap);
        File snapshotConfigStoreDir =
            new File(snapshotConfigRoot, backupStorename);
        File snapshtConfigSNDir =
            new File(snapshotConfigStoreDir,
                 backupStore.getSNA().getStorageNodeAgent().getServiceName());
        assertTrue(snapshotConfigRoot.exists());
        assertTrue(snapshotConfigStoreDir.exists());
        assertTrue(snapshtConfigSNDir.exists());

        /**
         * Remove the snapshot and assert the state is as expected.
         */
        assertTrue(removeBackupSnapshot(snap, dcId));

        assertTrue(listBackupSnapshots().length == 0);
        assertFalse(adminDir.exists());
        assertFalse(rnDir.exists());

        /* snapshot config dirs */
        assertFalse(snapshotConfigRoot.exists());
        assertFalse(snapshotConfigStoreDir.exists());
        assertFalse(snapshtConfigSNDir.exists());
    }

    @Test
    public void testBigSnapShot() throws Exception{
        createAndRemoveBigSnapshot(null);
    }

    @Test
    public void testBigSnapShotInZone() throws Exception{
        DatacenterId dcId = DatacenterId.parse("zn1");
        createAndRemoveBigSnapshot(dcId);
    }

    /**
     * Use a JE param to minimize the log file size, along with a larger number
     * of records to put multiple log files in the same link call.
     * If dcId is null the operation applies to the entire store else the
     * operation is zone specific.
     */
    public void createAndRemoveBigSnapshot(DatacenterId dcId)
        throws Exception {

        /*
         * Create a policy map
         */
        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.JE_MISC,
                         "je.log.fileMax=1000000");
        map.setParameter(ParameterState.SN_MAX_LINK_COUNT, "5");

        /*
         * Use the policy map to create the store.
         */
        createBackupStore(map);
        StoreUtils su = createBackupStoreUtils();
        su.load(100000);
        String snap = createBackupSnapshot("big", dcId);

        /**
         * Some assertions about the snapshot.
         */
        File adminDir = new File(getBackupAdminSnapshotDir(), snap);
        File rnDir = new File(getBackupRepNodeSnapshotDir(), snap);
        assertTrue(adminDir.isDirectory());
        assertTrue(rnDir.isDirectory());
        String[] logs = rnDir.list();
        assertTrue(logs.length > 2);
        String[] snaps = listBackupSnapshots();
        assertTrue(snaps.length == 1);
        assertTrue(snaps[0].equals(snap));

        /*
         * Exercise the pre-hard-recovery copying feature.  From the previous
         * assertion, we know that we have at least 3 log files.  Let's imagine
         * that logs[1] and logs[2] will be modified.
         */
        File logDir =
            FileNames.getEnvDir(backupRoot.getCanonicalPath(),
                                backupStorename, null,
                                new StorageNodeId(1),
                                new RepNodeId(1, 1));
        Set<File> modifyFiles = new HashSet<File>();
        File[] files = new File[3];
        for (int i = 0; i < 3; i++) {
            files[i] = new File(logDir, logs[i]);
        }
        modifyFiles.add(files[1]);
        modifyFiles.add(files[2]);
        new LogRewriteListener(rnDir, logger).rewriteLogFiles(modifyFiles);

        /*
         * How do we know anything happened?  Let's try touching file
         * modification times.  We'll touch the actual log files, and then
         * examine the snapshot files.  The files that have been copied
         * shouldn't reflect the newly touched times, but ones that are still
         * hard links should.
         *
         * (According to java.io.File, some systems have 1-sec resolution.)
         * Note that we're careful not to assume that JE background threads
         * might not be concurrently touching these log files too, since the JE
         * env is still running.
         */
        long[] origModTimes = new long[3];
        File[] snapshotFiles = new File[3];
        for (int i = 0; i < 3; i++) {
            snapshotFiles[i] = new File(rnDir, logs[i]);
            origModTimes[i] = snapshotFiles[i].lastModified();
        }
        Thread.sleep(1001);
        for (int i = 0; i < 3; i++) {
            files[i].setLastModified(System.currentTimeMillis());

            if (i == 0) {
                /*
                 * Still hard-linked.  Touching the log file ought to disturb
                 * the corresponding snapshot file (since they're really the
                 * same file).
                 */
                assertTrue(snapshotFiles[i].lastModified() > origModTimes[i]);
            } else {
                /* Copied, no longer any linkage to original log file. */
                assertEquals(snapshotFiles[i].lastModified(), origModTimes[i]);
            }
        }

        /**
         * Remove the snapshot and assert the state is as expected.
         */
        assertTrue(removeBackupSnapshot(snap, dcId));

        assertTrue(listBackupSnapshots().length == 0);
        assertFalse(adminDir.exists());
        assertFalse(rnDir.exists());
    }

    /*
     * Test snapshot operations against a store with specified adminDir.
     */
    @Test
    public void testCustomizedAdminDir() throws Exception {
        /*
         * Creates bootstrap with specified adminDir/adminDirSize, start store
         * with the bootstrap.
         */
        final File bootConfig = new File(backupRoot, "config.xml");
        final File adminDir = backupMount;
        final BootstrapParams bp =
            new BootstrapParams(backupRoot.getAbsolutePath(),
                                testhost,
                                testhost,
                                backupPortFinder.getHaRange(),
                                null,
                                backupStorename ,
                                backupPortFinder.getRegistryPort(),
                                -1,
                                1,
                                null,
                                null,
                                false,
                                null);

        /* Set adminDir = <testDir>/backup/mount, adminDirSize = 50MB */
        bp.setAdminDir(adminDir.getAbsolutePath(), "50 MB");
        ConfigUtils.createBootstrapConfig(bp, bootConfig);

        final File secfile = new File(backupRoot, "security.policy");
        TestUtils.generateSecurityPolicyFile(secfile.toString(),
                                             TestUtils.SEC_POLICY_STRING);

        /* Start store with existing bootstrap file created above */
        createBackupStore();

        final File adminSnapshotDir =
            FileNames.getSnapshotDir(backupRoot.toString(),
                                     backupStorename,
                                     adminDir,
                                     new StorageNodeId(1),
                                     new AdminId(1));

        /* create a snapshot */
        String snap = createBackupSnapshot("back01");
        File[] snapDirs = adminSnapshotDir.listFiles();
        assertEquals(1, snapDirs.length);
        assertEquals(snap, snapDirs[0].getName());

        /* list snapshot */
        String[] snapshots = listBackupSnapshots();
        assertEquals(1, snapshots.length);
        assertEquals(snap, snapshots[0]);

        /* remove the snapshot */
        boolean ret = removeBackupSnapshot(snap);
        assertTrue(ret);
        /* verify no snapshot left */
        snapshots = listBackupSnapshots();
        assertEquals(0, snapshots.length);
    }
}
