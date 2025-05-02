/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.backup;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.List;

import oracle.kv.KVVersion;
import oracle.kv.Key;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.StoreUtils;
import oracle.kv.util.kvlite.KVLite;

import org.junit.Test;

/**
 * Tests the ability to use a recovery directory.
 */
public class RecoveryTest extends BackupTestBase {

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
     * 1.  Create a store, add some records.
     * 2.  Create a snapshot
     * 3.  Add some new records, remember the keys.
     * 4.  Set up recovery from the snapshot (only the RN, ignore the admin)
     * 5.  Restart the service
     * 6.  Verify that the old snapshot was restored by looking for new keys.
     */
    @Test
    public void testBasicRecovery()
        throws Exception {

        createBackupStore();
        StoreUtils su = createBackupStoreUtils();
        List<Key> initialKeys = su.load(20);
        String snap = createBackupSnapshot("recovery");

        /**
         * 10 more records that won't be in the snapshot.
         */
        List<Key> newKeys = su.load(10);
        su.keysExist(newKeys);

        /**
         * Rename the snapshot into the recovery dir.
         */
        File rnDir = new File(getBackupRepNodeSnapshotDir(), snap);
        File recoveryDir = getBackupRepNodeRecoveryDir();
        recoveryDir.mkdirs();
        rnDir.renameTo(new File(recoveryDir, snap));

        /**
         * Restart the KVLite instance to recover to the snapshot
         */
        shutdownStores();
        createBackupStore();
        su.keysDoNotExist(newKeys);
        su.keysExist(initialKeys);
    }

    @Test
    public void testRestoreFromSnapshot() throws Exception {
        createBackupStore();
        StoreUtils su = createBackupStoreUtils();
        List<Key> initialKeys = su.load(20);
        String snap = createBackupSnapshot("recovery");

        /*
         * Add new records.
         */
        List<Key> newKeys = su.load(10);
        su.keysExist(newKeys);

        /*
         * Modify configurations.
         */
        final File bootConfig = new File(backupRoot, "config.xml");
        BootstrapParams bp = ConfigUtils.getBootstrapParams(bootConfig);
        ConfigUtils.createBootstrapConfig(bp, bootConfig, null);

        /* Restart with restore option, override the configurations */
        shutdownStores();
        backupStore = createStore
                (backupRoot, backupStorename, backupPortFinder,
                 KVLite.DEFAULT_NUM_PARTITIONS,
                 getBackupMountPoint(), null, snap);
        su.keysDoNotExist(newKeys);
        su.keysExist(initialKeys);

        /* Validate config */
        bp = ConfigUtils.getBootstrapParams(bootConfig);
        assertEquals(bp.getSoftwareVersion(), KVVersion.CURRENT_VERSION);
    }
}
