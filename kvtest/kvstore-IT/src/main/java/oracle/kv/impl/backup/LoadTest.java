/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.backup;

import static oracle.kv.util.DDLTestUtils.execStatement;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import oracle.kv.KVStore;
import oracle.kv.StatementResult;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.TopologyUtil;
import oracle.kv.impl.util.StoreUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Assume;
import org.junit.Test;

/**
 * Tests the basic operations of StorageNodeAgentImpl.
 */
public class LoadTest extends BackupTestBase {

    private static final String TEST_USER = "jack";
    private static final String TEST_USER_PW = "NoSql00__1234";
    private static String TEST_ROLE = "role_a";
    private static final String TEST_TABLE = "test_table";
    private static final String TEST_TABLE_DEF =
        " (id integer, name string, primary key(id))";

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

    @Test
    public void testBasicLoad()
        throws Exception {

        new LoadSnapshotKVLite().doLoad(100);
    }

    /**
     * Test an empty snapshot/load
     */
    @Test
    public void testEmptyLoad()
        throws Exception {

        new LoadSnapshotKVLite().doLoad(0);
    }

    @Test
    public void testLoadFrom3x1Store()
        throws Exception {

        new LoadSnapshot3x1Store().doLoad(10000);
    }

    @Test
    public void testLoadWitStatusFilesFrom3x1Store()
        throws Exception {

        File statusDir = new File(restoreRoot, "statusDir");
        statusDir.mkdirs();

        LoadSnapshot loadSnapshot = new LoadSnapshot3x1Store();
        /* Load 10000 records to store */
        String snap = loadSnapshot.doLoad(10000,
                      statusDir.getAbsolutePath());


        /* Verify the status file and its contents*/

        File[] statusFiles = statusDir.listFiles();
        assertTrue(statusFiles.length == 4);

        KVStoreImpl backupStoreImpl =
            (KVStoreImpl)loadSnapshot.getBackupStoreUtils().getKVStore();
        Map<RepGroupId, List<PartitionId>> rgPartitions =
            TopologyUtil.getRGIdPartMap(backupStoreImpl.getTopology());

        for (File file : statusFiles) {
            if (file.getName().contains("cp.lck")) {
                continue;
            }
            List<PartitionId> partitionIds = null;
            for (RepGroupId gid : rgPartitions.keySet()) {
                if (file.getName().contains(gid.toString())) {
                    partitionIds = rgPartitions.get(gid);
                    break;
                }
            }
            assertTrue(partitionIds != null);
            checkCheckpointFiles(file, restoreStorename, partitionIds);
        }
        backupStoreImpl.close();

        /*
         * Load again with status files, it is expected to skip all the
         * databases.
         */
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        List<File> dirs = getBackupRepNodeSnapshotDirs(backup3x1Store, snap);
        long numRecordsLoaded =
                loadToRestore(dirs.toArray(new File[dirs.size()]),
                              statusDir.getAbsolutePath(),
                              new PrintStream(output),
                              getKVLiteLoginFilePath(restoreRoot));
        assertEquals(0, numRecordsLoaded);
        assertTrue(output.toString().contains("No more database to load."));
    }

    /** Test that the Load command loads security metadata. [#24642] */
    @Test
    public void testLoadMetadata()
    
        throws Exception {
        Assume.assumeTrue("Permission case only work in secure store",
                           SECURITY_ENABLE);
        new LoadMetadataSnapshotKVLite().doLoadMetadata();
    }

    /**
     * Verify the checkpoint files.
     */
    private void checkCheckpointFiles(File shardDir,
                                      String storeName,
                                      List<PartitionId> partitionIds) {

        assertTrue(shardDir.isDirectory());
        assertTrue(shardDir.listFiles().length == partitionIds.size());

        for (PartitionId pid : partitionIds) {
            File file = new File(shardDir, pid.getPartitionName());
            assertTrue(file.exists() && file.isFile());
            checkCheckpointFile(file, storeName);
        }
    }

    /**
     * Check the content of checkpoint file.
     */
    private void checkCheckpointFile(File checkpointFile, String storeName) {

        FileInputStream fis = null;
        try {
            fis = new FileInputStream(checkpointFile);
            Properties props = new Properties();
            props.load(fis);
            assertTrue(((String)props.get("store"))
                           .equalsIgnoreCase(storeName));
            assertTrue(props.get("loadTime") != null);
        } catch (FileNotFoundException ex) {
            fail("Status file does not exist: " + checkpointFile);
        } catch (IOException ex) {
            fail("Failed to read the status file: " + checkpointFile);
        } finally {
            try {
                if (fis != null) {
                    fis.close();
                }
            } catch (IOException ignored) {
            }
        }
    }

    private class LoadSnapshotKVLite extends LoadSnapshot {

        @Override
        void startBackupStore()
            throws Exception{

            createBackupStore();
        }

        @Override
        File[] getSnapshotDirs(String snap) {
            File snapshotDir = backupSnapshotDir(snap);
            return new File[]{snapshotDir};
        }

        @Override
        StoreUtils getBackupStoreUtils() {
            if (SECURITY_ENABLE) {
                return new StoreUtils(backupStorename, testhost,
                    backupPortFinder.getRegistryPort(),
                    getKVLiteLoginFilePath(backupRoot),
                    StoreUtils.RecordType.UUID, 0, null,
                        null);
            }
            return new StoreUtils(backupStorename, testhost,
                                  backupPortFinder.getRegistryPort(),
                                  StoreUtils.RecordType.UUID);
        }

        @Override
        String getBackupSnapshot(String name) {
            try {
                return createBackupSnapshot("basic");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private class LoadSnapshot3x1Store extends LoadSnapshot {

        @Override
        void startBackupStore()
            throws Exception {

            createBackup3x1Store(null);
        }

        @Override
        File[] getSnapshotDirs(String snap) {
            List<File> dirs =
                getBackupRepNodeSnapshotDirs(backup3x1Store, snap);
            return dirs.toArray(new File[dirs.size()]);
        }

        @Override
        StoreUtils getBackupStoreUtils() {
            if (SECURITY_ENABLE) {
                return new StoreUtils(
                    backupStorename, testhost,
                    backupPortFinder.getRegistryPort(),
                    backup3x1Store.getDefaultUserLoginPath(),
                    StoreUtils.RecordType.UUID, 0, null,
                        null);
            }
            return new StoreUtils(backupStorename, testhost,
                                  backupPortFinder.getRegistryPort(),
                                  StoreUtils.RecordType.UUID);
        }

        @Override
        String getBackupSnapshot(String name) {
            try {
                return create3x1BackupSnapshot(name);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private class LoadMetadataSnapshotKVLite extends LoadSnapshotKVLite {

        /**
         * Test the "snapshot, load" sequence.
         * 1.  Create 2 stores - the "backup" store and the "restore" store
         * 2.  Create a user, role and table in backup store
         * 3.  Snapshot the backup store
         * 4.  Load metadata into the restore store, directly from the snapshot
         * 5.  Compare metadata of 2 stores for equality
         */
        void doLoadMetadata()
            throws Exception {

            startBackupStore();
            StoreUtils backup = getBackupStoreUtils();

            /*
             * Create user, role and table in backup store.
             */
            execStatement(backup.getKVStore(), "CREATE USER " +
                          TEST_USER + " IDENTIFIED BY \"" +
                          TEST_USER_PW + "\"" + "ADMIN");
            execStatement(backup.getKVStore(), "create role " +
                          TEST_ROLE);
            execStatement(backup.getKVStore(),
                "create table " + TEST_TABLE + TEST_TABLE_DEF);
            String snapshot = getBackupSnapshot("admin");
            assertTrue(backup.getKVStore() != null);
            backup.getKVStore().close();
            LoggerUtils.closeAllHandlers();

            /**
             * Create and load into the restore store. Modify name of restore
             * store as the same one of backup store, because loading security
             * metadata requires names of backup and restore stores are the
             * same.
             */
            restoreStorename = backupStorename;

            /*
             * Clear registry client socket factory and map, since we locate
             * csf by name of store, otherwise restore store cannot be deployed
             * using the same name of backup store.
             */
            RegistryUtils.clearRegistryCSF();
            createRestoreStore();
            KVStore restoreKVStore = createRestoreStoreUtils().getKVStore();

            /* force load if secure because secure kvlite has a default user */
            boolean forceLoad = SECURITY_ENABLE;
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            loadMetadataToRestore(backupAdminSnapshotDir(snapshot),
                                  new PrintStream(output) /* output */,
                                  getKVLiteLoginFilePath(restoreRoot),
                                  forceLoad);

            /* verify if metadata restored */
            StatementResult result;
            result = restoreKVStore.executeSync("show users");
            assertThat(result.getResult(), containsString(TEST_USER));
            result = restoreKVStore.executeSync("show roles");
            assertThat(result.getResult(), containsString(TEST_ROLE));
            assertNotNull(restoreKVStore.getTableAPI().getTable(TEST_TABLE));

            assertTrue(restoreKVStore != null);
            restoreKVStore.close();
        }
    }

    private abstract class LoadSnapshot {

        String doLoad(int numRecords)
            throws Exception {

            return doLoad(numRecords, null);
        }

        /**
         * Test the "snapshot, load" sequence.
         * 1.  Create 2 stores - the "backup" store and the "restore" store.
         * 2.  Put some records in the backup store
         * 3.  Snapshot the backup store
         * 4.  Load into the restore store, directly from the snapshot
         * 5.  Compare the 2 stores for equality
         * 6.  Modify one of the stores and compare again
         */
        String doLoad(int numRecords, String statusDirPath)
            throws Exception {

            /**
             * Create, populate, snapshot the backup store.
             */
            startBackupStore();

            StoreUtils backup = getBackupStoreUtils();
            backup.load(numRecords);
            String snap = getBackupSnapshot("basic");

            /**
             * Create and load into the restore store
             */
            createRestoreStore();

            /**
             * Load to the restore store and compare to backup.
             */

            File[] snapshotDirs = getSnapshotDirs(snap);
            long numRecordsLoaded =
                    loadToRestore(snapshotDirs, statusDirPath, null,
                                  getKVLiteLoginFilePath(restoreRoot));

            assertEquals(numRecords, numRecordsLoaded);
            StoreUtils restore = createRestoreStoreUtils();
            /*
             * The comparison here is based on all the data. In secure mode,
             * the session data will be different for two stores. So the
             * assertion will fail in security mode.
             * TODO add verification that works for secure mode
             */
            if (!SECURITY_ENABLE) {
                assertTrue(restore.compare(backup));
            }

            /**
             * Change a record and make sure that comparison fails.
             */
            backup.load(1);
            /*
             * Secure mode include the session data, the comparison will not be
             * accurate. So skip the meaningless assertion here.
             * TODO add verification that works for secure mode
             */
            if (!SECURITY_ENABLE) {
                assertFalse(restore.compare(backup));
            }

            assertTrue(backup.getKVStore() != null);
            backup.getKVStore().close();

            assertTrue(restore.getKVStore() != null);
            restore.getKVStore().close();

            return snap;
        }

        abstract void startBackupStore() throws Exception;

        abstract File[] getSnapshotDirs(String snap);

        abstract StoreUtils getBackupStoreUtils();

        abstract String getBackupSnapshot(String name);
    }
}

