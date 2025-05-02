/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.backup;

import static oracle.kv.util.CreateStore.mergeParameterMapDefaults;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import oracle.kv.LoginCredentials;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.Snapshot;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.StoreUtils;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;
import oracle.kv.util.Load;
import oracle.kv.util.kvlite.KVLite;

/**
 * Wrapper class for testing Backup and Restore, including snapshots.
 */
public class BackupTestBase extends TestBase {

    protected final PortFinder backupPortFinder;
    protected final PortFinder restorePortFinder;

    protected File backupRoot;
    protected File restoreRoot;
    protected File backupMount;
    protected File restoreMount;
    protected KVLite backupStore;
    protected CreateStore backup3x1Store;
    protected KVLite restoreStore;
    protected CommandServiceAPI backupAdmin;
    protected CommandServiceAPI restoreAdmin;
    protected boolean useMountPoint;
    protected LoginManager loginMgr;

    private static final String testdir = TestUtils.getTestDir().toString();
    protected static final String RESTORE_STORE_NAME = "restore";
    protected static final String testhost = "localhost";
    protected static final int backupStartPort = 6000;
    protected static final int restoreStartPort = 7000;
    protected static final int haRange = 5;
    protected static final String backupStorename = "backup";
    protected static final String mountPoint = "mount";

    protected static String restoreStorename = RESTORE_STORE_NAME;

    protected BackupTestBase() {
        backupPortFinder = new PortFinder(backupStartPort, haRange);
        restorePortFinder = new PortFinder(restoreStartPort, haRange);
        backupRoot = new File(testdir, backupStorename);
        restoreRoot = new File(testdir, restoreStorename);
        /* Create mount points even if they are not used */
        backupMount = new File(backupRoot, mountPoint);
        restoreMount = new File(restoreRoot, mountPoint);
        loginMgr = null;
    }

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        RegistryUtils.clearRegistryCSF();
        assertTrue(backupRoot.mkdirs());
        assertTrue(restoreRoot.mkdirs());
        assertTrue(backupMount.mkdirs());
        assertTrue(restoreMount.mkdirs());
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        shutdownStores();
        LoggerUtils.closeAllHandlers();

        /* reset restore store name */
        restoreStorename = RESTORE_STORE_NAME;
    }

    protected void shutdownStores() {
        if (backupStore != null) {
            backupStore.getSNA().getStorageNodeAgent().resetRMISocketPolicies();
            backupStore.stop(true);
            backupStore = null;
        }
        if (restoreStore != null) {
            restoreStore.getSNA().getStorageNodeAgent().
                resetRMISocketPolicies();
            restoreStore.stop(true);
            restoreStore = null;
        }
        if (backup3x1Store != null) {
            backup3x1Store.getStorageNodeAgent(0).resetRMISocketPolicies();
            backup3x1Store.shutdown(true);
            backup3x1Store = null;
        }
        if (backupAdmin != null) {
            backupAdmin = null;
        }
        if (restoreAdmin != null) {
            restoreAdmin = null;
        }
        if (loginMgr != null) {
            loginMgr = null;
        }
    }

    protected void createBackupStore() throws Exception {
        createBackupStore(null);
    }

    protected void createBackupStore(ParameterMap policyMap) 
        throws Exception {
        if (backupStore != null) {
            backupStore.stop(true);
        }
        backupStore = createStore
            (backupRoot, backupStorename, backupPortFinder,
             KVLite.DEFAULT_NUM_PARTITIONS,
             getBackupMountPoint(), policyMap);
        ensureBackupAdmin();
    }

    protected void createBackup3x1Store(ParameterMap policyMap)
        throws Exception {

        if (backup3x1Store != null) {
            backup3x1Store.shutdown();
        }
        backup3x1Store = create3x1Store(backupRoot,
                                        backupStorename,
                                        backupPortFinder,
                                        3 * KVLite.DEFAULT_NUM_PARTITIONS,
                                        policyMap);
        ensureBackupAdmin();
    }

    /**
     * TODO: this should really use the Topology but because it's KVLite it's
     * easy to know the proper ids.
     */
    protected File getBackupAdminSnapshotDir() {
        ensureBackupAdmin();
        return FileNames.getSnapshotDir(backupRoot.toString(),
                                        backupStorename,
                                        null,
                                        new StorageNodeId(1),
                                        new AdminId(1));
    }

    protected File backupAdminSnapshotDir(String snap) {
        return new File(getBackupAdminSnapshotDir(), snap);
    }

    protected File backupSnapshotDir(String snap) {
        return new File(getBackupRepNodeSnapshotDir(), snap);
    }

    protected File getBackupRepNodeSnapshotDir() {
        ensureBackupAdmin();
        return FileNames.getSnapshotDir(backupRoot.toString(),
                                        backupStorename,
                                        backupStore.getMountPoint(),
                                        new StorageNodeId(1),
                                        new RepNodeId(1,1));
    }

    protected File getBackupRepNodeRecoveryDir() {
        ensureBackupAdmin();
        return FileNames.getRecoveryDir(backupRoot.toString(),
                                        backupStorename,
                                        backupStore.getMountPoint(),
                                        new StorageNodeId(1),
                                        new RepNodeId(1,1));
    }

    protected String createBackupSnapshot(String name)
        throws Exception {

        Snapshot snapshot = getBackupSnapshot();
        return snapshot.createSnapshot(name);
    }

    protected String create3x1BackupSnapshot(String name) throws Exception {
        if (backupAdmin == null) {
            backupAdmin = RegistryUtils.getAdmin(
                backup3x1Store.getHostname(),
                backup3x1Store.getRegistryPort(),
                backup3x1Store.getAdminLoginManager(),
                logger);
        }
        Snapshot snapshot = new Snapshot(backupAdmin, false,
                null);
        return snapshot.createSnapshot(name);
    }

    protected String createBackupSnapshot(String name, DatacenterId dcId)
        throws Exception{

        Snapshot snapshot = getBackupSnapshot();
        return snapshot.createSnapshot(name, dcId);
    }

    protected String[] listBackupSnapshots()
        throws Exception {

        Snapshot snapshot = getBackupSnapshot();
        return snapshot.listSnapshots();
    }

    protected boolean removeBackupSnapshot(String name)
        throws Exception {

        Snapshot snapshot = getBackupSnapshot();
        snapshot.removeSnapshot(name);
        return snapshot.succeeded();
    }

    protected boolean removeBackupSnapshot(String name, DatacenterId dcId)
        throws Exception {

        Snapshot snapshot = getBackupSnapshot();
        snapshot.removeSnapshot(name, dcId);
        return snapshot.succeeded();
    }

    /**
     * Create the restore store with a different number of partitions from the
     * backup store to "test" topology changes.
     */
    protected void createRestoreStore() {
        createRestoreStore(null);
    }

    protected StoreUtils createBackupStoreUtils() {
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

    protected void createRestoreStore(ParameterMap policyMap) {
        if (restoreStore != null) {
            restoreStore.stop(true);
        }
        restoreStore =
            createStore(restoreRoot, restoreStorename, restorePortFinder,
                        KVLite.DEFAULT_NUM_PARTITIONS + 10,
                        getRestoreMountPoint(), policyMap);
    }

    protected StoreUtils createRestoreStoreUtils() {
        if (SECURITY_ENABLE) {
            return new StoreUtils(restoreStorename, testhost,
                restorePortFinder.getRegistryPort(),
                getKVLiteLoginFilePath(restoreRoot),
                StoreUtils.RecordType.UUID, 0, null,
                    null);
        }
        return new StoreUtils
            (restoreStorename, testhost, restorePortFinder.getRegistryPort(),
             StoreUtils.RecordType.UUID);
    }

    protected long loadToRestore(File from)
        throws Exception {

        return loadToRestore(new File[]{from}, null, null,
                            null);
    }

    protected long loadToRestore(File[] sourceDirs,
                                 String statusDir,
                                 PrintStream out,
                                 String securityFile)
        throws Exception {

        /**
         * If debugging of Load is required, change the parameters to set the
         * verbose state and pass a PrintStream (System.err).
         */
        Load load = new Load(sourceDirs,
                             restoreStorename,
                             testhost,
                             restorePortFinder.getRegistryPort(),
                             null, securityFile, statusDir,
                             false, out);

        return load.run();
    }

    protected void loadMetadataToRestore(File sourceDirs,
                                         PrintStream out,
                                         String securityFile,
                                         boolean forceLoad)
        throws Exception {

        /**
         * If debugging of Load is required, change the parameters to set the
         * verbose state and pass a PrintStream (System.err).
         */
        Load.loadAdmin(sourceDirs,
                       testhost,
                       restorePortFinder.getRegistryPort(),
                       null /* user */,
                       securityFile,
                       false /* verbose */,
                       forceLoad,
                       out);
    }

    /**
     * Returns a list of snapshot directories, one per RepGroup (shard).
     */
    List<File> getBackupRepNodeSnapshotDirs(CreateStore createStore,
                                            String snapshotName) {

        ArrayList<File> list = new ArrayList<File>();
        Set<String> groups = new HashSet<String>();

        for (StorageNodeId snid : createStore.getStorageNodeIds()) {
            List<RepNodeId> rns = createStore.getRNs(snid);
            for (RepNodeId rnid : rns) {
                String group = rnid.getGroupName();
                if (groups.contains(group)) {
                    continue;
                }
                groups.add(group);
                list.add(new File
                         (FileNames.getSnapshotDir(createStore.getRootDir(),
                                                   createStore.getStoreName(),
                                                   null,
                                                   snid,
                                                   rnid), snapshotName));
            }
        }
        return list;
    }

    protected String getBackupMountPoint() {
        if (useMountPoint) {
            return backupMount.getAbsolutePath();
        }
        return null;
    }

    private String getRestoreMountPoint() {
        if (useMountPoint) {
            return restoreMount.getAbsolutePath();
        }
        return null;
    }

    private Snapshot getBackupSnapshot()
        throws Exception {

        ensureBackupAdmin();
        return new Snapshot(backupAdmin, false, null);
    }

    private void ensureBackupAdmin() {
        while (backupAdmin == null) {
            try {
                if (SECURITY_ENABLE) {
                    KVStoreLogin login = new KVStoreLogin(
                        null, getKVLiteLoginFilePath(backupRoot));
                    login.loadSecurityProperties();
                    final LoginCredentials loginCreds =
                        login.makeShellLoginCredentials();
                    login.prepareRegistryCSF();

                    loginMgr =
                        KVStoreLogin.getAdminLoginMgr(
                            testhost, backupPortFinder.getRegistryPort(),
                            loginCreds, logger);
                }
                backupAdmin = RegistryUtils.getAdmin(
                    testhost, backupPortFinder.getRegistryPort(), loginMgr,
                    logger);
            } catch (Exception ignored) {
               
            }
        }
    }

    private KVLite createStore(File root,
                               String storename,
                               PortFinder portFinder,
                               int numPartitions,
                               String mp,
                               ParameterMap policyMap) {
        return createStore(root, storename, portFinder,
                           numPartitions, mp, policyMap,
                           null);
    }

    public KVLite createStore(File root,
                               String storename,
                               PortFinder portFinder,
                               int numPartitions,
                               String mp,
                               ParameterMap policyMap,
                               String restoreSnapshotName) {

        KVLite kvlite = new KVLite(root.toString(),
                                   storename,
                                   portFinder.getRegistryPort(),
                                   true,
                                   testhost,
                                   portFinder.getHaRange(),
                                   null,
                                   numPartitions,
                                   mp,
                                   true,
                                   SECURITY_ENABLE,
                                   restoreSnapshotName,
                                   -1);
        kvlite.setVerbose(false);
        kvlite.setPolicyMap(mergeParameterMapDefaults(policyMap));
        kvlite.start(true); /* wait for services before returning */
        return kvlite;
    }

    /*
     * Make a local 3x1 store.
     */
    private CreateStore create3x1Store(File root,
                                       String storename,
                                       PortFinder portFinder,
                                       int numPartitions,
                                       ParameterMap policyMap)
            throws Exception {

        CreateStore createStore =
            new CreateStore(storename,
                            portFinder.getRegistryPort(),
                            1, /* n SNs */
                            1, /* rf */
                            numPartitions, /* n partitions */
                            3, /* capacity per SN */
                            3 * CreateStore.MB_PER_SN,
                            false, /* use threads is false */
                            null);
        createStore.setRootDir(root.toString());
        createStore.setPolicyMap(policyMap);
        createStore.start();
        return createStore;
    }

    protected String getKVLiteLoginFilePath(File rootDir) {
        if (!SECURITY_ENABLE) {
            return null;
        }
        final File secDir = new File(rootDir, FileNames.SECURITY_CONFIG_DIR);
        return new File(secDir, FileNames.USER_SECURITY_FILE).getPath();
    }
}
