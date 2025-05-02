/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv;

import static java.util.concurrent.TimeUnit.SECONDS;
import static oracle.kv.KVLocalTestModeBase.TestMode;
import static oracle.kv.KVLocalTestModeBase.checkUnixDomainSocketsSupported;
import static oracle.kv.KVLocalTestModeBase.getConfigBuilder;
import static oracle.kv.impl.util.VersionUtil.getJavaMajorVersion;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import oracle.kv.impl.util.FileUtils;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.registry.AsyncControl;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.util.kvlite.KVLite;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Exercise KVLocal.
 */

/* Increase test timeout to 40 minutes -- test can take 30 minutes.*/
@TestClassTimeoutMillis(40*60*1000)
public class KVLocalTest extends TestBase {
    private static final boolean java16 = getJavaMajorVersion() >= 16;
    private static final boolean deleteTestDirOnExit = true;

    private KVLocal local;
    private File testDir;

    @BeforeClass
    public static void ensureAsyncEnabled() {
        assumeTrue("KVLocal requires async", AsyncControl.serverUseAsync);
    }

    @Override
    public void setUp() throws Exception {
        suppressSystemError();
        suppressSystemOut();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        /* Cleanup running store */
        if (local != null) {
            try {
                local.stop();
            } catch (Exception e) {
            }
            local = null;
        }
        resetSystemError();
        resetSystemOut();
        if (deleteTestDirOnExit && (testDir != null)) {
            FileUtils.deleteDirectory(testDir);
        }
    }

    @Test
    public void testInetBuilder() throws Exception {
        checkException(() -> new KVLocalConfig.InetBuilder(""),
                       IllegalArgumentException.class,
                       "The root directory cannot be null or empty");

        checkException(() -> new KVLocalConfig.InetBuilder(null),
                       IllegalArgumentException.class,
                       "The root directory cannot be null or empty");

        final KVLocalConfig.Builder builder =
            new KVLocalConfig.InetBuilder("kvrootDir");

        checkException(() -> builder.setStoreName(""),
                       IllegalArgumentException.class,
                       "Store name cannot be null or empty");

        checkException(() -> builder.setStoreName(null),
                       IllegalArgumentException.class,
                       "Store name cannot be null or empty");

        checkException(() -> builder.setHostName(""),
                       IllegalArgumentException.class,
                       "Host name cannot be null or empty");

        checkException(() -> builder.setHostName(null),
                       IllegalArgumentException.class,
                       "Host name cannot be null or empty");

        checkException(() -> builder.setPort(-1),
                       IllegalArgumentException.class,
                       "Illegal port");

        checkException(() -> builder.setMemoryMB(0),
                       IllegalArgumentException.class,
                       "Memory size must not be less");

        checkException(() -> builder.setStorageGB(-1),
                       IllegalArgumentException.class,
                       "Illegal storage directory size");

        /* Verify default values */
        KVLocalConfig config =
            new KVLocalConfig.InetBuilder("kvrootDir").build();
        assertEquals(5000, config.getPort());
        assertTrue(config.isSecure());
        assertEquals("kvstore", config.getStoreName());
        assertEquals("localhost", config.getHostName());
        assertEquals(8192, config.getMemoryMB());
        assertEquals(10, config.getStorageGB());
        assertEquals("kvrootDir", config.getRootDirectory());
        assertFalse(config.isUnixDomain());
        assertEquals("<KVLocalConfig kvroot=kvrootDir storeName=kvstore " +
                     "hostName=localhost memoryMB=8192 storageGB=10 " +
                     "port=5000 isSecure=true isUnixDomain=false>",
                     config.toString());

        config = new KVLocalConfig.InetBuilder("anotherRootDir")
                    .setPort(3000)
                    .isSecure(false)
                    .setStoreName("anotherStore")
                    .setHostName("anotherHost")
                    .setStorageGB(20)
                    .setMemoryMB(256)
                    .build();
        assertEquals(3000, config.getPort());
        assertFalse(config.isSecure());
        assertEquals("anotherStore", config.getStoreName());
        assertEquals("anotherHost", config.getHostName());
        assertEquals(256, config.getMemoryMB());
        assertEquals(20, config.getStorageGB());
        assertEquals("anotherRootDir", config.getRootDirectory());
        assertFalse(config.isUnixDomain());
    }

    @Test
    public void testUnixDomainBuilder() throws Exception {
        checkException(() -> new KVLocalConfig.UnixDomainBuilder(""),
                       IllegalArgumentException.class,
                       "The root directory cannot be null or empty");

        checkException(() -> new KVLocalConfig.UnixDomainBuilder(null),
                       IllegalArgumentException.class,
                       "The root directory cannot be null or empty");

        final KVLocalConfig.Builder builder =
            new KVLocalConfig.UnixDomainBuilder("kvrootDir");

        checkException(() -> builder.setStoreName(""),
                       IllegalArgumentException.class,
                       "Store name cannot be null or empty");

        checkException(() -> builder.setStoreName(null),
                       IllegalArgumentException.class,
                       "Store name cannot be null or empty");

        checkException(() -> builder.setHostName("host"),
                       UnsupportedOperationException.class,
                       "UnixDomainBuilder does not support setHostName");

        checkException(() -> builder.setPort(6000),
                       UnsupportedOperationException.class,
                       "UnixDomainBuilder does not support setPort");

        checkException(() -> builder.isSecure(false),
                       UnsupportedOperationException.class,
                       "UnixDomainBuilder does not support isSecure");

        checkException(() -> builder.setMemoryMB(0),
                       IllegalArgumentException.class,
                       "Memory size must not be less");

        checkException(() -> builder.setStorageGB(-1),
                       IllegalArgumentException.class,
                       "Illegal storage directory size");

        /* Verify default values */
        KVLocalConfig config =
            new KVLocalConfig.UnixDomainBuilder("kvrootDir").build();
        assertEquals(5000, config.getPort());
        assertFalse(config.isSecure());
        assertEquals("kvstore", config.getStoreName());
        assertEquals("unix_domain:kvrootDir/sockets/sock",
                     config.getHostName());
        assertEquals(8192, config.getMemoryMB());
        assertEquals(10, config.getStorageGB());
        assertEquals("kvrootDir", config.getRootDirectory());
        assertTrue(config.isUnixDomain());
        assertEquals("<KVLocalConfig kvroot=kvrootDir storeName=kvstore " +
                     "memoryMB=8192 storageGB=10 isUnixDomain=true>",
                     config.toString());

        config = new KVLocalConfig.UnixDomainBuilder("anotherRootDir")
                    .setStoreName("anotherStore")
                    .setStorageGB(20)
                    .setMemoryMB(256)
                    .build();
        assertEquals(5000, config.getPort());
        assertFalse(config.isSecure());
        assertEquals("anotherStore", config.getStoreName());
        assertEquals("unix_domain:anotherRootDir/sockets/sock",
                     config.getHostName());
        assertEquals(256, config.getMemoryMB());
        assertEquals(20, config.getStorageGB());
        assertEquals("anotherRootDir", config.getRootDirectory());
        assertTrue(config.isUnixDomain());
    }

    @Test
    public void testStartInetSecure() throws Exception {
        testStart(TestMode.INET_SECURE);
    }

    @Test
    public void testStartInetNonsecure() throws Exception {
        testStart(TestMode.INET_NONSECURE);
    }

    @Test
    public void testStartUnixDomain() throws Exception {
        checkUnixDomainSocketsSupported(TestMode.UNIX_DOMAIN);
        testStart(TestMode.UNIX_DOMAIN);
    }

    private void testStart(TestMode testMode) throws Exception {
        String rootDir = makeTestDir("start");
        /* The KVLocalConfig cannot be null */
        checkException(() -> KVLocal.start(null),
                       IllegalArgumentException.class,
                       "The KVLocalConfig cannot be null");

        final KVLocalConfig config = getConfigBuilder(rootDir, testMode)
            .build();
        local = KVLocal.start(config);
        assertTrue(local.isRunning());

        /* 2nd start() should fail */
        checkException(() -> KVLocal.start(config),
                       IllegalStateException.class,
                       "Only one instance of embedded NoSQL database" +
                       " can be running");
        local.stop();
        assertTrue(!local.isRunning());

        /* Stop an already stopped KVLocal */
        local.stop();

        /* Stop a KVLocal that has no embedded store */
        {
            KVLocal local2 = KVLocal.getKVLocal(rootDir);
            checkException(() -> local2.stop(),
                           IllegalStateException.class,
                           "not started as an embedded instance");
        }

        /* Wrong root dir */
        {
            String rootDir2 = makeTestDir("start2");
            FileUtils.copyDir(new File(rootDir), new File(rootDir2));
            KVLocalConfig config2 = getConfigBuilder(rootDir2, testMode)
                .build();
            checkException(
                () -> KVLocal.start(config2),
                IllegalArgumentException.class,
                "does not match the existing store's root directory");
        }

        /* Wrong store name */
        {
            KVLocalConfig config2 = getConfigBuilder(rootDir, testMode)
                .setStoreName("another-store")
                .build();
            checkException(() -> KVLocal.start(config2),
                           IllegalArgumentException.class,
                           "does not match the existing store's storename");
        }

        /* KVLocalConfig port doesn't match existing store's port. */
        {
            if (testMode != TestMode.UNIX_DOMAIN) {
                KVLocalConfig config2 = getConfigBuilder(rootDir, testMode)
                    .setPort(6000)
                    .build();
                checkException(() -> KVLocal.start(config2),
                               IllegalArgumentException.class,
                               "does not match the existing store's port");
            }
        }

        /* KVLocalConfig hostName doesn't match existing store's hostName. */
        {
            if (testMode != TestMode.UNIX_DOMAIN) {
                KVLocalConfig config2 = getConfigBuilder(rootDir, testMode)
                    .setHostName("kvstoreException")
                    .build();
                checkException(() -> KVLocal.start(config2),
                               IllegalArgumentException.class,
                               "does not match the existing store's hostname");
            }
        }

        /* KVLocalConfig security doesn't match existing store's security. */
        {
            if (testMode == TestMode.INET_SECURE) {
                KVLocalConfig config2 = getConfigBuilder(rootDir, testMode)
                    .isSecure(false)
                    .build();
                checkException(() -> KVLocal.start(config2),
                               IllegalArgumentException.class,
                               "does not match the existing store, " +
                               "which has security enabled");
            }
        }

        /* Wrong memoryMB */
        {
            KVLocalConfig config2 = getConfigBuilder(rootDir, testMode)
                .setMemoryMB(12345)
                .build();
            checkException(() -> KVLocal.start(config2),
                           IllegalArgumentException.class,
                           "does not match the existing store's memoryMB");
        }

        /* Wrong storageGB */
        {
            KVLocalConfig config2 = getConfigBuilder(rootDir, testMode)
                .setStorageGB(42)
                .build();
            checkException(() -> KVLocal.start(config2),
                           IllegalArgumentException.class,
                           "does not match the existing store's storageGB");
        }

        /* Root directory's parent directory does not exist
         * Note: the directory "/nonexist/parentdir" should not exist in
         * the machine where this test script is run.
         */
        {
            KVLocalConfig config2 =
                getConfigBuilder("/nonexist/parentdir/rootdir", testMode)
                .build();
            checkException(() -> KVLocal.start(config2),
                           KVLocalException.class,
                           "Problem creating root directory");
        }
        local = null;
    }

    @Test
    public void testStartExistingStoreInetSecure() throws Exception {
        testStartExistingStore(TestMode.INET_SECURE);
    }

    @Test
    public void testStartExistingStoreInetNonsecure() throws Exception {
        testStartExistingStore(TestMode.INET_NONSECURE);
    }

    @Test
    public void testStartExistingStoreUnixDomain() throws Exception {
        checkUnixDomainSocketsSupported(TestMode.UNIX_DOMAIN);
        testStartExistingStore(TestMode.UNIX_DOMAIN);
    }

    private void testStartExistingStore(TestMode testMode) throws IOException {

        String rootDirStr = makeTestDir("startExist");
        /* startExistingStore() should fail if no store directory was found */
        try  {
            KVLocal.startExistingStore(rootDirStr);
        } catch(IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage().contains
            ("Configuration file of the existing store was not found"));
        }

        KVLocalConfig config = getConfigBuilder(rootDirStr, testMode)
                .build();
        local = KVLocal.start(config);
        local.stop();

        /* 2nd startExistingStore() should fail */
        local = KVLocal.startExistingStore(rootDirStr);
        try {
            KVLocal.startExistingStore(rootDirStr);
            fail("2nd startExistingStore didn't fail");
        } catch(IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage()
                .contains("Only one instance of embedded NoSQL database" +
                " can be running"));
        }
        local.stop();

        /* startExistingStore() should set KVLocal configuration parameters
         * correctly
         */
        local = KVLocal.startExistingStore(rootDirStr);
        local.stop();

        /* No root directory exists */
        String nonExistentRoot = makeTestDir("non-existent");
        try {
            KVLocal.startExistingStore(nonExistentRoot);
            fail("startExistingStore() didn't fail when no root directory" +
                 " was found");
        } catch(IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage().contains
                ("Configuration file of the existing store was not found"));
        }

        /* Root directory is empty */
        File rootDir = new File(nonExistentRoot);
        rootDir.mkdir();
        try {
            KVLocal.startExistingStore(nonExistentRoot);
            fail("startExistingStore() didn't fail when root directory" +
                 " was empty");
        } catch(IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage().contains
                ("Configuration file of the existing store was not found"));
        }

        /* start() should reuse the empty root directory */
        config = getConfigBuilder(nonExistentRoot, testMode)
            .build();
        local = KVLocal.start(config);
        local.stop();

        /* startExistingStore() should fail if kvroot/kvstore directory
         * is missing
         */
        File kvstorePath = new File(nonExistentRoot+File.separator+"kvstore");
        FileUtils.deleteDirectory(kvstorePath);
        checkException(() -> KVLocal.startExistingStore(nonExistentRoot),
                       IllegalStateException.class,
                       "Store directory was not found");
        local = null;
    }

    @Test
    public void testSecurity() throws Exception {
        String rootDir = makeTestDir("secure");
        KVLocalConfig config1 =
            new KVLocalConfig.InetBuilder(rootDir)
                             .isSecure(true)
                             .build();
        local = KVLocal.start(config1);
        loadData(UUID.randomUUID().toString());
        local.verifyConfiguration(false);
        local.stop();
        local = KVLocal.startExistingStore(rootDir);
        local.stop();

        rootDir = makeTestDir("nonsecure");
        KVLocalConfig config2 =
            new KVLocalConfig.InetBuilder(rootDir)
                            .isSecure(false)
                            .build();

        local = KVLocal.start(config2);
        loadData(UUID.randomUUID().toString());
        local.verifyConfiguration(false);
        local.stop();
        local = KVLocal.startExistingStore(rootDir);
        local.stop();
        local = null;
    }

    @Test
    public void testDiagnosticToolInetSecure() throws Exception {
        testDiagnosticTool(TestMode.INET_SECURE);
    }

    @Test
    public void testDiagnosticToolInetNonsecure() throws Exception {
        testDiagnosticTool(TestMode.INET_NONSECURE);
    }

    @Test
    public void testDiagnosticToolUnixDomain() throws Exception {
        checkUnixDomainSocketsSupported(TestMode.UNIX_DOMAIN);
        testDiagnosticTool(TestMode.UNIX_DOMAIN);
    }

    private void testDiagnosticTool(TestMode testMode) throws Exception {
        String rootDir = makeTestDir("diagnostic");
        KVLocalConfig config = getConfigBuilder(rootDir, testMode).build();
        local = KVLocal.start(config);

        KVLocal handle = KVLocal.getKVLocal(rootDir);
        String verifyVerbose = handle.verifyConfiguration(true);
        assertTrue(verifyVerbose.contains("Operation ends successfully"));
        String verifyNonVerbose = handle.verifyConfiguration(false);
        assertTrue(verifyNonVerbose .equals("{}"));

        String verifyDataRes = handle.verifyData();
        assertTrue(verifyDataRes.contains("No Btree Corruptions"));
        assertTrue(verifyDataRes.contains("No Log File Corruptions"));
        local.stop();

        /* Verify configuration when store is not running. */
        try {
            handle.verifyConfiguration(true);
            fail("verifyConfiguration executed when store is not running");
        } catch(KVLocalException e) {
            assertTrue(e.getMessage(), e.getMessage().contains
                ("Exception in getting admin command service," +
                " maybe the store is not running"));
        }
        /* Verify data when store is not running. */
        try {
            handle.verifyData();
            fail("verifyData executed when store is not running");
        } catch(KVLocalException e) {
            assertTrue(e.getMessage(), e.getMessage().contains
            ("Exception in getting admin command service," +
            " maybe the store is not running"));
        }
        local = null;
    }

    @Test
    public void testSnapshotInetSecure() throws Exception {
        testSnapshot(TestMode.INET_SECURE);
    }

    @Test
    public void testSnapshotInetNonsecure() throws Exception {
        testSnapshot(TestMode.INET_NONSECURE);
    }

    @Test
    public void testSnapshotUnixDomain() throws Exception {
        checkUnixDomainSocketsSupported(TestMode.UNIX_DOMAIN);
        testSnapshot(TestMode.UNIX_DOMAIN);
    }

    private void testSnapshot(TestMode testMode) throws Exception {
        String rootDir = makeTestDir("testSnapshot");
        KVLocalConfig config = getConfigBuilder(rootDir, testMode).build();
        local = KVLocal.start(config);
        String randomBefore = UUID.randomUUID().toString();
        loadData(randomBefore);
        String snapshotName1 = local.createSnapshot("sp1");
        String randomAfter = UUID.randomUUID().toString();
        loadData(randomAfter);
        String[] snapshotNames = local.listSnapshots();
        assertTrue(snapshotNames[0].equals(snapshotName1));
        local.stop();

        /*
         * Restore from snapshot. Verify the following:
         * The data created before snapshot should exist.
         * The data created after snapshot should not exist.
         */
        local = KVLocal.restoreFromSnapshot(rootDir, snapshotName1);
        assertTrue(verifyDataExisted(randomBefore));
        assertFalse(verifyDataExisted(randomAfter));

        /* Restore from a snapshot when store is already running */
        try {
            KVLocal.restoreFromSnapshot(rootDir, snapshotName1);
            fail("Restore from Snapshot when there is an already running" +
                 " store");
        } catch(IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage()
                .contains("Only one instance of embedded NoSQL database" +
                " can be running"));
        }
        local.stop();

        /* Restore from a non-existent snapshot */
        checkException(() -> KVLocal.restoreFromSnapshot(
                           rootDir, "non-exist-snapshot"),
                       IllegalStateException.class,
                       "Snapshot directory not found");

        /*
         * Start store again. Verify the following:
         * The data created before snapshot should exist.
         * The data created after snapshot should not exist.
         */
        local = KVLocal.start(config);
        assertTrue(verifyDataExisted(randomBefore));
        assertFalse(verifyDataExisted(randomAfter));

        /* Remove snapshot. */
        local.removeSnapshot(snapshotName1);
        snapshotNames = local.listSnapshots();
        assertTrue(snapshotNames.length == 0);

        /* Remove a non-existent snapshot should complete with no error */
        local.removeSnapshot(snapshotName1);
        local.stop();

        /* Create snapshot when store is not running */
        {
            KVLocal localFinal = local;
            checkException(() -> localFinal.createSnapshot("sp2"),
                           KVLocalException.class,
                           "maybe the store is not running");
        }
        local = null;
    }

    @Test
    public void testGetStoreInetSecure() throws Exception {
        testGetStore(TestMode.INET_SECURE);
    }

    @Test
    public void testGetStoreInetNonsecure() throws Exception {
        testGetStore(TestMode.INET_NONSECURE);
    }

    @Test
    public void testGetStoreUnixDomain() throws Exception {
        checkUnixDomainSocketsSupported(TestMode.UNIX_DOMAIN);
        testGetStore(TestMode.UNIX_DOMAIN);
    }

    private void testGetStore(TestMode testMode) throws Exception {
        String rootDir = makeTestDir("getStore");
        KVLocalConfig config1 = getConfigBuilder(rootDir, testMode).build();

        local = KVLocal.start(config1);
        KVStore storeHandle1 = local.getStore();
        testKeyValueAPI(storeHandle1, UUID.randomUUID().toString());
        TableAPI tableAPI1 = local.getStore().getTableAPI();
        testTableAPI(tableAPI1, UUID.randomUUID().toString());

        /* stop store and start it again, the store handle and tableAPI handle
         * should still work
         */
        local.stop();
        local = KVLocal.start(config1);
        testKeyValueAPI(storeHandle1, UUID.randomUUID().toString());
        testTableAPI(tableAPI1, UUID.randomUUID().toString());

        /*
         * Close store via KVStore.close() API. The storeHandle1 should
         * signal error.
         */
        storeHandle1.close();
        try {
            testKeyValueAPI(storeHandle1, UUID.randomUUID().toString());
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage().contains
            ("Request dispatcher has been shutdown"));
        }
        local.closeStore();
        local.stop();

        /* Invoke getStore when store is not running. */
        checkException(() -> local.getStore(),
                       FaultException.class,
                       (testMode == TestMode.INET_SECURE) ?
                       "Could not establish an initial login" :
                       "Could not contact any RepNode");

        /* Invoke key/value API when store is not running */
        try {
            testKeyValueAPI(storeHandle1, UUID.randomUUID().toString());
            fail("key/value API didn't fail");
        } catch(IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage().contains
                ("Request dispatcher has been shutdown"));
        }

        /* Invoke TableAPI when KVLocal is not running. */
        try {
            testTableAPI(tableAPI1, UUID.randomUUID().toString());
            fail("TableAPI didn't fail");
        } catch(IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage().contains
                ("Request dispatcher has been shutdown"));
        }

        rootDir = makeTestDir("getStore2");

        /* Configure a store in an incompatible mode */
        final TestMode otherMode;
        switch (testMode) {
        case INET_SECURE:
            otherMode = TestMode.INET_NONSECURE;
            break;
        case INET_NONSECURE:
            otherMode = java16 ? TestMode.UNIX_DOMAIN : TestMode.INET_SECURE;
            break;
        case UNIX_DOMAIN:
            otherMode = TestMode.INET_SECURE;
            break;
        default:
            throw new AssertionError();
        }
        KVLocalConfig config2 = getConfigBuilder(rootDir, otherMode).build();

        local = KVLocal.start(config2);
        KVStore storeHandle2 = local.getStore();
        testKeyValueAPI(storeHandle2, UUID.randomUUID().toString());
        TableAPI tableAPI2 = local.getStore().getTableAPI();
        testTableAPI(tableAPI2, UUID.randomUUID().toString());
        local.closeStore();
        local.stop();
        /* Invoke getStore when store is not running. */
        checkException(() -> local.getStore(),
                       FaultException.class,
                       (otherMode == TestMode.INET_SECURE) ?
                       "Could not establish an initial login" :
                       "Could not contact any RepNode");

        /* Invoke closeStore when store is not running. */
        storeHandle2.close();
        storeHandle2.close();

        local = null;
    }

    /**
     * Test starting KVLocal when there is an existing KVLite running on the
     * same port.
     */
    @Test
    public void testGetStoreRunningKVLite() throws Exception {
        final String rootDir = makeTestDir("getStoreRunningKVLite");

        /*
         * Use a different store name because it seems that KVLite modifies the
         * default client socket factory otherwise, which causes trouble for
         * KVLocal
         */
        final String storeName = "kvlite";

        /* Create a separate KVLite */
        final KVLite kvlite = new KVLite(
            rootDir,
            storeName,
            KVLocalConfig.DEFAULT_PORT,
            true /* runBootAdmin */,
            KVLocalConfig.InetBuilder.DEFAULT_HOSTNAME,
            null /* haPortRange */,
            null /* servicePortRange */,
            1 /* numPartitions */,
            null /* mountPoint */,
            true /* useThreads */,
            true /* isSecure */,
            null /* restoreSnapshotName */);
        kvlite.setMemoryMB(KVLocalConfig.DEFAULT_MEMORY_SIZE_MB);
        kvlite.setStorageSizeGB(KVLocalConfig.DEFAULT_STORAGE_SIZE_GB);
        kvlite.setVerbose(false);

        /* Start it and wait for it to be ready */
        tearDowns.add(() -> kvlite.stop(false));
        kvlite.start(true /* waitForServices */);

        /* Wait for the user security file to be created */
        final File securityPath = new File(KVLocal.getSecurityPath(rootDir));
        assertTrue(PollCondition.await(1000, 30000, securityPath::exists));

        /* Create KVLocal, but don't start it, and get KVStore */
        final KVLocal kvlocal = KVLocal.getKVLocal(rootDir);

        /* Wait for the store to be working */
        assertTrue(
            PollCondition.await(
                1000, 30000,
                () -> {
                    try {
                        return KVLocal.isStoreReady(kvlocal.getStore());
                    } catch (IllegalStateException e) {
                        return false;
                    }
                }));

        /*
         * Try starting KVLocal, should fail more quickly than the 100 second
         * start up timeout because it should detect that the service is not
         * coming up.
         */
        final KVLocalConfig config =
            new KVLocalConfig.InetBuilder(rootDir)
            .setStoreName(storeName)
            .build();
        CompletableFuture.runAsync(
            () -> checkException(() -> KVLocal.start(config),
                                 KVLocalException.class,
                                 "Service was not started"))
            .get(30, SECONDS);

        /* Try with a KVLocal in another directory */
        final String rootDir2 = makeTestDir("getStoreRunningKVLite2");
        final KVLocalConfig config2 =
            new KVLocalConfig.InetBuilder(rootDir2).build();
        CompletableFuture.runAsync(
            () -> checkException(() -> KVLocal.start(config2),
                                 KVLocalException.class,
                                 "Service was not started"))
            .get(30, SECONDS);
    }

    /* Basic test */
    @Test
    public void testUnixDomainSocket() throws Exception {
        final String rootDir = makeTestDir("unixDomain");
        final KVLocalConfig config =
            new KVLocalConfig.UnixDomainBuilder(rootDir).build();
        if (!java16) {
            checkException(() -> KVLocal.start(config),
                           IllegalStateException.class,
                           "Starting KVLocal using Unix domain sockets" +
                           " requires Java 16");
        } else {
            local = KVLocal.start(config);
        }
    }

    /** Test using a too-long directory name [KVSTORE-1478] */
    @Test
    public void testUnixDomainSocketDirTooLong() throws IOException {

        /*
         * Create a test directory name that is long enough to exceed the Unix
         * domain socket pathname limit -- about 100 -- but not too long that
         * the file system can't create the directory.
         */
        final char[] chars = new char[100];
        Arrays.fill(chars, 'a');
        final String rootDir = makeTestDir("long-" + new String(chars));
        final KVLocalConfig config =
            new KVLocalConfig.UnixDomainBuilder(rootDir).build();
        if (!java16) {
            checkException(() -> KVLocal.start(config),
                           IllegalStateException.class,
                           "Starting KVLocal using Unix domain sockets" +
                           " requires Java 16");
        } else {
            checkException(() -> { local = KVLocal.start(config); },
                           KVLocalException.class,
                           "path too long");
        }
    }

    private void testKeyValueAPI(KVStore store, String random)
        throws Exception {

        final Key key = Key.createKey("key: " + random);
        final String valueString = "value: " + random;
        final Value value = Value.createValue(valueString.getBytes());
        store.put(key, value);

        final ValueVersion valueVersion = store.get(key);
        assertEquals(value, valueVersion.getValue());

        String statement = "CREATE TABLE if not exists employee (" +
                           "id STRING, " +
                           "firstName STRING, " +
                           "lastName STRING, " +
                           "PRIMARY KEY (id))";

        ExecutionFuture future = store.execute(statement);
        StatementResult statementRes = future.get();
        if (future != null) {
            assertTrue(future.isDone());
            assertFalse(future.isCancelled());
        }
        assertTrue(statementRes.toString(), statementRes.isSuccessful());
        assertTrue(statementRes.isDone());
    }

    private void testTableAPI(TableAPI tableAPI, String random) {
        Table employee = tableAPI.getTable("employee");
        Row row = employee.createRow();
        row.put("id", random);
        row.put("firstName", "firstName"+random);
        row.put("lastName", "lastName"+random);
        tableAPI.put(row, null, null);

        final PrimaryKey key = employee.createPrimaryKey();
        key.put("id", random);
        final String value = tableAPI.get(key, null).get("firstName")
                            .asString().get();
        assertEquals("firstName" + random, value);
    }

   private void loadData(String random) throws Exception {
       KVStore store = local.getStore();
       testKeyValueAPI(store, random);
       TableAPI tableAPI = local.getStore().getTableAPI();
       testTableAPI(tableAPI, random);
       local.closeStore();
   }

   /**
    * Verify whether the employee record with the specified key existed.
    * Return true if record exists, false otherwise.
    */
   private boolean verifyDataExisted(String key) throws Exception {
       TableAPI tableAPI = local.getStore().getTableAPI();
       Table employee = tableAPI.getTable("employee");
       final PrimaryKey primaryKey = employee.createPrimaryKey();
       primaryKey.put("id", key);
       final Row row = tableAPI.get(primaryKey, null);
       return (row != null);
   }

   private String makeTestDir(String subDir) throws IOException {
       testDir = Files.createTempDirectory(subDir).toFile();
       return testDir.toString();
   }
}
