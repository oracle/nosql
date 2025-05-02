/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.sna;

import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.metadata.KVStoreUser;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.test.RemoteTestAPI;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.FilterableParameterized;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.AsyncControl;
import oracle.kv.impl.util.registry.RegistryUtils;

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests the RepNode operations of StorageNodeAgentImpl.
 */
@RunWith(FilterableParameterized.class)
public class StorageNodeRepNode extends StorageNodeTestBase {

    private static final String SEC_ID = "md-" + System.currentTimeMillis();
    private Topology topo;
    private SecurityMetadata sec;

    public StorageNodeRepNode(boolean useThreads, boolean useMountPoints) {
        super(useThreads);
        this.useMountPoints = useMountPoints;
    }

    /**
     * Override superclass genParams method to provide default values for
     * use_thread and use_mountpoint that are tests parameterized to run
     * in thread/process modes and use/not use mount point configuration.
     */
    @Parameters(name="Use_Thread={0},Use_MountPoint={1}")
    public static List<Object[]> genParams() {
        if (PARAMS_OVERRIDE != null) {
            return PARAMS_OVERRIDE;
        }
        return Arrays.asList(new Object[][] {{true, false},
                                             {true, true},
                                             {false, true},
                                             {false, false}});
    }

    /**
     * Notes: It is required to call the super methods if override
     * setUp and tearDown methods.
     */
    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        topo = createTopology(testhost, replicationFactor);
        sec = new SecurityMetadata(kvstoreName, SEC_ID);
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        StorageNodeAgent.rnShutdownHook = null;
        // oracle.kv.impl.async.AsyncTestUtils.checkActiveDialogTypes();
    }

    /**
     * Start, register, create a single RepNodeService
     */
    @Test
    public void testBasicRepNode()
        throws Exception {

        RepNodeId[] rnids = new RepNodeId[replicationFactor];
        RepNodeParams[] rnps = new RepNodeParams[replicationFactor];
        ParameterMap[] ps = new ParameterMap[replicationFactor];
        initRepNodes(topo, rnids, rnps, ps);
        StorageNodeId snid = topo.get(rnids[0]).getStorageNodeId();

        StorageNodeAgentAPI snai =
            createRegisteredStore(snid, false);

        /* test SNAInterface.repNodeExists() call */
        assert(snai.repNodeExists(rnids[0]) == false);

        assertTrue(snai.createRepNode(ps[0], getMetadataSet()));

        waitForRNAdmin(rnids[0]);

        /* test SNAInterface.repNodeExists() call */
        assert(snai.repNodeExists(rnids[0]));

        /* test that a second create returns true (idempotency) */
        assertTrue(snai.createRepNode(ps[0], getMetadataSet()));

        snai.shutdown(true, true);
    }

    /**
     * Start, register, create multiple RepNodes.  Add a stop/start on one of
     * them for good measure.
     */
    @Test
    public void testMultipleRepNode()
        throws Exception {

        RepNodeId[] rnids = new RepNodeId[2];
        StorageNodeAgentAPI snai = startRepNodes(topo, rnids, 2);

        RepNodeAdminAPI rnai0 = waitForRNAdmin(rnids[0]);
        RepNodeAdminAPI rnai1 = waitForRNAdmin(rnids[1]);

        /**
         * Stop and restart one of them.
         */
        assertTrue(snai.stopRepNode(rnids[1], true, false));
        assertShutdown(rnai1);
        assertFalse(snai.stopRepNode(rnids[1], true, false));
        snai.startRepNode(rnids[1]);
        assertTrue(snai.startRepNode(rnids[1]));

        rnai1 = waitForRNAdmin(rnids[1]);
        assertTrue(snai.startRepNode(rnids[1]));

        snai.shutdown(true, true);
        awaitShutdown(rnai0);
        awaitShutdown(rnai1);
    }

    /**
     * Test stopping a RepNode.  Start 3, stop one.  Shutdown, restart.  This
     * should exercise the code to change a RepNode's state to stopped as well
     * as the SNA startup in registered mode.  It is different from the test
     * above because it leaves one of the 3 in stopped state and starts the SNA
     * from scratch in registered mode.
     */
    @Test
    public void testStopRestartRepNode()
        throws Exception {

        RepNodeId[] rnids = new RepNodeId[3];
        StorageNodeAgentAPI snai = startRepNodes(topo, rnids, 3);
        StorageNodeId snid = topo.get(rnids[0]).getStorageNodeId();
        RepNodeAdminAPI rnai1 = waitForRNAdmin(rnids[1]);
        /**
         * Assert that RN1 is running; stop it; assert that it's not running.
         */
        assertStopped(rnids[1], topo, snid, false);
        assertTrue(snai.stopRepNode(rnids[1], true, false));
        assertShutdown(rnai1);
        assertStopped(rnids[1], topo, snid, true);

        /**
         * Stop and restart SNA; it will restart rnid0 and rnid2, not rnid1.
         */
        snai.shutdown(true, true);

        /**
         * Restart the SNA -- this will start still-active RNs.
         */
        StorageNodeAgentAPI snai1 =
            startRegisteredStore(snid);

        /**
         * Assert that the SNA is running and wait for the 2 RNs.
         */
        assertPing(snai1, ServiceStatus.RUNNING);
        waitForRNAdmin(rnids[0]);
        waitForRNAdmin(rnids[2]);

        /**
         * Destroy the RNs this time, they won't be restarted.
         */
        snai1.destroyRepNode(rnids[0], true);
        snai1.destroyRepNode(rnids[2], true);

        /**
         * Attempt to start a destroyed RepNode should throw.
         */
        try {
            snai1.startRepNode(rnids[0]);
            fail("startRepNode() should have thrown -- no service");
        } catch (RuntimeException ignored) {
            /* this is success path */
        }

        snai1.shutdown(true, true);

        assert(null == ConfigUtils.getRepNodeParams
               (sna.getKvConfigFile(), rnids[0], null));
        assert(null == ConfigUtils.getRepNodeParams
               (sna.getKvConfigFile(), rnids[2], null));

        /**
         * Restart, assert that RNs are not running.
         */
        snai1 = startRegisteredStore(snid);
        assertPing(snai1, ServiceStatus.RUNNING);
        assertNull("RN should not be running",
                   waitForRNAdmin(rnids[0], 2));
        assertNull("RN should not be running",
                   waitForRNAdmin(rnids[1], 2));
        assertNull("RN should not be running",
                   waitForRNAdmin(rnids[2], 2));
        snai1.shutdown(true, true);
    }

    /**
     * Test auto-restart of a dead RepNode.  Start 3, kill one, make sure that
     * it gets restarted by the ProcessMonitor.
     */
    @Test
    public void testAutoRestartRepNode()
        throws Exception {

        /*
         * Call Assume so that this test only run with process mode and
         * not use mount points.
         */
        Assume.assumeTrue(!useThreads && !useMountPoints);
        RepNodeId[] rnids = new RepNodeId[3];
        StorageNodeAgentAPI snai = startRepNodes(topo, rnids, 3);

        /**
         * Kill a RepNode.  There is no need to wait for the handle to go bad
         * because the process is killed by this code.
         */

        RegistryUtils ru = new RegistryUtils(topo, NULL_LOGIN_MGR, logger);
        RemoteTestAPI rti = ru.getRepNodeTest(rnids[1]);

        /**
         * This RMI call will throw an exception because it results in server
         * process exit.
         */
        try {
            rti.processExit(true /* restart */);
            fail("Call should have failed");
        } catch (Exception ignored) {
        }

        /**
         * Wait for the new handle to be available
         */
        waitForRNAdmin(rnids[1]);

        /**
         * Now exit with NO_RESTART
         */
        rti = ru.getRepNodeTest(rnids[1]);
        try {
            rti.processExit(false /* restart */);
            fail("Call should have failed");
        } catch (Exception ignored) {
        }

        /**
         * Delay a second to let the ProcessMonitor run, and assert that the
         * service isn't running.  If this assertion fails it is probably due
         * to an insufficient delay.
         */
        delay(1);
        assert(!sna.isRunning(rnids[1]));

        snai.shutdown(true, true);
    }

    /**
     * Test a bad store file -- corrupted at run time.
     * 1.  create a store
     * 2.  corrupt the file
     * 3.  try to add a RN (which wants to use the file)
     *
     * TODO: come up with a better mechanism to report corrupt config files
     * in the SNA.
     */
    @Test
    public void testBadStoreFile()
        throws Exception {

        /*
         * Call Assume so that this test only run with process mode and
         * not use mount points.
         */
        Assume.assumeTrue(!useThreads && !useMountPoints);
        RepNodeId[] rnids = new RepNodeId[replicationFactor];
        RepNodeParams[] rnps = new RepNodeParams[replicationFactor];
        ParameterMap[] ps = new ParameterMap[replicationFactor];
        initRepNodes(topo, rnids, rnps, ps);
        StorageNodeId snid = topo.get(rnids[0]).getStorageNodeId();

        StorageNodeAgentAPI snai =
            createRegisteredStore(snid, false);

        snai.createRepNode(ps[0], getMetadataSet());
        waitForRNAdmin(rnids[0]);

        File configFile = sna.getKvConfigFile();

        /*
         * Since #25835, configuration file became read only,
         * check if file is writable first
         */
        if (!configFile.canWrite()) {
            configFile.setWritable(true);
        }
         /* now corrupt the file */
        FileWriter writer = new FileWriter(configFile);
        writer.write("FOO");
        writer.close();
        try {
            snai.createRepNode(ps[1], getMetadataSet());
            fail("Call should have failed");
        } catch (Exception ignored) {
        }

        snai.shutdown(true, true);
    }

    /**
     * Test case when a RepNode exists but exits with ERROR_NORESTART.
     * Several cases:
     *  1.  try to start it
     *  2.  try to stop it
     *  3.  destroy it
     */
    @Test
    public void testStoppedRepNode()
        throws Exception {

        /*
         * Call Assume so that this test only run with process mode and
         * not use mount points.
         */
        Assume.assumeTrue(!useThreads && !useMountPoints);
        RepNodeId[] rnids = new RepNodeId[replicationFactor];
        RepNodeParams[] rnps = new RepNodeParams[replicationFactor];
        ParameterMap[] ps = new ParameterMap[replicationFactor];
        initRepNodes(topo, rnids, rnps, ps);
        StorageNodeId snid = topo.get(rnids[0]).getStorageNodeId();

        StorageNodeAgentAPI snai =
            createRegisteredStore(snid, false);

        snai.createRepNode(ps[0], getMetadataSet());
        waitForRNAdmin(rnids[0]);

        /**
         * OK, it's running, now kill it...
         */
        RegistryUtils ru = new RegistryUtils(topo, NULL_LOGIN_MGR, logger);
        RemoteTestAPI rti = ru.getRepNodeTest(rnids[0]);

        /**
         * This RMI call will throw an exception because it results in server
         * process exit.
         */
        try {
            rti.processExit(false /* restart */);
            fail("Call should have failed");
        } catch (Exception ignored) {
        }

        /**
         * Wait for the process to exit (timeout in seconds).  This is
         * required to get the timing correct for the next few assertions.
         */
        assertTrue(sna.waitForRepNodeExit(rnids[0], 5));

        /* start, stop, destroy */
        assertTrue(snai.startRepNode(rnids[0]));
        assertTrue(snai.stopRepNode(rnids[0], true, false));
        assertTrue(snai.destroyRepNode(rnids[0], false));

        /**
         * Trying to change params on non-existent RN throws.
         */
        try {
            snai.newRepNodeParameters(rnps[0].getMap());
            fail("newRepNodeParameters() should have thrown");
        } catch (RuntimeException ignored) {
            /* success */
        }


        /* re-create */
        assertTrue(snai.createRepNode(ps[0], getMetadataSet()));
        waitForRNAdmin(rnids[0]);

        snai.shutdown(true, true);
    }

    /**
     * Test case when a RepNode exists but exits because of EFE, which is
     * caused by JE data corruption.
     */
    @Test
    public void testCorruptedRepNode()
        throws Exception {

        /*
         * Call Assume so that this test only run with process mode and
         * not use mount points.
         */
        Assume.assumeTrue(!useThreads && !useMountPoints);
        RepNodeId[] rnids = new RepNodeId[replicationFactor];
        RepNodeParams[] rnps = new RepNodeParams[replicationFactor];
        ParameterMap[] ps = new ParameterMap[replicationFactor];
        initRepNodes(topo, rnids, rnps, ps);
        StorageNodeId snid = topo.get(rnids[0]).getStorageNodeId();

        StorageNodeAgentAPI snai =
            createRegisteredStore(snid, false);

        snai.createRepNode(ps[0], getMetadataSet());
        waitForRNAdmin(rnids[0]);

        /*
         * RN is running, simulate EFE caused by data corruption.
         */
        RegistryUtils ru = new RegistryUtils(topo, NULL_LOGIN_MGR, logger);
        RemoteTestAPI rti = ru.getRepNodeTest(rnids[0]);
        rti.processInvalidateEnvironment(true /* corrupted */);

        /*
         * Wait for the process to exit (timeout in seconds).  This is
         * required to get the timing correct for the next few assertions.
         */
        assertTrue(sna.waitForRepNodeExit(rnids[0], 5));
        assert(!sna.isRunning(rnids[0]));
        ProcessServiceManager serviceManager =
            (ProcessServiceManager) sna.getServiceManager(rnids[0]);
        assertEquals(ProcessExitCode.INJECTED_FAULT_NO_RESTART.getValue(),
                     serviceManager.getExitCode());
        assert(!sna.isRunning(rnids[0]));
        snai.shutdown(true, true);
    }

    /**
     * Test a bad mount point.
     */
    @Test
    public void testBadMountPoint()
        throws Exception {

        /*
         * Call Assume so that this test only run with process mode and
         * not use mount points.
         */
        Assume.assumeTrue(!useThreads && !useMountPoints);
        RepNodeId[] rnids = new RepNodeId[replicationFactor];
        RepNodeParams[] rnps = new RepNodeParams[replicationFactor];
        ParameterMap[] ps = new ParameterMap[replicationFactor];
        initRepNodes(topo, rnids, rnps, ps);
        StorageNodeId snid = topo.get(rnids[0]).getStorageNodeId();

        StorageNodeAgentAPI snai =
            createRegisteredStore(snid, false);

        /* test SNAInterface.repNodeExists() call */
        assert(snai.repNodeExists(rnids[0]) == false);

        /* set bad mount point */
        rnps[0].setStorageDirectory("/nothing/y/z", 0L);
        try {
            snai.createRepNode(ps[0], getMetadataSet());
            fail ("Expected failure due to bad mount point");
        } catch (SNAFaultException e) {
            // ignore the exception
        }
        RepNodeAdminAPI rnai = waitForRNAdmin(rnids[0], 3);
        assertNull("RepNode should not be running", rnai);
        snai.shutdown(true, true);
    }

    /**
     * Test mismatched JVM and cache sizes.  This will only work in process
     * mode where the RepNodes run in independent processes.
     */
    @Test
    public void testBadCacheSize()
        throws Exception {

        /*
         * Call Assume so that this test only run with process mode and
         * not use mount points.
         */
        Assume.assumeTrue(!useThreads && !useMountPoints);
        RepNodeId[] rnids = new RepNodeId[replicationFactor];
        RepNodeParams[] rnps = new RepNodeParams[replicationFactor];
        ParameterMap[] ps = new ParameterMap[replicationFactor];
        initRepNodes(topo, rnids, rnps, ps);
        StorageNodeId snid = topo.get(rnids[0]).getStorageNodeId();

        StorageNodeAgentAPI snai =
            createRegisteredStore(snid, false);

        /*
         * This should technically done via params but this is simpler.  Reduce
         * the wait time for a RepNode that's not coming up.  It speeds up the
         * test.
         */
        sna.setRepNodeWaitSecs(5);

        /*
         * The failures tested here are failures for the RepNodes to start
         * properly, but the RNs themselves get created.  That means they
         * must be removed before proceeding to additional test case.
         */

        /*
         * Use a too-large cache size
         */
        int heapMB = ParameterUtils.applyMinHeapMB(80);
        long cacheB = (((int) (heapMB * 0.9)) + 1) << 20;
        rnps[0].setJavaMiscParams("-Xmx" + heapMB + "M");
        rnps[0].setJECacheSize(cacheB);
        snai.createRepNode(ps[0], getMetadataSet());
        RepNodeAdminAPI rnai = waitForRNAdmin(rnids[0], 5);
        assertNull("RepNode start should have failed", rnai);
        assert(snai.destroyRepNode(rnids[0], true));

        /*
         * Use a too-small heap size
         */
        heapMB = ParameterUtils.applyMinHeapMB(31);
        cacheB = (heapMB * 2) << 20;
        rnps[0].setJavaMiscParams("-Xmx" + heapMB + "M");
        rnps[0].setJECacheSize(cacheB);
        snai.createRepNode(ps[0], getMetadataSet());
        rnai = waitForRNAdmin(rnids[0], 5);
        assertNull("RepNode start should have failed", rnai);
        assert(snai.destroyRepNode(rnids[0], true));

        /*
         * This can be made to work by one of:
         *  - shrink cache size below 90% of heap, or to 0
         *  - use a default heap (set heap to 0)
         * In this case, just shrink the cache
         *
         * Heap/cache sizes are designed to work on Zing (1 GB minimum heap)
         * and Oracle JVMs:
         *  - On Oracle JVM: 0.35 * 150 > 33 (RN min cache size)
         *  - On Zing: 0.35 * 1 GB < 400 MB (heap size available)
         */
        heapMB = ParameterUtils.applyMinHeapMB(150);
        cacheB = ((int) (heapMB * 0.35)) << 20;
        rnps[0].setJavaMiscParams("-Xmx" + heapMB + "M");
        rnps[0].setJECacheSize(cacheB);
        snai.createRepNode(ps[0], getMetadataSet());
        waitForRNAdmin(rnids[0]);
        assert(snai.repNodeExists(rnids[0]));

        /*
         * Change a parameter to cause failure, then fix it.  Use a too-large
         * cache.
         */
        cacheB = (((int) (heapMB * 0.9)) + 1) << 20;
        rnps[0].setJECacheSize(cacheB);
        snai.newRepNodeParameters(rnps[0].getMap());
        restartRepNode(snai, rnids[0]);
        rnai = waitForRNAdmin(rnids[0], 5);
        assertNull("RepNode start should have failed", rnai);
        /* now fix it */
        cacheB = ((int) (heapMB * 0.35)) << 20;
        rnps[0].setJECacheSize(cacheB);
        snai.newRepNodeParameters(rnps[0].getMap());
        restartRepNode(snai, rnids[0]);
        rnai = waitForRNAdmin(rnids[0], 10);
        assertNotNull("RepNode start should have worked", rnai);
        snai.shutdown(true, true);
    }

    private void restartRepNode(StorageNodeAgentAPI snai,
                                RepNodeId rnid)
        throws RemoteException {

        snai.stopRepNode(rnid, false, false);
        snai.startRepNode(rnid);
    }

    @Test
    public void testDeleteRepNodeData()
        throws Exception {

        RepNodeId[] rnids = new RepNodeId[replicationFactor];
        RepNodeParams[] rnps = new RepNodeParams[replicationFactor];
        ParameterMap[] ps = new ParameterMap[replicationFactor];
        initRepNodes(topo, rnids, rnps, ps);
        StorageNodeId snid = topo.get(rnids[0]).getStorageNodeId();

        StorageNodeAgentAPI snai =
            createRegisteredStore(snid, false);

        snai.createRepNode(ps[0], getMetadataSet());
        waitForRNAdmin(rnids[0]);
        snai.createRepNode(ps[1], getMetadataSet());
        waitForRNAdmin(rnids[1]);
        /* Test normal startup -- startup buffer should be null */
        StringBuilder startupProblem = snai.getStartupBuffer(rnids[0]);
        assertNull("Startup buffer should be null", startupProblem);


        /**
         * Remove the env from the first RN, not from the second.
         */
        assertTrue(snai.destroyRepNode(rnids[0], true));
        assertTrue(snai.destroyRepNode(rnids[1], false));

        File serviceDir0 = rnps[0].getStorageDirectoryFile();
        File serviceDir1 = rnps[1].getStorageDirectoryFile();

        File rnDataDir0 =
            FileNames.getServiceDir(TestUtils.getTestDir().toString(),
                                    topo.getKVStoreName(),
                                    serviceDir0,
                                    snid, rnids[0]);
        File rnDataDir1 =
            FileNames.getServiceDir(TestUtils.getTestDir().toString(),
                                    topo.getKVStoreName(),
                                    serviceDir1,
                                    snid, rnids[1]);
        assertFalse(rnDataDir0.exists());
        assertTrue(rnDataDir1.exists());

        snai.shutdown(true, true);
    }

    /*
     * This test must run in process mode.  It creates and tests conditions
     * that will cause a RepNode process to fail to start and/or initialize,
     * such as bad JVM parameters or bad RN params.
     *
     * In both cases tested below it'd be possible to assert on the content
     * of the startupProblem StringBuffer but that'd be fragile.
     */
    @Test
    public void testProcessStartFailure()
        throws Exception {

        /*
         * Call Assume so that this test only run with process mode and
         * not use mount points.
         */
        Assume.assumeTrue(!useThreads && !useMountPoints);
        RepNodeId[] rnids = new RepNodeId[replicationFactor];
        RepNodeParams[] rnps = new RepNodeParams[replicationFactor];
        ParameterMap[] ps = new ParameterMap[replicationFactor];
        initRepNodes(topo, rnids, rnps, ps);
        StorageNodeId snid = topo.get(rnids[0]).getStorageNodeId();

        StorageNodeAgentAPI snai =
            createRegisteredStore(snid, false);

        /*
         * Specify an unknown option to cause the JVM to fail to initialize
         */
        ps[0].put(Parameter.createParameter(ParameterState.JVM_MISC,
                                            "-XunknownOption"));
	try {
	    snai.createRepNode(ps[0], getMetadataSet());
	    fail("Startup should have failed");
	} catch (Exception expected) {
	}

        StringBuilder startupProblem = snai.getStartupBuffer(rnids[0]);
        assertNotNull("Startup buffer should be non-null", startupProblem);

        /*
         * now try a failure that should affect the RepNode vs the JVM
         * -- an excessive cache size
         */
        ps[0].remove(ParameterState.JVM_MISC);
        ps[0].put(Parameter.createParameter(ParameterState.JE_CACHE_SIZE,
                                            "1000000000000"));
        snai.destroyRepNode(rnids[0], true);
	try {
	    snai.createRepNode(ps[0], getMetadataSet());
	    fail ("Startup should have failed");
	} catch (Exception e) {
	}

        startupProblem = snai.getStartupBuffer(rnids[0]);
        assertNotNull("Startup buffer should be non-null", startupProblem);

        /*
         * now fix things and start the (existing) RN and verify that works
         */
        ps[0].put(Parameter.createParameter(ParameterState.JE_CACHE_SIZE,
                                            "0"));
        snai.newRepNodeParameters(ps[0]);
        snai.startRepNode(rnids[0]);
        RepNodeAdminAPI rnai = waitForRNAdmin(rnids[0]);
        assertNotNull("Startup should have worked", rnai);
        startupProblem = snai.getStartupBuffer(rnids[0]);
        assertNull("Startup buffer should be null", startupProblem);

        snai.shutdown(true, true);
    }

    /**
     * Test Service clean up.
     */
    @Test
    public void testRepNodeCleanup()
        throws Exception {

        RepNodeId[] rnids = new RepNodeId[replicationFactor];
        RepNodeParams[] rnps = new RepNodeParams[replicationFactor];
        ParameterMap[] ps = new ParameterMap[replicationFactor];
        initRepNodes(topo, rnids, rnps, ps);
        StorageNodeId snid = topo.get(rnids[0]).getStorageNodeId();

        StorageNodeAgentAPI snai =
            createRegisteredStore(snid, false);
        assertTrue(snai.createRepNode(ps[0], getMetadataSet()));
        waitForRNAdmin(rnids[0]);
        Registry registry = sna.getRegistry();
        String serviceName =
            RegistryUtils.bindingName(kvstoreName,
                                      rnids[0].getFullName(),
                                      RegistryUtils.InterfaceType.ADMIN);
        Remote stub = registry.lookup(serviceName);
        assertNotNull("RepNodeService should exist", stub);

        /* stop it gracefully */
        snai.stopRepNode(rnids[0], false, false);
        try {
            stub = registry.lookup(serviceName);
            fail("RepNodeService should not exist");
        } catch (NotBoundException nbe) {
        }

        /* start it again */
        snai.startRepNode(rnids[0]);
        waitForRNAdmin(rnids[0]);

        /* kill it forcefully */
        snai.stopRepNode(rnids[0], true, false);
        waitForRNAdmin(rnids[0], 2);
        try {
            stub = registry.lookup(serviceName);
            fail("RepNodeService should not exist");
        } catch (NotBoundException nbe) {
        }

        snai.shutdown(true, true);
    }

    /**
     * Test that attempting to shut down an RN unbinds the RN services from the
     * async registry even if the shutdown call to the RN fails. [KVSTORE-2049]
     */
    @Test
    public void testRepNodeShutdownRegistryCleanup() throws Exception {

        /*
         * The service cache used with RMI makes it difficult to test that the
         * entry has been removed in the sync case, even though it was possible
         * to confirm with hand testing that the entry really is being removed,
         * so only test in async mode
         */
        Assume.assumeTrue("Test requires async", AsyncControl.serverUseAsync);

        /* Need to set test hooks */
        Assume.assumeTrue("Test requires using threads", useThreads);

        /*
         * And the mount point case also seems to cause trouble for some
         * reason, and isn't particularly relevant
         */
        Assume.assumeFalse("Skip mount point case", useMountPoints);

        /* Create a cluster */
        RepNodeId[] rnids = new RepNodeId[replicationFactor];
        RepNodeParams[] rnps = new RepNodeParams[replicationFactor];
        ParameterMap[] ps = new ParameterMap[replicationFactor];
        initRepNodes(topo, rnids, rnps, ps);
        StorageNodeId snid = topo.get(rnids[0]).getStorageNodeId();
        StorageNodeAgentAPI snai = createRegisteredStore(snid, false);
        snai.createRepNode(ps[0], getMetadataSet());
        waitForRNAdmin(rnids[0]);

        /* Make sure the RN admin entry is in the registry */
        RegistryUtils ru = new RegistryUtils(topo, NULL_LOGIN_MGR, logger);
        RepNodeAdminAPI rnAdmin = ru.getRepNodeAdmin(rnids[0]);
        assertNotNull(rnAdmin);

        /* Calls to shutdown the RN should fail */
        AtomicBoolean hookCalled = new AtomicBoolean();
        StorageNodeAgent.rnShutdownHook = x -> {
            hookCalled.set(true);
            throw new RuntimeException("Injected failure");
        };

        /* Stop RN */
        assertStopped(rnids[0], topo, snid, false);
        assertTrue(snai.stopRepNode(rnids[0], true, false));
        assertStopped(rnids[0], topo, snid, true);

        /* Make sure the hook was called when attempting to stop the RN */
        assertTrue(hookCalled.get());

        /* Make sure the RN entry has been removed from the registry */
        checkException(() -> ru.getRepNodeAdmin(rnids[0]),
                       NotBoundException.class);

        StorageNodeAgent.rnShutdownHook = null;
        rnAdmin.shutdown(false);
    }

    /**
     * Add a user to security metadata and return a set consists of security
     * and topology metadata
     */
    private Set<Metadata<? extends MetadataInfo>> getMetadataSet() {
        sec.addUser(KVStoreUser.newInstance("root"));
        return createMetadataSet(sec, topo);
    }
}
