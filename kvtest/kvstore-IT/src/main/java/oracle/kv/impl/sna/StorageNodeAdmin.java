/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.sna;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.rmi.Remote;
import java.rmi.registry.Registry;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.test.RemoteTestAPI;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.FilterableParameterized;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.RegistryUtils;

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests the Admin operations of StorageNodeAgentImpl.
 */
@RunWith(FilterableParameterized.class)
public class StorageNodeAdmin extends StorageNodeTestBase {

    public StorageNodeAdmin(boolean useThreads) {
        super(useThreads);
    }

    /**
     * Notes: It is required to call the super methods if override
     * setUp and tearDown methods.
     */
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
     * Start, register, create an AdminService.
     */
    @Test
    public void testBasicAdmin()
        throws Exception {

        Topology topo = createTopology(testhost, replicationFactor);
        RepNodeId rnid = new RepNodeId(1,1);
        StorageNodeId snid = topo.get(rnid).getStorageNodeId();
        int port = topo.get(snid).getRegistryPort();
        StorageNodeAgentAPI snai = createRegisteredStore(snid, false);

        /**
         * Need to manufacture AdminParams
         */
        AdminId aid = new AdminId(1);
        AdminParams ap = createAdminParams(aid, snid, "localhost");

        assertTrue(snai.createAdmin(ap.getMap()));
        waitForAdmin(testhost, port);

        /**
         * Test that a second attempt to create the admin results in a simple
         * boolean return.
         */
        assertTrue(snai.createAdmin(ap.getMap()));

        snai.shutdown(true, false);
    }

    /**
     * Test handling of bootstrap admin which means not registering the store
     */
    @Test
    public void testBootstrapAdmin()
        throws Exception {

        /*
         * Call Assume so that this test only run with process mode.
         */
        Assume.assumeFalse(useThreads);
        StorageNodeAgentAPI snai = createUnregisteredSNA();
        /**
         * Test restart of the bootstrap admin, use a forced shutdown
         */
        int port = portFinder.getRegistryPort();
        CommandServiceAPI cs = waitForAdmin(testhost, port);
        cs.stop(true);
        waitForAdmin(testhost, port);
        snai.shutdown(true, false);
        cs = waitForAdmin(testhost, port, 5);
        assertNull("Admin should not be running", cs);
    }

    /**
     * Start, register, create an AdminService that is the hosted instance.
     * Destroy it and make sure it doesn't come back.  Do the same with a
     * stopped (vs destroyed) instance.
     */
    @Test
    public void testHostedAdmin()
        throws Exception {

        Topology topo = createTopology(testhost, replicationFactor);
        RepNodeId rnid = new RepNodeId(1,1);
        StorageNodeId snid = topo.get(rnid).getStorageNodeId();
        int port = topo.get(snid).getRegistryPort();
        StorageNodeAgentAPI snai = createRegisteredStore(snid, true);

        /**
         * Need to manufacture AdminParams
         */
        AdminId aid = new AdminId(1);
        AdminParams ap = createAdminParams(aid, snid, testhost);

        assertTrue(snai.createAdmin(ap.getMap()));

        /**
         * If using processes, test automatic restart of the admin.
         */
        if (!useThreads) {
            CommandServiceAPI cs = waitForAdmin(testhost, port);
            cs.stop(true);
        }
        waitForAdmin(testhost, port);

        /**
         * Destroy the admin and assert that it's gone, along with its data.
         */
        snai.destroyAdmin(aid, true /* deleteData */, null /* reason */);
        assert(null == ConfigUtils.getAdminParams(sna.getKvConfigFile()));
        File rnDataDir =
            FileNames.getServiceDir(TestUtils.getTestDir().toString(),
                                    kvstoreName, null, snid, ap.getAdminId());
        assertFalse(rnDataDir.exists());


        /**
         * Assert that another attempt to destroy fails correctly.
         */
        assertFalse(snai.destroyAdmin(aid, false /* destroyData */,
                                      null /* reason */));

        /**
         * Assert that an attempt to start the non-existent admin throws.
         */
        try {
            snai.startAdmin();
            fail("startAdmin() should have thrown, no service");
        } catch (RuntimeException ignored) {
            /* success path */
        }

        /**
         * Assert that an attempt to stop the non-existent admin throws.
         */
        try {
            snai.stopAdmin(true);
            fail("stopAdmin() should have thrown, no service");
        } catch (RuntimeException ignored) {
            /* success path */
        }

        snai.shutdown(true, true);
        CommandServiceAPI cs = waitForAdmin(testhost, port, 5);
        assertNull("Admin should not be running", cs);

        /**
         * Restart the SNA and make sure that the admin is NOT started.
         */
        StorageNodeAgentAPI snai1 = startRegisteredStore(snid);
        assertPing(snai1, ServiceStatus.RUNNING);
        cs = waitForAdmin(testhost, port, 5);
        assertNull("Admin should not be running", cs);

        /**
         * Now create another and repeat the testing with stopAdmin():
         * Create, stop, assert that it's not restarted.
         */
        assertTrue(snai1.createAdmin(ap.getMap()));
        waitForAdmin(testhost, port);

        /**
         * start is idempotent so this should silently succeed
         */
        assertTrue(snai1.startAdmin());
        assertTrue(snai1.stopAdmin(false));
        cs = waitForAdmin(testhost, port, 5);
        assertNull("Admin should not be running", cs);
        snai1.shutdown(true, true);

        /**
         * Restart the SNA and assert that it does not start the admin.
         */
        snai = startRegisteredStore(snid);
        assertPing(snai, ServiceStatus.RUNNING);
        cs = waitForAdmin(testhost, port, 5);
        assertNull("Admin should not be running", cs);

        /**
         * Start it (create is not needed here)
         */
        assertTrue(snai.startAdmin());
        waitForAdmin(testhost, port);

        /**
         * Clean up.
         */
        snai.shutdown(true, true);
    }

    /**
     * Test handling of the case where no admin port is specified which
     * causes the SNA to not start a bootstrap admin.
     */
    @Test
    public void testNoBootstrapAdmin()
        throws Exception {

        /*
         * Call Assume so that this test only run with process mode.
         */
        Assume.assumeFalse(useThreads);
        StorageNodeAgentAPI snai =
            createNoBootstrapSNA(portFinder, 1, configName);

        /* This needs to fail */
        CommandServiceAPI cs =
            waitForAdmin(testhost, portFinder.getRegistryPort(), 5);
        assertNull("Admin should not be running", cs);
        snai.shutdown(true, true);
    }

    /**
     * Test changing no-restart parameters on an admin.
     */
    @Test
    public void testChangeAdminParams()
        throws Exception {

        /*
         * Call Assume so that this test only run with process mode.
         */
        Assume.assumeFalse(useThreads);
        Topology topo = createTopology(testhost, replicationFactor);
        RepNodeId rnid = new RepNodeId(1,1);
        StorageNodeId snid = topo.get(rnid).getStorageNodeId();
        int port = topo.get(snid).getRegistryPort();
        StorageNodeAgentAPI snai = createRegisteredStore(snid, true);

        /**
         * Need to manufacture AdminParams
         */
        AdminId aid = new AdminId(1);
        AdminParams ap = createAdminParams(aid, snid, testhost);

        assertTrue(snai.createAdmin(ap.getMap()));
        ParameterMap newmap = ap.getMap().copy();
        /* Modify some monitor params */
        DurationParameter dp =
            (DurationParameter) newmap.get(ParameterState.MP_POLL_PERIOD);
        assertTrue(dp != null);
        dp.setMillis(1000);
        newmap.setParameter(ParameterState.MP_CREATE_CSV, "true");
        snai.newAdminParameters(newmap);
        CommandServiceAPI cs = waitForAdmin(testhost, port);
        cs.newParameters();
        /**
         * Clean up.
         */
        delay(10);
        snai.shutdown(true, false);
    }

    /**
     * Test proper shutdown of a bootstrap admin.
     * 1.  Create unregistered SNA w/bootstrap admin
     * 2.  Register the SNA but don't host admin
     * 3.  Make sure admin is truly gone as a process/service and from
     * the RMI registry.
     * This tests a situation described in SR [#22639].
     */
    @Test
    public void testBootstrapAdminState()
        throws Exception {

        StorageNodeAgentAPI snai = createUnregisteredSNA();
        int port = portFinder.getRegistryPort();
        StorageNodeId snid = new StorageNodeId(1);
        StorageNodeParams snp =
            new StorageNodeParams(snid, testhost, port, "");
        GlobalParams gp = new GlobalParams(kvstoreName);
        CommandServiceAPI cs = waitForAdmin(testhost, port, 5);
        assertNotNull("Admin should be running", cs);
        Registry registry = sna.getRegistry();
        Remote stub = registry.lookup(GlobalParams.COMMAND_SERVICE_NAME);
        assertNotNull("CommandService should exist", stub);

        /*
         * Register, but don't host admin.  This eliminates bootstrap admin.
         * Make sure it is gone, as well as its registry information.
         */
        snai.register(gp.getMap(), snp.getMap(), false);
        cs = waitForAdmin(testhost, port, 5);
        assertNull("Admin should not be running", cs);
        try {
            stub = registry.lookup(GlobalParams.COMMAND_SERVICE_NAME);
            fail("Registry lookup should have failed");
        } catch (Exception e) {
            /* success */
        }
        sna.shutdown(true, false);
    }

    /**
     * Test case when an Admin exists but exits because of EFE, which is
     * caused by JE data corruption.
     */
    @Test
    public void testCorruptedAdmin()
        throws Exception {

        /*
         * Call Assume so that this test only run with process mode.
         */
        Assume.assumeFalse(useThreads);
        Topology topo = createTopology(testhost, replicationFactor);
        RepNodeId rnid = new RepNodeId(1,1);
        StorageNodeId snid = topo.get(rnid).getStorageNodeId();
        int port = topo.get(snid).getRegistryPort();
        StorageNodeAgentAPI snai = createRegisteredStore(snid, false);

        /*
         * Need to manufacture AdminParams
         */
        AdminId aid = new AdminId(1);
        AdminParams ap = createAdminParams(aid, snid, "localhost");

        assertTrue(snai.createAdmin(ap.getMap()));
        waitForAdmin(testhost, port);

        /*
         * Admin is running, simulate EFE caused by data corruption.
         */
        RegistryUtils ru = new RegistryUtils(topo, NULL_LOGIN_MGR, logger);
        RemoteTestAPI rti = ru.getAdminTest(snid);
        rti.processInvalidateEnvironment(true /* corrupted */);

        /*
         * Wait for the process to exit (timeout in seconds).  This is
         * required to get the timing correct for the next few assertions.
         */
        ProcessServiceManager serviceManager =
            (ProcessServiceManager) sna.getAdminServiceManager();
        serviceManager.waitFor(15 * 1000);
        assertFalse(serviceManager.isRunning());
        assertEquals(ProcessExitCode.INJECTED_FAULT_NO_RESTART.getValue(),
                     serviceManager.getExitCode());
        snai.shutdown(true, true);
    }
}
