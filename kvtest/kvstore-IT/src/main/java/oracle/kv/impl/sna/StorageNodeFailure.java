/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.sna;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.FilterableParameterized;
import oracle.kv.impl.util.ServiceUtils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests various failure paths where the SNA exits at inopportune times. Tests
 * how it behaves when it starts up again and makes sure that things go as
 * expected.  These tests work in process mode only.
 */
@RunWith(FilterableParameterized.class)
public class StorageNodeFailure extends StorageNodeTestBase {

    public StorageNodeFailure(boolean useThread) {
        super(useThread);
    }

    /**
     * Override superclass genParams method to provide use_thread as false
     * only for the tests in this class.
     */
    @Parameters(name="Use_Thread={0}")
    public static List<Object[]> genParams() {
        if (PARAMS_OVERRIDE != null) {
            return PARAMS_OVERRIDE;
        }
        return Arrays.asList(new Object[][] {{false}}); 
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
     * Start, register, create a single RepNodeService but simulate a failure
     * before the RepNode is started/configured.
     */
    @Test
    public void testRepNodeCreate()
        throws Exception {

        Topology topo = createTopology(testhost, replicationFactor);
        RepNodeId[] rnids = new RepNodeId[replicationFactor];
        RepNodeParams[] rnps = new RepNodeParams[replicationFactor];
        ParameterMap[] ps = new ParameterMap[replicationFactor];
        initRepNodes(topo, rnids, rnps, ps);
        StorageNodeId snid = topo.get(rnids[0]).getStorageNodeId();

        StorageNodeAgentAPI snai =
            createRegisteredStore(snid, false);

        assert(snai.repNodeExists(rnids[0]) == false);
        sna.setRNTestHook(new RestartHook(this, ps[0], rnids[0], topo));

        try {
            snai.createRepNode(ps[0], createMetadataSet(topo));
            fail("Call should have thrown");
        } catch (RuntimeException e) {
            /* this is success path */
        }
    }

    /**
     * Start, register, create a single RepNodeService but simulate a failure
     * after the RepNode is started but before it is configured.
     */
    @Test
    public void testRepNodeCreate1()
        throws Exception {

        Topology topo = createTopology(testhost, replicationFactor);
        RepNodeId[] rnids = new RepNodeId[replicationFactor];
        RepNodeParams[] rnps = new RepNodeParams[replicationFactor];
        ParameterMap[] ps = new ParameterMap[replicationFactor];
        initRepNodes(topo, rnids, rnps, ps);
        StorageNodeId snid = topo.get(rnids[0]).getStorageNodeId();

        StorageNodeAgentAPI snai =
            createRegisteredStore(snid, false);

        sna.setStopRNTestHook(new StopRepNodeHook(rnids[0]));

        try {
            snai.createRepNode(ps[0], createMetadataSet(topo));
        } catch (RuntimeException e) {
            fail("Call should have succeeded");
        }
        assert(snai.repNodeExists(rnids[0]) == true);
    }

    /**
     * Start, register, create a single RepNodeService but simulate a failure
     * after the RepNode is configured.  This tests idempotency of the
     * createRepNode call in the case where the initial create completes.
     */
    @Test
    public void testRepNodeCreateSuccess()
        throws Exception {

        Topology topo = createTopology(testhost, replicationFactor);
        RepNodeId[] rnids = new RepNodeId[replicationFactor];
        RepNodeParams[] rnps = new RepNodeParams[replicationFactor];
        ParameterMap[] ps = new ParameterMap[replicationFactor];
        initRepNodes(topo, rnids, rnps, ps);
        StorageNodeId snid = topo.get(rnids[0]).getStorageNodeId();

        StorageNodeAgentAPI snai =
            createRegisteredStore(snid, false);

        assert(snai.repNodeExists(rnids[0]) == false);
        snai.createRepNode(ps[0], createMetadataSet(topo));

        /* try another create with the test hook that will simulate failure */
        sna.setRNTestHook(new RestartHook(this, ps[0], rnids[0], topo));
        try {
            snai.createRepNode(ps[0], createMetadataSet(topo));
            fail("Call should have thrown");
        } catch (RuntimeException e) {
            /* this is success path */
        }
    }

    /**
     * Test idempotency of admin creation.  It does not suffer from the 2-stage
     * start issue of the RepNode but the SNA could still fail after writing
     * the config file before starting the process.
     */
    @Test
    public void testAdminCreate()
        throws Exception {

        Topology topo = createTopology(testhost, replicationFactor);
        RepNodeId rnid = new RepNodeId(1,1);
        StorageNodeId snid = topo.get(rnid).getStorageNodeId();
        int registryPort = portFinder.getRegistryPort();
        StorageNodeAgentAPI snai =
            createRegisteredStore(snid, false);

        /*
         * Need to manufacture AdminParams
         */
        AdminId aid = new AdminId(1);
        AdminParams ap = createAdminParams(aid, snid, "localhost");

        /*
         * Add a hook that will cause the start to be "aborted"
         */
        sna.setAdminTestHook(new RestartAdminHook(this, ap, registryPort));
        try {
            snai.createAdmin(ap.getMap());
            fail("Call should have thrown");
        } catch (RuntimeException e) {
            /* this is success path */
        }

        CommandServiceAPI cs = StorageNodeTestBase.waitForAdmin
            (StorageNodeTestBase.testhost, registryPort);
        assertTrue(cs != null);

        /* Shutdown the SNA */
        sna.shutdown(true, false);
    }

    /*
     * Test SR [#22828] -- make sure that RN will automatically restart after
     * an excessive restart situation diables it.
     */
    @Test
    public void testExcessiveRestarts()
        throws Exception {

        Topology topo = createTopology(testhost, replicationFactor);
        RepNodeId[] rnids = new RepNodeId[replicationFactor];
        RepNodeParams[] rnps = new RepNodeParams[replicationFactor];
        ParameterMap[] ps = new ParameterMap[replicationFactor];
        initRepNodes(topo, rnids, rnps, ps);
        RepNodeId targetRnid = rnids[0];
        StorageNodeId snid = topo.get(targetRnid).getStorageNodeId();

        StorageNodeAgentAPI snai =
            createRegisteredStore(snid, false);

        assert(snai.repNodeExists(targetRnid) == false);
        snai.createRepNode(ps[0], createMetadataSet(topo));

        /*
         * 5 restarts in a minute will trigger the disabled condition
         */
        ServiceManager mgr;
        for (int i = 0; i < 7; i++) {
            mgr = sna.getServiceManager(targetRnid);
            if (mgr.isRunning()) {
                ((ProcessServiceManager)mgr).destroy();
                /* wait for rn */
                waitForRNAdmin(targetRnid, 2);
            }
        }

        /*
         * Do explicit start, make sure it restarts
         */
        sna.startRepNode(targetRnid);
        waitForRNAdmin(targetRnid, 2);
        mgr = sna.getServiceManager(targetRnid);
        assert(mgr.isRunning());

        /*
         * Now restart the SNA and make sure that the RepNode restarts with it
         */
        snai.shutdown(true, false);
        snai = startRegisteredStore(snid);
        waitForRNAdmin(targetRnid, 2);
        mgr = sna.getServiceManager(targetRnid);
        if (mgr == null || !mgr.isRunning()) {
            fail("RepNode should have been restarted");
        }
        snai.shutdown(true, false);
    }

    /**
     * This hook creates a new SNA based on the argument. Callers/users must
     * ensure that the passed-in sna is not used post-hook.
     */
    class RestartHook implements TestHook<StorageNodeAgent> {

        StorageNodeTestBase testBase;
        ParameterMap map;
        RepNodeId rnid;
        Topology topo;

        RestartHook(StorageNodeTestBase testBase,
                    ParameterMap map,
                    RepNodeId rnid,
                    Topology topo) {
            this.testBase = testBase;
            this.map = map;
            this.rnid = rnid;
            this.topo = topo;
        }

        @Override
        public void doHook(StorageNodeAgent arg) {

            try {
                /* Clear service state from SNA but leave them running */
                arg.shutdown(false, false);
                StorageNodeAgentAPI snai =
                    testBase.startRegisteredStore(arg.getStorageNodeId());
                assertTrue(snai.createRepNode(map, createMetadataSet(topo)));
                testBase.waitForRNAdmin(rnid);
                snai.shutdown(true, false);
            } catch (Exception e) {
                fail(e.getMessage());
            }
            throw new RuntimeException("this is ignored");
        }
    }

    class StopRepNodeHook implements TestHook<StorageNodeAgent> {

        RepNodeId rnid;

        StopRepNodeHook(RepNodeId rnid) {
            this.rnid = rnid;
        }

        @Override
        public void doHook(StorageNodeAgent arg) {

            try {
                /*
                 * Wait for the node to be running before stopping it now that,
                 * with the fix for [#24575] Coordinate RN start and stop,
                 * calling stop during start up will cause the start up to
                 * fail.
                 */
                ServiceStatus[] targets = {ServiceStatus.RUNNING};
                RepNodeAdminAPI rnai =
                    ServiceUtils.waitForRepNodeAdmin(arg.getStoreName(),
                                                     arg.getHostname(),
                                                     arg.getRegistryPort(),
                                                     rnid,
                                                     arg.getStorageNodeId(),
                                                     getLoginMgr(),
                                                     5, /* wait seconds */
                                                     targets,
                                                     logger);
                rnai.shutdown(true);

                /*
                 * Reduce wait time because this will time out.
                 */
                arg.setRepNodeWaitSecs(2);
            } catch (Exception e) {
                fail(e.getMessage());
            }
        }
    }

    /**
     * This hook creates a new SNA based on the argument. Callers/users must
     * ensure that the passed-in sna is not used post-hook.
     */
    class RestartAdminHook implements TestHook<StorageNodeAgent> {

        StorageNodeTestBase testBase;
        AdminParams ap;
        int port;

        RestartAdminHook(StorageNodeTestBase testBase,
                         AdminParams ap,
                         int port) {
            this.testBase = testBase;
            this.ap = ap;
            this.port = port;
        }

        @Override
        public void doHook(StorageNodeAgent arg) {

            try {
                /* Clear service state from SNA but leave them running */
                arg.shutdown(false, false);
                StorageNodeAgentAPI snai =
                    testBase.startRegisteredStore(arg.getStorageNodeId());
                assertTrue(snai.createAdmin(ap.getMap()));
                StorageNodeTestBase.waitForAdmin
                	(StorageNodeTestBase.testhost, port);
            } catch (Exception e) {
                fail(e.getMessage());
            }
            throw new RuntimeException("this is ignored");
        }
    }
}
