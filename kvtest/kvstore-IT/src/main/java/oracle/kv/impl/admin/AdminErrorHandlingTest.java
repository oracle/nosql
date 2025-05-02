/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.rmi.RemoteException;

import oracle.kv.TestBase;
import oracle.kv.impl.fault.InternalFaultException;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeTestBase;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;

/**
 * Test that the admin service responds with the appropriate exceptions in
 * failure cases, and that the admin service does not go down.
 */
public class AdminErrorHandlingTest extends TestBase {

    private PortFinder portFinder;
    private StorageNodeAgent sna;
    private static final int startPort = 5000;
    private static final int haRange = 5;
    private final LoginManager loginMgr = null;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        TestUtils.clearTestDirectory();
        portFinder = new PortFinder(startPort, haRange);
        sna = StorageNodeTestBase.createUnregisteredSNA(portFinder,
                                                        1,
                                                        "config.xml",
                                                        false,
                                                        true,
                                                        1024);
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        if (sna != null) {
            sna.shutdown(true, true);
        }
        LoggerUtils.closeAllHandlers();
    }

    @Test
    public void testBadDataCenterAndSNPool()
        throws Exception {

        final CommandServiceAPI cmdSvc =
            ServiceUtils.waitForAdmin(sna.getHostname(), sna.getRegistryPort(),
                                      loginMgr, 5, ServiceStatus.RUNNING,
                                      logger);

        cmdSvc.configure(kvstoreName);

        try {
            /* Execute bad storage node pool commands. */
            stressSNPoolSetup(cmdSvc);
            stressDataCenterSetup(cmdSvc);
        } catch (RemoteException remoteEx) {
            fail("The command service should have stayed up and available " +
                 " but got " + remoteEx);
        }
    }

    /**
     * Provide bad parameters for creating a topology
     */
    @Test
    public void testBadCreateTopoParams()
        throws Exception {

        try {
           final CommandServiceAPI cmdSvc =
            ServiceUtils.waitForAdmin(sna.getHostname(), sna.getRegistryPort(),
                                      loginMgr, 5, ServiceStatus.RUNNING,
                                      logger);

            cmdSvc.configure(kvstoreName);
            setupSNs(cmdSvc);

            /* Null storage node pool. */
            new ExpectBadCreateTopoCmd(cmdSvc, "Candidate", null, 0).run();

            /* Unknown storage node pool */
            new ExpectBadCreateTopoCmd(cmdSvc, "Candidate", "foo", 0).run();

            /* 0 partitions specified. */
            new ExpectBadCreateTopoCmd(cmdSvc, "Candidate", "SNPool", 0).run();

            /* Empty storage node pool. */
            cmdSvc.addStorageNodePool("Empty");
            new ExpectBadCreateTopoCmd(cmdSvc, "Candidate", "Empty", 100).run();

            /* As a sanity check, this one should be successful */
            cmdSvc.createTopology("shouldBeOk", "SNPool", 100, false);

            /* Create the same topo candidate again should be successfully */
            cmdSvc.createTopology("shouldBeOk", "SNPool", 100, false);

            /* We shouldn't be able to create it again with different layout */

            /* Different partition number */
            new ExpectBadCreateTopoCmd
                (cmdSvc, "shouldBeOk", "SNPool", 10).run();

            /* Different pool */
            try {
                cmdSvc.addStorageNodePool("NewSNPool");
                new ExpectBadCreateTopoCmd
                    (cmdSvc, "shouldBeOk", "NewSNPool", 100).run();
            } finally {
                cmdSvc.removeStorageNodePool("NewSNPool");
            }

        } catch (RemoteException remoteEx) {
            fail("The command service should have stayed up and available " +
                 " but got " + remoteEx);
        }
    }

    /**
     * Execute a variety of bad SNPool setup commands.
     * @throws RemoteException
     */
    private void stressSNPoolSetup(final CommandServiceAPI cmdSvc)
        throws RemoteException {

        /**
         * Specify incorrect storage nodes and pools
         */
        cmdSvc.addStorageNodePool(null);

        /* Try to add a SN to a non-existing pool. */
        new ExpectBadCmd() {
            @Override
                void doCmd() throws RemoteException {
                cmdSvc.addStorageNodeToPool("BadPool",
                                            new StorageNodeId(1));
            }
        }.run();

        cmdSvc.addStorageNodePool("SNPool");

        /* Try to add an invalid SN to an existing pool. */
        new ExpectBadCmd() {
            @Override
                void doCmd() throws RemoteException {
                cmdSvc.addStorageNodeToPool("SNPool",
                                            new StorageNodeId(5));
            }
        }.run();
    }

    /**
     * Execute a variety of bad data center setup plans.
     * @throws RemoteException
     */
    private void stressDataCenterSetup(final CommandServiceAPI cmdSvc)
        throws RemoteException {

        /* Create a datacenter with an illegal repfactor */
        new ExpectBadCmd() {
            @Override
                void doCmd() throws RemoteException {
                cmdSvc.createDeployDatacenterPlan(
                    "dcPlan", "East", -1, DatacenterType.PRIMARY, false, false);
            }
        }.run();

        final int planId = cmdSvc.createDeployDatacenterPlan(
            "dcPlan", "East", 1, DatacenterType.PRIMARY, false, false);
        /* Try to execute without approval. */
        new ExpectBadCmd() {
            @Override
                void doCmd() throws RemoteException {
                cmdSvc.executePlan(planId, false);
            }
        }.run();

        /* Try to approve some other plan. */
        new ExpectBadCmd() {
            @Override
                void doCmd() throws RemoteException {
                cmdSvc.executePlan(planId + 10, false);
            }
        }.run();

        /* Try to approve some other plan. */
        new ExpectBadCmd() {
            @Override
                void doCmd() throws RemoteException {
                cmdSvc.approvePlan(planId + 10);
            }
        }.run();

        cmdSvc.approvePlan(planId);
        cmdSvc.executePlan(planId, false);
        cmdSvc.awaitPlan(planId, 0, null);
        cmdSvc.assertSuccess(planId);

        /* We should be able to repeat the command */
        int id2 = cmdSvc.createDeployDatacenterPlan(
            "dcPlan", "East", 1, DatacenterType.PRIMARY, false, false);
        cmdSvc.approvePlan(id2);
        cmdSvc.executePlan(id2, false);
        cmdSvc.awaitPlan(id2, 0, null);
        cmdSvc.assertSuccess(id2);

        /* Try to repeat creating DC with different rep factor */
        new ExpectBadCmd() {
            @Override
                void doCmd() throws RemoteException {
                cmdSvc.createDeployDatacenterPlan(
                    "dcPlan", "East", 2, DatacenterType.PRIMARY, false, false);
            }
        }.run();

        /* Try to repeat creating DC with different type */
        new ExpectBadCmd() {
            @Override
                void doCmd() throws RemoteException {
                cmdSvc.createDeployDatacenterPlan(
                    "dcPlan", "East", 1, DatacenterType.SECONDARY, false,
                    false);
            }
        }.run();
    }

    /** Setup some SNs, should have a successful outcome. */
    private void setupSNs(CommandServiceAPI cmdSvc) throws RemoteException {

        /**
         * Deploy Datacenter.
         */
        int planId = cmdSvc.createDeployDatacenterPlan(
            "DCPlan", "Miami", 1, DatacenterType.PRIMARY, false, false);
        cmdSvc.approvePlan(planId);
        cmdSvc.executePlan(planId, false);
        cmdSvc.awaitPlan(planId, 0, null);
        cmdSvc.assertSuccess(planId);

        /**
         * Deploy first SN.
         */
        planId = cmdSvc.createDeploySNPlan
            ("Deploy SN", new DatacenterId(1), sna.getHostname(),
             sna.getRegistryPort(), "comment");
        cmdSvc.approvePlan(planId);
        cmdSvc.executePlan(planId, false);
        cmdSvc.awaitPlan(planId, 0, null);
        cmdSvc.assertSuccess(planId);

        /**
         * Deploy admin before second SN.
         */
        planId = cmdSvc.createDeployAdminPlan
            ("Deploy admin", new StorageNodeId(1));
        cmdSvc.approvePlan(planId);
        cmdSvc.executePlan(planId, false);
        cmdSvc.awaitPlan(planId, 0, null);
        cmdSvc.assertSuccess(planId);

        /**
         * Need a storage pool for the store deployment.
         */
        cmdSvc.addStorageNodePool("SNPool");
        cmdSvc.addStorageNodeToPool("SNPool", new StorageNodeId(1));
    }

    /**
     * ExpectBadCmd runs an admin operation that is expected to fail with an
     * IllegalCommandException
     */
    private abstract class ExpectBadCmd {

        abstract void doCmd() throws RemoteException;

        void run() {
            try {
                doCmd();
                fail("Was expected to fail");
            } catch (InternalFaultException expected) {
                /*
                 * Check that the underlying cause is an
                 * IllegalCommandException.
                 */
                assertEquals("got " + expected,
                             "oracle.kv.impl.admin.IllegalCommandException",
                             expected.getFaultClassName());
            } catch (Exception e) {
                fail ("Unexpected exception: " + e);
            }
        }
    }

    /**
     * Create a topology with bad inputs
     */
    private class ExpectBadCreateTopoCmd extends ExpectBadCmd {
        private final String pool;
        private final String candidateName;
        private final int nPartitions;
        private final CommandServiceAPI cmdSvc;

        ExpectBadCreateTopoCmd(CommandServiceAPI cmdSvc,
                                String candidateName,
                                String pool,
                                int nPartitions) {
            super();
            this.pool = pool;
            this.candidateName = candidateName;
            this.nPartitions = nPartitions;
            this.cmdSvc = cmdSvc;
        }

        @Override
        void doCmd() throws RemoteException {
            cmdSvc.createTopology(candidateName, pool, nPartitions, false);
        }
    }
}
