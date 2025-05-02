/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static org.junit.Assert.assertEquals;

import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeTestBase;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.StorageNodeMap;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.PingCollector;

import org.junit.Test;

/**
 * Tests the Admin, interfaces and persistability.
 */
public class IM1Test extends TestBase {

    private PortFinder portFinder1;
    private PortFinder portFinder2;
    private static final int startPort1 = 5000;
    private static final int startPort2 = 6000;
    private static final int haRange = 5;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        portFinder1 = new PortFinder(startPort1, haRange);
        portFinder2 = new PortFinder(startPort2, haRange);
        TestStatus.setManyRNs(true);
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        LoggerUtils.closeAllHandlers();
    }

    /**
     * Exercise the basic interfaces.
     * @throws Exception
     * @throws TimeoutException
     * @throws ExecutionException
     */
    @Test
    public void testBasic()
        throws Exception {

        StorageNodeAgent sna1 =
            StorageNodeTestBase.createUnregisteredSNA(portFinder1,
                                                      1,
                                                      "config0.xml",
                                                      false,
                                                      true,
                                                      1024);
        StorageNodeAgent sna2 =
            StorageNodeTestBase.createUnregisteredSNA(portFinder2,
                                                      1,
                                                      "config1.xml",
                                                      false,
                                                      true,
                                                      1024);

        StorageNodeAgentAPI snai1 =
            StorageNodeTestBase.getBootstrapHandle(sna1.getHostname(),
                                                   sna1.getRegistryPort(),
                                                   sna1.getLoginManager());
        assert(snai1 != null);
        StorageNodeAgentAPI snai2 =
            StorageNodeTestBase.getBootstrapHandle(sna2.getHostname(),
                                                   sna2.getRegistryPort(),
                                                   sna2.getLoginManager());
        assert(snai2 != null);

        CommandServiceAPI admin = ServiceUtils.waitForAdmin
            (sna1.getHostname(), sna1.getRegistryPort(),
             sna1.getLoginManager(), 5, ServiceStatus.RUNNING, logger);

        /**
         * Need to configure the admin before doing anything.
         */
        admin.configure(kvstoreName);

        /**
         * Deploy Datacenter.
         */
        int planId = admin.createDeployDatacenterPlan(
            "DCPlan", "Miami", 2, DatacenterType.PRIMARY, false, false);
        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);

        /**
         * Deploy first SN.
         */
        planId = admin.createDeploySNPlan
            ("Deploy SN", new DatacenterId(1), sna1.getHostname(),
             sna1.getRegistryPort(), "comment");
        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);

        /**
         * Deploy admin before second SN.
         */
        planId = admin.createDeployAdminPlan
            ("Deploy admin", new StorageNodeId(1));
        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);

        /**
         * Deploy second SN.
         */
        planId = admin.createDeploySNPlan
            ("Deploy SN2", new DatacenterId(1), sna2.getHostname(),
             sna2.getRegistryPort(), "comment");
        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);

        /**
         * Check some state.
         */
        Topology t = admin.getTopology();
        assertEquals(kvstoreName, t.getKVStoreName());
        Parameters p = admin.getParameters();
        assertEquals(kvstoreName, p.getGlobalParams().getKVStoreName());

        StorageNodeMap smap = t.getStorageNodeMap();
        assertEquals(smap.size(), 2);

        /**
         * Need a storage pool for the store deployment.
         */
        admin.addStorageNodePool("IM1TestPool");
        admin.addStorageNodeToPool("IM1TestPool", new StorageNodeId(1));
        admin.addStorageNodeToPool("IM1TestPool", new StorageNodeId(2));
        admin.createTopology("Candidate1", "IM1TestPool", 100, false);
        planId = admin.createDeployTopologyPlan("Deploy IM1Store",
                                                "Candidate1",null);
        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);

        /* Create and execute a StopRepNodesPlan */
        planId = admin.createStopAllRepNodesPlan("StopAllRN");
        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);
        PingCollector collector = new PingCollector(t, logger);
        for (Entry<ResourceId, ServiceStatus> entry :
            collector.getTopologyStatus().entrySet()) {

            if (entry.getKey().getType() == ResourceId.ResourceType.REP_NODE) {
                /*
                 * The rep nodes should show an UNREACHABLE status and should
                 * be marked as inactive.
                 */
                assertEquals(ServiceStatus.UNREACHABLE, entry.getValue());
            }
        }

        /* Create and execute a StartRepNodesPlan */
        planId = admin.createStartAllRepNodesPlan("StartAllRN");
        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);

        for (Entry<ResourceId, ServiceStatus> entry :
             collector.getTopologyStatus().entrySet()) {

            if (entry.getKey().getType() == ResourceId.ResourceType.REP_NODE) {
                /*
                 * The rep nodes should show a RUNNING status.
                 */
                assertEquals(ServiceStatus.RUNNING, entry.getValue());
            }
        }

        /*
         * Only rep nodes are ever really shutdown. Shut down the SNAs.  This
         * will shut down the admin and RNs as well.
         */
        sna1.shutdown(true, true);
        sna2.shutdown(true, true);
    }
}
