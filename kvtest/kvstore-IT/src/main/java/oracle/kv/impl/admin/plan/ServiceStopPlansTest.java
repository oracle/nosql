/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.plan;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminStatus;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.arb.ArbNodeStatus;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.util.CreateStore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

public class ServiceStopPlansTest extends TestBase {

    private final int numSNs = 3;
    private CreateStore createStore = null;

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (createStore != null) {
            createStore.shutdown(false);
        }
    }

    @Test(timeout=120000)
    public void testStopServicesPlanAllRN() throws Exception {
        createStore = new CreateStore(kvstoreName,
                                      5000,
                                      numSNs, /* Storage Nodes */
                                      3, /* Replication Factor */
                                      3, /* Partitions */
                                      1  /* capacity */);
        createStore.start();

        verifyAllRNRunning();

        int planId = createStore.getAdmin().
            createStopAllRepNodesPlan("stop all rns");
        runPlanAndVerify(
                planId, false /* not force */, true /* succeed */);
        verifyAllRNStopped();
    }

    @Test(timeout=120000)
    public void testStopPrimaryRNPlan() throws Exception {
        createStore = new CreateStore(kvstoreName,
                                      5000,
                                      numSNs, /* Storage Nodes */
                                      3, /* Replication Factor */
                                      3, /* Partitions */
                                      2  /* capacity */);
        createStore.start();
        Set<RepNodeId> nodes;
        nodes = new HashSet<RepNodeId>(Arrays.asList(
                    new RepNodeId(1, 1),
                    new RepNodeId(1, 2),
                    new RepNodeId(2, 1),
                    new RepNodeId(2, 2)));
        waitForStoreHealthy();
        verifyNodes(nodes, true);

        int planId;
        /* Stop rg1-rn1, rg2-rn1, should succeed */
        nodes = new HashSet<RepNodeId>(
                Arrays.asList(new RepNodeId(1, 1), new RepNodeId(2, 1)));
        planId = createStore.getAdmin().
            createStopServicesPlan("stop (rg1-rn1, rg2-rn1)", nodes);
        runPlanAndVerify(
                planId, false /* not force */, true /* succeed */);
        verifyNodes(nodes, false);

        /* Stop rg1-rn2, rg2-rn2 should fail without force */
        nodes = new HashSet<RepNodeId>(
                Arrays.asList(new RepNodeId(1, 2), new RepNodeId(2, 2)));
        planId = createStore.getAdmin().
            createStopServicesPlan("stop (rg1-rn2, rg2-rn2) no force", nodes);
        runPlanAndVerify(
                planId, false /* not force */, false /* fail */);

        /* Stop rg1-rn2, rg2-rn2 should succeed with force */
        nodes = new HashSet<RepNodeId>(
                Arrays.asList(new RepNodeId(1, 2), new RepNodeId(2, 2)));
        planId = createStore.getAdmin().
            createStopServicesPlan("stop (rg1-rn2, rg2-rn2) force", nodes);
        runPlanAndVerify(
                planId, true /* force */, true /* succeed */);
        verifyNodes(nodes, false);
    }

    @Test(timeout=120000)
    public void testStopPrimaryAdminPlan() throws Exception {
        createStore = new CreateStore(kvstoreName,
                                      5000,
                                      numSNs, /* Storage Nodes */
                                      3, /* Replication Factor */
                                      3, /* Partitions */
                                      1  /* capacity */);
        createStore.start();
        Set<AdminId> nodes;

        List<AdminId> stopNodes = new ArrayList<AdminId>();
        addNonMasterAdmins(stopNodes);
        nodes = new HashSet<AdminId>(stopNodes);
        waitForStoreHealthy();
        verifyNodes(nodes, true);

        int planId;
        /* Stop first admin should succeed */
        nodes = new HashSet<AdminId>(Arrays.asList(stopNodes.get(0)));
        planId = createStore.getAdmin().
            createStopServicesPlan("stop " + stopNodes.get(0), nodes);
        runPlanAndVerify(
                planId, false /* not force */, true /* succeed */);
        verifyNodes(nodes, false);

        /* Stop second admin should fail without force */
        nodes = new HashSet<AdminId>(Arrays.asList(stopNodes.get(1)));
        planId = createStore.getAdmin().
            createStopServicesPlan(
                    "stop no force " + stopNodes.get(1), nodes);
        runPlanAndVerify(
                planId, false /* not force */, false /* fail */);
    }

    @Test(timeout=120000)
    public void testStopArbiterPlan() throws Exception {
        createStore = new CreateStore(kvstoreName,
                                      5000,
                                      numSNs, /* Storage Nodes */
                                      2, /* Replication Factor */
                                      3, /* Partitions */
                                      1  /* capacity */);
        createStore.setAllowArbiters(true);
        createStore.start();
        Set<? extends ResourceId> nodes;
        nodes = new HashSet<ResourceId>(Arrays.asList(
                    new RepNodeId(1, 1),
                    new ArbNodeId(1, 1)));
        waitForStoreHealthy();
        verifyNodes(nodes, true);

        int planId;
        /* Stop rg1-rn1 should succeed */
        nodes = new HashSet<ResourceId>(
                Arrays.asList(new RepNodeId(1, 1)));
        planId = createStore.getAdmin().
            createStopServicesPlan("stop rg1-rn1", nodes);
        runPlanAndVerify(
                planId, false /* not force */, true /* succeed */);
        verifyNodes(nodes, false);

        /* Stop rg1-an1 should fail without force */
        nodes = new HashSet<ResourceId>(
                Arrays.asList(new ArbNodeId(1, 1)));
        planId = createStore.getAdmin().
            createStopServicesPlan("stop rg1-an1 no force", nodes);
        runPlanAndVerify(
                planId, false /* not force */, false /* fail */);
    }

    @Test(timeout=120000)
    public void testStopSecondaryRNPlan() throws Exception {
        CreateStore.ZoneInfo zone1 = new CreateStore.ZoneInfo(3);
        CreateStore.ZoneInfo zone2 =
            new CreateStore.ZoneInfo(3, DatacenterType.SECONDARY);
        createStore = new CreateStore(kvstoreName,
                                      5000,
                                      6, /* Storage Nodes */
                                      Arrays.asList(zone1, zone2), /* zones */
                                      6, /* Partitions */
                                      1  /* capacity */);
        createStore.start();
        Set<RepNodeId> nodes;
        nodes = new HashSet<RepNodeId>(Arrays.asList(
                    new RepNodeId(1, 4),
                    new RepNodeId(1, 5),
                    new RepNodeId(1, 6)));
        waitForStoreHealthy();
        verifyNodes(nodes, true);

        int planId;
        /* Stop all secondary nodes should succeed */
        planId = createStore.getAdmin().
            createStopServicesPlan("stop secondary nodes", nodes);
        runPlanAndVerify(
                planId, false /* not force */, true /* succeed */);
        verifyNodes(nodes, false);
    }

    @Test(timeout=300000)
    public void testGetMetadataWithoutQuorum() throws Exception {
        createStore = new CreateStore(kvstoreName,
                                      5000,
                                      numSNs, /* Storage Nodes */
                                      3, /* Replication Factor */
                                      3, /* Partitions */
                                      1  /* capacity */);
        createStore.start();
        Set<AdminId> nodes;

        List<AdminId> stopNodes = new ArrayList<AdminId>();
        addNonMasterAdmins(stopNodes);
        nodes = new HashSet<AdminId>(stopNodes);
        waitForStoreHealthy();
        verifyNodes(nodes, true);

        int planId;
        /* Stop first admin should succeed */
        nodes = new HashSet<AdminId>(Arrays.asList(stopNodes.get(0)));
        planId = createStore.getAdmin().
            createStopServicesPlan("stop " + stopNodes.get(0), nodes);
        runPlanAndVerify(
                planId, false /* not force */, true /* succeed */, 0);
        verifyNodes(nodes, false);

        /* Stop second admin should succeed with force */
        nodes = new HashSet<AdminId>(Arrays.asList(stopNodes.get(1)));
        planId = createStore.getAdmin().
            createStopServicesPlan(
                    "stop no force " + stopNodes.get(1), nodes);
        runPlanAndVerify(
                planId, true /* force */, true /* succeed */, 30);
        verifyNodes(nodes, false);

        TableMetadata tableMd = createStore.getAdmin().
                getMetadata(TableMetadata.class, MetadataType.TABLE);
        if (tableMd == null) {
            // test failed
            fail("tableMd is null, test failed");
        }
    }

    private void runPlanAndVerify(int planId,
                                  boolean force,
                                  boolean shouldSucceed,
                                  int awaitTimeout) throws Exception {

        CommandServiceAPI cs = createStore.getAdmin();
        String name = cs.getPlanById(planId).getName();
        cs.approvePlan(planId);
        Throwable t = runPlan(planId, force, awaitTimeout);
        if (awaitTimeout == 0) {
           if (shouldSucceed) {
                if (t == null) {
                    cs.assertSuccess(planId);
                    return;
                }
                throw new RuntimeException(t);
            }

            if (t == null) {
                fail(name + " should fail");
            }
        }
    }

    private void runPlanAndVerify(int planId,
            boolean force,
            boolean shouldSucceed) throws Exception {
        runPlanAndVerify(planId, force, shouldSucceed, 0);
    }

    private Throwable runPlan(int planId, boolean force, int awaitTimeout) throws Exception {
        CommandServiceAPI cs = createStore.getAdmin();
        try {
            cs.executePlan(planId, force);
            TimeUnit timeUnit = (awaitTimeout != 0) ? TimeUnit.SECONDS : null;
            cs.awaitPlan(planId, awaitTimeout, timeUnit);
        } catch (Throwable t) {
            return t;
        }
        return null;
    }

    /**
     * Wait for all the nodes in store to become healthy.
     */
    private void waitForStoreHealthy() throws Exception {
        while (true) {
            if (isStoreHealthy()) {
                break;
            }
            Thread.sleep(1000);
        }
    }

    /**
     * Checks for store healthy.
     *
     * Checks by writing several records to the store which requires the
     * strongest durability.
     */
    private boolean isStoreHealthy() throws Exception {
        try {
            verifyAllRNRunning();
        } catch (Throwable t) {
            return false;
        }
        return true;
    }

    private void verifyNodes(Set<? extends ResourceId> nodes,
                             StatusVerifier<RepNodeStatus> rnVerifier,
                             StatusVerifier<AdminStatus> adminVerifier,
                             StatusVerifier<ArbNodeStatus> anVerifier)
        throws Exception {

        RegistryUtils regUtils = new RegistryUtils(
                createStore.getAdmin().getTopology(),
                createStore.getAdminLoginManager(), logger);
        for (ResourceId resId : nodes) {
            if (resId instanceof RepNodeId) {
                RepNodeId rnId = (RepNodeId) resId;
                RepNodeStatus s = null;
                Throwable t = null;
                try {
                    s = regUtils.getRepNodeAdmin(rnId).ping();
                } catch (Throwable tt) {
                    t = tt;
                }
                rnVerifier.verify(s, t);
            } else if (resId instanceof AdminId) {
                AdminId adminId = (AdminId) resId;
                AdminStatus s = null;
                Throwable t = null;
                try {
                    s = regUtils.
                        getAdmin(createStore.getStorageNodeId(adminId)).
                        getAdminStatus();
                } catch (Throwable tt) {
                    t = tt;
                }
                adminVerifier.verify(s, t);
            } else if (resId instanceof ArbNodeId) {
                ArbNodeId arbId = (ArbNodeId) resId;
                ArbNodeStatus s = null;
                Throwable t = null;
                try {
                    s = regUtils.getArbNodeAdmin(arbId).ping();
                } catch (Throwable tt) {
                    t = tt;
                }
                anVerifier.verify(s, t);
            }
        }
    }

    private interface StatusVerifier<S> {
        void verify(S status, Throwable t);
    }

    private void verifyAllRNRunning() throws Exception {
        Set<RepNodeId> nodes = new HashSet<RepNodeId>();
        for (StorageNodeId snId : createStore.getStorageNodeIds()) {
            nodes.addAll(createStore.getRNs(snId));
        }
        verifyNodes(nodes, true);
    }

    private void verifyAllRNStopped() throws Exception {
        Set<RepNodeId> nodes = new HashSet<RepNodeId>();
        for (StorageNodeId snId : createStore.getStorageNodeIds()) {
            nodes.addAll(createStore.getRNs(snId));
        }
        verifyNodes(nodes, false);
    }

    private void verifyNodes(Set<? extends ResourceId> nodes,
                             boolean running) throws Exception {

        verifyNodes(nodes,
                (s, t) -> {
                    if (running) {
                        assertEquals(
                                s.getServiceStatus(), ServiceStatus.RUNNING);
                    } else {
                        assertEquals(s, null);
                    }
                },
                (s, t) -> {
                    if (running) {
                        assertEquals(
                                s.getServiceStatus(), ServiceStatus.RUNNING);
                    } else {
                        assertEquals(s, null);
                    }
                },
                (s, t) -> {
                    if (running) {
                        assertEquals(
                                s.getServiceStatus(), ServiceStatus.RUNNING);
                    } else {
                        assertEquals(s, null);
                    }
                });
    }

    private void addNonMasterAdmins(List<AdminId> result)
        throws Exception {

        for (int i = 0; i < numSNs; ++i) {
            AdminId adminId = createStore.getAdminId(i);
            if (adminId == null) {
                continue;
            }
            CommandServiceAPI admin = createStore.getAdmin(i);
            if (admin.getAdminStatus().getIsAuthoritativeMaster()) {
                continue;
            }
            result.add(adminId);
        }
    }
}

