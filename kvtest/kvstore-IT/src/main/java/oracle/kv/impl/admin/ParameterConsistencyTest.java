/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static oracle.kv.impl.param.ParameterState.AP_WAIT_TIMEOUT;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import oracle.kv.TestBase;
import oracle.kv.TestClassTimeoutMillis;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.topo.Validations.ExcessAdmins;
import oracle.kv.impl.admin.topo.Validations.MissingRootDirectorySize;
import oracle.kv.impl.arb.admin.ArbNodeAdminAPI;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;

import com.sleepycat.je.CacheMode;

import org.junit.Test;

/**
 * Test parameter changes while there are components that
 * are not available and that the parameter consistency thread
 * corrects the inconsistency.
 *
 * The AP_WAIT_TIMEOUT controls the amount of time a plan/task will
 * wait for a node to become ready. This value is set to a smaller value because
 * the parameter consistency thread may be executing while the test shuts
 * down a KV Node (SN, RN, AN). Setting to a smaller than default value
 * limits the time the test thread may be blocked by parameter consistency
 * plan locks. Too small of a timeout causes the plan to fail because the
 * node is not ready in time.
 */
/* Increase test timeout to 60 minutes -- test can take 40 minutes */
@TestClassTimeoutMillis(60*60*1000)
public class ParameterConsistencyTest extends TestBase {

    private CreateStore createStore;
    private static final int startPort = 5000;
    private static final long MAX_WAIT = 2 * 60 * 1000;
    private static final String AP_WAIT_TIMEOUT_VALUE = "30 SECONDS";

    @Override
    public void setUp()
        throws Exception {
        TestStatus.setManyRNs(true);
        super.setUp();
    }

    @Override
    public void tearDown()
        throws Exception {
        super.tearDown();
        if (createStore != null) {
            createStore.shutdown();
        }
        LoggerUtils.closeAllHandlers();
    }

    /**
     * Test changing parameters when two SN's are down.
     * Checks that the parameter consistency
     * thread corrects the inconsistency.
     */
    @Test
    public void testPConWithSNDown()
                    throws Exception {
        int snToKill[] = {5,8};

        createStore =
            new CreateStore(kvstoreName,
                            startPort,
                            9, /* Storage Nodes */
                            3, /* Replication Factor */
                            300, /* Partitions */
                            1 /* capacity */);
        createStore.start();
        CommandServiceAPI cs = createStore.getAdmin();
        /* Change the interval for KVAdminMetadata thread. */
        ParameterMap map = new ParameterMap();
        map.setType(ParameterState.ADMIN_TYPE);
        map.setParameter(ParameterState.AP_PARAM_CHECK_INTERVAL, "10 SECONDS");
        map.setParameter(ParameterState.AP_VERSION_CHECK_INTERVAL,
                         "10 SECONDS");
       map.setParameter(AP_WAIT_TIMEOUT, AP_WAIT_TIMEOUT_VALUE);

        /*
         * Enable admin threads that may create and execute plans.
         */
        map.setParameter(ParameterState.AP_PARAM_CHECK_ENABLED, "true") ;
        map.setParameter(ParameterState.AP_VERSION_CHECK_ENABLED, "true");

        int p = cs.createChangeAllAdminsPlan("changeAdminParams", null, map);
        cs.approvePlan(p);
        cs.executePlan(p, false);
        cs.awaitPlan(p, 0, null);
        cs.assertSuccess(p);

        /*
         * Sleep to let the version thread update the SN versions so
         * parameter checks do not need to go to the remote SN.
         */
        Thread.sleep(1000 * 11);
        StorageNodeId sns[] = createStore.getStorageNodeIds();

        StorageNodeId snIdToKill[] = new StorageNodeId[snToKill.length];
        for (int i = 0; i< snToKill.length; i++) {
            snIdToKill[i] = sns[snToKill[i]];
        }

        /*
         * Look for violations after topology deployment
         */
        DeployUtils.checkDeployment(cs, 3 , 5000, logger,
                                    9, MissingRootDirectorySize.class);

        changeParamsInternal(snIdToKill, 9, 0, false);
    }

    /**
     * Test changing parameters when RN is down. As part of the parameter
     * change, the RN needs to be rebooted. The stop/start operations will
     * restart the node. Checks that the parameter consistency
     * thread corrects the inconsistency.
     */
    @Test
    public void testPConWithRNDown() throws Exception {
        testPConRNDownInternal(false);
    }

    /**
     * Test changing parameters RN is disabled. As part of the
     * parameter change, the RN needs to be rebooted.Checks that the parameter
     * consistency thread corrects the inconsistency.
     */
    @Test
    public void testPConWithRNDownDisabled() throws Exception {
        testPConRNDownInternal(true);
    }

    /**
     * Changes AN parameter with SN down. Checks that the parameter consistency
     * thread corrects the inconsistency.
     */
    @Test
    public void testPConWithAN() throws Exception {
        testPConANInternal(false);
    }

    /**
     * Change AN parameters SN is disabled. Checks that the parameter
     * consistency thread corrects the inconsistency.
     */
    @Test
    public void testPConWithANDisable() throws Exception {
        testPConANInternal(true);
    }

    /**
     * Test changing parameters when an AN is down. Waits for
     * parameter consistency thread to correct the inconsistency.
     */
    @Test
    public void testPConWithANDown()
        throws Exception {
        createStore =
            new CreateStore(kvstoreName,
                            startPort,
                            8, /* Storage Nodes */
                            2, /* Replication Factor */
                            300, /* Partitions */
                            1 /* capacity */);
        createStore.setAllowArbiters(true);
        createStore.start();
        CommandServiceAPI cs = createStore.getAdmin();
        /* Change the interval for KVAdminMetadata thread. */
        ParameterMap map = new ParameterMap();
        map.setType(ParameterState.ADMIN_TYPE);
        map.setParameter(ParameterState.AP_PARAM_CHECK_INTERVAL, "10 SECONDS");
        map.setParameter(
            ParameterState.AP_VERSION_CHECK_INTERVAL, "10 SECONDS") ;
        map.setParameter(AP_WAIT_TIMEOUT, AP_WAIT_TIMEOUT_VALUE);

        /*
         * Enable admin threads that may create and execute plans.
         */
        map.setParameter(ParameterState.AP_PARAM_CHECK_ENABLED, "true") ;
        map.setParameter(ParameterState.AP_VERSION_CHECK_ENABLED, "true");

        int p = cs.createChangeAllAdminsPlan("changeAdminParams", null, map);
        cs.approvePlan(p);
        cs.executePlan(p, false);
        cs.awaitPlan(p, 0, null);
        cs.assertSuccess(p);

        /*
         * Sleep to let the version thread update the SN versions so
         * parameter checks do not need to go to the remote SN.
         */
        Thread.sleep(1000 * 21);

        ArbNodeId idToKill[] = new ArbNodeId[1];
        idToKill[0] = new ArbNodeId(2,1);

        /*
         * Look for violations after topology deployment
         */
        DeployUtils.checkDeployment(cs, 3 , 5000, logger,
                                    1,ExcessAdmins.class,
                                    8, MissingRootDirectorySize.class);

        changeParamsInternal(idToKill, 8, 4, false);
    }

    private void testPConRNDownInternal(boolean disableNode)
        throws Exception {
        createStore =
            new CreateStore(kvstoreName,
                            startPort,
                            9, /* Storage Nodes */
                            3, /* Replication Factor */
                            300, /* Partitions */
                            1 /* capacity */);
        createStore.start();
        CommandServiceAPI cs = createStore.getAdmin();
        /* Change the interval for KVAdminMetadata thread. */
        ParameterMap map = new ParameterMap();
        map.setType(ParameterState.ADMIN_TYPE);
        map.setParameter(ParameterState.AP_PARAM_CHECK_INTERVAL, "10 SECONDS");
        map.setParameter(ParameterState.AP_VERSION_CHECK_INTERVAL,
                         "10 SECONDS");
        map.setParameter(AP_WAIT_TIMEOUT, AP_WAIT_TIMEOUT_VALUE);

        /*
         * Enable admin threads that may create and execute plans.
         */
        map.setParameter(ParameterState.AP_PARAM_CHECK_ENABLED, "true") ;
        map.setParameter(ParameterState.AP_VERSION_CHECK_ENABLED, "true");

        int p = cs.createChangeAllAdminsPlan("changeAdminParams", null, map);
        cs.approvePlan(p);
        cs.executePlan(p, false);
        cs.awaitPlan(p, 0, null);
        cs.assertSuccess(p);

        /*
         * Sleep to let the version thread update the SN versions so
         * parameter checks do not need to go to the remote SN.
         */
        Thread.sleep(1000 * 11);

        /*
         * Look for violations after topology deployment
         */
        DeployUtils.checkDeployment(cs, 3 , 5000, logger,
                                    9, MissingRootDirectorySize.class);
        RepNodeId[] rnsToKill = new RepNodeId[1];
        rnsToKill[0] = new RepNodeId(2,1);

        changeParamsInternal(rnsToKill, 9, 0, disableNode);
    }

    private void testPConANInternal(boolean disableNode)
        throws Exception {
        int snToKill[] = {4};
        createStore =
            new CreateStore(kvstoreName,
                            startPort,
                            8, /* Storage Nodes */
                            2, /* Replication Factor */
                            300, /* Partitions */
                            1 /* capacity */);
        createStore.setAllowArbiters(true);
        createStore.start();
        CommandServiceAPI cs = createStore.getAdmin();
        /* Change the interval for KVAdminMetadata thread. */
        ParameterMap map = new ParameterMap();
        map.setType(ParameterState.ADMIN_TYPE);
        map.setParameter(ParameterState.AP_PARAM_CHECK_INTERVAL, "10 SECONDS");
        map.setParameter(
            ParameterState.AP_VERSION_CHECK_INTERVAL, "10 SECONDS");
        map.setParameter(AP_WAIT_TIMEOUT, AP_WAIT_TIMEOUT_VALUE);

        /*
         * Enable admin threads that may create and execute plans.
         */
        map.setParameter(ParameterState.AP_PARAM_CHECK_ENABLED, "true") ;
        map.setParameter(ParameterState.AP_VERSION_CHECK_ENABLED, "true");

        int p = cs.createChangeAllAdminsPlan("changeAdminParams", null, map);
        cs.approvePlan(p);
        cs.executePlan(p, false);
        cs.awaitPlan(p, 0, null);
        cs.assertSuccess(p);

        /*
         * Sleep to let the version thread update the SN versions so
         * parameter checks do not need to go to the remote SN.
         */
        Thread.sleep(1000 * 31);
        StorageNodeId sns[] = createStore.getStorageNodeIds();

        StorageNodeId snIdToKill[] = new StorageNodeId[snToKill.length];
        for (int i = 0; i< snToKill.length; i++) {
           snIdToKill[i] = sns[snToKill[i]];
        }

        /*
         * Look for violations after topology deployment
         */
        DeployUtils.checkDeployment(cs, 3 , 5000, logger,
                                    1,ExcessAdmins.class,
                                    8, MissingRootDirectorySize.class);

        /*
         * Do check to insure that the SN killed hosts an AN.
         */
        Topology t = cs.getTopology();
        Set<ArbNodeId> anids = t.getArbNodeIds();
        boolean foundOne = false;
        for (ArbNodeId id : anids) {
            ArbNode an = t.get(id);
            if (an.getStorageNodeId().equals(snIdToKill[0])) {
                foundOne = true;
            }
        }
        assertTrue(foundOne);

        changeParamsInternal(snIdToKill, 8, 4, disableNode);
    }

    /*
     * Shuts down specified SN and change RN and AN parameters. The
     * SN may have RNs, ANs or both hosted on the node.
     * If specified, Shutdown node.
     * Change all RN's parameter. The parameter change does not require a
     * reboot.
     * Change all AN's parameter. The parameter change does require a AN reboot.
     * if node was shutdown restart node.
     * Check if parameter consistency thread corrects the parameter
     * inconsistency.
     */
    private void changeParamsInternal(ResourceId idsToKill[],
                                      int numRNs,
                                      int numANs,
                                      boolean disableNode)
        throws Exception {
        final String JE_NEW_MISC = "je.rep.repstreamOpenTimeout=10 s";
        ArbNodeAdminAPI anai;

        CommandServiceAPI cs = createStore.getAdmin();
        Topology t = cs.getTopology();

        Set<RepNodeId> ids = t.getRepNodeIds();
        assertEquals(numRNs, ids.size());
        Parameters parms = cs.getParameters();

        Set<ArbNodeId> anids = t.getArbNodeIds();
        assertEquals(numANs, anids.size());

        StorageNodeId sns[] = createStore.getStorageNodeIds();

        HashMap<StorageNodeId, Integer> snIdToIdx =
            new HashMap<StorageNodeId,Integer>();
        for (int i = 0; i < sns.length; i++) {
            snIdToIdx.put(sns[i], i);
        }

        ArbNodeId anIdToKill = null;
        HashSet<RepNodeId> rnIdToKill = new HashSet<RepNodeId>();
        StorageNodeId snHostingKilledObj[] =
            new StorageNodeId[idsToKill.length];

        Set<StorageNodeId> killedSNs = new HashSet<StorageNodeId>();

        /*
         * Setup some variables based on the object that
         * will be killed.
         */
        for (int i=0; i< idsToKill.length; i++) {
            ResourceId idToKill = idsToKill[i];
            if (idToKill instanceof StorageNodeId) {
                 killedSNs.add((StorageNodeId)idToKill);

                 Set<RepNodeId> rnsToKill =
                     t.getHostedRepNodeIds((StorageNodeId)idToKill);
                 if (idToKill != null) {
                     for (RepNodeId rnId : rnsToKill) {
                         rnIdToKill.add(rnId);
                         snHostingKilledObj[i] = (StorageNodeId)idToKill;
                         break;
                     }
                 }

                 Set<ArbNodeId> ansToKill =
                     t.getHostedArbNodeIds((StorageNodeId)idToKill);

                 if (idToKill != null) {
                     for (ArbNodeId anId : ansToKill) {
                         anIdToKill = anId;
                         break;
                     }
                 }
             } else if (idToKill instanceof RepNodeId) {
                 rnIdToKill.add((RepNodeId)idToKill);
                 RepNode rn = t.get((RepNodeId)idToKill);
                 snHostingKilledObj[i] = rn.getStorageNodeId();
             } else if (idToKill instanceof ArbNodeId) {
                 anIdToKill = (ArbNodeId)idToKill;
                 ArbNode an = t.get(anIdToKill);
                 snHostingKilledObj[i] = an.getStorageNodeId();
             }
        }

        /*
         * Set up new AN parameters. This change will cause AN to be restarted.
         */
        ParameterMap newANp;
        if (anids.size() > 0) {
            newANp = new ParameterMap();
            newANp.setType(ParameterState.ARBNODE_TYPE);
            newANp.setParameter(ParameterState.JE_MISC, JE_NEW_MISC);
        } else {
            newANp = null;
        }

        /*
         * Get and save original parameters as the canonical set.
         */
        RegistryUtils ru =
            new RegistryUtils(t, createStore.getSNALoginManager(0), logger);
        ParameterMap origGlobal = cs.getPolicyParameters();

        /* Get the thread dump interval so we can change it to a new value */
        DurationParameter dp =
            (DurationParameter)origGlobal.get(ParameterState.SP_DUMP_INTERVAL);

        HashMap<ResourceId,ParameterMap> originalParams =
            new HashMap<ResourceId,ParameterMap>(ids.size());

        for (StorageNodeId snid : sns) {
            StorageNodeAgentAPI snai = ru.getStorageNodeAgent(snid);
            LoadParameters lp = snai.getParams();
            ParameterMap map = lp.getMap(ParameterState.SNA_TYPE);
            originalParams.put(snid, map);
        }

        for (RepNodeId id : ids) {
            RepNodeAdminAPI rnai = ru.getRepNodeAdmin(id);
            LoadParameters lp = rnai.getParams();
            ParameterMap map = lp.getMap(id.getFullName(),
                                         ParameterState.REPNODE_TYPE);
            assertTrue(dp.equals(map.get(ParameterState.SP_DUMP_INTERVAL)));
            originalParams.put(id, map);
        }

        for (ArbNodeId id : anids) {
            anai = ru.getArbNodeAdmin(id);
            LoadParameters lp = anai.getParams();
            ParameterMap map = lp.getMap(id.getFullName(),
                                         ParameterState.ARBNODE_TYPE);
            originalParams.put(id, map);
        }

        /* Shutdown specified node */
        for (int i = 0; i < idsToKill.length; i++) {
            ResourceId idToKill = idsToKill[i];
            if (idToKill instanceof StorageNodeId) {
                createStore.shutdownSNA(snIdToIdx.get(idToKill),
                                        true);
            } else if (idToKill instanceof RepNodeId) {
                if (disableNode) {
                    Set<RepNodeId> stopRnIds = new HashSet<RepNodeId>();
                    stopRnIds.add((RepNodeId)idToKill);
                    int killPlanId =
                        cs.createStopServicesPlan("StopRepNode_"+idToKill,
                                                  stopRnIds);
                    cs.approvePlan(killPlanId);
                    cs.executePlan(killPlanId, false);
                    cs.awaitPlan(killPlanId, 0, null);
                    cs.assertSuccess(killPlanId);
                } else {
                    RepNodeAdminAPI rna =
                        createStore.getRepNodeAdmin((RepNodeId)idToKill);
                    rna.shutdown(true);
                }
            } else if (idToKill instanceof ArbNodeId) {
                if (disableNode) {
                    Set<ArbNodeId> stopAnIds = new HashSet<ArbNodeId>();
                    stopAnIds.add((ArbNodeId)idToKill);
                    int killPlanId =
                        cs.createStopServicesPlan("StopArbNode_"+idToKill,
                                                  stopAnIds);
                    cs.approvePlan(killPlanId);
                    cs.executePlan(killPlanId, false);
                    cs.awaitPlan(killPlanId, 0, null);
                    cs.assertSuccess(killPlanId);
                } else {
                    ArbNodeAdminAPI rna =
                        createStore.getArbNodeAdmin((ArbNodeId)idToKill);
                    rna.shutdown(true);
                }
            }
        }

        /* change parameter value */
        dp.setMillis(dp.toMillis() * 2);
        /* Check the new value. */
        for (RepNodeId id : ids) {
            /* Change RN parameters */
            ParameterMap newMap = originalParams.get(id).copy();
            newMap.put(dp);
            int planId =
                cs.createChangeParamsPlan("ChangeParams_"+id, id, newMap);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            cs.assertSuccess(planId);
        }

        /* Check the new value. */
        for (RepNodeId id : ids) {
            if (rnIdToKill.contains(id)) {
                continue;
            }
            RepNodeAdminAPI rnai = ru.getRepNodeAdmin(id);
            LoadParameters lp = rnai.getParams();
            ParameterMap map = lp.getMap(id.getFullName(),
                                         ParameterState.REPNODE_TYPE);
            ParameterMap diff = originalParams.get(id).diff(map, false);
            assertEquals(1, diff.size());
            assertTrue(dp.equals(map.get(ParameterState.SP_DUMP_INTERVAL)));
        }

        /* Change AN parameters */
        for (ArbNodeId anid : anids) {

            int planId =
                cs.createChangeParamsPlan("ChangeParams_"+anid,
                                               anid, newANp);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            cs.assertSuccess(planId);
        }
        /* Check the new value. */
        for (ArbNodeId id : anids) {
            if (anIdToKill != null &&
                anIdToKill.equals(id)) {
                continue;
            }
            anai = ru.getArbNodeAdmin(id);
            LoadParameters lp = anai.getParams();
            ParameterMap map =
                lp.getMap(id.getFullName(), ParameterState.ARBNODE_TYPE);
            ParameterMap diff = originalParams.get(id).diff(map, false);
            assertEquals(1, diff.size());
            String jep = map.get(ParameterState.JE_MISC).asString();
            assertTrue(JE_NEW_MISC.equals(jep));
        }

        for (int i = 0; i < idsToKill.length; i++)
        {
            ResourceId idToKill = idsToKill[i];
            /*
             * If a node was killed, restart it and wait for the
             * parameter consistency thread to correct the inconsistency.
             */
            if (idToKill instanceof StorageNodeId) {
                createStore.startSNA(snIdToIdx.get(idToKill));
            } else if (idToKill instanceof RepNodeId) {
                if (disableNode) {
                    boolean notRunning = false;
                    try {
                        ru.getRepNodeAdmin((RepNodeId)idToKill);
                    } catch (NotBoundException e) {
                        notRunning = true;
                    }
                    /*
                     * Check to insure node is not running.
                     */
                    assertTrue(notRunning);
                    Set<RepNodeId> rnIds = new HashSet<RepNodeId>();
                    rnIds.add((RepNodeId)idToKill);
                    int pid =
                        cs.createStartServicesPlan("StartRepNode_"+idToKill,
                                                    rnIds);
                    cs.approvePlan(pid);
                    cs.executePlan(pid, false);
                    cs.awaitPlan(pid, 0, null);
                    cs.assertSuccess(pid);
                } else {
                    StorageNodeAgent sna =
                        createStore.getStorageNodeAgent(snHostingKilledObj[i]);
                    sna.startRepNode((RepNodeId)idToKill);
                }
            } else if (idToKill instanceof ArbNodeId){
                if (disableNode) {
                    boolean notRunning = false;
                    try {
                        ru.getArbNodeAdmin((ArbNodeId)idToKill);
                    } catch (NotBoundException e) {
                        notRunning = true;
                    }
                    /*
                     * Check to insure node is not running.
                     */
                    assertTrue(notRunning);
                    Set<ArbNodeId> anIds = new HashSet<ArbNodeId>();
                    anIds.add((ArbNodeId)idToKill);
                    int pid =
                        cs.createStartServicesPlan("StartArbNode_"+idToKill,
                                                   anIds);
                    cs.approvePlan(pid);
                    cs.executePlan(pid, false);
                    cs.awaitPlan(pid, 0, null);
                    cs.assertSuccess(pid);
                } else {
                    StorageNodeAgent sna =
                        createStore.getStorageNodeAgent(snHostingKilledObj[i]);
                    sna.startArbNode((ArbNodeId)idToKill);
                }
            }
        }

        for (RepNodeId rnKilled : rnIdToKill) {
            /* Wait for admin thread to fix parameters */
            long startTime = System.currentTimeMillis();
            ParameterMap diff = null;
            while ((System.currentTimeMillis() - startTime) < MAX_WAIT) {
                try {
                    RepNodeAdminAPI rnai = ru.getRepNodeAdmin(rnKilled);
                    LoadParameters lp = rnai.getParams();
                    ParameterMap map =
                        lp.getMapByType(ParameterState.REPNODE_TYPE);
                    diff = originalParams.get(rnKilled).diff(map, false);
                    if (diff.size() > 0) {
                        break;
                    }
                } catch (RemoteException | NotBoundException e) {
                    /* Ignore may not be set up yet */
                }
                Thread.sleep(1000);
            }
            assertTrue(diff != null);
            assertEquals(1, diff.size());
        }

        if (anIdToKill != null) {
            long startTime = System.currentTimeMillis();
            ParameterMap diff = null;
            while ((System.currentTimeMillis() - startTime) < MAX_WAIT) {
                try {
                    /* Wait for admin thread to fix parameters */
                    anai = ru.getArbNodeAdmin(anIdToKill);
                    LoadParameters lp = anai.getParams();
                    ParameterMap map =
                        lp.getMapByType(ParameterState.ARBNODE_TYPE);
                    diff = originalParams.get(anIdToKill).diff(map, false);
                    if (diff.size() > 0) {
                        break;
                    }
                } catch (RemoteException | NotBoundException e) {
                    /* ignore may not be set up yet */
                }

                Thread.sleep(1000);
            }
            assertTrue(diff != null);
            assertEquals(1, diff.size());
        }

        /*
         * Now make an RN parameter change that requires restart.
         */
        HashMap<RepNodeId, ParameterMap>modParams =
            new HashMap<RepNodeId, ParameterMap>();
        for (RepNodeId id : ids) {
            RepNodeAdminAPI rnai = ru.getRepNodeAdmin(id);
            LoadParameters lp = rnai.getParams();
            modParams.put(id, lp.getMap(id.getFullName(),
                                        ParameterState.REPNODE_TYPE));
        }
        parms = cs.getParameters();
        for (RepNodeId id : ids) {
            RepNodeParams modified = parms.get(id);
            assert(!(modified.getJECacheMode() == CacheMode.EVICT_BIN));
        }

        for (int i = 0; i < idsToKill.length; i++) {
            ResourceId idToKill = idsToKill[i];
            if (idToKill instanceof StorageNodeId) {
                createStore.shutdownSNA(snIdToIdx.get(idToKill),
                                        true);
            } else if (idToKill instanceof RepNodeId) {
                if (disableNode) {
                    Set<RepNodeId> rnIds = new HashSet<RepNodeId>();
                    rnIds.add((RepNodeId)idToKill);
                    int pid =
                        cs.createStopServicesPlan("StopRepNode_"+idToKill,
                                                  rnIds);
                    cs.approvePlan(pid);
                    cs.executePlan(pid, false);
                    cs.awaitPlan(pid, 0, null);
                    cs.assertSuccess(pid);
                } else {
                    RepNodeAdminAPI rna =
                       createStore.getRepNodeAdmin((RepNodeId)idToKill);
                    rna.shutdown(true);
                }
            } else if (idToKill instanceof ArbNodeId) {
                if (disableNode) {
                    Set<ArbNodeId> stopAnIds = new HashSet<ArbNodeId>();
                    stopAnIds.add((ArbNodeId)idToKill);
                    int killPlanId =
                        cs.createStopServicesPlan("StopArbNode_"+idToKill,
                                                  stopAnIds);
                    cs.approvePlan(killPlanId);
                    cs.executePlan(killPlanId, false);
                    cs.awaitPlan(killPlanId, 0, null);
                    cs.assertSuccess(killPlanId);
                } else {
                    ArbNodeAdminAPI rna =
                        createStore.getArbNodeAdmin((ArbNodeId)idToKill);
                    rna.shutdown(true);
                }
            }
        }

        for (RepNodeId id : ids) {
            RepNodeParams rnp = new RepNodeParams(modParams.get(id).copy());
            rnp.setJECacheMode(CacheMode.EVICT_BIN);
            int planId = cs.createChangeParamsPlan
                ("ChangeParamsRebootRN_"+id, id, rnp.getMap());
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            cs.assertSuccess(planId);
        }

        parms = cs.getParameters();
        for (RepNodeId id : ids) {
            if (rnIdToKill.contains(id)) {
                continue;
            }
            RepNodeAdminAPI rnai = ru.getRepNodeAdmin(id);
            LoadParameters lp = rnai.getParams();
            ParameterMap map = lp.getMapByType(ParameterState.REPNODE_TYPE);
            ParameterMap diff = originalParams.get(id).diff(map, false);
            assertEquals(2, diff.size());
            RepNodeParams modified = new RepNodeParams(map);
            assertEquals(modified.getJECacheMode(), CacheMode.EVICT_BIN);
        }

        for (int i = 0; i < idsToKill.length; i++) {
            ResourceId idToKill = idsToKill[i];
            /* Wait for admin thread to fix parameters */
            if (idToKill instanceof StorageNodeId) {
                createStore.startSNA(snIdToIdx.get(idToKill));
            } else if (idToKill instanceof RepNodeId) {
                if (disableNode) {
                    boolean notRunning = false;
                    try {
                        ru.getRepNodeAdmin((RepNodeId)idToKill);
                    } catch (NotBoundException e) {
                        notRunning = true;
                    }

                    /*
                     * Check to insure node is not running since it is
                     * disabled.
                     */
                    assertTrue(notRunning);
                    Set<RepNodeId> rnIds = new HashSet<RepNodeId>();
                    rnIds.add((RepNodeId)idToKill);
                    int pid =
                        cs.createStartServicesPlan("StartRepNode_"+idToKill,
                                                   rnIds);
                    cs.approvePlan(pid);
                    cs.executePlan(pid, false);
                    cs.awaitPlan(pid, 0, null);
                    cs.assertSuccess(pid);
                } else {
                    StorageNodeAgent sna =
                        createStore.getStorageNodeAgent(snHostingKilledObj[i]);
                    sna.startRepNode((RepNodeId)idToKill);
                }
            } else if (idToKill instanceof ArbNodeId){
                if (disableNode) {
                    boolean notRunning = false;
                    try {
                        ru.getArbNodeAdmin((ArbNodeId)idToKill);
                    } catch (NotBoundException e) {
                        notRunning = true;
                    }

                    /*
                     * Check to insure node is not running.
                     */
                    assertTrue(notRunning);
                    Set<ArbNodeId> stopAnIds = new HashSet<ArbNodeId>();
                    stopAnIds.add((ArbNodeId)idToKill);
                    int killPlanId =
                        cs.createStartServicesPlan("StartArbNode_"+idToKill,
                                                   stopAnIds);
                    cs.approvePlan(killPlanId);
                    cs.executePlan(killPlanId, false);
                    cs.awaitPlan(killPlanId, 0, null);
                    cs.assertSuccess(killPlanId);
                } else {
                    StorageNodeAgent sna =
                        createStore.getStorageNodeAgent(snHostingKilledObj[i]);
                    sna.startArbNode(anIdToKill);
                }
            }
        }

        for (RepNodeId killedRN : rnIdToKill) {
            long startTime = System.currentTimeMillis();
            ParameterMap diff = null;
            RepNodeAdminAPI rnai = null;
            while ((System.currentTimeMillis() - startTime) < MAX_WAIT) {
                try {
                    rnai = ru.getRepNodeAdmin(killedRN);
                } catch (NotBoundException | RemoteException e) {
                    Thread.sleep(1000);
                    continue;
                }
                LoadParameters lp = rnai.getParams();
                ParameterMap map =
                    lp.getMapByType(ParameterState.REPNODE_TYPE);
                diff = originalParams.get(killedRN).diff(map, false);
                if (diff.size() == 2) {
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {}
            }
            assertTrue(diff != null);
            assertEquals(2, diff.size());
        }

        /*
         * Test change of SN parameters that changes RN config.
         */

        for (int i = 0; i < idsToKill.length; i++) {
            ResourceId idToKill = idsToKill[i];
            if (idToKill instanceof StorageNodeId) {
                createStore.shutdownSNA(snIdToIdx.get(idToKill),
                                        true);
            } else if (idToKill instanceof RepNodeId) {
                if (disableNode) {
                    Set<RepNodeId> rnIds = new HashSet<RepNodeId>();
                    rnIds.add((RepNodeId)idToKill);
                    int pid =
                        cs.createStopServicesPlan("StopRepNode_"+idToKill,
                                                  rnIds);
                    cs.approvePlan(pid);
                    cs.executePlan(pid, false);
                    cs.awaitPlan(pid, 0, null);
                    cs.assertSuccess(pid);
                } else {
                    RepNodeAdminAPI rna =
                        createStore.getRepNodeAdmin((RepNodeId)idToKill);
                    rna.shutdown(true);
                }
            } else if (idToKill instanceof ArbNodeId) {
                if (disableNode) {
                    Set<ArbNodeId> stopAnIds = new HashSet<ArbNodeId>();
                    stopAnIds.add((ArbNodeId)idToKill);
                    int killPlanId =
                        cs.createStopServicesPlan("StopArbNode_"+idToKill,
                                                  stopAnIds);
                    cs.approvePlan(killPlanId);
                    cs.executePlan(killPlanId, false);
                    cs.awaitPlan(killPlanId, 0, null);
                    cs.assertSuccess(killPlanId);
                    } else {
                        ArbNodeAdminAPI rna =
                            createStore.getArbNodeAdmin((ArbNodeId)idToKill);
                        rna.shutdown(true);
                    }
                }
        }

        /*
         * Change SN parameter that causes changes to the RN parameters.
         */
        for (StorageNodeId id : sns) {
            ParameterMap map = new ParameterMap();
            map.setType(ParameterState.SNA_TYPE);
            map.setName(ParameterState.SNA_TYPE);
            map.setParameter(ParameterState.SN_RN_HEAP_PERCENT, "90");
            map.setParameter(ParameterState.SN_LOG_FILE_COUNT, "21");
            int planId = cs.createChangeParamsPlan
                ("ChangeParamsSN_"+id, id, map);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            cs.assertSuccess(planId);
        }

        /*
         * Check parameters were changed for running SNs.
         */
        for (StorageNodeId id : sns) {
            if (killedSNs.contains(id)) {
                continue;
            }
            StorageNodeAgentAPI snai = ru.getStorageNodeAgent(id);
            LoadParameters lp = snai.getParams();
            ParameterMap map = lp.getMapByType(ParameterState.SNA_TYPE);
            ParameterMap diff = originalParams.get(id).diff(map, false);
            assertEquals(2, diff.size());
        }

        /*
         * Check that the parameters were changed for the running RNs.
         */
        for (RepNodeId id : ids) {
            if (rnIdToKill.contains(id)) {
                continue;
            }
            RepNodeAdminAPI rnai = ru.getRepNodeAdmin(id);
            LoadParameters lp = rnai.getParams();
            ParameterMap map = lp.getMapByType(ParameterState.REPNODE_TYPE);
            ParameterMap diff = originalParams.get(id).diff(map, false);
            assertEquals(3, diff.size());
        }

        /*
         * Restart the stopped resources.
         */
        for (int i = 0; i < idsToKill.length; i++) {
            ResourceId idToKill = idsToKill[i];
            /* Wait for admin thread to fix parameters */
            if (idToKill instanceof StorageNodeId) {
                createStore.startSNA(snIdToIdx.get(idToKill));
            } else if (idToKill instanceof RepNodeId) {
                if (disableNode) {
                    boolean notRunning = false;
                    try {
                        ru.getRepNodeAdmin((RepNodeId)idToKill);
                    } catch (NotBoundException e) {
                        notRunning = true;
                    }

                    /*
                     * Check to insure node is not running since it is
                     * disabled.
                     */
                    assertTrue(notRunning);
                    Set<RepNodeId> rnIds = new HashSet<RepNodeId>();
                    rnIds.add((RepNodeId)idToKill);
                    int pid =
                        cs.createStartServicesPlan("StartRepNode_"+idToKill,
                                                   rnIds);
                    cs.approvePlan(pid);
                    cs.executePlan(pid, false);
                    cs.awaitPlan(pid, 0, null);
                    cs.assertSuccess(pid);
                } else {
                    StorageNodeAgent sna =
                        createStore.getStorageNodeAgent(snHostingKilledObj[i]);
                    sna.startRepNode((RepNodeId)idToKill);
                }
            } else if (idToKill instanceof ArbNodeId){
                if (disableNode) {
                    boolean notRunning = false;
                    try {
                        ru.getArbNodeAdmin((ArbNodeId)idToKill);
                    } catch (NotBoundException e) {
                        notRunning = true;
                    }

                    /*
                     * Check to insure node is not running.
                     */
                    assertTrue(notRunning);
                    Set<ArbNodeId> stopAnIds = new HashSet<ArbNodeId>();
                    stopAnIds.add((ArbNodeId)idToKill);
                    int killPlanId =
                        cs.createStartServicesPlan("StartArbNode_"+idToKill,
                                                   stopAnIds);
                    cs.approvePlan(killPlanId);
                    cs.executePlan(killPlanId, false);
                    cs.awaitPlan(killPlanId, 0, null);
                    cs.assertSuccess(killPlanId);
                } else {
                    StorageNodeAgent sna =
                        createStore.getStorageNodeAgent(snHostingKilledObj[i]);
                    sna.startArbNode(anIdToKill);
                }
            }
        }

        /*
         * Check to insure the parameter consistency thread fixed
         * the inconsistency.
         */
        for (RepNodeId killedRN : rnIdToKill) {
            long startTime = System.currentTimeMillis();
            ParameterMap diff = null;
            RepNodeAdminAPI rnai = null;
            while ((System.currentTimeMillis() - startTime) < MAX_WAIT) {
                try {
                    rnai = ru.getRepNodeAdmin(killedRN);
                } catch (NotBoundException | RemoteException e) {
                    Thread.sleep(1000);
                    continue;
                }
                LoadParameters lp = rnai.getParams();
                ParameterMap map =
                    lp.getMapByType(ParameterState.REPNODE_TYPE);
                diff = originalParams.get(killedRN).diff(map, false);
                if (diff.size() == 3) {
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {}
            }
            assertTrue(diff != null);
            assertEquals(3, diff.size());
            RepNodeParams rp = new RepNodeParams(diff);
            RepNodeParams gold =
                new RepNodeParams(originalParams.get(killedRN));
            assertTrue(rp.getMaxHeapBytes() != gold.getMaxHeapBytes());
        }

        /*
         * Check parameters were changed for previously stopped SNs.
         */
        for (StorageNodeId id : killedSNs) {
            StorageNodeAgentAPI snai = ru.getStorageNodeAgent(id);
            LoadParameters lp = snai.getParams();
            ParameterMap map = lp.getMapByType(ParameterState.SNA_TYPE);
            ParameterMap diff = originalParams.get(id).diff(map, false);
            assertEquals(2, diff.size());
        }
    }
}
