/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.param;

import static oracle.kv.impl.param.ParameterState.AP_WAIT_TIMEOUT;
import static oracle.kv.util.CreateStore.mergeParameterMapDefaults;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import oracle.kv.TestBase;
import oracle.kv.TestClassTimeoutMillis;
import oracle.kv.impl.admin.AdminUtils;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.SNParameterConsistencyPlan;
import oracle.kv.impl.arb.admin.ArbNodeAdminAPI;
import oracle.kv.impl.async.FutureUtils.CheckedBiFunction;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.rep.ReplicationMutableConfig;
import com.sleepycat.je.util.DbFilterStats;

import org.junit.Test;

/**
 * Start, stop, reconfigure nodes.
 */
/* Increase test timeout to 60 minutes -- test can take 30 minutes */
@TestClassTimeoutMillis(60*60*1000)
public class ChangeAllParamsTest extends TestBase {

    private CreateStore createStore;
    private static final int startPort = 5000;
    private static final long MAX_WAIT = 2 * 60 * 1000;
    private static final long VERSION_WAIT_MS = 15 * 1000;
    private static final String AP_WAIT_TIMEOUT_VALUE = "30 SECONDS";

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        TestStatus.setManyRNs(true);
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
     *  Test changing all parameters for RNs and ANs.
     */
    @Test
    public void testChangeAllParams()
        throws Exception {
        createStore =
            new CreateStore(kvstoreName,
                            startPort,
                            9, /* Storage Nodes */
                            3, /* Replication Factor */
                            300, /* Partitions */
                            1 /* capacity */);
        createStore.start();

        changeAllParamsInternal(null, 9, 0);
    }

    /**
     *  Test changing all RepNode parameters when one SN is down.
     *  The SN hosts one of the RN's in the topology.
     *  Make sure Admin parameter consistency thread
     *  corrects inconsistency.
     */
    @Test
    public void testChangeAllParamsSNDown()
        throws Exception {
        int snToKill = 3;
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
        Thread.sleep(VERSION_WAIT_MS);
        StorageNodeId sns[] = createStore.getStorageNodeIds();
        changeAllParamsInternal(sns[snToKill], 9, 0);
    }

    /**
     *  Test changing all RepNode parameters when one RN is down.
     *  Make sure Admin parameter consistency thread
     *  corrects inconsistency.
     */
    @Test
    public void testChangeAllParamsRNDown()
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
        Thread.sleep(VERSION_WAIT_MS);
        changeAllParamsInternal(new RepNodeId(2, 2), 9, 0);
    }

    /*
     * Change all parameters for Arbiters when an SN is down.
     * Make sure Admin parameter consistency thread
     * corrects inconsistency. The SN that is down hosts an RN and AN.
     */
    @Test
    public void testChangeAllParamsARBSNDown()
        throws Exception {
        /* A value of 2 indicates sn3, which hosts an RN and AN */
        int snToKill = 2;
        createStore =
            new CreateStore(kvstoreName,
                            startPort,
                            6, /* Storage Nodes */
                            2, /* Replication Factor */
                            300, /* Partitions */
                            1 /* capacity */);
        createStore.setAllowArbiters(true);
        createStore.start();
        CommandServiceAPI cs = createStore.getAdmin();
        /* Change the interval for KVAdminMetadata thread. */
        ParameterMap map = new ParameterMap();
        map.setType(ParameterState.ADMIN_TYPE);
        map.setParameter(ParameterState.AP_PARAM_CHECK_INTERVAL, "30 SECONDS");
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
        cs.awaitPlan(p, 20, TimeUnit.SECONDS);
        cs.assertSuccess(p);

        /* sleep to let the version thread update SN version */
        Thread.sleep(VERSION_WAIT_MS);
        StorageNodeId sns[] = createStore.getStorageNodeIds();
        changeAllParamsInternal(sns[snToKill], 6, 3);
    }

    /*
     * Change all parameters for Arbiters when an AN is down.
     * Make sure Admin parameter consistency thread
     * corrects inconsistency.
     */
    @Test
    public void testChangeAllParamsANDown()
        throws Exception {
        createStore =
            new CreateStore(kvstoreName,
                            startPort,
                            6, /* Storage Nodes */
                            2, /* Replication Factor */
                            300, /* Partitions */
                            1 /* capacity */);
        createStore.setAllowArbiters(true);
        createStore.start();
        CommandServiceAPI cs = createStore.getAdmin();
        /* Change the interval for KVAdminMetadata thread. */
        ParameterMap map = new ParameterMap();
        map.setType(ParameterState.ADMIN_TYPE);
        map.setParameter(ParameterState.AP_PARAM_CHECK_INTERVAL, "30 SECONDS");
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
        cs.awaitPlan(p, 20, TimeUnit.SECONDS);
        cs.assertSuccess(p);

        /* sleep to let the version thread update SN version */
        Thread.sleep(VERSION_WAIT_MS);
        changeAllParamsInternal(new ArbNodeId(2,1), 6, 3);
    }

    /**
     *  Test changing an illegal JE RepNode parameter fails.
     */
    @Test
    public void testChangeAllBadJEParams()
        throws Exception {
        final String BAD_JE_MISC =
            "je.cleaner.threads 1; je.rep.insufficientReplicasTimeout 100ms";
        createStore =
            new CreateStore(kvstoreName,
                            startPort,
                            9, /* Storage Nodes */
                            3, /* Replication Factor */
                            300, /* Partitions */
                            1 /* capacity */);
        createStore.start();
        RepNodeParams rnp = new RepNodeParams(new ParameterMap());

        rnp.getMap().setParameter(ParameterState.JE_MISC, BAD_JE_MISC);
        CommandServiceAPI cs = createStore.getAdmin();
        boolean gotException = false;
        try {
            cs.createChangeAllParamsPlan(
                "ChangeAllParams", null,
                mergeParameterMapDefaults(rnp.getMap()));
        } catch (Exception e) {
            gotException = true;
        }
        assertTrue(gotException);
    }

    /**
     *  Test changing JE parameters with and without restarting service for
     *  RNs.
     */
    @Test
    public void testChangeAllRNJEParams() throws Exception {
        testChangeAllJEParams(
            3, /* rf */
            "Starting RepNode",
            (m, cs) -> cs.createChangeAllParamsPlan(
                "ChangeAllParamsJEMutable", null, m),
            new RepNodeId(1,1),
            1, /* storage node id */
            (m, cs) -> cs.createChangeAllParamsPlan(
                "ChangeAllParamsJEMutableForceReboot", null, m));
    }

    private void testChangeAllJEParams(
        int rf,
        String lookForIt,
        CheckedBiFunction<ParameterMap, CommandServiceAPI, Integer>
        createChangeParamPlan,
        ResourceId resId,
        int storageNodeId,
        CheckedBiFunction<ParameterMap, CommandServiceAPI, Integer>
        createChangeParamRebootPlan) throws Exception
    {
        final String JE_EVICTOR =
            EnvironmentConfig.EVICTOR_MAX_THREADS + "=11";
        final String JE_DB_HANDLE =
            ReplicationMutableConfig.REPLAY_DB_HANDLE_TIMEOUT + "=33 s";

        final String NEW_JE_MISC_MUTABLE =
            JE_EVICTOR + ";" + JE_DB_HANDLE;
        final String verifyValues[] =
            new String[] {"je.evictor.maxThreads to 11",
                          "je.rep.replayOpenHandleTimeout to 33 s"};
        /*
         * Specify columns and last values expected in the RN's je.config.csv
         * file
         */
        final String verifyConfigColumns =
            "envcfg:je.evictor.maxThreads," +
                "envcfg:je.env.runEraser," +
                "envcfg:je.erase.period," +
                "envcfg:je.rep.replayOpenHandleTimeout";
        String verifyConfigValues =
            "\"11\",\"true\",\"1 s\",\"33 s\"";

        /*
         * The changes to JE Erasure parameters only apply in the REPNODE case.
         * This is because Erasure is only used for processing user data.
         */
        if (resId.getType() == ResourceId.ResourceType.REP_NODE) {
            verifyConfigValues = "\"11\",\"false\",\"91 DAYS\",\"33 s\"";
        }

        createStore =
            new CreateStore(kvstoreName,
                            startPort,
                            3, /* Storage Nodes */
                            rf,
                            100, /* Partitions */
                            1 /* capacity */);
        createStore.setAllowArbiters(true);
        createStore.start();

        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.JE_MISC, NEW_JE_MISC_MUTABLE);
        map.setParameter(ParameterState.JE_ENABLE_ERASURE, "FALSE");
        map.setParameter(ParameterState.JE_ERASURE_PERIOD, "91 DAYS");

        CommandServiceAPI cs = createStore.getAdmin();
        int p = createChangeParamPlan.apply(
            mergeParameterMapDefaults(map), cs);
        cs.approvePlan(p);
        cs.executePlan(p, false);
        cs.awaitPlan(p, 20, TimeUnit.SECONDS);
        cs.assertSuccess(p);

        StorageNodeId snId = new StorageNodeId(storageNodeId);
        File snLog =
            new File(TestUtils.getTestDir().toString() + File.separator +
                     kvstoreName + File.separator +
                     "log" + File.separator + snId + "_0.log");
        int foundCount = countOccurrences(snLog, lookForIt);

        assertTrue("Expected 1 [" + lookForIt +"] but found " + foundCount,
                   foundCount == 1);

        final String logDirectory = TestUtils.getTestDir() + File.separator +
            kvstoreName + File.separator + "log";

        File serviceLog =
            new File(logDirectory + File.separator + resId + "_0.log");
        for (String lookFor : verifyValues) {
            checkLogFile(serviceLog, lookFor);
        }

        /*
         * TODO: ANs currently do not have a je.config.csv file. Remove this
         * after [KVSTORE-637] is done.
         */
        if (!(resId instanceof ArbNodeId)) {
            final String serviceJeConfigFile =
                logDirectory + File.separator + resId + ".je.config.csv";
            checkConfigFile(serviceJeConfigFile, verifyConfigColumns,
                            verifyConfigValues);
        }

        /* remove a mutable parameter to force a reboot */
        map = new ParameterMap();
        map.setParameter(ParameterState.JE_MISC, JE_DB_HANDLE);

        p = createChangeParamRebootPlan.apply(
            mergeParameterMapDefaults(map), cs);
        cs.approvePlan(p);
        cs.executePlan(p, false);
        final int pid = p;
        new PollCondition(1000, TimeUnit.SECONDS.toMillis(180)) {
            @Override
            protected boolean condition() {
                /*
                 * The master admin may be restarted and hence we obtain the
                 * admin master command service each time we check.
                 */
                try {
                    final CommandServiceAPI mcs = createStore.getAdminMaster();
                    return !mcs.getPlanById(pid).getState()
                        .equals(Plan.State.RUNNING);
                } catch (Throwable t) {
                    return false;
                }
            }
        }.await();
        cs = createStore.getAdminMaster();
        cs.assertSuccess(p);

        foundCount = countOccurrences(snLog, lookForIt);
        assertTrue("Expected 2 [" + lookForIt +"] but found " + foundCount,
                        foundCount == 2);
    }

    /**
     *  Test changing JE parameters with and without restarting service for
     *  Admins.
     */
    @Test
    public void testChangeAllAdminJEParams() throws Exception {
        testChangeAllJEParams(
            3, /* rf */
            "Starting AdminService",
            (m, cs) -> cs.createChangeAllAdminsPlan(
                "ChangeAllParamsJEMutable", null, m),
            new AdminId(2),
            2, /* storage node id */
            (m, cs) -> cs.createChangeAllAdminsPlan(
                "ChangeAllParamsJEMutableForceReboot", null, m));
    }

    /**
     *  Test changing JE parameters with and without restarting service for
     *  Arbiters.
     */
    @Test
    public void testChangeAllArbiterJEParams() throws Exception {
        testChangeAllJEParams(
            2, /* rf */
            "Starting ArbNode",
            (m, cs) -> cs.createChangeAllANParamsPlan(
                "ChangeAllParamsJEMutable", null, m),
            new ArbNodeId(1, 1),
            3, /* storage node id */
            (m, cs) -> cs.createChangeAllANParamsPlan(
                "ChangeAllParamsJEMutableForceReboot", null, m));
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
    private void changeAllParamsInternal(ResourceId idToKill,
                                         int numRNs,
                                         int numANs)
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
        int snToKill = -1;
        ArbNodeId anIdToKill = null;
        RepNodeId rnIdToKill = null;
        StorageNodeId snHostingKilledObj = null;

        /*
         * Setup some variables based on the object that
         * will be killed.
         */
        if (idToKill instanceof StorageNodeId) {

            for (int i=0; i< sns.length; i++) {
                if (sns[i].equals(idToKill)) {
                    snToKill = i;
                    break;
                }
            }

            Set<RepNodeId> rnsToKill =
                t.getHostedRepNodeIds((StorageNodeId)idToKill);
            if (idToKill != null) {
                for (RepNodeId rnId : rnsToKill) {
                    rnIdToKill = rnId;
                    snHostingKilledObj = (StorageNodeId)idToKill;
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
            rnIdToKill = (RepNodeId)idToKill;
            RepNode rn = t.get(rnIdToKill);
            snHostingKilledObj = rn.getStorageNodeId();
        } else if (idToKill instanceof ArbNodeId) {
            anIdToKill = (ArbNodeId)idToKill;
            ArbNode an = t.get(anIdToKill);
            snHostingKilledObj = an.getStorageNodeId();
        }

        /*
         * Get sample parameters form the first RN. This set is
         * used as a base to modify.
         */
        RepNodeId rnid = new RepNodeId(1, 1);
        ParameterMap newRNp = parms.get(rnid).getMap();

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
        RepNodeAdminAPI rnai = ru.getRepNodeAdmin(rnid);
        ParameterMap origGlobal = cs.getPolicyParameters();

        /* Get the thread dump interval so we can change it to a new value */
        DurationParameter dp =
            (DurationParameter)origGlobal.get(ParameterState.SP_DUMP_INTERVAL);

        HashMap<ResourceId,ParameterMap> originalParams =
            new HashMap<ResourceId,ParameterMap>(ids.size());

        for (RepNodeId id : ids) {
            rnai = ru.getRepNodeAdmin(id);
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

        /*
         * Change the thread dump interval.
         * Double the duration and assign to parameters.
         * Parameter will not cause the RN to be restarted after
         * it is changed.
         */
        dp.setMillis(dp.toMillis() * 2);
        newRNp.put(dp);

        /* Shutdown specified node */
        if (idToKill != null) {
            if (idToKill instanceof StorageNodeId) {
                createStore.shutdownSNA(snToKill, true);
            } else if (idToKill instanceof RepNodeId) {
                StorageNodeAgent sna =
                    createStore.getStorageNodeAgent(snHostingKilledObj);
                sna.stopRepNode((RepNodeId)idToKill, true);
            } else if (idToKill instanceof ArbNodeId) {
                StorageNodeAgent sna =
                                createStore.getStorageNodeAgent(snHostingKilledObj);
                sna.stopArbNode((ArbNodeId)idToKill, true);
            }
        }

        /* Change all RN parameters */
        int planId =
            cs.createChangeAllParamsPlan(
                "change-all-RN-params", null,
                mergeParameterMapDefaults(newRNp));
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        /* Check the new value. */
        for (RepNodeId id : ids) {
            if (rnIdToKill != null &&
                rnIdToKill.equals(id)) {
                continue;
            }
            rnai = ru.getRepNodeAdmin(id);
            LoadParameters lp = rnai.getParams();
            ParameterMap map = lp.getMap(id.getFullName(),
                                         ParameterState.REPNODE_TYPE);
            ParameterMap diff = originalParams.get(id).diff(map, false);
            assertEquals(1, diff.size());
            assertTrue(dp.equals(map.get(ParameterState.SP_DUMP_INTERVAL)));
        }

        /* Change all AN parameters */
        if (newANp != null) {
            planId =
                cs.createChangeAllANParamsPlan("change-all-AN-params",
                                               null, newANp);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            cs.assertSuccess(planId);

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
        }

        if (idToKill != null) {
            /*
             * If a node was killed, restart it and wait for the
             * parameter consistency thread to correct the inconsistency.
             */
            if (idToKill instanceof StorageNodeId) {
                createStore.startSNA(snToKill);
            } else if (idToKill instanceof RepNodeId) {
                StorageNodeAgent sna =
                   createStore.getStorageNodeAgent(snHostingKilledObj);
                sna.startRepNode(rnIdToKill);
            } else if (idToKill instanceof ArbNodeId){
                StorageNodeAgent sna =
                    createStore.getStorageNodeAgent(snHostingKilledObj);
                sna.startArbNode(anIdToKill);
            }

            if (rnIdToKill != null) {

                /* Wait for admin thread to fix parameters */
                long startTime = System.currentTimeMillis();
                ParameterMap diff = null;
                while ((System.currentTimeMillis() - startTime) < MAX_WAIT) {
                    try {
                        rnai = ru.getRepNodeAdmin(rnIdToKill);
                        LoadParameters lp = rnai.getParams();
                        ParameterMap map =
                            lp.getMapByType(ParameterState.REPNODE_TYPE);
                        diff = originalParams.get(rnIdToKill).diff(map, false);
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

            if (snToKill != -1) {

                /* Wait for plan to fix SN parameters */
                awaitParameterConsistencyPlan(cs, planId + 1);
            }
        }

        /*
         * Now make an RN parameter change that requires restart.
         */
        parms = cs.getParameters();
        for (RepNodeId id : ids) {
            RepNodeParams modified = parms.get(id);
            assert(!(modified.getJECacheMode() == CacheMode.EVICT_BIN));
        }

        if (idToKill != null) {
            if (idToKill instanceof StorageNodeId) {
                createStore.shutdownSNA(snToKill, true);
            } else if (idToKill instanceof RepNodeId) {
                StorageNodeAgent sna =
                    createStore.getStorageNodeAgent(snHostingKilledObj);
                sna.stopRepNode((RepNodeId)idToKill, true);
            } else if (idToKill instanceof ArbNodeId) {
                StorageNodeAgent sna =
                    createStore.getStorageNodeAgent(snHostingKilledObj);
                sna.stopArbNode((ArbNodeId)idToKill, true);
            }
        }

        RepNodeParams rnp = new RepNodeParams(newRNp);
        rnp.setJECacheMode(CacheMode.EVICT_BIN);
        planId = cs.createChangeAllParamsPlan
            ("change-all-RN-params-restart", null,
             mergeParameterMapDefaults(newRNp));
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        parms = cs.getParameters();
        for (RepNodeId id : ids) {
            if (rnIdToKill != null &&
                rnIdToKill.equals(id)) {
                continue;
            }
            rnai = ru.getRepNodeAdmin(id);
            LoadParameters lp = rnai.getParams();
            ParameterMap map = lp.getMapByType(ParameterState.REPNODE_TYPE);
            ParameterMap diff = originalParams.get(id).diff(map, false);
            assertEquals(2, diff.size());
            RepNodeParams modified = new RepNodeParams(map);
            assertEquals(modified.getJECacheMode(), CacheMode.EVICT_BIN);
        }

        if (idToKill != null) {
            /* Wait for admin thread to fix parameters */
            if (idToKill instanceof StorageNodeId) {
                createStore.startSNA(snToKill);
            } else if (idToKill instanceof RepNodeId) {
                StorageNodeAgent sna =
                   createStore.getStorageNodeAgent(snHostingKilledObj);
                sna.startRepNode(rnIdToKill);
            } else if (idToKill instanceof ArbNodeId){
                 StorageNodeAgent sna =
                     createStore.getStorageNodeAgent(snHostingKilledObj);
                 sna.startArbNode(anIdToKill);
            }

            if (rnIdToKill != null) {
                long startTime = System.currentTimeMillis();
                ParameterMap diff = null;
                while ((System.currentTimeMillis() - startTime) < MAX_WAIT) {
                    try {
                        rnai = ru.getRepNodeAdmin(rnIdToKill);
                    } catch (NotBoundException | NoSuchObjectException e) {
                        Thread.sleep(1000);
                        continue;
                    }
                    LoadParameters lp = rnai.getParams();
                    ParameterMap map =
                        lp.getMapByType(ParameterState.REPNODE_TYPE);
                    diff = originalParams.get(rnIdToKill).diff(map, false);
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
             * Make sure the RN is running before moving on to the next step,
             * which will stop the RN. If we stop the RN before it is fully
             * started, the start will fail, causing the test to fail, but for
             * a reason this test is not intending to test.
             */
            final RepNodeAdminAPI rnaiFinal = rnai;
            assertTrue(PollCondition.await(1000, MAX_WAIT, () -> {
                        try {
                            final RepNodeStatus rnStatus = rnaiFinal.ping();
                            return rnStatus.getServiceStatus() ==
                                ServiceStatus.RUNNING;
                        } catch (RemoteException e) {
                            return false;
                        }
                    }));

            if (snToKill != -1) {

                /* Wait for plan to fix SN parameters */
                awaitParameterConsistencyPlan(cs, planId + 1);
            }
        }

        /*
         * Make a change that requires a restart, but this time do it with one
         * RN shut down, to provide a regression test for a
         * NullPointerException that used to occur from creating a
         * ChangeAllParams plan when an RN was offline. [#22673]
         */
        final CacheMode defaultCacheMode =
            CacheMode.valueOf(ParameterState.KV_CACHE_MODE_DEFAULT);
        parms = cs.getParameters();
        for (RepNodeId id : ids) {
            assertNotEquals(defaultCacheMode,
                            parms.get(id).getJECacheMode());
        }
        rnp.setJECacheMode(defaultCacheMode);

        if (rnIdToKill != null) {
            /* Shutdown RN */
            Set<RepNodeId> stopRnIds = new HashSet<RepNodeId>();
            stopRnIds.add(rnIdToKill);
            planId = cs.createStopServicesPlan("stop-RN", stopRnIds);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            cs.assertSuccess(planId);
        }

        planId = cs.createChangeAllParamsPlan(
            "change-all-RN-params-restart2", null,
            mergeParameterMapDefaults(newRNp));
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        parms = cs.getParameters();
        for (RepNodeId id : ids) {
            try {
                rnai = ru.getRepNodeAdmin(id);
            } catch (NotBoundException e) {
                assertTrue(id.equals(rnIdToKill));
                continue;
            }

            LoadParameters lp = rnai.getParams();
            ParameterMap map = lp.getMapByType(ParameterState.REPNODE_TYPE);
            ParameterMap diff = originalParams.get(id).diff(map, false);
            assertEquals("Diff: " + diff, 1, diff.size());
            assertEquals(defaultCacheMode,
                         new RepNodeParams(map).getJECacheMode());
        }
    }

    /**
     * Tests that ChangeAllParams Plan still succeeds when some of the nodes
     * at time of plan creation no longer exist.
     */
    @Test
    public void testChangeParamsWithContraction() throws Exception {
        /* create store with 6 SNs */
        createStore =
                new CreateStore(kvstoreName,
                        startPort,
                        6, /* Storage Nodes */
                        3, /* Replication Factor */
                        300, /* Partitions */
                        1 /* capacity */);
        createStore.start();

        /* create change parameters plan */
        CommandServiceAPI cs = createStore.getAdmin();
        Parameters params = cs.getParameters();
        RepNodeId rnId = new RepNodeId(1, 1);
        ParameterMap newRNp = params.get(rnId).getMap();
        int paramsPlanId =
                cs.createChangeAllParamsPlan(
                        "change-all-RN-params", null,
                        mergeParameterMapDefaults(newRNp));
        cs.approvePlan(paramsPlanId);

        /* contract SNs 4, 5, 6 from store */
        final StorageNodeId[] snIds = createStore.getStorageNodeIds();
        final StorageNodeId sn1 = snIds[0];
        final StorageNodeId sn2 = snIds[1];
        final StorageNodeId sn3 = snIds[2];

        final String poolName = "snPool";
        AdminUtils.makeSNPool(cs, poolName, sn1, sn2, sn3);
        final String newTopo = "newTopo";
        cs.copyCurrentTopology(newTopo);
        cs.contractTopology(newTopo, poolName);

        /* create and deploy new topology with SNs 1, 2, 3 */
        int contractPlanId = cs.createDeployTopologyPlan("deploy-new-topo", newTopo, null);
        cs.approvePlan(contractPlanId);
        cs.executePlan(contractPlanId, false);
        cs.awaitPlan(contractPlanId, 0, null);

        /* run change parameters plan */
        cs.executePlan(paramsPlanId, false);
        cs.awaitPlan(paramsPlanId, 0, null);
        cs.assertSuccess(paramsPlanId);
    }

    private void checkLogFile(File logf, String lookFor) throws Exception  {
        BufferedReader in = new BufferedReader(new FileReader(logf));
        boolean foundNewValue = false;
        try {
            while (true) {
                String line = in.readLine();
                if (line == null) {
                       break;
                }
                if (line.contains(lookFor)) {
                    foundNewValue = true;
                    break;
                }
            }
        } finally {
            in.close();
        }
        assertTrue(logf.getAbsolutePath() +
                   " log does not contain [" + lookFor +"]",
                   foundNewValue);
    }

    private int countOccurrences(File logf, String lookFor) throws Exception  {
        int foundCount = 0;
        BufferedReader in = new BufferedReader(new FileReader(logf));
        try {
            while (true) {
                String line = in.readLine();
                if (line == null) {
                       break;
                }
                if (line.contains(lookFor)) {
                    foundCount++;
                }
            }
        } finally {
            in.close();
        }
        return foundCount;
    }

    /**
     * Check that the specified columns, using comma-separated column names
     * specified as the for value of the -p flag to DbFilterStats, of the
     * specified JE config.csv file have the specified values as the last line,
     * using DbFilterStats to do the column filtering.
     */
    private void checkConfigFile(String configFile,
                                 String configColumns,
                                 String configValues)
        throws IOException
    {
        /* Use DbFilterStats to filter columns */
        final PrintStream sysOut = System.out;
        final PrintStream sysErr = System.err;
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            final PrintStream out = new PrintStream(baos);
            System.setOut(out);
            System.setErr(out);
            if (!new DbFilterStats().execute(
                    new String[] { configFile, "-p", configColumns })) {
                fail("DbFilterStats.execute failed. Output: " + baos);
            }
        } finally {
            System.setOut(sysOut);
            System.setErr(sysErr);
        }

        /* Check column names and last line */
        try (final BufferedReader in =
             new BufferedReader(
                 new InputStreamReader(
                     new ByteArrayInputStream(baos.toByteArray())))) {
            final String firstLine = in.readLine();
            assertEquals("Column names line", configColumns, firstLine);
            String lastLine = null;
            while (true) {
                final String nextLine = in.readLine();
                if (nextLine == null) {
                    break;
                }
                lastLine = nextLine;
            }
            assertEquals("Last line", configValues, lastLine);
        }
    }

    /**
     * Wait for the next SNParameterConsistencyPlan to complete successfully.
     * The nextPlanId is the plan ID immediately after the plan that changed
     * the parameters, so look at plans starting with that ID.
     */
    void awaitParameterConsistencyPlan(CommandServiceAPI cs, int planId)
        throws RemoteException
    {
        final long stop = System.currentTimeMillis() + 30000;
        do {
            final Plan plan = cs.getPlanById(planId);
            if (plan instanceof SNParameterConsistencyPlan) {
                final Plan.State planState =
                    cs.awaitPlan(planId, 30, TimeUnit.SECONDS);
                if (planState == Plan.State.SUCCEEDED) {
                    break;
                } else if (planState == Plan.State.CANCELED) {
                    /* Plan will be retried -- find the next one */
                    planId++;
                } else {
                    fail("Unexpected final plan state: " + planState);
                }
            } else if (plan != null) {
                planId++;
            } else {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    break;
                }
            }
        } while (System.currentTimeMillis() < stop);

        cs.assertSuccess(planId);
    }
}
