/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static oracle.kv.impl.param.ParameterState.AP_LOG_FILE_COUNT;
import static oracle.kv.impl.param.ParameterState.AP_LOG_FILE_COUNT_DEFAULT;
import static oracle.kv.impl.param.ParameterState.AP_WAIT_ADMIN_FAILOVER;
import static oracle.kv.impl.param.ParameterState.AP_WAIT_ADMIN_FAILOVER_DEFAULT;
import static oracle.kv.impl.param.ParameterState.RN_GC_LOG_FILE_SIZE;
import static oracle.kv.impl.param.ParameterState.RN_GC_LOG_FILE_SIZE_DEFAULT;
import static oracle.kv.impl.param.ParameterState.RN_MAX_TOPO_CHANGES;
import static oracle.kv.impl.param.ParameterState.RN_MAX_TOPO_CHANGES_DEFAULT;
import static oracle.kv.impl.param.ParameterState.RN_SESS_LOOKUP_CONSISTENCY_LIMIT;
import static oracle.kv.impl.param.ParameterState.RN_SESS_LOOKUP_CONSISTENCY_LIMIT_DEFAULT;
import static oracle.kv.impl.param.ParameterState.SP_COLLECT_ENV_STATS;
import static oracle.kv.impl.param.ParameterState.SP_COLLECT_ENV_STATS_DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.VerifyConfiguration.ParamMismatch;
import oracle.kv.impl.admin.VerifyConfiguration.Problem;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.arb.admin.ArbNodeAdminAPI;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;

import com.sleepycat.je.rep.ReplicatedEnvironment.State;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test verifying and repairing problems with configuration and service
 * parameters for admins and RNs.
 */
public class RepairParamsTest extends TestBase {
    private static final int numSNs = 3;
    private static final int startPort = 5000;
    private static CreateStore createStore;

    private static final String ADMIN_RESTART_PARAM = AP_LOG_FILE_COUNT;
    private static final String ADMIN_RESTART_PARAM_VALUE = "1111";
    private static final String ADMIN_RESTART_PARAM_DEFAULT =
        AP_LOG_FILE_COUNT_DEFAULT;
    static {
        assertTrue(ParameterState.restartRequired(ADMIN_RESTART_PARAM));
    }

    private static final String ADMIN_NORESTART_PARAM =
        AP_WAIT_ADMIN_FAILOVER;
    private static final String ADMIN_NORESTART_PARAM_VALUE = "2222 s";
    private static final String ADMIN_NORESTART_PARAM_DEFAULT =
        AP_WAIT_ADMIN_FAILOVER_DEFAULT;
    static {
        assertFalse(ParameterState.restartRequired(ADMIN_NORESTART_PARAM));
    }

    private static final String RN_RESTART_PARAM =
        RN_SESS_LOOKUP_CONSISTENCY_LIMIT;
    private static final String RN_RESTART_PARAM_VALUE = "3333 s";
    private static final String RN_RESTART_PARAM_DEFAULT =
        RN_SESS_LOOKUP_CONSISTENCY_LIMIT_DEFAULT;
    static {
        assertTrue(ParameterState.restartRequired(RN_RESTART_PARAM));
    }

    private static final String RN_NORESTART_PARAM = RN_MAX_TOPO_CHANGES;
    private static final String RN_NORESTART_PARAM_VALUE = "4444";
    private static final String RN_NORESTART_PARAM_DEFAULT =
        RN_MAX_TOPO_CHANGES_DEFAULT;
    static {
        assertFalse("RN_NORESTART_PARAM no restart",
                    ParameterState.restartRequired(RN_NORESTART_PARAM));
    }

    private static final String AN_RESTART_PARAM =
        RN_GC_LOG_FILE_SIZE;
    private static final String AN_RESTART_PARAM_VALUE = "1048666";
    private static final String AN_RESTART_PARAM_DEFAULT =
        RN_GC_LOG_FILE_SIZE_DEFAULT;
    static {
        assertTrue(ParameterState.restartRequired(AN_RESTART_PARAM));
    }

    private static final String AN_NORESTART_PARAM = SP_COLLECT_ENV_STATS;
    private static final String AN_NORESTART_PARAM_VALUE = "false";
    private static final String AN_NORESTART_PARAM_DEFAULT =
        SP_COLLECT_ENV_STATS_DEFAULT;
    static {
        assertFalse("AN_NORESTART_PARAM no restart",
                    ParameterState.restartRequired(AN_NORESTART_PARAM));
    }

    private CommandServiceAPI csMaster;
    private Topology topo;
    private RegistryUtils regUtils;

    @BeforeClass
    public static void setUpClass()
        throws Exception {

        TestUtils.clearTestDirectory();
        TestStatus.setManyRNs(true);

        /* Create store */
        createStore = new CreateStore(RepairParamsTest.class.getName(),
                                      startPort,
                                      numSNs, /* Storage Nodes */
                                      2, /* Replication Factor */
                                      90, /* Partitions */
                                      1 /* capacity */);
        createStore.setAllowArbiters(true);
        createStore.start();
    }

    @AfterClass
    public static void tearDownClass()
        throws Exception {

        if (createStore != null) {
            createStore.shutdown();
        }
        LoggerUtils.closeAllHandlers();
    }

    @Override
    public void setUp()
        throws Exception {

        /* Don't call super method -- doing setUp at class level */
        csMaster = createStore.getAdminMaster();
        topo = csMaster.getTopology();
        regUtils = new RegistryUtils(topo, createStore.getSNALoginManager(0),
                                     logger);
    }

    @Override
    public void tearDown() {
        /* Don't call super method -- doing tearDown at class level */
    }

    /* Admin parameter tests */

    @Test
    public void testAdminReplicaRestartConfigDiffs() throws Exception {
        adminTest(NodeType.REPLICA, DiffsLocation.CONFIG, DiffsLocation.NONE);
    }
    @Test
    public void testAdminReplicaRestartServiceDiffs() throws Exception {
        adminTest(NodeType.REPLICA, DiffsLocation.SERVICE, DiffsLocation.NONE);
    }
    @Test
    public void testAdminReplicaRestartConfigServiceDiffs() throws Exception {
        adminTest(NodeType.REPLICA, DiffsLocation.BOTH, DiffsLocation.NONE);
    }
    @Test
    public void testAdminReplicaNoRestartConfigDiffs() throws Exception {
        adminTest(NodeType.REPLICA, DiffsLocation.NONE, DiffsLocation.CONFIG);
    }
    @Test
    public void testAdminReplicaNoRestartServiceDiffs() throws Exception {
        adminTest(NodeType.REPLICA, DiffsLocation.NONE, DiffsLocation.SERVICE);
    }
    @Test
    public void testAdminReplicaNoRestartConfigServiceDiffs() throws Exception {
        adminTest(NodeType.REPLICA, DiffsLocation.NONE, DiffsLocation.BOTH);
    }
    @Test
    public void testAdminMasterRestartConfigDiffs() throws Exception {
        adminTest(NodeType.MASTER, DiffsLocation.CONFIG, DiffsLocation.NONE);
    }
    @Test
    public void testAdminMasterRestartServiceDiffs() throws Exception {
        adminTest(NodeType.MASTER, DiffsLocation.SERVICE, DiffsLocation.NONE);
    }
    @Test
    public void testAdminMasterRestartConfigServiceDiffs() throws Exception {
        adminTest(NodeType.MASTER, DiffsLocation.BOTH, DiffsLocation.NONE);
    }
    @Test
    public void testAdminMasterNoRestartConfigDiffs() throws Exception {
        adminTest(NodeType.MASTER, DiffsLocation.NONE, DiffsLocation.CONFIG);
    }
    @Test
    public void testAdminMasterNoRestartServiceDiffs() throws Exception {
        adminTest(NodeType.MASTER, DiffsLocation.NONE, DiffsLocation.SERVICE);
    }
    @Test
    public void testAdminMasterNoRestartConfigServiceDiffs() throws Exception {
        adminTest(NodeType.MASTER, DiffsLocation.NONE, DiffsLocation.BOTH);
    }

    /* RN parameter tests */

    @Test
    public void testRNReplicaRestartConfigDiffs() throws Exception {
        rnTest(NodeType.REPLICA, DiffsLocation.CONFIG, DiffsLocation.NONE);
    }
    @Test
    public void testRNReplicaRestartServiceDiffs() throws Exception {
        rnTest(NodeType.REPLICA, DiffsLocation.SERVICE, DiffsLocation.NONE);
    }
    @Test
    public void testRNReplicaRestartConfigServiceDiffs() throws Exception {
        rnTest(NodeType.REPLICA, DiffsLocation.BOTH, DiffsLocation.NONE);
    }
    @Test
    public void testRNReplicaNoRestartConfigDiffs() throws Exception {
        rnTest(NodeType.REPLICA, DiffsLocation.NONE, DiffsLocation.CONFIG);
    }
    @Test
    public void testRNReplicaNoRestartServiceDiffs() throws Exception {
        rnTest(NodeType.REPLICA, DiffsLocation.NONE, DiffsLocation.SERVICE);
    }
    @Test
    public void testRNReplicaNoRestartConfigServiceDiffs() throws Exception {
        rnTest(NodeType.REPLICA, DiffsLocation.NONE, DiffsLocation.BOTH);
    }
    @Test
    public void testRNMasterRestartConfigDiffs() throws Exception {
        rnTest(NodeType.MASTER, DiffsLocation.CONFIG, DiffsLocation.NONE);
    }
    @Test
    public void testRNMasterRestartServiceDiffs() throws Exception {
        rnTest(NodeType.MASTER, DiffsLocation.SERVICE, DiffsLocation.NONE);
    }
    @Test
    public void testRNMasterRestartConfigServiceDiffs() throws Exception {
        rnTest(NodeType.MASTER, DiffsLocation.BOTH, DiffsLocation.NONE);
    }
    @Test
    public void testRNMasterNoRestartConfigDiffs() throws Exception {
        rnTest(NodeType.MASTER, DiffsLocation.NONE, DiffsLocation.CONFIG);
    }
    @Test
    public void testRNMasterNoRestartServiceDiffs() throws Exception {
        rnTest(NodeType.MASTER, DiffsLocation.NONE, DiffsLocation.SERVICE);
    }
    @Test
    public void testRNMasterNoRestartConfigServiceDiffs() throws Exception {
        rnTest(NodeType.MASTER, DiffsLocation.NONE, DiffsLocation.BOTH);
    }

    /* AN parameter tests */

    @Test
    public void testANRestartConfigDiffs() throws Exception {
        anTest(DiffsLocation.CONFIG, DiffsLocation.NONE);
    }
    @Test
    public void testANRestartServiceDiffs() throws Exception {
        anTest(DiffsLocation.SERVICE, DiffsLocation.NONE);
    }
    @Test
    public void testANRestartConfigServiceDiffs() throws Exception {
        anTest(DiffsLocation.BOTH, DiffsLocation.NONE);
    }
    @Test
    public void testANNoRestartConfigDiffs() throws Exception {
        anTest(DiffsLocation.NONE, DiffsLocation.CONFIG);
    }
    @Test
    public void testANNoRestartServiceDiffs() throws Exception {
        anTest(DiffsLocation.NONE, DiffsLocation.SERVICE);
    }
    @Test
    public void testANNoRestartConfigServiceDiffs() throws Exception {
        anTest(DiffsLocation.NONE, DiffsLocation.BOTH);
    }

    /* Utilities */

    enum NodeType {
        MASTER, REPLICA, SECONDARY;
        boolean isMaster() { return this == MASTER; }
        boolean isReplica() { return this == REPLICA; }
        boolean isSecondary() { return this == SECONDARY; }
    }

    enum DiffsLocation {
        CONFIG(true, false), SERVICE(false, true), BOTH(true, true),
        NONE(false, false);
        final boolean config;
        final boolean service;
        DiffsLocation(boolean config, boolean service) {
            this.config = config;
            this.service = service;
        }
        int getDiffsCount() {
            return (config ? 1 : 0) + (service ? 1 : 0);
        }
    }

    /* TODO: Test secondaries */
    private void adminTest(NodeType nodeType,
                           DiffsLocation restartDiffs,
                           DiffsLocation nonRestartDiffs)
        throws Exception {

        logger.info(testName.getMethodName());

        /* Get the SNA for the admin */
        final int adminIndex = nodeType.isMaster() ?
            createStore.getAdminIndex() :
            (createStore.getAdminIndex() + 1) % numSNs;
        final StorageNodeAgent sna =
            createStore.getStorageNodeAgent(adminIndex);
        final StorageNodeId snId = sna.getStorageNodeId();

        /* Verify no problems */
        verifyViolations(testName.getMethodName() + " start", csMaster);

        /* Modify admin configuration */
        final File snaConfigFile = sna.getKvConfigFile();
        final LoadParameters snParams =
            LoadParameters.getParameters(snaConfigFile, logger);
        final ParameterMap adminParamMap =
            snParams.getMapByType(ParameterState.ADMIN_TYPE);

        /* Update the config parameters */
        int service = 0;
        if (restartDiffs.service) {
            adminParamMap.setParameter(ADMIN_RESTART_PARAM,
                                       ADMIN_RESTART_PARAM_VALUE);
            service = 1;
        }
        if (nonRestartDiffs.service) {
            adminParamMap.setParameter(ADMIN_NORESTART_PARAM,
                                       ADMIN_NORESTART_PARAM_VALUE);
            service = 1;
        }
        if (service > 0) {
            snParams.saveParameters(snaConfigFile);
            regUtils.getAdmin(snId).newParameters();
            adminParamMap.setParameter(ADMIN_RESTART_PARAM,
                                       ADMIN_RESTART_PARAM_DEFAULT);
            adminParamMap.setParameter(ADMIN_NORESTART_PARAM,
                                       ADMIN_NORESTART_PARAM_DEFAULT);
            snParams.saveParameters(snaConfigFile);
        }
        int config = 0;
        if (restartDiffs.config) {
            adminParamMap.setParameter(ADMIN_RESTART_PARAM,
                                       ADMIN_RESTART_PARAM_VALUE);
            config = 1;
        }
        if (nonRestartDiffs.config) {
            adminParamMap.setParameter(ADMIN_NORESTART_PARAM,
                                       ADMIN_NORESTART_PARAM_VALUE);
            config = 1;
        }
        if (config > 0) {
            snParams.saveParameters(snaConfigFile);
            adminParamMap.setParameter(ADMIN_RESTART_PARAM,
                                       ADMIN_RESTART_PARAM_DEFAULT);
            adminParamMap.setParameter(ADMIN_NORESTART_PARAM,
                                       ADMIN_NORESTART_PARAM_DEFAULT);
        }

        verifyViolations(testName.getMethodName() + " before repair", csMaster,
                         config + service, ParamMismatch.class);

        logger.info("Run repair for: " + testName.getMethodName());
        final int planId = csMaster.createRepairPlan("repair");
        csMaster.approvePlan(planId);
        csMaster.executePlan(planId, false);
        Plan.State planState;
        try {
            planState = csMaster.awaitPlan(planId, 0, null);
        } catch (Exception e) {
            if (e.getCause() instanceof IllegalCommandException) {
                /*
                 * Happens if the admin was shutdown -- treat as interrupt
                 */
                planState = Plan.State.INTERRUPTED;
            } else {
                throw e;
            }
        }
        if ((restartDiffs.getDiffsCount() > 0) &&
            nodeType.isMaster() &&
            (planState == Plan.State.INTERRUPTED)) {

            /*
             * The plan was interrupted after restarting the admin master --
             * wait for the node to restart as a replica
             */
            final boolean ok = new PollCondition(1000, 60000) {
                @Override
                protected boolean condition() {
                    try {
                        if (!createStore.getAdmin(adminIndex)
                            .getAdminStatus()
                            .getReplicationState()
                            .isMaster()) {
                            return true;
                        }
                    } catch (Exception e) {
                    }
                    return false;
                }
            }.await();
            assertTrue("Waiting for admin" + (adminIndex+1) +
                       " to restart as replica",
                       ok);

            csMaster = createStore.getAdminMaster();
            runPlanSuccess(csMaster, csMaster.createRepairPlan("repair"));
        } else {
            /*
             * There is a race condition with the admin master being shutdown
             * as part of a admin parameter change.
             * The shutdown is executed as a separate thread (and separate plan)
             * that waits for the parameter change plan to complete.
             * This creates a race condition here so we wait a bit before
             * getting the master admin.
	     * Wait a little so we don't get the original admin handle before
	     * it is shutdown.
             */
            Thread.sleep(3 *1000);
            assertTrue("Waiting for admin master",
                       PollCondition.await(
                           500, 10000,
                           () -> {
                               csMaster = null;
                               try {
                                   csMaster = createStore.getAdminMaster();
                               } catch (Exception e) {
                               }
                               return csMaster != null;
                           }));
            csMaster.assertSuccess(planId);
        }

        /* Wait for all admins to be up. */
        assertTrue("Waiting for all admins",
                   PollCondition.await(
                       500, 10000,
                       () -> {
                           for (int admId : createStore.getAdminLocations()) {
                               try {
                                   /*
                                    * Admin IDs are 0-based, SN IDs returned
                                    * for admin locations are one-based
                                    */
                                   createStore.getAdmin(admId - 1);
                               } catch (Exception e) {
                                   return false;
                               }
                           }
                           return true;
                       }));

        /* Verify repair worked */
        csMaster = createStore.getAdminMaster();
        verifyViolations(testName.getMethodName() + " after repair", csMaster);
    }

    /* TODO: Test secondaries */
    private void rnTest(NodeType nodeType,
                        DiffsLocation restartDiffs,
                        DiffsLocation nonRestartDiffs)
        throws Exception {

        logger.info(testName.getMethodName());

        /* Get the RNA for the RN */
        RepNodeAdminAPI rna = null;
        RepNodeId rnId = null;
        StorageNodeId snId = null;
        final RepGroup rg = topo.getRepGroupMap().getAll().iterator().next();
        final ServiceStatus[] target = { ServiceStatus.RUNNING };
        for (final RepNode rn : rg.getRepNodes()) {
            final RepNodeAdminAPI newRNA = ServiceUtils.waitForRepNodeAdmin(
                topo, rn.getResourceId(),
                createStore.getStorageNodeAgent(0).getLoginManager(),
                60, target, logger);
            final boolean isMaster =
                (newRNA.ping().getReplicationState() == State.MASTER);
            if (isMaster == nodeType.isMaster()) {
                rna = newRNA;
                rnId = rn.getResourceId();
                snId = rn.getStorageNodeId();
                break;
            }
        }
        assertNotNull("Didn't find RNA", rna);

        /* Get SNA */
        final StorageNodeAgent sna = createStore.getStorageNodeAgent(snId);

        /* Verify no problems */
        verifyViolations(testName.getMethodName() + " start", csMaster);

        /* Modify admin configuration */
        final File snaConfigFile = sna.getKvConfigFile();
        final LoadParameters snParams =
            LoadParameters.getParameters(snaConfigFile, logger);

        /* Ignore null warning about rnId */
        @SuppressWarnings("null")
        final ParameterMap rnParamMap =
            snParams.getMap(rnId.getFullName(), ParameterState.REPNODE_TYPE);

        /* Update the config parameters */
        int service = 0;
        if (restartDiffs.service) {
            rnParamMap.setParameter(RN_RESTART_PARAM, RN_RESTART_PARAM_VALUE);
            service = 1;
        }
        if (nonRestartDiffs.service) {
            rnParamMap.setParameter(RN_NORESTART_PARAM,
                                    RN_NORESTART_PARAM_VALUE);
            service = 1;
        }
        if (service > 0) {
            snParams.saveParameters(snaConfigFile);
            rna.newParameters();
            rnParamMap.setParameter(RN_RESTART_PARAM,
                                    RN_RESTART_PARAM_DEFAULT);
            rnParamMap.setParameter(RN_NORESTART_PARAM,
                                    RN_NORESTART_PARAM_DEFAULT);
            snParams.saveParameters(snaConfigFile);
        }
        int config = 0;
        if (restartDiffs.config) {
            rnParamMap.setParameter(RN_RESTART_PARAM, RN_RESTART_PARAM_VALUE);
            config = 1;
        }
        if (nonRestartDiffs.config) {
            rnParamMap.setParameter(RN_NORESTART_PARAM,
                                    RN_NORESTART_PARAM_VALUE);
            config = 1;
        }
        if (config > 0) {
            snParams.saveParameters(snaConfigFile);
            rnParamMap.setParameter(RN_RESTART_PARAM,
                                    RN_RESTART_PARAM_DEFAULT);
            rnParamMap.setParameter(RN_NORESTART_PARAM,
                                    RN_NORESTART_PARAM_DEFAULT);
        }

        verifyViolations(testName.getMethodName() + " before repair", csMaster,
                         config + service, ParamMismatch.class);

        logger.info("Run repair for: " + testName.getMethodName());
        runPlanSuccess(csMaster, csMaster.createRepairPlan("repair"));

        /* Handle restart cases */
        if (restartDiffs.getDiffsCount() > 0) {

            /* Wait for the RN to restart */
            ServiceUtils.waitForRepNodeAdmin(
                topo, rnId, null, 60,
                new ServiceStatus[] { ServiceStatus.RUNNING }, logger);
        }

        /* Verify repair worked */
        verifyViolations(testName.getMethodName() + " after repair", csMaster);
    }

    @SuppressWarnings("null")
    private void anTest(DiffsLocation restartDiffs,
                        DiffsLocation nonRestartDiffs)
        throws Exception {

        logger.info(testName.getMethodName());

        /* Get the ANA for the AN */
        ArbNodeAdminAPI rna = null;
        ArbNodeId anId = null;
        StorageNodeId snId = null;
        StorageNode sn = null;
        final RepGroup rg = topo.getRepGroupMap().getAll().iterator().next();
        final ServiceStatus[] target = { ServiceStatus.RUNNING };
        for (final ArbNode an : rg.getArbNodes()) {
            anId = an.getResourceId();
            snId = an.getStorageNodeId();
            sn = topo.get(snId);
            final ArbNodeAdminAPI newANA = ServiceUtils.waitForArbNodeAdmin(
                topo.getKVStoreName(),
                sn.getHostname(),
                sn.getRegistryPort(),
                anId,
                snId,
                createStore.getStorageNodeAgent(0).getLoginManager(),
                60, target, logger);
                rna = newANA;
                break;
        }
        assertNotNull("Didn't find ANA", rna);

        /* Get SNA */
        final StorageNodeAgent sna = createStore.getStorageNodeAgent(snId);

        /* Verify no problems */
        verifyViolations(testName.getMethodName() + " start", csMaster);

        /* Modify admin configuration */
        final File snaConfigFile = sna.getKvConfigFile();
        final LoadParameters snParams =
            LoadParameters.getParameters(snaConfigFile, logger);

        /* Ignore null warning about anId */
        final ParameterMap anParamMap =
            snParams.getMap(anId.getFullName(), ParameterState.ARBNODE_TYPE);

        /* Update the config parameters */
        int service = 0;
        if (restartDiffs.service) {
            anParamMap.setParameter(AN_RESTART_PARAM, AN_RESTART_PARAM_VALUE);
            service = 1;
        }
        if (nonRestartDiffs.service) {
            anParamMap.setParameter(AN_NORESTART_PARAM,
                                    AN_NORESTART_PARAM_VALUE);
            service = 1;
        }
        if (service > 0) {
            snParams.saveParameters(snaConfigFile);
            rna.newParameters();
            anParamMap.setParameter(AN_RESTART_PARAM,
                                    AN_RESTART_PARAM_DEFAULT);
            anParamMap.setParameter(AN_NORESTART_PARAM,
                                    AN_NORESTART_PARAM_DEFAULT);
            snParams.saveParameters(snaConfigFile);
        }
        int config = 0;
        if (restartDiffs.config) {
            anParamMap.setParameter(AN_RESTART_PARAM, AN_RESTART_PARAM_VALUE);
            config = 1;
        }
        if (nonRestartDiffs.config) {
            anParamMap.setParameter(AN_NORESTART_PARAM,
                                    AN_NORESTART_PARAM_VALUE);
            config = 1;
        }
        if (config > 0) {
            snParams.saveParameters(snaConfigFile);
            anParamMap.setParameter(AN_RESTART_PARAM,
                                    AN_RESTART_PARAM_DEFAULT);
            anParamMap.setParameter(AN_NORESTART_PARAM,
                                    AN_NORESTART_PARAM_DEFAULT);
        }

        verifyViolations(testName.getMethodName() + " before repair", csMaster,
                         config + service, ParamMismatch.class);

        logger.info("Run repair for: " + testName.getMethodName());
        runPlanSuccess(csMaster, csMaster.createRepairPlan("repair"));

        /* Handle restart cases */
        if (restartDiffs.getDiffsCount() > 0) {

            /* Wait for the RN to restart */
            ServiceUtils.waitForArbNodeAdmin(
                topo.getKVStoreName(),
                sn.getHostname(),
                sn.getRegistryPort(),
                anId,
                snId,
                createStore.getStorageNodeAgent(0).getLoginManager(),
                60,
                new ServiceStatus[] { ServiceStatus.RUNNING },
                logger);
        }

        /* Verify repair worked */
        verifyViolations(testName.getMethodName() + " after repair", csMaster);
    }

    private static void runPlanSuccess(CommandServiceAPI cs, int planId)
        throws RemoteException {

        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
    }

    private static void verifyViolations(String test,
                                         CommandServiceAPI cs,
                                         Object... countsAndProblemClasses)
        throws RemoteException {

        if (countsAndProblemClasses.length % 2 != 0) {
            throw new IllegalArgumentException(
                "The countsAndProblemClasses argument must have an even" +
                " number of elements");
        }
        final Map<Class<?>, Integer> expectedProblems =
            new HashMap<Class<?>, Integer>();
        for (int i = 0; i < countsAndProblemClasses.length; i += 2) {
            expectedProblems.put((Class<?>) countsAndProblemClasses[i+1],
                                 (Integer) countsAndProblemClasses[i]);
        }
        final VerifyResults results =
            cs.verifyConfiguration(false, false, false);
        final Map<Class<? extends Problem>, Integer> foundProblems =
            new HashMap<Class<? extends Problem>, Integer>();
        for (final Problem problem : results.getViolations()) {
            final Integer count = foundProblems.get(problem.getClass());
            foundProblems.put(problem.getClass(),
                              (count == null ? 1 : count + 1));
        }
        assertEquals(test + ": " + results.getViolations(),
                     expectedProblems, foundProblems);
    }
}
