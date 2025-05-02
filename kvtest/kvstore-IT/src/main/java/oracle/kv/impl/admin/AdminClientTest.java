/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static java.util.concurrent.TimeUnit.MINUTES;
import static oracle.kv.util.CreateStore.MB_PER_SN;
import static oracle.nosql.common.json.JsonUtils.getArray;
import static oracle.nosql.common.json.JsonUtils.getAsText;
import static oracle.nosql.common.json.JsonUtils.getObject;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.TestClassTimeoutMillis;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.DatacenterParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.PlanExecutor;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeTestBase;
import oracle.kv.impl.systables.TableStatsPartitionDesc;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.Verifier;

import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

import org.junit.Test;

/**
 * Test the CommandShell command-line client by running a few deployment plans
 * through it.
 */
@TestClassTimeoutMillis(40*60*1000)
public class AdminClientTest extends AdminClientTestBase {

    private static final long randomSeed =
        Long.getLong("test.randomSeed", System.currentTimeMillis());
    private static final Random random = new Random(randomSeed);
    private static boolean printedSeed;

    private final static String MYPOOLNAME = "mypool";
    private static final int startPort4 = 13290;
    private int nStorageNodes = 0;

    private AdminService service = null;

    /*
     * For saving the additional SNAs that are started by some of the tests;
     * so they can be shut down at the end of the test. Note that the 3
     * initial SNAs that are automatically started by the parent class will
     * be shut down by the parent's tearDown method; whereas any additional
     * SNAs will be shutdown in this test's tearDown method.
     */
    private List<StorageNodeAgent> snaList = null;

    public AdminClientTest() {
        super(false);
    }

    @Override
    public void setUp()
        throws Exception {

        super.setUp();

        /*
         * Try to be sure that the bootstrap admin on the default admin port is
         * started before continuing. Otherwise, the test method might deploy
         * an admin before the bootstrap one was ready, and the bootstrap one
         * could win out, causing tests to fail. Something about the
         * combination of async and Java 17 changed the timing in just such a
         * way to cause this to happen.
         *
         * Note that waitForAdmin calls CommandService.ping, which always
         * returns RUNNING, so this just checks to see that the admin server
         * has been started and bound in the registry. But that should be
         * enough to make sure that the bootstrap admin gets set up first.
         * [KVSTORE-1415]
         */
        ServiceUtils.waitForAdmin("localhost",
                                   Integer.valueOf(adminPort),
                                  loginMgr, 20 /* timeoutSec */,
                                  ServiceStatus.RUNNING, logger);

        if (!printedSeed) {
            System.err.println("test.randomSeed=" + randomSeed);
            printedSeed = true;
        }
    }

    @Override
    public void tearDown()
        throws Exception {

        if (service != null) {
            service.stop(true, "test teardown");
            service.waitForActive(false);
            service = null;
        }

        /*
         * The super.tearDown method will shutdown sna1, sna2, and sna3.
         * Shutdown any additional SNAs created as part of the test.
         */
        if (snaList != null) {
            for (StorageNodeAgent sna : snaList) {
                sna.shutdown(true, true);
            }
            snaList = null;
        }

        PlanExecutor.FAULT_HOOK = null;

        super.tearDown();
    }

    /**
     * Run a plan command, which should include the -wait keyword, and check
     * that it completes successfully.  The command is divided into
     * space-separated tokens, so don't use this method if command arguments
     * contain spaces.
     */
    private void assertPlanSuccess(final String command)
        throws Exception {

        if (!command.startsWith("plan") || !command.contains("-wait")) {
            fail("Should be plan command with -wait: " + command);
        }
        assertCLICommandResult("AdminClientTest", command,
                               "successfully");
    }

    /**
     * Starts the Admin service and waits for it to become active before
     * returning.
     */
    private void startAdminService() {
        AdminServiceParams asp = atc.getParams();
        SecurityParams sp = asp.getSecurityParams();
        StorageNodeParams snp = asp.getStorageNodeParams();
        GlobalParams gp = asp.getGlobalParams();
        AdminParams ap = asp.getAdminParams();
        ap.setMetadataAdminThreadEnabled(false);
        ap.setVersionThreadEnabled(false);

        LoadParameters lp = new LoadParameters();
        lp.addMap(snp.getMap());
        lp.addMap(gp.getMap());
        lp.addMap(ap.getMap());

        service = new AdminService(true);
        service.initialize(sp, ap, lp);

        service.start();

        /* Wait until the service is ready to process requests. */
        try {
            service.waitForActive(true);
        } catch (InterruptedException e) {
            fail("unexpected interrupt");
        }

        /*
         * Clear the registry service cache so that callers don't find the old
         * bootstrap admin in there. Since the bootstrap admin is not being
         * shutdown, it will not be removed from the cache automatically. But
         * that is a situation that only occurs during testing.
         */
        RegistryUtils.clearServiceCache();
    }

    private KVStore getStore() throws Exception {
        KVStoreConfig kvstoreConfig = new KVStoreConfig(
                kvstoreName, "localhost" + ":" +
                Integer.parseInt(adminPort));
        return KVStoreFactory.getStore(kvstoreConfig);
    }

    /* Test deploy-sn command with invalid zone id */
    @Test
    public void testDeploySNInvalidZoneId()
        throws Exception {

        final String zoneName = "zn_1";
        final String expectedSubstring =
            zoneName + " is not a valid id. It must have the form " +
            "<prefix>X where <prefix> is one of: [zn, dc] " +
            "(java.lang.IllegalArgumentException)";

        /* start admin service */
        startAdminService();

        /* deploy zone */
        deployZone(zoneName, "1");

        /* deploy sn with invalid zone id */
        assertCLICommandResult("AdminClientTest",
                               "plan deploy-sn " + getZoneIdFlag() +
                                       " " +
                               zoneName + " -host localhost -port " +
                               portFinder1.getRegistryPort() + " -wait",
                               expectedSubstring);
    }

    /* Test the pool command */
    @Test
    public void testPoolCommand()
        throws Exception {

        startAdminService();
        String dcname = deployDatacenter();

        /* Deploy a StorageNode */
        int sn1id = deployStorageNode(dcname, portFinder1.getRegistryPort());

        /* Deploy an Admin */
        deployAdmin(sn1id);

        String result;

        /* Added Pool Message Displayed */
        result = runCLICommand("pool", "create", "-name", "new-pool-1");
        assertEquals("Added pool new-pool-1\n", result);

        /* Removed Pool Message Displayed */
        result = runCLICommand("pool", "remove", "-name", "new-pool-1");
        assertEquals("Removed pool new-pool-1\n", result);

        /* Added Pool Message Displayed with verbose option*/
        result = runCLICommand("pool", "create", "-name",
                               "new-pool-verbose", "-verbose");
        assertEquals("Added pool new-pool-verbose\n", result);

        /* Removed Pool Message Displayed with verbose option*/
        result = runCLICommand("pool", "remove", "-name",
                               "new-pool-verbose", "-verbose");
        assertEquals("Removed pool new-pool-verbose\n", result);

    }

    @Test
    public void testBasic()
        throws Exception {

        startAdminService();

        String dcname = deployDatacenter();

        /* Change policy parameters */
        changePolicy();

        /* Deploy a StorageNode */
        int sn1id = deployStorageNode(dcname, portFinder1.getRegistryPort());

        /* Deploy an Admin */
        deployAdmin(sn1id);

        /* Deploy another couple of StorageNodes */
        int sn2id = deployStorageNode(dcname, portFinder2.getRegistryPort());
        int sn3id = deployStorageNode(dcname, portFinder3.getRegistryPort());

        /* deploy another couple of Admins. */
        deployAdmin(sn2id);
        deployAdmin(sn3id);

        /* Add a new StorageNodePool */
        addpool();

        clonepool();

        /* Deploy some RepNodes */
        deployKVStore();

        /*
         * Check the policy settings
         */
        checkPolicy();

        /*
         * Check that a RepNode inherited the policy parameters, and try
         * changing that RepNode's parameters.
         */
        changeRNParams();

        /* Exercise "show plans" */
        showPlans();

        /* Test hidden parameter mechanism */
        hidden();

        /* The move-repnode command should be hidden */
        String s = runCLICommand(new String[] {"topology", "move-repnode"});
        assertFalse("Unexpected output from topology move-repnode " +
                    "command: " + s, s.contains("move-repnode -name"));

        /* Exercise pool removal. */
        rmpool();

        /* Try out expiry period setting. */
        expiry();

        /* Try an admin parameter change, resulting in a new AdminService. */
        changeAdminParam();

        /* Test admin removal and re-deployment. */
        adminRemoval(atc.getParams().getAdminParams().getAdminId());

        /* Try verification */
        verify();

        /* Try a repair */
        String repairInfo = runCLICommand("plan", "repair-topology");
        logger.fine("repairResult = " +  repairInfo);
    }

    /** Test the 'plan deploy-zone' command. */
    @Test
    public void testDeployDatacenter()
        throws Exception {

        startAdminService();

        /* Topology candidate name */
        assertCLICommandResult("Missing name",
                               getPlanDeployZone() + " -wait -rf 1",
                               "Missing required argument");
        assertCLICommandResult("Missing name value",
                               getPlanDeployZone() +
                                 " -wait -rf 1 -name",
                               "Flag -name requires " +
                                "an argument");

        /* Replication factor */
        assertCLICommandResult("Missing replication factor",
                               getPlanDeployZone() +
                                " -wait -name Miami",
                               "Missing required argument");
        assertCLICommandResult("Missing replication factor value",
                               getPlanDeployZone() +
                                " -wait -name Miami -rf",
                               "Flag -rf requires an argument");
        assertCLICommandResult(
            "Invalid argument: nope",
            getPlanDeployZone() + " -wait -name Miami -rf nope",
            "Invalid argument: nope");
        assertCLICommandResult(
            "Invalid argument: -1",
            getPlanDeployZone() + " -wait -name Miami -rf -1",
            "Invalid argument: -1");
        assertCLICommandResult(
            "Replication factor too large",
            getPlanDeployZone() + " -wait -name Miami -rf 50",
            "Illegal replication factor");
        assertCLICommandResult(
            "Create RF 0 primary zone",
            getPlanDeployZone() + " -wait -name PrimaryRF0 -rf 0 -arbiters",
            "ended successfully");
        assertCLICommandResult(
            "Create RF 0 secondary zone",
            getPlanDeployZone() +
            " -wait -name SecondaryRF0 -rf 0 -type secondary",
            "ended successfully");

        /* Master affinity */
        assertCLICommandResult(
            "Create primary RF 1 zone with master affinity",
            getPlanDeployZone() +
            " -wait -name PrimaryRF1MasterAffinity -rf 1 -master-affinity",
            "ended successfully");
        assertCLICommandResult(
            "Create primary RF 1 zone with no master affinity",
            getPlanDeployZone() +
            " -wait -name PrimaryRF1NoMasterAffinity -rf 1" +
            " -no-master-affinity",
            "ended successfully");
        assertCLICommandResult(
            "Create primary RF 0 zone with master affinity fails",
            getPlanDeployZone() +
            " -wait -name PrimaryRF0NoMasterAffinity -rf 0 -arbiters" +
            " -master-affinity",
            "Master affinity is not allowed for primary zones with" +
            " replication factor 0.");
        assertCLICommandResult(
            "Create primary RF 0 zone with no master affinity",
            getPlanDeployZone() +
            " -wait -name PrimaryRF0NoMasterAffinity -rf 0 -arbiters" +
            " -no-master-affinity",
            "ended successfully");
        assertCLICommandResult(
            "Create secondary zone with master affinity fails",
            getPlanDeployZone() +
            " -wait -debug -name SecondaryMasterAffinity -rf 1" +
            " -type secondary -master-affinity",
            "Master affinity is not allowed for secondary zones.");

        /* Allow arbiters */
        assertCLICommandResult(
            "Missing allow arbiters for RF 0 fails",
            getPlanDeployZone() +
            " -wait -name PrimaryRF0DefaultAllowArbiters -rf 0",
            "Allowing arbiters is required on primary zones with replication" +
            " factor 0.");
        assertCLICommandResult(
            "Create primary RF 0 zone that allows arbiters",
            getPlanDeployZone() +
            " -wait -name PrimaryRF0AllowArbiters -rf 0 -arbiters",
            "ended successfully");
        assertCLICommandResult(
            "Create primary RF 0 zone that does not allow arbiters fails",
            getPlanDeployZone() +
            " -wait -name PrimaryRF0NoAllowArbiters -rf 0 -no-arbiters",
            "Allowing arbiters is required on primary zones with replication" +
            " factor 0.");
        assertCLICommandResult(
            "Create primary RF 1 zone that allows arbiters",
            getPlanDeployZone() +
            " -wait -name PrimaryRF1AllowArbiters -rf 1 -arbiters",
            "ended successfully");
        assertCLICommandResult(
            "Create primary RF 1 zone without arbiters",
            getPlanDeployZone() +
            " -wait -name PrimaryRF1NoAllowArbiters -rf 1 -no-arbiters",
            "ended successfully");
        assertCLICommandResult(
            "Create secondary zone that allows arbiters fails",
            getPlanDeployZone() +
            " -wait -name SecondaryAllowArbiters -rf 1 -type secondary" +
            " -arbiters",
            "Allowing arbiters is not permitted on secondary zones.");
        assertCLICommandResult(
            "Create secondary zone without arbiters",
            getPlanDeployZone() +
            " -wait -name SecondaryNoAllowArbiters -rf 1 -type secondary" +
            " -no-arbiters",
            "ended successfully");

        assertCLICommandResult(
            "Create Atlanta",
            getPlanDeployZone() + " -wait -name Atlanta -rf 1",
            "ended successfully");
        assertCLICommandResult(
            "Create Atlanta again with different rep factor",
            getPlanDeployZone() + " -wait -name Atlanta -rf 2",
            "Zone Atlanta already exists");

        assertCLICommandResult(
            "Unknown option",
            getPlanDeployZone() + " -wait -name Miami -rf 1 -muddle",
            "Invalid argument");

        assertCLICommandResult(
            "Missing type value",
            getPlanDeployZone() + " -wait -name Miami -rf 1 -type",
            "Flag -type requires an argument");
        assertCLICommandResult(
            "Bad type value",
            getPlanDeployZone() + " -wait -name Miami " +
             "-rf 1 -type blurp",
            "Invalid zone type");

        assertCLICommandResult(
            "Create Boston",
            getPlanDeployZone() + " -wait -name Boston -rf 1",
            "ended successfully");
        assertCLICommandResult(
            "Create Boston again as a secondary zone",
            getPlanDeployZone() + " -wait -name Boston " +
             "-rf 1 -type secondary",
            "Zone Boston already exists");

        assertCLICommandResult(
            "Create Chicago",
            getPlanDeployZone() + " -wait -name Chicago -rf 1" +
            " -type secondary",
            "ended successfully");
        assertCLICommandResult(
            "Create Chicago again as a primary zone",
            getPlanDeployZone() + " -wait -name Chicago " +
             "-rf 1 -type primary",
            "Zone Chicago already exists");
    }

    @Test
    public void testRemoveSN()
        throws Exception {

        startAdminService();

        String dcname = deployDatacenter();

        /* Deploy a StorageNode */
        int sn1id = deployStorageNode(dcname, portFinder1.getRegistryPort());

        /* Deploy an Admin */
        deployAdmin(sn1id);

        /* Deploy another SN */
        int port = portFinder2.getRegistryPort();
        int sn2id = deployStorageNode(dcname, port);
        removeStorageNode(sn2id, port);
        verify();
    }


    @Test
    public void testArbiters()
        throws Exception {

        startAdminService();

        String dcname = deployArbiterZone();

        /* Deploy a StorageNode */
        int sn1id = deployStorageNode(dcname, portFinder1.getRegistryPort());

        /* Deploy an Admin */
        deployAdmin(sn1id);

        /* Deploy another couple of StorageNodes */
        int sn2id = deployStorageNode(dcname, portFinder2.getRegistryPort());
        deployStorageNode(dcname, portFinder3.getRegistryPort());

        /* deploy another couple of Admins. */
        deployAdmin(sn2id);

        /* Add a new StorageNodePool */
        addpool("sn1", "sn2", "sn3");

        /* Deploy some RepNodes and ArbNodes */
        deployKVStore(true);

        getANParams();

        runCLICommand(new String[] {"plan", "stop-service", "-service",
                      "rg1-an1", "-wait"});

        assertCLICommandResult("show plans -last",
                                "SUCCEEDED");

        runCLICommand(new String[] {"plan", "start-service", "-service",
                      "rg1-an1", "-wait"});
        assertCLICommandResult("show plans -last",
                              "SUCCEEDED");

        runCLICommand(new String[] {"plan", "stop-service", "-all-ans",
                      "-wait"});

        assertCLICommandResult("show plans -last",
                               "SUCCEEDED");

        runCLICommand(new String[] {"plan", "start-service", "-all-ans",
                      "-wait"});
        assertCLICommandResult("show plans -last",
                                "SUCCEEDED");

        /*
         * change AN parameter
         */
        changeANParams();

        /*
         * change zone allow arbiter flag
         */
        String s =
            runCLICommand(new String[] {"topology", "change-zone-arbiters",
                                        "-name",
                                        "candidateA", "-znname", dcname,
                                        "-no-arbiters"});
        assertTrue(s.contains("Changed"));
    }

    @Test
    public void testSecurity()
            throws Exception {

        startAdminService();

        /* RF=2 zone */
        String dcname = deployDatacenter();

        /* Change policy parameters */
        changePolicy();

        /* Deploy a StorageNode */
        int sn1id = deployStorageNode(dcname, portFinder1.getRegistryPort());

        /* Deploy an Admin */
        deployAdmin(sn1id);

        /* Deploy another couple of StorageNodes */
        int sn2id = deployStorageNode(dcname, portFinder2.getRegistryPort());

        /* deploy another couple of Admins. */
        deployAdmin(sn2id);

        /* Add a new StorageNodePool */
        addpool();

        /* Deploy some RepNodes */
        deployKVStore();

        assertCLICommandResult(
                "Unable to perform operation",
                "plan create-user -name nivi -password Tennisstar1!",
                "Cannot execute Create User plan. " +
                        "Create User plan requires a secure store " +
                        "to be performed successfully.");
        assertCLICommandResult(
                "Unable to perform operation",
                "plan change-user -name nivi -set-password " +
                        "-password Tennisstar1!",
                "Cannot execute Change User plan. " +
                        "Change User plan requires a secure store " +
                        "to be performed successfully.");
        assertCLICommandResult(
                "Unable to perform operation",
                "plan grant -role readonly -user nivi",
                "Cannot execute Grant Roles plan. " +
                        "Grant Roles plan requires a secure store " +
                        "to be performed successfully.");
        assertCLICommandResult(
                "Unable to perform operation",
                "plan revoke -role readonly -user nivi",
                "Cannot execute Revoke Roles plan. " +
                        "Revoke Roles plan requires a secure store " +
                        "to be performed successfully.");
        assertCLICommandResult(
                "Unable to perform operation",
                "plan drop-user -name nivi",
                "Cannot execute Drop User plan. " +
                        "Drop User plan requires a secure store " +
                        "to be performed successfully.");
        executeCLICommandResult(
                "Unable to perform operation",
                "CREATE ROLE operator",
                "Cannot execute CreateRole plan. " +
                        "CreateRole plan requires a secure store " +
                        "to be performed successfully.");
        executeCLICommandResult(
                "Unable to perform operation",
                "CREATE USER nn IDENTIFIED BY \"password\"",
                "Cannot execute CreateUser plan. " +
                        "CreateUser plan requires a secure store " +
                        "to be performed successfully.");
        executeCLICommandResult(
                "Unable to perform operation",
                "CREATE TABLE users (name STRING, age INTEGER, " +
                        "PRIMARY KEY (name))",
                "Statement completed successfully");
        executeCLICommandResult(
                "Unable to perform operation",
                "CREATE NAMESPACE IF NOT EXISTS ns1",
                "Statement completed successfully");
        executeCLICommandResult(
                "Unable to perform operation",
                "GRANT READ_TABLE ON NAMESPACE ns1 TO operator",
                "Cannot execute GrantNamespacePrivileges plan. "
                        + "GrantNamespacePrivileges plan requires a secure " +
                        "store to be performed successfully.");
        executeCLICommandResult(
                "Unable to perform operation",
                "REVOKE READ_TABLE ON NAMESPACE ns1 FROM operator",
                "Cannot execute RevokeNamespacePrivileges plan. " +
                        "RevokeNamespacePrivileges plan requires a " +
                        "secure store to be performed successfully.");
        executeCLICommandResult(
                "Unable to perform operation",
                "GRANT READ_TABLE ON NAMESPACE ns1 TO nn",
                "Cannot execute GrantNamespacePrivileges plan. " +
                        "GrantNamespacePrivileges plan requires a " +
                        "secure store to be performed successfully.");
        executeCLICommandResult(
                "Unable to perform operation",
                "REVOKE READ_TABLE ON NAMESPACE ns1 FROM nn",
                "Cannot execute RevokeNamespacePrivileges plan. " +
                        "RevokeNamespacePrivileges plan requires a " +
                        "secure store to be performed successfully.");
        executeCLICommandResult(
                "Unable to perform operation",
                "GRANT READ_TABLE ON users TO operator",
                "Cannot execute GrantPrivileges plan. " +
                        "GrantPrivileges plan requires a secure store " +
                        "to be performed successfully.");
        executeCLICommandResult(
                "Unable to perform operation",
                "REVOKE READ_TABLE ON users FROM operator",
                "Cannot execute RevokePrivileges plan. " +
                        "RevokePrivileges plan requires a secure store " +
                        "to be performed successfully.");
        executeCLICommandResult(
                "Unable to perform operation",
                "GRANT READ_TABLE ON users TO nn",
                "Cannot execute GrantPrivileges plan. " +
                        "GrantPrivileges plan requires a secure store " +
                        "to be performed successfully.");
        executeCLICommandResult(
                "Unable to perform operation",
                "REVOKE READ_TABLE ON users FROM nn",
                "Cannot execute RevokePrivileges plan. " +
                        "RevokePrivileges plan requires a secure store " +
                        "to be performed successfully.");
        executeCLICommandResult(
                "Unable to perform operation",
                "DROP ROLE operator",
                "Cannot execute DropRole plan. " +
                        "DropRole plan requires a secure store " +
                        "to be performed successfully.");
        executeCLICommandResult(
                "Unable to perform operation",
                "ALTER USER nn",
                "Cannot execute AlterUser plan. " +
                        "AlterUser plan requires a secure store " +
                        "to be performed successfully.");
        executeCLICommandResult(
                "Unable to perform operation",
                "DROP USER nn",
                "Cannot execute DropUser plan. " +
                        "DropUser plan requires a secure store " +
                        "to be performed successfully.");
        executeCLICommandResult(
                "Unable to perform operation",
                "GRANT EMPLOYEE TO ROLE manager",
                "Cannot execute GrantRoles plan. " +
                        "GrantRoles plan requires a secure store " +
                        "to be performed successfully.");
        executeCLICommandResult(
                "Unable to perform operation",
                "REVOKE employee FROM ROLE manager",
                "Cannot execute RevokeRoles plan. " +
                        "RevokeRoles plan requires a secure store " +
                        "to be performed successfully.");
    }

    @Test
    public void testNetworkRestore()
        throws Exception {

        startAdminService();

        /* RF=2 zone */
        String dcname = deployDatacenter();

        /* Change policy parameters */
        changePolicy();

        /* Deploy a StorageNode */
        int sn1id = deployStorageNode(dcname, portFinder1.getRegistryPort());

        /* Deploy an Admin */
        deployAdmin(sn1id);

        /* Deploy another couple of StorageNodes */
        int sn2id = deployStorageNode(dcname, portFinder2.getRegistryPort());

        /* deploy another couple of Admins. */
        deployAdmin(sn2id);

        /* Add a new StorageNodePool */
        addpool();

        /* Deploy some RepNodes */
        deployKVStore();

        /* Missing required arguments */
        assertCLICommandResult(
            "Missing source node id",
            "plan network-restore -to rg1-rn1 -retain-logs -wait",
            "Missing required argument");
        assertCLICommandResult(
            "Missing target node id",
            "plan network-restore -from rg1-rn1 -retain-logs -wait",
            "Missing required argument");

        /* Invalid node id */
        assertCLICommandResult(
            "Invalid Source RepNode ID",
            "plan network-restore -from admin1 -to " +
                    "rg1-rn1 -retain-logs -wait",
            "Invalid RepNode ID");

        assertCLICommandResult(
            "Invalid Target RepNode ID",
            "plan network-restore -from rg1-rn1 -to " +
                    "admin1 -retain-logs -wait",
            "Invalid RepNode ID");

        /* Non-existent nodes */
        assertCLICommandResult(
            "Non-existent nodes",
            "plan network-restore -from rg2-rn1 -to " +
                    "rg1-rn1 -retain-logs -wait",
            "RepNode does not exist");

        assertCLICommandResult(
            "Non-existent nodes",
            "plan network-restore -from rg1-rn1 -to " +
                    "rg2-rn1 -retain-logs -wait",
            "RepNode does not exist");
    }

    /**
     * Create and run a DeployDatacenterPlan, returning the name of the newly
     * created zone.
     */
    String deployDatacenter()
        throws Exception {

        assertPlanSuccess(getPlanDeployZone() +
                          " -name Miami -rf 2 -wait");
        return "Miami";
    }

    String deployArbiterZone()
            throws Exception {

            assertPlanSuccess("plan deploy-zone -name Miami " +
                              "-rf 2 -arbiters -wait");
            return "Miami";
    }



    /* Create and run a DeployStorageNode plan. */
    int deployStorageNode(String dcname, int registryPort)
        throws Exception {

        nStorageNodes++;
        assertPlanSuccess("plan deploy-sn " + getZoneNameFlag() + " " +
                          dcname + " -host localhost -port " + registryPort +
                          " -wait");

        String s = runCLICommand("show", "topology", "-sn");

        String[] lines = smartSplit(s);

        int nsns = 0;
        for (String line : lines) {
            if (line.matches(".*sn=.*")) {
                nsns++;
            }
        }

        assertEquals(nStorageNodes, nsns);

        /* Cheating a bit here, it should really parse the id from output, but
         * this works for now.
         */
        return nsns;
    }

    /*
     * Remove a storage node:
     * 1.  verify it exists
     * 2.  stop it, remove it, verify it has been removed
     */
    private void  removeStorageNode(int snId, int port)
        throws Exception {

        Verifier verifier =
            new Verifier("localhost",
                         atc.getParams().getStorageNodeParams().
                         getRegistryPort());
        StorageNodeId snid = new StorageNodeId(snId);
        assertTrue(verifier.run("topology " + snid + " RUNNING"));

        String name = RegistryUtils.bindingName
            (kvstoreName,
             snid.getFullName(),
             RegistryUtils.InterfaceType.MAIN);

        /* SN must be stopped before it can be removed */
        StorageNodeAgentAPI snai = RegistryUtils.getStorageNodeAgent
            ("localhost", port, name, service.getLoginManager(),
                    logger);
        snai.shutdown(true, true);
        assertPlanSuccess("plan remove-sn -sn " + snid + " -wait");

        runCLICommand
            (new String[] {"show", "topology", "-sn", "-status"});
        assertTrue(verifier.run("topology " + snid + " ABSENT"));
    }

    /* Create and run a DeployAdmin plan. */
    void deployAdmin(int snid)
        throws Exception {

        assertPlanSuccess("plan deploy-admin -sn " + snid + " -wait");

        String s = runCLICommand("show", "admins");

        String[] lines = smartSplit(s);

        /* The number of admins should equal the SN id number */
        assertEquals(snid, lines.length);
    }

    /*
     * This is an interesting case.  It changes an admin parameter that
     * requires a restart of the admin.  Hence it also exercises admin failover
     * and redirection of the command service.
     */
    private void changeAdminParam()
        throws Exception {
        /*
         * Since there is no SNA to restart the Admin the plan cannot
         * wait.
         */
        runCLICommand("plan", "change-param", "-all-admins",
                      "-params",
                      "loggingConfigProps=" +
                      "oracle.kv.impl.admin.Admin.level=FINE");

        /*
         * The Admin will have shut down, and we must restart it here since
         * there is no SNA.
         */
        service.waitForActive(false);

        /* Unfortunate timing issue, where we start the service too soon */
        int retry = 10;
        while (true) {
            try {
                startAdminService();
                break;
            } catch (Exception e) {
                if (retry-- <= 0) {
                    throw e;
                }
                Thread.sleep(1000);
            }
        }
        runCLICommand("plan", "wait", "-last");
        assertCLICommandResult("show plans -last",
                "SUCCEEDED");
    }

    private void adminRemoval(AdminId myId)
        throws Exception {

        String s = runCLICommand(new String[] {"show", "admins"});
        String[] lines = smartSplit(s);
        assertEquals(3, lines.length);

        /* We know how many admins there are, but not which is the master. */
        String master = getAdminMasterId();
        String victim = null;
        for (String id : getAdminReplicaIds()) {
            if (! id.equals(myId.toString())) {
                victim = id;
                break;
            }
        }
        assertTrue(victim != null);

        assertCLICommandResult("remove-admin should require -force",
                               "plan remove-admin -admin " + victim,
                               "specify the -force flag");

        assertPlanSuccess("plan remove-admin -admin " + victim +
                          " -force -wait");

        s = runCLICommand(new String[] {"show", "admins"});
        lines = smartSplit(s);
        assertEquals(2, lines.length);

        /* Now try removing the master. */
        assertCLICommandResult(
            "Expected plan complete on new master",
            "plan remove-admin -admin " + master + " -force -wait",
            "waiting");

        runCLICommand("plan", "wait", "-last");
        assertCLICommandResult("show plans -last",
                "SUCCEEDED");

        /* We should not be able to remove the sole remaining Admin. */
        victim = myId.toString();
        assertCLICommandResult(
            "Removing sole admin should fail",
            "plan remove-admin -admin " + victim + " -force -wait",
            "cannot remove the sole Admin");

        /* Restore the two Admins that we removed; they will have new ids. */

        s = runCLICommand(new String[] {"plan",
                                        "deploy-admin",
                                        "-sn",
                                        "sn2",
                                        "-wait"});

        s = runCLICommand(new String[] {"plan",
                                        "deploy-admin",
                                        "-sn",
                                        "sn3",
                                        "-wait"});

        s = runCLICommand(new String[] {"show", "admins"});
        assertTrue("New Admins have unexpected id numbers: " + s,
                   s.contains("admin5"));

        /* Now we'll try removing an Admin from a dead SNA. */

        sna3.shutdown(true, true);

        assertCLICommandResult("Remove Admin From Dead SNA",
                               "plan remove-admin -admin admin5 " +
                                       "-force -wait",
                               "ended with errors");

        /* Check that the failing step was DestroyAdmin, the final task. */
        assertCLICommandResult("Check failed plan state",
                               "show plan -last",
                               "DestroyAdmin admin5 failed");

        /* Restart the SNA. */
        sna3 = StorageNodeTestBase.startSNA(sna3.getBootstrapDir(),
                                            sna3.getBootstrapFile(),
                                            false, false);

        /* Wait for the SNA to become available in RMI registry. */

        String name = RegistryUtils.bindingName
            (kvstoreName,
             sna3.getStorageNodeId().getFullName(),
             RegistryUtils.InterfaceType.MAIN);

        int nretries = 0;
        StorageNodeAgentAPI  snai = null;
        while (snai == null) {
            try {
                snai = RegistryUtils.getStorageNodeAgent
                    ("localhost", sna3.getRegistryPort(), name,
                     service.getLoginManager(), logger);
            } catch (java.rmi.NotBoundException e) {
                if (nretries++ > 100) {
                    throw e;
                }
                System.err.println("SNA not available yet; retrying " +
                                   sna3.getRegistryPort() + " " + name +
                                   e.toString());
                try {Thread.sleep(500);} catch (Exception f) {}
            }
        }

        /* Re-run the failing plan, it should succeed now. */
        assertCLICommandResult("Re-try Remove Admin From Dead SNA",
                               "plan execute -last -wait",
                               "ended successfully");
    }

    /* Deploy some RepNodes */
    void deployKVStore() throws Exception {
        deployKVStore(false);
    }

    void deployKVStore(boolean withANs)
        throws Exception {

        String s = runCLICommand
            (new String[] {"topology",
                           "create",
                           "-name",
                           "candidateA",
                           "-pool",
                           MYPOOLNAME,
                           "-partitions",
                           "20"  /* n partitions */});
        s = runCLICommand
            (new String[] {"plan",
                           "deploy-topology",
                           "-name",
                           "candidateA",
                           "-wait"});
        s = runCLICommand(new String[] {"topology", "validate"});
        logger.fine(s);
        s = runCLICommand(new String[] {"topology", "validate",
                                        "-name", "candidateA"});
        logger.fine(s);

        /* There should be 2 RNs */
        s = runCLICommand(new String[] {"show", "topology", "-rn"});
        String[] lines = smartSplit(s);
        assertEquals("Should have two lines: " + s, 2,
                lines.length);

        if (withANs) {
            /* There should be 1 ANs */
            s = runCLICommand(new String[] {"show", "topology", "-an"});
            lines = smartSplit(s);
            assertEquals("Should have one lines: " + s, 1,
                    lines.length);
        }
    }

    /* Clone a StorageNode Pool */
    void clonepool()
        throws Exception {
        /* Clone a StorageNode Pool */
        String s = runCLICommand (new String[] {"pool", "clone", "-name",
                                                "clonedPool", "-from",
                                                MYPOOLNAME});
        // This check fails because this command returns the string
        // "Cloned Pool: <name>
        // assertTrue("output=" + s, s.trim().isEmpty());
        // TODO: the command should generate the standard json result for
        // Admin commands, and this test should check that json output
        // to see if it's run properly.

        /* Check cloned StorageNode Pool */
        s = runCLICommand(new String[] {"show", "pools"});

        assertTrue(s.indexOf("clonedPool") > -1);
        assertTrue(s.indexOf(MYPOOLNAME) > -1);

        s = runCLICommand(new String[] {"show", "pools", "-name",
                                        "clonedPool"});
        assertTrue(s.indexOf("sn1") == -1);
        assertTrue(s.indexOf("sn2") > -1);
        assertTrue(s.indexOf("sn3") > -1);

        /* remove a StorageNode from cloned StorageNode Pool */
        s = runCLICommand (new String[] {"pool", "leave", "-name",
                                         "clonedPool", "-sn", "sn2"});

        s = runCLICommand(new String[] {"show", "pools", "-name",
                                        "clonedPool"});
        assertTrue(s.indexOf("sn1") == -1);
        assertTrue(s.indexOf("sn2") == -1);
        assertTrue(s.indexOf("sn3") > -1);

        s = runCLICommand(new String[] {"pool", "remove", "-name",
                                        "clonedPool"});
    }

    /** Create a new StorageNodePool */
    void addpool()
        throws Exception {

        String s = runCLICommand
            (new String[] {"show", "pools"});

        String lines[] = smartSplit(s);

        assertEquals(1, lines.length);

        Pattern pattern =
            Pattern.compile("^.*sn(\\d+).* sn(\\d+).*\\s*$");

        Matcher matcher = pattern.matcher(s);
        assertTrue(s, matcher.matches());

        String snid1 = matcher.group(1);
        String snid2 = matcher.group(2);

        s = runCLICommand
            (new String[] {"pool", "create", "-name", MYPOOLNAME});

        s = runCLICommand
            (new String[] {"pool", "join", "-name", MYPOOLNAME,
                           "-sn", snid1});

        s = runCLICommand
            (new String[] {"pool", "join", "-name", MYPOOLNAME,
                           "-sn", snid2});

        s = runCLICommand
            (new String[] {"show", "pools"});

        lines = smartSplit(s);

        assertEquals(2, lines.length);

        matcher = pattern.matcher(lines[0]);
        assertTrue(matcher.matches());
        matcher = pattern.matcher(lines[1]);
        assertTrue(matcher.matches());
    }

    void addpool(String... snNames) throws Exception {
        String s;
        s = runCLICommand(new String[] {"pool", "create", "-name", MYPOOLNAME});
        for (String snName : snNames) {
            s = runCLICommand(new String[] {"pool", "join", "-name", MYPOOLNAME,
                              "-sn", snName});
        }
        s = runCLICommand
                (new String[] {"show", "pools"});

        String[] lines = smartSplit(s);

        assertEquals(2, lines.length);
    }

    /** Remove the StorageNodePool */
    void rmpool()
        throws Exception {

        String s = runCLICommand
            (new String[] {"show", "pools"});

        String lines[] = smartSplit(s);

        assertEquals(2, lines.length);

        s = runCLICommand
            (new String[] {"pool", "remove", "-name", MYPOOLNAME});

        s = runCLICommand
            (new String[] {"show", "pools"});

        lines = smartSplit(s);

        assertEquals(1, lines.length);
    }

    private void showPlans() throws Exception {

        /* Get the existing number of plans */
        JsonNode json = runJsonCLICommand("show plans -num 1000 -json");
        ArrayNode plans =
            (ArrayNode) getJsonPath(json, "returnValue", "plans");
        final int initialNPlans = plans.size();

        /* Now add a bunch of plans to make the queries interesting. */
        final int nplans = 20;
        final int idleInterval = 7;
        int idleBeforePlanId1 = 0;
        int idleBeforePlanId2 = 0;
        for (int i = 0; i < nplans; i++) {
            final boolean idle1 = (i == idleInterval);
            final boolean idle2 = (i == 2*idleInterval);

            /*
             * for timestamp query testing purposes, insert idle time before
             * plans idleBeforePlanId1 and idleBeforePlanId2.
             */
            if (idle1 || idle2) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                }
            }

            json = runJsonCLICommand(
                "plan deploy-sn -dc 1 -host localhost" +
                " -port " + portFinder1.getRegistryPort() +
                " -wait -json");
            int planId = getJsonPath(json, "returnValue", "id").asInt();
            if (idle1) {
                idleBeforePlanId1 = planId;
            } else if (idle2) {
                idleBeforePlanId2 = planId;
            }
        }

        /* See if we have the expected number of plans now */
        json = runJsonCLICommand("show plans -num 1000 -json");
        plans = (ArrayNode) getJsonPath(json, "returnValue", "plans");
        int currentNPlans = plans.size();
        assertTrue("Expected at least " + (initialNPlans + nplans) +
                   " plans, found " + currentNPlans,
                   currentNPlans >= initialNPlans + nplans);

        /* Test no-arguments default: show last ten plans */
        json = runJsonCLICommand("show plans -json");
        plans = (ArrayNode) getJsonPath(json, "returnValue", "plans");
        assertEquals(10, plans.size());

        /* Test -id -num */
        json = runJsonCLICommand("show plans -json -id " + idleBeforePlanId1);
        assertEquals(idleBeforePlanId1,
                     getJsonPath(json, "returnValue", "id").asInt());

        /*
         * Find start timestamps for the two plans with idling before them. The
         * time format is "2023-06-06 15:05:28 UTC". Strip off the UTC part.
         */
        final String idleBeforePlanStart1 =
            getJsonPath(json, "returnValue", "start").asText()
            .replace(" UTC", "");
        json = runJsonCLICommand("show plans -json -id " + idleBeforePlanId2);
        final String idleBeforePlanStart2 =
            getJsonPath(json, "returnValue", "start").asText()
            .replace(" UTC", "");

        /* Test time range -from -to */
        json = runJsonCLICommand("show", "plans",
                                 "-from", idleBeforePlanStart1,
                                 "-to", idleBeforePlanStart2,
                                 "-json");
        plans = (ArrayNode) getJsonPath(json, "returnValue", "plans");

        /* Allow some slop here */
        assertTrue("Found " + plans.size(),
                   (plans.size() >= idleInterval) &&
                   (plans.size() < (2*idleInterval)));

        /* Test -from -num */
        json = runJsonCLICommand("show", "plans",
                                 "-from", idleBeforePlanStart1,
                                 "-num", "5",
                                 "-json");
        plans = (ArrayNode) getJsonPath(json, "returnValue", "plans");
        assertEquals(5, plans.size());

        /* Test -to -num */
        json = runJsonCLICommand("show", "plans",
                                 "-to", idleBeforePlanStart1,
                                 "-num", "7",
                                 "-json");
        plans = (ArrayNode) getJsonPath(json, "returnValue", "plans");
        assertEquals(7, plans.size());
    }

    /**
     * Run the specified space-separated CLI command in JSON mode, check for
     * success, and return the JSON result. Note that the caller must include
     * the '-json' flag.
     */
    private JsonNode runJsonCLICommand(String command) throws Exception {
        return runJsonCLICommand(command.split(" "));
    }

    /**
     * Run the specified array-based CLI command in JSON mode, check for
     * success, and return the JSON result. Note that the caller must include
     * the '-json' flag.
     */
    private JsonNode runJsonCLICommand(String... command) throws Exception {
        assertTrue("Command doesn't contain '-json': " +
                   Arrays.toString(command),
                   Arrays.stream(command).anyMatch(s -> "-json".equals(s)));
        final JsonNode json =
            JsonUtils.parseJsonNode(runCLICommand(command));
        assertEquals(String.format("Wrong return code for command %s: %s",
                                   Arrays.toString(command),
                                   json),
                     5000, json.get("returnCode").asInt());
        return json;
    }

    /**
     * Navigate to a JsonNode by doing a series of gets, failing if a field
     * along the path is missing.
     */
    private static JsonNode getJsonPath(JsonNode node, String... fields) {
        JsonNode result = node;
        for (final String field : fields) {
            result = result.get(field);
            if (result == null) {
                throw new AssertionError(
                    String.format("Field '%s' was not found" +
                                  " when accessing fields %s in %s",
                                  field,
                                  Arrays.toString(fields),
                                  node));
            }
        }
        return result;
    }

    private void expiry()
        throws Exception {

        Verifier verifier =
            new Verifier("localhost",
                         atc.getParams().getStorageNodeParams().
                         getRegistryPort());

        Admin admin = service.getAdmin();

        assertTrue
            (verifier.run("parameter admin1 eventExpiryAge " +
                    "equal 30_DAYS"));

        /* Double-check the value against the Admin itself. */
        final long expectedEventExpiryAge = 1000L * 60L * 60L * 24L * 30L;
        assertEquals(expectedEventExpiryAge, admin.getEventExpiryAge());

        /* Run a plan to update the expiry age. */

        runCLICommand
            (new String[] {"plan",
                           "change-param",
                           "-all-admins",
                           "-wait",
                           "-params",
                           "eventExpiryAge=0 DAYS"});

        /* Now check to be sure the new value is available in the Admin. */
        assertEquals(0L, admin.getEventExpiryAge());
    }

    void verify()
        throws Exception {

        runCLICommand(new String[] {"verify"});
    }

    void changePolicy()
        throws Exception {

        Verifier verifier =
            new Verifier("localhost",
                         atc.getParams().getStorageNodeParams().
                         getRegistryPort());

        String s = runCLICommand
            (new String[] {"show", "param", "-policy"});

        /*
         * Check for a hard coded default policy, one that affects a calculated
         * field.
         */
        Pattern pattern =
            Pattern.compile("^.*rnStatisticsLeaseDuration=10 MINUTES.*$",
                            Pattern.DOTALL);
        Matcher matcher = pattern.matcher(s);
        assertTrue("The output was unexpected: " + s, matcher.matches());
        /* Now change the policy. */

        s = runCLICommand
            (new String[] {"change-policy", "-params",
                           "rnStatisticsLeaseDuration=30 SECONDS"});
        assertTrue(verifier.run(
            "parameter policy rnStatisticsLeaseDuration " +
                    "equal 30_SECONDS"));

        /*
         * Change a couple from the default and see that it works
         * after deployment
         */
        s = runCLICommand
            (new String[] {"change-policy", "-params",
                           "adminLogFileLimit=8000000"});
        assertTrue(verifier.run
                   ("parameter policy adminLogFileLimit " +
                           "equal 8000000"));

        s = runCLICommand
            (new String[] {"change-policy", "-params",
                           "serviceLogFileLimit=6000000"});
        assertTrue(verifier.run
                   ("parameter policy serviceLogFileLimit " +
                           "equal 6000000"));
    }

    void checkPolicy()
        throws Exception {

        String s = runCLICommand
            (new String[] {"show", "param", "-service", "admin1"});
        Pattern pattern =
            Pattern.compile("^.*adminLogFileLimit=8000000.*$",
                    Pattern.DOTALL);
        Matcher matcher = pattern.matcher(s);
        assertTrue(matcher.matches());

        s = runCLICommand
            (new String[] {"show", "param", "-service", "sn1"});
        pattern = Pattern.compile("^.*serviceLogFileLimit=6000000.*$",
                                  Pattern.DOTALL);
        matcher = pattern.matcher(s);
        assertTrue(matcher.matches());
    }

    void changeRNParams()
        throws Exception {

        Verifier verifier =
            new Verifier("localhost",
                         atc.getParams().getStorageNodeParams().
                         getRegistryPort());

        String s = runCLICommand
            (new String[] {"show", "param", "-service", "1,1"});

        /*
         * Check that the repnode inherited the policy params as set up in
         * changePolicy, above.
         */
        Pattern pattern =
            Pattern.compile("^.*rnStatisticsLeaseDuration=30 SECONDS.*$",
                            Pattern.DOTALL);
        Matcher matcher = pattern.matcher(s);
        assertTrue("Unexpected output: " + s, matcher.matches());

        /* Now change the RepNode's params. */

        s = runCLICommand(new String[] { "plan", "change-param", "-wait",
            "-service", "rg1-rn1", "-verbose", "-params",
            "rnStatisticsLeaseDuration=70000000 MICROSECONDS" });
        /*
         * Check that the params change took effect, including the calculated
         * field.
         */
        verifier.run
            ("parameter rg1-rn1 rnStatisticsLeaseDuration equal " +
             "70000000_MICROSECONDS");
    }

    void getANParams()
        throws Exception {

        String s = runCLICommand
            (new String[] {"show", "param", "-service", "rg1-an1"});
        Pattern pattern =
            Pattern.compile("^.*arbNodeId=rg1-an1.*$",
                            Pattern.DOTALL);
        Matcher matcher = pattern.matcher(s);
        assertTrue("Unexpected output: " + s, matcher.matches());
    }

    void changeANParams()
            throws Exception {
        Verifier verifier =
            new Verifier("localhost",
                         atc.getParams().getStorageNodeParams().
                         getRegistryPort());

        /* Now change the ArbNode's params */
        runCLICommand(new String[] {"plan", "change-param", "-wait", "-service",
                                    "rg1-an1", "-verbose", "-params",
                                     ParameterState.JE_MISC +
                                     "=je.rep.repstreamOpenTimeout=10 s"});
        /*
         * Check that the params change took effect
         */
        assertTrue(
            verifier.run("parameter rg1-an1 configProperties equal " +
                         "'je.rep.repstreamOpenTimeout=10 s'"));
        runCLICommand(new String[] {"plan", "change-param", "-wait",
                                    "-all-ans", "-verbose", "-params",
                                    ParameterState.JE_MISC +
                                    "=je.rep.repstreamOpenTimeout=11 s"});
        /*
         * Check that the params change took effect
         */
        assertTrue(
            verifier.run("parameter rg1-an1 configProperties equal " +
                         "'je.rep.repstreamOpenTimeout=11 s'"));
    }

    void hidden()
        throws Exception {

        Verifier verifier =
            new Verifier("localhost",
                         atc.getParams().getStorageNodeParams().
                         getRegistryPort());

        /*
         * Use a known hidden policy parameter.
         */
        String s = runCLICommand
            (new String[] {"show", "param", "-policy"});

        assertTrue(s.indexOf("serviceStopWait") == -1);

        /* Try to set a hidden parameter -- this fails */
        s = runCLICommand
            (new String[] {"change-policy", "-params",
                           "serviceStopWait=400 SECONDS"});
        assertTrue(verifier.run
                   ("parameter policy serviceStopWait notequal " +
                           "400_SECONDS"));

        /*
         * Run the command -- this is a no-op since runCLICommand is not
         * interactive.  It just tests that the command does something.
         */
        s = runCLICommand
            (new String[] {"hidden"});

        /*
         * Run commands using -hidden flag.
         */
        s = runCLICommand
            (new String[] {"show", "param", "-policy", "-hidden"});
        assertTrue(s.indexOf("serviceStopWait") >= 0);
        assertTrue(s.indexOf("serviceStopWait=400 SECONDS") == -1);

        s = runCLICommand
            (new String[] {"change-policy", "-hidden", "-params",
                           "serviceStopWait=400_SECONDS"});
        assertTrue(verifier.run
                   ("parameter policy serviceStopWait equal " +
                           "400_SECONDS"));
    }

    /**
     * Test that the load command exits immediately after a failing command.
     */
    @Test
    public void testLoadExitOnFailure()
        throws Exception {

        String msg = "Testing unknown command";
        String file = printlnToTempFile("gort\n" +
                                        "klaatu");
        String s = runCLICommand("load", "-file", file);
        assertContains(msg, "gort", s);
        assertDoesNotContain(msg, "klaatu", s);

        msg = "Testing unknown sub-command";
        file = printlnToTempFile("show gort\n" +
                                 "show klaatu");
        s = runCLICommand("load", "-file", file);
        assertContains(msg, "gort", s);
        assertDoesNotContain(msg, "klaatu", s);

        msg = "Testing extra argument";
        file = printlnToTempFile("ping -shard rg2 -extra-gort\n" +
                                 "ping -shard rg2 -extra-klaatu");
        s = runCLICommand("load", "-file", file);
        assertContains(msg, "gort", s);
        assertDoesNotContain(msg, "klaatu", s);

        msg = "Testing unknown argument";
        file = printlnToTempFile("show events -gort\n" +
                                 "show events -klaatu");
        s = runCLICommand("load", "-file", file);
        assertContains(msg, "gort", s);
        assertDoesNotContain(msg, "klaatu", s);

        msg = "Testing failing command";
        file = printlnToTempFile("add schema -file /unknown-directory/gort\n"
                + "add schema -file /unknown-directory/klaatu");
        s = runCLICommand("load", "-file", file);
        assertContains(msg, "gort", s);
        assertDoesNotContain(msg, "klaatu", s);
    }

    /**
     * Prints a string to a temporary file that will be deleted on exit, and
     * returns the name of the file.
     */
    private String printlnToTempFile(final String s)
        throws IOException {

        final File file = File.createTempFile("temp", "out");
        file.deleteOnExit();
        final PrintStream out = new PrintStream(file);
        try {
            out.println(s);
            out.flush();
            return file.getPath();
        } finally {
            out.close();
        }
    }

    private void assertContains(final String message,
                                final String substring,
                                final String s) {
        assertTrue(message + ": Expected substring: " + substring +
                   "\n  Found: " + s,
                   s.indexOf(substring) != -1);
    }

    private void assertDoesNotContain(final String message,
                                      final String substring,
                                      final String s) {
        assertTrue(message + ": Did not expect substring: " + substring +
                   "\n  Found: " + s,
                   s.indexOf(substring) == -1);
    }

    /**
     * Return the Admin id, in String form, of the current master Admin.
     */
    private String getAdminMasterId()
        throws Exception {

        String s = runCLICommand(new String[] {"show", "admins"});
        for (String a : smartSplit(s)) {
            if (a.contains("(connected RUNNING,MASTER)")) {
                return a.split(":")[0];
            }
        }

        /* Really, this will not happen. */
        throw new RuntimeException("No master Admin found in 'show admins'");
    }

    /**
     * Return an list of the non-master Admin ids
     */
    private List<String> getAdminReplicaIds()
        throws Exception {

        List<String> replicas = new ArrayList<>();
        String s = runCLICommand(new String[] {"show", "admins"});
        for (String a : smartSplit(s)) {
            if (! a.contains("(connected RUNNING,MASTER)")) {
                replicas.add(a.split(":")[0]);
            }
        }

        return replicas;
    }

    /**
     * Verifies that the CLI command 'plan remove-zone' can successfully remove
     * a zone that contains no storage nodes (is empty).
     */
    @Test
    public void testRemoveZoneEmpty() throws Exception {

         /* Create store */
        startAdminService();
        final String dcname = deployDatacenter();
        final int sn1id =
            deployStorageNode(dcname, portFinder1.getRegistryPort());
        deployAdmin(sn1id);
        deployStorageNode(dcname, portFinder2.getRegistryPort());
        addpool();
        deployKVStore();

        /* Add and remove additional zone */
        deployZone("zone2", "1");
        final String result = removeZone("zone2", 1);
        assertTrue(result.contains("SUCCEEDED"));
    }

    /**
     * Verifies that the CLI command 'plan remove-zone' cannot remove a zone
     * that contains at least 1 storage node (is non-empty). This test does the
     * following:
     * <ul>
     * <li> Deploys 1 storage node belonging to a given zone
     * <li> Attempts to remove the zone that the storage node belonged to and
     * verifies that the removal fails as expected.
     * </ul>
     */
    @Test
    public void testRemoveZoneNonEmpty() throws Exception {

        startAdminService();

        final String zone = "east-zone";
        final String repFactor = "1";
        final String pool = "snpool";
        final int port = portFinder1.getRegistryPort();
        final String expectedStr =
            "oracle.kv.impl.admin.IllegalCommandException: " +
            "Cannot remove non-empty";

        /* The expected SNA was started by the AdminBaseTest super class.
         * Deploy the zone to which the SN will be deployed, and create
         * the pool that SN will join.
         */
        deployZone(zone, repFactor);
        addPool(pool);

        /* Deploy sn1 to the zone and join the pool. */
        final int sn1id = deployStorageNode(zone, port);
        joinPool(pool, sn1id);

        /* Attempt to remove the non-empty zone, and verify that the zone is
         * not removed.
         */
        final String s = runCLICommand("show", getZoneTerm());
        final int nZonesExpected = countMatches("^zn: .*", smartSplit(s));
        final String resultStr = removeZone(zone, nZonesExpected);
        assertTrue(resultStr.contains(expectedStr));
    }

    private void deployZone(String name, String repFactor) throws Exception {
        assertPlanSuccess(
                          getPlanDeployZone() + " -name " + name +
                          " -rf " + repFactor + " -wait");
    }

    private void addPool(String name) throws Exception {

        runCLICommand("pool", "create", "-name", name);
    }

    private void joinPool(String name, int snid) throws Exception {

        runCLICommand(
            "pool", "join", "-name", name, "-sn", Integer.toString(snid));
    }

    private void createDeployTopology(String name,
                                      String pool,
                                      String nPartitions,
                                      int nExpectedSns)
        throws Exception {

        String s = runCLICommand("topology", "create", "-name", name,
                                 "-pool", pool, "-partitions", nPartitions);
        assertTrue("Topology creation failed with CLI error message:\n"
                   + s, s.contains("Created: "));
        assertPlanSuccess("plan deploy-topology -name " + name
                           + " -wait");

        s = runCLICommand("topology", "validate");
        logger.fine(s);

        s = runCLICommand("topology", "validate", "-name", name);
        logger.fine(s);

        /* Verify the expected number of SNs are contained in the topology. */
        s = runCLICommand("show", "topology", "-sn");
        final String[] lines = smartSplit(s);
        assertEquals(nExpectedSns, lines.length);
    }

    private String removeZone(String name, final int nExpected)
        throws Exception {

        runCLICommand("plan", getRemoveZone(), getZoneNameFlag(), name,
                      "-wait");

        /* Verify that the number of zones after the removal attempt is as
         * expected; either successfully removed for the case of an empty
         * zone, or not removed when the zone is non-empty.
         */
        String s = runCLICommand("show", getZoneTerm());
        assertEquals(nExpected, countMatches("^zn: .*", smartSplit(s)));
        return runCLICommand("show", "plans", "-last");
    }

    private static int countMatches(final String pattern,
                                    final String[] lines) {
        final Pattern p = Pattern.compile(pattern);
        int count = 0;
        for (final String line : lines) {
            if (p.matcher(line).matches()) {
                count++;
            }
        }
        return count;
    }

    /**
     * Test case intended to verify new functionality added to the 'plan
     * change-parameters' command of the CLI. To address SR #22563, the 'plan
     * change-parameters' command was modified to handle two new arguments:
     * '-zn' and '-znname'; where if either argument is specified with that
     * command, then the desired parameter change will be restricted to all and
     * only the instances of the desired service type -- RepNode or Admin --
     * deployed to the specified zone.
     *
     * Note that this test case adresses parameter changes for both RNs and
     * Admins; rather than defining separate test cases, one for RNs and one
     * for Admins. This is done because creating a store and deploying SNAs,
     * RNs, and Admins can be time consuming. Thus, to save time, the store is
     * deployed once; after which, an RN parameter is first changed for all the
     * RNs in a given zone, and then an Admin parameter is changed for all the
     * Admins in the zone.
     */
    @Test
    public void testChangeParamsByZone() throws Exception {

        startAdminService();

        final AdminServiceParams params = service.getParams();
        assertNotNull("AdminService should not return null params",
                params);

        /* SETUP AND CREATE THE DESIRED TOPOLOGY */

        /*
         * Initialize the map of zone names and corresponding replication
         * factors. Add/remove entries and/or change the zone names and
         * replication factors to effect changes in the desired topology.
         */
        final Map<String, String> zoneRfMap = new HashMap<>();

        zoneRfMap.put("east-zone", "3");
        zoneRfMap.put("west-zone", "2");
        zoneRfMap.put("asia-zone", "1");

        final String targetName = "west-zone";
        String targetId = null;

        /*
         * Initialize the map of SN ids and corresponding deployment
         * information; which consists of the zone in which to deploy
         * the SN, the SN's port, and whether or not to deploy an
         * Admin on that SN (the adminFlag).
         *
         * Notes regarding the initialization of this map:
         *
         * 1. The zone names used should be one of the zone names
         *    specified in zoneRfMap above.
         * 2. The first 3 entries of the map should correspond to
         *    the 3 SNAs initially started by the parent class;
         *    which is achieved by specifying the port using the
         *    portFinder1, 2, & 3 instances that are defined in that
         *    parent class.
         * 3. Ports for any additional SNs are NOT set here (0 is
         *    specified), but will be set during the deployment process.
         * 4. The adminFlag of the first entry MUST be true.
         * 5. If N Admins are to be deployed (where 1 < N <= #-of-SNs),
         *    then the adminFlag of the first N entries MUST be true;
         *    that is, "gaps" are not allowed. If this requirement is
         *    not satisfied, then an error will occur when the next SN
         *    is deployed. An error occurs because the deployAdmin method
         *    requires that the number of Admins deployed must equal the
         *    number of SNs currently deployed when an Admin is deployed;
         *    which will not be true in the presence of such "gaps".
         *
         * Add/remove entries and/or change the contents of the entries
         * to effect changes in the desired topology.
         */
        final TreeMap<String, SnDeployInfo> snDeployMap = new TreeMap<>();

        /* Entries corresponding to the pre-started SNAs. */
        snDeployMap.put("sn1", new SnDeployInfo("east-zone",
                                                portFinder1.getRegistryPort(),
                                                true));
        snDeployMap.put("sn2", new SnDeployInfo("west-zone",
                                                portFinder2.getRegistryPort(),
                                                true));
        snDeployMap.put("sn3", new SnDeployInfo("east-zone",
                                                portFinder3.getRegistryPort(),
                                                true));
        /* Entries corresponding to the additional SNAs to deploy. */
        snDeployMap.put("sn4", new SnDeployInfo("west-zone", 0,
                true));
        snDeployMap.put("sn5", new SnDeployInfo("east-zone", 0,
                false));
        snDeployMap.put("sn6", new SnDeployInfo("asia-zone", 0,
                false));

        /* Initialize the remaining required arguments. */
        final int capacity = 1;
        final String pool = "snpool";
        final String topoName = "testChangeParamsByZone-topology";
        final String nPartitions = "20";

        /* Deploy the additional SNAs and save the results. */
        snaList = deployCluster(zoneRfMap, snDeployMap, capacity,
                                pool, topoName, nPartitions);

        /*
         * At this point the desired topology should be deployed to the
         * store. Use the deployed store's Admin to retrieve the id
         * corresponding to the zone name that is targeted by this test case.
         */
        final Admin admin = service.getAdmin();
        targetId = getZoneId(admin, targetName);
        assertNotNull("testChangeParamsByZone: null targetId ",
                       targetId);

        /* Used by both the RN and Admin param change code below. */
        Map<StorageNodeId, String> expectedMap = null;
        String paramName = null;
        String newParamVal = null;

        /*
         * 1a. Do RN param change by ZONE, using the zone NAME:
         *     - set up the expected results
         *     - request the desired param change.
         *     - verify the results.
         */
        paramName = ParameterState.SP_LATENCY_CEILING;
        newParamVal = "25";
        expectedMap = new TreeMap<>();

        /* Expect param changes for only RNs in the targeted zone. */
        expectedMap.put(StorageNodeId.parse("sn2"), newParamVal);
        expectedMap.put(StorageNodeId.parse("sn4"), newParamVal);

        /* Verify old param val (val to be changed) != new param val. */
        for (RepNodeParams p :
                 admin.getCurrentParameters().getRepNodeParams()) {

            final RepNodeId rnId = p.getRepNodeId();
            final StorageNodeId snId = p.getStorageNodeId();
            final ParameterMap pMap = p.getMap();

            final String curParamVal = pMap.get(paramName).asString();
            final String expectedVal = expectedMap.get(snId);

            if (expectedVal == null) {
                continue;
            }
            assertFalse("When changing RepNode parameter BY NAME " +
                        "[SN=" + snId + ", RN=" + rnId + "]: " +
                        "current value of parameter to change [" +
                        paramName + "] already equals the expected " +
                        "new value [" + newParamVal + "]",
                        expectedVal.equals(curParamVal));
        }

        /*
         * Exercise the functionality of the new '-znname' argument by
         * requesting a change in the desired param for only the RNs
         * in the targeted zone.
         */
        runCLICommand("plan", "change-parameters", "-all-rns",
                      getZoneNameFlag(), targetName, "-wait",
                      "-params", paramName + "=" + newParamVal);

        /* Verify that the desired param change occurred. */
        for (RepNodeParams p :
                 admin.getCurrentParameters().getRepNodeParams()) {

            final RepNodeId rnId = p.getRepNodeId();
            final StorageNodeId snId = p.getStorageNodeId();
            final ParameterMap pMap = p.getMap();

            final String changedVal = pMap.get(paramName).asString();
            final String expectedVal = expectedMap.get(snId);

            if (expectedVal == null) {
                continue;
            }
            assertTrue("When changing RepNode parameter BY NAME " +
                       "[SN=" + snId + ", RN=" + rnId + "]: new " +
                       "value of parameter to change [" + paramName +
                       "] does not equal the expected value [" +
                       newParamVal + "]",
                       expectedVal.equals(changedVal));
        }

        /* 1b. Do RN param change by ZONE, using the zone ID */
        newParamVal = "40";

        /* Re-initialize expectations for the next param change */
        expectedMap = new TreeMap<>();
        expectedMap.put(StorageNodeId.parse("sn2"), newParamVal);
        expectedMap.put(StorageNodeId.parse("sn4"), newParamVal);

        /* Verify old param val (val to be changed) != new param val. */
        for (RepNodeParams p :
                 admin.getCurrentParameters().getRepNodeParams()) {

            final RepNodeId rnId = p.getRepNodeId();
            final StorageNodeId snId = p.getStorageNodeId();
            final ParameterMap pMap = p.getMap();

            final String curParamVal = pMap.get(paramName).asString();
            final String expectedVal = expectedMap.get(snId);

            if (expectedVal == null) {
                continue;
            }
            assertFalse("When changing RepNode parameter BY ID " +
                        "[SN=" + snId + ", RN=" + rnId + "]: " +
                        "current value of parameter to change [" +
                        paramName + "] already equals the expected " +
                        "new value [" + newParamVal + "]",
                        expectedVal.equals(curParamVal));
        }

        /*
         * Exercise the functionality of the new '-zn' argument by
         * requesting a change in the desired param for only the RNs
         * in the targeted zone.
         */
        runCLICommand("plan", "change-parameters", "-all-rns",
                      getZoneIdFlag(), targetId, "-wait",
                      "-params", paramName + "=" + newParamVal);

        /* Verify that the desired param change occurred. */
        for (RepNodeParams p :
                 admin.getCurrentParameters().getRepNodeParams()) {

            final RepNodeId rnId = p.getRepNodeId();
            final StorageNodeId snId = p.getStorageNodeId();
            final ParameterMap pMap = p.getMap();

            final String changedVal = pMap.get(paramName).asString();
            final String expectedVal = expectedMap.get(snId);

            if (expectedVal == null) {
                continue;
            }
            assertTrue("When changing RepNode parameter BY ID " +
                       "[SN=" + snId + ", RN=" + rnId + "]: new " +
                       "value of parameter to change [" + paramName +
                       "] does not equal the expected value [" +
                       newParamVal + "]",
                       expectedVal.equals(changedVal));
        }

        /*
         * 2a. Do ADMIN param change by ZONE, using the zone NAME:
         *     - set up the expected results
         *     - request the desired param change.
         *     - verify the results.
         */
        paramName = ParameterState.AP_EVENT_EXPIRY_AGE;
        newParamVal = "15 DAYS";
        expectedMap = new TreeMap<>();

        /* Expect param changes for only Admins in the "west-zone" zone. */
        expectedMap.put(StorageNodeId.parse("sn2"), newParamVal);
        expectedMap.put(StorageNodeId.parse("sn4"), newParamVal);

        /* Verify old param val (val to be changed) != new param val. */
        for (AdminParams p :
                 admin.getCurrentParameters().getAdminParams()) {

            final AdminId adminId = p.getAdminId();
            final StorageNodeId snId = p.getStorageNodeId();
            final ParameterMap pMap = p.getMap();

            final String curParamVal = pMap.get(paramName).asString();
            final String expectedVal = expectedMap.get(snId);

            if (expectedVal == null) {
                continue;
            }
            assertFalse("When changing Admin parameter BY NAME " +
                        "[SN=" + snId + ", ADMIN=" + adminId + "]: " +
                        "current value of parameter to change [" +
                        paramName + "] already equals the expected " +
                        "new value [" + newParamVal + "]",
                        expectedVal.equals(curParamVal));
        }

        /*
         * Exercise the functionality of the new '-znname' argument by
         * requesting a change in the desired param for only the Admins
         * in the targeted zone.
         */
        runCLICommand("plan", "change-parameters", "-all-admins",
                      getZoneNameFlag(), targetName, "-wait",
                      "-params", paramName + "=" + newParamVal);

        /* Verify that the desired param change occurred. */
        for (AdminParams p :
                 admin.getCurrentParameters().getAdminParams()) {

            final AdminId adminId = p.getAdminId();
            final StorageNodeId snId = p.getStorageNodeId();
            final ParameterMap pMap = p.getMap();

            final String changedVal = pMap.get(paramName).asString();
            final String expectedVal = expectedMap.get(snId);

            if (expectedVal == null) {
                continue;
            }
            assertTrue("When changing Admin parameter BY NAME " +
                       "[SN=" + snId + ", ADMIN=" + adminId + "]: " +
                       "new value of parameter to change [" +
                       paramName + "] does not equal the expected " +
                       "value [" + newParamVal + "]",
                       expectedVal.equals(changedVal));
        }

        /* 2b. Do ADMIN param change by ZONE, using the zone ID */
        newParamVal = "45 DAYS";

        /* Re-initialize expectations for the next param change */
        expectedMap = new TreeMap<>();
        expectedMap.put(StorageNodeId.parse("sn2"), newParamVal);
        expectedMap.put(StorageNodeId.parse("sn4"), newParamVal);

        /* Verify old param val (val to be changed) != new param val. */
        for (AdminParams p :
                 admin.getCurrentParameters().getAdminParams()) {

            final AdminId adminId = p.getAdminId();
            final StorageNodeId snId = p.getStorageNodeId();
            final ParameterMap pMap = p.getMap();

            final String curParamVal = pMap.get(paramName).asString();
            final String expectedVal = expectedMap.get(snId);

            if (expectedVal == null) {
                continue;
            }
            assertFalse("When changing Admin parameter BY ID " +
                        "[SN=" + snId + ", ADMIN=" + adminId + "]: " +
                        "current value of parameter to change [" +
                        paramName + "] already equals the expected " +
                        "new value [" + newParamVal + "]",
                        expectedVal.equals(curParamVal));
        }

        /*
         * Exercise the functionality of the new '-zn' argument by
         * requesting a change in the desired param for only the Admins
         * in the targeted zone.
         */
        runCLICommand("plan", "change-parameters", "-all-admins",
                      getZoneIdFlag(), targetId, "-wait",
                      "-params", paramName + "=" + newParamVal);

        /* Verify that the desired param change occurred. */
        for (AdminParams p :
                 admin.getCurrentParameters().getAdminParams()) {

            final AdminId adminId = p.getAdminId();
            final StorageNodeId snId = p.getStorageNodeId();
            final ParameterMap pMap = p.getMap();

            final String changedVal = pMap.get(paramName).asString();
            final String expectedVal = expectedMap.get(snId);

            if (expectedVal == null) {
                continue;
            }
            assertTrue("When changing Admin parameter BY ID " +
                       "[SN=" + snId + ", ADMIN=" + adminId + "]: " +
                       "new value of parameter to change [" +
                       paramName + "] does not equal the expected " +
                       "value [" + newParamVal + "]",
                       expectedVal.equals(changedVal));
        }
    }

    /**
     * Test case intended to verify part of the new functionality added to the
     * 'plan remove-admin' command of the CLI. To address SR #22563, the 'plan
     * remove-admin' command was modified to handle two new arguments: '-zn'
     * and '-znname'; where if either argument is specified with that command,
     * then all Admins will be removed from the specified zone. This test case
     * verifies the new functionality when either the '-znname' flag or the
     * '-zn' flag when the specified zone does not contain the master
     * Admin. This test case also covers the use of the '-force' flag in
     * conjunction with Admin removal from a zone.
     *
     * For this test case, a store with the following configuration is used:
     * <ul>
     *   <li>3 zones with RF factors 1, 3, and 2 respectively
     *   <li>6 SNs
     *   <li>6 Admins
     *   <li>sn1 belongs to the first zone
     *   <li>sn2, sn4, and sn5 each belong to the second zone
     *   <li>sn3 and sn6 belong to the third zone
     *   <li>admin1 is the master and belongs to the first zone
     *   <li>admin2, admin4, and admin5 each belong to the second zone
     *   <li>admin3 and admin6 belong to the third zone
     * </ul>
     *
     * After deploying the store described above, an attempt is made to first
     * use the '-znname' flag with the 'plan remove-admin' command to remove
     * admin2, admin4, and admin5 from the second zone.  Then, after verifying
     * the first removal attempt was successful, the 'plan remove-admin'
     * command is again executed -- this time using the '-zn' flag in
     * conjunction with the '-force' flag -- in an attempt to remove admin3 and
     * admin6 from the third zone; after which the results are verified.
     *
     * Note that if an attempt is made to remove the master Admin, the
     * RemoveAdminPlan class will shutdown and restart that Admin so that
     * mastership is transferred to one of the other replicas; and the Admin
     * removal plan will be interrupted and must be re-executed on the new
     * master. As a result, this test case specifically avoids attempting to
     * remove the master Admin; whereas the test case named
     * testRemoveAdminByZoneMaster addresses removing the Admins from a zone
     * that initially contains the master Admin.
     */
    @Test
    public void testRemoveAdminByZoneNoMaster() throws Exception {

        startAdminService();

        final AdminServiceParams params = service.getParams();
        assertNotNull("AdminService should not return null params",
                       params);

        /* SETUP AND CREATE THE DESIRED TOPOLOGY */

        /*
         * Initialize the map of zone names and corresponding replication
         * factors. Add/remove entries and/or change the zone names and
         * replication factors to effect changes in the desired topology.
         */
        final Map<String, String> zoneRfMap = new HashMap<>();

        zoneRfMap.put("east-zone", "1"); /* Will be Master Admin */
        zoneRfMap.put("west-zone", "3");
        zoneRfMap.put("asia-zone", "2");

        /*
         * Initialize the map of SN ids and corresponding deployment
         * information. See the testChangeParamsByZone test case for a
         * detailed explanation regarding the initialization of this map.
         */
        final TreeMap<String, SnDeployInfo> snDeployMap = new TreeMap<>();

        /* Entries corresponding to the pre-started SNAs. */
        snDeployMap.put("sn1", new SnDeployInfo("east-zone",
                                                portFinder1.getRegistryPort(),
                                                true));
        snDeployMap.put("sn2", new SnDeployInfo("west-zone",
                                                portFinder2.getRegistryPort(),
                                                true));
        snDeployMap.put("sn3", new SnDeployInfo("asia-zone",
                                                portFinder3.getRegistryPort(),
                                                true));
        /* Entries corresponding to the additional SNAs to deploy. */
        snDeployMap.put("sn4", new SnDeployInfo("west-zone", 0,
                       true));
        snDeployMap.put("sn5", new SnDeployInfo("west-zone", 0,
                       true));
        snDeployMap.put("sn6", new SnDeployInfo("asia-zone", 0,
                       true));

        /* Initialize the remaining required arguments. */
        final int capacity = 1;
        final String pool = "snpool";
        final String topoName = "testRemoveAdminByZoneName-topology";
        final String nPartitions = "20";

        /* Deploy the additional SNAs and save the results. */
        snaList = deployCluster(zoneRfMap, snDeployMap, capacity,
                                pool, topoName, nPartitions);

        /*
         * At this point the desired topology should be deployed to the
         * store. Use the deployed store's Admin to retrieve the id
         * corresponding to the zone name that is targeted by this test case.
         */
        final Admin admin = service.getAdmin();

        String targetName = "west-zone";

        /*
         * 1. Remove all Admins from the ZONE with the given zone NAME:
         *    - determine total # of Admins prior to removal
         *    - determine # of Admins in targeted zone prior to removal
         *    - remove the targeted Admins using the -znname flag
         *    - determine total # of Admins after removal
         *    - determine # of Admins remaining in the targeted zone
         *    - verify that all and only the targeted Admins are removed
         */
        int nTotalPre = 0;
        int nTargetPre = 0;
        for (AdminParams p :
                 admin.getCurrentParameters().getAdminParams()) {
            nTotalPre++;
            if (targetName.equals(snDeployMap.get(
                    p.getStorageNodeId().getFullName()).getZone())) {
                nTargetPre++;
            }
        }

        String s = runCLICommand(
                   "plan", "remove-admin", getZoneNameFlag(), targetName,
                   "-wait");

        int nTotalPost = 0;
        int nTargetPost = 0;

        for (AdminParams p : admin.getCurrentParameters().getAdminParams()) {

            nTotalPost++;
            if (targetName.equals(snDeployMap.get(
                    p.getStorageNodeId().getFullName()).getZone())) {
                nTargetPost++;
            }
        }

        /*
         * First verify that the number of targeted Admins is 0; that is,
         * verify that all of the Admins in the targeted zone have indeed
         * been removed.
         */
        assertTrue("Admin removal failed with CLI error msg:\n" + s,
                   nTargetPost == 0);

        /*
         * Finally, verify that only the targeted Admins have been removed;
         * that is, verify that the Admins in the non-targeted zones have
         * not been removed.
         */
        assertTrue("Admin removal failed with CLI error msg:\n" + s +
                   "\nAlthough all targeted Admins were removed, " +
                   "non-targeted Admins were also removed " +
                   "[# targeted Admins pre-removal = " + nTargetPre +
                   ", # targeted Admins post-removal = " + nTargetPost +
                   ", total # Admins pre-removal = " + nTotalPre +
                   ", total # Admins post-removal = " + nTotalPost  + "]",
                   nTotalPost == nTotalPre - nTargetPre);

        /*
         * 2. Remove all Admins from the ZONE with the given zone ID:
         *    - determine total # of Admins prior to removal
         *    - determine # of Admins in targeted zone prior to removal
         *    - remove the targeted Admins using both -znname & -force
         *    - determine total # of Admins after removal
         *    - determine # of Admins remaining in the targeted zone
         *    - verify that all and only the targeted Admins are removed
         */
        targetName = "asia-zone";
        final String targetId = getZoneId(admin, targetName);
        assertNotNull("testRemoveAdminByZoneId: null targetId ",
                      targetId);

        nTotalPre = 0;
        nTargetPre = 0;

        for (AdminParams p : admin.getCurrentParameters().getAdminParams()) {

            nTotalPre++;
            if (targetName.equals(snDeployMap.get(
                    p.getStorageNodeId().getFullName()).getZone())) {
                nTargetPre++;
            }
        }

        s = runCLICommand(
            "plan", "remove-admin", getZoneIdFlag(), targetId, "-force",
            "-wait");

        nTotalPost = 0;
        nTargetPost = 0;

        for (AdminParams p :
                 admin.getCurrentParameters().getAdminParams()) {
            nTotalPost++;
            if (targetName.equals(snDeployMap.get(
                    p.getStorageNodeId().getFullName()).getZone())) {
                nTargetPost++;
            }
        }

        /*
         * First verify that the number of targeted Admins is 0; that is,
         * verify that all of the Admins in the targeted zone have indeed
         * been removed.
         */
        assertTrue("Admin removal failed with CLI error msg:\n" + s,
                   nTargetPost == 0);

        /*
         * Finally, verify that only the targeted Admins have been removed;
         * that is, verify that the Admins in the non-targeted zones have
         * not been removed.
         */
        assertTrue("Admin removal failed with CLI error msg:\n" + s +
                   "\nAlthough all targeted Admins were removed, " +
                   "non-targeted Admins were also removed " +
                   "[# targeted Admins pre-removal = " + nTargetPre +
                   ", # targeted Admins post-removal = " + nTargetPost +
                   ", total # Admins pre-removal = " + nTotalPre +
                   ", total # Admins post-removal = " + nTotalPost  + "]",
                   nTotalPost == nTotalPre - nTargetPre);
    }

    /**
     * Test case intended to verify part of the new functionality added to the
     * 'plan remove-admin' command of the CLI. To address SR #22563, the 'plan
     * remove-admin' command was modified to allow one to request the removal
     * of all of the Admin(s) running in a given zone. This test case verifies
     * that the functionality added to the 'plan remove-admin' command can
     * handle the case where the specified zone contains the master Admin.
     *
     * For this test case, a store with the following configuration is used:
     * <ul>
     *   <li>3 zones with RF factors 3, 2, and 1 respectively
     *   <li>6 SNs
     *   <li>6 Admins
     *   <li>sn1, sn2, and sn3 each belong to the first zone
     *   <li>sn4 and sn5 belong to the second zone
     *   <li>sn6 belongs to the third zone
     *   <li>admin1 is the master and belongs to the first zone
     *   <li>admin2 & admin3 are replicas and also belong to the 1st zone
     *   <li>admin4 and admin5 are replicas and belong to the second zone
     *   <li>admin6 is a replica and belongs to the third zone
     * </ul>
     *
     * After deploying the store described above, an attempt is made to use the
     * 'plan remove-admin' command to remove all the Admins in the zone
     * containing the master Admin. Because an attempt will be made to remove
     * the master Admin, the RemoveAdminPlan class will shutdown and restart
     * that Admin so that mastership is transferred to one of the other Admin
     * replicas in the store, possibly in the same zone. When this
     * occurs, the Admin removal plan will be re-executed on the new master.
     *
     * Notes:
     *
     * For convenience, this test case uses only the -znname flag rather than
     * the -zn flag; because the use of both flags is already covered in the
     * testRemoveAdminByZoneNoMaster test case.
     *
     * There is an issue that this test case encountered that was not
     * encountered by the testRemoveAdminByZoneNoMaster test case. It seems to
     * be related to the fact that the parent class of this
     * test class employs the StorageNodeTestBase and AdminTestConfig to
     * provide the KVStore functionality. That is, the first issue seems to be
     * an issue with the test framework used, not a bug in product code. The
     * second issue may actually be a bug in product code (see SR22947 for the
     * details).
     *
     * The issue to be aware of is that, unlike
     * testRemoveAdminByZoneNoMaster, the Admin object retrieved from the
     * AdminService started via the parent class cannot be used to retrieve the
     * updated Admin info that is used for verification after the Admins are
     * removed from the zone. This is because for some reason (not yet fully
     * understood), the values returned by
     * admin.getCurrentParameters().getAdminParams are not updated with the
     * Admin information that was modified as a result of the removal. As a
     * result, another mechanism is used to verify that the desired Admins have
     * indeed been removed. That alternate mechanism consists of using the new
     * Admin master (via that Admin's registry port) to retrieve that Admin's
     * CommandService interface and then using that interface to retrieve the
     * AdminParams that were updated as a result of the Admin removal that was
     * performed.
     */
    @Test
    public void testRemoveAdminByZoneMaster() throws Exception {

        startAdminService();

        final AdminServiceParams params = service.getParams();
        assertNotNull("AdminService should not return null params",
                params);

        /* SETUP AND CREATE THE DESIRED TOPOLOGY */

        /*
         * Initialize the map of zone names and corresponding replication
         * factors. Add/remove entries and/or change the zone names and
         * replication factors to effect changes in the desired topology.
         */
        final Map<String, String> zoneRfMap = new HashMap<>();

        zoneRfMap.put("east-zone", "3"); /* Will contain Master Admin */
        zoneRfMap.put("west-zone", "2");
        zoneRfMap.put("asia-zone", "1");

        /*
         * Initialize the map of SN ids and corresponding deployment
         * information. See the testChangeParamsByZone test case for a
         * detailed explanation regarding the initialization of this map.
         */
        final TreeMap<String, SnDeployInfo> snDeployMap = new TreeMap<>();

        /* Entries corresponding to the pre-started SNAs. */
        snDeployMap.put("sn1", new SnDeployInfo("east-zone",
                                                portFinder1.getRegistryPort(),
                                                true));
        snDeployMap.put("sn2", new SnDeployInfo("east-zone",
                                                portFinder2.getRegistryPort(),
                                                true));
        snDeployMap.put("sn3", new SnDeployInfo("east-zone",
                                                portFinder3.getRegistryPort(),
                                                true));
        /* Entries corresponding to the additional SNAs to deploy. */
        snDeployMap.put("sn4", new SnDeployInfo("west-zone", 0,
                       true));
        snDeployMap.put("sn5", new SnDeployInfo("west-zone", 0,
                       true));
        snDeployMap.put("sn6", new SnDeployInfo("asia-zone", 0,
                       true));

        /* Initialize the remaining required arguments. */
        final int capacity = 1;
        final String pool = "snpool";
        final String topoName = "testRemoveAdminByZoneNameMaster-topology";
        final String nPartitions = "20";

        /* Deploy the additional SNAs and save the results. */
        snaList = deployCluster(zoneRfMap, snDeployMap, capacity,
                                pool, topoName, nPartitions);

        /*
         * At this point the desired topology should be deployed to the
         * store. Use the deployed store's Admin to retrieve the id
         * corresponding to the zone name that is targeted by this test case.
         */
        final Admin admin = service.getAdmin();

        final String targetName = "east-zone";

        /*
         * Remove all Admins from the ZONE with the given zone NAME:
         *  - determine total # of Admins prior to removal
         *  - determine # of Admins in targeted zone prior to removal
         *  - remove the targeted Admins using the -znname flag
         *  - every time the plan is interrupted when it attempts to
         *    remove the master Admin, wait for mastership to be
         *    transferred and then re-execute the plan
         *  - determine total # of Admins after removal
         *  - determine # of Admins remaining in the targeted zone
         *  - verify that all and only the targeted Admins are removed
         */
        int nTotalPre = 0;
        int nTargetPre = 0;
        for (AdminParams p :
                 admin.getCurrentParameters().getAdminParams()) {
            nTotalPre++;
            if (targetName.equals(snDeployMap.get(
                    p.getStorageNodeId().getFullName()).getZone())) {
                nTargetPre++;
            }
        }

        final String s = runCLICommand("plan", "remove-admin",
                                       getZoneNameFlag(), targetName, "-wait");

        /* Lookup the new master Admin */
        final CommandServiceAPI cs = RegistryUtils.getAdmin(
            "localhost", Integer.parseInt(adminPort), loginMgr,
                logger);

        /* Verify the results */
        int nTotalPost = 0;
        int nTargetPost = 0;

        /* Determine, post removal, the the total number of Admins remaining in
         * the store ,as well as the number of Admins remaining in the targeted
         * zone (which should be 0).
         */
        for (ParameterMap pMap : cs.getAdmins()) {
            nTotalPost++;

            final String snid =
                new StorageNodeId(pMap.getOrZeroInt(
                    ParameterState.COMMON_SN_ID)).getFullName();
            if (targetName.equals(snDeployMap.get(snid).getZone())) {
                nTargetPost++;
            }
        }

        /*
         * First verify that the number of targeted Admins is 0; that is,
         * verify that all of the Admins in the targeted zone have indeed
         * been removed.
         */
        assertTrue("Admin removal failed with CLI error msg:\n" + s,
                   nTargetPost == 0);

        /*
         * Finally, verify that only the targeted Admins have been removed;
         * that is, verify that the Admins in the non-targeted zones have
         * not been removed.
         */
        assertTrue("Admin removal failed with CLI error msg:\n" + s +
                   "\nAlthough all targeted Admins were removed, " +
                   "non-targeted Admins were also removed " +
                   "[# targeted Admins pre-removal = " + nTargetPre +
                   ", # targeted Admins post-removal = " + nTargetPost +
                   ", total # Admins pre-removal = " + nTotalPre +
                   ", total # Admins post-removal = " + nTotalPost  + "]",
                   nTotalPost == nTotalPre - nTargetPre);
    }

    @Test
    public void testShowPlansInvalidArgValues()
        throws Exception {

        assertCLICommandResult("show plans -id invalid-plan-id",
                               "Invalid argument: " +
                                       "invalid-plan-id");
        assertCLICommandResult("show plans -num not-a-number",
                               "Invalid argument: " +
                                       "not-a-number");
        assertCLICommandResult("show plans -id invalid-plan-id -num 3",
                               "Invalid argument: " +
                                       "invalid-plan-id");
        assertCLICommandResult("show plans -from invalid-date",
                               "Invalid date format: " +
                                       "invalid-date");
        assertCLICommandResult("show plans -to invalid-date",
                               "Invalid date format: " +
                                       "invalid-date");
    }

    @Test
    public void testShowZonesInvalidArgValues()
        throws Exception {

        assertCLICommandResult("show zones -zn invalid-zone",
                               "Invalid zone ID: invalid-zone");
    }

    @Test
    public void testTableCommandOnSystemTable()
        throws Exception {

        final String testTable = TableStatsPartitionDesc.TABLE_NAME;
        final String testField = "shardId";

        /* Start a store so system tables are created */
        startAdminService();
        final String dcname = deployDatacenter();
        final int sn1id =
            deployStorageNode(dcname, portFinder1.getRegistryPort());
        deployAdmin(sn1id);
        deployStorageNode(dcname, portFinder2.getRegistryPort());
        addpool();
        deployKVStore();

        waitForTable(getStore().getTableAPI(), testTable);

        /* Test create and remove a system table */
        assertCLICommandResult("table create -name " + testTable,
                               "may contain only " +
                               "alphanumeric values plus");
        assertCLICommandResult("plan remove-table -name " + testTable,
                               "Cannot remove system table");

        /* Test add index on system table */
        assertCLICommandResult("plan add-index -name idx1 -table " +
                        testTable + " -field " + testField + " -wait",
                               "Cannot add index idx1 on " +
                                "system table");
    }

    /**
     * Convenience method that starts additional SNAs, deploys an SN for
     * each SNA -- both pre-started by the parent class and started by
     * this method, and creates and deploys a topology based on the
     * information contained in the arguments specified to this method.
     *
     * Note that the first entry in snDeployMap argument MUST have its
     * adminFlag set to true; otherwise, the deployStorageNode method will
     * throw an AssertionFailedError because of the assumptions that method
     * makes about how many plans it expects to be executed.
     *
     * Note also that key order in the snDeployMap argument is important;
     * which is why this method requires a TreeMap for the snDeployMap
     * parameter. Requiring an ordered map allows the caller to guarantee
     * that the adminFlag of the first entry is set to true, so that an
     * admin is always deployed with at least the first deployed SN.
     */
    private List<StorageNodeAgent> deployCluster(
                            final Map<String, String> zoneRfMap,
                            final TreeMap<String, SnDeployInfo> snDeployMap,
                            final int capacity,
                            final String pool,
                            final String topoName,
                            final String nPartitions)
        throws Exception {

        /* Parent class pre-starts 3 SNAs, so create nSns-3 additional SNAs. */
        final int nExistingSns = 3;

        /*
         * Parent class uses startPorts 13230, 13250, & 13270 for the 3 intial
         * SNAs it starts. Also, portFinder4 uses a startPort of 13290. So
         * additional SNAs should have startPorts of 13290+20, 13290+40,
         * 13290+60, etc.
         */
        final int startPortDelta = 20;
        final int startPortBase = startPort4 + startPortDelta;

        final int nSns = snDeployMap.size();

        /*
         * Return the additional SNAs that are started so they can be shut down
         * at the end of the test. Note that the 3 initial SNAs started by
         * the parent class will be shut down by the parent's tearDown method.
         */
        final List<StorageNodeAgent> retList = new ArrayList<>();

        /* Create the additional SNAs. */
        for (int i = 0; i < nSns - nExistingSns; i++) {
            final int id = nExistingSns + i + 1;
            final String snid = "sn" + id;
            final SnDeployInfo info = snDeployMap.get(snid);

            final PortFinder portFinder =
               new PortFinder(startPortBase + i * startPortDelta,
                       haRange);

            /* Save port for SN deployment below. */
            snDeployMap.put(
                snid, info.replacePort(portFinder.getRegistryPort()));

            retList.add(createUnregisteredSna(
                            id, capacity, portFinder, false));
        }

        /*
         * Deploy the zones to which the SNs will be deployed, and create the
         * pool that all SNs will join.
         */
        for (Map.Entry<String, String> zoneRfEntry : zoneRfMap.entrySet()) {
            deployZone(zoneRfEntry.getKey(), zoneRfEntry.getValue());
        }
        addPool(pool);

        /*
         * Deploy each sn to the desired zone and join the pool. Deploy an
         * admin on the first N sn's, where N = nAdmin.
         */
        for (Map.Entry<String, SnDeployInfo> entry : snDeployMap.entrySet()) {
            final SnDeployInfo info = entry.getValue();
            final int snid = deployStorageNode(info.getZone(), info.getPort());
            joinPool(pool, snid);
            if (info.getAdminFlag()) {
                deployAdmin(snid);
            }
        }

        /* Deploy a topology consisting of the SNs deployed above. */
        createDeployTopology(topoName, pool, nPartitions, nSns);

        return retList;
    }

    /**
     * Convenience method that creates and starts an SNA.
     */
    private StorageNodeAgent createUnregisteredSna(final int snid,
                                                   final int capacity,
                                                   final PortFinder portFinder,
                                                   final boolean createAdmin)
        throws Exception {

        /*
         * Start the new SNA with a storage directory constructed from the
         * given id.
         */
        final AdminServiceParams params = atc.getParams();
        assertNotNull("AdminTestConfig should not return null params",
                params);

        final String storeName = params.getGlobalParams().getKVStoreName();
        final String fSep = System.getProperty("file.separator");
        final String rootDir = TestUtils.getTestDir().toString();
        final List<String> storageDirList = new ArrayList<>();
        storageDirList.add(rootDir + fSep + storeName + fSep +
                              (new StorageNodeId(snid)).getFullName());
        final String config = "config" + snid + ".xml";

        return StorageNodeTestBase.createUnregisteredSNA
                   (rootDir, portFinder, capacity, config,
                    false, createAdmin, MB_PER_SN, storageDirList);
    }

    /**
     * Convenience method that returns the zone id corresponding to the given
     * name of a previously deployed zone.
     */
    private String getZoneId(Admin admin, String zoneName) {
        if (admin == null || zoneName == null) {
            return null;
        }

        for (DatacenterParams p :
                 admin.getCurrentParameters().getDatacenterMap().values()) {
            if (zoneName.equals(p.getName())) {
                return p.getDatacenterId().toString();
            }
        }
        return null;
    }

    /*
     * Convenience data structure used when deploying SNs.
     */
    private static final class SnDeployInfo {
        private final String zone;
        private final int port;
        private final boolean adminFlag;

        SnDeployInfo(String zone, int port, boolean adminFlag) {
            this.zone = zone;
            this.port = port;
            this.adminFlag = adminFlag;
        }

        String getZone() {
            return zone;
        }

        int getPort() {
            return port;
        }

        boolean getAdminFlag() {
            return adminFlag;
        }

        SnDeployInfo replacePort(int newPort) {
            return new SnDeployInfo(zone, newPort, adminFlag);
        }

        @Override
        public String toString() {
            return "[zone=" + zone + ", port=" + port +
                   ", adminFlag=" + adminFlag + "]";
        }
    }

    /**
     * Return the -znname or -dcname flag, to test compatibility for both
     * values.
     */
    String getZoneNameFlag() {
        return random.nextBoolean() ? "-znname" : "-dcname";
    }

    /** Return the -zn or -dc flag, to test compatibility for both values. */
    String getZoneIdFlag() {
        return random.nextBoolean() ? "-zn" : "-dc";
    }

    /**
     * Return 'plan deploy-zone' or 'plan deploy-datacenter', to test
     * compatibility for both values.
     */
    String getPlanDeployZone() {
        return random.nextBoolean() ?
            "plan deploy-zone" :
            "plan deploy-datacenter";
    }

    /** Return zone or datacenter, to test compatibility for both values. */
    String getZoneTerm() {
        return random.nextBoolean() ? "zone" : "datacenter";
    }

    /**
     * Return remove-zone or remove-datacenter, to test compatibility for both
     * values.
     */

    String getRemoveZone() {
        return random.nextBoolean() ? "remove-zone" : "remove-datacenter";
    }

    private String trimEnd(String source) {
        int pos = source.length() - 1;
        while ((pos >= 0) && Character.isWhitespace(source.charAt(pos))) {
            pos--;
        }
        pos++;
        return (pos < source.length()) ? source.substring(0, pos) : source;
    }

    /**
     * This method splits a string based on new lines taking into
     * account windows and linux differences.
     */
    String [] smartSplit(String line) {
        line = trimEnd(line);
        String windows = "\r\n";
        String separator = "\n";
        if (line.contains(windows)){
            separator = windows;
        }
        return line.split(separator);


    }

    @Test
    public void testAllowDeployDCWhenSNDown()
        throws Exception {

        startAdminService();

        String dcname = deployDatacenter();

        /* Deploy a StorageNode */
        int sn1id = deployStorageNode(dcname, portFinder1.getRegistryPort());

        /* Deploy an Admin */
        deployAdmin(sn1id);

        /* Deploy another couple of StorageNodes */
        int sn2id = deployStorageNode(dcname, portFinder2.getRegistryPort());
        int sn3id = deployStorageNode(dcname, portFinder3.getRegistryPort());

        /* deploy another couple of Admins. */
        deployAdmin(sn2id);
        deployAdmin(sn3id);

        /* Add a new StorageNodePool */
        addpool();

        clonepool();

        /* Deploy some RepNodes */
        deployKVStore();

        /* Exercise "show plans" */
        showPlans();

        sna3.shutdown(true, true);

        assertPlanSuccess("plan deploy-zone -name newZone -rf 1 " +
                "-wait");
    }

    /**
     * Tests if the start service plan shows an explicit message when timed
     * out.
     *
     * [#26663]
     */
    @Test
    public void testStartNodeTimeoutMessage() throws Exception {
        startAdminService();
        String dcname = deployDatacenter();
        /* Deploy a StorageNode */
        int sn1id = deployStorageNode(dcname, portFinder1.getRegistryPort());
        /* Deploy an Admin */
        deployAdmin(sn1id);
        /* Deploy another couple of StorageNodes */
        int sn2id = deployStorageNode(dcname, portFinder2.getRegistryPort());
        deployAdmin(sn2id);
        /* Add a new StorageNodePool */
        addpool();
        /* Deploy some RepNodes */
        deployKVStore();

        assertPlanSuccess("plan stop-service -wait -service rg1-rn1");
        /*
         * Changes the wait timeout parameter to zero seconds so that the start
         * service plan will fail with timeout.
         */
        runCLICommand(new String[] {"plan",
                      "change-param",
                      "-all-admins",
                      "-hidden",
                      "-wait",
                      "-params",
                      "waitTimeout=0 s"});
        /* Runs the start service plan and it should fail */
        runCLICommand("plan", "start-service", "-wait", "-service",
                "rg1-rn1");
        /*
         * Obtains the show plan result, it should contain an explicit time out
         * message.
         */
        assertCLICommandResult("show plan -last",
                "Timed out after 0 SECONDS");
    }

    /**
     * Test that the haHostname and haPortRange parameters are read-only.
     *
     * [KVSTORE-437]
     */
    @Test
    public void testHaParamsReadOnly() throws Exception {
        startAdminService();
        final String dcname = deployDatacenter();
        final int sn1id =
            deployStorageNode(dcname, portFinder1.getRegistryPort());
        deployAdmin(sn1id);
        deployStorageNode(dcname, portFinder2.getRegistryPort());
        deployStorageNode(dcname, portFinder3.getRegistryPort());
        addpool();
        deployKVStore();

        assertCLICommandResult("plan change-param -service sn1 -wait" +
                               " -params haHostname=newhostname",
                               "Parameter is read-only: " +
                                       "haHostname");
        assertCLICommandResult("plan change-param -service sn1 -wait" +
                               " -params haPortRange=5000,5010",
                               "Parameter is read-only: " +
                                       "haPortRange");
    }

    /**
     * tests whether storageDir is displayed during "show admins" in cli
     */
    @Test
    public void testShowAdminsCommandDisplayStorageDir() throws Exception {
        startAdminService();
        final String dcname = deployDatacenter();
        final int sn1id =
            deployStorageNode(dcname, portFinder1.getRegistryPort());
        assertPlanSuccess("plan deploy-admin -sn " + sn1id + " -wait");
        String s = runCLICommand("show", "admins");
        assertTrue(s.contains("storageDir"));
        for (String s1: s.split(" ")) {
            if (s1.contains("storageDir")) {
                String[] pathKey = s1.split("=");
                assertTrue(pathKey.length == 2);
                File dir = new File(pathKey[1]);
                assertTrue(dir.isDirectory());
            }
        }
    }

    /**
     * tests whether available storage size for the admin is displayed 
     * during "ping" in cli
     */
    @Test
    public void testPingDisplayAdminStorageSize()
        throws Exception {

        startAdminService();

        String dcname = deployDatacenter();

        /* Deploy a StorageNode */
        int sn1id = deployStorageNode(dcname, portFinder1.getRegistryPort());

        /* Deploy an Admin */
        deployAdmin(sn1id);

        /* Deploy another StorageNode */
        int sn2id = deployStorageNode(dcname, portFinder2.getRegistryPort());

        deployAdmin(sn2id);

        addpool();

        /* Deploy some RepNodes */
        deployKVStore();
        String output= runCLICommand("ping","-json");
        final JsonNode jsonV2 = JsonUtils.parseJsonNode(output);
        final ObjectNode json = getObject(jsonV2, "returnValue");
        final Iterable<JsonNode> jsonSNs = getArray(json, "snStatus");
        int snCount = 0; 
        for (final JsonNode jsonSN : jsonSNs) {
            final ObjectNode jsonAdmin = getObject(jsonSN, "adminStatus");
            assertTrue(getAsText(jsonAdmin,
                    "availableStorageSize") != null);
            snCount++;
        }
        assertTrue(snCount == 2);
    }

    /**
     * Tests whether the disk limit violation is displayed when configuring the 
     * store with less admin dir size
     */
    @Test
    public void testConfigureStoreAdminDiskLimitViolation() throws Exception {
        sna1.shutdown(true, true);
        /*
         * Leave some time between shutdown and restart, otherwise startup can
         * fail because the server socket is still open
         */
        Thread.sleep(1000);
        final String configFile =
            TestUtils.getTestDir() + File.separator + "config4.xml";
        final String adminDir = 
            TestUtils.getTestDir() + File.separator + "adminRoot";
        File file = new File(adminDir);
        file.mkdir();
        final String adminDirSize = "1_kb";
        try {
            TestUtils.generateBootstrapFile(
                configFile,
                sna1.getBootstrapDir(),
                sna1.getHostname(),
                sna1.getRegistryPort(),
                sna1.getHAPortRange(),
                sna1.getHAHostname(),
                false,
                sna1.getCapacity(),
                sna1.getBootstrapParams().getStorageDirPaths(),
                adminDir,
                adminDirSize);
            sna1 = StorageNodeTestBase.startSNA(sna1.getBootstrapDir(),
                                                "config4.xml",
                                                false,
                    true);

            String s = runCLICommand("configure", "-name",
                    "kvAdminStore");

            assertTrue(s.contains("Disk limit violated in Admin environment" +
                                  " directory"));
        } finally {
            FileUtils.deleteDirectory(file);
            file = new File(TestUtils.getTestDir() + 
                            File.separator + "kvAdminStore");
            FileUtils.deleteDirectory(file);
            file = new File(configFile);
            file.delete();
        }
    }

    /**
     * Test plan state transitions when canceling, interrupting, and executing
     * a plan. Added this test to check the text of the exception message when
     * canceling a running plan [KVSTORE-874].
     *
     * Note that the APPROVED to RUNNING transition is not expected to block,
     * so there is no easy way to check transitions for the APPROVED state.
     */
    @Test
    public void testPlanTransitions() throws Exception {

        /* Deploy store */
        startAdminService();
        String dcname = deployDatacenter();
        int sn1id = deployStorageNode(dcname, portFinder1.getRegistryPort());
        deployAdmin(sn1id);
        int sn2id = deployStorageNode(dcname, portFinder2.getRegistryPort());
        int sn3id = deployStorageNode(dcname, portFinder3.getRegistryPort());
        deployAdmin(sn2id);
        deployAdmin(sn3id);
        addpool();
        deployKVStore();

        /* Create a plan and don't execute it */
        JsonNode json = JsonUtils.parseJsonNode(
            runCLICommand(
                "plan", "change-parameters", "-json", "-noexecute",
                "-service", "rg1-rn1",
                "-params", "rnStatisticsLeaseDuration=10000000 MICROSECONDS"));
        assertEquals(5000, json.get("returnCode").asInt());
        String planId = getAsText(json.get("returnValue"), "planId");

        /* Interrupt PENDING -> CANCELED */
        json = JsonUtils.parseJsonNode(
            runCLICommand("plan", "interrupt", "-json", "-id", planId));
        assertEquals(5000, json.get("returnCode").asInt());

        json = JsonUtils.parseJsonNode(
            runCLICommand("show", "plan", "-json", "-id", planId));
        assertEquals(5000, json.get("returnCode").asInt());
        assertEquals("CANCELED", getAsText(json.get("returnValue"),
                "state"));

        /* Interrupt CANCELED -> error */
        json = JsonUtils.parseJsonNode(
            runCLICommand("plan", "interrupt", "-json", "-id", planId));
        assertEquals(5100, json.get("returnCode").asInt());

        /* Execute CANCELED -> error */
        json = JsonUtils.parseJsonNode(
            runCLICommand("plan", "execute", "-json", "-id", planId));
        assertEquals(5100, json.get("returnCode").asInt());

        /* Create another plan and don't execute it */
        json = JsonUtils.parseJsonNode(
            runCLICommand(
                "plan", "change-parameters", "-json", "-noexecute",
                "-service", "rg1-rn1",
                "-params", "rnStatisticsLeaseDuration=20000000 MICROSECONDS"));
        assertEquals(5000, json.get("returnCode").asInt());
        planId = getAsText(json.get("returnValue"), "planId");

        /* Cancel PENDING -> CANCELED */
        json = JsonUtils.parseJsonNode(
            runCLICommand("plan", "cancel", "-json", "-id", planId));
        assertEquals(5000, json.get("returnCode").asInt());

        json = JsonUtils.parseJsonNode(
            runCLICommand("show", "plan", "-json", "-id", planId));
        assertEquals(5000, json.get("returnCode").asInt());
        assertEquals("CANCELED", getAsText(json.get("returnValue"),
                "state"));

        /* Cancel CANCELED -> error */
        json = JsonUtils.parseJsonNode(
            runCLICommand("plan", "cancel", "-json", "-id", planId));
        assertEquals(5100, json.get("returnCode").asInt());

        /* Execute PENDING -> SUCCEEDED */
        json = JsonUtils.parseJsonNode(
            runCLICommand(
                "plan", "change-parameters", "-noexecute", "-json",
                "-service", "rg1-rn1",
                "-params", "rnStatisticsLeaseDuration=30000000 MICROSECONDS"));
        assertEquals(5000, json.get("returnCode").asInt());
        planId = getAsText(json.get("returnValue"), "planId");

        json = JsonUtils.parseJsonNode(
            runCLICommand("plan", "execute", "-wait", "-json", "-id",
                    planId));
        assertEquals(5000, json.get("returnCode").asInt());

        json = JsonUtils.parseJsonNode(
            runCLICommand("show", "plan", "-json", "-id", planId));
        assertEquals(5000, json.get("returnCode").asInt());
        assertEquals("SUCCEEDED", getAsText(json.get("returnValue"),
                "state"));

        /* Interrupt SUCCEEDED -> error */
        json = JsonUtils.parseJsonNode(
            runCLICommand("plan", "interrupt", "-json", "-id", planId));
        assertEquals(5100, json.get("returnCode").asInt());

        /* Cancel SUCCEEDED -> error */
        json = JsonUtils.parseJsonNode(
            runCLICommand("plan", "cancel", "-json", "-id", planId));
        assertEquals(5100, json.get("returnCode").asInt());

        /* Execute SUCCEEDED -> error */
        json = JsonUtils.parseJsonNode(
            runCLICommand("plan", "execute", "-json", "-id", planId));
        assertEquals(5100, json.get("returnCode").asInt());

        /* Create a plan that fails so we can test transitions from ERROR */
        PlanExecutor.FAULT_HOOK =
            x -> { throw new RuntimeException("Make plan fail"); };

        json = JsonUtils.parseJsonNode(
            runCLICommand(
                "plan", "change-parameters", "-noexecute", "-json",
                "-service", "rg1-rn1",
                "-params", "rnStatisticsLeaseDuration=40000000 MICROSECONDS"));
        assertEquals(5000, json.get("returnCode").asInt());
        planId = getAsText(json.get("returnValue"), "planId");

        /* Execute fails -> ERROR */
        json = JsonUtils.parseJsonNode(
            runCLICommand("plan", "execute", "-wait", "-json", "-id",
                    planId));
        assertEquals(5500, json.get("returnCode").asInt());

        json = JsonUtils.parseJsonNode(
            runCLICommand("show", "plan", "-json", "-id", planId));
        assertEquals(5000, json.get("returnCode").asInt());
        assertEquals("ERROR", getAsText(json.get("returnValue"),
                "state"));

        PlanExecutor.FAULT_HOOK = null;

        /* Interrupt ERROR -> error */
        json = JsonUtils.parseJsonNode(
            runCLICommand("plan", "interrupt", "-json", "-id", planId));
        assertEquals(5100, json.get("returnCode").asInt());

        /* Cancel ERROR -> succeed */
        json = JsonUtils.parseJsonNode(
            runCLICommand("plan", "cancel", "-json", "-id", planId));
        assertEquals(5000, json.get("returnCode").asInt());

        json = JsonUtils.parseJsonNode(
            runCLICommand("show", "plan", "-json", "-id", planId));
        assertEquals(5000, json.get("returnCode").asInt());
        assertEquals("CANCELED", getAsText(json.get("returnValue"),
                "state"));

        PlanExecutor.FAULT_HOOK =
            x -> { throw new RuntimeException("Make plan fail"); };

        json = JsonUtils.parseJsonNode(
            runCLICommand(
                "plan", "change-parameters", "-noexecute", "-json",
                "-service", "rg1-rn1",
                "-params", "rnStatisticsLeaseDuration=50000000 MICROSECONDS"));
        assertEquals(5000, json.get("returnCode").asInt());
        planId = getAsText(json.get("returnValue"), "planId");

        json = JsonUtils.parseJsonNode(
            runCLICommand("plan", "execute", "-wait", "-json", "-id",
                    planId));
        assertEquals(5500, json.get("returnCode").asInt());

        json = JsonUtils.parseJsonNode(
            runCLICommand("show", "plan", "-json", "-id", planId));
        assertEquals(5000, json.get("returnCode").asInt());
        assertEquals("ERROR", getAsText(json.get("returnValue"),
                "" +
                "state"));

        PlanExecutor.FAULT_HOOK = null;

        /* Execute ERROR -> SUCCEEDED */
        json = JsonUtils.parseJsonNode(
            runCLICommand("plan", "execute", "-wait", "-json", "-id",
                    planId));
        assertEquals(5000, json.get("returnCode").asInt());

        json = JsonUtils.parseJsonNode(
            runCLICommand("show", "plan", "-json", "-id", planId));
        assertEquals(5000, json.get("returnCode").asInt());
        assertEquals("SUCCEEDED", getAsText(json.get("returnValue"),
                "state"));

        /*
         * Create a plan that hangs so we can test transitions from RUNNING and
         * INTERRUPTED
         */
        final Semaphore done = new Semaphore(0);
        PlanExecutor.FAULT_HOOK = x -> {
            try {
                done.tryAcquire(10, MINUTES);
            } catch (InterruptedException e) {
            }
        };

        json = JsonUtils.parseJsonNode(
            runCLICommand(
                "plan", "change-parameters", "-noexecute", "-json",
                "-service", "rg1-rn1",
                "-params", "rnStatisticsLeaseDuration=60000000 MICROSECONDS"));
        assertEquals(5000, json.get("returnCode").asInt());
        planId = getAsText(json.get("returnValue"), "planId");

        json = JsonUtils.parseJsonNode(
            runCLICommand("plan", "execute", "-json", "-id", planId));
        assertEquals(json.toString(), 5000,
                json.get("returnCode").asInt());

        {
            final String planIdFinal = planId;
            assertTrue(new PollCondition(1000, 30000) {
                @Override
                protected boolean condition() {
                    try {
                        final JsonNode jn =
                            JsonUtils.parseJsonNode(
                                runCLICommand("show", "plan", "-json",
                                        "-id", planIdFinal));
                        assertEquals(5000,
                                jn.get("returnCode").asInt());
                        return "RUNNING".equals(
                            getAsText(jn.get("returnValue"), "state"));
                    } catch (Exception e) {
                        throw new RuntimeException(
                            "Unexpected exception: " + e, e);
                    }
                }
            }.await());
        }

        /* Cancel RUNNING -> error */
        json = JsonUtils.parseJsonNode(
            runCLICommand("plan", "can", "-json", "-id", planId));
        assertEquals(5200, json.get("returnCode").asInt());
        /* Check error message [KVSTORE-874] */
        String description = getAsText(json, "description");
        assertTrue(description,
                   description.contains(
                       "plan must be interrupted before it can be canceled"));

        /* Execute RUNNING -> error */
        json = JsonUtils.parseJsonNode(
            runCLICommand("plan", "execute", "-json", "-id", planId));
        assertEquals(5200, json.get("returnCode").asInt());

        /*
         * Interrupt RUNNING -> INTERRUPT_REQUESTED (although might quickly
         * move to INTERRUPTED)
         */
        json = JsonUtils.parseJsonNode(
            runCLICommand("plan", "interrupt", "-json", "-id", planId));
        assertEquals(5000, json.get("returnCode").asInt());

        json = JsonUtils.parseJsonNode(
            runCLICommand("show", "plan", "-json", "-id", planId));
        assertEquals(5000, json.get("returnCode").asInt());
        String state = getAsText(json.get("returnValue"), "state");
        assertTrue("Expected INTERRUPT_REQUESTED or INTERRUPTED, found " +
                   state,
                   "INTERRUPT_REQUESTED".equals(state) ||
                   "INTERRUPTED".equals(state));

        /*
         * Clear the execute hook, and wait for state to change from
         * INTERRUPT_REQUESTED to INTERRUPTED
         */
        PlanExecutor.FAULT_HOOK = null;
        done.release(1);
        {
            final String planIdFinal = planId;
            assertTrue(new PollCondition(1000, 30000) {
                @Override
                protected boolean condition() {
                    try {
                        final JsonNode jn =
                            JsonUtils.parseJsonNode(
                                runCLICommand(
                                    "show", "plan", "-json", "-id",
                                    planIdFinal));
                        assertEquals(5000,
                                jn.get("returnCode").asInt());
                        return "INTERRUPTED".equals(
                            getAsText(jn.get("returnValue"), "state"));
                    } catch (Exception e) {
                        throw new RuntimeException(
                            "Unexpected exception: " + e, e);
                    }
                }
            }.await());
        }

        /* Interrupt INTERRUPTED => error */
        json = JsonUtils.parseJsonNode(
            runCLICommand("plan", "interrupt", "-json", "-id", planId));
        assertEquals(5100, json.get("returnCode").asInt());

        /* Cancel INTERRUPTED => CANCELED */
        json = JsonUtils.parseJsonNode(
            runCLICommand("plan", "cancel", "-json", "-id", planId));
        assertEquals(5000, json.get("returnCode").asInt());

        json = JsonUtils.parseJsonNode(
            runCLICommand("show", "plan", "-json", "-id", planId));
        assertEquals(5000, json.get("returnCode").asInt());
        assertEquals("CANCELED", getAsText(json.get("returnValue"),
                "state"));

        /* Create another INTERRUPTED plan so we can try executing it */
        PlanExecutor.FAULT_HOOK = x -> {
            try {
                done.tryAcquire(10, MINUTES);
            } catch (InterruptedException e) {
            }
        };

        json = JsonUtils.parseJsonNode(
            runCLICommand(
                "plan", "change-parameters", "-noexecute", "-json",
                "-service", "rg1-rn1",
                "-params", "rnStatisticsLeaseDuration=70000000 MICROSECONDS"));
        assertEquals(5000, json.get("returnCode").asInt());
        planId = getAsText(json.get("returnValue"), "planId");

        json = JsonUtils.parseJsonNode(
            runCLICommand("plan", "execute", "-json", "-id", planId));
        assertEquals(5000, json.get("returnCode").asInt());

        json = JsonUtils.parseJsonNode(
            runCLICommand("show", "plan", "-json", "-id", planId));
        assertEquals(5000, json.get("returnCode").asInt());
        assertEquals("RUNNING", getAsText(json.get("returnValue"),
                "state"));

        /*
         * Interrupt RUNNING -> INTERRUPT_REQUESTED (although might quickly
         * move to INTERRUPTED)
         */
        json = JsonUtils.parseJsonNode(
            runCLICommand("plan", "interrupt", "-json", "-id", planId));
        assertEquals(5000, json.get("returnCode").asInt());

        json = JsonUtils.parseJsonNode(
            runCLICommand("show", "plan", "-json", "-id", planId));
        assertEquals(5000, json.get("returnCode").asInt());
        state = getAsText(json.get("returnValue"), "state");
        assertTrue("Expected INTERRUPT_REQUESTED or INTERRUPTED, found " +
                   state,
                   "INTERRUPT_REQUESTED".equals(state) ||
                   "INTERRUPTED".equals(state));

        /*
         * Clear the execute hook, and wait for state to change from
         * INTERRUPT_REQUESTED to INTERRUPTED
         */
        PlanExecutor.FAULT_HOOK = null;
        done.release(1);
        {
            final String planIdFinal = planId;
            assertTrue(new PollCondition(1000, 30000) {
                @Override
                protected boolean condition() {
                    try {
                        final JsonNode jn =
                            JsonUtils.parseJsonNode(
                                runCLICommand(
                                    "show", "plan", "-json", "-id",
                                    planIdFinal));
                        assertEquals(5000,
                                jn.get("returnCode").asInt());
                        return "INTERRUPTED".equals(
                            getAsText(jn.get("returnValue"), "state"));
                    } catch (Exception e) {
                        throw new RuntimeException(
                            "Unexpected exception: " + e, e);
                    }
                }
            }.await());
        }

        /* Execute INTERRUPTED -> SUCCEEDED */
        json = JsonUtils.parseJsonNode(
            runCLICommand("plan", "execute", "-json", "-wait", "-id",
                    planId));
        assertEquals(5000, json.get("returnCode").asInt());

        json = JsonUtils.parseJsonNode(
            runCLICommand("show", "plan", "-json", "-id", planId));
        assertEquals(5000, json.get("returnCode").asInt());
        assertEquals("SUCCEEDED", getAsText(json.get("returnValue"),
                "state"));
    }

    @Test
    public void testChangeRepFactor() throws Exception {

        /* Deploy store */
        startAdminService();
        final String dcname = deployDatacenter();
        changePolicy();
        int sn1id = deployStorageNode(dcname, portFinder1.getRegistryPort());
        deployAdmin(sn1id);
        int sn2id = deployStorageNode(dcname, portFinder2.getRegistryPort());
        deployAdmin(sn2id);
        addpool();
        deployKVStore();

        /* Deploy another zone, SN, and admin */
        final String dcname2 = "Orlando";
        assertPlanSuccess(
            getPlanDeployZone() + " -name " + dcname2 + " -rf 1 -wait");
        int sn3id = deployStorageNode(dcname2, portFinder3.getRegistryPort());
        deployAdmin(sn3id);
        addpool("sn3");
        assertCLICommandResult("topology clone -current -name topo1",
                               "Created topo1");
        assertCLICommandResult("topology rebalance -name topo1 -pool " +
                               MYPOOLNAME,
                               "Rebalanced");
        assertPlanSuccess("plan deploy-topo -name topo1 -wait");

        /* Attempt to reduce primary zone RF */
        assertCLICommandResult("topology clone -current -name topo2",
                               "Created topo2");
        assertCLICommandResult(
            "Reduce primary zone RF fails",
            "topology change-repfactor -name topo2 -zn 1 -rf 1" +
            " -pool " + MYPOOLNAME,
            "The replication factor of a primary zone cannot be made" +
            " smaller.");

        /*
         * Attempt to reduce primary zone by converting to secondary and back
         */
        assertCLICommandResult(
            "topology change-zone-type -name topo2 -zn 1 -type secondary",
            "Changed type of zone");
        assertCLICommandResult(
            "topology change-repfactor -debug -name topo2 -zn 1 -rf 1" +
            " -pool " + MYPOOLNAME,
            "Changed replication factor");
        assertCLICommandResult(
            "topology change-zone-type -name topo2 -zn 1 -type primary",
            "Changed type of zone");
        assertCLICommandResult(
            "plan deploy-topology -name topo2 -wait",
            "Attempt to reduce the replication factor of primary zone zn1");

        /* Attempt to reduce the overall primary replication factor */
        assertCLICommandResult("topology clone -current -name topo3",
                               "Created topo3");
        assertCLICommandResult(
            "topology change-zone-type -name topo3 -zn 1 -type secondary",
            "Changed type of zone");
        assertCLICommandResult(
            "plan deploy-topology -name topo3 -wait",
            "Attempt to reduce the overall primary replication factor by" +
            " 2 from 3 to 1.");
    }

    @Test
    public void testChangeZoneType() throws Exception {

        /* Deploy store */
        startAdminService();
        final String dcname = deployDatacenter();
        changePolicy();
        int sn1id = deployStorageNode(dcname, portFinder1.getRegistryPort());
        deployAdmin(sn1id);
        int sn2id = deployStorageNode(dcname, portFinder2.getRegistryPort());
        deployAdmin(sn2id);
        addpool();
        deployKVStore();

        /* Deploy another zone, SN, and admin */
        final String dcname2 = "Orlando";
        assertPlanSuccess(
            getPlanDeployZone() + " -name " + dcname2 + " -rf 1 -wait");
        int sn3id = deployStorageNode(dcname2, portFinder3.getRegistryPort());
        deployAdmin(sn3id);
        addpool("sn3");
        assertCLICommandResult("topology clone -current -name topo1",
                               "Created topo1");
        assertCLICommandResult("topology rebalance -name topo1 -pool " +
                               MYPOOLNAME,
                               "Rebalanced");
        assertPlanSuccess("plan deploy-topo -name topo1 -wait");

        /*
         * Changing primary to secondary removes allowing arbiters and master
         * affinity
         */
        assertCLICommandResult("topology clone -current -name topo2",
                               "Created topo2");
        assertCLICommandResult(
            "topology change-zone-arbiters -name topo2 -zn 1 -arbiters",
            "Changed allow arbiters for zone zn1 to true");
        assertCLICommandResult(
            "topology change-zone-master-affinity -name topo2 -zn 1" +
            " -master-affinity",
            "Changed zone master affinity for zone zn1 to true");
        assertCLICommandResult(
            "topology change-zone-type -name topo2 -zn 1 -type secondary",
            "Changed type of zone zn1 to SECONDARY");

        String result =
            runCLICommand("topology", "view", "-name", "topo2", "-json");
        JsonNode json = JsonUtils.parseJsonNode(result);

        assertEquals(5000, json.get("returnCode").asInt());

        JsonNode zn1 = null;
        for (final JsonNode elem :
                 json.get("returnValue").get("zns").asArray()) {
            if ("zn1".equals(getAsText(elem, "resourceId"))) {
                zn1 = elem;
                break;
            }
        }
        assertNotNull("zn1 not found", zn1);
        assertFalse(zn1.get("allowArbiters").asBoolean());
        assertFalse(zn1.get("masterAffinity").asBoolean());

        /* Changing secondary RF=0 to primary enables allowing arbiters */
        assertCLICommandResult("topology clone -current -name topo3",
                               "Created topo3");
        assertCLICommandResult(
            "topology change-zone-type -name topo3 -zn 1 -type secondary",
            "Changed type of zone zn1 to SECONDARY");
        assertCLICommandResult(
            "topology change-repfactor -name topo3 -zn 1 -rf 0 -pool " +
            MYPOOLNAME,
            "Changed replication factor");
        assertCLICommandResult(
            "topology change-zone-type -name topo3 -zn 1 -type primary",
            "Changed type of zone zn1 to PRIMARY");
        result = runCLICommand("topology", "view", "-name", "topo3", "-json");
        json = JsonUtils.parseJsonNode(result);

        assertEquals(5000, json.get("returnCode").asInt());

        for (final JsonNode elem :
                 json.get("returnValue").get("zns").asArray()) {
            if ("zn1".equals(getAsText(elem, "resourceId"))) {
                zn1 = elem;
                break;
            }
        }
        assertTrue(zn1.get("allowArbiters").asBoolean());
        assertFalse(zn1.get("masterAffinity").asBoolean());
    }

    @Test
    public void testChangeZoneAllowArbiters() throws Exception {

        /* Deploy store */
        startAdminService();
        final String dcname = "Miami";
        assertPlanSuccess(getPlanDeployZone() + " -name " + dcname +
                          " -rf 1 -wait");
        changePolicy();
        deployAdmin(deployStorageNode(dcname, portFinder1.getRegistryPort()));
        final String dcname2 = "Orlando";
        assertPlanSuccess(getPlanDeployZone() + " -name " + dcname2 +
                          " -rf 1 -wait");
        deployAdmin(deployStorageNode(dcname2, portFinder2.getRegistryPort()));
        addpool();
        deployKVStore();

        /* Deploy an RF=0 zone, SN */
        final String dcname3 = "BocaRaton";
        assertPlanSuccess(getPlanDeployZone() + " -name " + dcname3 +
                          " -rf 0 -arbiters -wait");
        deployStorageNode(dcname3, portFinder3.getRegistryPort());
        addpool("sn3");
        assertCLICommandResult("topology clone -current -name topo1",
                               "Created topo1");
        assertCLICommandResult("topology rebalance -name topo1 -pool " +
                               MYPOOLNAME,
                               "Rebalanced");
        assertPlanSuccess("plan deploy-topo -name topo1 -wait");

        /* Test changing allow arbiters */
        assertCLICommandResult("topology clone -current -name topo2",
                               "Created topo2");
        assertCLICommandResult(
            "topology change-zone-arbiters -name topo2 -zn 1 -arbiters",
            "Changed allow arbiters for zone zn1 to true");
        assertCLICommandResult(
            "topology change-zone-arbiters -name topo2 -zn 1 -arbiters",
            "Zone zn1 already allows arbiters");
        assertCLICommandResult(
            "topology change-zone-arbiters -name topo2 -zn 1 -no-arbiters",
            "Changed allow arbiters for zone zn1 to false");
        assertCLICommandResult(
            "topology change-zone-arbiters -name topo2 -zn 1 -no-arbiters",
            "Zone zn1 already does not allow arbiters");
        assertCLICommandResult(
            "topology change-zone-type -name topo2 -zn 1 -type secondary",
            "Changed type of zone zn1 to SECONDARY");
        assertCLICommandResult(
            "topology change-zone-arbiters -name topo2 -zn 1 -arbiters",
            "Allowing arbiters is not permitted on secondary zones.");
        assertCLICommandResult(
            "topology change-zone-arbiters -name topo2 -zn 1 -no-arbiters",
            "Zone zn1 already does not allow arbiters");
        assertCLICommandResult(
            "topology change-zone-arbiters -name topo2 -zn 3 -no-arbiters",
            "Allowing arbiters is required on primary zones with replication" +
            " factor 0.");
        assertCLICommandResult(
            "topology change-zone-arbiters -name topo2 -zn 3 -arbiters",
            "Zone zn3 already allows arbiters");
    }

    @Test
    public void testChangeZoneMasterAffinity() throws Exception {

        /* Deploy store */
        startAdminService();
        final String dcname = "Miami";
        assertPlanSuccess(getPlanDeployZone() + " -name " + dcname +
                          " -rf 1 -wait");
        changePolicy();
        deployAdmin(deployStorageNode(dcname, portFinder1.getRegistryPort()));
        final String dcname2 = "Orlando";
        assertPlanSuccess(getPlanDeployZone() + " -name " + dcname2 +
                          " -rf 1 -wait");
        deployAdmin(deployStorageNode(dcname2, portFinder2.getRegistryPort()));
        addpool();
        deployKVStore();

        /* Deploy an RF=0 zone, SN */
        final String dcname3 = "BocaRaton";
        assertPlanSuccess(getPlanDeployZone() + " -name " + dcname3 +
                          " -rf 0 -arbiters -wait");
        deployStorageNode(dcname3, portFinder3.getRegistryPort());
        addpool("sn3");
        assertCLICommandResult("topology clone -current -name topo1",
                               "Created topo1");
        assertCLICommandResult("topology rebalance -name topo1 -pool " +
                               MYPOOLNAME,
                               "Rebalanced");
        assertPlanSuccess("plan deploy-topo -name topo1 -wait");

        /* Test changing master affinity */
        assertCLICommandResult("topology clone -current -name topo2",
                               "Created topo2");
        assertCLICommandResult(
            "topology change-zone-master-affinity -name topo2 -zn 1" +
            " -master-affinity",
            "Changed zone master affinity for zone zn1 to true");
        assertCLICommandResult(
            "topology change-zone-master-affinity -name topo2 -zn 1" +
            " -master-affinity",
            "Zone zn1 already has master affinity");
        assertCLICommandResult(
            "topology change-zone-master-affinity -name topo2 -zn 1" +
            " -no-master-affinity",
            "Changed zone master affinity for zone zn1 to false");
        assertCLICommandResult(
            "topology change-zone-master-affinity -name topo2 -zn 1" +
            " -no-master-affinity",
            "Zone zn1 already has no master affinity");
        assertCLICommandResult(
            "topology change-zone-type -name topo2 -zn 1 -type secondary",
            "Changed type of zone zn1 to SECONDARY");
        assertCLICommandResult(
            "topology change-zone-master-affinity -name topo2 -zn 1" +
            " -master-affinity",
            "Master affinity is not allowed for secondary zones.");
        assertCLICommandResult(
            "topology change-zone-master-affinity -name topo2 -zn 1" +
            " -no-master-affinity",
            "Zone zn1 already has no master affinity");
        assertCLICommandResult(
            "topology change-zone-master-affinity -name topo2 -zn 3" +
            " -master-affinity",
            "Master affinity is not allowed for primary zones with" +
            " replication factor 0.");
    }
}
