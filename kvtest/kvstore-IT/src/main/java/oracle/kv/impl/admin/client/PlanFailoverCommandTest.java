/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.client;

import static java.util.Arrays.asList;
import static oracle.kv.impl.util.TestUtils.assertMatch;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Writer;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Set;

import oracle.kv.TestBase;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.AdminNotReadyException;
import oracle.kv.impl.admin.AdminUtils;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.VerifyConfiguration.RMIFailed;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;
import oracle.kv.util.CreateStore.ZoneInfo;
import oracle.kv.util.shell.ShellException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * Non-syntax tests of the 'plan failover' CLI command.
 */
public class PlanFailoverCommandTest extends TestBase {
    protected static final int numSNs = 3;
    protected static final int startPort = 5000;
    protected static CreateStore createStore;

    private ByteArrayOutputStream shellOutput;
    protected CommandShell shell;
    protected CommandServiceAPI cs;
    protected PlanCommand.FailoverSub command;

    /** If set to true by a test, shuts down the store during tearDown */
    private boolean shutdown;

    /*
     * Used to test fail over command authorization.
     */
    private static final String USER_USERNAME = "user";
    private static final String USER_PASSWORD = "NoSql00__userPW";
    private static final File TEST_DIR = TestUtils.getTestDir();
    private static final File SECURITY_DIR = new File(TEST_DIR, "security");
    private static final File USER_LOGIN_FILE =
        new File(TEST_DIR, USER_USERNAME + ".login");

    @Before
    @Override
    public void setUp()
        throws Exception {

        if (createStore == null) {
            TestUtils.clearTestDirectory();
            TestStatus.setManyRNs(true);
            createStore();
            createStore.setAdminLocations(1, 2, 3);
            createStore.start();
        }
        shellOutput = new ByteArrayOutputStream();
        shell = new CommandShell(System.in, new PrintStream(shellOutput));
        shell.connectAdmin("localhost", createStore.getRegistryPort(),
                           createStore.getDefaultUserName(),
                           createStore.getDefaultUserLoginPath());
        cs = shell.getAdmin();
        command = new PlanCommand.FailoverSub();
    }

    /** Create the store. */
    protected void createStore()
        throws Exception {

        createStore = new CreateStore(
            getClass().getName(),
            startPort,
            numSNs, /* Storage Nodes */
            asList(new ZoneInfo(1),
                   new ZoneInfo(1),
                   new ZoneInfo(1, DatacenterType.SECONDARY)),
            30, /* Partitions */
            1 /* capacity */,
            CreateStore.MB_PER_SN,
            false /* useThreads */,
            null /* mgmtImpl */,
            true /* mgmtPortsShared */,
            SECURITY_ENABLE);
    }

    @AfterClass
    public static void tearDownClass()
        throws Exception {

        if (createStore != null) {
            createStore.shutdown();
        }
        LoggerUtils.closeAllHandlers();
    }

    @After
    @Override
    public void tearDown() {
        if (shutdown) {
            shutdown = false;
            if (createStore != null) {
                createStore.shutdown();
                createStore = null;
            }
        }
        logger.info("Shell output for " + testName.getMethodName() + ": " +
                    shellOutput);
    }

    /* Tests */

    @Test
    public void testUnknownPrimaryZoneId()
        throws Exception {

        try {
            execute("failover -zn 42 -type primary" +
                    " -zn zn1 -type offline-secondary");
            fail("Expected AdminFaultException");
        } catch (AdminFaultException e) {
            assertEquals("Fault class",
                         IllegalCommandException.class.getName(),
                         e.getFaultClassName());
            assertMatch(IllegalCommandException.class.getName() +
                        ":Zones were not found: \\[zn42\\].*", e.getMessage());
        }
        assertMatch("Output", getDefaultOutput(), shellOutput.toString());
        assertNull("no internal candidate", findInternalCandidate());
        verifyOriginalTopology();
    }

    @Test
    public void testUnknownOfflineZoneId()
        throws Exception {

        try {
            execute("failover -zn 42 -type offline-secondary");
            fail("Expected AdminFaultException");
        } catch (AdminFaultException e) {
            assertEquals("Fault class",
                         IllegalCommandException.class.getName(),
                         e.getFaultClassName());
            assertMatch(IllegalCommandException.class.getName() +
                        ":Zones were not found: \\[zn42\\].*", e.getMessage());
        }
        assertMatch("Output", getDefaultOutput(), shellOutput.toString());
        assertNull("no internal candidate", findInternalCandidate());
        verifyOriginalTopology();
    }

    @Test
    public void testNoPrimaries()
        throws Exception {

        try {
            execute("failover -zn zn1 -type offline-secondary" +
                    " -zn zn2 -type offline-secondary");
            fail("Expected AdminFaultException");
        } catch (AdminFaultException e) {
            assertEquals("Fault class",
                         IllegalCommandException.class.getName(),
                         e.getFaultClassName());
            assertMatch(IllegalCommandException.class.getName() +
                        ":The options to the failover command did not result" +
                        " in any online primary zones.*",
                        e.getMessage());
        }
        assertMatch("Output", getDefaultOutput(), shellOutput.toString());
        assertNull("no internal candidate", findInternalCandidate());
        verifyOriginalTopology();
    }

    @Test
    public void testFailoverToPrimary()
        throws Exception {

        /* Shutdown store afterwards to simplify debugging */
        shutdown = true;

        /* Shutdown SNs in zone 2 */
        shutdownSNs(1);

        /* Repair admin quorum */
        repairAdminQuorum("-zn 1");

        /* Failover to zone 1 as the only primary zone */
        String result =
            execute("failover -zn zn2 -type offline-secondary -wait");
        assertMatch("Plan [0-9]* ended successfully", result);
        assertNull("internal candidate gone with plan complete",
                   findInternalCandidate());
        verifyViolations(3, RMIFailed.class); /* 1 SN, 1 RN, 1 admin */

        /* Restart stopped SNs with services disabled */
        startSNsDisableServices(1);

        /* Repair topology */
        executePlan(cs.createRepairPlan(null));
        verifyViolations();

        /* Convert zone 2 back to a primary zone */
        cs.copyCurrentTopology("original-topo-1");
        cs.changeZoneType("original-topo-1", new DatacenterId(2),
                          DatacenterType.PRIMARY);
        executePlan(cs.createDeployTopologyPlan("Restore original topology",
                                                "original-topo-1", null));
        verifyOriginalTopology();
    }

    @Test
    public void testFailoverToOtherPrimary()
        throws Exception {

        /* Shutdown store afterwards to simplify debugging */
        shutdown = true;

        /* Shutdown SNs in zone 1 */
        shutdownSNs(0);

        /* Connect to admin in zone 2 */
        shell.connectAdmin("localhost",
                           createStore.getRegistryPort(1),
                           createStore.getDefaultUserName(),
                           createStore.getDefaultUserLoginPath());

        /* Repair admin quorum */
        repairAdminQuorum("-zn 2");

        /* Failover to zone 2 as the only primary zone */
        String result =
            execute("failover -zn zn1 -type offline-secondary -wait");
        assertMatch("Plan [0-9]* ended successfully", result);

        /*
         * Connect to the current admin master, which may have changed as a
         * result of the failover
         */
        cs = createStore.getAdminMaster();
        assertNull("internal candidate deleted when plan completes",
                   findInternalCandidate());
        verifyViolations(3, RMIFailed.class); /* 1 SN, 1 RN, 1 admin */

        /* Restart stopped SNs with services disabled */
        startSNsDisableServices(0);

        /* Repair topology */
        executePlan(cs.createRepairPlan(null));
        verifyViolations();

        /* Convert zone 1 back to a primary zone */
        cs.copyCurrentTopology("original-topo-2");
        cs.changeZoneType("original-topo-2", new DatacenterId(1),
                          DatacenterType.PRIMARY);
        executePlan(cs.createDeployTopologyPlan("Restore original topology",
                                                "original-topo-2", null));
        verifyOriginalTopology();
    }

    @Test
    public void testFailoverToSecondary()
        throws Exception {

        /* Shutdown store afterwards to simplify debugging */
        shutdown = true;

        /* Shutdown SNs in zones 1 and 2 */
        shutdownSNs(0, 1);

        /* Connect to admin in zone 3 */
        shell.connectAdmin("localhost",
                           createStore.getRegistryPort(2),
                           createStore.getDefaultUserName(),
                           createStore.getDefaultUserLoginPath());

        /* Repair admin quorum */
        repairAdminQuorum("-zn 3");
        shell.connectAdmin("localhost",
                           createStore.getRegistryPort(2),
                           createStore.getDefaultUserName(),
                           createStore.getDefaultUserLoginPath());
        cs = shell.getAdmin();

        /* Failover to zone 3 as the only primary zone */
        String result = execute("failover -zn zn1 -type offline-secondary" +
                                " -zn zn2 -type offline-secondary" +
                                " -zn zn3 -type primary -wait");
        assertMatch("Plan [0-9]* ended successfully", result);
        assertNull("internal candidate gone with plan complete",
                   findInternalCandidate());
        verifyViolations(6, RMIFailed.class); /* 2 SNs, 2 RNs, 2 admins */

        /* Restart stopped SNs with services disabled */
        startSNsDisableServices(0, 1);

        /* Repair topology */
        executePlan(cs.createRepairPlan(null));
        verifyViolations();

        /*
         * Convert zones 1 and 2 back to primary zones and zone 3 to a
         * secondary zone
         */
        cs.copyCurrentTopology("original-topo-3");
        cs.changeZoneType("original-topo-3", new DatacenterId(1),
                          DatacenterType.PRIMARY);
        cs.changeZoneType("original-topo-3", new DatacenterId(2),
                          DatacenterType.PRIMARY);
        cs.changeZoneType("original-topo-3", new DatacenterId(3),
                          DatacenterType.SECONDARY);

        /* Execute plan and retry if interrupted */
        deployTopo("Restore original topology", "original-topo-3");

        verifyOriginalTopology();
    }

    @Test
    public void testFailoverToPrimaryAndSecondary()
        throws Exception {

        /* Shutdown store afterwards to simplify debugging */
        shutdown = true;

        /* Shutdown SNs in zone 2 */
        shutdownSNs(1);

        /* Connect to admin in zone 1 */
        shell.connectAdmin("localhost",
                           createStore.getRegistryPort(0),
                           createStore.getDefaultUserName(),
                           createStore.getDefaultUserLoginPath());

        /* Repair admin quorum */
        repairAdminQuorum("-zn 1 -zn 3");

        /* Failover from zone 2 to zone 3 */
        String result = execute("failover -zn zn2 -type offline-secondary" +
                                " -zn zn3 -type primary -wait");
        assertMatch("Plan [0-9]* ended successfully", result);
        shell.connectAdmin("localhost",
                           createStore.getRegistryPort(0),
                           createStore.getDefaultUserName(),
                           createStore.getDefaultUserLoginPath());
        assertNull("internal candidate gone with plan complete",
                   findInternalCandidate());
        verifyViolations(3, RMIFailed.class); /* 1 SN, 1 RN, 1 admin */

        /* Restart stopped SN with services disabled */
        startSNsDisableServices(1);

        /* Repair topology */
        executePlan(cs.createRepairPlan(null));
        verifyViolations();

        /* Swap zones 2 and 3 back */
        cs.copyCurrentTopology("original-topo-4");
        cs.changeZoneType("original-topo-4", new DatacenterId(2),
                          DatacenterType.PRIMARY);
        cs.changeZoneType("original-topo-4", new DatacenterId(3),
                          DatacenterType.SECONDARY);

        /* Execute plan and retry if interrupted */
        deployTopo("Restore original topology", "original-topo-4");

        verifyOriginalTopology();
    }

    /*
     * Tests failover when there is an RN running in the failed zone. This
     * should be detected and the failover plan should fail.
     */
    @Test
    public void testReachableRN()
        throws Exception {

        try {
            execute("failover -zn zn2 -type offline-secondary -wait");
            fail("Expected AdminFaultException");
        } catch (AdminFaultException e) {
            assertEquals("Fault class",
                         IllegalCommandException.class.getName(),
                         e.getFaultClassName());
            assertMatch("(?s)" + IllegalCommandException.class.getName() +
                         ":Verification for failover failed:.*",
                        e.getMessage());
        } finally {
            cs.cancelPlan(PlanCommand.PlanSubCommand.getLastPlanId(cs));
        }
        verifyOriginalTopology();
    }

    /*
     * Tests failover when there is an Admin running in the failed zone. This
     * should be detected and the failover plan should fail.
     */
    @Test
    public void testReachableAdmin()
        throws Exception {

        /* Shutdown -- doesn't leave clean state */
        shutdown = true;

        final Topology topo = createStore.getAdmin().getTopology();
        final DatacenterId dcId = new DatacenterId(1);
        final Set<RepNodeId> zoneOneRNs = topo.getRepNodeIds(dcId);
        stopRNs(zoneOneRNs);
        try {
            execute("failover -zn zn1 -type offline-secondary -wait");
            fail("Expected AdminFaultException");
        } catch (AdminFaultException e) {
            assertEquals("Fault class",
                         IllegalCommandException.class.getName(),
                         e.getFaultClassName());
            assertMatch("(?s)" + IllegalCommandException.class.getName() +
                         ":Verification for failover failed:.*",
                        e.getMessage());
        }
    }

    @Test
    public void testCancelNoExecute()
        throws Exception {

        execute("failover -zn zn1 -type offline-secondary" +
                " -zn zn3 -type primary -noexecute");
        assertNotNull("internal candidate present with plan active",
                      findInternalCandidate());
        cs.cancelPlan(PlanCommand.PlanSubCommand.getLastPlanId(cs));
        assertMatch("Output", getDefaultOutput(), shellOutput.toString());
        assertNull("internal candidate gone with plan canceled",
                   findInternalCandidate());
        verifyOriginalTopology();
    }

    /**
     * Make sure that the 'plan failover' command fails without authorization.
     */
    @Test
    public void testFailoverNoPermission()
        throws Exception {
        Assume.assumeTrue("Permission case only work in secure store",
                          SECURITY_ENABLE);
        createUser(USER_LOGIN_FILE, USER_USERNAME, USER_PASSWORD, false);
        shell.connectAdmin("localhost", createStore.getRegistryPort(),
                           USER_USERNAME, USER_LOGIN_FILE.toString());
        try {
            execute("failover -zn zn1 -type offline-secondary -wait");
            fail("Expected UnauthorizedException");
        } catch (UnauthorizedException e) {
        }
    }

    /* Utilities */

    /**
     * Return a pattern that should match default output expected from a
     * command.
     */
    protected String getDefaultOutput() {
        return "";
    }

    /** Execute a plan command. */
    protected String execute(String cmdLine)
        throws ShellException {

        return command.execute(cmdLine.split(" "), shell);
    }

    private void stopRNs(Set<RepNodeId> rnIds)
        throws RemoteException {

        executePlan(cs.createStopServicesPlan(null, rnIds));
    }

    private void executePlan(int planId)
        throws RemoteException {

        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
    }

    /* Execute plan to deploy a topology, retrying it if it is interrupted. */
    private void deployTopo(String planName, String candidate)
        throws Exception {

        final int planId = cs.createDeployTopologyPlan(planName, candidate,
                                                       null);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);

        /* Wait for the plan to end */
        while (true) {
            try {
                cs.awaitPlan(planId, 0, null);
                break;
            } catch (AdminFaultException afe) {
                /*
                 * If the Admin is not available, retry, possibly at some
                 * other Admin
                 */
                if (!afe.getFaultClassName().equals(
                                      AdminNotReadyException.class.getName())) {
                    throw afe;
                }
                final int masterAdminIndex = createStore.getAdminIndex();

                final boolean ok = new PollCondition(1000, 60000) {
                    @Override
                    protected boolean condition() {
                        try {
                            if (!createStore.getAdmin(masterAdminIndex)
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
                assertTrue("Waiting for admin" + (masterAdminIndex+1) +
                           " to restart as replica",
                           ok);

                cs = createStore.getAdminMaster();
            }
        }
        cs.assertSuccess(planId);
    }

    private String findInternalCandidate() throws RemoteException {
        for (String candidate : cs.listTopologies()) {
            if (candidate.contains(TopologyCandidate.INTERNAL_NAME_PREFIX)) {
                return candidate;
            }
        }
        return null;
    }

    private void verifyViolations(Object... countsAndProblemClasses)
        throws RemoteException {

        AdminUtils.verifyViolations(cs, countsAndProblemClasses);
    }

    private void shutdownSNs(final int... sns) {
        for (int sn : sns) {
            createStore.shutdownSNA(sn, true);
        }
        final boolean ok = new PollCondition(1000, 30000) {
            @Override
            protected boolean condition() {
                for (int sn : sns) {
                    try {
                        RegistryUtils.getAdmin(
                            "localhost", createStore.getRegistryPort(sn),
                            null /* loginMgr */, logger).ping();
                        logger.info("Admin " + sn + " is still active");
                        return false;
                    } catch (RemoteException e) {
                        continue;
                    } catch (NotBoundException e) {
                        continue;
                    } catch (Exception e) {
                        logger.info("Problem with Admin " + sn + ": " + e);
                        return false;
                    }
                }
                return true;
            }
        }.await();
        assertTrue("Wait for admins to stop", ok);
    }

    private void startSNsDisableServices(int... sns)
        throws Exception {

        for (int sn : sns) {
            createStore.startSNA(sn, true /* disableServices */);
        }
    }

    private void repairAdminQuorum(String args)
        throws ShellException {
        final String result = new RepairAdminQuorumCommand().execute(
            ("repair-admin-quorum " + args).split(" "), shell);
        assertMatch("Repaired admin quorum using admins: .*", result);
    }

    private void verifyOriginalTopology()
        throws Exception {

        shell.connectAdmin("localhost", createStore.getRegistryPort(),
                           createStore.getDefaultUserName(),
                           createStore.getDefaultUserLoginPath());
        cs = shell.getAdmin();
        verifyViolations();
        final Topology topo = cs.getTopology();
        assertEquals("Zone 1", DatacenterType.PRIMARY,
                     topo.get(new DatacenterId(1)).getDatacenterType());
        assertEquals("Zone 2", DatacenterType.PRIMARY,
                     topo.get(new DatacenterId(1)).getDatacenterType());
        assertEquals("Zone 3", DatacenterType.PRIMARY,
                     topo.get(new DatacenterId(1)).getDatacenterType());
    }

    private static void createUser(File loginFile,
                                   String username,
                                   String password,
                                   boolean admin)
        throws IOException {

        createStore.addUser(username, password, admin);
        final File passwordFile = new File(TEST_DIR, username + ".passwd");
        writeFile("Password Store:\n" +
                  "secret." + username + "=" + password + "\n",
                  passwordFile);
        writeFile("oracle.kv.auth.pwdfile.file=" + passwordFile +
                  "\noracle.kv.auth.username=" + username +
                  "\noracle.kv.transport=ssl" +
                  "\noracle.kv.ssl.trustStore=" +
                  new File(SECURITY_DIR, "store.trust"),
                  loginFile);
    }

    private static void writeFile(String string, File file)
        throws IOException {

        final Writer out = new FileWriter(file);
        try {
            out.write(string);
            out.flush();
        } finally {
            out.close();
        }
    }
}
