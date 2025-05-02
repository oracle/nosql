/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.StorageNodeUtils;
import oracle.kv.impl.util.TestUtils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.stream.Stream;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertFalse;

/**
 * Tests the deployment of a store with admin only SN
 */
public class DeployAdminAndArbiterOnlySNTest extends TestBase {

    private AdminTestConfig atc;
    private StorageNodeAgent[] snas;
    private PortFinder[] portFinders;
    private Admin admin;

    private final String storeName = "AdminAndArbiterOnlySNTestStore";
    private final int startPort = 13250;
    private final int haRange = 8;

    @Override
    @Before
    public void setUp()
        throws Exception {

        TestUtils.clearTestDirectory();
    }

    @Override
    @After
    public void tearDown()
        throws Exception {

        if (admin != null) {
            admin.shutdown(true, "tear down");
        }
        for (StorageNodeAgent sna : snas) {
            if (sna != null) {
                sna.shutdown(true, true);
            }
        }

        // sleep to allow all processes to shutdown
        Thread.sleep(20 * 1000);
    }

    /*
     * Deploy 1x1 with 2 SN's:
     *   - deploy zone with RF=1
     *   - deploy sn1 as admin only sn with capacity=0
     *   - deploy sn2 with capacity=1
     *   - deploy topology
     * verify fix for KVSTORE-671:
     *   - verify sna log of sn1 does not repeatedly log the following message:
     *     - "Acquiring initial topology from RNs"
     */
    @Test
    public void testAdminOnlySNLogMessage()
        throws Exception {
        final int numSNs = 2;
        final int [] capacitySNs = {0, 1};
        final int rf = 1;
        final boolean allowArbiters = false;
        final String sn1LogFile = TestUtils.getTestDir().toString() + "/" +
                                  storeName + "/log/sn1_0.log";
        final Path sn1LogPath = Paths.get(sn1LogFile);
        final Stream<String> sn1LogStream;
        final StringBuilder sn1LogContents = new StringBuilder();
        final String notExpectedMsg = "Acquiring initial topology from RNs";
        final String assertFailMsg = "Unexpected log message: " + "\"" +
                                     notExpectedMsg + "\" " +
                                     "found in sn1 log file";

        initStorageNodes(numSNs, capacitySNs);
    	atc = new AdminTestConfig(storeName, portFinders[0]);
        runSetup(rf, allowArbiters);

        // sleep for 2 mins to verify if the notExpectedMsg gets logged
        Thread.sleep(120 * 1000);

        sn1LogStream = Files.lines(sn1LogPath);
        sn1LogStream.forEach(l -> sn1LogContents.append(l).append("\n"));
        sn1LogStream.close();

        assertFalse(assertFailMsg,
                    Pattern.compile(notExpectedMsg)
                           .matcher(sn1LogContents.toString())
                           .find());
    }

    /*
     * Deploy 1x3 with 3 SN's:
     *   - deploy zone with RF=2 and arbiter enabled
     *   - deploy sn1 with capacity=1
     *   - deploy admin in sn1
     *   - deploy sn2 with capacity=1
     *   - deploy sn3 as arbiter only sn with capacity=0
     *   - deploy topology
     * verify fix for KVSTORE-671:
     *   - verify sna log of sn3 does not repeatedly log the following message:
     *     - "Acquiring initial topology from RNs"
     */
    @Test
    public void testArbiterOnlySNLogMessage()
        throws Exception {
        final int numSNs = 3;
        final int [] capacitySNs = {1, 1, 0};
        final int rf = 2;
        final boolean allowArbiters = true;
        final String sn3LogFile = TestUtils.getTestDir().toString() + "/" +
                                  storeName + "/log/sn3_0.log";
        final Path sn3LogPath = Paths.get(sn3LogFile);
        final Stream<String> sn3LogStream;
        final StringBuilder sn3LogContents = new StringBuilder();
        final String notExpectedMsg = "Acquiring initial topology from RNs";
        final String assertFailMsg = "Unexpected log message: " + "\"" +
                                     notExpectedMsg + "\" " +
                                     "found in sn3 log file";

        initStorageNodes(numSNs, capacitySNs);
    	atc = new AdminTestConfig(storeName, portFinders[0]);
        runSetup(rf, allowArbiters);

        // sleep for 2 mins to verify if the notExpectedMsg gets logged
        Thread.sleep(120 * 1000);

        sn3LogStream = Files.lines(sn3LogPath);
        sn3LogStream.forEach(l -> sn3LogContents.append(l).append("\n"));
        sn3LogStream.close();

        assertFalse(assertFailMsg,
                    Pattern.compile(notExpectedMsg)
                           .matcher(sn3LogContents.toString())
                           .find());
    }

    private void runSetup(int rf, boolean allowArbiters)
        throws Exception {

        AdminServiceParams adminServiceParams = atc.getParams();

        admin = new Admin(adminServiceParams);

        /* Deploy a Datacenter */
        deployDatacenter(rf, allowArbiters);

        /* Deploy SNs and Admin */
        deployStorageNodesAndAdmin();

        /* Deploy the store */
        deployStore();
    }

    private void deployDatacenter(int rf, boolean allowArbiters) {
        int id = admin.getPlanner().
            createDeployDatacenterPlan("deploy data center",
                                       "Boston",
                                       rf,
                                       DatacenterType.PRIMARY,
                                       allowArbiters,
                                       false);
        runPlan(id);
    }

    private void deployStorageNodesAndAdmin() {

        DatacenterId dcid = new DatacenterId(1);
        boolean deployedAdmin = false;
        for (StorageNodeAgent sna : snas) {
            StorageNodeParams snp = new StorageNodeParams
                (sna.getHostname(), sna.getRegistryPort(), null);
            int id = admin.getPlanner().
                createDeploySNPlan("Deploy SN", dcid, snp);
            runPlan(id);

            if (!deployedAdmin) {
                deployAdmin();
                deployedAdmin = true;
            }
        }
    }

    private void deployAdmin() {
        int id = admin.getPlanner().createDeployAdminPlan
            ("Deploy Admin", snas[0].getStorageNodeId());
        runPlan(id);
    }

    private void deployStore() {
        admin.createTopoCandidate("DDL", Parameters.DEFAULT_POOL_NAME,
                                  10, false,
                                  SerialVersion.ADMIN_CLI_JSON_V1_VERSION);
        int id = admin.getPlanner().createDeployTopoPlan("Deploy Store", "DDL",
                                                         null);
        runPlan(id);
    }

    private void runPlan(int planId) {

        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);
    }

    private void initStorageNodes(int numSNs, int [] capacitySNs)
        throws Exception {

        snas = new StorageNodeAgent[numSNs];
        portFinders = new PortFinder[numSNs];
        int port = startPort;

        for (int i = 0; i < snas.length; i++) {
            boolean createAdmin = (i == 0) ? true : false;
            portFinders[i] = new PortFinder(port, haRange);

            snas[i] = StorageNodeUtils.createUnregisteredSNA(
                TestUtils.getTestDir().toString(),
                portFinders[i],
                capacitySNs[i],
                ("config" + i + ".xml"),
                false, /* useThreads */
                createAdmin,  /* create admin */
                null, 0, 0, 1024, null, null);
            port += 10;
        }
    }
}
