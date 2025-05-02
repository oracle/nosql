/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.sna;

import static oracle.kv.impl.sna.StorageNodeAgent.RMI_REGISTRY_FILTER_DELIMITER;
import static oracle.kv.impl.sna.StorageNodeAgent.RMI_REGISTRY_FILTER_NAME;
import static oracle.kv.impl.sna.StorageNodeAgent.RMI_REGISTRY_FILTER_REQUIRED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.rmi.NoSuchObjectException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.test.RemoteTestAPI;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.FilterableParameterized;
import oracle.kv.impl.util.KVStoreMain;
import oracle.kv.impl.util.StorageNodeUtils;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.RegistryUtils;

import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests the basic operations of StorageNodeAgentImpl.
 */
@RunWith(FilterableParameterized.class)
public class StorageNodeBasic extends StorageNodeTestBase {

    public StorageNodeBasic(boolean useThreads) {
        super(useThreads);
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
        System.clearProperty(RMI_REGISTRY_FILTER_NAME);
        // oracle.kv.impl.async.AsyncTestUtils.checkActiveDialogTypes();
    }

    /**
     * Create and start SNA, ping it, shutdown.
     */
    @Test
    public void testUnregisteredStartStop()
        throws Exception {

        final StorageNodeAgentAPI snai = createUnregisteredSNA();

        /**
         * Wait for the bootstrap admin.
         */
        waitForAdmin(testhost, sna.getRegistryPort());
        if (!useThreads) {
            assertTrue(1 == ManagedService.findManagedProcesses
                       (null, sna.makeBootstrapAdminName(), logger).size());
        }
        snai.shutdown(true, true);
        assertShutdown(snai);
        assertTrue(0 == ManagedService.findManagedProcesses
                   (null, sna.makeBootstrapAdminName(), logger).size());
    }

    /**
     * Create and start SNA, ping it, shutdown using main().
     */
    @Test
    public void testUnregisteredUtilityShutdown()
        throws Exception {

        final StorageNodeAgentAPI snai = createUnregisteredSNA();

        /* Wait for the bootstrap admin. */
        waitForAdmin(testhost, sna.getRegistryPort());
        if (!useThreads) {
            assertTrue(1 == ManagedService.findManagedProcesses
                       (null, sna.makeBootstrapAdminName(), logger).size());
        }
        final String[] args = new String[] { "-root",
                                       TestUtils.getTestDir().toString(),
                                       "-config",
                                       CONFIG_FILE_NAME,
                                       "-shutdown" };
        StorageNodeAgentImpl.main(args);

        assertShutdown(snai);
        assertTrue(0 == ManagedService.findManagedProcesses
                   (null, sna.makeBootstrapAdminName(), logger).size());
    }

    @Test
    public void testNoAdminStorageNode()
        throws Exception {

        final File testDir = TestUtils.getTestDir();
        final File rootDir = new File(testDir.getAbsolutePath() +
                                      File.separator +
                                      "kvroot");
        rootDir.mkdirs();
        String[] args =
            {"makebootconfig",
             "-root", rootDir.getAbsolutePath(),
             "-host", testhost,
             "-port", Integer.toString(portFinder.getRegistryPort()),
             "-harange", portFinder.getHaRange(),
             "-capacity", "0",
             "-noadmin",
             "-store-security", "none"};
        KVStoreMain.main(args);

        /* Use non-argument construct to mimic StorageNodeAgentImpl.main path */
        args = new String[] { "-root", rootDir.getAbsolutePath(),
                              "-config", CONFIG_FILE_NAME };
        final StorageNodeAgentImpl snaImpl = new StorageNodeAgentImpl();
        snaImpl.parseArgs(args);

        int nretries = 0;
        boolean started = false;
        while (!started) {
            try {
                snaImpl.start();
                started = true;
            } catch (IOException e) {
                if (nretries++ > 100 ||
                    !(e instanceof NoSuchObjectException ||
                      e.getCause() instanceof NoSuchObjectException)) {

                    throw e;
                }
            }
        }
        sna = snaImpl.getStorageNodeAgent();
        final StorageNodeAgentAPI snai = StorageNodeUtils.getBootstrapHandle(
            testhost, portFinder.getRegistryPort(), sna.getLoginManager());
        assertPing(snai, ServiceStatus.WAITING_FOR_DEPLOY);
        assertTrue(0 == ManagedService.findManagedProcesses(
            null, sna.makeBootstrapAdminName(), logger).size());

        args = new String[] { "-root", rootDir.getAbsolutePath(),
                              "-config", CONFIG_FILE_NAME,
                              "-shutdown"};
        StorageNodeAgentImpl.main(args);

        assertShutdown(snai);
    }

    /**
     * Create and start SNA, get status, shutdown using main().
     */
    @Test
    public void testUnregisteredUtilityStatus()
        throws Exception {

        final StorageNodeAgentAPI snai = createUnregisteredSNA();

        /* Wait for the bootstrap admin. */
        waitForAdmin(testhost, sna.getRegistryPort());
        if (!useThreads) {
            assertTrue(1 == ManagedService.findManagedProcesses
                       (null, sna.makeBootstrapAdminName(), logger).size());
        }
        String[] args = new String[] { "-root",
                                       TestUtils.getTestDir().toString(),
                                       "-config",
                                       CONFIG_FILE_NAME,
                                       "-status" };

        /* Check human output */
        final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        final PrintStream originalOut = System.out;
        try {
            System.setOut(new PrintStream(outBytes));
            StorageNodeAgentImpl.main(args);
        } finally {
            System.setOut(originalOut);
        }
        String output = outBytes.toString();
        assertEquals("SNA Status", output, "SNA Status : WAITING_FOR_DEPLOY\n");

        /* Re-execute status and check json output */
        args = new String[] { "-root",
                TestUtils.getTestDir().toString(),
                "-config",
                CONFIG_FILE_NAME,
                "-status",
                "-json-v1" };
        outBytes.reset();
        try {
            System.setOut(new PrintStream(outBytes));
            StorageNodeAgentImpl.main(args);
        } finally {
            System.setOut(originalOut);
        }
        output = outBytes.toString();

        final JsonNode json = JsonUtils.parseJsonNode(output);
        JsonNode sna_status = json.get("return_value").get("sna_status");
        assertEquals("WAITING_FOR_DEPLOY", sna_status.asText());

        snai.shutdown(true, true);
        assertShutdown(snai);
        assertTrue(0 == ManagedService.findManagedProcesses
                   (null, sna.makeBootstrapAdminName(), logger).size());
    }

    /**
     * Start unregistered, then register.
     */
    @Test
    public void testRegister()
        throws Exception {

        final StorageNodeAgentAPI snai =
            createRegisteredStore(new StorageNodeId(1), true);
        snai.shutdown(true, false);
        assertShutdown(snai);
    }

    /**
     * Shutdown registered store via main().
     */
    @Test
    public void testRegisteredUtilityShutdown()
        throws Exception {

        final StorageNodeAgentAPI snai =
            createRegisteredStore(new StorageNodeId(1), true);

        final String[] args = new String[] { "-root",
                                       TestUtils.getTestDir().toString(),
                                       "-config",
                                       CONFIG_FILE_NAME,
                                       "-shutdown" };
        StorageNodeAgentImpl.main(args);

        assertShutdown(snai);
    }

    /**
     * Register, make the config file and/or store directory bad, restart and
     * verify that it reverts to unregistered.
     */
    @Test
    public void testRegisterRevert()
        throws Exception {

        final StorageNodeId snid = new StorageNodeId(1);
        StorageNodeAgentAPI snai =
            createRegisteredStore(snid, true);
        final File configFile = sna.getKvConfigFile();
        waitForAdmin(testhost, sna.getRegistryPort());
        snai.shutdown(true, true);
        assertShutdown(snai);

        /**
         * Remove the config file.
         */
        assertTrue(configFile.delete());
        snai = startRegisteredStore(snid);
        snai.shutdown(true, true);
    }

    /**
     * Test random error conditions
     */

    /**
     * Register twice.  Second should succeed also to allow planner retries.
     */
    @Test
    public void testRegisterTwice()
        throws Exception {

        /* Call Assume so that this test only run with process mode. */
        Assume.assumeFalse("Skipping a process-only test", useThreads);
        final StorageNodeId snid = new StorageNodeId(1);
        StorageNodeAgentAPI snai =
            createRegisteredStore(snid, true);
        final GlobalParams gp = new GlobalParams(kvstoreName);
        /* empty ParameterMap is ignored */
        final List<ParameterMap> snMaps =
            snai.register(gp.getMap(), new ParameterMap(), true);
        final StorageNodeAgent.RegisterReturnInfo rri = new
            StorageNodeAgent.RegisterReturnInfo(snMaps);
        assertTrue(rri.getBootMap().get(ParameterState.COMMON_PORTRANGE).
                   asString().equals(portFinder.getHaRange()));
        assertTrue(rri.getBootMap().get(ParameterState.COMMON_HA_HOSTNAME).
                   asString().equals(testhost));
        snai.shutdown(true, true);

        /**
         * Now, trickier... storename isn't set so the SNA does not appear
         * registered, but the store has a config file.  Do this by resetting
         * the bootstrap dir.
         */
        cleanBootstrapDir();
        try {
            snai = createRegisteredStore(snid, true);
            snai.shutdown(true, true);
            fail("Register should have failed");
        } catch (Exception ignore) {
            /**
             * SNA is running but not registered.  Register it for real and
             * verify this works.  Need to clean up the store directory to
             * avoid another failure.  Need to get a handle first.
             */
            cleanStoreDir(kvstoreName);

            snai = getBootstrapHandle(testhost, portFinder.getRegistryPort(),
                                      sna.getLoginManager());
            snai = registerStore(snai, sna.getLoginManager(), portFinder.getRegistryPort(),
                                 kvstoreName, snid, true);
            snai.shutdown(true, true);
        }
    }

    /**
     * Bad root directory.
     */
    @Test
    public void testBadRoot()
        throws Exception {

        /* Call Assume so that this test only run with thread mode. */
        Assume.assumeTrue("Skipping a thread-only test", useThreads);
        final String testDir = BAD_DIR;
        final String portRange = "1,2";
        final String configfile = testDir + File.separator + CONFIG_FILE_NAME;
        try {
            TestUtils.generateBootstrapFile(configfile, testDir, testhost,
                                            988, portRange, testhost, false, 1,
                                            null);
            fail("operation should have failed with a bad directory");
        } catch (Exception e) /* CHECKSTYLE:OFF */ {
            /* This is the success path */
        } /* CHECKSTYLE:ON */
    }

    /**
     * Bad config file.
     */
    @Test
    public void testBadFile()
        throws Exception {

        /* Call Assume so that this test only run with thread mode. */
        Assume.assumeTrue("Skipping a thread-only test", useThreads);
        final String testDir = TestUtils.getTestDir().toString();
        final String configfile = testDir + File.separator + BAD_FILE;
        final String portRange = "1,2";
        final String secfile =
            testDir + File.separator + FileNames.JAVA_SECURITY_POLICY_FILE;
        TestUtils.generateBootstrapFile(configfile, testDir, testhost,
                                        988, portRange, testhost, false, 1, null);
        TestUtils.generateSecurityPolicyFile
            (secfile, TestUtils.SEC_POLICY_STRING);
        try {
            startSNA(testDir, CONFIG_FILE_NAME, useThreads, true);
            fail("Start should have failed with a bad config file");
        } catch (Exception e) /* CHECKSTYLE:OFF */ {
            /* This is the success path */
        } /* CHECKSTYLE:ON */
    }

    /**
     * Bad port -- privileged.  Only run this on *nix.
     */
    @Test
    public void testPrivilegedPort()
        throws Exception {

        /* Call Assume so that this test only run with thread mode. */
        Assume.assumeTrue("Skipping a thread-only test", useThreads);

        /*
         * Do not run on Windows -- it has no concept of privileged ports.
         */
        Assume.assumeFalse("Skipping test on Windows", isWindows);

        /*
         * Skip on the Mac, too, since it doesn't seem to prevent opening
         * server sockets on privileged ports.
         */
        Assume.assumeFalse("Skipping test on Mac",
                           "Mac OS X".equals(System.getProperty("os.name")));

        final String testDir = TestUtils.getTestDir().toString();
        final String portRange = "1,2";
        final String configfile = testDir + File.separator + CONFIG_FILE_NAME;
        final String secfile =
            testDir + File.separator + FileNames.JAVA_SECURITY_POLICY_FILE;
        TestUtils.generateBootstrapFile(configfile, testDir, testhost,
                                        988, portRange, testhost, false, 1, null);
        TestUtils.generateSecurityPolicyFile
            (secfile, TestUtils.SEC_POLICY_STRING);
        try {
            startSNA(testDir, CONFIG_FILE_NAME, useThreads, true);
            fail("Bind should have failed, are you running as root?");
        } catch (Exception e) /* CHECKSTYLE:OFF */ {
            /* This is the success path */
        } /* CHECKSTYLE:ON */
    }

    /**
     * Use the same port twice, second time fails.
     */
    @Test
    public void testPortInUse()
        throws Exception {

        /* Call Assume so that this test only run with thread mode. */
        Assume.assumeTrue("Skipping a thread-only test", useThreads);

        final String testDir = TestUtils.getTestDir().toString();
        final String portRange = "1,2";
        final String configfile = testDir + File.separator + "config1.xml";

        final StorageNodeAgentAPI snai = createUnregisteredSNA();

        /* Wait for the bootstrap admin. */
        waitForAdmin(testhost, sna.getRegistryPort());
        if (!useThreads) {
            assertTrue(1 == ManagedService.findManagedProcesses
                       (null, sna.makeBootstrapAdminName(), logger).size());
        }

        TestUtils.generateBootstrapFile(configfile, testDir, testhost,
                                        portFinder.getRegistryPort(),
                                        portRange, testhost, false, 1, null);
        try {
            createUnregisteredSNA(testDir, portFinder, 1,
                                  "config1.xml", useThreads, false, 1024);
            fail("SNA creation should have failed");
        } catch (Exception e) /* CHECKSTYLE:OFF */ {
            /* This is the success path */
        } /* CHECKSTYLE:ON */

        snai.shutdown(true, true);
        assertShutdown(snai);
    }

    /**
     * Make sure the StorageNodeTestInterface works.
     */
    @Test
    public void testStorageNodeTestInterface()
        throws Exception {

        final StorageNodeId snid = new StorageNodeId(1);
        final StorageNodeAgentAPI snai =
            createRegisteredStore(snid, true);
        final RemoteTestAPI rta =
            RegistryUtils.getStorageNodeAgentTest(sna.getStoreName(),
                                                  sna.getHostname(),
                                                  sna.getRegistryPort(),
                                                  snid,
                                                  logger);
        rta.stop();

        snai.shutdown(true, true);
        assertShutdown(snai);
    }

    /**
     * Make sure the StorageNodeTestInterface works.
     */
    @Test
    public void testStorageNodeTestInterfaceUnregistered()
        throws Exception {

        /*
         * Create unregistered SNA and assert that the interface is not
         * available.
         */
        final StorageNodeAgentAPI snai = createUnregisteredSNA();
        try {
            RegistryUtils.getStorageNodeAgentTest(sna.getStoreName(),
                                                  sna.getHostname(),
                                                  sna.getRegistryPort(),
                                                  sna.getStorageNodeId(),
                                                  logger);
            fail("Test interface should have failed");
        } catch (Exception expected) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        snai.shutdown(true, true);
        assertShutdown(snai);
    }

    /**
     * Make sure the CommandServiceTestInterface works.
     */
    @Test
    public void testAdminTestInterface()
        throws Exception {

        final StorageNodeId snid = new StorageNodeId(1);
        final StorageNodeAgentAPI snai =
            createRegisteredStore(snid, true);
        final RemoteTestAPI rta =
            RegistryUtils.getAdminTest(sna.getHostname(),
                                       sna.getRegistryPort(),
                                       logger);
        rta.stop();
        snai.shutdown(true, true);
        assertShutdown(snai);
    }

    @Test
    public void testMainUnknownOption() {
        testMainFailure("unknown option",
                        IllegalArgumentException.class,
                        "-unknown-option");
    }

    @Test
    public void testMainBadRootPath() {
        testMainFailure("bad root path",
                        IllegalArgumentException.class,
                        "-root", "/this-is-not-a-directory/abc/");
    }

    @Test
    public void testMainBadConfigFile() {
        testMainFailure("bad config file",
                        IllegalStateException.class,
                        "-root", TestUtils.getTestDir().toString(),
                        "-config", "this-file-is-not-found");
    }

    @Test
    public void testMainDisableServicesRunningUnregistered()
        throws Exception {

        StorageNodeAgentAPI snai = createUnregisteredSNA();
        testMainFailure("-disable-services when running unregistered",
                        IllegalStateException.class,
                        "-root", TestUtils.getTestDir().toString(),
                        "-config", CONFIG_FILE_NAME,
                        "-disable-services");
        snai.shutdown(true, true);
        assertShutdown(snai);
    }

    @Test
    public void testMainDisableServicesRunningRegistered()
        throws Exception {

        StorageNodeAgentAPI snai =
            createRegisteredStore(new StorageNodeId(1), true);
        testMainFailure("-disable-services when running registered",
                        IllegalStateException.class,
                        "-root", TestUtils.getTestDir().toString(),
                        "-config", CONFIG_FILE_NAME,
                        "-disable-services");
        snai.shutdown(true, true);
        assertShutdown(snai);
    }

    @Test
    public void testMainDisableServicesRunningSnaAndRepNodes()
        throws Exception {

        Topology topo = createTopology(testhost, replicationFactor);
        RepNodeId[] rnids = new RepNodeId[replicationFactor];
        StorageNodeAgentAPI snai =
            startRepNodes(topo, rnids, replicationFactor);

        RepNodeAdminAPI[] rnAdmins = new RepNodeAdminAPI[replicationFactor];
        for (int i = 0; i < replicationFactor; i++) {
            rnAdmins[i] = waitForRNAdmin(rnids[i]);
        }

        testMainFailure("-disable-services when running SNA with RNs",
                        IllegalStateException.class,
                        "-root", TestUtils.getTestDir().toString(),
                        "-config", CONFIG_FILE_NAME,
                        "-disable-services");
        snai.shutdown(true, true);
        assertShutdown(snai);
        for (final RepNodeAdminAPI rnAdmin : rnAdmins) {
            awaitShutdown(rnAdmin);
        }
    }

    @Test
    public void testMainDisableServicesRunningRepNodesOnly()
        throws Exception {

        /*
         * Only run this test in process mode, because we need to be able to
         * kill the SNA process
         */
        Assume.assumeFalse("Skipping a process-only test", useThreads);

        Topology topo = createTopology(testhost, replicationFactor);
        RepNodeId[] rnids = new RepNodeId[replicationFactor];
        StorageNodeAgentAPI snai =
            startRepNodes(topo, rnids, replicationFactor);

        RepNodeAdminAPI[] rnAdmins = new RepNodeAdminAPI[replicationFactor];
        for (int i = 0; i < replicationFactor; i++) {
            rnAdmins[i] = waitForRNAdmin(rnids[i]);
        }

        /* Kill the SNA process */
        ManagedService.killManagedProcesses(
            "-root " + TestUtils.getTestDir(),
            "-config " + CONFIG_FILE_NAME,
            logger);

        testMainFailure("-disable-services when running RNs only",
                        IllegalStateException.class,
                        "-root", TestUtils.getTestDir().toString(),
                        "-config", CONFIG_FILE_NAME,
                        "-disable-services");
        snai.shutdown(true, true);
        assertShutdown(snai);
        for (final RepNodeAdminAPI rnAdmin : rnAdmins) {
            awaitShutdown(rnAdmin);
        }
    }

    @Test
    public void testMainStartDisabled()
        throws Exception {

        /*
         * Only run this test in process mode, because we need to be able to
         * kill the SNA process
         */
        Assume.assumeFalse("Skipping a process-only test", useThreads);

        Topology topo = createTopology(testhost, replicationFactor);
        RepNodeId[] rnids = new RepNodeId[replicationFactor];
        StorageNodeAgentAPI snai =
            startRepNodes(topo, rnids, replicationFactor);

        RepNodeAdminAPI[] rnAdmins = new RepNodeAdminAPI[replicationFactor];
        for (int i = 0; i < replicationFactor; i++) {
            rnAdmins[i] = waitForRNAdmin(rnids[i]);
        }

        doMain("-root", TestUtils.getTestDir().toString(),
               "-config", CONFIG_FILE_NAME,
               "-shutdown");
        assertShutdown(snai);

        /* Restart the SNA but disable managed services */
        doMain("-root", TestUtils.getTestDir().toString(),
                "-config", CONFIG_FILE_NAME,
                "-disable-services");

        /* Make sure no RNs were started */
        assertEquals("numManagedProcesses", 0, numManagedProcesses());

        doMain("-root", TestUtils.getTestDir().toString(),
               "-config", CONFIG_FILE_NAME,
               "-shutdown");
        assertShutdown(snai);
        for (final RepNodeAdminAPI rnAdmin : rnAdmins) {
            awaitShutdown(rnAdmin);
        }
    }

    /** Test that disabling services on an unregistered SNA has no effect. */
    @Test
    public void testMainStopDisableServicesUnregistered()
        throws Exception {

        StorageNodeAgentAPI snai = createUnregisteredSNA();

        /* Wait for the bootstrap admin. */
        waitForAdmin(testhost, sna.getRegistryPort());

        /* Stop SNA and disable services. */
        doMain("-root", TestUtils.getTestDir().toString(),
               "-config", CONFIG_FILE_NAME,
               "-shutdown",
               "-disable-services");

        /* Start the SNA and make sure the admin comes up */
        snai = createRegisteredStore(new StorageNodeId(1), true);
        waitForAdmin(testhost, sna.getRegistryPort());

        /* Clean up */
        snai.shutdown(true, true);
        assertShutdown(snai);
    }

    @Test
    public void testRMIFilterPatternSet()
        throws Exception {

        createUnregisteredSNA();
        String existingPatterns = "test.package.*;test.package2.*";
        String patternSet = sna.getRMIFilterSet(existingPatterns);
        String expectedPattern =
            Arrays.stream(RMI_REGISTRY_FILTER_REQUIRED).collect(
            Collectors.joining(RMI_REGISTRY_FILTER_DELIMITER)) +
            RMI_REGISTRY_FILTER_DELIMITER + existingPatterns;
        assertEquals(patternSet, expectedPattern);

        /*
         * Test existing pattern doesn't contains required pattern,
         * it's expected to append required one to existing pattern.
         */
        System.setProperty(RMI_REGISTRY_FILTER_NAME, existingPatterns);
        sna.setRMIRegistryFilterProperty();
        assertEquals(System.getProperty(RMI_REGISTRY_FILTER_NAME),
                     expectedPattern);

        /*
         * Test existing pattern contains required pattern, no need to change
         * the existing one. We don't do the similar tests if there is existing
         * security properties, since security properties cannot be cleared
         * in unit test if we set.
         */
        existingPatterns =
            RMI_REGISTRY_FILTER_REQUIRED[0] +
            RMI_REGISTRY_FILTER_DELIMITER +
            "test.package.*" +
            RMI_REGISTRY_FILTER_DELIMITER +
            RMI_REGISTRY_FILTER_REQUIRED[1];

        System.setProperty(RMI_REGISTRY_FILTER_NAME, existingPatterns);
        sna.setRMIRegistryFilterProperty();
        assertEquals(System.getProperty(RMI_REGISTRY_FILTER_NAME),
                     existingPatterns);
    }

    /**
     * Test that disabling services on an registered SNA with services prevents
     * the services from starting when the SNA is started again.
     */
    @Test
    public void testMainStopDisableServices()
        throws Exception {

        /* Create a cluster */
        Topology topo = createTopology(testhost, replicationFactor);
        RepNodeId[] rnids = new RepNodeId[replicationFactor];
        StorageNodeAgentAPI snai =
            startRepNodes(topo, rnids, replicationFactor);
        StorageNodeId snid = topo.get(rnids[0]).getStorageNodeId();

        RepNodeAdminAPI[] rnAdmins = new RepNodeAdminAPI[replicationFactor];
        for (int i = 0; i < replicationFactor; i++) {
            rnAdmins[i] = waitForRNAdmin(rnids[i]);
        }

        /* Shut down the SNA and disable services */
        doMain("-root", TestUtils.getTestDir().toString(),
               "-config", CONFIG_FILE_NAME,
               "-shutdown",
               "-disable-services");
        assertShutdown(snai);

        /* Restart and check that services don't come up */
        snai = startRegisteredStore(snid);
        assertEquals("numManagedProcesses", 0, numManagedProcesses());

        /* Clean up */
        snai.shutdown(true, true);
        assertShutdown(snai);
        for (final RepNodeAdminAPI rnAdmin : rnAdmins) {
            awaitShutdown(rnAdmin);
        }
    }

    private void testMainFailure(String description,
                                 Class<?> expectedCauseClass,
                                 String... args) {
        testMainFailure(description, expectedCauseClass, logger, args);
    }

    /**
     * Test that calling SNA main with the specified arguments causes a
     * RuntimeException to be thrown whose cause is of the specified class.
     */
    public static void testMainFailure(String description,
                                       Class<?> expectedCauseClass,
                                       Logger logger,
                                       String... args) {
        /*
         * Suppress noise going to stdout and stderr
         */
        PrintStream original = System.out;
        PrintStream originalErr = System.err;
        try {
            System.setOut(new PrintStream(new FileOutputStream("/dev/null")));
            System.setErr(new PrintStream(new FileOutputStream("/dev/null")));
            doMain(logger, args);
            fail("Expected " + expectedCauseClass.getSimpleName());
        } catch (RuntimeException e) {
            logger.info("Exception for " + description + ": " + e);
            assertEquals(
                description + ", cause: " + e.getCause(),
                expectedCauseClass,
                (e.getCause() == null ? null : e.getCause().getClass()));
        } catch (FileNotFoundException fnfe) {
            /* ignore, won't happen */
        } finally {
            System.setOut(original);
            System.setErr(originalErr);
        }
    }

    void doMain(String... args) {
        doMain(logger, args);
    }

    /**
     * Run a test version of the SNA main method with the specified arguments.
     */
    public static void doMain(final Logger logger, String... args) {
        new StorageNodeAgentImpl.Main() {
            @Override
            void printlnVerbose(String message) {
                logger.info("Verbose output: " + message);
            }
        }.doMain(args);
    }
}
