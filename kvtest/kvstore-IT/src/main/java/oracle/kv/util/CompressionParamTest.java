/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.util;

import static java.util.logging.Level.INFO;
import static oracle.kv.impl.admin.AdminUtils.awaitPlanSuccess;
import static oracle.kv.impl.param.ParameterState.AP_LOG_FILE_COUNT;
import static oracle.kv.impl.param.ParameterState.AP_LOG_FILE_LIMIT;
import static oracle.kv.impl.param.ParameterState.SN_LOG_FILE_COMPRESSION;
import static oracle.kv.impl.param.ParameterState.SN_LOG_FILE_COUNT;
import static oracle.kv.impl.param.ParameterState.SN_LOG_FILE_LIMIT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.RemoteAPI;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore.ZoneInfo;

import org.junit.Test;

/** Test setting parameters related to log file compression. */
public class CompressionParamTest extends TestBase {
    private static final char[] CHARS = new char[2000];
    static {
        Arrays.fill(CHARS, 'x');
    }
    private static final String LONG_MESSAGE = new String(CHARS);

    private CreateStore createStore;

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
     * Test enabling and disabling log file compression. [KVSTORE-879]
     */
    @Test
    public void testCompression() throws Exception {

        /* Create an RF=2 store so we can test arbiters */
        createStore = new CreateStore(
            kvstoreName,
            5000, /* startPort */
            3, /* numStorageNodes */
            Arrays.asList(new ZoneInfo(1).setAllowArbiters(true),
                          new ZoneInfo(1).setAllowArbiters(true)),
            10, /* partitions */
            1 /* capacity */);
        createStore.start();

        CommandServiceAPI cs = createStore.getAdmin();
        final StorageNodeId sns[] = createStore.getStorageNodeIds();
        final Topology topo = cs.getTopology();
        final Parameters parameters = cs.getParameters();

        /* Confirm log compression is disabled by default */
        for (final StorageNodeId sn : sns) {
            final StorageNodeParams snParams = parameters.get(sn);
            assertFalse(snParams.getLogFileCompression());
        }

        /*
         * Reduce log file size limit and count so we can test log rotation.
         * The changes don't take affect until the SN restarts so, for
         * simplicity, so restart the whole store afterwards.
         */
        for (int i = 0; i < sns.length; i++) {
            final StorageNodeId sn = sns[i];
            final ParameterMap snChanges = new ParameterMap(
                ParameterState.SNA_TYPE, ParameterState.SNA_TYPE);
            snChanges.setParameter(SN_LOG_FILE_LIMIT, "2000");
            snChanges.setParameter(SN_LOG_FILE_COUNT, "10");
            awaitPlanSuccess(cs,
                             cs.createChangeParamsPlan(
                                 "Reduce log file limit for " + sn,
                                 sn, snChanges));

            /*
             * For admins, these are restart parameters, so retry if there is
             * an AdminFaultException, which will happen when modifying the
             * parameter on the master
             */
            final ParameterMap adminChanges = new ParameterMap(
                ParameterState.ADMIN_TYPE, ParameterState.ADMIN_TYPE);
            adminChanges.setParameter(AP_LOG_FILE_LIMIT, "2000");
            adminChanges.setParameter(AP_LOG_FILE_COUNT, "10");
            final AdminId admin = new AdminId(i + 1);
            for (int j = 0; j < 10; j++) {
                try {
                    awaitPlanSuccess(cs,
                                     cs.createChangeParamsPlan(
                                         "Reduce log file limit for " + admin,
                                         admin, adminChanges));
                    break;
                } catch (AdminFaultException e) {
                    Thread.sleep(1000);
                    cs = createStore.getAdminMaster();
                }
            }
        }

        /*
         * Close log handler after shutting down the store so the SN log
         * handlers get changed since SNs run in the test process during
         * testing
         */
        createStore.shutdown();
        LoggerUtils.closeAllHandlers();
        createStore.restart();
        cs = createStore.getAdmin();

        /* Log to all SNs, RNs, Admins, and Arbiters */
        final RegistryUtils registryUtils =
            new RegistryUtils(topo, createStore.getSNALoginManager(sns[0]),
                              logger);
        for (final StorageNodeId sn : sns) {
            withRetry(() -> registryUtils.getStorageNodeAgentTest(sn))
                .logMessage(INFO, LONG_MESSAGE, false /* useJeLogger */);
            withRetry(() -> registryUtils.getAdminTest(sn))
                .logMessage(INFO, LONG_MESSAGE, false /* useJeLogger */);
        }
        for (final RepNodeId rn : topo.getRepNodeIds()) {
            withRetry(() -> registryUtils.getRepNodeTest(rn))
                .logMessage(INFO, LONG_MESSAGE, false /* useJeLogger */);
        }
        for (final ArbNodeId an : topo.getArbNodeIds()) {
            withRetry(() -> registryUtils.getArbNodeTest(an))
                .logMessage(INFO, LONG_MESSAGE, false /* useJeLogger */);
        }

        /*
         * Enable compression. Again, the changes don't take affect until the
         * SN restarts so, for simplicity, just restart the whole store.
         */
        for (final StorageNodeId sn : sns) {
            final ParameterMap changes = new ParameterMap(
                ParameterState.SNA_TYPE, ParameterState.SNA_TYPE);
            changes.setParameter(SN_LOG_FILE_COMPRESSION, "true");
            awaitPlanSuccess(cs,
                             cs.createChangeParamsPlan(
                                 "Enable log compression for " + sn,
                                 sn, changes));
        }
        createStore.shutdown();
        LoggerUtils.closeAllHandlers();
        createStore.restart();
        cs = createStore.getAdmin();

        /*
         * Log 10X to SNs, RNs, Admins, and Arbiters to make sure we get log
         * rotation
         */
        for (final StorageNodeId sn : sns) {
            for (int i = 0; i < 10; i++) {
                withRetry(() -> registryUtils.getStorageNodeAgentTest(sn))
                    .logMessage(INFO, LONG_MESSAGE, false /* useJeLogger */);
                withRetry(() -> registryUtils.getAdminTest(sn))
                    .logMessage(INFO, LONG_MESSAGE, false /* useJeLogger */);
            }
        }
        for (final RepNodeId rn : topo.getRepNodeIds()) {
            for (int i = 0; i < 10; i++) {
                withRetry(() -> registryUtils.getRepNodeTest(rn))
                    .logMessage(INFO, LONG_MESSAGE, false /* useJeLogger */);
            }
        }
        for (final ArbNodeId an : topo.getArbNodeIds()) {
            for (int i = 0; i < 10; i++) {
                withRetry(() -> registryUtils.getArbNodeTest(an))
                    .logMessage(INFO, LONG_MESSAGE, false /* useJeLogger */);
            }
        }

        /*
         * Collect all service names and store name -- log files should all
         * contain one of these
         */
        final Set<String> expectedNames = new HashSet<>();
        for (int i = 0; i < sns.length; i++) {
            expectedNames.add(sns[i].toString());
            expectedNames.add("admin" + (i+1));
        }
        topo.getRepNodeIds().forEach(rn -> expectedNames.add(rn.toString()));
        topo.getArbNodeIds().forEach(an -> expectedNames.add(an.toString()));
        final String storeName = createStore.getStoreName();
        expectedNames.add(storeName);
        final Set<String> expectedNamesCheck = new HashSet<>(expectedNames);

        /* Make sure we're seeing the right compressed log files */
        final Path logDir = Paths.get(createStore.getRootDir(),
                                      storeName,
                                      "log");
        try (final DirectoryStream<Path> dirStream =
             Files.newDirectoryStream(logDir, "*.log.*")) {
            for (final Path path : dirStream) {
                final String filename = path.getFileName().toString();
                final Matcher matcher =
                    Pattern.compile("^([^_]*)_([^.]*)[.](.*)$").matcher(
                        filename);
                assertTrue("matcher: " + matcher, matcher.matches());
                final String name = matcher.group(1);
                assertTrue("Check name is in expected names: " + name,
                           expectedNames.contains(name));
                final String version = matcher.group(2);

                /* Current log files is not compressed */
                if (version.equals("0")) {
                    continue;
                }

                final String extension = matcher.group(3);

                /*
                 * Ignore store-wide log files with unique suffixes. This files
                 * are created when the admin master changes and all of the
                 * admins share the same logging directory, something that
                 * should only happen during testing.
                */
                if (extension.matches("log[.][0-9]+") &&
                    name.contains(storeName)) {
                    continue;
                }
                assertTrue("extension: " + extension,
                           extension.equals("log.gz") ||
                           extension.equals("log.tmp"));
                expectedNamesCheck.remove(name);
            }
        }

        assertTrue("expectedNamesCheck: " + expectedNamesCheck,
                   expectedNamesCheck.isEmpty());

        /* Disable compression and restart */
        for (final StorageNodeId sn : sns) {
            final ParameterMap changes = new ParameterMap(
                ParameterState.SNA_TYPE, ParameterState.SNA_TYPE);
            changes.setParameter(SN_LOG_FILE_COMPRESSION, "false");
            awaitPlanSuccess(cs,
                             cs.createChangeParamsPlan(
                                 "Disable log compression for " + sn,
                                 sn, changes));
        }
        createStore.shutdown();
        LoggerUtils.closeAllHandlers();
        createStore.restart();
        cs = createStore.getAdmin();

        /*
         * Log 10X to SNs, RNs, Admins, and Arbiters to make sure we get log
         * rotation
         */
        for (final StorageNodeId sn : sns) {
            for (int i = 0; i < 10; i++) {
                withRetry(() -> registryUtils.getStorageNodeAgentTest(sn))
                    .logMessage(INFO, LONG_MESSAGE, false /* useJeLogger */);
                withRetry(() -> registryUtils.getAdminTest(sn))
                    .logMessage(INFO, LONG_MESSAGE, false /* useJeLogger */);
            }
        }
        for (final RepNodeId rn : topo.getRepNodeIds()) {
            for (int i = 0; i < 10; i++) {
                withRetry(() -> registryUtils.getRepNodeTest(rn))
                    .logMessage(INFO, LONG_MESSAGE, false /* useJeLogger */);
            }
        }
        for (final ArbNodeId an : topo.getArbNodeIds()) {
            for (int i = 0; i < 10; i++) {
                withRetry(() -> registryUtils.getArbNodeTest(an))
                    .logMessage(INFO, LONG_MESSAGE, false /* useJeLogger */);
            }
        }

        /* Make sure there are no compressed files */
        try (final DirectoryStream<Path> dirStream =
             Files.newDirectoryStream(logDir, "*.log.*")) {
            for (final Path path : dirStream) {
                final String pathname = path.toString();

                /*
                 * But ignore store-wide log files, which may have compressed
                 * versions because all of the admins are logging to the same
                 * directory during testing
                 */
                if (pathname.contains(storeName)) {
                    continue;
                }
                assertTrue("pathname: " + pathname,
                           !pathname.endsWith(".log.gz") &&
                           !pathname.endsWith(".log.tmp"));
            }
        }
    }

    interface RemoteSupplier<T> {
        T get() throws RemoteException, NotBoundException;
    }

    /**
     * Call the supplier to fetch a remote service object, retrying for up to a
     * time limit if there is a RemoteException or NotBoundException.
     */
    static <T extends RemoteAPI> T withRetry(RemoteSupplier<T> supplier)
        throws RemoteException, NotBoundException
    {
        final long untilMillis = System.currentTimeMillis() + 30000;
        while (true) {
            try {
                return supplier.get();
            } catch (RemoteException|NotBoundException e) {
                if (System.currentTimeMillis() > untilMillis) {
                    throw e;
                }
            }
        }
    }
}
