/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv;

import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;
import java.util.regex.Pattern;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.test.RemoteTestAPI;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.impl.util.FilterableParameterized;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.impl.util.registry.AsyncControl;
import oracle.kv.impl.util.registry.RegistryUtils;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Test KVLocal when services fail
 */
@RunWith(FilterableParameterized.class)
public class KVLocalFailureTest extends KVLocalTestModeBase {
    private static final boolean deleteTestDirOnExit = true;

    private KVLocal local;
    private File testDir;

    public KVLocalFailureTest(TestMode testMode) {
        super(testMode);
    }

    @BeforeClass
    public static void ensureAsyncEnabled() {
        assumeTrue("KVLocal requires async", AsyncControl.serverUseAsync);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        /* Cleanup running store */
        if (local != null) {
            try {
                local.stop();
            } catch (Exception e) {
            }
            local = null;
        }
        if (deleteTestDirOnExit && (testDir != null)) {
            FileUtils.deleteDirectory(testDir);
        }
    }

    @Test
    public void testRepNodeFailure() throws Exception {
        final String rootDir = makeTestDir("killRepNode");
        final KVLocalConfig config = getConfigBuilder(rootDir).build();
        local = KVLocal.start(config);
        final KVStore store = local.getStore();

        final CommandServiceAPI admin = RegistryUtils.getAdmin(
            config.getHostName(), config.getPort(),
            KVLocal.getAdminLoginMgr(config));
        final Topology topology = admin.getTopology();
        final RegistryUtils registryUtils = new RegistryUtils(topology, null,
                                                              logger);

        final RepNodeId rnId = new RepNodeId(1, 1);
        final RemoteTestAPI testAPI = registryUtils.getRepNodeTest(rnId);

        /*
         * Tell the RepNode service to invalidate it's JE environment.
         * The simulated EnvironmentFailureException will crash RepNode
         * thread.
         */
        if (!testAPI.processInvalidateEnvironment(true)) {
            throw new RuntimeException(
                "Failed to invalidate environment");
        }

        /* Wait for the RN process to exit */
        assertTrue(
            PollCondition.await(
                1000, 30000,
                () ->
                ServiceUtils.ping(rnId, topology, logger) ==
                ServiceStatus.UNREACHABLE));

        /* Since RepNode service is crashed, the key/value API should fail */
        checkException(() -> testKeyValueAPI(
                           store, UUID.randomUUID().toString()),
                       FaultException.class);

        /*
         * The invalidation failure means the RN will not restart automatically
         */
        Thread.sleep(2000);
        checkException(() -> testKeyValueAPI(
                           store, UUID.randomUUID().toString()),
                       RequestTimeoutException.class);

        final String cantConnectPattern =
            "(Problem connecting|Unable to connect) to the storage node" +
            " agent";
        final String verifyResult = local.verifyConfiguration(false);
        assertTrue("Problem with verify result: " + verifyResult,
                   Pattern.compile(cantConnectPattern, Pattern.MULTILINE)
                   .matcher(verifyResult)
                   .find());

        // TODO: If there is a problem in the verify data path, it hangs for a
        // very long time. Looks like it waits 2 minutes for each RN, and does
        // that 10 times -- too much time for a unit test.
        // checkException(() -> local.verifyData(),
        //                KVLocalException.class);

        checkException(() -> local.createSnapshot("sp1"),
                       KVLocalException.class,
                       "Problem creating snapshot");

        /* Restart store. Everything should work again. */
        local.stop();
        local = KVLocal.startExistingStore(rootDir);
        testKeyValueAPI(store, UUID.randomUUID().toString());
        String verifyConfig2 = local.verifyConfiguration(false);
        assertTrue(verifyConfig2, verifyConfig2.contains("{}"));
    }

    @Test
    public void testAdminFailure() throws Exception {
        final String rootDir = makeTestDir("killAdmin");
        final KVLocalConfig config = getConfigBuilder(rootDir).build();
        local = KVLocal.start(config);
        final KVStore store = local.getStore();

        final CommandServiceAPI admin = RegistryUtils.getAdmin(
            config.getHostName(), config.getPort(),
            KVLocal.getAdminLoginMgr(config));
        final Topology topology = admin.getTopology();
        final RegistryUtils ru = new RegistryUtils(topology, null, logger);
        final RemoteTestAPI testAPI = ru.getAdminTest(new StorageNodeId(1));

        /*
         * Tell the admin service to invalidate it's JE environment. The
         * simulated EnvironmentFailureException will crash the admin and it
         * will not restart automatically.
         */
        if (!testAPI.processInvalidateEnvironment(true)) {
            throw new RuntimeException(
                "Failed to invalidate environment");
        }

        /* Key/value API should still work */
        testKeyValueAPI(store, UUID.randomUUID().toString());

        /* verifyConfiguration() should throw exception */
        checkException(() -> local.verifyConfiguration(false),
                       KVLocalException.class,
                       "Exception getting admin command service|" +
                       "Problem verifying configuration");

        local.stop();
        local = KVLocal.start(config);
        testKeyValueAPI(store, UUID.randomUUID().toString());

        /* Restart store. Everything should work again. */
        final String verifyConfig = local.verifyConfiguration(false);
        assertTrue(verifyConfig, verifyConfig.contains("{}"));
    }

    private void testKeyValueAPI(KVStore store, String random) {
        final Key key = Key.createKey("key: " + random);
        final String valueString = "value: " + random;
        final Value value = Value.createValue(valueString.getBytes());

        store.put(key, value);

        final ValueVersion valueVersion = store.get(key);
        assertEquals(value, valueVersion.getValue());
    }

    private String makeTestDir(String subDir) throws IOException {
        testDir = Files.createTempDirectory(subDir).toFile();
        return testDir.toString();
    }
}
