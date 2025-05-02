/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.RequestTimeoutException;
import oracle.kv.Value;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.VerifyConfiguration.Problem;
import oracle.kv.impl.admin.VerifyConfiguration.RMIFailed;
import oracle.kv.impl.admin.VerifyConfiguration.RequestsDisabled;
import oracle.kv.impl.admin.VerifyConfiguration.ServiceStopped;
import oracle.kv.impl.admin.VerifyResults;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.rep.RequestTypeUpdater.RequestType;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.SecureTestBase;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.PartitionMap;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.util.TestUtils;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;
import oracle.kv.util.CreateStore.SecureUser;
import oracle.kv.util.Ping;
import oracle.kv.util.PingTest;
import oracle.kv.util.PingUtils;

import oracle.nosql.common.json.JsonNode;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class EnableRequestsTypeTest extends SecureTestBase {

    private static final String ADMIN_NAME = "admin";
    private static final String ADMIN_PW = "NoSql00__7654321";
    private static KVStore store;
    Map<RepGroupId, Key> testKeys = new HashMap<>();

    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        users.add(new SecureUser(ADMIN_NAME, ADMIN_PW, true /* admin */));
        numSNs = 2;
        repFactor = 1;
        partitions = 2;
        startup();
    }

    @AfterClass
    public static void staticTearDown()
        throws Exception {

        shutdown();
    }

    @Override
    public void setUp()
        throws Exception {

        TestStatus.setActive(true);
    }

    @Override
    public void tearDown()
        throws Exception {
    }

    @Test
    public void testChangeStoreRequestsType()
        throws Exception {

        prepareTest();
        testChangeRequestsType(null);
    }

    @Test
    public void testChangeShardRequestsType()
        throws Exception {

        prepareTest();
        testChangeRequestsType(new RepGroupId(1));
    }

    @Test
    public void testChangeStoreReadOnlyType()
        throws Exception {
        prepareTest();
        try {

            /* Disable write requests for the entire store */
            CommandServiceAPI cs = createStore.getAdmin();
            int planId = cs.createEnableRequestsPlan(
                "Enable Requests",
                "readonly",
                null, true);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            cs.assertSuccess(planId);
            checkReadOnlyNonePingOutput("readonly");
        } finally {
            createStore.restart();
            CommandServiceAPI cs = createStore.getAdmin();
            int planId = cs.createEnableRequestsPlan(
                "Enable Requests",
                "all",
                Collections.emptySet(),
                true);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            cs.assertSuccess(planId);
        }
    }

    @Test
    public void testChangeStoreNoneType()
        throws Exception {
        prepareTest();
        try {

            /* Disable both write and read requests for the entire store */
            CommandServiceAPI cs = createStore.getAdmin();
            int planId = cs.createEnableRequestsPlan(
                "Enable Requests",
                "none",
                null, true);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            cs.assertSuccess(planId);
            checkReadOnlyNonePingOutput("none");
        } finally {
            createStore.restart();
            CommandServiceAPI cs = createStore.getAdmin();
            int planId = cs.createEnableRequestsPlan(
                "Enable Requests",
                "all",
                Collections.emptySet(),
                true);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            cs.assertSuccess(planId);
        }
    }

    @Test
    public void testChangeRequestsTypeWithNodeDown()
        throws Exception {

        StorageNodeId stopSNId = createStore.getStorageNodeIds()[1];
        RepNodeId stopRN = createStore.getRNs(stopSNId).get(0);
        RepGroupId rgId = new RepGroupId(stopRN.getGroupId());
        StorageNodeAgent stopSN = createStore.getStorageNodeAgent(1);
        boolean rnStopped = false;
        try {
            /* Shut down RN */
            RepNodeAdminAPI targetNodeRna =
                createStore.getRepNodeAdmin(stopRN);
            assertTrue(stopSN.stopRepNode(stopRN, true));
            assertTrue(TestUtils.isShutdown(targetNodeRna));
            rnStopped = true;

            /*
             * Verify unreachable RN doesn't show incorrect enabled
             * request type. [#26695]
             */
            checkDefaultRequestsType(rgId);
            CommandServiceAPI cs = createStore.getAdminMaster();

            /* Verify there are two problems because of RN stopped */
            VerifyResults verifyResults =
                cs.verifyConfiguration(true, true, false);
            List<Problem> problems = verifyResults.getViolations();
            assertTrue(problems.size() == 2);
            for (Problem p : problems) {
                if (p instanceof RMIFailed) {
                    RMIFailed rf = (RMIFailed) p;
                    RepNodeId rnId = (RepNodeId)rf.getResourceId();
                    assertEquals(rnId, stopRN);
                } else if (p instanceof ServiceStopped) {
                    ServiceStopped ss = (ServiceStopped)p;
                    RepNodeId rnId = (RepNodeId)ss.getResourceId();
                    assertEquals(rnId, stopRN);
                }
            }

            /* Execute enable-requests plan, end with error */
            Topology topo = cs.getTopology();
            Parameters params = cs.getParameters();
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(os);
            int planId = cs.createEnableRequestsPlan(
                "Enable Requests",
                "none",
                Collections.emptySet(),
                true);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            Plan.State state = cs.awaitPlan(planId, 3000, TimeUnit.SECONDS);
            assertEquals(Plan.State.ERROR, state);

            /* Verify enabled request of other nodes has been changed */
            Ping.pingTopology(
                topo, params, false /* showHidden */,
                CommandParser.JSON_V1, ps,
                createStore.getAdminLoginManager(),
                null /* shard id */, logger);
            String pingResult = os.toString();
            ObjectNode json = JsonUtils.parseJsonObject(pingResult);
            Iterable<JsonNode> sns = JsonUtils.getArray(json, "snStatus");
            Iterator<JsonNode> snIter = sns.iterator();
            JsonNode sn = snIter.next();
            Iterable<JsonNode> rns = JsonUtils.getArray(sn, "rnStatus");
            Iterator<JsonNode> rnIter = rns.iterator();
            while (rnIter.hasNext()) {
                JsonNode rn = rnIter.next();
                String enabled = JsonUtils.getAsText(rn, "requestsEnabled");
                String rnIdInResult = JsonUtils.getAsText(rn, "resourceId");
                if (stopRN.toString().equals(rnIdInResult)) {
                    assertNull(enabled);
                } else {
                    assertEquals(RequestType.NONE.name(), enabled);
                }
            }
            checkDefaultRequestsType(rgId);

            /* Restart RN */
            assertTrue(stopSN.startRepNode(stopRN));
            TestUtils.isRunning(createStore, stopSNId, stopRN, logger);
            rnStopped = false;

            /*
             * Verify enabled request type of unavailable RN is
             * become none after restart
             */
            checkNoneRequstsEnabled(null);
        } finally {
            if (rnStopped) {
                assertTrue(stopSN.startRepNode(stopRN));
            }
            TestUtils.isRunning(createStore, stopSNId, stopRN, logger);
            CommandServiceAPI cs = createStore.getAdmin();
            int planId = cs.createEnableRequestsPlan(
                "Enable Requests",
                "all",
                Collections.emptySet(),
                true);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            cs.assertSuccess(planId);
        }
    }

    private void testChangeRequestsType(RepGroupId rgId)
        throws Exception {

        try {
            Set<RepGroupId> shardId = new HashSet<>();
            if (rgId != null) {
                shardId.add(rgId);
            }
            boolean entireStore = shardId.isEmpty();

            /* Disable all requests for the entire store */
            CommandServiceAPI cs = createStore.getAdmin();
            int planId = cs.createEnableRequestsPlan(
                "Enable Requests",
                "none",
                shardId,
                entireStore);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            cs.assertSuccess(planId);

            /* Expect write and read operations to be failed */
            checkNoneRequstsEnabled(rgId);

            /* Restart the store to check if enabled request type is changed */
            createStore.restart();
            cs = createStore.getAdmin();
            getStore();

            /* Expect write and read operations still to be failed */
            checkNoneRequstsEnabled(rgId);

            /* Enabled all requests */
            planId = cs.createEnableRequestsPlan(
                "Enable Requests",
                "all",
                shardId,
                entireStore);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            cs.assertSuccess(planId);
            checkAllRequestsEnabled();

            /* Make the store read-only */
            planId = cs.createEnableRequestsPlan(
                "Enable Requests",
                "readonly",
                shardId,
                entireStore);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            cs.assertSuccess(planId);
            checkReadOnly(rgId);

            /* Restart the store to check if enabled request type is changed */
            createStore.restart();
            cs = createStore.getAdmin();
            getStore();
            checkReadOnly(rgId);

            /* Enabled all requests */
            planId = cs.createEnableRequestsPlan(
                "Enable Requests",
                "all",
                shardId,
                entireStore);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            cs.assertSuccess(planId);
            checkAllRequestsEnabled();
        } finally {
            createStore.restart();
            CommandServiceAPI cs = createStore.getAdmin();
            int planId = cs.createEnableRequestsPlan(
                "Enable Requests",
                "all",
                Collections.emptySet(),
                true);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            cs.assertSuccess(planId);
        }
    }

    private void getStore() {
        final LoginCredentials creds =
            new PasswordCredentials(ADMIN_NAME, ADMIN_PW.toCharArray());
        final KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0]);
        final Properties props = new Properties();
        createStore.addTransportProps(props);
        kvConfig.setSecurityProperties(props);
        store = KVStoreFactory.getStore(kvConfig, creds, null);
    }

    private void prepareTest()
        throws Exception {

        grantRoles(ADMIN_NAME, "readwrite");
        getStore();
        CommandServiceAPI cs = createStore.getAdmin();
        Topology topo = cs.getTopology();
        Set<RepGroupId> shards = topo.getRepGroupIds();
        assertTrue(shards.size() == 2);

        /* Put one record for testing */
        for (RepGroupId repGroupId : shards) {
            testKeys.put(repGroupId, putKeyInShard(topo, repGroupId));
        }

        for (Key key : testKeys.values()) {
            assertNotNull(store.get(key));
        }
    }

    private Key putKeyInShard(Topology topo, RepGroupId repGroupId) {
        KVStoreImpl kvstoreImpl = (KVStoreImpl) store;
        PartitionMap pMap = topo.getPartitionMap();
        PartitionId pIdInShard = null;
        for (PartitionId pId : pMap.getAllIds()) {
            if (repGroupId.equals(pMap.getRepGroupId(pId))) {
                pIdInShard = pId;
            }
        }
        assertNotNull(pIdInShard);
        long keyValue = 0;
        Key key = null;
        while (true) {
            /* Loop until we get a value that lives on this RepNode. */
            keyValue++;
            key = Key.createKey(Long.toString(keyValue));
            PartitionId partitionId = kvstoreImpl.getPartitionId(key);
            if (partitionId.equals(pIdInShard)) {
                break;
            }
        }
        store.put(key, Value.createValue(new byte[0]));
        return key;
    }

    private void checkAllRequestsEnabled()
        throws Exception {

        CommandServiceAPI cs = createStore.getAdmin();
        Topology topo = cs.getTopology();
        Parameters params = cs.getParameters();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(os);
        Ping.pingTopology(
            topo, params, false /* showHidden */,
            CommandParser.JSON_V1, ps,
            createStore.getAdminLoginManager(),
            null /* shard id */, logger);

        for (Key key : testKeys.values()) {
            assertNotNull(store.put(key, Value.createValue(new byte[0])));
            assertNotNull(store.get(key));
        }
        /* Verify ping and verify show the expected text */
        String pingResult = os.toString();
        assertFalse(pingResult.contains(RequestType.NONE.name()));
        assertFalse(pingResult.contains(RequestType.READONLY.name()));
        VerifyResults verifyResults =
            cs.verifyConfiguration(true, true, false);
        List<Problem> problems = verifyResults.getViolations();
        assertTrue(problems.size() == 0);
    }

    private void checkNoneRequstsEnabled(RepGroupId rgId)
        throws Exception {

        checkPingVerifyOutput(rgId, RequestType.NONE.name());

        /* Expect write and read operations to be failed */
        if (rgId == null) {
            for (Key key : testKeys.values()) {
                try {
                    store.put(key, Value.createValue(new byte[0]));
                    fail("expected request timeout");
                } catch (RequestTimeoutException rte) {
                }
                try {
                    store.get(key);
                    fail("expected request timeout");
                } catch (RequestTimeoutException rte) {
                }
            }
        } else {
            Key key = testKeys.get(rgId);
            try {
                store.put(key, Value.createValue(new byte[0]));
                fail("expected request timeout");
            } catch (RequestTimeoutException rte) {
            }
            try {
                store.get(key);
                fail("expected request timeout");
            } catch (RequestTimeoutException rte) {
            }

            for (RepGroupId other : testKeys.keySet()) {
                if (!other.equals(rgId)) {
                    key = testKeys.get(other);
                    assertNotNull(
                        store.put(key, Value.createValue(new byte[0])));
                    assertNotNull(store.get(key));
                }
            }
        }

    }

    private void checkReadOnly(RepGroupId rgId)
        throws Exception {

        checkPingVerifyOutput(rgId, RequestType.READONLY.name());

        /* Expect write operations to fail and read operations
         * to succeed */
        if (rgId == null) {
            for (Key key : testKeys.values()) {
                try {
                    store.put(key, Value.createValue(new byte[0]));
                    fail("expected request timeout");
                } catch (RequestTimeoutException rte) {
                }
                assertNotNull(store.get(key));
            }
        } else {
            Key key = testKeys.get(rgId);
            try {
                store.put(key, Value.createValue(new byte[0]));
                fail("expected request timeout");
            } catch (RequestTimeoutException rte) {
            }
            assertNotNull(store.get(key));

            for (RepGroupId other : testKeys.keySet()) {
                if (!other.equals(rgId)) {
                    key = testKeys.get(other);
                    assertNotNull(
                        store.put(key, Value.createValue(new byte[0])));
                    assertNotNull(store.get(key));
                }
            }
        }
    }

    private void checkPingVerifyOutput(RepGroupId rgId, String expectedString)
        throws Exception {

        CommandServiceAPI cs = createStore.getAdmin();
        Topology topo = cs.getTopology();
        Parameters params = cs.getParameters();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(os);
        Ping.pingTopology(
            topo, params, false /* showHidden */,
            CommandParser.JSON_V1, ps,
            createStore.getAdminLoginManager(),
            null /* shard id */, logger);

        /* Verify ping and verify show the expected text */
        String pingResult = os.toString();
        ObjectNode jsonResults = JsonUtils.parseJsonObject(pingResult);
        Iterable<JsonNode> sns = JsonUtils.getArray(jsonResults, "snStatus");
        Iterator<JsonNode> snIter = sns.iterator();

        while (snIter.hasNext()) {
            JsonNode sn = snIter.next();
            Iterable<JsonNode> rns = JsonUtils.getArray(sn, "rnStatus");
            Iterator<JsonNode> rnIter = rns.iterator();
            while (rnIter.hasNext()) {
                JsonNode rn = rnIter.next();
                String enabled = JsonUtils.getAsText(rn, "requestsEnabled");
                if (rgId == null) {
                    assertEquals(expectedString, enabled);
                } else {
                    String rnId = JsonUtils.getAsText(rn, "resourceId");
                    if (rnId.contains(rgId.toString())) {
                        assertEquals(expectedString, enabled);
                    } else {
                        assertFalse(enabled.contains(expectedString));
                    }
                }
            }
        }
        VerifyResults verifyResults =
            cs.verifyConfiguration(true, true, false);
        List<Problem> problems = verifyResults.getViolations();
        int numProblems = (rgId == null) ? 2 : 1;
        assertTrue(problems.size() == numProblems);
        for (Problem p : problems) {
            assertTrue(p instanceof RequestsDisabled);

            if (rgId != null) {
                RequestsDisabled rd = (RequestsDisabled) p;
                RepNodeId rnId = (RepNodeId)rd.getResourceId();
                assertEquals(rnId.getGroupId(), rgId.getGroupId());
            }
        }
    }

    private void checkReadOnlyNonePingOutput(String enabledRequestType)
        throws Exception {

        CommandServiceAPI cs = createStore.getAdmin();
        Topology topo = cs.getTopology();
        Parameters params = cs.getParameters();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(os);
        Ping.pingTopology(
            topo, params, false /* showHidden */,
           -1, ps,
            createStore.getAdminLoginManager(),
            null /* shard id */, logger);

        /* Verify ping to show the expected text */
        String pingResult = os.toString();
        if (enabledRequestType.equals("readonly")) {
            PingTest.checkReadOnlyHumanOutput(pingResult,
                topo.getRepGroupIds().size(),
                topo.getRepNodeIds().size());
        } else {
            PingTest.checkOfflineHumanOutput(pingResult);
        }

        os = new ByteArrayOutputStream();
        ps = new PrintStream(os);
        Ping.pingTopology(
            topo, params, false /* showHidden */,
            CommandParser.JSON_V1, ps,
            createStore.getAdminLoginManager(),
            null /* shard id */, logger);

        pingResult = os.toString();
        if (enabledRequestType.equals("readonly")) {
            PingUtils.checkReadOnlyShardJsonOutput(pingResult, topo,
                topo.getRepGroupIds().size(),
                topo.getRepNodeIds().size());
        } else {
            PingUtils.checkOfflineShardJsonOutput(pingResult, topo);
        }
    }

    /*
     * Check default request type in ping output.
     */
    private void checkDefaultRequestsType(RepGroupId rgId)
        throws Exception {

        CommandServiceAPI cs = createStore.getAdminMaster();
        Topology topo = cs.getTopology();
        Parameters params = cs.getParameters();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(os);

        Ping.pingTopology(
            topo, params, false /* showHidden */,
            -1 /* No JSON */ , ps,
            createStore.getAdminLoginManager(),
            rgId, logger);
        String pingResult = os.toString();
        assertFalse(pingResult.contains("requests disabled"));
        assertFalse(pingResult.contains("readonly requests enabled"));
    }
}
