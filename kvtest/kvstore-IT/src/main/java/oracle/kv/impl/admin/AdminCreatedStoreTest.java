/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static oracle.kv.impl.admin.AdminUtils.awaitPlanSuccess;
import static oracle.kv.impl.util.TestUtils.assertMatch;
import static oracle.nosql.common.json.JsonUtils.getArray;
import static oracle.nosql.common.json.JsonUtils.getAsText;
import static oracle.nosql.common.json.JsonUtils.getObject;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import oracle.kv.Consistency;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.KVVersion;
import oracle.kv.StatementResult;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.VerifyConfiguration.BadDowngrade;
import oracle.kv.impl.admin.VerifyConfiguration.Problem;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.task.Utils;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.RequestDispatcherImpl;
import oracle.kv.impl.api.TopologyManager;
import oracle.kv.impl.api.table.TableSysTableUtil;
import oracle.kv.impl.api.table.TableTestBase;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.systables.TopologyHistoryDesc;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.PollConditionFunc;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.StorageNodeUtils;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.util.CreateStore;
import oracle.kv.util.PingUtils;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;

import org.junit.Test;

/**
 * An admin test base needs a store being created.
 *
 * Other admin tests do not start with a deployed store, it seems. This test
 * suite holds the tests that starts with a fully-deployed store.
 */
public class AdminCreatedStoreTest extends TestBase {

    private final static int startPort = 5000;
    private final static int haRange = 5;

    private CreateStore createStore = null;
    private KVStore kvstore = null;

    @Override
    public void tearDown() throws Exception {

        if (kvstore != null) {
            kvstore.close();
        }

        if (createStore != null) {
            createStore.shutdown(true);
        }

        RegistryUtils.clearRegistryCSF();
    }

    /* Tests */

    @Test
    public void testPruneWithThreadFailure() throws Exception {
        final int planLimit = 16;
        final String storeName = "kvstore";
        final int port =
            (new PortFinder(startPort, haRange)).getRegistryPort();

        Admin.RunTransaction.TRANSACTION_RETRY_MAX = 1;
        PlanStore.planLimitOverride = planLimit;
        PlanStore.maxPerScanOverride = 10;
        createStore =
            new CreateStore(
                storeName, port,
                3, /* nsns */
                3, /* rf */
                10, /* partitions */
                1, /* capacity */
                2, /* mb */
                true, /* use threads */
                null);
        createStore.setAdminLocations(1, 2, 3);
        final AtomicBoolean isPruneThreadRunning = new AtomicBoolean(false);
        PlanStore.prunePreRunTestHook = (thread) -> {
            isPruneThreadRunning.set(true);
        };
        PlanStore.prunePostScanTestHook = (thread) -> {
            thread.getLogger().info(String.format(
                "PRUNE_TEST: Starting prune scan"));
            if (thread.getNumPlansPruned() == 0) {
                /* At least prune some threads before we fail the thread. */
                return;
            }
            /*
             * Stopping the admins for follow-up scans so that the threads will
             * fail and the cached value will not be updated.
             */
            try {
                thread.getLogger().info(String.format(
                    "PRUNE_TEST: Pruned %s plans",
                    thread.getNumPlansPruned()));
                thread.getLogger().info(String.format(
                    "PRUNE_TEST: Stopping admins during plan pruning"));
                stopAdmin(1);
                stopAdmin(2);
                thread.getLogger().info(String.format(
                    "PRUNE_TEST: Stopped admins during plan pruning"));
            } catch (Throwable t) {
                thread.getLogger().info(String.format(
                    "PRUNE_TEST: Error stop admin: %s",
                    CommonLoggerUtils.getStackTrace(t)));
            }
        };
        final AtomicInteger planPruneCount = new AtomicInteger(0);
        final AtomicInteger cachedNumPlans = new AtomicInteger(0);
        final AtomicInteger actualNumPlans = new AtomicInteger(0);
        PlanStore.prunePostRunTestHook = (thread) -> {
            planPruneCount.getAndIncrement();
            cachedNumPlans.set((int)thread.getPlanStore().getCachedNumPlans());
            actualNumPlans.set((int)thread.getPlanStore().getActualNumPlans());
            thread.getLogger().info(String.format(
                "PRUNE_TEST: Prune plan finished, nPlans=%s, planDb#count=%s",
                cachedNumPlans.get(), actualNumPlans.get()));
            isPruneThreadRunning.set(false);
        };
        createStore.start();
        kvstore = KVStoreFactory.getStore(
            new KVStoreConfig(storeName, String.format("localhost:%s", port)));
        final CommandServiceAPI cs = createStore.getAdminMaster();
        int j = 0;
        for (int i = 0; i < 5; ++i) {
            /* Run plans until the scan count is not 0. */
            while (!isPruneThreadRunning.get()) {
                createTable(j++);
                Thread.sleep(100);
            }
            /* Waits for the plan pruning thread to fail */
            final int expectedPruneCount = i + 1;
            PollCondition.await(1000, Integer.MAX_VALUE, () -> {
                return planPruneCount.get() >= expectedPruneCount;
            });
            /* Starts up admins so that we could run more plans */
            startAdmin(1, 60);
            startAdmin(2, 60);
            if (actualNumPlans.get() < planLimit / 2) {
                break;
            }
        }
        final int lastPlanID =
            cs.getPlanIdRange(0, (new Date()).getTime(), 1)[0];
        assertEquals(
            Plan.State.SUCCEEDED,
            cs.awaitPlan(lastPlanID - planLimit / 2, 1, TimeUnit.SECONDS));
        assertEquals(
            actualNumPlans.get(),
            cachedNumPlans.get(), planLimit / 2);
    }

    @Test
    public void testPingForDisabledServiceStatus() throws Exception {
        final boolean runningAlone =
                isRunningAlone("testPingForDisabledServiceStatus");
        final String storeName = "kvstore";
        final int port =
                (new PortFinder(startPort, haRange)).getRegistryPort();

        CreateStore.ZoneInfo zoneInfo = new CreateStore.ZoneInfo(3);
        zoneInfo.setAllowArbiters(true);
        List<CreateStore.ZoneInfo> zoneInfos = new ArrayList<CreateStore.ZoneInfo>(1);
        zoneInfos.add(zoneInfo);

        createStore =
                new CreateStore(
                        storeName, port,
                        3, /* nsns */
                        2, /* rf */
                        10, /* partitions */
                        1, /* capacity */
                        2, /* mb */
                        true, /* use threads */
                        null);
        createStore.setAllowArbiters(true);
        createStore.setAdminLocations(1, 2, 3);
        createStore.start();

        CommandServiceAPI commandServiceAPI = createStore.getAdmin();

        String output = PingUtils.doPing(logger, "-host", "localhost", "-port",
                Integer.toString(port),
                "-no-exit");
        if (runningAlone) {
            System.out.println("Ping command output: " + output);
        }
        checkPingHumanOutput(output, commandServiceAPI.getTopology(), false,
                3);

        /* validate JSON output */
        output = PingUtils.doPing(logger, "-host", "localhost", "-port",
                Integer.toString(port),
                "-json", "-no-exit", "-hidden");
        if (runningAlone) {
            System.out.println("Ping command JSON output: " + output);
        }
        checkPingJsonOutput(output, false, 3);

        /* stop services */
        Set<ResourceId> stopServiceIds =
                getStorageNodeServices(2);
        awaitPlanSuccess(commandServiceAPI,
                commandServiceAPI.createStopServicesPlan("disable-services",
                        stopServiceIds), true);

        output = PingUtils.doPing(logger, "-host", "localhost", "-port",
                Integer.toString(port),
                "-no-exit");
        if (runningAlone) {
            System.out.println("Ping command output: " + output);
        }
        checkPingHumanOutput(output, commandServiceAPI.getTopology(), true, 3);

        /* validate JSON output */
        output = PingUtils.doPing(logger, "-host", "localhost", "-port",
                Integer.toString(port),
                "-json", "-no-exit", "-hidden");
        if (runningAlone) {
            System.out.println("Ping command JSON output: " + output);
        }
        checkPingJsonOutput(output, true, 3);
    }

    /**
     * Check that calling 'verify prerequisite' when the current version
     * supplied by the admin CLI is lower than the version running on an SNA
     * produces an error message that mentions the correct version numbers.
     * [KVSTORE-1803]
     */
    @Test
    public void testVerifyPrerequisiteDowngrade() throws Exception {
        createStore = new CreateStore(kvstoreName,
                                      startPort,
                                      1, /* numStorageNodes */
                                      1, /* replicationFactor */
                                      1, /* numPartitions */
                                      1); /* capacity */
        createStore.start();

        final CommandServiceAPI admin = createStore.getAdmin();
        final KVVersion prevMajorVersion =
            new KVVersion(KVVersion.CURRENT_VERSION.getMajor() - 1,
                          0, /* minor */
                          0, /* patch */
                          null /* name */);
        final StorageNodeId snId = createStore.getStorageNodeIds()[0];

        final VerifyResults results =
            admin.verifyPrerequisite(prevMajorVersion,
                                     KVVersion.PREREQUISITE_VERSION,
                                     Collections.singletonList(snId),
                                     false, /* showProgress */
                                     true, /* listAll */
                                     true /* json */);

        assertEquals(1, results.numViolations());
        final Problem violation = results.getViolations().get(0);
        assertEquals(BadDowngrade.class, violation.getClass());
        assertEquals(snId, violation.getResourceId());

        final String msg = violation.toString();
        final String pattern =
            String.format(".* version %s .* version %s .*",
                          prevMajorVersion.getNumericVersionString(),
                          KVVersion.CURRENT_VERSION.getNumericVersionString());
        assertTrue("Expected pattern '" + pattern + "', found '" + msg + "'",
                   msg.matches(pattern));
    }

    /* Other classes and methods */

    private Set<ResourceId> getStorageNodeServices(int disableAdminIndex) throws RemoteException {
        final Set<ResourceId> resultSet = new HashSet<>();
        Topology topo = createStore.getAdmin().getTopology();
        for (StorageNodeId snId : topo.getStorageNodeIds()) {
            resultSet.addAll(topo.getHostedRepNodeIds(snId));
            resultSet.addAll(topo.getHostedArbNodeIds(snId));
        }

        /* admin service */
        resultSet.add(createStore.getAdminId(disableAdminIndex));

        return resultSet;
    }

    private static void checkPingJsonOutput(String output,
                                            boolean isServicesDisabled,
                                            int disabledAdminId) {
        final JsonNode json = JsonUtils.parseJsonNode(output);
        for (JsonNode jsonSN : getArray(json, "snStatus")) {
            for (JsonNode jsonRN : getArray(jsonSN, "rnStatus")) {
                final String expectedStatus = getAsText(jsonRN,
                        "expectedStatus");
                assertEquals(isServicesDisabled ? "UNREACHABLE" : "RUNNING",
                        expectedStatus);
            }

            for (JsonNode jsonAN : getArray(json, "anStatus")) {
                final String expectedStatus = getAsText(jsonAN,
                        "expectedStatus");
                assertEquals(isServicesDisabled ? "UNREACHABLE" : "RUNNING",
                        expectedStatus);
            }

            final JsonNode jsonAdmin = getObject(jsonSN, "adminStatus");
            final String expectedStatus = getAsText(jsonAdmin, "expectedStatus");
            final String adminId = getAsText(jsonAdmin, "resourceId");
            final String disabledAdminIdStr = "admin" + disabledAdminId;
            if (adminId.equals(disabledAdminIdStr)) {
                assertEquals(isServicesDisabled ? "UNREACHABLE" : "RUNNING",
                        expectedStatus);
            }
        }
    }

    /** @see PingUtils#checkHumanOutput */
    private static void checkPingHumanOutput(String output, Topology topo,
                                             boolean isServicesDisabled,
                                             int disabledAdminId) {
        final List<String> lines = new ArrayList<String>();
        Collections.addAll(lines, output.split("\n"));

        /* Topology overview, skip*/
        lines.remove(0);
        lines.remove(0);
        lines.remove(0);

        /* Shard status, skip */
        lines.remove(0);

        /* Admin status, skip*/
        if (lines.get(0).matches("Admin Status: [a-z-]*")) {
            lines.remove(0);
        }

        /* Zone status, skip */
        for (int i = 0; i < topo.getDatacenterMap().getAllIds().size(); i++) {
            lines.remove(0);
        }

        /* SN status */
        final List<StorageNodeId> snIds = topo.getStorageNodeIds();
        Collections.sort(snIds);
        for (final StorageNodeId snId : snIds) {
            lines.remove(0); /* SN status, skip*/

            String adminId = "admin" + disabledAdminId;

            if (!lines.get(0).contains(adminId)) {
                lines.remove(0);  /* Admin status, skip*/
            }
            else {
                /* admin Status */
                final String adminLine = lines.remove(0);
                if (isServicesDisabled) {
                    assertTrue(adminLine.contains("Stopped"));
                }
                else {
                    assertFalse(adminLine.contains("Stopped"));
                }
            }

            /* RN status */
            final List<RepNodeId> rnIds = new ArrayList<>(topo.getHostedRepNodeIds(snId));
            Collections.sort(rnIds);
            for (final RepNodeId rnId : rnIds) {
                final String rnLine = lines.remove(0);
                assertMatch(rnId.toString(), "\tRep Node \\[" + rnId + "\\].*", rnLine);

                if (isServicesDisabled) {
                    assertTrue(rnLine.contains("Stopped"));
                }
                else {
                    assertFalse(rnLine.contains("Stopped"));
                }
            }

            /* AN status */
            final List<ArbNodeId> arIds = new ArrayList<ArbNodeId>(topo.getHostedArbNodeIds(snId));
            Collections.sort(arIds);
            for (final ArbNodeId arId : arIds) {
                final String anLine = lines.remove(0);
                assertMatch(arId.toString(), "\tArb Node \\[" + arId + "\\].*", anLine);

                if (isServicesDisabled) {
                    assertTrue(anLine.contains("Stopped"));
                }
                else {
                    assertFalse(anLine.contains("Stopped"));
                }
            }
        }

        assertEquals("No more output", 0, lines.size());
    }

    private void stopAdmin(int index)
        throws Exception
    {
        createStore.getStorageNodeAgent(index)
            .stopAdmin(new AdminId(index + 1), true);
        waitForAdminStop(index);
    }

    private void waitForAdminStop(int index)
        throws Exception
    {
        PollCondition.await(1000, 60000, () -> {
            try {
                final CommandServiceAPI cs = createStore.getAdmin(index);
                if (cs.getAdminStatus().getServiceStatus()
                    .equals(ServiceStatus.RUNNING)) {
                    return true;
                }
                return true;
            } catch (Throwable t) {
                return true;
            }
        });
    }

    private StatementResult createTable(long tableID) throws Exception
    {
        int remainingRetryCount = 10;
        final String ddl =
                String.format("CREATE TABLE IF NOT EXISTS simpleUsers%s " +
                              "(firstName STRING, " +
                                " lastName STRING, " +
                                " userID INTEGER, " +
                                " PRIMARY KEY (userID))", tableID);
        while (true) {
            try {
                final StatementResult result = kvstore.executeSync(ddl);
                return result;
            } catch (Exception e) {
                if (remainingRetryCount-- < 0) {
                    throw e;
                }
                Thread.sleep(1000);
            }
        }
    }

    private void startAdmin(int index,
                            int timeoutSecs) throws Exception {
        final StorageNodeAgent sna =
            createStore.getStorageNodeAgent(index);
        sna.startAdmin(
            ConfigUtils.getAdminParams(sna.getKvConfigFile()));
        createStore.getStorageNodeAgent(index)
            .waitForAdmin(ServiceStatus.RUNNING, timeoutSecs);
    }

    @Test
    public void testWriteTopologyHistory() throws Exception {
        createStoreForTopologyHistoryTests();
        /*
         * It seems not all topology generated at the Admin is broadcasted.
         * Using the hook to obtain the broadcast set.
         */
        final Set<Integer> broadcastedTopos =
            Collections.synchronizedSet(new HashSet<>());
        Utils.beforeBroadcastTopoTestHook = (topo) -> {
            broadcastedTopos.add(topo.getSequenceNumber());
        };
        final StorageNodeAgent[] extraSNAs = expandStore();
        contractStore(extraSNAs);
        broadcastedTopos.forEach((s) -> verifyTopologyWritten(s));
    }

    private void createStoreForTopologyHistoryTests() throws Exception {
        final String storeName = "kvstore";
        final int port =
            (new PortFinder(startPort, haRange)).getRegistryPort();
        createStore =
            new CreateStore(
                storeName, port,
                3, /* nsns */
                3, /* rf */
                10, /* partitions */
                1, /* capacity */
                2, /* mb */
                true, /* use threads */
                null);
        createStore.start();
        kvstore = KVStoreFactory.getStore(
            new KVStoreConfig(storeName, String.format("localhost:%s", port)));
        final TableAPI api = kvstore.getTableAPI();
        /* Create a user table so that system table is functioning. */
        final String ddl =
            "CREATE TABLE dummy "
            + "(id INTEGER, name STRING, PRIMARY KEY (id))";
        kvstore.executeSync(ddl);
        TableTestBase.waitForTable(api, TopologyHistoryDesc.TABLE_NAME);
    }

    private int verifyTopologyWritten(int sequenceNumber) {
        logger.info(String.format("verifying topology #%s", sequenceNumber));
        final Row row = getTopologyRow(sequenceNumber);
        assertTrue(String.format("topology of #%s is not written",
                                 sequenceNumber),
                   row != null);
        final byte[] bytes =
            row.get(TopologyHistoryDesc.COL_NAME_SERIALIZED_TOPOLOGY)
            .asBinary().get();
        final Topology actualTopology =
            SerializationUtil.getObject(bytes, Topology.class);
        assertEquals(sequenceNumber,
                     actualTopology.getSequenceNumber());
        return sequenceNumber;
    }

    private Row getTopologyRow(int sequenceNumber) {
        final TableAPI api = kvstore.getTableAPI();
        final Table table = api.getTable(TopologyHistoryDesc.TABLE_NAME);
        final PrimaryKey key = table.createPrimaryKey();
        key.put(TopologyHistoryDesc.COL_SHARD_KEY,
                TopologyHistoryWriteSysTableUtil.SHARD_KEY);
        key.put(TopologyHistoryDesc.COL_NAME_TOPOLOGY_SEQUENCE_NUMBER,
                sequenceNumber);
        return api.get(key, new ReadOptions(Consistency.ABSOLUTE, 0, null));
    }

    /**
     * Expands the store by adding 3 more sns.
     */
    private StorageNodeAgent[] expandStore() throws Exception {
        final StorageNodeAgent[] snas = new StorageNodeAgent[3];
        final CommandServiceAPI cs = createStore.getAdmin();
        final String hostname = createStore.getHostname();
        final String poolname = CreateStore.STORAGE_NODE_POOL_NAME;
        final int portsPerFinder = 20;
        /* deploy 3 more sns */
        for (int i = 0; i < 3; ++i) {
            final PortFinder pf = new PortFinder(
                startPort + (3 + i) * portsPerFinder, haRange, hostname);
            final int port = pf.getRegistryPort();
            snas[i] = StorageNodeUtils.createUnregisteredSNA(
                createStore.getRootDir(), pf, 1 /* capacity */,
                String.format("config%s.xml", i + 3),
                true /* useThreads */, false /* createAdmin */,
                null /* mgmtImpl */, 0 /* mgmtPollPort */,
                0 /* mgmtTrapPort */, 2 /* mb */);
            StorageNodeUtils.waitForAdmin(hostname, port);
            final int planId = cs.createDeploySNPlan(
                String.format("deploy sn%s", i + 4), new DatacenterId(1),
                hostname, port, "comment");
            runPlan(planId);
            final StorageNodeId snid = snas[i].getStorageNodeId();
            cs.addStorageNodeToPool(poolname, snid);
        }
        final String expandTopoName = "expand";
        cs.copyCurrentTopology(expandTopoName);
        cs.redistributeTopology(expandTopoName, poolname);
        final int planId = cs.createDeployTopologyPlan(
            "deploy expansion", expandTopoName, null);
        runPlan(planId);
        return snas;
    }

    private void runPlan(int planId) throws Exception {
        final CommandServiceAPI cs = createStore.getAdmin();
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
    }

    /**
     * Contracts the store by removing the previously added 3 sns.
     */
    private void contractStore(StorageNodeAgent[] extraSNAs) throws Exception {
        final CommandServiceAPI cs = createStore.getAdmin();
        final String poolname = CreateStore.STORAGE_NODE_POOL_NAME;
        for (int i = 0; i < 3; ++i) {
            cs.removeStorageNodeFromPool(poolname, new StorageNodeId(i + 4));
        }
        String contractTopoName = "contract";
        cs.copyCurrentTopology(contractTopoName);
        cs.contractTopology(contractTopoName, poolname);
        int planId = cs.createDeployTopologyPlan(
            "deploy contraction", contractTopoName, null);
        runPlan(planId);
        for (int i = 0; i < 3; ++i) {
            planId = cs.createRemoveSNPlan(
                String.format("remove sn%s", i + 4),
                new StorageNodeId(i + 4));
            runPlan(planId);

            extraSNAs[i].shutdown(true, true, "contration");
            Files.deleteIfExists(
                Paths.get(createStore.getRootDir(),
                          String.format("config%s.xml", i + 3)));
            FileUtils.deleteDirectory(
                Paths.get(createStore.getRootDir(),
                          createStore.getStoreName(),
                          String.format("sn%s", i + 4))
                .toFile());
        }
    }

    /** Tests topology history read. */
    @Test
    public void testReadTopologyHistory() throws Exception {
        createStoreForTopologyHistoryTests();
        final TopologyManager topologyManager =
            ((KVStoreImpl) kvstore).getDispatcher().getTopologyManager();
        /* Expand the store to bump the sequence number. */
        expandStore();
        final int seq = getCurrentTopologySequenceNumber();
        topologyManager.getCache().clearAll();
        /* Registers the hook. */
        final AtomicInteger readStoreCount = new AtomicInteger(0);
        final int sleepTimeMillis = 1000;
        TopologyManager.beforeReadTopologyFromStore = (v) -> {
            try {
                Thread.sleep(sleepTimeMillis);
            } catch (InterruptedException e) {
                /* ignore */
            }
            readStoreCount.getAndIncrement();
        };
        /* Issues several history topology reads concurrently. */
        final int nthreads = 16;
        final Thread[] threads = new Thread[nthreads];
        final AtomicInteger errorCount = new AtomicInteger(0);
        for (int i = 0; i < 16; ++i) {
            final Thread thread =
                (new Thread(() -> {
                    try {
                        final Topology topo = topologyManager
                            .getTopology(kvstore, seq, 2 * sleepTimeMillis);
                        assertEquals(seq, topo.getSequenceNumber());
                    } catch (Throwable t) {
                        logger.info(String.format("Got exception: %s", t));
                        errorCount.getAndIncrement();
                    }
                }));
            threads[i] = thread;
            thread.start();
        }
        for (int i = 0; i < 16; ++i) {
            threads[i].join();
        }
        assertEquals("wrong read store count", 1, readStoreCount.get());
        assertEquals("wrong error count", 0, errorCount.get());
        /* Reads again and it should be cached. */
        topologyManager.getTopology(kvstore, seq, 100 /* 100 ms */);
        assertEquals("wrong read store count", 1, readStoreCount.get());
        /* Checks if we removed outstanding reads correctly. */
        assertEquals("wrong outstanding reads count",
                     0, topologyManager.getOutstandingTopologyReadsCount());
    }

    private int getCurrentTopologySequenceNumber() throws Exception {
        /**
         * Executes a noop to rg1-rn1 so that our topology gets updated.
         * Choosing rg1-rn1 since rg2 might be removed during contraction.
         */
        final RequestDispatcherImpl dispatcher =
            ((RequestDispatcherImpl)((KVStoreImpl) kvstore).getDispatcher());
        dispatcher.executeNOP(
            dispatcher.getRepGroupStateTable()
            .getNodeState(new RepNodeId(1, 1)),
            5000, null);
        return ((KVStoreImpl) kvstore).getTopology().getSequenceNumber();
    }

    /**
     * Tests that an interruption happening during the executor close at the end
     * of the system table scan will not cause admin restart issue.
     *
     * [KVSTORE-2425].
     */
    @Test
    public void testSysTableScan() throws Exception {
        MDTableUtil.deletedThresholdOverride = Optional.of(5);
        TableSysTableUtil.batchResultSizeOverride = Optional.of(1);
        tearDowns.add(() -> {
            MDTableUtil.deletedThresholdOverride = Optional.empty();
            TableSysTableUtil.batchResultSizeOverride = Optional.empty();
        });

        /* Start the store. */
        final String storeName = "kvstore";
        final int port =
            (new PortFinder(startPort, haRange)).getRegistryPort();
        createStore = new CreateStore(storeName, port, 3, /* nsns */
            3, /* rf */
            10, /* partitions */
            1, /* capacity */
            2, /* mb */
            true, /* use threads */
            null, /* mgmtImpl*/
            false, /* mgmtPortsShared */
            true /* secure */);
        createStore.setAdminLocations(1, 2, 3);
        createStore.start();
        final int ntables = 10;
        assertTrue(MDTableUtil.waitForStoreReady(createStore.getAdmin(), true,
            1000, 60000));
        kvstore = KVStoreFactory.getStore(createStore.createKVConfig());

        logger.info("create tables");
        for (int i = 0; i < ntables; i++) {
            kvstore.executeSync("CREATE TABLE Users" + i +
                    " (id INTEGER, firstName STRING, lastName STRING," +
                    " PRIMARY KEY (id))");
        }

        logger.info("drop tables");
        for (int i = 0; i < ntables; i++) {
            kvstore.executeSync("DROP TABLE Users" + i);
        }

        logger.info("create one more table");

        kvstore.executeSync("CREATE TABLE Users" + ntables +
                " (id INTEGER, firstName STRING, lastName STRING," +
                " PRIMARY KEY (id))");

        final CommandServiceAPI masterAdmin = createStore.getAdminMaster();
        masterAdmin.stop(true, "kill master");

        logger.info("kill admin master");
        final PollConditionFunc checkMasterAlive =
            () -> (createStore.getAdminMaster() != null);

        assertTrue("new master not alive",
            PollCondition.await(1000, 5000, checkMasterAlive));

        kvstore.executeSync("DROP TABLE Users" + ntables);

        assertTrue("new master not alive",
            PollCondition.await(1000, 5000, checkMasterAlive));
    }
}
