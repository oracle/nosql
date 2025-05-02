/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.Value;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.plan.StatusReport;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.SecureTestBase;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.util.CreateStore.SecureUser;
import oracle.kv.util.CreateStore.ZoneInfo;

import com.sleepycat.je.rep.impl.networkRestore.NetworkBackupStatDefinition;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class RestoreRepNodeTest extends SecureTestBase {

    private static final String ADMIN_NAME = "admin";
    private static final String ADMIN_PW = "NoSql00__7654321";
    private static final String FAULT_MSG = "Injecting fault";
    private static final String BACKUP_FAULT_MSG = "Backup injecting fault";

    private static KVStore store;
    private RepNodeId targetRestoreNode;
    Map<StorageNodeAgent, RepNodeId> shutdownNodes = new HashMap<>();
    private boolean grantRole = false;

    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        users.add(new SecureUser(ADMIN_NAME, ADMIN_PW, true /* admin */));
        numSNs = 4;

        /* Two zone, primary zone RF=2, secondary zone RF=1*/
        zoneInfos = asList(new ZoneInfo(3),
                           new ZoneInfo(1, DatacenterType.SECONDARY));

        /* Need to inject test hook */
        useThreads = true;
    }

    @AfterClass
    public static void staticTearDown()
        throws Exception {

        shutdown();
    }

    @Override
    public void setUp()
        throws Exception {

        startup();
    }

    @Override
    public void tearDown()
        throws Exception {

        RepEnvHandleManager.NETWORK_BACKUP_HOOK = null;
        RepEnvHandleManager.FAULT_HOOK = null;
        createStore.getStorageNodeAgent(0).resetRMISocketPolicies();
        shutdown();
    }

    @Test
    public void testBasic()
        throws Exception {

        PollCondition checkSucceed = new PollCondition(1000, 90000) {
            @Override
            protected boolean condition() {

                try {
                    RepNodeAdminAPI targetNodeRna =
                        createStore.getRepNodeAdmin(targetRestoreNode);
                    NetworkRestoreStatus status =
                        targetNodeRna.getNetworkRestoreStatus();

                    if (status == null) {
                        return false;
                    }
                    Exception e = status.getException();
                    return status.isCompleted() && e == null;
                } catch (Exception re) {
                    throw new RuntimeException(re);
                }
            }
        };
        doTest(checkSucceed, "basic");
        restartShutdownRepNodes();
    }

    @Test
    public void testFailedNetworkRestore()
        throws Exception {

        RepEnvHandleManager.FAULT_HOOK = new ErrorHook(1);
        PollCondition checkFaults = new PollCondition(1000, 90000) {
            @Override
            protected boolean condition() {

                try {
                    RepNodeAdminAPI targetNodeRna =
                        createStore.getRepNodeAdmin(targetRestoreNode);
                    NetworkRestoreStatus status =
                        targetNodeRna.getNetworkRestoreStatus();

                    if (status == null) {
                        return false;
                    }
                    Exception e = status.getException();
                    return status.isCompleted() &&
                           e != null &&
                           (e.getMessage().indexOf(FAULT_MSG) >= 0);
                } catch (Exception re) {
                    throw new RuntimeException(re);
                }
            }
        };
        doTest(checkFaults, "FailureTest");
        restartShutdownRepNodes();
    }

    @Test
    public void testNetworkBackupRetry()
        throws Exception {

        int faultNum = 3;
        RepEnvHandleManager.NETWORK_BACKUP_HOOK =
            new BackupHook(faultNum);

        PollCondition checkRetry = new PollCondition(1000, 90000) {
            @Override
            protected boolean condition() {

                try {
                    RepNodeAdminAPI targetNodeRna =
                        createStore.getRepNodeAdmin(targetRestoreNode);
                    NetworkRestoreStatus status =
                        targetNodeRna.getNetworkRestoreStatus();

                    if (status == null) {
                        return false;
                    }
                    Exception e = status.getException();
                    return status.isCompleted() &&
                           e == null &&
                           (status.getRetryNum() == (faultNum + 1));
                } catch (Exception re) {
                    throw new RuntimeException(re);
                }
            }
        };
        doTest(checkRetry, "BackupRetry");
        restartShutdownRepNodes();
    }

    @Test
    public void testNetworkRestorePlan()
        throws Exception {

        RestorePair pair = prepareTest("RestorePlan");

        /* Run the plan, but pause network backup in the middle */
        CountDownLatch waitForRelease = new CountDownLatch(1);
        RepEnvHandleManager.FAULT_HOOK = new StallHook(waitForRelease);
        final CommandServiceAPI cs = createStore.getAdmin();
        int planId = cs.createNetworkRestorePlan(
            "Network restore",
            pair.sourceNode,
            pair.targetNode,
            false /* retain logs */);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        final String statsKeyword =
            NetworkBackupStatDefinition.TRANSFER_RATE.getName();

        /* Check getting plan detail and backup statistics */
        new PollCondition(1000, 90000) {
            @Override
            protected boolean condition() {
                String status;
                try {
                    status = cs.getPlanStatus(
                        planId, StatusReport.VERBOSE_BIT, false /* JSON */);
                    if (status.indexOf(statsKeyword) >= 0) {
                        return true;
                    }
                    return false;
                } catch (RemoteException e) {
                    throw new RuntimeException(e);
                }
            }
        }.await();

        /* release latch to let plan proceed */
        waitForRelease.countDown();
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
        restartShutdownRepNodes();
    }

    @Test
    public void testNetworkRestorePlanError()
        throws Exception {

        CommandServiceAPI cs = createStore.getAdmin();
        try {
            cs.createNetworkRestorePlan(
                "Network restore",
                new RepNodeId(1, 1),
                new AdminId(1),
                false /* retain logs */);
            fail("expected exception");
        } catch (AdminFaultException afe) {
            assertThat("type mismatch", afe.getMessage(),
                        containsString("other node with different type"));
        }

        try {
            cs.createNetworkRestorePlan(
                "Network restore",
                new AdminId(2),
                new AdminId(1),
                false /* retain logs */);
            fail("expected exception");
        } catch (AdminFaultException afe) {
            assertThat("not RN", afe.getMessage(),
                        containsString("Only RepNode can be restored"));
        }

        try {
            cs.createNetworkRestorePlan(
                "Network restore",
                new RepNodeId(1, 1),
                new RepNodeId(1, 1),
                false /* retain logs */);
            fail("expected exception");
        } catch (AdminFaultException afe) {
            assertThat("restore itself", afe.getMessage(),
                        containsString("is the same one"));
        }

        try {
            cs.createNetworkRestorePlan(
                "Network restore",
                new RepNodeId(1, 1),
                new RepNodeId(2, 1),
                false /* retain logs */);
            fail("expected exception");
        } catch (AdminFaultException afe) {
            assertThat("not the same rep group", afe.getMessage(),
                        containsString("not in the same replication"));
        }

        RepNodeId source = null;
        RepNodeId target = null;
        for (int i = 0; i < numSNs; i++) {
            StorageNodeAgent sna = createStore.getStorageNodeAgent(i);
            StorageNodeId snId = sna.getStorageNodeId();
            RepNodeId rnId = createStore.getRNs(snId).get(0);

            if (createStore.getRepNodeAdmin(rnId).
                    ping().getIsAuthoritativeMaster()) {

                target = rnId;
            } else if (source == null) {
                source = rnId;
            }
        }
        int planId = 0;
        try {
            planId = cs.createNetworkRestorePlan(
                "Network restore",
                source,
                target,
                false /* retain logs */);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            fail("expected exception");
        } catch (AdminFaultException afe) {
            assertThat("unable to restore a master", afe.getMessage(),
                        containsString("Unable to restore a master"));
        } finally {
            cs.cancelPlan(planId);
        }

        /*
         * Prepare test, found candidates of network restore, but switch
         * the source and target nodes.
         */
        RestorePair pair = prepareTest("RestorePlanError");
        try {
            planId = cs.createNetworkRestorePlan(
                "Network restore",
                pair.targetNode,
                pair.sourceNode,
                false /* retain logs */);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            fail("expected exception");
        } catch (AdminFaultException afe) {
            assertThat("no more recent data in source", afe.getMessage(),
                        containsString("doesn't have more recent data"));
        } finally {
            cs.cancelPlan(planId);
        }

        /* Find a shutdown RepNode */
        RepNodeId deadNode = null;
        for (int i = 0; i < numSNs; i++) {
            StorageNodeAgent sna = createStore.getStorageNodeAgent(i);
            StorageNodeId snId = sna.getStorageNodeId();
            RepNodeId rnId = createStore.getRNs(snId).get(0);

            try {
                createStore.getRepNodeAdmin(rnId);
            } catch (Exception expected) {
                deadNode = rnId;
                break;
            }
        }

        try {
            planId = cs.createNetworkRestorePlan(
                "Network restore",
                deadNode,
                pair.sourceNode,
                false /* retain logs */);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            fail("expected exception");
        } catch (AdminFaultException afe) {
            assertThat("node is not alive", afe.getMessage(),
                        containsString("is not alive"));
        } finally {
            cs.cancelPlan(planId);
        }
        restartShutdownRepNodes();
    }

    /**
     * Test that the network restore plan waits for the network restore to
     * complete.
     */
    @Test
    public void testNetworkRestoreRace()
        throws Exception {

        RestorePair pair = prepareTest("RestorePlan");

        final RepNodeAdminAPI sourceRNAdmin =
            createStore.getRepNodeAdmin(pair.sourceNode);
        final RepNodeAdminAPI targetRNAdmin =
            createStore.getRepNodeAdmin(pair.targetNode);
        final long sourceVLSN = sourceRNAdmin.ping().getVlsn();
        final long targetVLSN = targetRNAdmin.ping().getVlsn();
        assertTrue("sourceVLSN:" + sourceVLSN +
                   " targetVLSN:" + targetVLSN,
                   sourceVLSN > targetVLSN);

        final CommandServiceAPI cs = createStore.getAdmin();
        int planId = cs.createNetworkRestorePlan(
            "Network restore", pair.sourceNode, pair.targetNode,
            false /* retain logs */);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        /*
         * If the plan is done, the target VLSN should have advanced to at
         * least the original source VLSN
         */
        final long currentTargetVLSN = targetRNAdmin.ping().getVlsn();
        assertTrue("sourceVLSN:" + sourceVLSN +
                   " currentTargetVLSN:" + currentTargetVLSN,
                   currentTargetVLSN >= sourceVLSN);

        restartShutdownRepNodes();
    }

    private void doTest(PollCondition pollCondition, String keyPrefix)
        throws Exception {

        RestorePair pair = prepareTest(keyPrefix);
        RepNodeAdminAPI sourceNodeRna =
            createStore.getRepNodeAdmin(pair.sourceNode);

        /* Restore stopped node */
        long vlsn = sourceNodeRna.ping().getVlsn();
        RepNodeAdminAPI targetNodeRna =
             createStore.getRepNodeAdmin(pair.targetNode);
        targetNodeRna.startNetworkRestore(pair.sourceNode, true, vlsn);
        targetRestoreNode = pair.targetNode;
        boolean result = pollCondition.await();
        assertTrue(result);
    }

    private static void initStore()
        throws Exception {

        if (store != null) {
            store.close();
            store = null;
        }
        LoginCredentials creds =
            new PasswordCredentials(ADMIN_NAME, ADMIN_PW.toCharArray());
        KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0]);
        Properties props = new Properties();
        addTransportProps(props);
        kvConfig.setSecurityProperties(props);

        int nretries = 0;
        while (store == null) {
            try {
                store = KVStoreFactory.getStore(kvConfig, creds, null);
            } catch (Exception kve) {
                /* kve.getCause() == KVStoreException */
                if (++nretries == 10) {
                    throw kve;
                }
            }
        }
    }

    private void populateStore(String keyPrefix)
        throws Exception {

        initStore();
        byte[] bytes = new byte[10000];

        try {
            for (int i = 0; i < 10000; i++) {
                Arrays.fill(bytes, (byte) i);
                store.put(Key.createKey(keyPrefix + i), Value.createValue(bytes));
            }
        } catch (Exception e) {
            // ignore the insert exception, which caused by the node down.
        }
    }

    private void waitRNRunning(StorageNodeId snId, RepNodeId rnId)
        throws Exception {

        ServiceStatus[] targetStatus = {ServiceStatus.RUNNING};
        ServiceUtils.waitForRepNodeAdmin(
            createStore.getStoreName(),
            createStore.getHostname(),
            createStore.getRegistryPort(snId),
            rnId,
            snId,
            createStore.
            getSNALoginManager(snId),
            40,
            targetStatus,
            logger);
    }

    private RestorePair prepareTest(String keyPrefix)
        throws Exception {

        if (grantRole == false) {
            grantRoles(ADMIN_NAME, "readwrite");
            grantRole = true;
        }

        /* Locate secondary node as the restore source */
        Topology topology = createStore.getAdmin().getTopology();
        RepNodeId sourceNode = null;
        StorageNodeId secondarySN = null;
        for (StorageNodeId snId : createStore.getStorageNodeIds()) {
            if (topology.getDatacenter(snId).
                         getDatacenterType().isSecondary()) {
                sourceNode = createStore.getRNs(snId).get(0);
                secondarySN = snId;
            }
        }

        /* Choose one of primary node as target restore node */
        StorageNodeAgent targetRestoreSna = null;
        for (int i = 0; i < numSNs; i++) {
            StorageNodeAgent sna = createStore.getStorageNodeAgent(i);
            StorageNodeId snId = sna.getStorageNodeId();
            RepNodeId rnId = createStore.getRNs(snId).get(0);
            if (!snId.equals(secondarySN)) {
                if (shutdownNodes.size() < 2) {
                    shutdownNodes.put(sna, rnId);
                } else {
                    targetRestoreNode = rnId;
                    targetRestoreSna = sna;
                }
            }
        }
        if (targetRestoreSna == null) {
            throw new IllegalStateException("should not happen");
        }

        /*
         * Stop target restore node and populate store, so this node won't have
         * latest data.
         */
        RepNodeAdminAPI targetNodeRna =
            createStore.getRepNodeAdmin(targetRestoreNode);
        assertTrue(targetRestoreSna.stopRepNode(targetRestoreNode, true));
        assertTrue(isShutdown(targetNodeRna));
        populateStore(keyPrefix);

        /*
         * Stop the rest primary nodes, so target restore node won't get
         * data through sync-up.
         */
        for (Entry<StorageNodeAgent, RepNodeId> entry :
             shutdownNodes.entrySet()) {

            RepNodeAdminAPI rna = createStore.getRepNodeAdmin(entry.getValue());
            assertTrue(entry.getKey().stopRepNode(entry.getValue(), true));
            assertTrue(isShutdown(rna));
        }

        /* Restart target restore node */
        targetRestoreSna.startRepNode(targetRestoreNode);

        StorageNodeId snId = targetRestoreSna.getStorageNodeId();
        waitRNRunning(snId, targetRestoreNode);
        return new RestorePair(sourceNode, targetRestoreNode);
    }

    private void restartShutdownRepNodes()
        throws Exception {

        for (Entry<StorageNodeAgent, RepNodeId> entry :
             shutdownNodes.entrySet()) {
            assertTrue(entry.getKey().startRepNode(entry.getValue()));
            waitRNRunning(entry.getKey().getStorageNodeId(),
                          entry.getValue());
        }
        shutdownNodes.clear();
    }

    static private boolean isShutdown(RepNodeAdminAPI rnai)
        throws Exception {

        try {
            rnai.ping().getServiceStatus();
            return false;
        } catch (Exception expected) {
            /* success */
            return true;
        }
    }

    private class RestorePair {
        private final RepNodeId sourceNode;
        private final RepNodeId targetNode;

        RestorePair(RepNodeId sourceNode, RepNodeId targetNode) {
            this.sourceNode = sourceNode;
            this.targetNode = targetNode;
        }
    }

    class StallHook implements TestHook<Integer> {
        private final CountDownLatch waitForRelease;

        public StallHook(CountDownLatch waitForRelease) {
            this.waitForRelease = waitForRelease;
        }

        @Override
        public void doHook(Integer value) {
            if (value.intValue() == 2 && waitForRelease != null) {
                try {
                    waitForRelease.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException (e);
                }
            }
        }
    }

    public static class ErrorHook implements TestHook<Integer> {
        private final int target;

        ErrorHook(int target) {
            this.target = target;
        }

        @Override
        public void doHook(Integer value) {
            if (target == value.intValue()) {
                throw new RuntimeException(FAULT_MSG);
            }
        }
    }

    public static class BackupHook implements
        com.sleepycat.je.utilint.TestHook<File> {

        private int counter = 0;
        private final int totalFault;

        BackupHook(int totalNum) {
            this.totalFault = totalNum;
        }

        @Override
        public void doHook(File file) {
            if (file != null && counter < totalFault) {
                counter++;
                throw new RuntimeException(BACKUP_FAULT_MSG);
            }
        }

        @Override
        public void hookSetup() {
        }

        @Override
        public void doIOHook() throws IOException {
        }

        @Override
        public void doHook() {
        }

        @Override
        public File getHookValue() {
            return null;
        }
    }
}
