/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static java.util.Collections.emptySet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.StatementResult;
import oracle.kv.TestBase;
import oracle.kv.TestClassTimeoutMillis;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.api.table.TableTestBase;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.DatabaseUtils;
import oracle.kv.impl.util.DatabaseUtils.VerificationInfo;
import oracle.kv.impl.util.DatabaseUtils.VerificationOptions;
import oracle.kv.impl.util.DatabaseUtils.VerificationThread;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.ThreadUtils.ThreadPoolExecutorAutoClose;
import oracle.kv.util.CreateStore;
import oracle.kv.util.PingCollector;

import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

import com.sleepycat.je.DbInternal;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.test.util.Corruption;

import org.junit.Test;

/* Increase test timeout to 30 minutes -- test can take 15 minutes */
@TestClassTimeoutMillis(30*60*1000)
public class VerifyDataTest extends TestBase {
    private static boolean printThreads;

    private CreateStore createStore;
    private static final int startPort = 5000;
    private static final int rf = 3;
    private static final int numSns = 3;
    public static final String persistentCorrupt = "Persistent Corruption";
    private boolean btree;
    private boolean log;
    private boolean index;
    private boolean dataRecord;
    private long btreeDelay;
    private long logDelay;
    private long validTime = 500000;
    private boolean showMissingFiles = false;

    public VerifyDataTest() {
        super();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        TestStatus.setManyRNs(true);
        TestStatus.setActive(true);
        createStore = new CreateStore(kvstoreName, startPort, numSns, rf, 10, 2,
                                      2 * CreateStore.MB_PER_SN, true, null);

        /* Disable je.testMode so that verify errors are not fail-fast. */
        ParameterMap policies = new ParameterMap();
        policies.setParameter(ParameterState.JE_MISC, "je.testMode=false;");
        createStore.setPolicyMap(policies);
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
        DatabaseUtils.VERIFY_CORRUPTDATA_HOOK = null;
        DatabaseUtils.VERIFY_ERROR_HOOK = null;
        DatabaseUtils.VERIFY_INTERRUPT_HOOK = null;
        DatabaseUtils.VERIFY_CORRUPTFILE_HOOK = null;
        com.sleepycat.je.tree.IN.disableIDKeyUpdateForTesting(false);
        if (createStore != null) {
            /*
             * Don't use force because doing so fails to clean up JE
             * environments, which can result in OutOfMemoryErrors.
             */
            createStore.shutdown(false /* force */);
        }
        /* Enable to help track down thread leaks */
        if (printThreads) {
            System.err.println("Test: " + testName.getMethodName());
            // oracle.kv.impl.util.TestUtils.dumpThreads(System.err);
            Thread.getAllStackTraces().keySet().stream().forEach(
                System.err::println);
        }
    }

    @Test
    public void testVerifyNoError() throws Exception {
        createStore.start();
        CommandServiceAPI cs = createStore.getAdmin();
        DatacenterId dc1 = new DatacenterId(1);
        /*
         * Admin verification.
         */
        btree = true;
        log = false;
        index = true;
        dataRecord = true;
        btreeDelay = -1;
        logDelay = -1;
        validTime = 500000;

        testAdminVerify(null, null, cs);

        AdminParams admin1 = new AdminParams(cs.getAdmins().get(1));
        int verifyAdminPlanId = testAdminVerify(admin1.getAdminId(), null, cs);
        Plan adminPlan = cs.getPlanById(verifyAdminPlanId);
        assertTrue("test admin verify",
                   adminPlan.getCommandResult().getReturnValue().
                   contains("{\"admin2\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\"}"));

        verifyAdminPlanId = testAdminVerify(null, dc1, cs);
        adminPlan = cs.getPlanById(verifyAdminPlanId);
        assertTrue("test admin verify",
                   adminPlan.getCommandResult().getReturnValue().
                   contains("{\"admin1\":{\"Btree Verify\":\"No Btree " +
                       "Corruptions\"},\"admin2\":{\"Btree Verify\":\"No " +
                       "Btree Corruptions\"},\"admin3\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\"}}"));

        btree = false;
        log = true;
        verifyAdminPlanId = testAdminVerify(admin1.getAdminId(), null, cs);
        adminPlan = cs.getPlanById(verifyAdminPlanId);
        assertTrue("test admin verify",
                   adminPlan.getCommandResult().getReturnValue().
                   contains("{\"admin2\":{\"Log File Verify\":" +
                       "\"No Log File Corruptions\"}"));

        verifyAdminPlanId = testAdminVerify(null, dc1, cs);
        adminPlan = cs.getPlanById(verifyAdminPlanId);
        assertTrue("test admin verify",
                   adminPlan.getCommandResult().getReturnValue().
                   contains("{\"admin1\":{\"Log File Verify\":\"No Log File " +
                       "Corruptions\"},\"admin2\":{\"Log File Verify\":\"" +
                       "No Log File Corruptions\"},\"admin3\":{\"Log File " +
                       "Verify\":\"No Log File Corruptions\"}}"));

        btree = true;
        log = true;
        verifyAdminPlanId = testAdminVerify(admin1.getAdminId(), null, cs);
        adminPlan = cs.getPlanById(verifyAdminPlanId);
        assertTrue("test admin verify",
                   adminPlan.getCommandResult().getReturnValue().
                   contains("{\"admin2\":{\"Btree Verify\":\"No Btree " +
                       "Corruptions\",\"Log File Verify\":\"No Log File " +
                       "Corruptions\"}}"));

        verifyAdminPlanId = testAdminVerify(null, dc1, cs);
        adminPlan = cs.getPlanById(verifyAdminPlanId);
        assertTrue("test admin verify",
                   adminPlan.getCommandResult().getReturnValue().
                   contains("{\"admin1\":{\"Btree Verify\":\"No Btree " +
                       "Corruptions\",\"Log File Verify\":\"No Log File " +
                       "Corruptions\"},\"admin2\":{\"Btree Verify\":\"No " +
                       "Btree Corruptions\",\"Log File Verify\":\"No Log " +
                       "File Corruptions\"},\"admin3\":{\"Btree Verify\":\"" +
                       "No Btree Corruptions\",\"Log File Verify\":\"" +
                       "No Log File Corruptions\"}}"));

        /*
         * Rn verification.
         */
        btree = true;
        log = false;

        RepNode rep1 = cs.getTopology().getSortedRepNodes().get(1);

        testRNVerify(null, null, cs);

        int verifyRnPlanId = testRNVerify(rep1.getResourceId(), null, cs);
        Plan rnPlan = cs.getPlanById(verifyRnPlanId);
        assertTrue("test RN verify", rnPlan.getCommandResult().getReturnValue().
                   contains("{\"rg1-rn2\":{\"Btree Verify\":\"No Btree " +
                       "Corruptions\"}}"));

        verifyRnPlanId = testRNVerify(null, dc1, cs);
        rnPlan = cs.getPlanById(verifyRnPlanId);
        assertTrue("test RN verify", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg2-rn1" +
                       "\":{\"Btree Verify\":\"No Btree Corruptions\""));
        assertTrue("test RN verify", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg1-rn1\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\""));
        assertTrue("test RN verify", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg2-rn2\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\""));
        assertTrue("test RN verify", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg1-rn2\":{\"Btree Verify" +
                       "\":\"No Btree Corruptions\""));
        assertTrue("test RN verify", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg2-rn3\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\""));
        assertTrue("test RN verify", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg1-rn3\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\""));

        btree = false;
        log = true;

        verifyRnPlanId = testRNVerify(rep1.getResourceId(), null, cs);
        rnPlan = cs.getPlanById(verifyRnPlanId);
        assertTrue("test RN verify", rnPlan.getCommandResult().getReturnValue().
                   contains("{\"rg1-rn2\":{\"Log File Verify\":\"No Log File " +
                       "Corruptions\"}}"));

        verifyRnPlanId = testRNVerify(null, dc1, cs);
        rnPlan = cs.getPlanById(verifyRnPlanId);
        assertTrue("test RN verify", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg2-rn1" +
                       "\":{\"Log File Verify\":\"No Log File Corruptions\"}"));
        assertTrue("test RN verify", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg1-rn1\":{\"Log File " +
                       "Verify\":\"No Log File Corruptions\"}"));
        assertTrue("test RN verify", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg2-rn2\":{\"Log File " +
                       "Verify\":\"No Log File Corruptions\"},"));
        assertTrue("test RN verify", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg1-rn2\":{\"Log File " +
                       "Verify\":\"No Log File Corruptions\"}"));
        assertTrue("test RN verify", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg2-rn3\":{\"Log File " +
                       "Verify\":\"No Log File Corruptions\"}"));
        assertTrue("test RN verify", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg1-rn3\":{\"Log File " +
                       "Verify\":\"No Log File Corruptions\""));

        btree = true;
        log = true;
        verifyRnPlanId = testRNVerify(rep1.getResourceId(), null, cs);
        rnPlan = cs.getPlanById(verifyRnPlanId);
        assertTrue("test RN verify", rnPlan.getCommandResult().getReturnValue().
                   contains("{\"rg1-rn2\":{\"Btree Verify\":\"No Btree " +
                       "Corruptions\",\"Log File Verify\":\"No Log File " +
                       "Corruptions\"}}"));

        verifyRnPlanId = testRNVerify(null, dc1, cs);
        rnPlan = cs.getPlanById(verifyRnPlanId);
        assertTrue("test RN verify", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg2-rn1" +
                       "\":{\"Btree Verify\":\"No Btree Corruptions\",\"Log " +
                       "File Verify\":\"No Log File Corruptions\"}"));
        assertTrue("test RN verify", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg1-rn1\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\",\"Log File Verify\":\"No " +
                       "Log File Corruptions\"}"));
        assertTrue("test RN verify", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg2-rn2\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\",\"Log File Verify\":\"No " +
                       "Log File Corruptions\"},"));
        assertTrue("test RN verify", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg1-rn2\":{\"Btree Verify" +
                       "\":\"No Btree Corruptions\",\"Log File Verify\":" +
                       "\"No Log File Corruptions\"}"));
        assertTrue("test RN verify", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg2-rn3\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\",\"Log File Verify\":\"No " +
                       "Log File Corruptions\"}"));
        assertTrue("test RN verify", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg1-rn3\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\",\"Log File Verify\":\"No " +
                       "Log File Corruptions\""));
        /*
         * Admin and rn verification.
         */

        btree = true;
        log = true;
        validTime = 0;
        testAllServicesVerify(null, cs);

        int verifyServicesPlanId = testAllServicesVerify(dc1, cs);
        Plan servicesPlan = cs.getPlanById(verifyServicesPlanId);
        assertTrue("test all servcies verify", servicesPlan.getCommandResult().
                   getReturnValue().contains("\"rg2-rn1" +
                       "\":{\"Btree Verify\":\"No Btree Corruptions\",\"Log " +
                       "File Verify\":\"No Log File Corruptions\"}"));
        assertTrue("test all servcies verify", servicesPlan.getCommandResult().
                   getReturnValue().contains("\"admin1\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\",\"Log File Verify\":\"No " +
                       "Log File Corruptions\"}"));
        assertTrue("test all servcies verify", servicesPlan.getCommandResult().
                   getReturnValue().contains("\"rg1-rn1\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\",\"Log File Verify\":\"No " +
                       "Log File Corruptions\"}"));
        assertTrue("test all servcies verify", servicesPlan.getCommandResult().
                   getReturnValue().contains("\"rg2-rn2\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\",\"Log File Verify\":\"No " +
                       "Log File Corruptions\"},"));
        assertTrue("test all servcies verify", servicesPlan.getCommandResult().
                   getReturnValue().contains("\"admin2\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\",\"Log File Verify\":" +
                       "\"No Log File Corruptions\"}"));
        assertTrue("test all servcies verify", servicesPlan.getCommandResult().
                   getReturnValue().contains("\"rg1-rn2\":{\"Btree Verify" +
                       "\":\"No Btree Corruptions\",\"Log File Verify\":" +
                       "\"No Log File Corruptions\"}"));
        assertTrue("test all servcies verify", servicesPlan.getCommandResult().
                   getReturnValue().contains("\"rg2-rn3\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\",\"Log File Verify\":\"No " +
                       "Log File Corruptions\"}"));
        assertTrue("test all servcies verify", servicesPlan.getCommandResult().
                   getReturnValue().contains("\"admin3\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\",\"Log File Verify\":\"No " +
                       "Log File Corruptions\"}"));
        assertTrue("test all servcies verify", servicesPlan.getCommandResult().
                   getReturnValue().contains("\"rg1-rn3\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\",\"Log File Verify\":\"No " +
                       "Log File Corruptions\""));

    }

    private int testAllServicesVerify(DatacenterId cid,
                                       CommandServiceAPI cs) {
        return testAllServicesVerify(cid, cs, false);
    }

    /* After fix for KVSTORE-1747 the verify data plan will return ERROR if
     * the verification reports btree/log file corruption. The planShouldFail
     * argument should be true if the plan is expected to fail and false
     * other wise
     */
    private int testAllServicesVerify(DatacenterId cid,
                                       CommandServiceAPI cs,
                                       boolean planShouldFail) {
        int planId = 0;
        VerificationOptions options =
            new VerificationOptions(btree, log, index,
                                    dataRecord, btreeDelay, logDelay,
                                    500000, showMissingFiles);
        try {
            planId = cs.createVerifyAllServicesPlan(null, cid, options);

            runPlan(cs, planId);
            if (planShouldFail) {
                fail("plan should fail.");
            }
        } catch (Exception e) {
            if (!planShouldFail) {
                fail("plan should be successful.");
            }
            assertEquals("oracle.kv.impl.admin.AdminFaultException",
                         e.getClass().getName());
            final String eMsg = e.getMessage();
            assertTrue("Unexpected exception message: " + eMsg,
                       eMsg.contains("Verify Data") &&
                       eMsg.contains("ended with ERROR"));
        }
        return planId;
    }

    private int testRNVerify(ResourceId rid,
                              DatacenterId cid,
                              CommandServiceAPI cs) {
        return testRNVerify(rid, cid, cs, false);
    }

    /* After fix for KVSTORE-1747 the verify data plan will return ERROR if
     * the verification reports btree/log file corruption. The planShouldFail
     * argument should be true if the plan is expected to fail and false
     * other wise
     */
    private int testRNVerify(ResourceId rid,
                              DatacenterId cid,
                              CommandServiceAPI cs,
                              boolean planShouldFail) {
        VerificationOptions options =
            new VerificationOptions(btree, log, index,
                                    dataRecord, btreeDelay, logDelay,
                                    validTime, showMissingFiles);
        int planId = 0;
        try {
            if (rid != null) {
                planId = cs.createVerifyServicePlan(null, rid, options);

            } else {

                planId = cs.createVerifyAllRepNodesPlan(null, cid, options);

            }
            runPlan(cs, planId);
            if (planShouldFail) {
                fail("plan should fail.");
            }
        } catch (Exception e) {
            if (!planShouldFail) {
                fail("plan should be successful.");
            }
            assertEquals("oracle.kv.impl.admin.AdminFaultException",
                         e.getClass().getName());
            final String eMsg = e.getMessage();
            assertTrue("Unexpected exception message: " + eMsg,
                       eMsg.contains("Verify Data") &&
                       eMsg.contains("ended with ERROR"));
        }
        return planId;
    }

    private int testAdminVerify(ResourceId rid,
                                 DatacenterId cid,
                                 CommandServiceAPI cs) {
        return testAdminVerify(rid, cid, cs, false);
    }

    /* After fix for KVSTORE-1747 the verify data plan will return ERROR if
     * the verification reports btree/log file corruption. The planShouldFail
     * argument should be true if the plan is expected to fail and false
     * other wise
     */
    private int testAdminVerify(ResourceId rid,
                                 DatacenterId cid,
                                 CommandServiceAPI cs,
                                 boolean planShouldFail) {
        int planId = 0;
        VerificationOptions options =
            new VerificationOptions(btree, log, index,
                                    dataRecord, btreeDelay, logDelay,
                                    validTime, showMissingFiles);
        try {
            if (rid != null) {
                planId = cs.createVerifyServicePlan(null, rid, options);

            } else {
                planId = cs.createVerifyAllAdminsPlan(null, cid, options);

            }
            runPlan(cs, planId);
            if (planShouldFail) {
                fail("plan should fail.");
            }
        } catch (Exception e) {
            if (!planShouldFail) {
                fail("plan should be successful.");
            }
            assertEquals("oracle.kv.impl.admin.AdminFaultException",
                         e.getClass().getName());
            final String eMsg = e.getMessage();
            assertTrue("Unexpected exception message: " + eMsg,
                       eMsg.contains("Verify Data") &&
                       eMsg.contains("ended with ERROR"));
        }
        return planId;
    }

    @Test
    public void testSingleAdminBtreeRecordCorruption() throws Exception {
        com.sleepycat.je.tree.IN.disableIDKeyUpdateForTesting(true);
        createStore.start();
        btree = true;
        log = true;
        index = true;
        dataRecord = true;
        btreeDelay = 0;
        logDelay = -1;
        showMissingFiles = true;

        CommandServiceAPI cs = createStore.getAdmin();
        AdminParams admin0 = new AdminParams(cs.getAdmins().get(0));
        String dir = System.getProperty("testdestdir") + "/" +
            createStore.getStoreName() + "/sn1/";

        Map<String, VerifyCorruptionHook.DBCorruption[]> corruptionList =
            new HashMap<>();
        VerifyCorruptionHook.DBCorruption[] corruptions =
            new VerifyCorruptionHook.DBCorruption[]
                { new VerifyCorruptionHook.
                  DBCorruption(VerifyCorruptionHook.Type.LSNOutOfBounds,
                               "AdminPlanDatabase"),
                  new VerifyCorruptionHook.
                  DBCorruption(VerifyCorruptionHook.Type.INKeyOrder,
                               "AdminCriticalEventsDatabase"),
                  new VerifyCorruptionHook.
                  DBCorruption(VerifyCorruptionHook.Type.IdentifierKey,
                               "AdminTopologyHistoryDatabase")
                  };
        corruptionList.put("admin1", corruptions);


        VerifyCorruptionHook corruptHook =
            new VerifyCorruptionHook(corruptionList,
                                     dir, "admin1");

        DatabaseUtils.VERIFY_CORRUPTDATA_HOOK = corruptHook;
        int verifyAdminPlanId =
            testAdminVerify(admin0.getAdminId(), null, cs, true);
        Plan adminPlan = cs.getPlanById(verifyAdminPlanId);

        assertTrue("test single admin corruption", adminPlan.getCommandResult().
                   getReturnValue().contains("\"admin1\":{\"Btree Verify\":{" +
                       "\"Database corruptions\":{\"Total Number\":2,\"List" +
                       "\":{\"AdminTopologyHistoryDatabase\":{\"1\":\"Btree " +
                       "corruption was detected. IdentifierKey"));
        assertTrue("test single admin corruption", adminPlan.getCommandResult().
                   getReturnValue().contains("\"AdminCriticalEventsDatabase\"" +
                       ":{\"1\":\"Btree corruption was detected. IN keys are " +
                       "out of order."));
        assertTrue("test single admin corruption", adminPlan.getCommandResult().
                   getReturnValue().contains("\"Corrupt Keys\":{\"Total " +
                       "number\":1,\"List\":{\"dbName: AdminPlanDatabase " +
                       "priKey:"));
        assertTrue("test single admin corruption", adminPlan.getCommandResult().
                   getReturnValue().contains("\"Log File Verify\":{\"Log " +
                       "File Error\":{\"Total number\":1,\"List\":{\"Log " +
                       "File 0\":{\"1\":\"com.sleepycat.je.util." +
                       "LogVerificationException:"));
    }

    @Test
    public void testGroupAdminBtreeRecordCorruption() throws Exception {
        createStore.start();
        btree = true;
        log = true;
        index = true;
        dataRecord = true;
        btreeDelay = 0;
        logDelay = -1;
        showMissingFiles = true;

        CommandServiceAPI cs = createStore.getAdmin();
        DatacenterId dc1 = new DatacenterId(1);

        Map<String, VerifyCorruptionHook.DBCorruption[]> corruptionList =
            new HashMap<>();
        corruptionList.put("admin", new VerifyCorruptionHook.DBCorruption[]
            { new VerifyCorruptionHook.
            DBCorruption(VerifyCorruptionHook.Type.LSNOutOfBounds,
                         "AdminPlanDatabase") });
        VerifyCorruptionHook corruptHook =
            new VerifyCorruptionHook(corruptionList,
                                     null, "admin");

        DatabaseUtils.VERIFY_CORRUPTDATA_HOOK = corruptHook;
        int verifyAdminPlanId = testAdminVerify(null, dc1, cs, true);
        Plan adminPlan = cs.getPlanById(verifyAdminPlanId);
        assertTrue("test group admin corruption", adminPlan.getCommandResult().
                   getReturnValue().contains("\"admin1\":{\"Btree Verify\":{" +
                       "\"Corrupt Keys\":{\"Total number\":1,\"List\":{" +
                       "\"dbName: AdminPlanDatabase priKey:"));
        assertFalse("test group admin corruption", adminPlan.getCommandResult().
                    getReturnValue().contains("\"admin2\":{\"Btree Verify\":{" +
                        "\"Corrupt Keys\":{\"Total number\":1,\"List\":{" +
                        "\"dbName: AdminPlanDatabase priKey:"));
        assertFalse("test group admin corruption", adminPlan.getCommandResult().
                    getReturnValue().contains("\"admin3\":{\"Btree Verify\":{" +
                        "\"Corrupt Keys\":{\"Total number\":1,\"List\":{" +
                        "\"dbName: AdminPlanDatabase priKey:"));

    }

    private void createTables() throws InterruptedException {
        try (final ThreadPoolExecutorAutoClose executor =
             new ThreadPoolExecutorAutoClose(
                 0, 200, 0L, TimeUnit.MILLISECONDS,
                 new SynchronousQueue<Runnable>(),
                 new KVThreadFactory("testRNBtreeCorruption", null))) {
            KVStore store = KVStoreFactory.getStore(
                TableTestBase.createKVConfig(createStore));
            for (int i = 0; i < 100; i++) {
                final int j = i;
                Runnable r = new Runnable() {
                    @Override
                    public void run() {
                        StatementResult sr =
                            store.executeSync(
                                "CREATE Table Tid_Long" + j + " " +
                                "(id LONG GENERATED ALWAYS AS IDENTITY " +
                                "    (START WITH 1 INCREMENT BY 1" +
                                " MAXVALUE 100 CYCLE CACHE 3)," +
                                " name STRING, PRIMARY KEY (id))");
                        assertTrue(sr.isSuccessful());
                        assertTrue(sr.isDone());

                    }};
                executor.submit(r);
            }
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    private void dropTables() throws InterruptedException {
        try (final ThreadPoolExecutorAutoClose executor =
             new ThreadPoolExecutorAutoClose(
                 0, 200, 0L, TimeUnit.MILLISECONDS,
                 new SynchronousQueue<Runnable>(),
                 new KVThreadFactory("testRNBtreeCorruption", null))) {
            KVStore store = KVStoreFactory.getStore(
                TableTestBase.createKVConfig(createStore));
            for (int i = 0; i < 100; i++) {
                final int j = i;
                Runnable r = new Runnable() {
                    @Override
                    public void run() {
                        StatementResult sr =
                            store.executeSync(
                                "DROP Table Tid_Long" + j);
                        assertTrue(sr.isSuccessful());
                        assertTrue(sr.isDone());

                    }};
                executor.submit(r);
            }
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSingleRNCorruption() throws Exception {
        com.sleepycat.je.tree.IN.disableIDKeyUpdateForTesting(true);
        createStore.start();
        createTables();

        btree = true;
        log = true;
        index = true;
        dataRecord = true;
        btreeDelay = 0;
        logDelay = -1;
        showMissingFiles = true;

        CommandServiceAPI cs = createStore.getAdmin();
        RepNode rep1 = cs.getTopology().getSortedRepNodes().get(0);
        String dir = System.getProperty("testdestdir") + "/" +
            createStore.getStoreName() + "/sn1/";

        Map<String, VerifyCorruptionHook.DBCorruption[]> corruptionList =
            new HashMap<>();
        VerifyCorruptionHook.DBCorruption[] corruptions =
            { new VerifyCorruptionHook.
                  DBCorruption(VerifyCorruptionHook.Type.LSNOutOfBounds, "p1"),
              new VerifyCorruptionHook.
                  DBCorruption(VerifyCorruptionHook.Type.INKeyOrder, "p2"),
              new VerifyCorruptionHook.
                  DBCorruption(VerifyCorruptionHook.Type.IdentifierKey, "p3")
              };
        corruptionList.put("rg1-rn1", corruptions);
        VerifyCorruptionHook corruptHook =
            new VerifyCorruptionHook(corruptionList, dir, "rg1-rn1");
        DatabaseUtils.VERIFY_CORRUPTDATA_HOOK = corruptHook;

        int verifyRnPlanId = testRNVerify(rep1.getResourceId(), null, cs, true);
        Plan rnPlan = cs.getPlanById(verifyRnPlanId);
        String result = rnPlan.getCommandResult().
            getReturnValue();

        assertTrue("test rn btree corruption " + result, result.
                   contains("\"p2\":{\"1\":" +
                       "\"Btree corruption was detected"));
        assertTrue("test rn btree corruption " + result, result.
                   contains("\"Corrupt Keys\":{\"Total " +
                       "number\":1,\"List\":{\"dbName: p1"));
        assertTrue("test rn btree corruption " + result, result.
                   contains("\"Log File Error\":{\"Total " +
                       "number\":1,\"List\":{\"Log File 0\":{\"1\":" +
                       "\"com.sleepycat.je.util.LogVerificationException:"));
        assertTrue("test rn btree corruption " + result, result.
                   contains("\"p3\":{\"1\":\"Btree " +
                       "corruption was detected. IdentifierKey is null " +
                       "or not present in any slot."));
        /* Verify fix for KVSTORE-1747 */
        assertEquals(5601, rnPlan.getCommandResult().getErrorCode());
        dropTables();
    }

    @Test
    public void testGroupRNBtreeCorruption() throws Exception {
        createStore.start();
        createTables();

        btree = true;
        log = true;
        index = true;
        dataRecord = true;
        btreeDelay = 0;
        logDelay = -1;
        showMissingFiles = true;

        CommandServiceAPI cs = createStore.getAdmin();
        DatacenterId dc1 = new DatacenterId(1);
        String dir = System.getProperty("testdestdir") + "/" +
            createStore.getStoreName() + "/sn1/";

        Map<String, VerifyCorruptionHook.DBCorruption[]> corruptionList =
            new HashMap<>();
        corruptionList.put("rg1-rn", new VerifyCorruptionHook.DBCorruption[]
            { new VerifyCorruptionHook.
            DBCorruption(VerifyCorruptionHook.Type.LSNOutOfBounds, "p1")});
        VerifyCorruptionHook corruptHook =
            new VerifyCorruptionHook(corruptionList, dir, "rg1");
        DatabaseUtils.VERIFY_CORRUPTDATA_HOOK = corruptHook;

        int verifyRnPlanId = testRNVerify(null, dc1, cs, true);
        Plan rnPlan = cs.getPlanById(verifyRnPlanId);
        assertTrue("test rn btree corruption", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg1-rn1\":{\"Btree Verify\":" +
                       "{\"Corrupt Keys\":{\"Total number\":1,\"List\":" +
                       "{\"dbName: p1"));
        assertFalse("test rn btree corruption", rnPlan.getCommandResult().
                    getReturnValue().contains("\"rg1-rn2\":{\"Btree Verify\":" +
                        "{\"Corrupt Keys\":{\"Total number\":1,\"List\":" +
                        "{\"dbName: p1"));
        assertFalse("test rn btree corruption", rnPlan.getCommandResult().
                    getReturnValue().contains("\"rg1-rn3\":{\"Btree Verify\":" +
                        "{\"Corrupt Keys\":{\"Total number\":1,\"List\":" +
                        "{\"dbName: p1"));
        assertTrue("test rn btree corruption", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg2-rn3\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\""));
        assertTrue("test rn btree corruption", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg2-rn1\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\""));
        assertTrue("test rn btree corruption", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg2-rn2\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\""));
    }

    @Test
    public void testGroupRNLogCorruption() throws Exception {
        createStore.start();
        createTables();

        btree = true;
        log = true;
        index = true;
        dataRecord = true;
        btreeDelay = 0;
        logDelay = -1;
        showMissingFiles = true;

        CommandServiceAPI cs = createStore.getAdmin();
        DatacenterId dc1 = new DatacenterId(1);
        String dir = System.getProperty("testdestdir") + "/" +
            createStore.getStoreName() + "/sn1/";

        VerifyCorruptionHook corruptHook =
            new VerifyCorruptionHook(null, dir, "rn1");
        DatabaseUtils.VERIFY_CORRUPTDATA_HOOK = corruptHook;

        int verifyRnPlanId = testRNVerify(null, dc1, cs, true);
        Plan rnPlan = cs.getPlanById(verifyRnPlanId);

        ObjectNode resultNode = JsonUtils.
            parseJsonObject(rnPlan.getCommandResult().getReturnValue());

        assertTrue("test rn log corruption: ",
                   resultNode.getObject("Verify Report").
                   getObject("rg2-rn1").
                   getObject("Log File Verify").
                   getObject("Log File Error").
                   get("Total number").asInt() == 1);
        assertTrue("test rn log corruption: ",
                   resultNode.getObject("Verify Report").
                   getObject("rg1-rn1").
                   getObject("Log File Verify").
                   getObject("Log File Error").
                   get("Total number").asInt() == 1);
    }

    @Test
    public void testGroupRNAdminCorruption() throws Exception {
        createStore.start();
        createTables();

        btree = true;
        log = true;
        index = true;
        dataRecord = true;
        btreeDelay = 0;
        logDelay = -1;
        showMissingFiles = true;

        CommandServiceAPI cs = createStore.getAdmin();
        DatacenterId dc1 = new DatacenterId(1);
        String dir = System.getProperty("testdestdir") + "/" +
            createStore.getStoreName() + "/sn1/";


        Map<String, VerifyCorruptionHook.DBCorruption[]> corruptionList =
            new HashMap<>();

        corruptionList.put("rg1-rn", new VerifyCorruptionHook.DBCorruption[]
            { new VerifyCorruptionHook.
             DBCorruption(VerifyCorruptionHook.Type.LSNOutOfBounds, "p1") });

        corruptionList.put("admin", new VerifyCorruptionHook.DBCorruption[]
            { new VerifyCorruptionHook.
            DBCorruption(VerifyCorruptionHook.Type.LSNOutOfBounds,
                         "AdminPlanDatabase") });

        VerifyCorruptionHook corruptHook =
            new VerifyCorruptionHook(corruptionList, dir, "");
        DatabaseUtils.VERIFY_CORRUPTDATA_HOOK = corruptHook;

        int verifyPlanId = testAllServicesVerify(dc1, cs, true);
        Plan plan = cs.getPlanById(verifyPlanId);

        final ObjectNode returnValue = JsonUtils.parseJsonObject(
            plan.getCommandResult().getReturnValue());
        final ObjectNode verifyReport = returnValue.getObject("Verify Report");
        logger.info("Verify report: " + verifyReport);

        /* Check verification results for admins */
        for (int i = 1; i <= 3; i++) {
            final String admin = "admin" + i;
            final ObjectNode adminReport = verifyReport.getObject(admin);

            if (adminReport != null) {
                /* Expect log file error in admin1 */
                checkLogFileErrors(admin, adminReport, i == 1);

                /* Expect corrupt key in btree for all admins */
                final Set<String> corruptKeys =
                    getBtreeCorruptionKeys(admin, adminReport);
                assertEquals(admin, 1, corruptKeys.size());
                final String key = corruptKeys.iterator().next();
                assertTrue(admin + ": " + key,
                           key.startsWith("dbName: AdminPlanDatabase priKey:"));
            }
        }

        /* Check verification results for RNs */
        for (int i = 0; i < 6; i++) {
            final int s = i/3 + 1; /* shard 1 or 2 */
            final int r = i%3 + 1; /* RN 1, 2, or 3 */
            final String rn = "rg" + s + "-rn" + r;
            final ObjectNode rnReport = verifyReport.getObject(rn);
            final String desc = rn + ": " + rnReport;

            if (rnReport != null) {
                /* Expect log file error on rg1-rn1 and rg2-rn1 */
                checkLogFileErrors(rn, rnReport, r == 1);

                final Set<String> corruptKeys =
                    getBtreeCorruptionKeys(rn, rnReport);

                /* Table metadata corruption might appear on any RN */
                for (final String key : corruptKeys) {
                    if (key.startsWith("dbName: TableMetadata priKey:")) {
                        corruptKeys.remove(key);
                        break;
                    }
                }

                if (s == 2) {
                    /* No other corruption in shard 2 */
                    assertEquals(desc, 0, corruptKeys.size());
                } else {
                    /* Check for expected key, ignore others */
                    boolean found = false;
                    for (final String key : corruptKeys) {
                        if (key.startsWith("dbName: p1 priKey:")) {
                            found = true;
                            break;
                        }
                    }
                    assertTrue(desc, found);
                }
            }
        }
    }

    private void checkLogFileErrors(String serviceName,
                                    ObjectNode serviceReport,
                                    boolean expectError) {
        final String desc = serviceName + ": " + serviceReport;
        final String logFileError =
            getLogFileError(serviceName, serviceReport);
        if (!expectError) {
            assertEquals(desc, null, logFileError);
        } else {
            assertTrue(desc,
                       logFileError.startsWith(
                           "com.sleepycat.je.util.LogVerificationException"));
        }
    }

    private String getLogFileError(String serviceName,
                                   ObjectNode serviceReport) {
        final String desc = serviceName + ": " + serviceReport;
        final JsonNode logFileVerify = serviceReport.get("Log File Verify");
        if (logFileVerify.isString()) {
            assertEquals(desc,
                         "No Log File Corruptions", logFileVerify.asText());
            return null;
        }
        assertTrue(desc, logFileVerify.isObject());
        final ObjectNode logFileError =
            logFileVerify.asObject().getObject("Log File Error");
        assertEquals(desc, 1, logFileError.get("Total number").asInt());
        return logFileError.getObject("List")
            .getObject("Log File 0")
            .get("1")
            .asText();
    }

    private Set<String> getBtreeCorruptionKeys(String serviceName,
                                               ObjectNode serviceReport)
    {
        final String desc = serviceName + ": " + serviceReport;
        final JsonNode btreeVerify = serviceReport.get("Btree Verify");
        if (btreeVerify.isString()) {
            assertEquals(desc, "No Btree Corruptions", btreeVerify.asText());
            return emptySet();
        }
        final ObjectNode corruptKeys =
            btreeVerify.asObject().getObject("Corrupt Keys");
        final int totalNumber = corruptKeys.get("Total number").asInt();
        final Set<String> keys = corruptKeys.getObject("List").fieldNames();
        assertEquals(desc, totalNumber, keys.size());
        return keys;
    }

    @Test
    public void testInterruptAdminBtreeVerify() throws Exception {
        createStore.start();
        CommandServiceAPI cs = createStore.getAdmin();
        btree = true;
        log = false;
        index = true;
        dataRecord = true;
        btreeDelay = 0;
        logDelay = -1;

        AdminParams admin1 = new AdminParams(cs.getAdmins().get(1));

        VerificationOptions options =
            new VerificationOptions(btree, log, index,
                                    dataRecord, btreeDelay, logDelay,
                                    500000, showMissingFiles);

        int planId = cs.createVerifyServicePlan(null,
                                                admin1.getAdminId(),
                                                options);
        VerifyInterruptHook verifyHook = new VerifyInterruptHook(cs, planId);
        DatabaseUtils.VERIFY_INTERRUPT_HOOK = verifyHook;

        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        Plan.State state = cs.awaitPlan(planId, 0, null);
        assert state == Plan.State.INTERRUPTED;
        assertTrue(createStore.getAdmin(1).startAndPollVerify(null, planId).
                   isInterrupted);

    }

    @Test
    public void testInterruptRNBtreeVerify() throws Exception {
        createStore.start();
        CommandServiceAPI cs = createStore.getAdmin();
        btree = true;
        log = false;
        index = true;
        dataRecord = true;
        btreeDelay = 0;
        logDelay = -1;

        RepNode rep1 = cs.getTopology().getSortedRepNodes().get(1);

        VerificationOptions options =
            new VerificationOptions(btree, log, index,
                                    dataRecord, btreeDelay, logDelay,
                                    500000, showMissingFiles);

        int planId = cs.createVerifyServicePlan(null,
                                                rep1.getResourceId(),
                                                options);

        VerifyInterruptHook verifyHook = new VerifyInterruptHook(cs, planId);
        DatabaseUtils.VERIFY_INTERRUPT_HOOK = verifyHook;

        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        Plan.State state = cs.awaitPlan(planId, 0, null);
        assert state == Plan.State.INTERRUPTED;
        assertTrue(createStore.getRepNodeAdmin(rep1.getResourceId()).
                   startAndPollVerifyData(null, planId).isInterrupted);

    }

    @Test
    public void testRNLostConnection() throws Exception {
        createStore.start();
        CommandServiceAPI cs = createStore.getAdmin();
        btree = true;
        log = true;
        index = true;
        dataRecord = true;
        btreeDelay = -1;
        logDelay = -1;

        RepNode rep1 = cs.getTopology().getSortedRepNodes().get(0);
        VerifyTimeHook verifyHook = new VerifyTimeHook(10000);
        DatabaseUtils.VERIFY_ERROR_HOOK = verifyHook;
        int planId = doNodeCrushTest(null, cs,
                                     rep1.getResourceId(), false);

        Plan rnPlan = cs.getPlanById(planId);
        assertTrue("test rn lost connection", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg2-rn1" +
                       "\":{\"Btree Verify\":\"No Btree Corruptions\",\"Log " +
                       "File Verify\":\"No Log File Corruptions\"}"));
        assertTrue("test rn lost connection", rnPlan.getCommandResult().
                   getReturnValue().contains("\"admin1\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\",\"Log File Verify\":\"No " +
                       "Log File Corruptions\"}"));
        assertTrue("test rn lost connection", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg1-rn1\":\"ERROR\""));
        assertTrue("test rn lost connection", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg2-rn2\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\",\"Log File Verify\":\"No " +
                       "Log File Corruptions\"},"));
        assertTrue("test rn lost connection", rnPlan.getCommandResult().
                   getReturnValue().contains("\"admin2\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\",\"Log File Verify\":" +
                       "\"No Log File Corruptions\"}"));
        assertTrue("test rn lost connection", rnPlan.getCommandResult().
                   getReturnValue().contains("\"rg2-rn3\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\",\"Log File Verify\":\"No " +
                       "Log File Corruptions\"}"));
        assertTrue("test rn lost connection", rnPlan.getCommandResult().
                   getReturnValue().contains("\"admin3\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\",\"Log File Verify\":\"No " +
                       "Log File Corruptions\"}"));
    }

    @Test
    public void testAdminLostConnection() throws Exception {
        createStore.start();
        CommandServiceAPI cs = createStore.getAdmin();
        btree = true;
        log = true;
        index = true;
        dataRecord = true;
        btreeDelay = -1;
        logDelay = -1;

        AdminId admin1 = createStore.getAdminId(0);
        VerifyTimeHook verifyHook = new VerifyTimeHook(10000);
        DatabaseUtils.VERIFY_ERROR_HOOK = verifyHook;

        int planId = doNodeCrushTest(null, cs,
                                     admin1, false);
        cs = createStore.getAdminMaster();
        Plan adminPlan = cs.getPlanById(planId);
        assertTrue("test all servcies verify", adminPlan.getCommandResult().
                   getReturnValue().contains("\"rg2-rn1" +
                       "\":{\"Btree Verify\":\"No Btree Corruptions\",\"Log " +
                       "File Verify\":\"No Log File Corruptions\"}"));
        assertTrue("test all servcies verify", adminPlan.getCommandResult().
                   getReturnValue().contains("\"admin1\":\"ERROR\""));
        assertTrue("test all servcies verify", adminPlan.getCommandResult().
                   getReturnValue().contains("\"rg1-rn1\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\",\"Log File Verify\":\"No " +
                       "Log File Corruptions\"}"));
        assertTrue("test all servcies verify", adminPlan.getCommandResult().
                   getReturnValue().contains("\"rg2-rn2\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\",\"Log File Verify\":\"No " +
                       "Log File Corruptions\"},"));
        assertTrue("test all servcies verify", adminPlan.getCommandResult().
                   getReturnValue().contains("\"rg1-rn2\":{\"Btree Verify" +
                       "\":\"No Btree Corruptions\",\"Log File Verify\":" +
                       "\"No Log File Corruptions\"}"));
        assertTrue("test all servcies verify", adminPlan.getCommandResult().
                   getReturnValue().contains("\"rg2-rn3\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\",\"Log File Verify\":\"No " +
                       "Log File Corruptions\"}"));
        assertTrue("test all servcies verify", adminPlan.getCommandResult().
                   getReturnValue().contains("\"rg1-rn3\":{\"Btree Verify\":" +
                       "\"No Btree Corruptions\",\"Log File Verify\":\"No " +
                       "Log File Corruptions\""));
    }

    @Test
    public void testRNVerifyRNCrush() throws Exception {
        createStore.start();
        CommandServiceAPI cs = createStore.getAdmin();
        btree = true;
        log = true;
        index = true;
        dataRecord = true;
        btreeDelay = -1;
        logDelay = -1;

        RepNode rep1 = cs.getTopology().getSortedRepNodes().get(0);
        VerifyTimeHook verifyHook = new VerifyTimeHook(20000);
        DatabaseUtils.VERIFY_ERROR_HOOK = verifyHook;
        int planId = doNodeCrushTest(rep1.getResourceId(), cs,
                                     rep1.getResourceId(), true);
        cs.assertSuccess(planId);
    }

    @Test
    public void testRNVerifyAdminCrush() throws Exception {
        createStore.start();
        CommandServiceAPI cs = createStore.getAdmin();
        btree = true;
        log = true;
        index = true;
        dataRecord = true;
        btreeDelay = -1;
        logDelay = -1;

        AdminId admin1 = createStore.getAdminId(0);
        RepNode rep1 = cs.getTopology().getSortedRepNodes().get(0);
        VerifyTimeHook verifyHook = new VerifyTimeHook(20000);
        DatabaseUtils.VERIFY_ERROR_HOOK = verifyHook;
        int planId = doNodeCrushTest(rep1.getResourceId(), cs, admin1, true);
        cs = createStore.getAdminMaster();
        cs.assertSuccess(planId);
    }

    @Test
    public void testAdminVerifyAdminCrush() throws Exception {
        createStore.start();
        CommandServiceAPI cs = createStore.getAdmin();
        btree = true;
        log = true;
        index = true;
        dataRecord = true;
        btreeDelay = -1;
        logDelay = -1;

        AdminId admin1 = createStore.getAdminId(0);
        VerifyTimeHook verifyHook = new VerifyTimeHook(20000);
        DatabaseUtils.VERIFY_ERROR_HOOK = verifyHook;
        int planId = doNodeCrushTest(admin1, cs, admin1, true);
        cs = createStore.getAdminMaster();
        /* after fix for KVSTORE-1747 the verify data plan returns ERROR
         * if the verification reports btree/log file corruption
         */
        try {
            cs.assertSuccess(planId);
        } catch (Exception e) {
            assertEquals("oracle.kv.impl.admin.AdminFaultException",
                         e.getClass().getName());
        }
    }



    @Test
    public void testRNVerifyError() throws Exception {
        createStore.start();
        btree = true;
        log = true;
        index = true;
        dataRecord = true;
        btreeDelay = -1;
        logDelay = -1;

        CommandServiceAPI cs = createStore.getAdmin();
        RepNode rep1 = cs.getTopology().getSortedRepNodes().get(0);

        DatabaseUtils.VERIFY_ERROR_HOOK = new VerifyErrorHook(false);

        int verifyRnPlanId = testRNVerify(rep1.getResourceId(), null, cs, true);
        Plan rnPlan = cs.getPlanById(verifyRnPlanId);
        assertTrue("test RN Error", rnPlan.getCommandResult().getReturnValue().
                   contains("{\"rg1-rn1\":{\"Verification failed. " +
                       "Error\":\"Verification fails. Exception: " +
                       "Test Error\"}"));

        /* Last result is still valid so verification is not rerun. */
        DatabaseUtils.VERIFY_ERROR_HOOK = new VerifyErrorHook(true);
        verifyRnPlanId = testRNVerify(rep1.getResourceId(), null, cs, true);
        rnPlan = cs.getPlanById(verifyRnPlanId);
        assertTrue("test RN Error", rnPlan.getCommandResult().getReturnValue().
                   contains("{\"rg1-rn1\":{\"Verification failed. " +
                       "Error\":\"Verification fails. Exception: " +
                       "Test Error\"}"));


        /* Set the valid time to 0 and rerun the verification.
         * RN crashes so the verification is stopped. */
        validTime = 0;
        VerificationOptions options =
            new VerificationOptions(btree, log, index,
                                    dataRecord, btreeDelay, logDelay,
                                    validTime, showMissingFiles);
        verifyRnPlanId = cs.createVerifyServicePlan(null, rep1.getResourceId(),
                                                    options);
        cs.approvePlan(verifyRnPlanId);
        cs.executePlan(verifyRnPlanId, false);
        Plan.State state = cs.awaitPlan(verifyRnPlanId, 0, null);
        assert state == Plan.State.ERROR;
        rnPlan = cs.getPlanById(verifyRnPlanId);
        assertTrue("test RN Error", rnPlan.getCommandResult().getReturnValue().
                   contains("{\"Verify Report\":{\"rg1-rn1\":\"ERROR\"}}"));
    }

    @Test
    public void testGroupRNVerifyError() throws Exception {
        createStore.start();
        btree = true;
        log = true;
        index = true;
        dataRecord = true;
        btreeDelay = -1;
        logDelay = -1;

        CommandServiceAPI cs = createStore.getAdmin();
        DatacenterId dc1 = new DatacenterId(1);

        DatabaseUtils.VERIFY_ERROR_HOOK = new VerifyErrorHook(true);

        VerificationOptions options =
            new VerificationOptions(btree, log, index,
                                    dataRecord, btreeDelay, logDelay,
                                    validTime, showMissingFiles);
        int verifyRnPlanId = cs.createVerifyAllRepNodesPlan(null, dc1, options);

        cs.approvePlan(verifyRnPlanId);
        cs.executePlan(verifyRnPlanId, false);
        Plan.State state = cs.awaitPlan(verifyRnPlanId, 0, null);
        assert state == Plan.State.ERROR;
        Plan rnPlan = cs.getPlanById(verifyRnPlanId);
        assertTrue("test RN Error", rnPlan.getCommandResult().getReturnValue().
                   contains("{\"Verify Report\":{\"rg1-rn1\":" +
                       "\"ERROR\",\"rg2-rn1\":\"ERROR\"}}"));

        /* Only one RN in each shard crashes. */
        int activeRNForRg1 = 0;
        int activeRNForRg2 = 0;
        PingCollector collector = new PingCollector(cs.getTopology(), logger);
        Map<ResourceId, ServiceStatus> pingMap = collector.getTopologyStatus();
        for (Entry<ResourceId, ServiceStatus> entry : pingMap.entrySet()) {
            if (entry.getValue() == ServiceStatus.RUNNING) {
                if (entry.getKey().toString().contains("rg1")) {
                    activeRNForRg1++;
                } else if (entry.getKey().toString().contains("rg2")) {
                    activeRNForRg2++;
                }
            }
        }
        assert activeRNForRg1 == 2;
        assert activeRNForRg2 == 2;
    }

    @Test
    public void testAdminVerifyError() throws Exception {
        createStore.start();
        btree = true;
        log = true;
        index = true;
        dataRecord = true;
        btreeDelay = -1;
        logDelay = -1;

        CommandServiceAPI cs = createStore.getAdmin();
        AdminParams admin0 = new AdminParams(cs.getAdmins().get(1));

        DatabaseUtils.VERIFY_ERROR_HOOK = new VerifyErrorHook(false);
        int verifyAdminPlanId = testAdminVerify(admin0.getAdminId(), null, cs,
                                                true);
        Plan adminPlan = cs.getPlanById(verifyAdminPlanId);
        assertTrue("test Admin Error", adminPlan.getCommandResult().
                   getReturnValue().contains("{\"admin2\":{\"Verification " +
                       "failed. Error\":\"Verification fails. " +
                       "Exception: Test Error\"}"));

        /* Last result is still valid so verification is not rerun. */
        DatabaseUtils.VERIFY_ERROR_HOOK = new VerifyErrorHook(true);
        verifyAdminPlanId = testAdminVerify(admin0.getAdminId(), null, cs,
                                            true);
        adminPlan = cs.getPlanById(verifyAdminPlanId);
        assertTrue("test Admin Error", adminPlan.getCommandResult().
                   getReturnValue().contains("{\"admin2\":{\"Verification " +
                       "failed. Error\":\"Verification fails. Exception: " +
                       "Test Error\"}"));

        /* Rerun the verification. */
        validTime = 0;
        verifyAdminPlanId = testAdminVerify(admin0.getAdminId(), null, cs,
                                            true);
        adminPlan = cs.getPlanById(verifyAdminPlanId);
        assertTrue("test Admin Error", adminPlan.getCommandResult().
                   getReturnValue().contains("{\"admin2\":{\"Verification " +
                       "failed. Error"));
        assertTrue("test Admin Error", adminPlan.getCommandResult().
                   getReturnValue().contains("Generated data corruption " +
                       "exception for testing UNEXPECTED_STATE_FATAL: " +
                       "Unexpected internal state, unable to continue. " +
                       "Environment is invalid and must be closed."));
    }

    @Test
    public void testMultiplePlansOnSingleNode() throws Exception {
        createStore.start();
        CommandServiceAPI cs = createStore.getAdmin();
        btree = true;
        log = true;
        index = true;
        dataRecord = true;
        btreeDelay = -1;
        logDelay = -1;

        RepNode rep1 = cs.getTopology().getSortedRepNodes().get(1);
        DatacenterId dc1 = new DatacenterId(1);
        VerifyTimeHook verifyHook = new VerifyTimeHook(10000);
        DatabaseUtils.VERIFY_ERROR_HOOK = verifyHook;

        Thread thread = new Thread(() -> {
            int verifyRnPlanId1 = testRNVerify(null, dc1, cs);
            Plan rnPlan1;
            try {
                rnPlan1 = cs.getPlanById(verifyRnPlanId1);
                assertTrue("test multiple plans", rnPlan1.getCommandResult().
                           getReturnValue().contains("\"rg1-rn2\":{" +
                               "\"INTERRUPTED\":\"Verification interrupted " +
                               "by another plan.\"}"));


                assertTrue("test multiple plans", rnPlan1.getCommandResult().
                    getReturnValue().contains("\"rg2-rn1" +
                    "\":{\"Btree Verify\":\"No Btree Corruptions\",\"Log " +
                    "File Verify\":\"No Log File Corruptions\"}"));
                assertTrue("test multiple plans", rnPlan1.getCommandResult().
                    getReturnValue().contains("\"rg2-rn2\":{\"Btree Verify\":" +
                    "\"No Btree Corruptions\",\"Log File Verify\":\"No " +
                    "Log File Corruptions\"},"));
                assertTrue("test multiple plans", rnPlan1.getCommandResult().
                    getReturnValue().contains("\"rg2-rn3\":{\"Btree Verify\":" +
                    "\"No Btree Corruptions\",\"Log File Verify\":\"No " +
                    "Log File Corruptions\"}"));
                assertTrue("test multiple plans", rnPlan1.getCommandResult().
                    getReturnValue().contains("\"rg1-rn3\":{\"Btree Verify\":" +
                    "\"No Btree Corruptions\",\"Log File Verify\":\"No " +
                    "Log File Corruptions\""));
                assertTrue("test multiple plans", rnPlan1.getCommandResult().
                    getReturnValue().contains("\"rg1-rn1\":{\"Btree Verify\":" +
                    "\"No Btree Corruptions\",\"Log File Verify\":\"No " +
                    "Log File Corruptions\"}"));
            } catch (RemoteException e) {
                fail("Error: " + e.getMessage());
            }

        });
        thread.start();
        Thread.sleep(1000);
        int verifyRnPlanId2 = testRNVerify(rep1.getResourceId(), null, cs);
        Plan rnPlan2 = cs.getPlanById(verifyRnPlanId2);
        assertTrue("test RN verify", rnPlan2.getCommandResult().
                   getReturnValue().contains("\"rg1-rn2\":{\"Btree Verify" +
                       "\":\"No Btree Corruptions\",\"Log File Verify\":" +
                       "\"No Log File Corruptions\""));
        thread.join();

    }

    @Test
    public void testShowCorruptFiles() throws Exception {
        createStore.start();
        createTables();

        btree = true;
        log = true;
        index = true;
        dataRecord = true;
        btreeDelay = 0;
        logDelay = -1;
        showMissingFiles = true;

        CommandServiceAPI cs = createStore.getAdmin();

        VerifyCorruptFileHook corruptHook =
            new VerifyCorruptFileHook();
        DatabaseUtils.VERIFY_CORRUPTFILE_HOOK = corruptHook;

        RepNode rep1 = cs.getTopology().getSortedRepNodes().get(1);

        int verifyRnPlanId = testRNVerify(rep1.getResourceId(), null, cs, true);
        Plan rnPlan = cs.getPlanById(verifyRnPlanId);
        /* Check if the result report contains the expected corrupt files. */
        assertTrue("test rn corruption file", rnPlan.getCommandResult().
                   getReturnValue().contains("{\"Verify Report\":" +
                       "{\"rg1-rn2\":{\"Btree Verify\":{\"Missing " +
                       "Files Referenced\":{\"Total number\":2,\"List\":" +
                       "{\"1\":\"456\",\"2\":\"123\"}},\"Reserved Files " +
                       "Referenced\":{\"Total number\":2,\"List\":{\"1\":" +
                       "\"456\",\"2\":\"123\"}},\"Reserved Files Repaired\":" +
                       "{\"Total number\":2,\"List\":{\"1\":\"456\",\"2\":" +
                       "\"123\"}}},\"Log File Verify\":\"No Log File " +
                       "Corruptions\"}}}"));


    }

    private class VerifyCorruptFileHook implements TestHook<VerificationInfo> {

        @Override
        public void doHook(VerificationInfo info) {
            Set<Long> files = new HashSet<Long>();
            files.add((long) 123);
            files.add((long) 456);
            info.missingFilesReferenced.addAll(files);
            info.reservedFilesReferenced.addAll(files);
            info.reservedFilesRepaired.addAll(files);

        }

    }

    private class VerifyInterruptHook implements TestHook<VerificationThread> {
        CommandServiceAPI cs;
        int planId;

        public VerifyInterruptHook(CommandServiceAPI cs, int planId) {
            this.cs = cs;
            this.planId = planId;
        }

        @Override
        public void doHook(VerificationThread runThread) {
            try {
                cs.interruptPlan(planId);
                Thread.sleep(5000);
                runThread.isShutdown();
            } catch (RemoteException | InterruptedException e) {
                e.printStackTrace();
            }

        }

    }

    private class VerifyTimeHook implements TestHook<ReplicatedEnvironment> {
        public int waitTime;

        public VerifyTimeHook(int waitTime) {
            this.waitTime = waitTime;
        }


        @Override
        public void doHook(ReplicatedEnvironment unused) {
            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
            }
        }
    }

    private static class VerifyCorruptionHook implements TestHook<Object[]> {

        public static enum Type {
            LSNOutOfBounds,
            INKeyOrder,
            IdentifierKey
        }

        public static class DBCorruption {
            public Type corruptionType;
            public String dbName;

            public DBCorruption(Type corruptionType, String dbName) {
                this.corruptionType = corruptionType;
                this.dbName = dbName;
            }
        }

        public Map<String, DBCorruption[]> corruptionMap;
        public String snDir;
        public String group;

        public VerifyCorruptionHook(Map<String, DBCorruption[]> corruptionMap,
                                    String snDir,
                                    String group) {
           this.corruptionMap = corruptionMap;
           this.snDir = snDir;
           this.group = group;
        }

        @Override
        public void doHook(Object[] arg) {
            ReplicatedEnvironment env = (ReplicatedEnvironment) (arg[0]);
            ResourceId id = (ResourceId)(arg[1]);

            if (!id.toString().contains(group)) {
                return;
            }

            if (corruptionMap != null) {
                for (String s : corruptionMap.keySet()) {
                    if (!id.getFullName().contains(s)) {
                        continue;
                    }
                    DBCorruption[] corruptions = corruptionMap.get(s);
                    for (DBCorruption corruption : corruptions) {
                        switch(corruption.corruptionType) {
                        case LSNOutOfBounds:
                            Corruption.corruptLSNOutOfBounds(env,
                                                             corruption.dbName,
                                                             null);
                            break;
                        case INKeyOrder:
                            Corruption.corruptINKeyOrder(env,
                                                         corruption.dbName);
                            break;
                        case IdentifierKey:
                            Corruption.corruptIdentifierKey(env,
                                                            corruption.dbName);
                            break;
                        default:
                            throw new IllegalArgumentException("Unsupported " +
                                "corruption type.");

                        }
                    }

                }
            }
            if (snDir != null) {
                File corruptDir = new File(snDir + id + "/env/");
                try {
                    Corruption.corruptLog(corruptDir, 0);
                } catch (IOException e) {
                    /* ignore */
                }
            }

        }

    }

    private class VerifyErrorHook implements TestHook<ReplicatedEnvironment> {
        boolean environmentError;

        public VerifyErrorHook(boolean environmentError) {
            this.environmentError = environmentError;
        }

        @Override
        public void doHook(ReplicatedEnvironment env) {
            if (environmentError) {
                EnvironmentImpl envImpl = DbInternal.getEnvironmentImpl(env);
                EnvironmentFailureException efe =
                    new EnvironmentFailureException(envImpl,
                    EnvironmentFailureReason.UNEXPECTED_STATE_FATAL,
                    "Generated data corruption exception " + "for testing");
                throw efe;
            }
            throw new IllegalArgumentException("Test Error");
        }

    }

    private int doNodeCrushTest(ResourceId verifyNode,
                                 CommandServiceAPI cs,
                                 ResourceId crushNode,
                                 boolean restart) {
        VerificationOptions options =
            new VerificationOptions(btree, log, index,
                                    dataRecord, btreeDelay, logDelay,
                                    validTime, showMissingFiles);
        Set<ResourceId> crushNodeSet = new HashSet<ResourceId>();
        crushNodeSet.add(crushNode);
        try {
            int veriplanId;
            if (verifyNode != null) {
                veriplanId = cs.createVerifyServicePlan("test node crush",
                                                        verifyNode, options);
            } else {
                /* run verification on all nodes */
                veriplanId = cs.createVerifyAllServicesPlan("test node crush",
                                                            new DatacenterId(1),
                                                            options);
            }
            cs.approvePlan(veriplanId);

            int stopPlanId = cs.createStopServicesPlan("stop node",
                                                       crushNodeSet);
            cs.approvePlan(stopPlanId);
            cs.executePlan(stopPlanId, false);
            /* moved here as the plan sometimes completes even before the
             * specific rn/admin could be stopped
             */
            cs.executePlan(veriplanId, false);
            Thread.sleep(4000);
            cs = createStore.getAdminMaster();
            if (restart) {
                int startPlanId = cs.createStartServicesPlan("start node",
                                                             crushNodeSet);
                runPlan(cs, startPlanId);
                cs.awaitPlan(veriplanId, 0, null);
            } else {
                cs.awaitPlan(veriplanId, 2, TimeUnit.MINUTES);
            }

            return veriplanId;

        } catch (Exception e) {
            fail("Plan should be successful.");
        }

        return -1;
    }

    private void runPlan(CommandServiceAPI cs,
                         int planId)
        throws Exception {
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
    }
}
