/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import oracle.kv.ExecutionFuture;
import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.StatementResult;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.plan.PlanExecutor;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.admin.plan.Planner.LockCategory;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;

/**
 * Base class for tests of DDL API execution framework.
 */
public abstract class DdlExecutionBase extends TestBase {

    protected CreateStore createStore;
    protected static final int startPort = 5000;
    protected static final int rf = 1;
    protected static final int numSns = 1;

    @Override
    public void setUp() throws Exception {

        super.setUp();
        TestUtils.clearTestDirectory();
        TestStatus.setManyRNs(true);
        PlanExecutor.FAULT_HOOK = null;
        Admin.EXECUTE_HOOK = null;
        RegistryUtils.clearRegistryCSF();
    }

    @Override
    public void tearDown() throws Exception {
        logger.info("Tearing down test case");
        super.tearDown();
        if (createStore != null) {
            createStore.getStorageNodeAgent(0).resetRMISocketPolicies();
            createStore.shutdown(true);
            createStore = null;
        }
        LoggerUtils.closeAllHandlers();
        mrTableTearDown();
    }

    /**
     * Assert that all fields in the future and result are correct for a
     * successful execution.
     * @param future may be null.
     */
    protected void checkSuccess(ExecutionFuture future,
                                StatementResult result) {
        if (future != null) {
            assertTrue(future.isDone());
            assertFalse(future.isCancelled());
        }

        assertTrue(result.toString(), result.isSuccessful());
        assertTrue(result.toString(), result.getErrorMessage() == null);
        assertTrue(result.isDone());
        assertFalse(result.isCancelled());
        assertTrue(result.toString(), result.getInfo() != null);
    }

    /**
     * Create a one node server and get a kvstore handle
     *
     * @throws Exception
     */
    protected KVStore createMinimalStore() throws Exception {
        createStore = new CreateStore(kvstoreName,
                                      startPort, numSns, rf,
                                      2 /* numPartitions */,
                                      1 /* capacity */,
                                      CreateStore.MB_PER_SN, /* Memory_mb */
                                      true /* useThreads */,
                                      null /* mgmtImpl */);
        createStore.start();
        KVStore store = KVStoreFactory.getStore(createStore.createKVConfig());
        TestBase.mrTableSetUp(store);
        return store;
    }

    /**
     * Cover basic operations and the proxy get-Future oriented API
     * @param store
     * @throws ExecutionException
     * @throws InterruptedException
     */
    protected void doBasicOps(KVStore store)
        throws InterruptedException, ExecutionException {

        /* Create a table */
        String statement = "CREATE TABLE people (id INTEGER, " +
            "name STRING, PRIMARY KEY (id))";
        statement = TestBase.addRegionsForMRTable(statement);
        ExecutionFuture future = store.execute(statement);

        /*
         * Get status. Use the initial status, which was set at execute time,
         * so the checks are correct whether or not the operation is finished.
         * In particular, the statement should not be canceled or have an error
         * message, whether or not it is done.
         */
        StatementResult initialResult = future.getLastStatus();
        assertFalse("canceled", future.isCancelled());
        assertEquals("error message", null, initialResult.getErrorMessage());

        /* Wait for the operation to finish */
        StatementResult result = future.get();
        checkSuccess(future, result);

        /* Ask for status again from the same future */
        result = future.get();
        checkSuccess(future, result);

        /* Create the same table, with an IF NOT EXISTS clause */
        statement = "CREATE TABLE IF NOT EXISTS people (id INTEGER, "
                + "name STRING, PRIMARY KEY (id))";
        statement = TestBase.addRegionsForMRTable(statement);
        future = store.execute(statement);
        result = future.get();
        logger.info(result.toString());
        checkSuccess(future, result);

        result = store.executeSync("show as json tables");
        logger.info("==> show " + result.toString());

        result = store.executeSync("describe as json table people");
        logger.info("==> describe " + result.toString());

        /* Insert and retrieve a row as a sanity check that the table was made*/
        writeAndReadRow(store, 10, "Mia");

        /*
         * Create a second table, test the paths taken by the proxy/thin
         * clients. Since the thin client and proxy don't save a handle to a
         * Future, they need to use the commandId to access all information.
         */
        statement = "CREATE TABLE homes (number INTEGER, street STRING, "+
            "city STRING, PRIMARY KEY (number, street))";
        statement = TestBase.addRegionsForMRTable(statement);
        ExecutionFuture proxyFuture = store.execute(statement);
        result = proxyFuture.getLastStatus();
        byte[] futureBytes = proxyFuture.toByteArray();
        /* Wait for completion. */
        result = store.getFuture(futureBytes).get();
        checkSuccess(null, result);

        /* Get a new, different future instance, and check status again */
        result = store.getFuture(futureBytes).updateStatus();
        checkSuccess(null, result);
    }

    /**
     * Insert the id/name pair into the people table.
     */
    private void writeAndReadRow(KVStore store, int idVal, String nameVal) {
        TableAPI tableAPI = store.getTableAPI();
        Table userT = tableAPI.getTable("people");
        Row insertRow =
            userT.createRowFromJson("{\"id\" : " + idVal +
                                    ", \"name\" : \"" + nameVal + "\"}", true);
        tableAPI.put(insertRow, null, null);
        PrimaryKey pk = userT.createPrimaryKeyFromJson
            ("{\"id\" : " + idVal + "}", true);

        Row foundRow = tableAPI.get(pk, null);
        assertEquals(nameVal, foundRow.get("name").asString().get());
    }

    /**
     * Grab the plan catalog's locks on the behalf of an trumped-up
     * plan, in order to test that the DDL handles the fault exception
     * correctly. Note that the provided planId should be for a legitimate
     * plan, because the code that tries to do logical compares for in the
     * face of lock conflicts will expect a real plan. It's okay for the planId
     * to be for a finished plan.
     */
    class LockOutDDL implements TestHook<Admin> {

        private Planner planner;
        private final int planId;
        private final String tableName;

        LockOutDDL(int planId, String tableName) {
            this.planId = planId;
            this.tableName = tableName;
        }

        @Override
        public void doHook(Admin admin) {
            planner = admin.getPlanner();
            try {
                planner.lock(planId, "Plan" + planId, LockCategory.TABLE,
                             "", tableName);
            } catch (PlanLocksHeldException e) {
                fail("Test plan should be successful at plan locking");
            }
        }

        /** Returns true so it can be used in asserts */
        boolean clearLocks() {
            planner.clearLocks(planId);
            return true;
        }
    }

    class StallHook implements TestHook<Integer> {
        private final CountDownLatch stall;

        StallHook(CountDownLatch stall) {
            this.stall = stall;
        }

        @Override
        public void doHook(Integer unused) {
            try {
                stall.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    class SpawnWait extends Thread {
        private long start;
        private long end;
        private final ExecutionFuture future;
        private final long time;
        private final TimeUnit timeUnit;
        private StatementResult result;
        private Exception problem;

        public SpawnWait(ExecutionFuture future, long time, TimeUnit timeUnit) {
            super();
            this.future = future;
            this.time = time;
            this.timeUnit = timeUnit;
        }

        @Override
        public void run() {
            start = System.currentTimeMillis();
            logger.info("Start waiting for operation finish");
            try {
                result = future.get(time, timeUnit);
            } catch (TimeoutException e) {
                problem = e;
            } catch (InterruptedException e) {
                problem = e;
            } catch (ExecutionException e) {
                problem = e;
            }
            end = System.currentTimeMillis();
            logger.info("Stop waiting for operation finish, elapsed = "
                    + getElapsed());
        }

        public StatementResult getResult() {
            return result;
        }

        long getElapsed() {
            return end - start;
        }

        Exception getProblem() {
            return problem;
        }
    }
}
