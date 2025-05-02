/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import oracle.kv.ExecutionFuture;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.StatementResult;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.DdlResultsReport;
import oracle.kv.impl.admin.plan.PlanExecutor;
import oracle.kv.impl.client.DdlJsonFormat;
import oracle.kv.impl.systables.SysTableDescriptor;
import oracle.kv.impl.systables.SysTableRegistry;
import oracle.kv.impl.xregion.XRegionTestBase;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;

import org.junit.Test;

/**
 * Basic tests of Table DDL API.Focus is on the API execution rather than
 * comprehensive testing of the syntax and semantics of DDL statements.
 */
public class DdlExecutionTest extends DdlExecutionBase {

    /**
     * Mimic the APIs used by the proxy and drivers.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testProxy() throws Exception {
        KVStore store = createMinimalStore();

        /* ----  Test basic execute ------ */
        /* Same as ONDBHandler.execute(statement) */
        String statement =
            "create table t1 " +
            "(id integer, sid integer, stringF string, " +
            "primary key(id, sid))";
        statement = TestBase.addRegionsForMRTable(statement);
        ExecutionFuture ef = store.execute(statement);
        StatementResult result = ef.getLastStatus();

        /* Same as ONDBHandler.executionFutureGet */
        ExecutionFuture recreatedFuture = store.getFuture(ef.toByteArray());
        result = recreatedFuture.get();
        checkSuccess(null, result);
        assertEquals(statement,  recreatedFuture.getStatement());
        assertTrue(jsonObjectExists(result.getInfoAsJson(), "planInfo"));

        /* Make another future with the bytes */
        ExecutionFuture yetAnotherFuture =
                store.getFuture(recreatedFuture.toByteArray());
        result = yetAnotherFuture.get();
        checkSuccess(null, result);
        assertEquals(statement,  yetAnotherFuture.getStatement());

        /* ----  Test no-op execute ------ */
        /*
         * Same as ONDBHandler.execute(statement). This is a no-op statement
         * KVStore.getFuture(byte[]) must work for non-executing statements.
         */
        statement = "create table if not exists t1 " +
            "(id integer, sid integer, stringF string, " +
            "primary key(id, sid))";
        statement = TestBase.addRegionsForMRTable(statement);
        ef = store.execute(statement);
        byte[] futureBytes = ef.toByteArray();
        result = ef.getLastStatus();
        assertEquals(statement, ef.getStatement());
        logger.info("no-exec result:" + result);

        /* No plan was executed */
        assertEquals(0, result.getPlanId());
        checkSuccess(null, result);

        /* Test this deprecated API test */
        try {
            store.getTableAPI().getFuture(result.getPlanId());
            fail("the thin client is supposed to refrain calling get() " +
                 "if the future is done and planId == 0");
        } catch (IllegalArgumentException expected) {
        }

        /*
         * Check that a reconstituted future for a non-executing statement
         * still works.
         */
        recreatedFuture = store.getFuture(futureBytes);
        logger.info("recreatedFuture:" + recreatedFuture.isDone());

        /* No call to server -- are the data structures still valid? */
        StatementResult lastStatus = recreatedFuture.getLastStatus();
        checkSuccess(recreatedFuture, lastStatus);
        assertEquals(statement, recreatedFuture.getStatement());
        assertEquals(DdlResultsReport.NOOP_STATUS_JSON,
                     lastStatus.getInfoAsJson());

        /* Call to server and get updated status */
        lastStatus = recreatedFuture.get();
        checkSuccess(recreatedFuture, lastStatus);
        assertEquals(statement, recreatedFuture.getStatement());

        /* try cancelling */
        boolean cancelStatus = recreatedFuture.cancel(true);
        assertFalse(cancelStatus);

        /* ---- check status of a statement before it finishes executing */
        /* Drop a table */
        CountDownLatch staller = new CountDownLatch(1);
        PlanExecutor.FAULT_HOOK = new StallHook(staller);
        statement = "drop table t1";
        ef = store.execute(statement);
        result = ef.getLastStatus();
        assertFalse(result.isDone());
        assertFalse(result.isSuccessful());

        /* Check status before it's finished */
        recreatedFuture = store.getFuture(ef.toByteArray());
        try {
            result = recreatedFuture.get(10, TimeUnit.MILLISECONDS);
        } catch (TimeoutException expected) {
            assertFalse(result.isSuccessful());
            assertFalse(result.isDone());
            assertEquals(statement, recreatedFuture.getStatement());
        }

        /* Let the statement finish, then get a new future again */
        staller.countDown();
        recreatedFuture = store.getFuture(ef.toByteArray());
        assertEquals(statement, recreatedFuture.getStatement());
        result = recreatedFuture.get();
        checkSuccess(recreatedFuture, result);

        /* --------- check how long a wait takes ------ */
        staller = new CountDownLatch(1);
        PlanExecutor.FAULT_HOOK = new StallHook(staller);
        ef = store.execute
            (TestBase.addRegionsForMRTable("create table t2 (id " +
                "integer, name string, primary key (id))"));
        /* Check status before it's finished */
        futureBytes = ef.toByteArray();
        ExecutionFuture newF = store.getFuture(futureBytes);
        try {
            result = newF.get(10, TimeUnit.MILLISECONDS);
        } catch (TimeoutException expected) {
            result = newF.updateStatus();
            assertFalse(result.isDone());
            assertFalse(result.isSuccessful());
        }

        /* call get w/a time period, then let the plan finish */
        ef = store.getFuture(futureBytes);
        SpawnWait waiter = new SpawnWait(ef, 50, TimeUnit.SECONDS);
        waiter.start();
        staller.countDown();
        waiter.join();
        long elapsed = waiter.getElapsed();
        assertTrue("Waiter saw " + waiter.getProblem(),
                   waiter.getProblem() == null);
        assertTrue("elapsed = " + elapsed,
                   TimeUnit.MILLISECONDS.toSeconds(elapsed) < 3);
        store.close();
    }

    /**
     * Basic execute/get
     *
     * @throws Exception
     */
    @Test
    public void testBasic() throws Exception {
        KVStore store = createMinimalStore();
        doBasicOps(store);
        store.close();
    }

    /**
     * This test just validates the example used in the javadoc for
     * KVStore.execute, to make sure it's valid and works.
     *
     * @throws Exception
     */
    @Test
    public void testJavadocExample() throws Exception {
        KVStore store = createMinimalStore();

        /*
         * Use non-standard comments so the code fragment can be cut and
         * pasted into javadoc. This snippet is for KVStore.execute()
         * Intentionally don't exit out of the catches -- trying to keep
         * the example small.
         */
        // Create a table
        ExecutionFuture future = null;
        try {
            future = store.execute
                (TestBase.addRegionsForMRTable("CREATE TABLE people (" +
                 "id INTEGER, " +
                 "firstName STRING, " +
                 "lastName STRING, " +
                 "age INTEGER, " +
                 "PRIMARY KEY (id))"));
        } catch (IllegalArgumentException e) {
            System.out.println("The statement is invalid: " + e);
        } catch (FaultException e) {
            System.out.println("There is a transient problem, retry the " +
                               "operation: " + e);
        }

        // Wait for the operation to finish
        @SuppressWarnings("null")
        StatementResult result = future.get();

        /* --- end of KVStore.execute() snippet --- */
        /* --- snippet for ExecutionFuture.toByteArray --- */
        byte[] futureBytes = future.toByteArray();

        // futureBytes can be saved and used later to recreate an
        // ExecutionFuture instance
        ExecutionFuture laterFuture = store.getFuture(futureBytes);

        // laterFuture doesn't have any status yet. Call ExecutionFuture.get
        // or updateStatus to communicate with the server and get new
        // information
        @SuppressWarnings("unused")

        StatementResult laterResult = laterFuture.get();

        /*
         * Print out the results of a no-op, a describe, a show, and a plan,
         * for the javadoc of StatementResult.getResult, getResultAsAJson
         */
        result = store.executeSync
            ("CREATE INDEX IF NOT EXISTS LastName ON people(lastName)");
        logger.info("result for plan:\n" + result.getInfoAsJson());
        result = store.executeSync("SHOW AS JSON TABLE people");
        logger.info("result for show:\n" + result.getInfoAsJson());
        result = store.executeSync("DESCRIBE AS JSON TABLE people");
        logger.info("result for describe:\n" + result.getInfoAsJson());
        result = store.executeSync
            ("CREATE INDEX IF NOT EXISTS LastName ON people(lastName)");
        logger.info("result for noop:\n" + result.getInfoAsJson());

        /*
         * Code sample for illustrating concurrent examples -- currently
         * provoking failure.
         * The fix for [#23919] fixed one situation that caused this to fail.
         * But the lack of idempotency in table plans causes additional
         * problems. Specifically, ddl execution consists of two steps:
         * 1) parsing/metadata checks
         * 2) plan execution
         *
         * Because plan execution isn't fully idempotent, the following
         * interleaving can happen:
         * Statement A does parsing/metadata check
         * Statement A' does parsing/metadata check
         * Statement A executes plan, completes
         * Statement A' executes plan, but gets error because the plan isn't
         * idempotent -- i.e. the index exists, etc.
         *
         * This is akin to the problem if a index or table creation is
         * cancelled before the index or table becomes READY. In that
         * case, the plan must be re-executed, but because there's been no
         * cleanup, and also because the plan is not idempotent, there's no
         * easy way to make progress.
         */

        // process A starts an index creation
        //ExecutionFuture futureA =
          //  store.execute("CREATE INDEX age ON people(age)");

        // process B starts the same index creation. If the index creation is
        // still running, futureA and futureB will refer to the same operation
        //ExecutionFuture futureB =
            //store.execute("CREATE INDEX age ON people(age)");

        store.close();
    }

    /**
     * Test operation cancellation
     * @throws Exception
     */
    @Test
    public void testCancel() throws Exception {

        KVStore store = createMinimalStore();

        /*
         * Start an operation, and let it finish. Check that cancellation of
         * a successful operation fails.
         */
        String statement = "CREATE TABLE people (id INTEGER, " +
            "name STRING, PRIMARY KEY (id))";
        statement = TestBase.addRegionsForMRTable(statement);
        final ExecutionFuture ef = store.execute(statement);
        StatementResult result = ef.get();
        checkSuccess(ef, result);
        assertFalse(ef.cancel(true));

        /*
         * Setup an artificial stall, so we can test the cancellation of an
         * operation that is sure to be running. Setting up the PlanExecutor's
         * FAULT_HOOK ensure the plan will hang until we release the latch.
         */
        CountDownLatch staller = new CountDownLatch(1);
        PlanExecutor.FAULT_HOOK = new StallHook(staller);
        final ExecutionFuture dropEf = store.execute("drop table people");

        /* operation should be stalling */
        result = dropEf.getLastStatus();
        assertFalse(result.isDone());
        assertFalse(result.isSuccessful());
        assertFalse(result.isCancelled());
        assertFalse(dropEf.isDone());
        assertFalse(dropEf.isCancelled());

        /*
         * Try canceling w/the mayInterruptIfRunning flag to false. Should
         * fail, the operation is already running.
         */
        assertFalse(dropEf.cancel(false));

        /*
         * Now coordinate the cancellation. The goal is to
         * - be sure the operation is running (accomplished w/staller)
         * - call cancel, expecting it to block, because the operation hasn't
         * finished
         * - release the staller, so the operation finishes
         * - check the status of the operation after the plan is known to
         * have completed.
         */
        Executor exec = Executors.newCachedThreadPool();
        final AtomicBoolean cancelTaskDone = new AtomicBoolean();
        final CountDownLatch cancelTaskLatch = new CountDownLatch(1);
        exec.execute(new Runnable() {
                @Override
                public void run() {
                    dropEf.cancel(true);
                    cancelTaskDone.set(true);
                    cancelTaskLatch.countDown();
                }
            });
        /* The cancel hasn't finished, because the plan is stalled */
        assertFalse(cancelTaskDone.get());

        /* Now free the plan */
        staller.countDown();

        /* Wait for the cancel to return */
        cancelTaskLatch.await();

        result = dropEf.getLastStatus();
        assertTrue(result.isDone());
        assertFalse(result.isSuccessful());
        assertTrue(result.isCancelled());
        assertTrue(dropEf.isDone());
        assertTrue(dropEf.isCancelled());

        /* Try a cancel again -- should fail because it's already cancelled. */
        assertFalse(dropEf.cancel(true));

        /* Try a get, should throw. */
        try {
            dropEf.get();
            fail("Cancel should fail");
        } catch (CancellationException expected) {
        }
        store.close();
    }

    /**
     * Test concurrent handles
     * @throws Exception
     */
    @Test
    public void testConcurrentHandles() throws Exception {

        KVStore store = createMinimalStore();

        /* Test creating tables with the same name */
        String statement =
            "create table foo(A string, B integer, C array (string)," +
            "primary key (A, B))";
        statement = TestBase.addRegionsForMRTable(statement);
        String[] concurrent = new String[]{
            TestBase.addRegionsForMRTable("create table foo(A integer, " +
                "B integer, C array (string), primary key (A, B))"),
            TestBase.addRegionsForMRTable("create table foo(A string, " +
                "B integer, C array (integer), primary key (A, B))"),
            TestBase.addRegionsForMRTable("create table foo(A string, " +
                "B integer, C array (string), primary key (B))"),
            TestBase.addRegionsForMRTable("create table foo(A string, " +
                "B integer, C array (string), primary key (shard (A), B))")//,
        };
        tryStatements(store, statement, concurrent, false);

        /* Test creatating tables with different names */
        statement =
            "create table bar(A string, B integer, C array (string)," +
            "primary key (A, B))";
        statement = TestBase.addRegionsForMRTable(statement);
        concurrent = new String[]{
            TestBase.addRegionsForMRTable("create table someOtherTable " +
                "(id integer, value string, primary key (id))")
        };
        tryStatements(store, statement, concurrent, true);

        /* Test creatating indices on the same table with the same name */
        statement = "create index AIndex on foo(B, C[])";
        concurrent  = new String[]{
            "create index AIndex on foo(B)",
        };
        tryStatements(store, statement, concurrent, false);

        /* Test creatating indices on the same table with different names */
        statement = "create index XIndex on foo(A, B, C[])";
        concurrent  = new String[]{
            "create index YIndex on foo(B)",
        };
        tryStatements(store, statement, concurrent, true);


        /* Test creating and dropping indices */
        statement = "drop index AIndex on foo";
        concurrent  = new String[]{
            "create index BIndex on foo(C[])",
        };
        tryStatements(store, statement, concurrent, true);

        /* Test alter table of the same table */
        statement = "alter table foo (add extraF integer)";
        concurrent  = new String[]{
            "alter table foo (add F integer)",
        };
        tryStatements(store, statement, concurrent, false);

        /* Test alter table and drop table */
        statement = "drop table foo";
        concurrent  = new String[]{"alter table foo (add F integer)"};

        tryStatements(store, statement, concurrent, false);

        store.close();
    }

    /**
     * Starts execution of statement and then blocks. Then concurrentStatements
     * are executed. If shouldWork is true, the concurrent statements should
     * complete. Otherwise they should fail being blocked on the initial
     * statement. The initial statement is released so that it completes.
     */
    private void tryStatements(KVStore kvstore,
                               String statement,
                               String[] concurrentStatements,
                               boolean shouldWork)
        throws Exception {

        /*
         * Setup an artificial stall
         */
        CountDownLatch staller = new CountDownLatch(1);
        PlanExecutor.FAULT_HOOK = new StallHook(staller);
        final ExecutionFuture efA = kvstore.execute(statement);

        /* operation should be stalling */
        StatementResult result = efA.getLastStatus();
        assertFalse(result.isDone());
        assertFalse(result.isSuccessful());
        assertFalse(result.isCancelled());
        assertFalse(efA.isCancelled());
        int planIdA = result.getPlanId();

        /*
         * Spawn a concurrent table create. It should get the same plan id
         * as the first.
         */
        final ExecutionFuture efB = kvstore.execute(statement);
        result = efB.getLastStatus();
        assertFalse(result.isDone());
        assertFalse(result.isSuccessful());
        assertFalse(result.isCancelled());
        assertFalse(efB.isCancelled());
        assertEquals(planIdA, result.getPlanId());

        /*
         * Spawn other concurrent statements. Save the futures of the ones
         * that succeed so that they can be waited on when finished.
         */
        final List<ExecutionFuture> concurrentEFs =
                new ArrayList<>(concurrentStatements.length);
        for (String stmt : concurrentStatements) {
            try {
                concurrentEFs.add(kvstore.execute(stmt));
                if (!shouldWork) {
                    fail("Should fail: " + stmt);
                }
            } catch (FaultException expected) {
                if (shouldWork) {
                    fail("Should work: " + stmt);
                }
                assertTrue("Statement: " + stmt +
                           " failed with unexpected fault: " +
                           expected.getMessage(),
                           expected.getMessage().
                                        contains("Wait until that plan"));
                logger.info(">>> " + stmt + " -- " + expected.toString());
            }
        }

        /* Now free the first plan */
        staller.countDown();

        result = efA.get();
        assertTrue(result.isDone());
        assertTrue(result.isSuccessful());
        assertFalse(result.isCancelled());
        assertTrue(efA.isDone());
        assertFalse(efA.isCancelled());

        result = efB.get();
        assertTrue(result.isDone());
        assertTrue(result.isSuccessful());
        assertFalse(result.isCancelled());
        assertTrue(efB.isDone());
        assertFalse(efB.isCancelled());

        /*
         * Wait for all concurrent statements that were successfully executed.
         */
        for (ExecutionFuture ef : concurrentEFs) {
            ef.get();
        }
        PlanExecutor.FAULT_HOOK = null;
    }

    /**
     * Test the deprecated TableAPI.execute paths and result classes from
     * 3.2. Those methods delegate to the newer KVStore.execute() methods,
     * and wrap the newer oracle.kv result classes, so a single test is
     * sufficient. Remove this test when the deprecated methods are removed.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testDeprecatedAPIs() throws Exception {
        KVStore store = createMinimalStore();
        TableAPI tAPI = store.getTableAPI();

        /* ----  Test basic execute ------ */
        /* Same as ONDBHandler.execute(statement) */
        oracle.kv.table.ExecutionFuture ef = tAPI.execute
            (TestBase.addRegionsForMRTable("create table t1 " +
             "(id integer, sid integer, stringF string, " +
             "primary key(id, sid))"));
        oracle.kv.table.StatementResult result = ef.getLastStatus();

        /* Same as ONDBHandler.executionFutureGet */
        result = tAPI.getFuture(result.getPlanId()).get();

        /* ----  Test no-op execute ------ */
        /* Same as ONDBHandler.execute(statement). This is a no-op statement */
        ef = tAPI.execute
                (TestBase.addRegionsForMRTable("create table if not exists " +
                 "t1 (id integer, sid integer, stringF string, " +
                 "primary key(id, sid))"));
            result = ef.getLastStatus();
            /* No plan was executed */
            assertEquals(0, result.getPlanId());
            assertTrue(result.toString(), result.isSuccessful());
            assertTrue(result.toString(), result.getErrorMessage() == null);
            assertTrue(result.isDone());
            assertFalse(result.isCancelled());
            assertTrue(result.toString(), result.getInfo() != null);

        try {
            tAPI.getFuture(result.getPlanId()).get();
            fail("the thin client is supposed to refrain calling get() " +
                 "if the future is done and planId == 0");
        } catch (IllegalArgumentException expected) {
        }

        /* ---- check status of a statement before it finishes executing */
        /* Drop a table */
        CountDownLatch staller = new CountDownLatch(1);
        PlanExecutor.FAULT_HOOK = new StallHook(staller);
        ef = tAPI.execute("drop table t1 ");
        result = ef.getLastStatus();
        assertFalse(result.isDone());
        assertFalse(result.isSuccessful());

        /* Check status before it's finished */
        try {
            result = tAPI.getFuture(result.getPlanId()).
                get(10, TimeUnit.MILLISECONDS);
        } catch (TimeoutException expected) {
             assertFalse(result.isSuccessful());
             assertFalse(result.isDone());
        }

        /* Let the statement finish, then check status */
        staller.countDown();
        result = tAPI.getFuture(result.getPlanId()).get();
        assertTrue(result.toString(), result.isSuccessful());
        assertTrue(result.toString(), result.getErrorMessage() == null);
        assertTrue(result.isDone());
        assertFalse(result.isCancelled());
        assertTrue(result.toString(), result.getInfo() != null);

        result = tAPI.executeSync
                (TestBase.addRegionsForMRTable("create table if not exists " +
                 "t1 (id integer, sid integer, stringF string, " +
                 "primary key(id, sid))"));
            assertTrue(result.toString(), result.isSuccessful());
            assertTrue(result.toString(), result.getErrorMessage() == null);
            assertTrue(result.isDone());
            assertFalse(result.isCancelled());
            assertTrue(result.toString(), result.getInfo() != null);

        store.close();
    }

    /**
     * Check that the StatementResult.getInfo, getInfoAsJson and getResults
     * return the right values for statements that generate plans,
     * statements that don't execute, and statements that return information.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testGetInfo() throws Exception {
        KVStore store = createMinimalStore();
        XRegionTestBase.waitForMRSystemTables(store.getTableAPI());

        /* ----  Test basic execute ------ */
        /* Same as ONDBHandler.execute(statement) */
        String statement =
            "create table if not exists t1 " +
            "(id integer, sid integer, stringF string, " +
            "primary key(id, sid))";
        statement = TestBase.addRegionsForMRTable(statement);
        StatementResult sr = store.executeSync(statement);
        String textInfo = sr.getInfo();
        String jsonInfo = sr.getInfoAsJson();
        logger.info("first");
        logger.info(textInfo);
        logger.info(jsonInfo);
        assertTrue(textInfo.startsWith("Plan"));
        /* Make sure the output is valid Json */
        assertEquals("2", getJsonField(jsonInfo, "version"));
        assertNull(sr.getResult());

        /* ----  Test no-op execute ------ */
        /*
         * Same as ONDBHandler.execute(statement). This is a no-op statement
         * KVStore.getFuture(byte[]) must work for non-executing statements.
         */
        logger.info("no-exec");
        sr = store.executeSync(statement);
        textInfo = sr.getInfo();
        jsonInfo = sr.getInfoAsJson();
        logger.info(textInfo);
        logger.info(jsonInfo);
        assertEquals(textInfo, textInfo, DdlJsonFormat.NOOP_STATUS);
        assertEquals(jsonInfo, jsonInfo, DdlResultsReport.NOOP_STATUS_JSON);
        assertNull(sr.getResult());

        sr = store.executeSync("show tables");
        logger.info("show");
        textInfo = sr.getInfo();
        jsonInfo = sr.getInfoAsJson();
        logger.info(textInfo);
        logger.info(jsonInfo);
        logger.info(sr.getResult());
        assertEquals(textInfo, textInfo, DdlResultsReport.STATEMENT_COMPLETED);
        assertEquals(jsonInfo, jsonInfo,
                DdlResultsReport.STATEMENT_COMPLETED_JSON);
        assertTrue(sr.getResult().contains("t1"));

        sr = store.executeSync("show as json tables");
        logger.info("show as json");
        textInfo = sr.getInfo();
        jsonInfo = sr.getInfoAsJson();
        String results = sr.getResult();
        logger.info(textInfo);
        logger.info(jsonInfo);
        logger.info(results);
        assertEquals(textInfo, textInfo, DdlResultsReport.STATEMENT_COMPLETED);
        assertEquals(jsonInfo, jsonInfo,
                DdlResultsReport.STATEMENT_COMPLETED_JSON);

        /*
         * Build a list of tables, starting with system tables based on the
         * registry.
         */
        final StringBuilder sb = new StringBuilder();
        sb.append("{\"tables\" : [");
        for (SysTableDescriptor desc : SysTableRegistry.descriptors) {
            sb.append("\"").append(desc.getTableName()).append("\",");
        }
        /* Add t1 */
        sb.append("\"t1\"]}");
        final String systemTableList = sb.toString();

        assertEquals(systemTableList, results);

        sr = store.executeSync("describe as json table t1");
        logger.info("describe as json");
        textInfo = sr.getInfo();
        jsonInfo = sr.getInfoAsJson();
        results = sr.getResult();
        assertEquals(textInfo, textInfo, DdlResultsReport.STATEMENT_COMPLETED);
        assertEquals(jsonInfo, jsonInfo,
                DdlResultsReport.STATEMENT_COMPLETED_JSON);
        logger.info(results);
        assertEquals("table", getJsonField(results, "type"));

        /*
         * Mimic the pre R3.3 path. A non-executing statement would generate
         * a planId of 0, and the caller would have to refrain from using it.
         */
        TableAPI tableAPI = store.getTableAPI();
        oracle.kv.table.ExecutionFuture oldF = tableAPI.execute("show tables");
        int planId = oldF.getLastStatus().getPlanId();

        assertEquals(0, planId);
        try {
            tableAPI.getFuture(planId);
        } catch (IllegalArgumentException expected) {
        }

        /*
         * Mimic the path of a stateless client. Ensure that the results for
         * a show/describe are available for futures created from serialized
         * future bytes.
         */
        ExecutionFuture show1F = store.execute("show as json tables");
        sr = show1F.getLastStatus();
        assertEquals(sr.getInfo(), DdlResultsReport.STATEMENT_COMPLETED);
        assertEquals(sr.getInfoAsJson(),
                     DdlResultsReport.STATEMENT_COMPLETED_JSON);
        assertEquals(systemTableList, sr.getResult());

        /* Serialize the future, then resurrect it. */
        byte[] show1FBytes = show1F.toByteArray();
        ExecutionFuture show2F = store.getFuture(show1FBytes);

        sr = show2F.get();
        assertTrue(sr.isDone());
        assertTrue(sr.isSuccessful());
        assertEquals(sr.getInfo(), DdlResultsReport.STATEMENT_COMPLETED);
        assertEquals(sr.getInfoAsJson(),
                     DdlResultsReport.STATEMENT_COMPLETED_JSON);
        assertEquals(systemTableList, sr.getResult());
        store.close();
    }

    /**
     * Checks alter table on records with embedded maps and arrays.
     * SR #24049  enhance alter table on records embedded in maps and arrays
     */
    //@SuppressWarnings("deprecation")
    @Test
    public void testAlterTableWithEmbMapsArrays() throws Exception {
        KVStore store = createMinimalStore();

        /* ----  Test basic execute ------ */
        String statement =
            "create table t24049 (id integer, " +
                "map1 map(record(i1 integer))," +
                "arr1 array(record(i2 integer)), " +
                "rec1 record(rec2 record(i3 integer))," +
                "primary key(id))";
        statement = TestBase.addRegionsForMRTable(statement);
        StatementResult sr = store.executeSync(statement);
        assertTrue(sr.isDone() && sr.isSuccessful());

        Table t = store.getTableAPI().getTable("t24049");

        assertNotNull(t.getField("map1"));
        assertTrue(t.getField("map1").isMap());
        assertNotNull(t.getField("map1").asMap().getElement());
        assertTrue(t.getField("map1").asMap().getElement().isRecord());
        assertNotNull(t.getField("map1").asMap().getElement()
            .asRecord().getFieldDef("i1"));
        assertTrue(t.getField("map1").asMap().getElement()
            .asRecord().getNumFields() == 1);

        assertNotNull(t.getField("arr1"));
        assertTrue(t.getField("arr1").isArray());
        assertNotNull(t.getField("arr1").asArray().getElement());
        assertTrue(t.getField("arr1").asArray().getElement().isRecord());
        assertNotNull(t.getField("arr1").asArray().getElement()
            .asRecord().getFieldDef("i2"));
        assertTrue(t.getField("arr1").asArray().getElement()
            .asRecord().getNumFields() == 1);

        assertNotNull(t.getField("rec1"));
        assertTrue(t.getField("rec1").isRecord());
        assertNotNull(t.getField("rec1").asRecord().getFieldDef("rec2"));
        assertTrue(t.getField("rec1").asRecord().getFieldDef("rec2").isRecord());
        assertNotNull(t.getField("rec1").asRecord().getFieldDef("rec2")
            .asRecord().getFieldDef("i3"));
        assertTrue(t.getField("rec1").asRecord().getFieldDef("rec2")
            .asRecord().getNumFields() == 1);



        statement = "alter table t24049 (add rec1.rec2.new3 boolean )";
        sr = store.executeSync(statement);
        assertTrue(sr.isDone() && sr.isSuccessful());
        t = store.getTableAPI().getTable("t24049");

        assertNotNull(t.getField("rec1"));
        assertTrue(t.getField("rec1").isRecord());
        assertNotNull(t.getField("rec1").asRecord().getFieldDef("rec2"));
        assertTrue(t.getField("rec1").asRecord().getFieldDef("rec2").isRecord());
        assertTrue(t.getField("rec1").asRecord().getFieldDef("rec2")
            .asRecord().getFieldNames().size() == 2);
        assertNotNull(t.getField("rec1").asRecord().getFieldDef("rec2")
            .asRecord().getFieldDef("i3"));
        assertNotNull(t.getField("rec1").asRecord().getFieldDef("rec2")
            .asRecord().getFieldDef("new3"));


        statement = "alter table t24049 (drop rec1.rec2.i3 )";
        sr = store.executeSync(statement);
        assertTrue(sr.isDone() && sr.isSuccessful());
        t = store.getTableAPI().getTable("t24049");

        assertNotNull(t.getField("rec1"));
        assertTrue(t.getField("rec1").isRecord());
        assertNotNull(t.getField("rec1").asRecord().getFieldDef("rec2"));
        assertTrue(t.getField("rec1").asRecord().getFieldDef("rec2").isRecord());
        assertNotNull(t.getField("rec1").asRecord().getFieldDef("rec2")
            .asRecord().getFieldDef("new3"));
        assertTrue(t.getField("rec1").asRecord().getFieldDef("rec2")
            .asRecord().getFieldNames().size() == 1);



        statement = "alter table t24049 (add map1.values().new1 integer default 5)";
        sr = store.executeSync(statement);
        assertTrue(sr.isDone() && sr.isSuccessful());
        t = store.getTableAPI().getTable("t24049");

        assertNotNull(t.getField("map1"));
        assertTrue(t.getField("map1").isMap());
        assertNotNull(t.getField("map1").asMap().getElement());
        assertTrue(t.getField("map1").asMap().getElement().isRecord());
        assertTrue(t.getField("map1").asMap().getElement()
            .asRecord().getFieldNames().size() == 2);
        assertNotNull(t.getField("map1").asMap().getElement()
            .asRecord().getFieldDef("i1"));
        assertNotNull(t.getField("map1").asMap().getElement()
            .asRecord().getFieldDef("new1"));


        statement = "alter table t24049 (drop map1.values().i1 )";
        sr = store.executeSync(statement);
        assertTrue(sr.isDone() && sr.isSuccessful());
        t = store.getTableAPI().getTable("t24049");

        assertNotNull(t.getField("map1"));
        assertTrue(t.getField("map1").isMap());
        assertNotNull(t.getField("map1").asMap().getElement());
        assertTrue(t.getField("map1").asMap().getElement().isRecord());
        assertNotNull(t.getField("map1").asMap().getElement()
            .asRecord().getFieldDef("new1"));
        assertTrue(t.getField("map1").asMap().getElement()
            .asRecord().getFieldNames().size() == 1);


        statement = "alter table t24049 (add arr1[].new2 string " +
            "default 'string')";
        sr = store.executeSync(statement);
        assertTrue(sr.isDone() && sr.isSuccessful());
        t = store.getTableAPI().getTable("t24049");

        assertNotNull(t.getField("arr1"));
        assertTrue(t.getField("arr1").isArray());
        assertNotNull(t.getField("arr1").asArray().getElement());
        assertTrue(t.getField("arr1").asArray().getElement().isRecord());
        assertTrue(t.getField("arr1").asArray().getElement()
            .asRecord().getFieldNames().size() == 2);
        assertNotNull(t.getField("arr1").asArray().getElement()
            .asRecord().getFieldDef("i2"));
        assertNotNull(t.getField("arr1").asArray().getElement()
            .asRecord().getFieldDef("new2"));


        statement = "alter table t24049 (drop arr1[].i2 )";
        sr = store.executeSync(statement);
        assertTrue(sr.isDone() && sr.isSuccessful());
        t = store.getTableAPI().getTable("t24049");

        assertNotNull(t.getField("arr1"));
        assertTrue(t.getField("arr1").isArray());
        assertNotNull(t.getField("arr1").asArray().getElement());
        assertTrue(t.getField("arr1").asArray().getElement().isRecord());
        assertTrue(t.getField("arr1").asArray().getElement()
            .asRecord().getFieldNames().size() == 1);
        assertNotNull(t.getField("arr1").asArray().getElement()
            .asRecord().getFieldDef("new2"));


        // add errors
        try {
            statement = "alter table t24049 ( add arr1 integer )";
            store.executeSync(statement);
            assertTrue(false);
        } catch (FaultException e) {
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            String msg = e.getMessage();
            assertTrue(msg.contains("Error: at (1, 25)") &&
                       msg.contains("already exists"));
        }

        try {
            statement = "alter table t24049 (add arr1[].new2 string)";
            store.executeSync(statement);
            assertTrue(false);
        } catch (FaultException e) {
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertEquals(
                "Error: at (1, 24) Cannot add field, arr1[].new2, " +
                "it already exists",
                e.getMessage());
        }

        try {
            statement = "alter table t24049 (add arr2[].new2 string)";
            store.executeSync(statement);
            assertTrue(false);
        } catch (FaultException e) {
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("path does not exist"));
        }

        try {
            statement = "alter table t24049 (add arr1[].new2[].foo string)";
            store.executeSync(statement);
            assertTrue(false);
        } catch (FaultException e) {
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            String msg = e.getMessage();
            assertTrue(msg.contains("Error: at (1, 24)") &&
                       msg.contains("path does not exist"));
        }

        try {
            statement = "alter table t24049 (add arr1[].new2.foo string)";
            store.executeSync(statement);
            assertTrue(false);
        } catch (FaultException e) {
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            String msg = e.getMessage();
            assertTrue(msg.contains("Error: at (1, 24)") &&
                       msg.contains("path does not have a record type"));
        }

        try {
            statement = "alter table t24049 (add arr1[].notExistent.foo " +
                "string)";
            store.executeSync(statement);
            assertTrue(false);
        } catch (FaultException e) {
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            String msg = e.getMessage();
            assertTrue(msg.contains("Error: at (1, 24)") &&
                       msg.contains("path does not exist"));
        }

        // drop errors
        try {
            statement = "alter table t24049 ( drop arr2 )";
            store.executeSync(statement);
            assertTrue(false);
        } catch (FaultException e) {
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            String msg = e.getMessage();
            assertTrue(msg.contains("Error: at (1, 26)") &&
                       msg.contains("does not exist"));
        }

        try {
            statement = "alter table t24049 (drop arr1[].notExistent)";
            store.executeSync(statement);
            assertTrue(false);
        } catch (FaultException e) {
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            String msg = e.getMessage();
            assertTrue(msg.contains("Error: at (1, 25)") &&
                       msg.contains("does not exist"));
        }

        try {
            statement = "alter table t24049 (drop arr1[][].notExistent)";
            store.executeSync(statement);
            assertTrue(false);
        } catch (FaultException e) {
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            String msg = e.getMessage();
            assertTrue(msg.contains("Error: at (1, 25)") &&
                       msg.contains("does not exist"));
        }

        try {
            statement = "alter table t24049 (drop arr1[].new2.foo)";
            store.executeSync(statement);
            assertTrue(false);
        } catch (FaultException e) {
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            String msg = e.getMessage();
            assertTrue(msg.contains("Error: at (1, 25)") &&
                       msg.contains("does not exist"));
        }
    }

    /**
     * Parse the json status output and return the value of the
     * required field. Also ends up validating the json.
     */
    private String getJsonField(String output, String field) {
        final JsonNode json = JsonUtils.parseJsonNode(output);
        return JsonUtils.getAsText(json, field);
    }


    /**
     * Parse the json status output and return the value of the
     * required field. Also ends up validating the json.
     */
    private boolean jsonObjectExists(String output, String field) {
        final JsonNode json = JsonUtils.parseJsonNode(output);
        return (JsonUtils.getObject(json, field) != null);
    }
}
