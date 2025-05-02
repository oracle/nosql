/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.rmi.RemoteException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import oracle.kv.ExecutionFuture;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.StatementResult;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.AdminService;
import oracle.kv.impl.admin.AdminServiceFaultHandler;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.plan.PlanExecutor;
import oracle.kv.impl.client.admin.ClientAdminServiceAPI;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.test.ExceptionTestHook;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/**
 * Exercise error handling of Table DDL API execution engine.
 */
public class DdlExecutionErrorsTest extends DdlExecutionBase {

    class InduceFailure<T> implements TestHook<T> {
        final RuntimeException t;

        InduceFailure(RuntimeException t) {
            this.t = t;
        }

        @Override
        public void doHook(T arg){
            throw t;
        }
    }

    class InduceCheckedException
        implements ExceptionTestHook<String, RemoteException> {
        final String errorMsg;

        InduceCheckedException(String errorMsg) {
            this.errorMsg = errorMsg;
        }

        @Override
        public void doHook(String arg) throws RemoteException {
            throw new RemoteException(errorMsg);
        }
    }

    class InduceAdminFailure implements TestHook<Admin> {
        final RuntimeException t;

        InduceAdminFailure(RuntimeException t) {
            this.t = t;
        }

        @Override
        public void doHook(Admin arg) {
            AdminService owner = arg.getOwner();
            if (owner != null) {
                ((AdminServiceFaultHandler) owner.getFaultHandler())
                .setSuppressPrinting();
            }
            throw t;
        }
    }

    @Override
    public void tearDown() throws Exception {
        /* clean any test hook uncleared by failed tests */
        ClientAdminServiceAPI.REMOTE_FAULT_HOOK = null;
        super.tearDown();
        mrTableTearDown();
    }

    /**
     * Test basic error paths, mostly of the synchronous,
     * IllegalArgumentException variety
     * @throws Exception if test fails
     */
    @Test
    public void testBasicErrors() throws Exception {

        KVStore store = createMinimalStore();

        /* Try to create a table, but induce a syntax error */
        String statement = "CREATE TABLE users (id INTEGER, " +
            "name STRING, PRIMARY KEY (id))";
        statement = TestBase.addRegionsForMRTable(statement);
        try {
            store.execute("x" + statement);
            fail("Should have thrown exception");
        } catch (IllegalArgumentException expected) {
            logger.info("Expected error:" + expected);
        } catch (Throwable t) {
            fail("Wrong exception, didn't expect " + t);
        }

        /* Make sure the synchronous execute fails too. */
        try {
            store.executeSync("x" + statement);
            fail("Should have thrown exception");
        } catch (IllegalArgumentException expected ) {
            logger.info("Expected error:" + expected);
        } catch (Throwable t) {
            fail("Wrong exception, didn't expect " + t);
        }

        /* Two synchronous creates should be fine. */
        store.executeSync(statement);
        store.executeSync(TestBase.addRegionsForMRTable("CREATE TABLE IF " +
                          "NOT EXISTS users (id INTEGER, name STRING, " +
                          "PRIMARY KEY (id))"));

        /*
         * But we shouldn't be able to create a table w/different columns and
         * the same name.
         */
        try {
            store.executeSync(TestBase.addRegionsForMRTable("CREATE TABLE " +
                "IF NOT EXISTS users (id INTEGER, PRIMARY KEY(id))"));
            fail("Should have thrown exception");
        } catch (IllegalArgumentException expected) {
            logger.info("Expected error:" + expected);
        } catch (Throwable t) {
            fail("Wrong exception, didn't expect " + t);
        }

        /* We shouldn't be able to create a future with a bad byte array */
        byte[] bad = new byte[5];
        try {
            store.getFuture(bad);
            fail("Should throw illegal arg exception");
        } catch (IllegalArgumentException expected) {
        }
    }

    /**
     * Test exceptions that come from the server or RMI
     */
    @Test
    public void testServerSideErrors() throws Exception {
        KVStore store = createMinimalStore();

        /*
         * Exceptions that occur from the planner during ExecutionFuture.get()
         * should get ExecutionException.
         */
        String errMessage = "a fake plan execution problem";
        PlanExecutor.FAULT_HOOK =
            new InduceFailure<>(new FaultException(errMessage, true));

        String statement = "create table foo (id integer, " +
            " value string, primary key(id))";
        statement = TestBase.addRegionsForMRTable(statement);

        ExecutionFuture ef = store.execute(statement);
        StatementResult result;
        try {
            ef.get();
            fail("Should throw execution exception");
        } catch (ExecutionException expected) {
            assertTrue(ef.isDone());
            assertFalse(ef.isCancelled());

            result = ef.getLastStatus();
            assertTrue(result.isDone());
            assertFalse(result.isSuccessful());
            assertFalse(result.isCancelled());
            assertEquals(statement, ef.getStatement());
            logger.info("error result = " + result);
            assertTrue(result.getErrorMessage().contains(errMessage));
            assertTrue(expected.getCause().getMessage().contains(errMessage));
        } catch (Exception other) {
            fail("Shouldn't be this kind of exception " + other);
        }
        PlanExecutor.FAULT_HOOK = null;

        /*
         * AdminFault exceptions not from the plan execution, but
         * from the infrastructure outside the plan
         */
        errMessage = "fake server side, non-plan exception";
        statement = "create table if not exists bar (id integer, " +
            " value string, primary key(id))";
        statement = TestBase.addRegionsForMRTable(statement);

        Admin.EXECUTE_HOOK =
            new InduceAdminFailure(new FaultException(errMessage, true));
        /* First induce error from execute */
        try {
            store.execute(statement);
            fail("Should fail");
        } catch (FaultException expected) {
            assertTrue(expected.getMessage().contains(errMessage));
        } catch (Exception other) {
            fail("Should have gotten FaultException, got " + other);
        }
        Admin.EXECUTE_HOOK = null;

        /* Next induce admin error from getting status */
        ef = store.execute(statement);
        Admin.EXECUTE_HOOK =
            new InduceAdminFailure(new FaultException(errMessage, true));
        try {
            ef.updateStatus();
            fail("Should fail");
        } catch (FaultException expected) {
            assertFalse(ef.isCancelled());
            result = ef.getLastStatus();
            assertFalse(result.isSuccessful());
            assertFalse(result.isCancelled());
            assertEquals(statement, ef.getStatement());
            logger.info("error result = " + result);
            assertTrue(expected.getMessage().contains(errMessage));
        } catch (Exception other) {
            fail("Shouldn't be this kind of exception " + other);
        }

        /* Now try polling */
        try {
            ef.get();
            fail("Should fail");
        } catch (ExecutionException expected) {
            assertFalse(ef.isCancelled());
            result = ef.getLastStatus();
            assertFalse(result.isSuccessful());
            assertFalse(result.isCancelled());
            assertEquals(statement, ef.getStatement());
            logger.info("error result = " + result);
            assertNotNull(result.getErrorMessage());
            assertTrue(expected.getCause().getMessage().
                       contains(errMessage));
        } catch (Exception other) {
            fail("Shouldn't be this kind of exception " + other);
        }
        Admin.EXECUTE_HOOK = null;

        /*
         * Try calling get() again on the same future, with hooks cleared.
         * the Future will still throw, because once get() has thrown
         * execution exception, all further calls will continue to throw.
         */
        logger.info("------- all hooks cleared, wait for completion ");
        try {
            ef.get();
            /*
             * Since the last get failed, any get on this future will no
             * longer succeed. Need to re-execute.
             */
            fail ("Should have failed");
        } catch (ExecutionException expected) {
            result = ef.getLastStatus();
            logger.info("last status = " + result);
            assertFalse(result.toString(), ef.isCancelled());
            assertFalse(result.isSuccessful());
            assertFalse(result.isCancelled());
            assertEquals(statement, ef.getStatement());
            assertTrue(result.getErrorMessage().contains(errMessage));
            assertTrue(expected.getCause().getMessage().
                       contains(errMessage));
        }

        /*
         * a retry should be successful. BOZO: disable this test because
         * the multi-handle create is not working yet.
         * See SR [#23919]
         */

        /*try {
          ef = store.execute(statement);
          result = ef.get();
          assertTrue(result.isSuccessful());
          assertTrue(result.isDone());
          assertFalse(result.isCancelled());
          assertEquals(statement, ef.getStatement());
          assertTrue(result.getErrorMessage() == null);
          assertTrue(ef.isDone());
          assertFalse(ef.isCancelled());
          } catch (Exception unexpected) {
          logger.info("Failed with : + unexpected");
          StatementResult describeResult =
          store.executeSync("Describe as json table bar");
          fail(unexpected.toString() + "\n" + describeResult);
          }*/
    }

    /**
     * Test RemoteExceptions.
     */
    @Test
    public void testRemoteExceptions() throws Exception {

        createStore = new CreateStore(kvstoreName,
                                      startPort, numSns, rf,
                                      2 /* numPartitions */,
                                      1 /* capacity */,
                                      CreateStore.MB_PER_SN,
                                      true /* useThreads */,
                                      null /* mgmtImpl */);
        createStore.start();
        KVStoreConfig config = createStore.createKVConfig();
        config.setCheckInterval(1, TimeUnit.SECONDS);
        KVStore store = KVStoreFactory.getStore(config);
        TestBase.mrTableSetUp(store);

        /* Create problems with RMI exceptions */
        String errMessage = "fake RMI exception ";
        String statement = "create table if not exists baz (id integer, " +
            " name string, primary key (id))";
        statement = TestBase.addRegionsForMRTable(statement);

        ClientAdminServiceAPI.REMOTE_FAULT_HOOK =
            new InduceCheckedException(errMessage);
        ExecutionFuture ef;
        try {
            store.execute(statement);
            fail("Should fail");
        } catch (FaultException expected) {
            assertTrue(expected.getMessage().contains(errMessage));
        } catch (Exception other) {
            fail("Shouldn't be this kind of exception " + other);
        } finally {
            ClientAdminServiceAPI.REMOTE_FAULT_HOOK = null;
        }

        ef = store.execute(statement);

        /* First try should time out */
        try {
            ClientAdminServiceAPI.REMOTE_FAULT_HOOK =
                new InduceCheckedException(errMessage);
            /*
             * init delay of check is 10 ms defined in
             * KVStoreConfig#DEFAULT_CHECK_INIT_DELAY_MILLIS, we need a smaller
             * timeout to produce timeout exception
             */
            ef.get(1, TimeUnit.MILLISECONDS);
            fail("Should throw");
        } catch (TimeoutException expected) {
            /*
             * Don't check ef.isDone(), that will cause another server side
             * communication.
             */
            assertFalse(ef.isCancelled());
            assertEquals(statement, ef.getStatement());
        } catch (Exception other) {
            fail("Shouldn't be this kind of exception " + other);
        } finally {
            ClientAdminServiceAPI.REMOTE_FAULT_HOOK = null;
        }

        /* Second try should get an execution exception */
        try {
            ClientAdminServiceAPI.REMOTE_FAULT_HOOK = new
                InduceCheckedException(errMessage);
            ef.get(20, TimeUnit.SECONDS);
            fail("Should throw");
        } catch (ExecutionException expected) {
            assertFalse(ef.isCancelled());
            assertEquals(statement, ef.getStatement());
            assertTrue(expected.getCause().getMessage().contains(errMessage));
        } catch (Exception other) {
            fail("Shouldn't be this kind of exception " + other);
        } finally {
            ClientAdminServiceAPI.REMOTE_FAULT_HOOK = null;
        }

        /*
         * These exceptions have all been manufactured network errors. Try
         * again, the original should have succeeded at the server.
         */
        ef = store.execute(statement);
        StatementResult result = ef.get(10, TimeUnit.SECONDS);
        assertTrue(ef.isDone());
        assertFalse(ef.isCancelled());
        assertEquals(statement, ef.getStatement());
        assertTrue(result.isSuccessful());
    }

    /**
     * Test FaultException type error paths on the initial execute
     * @throws Exception if test fails
     */
    @Test
    public void testFaultExceptionOnExec() throws Exception {

        KVStore store = createMinimalStore();

        /*
         * Create a table, but make sure there's a conflicting plan lock
         * before it starts executing.
         */
        String statement = "CREATE TABLE users (id INTEGER, " +
            "name STRING, PRIMARY KEY (id))";
        statement = TestBase.addRegionsForMRTable(statement);
        LockOutDDL lockout = new LockOutDDL(1, "users");
        oracle.kv.impl.admin.Admin.EXECUTE_HOOK = lockout;
        try {
            store.execute(statement);
            fail("Should have thrown exception");
        } catch (FaultException expected) {
            logger.info("Expected error:" + expected);
        } catch (Throwable t) {
            fail("Wrong exception, didn't expect " + t);
        } finally {
            assert lockout.clearLocks();
        }

        /* Now create the table, locks should be cleared. */
        oracle.kv.impl.admin.Admin.EXECUTE_HOOK = null;
        StatementResult result = store.executeSync(statement);
        assertTrue(result.isSuccessful());
    }

    /** Test Admin failover */
    @Test
    public void testAdminFailover()
        throws Exception {

        /* 3 SNs, Admin services on all */
        createStore = new CreateStore(kvstoreName, 17374, // startPort
                                      3,  rf, 10, 1, CreateStore.MB_PER_SN,
                                      true /* useThreads */, null);
        createStore.setAdminLocations(1, 2, 3);
        createStore.start();
        KVStore store = KVStoreFactory.getStore(createStore.createKVConfig());
        TestBase.mrTableSetUp(store);

        /* Create a table */
        String statement = "CREATE TABLE people (id INTEGER, " +
            "name STRING, PRIMARY KEY (id))";
        statement = TestBase.addRegionsForMRTable(statement);
        StatementResult result = store.executeSync(statement);
        checkSuccess(null, result);

        /*
         * Cause a failover by killing Admin1, wait for a new Admin to take
         * over.
         */
        CommandServiceAPI cs = createStore.getAdmin();
        RegistryUtils ru = new RegistryUtils(
            cs.getTopology(),
            createStore.getSNALoginManager(new StorageNodeId(1)), logger);
        StorageNodeAgentAPI snai = ru.getStorageNodeAgent(new StorageNodeId(1));
        assertNotNull(snai);
        assertTrue(snai.stopAdmin(false));
        CommandServiceAPI newCS = createStore.getAdminMaster();
        assertNotEquals(cs, newCS);

        /* Execute another statement  (it will actually run on another admin) */
        result = store.executeSync("drop table people");
        checkSuccess(null, result);

        store.close();
    }
}
