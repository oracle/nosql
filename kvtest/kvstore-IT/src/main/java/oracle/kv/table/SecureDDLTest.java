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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.ExecutionFuture;
import oracle.kv.KVSecurityConstants;
import oracle.kv.KVSecurityException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.ReauthenticateHandler;
import oracle.kv.StatementResult;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.plan.PlanExecutor;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.client.admin.DdlStatementExecutor;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/**
 * Test DDL within a secure store. This test case should be in its own
 * test, as mixing secure and non-secure tests within a single process
 * can create thread local related issues.
 */
public class SecureDDLTest extends DdlExecutionBase {
    private static final String adminUser = "ADMIN_USER";
    private static final String adminUserPassword = "NoSql00__1234";

    /**
     * Test execution in a secure store
     * @throws Exception
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testSecureDdl() throws Exception {
        createStoreWithAdmin();

        /* Make users with different roles, and login. */
        String ddlOkUser = "DDL_OK";
        String ddlOkUserPassword = "NoSql00__5678";
        String noDdlUser = "NO_DDL_FOR_YOU";
        String noDdlUserPassword = "NoSql00__333";
        createStore.addUser(ddlOkUser, ddlOkUserPassword, false /* admin */);
        createStore.addUser(noDdlUser, noDdlUserPassword, false /* admin */);
        createStore.start();

        createStore.grantRoles(ddlOkUser, "readwrite", "dbadmin",
                               "writesystable");
        createStore.grantRoles(noDdlUser, "readonly");

        /* One store handle can do DDL, another cant. */
        final KVStore storeDdlOk=
                loginKVStoreUser(ddlOkUser, ddlOkUserPassword);
        final KVStore storeNoDdl=
                loginKVStoreUser(noDdlUser, noDdlUserPassword);

        mrTableTearDown();
        TestBase.mrTableSetUp(storeDdlOk);
        /* Run basic DDL using the store handle that supports DDL */
        doBasicOps(storeDdlOk);

        /*
         * This user doesn't have permission for DDL. All TableAPI.execute
         * methods should fail.
         */
        new ExpectSecurityError() {
            @Override
            void doStatement() {
                storeNoDdl.execute(TestBase.addRegionsForMRTable(
                                  "create table should_fail " +
                                  "(id integer, name string, primary key(id))"));
            }
        }.exec();

        new ExpectSecurityError() {
            @Override
            void doStatement() {
                storeNoDdl.executeSync(TestBase.addRegionsForMRTable(
                                       "create table should_fail " +
                                       "(id integer, name string, " +
                                       " primary key(id))"));
            }
        }.exec();

        /*
         * Test security exceptions on the ExecutionFuture handle. Initiate an
         * execute that doesn't finish right away.  Use a stall hook to make
         * sure the statement doesn't complete immediately, so we can truly
         * test the ExecutionFuture paths.
         */
        CountDownLatch staller = new CountDownLatch(1);
        PlanExecutor.FAULT_HOOK = new StallHook(staller);
        final ExecutionFuture f = storeDdlOk.execute("drop table people");
        final byte [] savedFuture = f.toByteArray();

        /*
         * The operation should be stalling, try all the ExecutionFuture
         * methods that require server communications: isDone(),
         * updateStatus(), get(), and get(int, TimeUnit)
         */
        assertFalse(f.isDone());
        StatementResult result = f.updateStatus();
        final int planId = result.getPlanId();
        assertFalse(result.isDone());
        try {
            f.get(45, TimeUnit.SECONDS);
            fail("Should time out");
        } catch (TimeoutException expected) {
        } catch (Exception e) {
            fail("Shouldn't get " + e);
        }

        staller.countDown();
        result = f.get();
        assertTrue(result.isDone());

        /*
         * Get another ExecutionFuture through the unadvertised getFuture
         * method, as an artificial way of testing insufficient security on
         * these APIs. In general usage, there's really no way for an
         * application to get a a future instance for a plan it didn't own if
         * it doesn't have the proper credentials, because it would have to
         * call TableAPI.execute to get a future. Also, privileges can't
         * be changed for any given user while there is a plan open on it,
         * so it's not possible for a user to have privileges to call execute,
         * and then have those credentials change while she is calling the
         * methods of ExecutionFuture.
         *
         * In this artificial case, this future doesn't know that the plan is
         * done, and it will need to go to the server to ask for status. But
         * because it's been created for an unprivileged user, all invocations
         * should get security exceptions.
         */
        /* Test this deprecated API */
        TableAPI tableAPI = storeNoDdl.getTableAPI();
        final oracle.kv.table.ExecutionFuture noDDLDeprecatedFuture =
                tableAPI.getFuture(planId);
        assertFalse(noDDLDeprecatedFuture.getLastStatus().isDone());

        new ExpectSecurityError() {
            @Override
            void doStatement() {
                noDDLDeprecatedFuture.isDone();
            }
        }.exec();

        new ExpectSecurityError() {
            @Override
            void doStatement() {
                noDDLDeprecatedFuture.updateStatus();
            }
        }.exec();
        new ExpectSecurityError() {
            @Override
            void doStatement() throws Exception {
                noDDLDeprecatedFuture.get();
            }
        }.exec();
        new ExpectSecurityError() {
            @Override
            void doStatement() throws Exception {
                noDDLDeprecatedFuture.get(1, TimeUnit.MINUTES);
            }
        }.exec();

        /*
         * Test an Execution future that was obtained with a serialized
         * ExecutionFuture bytes.
         */
        new ExpectSecurityError() {
            @Override
            void doStatement() throws InterruptedException, ExecutionException {
                ExecutionFuture serF = storeNoDdl.getFuture(savedFuture);
                serF.get();
            }
        }.exec();

        PlanExecutor.FAULT_HOOK = null;
        storeDdlOk.close();
        storeNoDdl.close();
    }

    @Test
    public void testReauthenticate() throws Exception {
        createStoreWithAdmin();
        createStore.start();
        final LoginCredentials creds =
            new PasswordCredentials(adminUser, adminUserPassword.toCharArray());
        final Reauth reauth = new Reauth(creds);
        final KVStore store = loginKVStoreUser(creds, reauth);
        tearDowns.add(store::close);

        final String tableName = "people";
        final String statement = "CREATE TABLE IF NOT EXISTS " + tableName +
                                 "(id INTEGER, name STRING, PRIMARY KEY (id))";

        tearDowns.add(() -> DdlStatementExecutor.beforeExecuteHook = null);
        TestUtils.CountDownFaultHook testHook = new TestUtils
            .CountDownFaultHook(1 /* fault count */,
                                new AuthenticationRequiredException("test",
                                                                    true));
        DdlStatementExecutor.beforeExecuteHook = testHook;

        testExecuteOperation(
            store, reauth, testHook,
            () -> ((KVStoreImpl)store).execute(
                statement.toCharArray(), null, new TableLimits(1, 1, 1)));

        testExecuteOperation(
            store, reauth, testHook, () -> store.execute(statement));

        testExecuteOperation(
            store, reauth, testHook,
            () -> ((KVStoreImpl)store).setTableLimits(
                null, tableName, new TableLimits(1, 2, 3)));
    }

    private void testExecuteOperation(KVStore store,
                                      Reauth reauth,
                                      TestUtils.CountDownFaultHook testHook,
                                      Supplier<ExecutionFuture> op)
        throws Exception {

        reauth.completed = 0;
        testHook.resetCounter(1);
        LoginManager curLoginManager =
            ((KVStoreImpl)store).getDdlStatementExecutor().getLoginManager();
        ExecutionFuture future = op.get();
        checkSuccess(future, future.get());
        assertEquals(1, reauth.completed);
        LoginManager renewedLoginManager =
            ((KVStoreImpl)store).getDdlStatementExecutor().getLoginManager();
        assertNotEquals(renewedLoginManager, curLoginManager);
    }

    private void createStoreWithAdmin() throws Exception {
        createStore = new CreateStore(kvstoreName,
                                      startPort,
                                      1, /* Storage nodes */
                                      1, /* RF */
                                      2, /* Partitions */
                                      1, /* Capacity */
                                      CreateStore.MB_PER_SN, /* memory */
                                      true, /* useThreads */ // tODO, change
                                      null, /* mgmtImpl */
                                      true, /* mgmtPortsShared */
                                      true); /* secure */
        createStore.addUser(adminUser, adminUserPassword, true /* admin */);
    }

    /**
     * Run a code snippet and expect a KVSecurityException.
     */
    private abstract class ExpectSecurityError {
        void exec() {
            try {
                doStatement();
                fail("Should have security exception");
            } catch (KVSecurityException expected) {
                logger.info("Expected : " + expected);
            } catch (Exception other) {
                fail("Didn't expect " + other);
            }
        }

        abstract void doStatement() throws Exception;
    }

    private KVStore loginKVStoreUser(String userName, String password) {
        LoginCredentials creds =
            new PasswordCredentials(userName, password.toCharArray());
        return loginKVStoreUser(creds, null);
    }

    /**
     * Log this user into a secured store.
     */
    private KVStore loginKVStoreUser(LoginCredentials creds,
                                     ReauthenticateHandler reauthHandler) {
        KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              createStore.getHostname() + ":" +
                              createStore.getRegistryPort());
        kvConfig.setCheckInterval(1, TimeUnit.SECONDS);
        Properties props = new Properties();
        props.put(KVSecurityConstants.TRANSPORT_PROPERTY,
                  KVSecurityConstants.SSL_TRANSPORT_NAME);
        props.put(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                  createStore.getTrustStore().getPath());
        kvConfig.setSecurityProperties(props);

        return KVStoreFactory.getStore(kvConfig, creds, reauthHandler);
    }

    private class Reauth implements ReauthenticateHandler {
        private volatile int completed ;
        private final LoginCredentials creds;

        Reauth(LoginCredentials creds) {
            this.creds = creds;
        }

        @Override
        public void reauthenticate (KVStore store) {
            store.login(creds);
            completed++;
        }
    }
}
