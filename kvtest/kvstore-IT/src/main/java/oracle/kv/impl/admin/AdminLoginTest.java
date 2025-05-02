/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static oracle.kv.impl.security.PasswordManager.FILE_STORE_MANAGER_CLASS;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.PrintStream;
import java.util.Properties;

import javax.security.auth.Subject;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.ExecutionFuture;
import oracle.kv.KVSecurityConstants;
import oracle.kv.KVStore;
import oracle.kv.LoginCredentials;
import oracle.kv.LoginCredentials.LoginCredentialsType;
import oracle.kv.LoginCredentialsTypeFinders;
import oracle.kv.PasswordCredentials;
import oracle.kv.StatementResult;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.security.PasswordManager;
import oracle.kv.impl.security.PasswordStore;
import oracle.kv.impl.security.login.AdminLoginManager;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.DelayedALM;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.impl.util.TestPasswordReader;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;
import oracle.kv.util.DDLTestUtils;

import org.junit.Test;

public class AdminLoginTest extends TestBase {

    private CreateStore createStore;
    private KVStore store;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown()
        throws Exception {

        if (store != null) {
            store.logout();
            store.close();
            store = null;
        }

        if (createStore != null) {
            createStore.getStorageNodeAgent(0).resetRMISocketPolicies();
            createStore.shutdown();
            createStore = null;
        }
        LoggerUtils.closeAllHandlers();
        super.tearDown();
    }

    @Test
    public void testVerifyUserWithOddCredentials() {
        AdminUserVerifier auv = new AdminUserVerifier(null);
        Subject result = auv.verifyUser(new OddCredentials(), null);
        assertNull(result);
    }

    //TODO: Adding cases covering more login cases
    @Test
    public void testAdminLogin() {

        initializeStore();
        final String hostname =
            createStore.getStorageNodeAgent(0).getHostname();
        final int port = createStore.getStorageNodeAgent(0).getRegistryPort();
        LoginManager loginMgr = new DelayedALM(
            new String[] { hostname + ":" + port },
            null);


        try {
            final CommandServiceAPI cs =
                ServiceUtils.waitForAdmin(hostname, port, loginMgr, 5,
                                          ServiceStatus.RUNNING, logger);
            cs.configure(kvstoreName);

            final AdminLoginManager alm =
                new AdminLoginManager("foo", false, logger);

            /* Verify that credentials are invalid if NO user data exists */
            try {
                alm.bootstrap(hostname, port,
                              new PasswordCredentials(
                                  "foo", "NoSql00__bar".toCharArray()));
                fail("Expect AuthenticationFailureException");
            } catch (AuthenticationFailureException afe) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */

            int planId = cs.createCreateUserPlan("Add a new user", "admin",
                                                 true /* isEnabled */,
                                                 true /* isAdmin */,
                                                 "NoSql00__admin".
                                                     toCharArray());
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            cs.assertSuccess(planId);

            planId = cs.createCreateUserPlan("Add a disabled new user", "user",
                                             false /* isEnabled */,
                                             false /* isAdmin */,
                                             "NoSql00__user".toCharArray());
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            cs.assertSuccess(planId);

            /* Verify that anonymous credential is no more valid now */
            try {
                alm.bootstrap(hostname, port, null);
                fail("Expect AuthenticationFailureException");
            } catch (AuthenticationFailureException afe) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */

            /* Wrong password */
            try {
                alm.bootstrap(hostname, port,
                              new PasswordCredentials(
                                  "admin", "wrong pass".toCharArray()));
                fail("Expect AuthenticationFailureException");
            } catch (AuthenticationFailureException afe) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */

            /* Wrong user */
            try {
                alm.bootstrap(hostname, port,
                              new PasswordCredentials(
                                  "admin1", "NoSql00__admin".toCharArray()));
                fail("Expect AuthenticationFailureException");
            } catch (AuthenticationFailureException afe) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */

            /* Disabled user */
            try {
                alm.bootstrap(hostname, port,
                              new PasswordCredentials(
                                  "user", "NoSql00__user".toCharArray()));
                fail("Expect AuthenticationFailureException");
            } catch (AuthenticationFailureException afe) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */

            /* Successful login */

            final boolean loginResult =
                alm.bootstrap(hostname, port,
                              new PasswordCredentials(
                                  "admin", "NoSql00__admin".toCharArray()));
            assertTrue(loginResult);

        } catch (Exception e1) {
            fail("Unexpected exception: " + e1);
        }
    }

    @Test
    public void testPasswordRenew()
        throws Exception {

        initializeStore();
        startTestStore();
        final String hostname =
            createStore.getStorageNodeAgent(0).getHostname();
        final int port = createStore.getStorageNodeAgent(0).getRegistryPort();
        LoginManager loginMgr = new DelayedALM(
            new String[] { hostname + ":" + port }, null);

        /* Create the first admin user */
        final CommandServiceAPI cs =
            ServiceUtils.waitForAdmin(hostname, port, loginMgr, 5,
                                      ServiceStatus.RUNNING, logger);
        int planId = cs.createCreateUserPlan("Add a new user", "admin",
                                             true /* isEnabled */,
                                             true /* isAdmin */,
                                             "NoSql00__admin".
                                             toCharArray());
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        /* Successful login with admin user */
        final AdminLoginManager alm =
            new AdminLoginManager("foo", false, logger);
        boolean loginResult =
            alm.bootstrap(hostname, port,
                          new PasswordCredentials(
                              "admin", "NoSql00__admin".toCharArray()));
        assertTrue(loginResult);

        /* Create user whose password expire immediately */
        store = createStore.getSecureStore(
            new PasswordCredentials("admin",
                                    "NoSql00__admin".toCharArray()));

        final String expireUser = "expire";
        final String initPwd = "NoSql00__admin";
        String statement = "CREATE USER " + expireUser + " IDENTIFIED BY " +
            "\"" + initPwd + "\" PASSWORD EXPIRE";
        ExecutionFuture future = store.execute(statement);

        /* Wait for the operation to finish */
        StatementResult result = future.get();
        DDLTestUtils.checkSuccess(future, result);

        /* Ask for status again from the same future */
        result = future.get();
        DDLTestUtils.checkSuccess(future, result);

        try {
            alm.bootstrap(hostname, port,
                          new PasswordCredentials(
                              expireUser, initPwd.toCharArray()));
            fail("Expect AuthenticationFailureException");
        } catch (AuthenticationFailureException afe) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        try {
            alm.renewPassword(hostname, port,
                              new PasswordCredentials(
                                  expireUser, "wrong".toCharArray()),
                              "NoSql00__new".toCharArray());
            fail("Expect AuthenticationFailureException");
        } catch (AuthenticationFailureException afe) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        final String loginFile = createLoginFile(expireUser, initPwd);

        /* Renew with the same password, should fail due to password rule */
        String output = testCLIRenewError(loginFile, initPwd, initPwd);
        assertTrue(output.contains("Password must not be one of " +
                                   "the previous 3 remembered passwords"));

        /* Renew with a short password, should fail due to password rule */
        output = testCLIRenewError(loginFile, "short", "short");
        assertTrue(output.contains("Password must have at least 9 characters"));

        alm.renewPassword(hostname, port,
                          new PasswordCredentials(
                              "expire", initPwd.toCharArray()),
                          "NoSql00__new".toCharArray());
        loginResult = alm.bootstrap(
            hostname, port,
            new PasswordCredentials("expire",
                                    "NoSql00__new".toCharArray()));
        alm.logout();
        assertTrue(loginResult);
    }

    private void initializeStore() {
        try {
            createStore = new CreateStore(kvstoreName, 13230,
                                          1, /* Storage nodes */
                                          1, /* Replication factor */
                                          10, /* Partitions */
                                          1, /* Capacity */
                                          CreateStore.MB_PER_SN,
                                          false, /* useThreads */
                                          null, /* mgmtImpl */
                                          true, /* mgmtPortsShared */
                                          true  /* secure */);
            createStore.initStorageNodes();
        } catch (Exception e) {
            fail("Unexpected exception: " + e);
        }
    }

    private void startTestStore() {
        try {
            createStore.start(false);
        } catch (Exception e) {
            fail("Unexpected exception: " + e);
        }
    }

    private String createLoginFile(String user, String pwd)
        throws Exception {

        final PasswordManager pwdMgr =
            PasswordManager.load(FILE_STORE_MANAGER_CLASS);
        final File passStore = new File(TestUtils.getTestDir(), user + ".pass");
        final File loginFile = new File(TestUtils.getTestDir(), user + ".login");
        final PasswordStore pwdStore = pwdMgr.getStoreHandle(passStore);
        pwdStore.create(null);
        pwdStore.setSecret(user, pwd.toCharArray());
        pwdStore.save();
        pwdStore.discard();
        final Properties loginProps = new Properties();
        loginProps.put(KVSecurityConstants.AUTH_USERNAME_PROPERTY, user);
        loginProps.setProperty(KVSecurityConstants.AUTH_PWDFILE_PROPERTY,
                               passStore.getPath());
        createStore.addTransportProps(loginProps);
        ConfigUtils.storeProperties(loginProps, null, loginFile);
        return loginFile.getAbsolutePath();
    }

    /*
     * Run CLI command with a login file that has a user with expired password
     * to trigger password renewal, which will try with specified renewPwds and
     * return the output of the CLI.
     */
    private String testCLIRenewError(String loginFile, String... renewPwds)
        throws Exception {

        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        final PrintStream cmdOut = new PrintStream(outStream);
        final CommandShell shell = new CommandShell(System.in, cmdOut);
        final TestPasswordReader reader = new TestPasswordReader(null);
        reader.setPasswords(renewPwds);
        shell.setRenewPasswordReader(reader);
        shell.parseArgs(new String[] {
            "-host", createStore.getHostname(),
            "-port", "" + createStore.getRegistryPort(),
            "-security", loginFile,
            "show", "topology"
        });
        shell.start();
        return outStream.toString();
    }

    private static class OddCredentialsType implements LoginCredentialsType {
        private static final int INT_VALUE = 77;
        private static final OddCredentialsType TYPE =
            new OddCredentialsType();
        static {
            LoginCredentialsTypeFinders.addFinder(
                OddCredentialsType::getType);
        }
        static OddCredentialsType getType(int intValue) {
            return (intValue == INT_VALUE) ? TYPE : null;
        }
        @Override
        public int getIntValue() {
            return INT_VALUE;
        }
        @Override
        public LoginCredentials readLoginCredentials(DataInput in, short sv) {
            return new OddCredentials();
        }
    }

    private static class OddCredentials implements LoginCredentials {
        @Override
        public String getUsername() {
            return "Odd";
        }
        @Override
        public void writeFastExternal(DataOutput out, short sv) { }
        @Override
        public LoginCredentialsType getLoginCredentialsType() {
            return OddCredentialsType.TYPE;
        }
    }
}
