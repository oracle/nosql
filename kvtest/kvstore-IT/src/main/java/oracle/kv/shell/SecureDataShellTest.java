/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.shell;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import oracle.kv.KVSecurityConstants;
import oracle.kv.KVSecurityException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.TestBase;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.query.shell.OnqlShell;
import oracle.kv.impl.security.PasswordManager;
import oracle.kv.impl.security.PasswordStore;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.util.CreateStore;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;

/**
 * Test DDL within a secure store. This test case should be in its own
 * test, as mixing secure and non-secure tests within a single process
 * can create thread local related issues.
 */
public class SecureDataShellTest extends TestBase {

    static CreateStore createStore;

    /* One store handle can do DDL, another can't. */
    static KVStore storeWithAccess;
    static KVStore storeNoAccess;
    static TableAPI tableImplWithAccess;
    static TableAPI tableImplNoAccess;

    /*
     * Several shells, with differing identities
     */
    static CommandShell shellWithAccess;    /* will have read-write priv */
    static CommandShell shellNoAccess;      /* will have no priv */
    static OnqlShell sqlShellWithAccess;    /* will have read-write priv */
    static OnqlShell sqlShellNoAccess;      /* will have no priv */

    /*
     * A shared, local PrintStream
     */
    static ByteArrayOutputStream baos = new ByteArrayOutputStream();
    static PrintStream printStream = new PrintStream(baos);


    static File jsonFile = new File(TestUtils.getTestDir(), "Import.json");

    private static final int startPort = 13240;

    /* adminUser is the first user with -admin enabled, which is required */
    final static String adminUser= "ADMIN_USER";
    final static String adminUserPassword = "ABcd@#1234";

    /* userWithAccess will have dbadmin, and read-write privilege */
    final static String userWithAccess = "ACCESS_OK";
    final static String userWithAccessPassword = "ABcd@#1234";

    /* userNoAccess has read-only user access */
    final static String userNoAccess = "NO_ACCESS_FOR_YOU";
    final static String userNoAccessPassword = "ABcd@#1234";
    final static Logger staticLogger =
        LoggerUtils.getLogger(SecureDataShellTest.class,"--DataShell--");

    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        TestUtils.clearTestDirectory();
        TestStatus.setManyRNs(true);
        startStore();
        createJsonImportFile();
    }

    @AfterClass
    public static void staticTearDown()
        throws Exception {

        if (storeWithAccess != null) {
            storeWithAccess.close();
        }
        if (storeNoAccess != null) {
            storeNoAccess.close();
        }

        if (createStore != null) {
            createStore.shutdown();
        }
        LoggerUtils.closeAllHandlers();
    }

    @Override
    @Before
    public void setUp() {
    }

    @Override
    @After
    public void tearDown() {
    }

    private static void startStore()
        throws Exception {

        /*
         * Make a local 1x1, optionally secure
         */
        createStore = new CreateStore("kvtest-" +
                                      SecureDataShellTest.class.getName() +
                                      "-store",
                                      startPort,
                                      1, /* n SNs */
                                      1, /* rf */
                                      2, /* n partitions */
                                      1, /* capacity per SN */
                                      CreateStore.MB_PER_SN,
                                      false, /* use threads is false */
                                      null,  /* mgmtImpl */
                                      true, /* mgmtPortsShared */
                                      true); /* secure store */

        /* first user must have admin enabled */
        createStore.addUser(adminUser, adminUserPassword, true /* admin */);

        /* add 2 users and give one of the readwrite access */
        createStore.addUser(userWithAccess, userWithAccessPassword,
                            false /* admin */);
        createStore.addUser(userNoAccess, userNoAccessPassword,
                            false /* admin */);

        createStore.start();

        /* access */
        createStore.grantRoles(userWithAccess, "readwrite", "dbadmin");

        /*
         * Give no access to this user at all. Reads and writes will fail.
         *
         * createStore.grantRoles(userNoAccess, "readonly");
         */

        /* One store handle can do DDL, another cant. */
        storeWithAccess = loginKVStoreUser(userWithAccess,
                                           userWithAccessPassword);
        storeNoAccess = loginKVStoreUser(userNoAccess,
                                         userNoAccessPassword);
        tableImplWithAccess = storeWithAccess.getTableAPI();
        tableImplNoAccess = storeNoAccess.getTableAPI();

        sqlShellWithAccess = new OnqlShell(System.in, printStream);
        sqlShellNoAccess = new OnqlShell(System.in, printStream);
        shellWithAccess = new CommandShell(System.in, printStream);
        shellNoAccess = new CommandShell(System.in, printStream);

        /*
         * Connect all shells
         */
        connectShell(sqlShellWithAccess, userWithAccess,
                     userWithAccessPassword);
        connectShell(sqlShellNoAccess, userNoAccess,
                     userNoAccessPassword);
        connectShell(shellWithAccess, userWithAccess,
                     userWithAccessPassword);
        connectShell(shellNoAccess, userNoAccess,
                     userNoAccessPassword);


        /* Create a simple table and add some rows */
        storeWithAccess.executeSync("create table users " +
                           "(id integer, name string, primary key(id))");
        Table users = tableImplWithAccess.getTable("users");
        for (int i = 0; i < 100; i++) {
            final Row row = users.createRow();
            row.put("id", i);
            row.put("name", ("name" + i));
            tableImplWithAccess.put(row, null, null);

            /* verify that the no-ddl handle cannot write */
            new ExpectSecurityError() {
                @Override
                void doStatement() {
                    tableImplNoAccess.put(row, null, null);
                }
            }.exec(UnauthorizedException.class);
        }

        /* verify that the no-ddl handle cannot create tables */
        new ExpectSecurityError() {
            @Override
            void doStatement() {
                storeNoAccess.executeSync(
                    "create table users1 " +
                    "(id integer, name string, primary key(id))");
            }
        }.exec(UnauthorizedException.class);

        /*
         * Try a DDL operation using the no-access user to
         * be sure they fail properly
         */
        try {
            storeNoAccess.executeSync("create table users1 " +
                                  "(id integer, name string, primary key(id))");
            fail("Table creation should have failed");
        } catch (KVSecurityException kvse) {}

        /* try using the no access shell -- this should fail */
        final String[] commandArgs = new String[]
            {"execute", "create table users1 (id integer, name string, "+
             "primary key(id))"};
        runCommand(shellNoAccess, "execute", commandArgs, true);
    }

    @Test
    public void testQuery() {
        try {
            sqlShellWithAccess.runLine("select * from users");
        } catch (ShellException se) {
            fail("Query failed: " + se.getMessage());
        }

        try {
            sqlShellNoAccess.runLine("select * from users");
            fail("Query should have thrown");
        } catch (ShellException se) {
            assertTrue(se.getCause() instanceof UnauthorizedException);
        }
    }

    @Test
    public void testGet() {
        final String jsonRow = "{\"id\": 1}";
        final String[] commandArgs = new String[]
            {"get", "table", "-name", "users", "-json", jsonRow};
        runCommand(shellWithAccess, "put", commandArgs, false);
        runCommand(shellNoAccess, "put", commandArgs, true);
    }

    @Test
    public void testPut() {
        final String jsonRow = "{\"id\": 1, \"name\":\"joe\"}";
        final String[] commandArgs = new String[]
            {"put", "table", "-name", "users", "-json", jsonRow};
        runCommand(shellWithAccess, "put", commandArgs, false);
        runCommand(shellNoAccess, "put", commandArgs, true);
    }

    @Test
    public void testDelete() {
        final String jsonRow = "{\"id\": 1}";
        final String[] commandArgs = new String[]
            {"delete", "table", "-name", "users", "-json", jsonRow};
        runCommand(shellWithAccess, "delete", commandArgs, false);
        runCommand(shellNoAccess, "delete", commandArgs, true);
    }

    @Test
    public void testDeleteMultiple() {
        final String[] commandArgs = new String[]
            {"delete", "table", "-name", "users", "-field", "id",
             "-start", "1", "-end", "10"};
        runCommand(shellNoAccess, "delete", commandArgs, true);
        runCommand(shellWithAccess, "delete", commandArgs, false);
    }

    @Test
    public void testImport() {
        final String[] commandArgs = new String[]
            {"import", "-table", "users", "-file",
             jsonFile.getAbsolutePath(), "JSON"};
        runCommand(sqlShellNoAccess, "import", commandArgs, true);
        runCommand(sqlShellWithAccess, "import", commandArgs, false);
    }


    private static String runCommand(Shell shell, String command, String[] args,
                                     boolean expectException) {
        try {
            String ret = shell.run(command, args);
            if (expectException) {
                fail("Expected an exception from command: " + command);
            }
            return ret;
        } catch (Exception se) {
            if (!expectException) {
                fail("Did not expect an exception from command: " + command +
                     ", got exception: " + se);
            }
        }
        return null;
    }

    /**
     * Run a code snippet and expect a KVSecurityException.
     */
    private static abstract class ExpectSecurityError {
        void exec(final Class<? extends KVSecurityException> expected) {
            try {
                doStatement();
                fail("Should have security exception");
            } catch (KVSecurityException kvse) {
                staticLogger.info("Expected : " + expected);
                if (!kvse.getClass().equals(expected)) {
                    String msg = "Expected: " + expected + ", got " + kvse;
                    fail(msg);
                }
            } catch (Exception other) {
                fail("Didn't expect exception " + other);
            }
        }

        abstract void doStatement() throws Exception;
    }

    /**
     * Log this user into a secured store.
     */
    private static KVStore loginKVStoreUser(String userName, String password) {
        LoginCredentials creds =
                new PasswordCredentials(userName, password.toCharArray());
        KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              createStore.getHostname() + ":" +
                              createStore.getRegistryPort());
        kvConfig.setCheckInterval(1, TimeUnit.SECONDS);
        Properties props = createSecurityProperties();
        kvConfig.setSecurityProperties(props);

        return KVStoreFactory.getStore(kvConfig, creds, null);
    }

    /**
     * Create security properties includes:
     *  oracle.kv.transport=ssl
     *  oracle.kv.ssl.trustStore=<path-to-trust-store>
     */
    private static Properties createSecurityProperties() {
        Properties props = new Properties();
        props.put(KVSecurityConstants.TRANSPORT_PROPERTY,
                  KVSecurityConstants.SSL_TRANSPORT_NAME);
        props.put(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                  createStore.getTrustStore().getPath());
        return props;
    }

    /**
     * Create security properties includes:
     *  oracle.kv.transport=ssl
     *  oracle.kv.ssl.trustStore=<path-to-trust-store>
     *  oracle.kv.auth.pwdfile.file=<path-to-pwd-file>
     */
    private static Properties createSecurityProperties(String user,
                                                       String password){

        Properties props = createSecurityProperties();
        File pwdFile = createPasswordStore(user, password);

        props.put(KVSecurityConstants.AUTH_PWDFILE_PROPERTY, pwdFile.getPath());
        return props;
    }

    /**
     * Creates a password store for auto login of client tests. The filestore
     * is used currently. The path of password store is <test-dir>/<user>.pwd.
     */
    private static File createPasswordStore(String user, String password) {

        try {
            final File pwdFile = new File(TestUtils.getTestDir(), user + ".pwd");
            if (pwdFile.exists()) {
                return pwdFile;
            }
            final PasswordManager pwdMgr = PasswordManager.load(
                "oracle.kv.impl.security.filestore.FileStoreManager");

            final PasswordStore pwdfile = pwdMgr.getStoreHandle(pwdFile);
            pwdfile.create(null /* auto login */);
            pwdfile.setSecret(user, password.toCharArray());
            pwdfile.save();

            assertTrue(pwdFile.exists());
            return pwdFile;
        } catch (Exception e) {
            fail("Can't find password or access manager class: " + e);
        }
        return null;
    }


    /**
     * Creates a security file for login to admin, the path of password store
     * is <test-dir>/<user>.sec.
     */
    private static String createSecurityFile(String user, String password)
        throws IOException {

        File secFile = new File(TestUtils.getTestDir(), user + ".sec");
        FileOutputStream fos = new FileOutputStream(secFile);

        Properties props = createSecurityProperties(user, password);
        props.store(fos, null);
        fos.flush();
        fos.close();

        assertTrue(secFile.exists());
        return secFile.getPath();
    }

    /**
     * Creates a small file to test the SQL shell import command
     */
    private static void createJsonImportFile()
        throws IOException {

        PrintWriter writer = new PrintWriter(jsonFile);
        writer.println("{\"id\":21, \"name\":\"joe\"}");
        writer.println("{\"id\":22, \"name\":\"joe\"}");
        writer.println("{\"id\":23, \"name\":\"joe\"}");
        writer.close();
        assertTrue(jsonFile.exists());
    }

    /**
     * Utility method to connect a specific Shell instance to a store using
     * the specified user identity. The user must have already been created.
     */
    private static void connectShell(Shell shell, String user, String pass) {

        try {
            ShellCommand connect = shell.findCommand("connect");
            boolean isCommandShell = shell instanceof CommandShell;
            ArrayList<String> args = new ArrayList<String>();
            args.add("connect");
            if (isCommandShell) {
                args.add("store");
            }
            args.add("-host");
            args.add(createStore.getHostname());
            args.add("-port");
            args.add(Integer.toString(createStore.getRegistryPort(0)));
            args.add("-name");
            args.add(createStore.getStoreName());
            args.add("-username");
            args.add(user);
            args.add("-security");
            args.add(createSecurityFile(user, pass));

            String res =
                connect.execute(args.toArray(new String[args.size()]), shell);
            assertTrue(res.contains("onnected"));
        } catch (Exception e) {
            fail("Exception connecting shell: " + e);
        }
    }

    /*
     * For debugging shell output
     */
    @SuppressWarnings("unused")
    private static String flushOutput() {
        String ret = baos.toString();
        printStream.flush();
        baos.reset();
        return ret;
    }
}
