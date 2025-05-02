/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.util;

import static oracle.kv.KVSecurityConstants.AUTH_PWDFILE_PROPERTY;
import static oracle.kv.KVSecurityConstants.AUTH_USERNAME_PROPERTY;
import static oracle.kv.KVSecurityConstants.AUTH_WALLET_PROPERTY;
import static oracle.kv.KVSecurityConstants.CMD_PASSWORD_NOPROMPT_PROPERTY;
import static oracle.kv.KVSecurityConstants.SECURITY_FILE_PROPERTY;
import static oracle.kv.KVSecurityConstants.TRANSPORT_PROPERTY;
import static oracle.kv.impl.security.ssl.SSLConfig.KEYSTORE_FILE;
import static oracle.kv.impl.security.ssl.SSLConfig.TRUSTSTORE_FILE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Properties;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.KVSecurityConstants;
import oracle.kv.PasswordCredentials;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.security.PasswordManager;
import oracle.kv.impl.security.PasswordStore;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.util.SSLTestUtils;
import oracle.kv.impl.util.StorageNodeUtils.SecureOpts;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.util.CreateStore;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.Ping;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellInputReader;

import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class KVStoreLoginTest extends TestBase {
    private static final String TESTUSER = "testuser";
    private static final String CTS_PWD_ALIAS = "clienttrust";
    private static final char[] TESTPASS = "NoSql00__testpass".toCharArray();
    private static CreateStore createStore;
    private static boolean noSSL = false;
    private static String loginFile;
    private static String pwdFile;

    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        noSSL = TestUtils.isSSLDisabled();
        TestUtils.clearTestDirectory();
        createStore = new CreateStore(
            testClassName, 13230,
            1, /* Storage nodes */
            1, /* Replication factor */
            10, /* Partitions */
            1, /* Capacity */
            CreateStore.MB_PER_SN,
            true, /* useThreads */
            null, /* mgmtImpl */
            true, /* mgmtPortsShared */
            true  /* secure */);
        createStore.addUser(TESTUSER, new String(TESTPASS), true /* admin */);

        if (noSSL) {
            SecureOpts secureOpts =
                new SecureOpts().setSecure(true).setNoSSL(true);
            createStore.setSecureOpts(secureOpts);
        }
        createStore.start();

        File subDir = createSubTestDir();
        loginFile = new File(subDir, "loginFile").getAbsolutePath();
        pwdFile = new File(subDir, "pwdfile").getAbsolutePath();
        createPwdFileStore(pwdFile);
    }

    @AfterClass
    public static void staticTearDown() {
        if (createStore != null) {
            createStore.shutdown(true /* force */);
        }
        TestUtils.clearTestDirectory();
    }

    @Override
    public void setUp() {
        cleanup(loginFile);
    }

    @Test(expected = IllegalStateException.class)
    public void testInitializeWithBadLoginFile() {
        /* Test non-exist login file, same test run for SSL and non-SSL cases */
        final KVStoreLogin login = new KVStoreLogin(null, "non-exist-file");
        login.loadSecurityProperties();
    }

    @Test
    public void testInitializeWithNullLoginInfo() {
        /* Test empty login file, same test run for SSL and non-SSL cases */
        final KVStoreLogin login = new KVStoreLogin();
        login.loadSecurityProperties();
        assertNull(login.getUserName());
        assertNull(login.getSecurityFilePath());
    }

    @Test
    public void testUpdateLoginInto() throws FileNotFoundException {
        /*
         * Test updating content of login file, same test run for SSL and
         * non-SSL cases
         */
        final KVStoreLogin login = new KVStoreLogin("foo", "bar");
        assertEquals(login.getUserName(), "foo");
        assertEquals(login.getSecurityFilePath(), "bar");

        /* Test update to both null user and security file */
        login.updateLoginInfo(null, null);
        assertNull(login.getUserName());
        assertNull(login.getSecurityFilePath());

        /* Test update to not-null user and null security file */
        login.updateLoginInfo(TESTUSER, null);
        assertNull(login.getSecurityFilePath());
        assertEquals(login.getUserName(), TESTUSER);

        /* Test update to null user and non-exist security file */
        try {
            login.updateLoginInfo(null, "non-exist-file");
            fail("Expected IllegalStateException");
        } catch (IllegalStateException ise) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        final FileOutputStream fos = new FileOutputStream(loginFile);
        final PrintStream ps = new PrintStream(fos);
        ps.println(AUTH_USERNAME_PROPERTY + "=" + TESTUSER);
        ps.println(TRANSPORT_PROPERTY + "=" + "ssl");
        ps.close();

        /*
         * Username specified in login info should override that in security
         * file when updating.
         */
        login.updateLoginInfo("new_user", loginFile);
        assertEquals(login.getUserName(), "new_user");
        assertEquals(login.getSecurityFilePath(), loginFile);
    }

    @Test
    public void testGetShellLoginCredentials() throws Exception {
        /*
         * Test reading shell login credentials, same test run for SSL and
         * non-SSL cases
         */

        /* Verify that null login info will ask for creds from shell */
        final KVStoreLogin login = new KVStoreLogin();
        login.setReader(new MockedShellInputReader());
        PasswordCredentials pwdCreds =
            (PasswordCredentials)login.makeShellLoginCredentials();
        assertEquals(MockedShellInputReader.SHELL_USER,
                     pwdCreds.getUsername());
        assertArrayEquals(MockedShellInputReader.SHELL_PASS,
                          pwdCreds.getPassword());

        /* Verify that no shell input is needed if valid pwd creds is found */
        final FileOutputStream fos = new FileOutputStream(loginFile);
        final PrintStream ps = new PrintStream(fos);
        ps.println(AUTH_USERNAME_PROPERTY + "=" + TESTUSER);
        ps.println(TRANSPORT_PROPERTY + "=" + "ssl");
        ps.println(AUTH_PWDFILE_PROPERTY + "=" + pwdFile);
        ps.close();

        login.updateLoginInfo(TESTUSER, loginFile);
        pwdCreds = (PasswordCredentials)login.makeShellLoginCredentials();
        assertEquals(TESTUSER, pwdCreds.getUsername());
        assertArrayEquals(TESTPASS, pwdCreds.getPassword());
    }

    @Test
    public void testGetShellLoginCredentialsWithExclamationMark()
        throws Exception {
        /*
         * Test reading shell login credentials, same test run for SSL and
         * non-SSL cases
         */

        /* Verify that null login info will ask for creds from shell */
        final KVStoreLogin login = new KVStoreLogin();
        final String user = "test";
        final String password = "ADmin!@@1234";
        final byte[] buf = (user + Shell.eol + password + Shell.eol).getBytes();
        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        final ShellInputReader reader =
            new MockedShellInputReader(new ByteArrayInputStream(buf),
                                       new PrintStream(outStream));
        login.setReader(reader);
        PasswordCredentials pwdCreds =
            (PasswordCredentials)login.makeShellLoginCredentials();

        assertTrue(outStream.toString().contains(user));
        assertEquals(user, pwdCreds.getUsername());
        assertArrayEquals(password.toCharArray(), pwdCreds.getPassword());
    }

    @Test
    public void testMakeLoginCredentials() throws Exception {
        /*
         * Test reading login credentials from password storage, same test run
         * for SSL and non-SSL cases
         */

        /* Verify return null with null security props */
        assertNull(KVStoreLogin.makeLoginCredentials(null));

        final Properties secProps = new Properties();

        /* Verify pwd creds can be obtained successfully from valid store */
        secProps.put(AUTH_USERNAME_PROPERTY, TESTUSER);
        secProps.put(AUTH_PWDFILE_PROPERTY, pwdFile);
        final PasswordCredentials pwdCreds =
            (PasswordCredentials)KVStoreLogin.makeLoginCredentials(secProps);
        assertEquals(pwdCreds.getUsername(), TESTUSER);
        assertArrayEquals(pwdCreds.getPassword(), TESTPASS);

        /* Verify return null with security props without username */
        secProps.remove(AUTH_USERNAME_PROPERTY);
        assertFalse(secProps.containsKey(AUTH_USERNAME_PROPERTY));
        assertNull(KVStoreLogin.makeLoginCredentials(secProps));

        /* Verify return null with null or empty pwd store */
        secProps.put(AUTH_USERNAME_PROPERTY, TESTUSER);
        secProps.remove(AUTH_PWDFILE_PROPERTY);
        assertTrue(secProps.containsKey(AUTH_USERNAME_PROPERTY));
        assertFalse(secProps.containsKey(AUTH_PWDFILE_PROPERTY));
        assertNull(KVStoreLogin.makeLoginCredentials(secProps));

        secProps.put(AUTH_PWDFILE_PROPERTY, "");
        assertTrue(secProps.containsKey(AUTH_PWDFILE_PROPERTY));
        assertNull(KVStoreLogin.makeLoginCredentials(secProps));
    }

    @Test
    public void testCreateSecurityProps() throws IOException {
        /*
         * Test creating login file, same test run for SSL and non-SSL cases
         */

        /* Verify return null with null security file */
        assertNull(KVStoreLogin.createSecurityProperties(null));

        /* Test create security props from security file correctly */
        final Properties secProps = new Properties();
        final String stuffs = "stuffs";

        /* Use absolute path to avoid automatic resolving */
        secProps.put(AUTH_USERNAME_PROPERTY, stuffs);
        secProps.put(AUTH_PWDFILE_PROPERTY, stuffs);
        secProps.put(TRANSPORT_PROPERTY, stuffs);
        secProps.put(SECURITY_FILE_PROPERTY, loginFile);
        secProps.put(AUTH_WALLET_PROPERTY, loginFile);
        secProps.put(AUTH_PWDFILE_PROPERTY, loginFile);
        secProps.put(KEYSTORE_FILE, loginFile);
        secProps.put(TRUSTSTORE_FILE, loginFile);
        secProps.store(new FileOutputStream(loginFile), null);

        final Properties reloadedSecProps =
            KVStoreLogin.createSecurityProperties(loginFile);
        assertEquals(secProps, reloadedSecProps);
    }

    @Test
    public void testLoginPaths()
        throws FileNotFoundException {

        /*
         * Test different paths of login file, same test run for SSL and
         * non-SSL cases
         */
        final File rootDir = TestUtils.getTestDir().getAbsoluteFile();
        final File testDir = new File(rootDir, "subdir");
        testDir.mkdir();

        tryLoginPath(new File("file1"), new File(testDir, "file1"));

        tryLoginPath(new File(".", "file2"),
                     new File(new File(testDir, "."), "file2"));

        tryLoginPath(new File("subdir", "file3"),
                     new File(new File(testDir, "subdir"), "file3"));

        tryLoginPath(new File("/tmp/file4"), new File("/tmp/file4"));
    }

    @Test
    public void testUserNameOverride() throws FileNotFoundException {
        /*
         * Test user name override, same test run for SSL and non-SSL cases
         */
        final FileOutputStream fos = new FileOutputStream(loginFile);
        final PrintStream ps = new PrintStream(fos);
        ps.println(AUTH_USERNAME_PROPERTY + "=" + TESTUSER);
        ps.println(TRANSPORT_PROPERTY + "=" + "ssl");
        ps.close();

        /*
         * Username specified in login info should override that in security
         * file.
         */
        KVStoreLogin storeLogin =
            new KVStoreLogin("new_user", loginFile);
        storeLogin.loadSecurityProperties();
        assertEquals(storeLogin.getUserName(), "new_user");

        /*
         * Username in security file should take effect if no username is
         * specified in login info.
         */
        storeLogin = new KVStoreLogin(null, loginFile);
        storeLogin.loadSecurityProperties();
        assertEquals(storeLogin.getUserName(), TESTUSER);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLoadLoginFileWithoutTransport()
        throws FileNotFoundException {

        /*
         * Test login file without transport setting, same test run for
         * SSL and non-SSL cases
         */
        final FileOutputStream fos = new FileOutputStream(loginFile);
        final PrintStream ps = new PrintStream(fos);
        ps.println(AUTH_USERNAME_PROPERTY + "=" + TESTUSER);
        ps.close();

        /* IAE should be thrown if no SSL transport */
        final KVStoreLogin storeLogin = new KVStoreLogin(null, loginFile);
        storeLogin.loadSecurityProperties();
    }

    @Test
    public void testSSLTranportCheck() throws FileNotFoundException {

        /*
         * Test setting transport, test run for SSL and non-SSL cases
         */

        /* Null security property */
        final KVStoreLogin storeLogin = new KVStoreLogin();
        assertFalse(storeLogin.foundTransportSettings());

        FileOutputStream fos = new FileOutputStream(loginFile);
        PrintStream ps = new PrintStream(fos);
        ps.println(AUTH_USERNAME_PROPERTY + "=" + TESTUSER);
        ps.close();

        /* Security property without SSL transport setting */
        try {
            storeLogin.updateLoginInfo(null, loginFile);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
        assertFalse(storeLogin.foundTransportSettings());

        /* Security property with SSL transport setting */
        fos = new FileOutputStream(loginFile);
        ps = new PrintStream(fos);
        ps.println(TRANSPORT_PROPERTY + "=" + "ssl");
        ps.close();
        storeLogin.updateLoginInfo(null, loginFile);
        assertTrue(storeLogin.foundTransportSettings());
    }

    @Test
    public void testGetLoginManager() throws Exception {
        final int port = createStore.getRegistryPort();
        final String host = createStore.getHostname();
        final String hostPort = host + ":" + port;

        final PasswordCredentials validPwdCreds =
            new PasswordCredentials(TESTUSER, TESTPASS);
        final PasswordCredentials invalidPwdCreds =
            new PasswordCredentials("foo", "bar".toCharArray());

        /* Verify return null with null pwd creds*/
        assertNull(KVStoreLogin.getAdminLoginMgr(host, port,
                                                 null /* pwdCreds*/, logger));
        assertNull(KVStoreLogin.getAdminLoginMgr(new String[] { hostPort },
                                                 null /* pwdCreds*/, logger));
        assertNull(KVStoreLogin.getRepNodeLoginMgr(host, port,
                                                   null /* pwdCreds*/,
                                                   kvstoreName));
        assertNull(KVStoreLogin.getRepNodeLoginMgr(new String[] { hostPort },
                                                   null /* pwdCreds*/,
                                                   kvstoreName));

        /* Verify get login manager successfully using valid creds */
        LoginManager loginMgr =
            KVStoreLogin.getAdminLoginMgr(host, port, validPwdCreds, logger);
        assertNotNull(loginMgr);
        assertEquals(loginMgr.getUsername(), TESTUSER);

        loginMgr = KVStoreLogin.getRepNodeLoginMgr(
            host, port, validPwdCreds, kvstoreName);
        assertNotNull(loginMgr);
        assertEquals(loginMgr.getUsername(), TESTUSER);

        /* Verify fail to get login manager using invalid creds */
        try {
            KVStoreLogin.getAdminLoginMgr(host, port, invalidPwdCreds, logger);
            fail("Expected AuthenticationFailureException");
        } catch (AuthenticationFailureException afe) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        try {
            KVStoreLogin.getRepNodeLoginMgr(
                host, port, invalidPwdCreds, kvstoreName);
            fail("Expected AuthenticationFailureException");
        } catch (AuthenticationFailureException afe) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
    }

    @Test
    public void testNonInteractiveCommandLineLogin()
        throws Exception {

        final Properties secProps = new Properties();

        CommandShell.loginTest = true;
        /*
         * Create a login file without user name specified but disable password
         * prompt for command line program login.
         */
        if (noSSL) {
            secProps.put(KVSecurityConstants.TRANSPORT_PROPERTY, "clear");
        } else {
            secProps.put(KVSecurityConstants.TRANSPORT_PROPERTY,
                         KVSecurityConstants.SSL_TRANSPORT_NAME);
            secProps.put(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                         SSLTestUtils.getSSLFilePath(SSLTestUtils.SSL_CTS_NAME));
        }
        secProps.put(CMD_PASSWORD_NOPROMPT_PROPERTY, "true");
        secProps.store(new FileOutputStream(loginFile), null);

        commandLineTests(loginFile,
                         "Must specify user name when " +
                         "password prompting is disabled");

        /*
         * Remove and recreate the login file with user name. Also create a
         * password store that does not contain password of given user name.
         */
        cleanup(loginFile);
        secProps.put(AUTH_USERNAME_PROPERTY, "non-existent");
        secProps.put(AUTH_PWDFILE_PROPERTY, pwdFile);
        secProps.store(new FileOutputStream(loginFile), null);
        commandLineTests(loginFile, "Invalid user name or password");
    }

    @Test
    public void testClientTrust()
        throws Exception {

        if (noSSL) {
            return;
        }

        Properties secProps = new Properties();

        /* login with password-less PKCS12 client.trust */
        secProps.put(AUTH_USERNAME_PROPERTY, TESTUSER);
        secProps.put(AUTH_PWDFILE_PROPERTY, pwdFile);
        secProps.put(KVSecurityConstants.TRANSPORT_PROPERTY,
                     KVSecurityConstants.SSL_TRANSPORT_NAME);
        secProps.put(KVSecurityConstants.SSL_TRUSTSTORE_TYPE_PROPERTY, "PKCS12");
        secProps.put(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                     SSLTestUtils.getSSLFilePath(SSLTestUtils.SSL_CTS_NAME));
        secProps.store(new FileOutputStream(loginFile), null);

        commandLineTests(loginFile, null /* no error */);

        /* login with JKS client.trust */
        cleanup(loginFile);
        secProps.put(KVSecurityConstants.SSL_TRUSTSTORE_TYPE_PROPERTY, "JKS");
        secProps.put(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                     SSLTestUtils.getSSLFilePath(SSLTestUtils.SSL_CTS_NAME));
        secProps.store(new FileOutputStream(loginFile), null);
        commandLineTests(loginFile, null /* no error */);

        /* login with password-less PKCS12 client.trust, no store type */
        secProps.put(AUTH_USERNAME_PROPERTY, TESTUSER);
        secProps.put(AUTH_PWDFILE_PROPERTY, pwdFile);
        secProps.put(KVSecurityConstants.TRANSPORT_PROPERTY,
                     KVSecurityConstants.SSL_TRANSPORT_NAME);
        secProps.remove(KVSecurityConstants.SSL_TRUSTSTORE_TYPE_PROPERTY);
        secProps.put(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                     SSLTestUtils.getSSLFilePath(SSLTestUtils.SSL_CTS_NAME));
        secProps.store(new FileOutputStream(loginFile), null);

        commandLineTests(loginFile, null /* no error */);

        /* login with JKS client.trust, no store type */
        cleanup(loginFile);
        secProps.put(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                     SSLTestUtils.getSSLFilePath(SSLTestUtils.SSL_JKS_CTS_NAME));
        secProps.store(new FileOutputStream(loginFile), null);
        commandLineTests(loginFile, null /* no error */);

        /* PKCS12 password-protected client.trust requires password */
        cleanup(loginFile);
        secProps.put(KVSecurityConstants.SSL_TRUSTSTORE_TYPE_PROPERTY, "PKCS12");
        secProps.put(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
            SSLTestUtils.getSSLFilePath(SSLTestUtils.SSL_PASS_CTS_NAME));
        secProps.store(new FileOutputStream(loginFile), null);
        commandLineTests(loginFile, "No trusted certificate found");

        /*
         * login with PKCS12 password-protected client.trust by specifying
         * password in login properties
         */
        cleanup(loginFile);
        secProps.put(KVSecurityConstants.SSL_TRUSTSTORE_PASSWORD_PROPERTY,
                     SSLTestUtils.SSL_KS_PWD_DEF);
        secProps.store(new FileOutputStream(loginFile), null);
        commandLineTests(loginFile, null /* no error */);

        /*
         * login with PKCS12 password-protected client.trust by specifying
         * password alias in password store
         */
        cleanup(loginFile);
        secProps.remove(KVSecurityConstants.SSL_TRUSTSTORE_PASSWORD_PROPERTY);
        secProps.put(KVSecurityConstants.SSL_TRUSTSTORE_PASSWORD_ALIAS_PROPERTY,
                     SSLTestUtils.SSL_KS_PWD_DEF);

    }

    private static void cleanup(String path) {
        File file = new File(path);
        if (file.exists()) {
            assertTrue(file.delete());
            assertFalse(file.exists());
        }
    }

    private void commandLineTests(final String securityFile,
                                  final String errorMessage)
        throws Exception {

        final int port = createStore.getRegistryPort();
        final String host = createStore.getHostname();
        final String hostPort = host + ":" + port;

        new CommandLineProgram() {

            @Override
            void checkResult(String result) throws Exception {
                if (errorMessage != null) {
                    assertThat("Ping error", result,
                        containsString(errorMessage));
                } else {
                    assertCLISucceed(result);
                }
            }

            @Override
            void performCommand() {
                Ping.main(new String[] { "-helper-hosts", hostPort,
                                         "-security", securityFile,
                                         "-json",
                                         "-no-exit",
                                         "-hidden"});
            }
        }.run();


        new CommandLineProgram() {

            @Override
            void checkResult(String result) throws Exception {
                if (errorMessage != null) {
                    assertThat("CLI error", result, containsString(errorMessage));
                    assertCLIFailed(result);
                } else {
                    assertCLISucceed(result);
                }
            }

            @Override
            void performCommand() throws Exception {
                CommandShell.main(new String[] { "-host", host,
                                                 "-port", String.valueOf(port),
                                                 "-security", securityFile,
                                                 "-no-exit",
                                                 "show", "topology",
                                                 "-json"});
            }
        }.run();

        new CommandLineProgram() {

            @Override
            void checkResult(String result) throws Exception {
                if (errorMessage != null) {
                    assertThat("CLI error", result, containsString(errorMessage));
                    assertCLIFailed(result);
                } else {
                    assertCLISucceed(result);
                }
            }

            @Override
            void performCommand() throws Exception {
                CommandShell.main(new String[] { "-host", host,
                                                 "-port", String.valueOf(port),
                                                 "-security", securityFile,
                                                 "-store",
                                                 createStore.getStoreName(),
                                                 "-no-exit",
                                                 "show", "tables",
                                                 "-json"});
            }
        }.run();
    }

    private static String createPwdFileStore(String path) throws Exception {
        File fileLoc = new File(path);
        fileLoc.delete();
        final PasswordManager pwdMgr = PasswordManager.load(
            "oracle.kv.impl.security.filestore.FileStoreManager");

        PasswordStore pwdfile = pwdMgr.getStoreHandle(fileLoc);
        pwdfile.create(null /* auto login */);
        pwdfile.setSecret(TESTUSER, TESTPASS);
        pwdfile.setSecret(CTS_PWD_ALIAS,
            SSLTestUtils.SSL_KS_PWD_DEF.toCharArray());
        pwdfile.save();
        return fileLoc.getAbsolutePath();
    }

    private static File createSubTestDir() {
        final File rootDir = TestUtils.getTestDir().getAbsoluteFile();
        final File testDir = new File(rootDir, "subdir");
        testDir.mkdir();
        return testDir;
    }

    private void tryLoginPath(File propFileName, File propAbsFileName)
        throws FileNotFoundException {

        final FileOutputStream fos = new FileOutputStream(loginFile);
        final PrintStream ps = new PrintStream(fos);

        final String path = propFileName.getPath();
        final String absPath = propAbsFileName.getPath();
        final String testKey = path;

        /* Files that should be adjusted */
        ps.println(SECURITY_FILE_PROPERTY + "=" + path);
        ps.println(AUTH_WALLET_PROPERTY + "=" + path);
        ps.println(AUTH_PWDFILE_PROPERTY + "=" + path);
        ps.println(KEYSTORE_FILE + "=" + path);
        ps.println(TRUSTSTORE_FILE + "=" + path);

        /* non-files */
        ps.println(TRANSPORT_PROPERTY + "=" + "ssl");
        ps.println(AUTH_USERNAME_PROPERTY + "=" + path);
        ps.println(PasswordManager.PWD_MANAGER + "=" + path);
        ps.close();

        final KVStoreLogin login = new KVStoreLogin();
        login.updateLoginInfo("joe", loginFile);
        final Properties props = login.getSecurityProperties();

        /* Check that filenames are converted to absolute */
        assertEquals(testKey, absPath,
                     props.getProperty(SECURITY_FILE_PROPERTY));
        assertEquals(testKey, absPath,
                     props.getProperty(AUTH_WALLET_PROPERTY));
        assertEquals(testKey, absPath,
                     props.getProperty(AUTH_PWDFILE_PROPERTY));
        assertEquals(testKey, absPath,
                     props.getProperty(KEYSTORE_FILE));
        assertEquals(testKey, absPath,
                     props.getProperty(TRUSTSTORE_FILE));

        /* Check that non-filenames are not converted to absolute */
        assertEquals(testKey, "ssl",
                     props.getProperty(TRANSPORT_PROPERTY));
        assertEquals(testKey, path,
                     props.getProperty(AUTH_USERNAME_PROPERTY));
        assertEquals(testKey, path,
                     props.getProperty(AUTH_USERNAME_PROPERTY));
    }

    private static class MockedShellInputReader extends ShellInputReader {
        public final static String SHELL_USER = "ShellUser";
        public final static char[] SHELL_PASS =
            "UUll11__ShellPass".toCharArray();

        private final boolean hasInput;

        public MockedShellInputReader() {
            this(System.in, System.out);
        }

        public MockedShellInputReader(InputStream in, PrintStream out) {
            super(in, out);
            hasInput = (in != System.in);
        }

        @Override
        public String readLine(String promptString) throws IOException {
            if (hasInput) {
                return super.readLine(promptString);
            }
            return SHELL_USER;
        }

        @Override
        public char[] readPassword(String promptString) throws IOException {
            if (hasInput) {
                return super.readPassword(promptString);
            }
            return SHELL_PASS;
        }
    }

    private static void assertCLIFailed(String json)
        throws Exception {

        checkCLIJsonOutput(json, ErrorMessage.NOSQL_5100.getValue());
    }

    private static void assertCLISucceed(String json)
        throws Exception {

        /* All security related exceptions are all wrapped into ShellException
         * in CommandShell, the error return code of ShellException is
         * NoSQL_5100.
         */
        checkCLIJsonOutput(json, ErrorMessage.NOSQL_5000.getValue());
    }

    /*
     * Check Admin CLI json output.
     */
    private static void checkCLIJsonOutput(String result, int expectedCode)
        throws Exception {

        ObjectNode json = JsonUtils.parseJsonObject(result);
        assertNotNull(json);
        int jsonReturnCode = json.get("returnCode").asInt();
        if (expectedCode != jsonReturnCode) {
            System.out.println(json);
        }
        assertTrue(expectedCode == jsonReturnCode);
    }

    /**
     * Perform a top-level command line program and collect the output as a
     * string.
     */
    private static abstract class CommandLineProgram {

        void run() throws Exception {
            final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
            final ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
            final PrintStream originalOut = System.out;
            final PrintStream originalErr = System.err;
            try {
                System.setOut(new PrintStream(outBytes));
                System.setErr(new PrintStream(errBytes));
                performCommand();
            } finally {
                System.setOut(originalOut);
                System.setErr(originalErr);
            }

            String output = errBytes.toString() + outBytes.toString();
            checkResult(output);
        }

        /* Check program output result */
        abstract void checkResult(String result) throws Exception;

        /* Perform the command line program */
        abstract void performCommand() throws Exception ;
    }
}
