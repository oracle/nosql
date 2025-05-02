/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.kerberos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.KVSecurityConstants;
import oracle.kv.LoginCredentials;
import oracle.kv.impl.security.login.KerberosClientCreds;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.security.util.KVStoreLogin.PasswordCallbackHandler;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.util.shell.ShellInputReader;

import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.kerby.kerberos.kerb.ccache.CredentialCache;
import org.apache.kerby.kerberos.kerb.common.EncryptionUtil;
import org.apache.kerby.kerberos.kerb.keytab.Keytab;
import org.apache.kerby.kerberos.kerb.keytab.KeytabEntry;
import org.apache.kerby.kerberos.kerb.type.KerberosTime;
import org.apache.kerby.kerberos.kerb.type.base.EncryptionKey;
import org.apache.kerby.kerberos.kerb.type.base.PrincipalName;
import org.apache.kerby.kerberos.kerb.type.ticket.TgtTicket;
import org.junit.Test;

/**
 * Test Kerberos login behavior
 */
public class KerberosLoginHelperTest extends KerberosTestBase {

    private static final String servicePrincipalName = "oraclenosql";
    private static final String serverHost = "localhost";

    private static final String clientPrincipalName = "client";
    private static final File clientCcache =
        new File(TestUtils.getTestDir(), "clientccache");
    private static final File clientKeytab =
        new File(TestUtils.getTestDir(), clientPrincipalName + ".keytab");

    /* Principal use for failure credentials cases */
    private static final String failurePrincipalName = "failure";
    private static final File failureCcache =
        new File(TestUtils.getTestDir(), "failureccache");
    private static final File failureKeytab =
        new File(TestUtils.getTestDir(), failurePrincipalName + ".keytab");

    private static InputStream inputStream;
    private static OutputStream outputStream;
    private static PrintStream printStream;


    @Override
    public void setUp() throws Exception {
        super.setUp();
        startKdc();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        stopKdc();
    }

    /**
     * - Test login to Kerberos for making the Kerberos credentials, use the
     * TGT saved in the specific credentials cache.
     *
     * - Test login to Kerberos for making the Kerberos credentials, use the
     * TGT saved in the default credentials cache.
     *
     * - Use the failure ccache, keytab, do not prompt, the login should fail.
     */
    @Test
    public void testKrbLoginUseCcache() throws Exception {
        /* Preparation */
        addPrincipal(clientKeytab, clientPrincipalName);
        createCcache(clientPrincipalName, clientKeytab, clientCcache);
        createCcache(clientPrincipalName, clientKeytab,
            getSysDefaultKrbCache());
        addPrincipal(failureKeytab, failurePrincipalName);
        createCcache(failurePrincipalName, failureKeytab, failureCcache);
        Properties secProps = createTestSecProperties();

        /* Test use specific ticket cache, set the wrong keytab */
        secProps.setProperty(KVSecurityConstants.AUTH_KRB_CCACHE_PROPERTY,
            clientCcache.getAbsolutePath());
        secProps.setProperty(KVSecurityConstants.AUTH_KRB_KEYTAB_PROPERTY,
            failureKeytab.getAbsolutePath());

        /* Set do not prompt to be true */
        LoginCredentials krbCred = KVStoreLogin.buildKerberosCreds(
            clientPrincipalName, secProps, null /* callback handler */);

        /* Validate the results */
        checkTestLoginCredentials(krbCred);

        /* Remove the ccache, it will use the default location */
        secProps.remove(KVSecurityConstants.AUTH_KRB_CCACHE_PROPERTY);
        krbCred = KVStoreLogin.buildKerberosCreds(
           clientPrincipalName, secProps, null /* callback handler */);

        /* Use the wrong ccache, wrong keytab, expect fail */
        secProps.setProperty(KVSecurityConstants.AUTH_KRB_CCACHE_PROPERTY,
            failureCcache.getAbsolutePath());
        secProps.setProperty(KVSecurityConstants.AUTH_KRB_KEYTAB_PROPERTY,
            failureKeytab.getAbsolutePath());

        try {
            krbCred = KVStoreLogin.buildKerberosCreds(
            clientPrincipalName, secProps, null /* callback handler */);
            fail("expect AuthenticationFailureException");
        } catch (AuthenticationFailureException afe) {
            assertTrue(afe.getCause() instanceof
                       javax.security.auth.login.LoginException);
        }
    }

    /**
     * - Test making the Kerberos credentials, use the key saved in the specific
     * keytab file to get the TGT.
     *
     * - Test making the Kerberos credentials, use the key saved in the default
     * keytab file to get the TGT.
     *
     * - Use the incorrect keytab, ccache, the login should fail.
     *
     */
    @Test
    public void testKrbLoginUseKeytab() throws Exception {
        /* Preparation */
        String clientPassword = "123456";
        addPrincipal(clientPrincipalName, clientPassword);
        createKeytab(clientKeytab, clientPrincipalName, clientPassword);
        createKeytab(getSysDefaultKrbKeytab(), clientPrincipalName,
            clientPassword);
        addPrincipal(failureKeytab, failurePrincipalName);
        createCcache(failurePrincipalName, failureKeytab, failureCcache);
        Properties secProps = createTestSecProperties();

        /* Test use specific keytab, set failure ccache */
        secProps.setProperty(KVSecurityConstants.AUTH_KRB_KEYTAB_PROPERTY,
            clientKeytab.getAbsolutePath());
        secProps.setProperty(KVSecurityConstants.AUTH_KRB_CCACHE_PROPERTY,
            failureCcache.getAbsolutePath());
        LoginCredentials krbCred = KVStoreLogin.buildKerberosCreds(
            clientPrincipalName, secProps, null);
        checkTestLoginCredentials(krbCred);

        /* Test use default keytab */
        secProps.remove(KVSecurityConstants.AUTH_KRB_KEYTAB_PROPERTY);
        KVStoreLogin.buildKerberosCreds(
            clientPrincipalName, secProps, null);
        checkTestLoginCredentials(krbCred);

        /* Use incorrect keytab, ccache, expect fail */
        secProps.setProperty(KVSecurityConstants.AUTH_KRB_CCACHE_PROPERTY,
            failureCcache.getAbsolutePath());
        secProps.setProperty(KVSecurityConstants.AUTH_KRB_KEYTAB_PROPERTY,
            failureKeytab.getAbsolutePath());
        try {
            krbCred = KerberosLoginHelper.buildKerberosCreds(
                clientPrincipalName, secProps, null/* doNotPrompt */);
            fail("expect AuthenticationFailureException");
        } catch (AuthenticationFailureException afe) {
            assertTrue(afe.getCause() instanceof
                       javax.security.auth.login.LoginException);
        }
    }

    /**
     * - Test making the Kerberos credentials via password prompt.
     *
     * - Use the incorrect password, the login should fail.
     */
    @Test
    public void testKrbLoginUsePasswordCallback() throws Exception {
        /* Preparation */
        final String clientPassword = "123456";
        final String incorrectPassword = "654321";
        final String clientPrincipalFullName =
            clientPrincipalName + "@" + getRealm();
        addPrincipal(clientPrincipalName, clientPassword);
        addPrincipal(failureKeytab, failurePrincipalName);
        createCcache(failurePrincipalName, failureKeytab, failureCcache);
        Properties secProps = createTestSecProperties();

        /* Test password callback, use the fail keytab, ccache */
        secProps.setProperty(KVSecurityConstants.AUTH_KRB_CCACHE_PROPERTY,
            failureCcache.getAbsolutePath());
        secProps.setProperty(KVSecurityConstants.AUTH_KRB_KEYTAB_PROPERTY,
            failureKeytab.getAbsolutePath());
        ShellInputReader pwdReader = createTestShellInputReader(clientPassword);
        PasswordCallbackHandler handler = new PasswordCallbackHandler(
            clientPrincipalFullName, pwdReader);
        LoginCredentials krbCred = KerberosLoginHelper.buildKerberosCreds(
            clientPrincipalName, secProps, handler);

        /* Check the prompt message */
        checkShellInputResults(clientPrincipalFullName +
            "'s kerberos password:");
        closeAllTestStream();
        checkTestLoginCredentials(krbCred);

        /* Test with incorrect password */
        pwdReader = createTestShellInputReader(incorrectPassword);
        handler = new PasswordCallbackHandler(clientPrincipalFullName, pwdReader);

        try {
            krbCred = KerberosLoginHelper.buildKerberosCreds(
                clientPrincipalName, secProps, handler);
            fail("expect AuthenticationFailureException");
        } catch (AuthenticationFailureException afe) {
            /* Check the prompt message */
            checkShellInputResults(clientPrincipalFullName +
                "'s kerberos password:");
            assertTrue(afe.getCause() instanceof
                       javax.security.auth.login.LoginException);
        } finally {
            closeAllTestStream();
        }
    }

    /**
     * Illegal argument set in the security properties, expect fail
     */
    @Test
    public void testKrbLoginWithIllegalArgument() throws Exception {
        addPrincipal(clientKeytab, clientPrincipalName);
        createCcache(clientPrincipalName, clientKeytab, clientCcache);
        Properties secProps = createTestSecProperties();
        /* Mechanism does not support */
        secProps.put(KVSecurityConstants.AUTH_EXT_MECH_PROPERTY, "LDAP");
        try {
            KerberosLoginHelper.buildKerberosCreds(
                clientPrincipalName, secProps, null);
            fail("expect IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertEquals(iae.getMessage(),
                "Unsupported external authentication mechanism in the " +
                "configuration.");
        }

        /* Empty mechanism */
        secProps.put(KVSecurityConstants.AUTH_EXT_MECH_PROPERTY, "");
        try {
            KerberosLoginHelper.buildKerberosCreds(
                clientPrincipalName, secProps, null);
            fail("expect IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertEquals(iae.getMessage(),
                "Unsupported external authentication mechanism in the " +
                "configuration.");
        }

        secProps = createTestSecProperties();
        /* Test the pattern "host:principal [,host:principal]*" is working */
        secProps.put("oracle.kv.auth.kerberos.services",
            serverHost + ":" +servicePrincipalName + ", " +
            serverHost + ":" +servicePrincipalName + ", " +
            serverHost + ":" +servicePrincipalName);
        LoginCredentials krbCred = KerberosLoginHelper.buildKerberosCreds(
            clientPrincipalName, secProps, null);
        checkTestLoginCredentials(krbCred);

        /* Specify only service principal name without host */
        secProps.put(KVSecurityConstants.AUTH_KRB_SERVICES_PROPERTY,
                     servicePrincipalName);
        try {
            KerberosLoginHelper.buildKerberosCreds(
                clientPrincipalName, secProps, null);
            fail("expect IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("does not match the pattern"));
        }

        /* Specify text that does not match the host:principal pattern */
        secProps.put(KVSecurityConstants.AUTH_KRB_SERVICES_PROPERTY,
                     "abc:abc:abc, cba:,");
        try {
            KerberosLoginHelper.buildKerberosCreds(
                clientPrincipalName, secProps, null);
            fail("expect IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("does not match the pattern"));
        }
        secProps.put(KVSecurityConstants.AUTH_KRB_SERVICES_PROPERTY, ",:,:,");
        try {
            KerberosLoginHelper.buildKerberosCreds(
               clientPrincipalName, secProps, null);
            fail("expect IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("does not match the pattern"));
        }

        /* Illegal mutualAuth field */
        secProps = createTestSecProperties();
        secProps.put(KVSecurityConstants.AUTH_KRB_MUTUAL_PROPERTY, "truth");
        try {
            KerberosLoginHelper.buildKerberosCreds(
                clientPrincipalName, secProps, null);
            fail("expect IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().
                contains("Invalid input for boolean field"));
        }
        secProps.put(KVSecurityConstants.AUTH_KRB_MUTUAL_PROPERTY, "");
        try {
            KerberosLoginHelper.buildKerberosCreds(
                clientPrincipalName, secProps, null);
            fail("expect IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().
                contains("Invalid input for boolean field"));
        }
        secProps.put(KVSecurityConstants.AUTH_KRB_MUTUAL_PROPERTY, "fals");
        try {
            KerberosLoginHelper.buildKerberosCreds(
                clientPrincipalName, secProps, null);
            fail("expect IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().
                contains("Invalid input for boolean field"));
        }
        final String nonExistCcache = "/tmp/non-exist-ccache";
        secProps = createTestSecProperties();
        secProps.put(KVSecurityConstants.AUTH_KRB_CCACHE_PROPERTY,
                     nonExistCcache);
        try {
            KerberosLoginHelper.buildKerberosCreds(
               clientPrincipalName, secProps, null);
            fail("expect IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertEquals(iae.getMessage(),
                "The specified path " + nonExistCcache + " does not exist");
        }
        final String nonExistKeytab = "/tmp/non-exist-keytab";
        secProps = createTestSecProperties();
        secProps.put(KVSecurityConstants.AUTH_KRB_KEYTAB_PROPERTY,
                     nonExistKeytab);
        try {
            KerberosLoginHelper.buildKerberosCreds(
                clientPrincipalName, secProps, null);
            fail("expect IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertEquals(iae.getMessage(),
                "The specified path " + nonExistKeytab + " does not exist");
        }
    }


    /**
     * - Set the correct ccache, keytab, use correct password, the login
     * should succeed.
     * - Set the incorrect ccache, correct keytab and password, the login
     * should pick to use the keytab, login succeed.
     * - Set the correct ccache, incorrect keytab, correct password, the login
     * should pick to use the ccache, login succeed.
     * - Set the incorrect ccache, keytab, but correct password, the login
     * should use the password, login succeed.
     * - Set the incorrect ccache, keytab, password, the login will fail
     * - Create the correct ccache, keytab in the default place, do not set
     * the ccache, keytab options, login succeed.
     */
    @Test
    public void testKrbLoginPriority() throws Exception {
        final String clientPassword = "123456";
        final String incorrectPass = "654321";
        final String clientPrincipalFullName =
            clientPrincipalName + "@" + getRealm();
        addPrincipal(clientPrincipalName, clientPassword);
        createKeytab(clientKeytab, clientPrincipalName, clientPassword);
        createKeytab(getSysDefaultKrbKeytab(), clientPrincipalName,
            clientPassword);
        createCcache(clientPrincipalName, clientKeytab, clientCcache);
        createCcache(clientPrincipalName, clientKeytab,
            getSysDefaultKrbCache());
        addPrincipal(failureKeytab, failurePrincipalName);
        createCcache(failurePrincipalName, failureKeytab, failureCcache);

        /* Set all correct properties, it will use the ccache first */
        Properties secProps = createTestSecProperties();
        secProps.setProperty(KVSecurityConstants.AUTH_KRB_CCACHE_PROPERTY,
            clientCcache.getAbsolutePath());
        secProps.setProperty(KVSecurityConstants.AUTH_KRB_KEYTAB_PROPERTY,
            clientKeytab.getAbsolutePath());
        ShellInputReader pwdReader = createTestShellInputReader(clientPassword);
        PasswordCallbackHandler handler = new PasswordCallbackHandler(
            clientPrincipalName, pwdReader);
        LoginCredentials krbCred = KerberosLoginHelper.buildKerberosCreds(
            clientPrincipalName, secProps, handler);

        /* Use ccache, the prompt should be empty */
        checkShellInputResults("");
        closeAllTestStream();
        checkTestLoginCredentials(krbCred);

        /* Correct ccache, incorrect keytab, correct password */
        secProps.setProperty(KVSecurityConstants.AUTH_KRB_KEYTAB_PROPERTY,
            failureKeytab.getAbsolutePath());
        krbCred = KerberosLoginHelper.buildKerberosCreds(
            clientPrincipalName, secProps, handler);
        checkShellInputResults("");
        closeAllTestStream();
        checkTestLoginCredentials(krbCred);

        /* Correct keytab, incorrect ccache, correct password */
        secProps = createTestSecProperties();
        secProps.setProperty(KVSecurityConstants.AUTH_KRB_KEYTAB_PROPERTY,
            clientKeytab.getAbsolutePath());
        secProps.setProperty(KVSecurityConstants.AUTH_KRB_CCACHE_PROPERTY,
            failureCcache.getAbsolutePath());
        krbCred = KerberosLoginHelper.buildKerberosCreds(
            clientPrincipalName, secProps, handler);
        checkShellInputResults("");
        closeAllTestStream();
        checkTestLoginCredentials(krbCred);

        /* Incorrect ccache, incorrect keytab, correct password */
        pwdReader = createTestShellInputReader(clientPassword);
        handler = new PasswordCallbackHandler(
            clientPrincipalFullName, pwdReader);
        secProps.setProperty(KVSecurityConstants.AUTH_KRB_CCACHE_PROPERTY,
            failureCcache.getAbsolutePath());
        secProps.setProperty(KVSecurityConstants.AUTH_KRB_KEYTAB_PROPERTY,
            failureKeytab.getAbsolutePath());
        krbCred = KerberosLoginHelper.buildKerberosCreds(
            clientPrincipalName, secProps, handler);

        /* Should ask for the password in the output */
        checkShellInputResults(
            clientPrincipalFullName + "'s kerberos password:");
        closeAllTestStream();
        checkTestLoginCredentials(krbCred);

        /* Incorrect ccache, incorrect keytab, incorrect password, fail */
        secProps.setProperty(KVSecurityConstants.AUTH_KRB_CCACHE_PROPERTY,
            failureCcache.getAbsolutePath());
        secProps.setProperty(KVSecurityConstants.AUTH_KRB_KEYTAB_PROPERTY,
            failureKeytab.getAbsolutePath());
        pwdReader = createTestShellInputReader(incorrectPass);
        handler = new PasswordCallbackHandler(
            clientPrincipalFullName, pwdReader);
        try {
            krbCred = KerberosLoginHelper.buildKerberosCreds(
                clientPrincipalName, secProps, handler);
            fail("expect AuthenticationFailureException");
        } catch (AuthenticationFailureException afe) {
            /* Should ask for the password in the output */
            checkShellInputResults(clientPrincipalFullName +
                "'s kerberos password:");
            assertTrue(afe.getCause() instanceof
                       javax.security.auth.login.LoginException);
        } finally {
            closeAllTestStream();
        }

        /* Correct ccache, keytab in default place, do not set the options */
        secProps.remove(KVSecurityConstants.AUTH_KRB_CCACHE_PROPERTY);
        secProps.remove(KVSecurityConstants.AUTH_KRB_KEYTAB_PROPERTY);

        pwdReader = createTestShellInputReader(clientPassword);
        handler = new PasswordCallbackHandler(
            clientPrincipalName, pwdReader);

        krbCred = KerberosLoginHelper.buildKerberosCreds(
           clientPrincipalName, secProps, handler);

        /* No prompt because the default place has the credentials needed */
        checkShellInputResults("");
        closeAllTestStream();
        checkTestLoginCredentials(krbCred);
    }

    /* Check the returned login credentials match the default input's results */
    private void checkTestLoginCredentials(LoginCredentials creds) {
        assertNotNull(creds);
        assertTrue(creds instanceof KerberosClientCreds);
        KerberosClientCreds krbCreds = (KerberosClientCreds)creds;
        assertEquals(krbCreds.getKrbServicePrincipals().
            getPrincipal(serverHost), servicePrincipalName);
        assertEquals(krbCreds.getUsername(),
                     clientPrincipalName + "@" + getRealm());

        /* Only TGT will be saved for default login */
        assertTrue(krbCreds.
            getLoginSubject().getPrivateCredentials().size() == 1);
    }

    /* Create the default test properties for security configuration */
    private Properties createTestSecProperties() {
        Properties result = new Properties();
        result.put(KVSecurityConstants.AUTH_USERNAME_PROPERTY,
                   clientPrincipalName);
        result.put(KVSecurityConstants.AUTH_KRB_SERVICES_PROPERTY,
            serverHost + ":" + servicePrincipalName);
        result.put(KVSecurityConstants.AUTH_EXT_MECH_PROPERTY, "KERBEROS");
        result.put(KVSecurityConstants.AUTH_KRB_CCACHE_PROPERTY,
            clientCcache.getAbsolutePath());
        result.put(KVSecurityConstants.AUTH_KRB_KEYTAB_PROPERTY,
            clientKeytab.getAbsolutePath());
        result.put(KVSecurityConstants.AUTH_KRB_MUTUAL_PROPERTY, "true");
        result.put(KVSecurityConstants.SSL_PROTOCOLS_PROPERTY, "ssl");
        result.put(KVSecurityConstants.AUTH_KRB_REALM_PROPERTY,
                   getRealm());
        return result;
    }

    /* Create the test reader which will enter the password automatically */
    private ShellInputReader createTestShellInputReader(String password)
        throws IOException {
        closeAllTestStream();
        inputStream = new ByteArrayInputStream((password + "\n").getBytes());
        outputStream = new ByteArrayOutputStream();
        printStream = new PrintStream(outputStream);
        return ShellInputReader.getReader(inputStream, printStream);
    }

    /* Check the output in the default output stream */
    private void checkShellInputResults(String expectedOutput) {
        assertEquals(expectedOutput, outputStream.toString().trim());
    }

    /* Close all the test streams */
    private void closeAllTestStream() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
        if (outputStream != null) {
            outputStream.close();
        }
        if (printStream != null) {
            printStream.close();
        }
    }

    /* Create the keytab for a user using the specified password */
    private void createKeytab(File keytabFile, String name, String pwd)
        throws IOException, KrbException {

        Keytab keytab = new Keytab();
        List<KeytabEntry> entries = new ArrayList<KeytabEntry>();
        String princName = name + "@" + getRealm();
        PrincipalName principal = new PrincipalName(princName);
        KerberosTime timestamp = new KerberosTime();

        List<EncryptionKey> keys = EncryptionUtil.generateKeys(princName, pwd,
            getKdc().getKdcConfig().getEncryptionTypes());

        keys.forEach(key -> {
            entries.add(new KeytabEntry(
                principal, timestamp, key.getKvno(), key));
        });
        keytab.addKeytabEntries(entries);
        keytab.store(keytabFile);
    }

    /* Create a ccache for user using the key saved in the keytab */
    private void createCcache(String name, File keytab, File ccache)
        throws Exception {

        TgtTicket ticket = getKdc().getKrbClient().requestTgt(name, keytab);
        CredentialCache cc =  new CredentialCache(ticket);
        cc.store(ccache);
    }

    /* Try to get the default location of Kerberos credentials cache */
    private static File getSysDefaultKrbCache() {
        Scanner sc = null;
        try {
            final String userName = System.getProperty("user.name");
            final String[] command = { "id", "-u", userName };
            Process proc = Runtime.getRuntime().exec(command);
            sc = new Scanner(proc.getInputStream());
            if (sc.hasNext()) {
                return new File("/tmp/krb5cc_" + sc.next().trim());
            }
        } catch (Exception e) {
            fail("Fail to get the user id");
        } finally {
            if (sc != null) {
                sc.close();
            }
        }
        fail("Fail to get the user id");
        return null;
    }

    /* Try to get the default location of Kerberos keytab file */
    private static File getSysDefaultKrbKeytab() {
        final String home = System.getProperty("user.home");
        if (home == null) {
            fail("fail to get the user home");
            return null;
        }
        return new File(home + "/krb5.keytab");
    }
}
