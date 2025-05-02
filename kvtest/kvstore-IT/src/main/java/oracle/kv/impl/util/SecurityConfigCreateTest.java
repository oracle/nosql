/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static oracle.kv.impl.security.PasswordManager.FILE_STORE_MANAGER_CLASS;
import static oracle.kv.impl.util.FileNames.SECURITY_CONFIG_DIR;
import static oracle.kv.impl.util.FileNames.SECURITY_UPDATES_DIR;
import static oracle.kv.impl.util.SecurityConfigCreator.checkSecurityDirectory;
import static oracle.kv.impl.util.TestUtils.assertMatch;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.param.ParameterState;

import org.junit.Test;

/**
 * Test the securityconfig create command-line interface.
 */
public class SecurityConfigCreateTest extends SecurityConfigTestBase {

    public final static String KRB_CONF_FILE = "krb5.conf";
    private File testRoot = new File(TestUtils.getTestDir(), "testroot");
    private File existingFile = null;

    TestPasswordReader pwReader = new TestPasswordReader(null);

    @Override
    public void setUp() throws Exception {

        super.setUp();
        removeDirectory(testRoot, 20);
        testRoot.mkdir();

        existingFile = new File(TestUtils.getTestDir(), "aFile");
        existingFile.createNewFile();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
        removeDirectory(testRoot, 20);
    }

    /**
     * Test creation error conditions.
     */
    @Test
    public void createWithErrors()
        throws Exception {

        String s;

        /* Auto-login wallets need no passphrase */
        pwReader.setPassword(null);

        /**
         * non-existent directory
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "create",
                                        "-root",
                                        "/nonexistent/directory"});

        assertTrue(s.indexOf("does not exist") != -1);

        /**
         * not a writable directory
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "create",
                                        "-root",
                                        "/"});

        assertTrue(s.indexOf("Unable to create") != -1);

        /**
         * not a directory
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "create",
                                        "-root",
                                        existingFile.getPath()});

        assertTrue(s.indexOf("Unable to create") != -1);

    }

    /**
     * Test creation error conditions specific to -params.
     */
    @Test
    public void createWithParamsErrors()
        throws Exception {

        String s;

        /* Auto-login wallets need no passphrase */
        pwReader.setPassword(null);

        /* Invalid format */
        s = runCLICommand(pwReader,
                          new String[] {"config", "create",
                                        "-root",
                                        testRoot.getPath(),
                                        "-kspwd",
                                        passphrase,
                                        "-param",
                                        "foo"});
        assertTrue(s.indexOf("parameter setting") != -1);

        /* Invalid format */
        s = runCLICommand(pwReader,
                          new String[] {"config", "create",
                                        "-root",
                                        testRoot.getPath(),
                                        "-kspwd",
                                        passphrase,
                                        "-param",
                                        "foo:bar:baz=zot"});
        assertTrue(s.indexOf("parameter name format") != -1);

        /* Invalid name */
        s = runCLICommand(pwReader,
                          new String[] {"config", "create",
                                        "-root",
                                        testRoot.getPath(),
                                        "-kspwd",
                                        passphrase,
                                        "-param",
                                        "foo=bar"});
        assertTrue(s.indexOf("not a valid parameter name") != -1);

        /* not a security param */
        s = runCLICommand(pwReader,
                          new String[] {"config", "create",
                                        "-root",
                                        testRoot.getPath(),
                                        "-kspwd",
                                        passphrase,
                                        "-param",
                                        "memoryMB=1024"});
        assertTrue(s.indexOf("not a valid parameter for " +
                             "a security configuration") != -1);

        /* not a valid transport name */
        s = runCLICommand(
            pwReader,
            new String[] {"config", "create",
                          "-root",
                          testRoot.getPath(),
                          "-kspwd",
                          passphrase,
                          "-param",
                          ("foo:" +
                           ParameterState.SEC_TRANS_ALLOW_PROTOCOLS +
                           "=TLSv1")});

        assertTrue(s.indexOf("foo is not a valid transport name") != -1);

        /* not a transport parameter */
        s = runCLICommand(
            pwReader,
            new String[] {"config", "create",
                          "-root",
                          testRoot.getPath(),
                          "-kspwd",
                          passphrase,
                          "-param",
                          (ParameterState.SECURITY_TRANSPORT_JE_HA + ":" +
                           ParameterState.SEC_TRUSTSTORE_TYPE + "=JKS")});

        assertTrue(s.indexOf("is not a transport parameter") != -1);

        /* not a Kerberos configuration parameter */
        s = runCLICommand(
            pwReader,
            new String[] {"config", "create",
                          "-root",
                          testRoot.getPath(),
                          "-kspwd",
                          passphrase,
                          "-princ-conf-param",
                          "clearpolicy=xxx"});
        assertTrue(s.indexOf(
            "is not a valid Kerberos configuration parameter") != -1);

        /* not a valid KeyStore type */
        s = runCLICommand(
            pwReader,
            new String[] {"config", "create",
                          "-root",
                          testRoot.getPath(),
                          "-kspwd",
                          passphrase,
                          "-kstype",
                          "xxx"});
        assertTrue(s.indexOf(
            "is not a valid KeyStore type") != -1);
    }

    /**
     * Create with JKS KeyStores, default security dir, inline password.
     */
    @Test
    public void createWithJKSKeyStore()
        throws Exception {

        pwReader.setPassword(null);

        final String s =
            runCLICommand(pwReader,
                          new String[] {"config", "create",
                                        "-root",
                                        testRoot.getPath(),
                                        "-pwdmgr",
                                        "wallet",
                                        "-kstype",
                                        "jks",
                                        "-kspwd",
                                        passphrase});
        assertTrue(s.indexOf("Created") != -1);
        assertWalletFilesExist(testRoot.getPath(), DEFAULT_SECDIR);
    }

    /**
     * Create with PKCS12 KeyStores, password-protected client.trust,
     * default security dir, inline password. Default password-less
     * configuration is tested by all other tests.
     */
    @Test
    public void createWithPKCS12KeyStore()
        throws Exception {

        pwReader.setPassword(null);

        final String s =
            runCLICommand(pwReader,
                          new String[] {"config", "create",
                                        "-root",
                                        testRoot.getPath(),
                                        "-pwdmgr",
                                        "wallet",
                                        "-ctspwd",
                                        passphrase,
                                        "-kspwd",
                                        passphrase});
        assertTrue(s.indexOf("Created") != -1);
        assertWalletFilesExist(testRoot.getPath(), DEFAULT_SECDIR);
    }

    /**
     * Create with wallet, default security dir, inline password.
     */
    @Test
    public void createWithWalletDefDirInlinePwd()
        throws Exception {

        pwReader.setPassword(null);

        final String s =
            runCLICommand(pwReader,
                          new String[] {"config", "create",
                                        "-root",
                                        testRoot.getPath(),
                                        "-pwdmgr",
                                        "wallet",
                                        "-kspwd",
                                        passphrase});
        assertTrue(s.indexOf("Created") != -1);
        assertWalletFilesExist(testRoot.getPath(), DEFAULT_SECDIR);
    }

    /**
     * Create with wallet, standard security dir, prompted password.
     */
    @Test
    public void createWithWalletDefDirPromptPwd()
        throws Exception {

        pwReader.setPassword(passphrase);

        /**
         * valid directory and good passphrase
         */
        final String s =
            runCLICommand(pwReader,
                          new String[] {"config", "create",
                                        "-root",
                                        testRoot.getPath(),
                                        "-pwdmgr",
                                        "wallet"});
        assertTrue(s.indexOf("Created") != -1);
        assertWalletFilesExist(testRoot.getPath(), DEFAULT_SECDIR);
    }

    /**
     * Create with wallet, alternate security dir, inline password.
     */
    @Test
    public void createWithWalletAltDirInlinePwd()
        throws Exception {

        pwReader.setPassword(null);

        final String s =
            runCLICommand(pwReader,
                          new String[] {"config", "create",
                                        "-root",
                                        testRoot.getPath(),
                                        "-secdir",
                                        ALT_SECDIR,
                                        "-pwdmgr",
                                        "wallet",
                                        "-kspwd",
                                        passphrase});
        assertTrue(s.indexOf("Created") != -1);
        assertWalletFilesExist(testRoot.getPath(), ALT_SECDIR);
    }

    /**
     * Create with pwdfile, default security dir, inline password.
     */
    @Test
    public void createWithPwdfileDefDirInlinePwd()
        throws Exception {

        pwReader.setPassword(null);

        final String s =
            runCLICommand(pwReader,
                          new String[] {"config", "create",
                                        "-root",
                                        testRoot.getPath(),
                                        "-pwdmgr",
                                        "pwdfile",
                                        "-kspwd",
                                        passphrase});
        assertTrue(s.indexOf("Created") != -1);
        assertPwdfileFilesExist(testRoot.getPath(), DEFAULT_SECDIR);
    }

    /**
     * Create with pwdfile, explict pwdmgr, default security dir,
     * inline password.
     */
    @Test
    public void createWithExplicitPwdfileDefDirInlinePwd()
        throws Exception {

        pwReader.setPassword(null);

        final String s =
            runCLICommand(pwReader,
                          new String[] {"config", "create",
                                        "-root",
                                        testRoot.getPath(),
                                        "-pwdmgr",
                                        FILE_STORE_MANAGER_CLASS,
                                        "-kspwd",
                                        passphrase});
        assertTrue(s.indexOf("Created") != -1);
        assertPwdfileFilesExist(testRoot.getPath(), DEFAULT_SECDIR);
    }

    /**
     * Create with default password manager (wallet), default security dir,
     * inline password, params.
     */
    @Test
    public void createWithDefDirInlinePwdParams()
        throws Exception {

        pwReader.setPassword(null);

        final String jeProtocols = "TLSv1";
        final String cipherSuites =
            "SSL_RSA_WITH_RC4_128_MD5,SSL_RSA_WITH_RC4_128_SHA";
        final String truststoreType = "JKS";

        final String s =
            runCLICommand(
                pwReader,
                new String[] {"config", "create",
                              "-root",
                              testRoot.getPath(),
                              "-param",
                              (ParameterState.SEC_TRUSTSTORE_TYPE + "=" +
                               truststoreType),
                              "-param",
                              (ParameterState.SECURITY_TRANSPORT_JE_HA + ":" +
                               ParameterState.SEC_TRANS_ALLOW_PROTOCOLS +
                               "=" + jeProtocols),
                              "-param",
                              (ParameterState.SEC_TRANS_ALLOW_CIPHER_SUITES +
                               "=" + cipherSuites),
                              "-kspwd",
                              passphrase});
        assertTrue(s.indexOf("Created") != -1);
        assertWalletFilesExist(testRoot.getPath(), DEFAULT_SECDIR);

        final SecurityParams sp =
            ConfigUtils.getSecurityParams(
                new File(new File(testRoot, DEFAULT_SECDIR), SEC_XML),
                Logger.getLogger("test"));

        /* truststoreType should be set */
        assertEquals(sp.getTruststoreType(), truststoreType);

        /* allowedProtocols should be applied to ha */
        assertEquals(sp.getTransAllowProtocols(
                         ParameterState.SECURITY_TRANSPORT_JE_HA),
                     jeProtocols);
        /* cipherSuites should be applied to ha, client, internal */
        assertEquals(sp.getTransAllowCipherSuites(
                         ParameterState.SECURITY_TRANSPORT_JE_HA),
                     cipherSuites);
        assertEquals(sp.getTransAllowCipherSuites(
                         ParameterState.SECURITY_TRANSPORT_CLIENT),
                     cipherSuites);
        assertEquals(sp.getTransAllowCipherSuites(
                         ParameterState.SECURITY_TRANSPORT_INTERNAL),
                     cipherSuites);
    }

    @Test
    public void testCreateWithoutKadmin()
        throws Exception {

        final File testkrbDir = TestUtils.getKrbTestDir();
        TestUtils.copyFile(KRB_CONF_FILE, testkrbDir, testRoot);
        final File krbConf = new File(testRoot, KRB_CONF_FILE);

        pwReader.setPassword(null);
        String s = runCLICommand(
            pwReader,
            "config", "create",
            "-root",
            testRoot.getPath(),
            "-kspwd",
            passphrase,
            "-external-auth",
            "kerberos",
            "-krb-conf",
            krbConf.getPath(),
            "-kadmin-path",
            "noNe",
            "-instance-name",
            "test.instance",
            "-param",
            "krbServiceName=test.service");
        assertTrue(s.indexOf("Created") != -1);
        assertTrue(s.indexOf("not creating a keytab") != -1);

        final SecurityParams sp = ConfigUtils.getSecurityParams(
            new File(new File(testRoot, DEFAULT_SECDIR), SEC_XML),
            Logger.getLogger("test"));
        assertEquals(sp.getKerberosInstanceName(), "test.instance");
        assertEquals(sp.getKerberosServiceName(), "test.service");
    }

    /**
     * Test SecurityConfigCreator.checkSecurityDirectory.
     */
    @Test
    public void testCheckSecurityDirectory() throws Exception {
        final File securityDir = new File(testRoot, SECURITY_CONFIG_DIR);
        final File updatesDir = new File(securityDir, SECURITY_UPDATES_DIR);
        tearDowns.add(() -> removeDirectory(securityDir, 1000));
        assertFalse(securityDir.exists());
        assertFalse(updatesDir.exists());

        assertEquals(null, checkSecurityDirectory(securityDir));
        assertEquals(1, securityDir.list().length);
        assertTrue(updatesDir.isDirectory());

        assertEquals(null, checkSecurityDirectory(securityDir));
        assertEquals(1, securityDir.list().length);
        assertTrue(updatesDir.isDirectory());

        updatesDir.setReadable(false);
        try {
            assertMatch("Problem reading the " + securityDir +
                        " directory: .* " + updatesDir,
                        checkSecurityDirectory(securityDir));
        } finally {
            updatesDir.setReadable(true);
        }

        updatesDir.delete();
        securityDir.setWritable(false);
        try {
            assertMatch("Unable to create directories: .* " + updatesDir,
                        checkSecurityDirectory(securityDir));
        } finally {
            securityDir.setWritable(true);
        }

        updatesDir.mkdir();
        final File updatesFile = new File(updatesDir, "a");
        Files.createFile(updatesFile.toPath());
        assertMatch("The directory " + securityDir + " exists and contains" +
                    " files other than the updates directory",
                    checkSecurityDirectory(securityDir));
    }
}
