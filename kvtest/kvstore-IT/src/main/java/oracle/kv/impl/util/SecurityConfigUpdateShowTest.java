/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static oracle.kv.impl.util.SecurityConfigCommand.CONFIG_UPDATE_SUCCEED;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.Enumeration;

import org.junit.Test;

import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.util.SecurityUtils;

/**
 * Test the securityconfig update and show command-line interface.
 */
public class SecurityConfigUpdateShowTest extends SecurityConfigTestBase {

    private File testRoot = null;
    private String securityConfigPath = null;
    private String jksConfigPath = null;
    private String emptyConfigPath = null;
    private final String existingFile = "aFile";
    private final String securityConfig = "securityx";
    private final String emptySecDir = "emptySecDir";
    private final String jksSecurityConfig = "jkssecurity";

    TestPasswordReader pwReader = new TestPasswordReader(null);

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        testRoot = new File(TestUtils.getTestDir(), "testroot");
        removeDirectory(testRoot, 22);
        testRoot.mkdir();

        new File(testRoot, existingFile).createNewFile();
        new File(testRoot, emptySecDir).mkdir();
        makeSecurityConfig(testRoot, new File(securityConfig));
        securityConfigPath =
            testRoot.getAbsolutePath() + File.separator + securityConfig;
        emptyConfigPath =
            testRoot.getAbsolutePath() + File.separator + emptySecDir;
        makeJKSSecurityConfig(testRoot, new File(jksSecurityConfig));
        jksConfigPath =
            testRoot.getAbsolutePath() + File.separator + jksSecurityConfig;
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
        removeDirectory(testRoot, 22);
    }

    /**
     * Test error conditions.
     */
    @Test
    public void testUpdateWithErrors()
        throws Exception {

        String s;

        /* Auto-login wallets need no passphrase */
        pwReader.setPassword(null);

        /**
         * non-existent directory
         */
        s = runCLICommand(pwReader,
                          "config", "update", "-secdir",
                          "/nonexistent/directory");
        assertTrue(s.indexOf("does not exist") != -1);

        /**
         * not a directory
         */
        s = runCLICommand(pwReader, "config", "update", "-secdir",
                          new File(testRoot, existingFile).getPath());
        assertTrue(s.indexOf("is not a directory") != -1);

        /**
         * security directory is empty
         */
        s = runCLICommand(pwReader, "config", "update", "-secdir",
                          emptyConfigPath);
        assertTrue(s.indexOf("Security file not found in") != -1);

        /**
         * Invalid security parameter
         */
        s = runCLICommand(pwReader, "config", "update", "-secdir",
                          securityConfigPath, "-param",
                          /* Specify a valid parameter but not security one */
                          "hideUserData=false");
        assertTrue(s.indexOf("not a valid parameter for a security") != -1);

        /**
         * Invalid transport security parameter
         */
        s = runCLICommand(pwReader, "config", "update", "-secdir",
                          securityConfigPath, "-param",
                          /*
                           * Specify a valid security parameter but not a
                           * transport one
                           */
                          ParameterState.SECURITY_TRANSPORT_JE_HA + ":" +
                          ParameterState.SEC_KERBEROS_CONFIG_FILE +
                          "=krb.config");
        assertTrue(s.indexOf("is not a transport parameter") != -1);

        /**
         * Invalid transport name
         */
        s = runCLICommand(pwReader, "config", "update", "-secdir",
                          securityConfigPath, "-param",
                          "invalid-transport:" +
                          ParameterState.SEC_TRANS_CLIENT_IDENT_ALLOW +
                          "=shared");
        assertTrue(s.indexOf("is not a valid transport name") != -1);

        /**
         * Invalid KeyStore type
         */
        s = runCLICommand(pwReader, "config", "update", "-secdir",
                          securityConfigPath, "-kstype", "XXX");
        assertTrue(s.indexOf("not a valid KeyStore type") != -1);

        /**
         * Not support update to JKS KeyStore type
         */
        s = runCLICommand(pwReader, "config", "update", "-secdir",
                          securityConfigPath, "-kstype", "JKS");
        assertTrue(s.indexOf("Unable to update Java KeyStore to") != -1);
    }

    @Test
    public void testUpdateParams()
        throws Exception {

        String s;

        /* Auto-login wallets need no passphrase */
        pwReader.setPassword(null);

        String internalAuth = "clear";
        s = runCLICommand(pwReader, "config", "update", "-secdir",
                          securityConfigPath, "-param",
                          ParameterState.SEC_INTERNAL_AUTH + "=clear");
        assertTrue(s.indexOf(CONFIG_UPDATE_SUCCEED) != -1);
        SecurityParams secParams =
            SecurityUtils.loadSecurityParams(new File(securityConfigPath));
        assertEquals(internalAuth, secParams.getInternalAuth());

        String identityAllowed = "CN=Nosql, O=Oracle";
        s = runCLICommand(pwReader, "config", "update", "-secdir",
                          securityConfigPath, "-param",
                          ParameterState.SECURITY_TRANSPORT_JE_HA + ":" +
                          ParameterState.SEC_TRANS_CLIENT_IDENT_ALLOW +
                          "=" + identityAllowed);
        assertTrue(s.indexOf(CONFIG_UPDATE_SUCCEED) != -1);
        secParams =
            SecurityUtils.loadSecurityParams(new File(securityConfigPath));
        assertEquals(identityAllowed, secParams.getTransClientIdentityAllowed(
            ParameterState.SECURITY_TRANSPORT_JE_HA));

        /**
         * Test basic show
         */
        s = runCLICommand(pwReader, "config", "show", "-secdir",
                          securityConfigPath);

        /* Check if output configured security parameters */
        assertTrue(s.indexOf(identityAllowed) != -1);
        assertTrue(s.indexOf(internalAuth) != -1);

        /* Check if output keystore information */
        assertTrue(s.indexOf("Keystore type: PKCS12") != -1);

        /* Provider name of Java 11 is SUN, but it's SunJSSE for Java 8 */
        assertTrue(s.indexOf("Keystore provider: SUN") != -1 ||
                   s.indexOf("Keystore provider: SunJSSE") != -1);
        assertTrue(s.indexOf("PrivateKeyEntry") != -1);
    }

    @Test
    public void testUpdateKeyStoreTypeDefault()
        throws Exception {

        String s;

        /* Auto-login wallets need no passphrase */
        pwReader.setPassword(null);

        File secDir = new File(jksConfigPath);
        File storeTrust = new File(secDir, SEC_TRUST);
        File storeKey = new File(secDir, SEC_KEYS);
        File clientTrust = new File(secDir, CLIENT_TRUST);

        Enumeration<String> ksEntries =
            SecurityUtils.listKeystore(storeKey, passphrase);
        Enumeration<String> tsEntries =
            SecurityUtils.listKeystore(storeTrust, passphrase);
        Enumeration<String> ctsEntries =
            SecurityUtils.listKeystore(clientTrust, passphrase);
        assertNotNull(ksEntries);
        assertEquals(1, count(ksEntries));
        assertNotNull(tsEntries);
        assertEquals(1, count(tsEntries));
        assertNotNull(ctsEntries);
        assertEquals(1, count(ctsEntries));

        /* update KeyStore type to PKCS12 */
        s = runCLICommand(pwReader, "config", "update", "-secdir",
                          jksConfigPath, "-kstype", "pkcs12");
        assertTrue(s.indexOf(CONFIG_UPDATE_SUCCEED) != -1);

        assertKeyStoreType("PKCS12", storeKey.getPath(), passphrase);
        assertKeyStoreType("PKCS12", storeTrust.getPath(), passphrase);

        SecurityParams params = SecurityUtils.loadSecurityParams(secDir);
        assertTrue(SecurityUtils.isPKCS12(params.getTruststoreType()));
        assertTrue(SecurityUtils.isPKCS12(params.getKeystoreType()));

        Enumeration<String> newKsEntries =
            SecurityUtils.listKeystore(storeKey, passphrase);
        Enumeration<String> newTsEntries =
            SecurityUtils.listKeystore(storeTrust, passphrase);
        assertNotNull(newKsEntries);
        assertNotNull(newTsEntries);
        assertEquals(1, count(newKsEntries));
        assertEquals(1, count(newTsEntries));

        ksEntries.asIterator().forEachRemaining(oldEntry -> {
            newKsEntries.asIterator().forEachRemaining(newEntry -> {
                assertEquals(oldEntry, newEntry);
            });
        });
        tsEntries.asIterator().forEachRemaining(oldEntry -> {
            newTsEntries.asIterator().forEachRemaining(newEntry -> {
                assertEquals(oldEntry, newEntry);
            });
        });

        /*
         * Not checking the type of client.trust, it may be created as a JKS or
         * password-less PKCS12 store, depending on the Java version, but the
         * entries can be listed without password in either case.
         */
        KeyStore ct = KeyStore.getInstance("JKS");
        try (FileInputStream fis = new FileInputStream(clientTrust)) {
            ct.load(fis, null);
        }
        assertTrue(ct.aliases().hasMoreElements());

        /* re-update should fail */
        s = runCLICommand(pwReader, "config", "update", "-secdir",
                          jksConfigPath, "-kstype", "pkcs12");
        assertTrue(s.indexOf("Java KeyStore type update failed") != -1);
    }

    @Test
    public void testUpdateKeyStoreTypeClientTrust()
        throws Exception {

        String s;

        /* Auto-login wallets need no passphrase */
        pwReader.setPassword(null);

        File secDir = new File(jksConfigPath);
        File clientTrust = new File(secDir, CLIENT_TRUST);
        Enumeration<String> ctsEntries =
            SecurityUtils.listKeystore(clientTrust, passphrase);
        assertNotNull(ctsEntries);
        assertEquals(1, count(ctsEntries));

        /* update to PKCS12, create a password-protected client.trust */
        s = runCLICommand(pwReader, "config", "update", "-secdir",
                          jksConfigPath, "-kstype", "pkcs12",
                          "-ctspwd", passphrase);
        assertTrue(s.indexOf(CONFIG_UPDATE_SUCCEED) != -1);

        assertKeyStoreType("PKCS12", clientTrust.getPath(), passphrase);
        Enumeration<String> newCtsEntries =
            SecurityUtils.listKeystore(clientTrust, passphrase);
        assertNotNull(newCtsEntries);
        assertEquals(1, count(newCtsEntries));

        ctsEntries.asIterator().forEachRemaining(oldEntry -> {
            newCtsEntries.asIterator().forEachRemaining(newEntry -> {
                assertEquals(oldEntry, newEntry);
            });
        });

        /* unable to load without password */
        KeyStore ct = KeyStore.getInstance("PKCS12");
        try (FileInputStream fis = new FileInputStream(clientTrust)) {
            ct.load(fis, null);
        }
        assertFalse(ct.aliases().hasMoreElements());
    }

    @Test
    public void testShowWithErrors()
        throws Exception {

        String s;

        /* Auto-login wallets need no passphrase */
        pwReader.setPassword(null);

        /**
         * non-existent directory
         */
        s = runCLICommand(pwReader, "config", "show", "-secdir",
                          "/nonexistent/directory");

        assertTrue(s.indexOf("does not exist") != -1);

        /**
         * not a directory
         */
        s = runCLICommand(pwReader, "config", "show", "-secdir",
                          new File(testRoot, existingFile).getPath());

        assertTrue(s.indexOf("is not a directory") != -1);

        /**
         * security directory is empty
         */
        s = runCLICommand(pwReader, "config", "show", "-secdir",
                          emptyConfigPath);
        assertTrue(s.indexOf("Security file not found in") != -1);
    }
}
