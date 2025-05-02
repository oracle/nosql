/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Enumeration;

import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.security.util.SecurityUtils;

import org.junit.Test;

/**
 * Test the securityconfig merge-trust command-line interface.
 */
public class SecurityConfigMergeTrustTest extends SecurityConfigTestBase {

    private File testBase = null;
    private File testRoot1 = null;
    private File testJksRoot1 = null;
    private File testJksRoot2 = null;
    private File testRoot2 = null;
    private final String existingFile = "aFile";
    private final String nonExistingFile = "notAFile";
    private final String securityConfig = "security";

    TestPasswordReader pwReader = new TestPasswordReader(null);

    @Override
    public void setUp() throws Exception {

        super.setUp();
        testBase = new File(TestUtils.getTestDir(), "testbase");
        removeDirectory(testBase, 50);
        testBase.mkdir();
        new File(testBase, existingFile).createNewFile();

        testRoot1 = new File(testBase, "testroot1");
        testRoot1.mkdir();
        new File(testRoot1, existingFile).createNewFile();

        testJksRoot1 = new File(testBase, "testJksRoot1");
        testJksRoot1.mkdir();
        new File(testJksRoot1, existingFile).createNewFile();

        testRoot2 = new File(testBase, "testroot2");
        testRoot2.mkdir();
        new File(testRoot2, existingFile).createNewFile();

        testJksRoot2 = new File(testBase, "testJksRoot2");
        testJksRoot2.mkdir();
        new File(testJksRoot2, existingFile).createNewFile();

        makeSecurityConfig(testRoot1, new File(securityConfig));
        makeSecurityConfig(testRoot2, new File(securityConfig));

        makeJKSSecurityConfig(testJksRoot1, new File(securityConfig));
        makeJKSSecurityConfig(testJksRoot2, new File(securityConfig));
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
        removeDirectory(testBase, 50);
    }

    /**
     * Test merging with error conditions.
     */
    @Test
    public void testWithErrors()
        throws Exception {

        String s;

        /* Auto-login wallets need no passphrase */
        pwReader.setPassword(null);

        /**
         * missing -root
         */
        s = runCLICommand(pwReader,
                          new String[] {
                              "config", "merge-trust",
                              "-source-root",
                              new File(testRoot1, securityConfig).getPath()});

        assertTrue(s.indexOf("Missing required argument (-root)") != -1);

        /**
         * missing -source-root
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "merge-trust",
                                        "-root",
                                        testRoot1.getPath()});

        assertTrue(s.indexOf("Missing required argument (-source-root)") != -1);

        /**
         * non-existent -root
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "merge-trust",
                                        "-root",
                                        "/nonexistent/directory",
                                        "-source-root",
                                        testRoot1.getPath()});

        assertTrue(s.indexOf("The -root argument") != -1);
        assertTrue(s.indexOf("does not exist") != -1);

        /**
         * non-directory -root
         */
        s = runCLICommand(pwReader,
                          new String[] {
                              "config", "merge-trust",
                              "-root",
                              new File(testRoot1, existingFile).getPath(),
                              "-source-root",
                              testRoot2.getPath()});

        assertTrue(s.indexOf("The -root argument") != -1);
        assertTrue(s.indexOf("is not a directory") != -1);

        /**
         * secdir not a relative path name
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "merge-trust",
                                        "-root",
                                        testRoot1.getPath(),
                                        "-secdir",
                                        new File(testRoot1, securityConfig).
                                        getAbsolutePath(),
                                        "-source-root",
                                        testRoot2.getPath()});
        assertTrue(s.indexOf("The -secdir argument") != -1);
        assertTrue(s.indexOf("must be a relative file name") != -1);

        /**
         * secdir does not exist
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "merge-trust",
                                        "-root",
                                        testRoot1.getPath(),
                                        "-secdir",
                                        nonExistingFile,
                                        "-source-root",
                                        testRoot2.getPath()});
        assertTrue(s.indexOf("does not exist in") != -1);

        /**
         * secdir not a directory
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "merge-trust",
                                        "-root",
                                        testRoot1.getPath(),
                                        "-secdir",
                                        existingFile,
                                        "-source-root",
                                        testRoot2.getPath()});
        assertTrue(s.indexOf("is not a directory") != -1);


        /**
         * non-existent -source-root
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "merge-trust",
                                        "-source-root",
                                        "/nonexistent/directory",
                                        "-root",
                                        testRoot2.getPath()});

        assertTrue(s.indexOf("The -source-root argument") != -1);
        assertTrue(s.indexOf("does not exist") != -1);


        /**
         * non-existent -source-root
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "merge-trust",
                                        "-source-root",
                                        "/nonexistent/directory",
                                        "-root",
                                        testRoot1.getPath()});

        assertTrue(s.indexOf("The -source-root argument") != -1);
        assertTrue(s.indexOf("does not exist") != -1);

        /**
         * non-directory -source-root
         */
        s = runCLICommand(pwReader,
                          new String[] {
                              "config", "merge-trust",
                              "-source-root",
                              new File(testRoot1, existingFile).getPath(),
                              "-root",
                              testRoot2.getPath()});

        assertTrue(s.indexOf("The -source-root argument") != -1);
        assertTrue(s.indexOf("is not a directory") != -1);

        /**
         * source-secdir not a relative path name
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "merge-trust",
                                        "-source-root",
                                        testRoot1.getPath(),
                                        "-source-secdir",
                                        new File(testRoot1, securityConfig).
                                        getAbsolutePath(),
                                        "-root",
                                        testRoot2.getPath()});
        assertTrue(s.indexOf("The -source-secdir argument") != -1);
        assertTrue(s.indexOf("must be a relative file name") != -1);

        /**
         * source-secdir does not exist
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "merge-trust",
                                        "-source-root",
                                        testRoot1.getPath(),
                                        "-source-secdir",
                                        nonExistingFile,
                                        "-root",
                                        testRoot2.getPath()});
        assertTrue(s.indexOf("does not exist in") != -1);

        /**
         * source-secdir not a directory
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "merge-trust",
                                        "-source-root",
                                        testRoot1.getPath(),
                                        "-source-secdir",
                                        existingFile,
                                        "-root",
                                        testRoot2.getPath()});
        assertTrue(s.indexOf("is not a directory") != -1);
    }

    /**
     * Test merge operation.
     */
    @Test
    public void testMerge()
        throws Exception {

        String s;

        /* Auto-login wallets need no passphrase */
        pwReader.setPassword(null);

        /*
         * merge trust using explicit args from two configurations
         * with PKCS12 KeyStore
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "merge-trust",
                                        "-root",
                                        testRoot1.getPath(),
                                        "-secdir",
                                        securityConfig,
                                        "-source-root",
                                        testRoot2.getPath(),
                                        "-source-secdir",
                                        securityConfig});
        assertTrue(s.indexOf("Configuration updated") != -1);

        Enumeration<String> entries =
            SecurityUtils.listKeystore(
                new File(new File(testRoot1, securityConfig), SEC_TRUST),
                passphrase);
        assertEquals(2, count(entries));

        /*
         * merge trust using explicit args from two configurations
         * with JKS KeyStore
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "merge-trust",
                                        "-root",
                                        testJksRoot1.getPath(),
                                        "-secdir",
                                        securityConfig,
                                        "-source-root",
                                        testJksRoot2.getPath(),
                                        "-source-secdir",
                                        securityConfig});
        assertTrue(s.indexOf("Configuration updated") != -1);

        File secDir = new File(testJksRoot1, securityConfig);
        File storeTrust = new File(secDir, SEC_TRUST);
        entries = SecurityUtils.listKeystore(storeTrust, passphrase);
        assertEquals(2, count(entries));

        /* after merge KeyStore type should still be JKS */
        assertKeyStoreType("JKS", storeTrust.getPath(), passphrase);
        SecurityParams params = SecurityUtils.loadSecurityParams(secDir);
        assertTrue(SecurityUtils.isJKS(params.getTruststoreType()));
        assertTrue(SecurityUtils.isJKS(params.getKeystoreType()));

        /*
         * merge trust using explicit args from a configuration with JKS
         * KeyStore to a configuration with PKCS12 KeyStore, which should
         * not be allowed
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "merge-trust",
                                        "-root",
                                        testRoot2.getPath(),
                                        "-secdir",
                                        securityConfig,
                                        "-source-root",
                                        testJksRoot2.getPath(),
                                        "-source-secdir",
                                        securityConfig});
        assertTrue(s.indexOf("config merge-trust failed") != -1);

        /*
         * merge trust using explicit args from a configuration with PKCS12
         * KeyStore to a configuration with JKS KeyStore
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "merge-trust",
                                        "-root",
                                        testJksRoot2.getPath(),
                                        "-secdir",
                                        securityConfig,
                                        "-source-root",
                                        testRoot2.getPath(),
                                        "-source-secdir",
                                        securityConfig});
        assertTrue(s.indexOf("Configuration updated") != -1);

        /* server key and trust store should be converted to PKCS12 */
        secDir = new File(testRoot1, securityConfig);
        storeTrust = new File(secDir, SEC_TRUST);
        entries = SecurityUtils.listKeystore(storeTrust, passphrase);
        assertEquals(2, count(entries));
        File storeKeys = new File(secDir, SEC_KEYS);
        entries = SecurityUtils.listKeystore(storeKeys, passphrase);

        /* after merge KeyStore type should be converted to PKCS12 */
        assertKeyStoreType("PKCS12", storeKeys.getPath(), passphrase);
        assertKeyStoreType("PKCS12", storeTrust.getPath(), passphrase);
        params = SecurityUtils.loadSecurityParams(secDir);
        assertTrue(SecurityUtils.isPKCS12(params.getTruststoreType()));
        assertTrue(SecurityUtils.isPKCS12(params.getKeystoreType()));
    }

    /**
     * Test merge operation with default security dirs.
     */
    @Test
    public void testMergeDefaultSecurity()
        throws Exception {

        String s;

        /* Auto-login wallets need no passphrase */
        pwReader.setPassword(null);

        /*
         * merge trust using defaultargs
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "merge-trust",
                                        "-root",
                                        testRoot1.getPath(),
                                        "-source-root",
                                        testRoot2.getPath()});
        assertTrue(s.indexOf("Configuration updated") != -1);

        Enumeration<String> entries =
            SecurityUtils.listKeystore(
                new File(new File(testRoot1, securityConfig), SEC_TRUST),
                passphrase);
        assertEquals(2, count(entries));

        /*
         * merge trust using explicit args from two configurations
         * with JKS KeyStore
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "merge-trust",
                                        "-root",
                                        testJksRoot1.getPath(),
                                        "-source-root",
                                        testJksRoot2.getPath()});
        assertTrue(s.indexOf("Configuration updated") != -1);

        File secDir = new File(testJksRoot1, securityConfig);
        File storeTrust = new File(secDir, SEC_TRUST);
        entries = SecurityUtils.listKeystore(storeTrust, passphrase);
        assertEquals(2, count(entries));

        /* after merge KeyStore type should still be JKS */
        assertKeyStoreType("JKS", storeTrust.getPath(), passphrase);
        SecurityParams params = SecurityUtils.loadSecurityParams(secDir);
        assertTrue(SecurityUtils.isJKS(params.getTruststoreType()));
        assertTrue(SecurityUtils.isJKS(params.getKeystoreType()));

        /*
         * merge trust using explicit args from a configuration with JKS
         * KeyStore to a configuration with PKCS12 KeyStore, which should
         * not be allowed
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "merge-trust",
                                        "-root",
                                        testRoot2.getPath(),
                                        "-source-root",
                                        testJksRoot2.getPath()});
        assertTrue(s.indexOf("config merge-trust failed") != -1);

        /*
         * merge trust using explicit args from a configuration with PKCS12
         * KeyStore to a configuration with JKS KeyStore
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "merge-trust",
                                        "-root",
                                        testJksRoot2.getPath(),
                                        "-source-root",
                                        testRoot2.getPath()});
        assertTrue(s.indexOf("Configuration updated") != -1);

        /* server trust store should be converted to PKCS12 */
        secDir = new File(testRoot1, securityConfig);
        storeTrust = new File(secDir, SEC_TRUST);
        entries = SecurityUtils.listKeystore(storeTrust, passphrase);
        assertEquals(2, count(entries));

        /* after merge KeyStore type should be converted to PKCS12 */
        assertKeyStoreType("PKCS12", storeTrust.getPath(), passphrase);
        params = SecurityUtils.loadSecurityParams(secDir);
        assertTrue(SecurityUtils.isPKCS12(params.getTruststoreType()));
        assertTrue(SecurityUtils.isPKCS12(params.getKeystoreType()));
    }
}
