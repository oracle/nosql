/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.security.util.SecurityUtils;

/**
 * Common utilities for the securityconfig command-line interfaces.
 */
public class SecurityConfigTestBase extends SecurityShellTestBase {

    static final String DEFAULT_SECDIR = "security";
    static final String ALT_SECDIR = "alt-security";
    static final String SEC_XML = "security.xml";
    static final String SEC_KEYS = "store.keys";
    static final String SEC_TRUST = "store.trust";
    static final String CLIENT_TRUST = "client.trust";
    static final String WALLET_DIR = "store.wallet";
    static final String PWD_FILE = "store.passwd";

    static String passphrase = "jn34pr12";

    @Override
    public void setUp()
        throws Exception {

        super.setUp();

        /* make unit test output clean */
        suppressSystemOut();
        suppressSystemError();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        resetSystemOut();
        resetSystemError();
    }

    /**
     * @param target the file in which the boot config should be created
     * @param securityDir the security directory relative to the root
     */
    public void makeBootConfig(File target,
                               String securityDir) {
        final BootstrapParams bp = new BootstrapParams
            (null /*rootDir*/,
             "localhost", "localhost",
             "5010,5020", /* ha range */
             "5030,5040", /*service range */
             null /*storeName*/,
             5000, /* registry port */
             -1,
             0, /* capacity */
             null /*storageType*/,
             securityDir,
             false,
             null);

        ConfigUtils.createBootstrapConfig(bp, target);
    }

    /**
     * @param target the directory  in which the security config should be
     * created.
     */
    public static void makeSecurityConfig(File root, File target)
        throws Exception {

        makeSecurityConfig(root, target, "pkcs12");
    }

    public static void makeJKSSecurityConfig(File root, File target)
        throws Exception {

        makeSecurityConfig(root, target, "jks");
    }

    protected static void assertKeyStoreType(String storeType,
                                             String storePath,
                                             String storePassword)
        throws Exception {

        /*
         * Test is compiled with Java 8, which doesn't have an API
         * to find out the type of KeyStore. 'keytool -list' print
         * store type, use the output to check type as a workaround.
         */
        List<String> listStoreCmd = new ArrayList<>();
        listStoreCmd.add("keytool");
        listStoreCmd.add("-list");
        listStoreCmd.add("-keystore");
        listStoreCmd.add(storePath);

        if (storePassword != null) {
            listStoreCmd.add("-storepass");
            listStoreCmd.add(storePassword);
        }
        final List<String> output = new ArrayList<String>();
        SecurityUtils.runCmd(listStoreCmd.toArray(new String[0]), output);

        boolean match = false;
        for (String s : output) {
            if (s.toLowerCase().contains(storeType.toLowerCase())) {
                match = true;
            }
        }
        assertTrue(match);
    }

    protected static int count(Enumeration<String> entries) {
        int count = 0;
        while (entries.hasMoreElements()) {
            entries.nextElement();
            count++;
        }
        return count;
    }

    private static void makeSecurityConfig(File root,
                                           File target,
                                           String storeType)
        throws Exception {

        final TestPasswordReader pwReader = new TestPasswordReader(null);

        final String s =
            runCLICommand(pwReader,
                          new String[] {"config", "create",
                                        "-root",
                                        root.getPath(),
                                        "-secdir",
                                        target.getPath(),
                                        "-pwdmgr",
                                        "pwdfile",
                                        "-kstype",
                                        storeType,
                                        "-kspwd",
                                        passphrase});
        assertTrue(s.indexOf("Created") != -1);
    }

    void assertWalletFilesExist(String rootPath, String secDirPath) {
        final File secDir = new File(rootPath, secDirPath);
        assertTrue(secDir.exists());
        final File walletDir = new File(secDir, WALLET_DIR);
        assertTrue(walletDir.exists());
        assertCommonFilesExist(secDir);
    }

    void assertPwdfileFilesExist(String rootPath, String secDirPath) {
        final File secDir = new File(rootPath, secDirPath);
        assertTrue(secDir.exists());
        final File pwdFile = new File(secDir, PWD_FILE);
        assertTrue(pwdFile.exists());
        assertCommonFilesExist(secDir);
    }

    private void assertCommonFilesExist(File secDir) {
        final File secFile = new File(secDir, SEC_XML);
        assertTrue(secFile.exists());
        final File storeTrust = new File(secDir, SEC_TRUST);
        assertTrue(storeTrust.exists());
        final File storeKeys = new File(secDir, SEC_KEYS);
        assertTrue(storeKeys.exists());
    }
}
