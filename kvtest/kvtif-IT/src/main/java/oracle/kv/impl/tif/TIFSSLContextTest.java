/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.tif;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.security.ssl.KeyStorePasswordSource;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.tif.esclient.security.TIFSSLContext;
import oracle.kv.impl.util.SecurityConfigTestBase;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.VersionUtil;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class TIFSSLContextTest extends TestBase {

    private static final String PKCS12 = "pkcs12security";
    private static final String JKS = "jkssecurity";
    private File testRoot = null;

    @BeforeClass
    public static void staticSetUp() throws Exception {
         Assume.assumeTrue(
            "FTS is currently incompatible with Java Versons < 11 ",
            VersionUtil.getJavaMajorVersion() >= 11);
    }

    @Override
    public void setUp()
        throws Exception {

        super.setUp();

        /* make unit test output clean */
        suppressSystemOut();
        suppressSystemError();

        testRoot = new File(TestUtils.getTestDir(), "testroot");
        testRoot.mkdir();
        SecurityConfigTestBase.makeSecurityConfig(testRoot, new File(PKCS12));
        SecurityConfigTestBase.makeJKSSecurityConfig(testRoot, new File(JKS));
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        resetSystemOut();
        resetSystemError();
    }

    private static void createFTSKey(File secDir)
        throws Exception {

        SecurityParams sp = SecurityUtils.loadSecurityParams(secDir);
        KeyStorePasswordSource pwdSrc = KeyStorePasswordSource.create(sp);
        String kspwd = new String(pwdSrc.getPassword());
        String keyStoreFile = new File(secDir.getPath(),
            sp.getKeystoreFile()).getPath();
        String[] genkeypair = new String[] {
                "keytool",
                "-genkeypair",
                "-keystore", keyStoreFile,
                "-storetype", sp.getKeystoreType(),
                "-storepass", kspwd,
                "-keypass", kspwd,
                "-alias", "FTS",
                "-dname", "CN=FTSTest",
                "-keyAlg", "RSA",
                "-keysize", "2048",
                "-validity", "365" };

        List<String> output = new ArrayList<>();
        int code = SecurityUtils.runCmd(genkeypair, output);
        assertEquals(code, 0);
    }

    @Test
    public void testBuildSSLContext()
        throws Exception {

        File pkcs12 = new File(testRoot, PKCS12);
        File jks = new File(testRoot, JKS);
        createFTSKey(pkcs12);
        createFTSKey(jks);

        SecurityParams secParams = SecurityUtils.loadSecurityParams(jks);
        AtomicReference<char[]> ksPwdAR = new AtomicReference<char[]>();
        assertNotNull(TIFSSLContext.makeSSLContext(secParams, ksPwdAR, logger));

        secParams = SecurityUtils.loadSecurityParams(pkcs12);
        assertNotNull(TIFSSLContext.makeSSLContext(secParams, ksPwdAR, logger));
    }
}
