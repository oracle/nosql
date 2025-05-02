/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static oracle.kv.impl.param.ParameterState.SECURITY_TRANSPORT_JE_HA;
import static oracle.kv.impl.util.SecurityConfigCommand.CONFIG_UPDATE_SUCCEED;
import static org.junit.Assert.assertTrue;

import java.io.File;

import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.util.SecurityUtils;

import org.junit.Test;

/**
 * Test the securityconfig verify command-line interface.
 */
public class SecurityConfigVerifyTest extends SecurityConfigTestBase {

    private File testRoot = null;

    /* security configuration using self-signed certificate */
    private String selfSignedConfigPath = null;

    /* security configuration using CA signed certificate */
    private String caSignedConfigPath = null;
    private String emptyConfigPath = null;
    private final String existingFile = "aFile";
    private final String selfSignedConfig = "securityx";
    private final String caSignedConfig = "caSecurity";
    private final String emptySecDir = "emptySecDir";

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

        /* Make a security configuration using self-signed certificate */
        makeSecurityConfig(testRoot, new File(selfSignedConfig));

        File predefinedConfig = new File(
            SSLTestUtils.getTestSSLDir(), SSLTestUtils.CA_SIGNED_CONFIG);
        TestUtils.copyDir(predefinedConfig, new File(testRoot, caSignedConfig));

        selfSignedConfigPath =
            testRoot.getAbsolutePath() + File.separator + selfSignedConfig;
        emptyConfigPath =
            testRoot.getAbsolutePath() + File.separator + emptySecDir;
        caSignedConfigPath =
            testRoot.getAbsolutePath() + File.separator + caSignedConfig;
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
    public void testVerifyWithErrors()
        throws Exception {

        String s;

        /* Auto-login wallets need no passphrase */
        pwReader.setPassword(null);

        /**
         * non-existent directory
         */
        s = runCLICommand(pwReader, "config", "verify", "-secdir",
                          "/nonexistent/directory");
        assertTrue(s.indexOf("does not exist") != -1);

        /**
         * not a directory
         */
        s = runCLICommand(pwReader, "config", "verify", "-secdir",
                          new File(testRoot, existingFile).getPath());
        assertTrue(s.indexOf("is not a directory") != -1);

        /**
         * security directory is empty
         */
        s = runCLICommand(pwReader, "config", "verify", "-secdir",
                          emptyConfigPath);
        assertTrue(s.indexOf("Security file not found in") != -1);
        SecurityParams secParams =
            SecurityUtils.loadSecurityParams(new File(selfSignedConfigPath));

        /**
         * Transports not using the preferred protocol
         */
        s = runCLICommand(pwReader, "config", "update", "-secdir",
                          selfSignedConfigPath, "-param",
                          SECURITY_TRANSPORT_JE_HA + ":" +
                          ParameterState.SEC_TRANS_ALLOW_PROTOCOLS +
                          "=" + "SSLV2");
        assertTrue(s.indexOf(CONFIG_UPDATE_SUCCEED) != -1);

        s = runCLICommand(pwReader, "config", "verify", "-secdir",
                          selfSignedConfigPath);
        assertTrue(s.indexOf("is not using preferred protocols") != -1);

        /* revert to original configuration */
        s = runCLICommand(pwReader, "config", "update", "-secdir",
                          selfSignedConfigPath, "-param",
                          SECURITY_TRANSPORT_JE_HA + ":" +
                          ParameterState.SEC_TRANS_ALLOW_PROTOCOLS +
                          "=" + secParams.getTransAllowProtocols(
                          SECURITY_TRANSPORT_JE_HA));
        assertTrue(s.indexOf(CONFIG_UPDATE_SUCCEED) != -1);

        /**
         * Transports not using the same private key
         */
        s = runCLICommand(pwReader, "config", "update", "-secdir",
                          selfSignedConfigPath, "-param",
                          SECURITY_TRANSPORT_JE_HA + ":" +
                          ParameterState.SEC_TRANS_SERVER_KEY_ALIAS +
                          "=" + "non-identity");
        assertTrue(s.indexOf(CONFIG_UPDATE_SUCCEED) != -1);
        s = runCLICommand(pwReader, "config", "verify", "-secdir",
                          selfSignedConfigPath);
        assertTrue(s.indexOf("not the same as") != -1);

        /* revert to original configuration */
        s = runCLICommand(pwReader, "config", "update", "-secdir",
                          selfSignedConfigPath, "-param",
                          SECURITY_TRANSPORT_JE_HA + ":" +
                          ParameterState.SEC_TRANS_SERVER_KEY_ALIAS +
                          "=" + secParams.getTransServerKeyAlias(
                          SECURITY_TRANSPORT_JE_HA));
        assertTrue(s.indexOf(CONFIG_UPDATE_SUCCEED) != -1);

        /**
         * Transports are configured different allowed identity
         */
        s = runCLICommand(pwReader, "config", "update", "-secdir",
                          selfSignedConfigPath, "-param",
                          SECURITY_TRANSPORT_JE_HA + ":" +
                          ParameterState.SEC_TRANS_SERVER_IDENT_ALLOW +
                          "=" + "invalid-identity");
        assertTrue(s.indexOf(CONFIG_UPDATE_SUCCEED) != -1);

        s = runCLICommand(pwReader, "config", "verify", "-secdir",
                          selfSignedConfigPath);
        assertTrue(s.indexOf("not the same as") != -1);

        /* revert to correct configuration */
        s = runCLICommand(pwReader, "config", "update", "-secdir",
                          selfSignedConfigPath, "-param",
                          SECURITY_TRANSPORT_JE_HA + ":" +
                          ParameterState.SEC_TRANS_SERVER_IDENT_ALLOW +
                          "=" + secParams.getTransServerIdentityAllowed(
                          SECURITY_TRANSPORT_JE_HA));
        assertTrue(s.indexOf(CONFIG_UPDATE_SUCCEED) != -1);

        /**
         * DN of certificate is not the allowed identity
         */
        s = runCLICommand(pwReader, "config", "update", "-secdir",
                          selfSignedConfigPath,
                          "-param",
                          SECURITY_TRANSPORT_JE_HA + ":" +
                          ParameterState.SEC_TRANS_SERVER_IDENT_ALLOW +
                          "=invalid-identity",
                          "-param",
                          SECURITY_TRANSPORT_JE_HA + ":" +
                          ParameterState.SEC_TRANS_CLIENT_IDENT_ALLOW +
                          "=invalid-identity",
                          "-param",
                          ParameterState.SECURITY_TRANSPORT_INTERNAL + ":" +
                          ParameterState.SEC_TRANS_SERVER_IDENT_ALLOW +
                          "=invalid-identity",
                          "-param",
                          ParameterState.SECURITY_TRANSPORT_INTERNAL + ":" +
                          ParameterState.SEC_TRANS_CLIENT_IDENT_ALLOW +
                          "=invalid-identity",
                          "-param",
                          ParameterState.SECURITY_TRANSPORT_CLIENT + ":" +
                          ParameterState.SEC_TRANS_SERVER_IDENT_ALLOW +
                          "=invalid-identity");
        assertTrue(s.indexOf(CONFIG_UPDATE_SUCCEED) != -1);

        s = runCLICommand(pwReader, "config", "verify", "-secdir",
                          selfSignedConfigPath);
        assertTrue(s.indexOf("does not match") != -1);
    }

    /**
     * Test performing verify command against a security configuration using
     * CA signed certificate that is located at test/ssl/ca-signed-config.
     */
    @Test
    public void testVerifyCASignedConfig()
        throws Exception {

        String s;

        /* Auto-login wallets need no passphrase */
        pwReader.setPassword(null);

        s = runCLICommand(pwReader, "config", "verify", "-secdir",
                          caSignedConfigPath);
        assertTrue(s.indexOf("verification passed") != -1);
    }

    /**
     * Test performing verify against a security configuration using CA signed
     * certificate, which has a keystore imported via openssl.
     */
    @Test
    public void testVerifyOpensslKeyStore()
        throws Exception {

        File caSignedConfigDir = new File(caSignedConfigPath);
        assertTrue(caSignedConfigDir.exists());
        TestUtils.copyFile(SSLTestUtils.OPENSSL_KS_NAME,
                           caSignedConfigDir,
                           SSLTestUtils.SSL_KS_NAME,
                           caSignedConfigDir);
        String s;

        /* Auto-login wallets need no passphrase */
        pwReader.setPassword(null);

        s = runCLICommand(pwReader, "config", "verify", "-secdir",
                          caSignedConfigPath);
        assertTrue(s.indexOf("verification passed") != -1);
    }

    /**
     * Test performing verify against a security configuration using CA signed
     * certificate, which has incorrect key store installed.
     */
    @Test
    public void testVerifyErrorKeyStore()
        throws Exception {

        /*
         * Replace with the error key store that does not have root and
         * intermediate certificate, the verification will fail because
         * cannot establish valid certificate path
         */
        File caSignedConfigDir = new File(caSignedConfigPath);
        assertTrue(caSignedConfigDir.exists());
        TestUtils.copyFile(SSLTestUtils.ERROR_KS_NAME,
                           caSignedConfigDir,
                           SSLTestUtils.SSL_KS_NAME,
                           caSignedConfigDir);
        String s;

        /* Auto-login wallets need no passphrase */
        pwReader.setPassword(null);

        /**
         * Keystore cert chain doesn't verify
         */
        s = runCLICommand(pwReader, "config", "verify", "-secdir",
                          caSignedConfigPath);
        assertTrue(s.indexOf("verification failed") != -1);
        assertTrue(s.indexOf("Problem with verifying certificate chain") != -1);
    }

    /**
     * Test performing verify against a security configuration using CA signed
     * certificate, which has incorrect trust store installed.
     */
    @Test
    public void testVerifyErrorTrustStore()
        throws Exception {
        /*
         * Replace with the error trust store that does not have signed store
         * certificate, the verification will fail because cannot located the
         * CA signed store certificate.
         */
        File caSignedConfigDir = new File(caSignedConfigPath);
        assertTrue(caSignedConfigDir.exists());
        TestUtils.copyFile(SSLTestUtils.ERROR_TS_NAME,
                           caSignedConfigDir,
                           SSLTestUtils.SSL_TS_NAME,
                           caSignedConfigDir);
        String s;

        /* Auto-login wallets need no passphrase */
        pwReader.setPassword(null);

        /**
         * Truststore doesn't have parent certificate
         */
        s = runCLICommand(pwReader, "config", "verify", "-secdir",
                          caSignedConfigPath);
        assertTrue(s.indexOf("verification failed") != -1);
        assertTrue(s.indexOf("Certificate not found") != -1);
    }
}
