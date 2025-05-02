/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.ssl;

import static oracle.kv.impl.security.ssl.SSLConfig.ENABLED_CIPHER_SUITES;
import static oracle.kv.impl.security.ssl.SSLConfig.ENABLED_PROTOCOLS;
import static oracle.kv.impl.security.ssl.SSLConfig.KEYSTORE_ALIAS;
import static oracle.kv.impl.security.ssl.SSLConfig.KEYSTORE_FILE;
import static oracle.kv.impl.security.ssl.SSLConfig.KEYSTORE_TYPE;
import static oracle.kv.impl.security.ssl.SSLConfig.TRUSTSTORE_FILE;
import static oracle.kv.impl.security.ssl.SSLConfig.TRUSTSTORE_TYPE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.security.KeyStoreException;
import java.util.Properties;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.ssl.SSLConfig.InstanceInfo;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.util.SSLTestUtils;

import org.junit.Test;

import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.ReplicationSSLConfig;
import com.sleepycat.je.rep.net.SSLAuthenticator;

/**
 * Test some individual components SSL classes, including SSLConfig,
 * SSLTransport and SSLControl.
 */
public class SSLComponentTest extends TestBase {

    private static final String TRANSPORT = "trans";

    final File sslDir = SSLTestUtils.getTestSSLDir();

    /**
     * Test that property validation accepts valid properties.
     */
    @Test
    public void testSSLConfigValidateOK()
        throws Exception {

        final String propName = ENABLED_PROTOCOLS;
        final String propVal = "TLSv1";
        final Properties props = new Properties();
        props.setProperty(propName, propVal);

        final Properties newProps = SSLConfig.validateProperties(props);
        assertEquals(1, newProps.size());
        assertEquals(propVal, newProps.getProperty(propName));
    }

    /**
     * Test that property validation accepts empty properties.
     */
    @Test
    public void testSSLConfigValidateEmpty()
        throws Exception {

        final Properties props = new Properties();
        final Properties newProps = SSLConfig.validateProperties(props);
        assertEquals(0, newProps.size());
    }

    /**
     * Test that property validation skips non-ssl properties.
     */
    @Test
    public void testSSLConfigValidateNotSSL()
        throws Exception {

        final Properties props = new Properties();
        props.setProperty("oracle.kv.foo", "bar");

        final Properties newProps = SSLConfig.validateProperties(props);
        assertEquals(0, newProps.size());
    }

    /**
     * Test that property validation rejects faux-ssl properties.
     */
    @Test
    public void testSSLConfigValidateSSLNotValid()
        throws Exception {

        final Properties props = new Properties();
        props.setProperty("oracle.kv.ssl.foo", "bar");

        try {
            SSLConfig.validateProperties(props);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            /* expected */
        }
    }

    /**
     * Test that property validation rejects non-absolute truststore names.
     */
    @Test
    public void testSSLConfigValidateTrustStoreNotAbsolute()
        throws Exception {

        final Properties props = new Properties();
        props.setProperty(TRUSTSTORE_FILE, "store.trust");

        try {
            SSLConfig.validateProperties(props);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            /* expected */
            assertTrue(iae.getMessage().contains("an absolute pathname"));
        }
    }

    /**
     * Test that property validation accepts absolute truststore names.
     */
    @Test
    public void testSSLConfigValidateTrustStoreIsAbsolute()
        throws Exception {

        final String truststorePath =
            new File(sslDir.getPath(), SSLTestUtils.SSL_TS_NAME).getPath();

        final Properties props = new Properties();
        props.setProperty(TRUSTSTORE_FILE, truststorePath);

        final Properties newProps = SSLConfig.validateProperties(props);
        assertEquals(truststorePath,
                     newProps.getProperty(TRUSTSTORE_FILE));
    }

    /**
     * Test that SSLConfig setKeystorePassword manages the password properly.
     */
    @Test
    public void testSSLConfigSetKSPwd()
        throws Exception {

        final SSLConfig config = new SSLConfig(new Properties());

        /* test various code paths */

        /* set to null */
        config.setKeystorePassword(null);
        char[] kspwd = config.getKeystorePassword();
        assertNull(kspwd);

        /* set to non-null */
        config.setKeystorePassword("pwd".toCharArray());
        kspwd = config.getKeystorePassword();
        assertArrayEquals("pwd".toCharArray(), kspwd);

        /* set to new non-null */
        config.setKeystorePassword("pwd2".toCharArray());
        kspwd = config.getKeystorePassword();
        assertArrayEquals("pwd2".toCharArray(), kspwd);

        /* set to null */
        config.setKeystorePassword(null);
        kspwd = config.getKeystorePassword();
        assertNull(kspwd);
    }

    /*
     * The following set of tests of SSLConfig.makeControl() focus on exercising
     * conditional code paths around keystore, truststore, cipher suite and
     * protocol configuration options.
     */

    /**
     * Test that makeSSLControl with empty properties works
     */
    @Test
    public void testSSLConfigMakeControl()
        throws Exception {

        final Properties props = new Properties();
        final SSLConfig config = new SSLConfig(props);
        final SSLControl control = config.makeSSLControl(false, logger);

        /* Limited validation - we are primarily exercising code paths here */
        final SSLParameters sslParams = control.sslParameters();
        assertNotNull(sslParams);
        /*
         * For the client side cipher suites, a default preference list will
         * be generated.
         */
        assertNotNull(sslParams.getCipherSuites());
        assertNull(sslParams.getProtocols());
        assertNotNull(control.sslContext());
        assertNull(control.peerAuthenticator());
        assertNull(control.hostVerifier());
    }

    /**
     * Test that makeSSLControl with single valid cipher works.
     */
    @Test
    public void testSSLConfigMakeControlCipher1()
        throws Exception {

        final String cipherSuite = "TLS_AES_128_GCM_SHA256";
        final Properties props = new Properties();
        props.setProperty(ENABLED_CIPHER_SUITES, cipherSuite);
        final SSLConfig config = new SSLConfig(props);
        final SSLControl control = config.makeSSLControl(false, logger);

        /* Limited validation - we are primarily exercising code paths here */
        final SSLParameters sslParams = control.sslParameters();
        assertNotNull(sslParams);
        assertArrayEquals(new String[] { cipherSuite },
                          sslParams.getCipherSuites());
        assertNull(sslParams.getProtocols());
        assertNotNull(control.sslContext());
        assertNull(control.peerAuthenticator());
        assertNull(control.hostVerifier());
    }

    /**
     * Test that makeSSLControl with valid cipher plus bad, empty ciphers
     * works.
     */
    @Test
    public void testSSLConfigMakeControlCipher2()
        throws Exception {

        final String cipherSuite = "TLS_AES_128_GCM_SHA256";
        final Properties props = new Properties();
        props.setProperty(ENABLED_CIPHER_SUITES,
                          cipherSuite + ",,bad");
        final SSLConfig config = new SSLConfig(props);
        final SSLControl control = config.makeSSLControl(false, logger);

        /* Limited validation - we are primarily exercising code paths here */
        final SSLParameters sslParams = control.sslParameters();
        assertNotNull(sslParams);
        assertArrayEquals(new String[] { cipherSuite },
                          sslParams.getCipherSuites());
        assertNull(sslParams.getProtocols());
        assertNotNull(control.sslContext());
        assertNull(control.peerAuthenticator());
        assertNull(control.hostVerifier());
    }

    /**
     * Test that makeSSLControl rejects non-null ciphers setting when none
     * are valid.
     */
    @Test
    public void testSSLConfigMakeControlCipher3()
        throws Exception {

        final Properties props = new Properties();
        props.setProperty(ENABLED_CIPHER_SUITES, "bad");
        final SSLConfig config = new SSLConfig(props);
        try {
            config.makeSSLControl(false, logger);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("SSL cipher suites"));
        }
    }

    /**
     * Test that makeSSLControl with single valid protocol works.
     */
    @Test
    public void testSSLConfigMakeControlProtcol1()
        throws Exception {

        final String protocol = "TLSv1";
        final Properties props = new Properties();
        props.setProperty(ENABLED_PROTOCOLS, protocol);
        final SSLConfig config = new SSLConfig(props);
        final SSLControl control = config.makeSSLControl(false, logger);

        /* Limited validation - we are primarily exercising code paths here */
        final SSLParameters sslParams = control.sslParameters();
        assertNotNull(sslParams);
        /*
         * For the client side cipher suites, a default preference list will
         * be generated.
         */
        assertNotNull(sslParams.getCipherSuites());
        assertArrayEquals(new String[] { protocol }, sslParams.getProtocols());
        assertNotNull(control.sslContext());
        assertNull(control.peerAuthenticator());
        assertNull(control.hostVerifier());
    }

    /**
     * Test that makeSSLControl with valid protocol plus empty and invalid
     * protocols works.
     */
    @Test
    public void testSSLConfigMakeControlProtcol2()
        throws Exception {

        final String protocol = "TLSv1";
        final Properties props = new Properties();
        props.setProperty(ENABLED_PROTOCOLS, protocol + ",,bad");
        final SSLConfig config = new SSLConfig(props);
        final SSLControl control = config.makeSSLControl(false, logger);

        /* Limited validation - we are primarily exercising code paths here */
        final SSLParameters sslParams = control.sslParameters();
        assertNotNull(sslParams);
        /*
         * For the client side cipher suites, a default preference list will
         * be generated.
         */
        assertNotNull(sslParams.getCipherSuites());
        assertArrayEquals(new String[] { protocol }, sslParams.getProtocols());
        assertNotNull(control.sslContext());
        assertNull(control.peerAuthenticator());
        assertNull(control.hostVerifier());
    }

    /**
     * Test that makeSSLControl rejects non-empty protocols list when none
     * are valid.
     */
    @Test
    public void testSSLConfigMakeControlProtcol3()
        throws Exception {

        final Properties props = new Properties();
        props.setProperty(ENABLED_PROTOCOLS, "bad");
        final SSLConfig config = new SSLConfig(props);
        try {
            config.makeSSLControl(false, logger);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("SSL protocols"));
        }
    }

    /**
     * Test that makeSSLControl rejects a bad Keystore type.
     */
    @Test
    public void testSSLConfigMakeControlKsTypeBad()
        throws Exception {

        final Properties props = new Properties();
        props.setProperty(KEYSTORE_TYPE, "bad");
        props.setProperty(KEYSTORE_FILE,
                          new File(sslDir.getPath(),
                                   SSLTestUtils.SSL_KS_NAME).getPath());

        final SSLConfig config = new SSLConfig(props);
        config.setKeystorePassword(SSLTestUtils.SSL_KS_PWD_DEF.toCharArray());
        try {
            config.makeSSLControl(false, logger);
            fail("expected exception");
        } catch (KeyStoreException kse) {
            assertTrue(kse.getMessage().contains("bad not found"));
        }
    }

    /**
     * Test that makeSSLControl allows a valid Keystore type.
     */
    @Test
    public void testSSLConfigMakeControlKsTypeOK()
        throws Exception {

        final Properties props = new Properties();
        props.setProperty(KEYSTORE_TYPE, "JKS");
        props.setProperty(KEYSTORE_FILE,
                          new File(sslDir.getPath(),
                                   SSLTestUtils.SSL_KS_NAME).getPath());

        final SSLConfig config = new SSLConfig(props);
        config.setKeystorePassword(SSLTestUtils.SSL_KS_PWD_DEF.toCharArray());
        final SSLControl control = config.makeSSLControl(false, logger);

        /* Limited validation - we are primarily exercising code paths here */
        final SSLParameters sslParams = control.sslParameters();
        assertNotNull(sslParams);
        /*
         * For the client side cipher suites, a default preference list will
         * be generated.
         */
        assertNotNull(sslParams.getCipherSuites());
        assertNull(sslParams.getProtocols());
        assertNotNull(control.sslContext());
        assertNull(control.peerAuthenticator());
        assertNull(control.hostVerifier());
    }

    /**
     * Test that makeSSLControl allows a keystore without an alias.
     */
    @Test
    public void testSSLConfigMakeControlKsNoAlias()
        throws Exception {

        final Properties props = new Properties();
        props.setProperty(KEYSTORE_FILE,
                          new File(sslDir.getPath(),
                                   SSLTestUtils.SSL_KS_NAME).getPath());

        final SSLConfig config = new SSLConfig(props);
        config.setKeystorePassword(SSLTestUtils.SSL_KS_PWD_DEF.toCharArray());
        final SSLControl control = config.makeSSLControl(false, logger);

        /* Limited validation - we are primarily exercising code paths here */
        final SSLParameters sslParams = control.sslParameters();
        assertNotNull(sslParams);
        /*
         * For the client side cipher suites, a default preference list will
         * be generated.
         */
        assertNotNull(sslParams.getCipherSuites());
        assertNull(sslParams.getProtocols());
        assertNotNull(control.sslContext());
        assertNull(control.peerAuthenticator());
        assertNull(control.hostVerifier());
    }

    /**
     * Test that makeSSLControl rejects a keystore with an invalid alias.
     */
    @Test
    public void testSSLConfigMakeControlKsAliasBad()
        throws Exception {

        final Properties props = new Properties();
        props.setProperty(KEYSTORE_FILE,
                          new File(sslDir.getPath(),
                                   SSLTestUtils.SSL_KS_NAME).getPath());
        props.setProperty(KEYSTORE_ALIAS, "BadAlias");

        final SSLConfig config = new SSLConfig(props);
        config.setKeystorePassword(SSLTestUtils.SSL_KS_PWD_DEF.toCharArray());
        try {
            config.makeSSLControl(false, logger);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("BadAlias not found"));
        }
    }

    /**
     * Test that makeSSLControl rejects a truststore with a bad type.
     */
    @Test
    public void testSSLConfigMakeControlTsTypeBad()
        throws Exception {

        final Properties props = new Properties();
        props.setProperty(TRUSTSTORE_TYPE, "bad");
        props.setProperty(TRUSTSTORE_FILE,
                          new File(sslDir.getPath(),
                                   SSLTestUtils.SSL_TS_NAME).getPath());

        final SSLConfig config = new SSLConfig(props);
        try {
            config.makeSSLControl(false, logger);
            fail("expected exception");
        } catch (KeyStoreException kse) {
            assertTrue(kse.getMessage().contains("bad not found"));
        }
    }

    /**
     * Test that makeSSLControl allows a truststore with a valid type.
     */
    @Test
    public void testSSLConfigMakeControlTsTypeOK()
        throws Exception {

        final Properties props = new Properties();
        props.setProperty(TRUSTSTORE_TYPE, "JKS");
        props.setProperty(TRUSTSTORE_FILE,
                          new File(sslDir.getPath(),
                                   SSLTestUtils.SSL_TS_NAME).getPath());

        final SSLConfig config = new SSLConfig(props);
        final SSLControl control = config.makeSSLControl(false, logger);

        /* Limited validation - we are primarily exercising code paths here */
        final SSLParameters sslParams = control.sslParameters();
        assertNotNull(sslParams);
        /*
         * For the client side cipher suites, a default preference list will
         * be generated.
         */
        assertNotNull(sslParams.getCipherSuites());
        assertNull(sslParams.getProtocols());
        assertNotNull(control.sslContext());
        assertNull(control.peerAuthenticator());
        assertNull(control.hostVerifier());
    }

    /**
     * Test that makeAuthenticator handles dnmatch.
     */
    @Test
    public void testSSLConfigMakeAuthenticatorInfoDN()
        throws Exception {

        final InstanceInfo<SSLAuthenticator> iinfo =
            SSLConfig.makeAuthenticatorInfo("dnmatch(foo)");

        assertNotNull(iinfo);
        assertEquals(SSLConfig.JE_SSL_DN_AUTHENTICATOR_CLASS,
                     iinfo.jeImplClass);
        assertEquals("foo", iinfo.jeImplParams);
    }

    /**
     * Test that makeAuthenticator rejects invalid setting.
     */
    @Test
    public void testSSLConfigMakeAuthenticatorInfoFail()
        throws Exception {

        try {
            SSLConfig.makeAuthenticatorInfo("dndontmatch(foo)");
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(
                           "not a valid server peer constraint"));
        }
    }

    /**
     * Test that makeHostVerifier handles hostname.
     */
    @Test
    public void testSSLConfigMakeHostVerifierInfoHostname()
        throws Exception {

        final InstanceInfo<HostnameVerifier> iinfo =
            SSLConfig.makeHostVerifierInfo("hostname");

        assertNotNull(iinfo);
        assertNotNull(SSLConfig.JE_SSL_STD_HOST_VERIFIER_CLASS,
                      iinfo.jeImplClass);
        assertNull(iinfo.jeImplParams);
    }

    /**
     * Test that makeHostVerifier handles dnmatch.
     */
    @Test
    public void testSSLConfigMakeHostVerifierInfoDNMatch()
        throws Exception {

        final InstanceInfo<HostnameVerifier> iinfo =
            SSLConfig.makeHostVerifierInfo("dnmatch(foo)");

        assertNotNull(iinfo);
        assertEquals(SSLConfig.JE_SSL_DN_HOST_VERIFIER_CLASS,
                     iinfo.jeImplClass);
        assertEquals("foo", iinfo.jeImplParams);
    }

    /**
     * Test that makeHostVerifier rejects invalid setting.
     */
    @Test
    public void testSSLConfigMakeHostVerifierInfoFail()
        throws Exception {

        try {
            SSLConfig.makeHostVerifierInfo("dndontmatch(foo)");
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(
                           "not a valid client peer constraint"));
        }
    }

    @Test
    public void testProtocolsAndCipherSuites()
        throws Exception {

        /* Configure via SecurityParams */
        final SecurityParams sp = new SecurityParams();
        sp.setSecurityEnabled(true);
        sp.setKeystoreFile(new File(sslDir.getPath(),
                                    SSLTestUtils.SSL_KS_NAME).getPath());
        sp.setTruststoreFile(new File(sslDir.getPath(),
                                    SSLTestUtils.SSL_TS_NAME).getPath());
        sp.setPasswordFile(new File(sslDir.getPath(),
                                    SSLTestUtils.SSL_PW_NAME).getPath());
        sp.addTransportMap(TRANSPORT);
        sp.setTransServerKeyAlias(TRANSPORT, SSLTestUtils.SSL_KS_ALIAS_DEF);
        sp.setTransFactory(TRANSPORT, SSLTestUtils.SSL_TRANSPORT_FACTORY);
        final ParameterMap transMap = sp.getTransportMap(TRANSPORT);

        /* Create SSLControl objects from the SecurityParams config */
        final SSLTransport sslt = new SSLTransport();

        /* First, check the defaults are empty */

        Properties clntProps = sslt.getSSLProperties(sp, transMap,
                                                     false, /* resolveFiles */
                                                     false); /* isServer */
        Properties srvrProps = sslt.getSSLProperties(sp, transMap,
                                                     false, /* resolveFiles */
                                                     true); /* isServer */

        assertNull(clntProps.getProperty(ENABLED_CIPHER_SUITES));
        assertNull(srvrProps.getProperty(ENABLED_CIPHER_SUITES));

        /*
         * Note that we now set the enabled protocols to the defaults if
         * nothing is specified.
         */
        assertEquals(SecurityUtils.PREFERRED_PROTOCOLS_DEFAULT,
                     clntProps.getProperty(ENABLED_PROTOCOLS));
        assertEquals(SecurityUtils.PREFERRED_PROTOCOLS_DEFAULT,
                     srvrProps.getProperty(ENABLED_PROTOCOLS));

        /*
         * Next, check that setting the basic property affects server and
         * client.
         */

        final String stdCipherSuites = "SSL_RSA_WITH_RC4_128_SHA";
        final String stdProtocols = "SSLv3";
        sp.setTransAllowCipherSuites(TRANSPORT, stdCipherSuites);
        sp.setTransAllowProtocols(TRANSPORT, stdProtocols);

        clntProps = sslt.getSSLProperties(sp, transMap,
                                          false, /* resolveFiles */
                                          false); /* isServer */
        srvrProps = sslt.getSSLProperties(sp, transMap,
                                          false, /* resolveFiles */
                                          true); /* isServer */

        assertEquals(stdCipherSuites,
                     clntProps.getProperty(ENABLED_CIPHER_SUITES));
        assertEquals(stdProtocols,
                     clntProps.getProperty(ENABLED_PROTOCOLS));
        assertEquals(stdCipherSuites,
                     srvrProps.getProperty(ENABLED_CIPHER_SUITES));
        assertEquals(stdProtocols,
                     srvrProps.getProperty(ENABLED_PROTOCOLS));

        /*
         * Finally, check that setting the client property affects only the
         * client and that the server uses the basic setting.
         */

        final String clntCipherSuites = "TLS_RSA_WITH_AES_128_CBC_SHA";
        final String clntProtocols = "TLSv1";

        /* Make sure the std and clnt settings are different */
        assertFalse(clntCipherSuites.equals(stdCipherSuites));
        assertFalse(clntProtocols.equals(stdProtocols));

        sp.setTransClientAllowCipherSuites(TRANSPORT, clntCipherSuites);
        sp.setTransClientAllowProtocols(TRANSPORT, clntProtocols);

        clntProps = sslt.getSSLProperties(sp, transMap,
                                          false, /* resolveFiles */
                                          false); /* isServer */
        srvrProps = sslt.getSSLProperties(sp, transMap,
                                          false, /* resolveFiles */
                                          true); /* isServer */

        assertEquals(clntCipherSuites,
                     clntProps.getProperty(ENABLED_CIPHER_SUITES));
        assertEquals(clntProtocols,
                     clntProps.getProperty(ENABLED_PROTOCOLS));
        assertEquals(stdCipherSuites,
                     srvrProps.getProperty(ENABLED_CIPHER_SUITES));
        assertEquals(stdProtocols,
                     srvrProps.getProperty(ENABLED_PROTOCOLS));
    }

    @Test
    public void testChannelProperties()
        throws Exception {

        /* Configure via SecurityParams */
        final SecurityParams sp = new SecurityParams();
        sp.setSecurityEnabled(true);
        sp.addTransportMap(TRANSPORT);
        sp.setTransFactory(TRANSPORT, SSLTestUtils.SSL_TRANSPORT_FACTORY);
        sp.setTruststoreType("PKCS12");
        sp.setKeystoreType("PKCS12");
        final ParameterMap transMap = sp.getTransportMap(TRANSPORT);

        final SSLTransport sslt = new SSLTransport();

        /* Verify the results of a blank set of security params */

        Properties chanProps = sslt.makeChannelProperties(sp, transMap);
        Properties expectProps = new Properties();

        expectProps.setProperty(ReplicationNetworkConfig.CHANNEL_TYPE, "ssl");
        expectProps.setProperty(ReplicationSSLConfig.SSL_KEYSTORE_TYPE,
                                "PKCS12");
        expectProps.setProperty(ReplicationSSLConfig.SSL_TRUSTSTORE_TYPE,
                                "PKCS12");
        expectProps.setProperty(ReplicationSSLConfig.SSL_PROTOCOLS,
                                SecurityUtils.PREFERRED_PROTOCOLS_DEFAULT);

        verifyProperties(expectProps, chanProps);

        /* Now fully populate and verify results */

        /* Keystore */
        final String ksPath = new File(sslDir.getPath(),
                                       SSLTestUtils.SSL_KS_NAME).getPath();
        sp.setKeystoreFile(ksPath);
        expectProps.setProperty(
            ReplicationSSLConfig.SSL_KEYSTORE_FILE, ksPath);

        /* Keystore type */
        sp.setKeystoreType("PKCS12");
        expectProps.setProperty(
            ReplicationSSLConfig.SSL_KEYSTORE_TYPE, "PKCS12");

        /* Keystore password */
        sp.setPasswordFile(new File(sslDir.getPath(),
                                    SSLTestUtils.SSL_PW_NAME).getPath());
        sp.setKeystorePasswordAlias(SSLTestUtils.SSL_KS_ALIAS_DEF);
        final KeyStorePasswordSource pwdSrc =
            KeyStorePasswordSource.create(sp);
        expectProps.setProperty(
            ReplicationSSLConfig.SSL_KEYSTORE_PASSWORD_CLASS,
            pwdSrc.getClass().getName());
        expectProps.setProperty(
            ReplicationSSLConfig.SSL_KEYSTORE_PASSWORD_PARAMS,
            pwdSrc.getParamString());

        /* Server keystore alias */
        sp.setTransServerKeyAlias(TRANSPORT, SSLTestUtils.SSL_KS_ALIAS_DEF);
        expectProps.setProperty(
            ReplicationSSLConfig.SSL_SERVER_KEY_ALIAS,
            SSLTestUtils.SSL_KS_ALIAS_DEF);

        /* Client keystore alias */
        sp.setTransClientKeyAlias(TRANSPORT, SSLTestUtils.SSL_KS_ALIAS_DEF);
        expectProps.setProperty(
            ReplicationSSLConfig.SSL_CLIENT_KEY_ALIAS,
            SSLTestUtils.SSL_KS_ALIAS_DEF);

        /* Truststore */
        final String tsPath = new File(sslDir.getPath(),
                                       SSLTestUtils.SSL_TS_NAME).getPath();
        sp.setTruststoreFile(tsPath);
        expectProps.setProperty(
            ReplicationSSLConfig.SSL_TRUSTSTORE_FILE, tsPath);

        /* Truststore type */
        sp.setTruststoreType("PKCS12");
        expectProps.setProperty(
            ReplicationSSLConfig.SSL_TRUSTSTORE_TYPE, "PKCS12");

        /* Truststore password */
        sp.setKeystorePasswordAlias(SSLTestUtils.SSL_KS_ALIAS_DEF);
        expectProps.setProperty(
            ReplicationSSLConfig.SSL_TRUSTSTORE_PASSWORD_CLASS,
            pwdSrc.getClass().getName());
        expectProps.setProperty(
            ReplicationSSLConfig.SSL_TRUSTSTORE_PASSWORD_PARAMS,
            pwdSrc.getParamString());

        /* Cipher suites */
        final String cipherSuite = "SSL_RSA_WITH_RC4_128_MD5";
        sp.setTransAllowCipherSuites(TRANSPORT, cipherSuite);
        expectProps.setProperty(
            ReplicationSSLConfig.SSL_CIPHER_SUITES, cipherSuite);

        /* SSL protocol */
        sp.setTransAllowProtocols(TRANSPORT, "TLSv1");
        expectProps.setProperty(
            ReplicationSSLConfig.SSL_PROTOCOLS, "TLSv1");

        /* Client identity */
        final String clntIdentAllowed = "dnmatch(me)";
        sp.setTransClientIdentityAllowed(TRANSPORT, clntIdentAllowed);
        final InstanceInfo<SSLAuthenticator> authInstInfo =
            SSLConfig.makeAuthenticatorInfo(clntIdentAllowed);
        expectProps.setProperty(
            ReplicationSSLConfig.SSL_AUTHENTICATOR_CLASS,
            authInstInfo.jeImplClass);
        expectProps.setProperty(
            ReplicationSSLConfig.SSL_AUTHENTICATOR_PARAMS,
            authInstInfo.jeImplParams);

        /* Server identity */
        final String srvrIdentAllowed = "dnmatch(you)";
        sp.setTransServerIdentityAllowed(TRANSPORT, srvrIdentAllowed);
        final InstanceInfo<HostnameVerifier> verifierInstInfo =
            SSLConfig.makeHostVerifierInfo(srvrIdentAllowed);
        expectProps.setProperty(
            ReplicationSSLConfig.SSL_HOST_VERIFIER_CLASS,
            verifierInstInfo.jeImplClass);
        expectProps.setProperty(
            ReplicationSSLConfig.SSL_HOST_VERIFIER_PARAMS,
            verifierInstInfo.jeImplParams);

        /* Now verify that the properties we get are correct */
        chanProps = sslt.makeChannelProperties(sp, transMap);
        verifyProperties(expectProps, chanProps);
    }

    private void verifyProperties(Properties expect, Properties actual) {
        assertEquals(expect.size(), actual.size());
        for (String propName : expect.stringPropertyNames()) {
            String expectPropVal = expect.getProperty(propName);
            String actualPropVal = actual.getProperty(propName);
            assertEquals(propName, expectPropVal, actualPropVal);
        }
    }
}
