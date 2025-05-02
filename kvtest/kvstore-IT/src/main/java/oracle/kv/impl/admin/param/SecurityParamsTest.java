/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.param;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import java.io.File;

import oracle.kv.TestBase;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.util.TestUtils;

import org.junit.Test;

/**
 * Tests some features of Parameter configuration.
 */
public class SecurityParamsTest extends TestBase {
    static final String fname = "testsecurity.xml";
    private static final int VERSION = 1;
    private static final boolean TEST_SEC_ENABLED = true;
    private static final String TEST_KS_FILE = "/tmp/store.keys";
    private static final String TEST_KS_TYPE = "JKS";
    private static final String TEST_TS_FILE = "/tmp/store.trust";
    private static final String TEST_TS_TYPE = "JKS";
    private static final String TEST_PW_FILE = "/tmp/store.pwd";
    private static final String TEST_PW_CLS = "oracle.kv.util.pwdmgr";
    private static final String TEST_WALL_DIR = "/tmp/store.wallet";
    private static final String TEST_INTERNAL_AUTH = "ssl";
    private static final String TEST_KS_PW_ALIAS = "kspw";
    private static final String TEST_TRANS = "xport";
    private static final String TEST_TRANS_TYPE = "ssl";
    private static final String TEST_TRANS_FACTORY = "oracle.kv.util.trans";
    private static final String TEST_TRANS_KEY_ALIAS = "xportKey";
    private static final String TEST_TRANS_CLNT_KEY_ALIAS = "xportClntKey";
    private static final String TEST_TRANS_CIPHER_SUITES =
        "SSL_RSA_WITH_RC4_128_MD5,SSL_RSA_WITH_RC4_128_SHA";
    private static final String TEST_TRANS_CLIENT_CIPHER_SUITES =
        "TLS_RSA_WITH_AES_128_CBC_SHA,SSL_RSA_WITH_RC4_128_SHA";
    private static final String TEST_TRANS_PROTOCOLS = "TLSv1,SSLv2";
    private static final String TEST_TRANS_CLIENT_PROTOCOLS = "TLSv1.3,TLSv1.2";
    private static final String TEST_TRANS_CLNT_PEER_IDENT = "Server";
    private static final String TEST_TRANS_SRVR_PEER_IDENT = "Client";
    private static final boolean TEST_TRANS_CLNT_AUTH = true;

    /**
     * Create a SecurityParams instance with parameters filled in.
     */
    private SecurityParams createParams()
        throws Exception {

        final SecurityParams params = new SecurityParams();
        params.setSecurityEnabled(TEST_SEC_ENABLED);
        params.setKeystoreFile(TEST_KS_FILE);
        params.setKeystoreType(TEST_KS_TYPE);
        params.setTruststoreFile(TEST_TS_FILE);
        params.setTruststoreType(TEST_TS_TYPE);
        params.setPasswordFile(TEST_PW_FILE);
        params.setPasswordClass(TEST_PW_CLS);
        params.setWalletDir(TEST_WALL_DIR);
        params.setInternalAuth(TEST_INTERNAL_AUTH);
        params.setKeystorePasswordAlias(TEST_KS_PW_ALIAS);

        params.addTransportMap(TEST_TRANS);
        params.setTransType(TEST_TRANS, TEST_TRANS_TYPE);
        params.setTransFactory(TEST_TRANS, TEST_TRANS_FACTORY);
        params.setTransServerKeyAlias(TEST_TRANS, TEST_TRANS_KEY_ALIAS);
        params.setTransClientKeyAlias(TEST_TRANS, TEST_TRANS_CLNT_KEY_ALIAS);
        params.setTransAllowCipherSuites(TEST_TRANS,
                                         TEST_TRANS_CIPHER_SUITES);
        params.setTransAllowProtocols(TEST_TRANS, TEST_TRANS_PROTOCOLS);
        params.setTransClientAllowCipherSuites(TEST_TRANS,
                                               TEST_TRANS_CLIENT_CIPHER_SUITES);
        params.setTransClientAllowProtocols(TEST_TRANS,
                                            TEST_TRANS_CLIENT_PROTOCOLS);
        params.setTransClientIdentityAllowed(TEST_TRANS,
                                             TEST_TRANS_CLNT_PEER_IDENT);
        params.setTransServerIdentityAllowed(TEST_TRANS,
                                             TEST_TRANS_SRVR_PEER_IDENT);
        params.setTransClientAuthRequired(TEST_TRANS, TEST_TRANS_CLNT_AUTH);

        return params;
    }

    /**
     * Check that parameter values are as expected based on createParams
     */
    private void checkParams(SecurityParams params)
        throws Exception {

        assertEquals(TEST_SEC_ENABLED, params.getSecurityEnabled());
        assertEquals(TEST_KS_FILE, params.getKeystoreFile());
        assertEquals(TEST_KS_TYPE, params.getKeystoreType());
        assertEquals(TEST_TS_FILE, params.getTruststoreFile());
        assertEquals(TEST_TS_TYPE, params.getTruststoreType());
        assertEquals(TEST_PW_FILE, params.getPasswordFile());
        assertEquals(TEST_PW_CLS, params.getPasswordClass());
        assertEquals(TEST_WALL_DIR, params.getWalletDir());
        assertEquals(TEST_INTERNAL_AUTH, params.getInternalAuth());
        assertEquals(TEST_KS_PW_ALIAS, params.getKeystorePasswordAlias());

        final ParameterMap tMap = params.getTransportMap(TEST_TRANS);
        assertFalse(tMap == null);
        assertEquals(TEST_TRANS_TYPE,
                     params.getTransType(TEST_TRANS));
        assertEquals(TEST_TRANS_FACTORY,
                     params.getTransFactory(TEST_TRANS));
        assertEquals(TEST_TRANS_KEY_ALIAS,
                     params.getTransServerKeyAlias(TEST_TRANS));
        assertEquals(TEST_TRANS_CLNT_KEY_ALIAS,
                     params.getTransClientKeyAlias(TEST_TRANS));
        assertEquals(TEST_TRANS_CIPHER_SUITES,
                     params.getTransAllowCipherSuites(TEST_TRANS));
        assertEquals(TEST_TRANS_PROTOCOLS,
                     params.getTransAllowProtocols(TEST_TRANS));
        assertEquals(TEST_TRANS_CLIENT_CIPHER_SUITES,
                     params.getTransClientAllowCipherSuites(TEST_TRANS));
        assertEquals(TEST_TRANS_CLIENT_PROTOCOLS,
                     params.getTransClientAllowProtocols(TEST_TRANS));
        assertEquals(TEST_TRANS_CLNT_PEER_IDENT,
                     params.getTransClientIdentityAllowed(TEST_TRANS));
        assertEquals(TEST_TRANS_SRVR_PEER_IDENT,
                     params.getTransServerIdentityAllowed(TEST_TRANS));
        assertEquals(TEST_TRANS_CLNT_AUTH,
                     params.getTransClientAuthRequired(TEST_TRANS));
    }

    @Test
    public void testLifecycle()
        throws Exception {

        final SecurityParams sp1 = createParams();
        checkParams(sp1);

        final ParameterMap sp1Map = sp1.getMap();
        final ParameterMap sp1TMap = sp1.getTransportMap(TEST_TRANS);

        final File configFile = new File(TestUtils.getTestDir(), fname);

        /**
         * Save the configuration
         */
        final LoadParameters lp1 = new LoadParameters();
        lp1.addMap(sp1Map);
        lp1.addMap(sp1TMap);
        lp1.setVersion(VERSION);
        lp1.saveParameters(configFile);

        /**
         * Get a new object and make sure it's all good.
         */
        final LoadParameters lp2 =
            LoadParameters.getParameters(configFile, null);
        assertSame(VERSION, lp2.getVersion());
        assertNotNull(lp1.getMap(ParameterState.SECURITY_PARAMS));
        assertNotNull(lp1.getMapByType(ParameterState.SECURITY_TYPE));

        final SecurityParams sp2 = new SecurityParams(lp1, configFile);
        checkParams(sp2);

        final SecurityParams sp3 = new SecurityParams(sp1Map);
        sp3.addTransportMap(sp1TMap, TEST_TRANS);
        checkParams(sp3);
    }

    @Test
    public void testResolveFile()
        throws Exception {

        final String absFile = "/tmp/foo";
        final String relFile = "foo";

        final SecurityParams sp = new SecurityParams();
        sp.setConfigDir(TestUtils.getTestDir());

        final File res1 = sp.resolveFile(absFile);
        assertEquals(absFile, res1.getPath());

        final File res2 = sp.resolveFile(relFile);
        final File relRes = new File(TestUtils.getTestDir().getPath(), relFile);
        assertEquals(res2.getPath(), relRes.getPath());

        assertNull(sp.resolveFile(null));
    }
}
