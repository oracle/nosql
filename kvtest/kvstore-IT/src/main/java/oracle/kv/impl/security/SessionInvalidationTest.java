/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import static oracle.kv.impl.param.ParameterState.GP_LOGIN_CACHE_TIMEOUT;

import java.rmi.RemoteException;
import java.util.Set;

import javax.security.auth.Subject;

import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.impl.security.login.LoginResult;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.security.login.SessionId.IdScope;
import oracle.kv.impl.security.login.UserLoginAPI;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.Protocols;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.CreateStore.SecureUser;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test session invalidation cases.
 */
public class SessionInvalidationTest extends SecureTestBase {

    private static final String USER1_NAME = "admin";
    private static final String USER1_PW = "NoSql00__7654321";
    private static final String USER2_NAME = "other";
    private static final String USER2_PW = "NoSql00__7654321";
    private static final int testSNs = 2;
    private static final int testRF = 2;

    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        /* configure */
        numSNs = testSNs;
        repFactor = testRF;
        users.add(new SecureUser(USER1_NAME, USER1_PW, true /* admin */));
        users.add(new SecureUser(USER2_NAME, USER2_PW, true /* admin */));

        startup();
    }

    @AfterClass
    public static void staticTearDown()
        throws Exception {

        shutdown();
    }

    /**
     * Notes: This test did not call super setUp method to clean test directory.
     */
    @Override
    public void setUp()
        throws Exception {

    }

    @Override
    public void tearDown() 
        throws Exception {

    }
    /**
     * Test that disabling and deletion of a user definition invalidates both
     * persistent and non-persistent sessions held by the user.
     */
    @Test
    public void testSessionInvalidation() throws Exception {

        final Topology storeTopo = getTopology();

        /*
         * Set the cache timeout to be very short so that we can quickly
         * force revalidation of logins.
         */
        setAuthPolicy(GP_LOGIN_CACHE_TIMEOUT + "=" + "1 s");

        final RegistryUtils ru =
            new RegistryUtils(storeTopo, getSNALoginManager(0), logger);

        final LoginCredentials user2Creds =
            new PasswordCredentials(USER2_NAME, USER2_PW.toCharArray());

        /* Get an admin login token */
        final UserLoginAPI adminULAPI =
            RegistryUtils.getAdminLogin(HOST, registryPorts[0],
                                        getSNALoginManager(0), logger);
        LoginResult lr = adminULAPI.login(user2Creds);
        assertNotNull(lr);
        final LoginToken adminToken = lr.getLoginToken();
        assertNotNull(adminToken);

        /*
         * Get a persistent login token.
         */
        Set<RepNodeId> rnIDs = storeTopo.getRepNodeIds();
        final RepNodeId initialRNId = rnIDs.iterator().next();

        final Protocols protocols =
            Protocols.get(createStore.createKVConfig());
        final UserLoginAPI initialRNULAPI =
            ru.getRepNodeLogin(initialRNId, protocols);
        lr = initialRNULAPI.login(user2Creds);
        assertNotNull(lr);
        final LoginToken rnToken = lr.getLoginToken();
        assertNotNull(rnToken);
        assertEquals(rnToken.getSessionId().getIdValueScope(),
                     IdScope.PERSISTENT);

        /* validate that the tokens are recognized as valid */
        testLoginTokenOK(adminULAPI, adminToken);

        for (RepNodeId rnId : rnIDs) {
            UserLoginAPI rnULAPI = ru.getRepNodeLogin(rnId, protocols);
            testLoginTokenOK(rnULAPI, rnToken);
        }

        /* disable the user */
        changeUserEnabled(USER2_NAME, false);

        /* Delay to let login caches expire */
        delayFor(1500);

        /* validate that the tokens are now recognized as invalid */
        testLoginTokenFail(adminULAPI, adminToken);

        for (RepNodeId rnId : rnIDs) {
            final UserLoginAPI rnULAPI = ru.getRepNodeLogin(rnId, protocols);
            testLoginTokenFail(rnULAPI, rnToken);
        }

        /* enable the user */
        changeUserEnabled(USER2_NAME, true);

        /* validate that the tokens are recognized as valid again */
        testLoginTokenOK(adminULAPI, adminToken);

        for (RepNodeId rnId : rnIDs) {
            final UserLoginAPI rnULAPI = ru.getRepNodeLogin(rnId, protocols);
            testLoginTokenOK(rnULAPI, rnToken);
        }

        /* delete the user */
        deleteUser(USER2_NAME, USER1_NAME, USER1_PW);

        /* Delay to let login caches expire */
        delayFor(1500);

        /* validate that the tokens are recognized as invalid again */
        testLoginTokenFail(adminULAPI, adminToken);

        for (RepNodeId rnId : rnIDs) {
            final UserLoginAPI rnULAPI = ru.getRepNodeLogin(rnId, protocols);
            testLoginTokenFail(rnULAPI, rnToken);
        }
    }

    private void testLoginTokenOK(UserLoginAPI ulAPI, LoginToken token)
        throws RemoteException {

        final Subject subj = ulAPI.validateLoginToken(token);
        assertNotNull(subj);
    }


    private void testLoginTokenFail(UserLoginAPI ulAPI, LoginToken token)
        throws RemoteException {

        final Subject subj = ulAPI.validateLoginToken(token);
        assertNull(subj);
    }

    private void delayFor(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            /* No cause for this and it might invalidate test */
            fail("unexpected interruption");
        }
    }
}
