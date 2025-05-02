/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep.login;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.KVStoreConfig;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.security.KVStoreRolePrincipal;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.LoginResult;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.security.login.SessionId;
import oracle.kv.impl.security.login.UserLoginAPI;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.StorageNodeUtils.SecureOpts;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.CreateStore;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * This is a basic test to make sure that a secure KVStore configuration
 * can be created and accessed, as well as a variety of error cases.
 */
public class RepNodeLoginTest extends TestBase {

    private static final String HOST = "localhost";
    private static final int START_PORT = 5000;
    private static final String USER_NAME = "admin";
    private static final String USER_PW = "NoSql00__7654321";
    private static final int NUM_SNS = 2;
    private static final int RF = 2;

    private static CreateStore createStore = null;
    private static int createStoreCount = 0;
    private static boolean noSSL = false;

    @BeforeClass
    public static void staticSetUp()
        throws Exception {
        TestUtils.clearTestDirectory();
        noSSL = TestUtils.isSSLDisabled();
        startKVStore();

        if (TestUtils.testDebugEnabled()) {
            /* To allow debugging without timeouts */
            ClientSocketFactory.configure(
                createStore.getStoreName() + "|" + "rn|login",
                new KVStoreConfig(createStore.getStoreName(), "dummyHost:9000").
                    setSocketOpenTimeout(60 * 1000, TimeUnit.MILLISECONDS).
                    setSocketReadTimeout(300 * 1000, TimeUnit.MILLISECONDS),
                null /* clientId */);
        }
    }

    @AfterClass
    public static void staticTearDown()
        throws Exception {

        if (createStore != null) {
            createStore.shutdown();
            createStore = null;
        }
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

    @Test
    public void testLogin()
        throws Exception {
        /*
         * Iterate over all SNs in the store, testing login at each one
         */
        for (int i = 0; i < NUM_SNS; i++) {
            StorageNodeId snId =
                createStore.getStorageNodeAgent(i).getStorageNodeId();
            List<RepNodeId> rnIds = createStore.getRNs(snId);

            for (RepNodeId rnId : rnIds) {
                testLogin(snId, rnId);
            }
        }
    }

    private void testLogin(StorageNodeId snId, RepNodeId rnId)
        throws Exception {
        final int registryPort =
            createStore.getRegistryPort(snId);

        UserLoginAPI ulNoAuthAPI =
            RegistryUtils.getRepNodeLogin(createStore.getStoreName(),
                                          HOST, registryPort, rnId,
                                          null /* loginMgr */, logger);
        LoginManager internalLoginMgr =
            createStore.getStorageNodeAgent(0).getLoginManager();

        UserLoginAPI ulAuthAPI =
            RegistryUtils.getRepNodeLogin(createStore.getStoreName(),
                                          HOST, registryPort, rnId,
                                          internalLoginMgr, logger);

        final LoginCredentials goodCreds =
            new PasswordCredentials(USER_NAME,
                                    USER_PW.toCharArray());

        final LoginCredentials badCreds =
            new PasswordCredentials(USER_NAME + "XXX",
                                    USER_PW.toCharArray());

        /*
         * Test login()
         */

        try {
            /* Check that null doesn't trip us up */
            ulNoAuthAPI.login(null);
            fail("expected exeption");
        } catch (AuthenticationFailureException fe) {
        }

        /* bad logins are rejected */
        try {
            ulNoAuthAPI.login(badCreds);
            fail("expected exeption");
        } catch (AuthenticationFailureException fe) {
        }

        /* good logins are accepted */
        final LoginResult lr = ulNoAuthAPI.login(goodCreds);
        assertNotNull(lr);
        assertNotNull(lr.getLoginToken());

        /* Make sure the persistent session manager is in use */
        assertEquals(SessionId.IdScope.PERSISTENT,
                     lr.getLoginToken().getSessionId().getIdValueScope());


        /* Persistent sessions must expire */
        assertFalse(lr.getLoginToken().getExpireTime() == 0L);

        /*
         * Test session extension. We assume here that extension is
         * allowed.
         */

        /* Check that null doesn't trip us up */
        final LoginToken nullLt = ulNoAuthAPI.requestSessionExtension(null);
        assertNull(nullLt);

        LoginToken lt = lr.getLoginToken();

        /**
         * Test for KVSTORE-1832: NullPointerException in
         * KVSessionManager.updateKVSessionExpire
         * Enable TestIOHook to inject IOException in
         * KVSessionManager.lookupKVSession to return a null session from
         * lookupKVSession which eventually causes
         * UserLoginHandler.requestSessionExtension to return null
         */
        KVSessionManager.LOOKUP_FAULT_HOOK = (v) -> {
            throw new java.io.IOException();
        };
        final LoginToken loginTok = ulNoAuthAPI.requestSessionExtension(lt);
        assertNull(loginTok);
        KVSessionManager.LOOKUP_FAULT_HOOK = null;

        for (int i = 0; i < 5; i++) {
            final LoginToken nlt = ulNoAuthAPI.requestSessionExtension(lt);
            assertNotNull(nlt);
            assertEquals(nlt.getSessionId(), lt.getSessionId());
            assertFalse(lt.getExpireTime() == nlt.getExpireTime());
            lt = nlt;
        }

        /*
         * Test validateLoginToken
         */
        /* Check that null doesn't trip us up */
        final Subject nullSubj = ulAuthAPI.validateLoginToken(null);
        assertNull(nullSubj);

        Subject subj = ulAuthAPI.validateLoginToken(lt);
        assertNotNull(subj);

        /*
         * Test validateLoginToken when session updated
         */
        grantRoles(USER_NAME, "readonly", "dbadmin");
        subj = ulAuthAPI.validateLoginToken(lt);
        assertNotNull(subj);

        /* Check roles in session subject have been changed */
        Set<KVStoreRolePrincipal> rolePrincs =
            subj.getPrincipals(KVStoreRolePrincipal.class);
        assertTrue(rolePrincs.contains(KVStoreRolePrincipal.READONLY));
        assertTrue(rolePrincs.contains(KVStoreRolePrincipal.DBADMIN));
        assertTrue(rolePrincs.contains(KVStoreRolePrincipal.SYSADMIN));
        revokeRoles(USER_NAME, "readonly");
        subj = ulAuthAPI.validateLoginToken(lt);
        assertNotNull(subj);
        rolePrincs = subj.getPrincipals(KVStoreRolePrincipal.class);
        assertFalse(rolePrincs.contains(KVStoreRolePrincipal.READONLY));
        assertTrue(rolePrincs.contains(KVStoreRolePrincipal.DBADMIN));
        assertTrue(rolePrincs.contains(KVStoreRolePrincipal.SYSADMIN));

        /*
         * Test logout
         */

        /* Check that null doesn't trip us up */
        try {
            ulNoAuthAPI.logout(null);
            fail("expected exception");
        } catch (AuthenticationRequiredException are) {
        }

        ulNoAuthAPI.logout(lt);

        /*
         * repeat API calls with logged-out tokens
         */
        subj = ulAuthAPI.validateLoginToken(lt);
        assertNull(subj);

        LoginToken nlt = ulAuthAPI.requestSessionExtension(lt);
        assertNull(nlt);
    }

    private static void startKVStore() {
        try {
            createStoreCount++;
            createStore = new CreateStore("kvtest-" +
                                          RepNodeLoginTest.class.getName() +
                                          "-" + createStoreCount,
                                          START_PORT,
                                          NUM_SNS, /* Storage nodes */
                                          RF, /* Replication factor */
                                          10, /* Partitions */
                                          1, /* Capacity */
                                          CreateStore.MB_PER_SN,
                                          true, /* useThreads */
                                          null, /* mgmtImpl */
                                          true, /* mgmtPortsShared */
                                          true); /* secure */
            createStore.addUser(USER_NAME, USER_PW, true /* admin */);
            if (noSSL) {
                SecureOpts secureOpts = new SecureOpts().
                                setSecure(true).setNoSSL(true);
                createStore.setSecureOpts(secureOpts);
            }
            createStore.start(false);

        } catch (Exception e) {
            fail("unexpected exception in createStore: " + e);
        }
    }

    private static void grantRoles(String user, String... roles)
            throws Exception {

            final CommandServiceAPI cs = createStore.getAdmin();
            final Set<String> roleList = new HashSet<>();
            Collections.addAll(roleList, roles);
            final int planId =
                cs.createGrantPlan("Grant roles", user, roleList);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            cs.assertSuccess(planId);
        }

    private static void revokeRoles(String user, String... roles)
        throws Exception {

        final CommandServiceAPI cs = createStore.getAdmin();
        final Set<String> roleList = new HashSet<>();
        Collections.addAll(roleList, roles);
        final int planId =
            cs.createRevokePlan("Revoke roles", user, roleList);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
    }

}
