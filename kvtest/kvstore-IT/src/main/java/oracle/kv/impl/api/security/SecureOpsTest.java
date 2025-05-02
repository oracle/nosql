/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.security;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import oracle.kv.KVSecurityConstants;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.UncaughtExceptionTestBase;
import oracle.kv.Value;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.RequestDispatcher;
import oracle.kv.impl.api.RequestDispatcherImpl;
import oracle.kv.impl.api.TopologyManager;
import oracle.kv.impl.security.login.InternalLoginManager;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.TopoTopoResolver;
import oracle.kv.impl.security.login.TopologyResolver;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.StorageNodeUtils.SecureOpts;
import oracle.kv.util.CreateStore;

/**
 * This is a basic test to make sure that a secure KVStore configuration
 * can be created and accessed, as well as a variety of error cases.
 */
public class SecureOpsTest extends UncaughtExceptionTestBase {

    private static final String HOST = "localhost";
    private static final int PORT = 5240;
    private static final String ADMIN = "admin";
    private static final String ADMIN_PW = "NoSql00__7654321";
    private static final String USER_NAME = "user";
    private static final String USER_PW = "NoSql00__1234567";

    private static CreateStore createStore = null;
    private static int createStoreCount = 0;
    private static KVStore userStore = null;
    private static KVStore userInternalStore = null;
    private static KVStore serverStore = null;
    private static boolean noSSL = false;

    /*
     * Keys in the server-private space
     */
    private static Key SRVR_KEY =
        Key.createKey(Arrays.asList("", "", "a"));

    /*
     * Keys in the client-accessible hidden space
     */
    private static Key INT_KEY =
        Key.createKey(Arrays.asList("", "a"));

    /*
     * Keys in the user space
     */
    private static Key CLNT_KEY =
        Key.createKey(Arrays.asList("a"));

    /*
     * Some values to use
     */
    private static Value VALUE_1 = Value.createValue(new byte[] { 1 });


    @BeforeClass
    public static void staticSetUp()
        throws Exception {
        TestUtils.clearTestDirectory();
        noSSL = TestUtils.isSSLDisabled();
        startKVStore();
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
        logoutAllStores();
    }

    @Test
    public void testAccessInternalKeyspace() throws Exception{
        loginKVStoreUser();
        loginKVStoreServer();

        /* Internal user can read and write private keyspace */
        try {
            serverStore.put(SRVR_KEY, VALUE_1);
            testValidReadOps(serverStore, SRVR_KEY, true /* isServer */);
        } finally {
            serverStore.delete(SRVR_KEY);
        }

        testValidWriteOps(serverStore, SRVR_KEY);
    }

    @Test
    public void testAccessInternalDeniedOps() throws Exception {
        try {
            loginKVStoreUser();
            grantRoles(USER_NAME, "readwrite");

            /* User space write OK */
            userStore.put(CLNT_KEY, VALUE_1);

            /* Internal space write is denied for non-internal user */
            testDeniedWriteOps(userInternalStore, SRVR_KEY,
                               true/* access internal space */);

            /* Internal space write is denied for non-internal user  */
            testDeniedReadOps(userInternalStore, SRVR_KEY,
                              true /* access internal space */);
        } finally {
            /* Clean-up */
            userStore.delete(CLNT_KEY);
            revokeRoles(USER_NAME, "readwrite");
        }
    }

    @Test
    public void testUserWriteOps() throws Exception {
        /* Users with write privilege can write successfully */
        try {
            loginKVStoreUser();
            grantRoles(USER_NAME, "writeonly");
            testValidWriteOps(userStore, CLNT_KEY);
            testValidWriteOps(userInternalStore, INT_KEY);
        } finally {
            revokeRoles(USER_NAME, "writeonly");
        }

        /* Users without write privilege cannot write to store */
        revokeRoles(USER_NAME, "writeonly");
        revokeRoles(USER_NAME, "readwrite");
        testDeniedWriteOps(userStore, CLNT_KEY, false /* user space */);
        testDeniedWriteOps(userInternalStore, INT_KEY, false /* user space */);
    }

    @Test
    public void testUserReadOps() throws Exception {

        /* Put some values in user space for read test */
        try {
            loginKVStoreUser();
            grantRoles(USER_NAME, "writeonly");
            grantRoles(USER_NAME, "readonly");
            userStore.put(CLNT_KEY, VALUE_1);
            testValidReadOps(userStore, CLNT_KEY, false /* isServer */);
        } finally {
            /* Clean-up last writes*/
            userStore.delete(CLNT_KEY);
        }

        try {
            /* Put some values in internal space */
            userInternalStore.put(INT_KEY, VALUE_1);
            testValidReadOps(userInternalStore, INT_KEY, false /* isServer */);
        } finally {
            /* Clean-up */
            userInternalStore.delete(INT_KEY);
            revokeRoles(USER_NAME, "writeonly");
            revokeRoles(USER_NAME, "readonly");
        }

        /* Remove read privilege from user, and no need re-login */
        revokeRoles(USER_NAME, "readonly");
        revokeRoles(USER_NAME, "readwrite");
        testDeniedReadOps(userStore, CLNT_KEY, false /* access user space */);
        testDeniedReadOps(userInternalStore, INT_KEY,
                          false /* access user space */);
    }

    /*
     * Test write ops by user or server that should not encounter any
     * exception
     */
    private static void testValidWriteOps(KVStore store, Key key)
        throws Exception {
        OpAccessCheckTestUtils.testValidPutOps(store, store, key, VALUE_1);

        try {
            oracle.kv.Version v = store.put(key, VALUE_1);
            assertNotNull(v);

            OpAccessCheckTestUtils.testValidDeleteOps(
                store, store, key, VALUE_1);
        } finally {
          /* Do some cleanup */
          store.delete(key);
        }
    }

    private static void testDeniedWriteOps(KVStore store,
                                           Key key,
                                           boolean accessInternalSpace)
        throws Exception {

        OpAccessCheckTestUtils.testDeniedPutOps(store, key, VALUE_1);

        OpAccessCheckTestUtils.testDeniedDeleteOps(store, key,
                                                   accessInternalSpace);
    }

    private static void testValidReadOps(KVStore store,
                                         Key key,
                                         boolean isServer) {
        OpAccessCheckTestUtils.testValidReadOps(store, key, isServer);
    }

    private static void testDeniedReadOps(KVStore store,
                                          Key key,
                                          boolean internalSpaceRead) {

        OpAccessCheckTestUtils.testDeniedReadOps(store, key,
                                                 internalSpaceRead);
    }

    private static void startKVStore() {
        try {
            createStoreCount++;
            createStore = new CreateStore("kvtest-" +
                                          SecureOpsTest.class.getName() +
                                          "-" + createStoreCount,
                                          PORT,
                                          1, /* Storage nodes */
                                          1, /* Replication factor */
                                          10, /* Partitions */
                                          1, /* Capacity */
                                          CreateStore.MB_PER_SN,
                                          false, /* useThreads */
                                          null, /* mgmtImpl */
                                          true, /* mgmtPortsShared */
                                          true); /* secure */
            createStore.addUser(ADMIN, ADMIN_PW, true /* admin */);
            createStore.addUser(USER_NAME, USER_PW, false /* admin */);
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

    private static void loginKVStoreUser() {
        try {
            LoginCredentials creds =
                new PasswordCredentials(USER_NAME,USER_PW.toCharArray());
            KVStoreConfig kvConfig =
                new KVStoreConfig(createStore.getStoreName(),
                                  HOST + ":" + PORT);
            Properties props = new Properties();
            addTransportProps(props);
            kvConfig.setSecurityProperties(props);

            userStore = KVStoreFactory.getStore(kvConfig, creds, null);
            userInternalStore = KVStoreImpl.makeInternalHandle(userStore);

        } catch (Exception e) {
            fail("unexpected exception in user login: " + e);
        }
    }

    private static void addTransportProps(Properties props) {
        if (noSSL) {
            props.put(KVSecurityConstants.TRANSPORT_PROPERTY, "clear");
        } else {
            props.put(KVSecurityConstants.TRANSPORT_PROPERTY,
                         KVSecurityConstants.SSL_TRANSPORT_NAME);
            props.put(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                         createStore.getTrustStore().getPath());
        }
    }

    private static void logoutAllStores() {
        if (userStore != null) {
            userStore.logout();
            userStore.close();
            userInternalStore = null;
            serverStore = null;
            userStore = null;
        }
    }

    private void loginKVStoreServer() {
        try {
            KVStoreConfig kvConfig =
                new KVStoreConfig(createStore.getStoreName(),
                                  HOST + ":" + PORT);
            Properties props = new Properties();
            addTransportProps(props);
            kvConfig.setSecurityProperties(props);

            TopologyManager topoMgr =
                ((KVStoreImpl) userStore).
                getDispatcher().
                getTopologyManager();
            LoginManager internalLoginMgr =
                new InternalLoginManager(
                    new TopoTopoResolver(
                        new TopoTopoResolver.TopoMgrTopoHandle(topoMgr),
                        new TopologyResolver.SNInfo(
                            HOST, PORT,
                            createStore.getStorageNodeIds()[0]),
                        logger),
                    logger);
            ClientId clientId = new ClientId(System.currentTimeMillis());
            RequestDispatcher dispatcher =
                RequestDispatcherImpl.createForClient(
                    kvConfig, clientId, internalLoginMgr, this, logger);

            KVStore serverInitialStore =
                new KVStoreImpl(logger, dispatcher, kvConfig,
                                internalLoginMgr,
                                null /* reauth handler */);
            /*
             * serverInitialStore has restrictions on the key.  We need to
             * remove them for the handle to be useful.
             */
            serverStore = KVStoreImpl.makeInternalHandle(serverInitialStore);

        } catch (Exception e) {
            fail("unexpected exception in system login: " + e);
        }
    }

    private static void grantRoles(String user, String... roles)
        throws Exception {
        final CommandServiceAPI cs = createStore.getAdmin();
        final Set<String> roleSet = new HashSet<String>();
        Collections.addAll(roleSet, roles);
        final int planId =
            cs.createGrantPlan("Grant roles", user, roleSet);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
    }

    private static void revokeRoles(String user, String... roles)
        throws Exception {
        final CommandServiceAPI cs = createStore.getAdmin();
        final Set<String> roleSet = new HashSet<String>();
        Collections.addAll(roleSet, roles);
        final int planId =
            cs.createRevokePlan("Revoke roles", user, roleSet);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
    }
}
