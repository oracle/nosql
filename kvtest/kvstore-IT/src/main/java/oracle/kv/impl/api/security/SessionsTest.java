/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Properties;

import oracle.kv.Consistency;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.RequestTimeoutException;
import oracle.kv.Value;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.KVStoreInternalFactory;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.SecureTestBase;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.login.InternalLoginManager;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.security.login.SessionId;
import oracle.kv.impl.security.login.SessionId.IdScope;
import oracle.kv.impl.security.login.TopologyResolver;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.util.CreateStore.SecureUser;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This test attempts to induce a variety of session access problems.
 */
public class SessionsTest extends SecureTestBase {

    private static final int testSNs = 2;
    private static final int testRF = 2;
    private static final String USER_NAME = "admin";
    private static final String USER_PW = "NoSql00__7654321";

    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        numSNs = testSNs;
        repFactor = testRF;
        users.add(new SecureUser(USER_NAME, USER_PW, true /* admin */));
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
     * Test various error conditions for RepNode sessions.
     */
    @Test
    public void testRNSessions()
        throws Exception {

        final LoginCredentials creds =
            new PasswordCredentials(USER_NAME,USER_PW.toCharArray());
        final KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0]);
        final Properties props = new Properties();
        addTransportProps(props);
        kvConfig.setSecurityProperties(props);

        final KVStore store1 = KVStoreFactory.getStore(kvConfig, creds, null);

        /*
         * We now have a valid login to the store - we expect that it
         * is persistent.
         */
        final SessionId.IdScope scope1 = getLoginScope(store1);
        assertEquals(scope1, IdScope.PERSISTENT);
        grantRoles(USER_NAME, "readwrite");

        final Key aKey = Key.createKey("foo");
        store1.put(aKey, Value.createValue(new byte[0]));

        store1.get(aKey); /* return value ignored */

        /* Force a session renewal */
        renewLogin(store1);

        /* Shut down the SNA, stopping services, but not force */
        createStore.shutdownSNA(1, false);

        /*
         * We should no longer have quorum.
         */
        try {
            store1.put(aKey, Value.createValue(new byte[0]));
            fail("expected exception");
        } catch (RequestTimeoutException de) {
            /* Expected */
        }

        /* Make sure the persistent token still functions */
        store1.get(aKey); /* return value ignored */

        /* try another session renewal - should fail */
        try {
            renewLogin(store1);
            fail ("expected exception");
        } catch (SessionAccessException sae) {
            /* Expected */
        }

        final KVStore store2 = KVStoreFactory.getStore(kvConfig, creds, null);

        /* This should be a non-persistent login */
        final SessionId.IdScope scope2 = getLoginScope(store2);
        assertEquals(scope2, IdScope.STORE);

        store2.get(aKey); /* return value ignored */

        /* restart the stopped SN */
        createStore.startSNA(1);

        /*
         * Retry ealier tests now that the SNA is back up
         */

        /* No longer expect a DurabilityException */
        store1.put(aKey, Value.createValue(new byte[0]));

        /* Make sure the persistent token still functions for read */
        store1.get(aKey); /* return value ignored */

        /* session renewal should now work */
        renewLogin(store1);

        store2.close();
        store1.close();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testInternalSessionRenewal()
        throws Exception {

        final LoginCredentials creds =
                new PasswordCredentials(USER_NAME,USER_PW.toCharArray());
        final KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0]);
        final Properties props = new Properties();
        addTransportProps(props);
        kvConfig.setSecurityProperties(props);

        final KVStore store1 = KVStoreFactory.getStore(kvConfig, creds, null);

        /*
         * We now have a valid login to the store - we expect that it
         * is persistent.
         */
        final SessionId.IdScope scope1 = getLoginScope(store1);
        assertEquals(scope1, IdScope.PERSISTENT);
        grantRoles(USER_NAME, "readwrite");

        final Key aKey = Key.createKey("foo");
        store1.put(aKey, Value.createValue(new byte[0]));

        store1.get(aKey); /* return value ignored */

        /* Force a session renewal */
        renewLogin(store1);

        /* Make a store handle using internal handle */
        final KVStore store2 = KVStoreInternalFactory.getStore(kvConfig,
            ((KVStoreImpl)store1).getDispatcher(),
            new InternalLoginManager(new TopoResolver(), logger), null);

        /*
         * Perform an operation to cache an login token issued by SN
         * where replica is located
         */
        store2.get(aKey, Consistency.NONE_REQUIRED_NO_MASTER, 0, null);

        int replicaSNId = findReplicaSNId();

        /*
         * Shut down the SNA, stopping services, but not force,
         * the login table of this SNA should be refreshed.
         */
        createStore.shutdownSNA(replicaSNId, false);

        /* restart the stopped SN  */
        createStore.startSNA(replicaSNId);

        /* Having quorum, should be no error */
        store1.put(aKey, Value.createValue(new byte[0]));

        /* Make sure the persistent token still functions for read */
        store1.get(aKey); /* return value ignored */

        /* session renewal should now work */
        renewLogin(store1);

        /*
         * The cached login token cannot be found in replica SN, but the request
         * dispatcher can renew the login token, so should be no error here
         */
        store2.get(aKey, Consistency.NONE_REQUIRED_NO_MASTER, 0, null);

        store2.close();
        store1.close();
    }

    private SessionId.IdScope getLoginScope(KVStore store) {
        final LoginManager lm = KVStoreImpl.getLoginManager(store);
        final RepNodeId rnId =
            createStore.getRNs(
                createStore.getStorageNodeAgent(0).getStorageNodeId()).get(0);
        final LoginHandle lh = lm.getHandle(rnId);
        final LoginToken lt = lh.getLoginToken();
        final SessionId sid = lt.getSessionId();
        return sid.getIdValueScope();
    }

    private void renewLogin(KVStore store)
        throws SessionAccessException {

        final LoginManager lm = KVStoreImpl.getLoginManager(store);
        final RepNodeId rnId =
            createStore.getRNs(
                createStore.getStorageNodeAgent(0).getStorageNodeId()).get(0);
        final LoginHandle lh = lm.getHandle(rnId);
        final LoginToken lt = lh.getLoginToken();
        lh.renewToken(lt);
    }

    private int findReplicaSNId()
        throws Exception {

        RepNodeAdminAPI rnAdmin = null;
        for (int i = 0; i < numSNs; i++) {
            rnAdmin = createStore.getRepNodeAdmin(i);
            if (!rnAdmin.ping().getReplicationState().isMaster()) {
                return i;
            }
        }
        return 0;
    }

    class TopoResolver implements TopologyResolver {
        private final SNInfo sn1;
        private final SNInfo sn2;

        TopoResolver() {
            sn1 = new SNInfo(createStore.getHostname(),
                             createStore.getRegistryPort(0),
                             new StorageNodeId(1));
            sn2 = new SNInfo(createStore.getHostname(),
                             createStore.getRegistryPort(1),
                             new StorageNodeId(2));
        }

        @Override
        public SNInfo getStorageNode(ResourceId target) {
            if (target instanceof StorageNodeId) {
                final StorageNodeId snid = (StorageNodeId) target;
                if (snid.getStorageNodeId() == 1) {
                    return sn1;
                } else if (snid.getStorageNodeId() == 2) {
                    return sn2;
                }
            } else if (target instanceof RepNodeId) {
                if (isStorageNode(target, sn1.getStorageNodeId())) {
                    return sn1;
                } else if (isStorageNode(target, sn2.getStorageNodeId())) {
                    return sn2;
                }
            }
            return null;
        }

        /*
         * Whether given resource is located at given Storage Node.
         */
        private boolean isStorageNode(ResourceId rnId, StorageNodeId snId) {
            return createStore.getRNs(snId).contains(rnId);
        }

        @Override
        public List<RepNodeId> listRepNodeIds(
            int maxRns) {
            return null;
        }
    }
}
