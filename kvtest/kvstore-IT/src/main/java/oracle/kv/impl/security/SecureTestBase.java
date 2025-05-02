/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security;

import static oracle.kv.util.CreateStore.mergeParameterMapDefaults;
import static oracle.kv.util.DDLTestUtils.execStatement;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.security.auth.callback.Callback;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.StatementResult;
import oracle.kv.UncaughtExceptionTestBase;
import oracle.kv.Value;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.api.security.OpAccessCheckTestUtils;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableKey;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.KVStorePrivilege.PrivilegeType;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.UserLoginCallbackHandler;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.StorageNodeUtils.KerberosOpts;
import oracle.kv.impl.util.StorageNodeUtils.SecureOpts;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.table.IndexKey;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.util.CreateStore;
import oracle.kv.util.CreateStore.SecureUser;
import oracle.kv.util.CreateStore.ZoneInfo;

/**
 * Base class of Secure KVStore tests that use CreateStore functionality.
 */
public class SecureTestBase extends UncaughtExceptionTestBase {

    protected static final String HOST = "localhost";
    protected static final int START_PORT = 5000;

    /* Derived classes may change these settings before calling startup() */
    protected static final List<SecureUser> users = new ArrayList<SecureUser>();
    protected static int numSNs = 1;
    protected static int repFactor = 1;
    protected static int partitions = 10;
    protected static boolean useThreads = false;
    protected static int[] registryPorts = null;
    protected static String userExternalAuth = null;
    protected static List<ZoneInfo> zoneInfos = null;

    protected static CreateStore createStore = null;
    private static int createStoreCount = 0;

    /* Customized Kerberos principal configuration for SNs */
    protected static KerberosOpts[] krbOpts = null;

    /* Customized Security configuration */
    protected static SecureOpts secureOpts = null;

    protected static void startup()
        throws Exception {
        TestUtils.clearTestDirectory();
        startKVStore();
    }

    protected static void shutdown()
        throws Exception {

        if (createStore != null) {
            createStore.shutdown();
            createStore = null;
        }
    }

    protected static void startKVStore() {
        try {
            if (zoneInfos == null) {
                zoneInfos = ZoneInfo.primaries(repFactor);
            }
            createStoreCount++;
            createStore = new CreateStore("kvtest-" +
                                          testClassName +
                                          "-" + createStoreCount,
                                          START_PORT,
                                          numSNs, /* Storage nodes */
                                          zoneInfos,
                                          partitions, /* Partitions */
                                          1, /* Capacity */
                                          CreateStore.MB_PER_SN,
                                          useThreads,
                                          null, /* mgmtImpl */
                                          true, /* mgmtPortsShared */
                                          true/* secure */,
                                          userExternalAuth);
            for (SecureUser user : users) {
                createStore.addUser(user);
            }
            if (krbOpts != null) {
                createStore.setKrbOpts(krbOpts);
            }

            /* Check if SSL is disabled in this test */
            if (secureOpts == null && TestUtils.isSSLDisabled()) {
                secureOpts = new SecureOpts().setSecure(true).setNoSSL(true);
            }

            if (secureOpts != null) {
                createStore.setSecureOpts(secureOpts);
            }
            createStore.start(false);

            registryPorts = new int[numSNs];
            for (int i = 0; i < numSNs; i++) {
                registryPorts[i] =
                    createStore.getRegistryPort(
                        createStore.getStorageNodeAgent(i).getStorageNodeId());
            }

        } catch (Exception e) {
            throw new RuntimeException(
                "Unexpected exception in createStore: " + e, e);
        }
    }

    /**
     * Given a set of security parameter assignments, joined with a ';'
     * separator, configure the store to use the specified parameters.
     */
    protected static void setAuthPolicy(String params)
        throws RemoteException {

        final ParameterState.Info info = ParameterState.Info.GLOBAL;
        final ParameterState.Scope scope = ParameterState.Scope.STORE;
        final ParameterMap map = parseParams(
            params, info, scope, ParameterState.GLOBAL_TYPE);

        final CommandServiceAPI cs = createStore.getAdmin();

        final int planId =
            cs.createChangeGlobalSecurityParamsPlan("_SetPolicy", map);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
    }

    protected void setPasswordPolicy(String params)
            throws RemoteException {
        final ParameterState.Info info = ParameterState.Info.POLICY;
        final ParameterState.Scope scope = ParameterState.Scope.STORE;
        ParameterMap map = parseParams(params, info, scope, null);
        final CommandServiceAPI cs = createStore.getAdmin();
        cs.setPolicies(mergeParameterMapDefaults(map));
    }

    private static ParameterMap parseParams(String params,
                                            ParameterState.Info info,
                                            ParameterState.Scope scope,
                                            String type) {
        final ParameterMap map = new ParameterMap();

        if (type != null) {
            map.setType(type);
        }

        final String[] paramStrings = params.split(";");
        for (String paramString : paramStrings) {
            final String[] paramSplit = paramString.split("=");
            assertEquals(2, paramSplit.length);
            final String name = paramSplit[0].trim();
            final String value = paramSplit[1].trim();

            final ParameterState pstate = ParameterState.lookup(name);
            assertNotNull(pstate);
            assertFalse(pstate.getReadOnly());
            assertEquals(scope, pstate.getScope());
            assertTrue(pstate.appliesTo(info));

            /* This method will validate the value if necessary */
            map.setParameter(name, value);
        }
        return map;
    }

    /**
     * Log this user into a secured store.
     */
    protected static KVStore loginKVStoreUser(String userName,
                                              String password) {
        LoginCredentials creds =
                new PasswordCredentials(userName, password.toCharArray());
        KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              createStore.getHostname() + ":" +
                              createStore.getRegistryPort());
        kvConfig.setCheckInterval(1, TimeUnit.SECONDS);
        Properties props = new Properties();
        createStore.addTransportProps(props);
        kvConfig.setSecurityProperties(props);

        return KVStoreFactory.getStore(kvConfig, creds, null);
    }

    protected static void addTransportProps(Properties props) {
        createStore.addTransportProps(props);
    }

    protected void changeUserEnabled(String username, boolean enabled)
        throws RemoteException {

        final CommandServiceAPI cs = createStore.getAdmin();

        final int planId =
            cs.createChangeUserPlan("_ChangeEnabled",
                                    username,
                                    enabled,
                                    null, /* newPassword */
                                    false, /* retain */
                                    false /* clearRetainedPassword */);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
    }

    /**
     * Delete a user.  Because there is a restriction against deleting
     * yourself, the caller must provide a login to use.
     */
    protected void deleteUser(String deleteUsername,
                              String adminUsername,
                              String adminPassword) throws Exception {
        final CommandServiceAPI delCs =
            createStore.getAdmin(adminUsername, adminPassword.toCharArray());

        final int planId =
            delCs.createDropUserPlan("_DropUser", deleteUsername);
        delCs.approvePlan(planId);
        delCs.executePlan(planId, false);
        delCs.awaitPlan(planId, 0, null);
        delCs.assertSuccess(planId);
    }

    protected void grantRoles(String user, String... roles)
        throws Exception {

        final CommandServiceAPI cs = createStore.getAdmin();
        final Set<String> roleList = new HashSet<String>();
        Collections.addAll(roleList, roles);
        final int planId =
            cs.createGrantPlan("Grant roles", user, roleList);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
    }

    protected void revokeRoles(String user, String... roles)
        throws Exception {

        final CommandServiceAPI cs = createStore.getAdmin();
        final Set<String> roleList = new HashSet<String>();
        Collections.addAll(roleList, roles);
        final int planId =
            cs.createRevokePlan("Revoke roles", user, roleList);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
    }

    protected Topology getTopology()
        throws RemoteException {
        final CommandServiceAPI cs = createStore.getAdmin();

        return cs.getTopology();
    }

    protected LoginManager getSNALoginManager(int idx) {
        return createStore.getStorageNodeAgent(idx).getLoginManager();
    }

    protected void changeAuthMethods(String authMethods)
        throws Exception {

        final ParameterMap map = new ParameterMap();
        map.setType(ParameterState.GLOBAL_TYPE);
        map.setParameter(ParameterState.GP_USER_EXTERNAL_AUTH, authMethods);

        final CommandServiceAPI cs = createStore.getAdmin();

        final int planId =
            cs.createChangeGlobalSecurityParamsPlan("Change_AuthMethods", map);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
    }

    protected String getAuthMethods()
        throws Exception {

        final CommandServiceAPI cs = createStore.getAdmin();
        final ParameterMap map = cs.getParameters().getGlobalParams().
            getGlobalSecurityPolicies();
        return map.get(ParameterState.GP_USER_EXTERNAL_AUTH).asString();
    }

    /** Return a string that provides the helper hosts for the store */
    protected String getHelperHosts() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numSNs; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(HOST).append(":").append(registryPorts[i]);
        }
        return sb.toString();
    }

    protected static void revokePrivFromRole(KVStore store,
                                             String role,
                                             KVStorePrivilegeLabel... sysPriv)
        throws Exception {
        for (KVStorePrivilegeLabel label : sysPriv) {
            execStatement(store,
                          "revoke " + label + " from " + role);
            assertRoleHasNoPriv(store, role, label.toString());
        }
    }

    protected static void revokePrivFromRole(KVStore store,
                                             String role,
                                             String table,
                                             KVStorePrivilegeLabel... tablePriv)
        throws Exception {
        for (KVStorePrivilegeLabel label : tablePriv) {
            execStatement(store,
                          "revoke " + label + " on " + table + " from " + role);
            assertRoleHasNoPriv(store, role, toTablePrivStr(label, table));
        }
    }

    protected static void grantPrivToRole(KVStore store,
                                          String role,
                                          KVStorePrivilegeLabel... sysPriv)
        throws Exception {
        for (KVStorePrivilegeLabel label : sysPriv) {
            execStatement(store, "grant " + label + " to " + role);
            assertRoleHasPriv(store, role, label.toString());
        }
    }

    protected static void grantPrivToRole(KVStore store,
                                          String role,
                                          String table,
                                          KVStorePrivilegeLabel... tablePriv)
        throws Exception {
        for (KVStorePrivilegeLabel label : tablePriv) {
            execStatement(store,
                          "grant " + label + " on " + table + " to " + role);
            assertRoleHasPriv(store, role, toTablePrivStr(label, table));
        }
    }

    protected static void assertRoleHasNoPriv(KVStore store,
                                              String role,
                                              String privStr) {
        final StatementResult result = store.executeSync("show role " + role);
        assertThat(result.getResult(), not(containsString(privStr)));
    }

    /**
     * Returns a KV value from the specified row.
     */
    protected static Value getKVValueFromRow(Row row) {
        final RowImpl rowImpl = (RowImpl) row;
        return rowImpl.createValue();
    }

    protected void testDeniedInsertOps(KVStore store, Row row)
        throws Exception {

        OpAccessCheckTestUtils.testDeniedTableInsertOps(store, row);
        final Key kvKey =
            TableKey.createKey(row.getTable(), row, false).getKey();
        OpAccessCheckTestUtils.testDeniedPutOps(store, kvKey,
                                                getKVValueFromRow(row));
    }

    protected void testDeniedReadOps(KVStore store, Row row, IndexKey idxKey) {
        final PrimaryKey key = row.createPrimaryKey();
        final Key kvKey =
            TableKey.createKey(row.getTable(), row, false).getKey();

        OpAccessCheckTestUtils.testDeniedTableReadOps(store, key, idxKey);
        OpAccessCheckTestUtils.testDeniedReadOps(store, kvKey, false);
    }

    protected void testDeniedDeleteOps(KVStore store, Row row)
        throws Exception {

        final PrimaryKey key = row.createPrimaryKey();
        final Key kvKey =
            TableKey.createKey(row.getTable(), row, false).getKey();

        OpAccessCheckTestUtils.testDeniedTableDeleteOps(store, key);
        OpAccessCheckTestUtils.testDeniedDeleteOps(store, kvKey, false);
    }

    protected void testValidInsertOps(KVStore testStore,
                                      KVStore helperStore,
                                      Row row)
        throws Exception {

        final Key kvKey =
            TableKey.createKey(row.getTable(), row, false).getKey();

        OpAccessCheckTestUtils.testValidTableInsertOps(
            testStore, helperStore, row);

        OpAccessCheckTestUtils.testValidPutOps(
            testStore, helperStore, kvKey, getKVValueFromRow(row));
    }

    protected void testValidDeleteOps(KVStore testStore,
                                      KVStore helperStore,
                                      Row row)
        throws Exception {

        final PrimaryKey key = row.createPrimaryKey();
        final Key kvKey =
            TableKey.createKey(row.getTable(), row, false).getKey();

        OpAccessCheckTestUtils.testValidTableDeleteOps(
            testStore, helperStore, key);

        OpAccessCheckTestUtils.testValidDeleteOps(
            testStore, helperStore, kvKey, getKVValueFromRow(row));
    }

    protected void testValidReadOps(KVStore store, Row row, IndexKey idxKey) {
        final PrimaryKey key = row.createPrimaryKey();
        final Key kvKey =
            TableKey.createKey(row.getTable(), row, false).getKey();

        OpAccessCheckTestUtils.testValidTableReadOps(store, key, idxKey);
        OpAccessCheckTestUtils.testValidReadOps(store, kvKey, false);
    }

    private static void assertRoleHasPriv(KVStore store,
                                          String role,
                                          String privStr) {
        final StatementResult result = store.executeSync("show role " + role);
        assertThat(result.getResult(), containsString(privStr));
    }

    private static String toTablePrivStr(KVStorePrivilegeLabel tablePriv,
                                         String table) {
        assertEquals(tablePriv.getType(), PrivilegeType.TABLE);
        return String.format("%s(%s)", tablePriv, table);
    }

    public static class ErrorMsgCallbackHandler
        extends UserLoginCallbackHandler {

        public ErrorMsgCallbackHandler(Logger logger) {
            super(logger);
        }

        public ErrorMsgCallbackHandler() {
            super(null);
        }

        private Level level;
        private String errorMsg;

        public Level getLevel() {
            return level;
        }

        public String getErrorMsg() {
            return errorMsg;
        }

        @Override
        public void handle(Callback callback) {
            if (callback instanceof LoggingCallback) {
                final LoggingCallback ck = (LoggingCallback) callback;
                level = ck.getLevel();
                errorMsg = ck.getMessage();
            }
        }
}
}
