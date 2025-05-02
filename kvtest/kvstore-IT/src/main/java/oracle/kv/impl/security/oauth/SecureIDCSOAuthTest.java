/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.oauth;

import static oracle.kv.impl.param.ParameterState.GP_LOGIN_CACHE_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.GP_SESSION_EXTEND_ALLOW;
import static oracle.kv.impl.param.ParameterState.GP_USER_EXTERNAL_AUTH;
import static oracle.kv.impl.param.ParameterState.GP_IDCS_OAUTH_AUDIENCE;
import static oracle.kv.util.DDLTestUtils.execStatement;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.IDCSOAuthCredentials;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.ReauthenticateHandler;
import oracle.kv.UnauthorizedException;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.SecureTestBase;
import oracle.kv.impl.util.StorageNodeUtils.SecureOpts;
import oracle.kv.impl.util.SSLTestUtils;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.lob.InputStreamVersion;
import oracle.kv.table.IndexKey;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.util.CreateStore.SecureUser;

public class SecureIDCSOAuthTest extends SecureTestBase {
    private static final String ADMIN_NAME = "admin";
    private static final String ADMIN_PW = "NoSql00__7654321";
    private static final String AUDIENCE = "nosql://test.example.com";

    private static final String TEST_TABLE = "test_table";
    private static final String TEST_TABLE_DEF =
        " (id integer, name string, primary key(id))";
    private static final String TEST_TABLE_INDEX = "test_name";
    private static final String TEST_TABLE_INDEX_FIELD = "name";
    private static final String TEST_TABLE_ROW_JSON_STR =
        "{\"id\":1, \"name\":\"jim\"}";

    private static final String TEST_TABLE_B = "test_table_b";
    private static final String TEST_TABLE_B_DEF =
        " (id integer, name string, salary integer, primary key(id))";


    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        TestUtils.clearTestDirectory();

        /* Add the first admin user */
        users.add(new SecureUser(ADMIN_NAME, ADMIN_PW, true /* admin */));

        /* Enable IDCS OAuth as authentication method */
        userExternalAuth = "IDCSOAUTH";
        /* OAuth needs keystore available, cannot be run with nossl */
        secureOpts = new SecureOpts().setSecure(true);
        startKVStore();

        /*Simply set IDCS OAuth public key as the same one SSL use*/
        setAuthPolicy(ParameterState.GP_IDCS_OAUTH_PUBLIC_KEY_ALIAS + "=" +
                      SSLTestUtils.SSL_KS_ALIAS_DEF);
    }

    @AfterClass
    public static void staticTearDown()
        throws Exception {

        shutdown();
    }

    @Override
    public void setUp() {
    }

    @Override
    public void tearDown() {
    }

    @Test
    public void testLogin()
        throws Exception {

        /*
         * Start by setting OAuth audience.
         */
        setAuthPolicy(GP_IDCS_OAUTH_AUDIENCE + "=" + AUDIENCE);
        assertEquals("IDCSOAUTH", getAuthMethods());

        KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0]);
        Properties props = createDefaultSecProperties();
        kvConfig.setSecurityProperties(props);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
        Date date = dateFormat.parse("2080/01/01");
        String accessToken = IDCSOAuthTestUtils.buildAccessToken(
            Arrays.asList(AUDIENCE),
            "/read",
            date,
            SSLTestUtils.SSL_KS_ALIAS_DEF,
            createStore.getKeyStore().getPath(),
            SSLTestUtils.SSL_KS_PWD_DEF.toCharArray());

        IDCSOAuthCredentials creds = new IDCSOAuthCredentials(accessToken);
        KVStore store = KVStoreFactory.getStore(
            kvConfig, creds, null /* re-authenticate handler*/);
        store.logout();
        store.close();
    }

    @Test
    public void testReauthenticate()
        throws Exception {

        /*
         * Start by changing login cache to have very short timeouts and
         * setting OAuth audience.
         */
        setAuthPolicy(GP_LOGIN_CACHE_TIMEOUT + "=" + "1 SECONDS" +
                ";" + GP_IDCS_OAUTH_AUDIENCE + "=" + AUDIENCE);
        assertEquals("IDCSOAUTH", getAuthMethods());

        KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0]);
        Properties props = createDefaultSecProperties();
        kvConfig.setSecurityProperties(props);

        /* Build access token with 5 SECONDS lifetime */
        Date date = new Date(System.currentTimeMillis() + 2000);
        String accessToken = IDCSOAuthTestUtils.buildAccessToken(
            Arrays.asList(AUDIENCE),
            "/readwrite",
            date,
            SSLTestUtils.SSL_KS_ALIAS_DEF,
            createStore.getKeyStore().getPath(),
            SSLTestUtils.SSL_KS_PWD_DEF.toCharArray());

        IDCSOAuthCredentials creds = new IDCSOAuthCredentials(accessToken);
        OAuthReauth reauth = new OAuthReauth("/readwrite", 5000);

        KVStore store = KVStoreFactory.getStore(kvConfig, creds, reauth);
        Key aKey = Key.createKey("foo");
        store.put(aKey, Value.createValue(new byte[0]));
        Key lobKey = Key.createKey("foo" + kvConfig.getLOBSuffix());
        store.putLOB(lobKey, new ByteArrayInputStream(
            new byte[1000]), null, 0, null);

        try {
            /*
             * Test to be sure that re-authentication happens transparently
             */
            for (int i = 0; i < 6; i++) {
                ValueVersion vv = store.get(aKey);
                assertNotNull(vv);

                if (reauth.attempts > 0) {
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) /* CHECKSTYLE:OFF */ {
                } /* CHECKSTYLE:ON */
            }
            assertTrue(reauth.completed > 0);

            /*
             * Test to be sure that re-authentication works for LOB API.
             */
            for (int i = 0; i < 6; i++) {
                InputStreamVersion isv = store.getLOB(lobKey, null, 0, null);
                assertNotNull(isv);

                if (reauth.attempts > 0) {
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) /* CHECKSTYLE:OFF */ {
                } /* CHECKSTYLE:ON */
            }
            assertTrue(reauth.completed > 0);

            /*
             * Test what happen when re-authentication fails.
             */

            /* Make bad access token */
            reauth.reset("/non-existent-scope");

            for (int i = 0; i < 6; i++) {
                try {
                    ValueVersion vv = store.get(aKey);
                    assertNotNull(vv);
                } catch (AuthenticationRequiredException are) {
                    break;
                }
                if (reauth.attempts > 0) {
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) /* CHECKSTYLE:OFF */ {
                } /* CHECKSTYLE:ON */
            }
            assertTrue(reauth.attempts > 0);
            assertTrue(reauth.completed == 0);
            assertTrue(reauth.afeCount > 0);

            /* Test to be sure that we get ARE when re-authentication fails */
            try {
                store.get(aKey);
                fail("Expected ARE");
            } catch (AuthenticationRequiredException are) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
            assertTrue(reauth.afeCount > 0);

            /*
             * Now, tell the reauthenticationHandler to squash AFE so that
             * it pretends to succeed.  We should get ARE here too.
             */
            reauth.setSquashAFE(true);

            try {
                store.get(aKey);
                fail("Expected ARE");
            } catch (AuthenticationRequiredException are) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
            assertTrue(reauth.afeCount > 0);

            /* Return the store to working order */
            accessToken = IDCSOAuthTestUtils.buildAccessToken(
                    Arrays.asList(AUDIENCE),
                    "/readwrite",
                    new Date(System.currentTimeMillis() + 5000),
                    SSLTestUtils.SSL_KS_ALIAS_DEF,
                    createStore.getKeyStore().getPath(),
                    SSLTestUtils.SSL_KS_PWD_DEF.toCharArray());
            creds = new IDCSOAuthCredentials(accessToken);
            store.login(creds);
            ValueVersion vv = store.get(aKey);
            assertNotNull(vv);
        } finally {
            store.delete(aKey);
            store.deleteLOB(lobKey,  null, 0, null);
            store.logout();
            store.close();
        }
    }

    @Test
    public void testIDCSOAuthScopes()
        throws Exception {

        /*
         * Start by setting OAuth audience.
         */
        setAuthPolicy(GP_IDCS_OAUTH_AUDIENCE + "=" + AUDIENCE);
        assertEquals("IDCSOAUTH", getAuthMethods());

        /* Create test table and populate one row */
        final LoginCredentials adminCreds =
            new PasswordCredentials(ADMIN_NAME, ADMIN_PW.toCharArray());
        KVStore adminUserStore = createStore.getSecureStore(adminCreds);
        execStatement(adminUserStore, "grant readwrite to user " + ADMIN_NAME);
        createTestTable(adminUserStore);
        final Row row = createOneRowForTable(adminUserStore, TEST_TABLE);
        final IndexKey idxKey =
            row.getTable().getIndex(TEST_TABLE_INDEX).createIndexKey();
        assertNotNull(adminUserStore.getTableAPI().put(row, null, null));

        KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0]);
        Properties props = createDefaultSecProperties();
        kvConfig.setSecurityProperties(props);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
        Date date = dateFormat.parse("2080/01/01");

        /* test non-existent scope */
        String accessToken = IDCSOAuthTestUtils.buildAccessToken(
            Arrays.asList(AUDIENCE),
            "non-existent-scope",
            date,
            SSLTestUtils.SSL_KS_ALIAS_DEF,
            createStore.getKeyStore().getPath(),
            SSLTestUtils.SSL_KS_PWD_DEF.toCharArray());

        IDCSOAuthCredentials creds = new IDCSOAuthCredentials(accessToken);
        KVStore store;
        try {
            store = KVStoreFactory.getStore(
                kvConfig, creds, null /* re-authenticate handler*/);
            fail("Expected AFE");
        } catch (AuthenticationFailureException afe) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        /* test /tables.ddl scope */
        accessToken = IDCSOAuthTestUtils.buildAccessToken(
            Arrays.asList(AUDIENCE),
            "/tables.ddl",
            date,
            SSLTestUtils.SSL_KS_ALIAS_DEF,
            createStore.getKeyStore().getPath(),
            SSLTestUtils.SSL_KS_PWD_DEF.toCharArray());
        creds = new IDCSOAuthCredentials(accessToken);
        store = KVStoreFactory.getStore(
            kvConfig, creds, null /* re-authenticate handler*/);
        testValidTableDDLOps(store);
        testDeniedSecureDDLOps(store);
        try {
            testDeniedReadOps(store, row, idxKey);
            testDeniedInsertOps(store, row);
            testDeniedDeleteOps(store, row);
        } finally {
            cleanOneRowFromTable(adminUserStore, row);
            store.logout();
            store.close();
        }

        /* test /read scope */
        accessToken = IDCSOAuthTestUtils.buildAccessToken(
            Arrays.asList(AUDIENCE),
            "/read",
            date,
            SSLTestUtils.SSL_KS_ALIAS_DEF,
            createStore.getKeyStore().getPath(),
            SSLTestUtils.SSL_KS_PWD_DEF.toCharArray());
        creds = new IDCSOAuthCredentials(accessToken);
        store = KVStoreFactory.getStore(
            kvConfig, creds, null /* re-authenticate handler*/);
        testDeniedTableDDLOps(store);
        assertNotNull(adminUserStore.getTableAPI().put(row, null, null));
        try {
            testValidReadOps(store, row, idxKey);
            testDeniedInsertOps(store, row);
            testDeniedDeleteOps(store, row);
        } finally {
            cleanOneRowFromTable(adminUserStore, row);
            store.logout();
            store.close();
        }

        /* test /readwrite scope */
        accessToken = IDCSOAuthTestUtils.buildAccessToken(
                Arrays.asList(AUDIENCE),
                "/readwrite",
                date,
                SSLTestUtils.SSL_KS_ALIAS_DEF,
                createStore.getKeyStore().getPath(),
                SSLTestUtils.SSL_KS_PWD_DEF.toCharArray());
        creds = new IDCSOAuthCredentials(accessToken);
        store = KVStoreFactory.getStore(
            kvConfig, creds, null /* re-authenticate handler*/);
        testDeniedTableDDLOps(store);
        assertNotNull(adminUserStore.getTableAPI().put(row, null, null));
        try {
            testValidReadOps(store, row, idxKey);

            cleanOneRowFromTable(adminUserStore, row);
            testValidInsertOps(store, adminUserStore, row);

            assertNotNull(adminUserStore.getTableAPI().put(row, null, null));
            testValidDeleteOps(store, adminUserStore, row);
        } finally {
            cleanOneRowFromTable(adminUserStore, row);
            store.logout();
            store.close();
        }

        /* test /readwrite and /tables.ddl scopes */
        accessToken = IDCSOAuthTestUtils.buildAccessToken(
                Arrays.asList(AUDIENCE),
                "/readwrite /tables.ddl",
                date,
                SSLTestUtils.SSL_KS_ALIAS_DEF,
                createStore.getKeyStore().getPath(),
                SSLTestUtils.SSL_KS_PWD_DEF.toCharArray());
        creds = new IDCSOAuthCredentials(accessToken);
        store = KVStoreFactory.getStore(
            kvConfig, creds, null /* re-authenticate handler*/);
        assertNotNull(adminUserStore.getTableAPI().put(row, null, null));
        testValidTableDDLOps(store);
        try {
            testValidReadOps(store, row, idxKey);

            cleanOneRowFromTable(adminUserStore, row);
            testValidInsertOps(store, adminUserStore, row);

            assertNotNull(adminUserStore.getTableAPI().put(row, null, null));
            testValidDeleteOps(store, adminUserStore, row);
        } finally {
            cleanOneRowFromTable(adminUserStore, row);
            store.logout();
            store.close();
        }
    }

    @Test
    public void testOAuthPolicy()
        throws Exception {

        assertEquals("IDCSOAUTH", getAuthMethods());
        assertFalse(getSessionExtensionAllow());

        /* cannot enable session extension if IDCS OAuth is already enabled */
        try {
            setAuthPolicy(GP_USER_EXTERNAL_AUTH + "=IDCSOAUTH" + ";" +
                          GP_SESSION_EXTEND_ALLOW + "=true");
            fail("expect afe");
        } catch (AdminFaultException afe) {
            assertThat("cannot enable session extension",
                        afe.getMessage(),
                        containsString("Cannot enable session extension"));
        }

        /* cannot enable Kerberos while IDCS OAuth is enabled */
        try {
            setAuthPolicy(GP_USER_EXTERNAL_AUTH + "=KERBEROS,IDCSOAUTH");
            fail("expect ice");
        } catch (AdminFaultException afe) {
            assertThat("cannot enable multiple authentication",
                        afe.getMessage(),
                        containsString("Cannot enable multiple external"));
        }

        /* cannot enable session extension while IDCS OAuth is enabled */
        try {
            setAuthPolicy(GP_SESSION_EXTEND_ALLOW + "=true");
            fail("expect ice");
        } catch (AdminFaultException afe) {
            assertThat("cannot enable session extension",
                        afe.getMessage(),
                        containsString("Cannot enable session extension"));
        }

        /* disable IDCS OAuth and enable session extension */
        setAuthPolicy(GP_USER_EXTERNAL_AUTH + "=NONE" + ";" +
                      GP_SESSION_EXTEND_ALLOW + "=true");
        assertEquals("NONE", getAuthMethods());
        assertTrue(getSessionExtensionAllow());

        /* enable IDCS OAuth without disable session extension */
        try {
            setAuthPolicy(GP_USER_EXTERNAL_AUTH + "=IDCSOAUTH");
            fail("expect ice");
        } catch (AdminFaultException afe) {
            assertThat("cannot enable session extension",
                        afe.getMessage(),
                        containsString("session extension must be disabled"));
        }
        setAuthPolicy(GP_USER_EXTERNAL_AUTH + "=IDCSOAUTH" + ";" +
                      GP_SESSION_EXTEND_ALLOW + "=false");
        assertEquals("IDCSOAUTH", getAuthMethods());
        assertFalse(getSessionExtensionAllow());
    }

    /* Create the default test properties for security configuration */
    private Properties createDefaultSecProperties() {
        Properties result = new Properties();
        addTransportProps(result);
        return result;
    }

    private void createTestTable(KVStore store)
        throws Exception {

        execStatement(store,
                      "create table " + TEST_TABLE + TEST_TABLE_DEF);
        execStatement(store,
                      "create index " + TEST_TABLE_INDEX + " on " +
                      TEST_TABLE + " (" + TEST_TABLE_INDEX_FIELD + ")");
        Table testTable = store.getTableAPI().getTable(TEST_TABLE);
        assertNotNull(testTable);
        assertNotNull(testTable.getIndex(TEST_TABLE_INDEX));
    }

    private void testValidTableDDLOps(KVStore store)
        throws Exception {

        execStatement(store,
                      "create table " + TEST_TABLE_B + TEST_TABLE_B_DEF);
        execStatement(store,
                      "create index " + TEST_TABLE_INDEX + " on " +
                      TEST_TABLE_B + " (" + TEST_TABLE_INDEX_FIELD + ")");
        Table testTable = store.getTableAPI().getTable(TEST_TABLE_B);
        assertNotNull(testTable);
        assertNotNull(testTable.getIndex(TEST_TABLE_INDEX));
        execStatement(store,
            "drop index " + TEST_TABLE_INDEX + " on " + TEST_TABLE_B);
        testTable = store.getTableAPI().getTable(TEST_TABLE_B);
        assertNull(testTable.getIndex(TEST_TABLE_INDEX));
        execStatement(store, "drop table " + TEST_TABLE_B);
        testTable = store.getTableAPI().getTable(TEST_TABLE_B);
        assertNull(testTable);
    }

    private void testDeniedTableDDLOps(KVStore store)
        throws Exception {

        try {
            execStatement(store,
                "create table " + TEST_TABLE_B + TEST_TABLE_B_DEF);
            fail("expected UnauthorizedException");
        } catch (UnauthorizedException ue) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        try {
            execStatement(store, "create index " + TEST_TABLE_INDEX + " on " +
                TEST_TABLE + " (" + TEST_TABLE_INDEX_FIELD + ")");
            fail("expected UnauthorizedException");
        } catch (UnauthorizedException ue) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
    }

    private void testDeniedSecureDDLOps(KVStore store)
        throws Exception {

        try {
            execStatement(store, "create user testuser" +
                " IDENTIFIED BY \"1234\"");
            fail("expected UnauthorizedException");
        } catch (UnauthorizedException ue) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
        try {
            execStatement(store, "create role testrole");
            fail("expected UnauthorizedException");
        } catch (UnauthorizedException ue) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
        try {
            execStatement(store, "grant sysadmin to user " + ADMIN_NAME);
            fail("expected UnauthorizedException");
        } catch (UnauthorizedException ue) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
        try {
            execStatement(store, "revoke sysadmin from user " + ADMIN_NAME);
            fail("expected UnauthorizedException");
        } catch (UnauthorizedException ue) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
        try {
            execStatement(store, "drop user " + ADMIN_NAME);
            fail("expected UnauthorizedException");
        } catch (UnauthorizedException ue) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
        try {
            execStatement(store, "drop role testrole");
            fail("expected UnauthorizedException");
        } catch (UnauthorizedException ue) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
    }

    /* Creates a row for test table */
    private static Row createOneRowForTable(KVStore store, String table) {
        final Table testTable = store.getTableAPI().getTable(table);
        assertNotNull(testTable);
        return testTable.createRowFromJson(TEST_TABLE_ROW_JSON_STR, true);
    }

    private static void cleanOneRowFromTable(KVStore store, Row row) {
        final PrimaryKey key = row.createPrimaryKey();
        store.getTableAPI().delete(key, null, null);
    }

    private static boolean getSessionExtensionAllow()
        throws Exception {

        final CommandServiceAPI cs = createStore.getAdmin();
        final ParameterMap map = cs.getParameters().getGlobalParams().
            getGlobalSecurityPolicies();
        return map.get(GP_SESSION_EXTEND_ALLOW).asBoolean();
    }

    /**
     * A simple re-authentication handler that provides some tracking
     * information so that we can see whether it gets called.
     */
    private static class OAuthReauth implements ReauthenticateHandler {
        private int attempts;
        private int completed;
        private int afeCount;
        private String scope;
        private boolean squashAFE;
        private final long sessionLifeTime;

        private OAuthReauth(String scope, long lifeTime) {
            reset(scope);
            sessionLifeTime = lifeTime;
        }

        private void setSquashAFE(boolean squashIt) {
            squashAFE = squashIt;
        }

        private void reset(String newScope) {
            attempts = 0;
            completed = 0;
            afeCount = 0;
            scope = newScope;
            squashAFE = false;
        }

        @Override
        public void reauthenticate (KVStore store) {
            attempts++;
            try {
                String accessToken = IDCSOAuthTestUtils.buildAccessToken(
                    Arrays.asList(AUDIENCE),
                    scope,
                    new Date(System.currentTimeMillis() + sessionLifeTime),
                    SSLTestUtils.SSL_KS_ALIAS_DEF,
                    createStore.getKeyStore().getPath(),
                    SSLTestUtils.SSL_KS_PWD_DEF.toCharArray());
                store.login(new IDCSOAuthCredentials(accessToken));
                completed++;
            } catch (AuthenticationFailureException afe) {
                afeCount++;
                if (!squashAFE) {
                    throw afe;
                }
            } catch (Exception e) {
                fail("unexpected exception: " + e);
            }
        }
    }
}
