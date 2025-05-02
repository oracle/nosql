/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.security;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static oracle.kv.KVStoreConfig.DEFAULT_REQUEST_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.GP_LOGIN_CACHE_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.GP_SESSION_EXTEND_ALLOW;
import static oracle.kv.impl.param.ParameterState.GP_SESSION_TIMEOUT;
import static oracle.kv.util.DDLTestUtils.execStatement;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

import javax.net.ssl.SSLHandshakeException;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.FaultException;
import oracle.kv.KVSecurityConstants;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreException;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.ReauthenticateHandler;
import oracle.kv.StatementResult;
import oracle.kv.UnauthorizedException;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.RequestDispatcherImpl;
import oracle.kv.impl.async.EndpointGroup;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.SecureTestBase;
import oracle.kv.impl.security.login.UserLoginManager;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.SSLTestUtils;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.AsyncControl;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.lob.InputStreamVersion;
import oracle.kv.query.PreparedStatement;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.util.CreateStore.SecureUser;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This is a basic test to make sure that a secure KVStore configuration
 * can be created and accessed, as well as a variety of error cases.
 *
 * TODO: Add test cases:
 *   - client with no protocol spec can connect to server with default config
 *   - client with SSLv3 protocol spec cannnot connect to server with TLSv1
 */
public class SecureKVTest extends SecureTestBase {

    private static final String USER_NAME = "admin";
    private static final String USER_PW = "NoSql00__7654321";

    private static final String TABLE = "test_table";
    private static final String TABLE_DEF =
        " (id integer, name string, primary key(id))";
    private static final String ROW_JSON = "{\"id\":1, \"name\":\"jim\"}";

    @BeforeClass
    public static void staticSetUp()
        throws Exception {

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

        /*
         * The CreateStore model executes the SNA in the same process as
         * the client.  When we run a tests that deliberately botches the
         * client transport config, we need to repair that config before
         * running additional tests.
         */
        createStore.getStorageNodeAgent(0).resetRMISocketPolicies();

    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
    }

    /**
     * This tests a successful login/logout sequence
     */
    @Test
    public void testLogin() {
        try {
            LoginCredentials creds =
                new PasswordCredentials(USER_NAME,USER_PW.toCharArray());
            KVStoreConfig kvConfig =
                new KVStoreConfig(createStore.getStoreName(),
                                  HOST + ":" + registryPorts[0]);
            Properties props = new Properties();
            addTransportProps(props);
            kvConfig.setSecurityProperties(props);

            KVStore store = KVStoreFactory.getStore(kvConfig, creds, null);
            store.logout();
            store.close();
        } catch (Exception e) {
            fail("unexpected exception: " + e);
        }
    }

    /**
     * This tests a successful login/logout sequence using login properties
     */
    @Test
    public void testLoginSecurityProps() {
        LoginCredentials creds =
            new PasswordCredentials(USER_NAME,USER_PW.toCharArray());
        KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0]);

        Properties props = new Properties();
        addTransportProps(props);
        kvConfig.setSecurityProperties(props);

        try {
            KVStore store = KVStoreFactory.getStore(kvConfig, creds, null);
            store.logout();
            store.close();
        } catch (Exception e) {
            fail("unexpected exception: " + e);
        }
    }

    /**
     * This tests that setting the oracle.kv.security property to a
     * non-existent file generates an IllegalArgumentException
     */
    @Test
    @SuppressWarnings("unused")
    public void testSecurityPropsFileException() {

        final File testDir = TestUtils.getTestDir();
        final File propFile = new File(testDir, "props.security.XXXX");

        /* Set system property and allow KVStore to find it */
        System.setProperty(KVSecurityConstants.SECURITY_FILE_PROPERTY,
                           propFile.getPath());

        try {
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0]);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            /* expected */
        } finally {
            System.clearProperty(KVSecurityConstants.SECURITY_FILE_PROPERTY);
        }
    }

    /**
     * This tests a successful login/logout sequence using security properties
     * file.
     */
    @Test
    public void testLoginSecurityPropsFile()
        throws Exception {

        LoginCredentials creds =
            new PasswordCredentials(USER_NAME, USER_PW.toCharArray());

        Properties props = new Properties();
        addTransportProps(props);

        final File testDir = TestUtils.getTestDir();
        final File propFile = new File(testDir, "props.security");

        ConfigUtils.storeProperties(props, "test properties", propFile);

        /* Set system property and allow KVStore to find it */
        System.setProperty(KVSecurityConstants.SECURITY_FILE_PROPERTY,
                           propFile.getPath());

        KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0]);

        try {
            KVStore store = KVStoreFactory.getStore(kvConfig, creds, null);
            store.logout();
            store.close();
        } catch (Exception e) {
            fail("unexpected exception: " + e);
        } finally {
            System.clearProperty(KVSecurityConstants.SECURITY_FILE_PROPERTY);
        }
    }

    /**
     * This test that specifying the wrong store name yields a fault exception
     */
    @Test
    public void testWrongStoreName() {
        LoginCredentials creds =
            new PasswordCredentials(USER_NAME, USER_PW.toCharArray());
        KVStoreConfig kvConfig =
            new KVStoreConfig("WrongStoreName", HOST + ":" + registryPorts[0]);
        Properties props = new Properties();
        addTransportProps(props);
        kvConfig.setSecurityProperties(props);

        try {
            KVStoreFactory.getStore(kvConfig, creds, null);
            fail("expected exception");
        } catch (FaultException fe) {
            assertTrue(fe.getMessage().contains(
                           "Could not establish an initial login"));
            assertTrue(fe.getMessage().contains(
                           "ignored non-matching store name"));
        }
    }

    /**
     * This test that specifying the wrong port yields a fault exception
     */
    @Test
    public void testWrongPort() {
        LoginCredentials creds =
            new PasswordCredentials(USER_NAME, USER_PW.toCharArray());
        KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0] + 1);
        Properties props = new Properties();
        addTransportProps(props);
        kvConfig.setSecurityProperties(props);

        try {
            KVStoreFactory.getStore(kvConfig, creds, null);
            fail("expected exception");
        } catch (FaultException fe) {
            assertTrue(fe.getMessage().contains(
                           "Could not establish an initial login"));
        }
    }

    /**
     * This test that a bad username/password combo yields AFE
     */
    @Test
    public void testAuthFailure() {
        LoginCredentials creds =
            new PasswordCredentials(USER_NAME + "X", USER_PW.toCharArray());
        KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0]);
        Properties props = new Properties();
        addTransportProps(props);
        kvConfig.setSecurityProperties(props);

        try {
            KVStoreFactory.getStore(kvConfig, creds, null);
            fail("expected exception");
        } catch (AuthenticationFailureException afe) {
        }
    }

    /**
     * This test that a missing login yields ARE
     */
    @Test
    public void testAuthRequired() {
        KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0]);
        Properties props = new Properties();
        addTransportProps(props);
        kvConfig.setSecurityProperties(props);

        try {
            KVStoreFactory.getStore(kvConfig);
            fail("expected exception");
        } catch (AuthenticationRequiredException are) {
        }
    }

    /**
     * This test that a configuration with a bad truststore yields a fault
     * exception.
     */
    @Test
    public void testBadTrustStore() {
        Assume.assumeFalse(secureOpts != null && secureOpts.noSSL());
        LoginCredentials creds =
            new PasswordCredentials(USER_NAME,USER_PW.toCharArray());
        KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0]);
        Properties props = new Properties();
        props.put(KVSecurityConstants.TRANSPORT_PROPERTY,
                  KVSecurityConstants.SSL_TRANSPORT_NAME);
        props.put(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                  createStore.getTrustStore().getPath() + "XXX");
        kvConfig.setSecurityProperties(props);

        try {
            KVStoreFactory.getStore(kvConfig, creds, null);
            fail("expected exception");
        } catch (FaultException fe) {
            assertSame(IllegalStateException.class, fe.getCause().getClass());
        }
    }

    /**
     * This test that a configuration without a truststore yields a fault
     * exception, since we're using self-signed certs.
     */
    @Test
    public void testNoTrustStore() {
        Assume.assumeFalse(secureOpts != null && secureOpts.noSSL());

        LoginCredentials creds =
            new PasswordCredentials(USER_NAME,USER_PW.toCharArray());
        KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0]);
        Properties props = new Properties();
        props.put(KVSecurityConstants.TRANSPORT_PROPERTY,
                  KVSecurityConstants.SSL_TRANSPORT_NAME);
        kvConfig.setSecurityProperties(props);

        try {
            KVStoreFactory.getStore(kvConfig, creds, null);
            fail("expected exception");
        } catch (AuthenticationFailureException afe) {
            assertTrue(afe.getCause() instanceof SSLHandshakeException);
        }
    }

    @Test
    public void testPasswordlessPKCS12TrustStore() {
        Assume.assumeFalse(secureOpts != null && secureOpts.noSSL());

        final LoginCredentials creds =
            new PasswordCredentials(USER_NAME, USER_PW.toCharArray());
        final KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0]);
        Properties props = new Properties();
        props.put(KVSecurityConstants.TRANSPORT_PROPERTY,
                  KVSecurityConstants.SSL_TRANSPORT_NAME);
        props.put(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                  SSLTestUtils.getSSLFilePath(SSLTestUtils.SSL_CTS_NAME));
        props.put(KVSecurityConstants.SSL_TRUSTSTORE_TYPE_PROPERTY, "PKCS12");
        kvConfig.setSecurityProperties(props);
        final KVStore store = KVStoreFactory.getStore(kvConfig, creds, null);
        tearDowns.add(() -> store.logout());
        tearDowns.add(() -> store.close());

        createStore.getStorageNodeAgent(0).resetRMISocketPolicies();

        props.remove(KVSecurityConstants.SSL_TRUSTSTORE_TYPE_PROPERTY);
        kvConfig.setSecurityProperties(props);
        final KVStore store2 = KVStoreFactory.getStore(kvConfig, creds, null);
        tearDowns.add(() -> store2.logout());
        tearDowns.add(() -> store2.close());
    }

    @Test
    public void testPasswordPKCS12TrustStore()
        throws Exception {

        Assume.assumeFalse(secureOpts != null && secureOpts.noSSL());

        final LoginCredentials creds =
            new PasswordCredentials(USER_NAME, USER_PW.toCharArray());
        final KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0]);
        Properties props = new Properties();
        props.put(KVSecurityConstants.TRANSPORT_PROPERTY,
                  KVSecurityConstants.SSL_TRANSPORT_NAME);
        props.put(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                  SSLTestUtils.getSSLFilePath(SSLTestUtils.SSL_PASS_CTS_NAME));
        kvConfig.setSecurityProperties(props);
        try {
            KVStoreFactory.getStore(kvConfig, creds, null);
            fail("expected exception");
        } catch (AuthenticationFailureException afe) {
            assertTrue(afe.getCause() instanceof SSLHandshakeException);
        }

        createStore.getStorageNodeAgent(0).resetRMISocketPolicies();
        props.put(KVSecurityConstants.SSL_TRUSTSTORE_PASSWORD_PROPERTY,
                  SSLTestUtils.SSL_KS_PWD_DEF);
        kvConfig.setSecurityProperties(props);
        final KVStore store = KVStoreFactory.getStore(kvConfig, creds, null);
        tearDowns.add(() -> store.logout());
        tearDowns.add(() -> store.close());

        createStore.getStorageNodeAgent(0).resetRMISocketPolicies();

        /*
         * Reuse the password file store in SSLTestUtils since all of them
         * are created with the same password, see test/ssl/make-ks.sh.
         */
        props.put(KVSecurityConstants.AUTH_PWDFILE_PROPERTY,
                  SSLTestUtils.getSSLFilePath(SSLTestUtils.SSL_PW_NAME));
        props.put(KVSecurityConstants.SSL_TRUSTSTORE_PASSWORD_ALIAS_PROPERTY,
                  SSLTestUtils.SSL_TS_ALIAS_DEF);
        kvConfig.setSecurityProperties(props);
        final KVStore store2 = KVStoreFactory.getStore(kvConfig, creds, null);
        tearDowns.add(() -> store2.logout());
        tearDowns.add(() -> store2.close());
    }

    @Test
    public void testJKSTrustStore() {
        Assume.assumeFalse(secureOpts != null && secureOpts.noSSL());
        /* Create store */
        final LoginCredentials creds =
            new PasswordCredentials(USER_NAME, USER_PW.toCharArray());
        final KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0]);
        Properties props = new Properties();
        props.put(KVSecurityConstants.TRANSPORT_PROPERTY,
                  KVSecurityConstants.SSL_TRANSPORT_NAME);
        props.put(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                  SSLTestUtils.getSSLFilePath(SSLTestUtils.SSL_JKS_CTS_NAME));
        props.put(KVSecurityConstants.SSL_TRUSTSTORE_TYPE_PROPERTY, "JKS");
        kvConfig.setSecurityProperties(props);
        final KVStore store = KVStoreFactory.getStore(kvConfig, creds, null);
        tearDowns.add(() -> store.logout());
        tearDowns.add(() -> store.close());

        props.remove(KVSecurityConstants.SSL_TRUSTSTORE_TYPE_PROPERTY);
        kvConfig.setSecurityProperties(props);
        final KVStore store2 = KVStoreFactory.getStore(kvConfig, creds, null);
        tearDowns.add(() -> store2.logout());
        tearDowns.add(() -> store2.close());
    }

    @Test
    public void testNoTransport() {
        LoginCredentials creds =
            new PasswordCredentials(USER_NAME,USER_PW.toCharArray());
        KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0]);

        try {
            KVStoreFactory.getStore(kvConfig, creds, null);
        } catch (FaultException fe) {
            assertSame(KVStoreException.class, fe.getCause().getClass());
        }
    }

    @Test
    public void testNoAuthNoTransport() {
        Assume.assumeFalse(secureOpts != null && secureOpts.noSSL());
        KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0]);

        try {
            KVStoreFactory.getStore(kvConfig);
        } catch (FaultException fe) {
            assertEquals(KVStoreException.class, fe.getCause().getClass());
        }
    }

    /**
     * This tests that session renewal works.
     * @throws Exception
     */
    @Test
    public void testSessionRenewal() throws Exception {
        try {
            grantRoles(USER_NAME, "readwrite");
            /*
             * Start by changing authentication policy to have very short
             * timeouts, but to allow session extension.
             */
            setAuthPolicy(GP_SESSION_TIMEOUT + "=" + "5 SECONDS" +
                          ";" + GP_LOGIN_CACHE_TIMEOUT + "=" + "1 SECONDS" +
                          ";" + GP_SESSION_EXTEND_ALLOW +"=" + "true");

            LoginCredentials creds =
                new PasswordCredentials(USER_NAME, USER_PW.toCharArray());
            KVStoreConfig kvConfig =
                new KVStoreConfig(createStore.getStoreName(),
                                  HOST + ":" + registryPorts[0]);
            Properties props = new Properties();
            addTransportProps(props);
            kvConfig.setSecurityProperties(props);

            KVStore store = KVStoreFactory.getStore(kvConfig, creds, null);
            Key aKey = Key.createKey("foo");
            store.put(aKey, Value.createValue(new byte[0]));

            /*
             * Test to be sure that session refresh occurs automatically.
             * run 20 iterations, which should cause multiple renewals to
             * be required.
             */
            for (int i = 0; i < 20; i++) {
                ValueVersion vv = store.get(aKey);
                assertNotNull(vv);

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    /* ignore */
                }
            }

            store.logout();
            store.close();
        } finally {
            revokeRoles(USER_NAME, "readwrite");
        }
    }

    @Test
    public void testSessionUpdate() throws Exception {
        try {
            grantRoles(USER_NAME, "writeonly");
            LoginCredentials creds =
                new PasswordCredentials(USER_NAME, USER_PW.toCharArray());
            KVStoreConfig kvConfig =
                new KVStoreConfig(createStore.getStoreName(),
                                  HOST + ":" + registryPorts[0]);
            Properties props = new Properties();
            addTransportProps(props);
            kvConfig.setSecurityProperties(props);

            KVStore store = KVStoreFactory.getStore(kvConfig, creds, null);
            Key aKey = Key.createKey("foo");
            store.put(aKey, Value.createValue(new byte[0]));
            ValueVersion vv = null;
            try {
                vv = store.get(aKey);
                fail("expected exception");
            } catch (UnauthorizedException ue) {
            }
            assertNull(vv);

            /* No need to login again */
            grantRoles(USER_NAME, "readonly");
            try {
                vv = store.get(aKey);
            } catch (UnauthorizedException ue) {
                fail("unexpected exception");
            }
            assertNotNull(vv);

            grantRoles(USER_NAME, "readonly");
            store.logout();
            store.close();
        } finally {
            revokeRoles(USER_NAME, "readwrite");
        }
    }

    /**
     * Tests that reauthentication works for KV API.
     */
    @Test
    public void testKVReauthenticate()
        throws Exception {

        testReauthenticate(new TestOperation() {

            private Key getKey() {
                return Key.createKey("foo");
            }

            @Override
            public void doOperation(KVStore store) {
                ValueVersion vv = store.get(getKey());
                assertNotNull(vv);
            }

            @Override
            public void doOperationExpectFail(KVStore store) {
                store.get(getKey());
            }

            @Override
            public void prepareTest(KVStore store) {
                store.put(getKey(), Value.createValue(new byte[0]));
            }
        });
    }

    /**
     * Tests that reauthentication works for LOB API.
     */
    @Test
    public void testLOBReauthenticate()
        throws Exception {

        testReauthenticate(new TestOperation() {

            private Key getKey() {
                KVStoreConfig kvConfig =
                    new KVStoreConfig(createStore.getStoreName(),
                                      HOST + ":" + registryPorts[0]);
                return Key.createKey("foo" + kvConfig.getLOBSuffix());
            }

            @Override
            public void doOperation(KVStore store) {
                InputStreamVersion isv = store.getLOB(getKey(), null, 0, null);
                assertNotNull(isv);
            }

            @Override
            public void doOperationExpectFail(KVStore store) {
                store.getLOB(getKey(), null, 0, null);
            }

            @Override
            public void prepareTest(KVStore store)
                throws Exception {

                store.putLOB(getKey(), new ByteArrayInputStream(new byte[1000]),
                             null, 0, null);
            }
        });
    }

    /**
     * Tests that reauthentication works for basic Table API.
     */
    @Test
    public void testTableReauthenticate()
        throws Exception {

        testReauthenticate(new TestOperation() {

            @Override
            public void doOperation(KVStore store) {
                Table testTable = store.getTableAPI().getTable(TABLE);
                Row row = testTable.createRowFromJson(ROW_JSON, true);
                assertNotNull(store.getTableAPI().
                              get(row.createPrimaryKey(), null));
            }

            @Override
            public void doOperationExpectFail(KVStore store) {
                store.getTableAPI().getTable(TABLE);
            }

            @Override
            public void prepareTest(KVStore store)
                throws Exception {

                execStatement(store, "CREATE TABLE IF NOT EXISTS " +
                              TABLE + TABLE_DEF);
                Table testTable = store.getTableAPI().getTable(TABLE);
                assertNotNull(testTable);
                Row row = testTable.createRowFromJson(ROW_JSON, true);
                assertNotNull(store.getTableAPI().put(row, null, null));
            }
        });
    }

    @Test
    public void testTableReauthenticateAsync() throws Exception {
        Assume.assumeTrue("Test requires async",
                          AsyncControl.serverUseAsync);

        testReauthenticate(new TestOperation() {
            @Override
            public void doOperation(KVStore store) {
                assertNotNull(get(store));
            }

            private Row get(KVStore store) {
                final Table testTable = store.getTableAPI().getTable(TABLE);
                final Row row = testTable.createRowFromJson(ROW_JSON, true);
                try {
                    try {
                        return store.getTableAPI()
                            .getAsync(row.createPrimaryKey(), null)
                            .get(DEFAULT_REQUEST_TIMEOUT, MILLISECONDS);
                    } catch (ExecutionException e) {
                        if (e.getCause() instanceof Exception) {
                            throw (Exception) e.getCause();
                        }
                        throw e;
                    }
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException("Unexpected exception: " + e,
                                               e);
                }
            }

            @Override
            public void doOperationExpectFail(KVStore store) {
                get(store);
            }

            @Override
            public void prepareTest(KVStore store)
                throws Exception {

                execStatement(store, "CREATE TABLE IF NOT EXISTS " +
                              TABLE + TABLE_DEF);
                final Table testTable = store.getTableAPI().getTable(TABLE);
                assertNotNull(testTable);
                final Row row = testTable.createRowFromJson(ROW_JSON, true);
                assertNotNull(store.getTableAPI().put(row, null, null));
            }
        });
    }

    /**
     * Tests that reauthentication works for DDL.
     */
    @Test
    public void testDDLReauthenticate()
        throws Exception {

        testReauthenticate(new TestOperation() {

            @Override
            public void doOperation(KVStore store) {
                String query = "SHOW TABLES";
                PreparedStatement ps = store.prepare(query, null);
                assertNotNull(ps);
                StatementResult sr = store.executeSync(ps);
                assertNotNull(sr);
                assertTrue(sr.isSuccessful());
                assertNotNull(sr.getResult());
            }

            @Override
            public void doOperationExpectFail(KVStore store) {
                String query = "SHOW TABLES";
                PreparedStatement ps = store.prepare(query, null);
                store.executeSync(ps);
            }

            @Override
            public void prepareTest(KVStore store)
                throws Exception {

                execStatement(store, "CREATE TABLE IF NOT EXISTS " +
                              TABLE + TABLE_DEF);
                Table testTable = store.getTableAPI().getTable(TABLE);
                assertNotNull(testTable);
            }
        });
    }

    /**
     * Tests that reauthentication works for DML.
     */
    @Test
    public void testDMLReauthenticate()
        throws Exception {

        testReauthenticate(new TestOperation() {

            @Override
            public void doOperation(KVStore store) {
                String query = "SELECT * FROM " + TABLE;
                PreparedStatement ps = store.prepare(query, null);
                assertNotNull(ps);
                StatementResult sr = store.executeSync(ps);
                assertNotNull(sr);
                assertTrue(sr.isSuccessful());
            }

            @Override
            public void doOperationExpectFail(KVStore store) {
                String query = "SELECT * FROM " + TABLE;
                PreparedStatement ps = store.prepare(query, null);
                store.executeSync(ps);
            }

            @Override
            public void prepareTest(KVStore store)
                throws Exception {

                execStatement(store, "CREATE TABLE IF NOT EXISTS " +
                              TABLE + TABLE_DEF);
                Table testTable = store.getTableAPI().getTable(TABLE);
                assertNotNull(testTable);
                Row row = testTable.createRowFromJson(ROW_JSON, true);
                assertNotNull(store.getTableAPI().put(row, null, null));
            }
        });
    }

    /**
     * Test that login token renewal does not block a thread from the async
     * thread pool. [KVSTORE-946]
     */
    @Test
    public void testAsyncRenewalNotBlocking() throws Exception {

        Assume.assumeTrue("Test requires async", AsyncControl.serverUseAsync);

        /* Grant access */
        grantRoles(USER_NAME, "readwrite");
        tearDowns.add(() -> {
                try {
                    revokeRoles(USER_NAME, "readwrite");
                } catch (Exception e) {
                    throw new RuntimeException("Unexpected exception: " + e, e);
                }
            });

        /*
         * Install an EndpointGroup with only 1 thread so we can easily test if
         * it is blocked
         */
        final EndpointGroup originalEndpointGroup =
            AsyncRegistryUtils.getEndpointGroup();
        tearDowns.add(() ->
                      AsyncRegistryUtils.setEndpointGroup(
                          originalEndpointGroup));
        AsyncRegistryUtils.setEndpointGroup(null);
        AsyncRegistryUtils.initEndpointGroup(
            logger, 1 /* threads */, 60 /* quiescence seconds */,
            false /* forClient */,
            ParameterState.RN_RH_ASYNC_MAX_CONCURRENT_REQUESTS_DEFAULT);
        final EndpointGroup newEndpointGroup =
            AsyncRegistryUtils.getEndpointGroup();

        /*
         * Change authentication policy to have very short timeouts, but allow
         * session extension.
         */
        setAuthPolicy(GP_SESSION_TIMEOUT + "=" + "5 SECONDS" +
                      ";" + GP_LOGIN_CACHE_TIMEOUT + "=" + "1 SECONDS" +
                      ";" + GP_SESSION_EXTEND_ALLOW +"=" + "true");

        /*
         * Disable auto renewal of login tokens since that wasn't the path that
         * blocked the async thread
         */
        tearDowns.add(() -> UserLoginManager.testDisableAutoRenew = false);
        UserLoginManager.testDisableAutoRenew = true;

        /* Create store */
        final LoginCredentials creds =
            new PasswordCredentials(USER_NAME, USER_PW.toCharArray());
        final KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0]);
        Properties props = new Properties();
        addTransportProps(props);
        kvConfig.setSecurityProperties(props);
        final KVStore store = KVStoreFactory.getStore(kvConfig, creds, null);
        tearDowns.add(() -> store.logout());
        tearDowns.add(() -> store.close());

        /* Add hook to block token renewal */
        final Semaphore hookCalled = new Semaphore(0);
        final Semaphore finishHook = new Semaphore(0);
        tearDowns.add(finishHook::release);
        final CompletableFuture<Void> hookDone = new CompletableFuture<>();
        ((RequestDispatcherImpl) ((KVStoreImpl) store).getDispatcher())
            .renewTokenHook = ct -> {
            try {
                hookCalled.release();
                assertTrue(finishHook.tryAcquire(60, SECONDS));
                hookDone.complete(null);
            } catch (Throwable t) {
                hookDone.completeExceptionally(t);
            }
        };

        /* Access store long enough to require renewal */
        final Key key = Key.createKey("foo");
        boolean gotHookCalled = false;
        for (int i = 0; i < 30; i++) {
            final CompletableFuture<Void> rowFuture =
                CompletableFuture.runAsync(() -> store.get(key));
            if (hookCalled.tryAcquire(1, SECONDS)) {
                gotHookCalled = true;
                break;
            }
            rowFuture.get();
        }
        assertTrue("Hook did not get called", gotHookCalled);

        /* Make sure endpoint group thread is not blocked */
        final ExecutorService executorService =
            newEndpointGroup.getSchedExecService();
        final Semaphore completedTask = new Semaphore(0);
        executorService.submit((Runnable) completedTask::release);
        assertTrue("Async thread was blocked",
                   completedTask.tryAcquire(30, SECONDS));

        /* Make sure the hook completed cleanly */
        finishHook.release();
        hookDone.get(10, SECONDS);
    }

    /**
     * Test that reauthentication does not block a thread from the async thread
     * pool. [KVSTORE-946]
     */
    @Test
    public void testAsyncReauthNotBlocking() throws Exception {

        Assume.assumeTrue("Test requires async", AsyncControl.serverUseAsync);

        /* Grant access */
        grantRoles(USER_NAME, "readwrite");
        tearDowns.add(() -> {
                try {
                    revokeRoles(USER_NAME, "readwrite");
                } catch (Exception e) {
                    throw new RuntimeException("Unexpected exception: " + e, e);
                }
            });


        /*
         * Install an EndpointGroup with only 1 thread so we can easily test if
         * it is blocked
         */
        final EndpointGroup originalEndpointGroup =
            AsyncRegistryUtils.getEndpointGroup();
        tearDowns.add(() ->
                      AsyncRegistryUtils.setEndpointGroup(
                          originalEndpointGroup));
        AsyncRegistryUtils.setEndpointGroup(null);
        AsyncRegistryUtils.initEndpointGroup(
            logger, 1 /* threads */, 60 /* quiescence seconds */,
            false /* forClient */,
            ParameterState.RN_RH_ASYNC_MAX_CONCURRENT_REQUESTS_DEFAULT);
        final EndpointGroup newEndpointGroup =
            AsyncRegistryUtils.getEndpointGroup();


        /*
         * Change authentication policy to have very short timeouts, and to not
         * allow session extension, so that reauthentication will be performed
         */
        setAuthPolicy(GP_SESSION_TIMEOUT + "=" + "5 SECONDS" +
                      ";" + GP_LOGIN_CACHE_TIMEOUT + "=" + "1 SECONDS" +
                      ";" + GP_SESSION_EXTEND_ALLOW +"=" + "false");

        /* Create store */
        LoginCredentials creds =
            new PasswordCredentials(USER_NAME, USER_PW.toCharArray());
        KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0]);
        Properties props = new Properties();
        addTransportProps(props);
        kvConfig.setSecurityProperties(props);

        /* Create a ReauthenticateHandler that will block */
        final Semaphore reauthCalled = new Semaphore(0);
        final Semaphore finishReauth = new Semaphore(0);
        tearDowns.add(finishReauth::release);
        final ReauthenticateHandler reauth = kvs -> {
            try {
                reauthCalled.release();
                finishReauth.acquire();
            } catch (InterruptedException e) {
            }
        };

        /* Create store, table, and row */
        final KVStore store = KVStoreFactory.getStore(kvConfig, creds, reauth);
        tearDowns.add(() -> store.logout());
        tearDowns.add(() -> store.close());
        execStatement(store,
                      "CREATE TABLE IF NOT EXISTS " + TABLE + TABLE_DEF);
        final TableAPI tableAPI = store.getTableAPI();
        final Table testTable = tableAPI.getTable(TABLE);
        final Row row = testTable.createRowFromJson(ROW_JSON, true);

        /* Provoke reauthentication */
        boolean inReauth = false;
        for (int i = 0; i < 30; i++) {
            final CompletableFuture<Row> future =
                tableAPI.getAsync(row.createPrimaryKey(), null);
            if (reauthCalled.tryAcquire(1, SECONDS)) {
                inReauth = true;
                break;
            }
            future.get(5, SECONDS);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                /* ignore */
            }
        }
        assertTrue("Didn't call ReauthenticateHandler", inReauth);

        /* Make sure endpoint group thread is not blocked */
        final ExecutorService executorService =
            newEndpointGroup.getSchedExecService();
        final Semaphore completedTask = new Semaphore(0);
        executorService.submit((Runnable) completedTask::release);
        assertTrue("Async thread was blocked",
                   completedTask.tryAcquire(5, SECONDS));
    }

    private void testReauthenticate(TestOperation op) throws Exception {
        try {

            grantRoles(USER_NAME, "readwrite");

            /*
             * Start by changing authentication policy to have very short
             * timeouts, and to not allow session extension.
             */
            setAuthPolicy(GP_SESSION_TIMEOUT + "=" + "5 SECONDS" +
                          ";" + GP_LOGIN_CACHE_TIMEOUT + "=" + "1 SECONDS" +
                          ";" + GP_SESSION_EXTEND_ALLOW +"=" + "false");

            LoginCredentials creds =
                new PasswordCredentials(USER_NAME, USER_PW.toCharArray());
            KVStoreConfig kvConfig =
                new KVStoreConfig(createStore.getStoreName(),
                                  HOST + ":" + registryPorts[0]);
            Properties props = new Properties();
            addTransportProps(props);
            kvConfig.setSecurityProperties(props);

            Reauth reauth = new Reauth(creds);
            KVStore store = KVStoreFactory.getStore(kvConfig, creds, reauth);
            op.prepareTest(store);

            /*
             * Test to be sure that reauthentication happens transparently
             */
            for (int i = 0; i < 30; i++) {
                op.doOperation(store);

                if (reauth.attempts > 0) {
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    /* ignore */
                }
            }
            assertTrue(reauth.completed > 0);

            /*
             * Now test to be what happens when reauthentication fails
             */
            LoginCredentials badCreds =
                new PasswordCredentials(USER_NAME,
                                        (USER_PW + "XXX").toCharArray());
            reauth.reset(badCreds);

            for (int i = 0; i < 30; i++) {
                try {
                    op.doOperation(store);
                } catch (AuthenticationRequiredException are) {
                    break;
                }

                if (reauth.attempts > 0) {
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    /* ignore */
                }
            }
            assertTrue(reauth.attempts > 0);
            assertTrue(reauth.completed == 0);
            assertTrue(reauth.afeCount > 0);

            /* Test to be sure that we get ARE when reauthentication fails */
            reauth.reset(badCreds);
            try {
                op.doOperationExpectFail(store);
                fail("Expected ARE");
            } catch (AuthenticationRequiredException are) {
                /* ignore */
            }
            assertTrue(reauth.afeCount > 0);

            /*
             * Now, tell the reauthenticationHandler to squash AFE so that
             * it pretends to succed.  We should get ARE here too.
             */
            reauth.reset(badCreds);
            reauth.setSquashAFE(true);

            try {
                op.doOperation(store);
                fail("Expected ARE");
            } catch (AuthenticationRequiredException are) {
                /* ignore */
            }
            assertTrue(reauth.afeCount > 0);

            /* Return the store to working order */
            store.login(creds);
            op.doOperation(store);

            store.logout();
            store.close();
        } finally {
            revokeRoles(USER_NAME, "readwrite");
        }
    }

    /**
     * A simple reauthentication handler that provides some tracking
     * information so that we can see whether it gets called.
     */
    private static class Reauth implements ReauthenticateHandler {
        private int attempts;
        private int completed;
        private int afeCount;
        private LoginCredentials creds;
        private boolean squashAFE;

        private Reauth(LoginCredentials creds) {
            reset(creds);
        }

        private void setSquashAFE(boolean squashIt) {
            squashAFE = squashIt;
        }

        private void reset(LoginCredentials newCreds) {
            attempts = 0;
            completed = 0;
            afeCount = 0;
            creds = newCreds;
            squashAFE = false;
        }

        @Override
        public void reauthenticate (KVStore store) {
            attempts++;
            try {
                store.login(creds);
                completed++;
            } catch (AuthenticationFailureException afe) {
                afeCount++;
                if (!squashAFE) {
                    throw afe;
                }
            }
        }
    }


    /**
     * Test operations used in the reauthentication test cases.
     */
    interface TestOperation {

        void prepareTest(KVStore store) throws Exception;

        void doOperation(KVStore store) throws Exception;

        void doOperationExpectFail(KVStore store) throws Exception;
    }
}
