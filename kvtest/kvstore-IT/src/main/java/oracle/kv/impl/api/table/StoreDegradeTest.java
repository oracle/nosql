/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static oracle.kv.util.CreateStore.mergeParameterMapDefaults;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.FaultException;
import oracle.kv.KVSecurityConstants;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.ReauthenticateHandler;
import oracle.kv.StatementResult;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.security.login.RepNodeLoginManager;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.PreparedStatement;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.util.TableTestUtils;
import oracle.kv.util.kvlite.KVLite;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This test cases in this class test on-prem security in various ways.
 * It creates a 1x3 store and tests:
 * o degraded (read-only) access
 * o degraded handling of getTable
 */

public class StoreDegradeTest extends TableTestBase {

    protected static CommandServiceAPI admin;
    protected static KVLite kvlite;
    protected static final String hostName = "localhost";
    protected static final String kvStoreName = "degradestore";
    private final static String portRange = "13255,13260";
    private final static String testDir =
        TestUtils.getTestDir().getAbsolutePath();
    private final static int numAdmins = 3;

    /* the kvlite security directory and client trust file */
    private final static String securityDir = testDir + "/security";
    private final static String trustFile = securityDir + "/client.trust";
    private final static String user = "test";
    private final static String passwd = "NoSql00__123456";
    private static PasswordCredentials loginCreds;
    private static LoginToken loginToken;

    protected TableAPIImpl tableApi;

    /* quiet kvlite */
    protected static PrintStream outStream = System.out;
    protected static PrintStream errStream = System.err;

    private static final Logger staticLogger =
        LoggerUtils.getLogger(StoreDegradeTest.class, "test");

    private static void quietSystemOutput() {
        System.setOut(new PrintStream(new OutputStream() {
                @Override
                public void write(int b) throws IOException {}
            }));
        System.setErr(new PrintStream(new OutputStream() {
                @Override
                public void write(int b) throws IOException {}
            }));
    }

    private static void revertSystemOutput() {
        System.setOut(outStream);
        System.setErr(errStream);
    }

    /**
     * Starts a secure 1x3 KVLite instance and adds a user for use by the
     * API calls.
     */
    @BeforeClass
    public static void staticSetUp() throws Exception {
        TestUtils.clearTestDirectory();
        TestStatus.setManyRNs(true);

        /*
         * Filter out the std output of kvlite. It prints to stdout
         * when generating security information.
         */
        quietSystemOutput();
        kvlite = startStore();
        revertSystemOutput();

        final String loginFile = securityDir + "/user.security";

        /*
         * Set up the login manager for the admin
         */
        final KVStoreLogin storeLogin =
            new KVStoreLogin(
                "admin", loginFile);
        storeLogin.loadSecurityProperties();
        storeLogin.prepareRegistryCSF();
        final LoginCredentials creds =
            storeLogin.makeShellLoginCredentials();
        final LoginManager loginMgr =
            KVStoreLogin.getAdminLoginMgr(hostName, startPort, creds,
                                          staticLogger);
        setAdmin(loginMgr);

        /*
         * Create a user "test" and give it permission to manage tables
         * and data
         */
        int planId = admin.createCreateUserPlan(
            "Create User", user, true, false,
            passwd.toCharArray());
        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);

        final Set<String> roles = new HashSet<String>();
        roles.add(RoleInstance.READWRITE_NAME);
        roles.add(RoleInstance.DBADMIN_NAME);
        roles.add(RoleInstance.SYSADMIN_NAME);
        planId = admin.createGrantPlan(
            "Grant User", "test",
            roles);
        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);

        /*
         * If number of admins is > 1, deploy the new ones. It's handy to
         * have an admin on all 3 nodes.
         */
        if (numAdmins > 1) {
            deployAdmins();
        }

        /*
         * set up the secure store handle and loginToken for use by
         * internal API methods.
         */
        getKVHandle();
        TestBase.mrTableSetUp(store);
    }

    /*
     * Start a 1x3, secure
     */
    private static KVLite startStore() {
        String storeName = kvStoreName;
        boolean useThreads = false;
        boolean verbose = true;
        boolean isSecure = true;
        int numStorageNodes = 3;
        int numPartitions = 20;
        int repfactor = 3;
        int port = startPort;
        String portstr = Integer.toString(port);
        String rangestr = portRange;
        int capacity = 1;
        portstr = Integer.toString(port) + KVLite.DEFAULT_SPLIT_STR +
            Integer.toString(port + 30) + KVLite.DEFAULT_SPLIT_STR +
            Integer.toString(port + 60);
        rangestr = portRange + KVLite.DEFAULT_SPLIT_STR +
            Integer.toString(port + 35) + "," +
            Integer.toString(port + 40) + KVLite.DEFAULT_SPLIT_STR +
            Integer.toString(port + 65) + "," +
            Integer.toString(port + 70);

        kvlite = new KVLite(testDir,
                            storeName,
                            portstr,
                            true, /* run bootadmin */
                            hostName,
                            rangestr,
                            null, /* service port range */
                            numPartitions,
                            null, /* mount point */
                            useThreads,
                            isSecure,
                            null, /* no backup to restore */
                            -1,
                            numStorageNodes,
                            repfactor,
                            capacity);
        kvlite.setVerbose(verbose);

        kvlite.setPolicyMap(mergeParameterMapDefaults(null));

        /*
         * reduce the default memory consumed. This reduces load on the
         * local machine
         */
        kvlite.setMemoryMB(256);

        try  {
            kvlite.start(true);
        } catch (Throwable t) {
            /*
             * Display any setup problems, which may happen in particular
             * when debugging standalone, outside a nightly build run.
             */
            System.err.println("problems starting up KVLite");
            t.printStackTrace();
            throw t;
        }
        return kvlite;
    }

    /**
     * Assumes there are 3 SNs and there's already an admin on the first
     * one.
     */
    protected static void deployAdmins() throws Exception {
        assertTrue(kvlite.getNumStorageNodes() >= numAdmins);
        for (int i = 1; i < numAdmins; i++) {
            int planId = admin.createDeployAdminPlan("deploy admin",
                                                     kvlite.getStorageNodeId(i),
                                                     null);
            admin.approvePlan(planId);
            admin.executePlan(planId, false);
            admin.awaitPlan(planId, 0, null);
            admin.assertSuccess(planId);
        }
    }

    protected static void setAdmin(LoginManager mgr) throws Exception {
        admin = RegistryUtils.getAdmin(
            hostName, startPort, mgr, staticLogger);
    }

    /**
     * Reauthenticate both the KVStore handle and optionally,
     * the loginToken.
     */
    protected static void reauthenticate(Throwable cause,
                                         boolean tokenAlso) throws Exception {
        try {
            /* try to re-authenticate */
            final LoginManager requestLoginMgr =
                KVStoreImpl.getLoginManager(store);
            if (!((KVStoreImpl)store).tryReauthenticate(requestLoginMgr)) {
                throw new AuthenticationRequiredException(cause, true);
            }
            if (tokenAlso) {
                loginToken = getLoginToken(loginCreds);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @AfterClass
    public static void staticTearDown() throws Exception {
        if (kvlite != null) {
            quietSystemOutput();
            kvlite.stop(true);
            revertSystemOutput();
            kvlite = null;
        }

        /* clear security properties set as side effects */
        System.clearProperty(KVSecurityConstants.SECURITY_FILE_PROPERTY);
        System.clearProperty("javax.net.ssl.trustStore");
    }

    /**
     * Cleanup.
     * Remove tables. Handle retries to potentially reauthenticate
     */
    private static void removeTables() throws Exception {
        if (store != null) {
            boolean retry = true;
            while (true) {
                try {
                    TableTestUtils.removeTables(tableImpl.getTables(), admin,
                                                store);
                    if (countStoreTables(tableImpl) != 0) {
                        TableTestUtils.removeTables(tableImpl.getTables(),
                                                    admin, store);
                    }
                    assertEquals("Store tables count", 0, countStoreTables(tableImpl));
                    return;
                } catch (FaultException fe) {
                    System.err.println("RemoveTables fe: " +
                                       fe.getMessage() + ", cause " +
                                       fe.getCause());
                    if (retry) {
                        reauthenticate(fe, false);
                        retry = false;
                        continue;
                    }
                    throw fe;
                } catch (AuthenticationRequiredException are) {
                    if (retry) {
                        reauthenticate(are, false);
                        retry = false;
                        continue;
                    }
                    throw are;
                }
            }
        }
    }

    /**
     * Acquire a KVStore handle for use by the application.
     */
    private static void getKVHandle() throws Exception {

        /*
         * Set security props to connect as user "test"
         */
        final Properties securityProps = new Properties();
        securityProps.setProperty(KVSecurityConstants.TRANSPORT_PROPERTY,
                KVSecurityConstants.SSL_TRANSPORT_NAME);
        securityProps.setProperty
            (KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY, trustFile);
        securityProps.setProperty(
            KVSecurityConstants.SSL_TRUSTSTORE_TYPE_PROPERTY, "pkcs12");

        final KVStoreConfig kvConfig =
            new KVStoreConfig(kvStoreName, hostName + ":" + startPort);
        kvConfig.setSecurityProperties(securityProps);
        kvConfig.setRequestTimeout(10, TimeUnit.SECONDS);

        loginCreds =
            new PasswordCredentials(user, passwd.toCharArray());

        try {
        store = KVStoreFactory.getStore
            (kvConfig, loginCreds,
             new ReauthenticateHandler() {
                 /*
                  * Automatically performs reauthentication so there's no
                  * subsequent need to retry operations by catching
                  * AuthenticationRequiredException.
                  */
                 @Override
                 public void reauthenticate(KVStore kstore) {
                     kstore.login(loginCreds);
                 }
             });
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        tableImpl = (TableAPIImpl) store.getTableAPI();

        /*
         * Get a LoginToken for use by internal interfaces
         */
        loginToken = getLoginToken(loginCreds);
    }

    @Override
    @Before
    public void setUp() throws Exception {
    }

    @Override
    @After
    public void tearDown() throws Exception {
        removeTables();
    }

    /**
     * o create a table
     * o add a few rows
     * o verify query works
     * o degrade the store to read-only
     * o query again
     */
    @Test
    public void testDegraded() throws Exception {
        final int numRows = 10;
        final String query = "select * from foo";
        final String createTable =
            "create table foo(i integer, primary key(i))";
        createTable(createTable, null);

        /*
         * get a non-existent table. This, and related calls below,
         * exercise the fix to [#28150]. This test doesn't require a
         * secure store, but it adds some extra testing paths.
         */
        Table notATable = getTable("not_a_table");
        assertNull(notATable);

        Table table = getTable("foo");
        for (int i = 0; i < numRows; i++) {
            Row row = table.createRow();
            row.put("i", i);
            tableImpl.put(row, null, null);
        }


        StatementResult sr = executeQuery(query);
        assertEquals("Unexpected number of results", numRows, countRecords(sr));

        /* make the store degraded */
        degradeStore();

        /*
         * Use getTable() as both a test case and a way to know when the
         * newly-restarted RN is available. This avoids a random sleep,
         * waiting for the RN.
         */
        int count = 0;
        while (true) {
            try {
                /* see comment above about this test case */
                notATable = getTable("not_a_table");
                assertNull(notATable);
                break;
            } catch (Exception e) {
                if (count > 20) {
                    throw e;
                }
                count++;
                Thread.sleep(1000);
            }
        }

        sr = executeQuery(query);
        assertEquals("Unexpected number of results", numRows, countRecords(sr));

        fixStore();

        /* see comment above */
        notATable = getTable("not_a_table");
        assertNull(notATable);
    }

    /**
     * Execute the query using the identity in the LoginToken
     */
    private StatementResult executeQuery(String query) throws Exception {

        /*
         * This exists to test the path where the auth context is
         * acquired from the KVStore handle and not the LoginToken.
         */
        final boolean useNormalPath = false;

        ExecuteOptions opts = new ExecuteOptions();

        int count = 0;
        while (true) {
            try {
                if (useNormalPath) {
                    return executeDml(query);
                }
                /*
                 * This is the path used by the HTTP proxy.
                 */
                AuthContext auth = new AuthContext(loginToken);
                opts.setAuthContext(auth);
                PreparedStatement prep = store.prepare(query, opts);
                StatementResult res = store.executeSync(prep, opts);
                return res;
            } catch (FaultException | AuthenticationRequiredException e) {
                /* only reauthenticate once */
                if (count > 1) {
                    throw e;
                }
                reauthenticate(e, true);
                ++count;
                continue;
            }
        }
    }

    /**
     * Make the store degraded
     * 1. stop all 3 RNs
     * 2. restart two of them
     * 3. wait a bit for it to be available
     */
    protected static void degradeStore() throws Exception {
        StorageNodeAgentAPI[] snas = kvlite.getSNAPIs();
        RepNodeId rnids[] = new RepNodeId[3];
        for (int i = 0; i < 3; i++) {
            rnids[i] = new RepNodeId(1, i + 1);
            boolean res = snas[i].stopRepNode(rnids[i], false, false);
            assertTrue(res);
        }
        for (int i = 0; i < 2; i++) {
            boolean res = snas[i].startRepNode(rnids[i]);
            assertTrue(res);
        }
        /*
         * it takes a while for the RN to decide it can start the LOGIN
         * service. The caller may need to loop waiting or sleep.
         */
    }

    protected static void fixStore() throws Exception {
        StorageNodeAgentAPI[] snas = kvlite.getSNAPIs();
        RepNodeId rnids[] = new RepNodeId[3];
        for (int i = 1; i < 3; i++) {
            rnids[i] = new RepNodeId(1, i + 1);
            boolean res = snas[i].startRepNode(rnids[i]);
            assertTrue(res);
        }
        /*
         * Caller may choose to loop or sleep, giving the RNs time
         * to reccover
         */
    }

    /**
     * Get a LoginToken from the user creds, used to construct AuthContext
     */
    private static LoginToken getLoginToken(PasswordCredentials creds) {
        final RepNodeLoginManager rnlm =
            new RepNodeLoginManager(creds.getUsername(), false);
        rnlm.setTopology(
            ((KVStoreImpl)store).getDispatcher().getTopologyManager());
        rnlm.login(creds);
        final LoginToken login = rnlm.getLoginHandle().getLoginToken();
        return login;
    }
}
