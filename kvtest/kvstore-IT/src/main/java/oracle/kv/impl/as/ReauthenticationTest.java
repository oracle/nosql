/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.as;

import static oracle.kv.impl.param.ParameterState.GP_LOGIN_CACHE_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.GP_LOGIN_CACHE_TIMEOUT_DEFAULT;
import static oracle.kv.impl.param.ParameterState.GP_SESSION_EXTEND_ALLOW;
import static oracle.kv.impl.param.ParameterState.GP_SESSION_EXTEND_ALLOW_DEFAULT;
import static oracle.kv.impl.param.ParameterState.GP_SESSION_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.GP_SESSION_TIMEOUT_DEFAULT;
import static oracle.kv.util.CreateStore.mergeParameterMapDefaults;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.KVSecurityConstants;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.LoginCredentials;
import oracle.kv.ReauthenticateHandler;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.rep.admin.ResourceInfo;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.systables.TableStatsIndexDesc;
import oracle.kv.impl.systables.TableStatsPartitionDesc;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.client.ClientLoggerUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.kvlite.KVLite;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ReauthenticationTest {
    protected static final Logger logger =
        ClientLoggerUtils.getLogger(ReauthenticationTest.class, "test");
    protected static CommandServiceAPI admin;
    protected static KVLite kvlite;
    protected static final String hostName = "localhost";
    protected static final String kvStoreName = "asreauthtest";
    protected static final int startPort = 13240;
    private final static String portRange = "13255,13260";
    private final static String testDir =
        TestUtils.getTestDir().getAbsolutePath();

    /* the kvlite security directory and client trust file */
    private final static String securityDir = testDir + "/security";
    private static AggregationService as;

    /* quiet kvlite */
    protected static PrintStream outStream = System.out;
    protected static PrintStream errStream = System.err;

    /**
     * Starts a secure 1x3 KVLite instance and adds a user for use by the
     * API calls.
     */
    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        TestUtils.clearTestDirectory();
        TestStatus.setManyRNs(true);
        TestStatus.setActive(true);

        /*
         * Filter out the std output of kvlite. It prints to stdout
         * when generating security information.
         */
        quietSystemOutput();
        kvlite = startStore();
        revertSystemOutput();


        System.setProperty(KVSecurityConstants.SECURITY_FILE_PROPERTY,
                           securityDir + "/user.security");

        final KVStore store = getKVHandle(null);
        TestBase.waitForTable(store.getTableAPI(),
                              TableStatsPartitionDesc.TABLE_NAME);
        TestBase.waitForTable(store.getTableAPI(),
                              TableStatsIndexDesc.TABLE_NAME);
        TestBase.mrTableSetUp(store);
        admin = getAdmin();
    }

    @AfterClass
    public static void staticTearDown()
        throws Exception {

        if (kvlite != null) {
            quietSystemOutput();
            kvlite.stop(true);
            revertSystemOutput();
            kvlite = null;
        }

        /* clear security properties set as side effects */
        System.clearProperty(KVSecurityConstants.SECURITY_FILE_PROPERTY);
        System.clearProperty("javax.net.ssl.trustStore");
        TestStatus.setActive(false);
    }

    @After
    public void tearDown()
        throws Exception {

        if (as != null) {
            as.stop();
            as = null;
        }
    }

    /**
     * Test that aggregation service can perform re-authentication while
     * exchanging the resource info. The test disables the session extension
     * to make re-authentication happen easily.
     */
    @Test
    public void testReauthenticate()
        throws Exception {

        disableSessionExtension();
        as = startAggregationService();

        Reauth reauth = new Reauth(getAdminCreds());
        as.setStoreHandle(getKVHandle(reauth));

        int lastAttempts = 0;
        for (int i = 0; i < 30; i++) {
            as.callAllNodes(System.currentTimeMillis(), null);
            if (reauth.attempts > 0) {
                if (lastAttempts < reauth.attempts) {
                    /* check if re-auth only happen once */
                    assertEquals(lastAttempts + 1, reauth.attempts);
                    lastAttempts++;
                }

                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                /* ignore */
            }
        }
        assertTrue(reauth.completed > 0);

        int errors = checkResourceInfoExchange();
        assertEquals(0, errors);
    }

    /**
     * Test that aggregation service can exchange resource info against
     * a degraded read-only store, it includes:
     *
     * - verify as can exchange resource info
     * - degrade the store to read-only
     * - exchange resource info
     */
    @Test
    public void testDegraded()
        throws Exception {

        as = startAggregationService();
        resetAuthPolicy();
        assertEquals(0, checkResourceInfoExchange());

        /* shutdown 2 RN, degrade to read-only store */
        degradeStore();

        int count = 0;
        while (true) {
            /* keep exchanging resource info till one of them succeed */
            int errors = checkResourceInfoExchange();
            if (errors == 2) {
                break;
            }

            if (count > 20) {
                fail("Unable to exchange with degraded store");
            }
            count++;
            Thread.sleep(1000);
        }

        assertEquals(2, checkResourceInfoExchange());
        fixStore();
    }

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

    /*
     * Start a 1x3, secure store
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

    private static KVStore getKVHandle(ReauthenticateHandler reauth)
        throws Exception {

        LoginCredentials creds = getAdminCreds();
        KVStoreConfig kvConfig =
            new KVStoreConfig(kvStoreName, hostName + ":" + startPort);
        kvConfig.setRequestTimeout(10, TimeUnit.SECONDS);

        try {
            return KVStoreFactory.getStore(kvConfig, creds, reauth);
        } catch (Exception e) {
            throw e;
        }
    }

    private static CommandServiceAPI getAdmin()
        throws Exception {

        LoginManager loginMgr = KVStoreLogin
            .getAdminLoginMgr(hostName, startPort, getAdminCreds(), logger);

        return RegistryUtils.getAdmin(hostName, startPort, loginMgr, logger);
    }

    private static LoginCredentials getAdminCreds()
        throws Exception {

        String loginFile = securityDir + "/user.security";
        KVStoreLogin storeLogin = new KVStoreLogin("admin", loginFile);
        storeLogin.loadSecurityProperties();
        storeLogin.prepareRegistryCSF();
        return storeLogin.makeShellLoginCredentials();
    }

    private static AggregationService startAggregationService()
        throws Exception {

        AggregationService service = AggregationService.
            createAggregationService(kvStoreName,
                                     new String[] { hostName + ":" + startPort },
                                     8 /* throughputHistorySec */,
                                     3 /* throughputPollPeriodSec */,
                                     1000 /* tableSizePollPeriodSec */,
                                     1000 /* peakThroughputCollectionSec */,
                                     1 /* peakThroughputTTLDay */,
                                     1 /* maxThreads */);

        /* Wait for the AS to start polling */
        assertTrue("Awaiting Aggregation Service startup",
                   new PollCondition(500, 5000) {
                       @Override
                       protected boolean condition() {
                           return service.getPollCount() > 0L;
                       }
                   }.await());
        return service;
    }

    private static void degradeStore() throws Exception {
        StorageNodeAgentAPI[] snas = kvlite.getSNAPIs();
        RepNodeId rnids[] = new RepNodeId[3];
        for (int i = 0; i < 3; i++) {
            rnids[i] = new RepNodeId(1, i + 1);
            boolean res = snas[i].stopRepNode(rnids[i], false, false);
            assertTrue(res);
        }
        for (int i = 0; i < 1; i++) {
            boolean res = snas[i].startRepNode(rnids[i]);
            assertTrue(res);
        }
    }

    private static void fixStore() throws Exception {
        StorageNodeAgentAPI[] snas = kvlite.getSNAPIs();
        RepNodeId rnids[] = new RepNodeId[3];
        for (int i = 1; i < 3; i++) {
            rnids[i] = new RepNodeId(1, i + 1);
            boolean res = snas[i].startRepNode(rnids[i]);
            assertTrue(res);
        }
    }

    /* Revert login and session parameters to defaults */
    private static void resetAuthPolicy()
        throws Exception {

        ParameterMap map = new ParameterMap();
        map.setParameter(GP_SESSION_TIMEOUT,
                         GP_SESSION_TIMEOUT_DEFAULT);
        map.setParameter(GP_LOGIN_CACHE_TIMEOUT,
                         GP_LOGIN_CACHE_TIMEOUT_DEFAULT);
        map.setParameter(GP_SESSION_EXTEND_ALLOW,
                         GP_SESSION_EXTEND_ALLOW_DEFAULT);

        int planId = admin.createChangeGlobalSecurityParamsPlan("_revert", map);
        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);
    }

    /* Disable session extension and shorten the timeout */
    private static void disableSessionExtension()
        throws Exception {

        ParameterMap map = new ParameterMap();
        map.setParameter(GP_SESSION_TIMEOUT, "5 SECONDS");
        map.setParameter(GP_LOGIN_CACHE_TIMEOUT, "1 SECONDS");
        map.setParameter(GP_SESSION_EXTEND_ALLOW, "false");

        int planId = admin.createChangeGlobalSecurityParamsPlan("_disable", map);
        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);
    }

    private int checkResourceInfoExchange()
        throws Exception {

        List<Future<ResourceInfo>> results =
            as.callAllNodes(System.currentTimeMillis(), null);
        assertEquals(3, results.size());
        int errors = 0;
        for (Future<ResourceInfo> f : results) {
            try {
                final ResourceInfo info = f.get();
                if (info == null) {
                    errors++;
                    continue;
                }
            } catch (Exception ex) {
                errors++;
            }
        }
        return errors;
    }

    private static class Reauth implements ReauthenticateHandler {
        private int attempts;
        private int completed;
        private LoginCredentials creds;

        private Reauth(LoginCredentials loginCreds) {
            creds = loginCreds;
        }

        @Override
        public void reauthenticate (KVStore store) {
            attempts++;
            try {
                store.login(creds);
                completed++;
            } catch (AuthenticationFailureException afe) {
                fail("re-auth failed " + afe.getMessage());
            }
        }
    }
}
