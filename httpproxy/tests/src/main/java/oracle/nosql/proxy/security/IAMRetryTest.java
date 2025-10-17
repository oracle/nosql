/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.util.kvlite.KVLite;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.proxy.Config;
import oracle.nosql.proxy.Proxy;
import oracle.nosql.proxy.ProxyMain;
import oracle.nosql.proxy.sc.LocalTenantManager;
import oracle.nosql.proxy.sc.TenantManager;
import oracle.nosql.proxy.util.KVLiteBase;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class IAMRetryTest extends KVLiteBase {

    /*
     * Proxy state
     */
    private static int PROXY_PORT = 8095;
    protected static String KVLITE_MEMORYMB_PROP = "test.memorymb";
    protected static String SIMULATE_IAM_PROP = "test.simulateiam";

    protected static String hostName = getHostName();
    protected static final int startPort = 13240;
    protected static KVLite kvlite;
    protected static Proxy proxy = null;
    protected static TenantManager tm = null;
    protected static AccessChecker ac = null;

    protected static int memoryMB = 0;
    protected static String prevSimValue = null;

    @BeforeClass
    public static void staticSetUp()
        throws Exception {
        startup();
    }

    @AfterClass
    public static void staticTearDown()
        throws Exception {

        stopProxy();

        if (kvlite != null) {
            kvlite.stop(false);
        }

        cleanupTestDir();

        /* reset system properties we set */
        if (prevSimValue == null) {
            System.clearProperty(SIMULATE_IAM_PROP);
        } else {
            System.setProperty(SIMULATE_IAM_PROP, prevSimValue);
        }
    }

    @After
    public void tearDown() throws Exception {
        stopProxy();
    }

    protected static void stopProxy()
        throws Exception {

        if (proxy != null) {
            proxy.shutdown(3, TimeUnit.SECONDS);
            proxy = null;
        }

        if (tm != null) {
            tm.close();
            tm = null;
        }
    }

    protected static void startup() throws Exception {
        assumeTrue("Skip IAMRetryTest if not cloudsim test",
                   !Boolean.getBoolean("onprem") &&
                   !Boolean.getBoolean("usemc") &&
                   !Boolean.getBoolean("usecloud"));

        prevSimValue = System.getProperty(SIMULATE_IAM_PROP);

        String proxyHost = System.getProperty("proxy.host");
        if (proxyHost != null) {
            hostName = proxyHost;
        }
        Integer proxyPort = Integer.getInteger("proxy.port");
        if (proxyPort != null) {
            PROXY_PORT = proxyPort;
        }

        cleanupTestDir();

        memoryMB = Integer.getInteger(KVLITE_MEMORYMB_PROP, 0);
        kvlite = startKVLite(hostName,
                             null, // default store name
                             false, // useThreads = false
                             false, // verbose = false
                             false, // multishard
                             memoryMB,
                             false); // secured
    }

    protected static void startProxy(boolean allowInternalRetries,
                                     int retryDelayMs,
                                     int maxRetriesPerRequest,
                                     int maxActiveRetryCount)
        throws Exception {


        Properties commandLine = new Properties();
        commandLine.setProperty(Config.STORE_NAME.paramName,
                                getStoreName());
        commandLine.setProperty(Config.HELPER_HOSTS.paramName,
                                (hostName + ":" + getKVPort()));
        commandLine.setProperty(Config.PROXY_TYPE.paramName, "CLOUDSIM");
        commandLine.setProperty(Config.HTTP_PORT.paramName,
                Integer.toString(PROXY_PORT));

        commandLine.setProperty(Config.VERBOSE.paramName,
                                Boolean.toString(
                                    Boolean.getBoolean("test.verbose")));

        commandLine.setProperty(Config.AUTH_RETRIES_ENABLED.paramName,
                                Boolean.toString(allowInternalRetries));
        commandLine.setProperty(Config.MAX_ACTIVE_RETRY_COUNT.paramName,
                                Integer.toString(maxActiveRetryCount));
        commandLine.setProperty(Config.MAX_RETRIES_PER_REQUEST.paramName,
                                Integer.toString(maxRetriesPerRequest));
        commandLine.setProperty(Config.RETRY_DELAY_MS.paramName,
                                Integer.toString(retryDelayMs));

        /* this will make IAM checks delay like real IAM */
        System.setProperty(SIMULATE_IAM_PROP, "true");

        ac = AccessCheckerFactory.createInsecureAccessChecker();

        /* create an appropriate TenantManager */
        Config cfg = new Config(commandLine);
        tm = LocalTenantManager.createTenantManager(cfg);

        proxy = ProxyMain.startProxy(commandLine, tm, ac, null);
    }

    protected NoSQLHandle configHandle(String endpoint) {
        NoSQLHandleConfig hconfig = new NoSQLHandleConfig(endpoint);
        hconfig.configureDefaultRetryHandler(50, 10);
        hconfig.setRequestTimeout(30000);
        SecureTestUtil.setAuthProvider(hconfig, true,
                                       "TestTenant");
        return getHandle(hconfig);
    }

    public static String getProxyEndpoint() {
        try {
            return "http://" + hostName + ":" + PROXY_PORT;
        } catch (Exception e) {
        }
        return null;
    }

    /**
     * Allows classes to create a differently-configured NoSQLHandle.
     */
    protected NoSQLHandle getHandle(NoSQLHandleConfig config) {
        /*
         * Create a Logger. Configuration for the logger is in proxy/build.xml
         */
        Logger logger = Logger.getLogger(getClass().getName());
        config.setLogger(logger);

        /*
         * Open the handle
         */
        return NoSQLHandleFactory.createNoSQLHandle(config);
    }

    /*
     * Utility methods for use by subclasses
     */

    /**
     * Simpler version of tableOperation. This will not support
     * a change of limits as it doesn't accept a table name.
     */
    protected static TableResult tableOperation(NoSQLHandle handle,
                                                String statement,
                                                TableLimits limits,
                                                int waitMillis) {
        assertTrue(waitMillis > 500);
        TableRequest tableRequest = new TableRequest()
            .setStatement(statement)
            .setTableLimits(limits)
            .setTimeout(15000);

        return handle.doTableRequest(tableRequest, waitMillis, waitMillis/10);
    }

    /**
     * Delays for the specified number of milliseconds, ignoring exceptions
     */
    static void delay(int delayMS) {
        try {
            Thread.sleep(delayMS);
        } catch (Exception e) {
        }
    }

    protected static int createTable(NoSQLHandle handle, String tableName) {
        String stmt = "create table if not exists " + tableName +
                      "(id integer, name string, primary key(id))";
        int retries = 0;
        TableResult tres = tableOperation(handle, stmt,
                              new TableLimits(1000, 1000, 10),
                              20000);
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());
        if (tres.getRetryStats() != null) {
            retries += tres.getRetryStats().getRetries();
        }
        /* do a single put to get it into the cache */
        MapValue value = new MapValue().put("id", 10).put("name", "jane");
        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName(tableName);
        PutResult res = handle.put(putRequest);
        assertNotNull("Put failed", res.getVersion());
        if (res.getRetryStats() != null) {
            retries += res.getRetryStats().getRetries();
        }
        return retries;
    }

    protected static int dropTable(NoSQLHandle handle, String tableName) {
        String stmt = "drop table " + tableName;
        TableResult tres = tableOperation(handle, stmt, null, 20000);
        assertEquals(TableResult.State.DROPPED, tres.getTableState());
        if (tres.getRetryStats() == null) {
            return 0;
        }
        return tres.getRetryStats().getRetries();
    }

    @Test
    public void testNormalRetries()
        throws Exception {
        /* start proxy with internal retries disabled */
        startProxy(false, 10, 1, 1);
        /* run test, see auth retries in client */
        NoSQLHandle handle = configHandle(getProxyEndpoint());
        int retries = createTable(handle, "retryTestTable");
        retries += dropTable(handle, "retryTestTable");
        /* expect that we had auth retries */
        assertTrue("expected at least one retry, got zero", retries > 0);
    }

    @Test
    public void testInternalRetries()
        throws Exception {
        /* start proxy with internal retries enabled */
        startProxy(true, 30, 10, 10);
        /* run test, see zero auth retries in client */
        NoSQLHandle handle = configHandle(getProxyEndpoint());
        int retries = createTable(handle, "retryTestTable");
        retries += dropTable(handle, "retryTestTable");
        /* expect that we had no auth retries */
        assertTrue("expected zero retries, got " + retries, retries == 0);
    }

    private void checkInvalidConfig(int retryDelayMs,
                                    int maxRetriesPerRequest,
                                    int maxActiveRetries) {
        try {
            startProxy(true, retryDelayMs,
                       maxRetriesPerRequest,
                       maxActiveRetries);
            stopProxy();
            fail("Invalid config should have failed");
        } catch (RuntimeException re) {
        } catch (Exception e) {
            fail("Expected RuntimeException, got " + e);
        }
    }

    @Test
    public void testInvalidConfigParams()
        throws Exception {
        checkInvalidConfig(0, 0, 0);
        checkInvalidConfig(-1, -1, -1);
        checkInvalidConfig(1, 20, 20);
        checkInvalidConfig(100, 1000, 20);
        checkInvalidConfig(100, 20, 1001);
    }
}
