/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
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
import oracle.nosql.proxy.kv.KVTenantManager;
import oracle.nosql.proxy.sc.LocalTenantManager;
import oracle.nosql.proxy.sc.TenantManager;
import oracle.nosql.proxy.security.AccessChecker;
import oracle.nosql.proxy.security.AccessCheckerFactory;
import oracle.nosql.proxy.security.SecureTestUtil;
import oracle.nosql.proxy.util.KVLiteBase;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class WarmupTest extends KVLiteBase {

    /*
     * Proxy state
     */
    private static int PROXY_PORT = 8095;
    protected static String KVLITE_MEMORYMB_PROP = "test.memorymb";

    protected static String hostName = getHostName();
    protected static final int startPort = 13240;
    protected static KVLite kvlite;
    protected static Proxy proxy = null;
    protected static TenantManager tm = null;
    protected static AccessChecker ac = null;

    protected static boolean onprem = false;
    protected static int memoryMB = 0;
    protected static String warmupFilename;
    protected static boolean miniCloud = false;
    protected final static String prefix = getTestDir();

    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        assumeTrue("Skip WarmupTest in minicloud or cloud test",
                   !Boolean.getBoolean("usemc") &&
                   !Boolean.getBoolean("usecloud"));
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
    }

    @After
    public void tearDown() throws Exception {
        stopProxy();
        if (warmupFilename != null && warmupFilename.isEmpty()==false) {
            new File(warmupFilename).delete();
        }
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

    protected static void startProxy(String warmupFile,
                                     int warmupTimeMs,
                                     int warmupFileRecencyMs,
                                     int warmupFileSaveIntervalMs)
        throws Exception {


        Properties commandLine = new Properties();
        commandLine.setProperty(Config.STORE_NAME.paramName,
                                getStoreName());
        commandLine.setProperty(Config.HELPER_HOSTS.paramName,
                                (hostName + ":" + getKVPort()));
        Config.ProxyType ptype = (onprem ? Config.ProxyType.KVPROXY :
                                  Config.ProxyType.CLOUDTEST);
        commandLine.setProperty(Config.PROXY_TYPE.paramName, ptype.name());
        commandLine.setProperty(Config.HTTP_PORT.paramName,
                Integer.toString(PROXY_PORT));

        if (warmupFile != null) {
            commandLine.setProperty(Config.WARMUP_FILE.paramName,
                                    warmupFile);
            commandLine.setProperty(Config.WARMUP_TIME_MS.paramName,
                                    Integer.toString(warmupTimeMs));
            commandLine.setProperty(Config.WARMUP_FILE_RECENCY_MS.paramName,
                                    Integer.toString(warmupFileRecencyMs));
            commandLine.setProperty(
                        Config.WARMUP_FILE_SAVE_INTERVAL_MS.paramName,
                        Integer.toString(warmupFileSaveIntervalMs));
        }

        commandLine.setProperty(Config.VERBOSE.paramName,
                                Boolean.toString(
                                    Boolean.getBoolean("test.verbose")));

        ac = AccessCheckerFactory.createInsecureAccessChecker();

        /* make sure we use the table cache */
        System.setProperty("test.usetablecache", "true");
        /* create an appropriate TenantManager */
        Config cfg = new Config(commandLine);
        if (onprem) {
            tm = KVTenantManager.createTenantManager(cfg);
        } else {
            tm = LocalTenantManager.createTenantManager(cfg);
        }

        proxy = ProxyMain.startProxy(commandLine, tm, ac, null);
    }

    protected NoSQLHandle configHandle(String endpoint) {
        NoSQLHandleConfig hconfig = new NoSQLHandleConfig(endpoint);
        return setupHandle(hconfig);
    }

    protected NoSQLHandle configHandle(URL url) {
        NoSQLHandleConfig hconfig = new NoSQLHandleConfig(url);
        return setupHandle(hconfig);
    }

    /* Set configuration values for the handle */
    protected NoSQLHandle setupHandle(NoSQLHandleConfig hconfig) {
        /*
         * 5 retries, default retry algorithm
         */
        hconfig.configureDefaultRetryHandler(5, 0);
        hconfig.setRequestTimeout(30000);
        SecureTestUtil.setAuthProvider(hconfig, false,
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

    protected static void createTable(NoSQLHandle handle, String tableName) {
        String stmt = "create table if not exists " + tableName +
                      "(id integer, name string, primary key(id))";
        TableResult tres = tableOperation(handle, stmt,
                              new TableLimits(1000, 1000, 10),
                              20000);
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());
        /* do a single put to get it into the cache */
        MapValue value = new MapValue().put("id", 10).put("name", "jane");
        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName(tableName);
        PutResult res = handle.put(putRequest);
        assertNotNull("Put failed", res.getVersion());
    }

    protected static void createTables(NoSQLHandle handle, String[] tables) {
        for (String tableName : tables) {
            createTable(handle, tableName);
        }
    }

    protected static void dropTable(NoSQLHandle handle, String tableName) {
        String stmt = "drop table " + tableName;
        TableResult tres = tableOperation(handle, stmt, null, 20000);
        assertEquals(TableResult.State.DROPPED, tres.getTableState());
    }

    protected static void dropTables(NoSQLHandle handle, String[] tables) {
        for (String tableName : tables) {
            dropTable(handle, tableName);
        }
    }

    protected void checkAllTables(String filename,
                                  String[] tableNames)
        throws Exception {
        /* read nsname keys */
        BufferedReader reader =
                new BufferedReader(new FileReader(filename));
        String nsname;
        Set<String> wTables = new HashSet<String>();
        while ((nsname = reader.readLine()) != null) {
            /* convert to namespace and tablename */
            if (nsname.isEmpty()) {
                continue;
            }
            String[] arr = nsname.split(":");
            if (arr.length == 1) {
                wTables.add(arr[0]);
            } else {
                wTables.add(arr[1]);
            }
        }
        reader.close();
        for (String tName : tableNames) {
            if (wTables.contains(tName) == false) {
                fail("Warmupfile missing table '" + tName + "'");
            }
        }
        assertEquals(tableNames.length, wTables.size());
    }


    protected static void writeFile(String fileName, String contents)
        throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
        writer.write(contents);
        writer.close();
    }

    @Test
    public void testEmptyWarmupFilename()
        throws Exception {
        assumeTrue(miniCloud == false);
        startProxy("", 1000, 5000, 500);
    }

    @Test
    public void testInvalidWarmupFilename()
        throws Exception {
        assumeTrue(miniCloud == false);
        warmupFilename = "sj % @~f\ndsf.. \b\\e sdjhj";
        startProxy(warmupFilename, 1000, 5000, 500);
    }

    @Test
    public void testOldWarmupFile()
        throws Exception {
        assumeTrue(miniCloud == false);
        startProxy("/bin/bash", 1000, 5000, 500);
    }

    @Test
    public void testBinaryWarmupFile()
        throws Exception {
        assumeTrue(miniCloud == false);
        warmupFilename = prefix + "/binary_warmup.test";
        writeFile(warmupFilename, "\u02cf\ucdc0ap\n\n\r\tgarbage\u0102");
        startProxy(warmupFilename, 1000, 5000, 500);
        delay(2000);
    }

    @Test
    public void testUnwriteableWarmupFile()
        throws Exception {
        assumeTrue(miniCloud == false);
        warmupFilename = prefix + "/unwriteable.test";
        writeFile(warmupFilename, "\n\n\n");
        File destFile = new File(warmupFilename);
        destFile.setWritable(false);
        startProxy(warmupFilename, 1000, 5000, 500);
        delay(2000);
        stopProxy();
        destFile.setWritable(true);
        destFile.delete();
    }

    @Test
    public void testBasicOperation()
        throws Exception {
        assumeTrue(miniCloud == false);
        warmupFilename = prefix + "/tablecache.test";
        startProxy(warmupFilename, 1000, 5000, 500);
        delay(600);
        /*
         * create a bunch of tables, then make sure the table
         * names make it into the cache warmup file
         */
        NoSQLHandle handle = configHandle(getProxyEndpoint());
        String[] tableNames = {"warm1", "foobar", "garbage", "audience"};
        createTables(handle, tableNames);
        delay(600);
        /* read cache warmup file, verify */
        checkAllTables(warmupFilename, tableNames);
        /* drop a table, check the rest */
        dropTable(handle, "foobar");
        delay(600);
        String[] remaining = {"warm1", "garbage", "audience"};
        checkAllTables(warmupFilename, remaining);
        /* restart proxy, wait a bit, verify tables are in warmup file */
        stopProxy();
        delay(600);
        startProxy(warmupFilename, 1000, 5000, 500);
        delay(3000);
        checkAllTables(warmupFilename, remaining);
        /* drop tables */
        dropTables(handle, remaining);
    }

}
