/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2018 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.proxy.kv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.PrintWriter;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.xregion.service.JsonConfig;
import oracle.kv.impl.xregion.service.XRegionService;
import oracle.kv.util.kvlite.KVLite;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.kv.StoreAccessTokenProvider;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.JsonOptions;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.proxy.Config;
import oracle.nosql.proxy.Proxy;
import oracle.nosql.proxy.ProxyMain;
import oracle.nosql.proxy.ProxyTestBase;


/**
 * The tests in KVProxyTest will be run using this class's
 * setUp
 */
public class KVNonSecureProxyTest extends KVProxyTest {

    @BeforeClass
    public static void staticSetUp() throws Exception {
        assumeTrue(!Boolean.getBoolean(USEMC_PROP) &&
                   !Boolean.getBoolean(USECLOUD_PROP));
        cleanupTestDir();
        kvlite = startKVLite(hostName,
                             null, // default store name
                             false, // useThreads = false
                             false, // verbose = false
                             false, // isMultiShard = false
                             0,     // memoryMB = 0
                             false); // isSecure = true

        proxy = startKVProxy(getStoreName(),
                             (hostName + ":" + getKVPort()),
                             ProxyTestBase.getProxyPort(),
                             false);

        waitForStoreInit(45);

        setAdmin(null);

        KVStoreConfig config = new KVStoreConfig(getStoreName(),
                                                 hostName+":"+
                                                 getKVPort());
        store = KVStoreFactory.getStore(config);

    }

    @AfterClass
    public static void staticTearDown() throws Exception {
        KVProxyTest.staticTearDown();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        endpoint = getProxyEndpoint();
        authProvider = new StoreAccessTokenProvider();
        getHandle();
    }

    /*
     * Override this to avoid (expected) failures related to secure
     * operations on a not-secure store.
     */
    @Test
    @Override
    public void testSecureSysOp() {
        /* this fails in a not secure configuration */
        try {
            doSysOp(kvhandle,
                    "create user newuser identified by 'ChrisToph \"_12&%'");
            fail("Should have failed with IAE");
        } catch (IllegalArgumentException iae) {
            // success
        }
    }

    @Test
    @Override
    public void testSystemExceptions() {
        /* don't run */
    }

    @Test
    @Override
    public void testTokenTimeout() {
        /* secure only, don't run */
    }

    @Test
    @Override
    public void testNonSecureAccess() {
        /* don't run */
    }

    @Override
    @Test
    public void testInvalidToken()
        throws Exception {
        /* secure only, don't run */
    }

    /*
     * More complex MR testing - create 2 kvlite instances, each in their
     * own region, start agents and try stuff... This is here to avoid the need
     * for a secure version. The process to create 2 kvlite instances and agents
     * is:
     *   0. one kvlite instance has already been started by the test base
     *   1. create a second kvlite instance using a different port, store
     *   name, etc. This test relies on those being available
     *   2. start a second proxy against the second kvlite instance
     *   3. set the local region for each kvlite instance
     *   4. create MR table config files, one for each region. The format
     *   is based on the public documentation for MR tables.
     *   5. create and start an XRegionService instance for each region, using
     *   the generated config files. These are the MR agents.
     *   6. create remote regions for each region, pointing to each other. This
     *   is possible now because the agents are running
     * At this point MR tests can be run. Cleanup needs to shut down all
     * services that require it -- agents, proxy, kvlite
     */
    @Test
    public void testMultiRegion() throws Exception {
        Proxy proxy2 = null;
        KVLite kvlite2 = null;
        NoSQLHandle kvhandle2 = null;
        final String storeName2 = "kvstore2";
        final String portRange2 = "13271,13280";
        final int port2 = 13270;
        final String root2 = getTestDir() + "/" + storeName2;
        final int proxyPort2 = 8097;
        final String endpoint2 = "http://localhost:" + proxyPort2;
        final boolean verbose2 = false;

        /* region info */
        final String region1 = "region1";
        final String region2 = "region2";
        XRegionService service1 = null;
        XRegionService service2 = null;

        TestStatus.setActive(true);

        try {

            kvlite2 = startKVLite(
                "localhost",
                storeName2,
                false, // useThreads
                false, // verbose
                false, // multishard,
                0, //memoryMB
                false, // secure
                port2, // port
                portRange2, // port range
                root2);

            /* create a proxy for the new kvlite instance */
            proxy2 = startKVProxy(storeName2,
                                  ("localhost:" + port2),
                                  proxyPort2,
                                  verbose2);

            proxy2.getTenantManager().waitForStoreInit(45);

            /* get a handle for the new store/proxy combo */
            NoSQLHandleConfig config = new NoSQLHandleConfig(endpoint2);
            config.setAuthorizationProvider(authProvider);
            kvhandle2 = NoSQLHandleFactory.createNoSQLHandle(config);

            /*
             * have 2 kvlite, 2 proxies, 2 handles
             * configure xregion...
             */
            doSysOp(kvhandle, "set local region " + region1);
            doSysOp(kvhandle2, "set local region " + region2);

            /* region1 config file */
            MapValue cfg1 = new MapValue();
            cfg1.put("path", getTestDir());
            cfg1.put("agentGroupSize", 1);
            cfg1.put("agentId", 0);
            cfg1.put("region", region1);
            cfg1.put("store", getStoreName());
            cfg1.put("helpers", new ArrayValue()
                     .add("localhost:" + getKVPort()));
            cfg1.put("regions", new ArrayValue().add(
                         new MapValue().put("name", region2)
                         .put("store", storeName2)
                         .put("helpers", new ArrayValue()
                              .add("localhost:"+port2))));

            /* region2 config file */
            MapValue cfg2 = new MapValue();
            cfg2.put("path", root2);
            cfg2.put("agentGroupSize", 1);
            cfg2.put("agentId", 0);
            cfg2.put("region", region2);
            cfg2.put("store", storeName2);
            cfg2.put("helpers", new ArrayValue().add("localhost:" + port2));
            cfg2.put("regions", new ArrayValue().add(
                         new MapValue().put("name", region1)
                         .put("store", getStoreName())
                         .put("helpers", new ArrayValue()
                              .add("localhost:"+getKVPort()))));

            String cfgFile1 = getTestDir() + "/config.json";
            String cfgFile2 = root2 + "/config.json";
            writeToFile(cfg1.toJson(JsonOptions.PRETTY), cfgFile1);
            writeToFile(cfg2.toJson(JsonOptions.PRETTY), cfgFile2);

            Logger logger = Logger.getLogger(getClass().getName());

            /* create and start xregion agents */
            service1 =
                new XRegionService(
                    JsonConfig.readJsonFile(cfgFile1, logger), logger);
            service2 =
                new XRegionService(
                    JsonConfig.readJsonFile(cfgFile2, logger), logger);
            service1.start();
            service2.start();

            /* create remote regions */
            doSysOp(kvhandle, "create region " + region2);
            doSysOp(kvhandle2, "create region " + region1);

            /* Now... create mr tables... */
            String createMRTable =
                "create table mrtable(id integer, primary key(id)) in " +
                "regions region1, region2";
            String createCounterTable =
                "create table mr_counter_table(id integer, counter integer " +
                " as mr_counter, primary key(id)) in regions region1, region2";
            String alterTable =
                "alter table mrtable(add counter integer as mr_counter)";

            TableResult tres = tableOperation(kvhandle,
                                              createCounterTable,
                                              null,
                                              10000);
            assertNotNull(tres.getSchema());

            tres = tableOperation(kvhandle,
                                  createMRTable,
                                  null,
                                  10000);
            assertNotNull(tres.getSchema());

            /* try adding a CRDT to test the evolution path */
            tres = tableOperation(kvhandle,
                                  alterTable,
                                  null,
                                  10000);
            assertNotNull(tres.getSchema());

            /* do an insert into the CRDT in original table */
            String insert =
                "insert into mr_counter_table values (1, default)";
            QueryRequest req = new QueryRequest().setStatement(insert);
            QueryResult ret = kvhandle.query(req);
            assertEquals(1, ret.getResults().get(0).asMap()
                         .get("NumRowsInserted").getInt());

            /* do an insert using a prepared query */
            String insertP = "declare $pkey integer; " +
                "insert into mr_counter_table values ($pkey, default)";
            PrepareRequest prepReq = new PrepareRequest()
                .setStatement(insertP);
            PrepareResult prepRet = kvhandle.prepare(prepReq);
            assertNotNull(prepRet.getPreparedStatement());
            prepRet.getPreparedStatement()
                .setVariable("$pkey", new IntegerValue(0));

            req = new QueryRequest().setPreparedStatement(prepRet);
            ret = kvhandle.query(req);
            List<MapValue> results = ret.getResults();
            assertEquals(results.size(), 1);
            int num = results.get(0).get("NumRowsInserted").getInt();
            assertEquals(num, 1);
        } finally {
            if (kvhandle2 != null) {
                kvhandle2.close();
            }
            if (proxy2 != null) {
                try {
                    proxy2.shutdown(3, TimeUnit.SECONDS);
                } catch (Exception e) {}
            }
            if (service1 != null) {
                service1.shutdown();
            }
            if (service2 != null) {
                service2.shutdown();
            }
            if (kvlite2 != null) {
                kvlite2.stop(false);
            }
        }
    }

    /*
     * Write the string content to the file specified by path
     */
    static void writeToFile(String content, String path) {
        try (PrintWriter out = new PrintWriter(path)) {
            out.println(content);
        } catch (Exception e) {
            fail("Exception writing to file: " + e);
        }
    }

    static Proxy startKVProxy(String storeName,
                              String helperHosts,
                              int proxyPort,
                              boolean verbose) {

        /* create a proxy */
        Properties commandLine = new Properties();
        commandLine.setProperty(Config.PROXY_TYPE.paramName,
                                Config.ProxyType.KVPROXY.name());

        commandLine.setProperty(Config.STORE_NAME.paramName,
                                storeName);

        commandLine.setProperty(Config.HELPER_HOSTS.paramName,
                                helperHosts);

        commandLine.setProperty(Config.HTTP_PORT.paramName,
                                Integer.toString(proxyPort));

        commandLine.setProperty(Config.VERBOSE.paramName,
                                Boolean.toString(verbose));

        return ProxyMain.startProxy(commandLine);
    }

}
