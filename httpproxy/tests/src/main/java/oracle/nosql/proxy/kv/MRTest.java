/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.proxy.kv;


/*
import static org.junit.Assert.assertEquals;
*/

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.KVStoreConfig;
import oracle.kv.StatementResult;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.xregion.service.JsonConfig;
import oracle.kv.impl.xregion.service.RegionInfo;
import oracle.kv.impl.xregion.service.XRegionService;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.kv.StoreAccessTokenProvider;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.SystemResult;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.values.MapValue;

import oracle.nosql.proxy.Proxy;
import oracle.nosql.proxy.ProxyTestBase;
import oracle.nosql.proxy.util.CreateStore;
import oracle.nosql.proxy.util.TestBase;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/*
 * Multi-region test using CreateStore
 * Steps to set up MR tables:
 * 1. create local/remote stores
 * 2. set local region names for each
 * 3. configure the XRegion service
 *  a. create config for each region
 *  b. start service for each region (service == agent)
 * 4. create remote regions (e.g. local needs to know about remote, vice versa)
 */
public class MRTest extends ProxyTestBase {

    private static final int startPort = 13250;
    private static final String localStoreName = "JsonMR-local";
    private static final String remoteStoreName = "JsonMR-remote";
    private static final String localRegion = "LOC";
    private static final String remoteRegion = "REM";
    private static final int localProxyPort = 8095;
    private static final int remoteProxyPort = 8096;
    private static final String localProxyEndpoint =
        "http://localhost:" + localProxyPort;
    private static final String remoteProxyEndpoint =
        "http://localhost:" + remoteProxyPort;

    private static CreateStore localStore;
    private static CreateStore remoteStore;

    private static KVStore localKV;
    private static KVStore remoteKV;

    private static TableAPIImpl localTableImpl;
    private static TableAPIImpl remoteTableImpl;

    private static HashSet<String> localHelpers;
    private static HashSet<String> remoteHelpers;

    private static Proxy localProxy;
    private static Proxy remoteProxy;

    private static NoSQLHandle localHandle;
    private static NoSQLHandle remoteHandle;

    private static RegionInfo localInfo;
    private static RegionInfo remoteInfo;

    private static XRegionService localService;
    private static XRegionService remoteService;

    private static StoreAccessTokenProvider authProvider =
        new StoreAccessTokenProvider();

    private static Logger testLogger =
        Logger.getLogger(MRTest.class.getName());

    @BeforeClass
    public static void staticSetUp() throws Exception {
        cleanupTestDir();

	TestStatus.setManyRNs(true);
        localStore = createStore((getTestDir() + "/" + localStoreName),
                                 localStoreName, startPort);
        localKV = KVStoreFactory.getStore(createKVConfig(localStore));
        localTableImpl = (TableAPIImpl)localKV.getTableAPI();
        String helperString = localStore.getHostname() + ":" +
            localStore.getRegistryPort();
        localHelpers = new HashSet<String>();
        localHelpers.add(helperString);
        localInfo = new RegionInfo(localRegion, localStoreName,
                                   new String[]{helperString});
        localProxy = KVNonSecureProxyTest.startKVProxy(localStoreName,
                                                       helperString,
                                                       localProxyPort,
                                                       false);
        localHandle = getHandle(localProxyEndpoint);
        assertNotNull(localHandle);

        remoteStore = createStore((getTestDir() + "/" + remoteStoreName),
                                  remoteStoreName, startPort);
        remoteKV = KVStoreFactory.getStore(createKVConfig(remoteStore));
        remoteTableImpl = (TableAPIImpl)remoteKV.getTableAPI();
        helperString = remoteStore.getHostname() + ":" +
            remoteStore.getRegistryPort();
        remoteHelpers = new HashSet<String>();
        remoteHelpers.add(helperString);
        remoteInfo = new RegionInfo(remoteRegion, remoteStoreName,
                                    new String[]{helperString});
        remoteProxy = KVNonSecureProxyTest.startKVProxy(remoteStoreName,
                                                        helperString,
                                                        remoteProxyPort,
                                                        false);
        remoteHandle = getHandle(remoteProxyEndpoint);
        assertNotNull(remoteHandle);

        /* defer system ops until fully initialized */
        localProxy.getTenantManager().waitForStoreInit(30);
        remoteProxy.getTenantManager().waitForStoreInit(30);
        doSysOp(localHandle, "set local region " + localRegion);
        doSysOp(remoteHandle, "set local region " + remoteRegion);

        // create XR services
        JsonConfig localConfig =
            createXRConfig(getTestDir().toString(),
                           localRegion,
                           localStoreName,
                           localHelpers);
        localConfig.addRegion(remoteInfo);
        localService = createXRService(localConfig, testLogger);
        Thread localServiceThread = new Thread(localService);
        localServiceThread.start();

        JsonConfig remoteConfig =
            createXRConfig(getTestDir().toString(),
                           remoteRegion,
                           remoteStoreName,
                           remoteHelpers);
        remoteConfig.addRegion(localInfo);
        remoteService = createXRService(remoteConfig, testLogger);
        Thread remoteServiceThread = new Thread(remoteService);
        remoteServiceThread.start();
        while (!(remoteService.isRunning() && localService.isRunning())) {
            Thread.sleep(1000);
        }

        /* add "other" regions to each region */
        doSysOp(localHandle, "create region " + remoteRegion);
        doSysOp(remoteHandle, "create region " + localRegion);
    }

    @AfterClass
    public static void staticTearDown() throws Exception {
        /* shutdown agents first */
        if (localService != null) {
            localService.shutdown();
        }
        if (remoteService != null) {
            remoteService.shutdown();
        }
        if (localProxy != null) {
            localProxy.shutdown(3, TimeUnit.SECONDS);
        }
        if (remoteProxy != null) {
            remoteProxy.shutdown(3, TimeUnit.SECONDS);
        }

        if (localStore != null) {
            localKV.close();
            localStore.shutdown();
        }
        if (remoteStore != null) {
            remoteKV.close();
            remoteStore.shutdown();
        }
        cleanupTestDir();
    }

    @Override
    public void setUp()
        throws Exception {
        KVProxyTest.dropAllMetadata(localHandle);
        KVProxyTest.dropAllMetadata(remoteHandle);
    }

    @Override
    public void tearDown()
        throws Exception {
        KVProxyTest.dropAllMetadata(localHandle);
        KVProxyTest.dropAllMetadata(remoteHandle);
    }

    @Test
    public void testJsonCollection() throws Exception {

        doSysOp(localHandle, "create table foo(id number, primary key(id), " +
                "counter as integer mr_counter) " +
                "in regions LOC, REM as json collection");
        doSysOp(remoteHandle, "create table foo(id number, primary key(id), " +
                "counter as integer mr_counter) " +
                "in regions REM, LOC as json collection");

        /* test fix for DDL generation for MR tables */
        GetTableRequest gtr = new GetTableRequest().setTableName("foo");
        TableResult tr = localHandle.getTable(gtr);
        assertNotNull(tr.getDdl());

        BigDecimal bd = new BigDecimal("1");

        // put row in local
        MapValue value = new MapValue();
        value.put("Id", bd); // use "Id" to test case-insensitivity of pkey
        value.putFromJson("some_json", "{\"a\": {\"b\": {\"c\":1}}}", null);
        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName("foo");

        /* initial local put */
        PutResult res = localHandle.put(putRequest);

        /* local get */
        MapValue key = new MapValue().put("id", bd);
        GetRequest getRequest = new GetRequest()
            .setKey(key)
            .setTableName("foo");
        GetResult gres = localHandle.get(getRequest);
        assertTrue(gres.getValue().get("counter").getInt() == 0);

        /* wait for replication to remote */
        gres = remoteHandle.get(getRequest);
        while (gres.getValue() == null) {
            Thread.sleep(500);
            gres = remoteHandle.get(getRequest);
        }
        assertTrue(gres.getValue().get("counter").getInt() == 0);

        List<MapValue> queryRes =
            doQuery(localHandle, "update foo $f set $f.counter = $ + 5 where " +
                    "id = 1");

        /* wait for replication to remote, failure is an infinite loop */
        while(gres.getValue().get("counter").getInt() != 5) {
            Thread.sleep(500);
            gres = remoteHandle.get(getRequest);
        }

        // update non-counter data, make sure it's not changed
        value.putFromJson("some_json", "{\"a\": 25}", null);
        res = localHandle.put(putRequest);
        gres = localHandle.get(getRequest);
        assertTrue(gres.getValue().get("counter").getInt() == 5);
        assertTrue(gres.getValue().
                   get("some_json").asMap().get("a").getInt() == 25);
    }

    private static CreateStore createStore(String rootDir,
                                           String storeName,
                                           int port) throws Exception {

        CreateStore cs = new CreateStore(rootDir,
                                         storeName,
                                         port,
                                         1, /* n SNs */
                                         1, /* rf */
                                         10, /* n partitions */
                                         1, /* capacity per SN */
                                         CreateStore.MB_PER_SN,
                                         false, /* use threads is false */
                                         null);
        final File root = new File(rootDir);
        root.mkdirs();
        cs.start();
        return cs;
    }

    private static KVStoreConfig createKVConfig(CreateStore cs) {
        final KVStoreConfig config = cs.createKVConfig();
        return config;
    }

    private static JsonConfig createXRConfig(String testPath,
                                             String regionName,
                                             String storeName,
                                             HashSet<String> helpers) {
        return new JsonConfig(testPath, 1, 0, regionName, storeName, helpers);
    }

    private static XRegionService createXRService(JsonConfig conf,
                                                  Logger logger) {
        return new XRegionService(conf, logger);
    }

    private static NoSQLHandle getHandle(String endpoint) {
        NoSQLHandleConfig config = new NoSQLHandleConfig(endpoint);
        config.setAuthorizationProvider(authProvider);

        /*
         * Open the handle
         */
        return NoSQLHandleFactory.createNoSQLHandle(config);
    }

    private static SystemResult doSysOp(NoSQLHandle handle,
                                String statement) {
        return KVProxyTest.doSysOp(handle, statement);
    }
}
