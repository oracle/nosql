/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2018 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.proxy.ProxyTestBase;
import oracle.nosql.proxy.security.SecureTestUtil;
import oracle.nosql.util.tmi.TableRequestLimits;
import oracle.nosql.util.tmi.TenantLimits;
/**
 * Class extends ProxyTestBase for set up proxy test environment
 */
public class ProxyOperation extends ProxyTestBase{
    protected static NoSQLHandle staticHandle;
    static NoSQLHandle sslHandle;

    /*
     * An instance with non-default limits to make tests run reasonably
     */

    protected static TenantLimits qtfTenantLimits =
        TenantLimits.getNewDefault();
    static {
        qtfTenantLimits.setNumTables(100)
                        .setTenantSize(500000)
                        .setTenantReadUnits(8000000)
                        .setTenantWriteUnits(4000000)
                        .setDdlRequestsRate(4000)
                        .setTableLimitReductionsRate(40);
        TableRequestLimits tableLimits =
            qtfTenantLimits.getStandardTableLimits();
        tableLimits.setTableSize(500000)
                   .setTableReadUnits(200000)
                   .setTableWriteUnits(200000)
                   .setIndexesPerTable(40)
                   .setSchemaEvolutions(6);
    }

    @BeforeClass
    public static void staticSetUp()
        throws Exception {
        System.setProperty(ProxyTestBase.KVLITE_MULTISHARD_PROP, "true");
        ProxyTestBase.staticSetUp(qtfTenantLimits);

        staticHandle = configHandleStatic(getProxyEndpoint());
        setOpThrottling(getTenantId(), NO_OP_THROTTLE);
        /*
         * Only configure https if not running in minicloud, for now.
         */
        if (!cloudRunning && SSLRunning) {
            sslHandle = configHandleStatic("https://"+ hostName + ":" +
                                           PROXY_HTTPS_PORT);
        }
    }

    @AfterClass
    public static void staticTearDown()
        throws Exception {
        if (staticHandle != null) {
            staticHandle.close();
        }

        setOpThrottling(getTenantId(), DEFAULT_OP_THROTTLE);

        if (sslHandle != null) {
            sslHandle.close();
        }
        String path = "oracle.nosql.query.QTest.test";
        copyEnvironments(path);
        ProxyTestBase.staticTearDown();
    }

    @Override
    @Before
    public void setUp() throws Exception {
    }

    @Override
    @After
    public void tearDown() throws Exception {
    }

    public static NoSQLHandle getNosqlHandle() {
        return staticHandle;
    }

    private static NoSQLHandle configHandleStatic(String endpoint) {
        return configHandleStatic(endpoint, onprem);
    }

    protected static NoSQLHandle configHandleStatic(String endpoint,
                                                    boolean onprem) {

        NoSQLHandleConfig hconfig = new NoSQLHandleConfig(endpoint);

        /*
         * 5 retries, default retry algorithm
         */
        hconfig.configureDefaultRetryHandler(5, 0);
        hconfig.setRequestTimeout(30000);

        SecureTestUtil.setAuthProvider(hconfig, isSecure(),
                                       onprem, getTenantId());
        return getHandleStatic(hconfig);
    }

    /**
     * Allows classes to create a differently-configured NoSQLHandle.
     */
    protected static NoSQLHandle getHandleStatic(NoSQLHandleConfig config) {
        /*
         * Open the handle
         */
        return NoSQLHandleFactory.createNoSQLHandle(config);
    }

}
