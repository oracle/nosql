/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.net.URL;

import org.junit.BeforeClass;
import org.junit.Test;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.TimeToLive;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.proxy.security.SecureTestUtil;

public class SslTest extends ProxyTestBase {

    @BeforeClass
    public static void staticSetUp()
        throws Exception {
        USE_SSL_HOOK = true;
        ProxyTestBase.staticSetUp();
        USE_SSL_HOOK = false;
    }

    @Override
    public void setUp() throws Exception {
        /*
         * skip this test if onprem. The proxy equates SSL with secure,
         * and expects an auth string, which is not presented
        */
        org.junit.Assume.assumeTrue(!onprem);
        super.setUp();
    }

    /*
     * Use SSL handle
     */
    @Override
    protected NoSQLHandle configHandle(String endpoint) {
        if (cloudRunning) {
            return super.configHandle(endpoint);
        }
        NoSQLHandleConfig hconfig =
            new NoSQLHandleConfig(getProxyHttpsEndpoint());
        return setupHandle(hconfig);
    }

    @Test
    public void sslTest()
        throws Exception {

        /* SSL is not tested in cloud environments. See ProxyTestBase.setUp() */
        if (sslHandle == null) {
            return;
        }
        sslTest(sslHandle);

        URL url = new URL("https", getProxyHost(), getProxyHttpsPort(), "/");

        /*
         * Get another handle with SSL configuration parameters to exercise
         * a handle with them configured.
         */
        sslTest(getSslHandle(url));

        /*
         * Test TLSv1.3
         */
        sslTest(getTls13SslHandle(url));
    }

    private NoSQLHandle getSslHandle(URL url) {
        NoSQLHandleConfig hconfig = new NoSQLHandleConfig(url);

        /*
         * 5 retries, default retry algorithm
         */
        hconfig.configureDefaultRetryHandler(5, 0);
        hconfig.setRequestTimeout(30000);

        hconfig.setSSLCipherSuites(
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
        hconfig.setSSLSessionCacheSize(100);
        hconfig.setSSLSessionTimeout(1200);

        SecureTestUtil.setAuthProvider(hconfig, isSecure(),
                                       onprem, getTenantId());
        return getHandle(hconfig);
    }

    private NoSQLHandle getTls13SslHandle(URL url) {
        NoSQLHandleConfig hconfig = new NoSQLHandleConfig(url);

        /*
         * 5 retries, default retry algorithm
         */
        hconfig.configureDefaultRetryHandler(5, 0);
        hconfig.setRequestTimeout(30000);

        hconfig.setSSLProtocols("TLSv1.3");

        /* set TLSv1.3 cipher suite */
        hconfig.setSSLCipherSuites("TLS_AES_128_GCM_SHA256");
        hconfig.setSSLSessionCacheSize(100);
        hconfig.setSSLSessionTimeout(1200);

        SecureTestUtil.setAuthProvider(hconfig, isSecure(),
                                       onprem, getTenantId());
        return getHandle(hconfig);
    }

    private void sslTest(NoSQLHandle testHandle) {
        MapValue value = new MapValue().put("id", 10).put("name", "jane");

        TableResult tres = tableOperation(
            testHandle,
            "create table if not exists users(id integer, " +
            "name string, primary key(id))",
            new TableLimits(500, 500, 50),
            TableResult.State.ACTIVE,
            10000);
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        /* PUT */
        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName("users");

        PutResult res = testHandle.put(putRequest);
        assertNotNull("Put failed", res.getVersion());
        assertWriteKB(res);
        /* put a few more. set TTL to test that path */
        putRequest.setTTL(TimeToLive.ofHours(2));
        for (int i = 20; i < 30; i++) {
            value.put("id", i);
            testHandle.put(putRequest);
        }

        /* QUERY */
        QueryRequest queryRequest =
            new QueryRequest().setStatement("select * from users");

        QueryResult queryRes = testHandle.query(queryRequest);
        assertEquals(11, queryRes.getResults().size());
    }
}
