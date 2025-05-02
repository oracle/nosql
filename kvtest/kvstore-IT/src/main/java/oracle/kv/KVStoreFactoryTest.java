/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv;

import static oracle.kv.util.TestUtils.checkCause;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.rmi.ConnectIOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.security.login.UserLoginManager;
import oracle.kv.impl.util.SSLTestUtils;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.AsyncControl;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.ssl.SSLClientSocketFactory;
import oracle.kv.util.CreateStore;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KVStoreFactoryTest extends TestBase {

    private static final String useAsyncProp =
        System.getProperty(KVStoreConfig.USE_ASYNC);
    private static final String serverUseAsyncProp =
        System.getProperty(AsyncControl.SERVER_USE_ASYNC);
    private static final boolean initialServerUseAsync =
        AsyncControl.serverUseAsync;
    private static final String EXTRAARGS = "oracle.kv.jvm.extraargs";
    private static final String extraargs = System.getProperty(EXTRAARGS);
    private static final String USER_NAME = "admin";
    private static final String USER_PW = "NoSql00__7654321";

    CreateStore createStore;
    KVStore kvstore;

    @Override
    @Before
    public void setUp()
        throws Exception {

        super.setUp();
    }

    @Override
    @After
    public void tearDown()
        throws Exception {

        super.tearDown();
        if (kvstore != null) {
            kvstore.close();
            kvstore = null;
        }
        if (createStore != null) {
            createStore.shutdown();
            createStore = null;
        }

        /* Revert any changes to async configuration */
        if (useAsyncProp == null) {
            System.clearProperty(KVStoreConfig.USE_ASYNC);
        } else {
            System.setProperty(KVStoreConfig.USE_ASYNC, useAsyncProp);
        }
        if (serverUseAsyncProp == null) {
            System.clearProperty(AsyncControl.SERVER_USE_ASYNC);
        } else {
            System.setProperty(AsyncControl.SERVER_USE_ASYNC,
                               serverUseAsyncProp);
        }
        AsyncControl.serverUseAsync = initialServerUseAsync;
        if (extraargs == null) {
            System.clearProperty(EXTRAARGS);
        } else {
            System.setProperty(EXTRAARGS, extraargs);
        }
    }

    /* Tests */

    /** Non-secure, sync-only server */
    @Test
    public void testGetStoreNonSecureSyncOnlyServer()
        throws Exception {

        createStore(false /* secure */, false /* async */,
                    false /* adminOnlySN */);

        testGetStoreNonSecureSyncOnlyServer(true /* storeIter */);
        testGetStoreNonSecureSyncOnlyServer(false /* storeIter */);
    }

    /**
     * Non-secure, sync-only server, using admin-only SN
     *
     * @since 24.2
     */
    @Test
    public void testGetStoreNonSecureSyncOnlyServerAdminOnlySN()
        throws Exception {

        createStore(false /* secure */, false /* async */,
                    true /* adminOnlySN */);

        testGetStoreNonSecureSyncOnlyServer(true /* storeIter */);
        testGetStoreNonSecureSyncOnlyServer(false /* storeIter */);
    }

    private void testGetStoreNonSecureSyncOnlyServer(boolean storeIter) {

        /* Non-secure, sync client */
        checkClient(false /* secure */, false /* async */, storeIter);

        /* Non-secure, async client */
        FaultException e = checkException(
            () -> checkClient(false /* secure */, true /* async */, storeIter),
            FaultException.class);
        Throwable cause = e.getCause();
        if (cause instanceof KVStoreException) {
            cause = cause.getCause();
        }
        checkException(cause, IOException.class,
                       "client using the async network protocol");

        /* Secure, sync client. Secure client can talk to non-secure server. */
        checkClient(true /* secure */, false /* async */, storeIter);

        /* Secure, async client */
        e = checkException(
            () -> checkClient(true /* secure */, true /* async */, storeIter),
            FaultException.class);
        cause = e.getCause();
        if (cause instanceof KVStoreException) {
            cause = cause.getCause();
        }
        checkException(cause, IOException.class,
                       "client using the async network protocol");
    }

    /** Non-secure, async server */
    @Test
    public void testGetStoreNonSecureAsyncServer()
        throws Exception {

        createStore(false /* secure */, true /* async */,
                    false /* adminOnlySN */);

        testGetStoreNonSecureAsyncServer(true /* storeIter */);
        testGetStoreNonSecureAsyncServer(false /* storeIter */);
    }

    /**
     * Non-secure, async server, using admin-only SN
     *
     * @since 24.2
     */
    @Test
    public void testGetStoreNonSecureAyncServerAdminOnlySN()
        throws Exception {

        createStore(false /* secure */, true /* async */,
                    true /* adminOnlySN */);

        testGetStoreNonSecureAsyncServer(true /* storeIter */);
        testGetStoreNonSecureAsyncServer(false /* storeIter */);
    }

    private void testGetStoreNonSecureAsyncServer(boolean storeIter) {

        /* Non-secure, sync client */
        checkClient(false /* secure */, false /* async */, storeIter);

        /* Non-secure, async client */
        checkClient(false /* secure */, true /* async */, storeIter);

        /* Secure sync client. Secure client can talk to non-secure server. */
        checkClient(true /* secure */, false /* async */, storeIter);

        /* Secure, async client. Secure client can talk to non-secure store. */
        checkClient(true /* secure */, true /* async */, storeIter);
    }

    /** Secure, sync-only server */
    @Test
    public void testGetStoreSecureSyncOnlyServer()
        throws Exception {

        createStore(true /* secure */, false /* async */,
                    false /* adminOnlySN */);

        testGetStoreSecureSyncOnlyServer(true /* storeIter */);
        testGetStoreSecureSyncOnlyServer(false /* storeIter */);
    }

    /**
     * Secure, sync-only server, using admin-only SN
     *
     * @since 24.2
     */
    @Test
    public void testGetStoreSecureSyncOnlyServerAdminOnlySN()
        throws Exception {

        createStore(true /* secure */, false /* async */,
                    true /* adminOnlySN */);

        testGetStoreSecureSyncOnlyServer(true /* storeIter */);
        testGetStoreSecureSyncOnlyServer(false /* storeIter */);
    }

    private void testGetStoreSecureSyncOnlyServer(boolean storeIter) {

        /* Non-secure, sync client */
        FaultException e = checkException(
            () -> checkClient(false /* secure */, false /* async */, storeIter),
            FaultException.class, "Could not contact");
        KVStoreException cause =
            checkCause(e, KVStoreException.class, "Could not contact");
        checkCause(cause, ConnectIOException.class, "security mismatch");

        /* Non-secure, async client */
        e = checkException(
            () -> checkClient(false /* secure */, true /* async */, storeIter),
            FaultException.class, "Could not contact");
        cause = checkCause(e, KVStoreException.class, "Could not contact");
        final ConnectIOException connectIOException =
            checkCause(cause, ConnectIOException.class, null);
        final String msg = connectIOException.getMessage();
        assertTrue("Unexpected exception message: " + msg,
                   msg.contains("security mismatch") ||
                   msg.contains(
                       "Problem connecting to the storage node agent"));

        /* Secure, sync client */
        checkClient(true /* secure */, false /* async */, storeIter);

        /* Secure, async client */
        e = checkException(
            () -> checkClient(true /* secure */, true /* async */, storeIter),
            FaultException.class);
        if (e.getCause() instanceof KVStoreException) {
            cause = checkCause(e, KVStoreException.class, "initial login");
            checkCause(cause, IOException.class,
                       "client using the async network protocol");
        } else {
            checkCause(cause, IOException.class, "security mismatch");
        }
    }

    /** Secure, async server */
    @Test
    public void testGetStoreSecureAsyncServer()
        throws Exception {

        createStore(true /* secure */, true /* async */,
                    false /* adminOnlySN */);

        testGetStoreSecureAsyncServer(true /* storeIter */);
        testGetStoreSecureAsyncServer(false /* storeIter */);
    }

    /**
     * Secure, async server, using admin-only SN
     *
     * @since 24.2
     */
    @Test
    public void testGetStoreSecureAsyncServerAdminOnlySN()
        throws Exception {

        createStore(true /* secure */, true /* async */,
                    true /* adminOnlySN */);

        testGetStoreSecureAsyncServer(true /* storeIter */);
        testGetStoreSecureAsyncServer(false /* storeIter */);
    }

    private void testGetStoreSecureAsyncServer(boolean storeIter) {

        /* Non-secure, sync client */
        FaultException e = checkException(
            () -> checkClient(false /* secure */, false /* async */, storeIter),
            FaultException.class, "Could not contact");
        KVStoreException cause =
            checkCause(e, KVStoreException.class, "Could not contact");
        checkCause(cause, ConnectIOException.class, "security mismatch");

        /* Non-secure, async client */
        e = checkException(
            () -> checkClient(false /* secure */, true /* async */, storeIter),
            FaultException.class, "Could not contact");
        cause = checkCause(e, KVStoreException.class, "Could not contact");
        checkCause(cause, ConnectIOException.class, "security mismatch");

        /* Secure, sync client */
        checkClient(true /* secure */, false /* async */, storeIter);

        /* Secure async client */
        checkClient(true /* secure */, true /* async */, storeIter);
    }

    /**
     * Test that resources are released when the attempt to open the store
     * fails. [KVSTORE-1517]
     */
    @Test
    public void testReleaseOnFailSecureAsync() throws Exception {
        testReleaseOnFail(true /* secure */, true /* async */);
    }

    @Test
    public void testReleaseOnFailNonSecureAsync() throws Exception {
        testReleaseOnFail(false /* secure */, true /* async */);
    }

    @Test
    public void testReleaseOnFailSecureRMI() throws Exception {
        testReleaseOnFail(true /* secure */, false /* async */);
    }

    @Test
    public void testReleaseOnFailNonSecureRMI() throws Exception {
        testReleaseOnFail(false /* secure */, false /* async */);
    }

    private void testReleaseOnFail(boolean secure, boolean async)
        throws Exception
    {
        /* Start with a known state */
        SSLClientSocketFactory.clearUserSSLControlMap();
        RegistryUtils.clearRegistryCSF();
        ClientSocketFactory.clearConfiguration();
        final int userLoginManagerActiveCount =
            UserLoginManager.getActiveCount();

        final Properties secProps =
            secure ? createUserLoginProperties() : null;
        final int count = 50;
        for (int i = 0; i < count; i++) {
            final KVStoreConfig kvConfig =
                /*
                 * Use the same store name each time. We retain a bunch of
                 * items keyed on both the client ID and the store name. We
                 * clear out the ones associated with the client ID, but not
                 * the store name ones, because there is no bookkeeping for
                 * multiple KVStore instances associated with the same store
                 * name. At least in KVSTORE-1517, the issue was attempting to
                 * open the same store multiple times, not opening multiple
                 * stores, so assume that the per-store-name information is not
                 * decached.
                 */
                new KVStoreConfig("mystore", "bad-host-name" + i + ":5000");
            if (secure) {
                kvConfig.setSecurityProperties(secProps);
            }
            kvConfig.setUseAsync(async);
            try {
                kvstore = KVStoreFactory.getStore(kvConfig);
                fail("Expected store creation to fail");
            } catch (FaultException e) {
            }
        }

        assertEquals("userSSLControlMapSize",
                     /* One entry associated with the store name */
                     secure ? 1 : 0,
                     SSLClientSocketFactory.getUserSSLControlMapSize());
        assertEquals("registryCSFMapSize",
                     /* One entry associated with the store name */
                     1, RegistryUtils.getRegistryCSFMapSize());
        assertEquals("userLoginManagerActiveCount",
                     userLoginManagerActiveCount,
                     UserLoginManager.getActiveCount());
        assertEquals("serviceToTimeoutsMapSize",
                     /* One entry associated with the store name */
                     1, ClientSocketFactory.getServiceToTimeoutsMapSize());
    }

    /* Other methods */

    private void createStore(boolean secure, boolean async, boolean adminOnlySN)
        throws Exception {

        System.setProperty(AsyncControl.SERVER_USE_ASYNC,
                           String.valueOf(async));
        AsyncControl.serverUseAsync = async;
        System.setProperty(KVStoreConfig.USE_ASYNC, String.valueOf(async));
        System.setProperty(
            EXTRAARGS,
            (extraargs == null ? "" : extraargs + ";") +
            "-D" + AsyncControl.SERVER_USE_ASYNC + "=" + async +
            " -D" + KVStoreConfig.USE_ASYNC + "=" + async);
        createStore = new CreateStore(kvstoreName,
                                      5000, /* startPort */
                                      adminOnlySN ? 2 : 1, /* numStorageNodes */
                                      1, /* replicationFactor */
                                      10, /* numPartitions */
                                      adminOnlySN ? new int[] {0, 1} :
                                          new int[] {1}, /* capacities */
                                      CreateStore.MB_PER_SN, /* memoryMB */
                                      true, /* useThreads */
                                      null, /* mgmtImpl */
                                      false, /* mgmtPortsShared */
                                      secure); /* secure */
        createStore.setAdminLocations(1);
        createStore.start();
    }

    private void checkClient(boolean secure,
                             boolean async,
                             boolean storeIter) {
        if (kvstore != null) {
            kvstore.close();
            kvstore = null;
        }
        final KVStoreConfig kvConfig = createStore.createKVConfig(secure);
        kvConfig.setUseAsync(async);
        kvstore = KVStoreFactory.getStore(kvConfig);
        final Key key = Key.createKey("/a");
        if (!storeIter) {
            kvstore.get(key);
            return;
        }
        try {
            kvstore.storeIterator(Collections.singletonList(key).iterator(),
                                  0, /* batch size */
                                  null, /* subRange */
                                  null, /* depth */
                                  null, /* consistency */
                                  5000, /* timeout */
                                  TimeUnit.MILLISECONDS,
                                  null) /* storeIteratorConfig */
                .hasNext();
        } catch (StoreIteratorException e) {
            throw checkCause(e, FaultException.class, null);
        }
    }

    private static Properties createUserLoginProperties() throws Exception {
        final File testDir = TestUtils.getTestDir();
        final File passwordFile = new File(testDir, "test.passwd");
        TestUtils.makePasswordFile(passwordFile, USER_NAME, USER_PW);
        final Properties props = new Properties();
        props.put(KVSecurityConstants.AUTH_USERNAME_PROPERTY,USER_NAME);
        props.put(KVSecurityConstants.AUTH_PWDFILE_PROPERTY,
                  passwordFile.toString());
        props.put(KVSecurityConstants.TRANSPORT_PROPERTY,
                  KVSecurityConstants.SSL_TRANSPORT_NAME);
        props.put(KVSecurityConstants.SSL_TRUSTSTORE_TYPE_PROPERTY, "PKCS12");
        props.put(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                  new File(SSLTestUtils.getTestSSLDir(),
                           SSLTestUtils.SSL_TS_NAME).toString());
        return props;
    }
}
