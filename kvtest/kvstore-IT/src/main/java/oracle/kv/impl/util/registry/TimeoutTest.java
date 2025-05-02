/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.util.registry;

import static oracle.kv.impl.util.TestUtils.DEFAULT_SSF;
import static oracle.kv.impl.util.TestUtils.DEFAULT_THREAD_POOL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.rmi.ConnectIOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.UnmarshalException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import oracle.kv.FaultException;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.RequestTimeoutException;
import oracle.kv.TestBase;
import oracle.kv.impl.async.CreatorEndpoint;
import oracle.kv.impl.async.DialogType;
import oracle.kv.impl.async.DialogTypeFamily;
import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.async.JavaSerialInitiatorProxy;
import oracle.kv.impl.async.JavaSerialResponder;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.TestUtils;

import org.junit.Test;

/**
 * Test client connection timeouts.  Do this independently of KVStore itself
 * but still exercise the ClientSocketFactory in utils/registry.
 */
public class TimeoutTest extends TestBase {

    public static final int registryPort = 13555;
    public static final String unroutable = "10.255.255.1";

    private Registry registry;
    private ListenHandle registryHandle;

    @Override
    public void setUp() throws Exception {

        super.setUp();
        createRegistry();
        ClientSocketFactory.setTimeoutLogger(logger);
    }

    @Override
    public void tearDown() throws Exception {

        destroyRegistry();
        ClientSocketFactory.setTimeoutLogger(null);
        super.tearDown();
    }

    protected ServerSocketFactory makeSSF() {
        return DEFAULT_SSF;
    }

    protected ClientSocketFactory makeCSF(String serviceName,
                                          int connectTMO,
                                          int otherTMO) {
        return new ClearClientSocketFactory(serviceName, connectTMO, otherTMO,
                                            null /* clientId */);
    }

    protected ClientSocketFactory makeRegistryCSF(String serviceName,
                                                  int connectTMO,
                                                  int otherTMO) {
        return new ClearClientSocketFactory(serviceName, connectTMO, otherTMO,
                                            null /* clientId */);
    }

    @Test
    public void testReadTimeout()
        throws Exception {

        Timeout to = new Timeout();

        ListenHandle listenHandle =
            export(to, makeSSF(),
                   makeCSF(TimeoutInterface.BINDING_NAME, 1000, 1000));
        final TimeoutInterface ti = RegistryUtils.getRemoteService(
            null /* storeName */, "localhost", registryPort,
            TimeoutInterface.BINDING_NAME,
            TimeoutTest::createTimeoutInitiatorProxy, logger);

        final AtomicReference<AssertionError> threadFailure =
            new AtomicReference<AssertionError>();
        final Thread[] threads = new Thread[4];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {
                { start(); }
                @Override
                public void run() {
                    try {
                        ti.callTimeout(SerialVersion.CURRENT);
                        failure("Call should have timed out", null);
                    } catch (UnmarshalException e) {
                        if (e.getCause() instanceof SocketTimeoutException) {
                            logger.info("Got expected exception: " + e);
                        } else {
                            failure("Expected SocketTimeoutException, found: " +
                                    e.getCause(),
                                    null);
                        }
                    } catch (RequestTimeoutException e) {
                        assertEquals(1000, e.getTimeoutMs());
                    } catch (RemoteException e) {
                        if (e.getCause() instanceof TimeoutException ||
                            e.getCause() instanceof RequestTimeoutException) {
                            logger.info("Got expected exception: " + e);
                        } else {
                            failure("Expected TimeoutException or" +
                                    " RequestTimeoutException, found: " +
                                    e.getCause(),
                                    e);
                        }
                    } catch (Throwable e) {
                        failure("Unexpected exception: " + e, e);
                    }
                }
                private void failure(String msg, Throwable cause) {
                    final AssertionError err = new AssertionError(msg);
                    err.initCause(cause);
                    threadFailure.compareAndSet(null, err);
                }
            };
        }
        for (final Thread thread : threads) {
            thread.join(3000);
            assertFalse("Thread should have exited", thread.isAlive());
        }
        if (threadFailure.get() != null) {
            throw threadFailure.get();
        }
        unexport(to, listenHandle);
    }

    @Test
    public void testConnectTimeout()
        throws Exception {

        final int connectTimeoutMs = 1000;
        final int readTimeoutMs = 1000;
        final ClientSocketFactory registryCSF =
            makeRegistryCSF("registry",
                    connectTimeoutMs,
                    readTimeoutMs);

        try {
            Registry r = LocateRegistry.getRegistry
                (unroutable, registryPort, registryCSF);
            r.lookup(TimeoutInterface.BINDING_NAME);
            fail("Call should have failed");
        } catch (ConnectIOException e) {
            logger.info("Got expected exception: " + e);
        }

        /*
         * Do it again but using the static RegistryUtils method.
         */
        try {
            RegistryUtils.setRegistrySocketTimeouts(connectTimeoutMs,
                                                    readTimeoutMs,
                                                    null /* storeName */,
                                                    null /* clientId */);
            RegistryUtils.getServiceNames(null /* storeName */,
                                          unroutable, registryPort,
                                          Protocols.getDefault(),
                                          null /* clientId */, logger);
            fail("Call should have failed");
        } catch (ConnectIOException e) {
            logger.info("Got expected exception: " + e);
        }
    }

    /*
     * Test timeouts using the public API.
     */
    @Test
    public void testKVStoreConfig()
        throws Exception {

        KVStoreConfig config;

        /* Make sure that the timeouts are large by default. */
        RegistryUtils.setRegistrySocketTimeouts(0, 0, null, null);

        String hostPort = unroutable + ":" + registryPort;
        config = new KVStoreConfig("mystore", hostPort);

        final int openTimeout = 500;
        final int maxTimeout =
            /*
             * If both async and RMI are enabled, then there will be two open
             * attempts. This behavior will go away when RMI is disabled but,
             * until then, allow for this.
             */
            (KVStoreConfig.getDefaultUseAsync() ? openTimeout : 0) +
            /*
             * The RMI timeout check is only performed once a second, so need
             * to add that amount of time to the expected timeout
             */
            (KVStoreConfig.getDefaultUseRmi() ? openTimeout + 1000 : 0) +
            KVStoreConfig.DEFAULT_NETWORK_ROUNDTRIP_TIMEOUT +
            /* Give it a 200 ms slop factor to allow for startup delays */
            200;
        config.setRegistryOpenTimeout(openTimeout, TimeUnit.MILLISECONDS);
        long time1 = System.currentTimeMillis();
        try {
            KVStoreFactory.getStore(config);
            fail("Call should have failed");
        } catch (FaultException e) {
            long actualTimeout = System.currentTimeMillis() - time1;
            logger.info("Got expected exception: " + e +
                        " timeout: " + actualTimeout);
            assertTrue("Time should be less than " + maxTimeout +
                       ", found: " + actualTimeout,
                       actualTimeout < maxTimeout);
        }
    }

    /*
     * Internal functions, interfaces, and classes.
     */
    private ListenHandle export(TimeoutInterface remote,
                                ServerSocketFactory ssf,
                                ClientSocketFactory csf)
        throws RemoteException {

        return RegistryUtils.rebind("localhost", registryPort,
                                    TimeoutInterface.BINDING_NAME,
                                    new StorageNodeId(1),
                                    remote, csf, ssf,
                                    TimeoutDialogTypeFamily.FAMILY,
                                    () -> new TimeoutResponder(remote, logger),
                                    logger);
    }

    private void unexport(Remote remote, ListenHandle listenHandle)
        throws RemoteException {

        RegistryUtils.unbind("localhost", registryPort,
                             TimeoutInterface.BINDING_NAME, remote,
                             listenHandle, logger);
    }

    private void createRegistry()
        throws IOException {

        registry = TestUtils.createRegistry(registryPort);
        if (AsyncRegistryUtils.serverUseAsync) {
            registryHandle = TestUtils.createServiceRegistry(registryPort);
        }
    }

    private void destroyRegistry() {

        if (registry != null) {
            TestUtils.destroyRegistry(registry);
        }
        if (registryHandle != null) {
            registryHandle.shutdown(true);
        }

        /* Wait for the registry socket to come free. */
        final int timeoutMs = 30000;
        final boolean registryPortIsFree =
            new PollCondition(100, timeoutMs) {
                @Override
                protected boolean condition() {
                    try {
                        ServerSocket serverSocket =
                            new ServerSocket(registryPort);
                        serverSocket.close();
                        return true;
                    } catch (IOException e) {
                       return false;
                    }
                }
            }.await();
        assertTrue("Registry port:" + registryPort + " not free. " +
                   " after " + timeoutMs + "ms",
                   registryPortIsFree);
    }

    /**
     * A simple interface to use for RMI for the test.
     */
    public interface TimeoutInterface extends VersionedRemote {
        String BINDING_NAME = "timeout_interface";
        String callTimeout(short serialVersion) throws RemoteException;
    }

    /**
     * An implementation of the interface that takes a long time to respond,
     * generating a client-side read timeout.
     */
    private class Timeout implements TimeoutInterface {

        @Override
        public short getSerialVersion() {
            return SerialVersion.CURRENT;
        }

        @Override
        public String callTimeout(short serialVersion) {
            try {
                Thread.sleep(4000);
            } catch (InterruptedException ignored)  {
            }
            return "This shouldn't happen";
        }
    }

    static class TimeoutDialogTypeFamily implements DialogTypeFamily {
        static TimeoutDialogTypeFamily FAMILY = new TimeoutDialogTypeFamily();
        private TimeoutDialogTypeFamily() {
            DialogType.registerTypeFamily(this);
        }
        @Override
        public int getFamilyId() {
            return 88;
        }
        @Override
        public String getFamilyName() {
            return "TimeoutDialogTypeFamily";
        }
        @Override
        public String toString() {
            return getFamilyName() + "(" + getFamilyId() + ")";
        }
    }

    static TimeoutInterface
        createTimeoutInitiatorProxy(CreatorEndpoint endpoint,
                                    DialogType dialogType,
                                    long timeoutMs,
                                    Logger logger) {
        return JavaSerialInitiatorProxy.createProxy(
            TimeoutInterface.class, TimeoutDialogTypeFamily.FAMILY,
            endpoint, dialogType, timeoutMs, logger);
    }

    static class TimeoutResponder
            extends JavaSerialResponder<TimeoutInterface> {
        TimeoutResponder(TimeoutInterface server, Logger logger) {
            super(server, TimeoutInterface.class, DEFAULT_THREAD_POOL,
                  TimeoutDialogTypeFamily.FAMILY, logger);
        }
    }
}
