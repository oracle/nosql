/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util.registry;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static oracle.kv.impl.util.TestUtils.fastSerialize;
import static oracle.kv.impl.util.TestUtils.safeUnexport;
import static oracle.kv.impl.util.TestUtils.serialize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.rmi.AccessException;
import java.rmi.ConnectIOException;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.UnmarshalException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.TimeUnit;

import oracle.kv.KVStoreConfig;
import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.util.SSLTestUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests the client socket factory used for RMI requests
 */

public class ClientSocketFactoryTest extends SocketFactoryTestBase {
    private static final String TIMER_THREAD_NAME = "KVClientSocketTimeout";

    private boolean hasTimerThread() {
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().equals(TIMER_THREAD_NAME)) {
                return true;
            }
        }
        return false;
    }

    /*
     * The value of this property represents the maximum length of time (in
     * milliseconds) for which socket connections may reside in a "unused"
     * state, before the Java RMI runtime will allow those connections to be
     * freed (closed). The default value is 15000 milliseconds (15 seconds).
     * For test purpose we set it to 1/10 of the default value to reduce the
     * over all test-duration.
     */
    @BeforeClass
    public static void setProperties() {
        System.setProperty("sun.rmi.transport.connectionTimeout", "1500");
    }

    @AfterClass
    public static void resetProperties() {
        System.setProperty("sun.rmi.transport.connectionTimeout", "15000");
    }


    @Override
    public void setUp() throws Exception {

        super.setUp();
        SSLTestUtils.setRMIRegistryFilter();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        SSLTestUtils.clearRMIRegistryFilter();
    }

    /**
     * Verify that a socket object is not leaked when the createSocket
     * operation fails.
     */
    @Test
    public void testOpenException() {

        ClientSocketFactory.getSockets().clear();

        final ClientSocketFactory csf = makeCSF(serviceName, 1000, 100);
        try {
            /*
             * Invalid IP address -- leading octet cannot be zero
             */
            csf.createSocket("0.2.3.4", 1000);
            fail("Expected exception");
        } catch (IOException e) {
            /* Expected */
        }

        /*
         * Verify that it's not leaked, that is, it's not retained in the
         * sockets list.
         */
        assertEquals(0, ClientSocketFactory.getSockets().size());
    }

    @Test
    public void testBasic()
        throws RemoteException, NotBoundException {

        ClientSocketFactory csf = makeCSF(serviceName, 1000, 100);
        ClientSocketFactory.setTimeoutLogger(logger);
        ServerSocketFactory ssf = makeSSF(10, 0, 0, 0, true /* optional */);

        bindObject(serviceName, csf, ssf);

        /*
         * Verify that socket factory is created on lookups.
         */
        ClientSocketFactory.setSocketFactoryCount(0);
        RMIObj o = (RMIObj) regStub.lookup(serviceName);
        assertNotNull(o);

        /* Verify that a new one is created on the client. */
        assertEquals(1, ClientSocketFactory.getSocketFactoryCount());

        /*
         * Every lookup results in a CSF being sent over and then potentially
         * being discarded based upon object equality.
         */
        regStub.lookup(serviceName);
        assertEquals(2, ClientSocketFactory.getSocketFactoryCount());

        /*
         * Verify that read timeouts happen. No good way to test connect
         * timeouts, since they are in the network layer.
         */
        try {
            o.test(2000);
            fail("expected timeout");
        } catch (UnmarshalException e) {
            /* Socket will be closed asynchronously by the timeout thread */
            assertTrue(LoggerUtils.getStackTrace(e),
                       e.getCause() instanceof SocketTimeoutException);
        }

        /**
         * Verify that a socket factory is not sent over with each method call.
         */
        ClientSocketFactory.setSocketFactoryCount(0);

        for (int i=0; i < 10; i++) {
            o.test(10);
        }

        assertEquals(0, ClientSocketFactory.getSocketFactoryCount());
    }

    @Test
    public void testTimerThreadManagement()
        throws IOException, InterruptedException, NotBoundException {

        ClientSocketFactory csf = makeCSF(serviceName, 1000, 100);
        ClientSocketFactory.setTimeoutLogger(logger);
        ServerSocketFactory ssf = makeSSF(10, 0, 0, 0, true /* optional */);

        bindObject(serviceName, csf, ssf);
        RMIObj o = (RMIObj) regStub.lookup(serviceName);
        assertNotNull(o);

        try {
            // Check for timeouts every 50 ms
            ClientSocketFactory.changeTimerInterval(50);

            assertTrue("Timer thread should be created", hasTimerThread());

            /* RMI runtime closes idle sockets (1500ms timeout)
             * Plus 3 extra ticks (50ms each) for terminating the timer thread
             */
            Thread.sleep(1650);
            assertFalse("Timer thread should be killed", hasTimerThread());

            o.test(10);
            assertTrue("Timer thread should be created", hasTimerThread());

            try {
                o.test(200);
                fail("expected timeout");
            } catch (UnmarshalException e) {
                // One extra tick for terminating timer thread
                Thread.sleep(90);
                assertFalse("Timer thread should be killed", hasTimerThread());
            }
        } finally {
            /* Revert to standard setting */
            ClientSocketFactory.changeTimerInterval(0);
        }
    }

    private void bindObject(String name, ClientSocketFactory csf,
                            ServerSocketFactory ssf)
        throws RemoteException,
        NoSuchObjectException {

        tearDowns.add(() -> safeUnexport(object));
        Remote stub = UnicastRemoteObject.exportObject(object, 0, csf, ssf);

        try {
            regStub.rebind(name, stub);
        } catch (RemoteException re) {
            fail(re.getMessage());
        }
    }

    /**
     * Test that setting the read timeout always causes RMI calls to fail with
     * a ConnectIOException or UnmarshalException caused by a
     * SocketTimeoutException, even if there is a race between the timeout and
     * the read completing successfully. [#23411]
     */
    @Test
    public void testTimeoutRace()
        throws RemoteException, NotBoundException {

        try {

            /*
             * Check for timeouts every 3 ms, twice each 6 ms timeout, to make
             * sure we notice timed out sockets promptly
             */
            ClientSocketFactory.changeTimerInterval(3);

            /*
             * Use a 6 ms timeout, instead of the 1 second default, to reduce
             * test time
             */
            final ClientSocketFactory csf = makeCSF(serviceName, 1000, 6);

            ClientSocketFactory.setTimeoutLogger(logger);
            final ServerSocketFactory ssf =
                makeSSF(10, 0, 0, 0, true /* optional */);
            bindObject(serviceName, csf, ssf);
            final RMIObj o = (RMIObj) regStub.lookup(serviceName);
            assertNotNull(o);

            final byte[] data = new byte[10000];
            int timeouts = 0;
            for (int i = 0; i < 100; i++) {
                Exception exception = null;
                try {

                    /*
                     * Test with large and small payloads, waiting the same
                     * amount of time as the timeout, to test edge cases where
                     * the read completes just as the timeout occurs.
                     */
                    if (i % 2 == 0) {
                        o.test(6);
                    } else {
                        o.test(6, data);
                    }
                } catch (ConnectIOException e) {
                    exception = e;
                } catch (UnmarshalException e) {
                    exception = e;
                }
                if (exception != null) {
                    final String stackTrace =
                        LoggerUtils.getStackTrace(exception);
                    logger.info(stackTrace);
                    if (!getIsSocketTimeoutException(exception.getCause())) {
                        fail("Cause was not a socket timeout exception: " +
                             stackTrace);
                    }
                    timeouts++;
                }
            }

            /*
             * Always print out the number of timeouts seen, to help determine
             * how much test coverage we are getting.  Since this test is
             * timing sensitive, it doesn't make sense to require there to be
             * timeouts in all cases.
             */
            System.err.println("testTimeoutRace: timeouts=" + timeouts);
        } finally {

            /* Revert to standard setting */
            ClientSocketFactory.changeTimerInterval(0);
        }
    }

    /**
     * Check that an exception was caused by a socket timeout.
     */
    protected boolean getIsSocketTimeoutException(Throwable exception) {
        return exception instanceof SocketTimeoutException;
    }

    /**
     * Verify that the client can override timeouts supplied by the server
     * for a specific service.
     */
    @Test
    public void testClientSideOverrides()
        throws AccessException, RemoteException, NotBoundException {

        ClientSocketFactory csf = makeCSF(serviceName, 1000, 100);
        ServerSocketFactory ssf = makeSSF(10, 0, 0, 0, true /* optional */);
        bindObject(serviceName, csf, ssf);

        RMIObj o = (RMIObj) regStub.lookup(serviceName);
        assertNotNull(o);

        try {
            o.test(3000);
            fail("expected timeout");
        } catch (UnmarshalException e) {
            /* Socket will be closed asynchronously by the timeout thread */
            assertTrue(LoggerUtils.getStackTrace(e),
                       e.getCause() instanceof SocketTimeoutException);
        }

        /* Five second read timeout. */
        ClientSocketFactory.
        configure(serviceName,
                  new KVStoreConfig("Dummy Store", "dummyHost:9000").
                  setSocketOpenTimeout(1000, TimeUnit.MILLISECONDS).
                  setSocketReadTimeout(5000, TimeUnit.MILLISECONDS),
                  null /* clientId */);

        o = (RMIObj) regStub.lookup(serviceName);

        /* Should not fail for the same 3 second request */
        o.test(3000);
    }

    /**
     * These are essential for correct operation of the CSF.
     */
    @Test
    public void testHashAndEquals()
        throws Exception {

        final ClientSocketFactory csf1 = makeCSF("csf", 1000, 100);
        assertEquals(csf1, csf1);

        assertTrue(!csf1.equals(makeCSF("xxx", 1000, 100)));
        assertTrue(!csf1.equals(makeCSF("csf", 0, 100)));
        assertTrue(!csf1.equals(makeCSF("csf", 1000, 0)));

        /* CSF ids prevent two equivalent configurations from being equal */
        assertTrue(!csf1.equals(makeCSF("csf", 1000, 100)));

        /* But CSFs that are created through serialization may be equal */
        final ClientSocketFactory csf2 = serialize(csf1);
        assertTrue(csf1 != csf2);
        assertEquals(csf1, csf2);
        assertEquals(csf1.hashCode(), csf2.hashCode());

        final ClientSocketFactory csf2a = fastSerialize(csf1);
        assertTrue(csf1 != csf2a);
        assertEquals(csf1, csf2a);
        assertEquals(csf1.hashCode(), csf2a.hashCode());

        /* Unless they were created in a different generation */
        ClientSocketFactory.newGeneration();
        final ClientSocketFactory csf3 = serialize(csf1);
        assertTrue(csf1 != csf3);
        assertTrue(!csf1.equals(csf3));

        final ClientSocketFactory csf3a = fastSerialize(csf1);
        assertTrue(csf1 != csf3a);
        assertTrue(!csf1.equals(csf3a));
    }

    /**
     * Test that specifying different client IDs get different socket factory
     * timeouts. [#27952]
     */
    @Test
    public void testClientId()
        throws Exception {

        final ClientId clientId1 = new ClientId(1);
        ClientSocketFactory.configure(
            "test-binding1",
            new KVStoreConfig("kvstore", "localhost:5000")
            .setSocketOpenTimeout(5, MILLISECONDS),
            clientId1);

        final ClientId clientId2 = new ClientId(2);
        ClientSocketFactory.configure(
            "test-binding2",
            new KVStoreConfig("kvstore", "localhost:6000")
            .setSocketOpenTimeout(6, MILLISECONDS),
            clientId2);

        /* Check value passed to constructor */
        final ClientSocketFactory csf =
            new ClientSocketFactory("test", 3, 4, null /* clientId */);
        assertEquals(3, csf.getConnectTimeoutMs());

        /* No client ID by default */
        assertEquals(null, ClientSocketFactory.getCurrentClientId());

        /* Should match constructor value */
        assertEquals(3, serialize(csf).getConnectTimeoutMs());
        assertEquals(3, fastSerialize(csf).getConnectTimeoutMs());

        /* Override with client ID 1 value */
        ClientSocketFactory.setCurrentClientId(clientId1);
        try {
            assertEquals(5, serialize(csf).getConnectTimeoutMs());
            assertEquals(5, fastSerialize(csf).getConnectTimeoutMs());
        } finally {
            ClientSocketFactory.setCurrentClientId(null);
        }

        /* Override with client ID 2 value */
        ClientSocketFactory.setCurrentClientId(clientId2);
        try {
            assertEquals(6, serialize(csf).getConnectTimeoutMs());
            assertEquals(6, fastSerialize(csf).getConnectTimeoutMs());
        } finally {
            ClientSocketFactory.setCurrentClientId(null);
        }
    }
}
