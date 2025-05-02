/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util.registry;

import static oracle.kv.impl.util.TestUtils.safeUnexport;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.ServerSocket;
import java.rmi.AccessException;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

/**
 * Test the SSF used for RMI requests.
 * This test is subclased by SSLServerSocketFactoryTest to test the same
 * functionality, but in SSL mode.
 */
public class ServerSocketFactoryTest extends SocketFactoryTestBase {

    @Override
    public void setUp() throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
    }

    /**
     * Ensure that an object using the SSF with no port range constraints can
     * be placed in the registry and used.
     */
    @Test
    public void testBasicRegistry()
        throws RemoteException, NotBoundException {

        final ClientSocketFactory csf =
            isSSL() ? makeCSF("csf", 1000, 1000) : null;
        final ServerSocketFactory ssf = makeSSF(10, 0, 0, 0);
        basicInternal(csf, ssf);
    }

    /**
     * Ensure that an object using the port range constrained SSF can be placed
     * in the registry and used.
     */
    @Test
    public void testRangeRegistry()
        throws RemoteException, NotBoundException {

        final ClientSocketFactory csf =
            isSSL() ? makeCSF("csf", 1000, 1000) : null;
        final ServerSocketFactory ssf = makeSSF(10, 5000, 6000, 0);
        basicInternal(csf, ssf);
    }

    /**
     * Test allocation of ports by a range constrained SSF:
     *
     * 1) Allocated ports should be in range
     * 2) Fixed port allocation is supported
     * 3) IOE is thrown when no ports are available in the range
     */
    @Test
    public void testBasicRangeSSF() throws IOException {

        try {
            @SuppressWarnings("unused")
            ServerSocketFactory ssf = makeSSF(10, 6000, 5000, 0);
            fail("Expected ISE");
        } catch (IllegalArgumentException ise) {
            /* Expected. */
        }

        /* Test for ports in range. */
        int portStart = 5000;
        int portEnd = 6000;

        ServerSocketFactory ssf = makeSSF(10, portStart, portEnd, 0);

        final Set<ServerSocket> sockets = new HashSet<ServerSocket>();
        final int sampleSize = 10;
        for (int i=0; i < sampleSize; i++) {
            try {
                ServerSocket so = ssf.createServerSocket(0);
                assertTrue("port=" + so.getLocalPort() +
                           " portStart=" + portStart +
                           " portEnd=" + portEnd,
                           (so.getLocalPort() >= portStart) &&
                           (so.getLocalPort() <= portEnd));
                sockets.add(so);
            } catch (IOException e) {
                fail("Expected to find free port");
            }
        }

        assertEquals(sampleSize, sockets.size());

        int freePort = 0;
        for (ServerSocket so : sockets) {
            freePort = so.getLocalPort();
            so.close();
        }

        /* Delay briefly to allow time for the sockets to be closed */
        try {
            Thread.sleep(100);
        } catch (InterruptedException ie) {
        }

        /* Test for specific port allocation requests. */
        assertTrue (freePort > 0);
        /* Check assignment of a fixed port. */
        final ServerSocket so = ssf.createServerSocket(freePort);
        assertEquals(freePort, so.getLocalPort());

        /* Test for IOE when no more ports are available. */
        portStart = 6000;
        portEnd = 6050;

        ssf = makeSSF(10, portStart, portEnd, 0);
        sockets.clear();
        IOException ex = null;
        for (int i=0; i <= (portEnd - portStart) + 1; i++) {
            try {
                sockets.add(ssf.createServerSocket(0));
            } catch (IOException ioe) {
                /* Expected once the port range has been exhausted. */
                ex = ioe;
                break;
            }
        }
        assertNotNull(ex);
        assertTrue(sockets.size() > 0);

        for (ServerSocket soc : sockets) {
            soc.close();
        }
    }

    protected void basicInternal(final ClientSocketFactory csf,
                                 final ServerSocketFactory ssf)
        throws RemoteException,
        NoSuchObjectException,
        NotBoundException,
        AccessException {

        basicInternal(csf, ssf, 0);
    }

    protected void basicInternal(final ClientSocketFactory csf,
                                 final ServerSocketFactory ssf,
                                 final int port)
        throws RemoteException,
        NoSuchObjectException,
        NotBoundException,
        AccessException {

        bindObject(serviceName, port, csf, ssf);

        for (int i=1; i < 10; i++ ) {

            RMIObj o = (RMIObj) regStub.lookup(serviceName);
            o.test(1);

            /* Check that it was used. */
            assertEquals(1, ssf.getSocketCount());
        }
    }

    protected void bindObject(String name,
                              int port,
                              ClientSocketFactory csf,
                              ServerSocketFactory ssf)
            throws RemoteException, NoSuchObjectException {

        tearDowns.add(() -> safeUnexport(object));
        Remote stub = UnicastRemoteObject.exportObject(object, port, csf, ssf);
        try {
            regStub.rebind(name, stub);
        } catch (RemoteException re) {
            fail(re.getMessage());
        }
    }

    @Test
    public void testHashAndEquals() {
        ServerSocketFactory ssf = makeSSF(10, 5000, 7000, 0);

        /* Equal */
        ServerSocketFactory otherSsf = makeSSF(10, 5000, 7000, 0);
        assertEquals(ssf, otherSsf);
        assertEquals(ssf.hashCode(), otherSsf.hashCode());

        /* Different backlog */
        assertNotEquals(ssf, makeSSF(20, 5000, 7000, 0));

        /* Different startPort */
        assertNotEquals(ssf, makeSSF(10, 6000, 7000, 0));

        /* Different endPort */
        assertNotEquals(ssf, makeSSF(10, 5000, 8000, 0));

        /* Different acceptMaxActiveConns */
        assertNotEquals(ssf, makeSSF(10, 5000, 7000, 10));
    }
}
