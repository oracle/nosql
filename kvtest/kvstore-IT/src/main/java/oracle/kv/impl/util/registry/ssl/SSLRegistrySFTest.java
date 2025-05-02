/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util.registry.ssl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;

import oracle.kv.TestBase;
import oracle.kv.impl.security.ssl.SSLConfig;
import oracle.kv.impl.security.ssl.SSLControl;
import oracle.kv.impl.topo.util.FreePortLocator;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.ServerSocketFactory;

import org.junit.Test;

/**
 * This test exists to understand the use of CSFs in conjunction with
 * SSL RMI registries.
 */
public class SSLRegistrySFTest extends TestBase {

    protected Registry registry;
    protected Registry regStub;
    protected final String serviceName = "registry";

    @Override
    public void setUp() throws Exception {

        super.setUp();
        SSLSocketFactoryTestUtils.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
        if (registry != null) {
            TestUtils.destroyRegistry(registry);
        }
    }

    private ClientSocketFactory makeCSF(String svcName,
                                       int connectTMO,
                                       int readTMO) {
        return new SSLClientSocketFactory(svcName, connectTMO, readTMO);
    }

    protected ServerSocketFactory makeSSF(int backlog,
                                          int startPort,
                                          int endPort,
                                          int acceptMaxActiveConns) {
        return new SSLServerSocketFactory(
            new SSLControl(SSLSocketFactoryTestUtils.getDefaultSSLParameters(),
                           getDefaultSSLContext(), null, null),
            backlog, startPort, endPort, acceptMaxActiveConns);
    }

    protected static SSLContext getDefaultSSLContext() {
        try {
            return SSLContext.getDefault();
        } catch (NoSuchAlgorithmException nsae) {
            nsae.printStackTrace(System.out);
        }
        return null;
    }

    /**
     * Verifies that using csf during registry creation does not result in it
     * being sent down to the client. This is unlike the csf used with an
     * exported object.
     */
    @Test
    public void testServerSideCSF() throws RemoteException {

        final int regPort =
                new FreePortLocator("localHost", 5050, 5060).next();

        final ClientSocketFactory csf =
            makeCSF(serviceName, 1000, 100);
        /* With an SSL client we must have an SSL server */
        final ServerSocketFactory ssf = makeSSF(10, 0, 0, 0);

        registry = LocateRegistry.createRegistry(regPort, csf, ssf);

        ClientSocketFactory.setSocketFactoryCount(0);

        regStub = LocateRegistry.getRegistry("localhost", regPort, csf);

        regStub.list();

        /* csf not sent down to client. */
        assertEquals(0, ClientSocketFactory.getSocketFactoryCount());

        /* We had to use a SF socket for SSL */
        assertEquals(1, csf.getSocketCount());
    }

    /**
     * Verify use of client side csf.
     */
    @Test
    public void testClientSideCSF() throws Exception {

        final int regPort =
                new FreePortLocator("localHost", 5050, 5060).next();

        // With an SSL client we must have an SSL server
        final ServerSocketFactory ssf = makeSSF(10, 0, 0, 0);
        final ClientSocketFactory csf = makeCSF(serviceName, 1000, 100);

        registry = LocateRegistry.createRegistry(regPort, null, ssf);

        ClientSocketFactory.setSocketFactoryCount(0);

        regStub = LocateRegistry.getRegistry("localhost", regPort, csf);

        regStub.list();

        /* Socket supplied by csf is used. */
        assertEquals(1, csf.getSocketCount());

        final Properties props = new Properties();
        final SSLConfig config = new SSLConfig(props);
        final SSLControl control = config.makeSSLControl(false, logger);
        SSLClientSocketFactory.setUserControl(control, null, null);
        SSLSocket socket =
            (SSLSocket) csf.createSocket("localhost", regPort);
        socket.startHandshake();
        /* Make sure GCM related cipher is selected first */
        assertTrue(socket.getSession().getCipherSuite().contains("GCM"));
    }
}
