/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util.registry;

import static org.junit.Assert.assertEquals;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import oracle.kv.TestBase;
import oracle.kv.impl.topo.util.FreePortLocator;
import oracle.kv.impl.util.TestUtils;

import org.junit.Test;

/**
 * This test exists to understand the use of CSFs in conjunction with
 * RMI registry. CSF behavior to count factory and socket usage does not apply
 * in the async case.
 */
public class RegistrySFTest extends TestBase {

    protected Registry registry;
    protected Registry regStub;
    protected final String serviceName = "registry";

    @Override
    public void setUp() throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
    }

    private ClientSocketFactory makeCSF(String svcName,
                                       int connectTMO,
                                       int readTMO) {
        return new ClearClientSocketFactory(svcName, connectTMO, readTMO,
                                            null /* clientId */);
    }

    protected ServerSocketFactory makeSSF(int backlog,
                                          int startPort,
                                          int endPort,
                                          int acceptMaxActiveConns) {
        return new ClearServerSocketFactory(backlog, startPort, endPort,
                                            acceptMaxActiveConns);
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

        final ClientSocketFactory csf = makeCSF(serviceName, 1000, 100);
        final ServerSocketFactory ssf = null;

        registry = LocateRegistry.createRegistry(regPort, csf, ssf);

        ClientSocketFactory.setSocketFactoryCount(0);

        regStub = LocateRegistry.getRegistry("localhost", regPort, null);
        
        regStub.list();

        /* csf not sent down to client. */
        assertEquals(0, ClientSocketFactory.getSocketFactoryCount());

        assertEquals(0, csf.getSocketCount());

        TestUtils.destroyRegistry(registry);
    }

    /**
     * Verify use of client side csf
     */
    @Test
    public void testClientSideCSF() throws RemoteException {

        final int regPort =
                new FreePortLocator("localHost", 5050, 5060).next();

        final ServerSocketFactory ssf = null;
        final ClientSocketFactory csf = makeCSF(serviceName, 1000, 100);

        registry = LocateRegistry.createRegistry(regPort, null, ssf);

        ClientSocketFactory.setSocketFactoryCount(0);

        regStub = LocateRegistry.getRegistry("localhost", regPort, csf);

        regStub.list();

        /* Socket supplied by csf is used. */
        assertEquals(1, csf.getSocketCount());

        TestUtils.destroyRegistry(registry);
    }

}
