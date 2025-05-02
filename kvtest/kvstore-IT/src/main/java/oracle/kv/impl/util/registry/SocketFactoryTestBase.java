/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util.registry;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import oracle.kv.TestBase;
import oracle.kv.impl.topo.util.FreePortLocator;

/**
 * base class for CSF and SSF unit tests
 */
public class SocketFactoryTestBase extends TestBase {

    protected Registry registry;
    protected Registry regStub;

    protected final String serviceName = "foo";
    protected final RMIObj object = new RMIObjImpl();

    public static class SocketFactoryPair {
        private final ServerSocketFactory ssf;
        private final ClientSocketFactory csf;

        public SocketFactoryPair(ServerSocketFactory ssf,
                          ClientSocketFactory csf) {
            this.ssf = ssf;
            this.csf = csf;
        }

        public ServerSocketFactory getSSF() {
            return ssf;
        }

        public ClientSocketFactory getCSF() {
            return csf;
        }
    }

    /* overridden in SSL implementations */
    protected ClientSocketFactory makeCSF(String svcName,
                                          int connectTMO,
                                          int readTMO) {
        return new ClearClientSocketFactory(svcName, connectTMO, readTMO,
                                            null /* clientId */);
    }

    protected ServerSocketFactory makeSSF(int backlog,
                                          int startPort,
                                          int endPort,
                                          int acceptMaxActiveConns) {
        return makeSSF(backlog, startPort, endPort, acceptMaxActiveConns,
                       false /* not optional */);
    }

    /* overridden in SSL implementations */
    protected ServerSocketFactory makeSSF(int backlog,
                                          int startPort,
                                          int endPort,
                                          int acceptMaxActiveConns,
                                          boolean optional) {
        return (optional ? null :
                new ClearServerSocketFactory(backlog, startPort, endPort,
                                             acceptMaxActiveConns));
    }

    /* overridden in SSL implementations */
    protected boolean isSSL() {
        return false;
    }

    @Override
    public void setUp() throws Exception {

        final int regPort =
            new FreePortLocator("localHost", 5050, 5060).next();

        SocketFactoryPair sfp = makeRegistrySFP();
        registry = LocateRegistry.createRegistry(regPort, sfp.getCSF(),
                                                 sfp.getSSF());
        regStub = LocateRegistry.getRegistry("localhost", regPort,
                                             sfp.getCSF());
        super.setUp();
    }

    /* Overridden in SSLSocketFactoryTestBase */
    protected SocketFactoryPair makeRegistrySFP() throws Exception {
        return new SocketFactoryPair(null, null);
    }

    @Override
    public void tearDown() throws Exception {

        UnicastRemoteObject.unexportObject(registry, true);
        ClientSocketFactory.clearConfiguration();
        super.tearDown();
    }
    public interface RMIObj extends Remote {
        public boolean test(int sleepMs) throws RemoteException;
        public boolean test(int sleepMs, byte[] data) throws RemoteException;
    }

    static class RMIObjImpl  implements RMIObj {
        @Override
        public boolean test(int sleepMs) {
            try {
                Thread.sleep(sleepMs);
                return true;
            } catch (InterruptedException e) {
                return false;
            }
        }
        @Override
        public boolean test(int sleepMs, byte[] data) {
            return test(sleepMs);
        }
    }

}
