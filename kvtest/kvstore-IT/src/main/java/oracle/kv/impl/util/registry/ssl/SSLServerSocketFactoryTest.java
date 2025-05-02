/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util.registry.ssl;

import java.io.IOException;
import java.rmi.NotBoundException;

import javax.net.ssl.SSLSession;

import oracle.kv.impl.util.SSLTestUtils;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.ServerSocketFactory;
import oracle.kv.impl.util.registry.ServerSocketFactoryTest;

import com.sleepycat.je.rep.net.SSLAuthenticator;

import org.junit.Test;

/**
 * Test the SSL SSF used for RMI requests.
 */
public class SSLServerSocketFactoryTest extends ServerSocketFactoryTest {

    @Override
    public void setUp() throws Exception {

        SSLTestUtils.setRMIRegistryFilter();
        super.setUp();
        SSLSocketFactoryTestUtils.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        SSLTestUtils.setRMIRegistryFilter();
        super.tearDown();
    }

    @Override
    protected boolean isSSL() {
        return true;
    }

    @Override
    protected ClientSocketFactory makeCSF(String svcName,
                                          int connectTMO,
                                          int readTMO) {
        return SSLSocketFactoryTestUtils.makeCSF(
            svcName, connectTMO, readTMO);
    }

    @Override
    protected ServerSocketFactory makeSSF(int backlog,
                                          int startPort,
                                          int endPort,
                                          int acceptMaxActiveConns,
                                          boolean optional) {
        return SSLSocketFactoryTestUtils.makeSSF(backlog, startPort, endPort,
                                                 acceptMaxActiveConns);
    }

    /**
     * Provide more vigorous exercise of the authentication capability.
     */
    @Test
    public void testAuthSS() throws NotBoundException, IOException {
        final ClientSocketFactory csf = makeCSF("csf", 1000, 1000);
        final ServerSocketFactory ssf =
            SSLSocketFactoryTestUtils.makeSSF(
                10, 5700, 5710, 0, null, new AllowAll());
        basicInternal(csf, ssf, 0);
    }

    private class AllowAll implements SSLAuthenticator {
        @Override
        public boolean isTrusted(SSLSession sslSession) {
            return true;
        }
    }
}
