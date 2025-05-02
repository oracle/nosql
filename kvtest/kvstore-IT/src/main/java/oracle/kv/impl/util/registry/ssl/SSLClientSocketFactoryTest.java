/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util.registry.ssl;

import javax.net.ssl.SSLException;

import oracle.kv.impl.util.SSLTestUtils;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.ClientSocketFactoryTest;
import oracle.kv.impl.util.registry.ServerSocketFactory;

/**
 * Tests the SSL client socket factory used for RMI requests.
 */
public class SSLClientSocketFactoryTest extends ClientSocketFactoryTest {

    @Override
    public void setUp() throws Exception {

        SSLTestUtils.setRMIRegistryFilter();
        super.setUp();
        SSLSocketFactoryTestUtils.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        SSLTestUtils.clearRMIRegistryFilter();
        super.tearDown();
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
     * Over SSL, a timeout can also produce various SSLExceptions where the
     * cause is a timeout exception.
     */
    @Override
    protected boolean getIsSocketTimeoutException(Throwable exception) {
        if (super.getIsSocketTimeoutException(exception)) {
            return true;
        }
        return (exception instanceof SSLException) &&
            super.getIsSocketTimeoutException(exception.getCause());
    }
}
