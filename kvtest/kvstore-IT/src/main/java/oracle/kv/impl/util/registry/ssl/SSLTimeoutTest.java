/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.util.registry.ssl;

import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.ServerSocketFactory;
import oracle.kv.impl.util.registry.TimeoutTest;

/**
 * Test client connection timeouts.  Do this independently of KVStore itself
 * but still exercise the ClientSocketFactory in utils/registry.
 */
public class SSLTimeoutTest extends TimeoutTest {

    @Override
    public void setUp() throws Exception {

        super.setUp();
        SSLSocketFactoryTestUtils.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
    }

    @Override
    protected ClientSocketFactory makeCSF(String serviceName,
                                          int connectTMO,
                                          int otherTMO) {
        return SSLSocketFactoryTestUtils.makeCSF(
            serviceName, connectTMO, otherTMO);
    }

    @Override
    protected ServerSocketFactory makeSSF() {
        return SSLSocketFactoryTestUtils.makeSSF(
            10,     /* backlog */
            0,      /* startPort */
            0,      /* endPort */
            0);     /* acceptMaxActiveConns */
    }

    /* Override only makeCSF - let makeRegistryCSF use the clear socket */
}
