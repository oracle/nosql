/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util.registry.ssl;

import java.security.NoSuchAlgorithmException;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import oracle.kv.impl.security.ssl.SSLControl;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.util.SSLTestUtils;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.ServerSocketFactory;
import oracle.kv.impl.util.registry.SocketFactoryTestBase.SocketFactoryPair;

import com.sleepycat.je.rep.net.SSLAuthenticator;

/**
 * Utilities for SSL CSF and SSF unit tests.
 */
final class SSLSocketFactoryTestUtils {

    private static SSLParameters defaultSSLParameters;

    /**
     * Private constructor to make checkstyle happy.
     */
    private SSLSocketFactoryTestUtils() {
    }

    static void setUp() {
        SSLTestUtils.setSSLProperties();
    }

    static ClientSocketFactory makeCSF(String serviceName,
                                       int connectTMO,
                                       int readTMO) {
        return new SSLClientSocketFactory(serviceName, connectTMO, readTMO);
    }

    static ServerSocketFactory makeSSF(int backlog,
                                       int startPort,
                                       int endPort,
                                       int acceptMaxActiveConns) {
        return
            new SSLServerSocketFactory(
                new SSLControl(getDefaultSSLParameters(),
                               getDefaultSSLContext(), null, null),
                backlog, startPort, endPort, acceptMaxActiveConns);
    }

    static ServerSocketFactory makeSSF(
        int backlog,
        int startPort,
        int endPort,
        int acceptMaxActiveConns,
        @SuppressWarnings("unused") HostnameVerifier hostVerifier,
        SSLAuthenticator peerAuth) {

        return
            new SSLServerSocketFactory(
                new SSLControl(getDefaultSSLParameters(),
                               getDefaultSSLContext(), null, peerAuth),
                backlog, startPort, endPort, acceptMaxActiveConns);
    }

    static SSLContext getDefaultSSLContext() {
        try {
            return SSLContext.getDefault();
        } catch (NoSuchAlgorithmException nsae) {
            nsae.printStackTrace(System.out);
        }
        return null;
    }

    static synchronized SSLParameters getDefaultSSLParameters() {
        if (defaultSSLParameters == null) {
            final SSLContext defaultSSLContext = getDefaultSSLContext();
            if (defaultSSLContext != null) {
                defaultSSLParameters =
                    defaultSSLContext.getDefaultSSLParameters();
                /* Apply default SSL protocols */
                defaultSSLParameters.setProtocols(
                    SecurityUtils.PREFERRED_PROTOCOLS_DEFAULT.split(","));
            }
        }
        return defaultSSLParameters;
    }

    static SocketFactoryPair makeRegistrySFP() throws Exception {
        return new SocketFactoryPair(
            new SSLServerSocketFactory(
                new SSLControl(getDefaultSSLParameters(),
                               getDefaultSSLContext(), null, null),
                0, 0, 0, 0),
            new SSLClientSocketFactory("reg", 1000, 1000));
    }
}
