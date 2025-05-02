/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv.impl.util.registry.ssl;

import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.security.ssl.SSLControl;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.ServerSocketFactory;
import oracle.kv.impl.util.registry.RMISocketPolicy;

/**
 * An RMISocketPolicy implementation that is responsible for producing socket
 * factories for RMI.  This class only supports the client side.  To support
 * compatibility, though, it maintains the generic name, but no longer refers
 * to the server socket factory implementation.
 */
public class SSLSocketPolicy implements RMISocketPolicy {

    final SSLControl serverSSLControl;
    final SSLControl clientSSLControl;

    /**
     * Create an SSLSocketPolicy.
     */
    public SSLSocketPolicy(SSLControl serverSSLControl,
                           SSLControl clientSSLControl) {
        this.serverSSLControl = serverSSLControl;
        this.clientSSLControl = clientSSLControl;
    }

    /**
     * Prepare for use as standard client policy.
     */
    @Override
    public void prepareClient(String storeContext, ClientId clientId) {
        /*
         * The client socket factory picks up configuration from its environment
         * because it may be serialized and sent to another process.  We use the
         * client factories locally as well, so set the default context to
         * match our requirements.
         */
        if (isTrusted()) {
            SSLClientSocketFactory.setTrustedControl(clientSSLControl);
        } else {
            SSLClientSocketFactory.setUserControl(clientSSLControl,
                                                  storeContext,
                                                  clientId);
        }
    }

    @Override
    public void clearPreparedClient(ClientId clientId) {
        if (!isTrusted()) {
            SSLClientSocketFactory.clearUserSSLControlMap(clientId);
        }
    }

    /**
     * This class does not support this method.
     */
    @Override
    public ServerSocketFactory getRegistrySSF(SocketFactoryArgs args) {
        throw new UnsupportedOperationException("Not supported on the client");
    }

    /**
     * Return a Client socket factory for appropriate for registry
     * access by the client.
     */
    @Override
    public ClientSocketFactory getRegistryCSF(SocketFactoryArgs args) {
        return new SSLClientSocketFactory(args.getCsfName(),
                                          args.getCsfConnectTimeout(),
                                          args.getCsfReadTimeout(),
                                          args.getKvStoreName(),
                                          args.getClientId());
    }

    /**
     * This class does not support this method.
     */
    @Override
    public SocketFactoryPair getBindPair(SocketFactoryArgs args) {
        throw new UnsupportedOperationException("Not supported on the client");
    }

    /**
     * Reports whether the policy allows a server to be able to "trust" an
     * incoming client connection.
     */
    @Override
    public boolean isTrustCapable() {
        return isTrusted();
    }

    /**
     * An SSL socket policy is a trusted policy if it includes a client
     * authenticator in the server side of the configuration.
     */
    boolean isTrusted() {
        return serverSSLControl != null &&
            serverSSLControl.peerAuthenticator() != null;
    }

}

