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

import static oracle.kv.impl.util.registry.ssl.SSLClientSocketFactory.Use;

import oracle.kv.impl.security.ssl.SSLControl;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.ServerSocketFactory;

/**
 * An RMISocketPolicy implementation that is responsible for producing socket
 * factories for RMI.  This class supports both client and server side
 * operations.
 */
public class SSLServerSocketPolicy extends SSLSocketPolicy {

    /**
     * Creates an instance of this class.
     */
    public SSLServerSocketPolicy(SSLControl serverSSLControl,
                                 SSLControl clientSSLControl) {
        super(serverSSLControl, clientSSLControl);
    }

    /**
     * Create a SocketFactoryPair appropriate for creation of an RMI registry.
     */
    @Override
    public ServerSocketFactory getRegistrySSF(SocketFactoryArgs args) {
        return
            SSLServerSocketFactory.create(serverSSLControl,
                                          args.getSsfBacklog(),
                                          args.getSsfPortRange(),
                                          args.getSsfAcceptMaxActiveConns());
    }

    /**
     * Create a SocketFactoryPair appropriate for exporting an object over RMI.
     */
    @Override
    public SocketFactoryPair getBindPair(SocketFactoryArgs args) {
        final ServerSocketFactory ssf =
            SSLServerSocketFactory.create(serverSSLControl,
                                          args.getSsfBacklog(),
                                          args.getSsfPortRange(),
                                          args.getSsfAcceptMaxActiveConns());

        final ClientSocketFactory csf = isTrusted() ?
            new SSLClientSocketFactory(args.getCsfName(),
                                       args.getCsfConnectTimeout(),
                                       args.getCsfReadTimeout(),
                                       Use.TRUSTED) :
            new SSLClientSocketFactory(args.getCsfName(),
                                       args.getCsfConnectTimeout(),
                                       args.getCsfReadTimeout(),
                                       args.getKvStoreName(),
                                       args.getClientId());
        return new SocketFactoryPair(ssf, csf);
    }
}

