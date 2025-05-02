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

package oracle.kv.impl.util.registry;

import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.mgmt.jmx.JmxConnectorUtil;
import oracle.kv.impl.util.PortRange;

/**
 * Provides an implementation of RMISocketPolicy that transmits information
 * "in the clear", with no encryption.
 */
public class ClearSocketPolicy implements RMISocketPolicy {

    public ClearSocketPolicy() {
    }

    /**
     * Prepare for use as standard client policy.
     */
    @Override
    public void prepareClient(String storeContext, ClientId clientId) {
        /* No action needed */
    }

    @Override
    public void clearPreparedClient(ClientId clientId) {
        /* No action needed */
    }

    @Override
    public ServerSocketFactory getRegistrySSF(SocketFactoryArgs args) {
        return (args.getSsfName() == null) ?
            /* Provide a default server socket factory if none is requested */
            ClearServerSocketFactory.create(0, PortRange.UNCONSTRAINED, 0) :
            ClearServerSocketFactory.create(args.getSsfBacklog(),
                                            args.getSsfPortRange(),
                                            args.getSsfAcceptMaxActiveConns());
    }

    /*
     * Return a Client socket factory appropriate for registry access by the
     * client.
     */
    @Override
    public ClientSocketFactory getRegistryCSF(SocketFactoryArgs args) {
        /*
         * Until we get to the next upgrade release boundary beyond 3.0,
         * return ClientSocketFactory.
         */
        return new ClearClientSocketFactory(args.getCsfName(),
                                            args.getCsfConnectTimeout(),
                                            args.getCsfReadTimeout(),
                                            args.getClientId());
    }

    /**
     * Standard RMI export socket factories.
     */
    @Override
    public SocketFactoryPair getBindPair(SocketFactoryArgs args) {
        /*
         * When exporting for JMX access, don't supply a CSF. JMX clients
         * probably won't have our client library available, so they'll need to
         * use the Java-provided CSF class.
         */
        if (JmxConnectorUtil.JMX_SSF_NAME.equals(args.getSsfName())) {
            return null;
        }

        String portRange = args.getSsfPortRange();
        if (portRange == null) {
            portRange = PortRange.UNCONSTRAINED;
        }
        final ServerSocketFactory ssf =
            ClearServerSocketFactory.create(args.getSsfBacklog(),
                                            portRange,
                                            args.getSsfAcceptMaxActiveConns());

        final ClientSocketFactory csf =
            new ClientSocketFactory(args.getCsfName(),
                                    args.getCsfConnectTimeout(),
                                    args.getCsfReadTimeout(),
                                    args.getClientId());
        return new SocketFactoryPair(ssf, csf);
    }

    /**
     * Reports whether the policy allows a server to be able to "trust" an
     * incoming client connection.
     */
    @Override
    public boolean isTrustCapable() {
        return false;
    }
}
