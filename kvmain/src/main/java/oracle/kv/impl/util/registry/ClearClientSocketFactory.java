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

import java.io.DataInput;
import java.io.IOException;
import java.net.Socket;

import oracle.kv.KVStoreConfig;
import oracle.kv.impl.api.ClientId;

/**
 * An implementation of RMIClientSocketFactory that permits configuration of
 * the following Socket timeouts:
 * <ol>
 * <li>Connection timeout</li>
 * <li>Read timeout</li>
 * </ol>
 * These are set to allow clients to become aware of possible network problems
 * in a timely manner.
 * <p>
 * CSFs with the appropriate timeouts for a registry are specified on the
 * client side.
 * <p>
 * CSFs for service requests (unrelated to the registry) have default values
 * provided by the server that can be overridden by the client as below:
 * <ol>
 * <li>Server side timeout parameters are set via the KVS admin as policy
 * parameters</li>
 * <li>Client side timeout parameters are set via {@link KVStoreConfig}. When
 * present, they override the parameters set at the server level.</li>
 * </ol>
 * <p>
 * Currently, read timeouts are implemented using a timer thread and the
 * TimeoutTask, which periodically checks open sockets and interrupts any that
 * are inactive and have exceeded their timeout period. We replaced the more
 * obvious approach of using the Socket.setSoTimeout() method with this manual
 * mechanism, because the socket implementation uses a poll system call to
 * enforce the timeout, which was too cpu intensive.
 * <p>
 * TODO: RMI does not make any provisions for request granularity timeouts, but
 * now that we have implemented our own timeout mechanism, request granularity
 * timeouts could be supported. If request timeouts are implemented, perhaps
 * that should encompass and replace connection and request timeouts.
 * <p>
 * TODO: Note that this class is not fully used in R3.  It is intended to be
 * the functional equivalent of the R3.0 ClientSocketFactory (which is a
 * superclass of this class). In order to maintain cross-version compatibility,
 * this class can not be used in R3.0 server code because we can't be sure that
 * all accessing clients have this class in their code base.  Once we reach the
 * next significant version boundary we can assume that accessing clients
 * have this class in their path and we can start using it.
 *
 * @since 3.0
 * @see #writeFastExternal FastExternalizable format
 */
public class ClearClientSocketFactory extends ClientSocketFactory {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the client socket factory.
     *
     * @param name the factory name
     *
     * @param connectTimeoutMs the connect timeout. A zero value denotes an
     *                          infinite timeout
     * @param readTimeoutMs the read timeout associated with the connection.
     *                       A zero value denotes an infinite timeout
     * @param clientId the client ID or null in a server context
     */
    public ClearClientSocketFactory(String name,
                                    int connectTimeoutMs,
                                    int readTimeoutMs,
                                    ClientId clientId) {
        super(name, connectTimeoutMs, readTimeoutMs, clientId);
    }

    /**
     * Create a client socket factory and specify the CSF ID, for testing.
     */
    public ClearClientSocketFactory(String name,
                                    int connectTimeoutMs,
                                    int readTimeoutMs,
                                    ClientId clientId,
                                    long csfId) {
        super(name, connectTimeoutMs, readTimeoutMs, clientId, csfId);
    }

    /**
     * Creates an instance using data from an input stream.
     */
    public ClearClientSocketFactory(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {

        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        return super.equals(obj);
    }

    /**
     * @see java.rmi.server.RMIClientSocketFactory#createSocket
     */
    @Override
    public Socket createSocket(String host, int port)
        throws java.net.UnknownHostException, IOException {

        return super.createTimeoutSocket(host, port);
    }
}
