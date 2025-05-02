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

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;

import oracle.kv.impl.util.PortRange;

/**
 * The server socket factory used by the SN and RNs to allocate any available
 * free port. It permits control of the server connect backlog.
 */
public class ClearServerSocketFactory extends ServerSocketFactory {

    /**
     * Create a server socket factory which yields socket connections with
     * the specified backlog.
     *
     * @param backlog the backlog associated with the server socket. A value
     * of zero means use the java default value.
     * @param startPort the start of the port range
     * @param endPort the end of the port range. Both end points are inclusive.
     * A zero start and end port is used to denote an unconstrained allocation
     * of ports as defined by the method
     * ServerSocketFactory.isUnconstrained.
     * @param acceptMaxActiveConns the maximum number of active incoming
     * connections or 0 for no limit
     */
    public ClearServerSocketFactory(int backlog,
                                    int startPort,
                                    int endPort,
                                    int acceptMaxActiveConns) {
        super(backlog, startPort, endPort, acceptMaxActiveConns);
    }

    @Override
    public ClearServerSocketFactory newInstance(int port) {
        return new ClearServerSocketFactory(
            backlog, port, port, acceptMaxActiveConns);
    }

    @Override
    public String toString() {
        return "<ClearServerSocketFactory" +
               " backlog=" + backlog +
               " portRange=" + startPort + "," + endPort +
               " acceptMaxActiveConns=" + acceptMaxActiveConns +
               (AsyncRegistryUtils.serverUseAsync ? " useAsync" : "") +
               ">";
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
        if (!super.equals(obj)) {
            return false;
        }
        return (obj instanceof ClearServerSocketFactory);
    }

    /**
     * Factory method to configure SSF appropriately.
     *
     * @return an SSF
     */
    public static ClearServerSocketFactory create(int backlog,
                                                  String portRange,
                                                  int acceptMaxActiveConns) {

        if (PortRange.isUnconstrained(portRange)) {
            return new ClearServerSocketFactory(
                backlog, 0, 0, acceptMaxActiveConns);
        }

        final List<Integer> range = PortRange.getRange(portRange);
        return new ClearServerSocketFactory(
            backlog, range.get(0), range.get(1), acceptMaxActiveConns);
    }

    @Override
    protected ServerSocket instantiateServerSocket(int port)
        throws IOException {

        return new ServerSocket(port, backlog);
    }

    /**
     * A trivial implementation that returns null, since we don't require
     * preallocation for clear sockets.
     */
    @Override
    public ServerSocket preallocateServerSocket() {
        return null;
    }

    /** Not supported. */
    @Override
    public void discardServerSocket(ServerSocket ss) {
        throw new UnsupportedOperationException(
            "discardServerSocket is not supported by this implementation");
    }

    @Override
    protected ServerSocket createAsyncServerSocket(int port)
        throws IOException {

        return new ClearAsyncServerSocket(port);
    }

    private final class ClearAsyncServerSocket extends AsyncServerSocket {

        ClearAsyncServerSocket(int port) throws IOException {
            super(port);
        }

        @Override
        public synchronized void onPrepared(ByteBuffer preReadData,
                                            Socket socket) {
            if (!addToQueue(preReadData, socket)) {
                if (connectionLogger != null) {
                    connectionLogger.info(
                        "Refused socket because accept queue is full");
                }
            }
        }

        /*
         * Suppress resource closing warnings: once the socket is added to the
         * queue, the party retrieving it from the queue is responsible for
         * closing it.
         */
        @SuppressWarnings("resource")
        private boolean addToQueue(ByteBuffer preReadData, Socket socket) {
            final Socket wrappedSocket = new ClearSocket(socket, preReadData);
            if (acceptQueue.offer(wrappedSocket)) {
                return true;
            }
            try {
                wrappedSocket.close();
            } catch (IOException e) {
            }
            return false;
        }
    }

    /**
     * A delegating socket that prepends pre-read input to its input stream.
     */
    private static final class ClearSocket extends SocketAdapter {
        private final byte[] preReadBytes;
        private PushbackInputStream in;

        ClearSocket(Socket socket, ByteBuffer preReadData) {
            super(socket);
            preReadBytes = new byte[preReadData.remaining()];
            preReadData.get(preReadBytes);
        }

        @Override
        public synchronized InputStream getInputStream()
            throws IOException {

            if (in == null) {
                in = new PushbackInputStream(super.getInputStream(),
                                             preReadBytes.length);
                in.unread(preReadBytes);
            }
            return in;
        }
    }
}
