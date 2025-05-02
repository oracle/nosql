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
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.ServerSocketChannel;

/**
 * A server socket for which each method throws {@link
 * UnsupportedOperationException}.  Use this class to create a server socket
 * that is only used to supply sockets accepted through some other mechanism.
 */
public class NullServerSocket extends ServerSocket {

    public NullServerSocket() throws IOException { }

    @Override
    public void bind(SocketAddress endpoint) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void bind(SocketAddress endpoint, int backlog) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InetAddress getInetAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getLocalPort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SocketAddress getLocalSocketAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Socket accept() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ServerSocketChannel getChannel() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isBound() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed() {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void setSoTimeout(int timeout) throws SocketException {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized int getSoTimeout() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setReuseAddress(boolean on) throws SocketException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getReuseAddress() throws SocketException {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void setReceiveBufferSize(int size)
        throws SocketException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized int getReceiveBufferSize() throws SocketException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setPerformancePreferences(int connectionTime,
                                          int latency,
                                          int bandwidth) {
        throw new UnsupportedOperationException();
    }
}
