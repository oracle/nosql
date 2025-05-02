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
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.SocketChannel;

/**
 * A socket for which each method throws {@link
 * UnsupportedOperationException}.
 */
public class NullSocket extends Socket {

    /** Creates an instance of this class. */
    public NullSocket() { }

    @Override
    public SocketChannel getChannel() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void connect(SocketAddress remote) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void connect(SocketAddress remote, int timeout) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void bind(SocketAddress local) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InetAddress getInetAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public InetAddress getLocalAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getPort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getLocalPort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getInputStream() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void setSoTimeout(int timeout) throws SocketException {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized int getSoTimeout() throws SocketException {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void setSendBufferSize(int size) throws SocketException {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized int getSendBufferSize() throws SocketException {
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
    public void setKeepAlive(boolean on) throws SocketException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getKeepAlive() throws SocketException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTrafficClass(int tc) throws SocketException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getTrafficClass() throws SocketException {
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
    public synchronized void close() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutdownInput() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutdownOutput() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isConnected() {
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
    public boolean isInputShutdown() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isOutputShutdown() {
        throw new UnsupportedOperationException();
    }
}
