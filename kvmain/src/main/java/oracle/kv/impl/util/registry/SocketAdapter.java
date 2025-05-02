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

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.SocketChannel;

/**
 * A socket that delegates all operations to another socket.
 */
public class SocketAdapter extends Socket {

    private final Socket socket;

    public SocketAdapter(Socket socket) {
        checkNull("socket", socket);
        this.socket = socket;
    }

    @Override
    public SocketChannel getChannel() {
        return socket.getChannel();
    }

    /**
     * Overriding this method to protect against changes in the superclass.
     */
    @Override
    public void connect(SocketAddress remote) throws IOException {
        connect(remote, 0);
    }

    @Override
    public void connect(SocketAddress remote, int timeout) throws IOException {
        socket.connect(remote, timeout);
    }

    @Override
    public void bind(SocketAddress local) throws IOException {
        socket.bind(local);
    }

    @Override
    public InetAddress getInetAddress() {
        return socket.getInetAddress();
    }

    @Override
    public InetAddress getLocalAddress() {
        return socket.getLocalAddress();
    }

    @Override
    public int getPort() {
        return socket.getPort();
    }

    @Override
    public int getLocalPort() {
        return socket.getLocalPort();
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return socket.getInputStream();
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        return socket.getOutputStream();
    }

    /*
     * Suppress warnings about overriding a synchronized method with a
     * non-synchronized one. These overrides are considered incorrect by the
     * Eclipse checker for Java 17, and
     * require @SuppressWarnings("sync-override"). But the overrides are
     * permitted for Java 21, and so specifying the override there is not
     * permitted. Suppressing all warnings works for both versions. We can
     * remove the warnings suppression when we require Java 21.
     */
    @SuppressWarnings("all")
    @Override
    public void setSoTimeout(int timeout) throws SocketException {
        socket.setSoTimeout(timeout);
    }

    @SuppressWarnings("all")
    @Override
    public int getSoTimeout() throws SocketException {
        return socket.getSoTimeout();
    }

    @SuppressWarnings("all")
    @Override
    public void setSendBufferSize(int size) throws SocketException {
        socket.setSendBufferSize(size);
    }

    @SuppressWarnings("all")
    @Override
    public int getSendBufferSize() throws SocketException {
        return socket.getSendBufferSize();
    }

    @SuppressWarnings("all")
    @Override
    public void setReceiveBufferSize(int size) throws SocketException {
        socket.setReceiveBufferSize(size);
    }

    @SuppressWarnings("all")
    @Override
    public int getReceiveBufferSize() throws SocketException {
        return socket.getReceiveBufferSize();
    }

    @Override
    public void setKeepAlive(boolean on) throws SocketException {
        socket.setKeepAlive(on);
    }

    @Override
    public boolean getKeepAlive() throws SocketException {
        return socket.getKeepAlive();
    }

    @Override
    public void setTrafficClass(int tc) throws SocketException {
        socket.setTrafficClass(tc);
    }

    @Override
    public int getTrafficClass() throws SocketException {
        return socket.getTrafficClass();
    }

    @Override
    public void setReuseAddress(boolean on) throws SocketException {
        socket.setReuseAddress(on);
    }

    @Override
    public boolean getReuseAddress() throws SocketException {
        return socket.getReuseAddress();
    }

    @SuppressWarnings("all")
    @Override
    public void close() throws IOException {
        socket.close();
    }

    @Override
    public void shutdownInput() throws IOException {
        socket.shutdownInput();
    }

    @Override
    public void shutdownOutput() throws IOException {
        socket.shutdownOutput();
    }

    @Override
    public String toString() {
        return String.format("%s@%x{%s}",
                             getClass().getName(),
                             System.identityHashCode(this),
                             socket);
   }

    @Override
    public boolean isConnected() {
        return socket.isConnected();
    }

    @Override
    public boolean isBound() {
        return socket.isBound();
    }

    @Override
    public boolean isClosed() {
        return socket.isClosed();
    }

    @Override
    public boolean isInputShutdown() {
        return socket.isInputShutdown();
    }

    @Override
    public boolean isOutputShutdown() {
        return socket.isOutputShutdown();
    }
}
