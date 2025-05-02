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
import java.io.InterruptedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.rmi.server.RMIServerSocketFactory;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.async.AsyncOption;
import oracle.kv.impl.async.EndpointGroup;
import oracle.kv.impl.async.ListenerConfig;
import oracle.kv.impl.async.ListenerConfigBuilder;
import oracle.kv.impl.async.ListenerPortRange;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.SocketPrepared;
import oracle.kv.impl.util.HostPort;

/**
 * The server socket factory used by the SN and RNs to allocate any available
 * free port. It permits control of the server connect backlog.
 */
public abstract class ServerSocketFactory
    implements RMIServerSocketFactory {

    /** Set to check that only async server sockets are being created. */
    static boolean DEBUG_ASYNC_ONLY = false;

    /** A dummy socket used to mark that the accept queue is closed. */
    private static final Socket CLOSED_MARKER_SOCKET = new NullSocket();

    /* A count of the allocated sockets. */
    private final AtomicInteger socketCount = new AtomicInteger(0);

    /* The backlog associated with server sockets */
    protected final int backlog;

    /* The port range. */
    protected final int startPort;
    protected final int endPort;

    /**
     * The maximum number of active incoming socket connections, or 0 for no
     * limit. Note that this limit is only applied to async connections, since
     * there is no easy way to customize socket closing for the sockets used in
     * RMI.
     */
    protected final int acceptMaxActiveConns;

    /*
     * The port at which the range scan will resume upon the next call to
     * createStandardServerSocket.
     */
    private int currentPort;

    /* The size of the port range, used by createStandardServerSocket. */
    private final int rangeSize;

    /* A logger for connection-related logging. */
    protected Logger connectionLogger;

    /**
     * Create a server socket factory which yields socket connections with
     * the specified backlog.
     *
     * @param backlog the backlog associated with the server socket. A value
     * of zero means use the java default value.
     * @param startPort the start of the port range
     * @param endPort the end of the port range. Both end points are inclusive.
     * A zero start and end port is used to denote an unconstrained allocation
     * of ports as defined by the method {@link #isUnconstrained()}.
     * @param acceptMaxActiveConns the maximum number of active incoming
     * connections or 0 for no limit
     */
    protected ServerSocketFactory(int backlog,
                                  int startPort,
                                  int endPort,
                                  int acceptMaxActiveConns) {
        super();

        if (endPort < startPort) {
            throw new IllegalArgumentException("End port " + endPort +
                                               " must be >= startPort " +
                                               startPort);
        }
        this.backlog = backlog;
        this.startPort = startPort;
        this.endPort = endPort;
        this.currentPort = startPort;
        /* The range is inclusive. */
        rangeSize = isUnconstrained() ?
            Integer.MAX_VALUE :
            (endPort - startPort) + 1;
        if (acceptMaxActiveConns < 0) {
            throw new IllegalArgumentException(
                "acceptMaxActiveConns must not be less than 0, got: " +
                acceptMaxActiveConns);
        }
        this.acceptMaxActiveConns = acceptMaxActiveConns;
    }

    /**
     * Creates a server socket. Port selection is accomplished as follows:
     *
     * 1) If a specific non-zero port is specified, it's created using just
     * that port.
     *
     * 2) If the port range is unconstrained, that is, isUnconstrained is true
     * then any available port is used.
     *
     * 3) Otherwise, a port from within the specified port range is allocated
     * and IOException is thrown if all ports in that range are busy.
     */
    @Override
    public final ServerSocket createServerSocket(int port) throws IOException {
        if (DEBUG_ASYNC_ONLY && !AsyncRegistryUtils.serverUseAsync) {
            throw new IllegalStateException("Async not enabled");
        }
        final ServerSocket ss = !AsyncRegistryUtils.serverUseAsync ?
            createStandardServerSocket(port) :
            createAsyncServerSocket(port);
        socketCount.incrementAndGet();
        return ss;
    }

    /**
     * Returns the number of sockets that have been allocated so far.
     */
    public int getSocketCount() {
        return socketCount.get();
    }

    public int getBacklog() {
        return backlog;
    }

    public int getAcceptMaxActiveConns() {
        return acceptMaxActiveConns;
    }

    public void setConnectionLogger(Logger logger) {
        this.connectionLogger = logger;
    }

    public Logger getConnectionLogger() {
        return connectionLogger;
    }

    /**
     * Returns the listener configuration for this factory when listening with
     * the specified hostname. The hostname is only used when it is a Unix
     * domain socket hostname. Otherwise, the listener configuration uses the
     * default hostname, which will listen on all network interfaces.
     */
    public ListenerConfig getListenerConfig(String hostName) {
        final ListenerConfigBuilder builder = getListenerConfigBuilder(0);

        /*
         * Supply the host name if specifies Unix domain sockets so that we
         * listen with the right protocol and using the correct socket file
         */
        if (NetworkAddress.isUnixDomainHostname(hostName)) {
            builder.hostName(hostName);
        }
        return builder.build();
    }

    /**
     * Returns a copy of this instance that uses the specified port.
     *
     * @param port the port
     * @return a new instance using the specified port
     */
    public abstract ServerSocketFactory newInstance(int port);

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + backlog;
        result = prime * result + endPort;
        result = prime * result + startPort;
        result = prime * result + acceptMaxActiveConns;
        return result;
    }

    /**
     * Do common equality checking.  Equality requires:
     * 1) obj is non-null
     * 2) obj is an instance of a ServerSocketFactory-derived class
     * 3) the following properties are equals() against obj
     *     - backlog
     *     - startPort
     *     - endPort
     *     - acceptMaxActiveConns
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof ServerSocketFactory)) {
            return false;
        }
        final ServerSocketFactory other = (ServerSocketFactory) obj;
        if (backlog != other.backlog) {
            return false;
        }
        if (startPort != other.startPort) {
            return false;
        }
        if (endPort != other.endPort) {
            return false;
        }
        if (acceptMaxActiveConns != other.acceptMaxActiveConns) {
            return false;
        }
        return true;
    }

    /**
     * Instantiate a standard ServerSocket using the specified port.
     */
    protected abstract ServerSocket instantiateServerSocket(int port)
        throws IOException;

    /**
     * Some situations require that the server socket be created with a port
     * number that we can specify in a call to
     * UnicastRemoteObject.exportObject, but where the factory only specifies a
     * range of port numbers.  The explicit port number is needed because RMI
     * only closes unused server sockets if the socket was created with an
     * explicit (non-zero) port.  This method allows the factory to pre-create
     * a ServerSocket using whatever port number constraints it has been
     * configured with, so that we have a known port available to us.  In a
     * subsequent call to createServerSocket that specifies the port number on
     * which that server socket is listening, the implementation should consume
     * the server socket.  In the event of an error occurring during the export
     * operation, the caller must call discardServerSocket to signal that the
     * server socket is to be discarded.
     *
     * @return the server socket or null if this factory does not support
     * preallocated server sockets
     */
    public abstract ServerSocket preallocateServerSocket()
        throws IOException;

    /**
     * Signals that a server socket previously created by prepareServerSocket
     * will not be used, and should be closed and discarded.  Any IOExceptions
     * that occur during socket closing should be suppressed.  This method can
     * throw UnsupportedOperationException if the class's
     * preallocateServerSocket always returns null.
     */
    public abstract void discardServerSocket(ServerSocket ss);

    /**
     * Create an async server socket by looking for the right port.
     */
    protected abstract ServerSocket createAsyncServerSocket(int port)
        throws IOException;

    /**
     * Returns true if the port range is unconstrained.
     */
    protected boolean isUnconstrained() {
        return (startPort == 0) && (endPort == 0);
    }

    /**
     * Create a server socket by looking for the right port.
     */
    protected ServerSocket createStandardServerSocket(int port)
        throws IOException {

        if ((port != 0) || isUnconstrained()) {
            return instantiateServerSocket(port);
        }

        /* Select a port from within the free range */
        for (int portCount = rangeSize; portCount > 0; portCount--) {
            try {
                return instantiateServerSocket(currentPort);
            } catch (IOException ioe) /* CHECKSTYLE:OFF */ {
                /* Port in use or unusable, continue the scan. */
            } /* CHECKSTYLE:ON */ finally {
                if (currentPort == endPort) {
                    currentPort = startPort;
                } else {
                    currentPort++;
                }
            }
        }

        throw new IOException("No free ports in the range: " +
                              startPort + "," + endPort);
     }

    /**
     * Returns a listener configuration builder for this factory using the
     * specified port.
     */
    protected ListenerConfigBuilder getListenerConfigBuilder(int port) {
        return new ListenerConfigBuilder()
            .portRange(getPortRange(port))
            .option(AsyncOption.SSO_BACKLOG, backlog)
            .option(AsyncOption.DLG_ACCEPT_MAX_ACTIVE_CONNS,
                    (acceptMaxActiveConns > 0) ?
                    acceptMaxActiveConns :
                    Integer.MAX_VALUE);
    }

    /**
     * Close the socket, logging errors to the connection logger if possible.
     */
    protected void forceCloseSocket(Socket socket) {
        try {
            socket.close();
        } catch (IOException ioe) {
            if (connectionLogger != null) {
                connectionLogger.info("Exception closing socket: " + ioe);
            }
        }
    }

    /**
     * A queue to manage accepted sockets.
     */
    protected class AcceptQueue {

        /** A queue of accepted sockets or accept exceptions */
        private final BlockingQueue<Object> queue =
            /*
             * No way to determine the default backlog, but the JDK is
             * hardcoded to use 50.
             */
            new ArrayBlockingQueue<Object>((backlog > 0) ? backlog : 50);

        private volatile boolean closed;
        private volatile IOException closedCause;

        public AcceptQueue() { }

        /**
         * Attempt to add a socket to the queue, returning whether successful.
         */
        public boolean offer(Socket socket) {
            return queue.offer(socket);
        }

        /**
         * Attempt to add an exception the queue.  Does nothing if the queue is
         * full, since there isn't a way to push back on exceptions.
         */
        public void noteException(Throwable exception) {
            if (!queue.offer(exception)) {
                if (connectionLogger != null) {
                    connectionLogger.log(Level.FINE,
                                         "Ignored accept exception because" +
                                         " accept queue is full: {0}",
                                         exception);
                }
            }
        }

        /** Return the next accepted socket. */
        public Socket accept() throws IOException {
            try {
                Object item = null;
                if (!closed) {
                    item = queue.take();
                }
                if (closed) {

                    /*
                     * Put a marker back to make sure any other pending take
                     * calls also wake up
                     */
                    queue.offer(CLOSED_MARKER_SOCKET);

                    final IOException ioe = closedCause;
                    if (ioe != null) {
                        throw ioe;
                    }
                    throw new SocketException("Server socket is closed");
                }
                if (item instanceof Socket) {
                    return (Socket) item;
                } else if (item instanceof IOException) {
                    throw (IOException) item;
                } else if (item instanceof RuntimeException) {
                    throw (RuntimeException) item;
                } else if (item instanceof Error) {
                    throw (Error) item;
                } else if (item instanceof Throwable) {
                    throw new IllegalStateException(
                        "Unexpected exception: " + item, (Throwable) item);
                } else {
                    throw new IllegalStateException(
                        "Unexpected item in accept queue: " + item);
                }
            } catch (InterruptedException e) {
                throw new InterruptedIOException("Accept was interrupted");
            }
        }

        /** Close the queue, adding a marker to notify waiters. */
        public void close(Throwable cause) throws IOException {
            synchronized (this) {
                if (!closed) {
                    closed = true;
                    closedCause = (cause == null) ? null :
                        (cause instanceof IOException) ? (IOException) cause :
                        new IOException("Server socket closed: " + cause,
                                        cause);

                    /*
                     * Try adding the closed marker to the queue. It's OK if
                     * the queue is full, since threads waiting in take calls
                     * will get those entries and notice that the socket is
                     * closed. The problem is the empty queue case, and there
                     * will be space then.
                     */
                    queue.offer(CLOSED_MARKER_SOCKET);
                } else if (closedCause != null) {
                    throw closedCause;
                } else {
                    return;
                }
            }

            /*
             * Drain the queue in case there wouldn't be space to add the
             * marker.  Since we are closing, we shouldn't have to do this for
             * very long.
             */
            while (true) {
                final Object item = queue.poll();
                if (item == null) {
                    break;
                }
                if (item == CLOSED_MARKER_SOCKET) {
                    /*
                     * Put the marker back to wake up any waiting calls to take
                     */
                    queue.add(item);
                    break;
                }
            }
        }
    }

    /**
     * Implement ServerSocket by cooperating with the EndpointGroup.
     */
    protected abstract class AsyncServerSocket extends NullServerSocket
            implements SocketPrepared {
        private final EndpointGroup.ListenHandle listenHandle;
        private final HostPort localAddress;
        protected final AcceptQueue acceptQueue = new AcceptQueue();
        protected final Object closeLock = new Object();
        protected boolean closed;

        protected AsyncServerSocket(int port) throws IOException {
            final EndpointGroup endpointGroup =
                AsyncRegistryUtils.getEndpointGroup();
            final ListenerConfigBuilder configBuilder =
                getListenerConfigBuilder(port);
            EndpointGroup.ListenHandle lh;
            try {
                lh = endpointGroup.listen(configBuilder.build(), this);
            } catch (IllegalStateException e) {

                /*
                 * If the request was for an anonymous port, then RMI must be
                 * asking for a new server socket even though there is an
                 * existing, equivalent, one.  Use a listener config with an
                 * unsharable port range so it can have what it wants.
                 */
                if (port != 0) {
                    throw e;
                }
                configBuilder.portRange(uniqueAnonymousPortRange());
                lh = endpointGroup.listen(configBuilder.build(), this);
            }
            listenHandle = lh;
            localAddress = listenHandle.getLocalAddress();
        }

        /* Override ServerSocket methods */

        @Override
        public Socket accept() throws IOException {
            return acceptQueue.accept();
        }

        @Override
        public int getLocalPort() {
            return localAddress.port();
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[" + localAddress + "]";
        }

        @Override
        public void close() throws IOException {

            /*
             * We're depending on the close method being called whenever the
             * server socket becomes closed so that these additional cleanups
             * will be performed.  That appears to be the case, although I
             * don't see that the Java specification requires it.
             */

            close(null);
        }

        private void close(Throwable exception) throws IOException {
            synchronized (closeLock) {
                if (!closed) {
                    listenHandle.shutdown(false);
                    acceptQueue.close(exception);
                }
                closed = true;
            }
        }

        @Override
        public boolean isClosed() {
            synchronized (closeLock) {
                return closed;
            }
        }

        /* Implement SocketPrepared */

        @Override
        public void onChannelError(ListenerConfig config,
                                   Throwable exception,
                                   boolean channelClosed) {
            if (channelClosed) {
                try {
                    close(exception);
                } catch (IOException e) {
                }
            } else {
                acceptQueue.noteException(exception);
            }
        }
    }

    /**
     * Returns a listener port range for the specified port.
     *
     * @param port the port, which can be 0
     */
    private ListenerPortRange getPortRange(int port) {
        if (port != 0) {
            return new ListenerPortRange(port, port);
        }
        if ((startPort == 0) && (endPort == 0)) {
            return new ListenerPortRange();
        }
        return new ListenerPortRange(startPort, endPort);
    }

    /**
     * Creates a listener port range for the anonymous port that is unique from
     * other such instances, to support tests where an anonymous server is
     * closed and then another one is used.
     */
    private ListenerPortRange uniqueAnonymousPortRange() {
        if ((startPort == 0) && (endPort == 0)) {
            return new UniqueAnonymousPortRange();
        }
        return new UniqueAnonymousPortRange(startPort, endPort);
    }

    private static class UniqueAnonymousPortRange extends ListenerPortRange {
        UniqueAnonymousPortRange() { }
        UniqueAnonymousPortRange(int startPort, int endPort) {
            super(startPort, endPort);
        }
        @Override
        public boolean equals(Object o) {
            return o == this;
        }
        @Override
        public int hashCode() {
            return System.identityHashCode(this);
        }
        @Override
        public String toString() {
            return super.toString() + ";unique=" + hashCode();
        }
    }
}
