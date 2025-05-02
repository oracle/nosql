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

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import javax.net.ssl.SSLSocket;

import oracle.kv.impl.async.ListenerConfigBuilder;
import oracle.kv.impl.security.ssl.SSLControl;
import oracle.kv.impl.util.PortRange;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.ServerSocketFactory;

/**
 * This implementation of ServerSocketFactory provides SSL protocol support.
 */
public class SSLServerSocketFactory extends ServerSocketFactory {

    /* Limit on number of handshake threads to allow */
    private static final int HANDSHAKE_THREAD_MAX = 10;

    /* Limit on number of entries in handshake queue to allow */
    private static final int HANDSHAKE_QUEUE_MAX = 10;

    /* The handshake executor thread keepalive time */
    private static final long KEEPALIVE_MS = 10000L;

    /* Handshake thread counter, for numbering threads */
    private static final AtomicInteger hsThreadCounter = new AtomicInteger(0);

    /* SSLControl used to set policy for SSL */
    private final SSLControl sslControl;

    /*
     * Map of pending preallocated sockets.  Synchronize on this instance when
     * accessing this field.
     */
    private final Map<Integer, ServerSocket> pendingSocketMap;

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
     * {@link oracle.kv.impl.util.registry.ServerSocketFactory#isUnconstrained}.
     * @param acceptMaxActiveConns the maximum number of active incoming
     * connections or 0 for no limit
     */
    public SSLServerSocketFactory(SSLControl sslControl,
                                  int backlog,
                                  int startPort,
                                  int endPort,
                                  int acceptMaxActiveConns) {
        super(backlog, startPort, endPort, acceptMaxActiveConns);
        this.sslControl = checkNull("sslControl", sslControl);
        pendingSocketMap = new HashMap<Integer, ServerSocket>();
    }

    @Override
    public SSLServerSocketFactory newInstance(int port) {
        return new SSLServerSocketFactory(sslControl, backlog, port, port,
                                          acceptMaxActiveConns);
    }

    @Override
    public String toString() {
        return "<SSLServerSocketFactory" +
               " backlog=" + backlog +
               " portRange=" + startPort + "," + endPort +
               " sslControl=" + sslControl +
               " acceptMaxActiveConns=" + acceptMaxActiveConns +
               (AsyncRegistryUtils.serverUseAsync ? " useAsync" : "") +
               ">";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + sslControl.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (!(obj instanceof SSLServerSocketFactory)) {
            return false;
        }
        final SSLServerSocketFactory other = (SSLServerSocketFactory) obj;
        if (!sslControl.equals(other.sslControl)) {
            return false;
        }
        return true;
    }

    /**
     * Factory method to configure SSF appropriately.
     *
     * @return an SSF
     */
    public static SSLServerSocketFactory create(SSLControl sslControl,
                                                int backlog,
                                                String portRange,
                                                int acceptMaxActiveConns) {

        if (portRange == null || PortRange.isUnconstrained(portRange)) {
            return new SSLServerSocketFactory(sslControl, backlog, 0, 0,
                                              acceptMaxActiveConns);
        }

        final List<Integer> range = PortRange.getRange(portRange);
        return new SSLServerSocketFactory(
            sslControl, backlog, range.get(0), range.get(1),
            acceptMaxActiveConns);
    }

    @Override
    protected ServerSocket instantiateServerSocket(int port)
        throws IOException {

        if (port != 0) {
            final ServerSocket ss = retrievePendingSocket(port);
            if (ss != null) {
                return ss;
            }
        }
        if (sslControl.peerAuthenticator() == null) {
            return new SSLNoPeerAuthServerSocket(port, backlog);
        }
        return new SSLPeerAuthServerSocket(port, backlog);
    }

    /** Attempt to locate a preallocated server socket. */
    private synchronized ServerSocket retrievePendingSocket(int port) {
        return pendingSocketMap.remove(port);
    }

    /**
     * Preallocates a server socket for eventual use by createServerSocket() in
     * cases where the caller needs to know the port first.
     */
    @Override
    public synchronized ServerSocket preallocateServerSocket()
        throws IOException {

        /*
         * We only appear to need preallocated sockets in cases where peer
         * authentication is being used.  The reason for that restriction seems
         * to be that we need to reexport the trusted login service on the same
         * port when switching from an unregistered to a registered admin, and
         * so we need to export on an explicit port to work around RMI's
         * limitation of only closing unused server sockets if they have
         * explicit ports.  There don't seem to be other cases where closing
         * server sockets is required, but mostly this restriction maintains
         * behavior introduced earlier when SSL support was originally added.
         */
        if (sslControl.peerAuthenticator() == null) {
            return null;
        }

        final ServerSocket ss = createServerSocket(0);
        pendingSocketMap.put(ss.getLocalPort(), ss);
        return ss;
    }

    /**
     * Discards a server socket created by prepareServerSocket, but which
     * will not be used.
     */
    @Override
    public synchronized void discardServerSocket(ServerSocket ss) {
        /*
         * Don't rely on being able to look up the server socket by its
         * listen port, just in case it somehow got closed already.
         */
        for (Iterator<ServerSocket> i = pendingSocketMap.values().iterator();
             i.hasNext(); ) {
            if (i.next() == ss) {
                i.remove();
                break;
            }
        }
        try {
            ss.close();
        } catch (IOException ioe) /* CHECKSTYLE:OFF */ {
            /* Squash this */
        } /* CHECKSTYLE:ON */
    }

    @Override
    protected ServerSocket createAsyncServerSocket(int port)
        throws IOException {

        if (port != 0) {
            final ServerSocket ss = retrievePendingSocket(port);
            if (ss != null) {
                return ss;
            }
        }
        return new SSLAsyncServerSocket(port);
    }

    @Override
    protected ListenerConfigBuilder getListenerConfigBuilder(int port) {
        final ListenerConfigBuilder listenerBuilder =
            super.getListenerConfigBuilder(port);
        listenerBuilder.getEndpointConfigBuilder().sslControl(sslControl);
        return listenerBuilder;
    }

    /**
     * A server socket that does not perform peer authentication.
     */
    private class SSLNoPeerAuthServerSocket extends ServerSocket {
        SSLNoPeerAuthServerSocket(int port, int backlog) throws IOException {
            super(port, backlog);
        }
        @Override
        public Socket accept() throws IOException {
            return wrapSSLSocket(super.accept(), null);
        }
    }

    /** Construct an SSL socket on top of the accepted socket */
    private SSLSocket wrapSSLSocket(Socket socket, ByteBuffer preReadData)
        throws IOException {

        InputStream preReadInput = null;
        if (preReadData != null) {
            final byte[] preReadBytes = new byte[preReadData.remaining()];
            preReadData.get(preReadBytes);
            preReadInput = new ByteArrayInputStream(preReadBytes);
        }
        final SSLSocket sslSocket = (SSLSocket)
            sslControl.sslContext().getSocketFactory().createSocket(
                socket, preReadInput, true /* autoClose */);

        /* By definition we are not using client mode */
        sslSocket.setUseClientMode(false);

        sslControl.applySSLParameters(sslSocket);

        return sslSocket;
    }

    /**
     * A server socket that authenticates peers.
     */
    private class SSLPeerAuthServerSocket extends ServerSocket {
        private final AcceptQueue acceptQueue = new AcceptQueue();
        private final ThreadPoolExecutor handshakeExecutor;
        private final Thread rawAcceptor;

        SSLPeerAuthServerSocket(int port, int backlog) throws IOException {
            super(port, backlog);

            final int listenPort = getLocalPort();
            handshakeExecutor = createHandshakeExecutor(listenPort);

            /*
             * Note that the RawAcceptor thread will exit on its own when the
             * server socket is closed.
             */
            final String acceptorName = "SSLAccept-" + listenPort;
            rawAcceptor = new Thread(
                new RawAcceptor(this, handshakeExecutor, acceptQueue),
                acceptorName);
            rawAcceptor.setDaemon(true);
            rawAcceptor.setUncaughtExceptionHandler(
                new LogUncaughtException());
            rawAcceptor.start();
        }

        /** Return a socket accepted the normal way. */
        Socket normalAccept() throws IOException {
            return super.accept();
        }

        @Override
        public Socket accept() throws IOException {
            return acceptQueue.accept();
        }

        @Override
        public void close() throws IOException {
            /* This field could be null if super contructor failed */
            if (handshakeExecutor != null) {
                handshakeExecutor.shutdown();
            }
            super.close();
        }
    }

    private ThreadPoolExecutor createHandshakeExecutor(int listenPort) {
        final BlockingQueue<Runnable> handshakeExecutionQueue =
            new LinkedBlockingQueue<Runnable>(HANDSHAKE_QUEUE_MAX);
        return new ThreadPoolExecutor(1, /* corePoolSize */
                                      HANDSHAKE_THREAD_MAX,
                                      KEEPALIVE_MS,
                                      TimeUnit.MILLISECONDS,
                                      handshakeExecutionQueue,
                                      new HandshakeThreadFactory(listenPort));
    }

    private class HandshakeThreadFactory implements ThreadFactory {
        private volatile int listenPort;
        HandshakeThreadFactory(int listenPort) {
            this.listenPort = listenPort;
        }
        @Override
        public Thread newThread(Runnable r) {
            final String tName = "SSLHandshake-" + listenPort + "-" +
                hsThreadCounter.getAndIncrement();
            final Thread t = new Thread(r, tName);
            t.setDaemon(true);
            t.setUncaughtExceptionHandler(new LogUncaughtException());
            return t;
        }
    }

    private class LogUncaughtException
            implements Thread.UncaughtExceptionHandler {

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            if (connectionLogger != null) {
                connectionLogger.log(
                    Level.SEVERE,
                    t + " experienced uncaught exception of " + e,
                    e);
            }
        }
    }

    /**
     * The RawAcceptor class performs the raw accept() operation on the
     * standard server socket.  Accepted sockets are put on a queue to be
     * processed in the background.
     */
    private final class RawAcceptor implements Runnable {
        private final SSLPeerAuthServerSocket serverSocket;
        private final ThreadPoolExecutor handshakeExecutor;
        private final AcceptQueue acceptQueue;
        RawAcceptor(SSLPeerAuthServerSocket serverSocket,
                    ThreadPoolExecutor handshakeExecutor,
                    AcceptQueue acceptQueue) {
            this.serverSocket = serverSocket;
            this.handshakeExecutor = handshakeExecutor;
            this.acceptQueue = acceptQueue;
        }
        @Override
        public void run() {
            while (true) {
                final SSLSocket sslSocket;
                try {
                    sslSocket =
                        wrapSSLSocket(serverSocket.normalAccept(), null);
                } catch (IOException ioe) {

                    /*
                     * An IOException indicates a shutdown of the server
                     * socket, which is our cue to quit.
                     */
                    try {
                        acceptQueue.close(ioe);
                    } catch (IOException e) {
                    }
                    /* Make sure the server socket is closed */
                    try {
                        serverSocket.close();
                    } catch (IOException e) {
                    }
                    if (connectionLogger != null) {
                        connectionLogger.info(
                            "Shutdown accept queue for port " +
                            serverSocket.getLocalPort());
                    }
                    return;
                }

                /*
                 * The SSLControl object wants to do SSL certificate
                 * verification, and we need to wait until the handshake is
                 * complete for that to happen.  Put the socket on the
                 * handshake queue.
                 */
                try {
                    handshakeExecutor.execute(
                        new HandshakeAndVerify(sslSocket, acceptQueue));
                } catch (RejectedExecutionException ie) {
                    if (connectionLogger != null) {
                        connectionLogger.info(
                            "Unable to queue socket for " +
                            "verification - interrupted.");
                    }
                    forceCloseSocket(sslSocket);
                }
            }
        }
    }

    /**
     * Simple class to wait for the the SSL handshake to complete, apply SSL
     * certificate authentication, and then add the socket to the accept queue.
     */
    private final class HandshakeAndVerify implements Runnable {
        private final SSLSocket sslSocket;
        private final AcceptQueue acceptQueue;

        private HandshakeAndVerify(SSLSocket sslSocket,
                                   AcceptQueue acceptQueue) {
            this.sslSocket = sslSocket;
            this.acceptQueue = acceptQueue;
        }

        @Override
        public void run() {
            if (authenticateNewSocket(sslSocket)) {
                if (!acceptQueue.offer(sslSocket)) {
                    if (connectionLogger != null) {
                        connectionLogger.info(
                            "Refused socket because accept queue is full");
                    }
                    forceCloseSocket(sslSocket);
                }
            }
        }
    }

    /**
     * Give a newly accepted socket, perform peer authentication and, if
     * acceptable, return true.  If there is a problem with the socket, log any
     * errors, close the socket and return false.
     */
    private boolean authenticateNewSocket(SSLSocket sslSocket) {
        try {
            /* blocks until completion on initial call */
            sslSocket.startHandshake();

            if (sslControl.peerAuthenticator().
                isTrusted(sslSocket.getSession())) {
                return true;
            }

            if (connectionLogger != null) {
                connectionLogger.info("Rejecting client connection");
            }
            forceCloseSocket(sslSocket);
        } catch (IOException ioe) {
            if (connectionLogger != null) {
                connectionLogger.info("error while handshaking: " + ioe);
            }
            forceCloseSocket(sslSocket);
        }

        return false;
    }

    private class SSLAsyncServerSocket extends AsyncServerSocket {
        private final Object handshakeExecutorLock = new Object();
        private ThreadPoolExecutor handshakeExecutor;

        SSLAsyncServerSocket(int port) throws IOException {
            super(port);
        }

        @Override
        public void close() throws IOException {
            synchronized (closeLock) {
                if (closed) {
                    return;
                }
            }
            synchronized (handshakeExecutorLock) {
                if (handshakeExecutor != null) {
                    handshakeExecutor.shutdown();
                }
            }
            super.close();
        }

        @Override
        public void onPrepared(ByteBuffer preReadData,
                               Socket socket) {
            synchronized (closeLock) {
                if (closed) {
                    return;
                }
            }
            boolean ok = false;
            try {
                final SSLSocket sslSocket;
                try {
                    sslSocket = wrapSSLSocket(socket, preReadData);
                } catch (IOException e) {
                    if (connectionLogger != null) {
                        connectionLogger.log(
                            Level.WARNING,
                            "Problem creating SSL socket: " + e,
                            e);
                    }
                    return;
                }
                if (sslControl.peerAuthenticator() != null) {
                    try {
                        getHandshakeExecutor().execute(
                            new HandshakeAndVerify(sslSocket, acceptQueue));
                        ok = true;
                    } catch (RejectedExecutionException ie) {
                        if (connectionLogger != null) {
                            connectionLogger.info(
                                "Unable to queue socket for " +
                                "verification - interrupted.");
                        }
                    }
                } else if (acceptQueue.offer(sslSocket)) {
                    ok = true;
                } else {
                    if (connectionLogger != null) {
                        connectionLogger.info(
                            "Refused socket because accept queue is full");
                    }
                }
            } finally {
                if (!ok) {
                    forceCloseSocket(socket);
                }
            }
        }

        private ThreadPoolExecutor getHandshakeExecutor() {
            synchronized (handshakeExecutorLock) {
                if (handshakeExecutor == null) {
                    handshakeExecutor =
                        createHandshakeExecutor(getLocalPort());
                }
                return handshakeExecutor;
            }
        }
    }
}
