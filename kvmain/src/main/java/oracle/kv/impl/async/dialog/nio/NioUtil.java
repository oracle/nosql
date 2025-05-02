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

package oracle.kv.impl.async.dialog.nio;

import static oracle.kv.impl.async.FutureUtils.checked;
import static oracle.kv.impl.async.FutureUtils.handleFutureGetException;

import java.io.IOException;
import java.net.BindException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketOption;
import java.net.StandardSocketOptions;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLEngine;

import oracle.kv.impl.async.AsyncOption;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.InetNetworkAddress;
import oracle.kv.impl.async.ListenerConfig;
import oracle.kv.impl.async.ListenerPortRange;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.security.ssl.SSLControl;
import oracle.kv.impl.util.HostPort;

import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.net.InstanceLogger;
import com.sleepycat.je.rep.utilint.net.SSLDataChannel;
import com.sleepycat.je.rep.utilint.net.SimpleDataChannel;

/**
 * Util class for nio channel operations.
 */
public class NioUtil {

    /**
     * A mapping from supported socket options to async options.
     *
     * The mapping includes all supported socket options that have an async
     * correspondence. When configuring a socket channel, all options are
     * looked up for their async value and set accordingly if the channel
     * supports it.
     */
    public static final Map<SocketOption<?>, AsyncOption<?>> socketOptions;
    static {
        final Map<SocketOption<?>, AsyncOption<?>> map = new HashMap<>();
        addSocketOption(StandardSocketOptions.SO_KEEPALIVE,
                        AsyncOption.SO_KEEPALIVE, map);
        addSocketOption(StandardSocketOptions.SO_LINGER,
                        AsyncOption.SO_LINGER, map);
        addSocketOption(StandardSocketOptions.SO_RCVBUF,
                        AsyncOption.SO_RCVBUF, map);
        addSocketOption(StandardSocketOptions.SO_REUSEADDR,
                        AsyncOption.SO_REUSEADDR, map);
        addSocketOption(StandardSocketOptions.SO_SNDBUF,
                        AsyncOption.SO_SNDBUF, map);
        addSocketOption(StandardSocketOptions.TCP_NODELAY,
                        AsyncOption.TCP_NODELAY, map);
        socketOptions = Collections.unmodifiableMap(map);
    }

    /**
     * A mapping from supported server socket options to async options.
     *
     * The mapping includes all supported server socket options that have an
     * async correspondence. When configuring a server socket channel, all
     * options are looked up for their async value and set accordingly if the
     * channel supports it.
     */
    public static final Map<SocketOption<?>, AsyncOption<?>>
        serverSocketOptions;
    static {
        final Map<SocketOption<?>, AsyncOption<?>> map = new HashMap<>();
        addSocketOption(StandardSocketOptions.SO_RCVBUF,
                        AsyncOption.SO_RCVBUF, map);
        addSocketOption(StandardSocketOptions.SO_REUSEADDR,
                        AsyncOption.SO_REUSEADDR, map);
        serverSocketOptions = Collections.unmodifiableMap(map);
    }

    /**
     * Store a socket option, with compile type checking that both options have
     * the same parameter type.
     */
    private static <T>
        void addSocketOption(SocketOption<T> socketOption,
                             AsyncOption<T> asyncOption,
                             Map<SocketOption<?>, AsyncOption<?>> map) {
        map.put(socketOption, asyncOption);
    }

    /**
     * Creates a socket channel for a client.  Note that the channel may not be
     * connected, so the caller should check for that and register for connect
     * events if it isn't connected yet.
     */
    public static CompletableFuture<SocketChannel>
        getSocketChannel(NetworkAddress address,
                         NetworkAddress localAddress,
                         EndpointConfig endpointConfig)
    {
        final CompletableFuture<SocketAddress> localResolution =
            (localAddress == null)
            ? CompletableFuture.completedFuture(null)
            : localAddress.resolveSocketAddress();
        final CompletableFuture<SocketAddress> remoteResolution =
            address.resolveSocketAddress();
        return localResolution.thenCombine(
            remoteResolution,
            checked(
                (local, remote) -> {
                    final SocketChannel socketChannel =
                        address.openSocketChannel();
                    return bindAndConnectSocketChannel(
                        socketChannel, local, remote, endpointConfig);
                }));
    }

    /**
     * Binds and connects the opened channel.
     */
    private static SocketChannel
        bindAndConnectSocketChannel(SocketChannel socketChannel,
                                    SocketAddress local,
                                    SocketAddress remote,
                                    EndpointConfig endpointConfig)
        throws IOException
    {
        boolean succeeded = false;
        try {
            configureSocketChannel(socketChannel, endpointConfig);
            if (local != null) {
                socketChannel.bind(local);
            }
            socketChannel.connect(remote);
            succeeded = true;
            return socketChannel;
        } catch (SocketException e) {
            final SocketException se = new SocketException(
                "Failed to connect:" +
                (local != null ? " local address=" + local : "") +
                " address=" + remote + ": " + e);
            se.initCause(e);
            throw se;
        } catch (UnresolvedAddressException e) {
            /*
             * Treat this as an IOException so it can be handled along with
             * other I/O failures. We can't be sure that the problem is
             * with the specified address and not the local address, but it
             * is a reasonable guess, and the cause will describe the real
             * problem.
             *
             * Note that the constructor of InetSocketAddress will resolve the
             * address, but does not throw exception and only flag the address
             * as unresolved. The connect will throw exception and therefore,
             * we handle the exception here.
             */
            final UnknownHostException uhe =
                new UnknownHostException(
                    "Unknown host for address: " + remote);
            uhe.initCause(e);
            throw uhe;
        } finally {
            if (!succeeded) {
                /*
                 * Ensure to close socketChannel upon any exception.
                 * [KVSTORE-1515] discovers that the sock file descriptor is
                 * leaked under certain exceptions such as
                 * UnresolvedAddressException if the channel is not closed
                 * explicitly.
                 */
                try {
                    socketChannel.close();
                } catch (IOException e) {
                    /* ignore */
                }
            }
        }
    }

    /**
     * Creates a data channel from a socket channel.
     */
    public static DataChannel getDataChannel(SocketChannel socketChannel,
                                             EndpointConfig endpointConfig,
                                             boolean isClient,
                                             NetworkAddress remoteAddress,
                                             Logger logger)
        throws IOException {

        configureSocketChannel(socketChannel, endpointConfig);
        return getDataChannel(
            socketChannel, endpointConfig.getSSLControl(),
            isClient, remoteAddress.getHostName(), logger);
    }

    /**
     * Listens with listener channel factory.
     *
     * This is a blocking method as it calls Future#get.
     */
    public static ServerSocketChannel listen(
            final ListenerConfig listenerConfig) throws IOException {

        final String hostName = listenerConfig.getHostName();
        /*
         * Create a network address that we can use to open a server socket
         * channel of the correct type
         */
        final NetworkAddress basicNetworkAddress =
            NetworkAddress.createNetworkAddress(hostName, 0);
        final ServerSocketChannel serverSocketChannel =
            basicNetworkAddress.openServerSocketChannel();
        /* Set options */
        for (final SocketOption<?> option : serverSocketOptions.keySet()) {
            setSocketOption(option, listenerConfig, serverSocketChannel);
        }

        final int backlog = listenerConfig.getOption(AsyncOption.SSO_BACKLOG);
        final ListenerPortRange portRange = listenerConfig.getPortRange();
        final int portStart = portRange.getPortStart();
        final int portEnd = portRange.getPortEnd();
        IOException lastException = null;
        for (int port = portStart; port <= portEnd; ++port) {
            try {
                NetworkAddress.createNetworkAddress(hostName, port)
                    .bind(serverSocketChannel, backlog)
                    .get();
                return serverSocketChannel;
            } catch (Exception e) {
                Exception cause;
                try {
                    cause = handleFutureGetException(e);
                    if (cause instanceof IOException) {
                        /* Address in use or unusable, continue the scan */
                        lastException = (IOException) cause;
                        continue;
                    }
                } catch (InterruptedException|TimeoutException ee) {
                    cause = ee;
                }
                throw new IllegalStateException(
                    "Unexpected exception: " + cause, cause);
            }
        }

        /*
         * For compatibility with
         * ServerSocketFactory.createStandardServerSocket, throw a
         * BindException in the single port case, since I believe that is what
         * the standard server socket constructor does.
         */
        final String msg = String.format(
            "No free local address to bind for host %s and port range %s%s",
            hostName,
            portRange,
            ((lastException != null) ?
             ": " + lastException.getMessage() :
             ""));
        if (portStart == portEnd) {
            final BindException bindException = new BindException(msg);
            bindException.initCause(lastException);
            throw bindException;
        }
        throw new IOException(msg, lastException);
    }

    private static <T> void setSocketOption(SocketOption<T> option,
                                            ListenerConfig listenerConfig,
                                            ServerSocketChannel channel)
        throws IOException
    {
        /* Checked that key and value had same type when they were added */
        @SuppressWarnings("unchecked")
        final AsyncOption<T> asyncOption =
            (AsyncOption<T>) serverSocketOptions.get(option);
        if (asyncOption != null) {
            final T val = listenerConfig.getOption(asyncOption);
            if ((val != null) && channel.supportedOptions().contains(option)) {
                channel.setOption(option, val);
            }
        }
    }

    /**
     * Returns the remote network address of a socket channel. Returns
     * InetNetworkAddress.ANY_LOCAL_ADDRESS if the channel is closed or an I/O
     * error occurs.
     */
    public static CompletableFuture<NetworkAddress>
        getRemoteAddress(SocketChannel socketChannel)
    {
        final SocketAddress addr;
        try {
            addr = socketChannel.getRemoteAddress();
        } catch (IOException e) {
            return CompletableFuture.completedFuture(
                InetNetworkAddress.ANY_LOCAL_ADDRESS);
        }
        return NetworkAddress.convert(addr).exceptionally(
            checked(
                (ex) -> {
                    if (ex instanceof IOException) {
                        return InetNetworkAddress.ANY_LOCAL_ADDRESS;
                    }
                    throw ex;
                }));
    }

    /**
     * Returns the remote network address of a socket channel without any
     * blocking operation, such as querying the DNS to resolve an
     * InetSocketAddress to a host name.
     *
     * @see NetworkAddress#convertNow
     */
    public static NetworkAddress
        getRemoteAddressNow(SocketChannel socketChannel)
    {
        final SocketAddress addr;
        try {
            addr = socketChannel.getRemoteAddress();
        } catch (IOException e) {
            return InetNetworkAddress.ANY_LOCAL_ADDRESS;
        }
        return NetworkAddress.convertNow(addr);
    }

    /**
     * Returns the local network address of a server socket channel. Returns
     * InetNetworkAddress.ANY_LOCAL_ADDRESS if the channel is closed or an I/O
     * error occurs.
     */
    public static HostPort getLocalAddress(
        ServerSocketChannel serverSocketChannel)
    {
        try {
            return HostPort.convert(serverSocketChannel.getLocalAddress());
        } catch (IOException e) {
            return HostPort.convert(InetNetworkAddress.ANY_LOCAL_ADDRESS);
        }
    }

    /**
     * Configures the socket channel with options.
     */
    public static void configureSocketChannel(
        final SocketChannel socketChannel,
        final EndpointConfig endpointConfig)
        throws IOException {

        socketChannel.configureBlocking(false);
        for (final SocketOption<?> option : socketOptions.keySet()) {
            setSocketOption(option, endpointConfig, socketChannel);
        }
    }

    private static <T> void setSocketOption(SocketOption<T> option,
                                            EndpointConfig endpointConfig,
                                            SocketChannel channel)
        throws IOException
    {
        /* Checked that key and value had same type when they were added */
        @SuppressWarnings("unchecked")
        final AsyncOption<T> asyncOption =
            (AsyncOption<T>) socketOptions.get(option);
        if (asyncOption != null) {
            final T val = endpointConfig.getOption(asyncOption);
            if ((val != null) && channel.supportedOptions().contains(option)) {
                channel.setOption(option, val);
            }
        }
    }

    /**
     * Creates the data channel.
     */
    public static DataChannel getDataChannel(SocketChannel socketChannel,
                                             SSLControl sslControl,
                                             boolean isClient,
                                             String hostname,
                                             Logger logger) {

        if (sslControl == null) {
            return new SimpleDataChannel(socketChannel);
        }

        /*
         * Only TCP/IP channels have sockets associated with them; ones based
         * on Unix domain sockets do not
        */
        final Socket socket;
        try {
            socket = socketChannel.socket();
        } catch (UnsupportedOperationException e) {
            throw new IllegalStateException(
                "Socket access not supported for this channel, maybe" +
                " security was enabled for wrong channel type: " + e,
                e);
        }

        /*
         * Note that Unix domain socket channels don't support the socket
         * method, but we shouldn't be using security with these channels.
         */
        final SSLEngine engine = sslControl.sslContext().createSSLEngine(
                hostname, socket.getPort());
        engine.setSSLParameters(sslControl.sslParameters());
        engine.setUseClientMode(isClient);
        if (!isClient) {
            if (sslControl.peerAuthenticator() != null) {
                engine.setNeedClientAuth(true);
            }
        }
        return new SSLDataChannel(
                socketChannel, engine, hostname,
                sslControl.hostVerifier(), sslControl.peerAuthenticator(),
                new WrappedLogger(logger));
    }

    /**
     * A Wrapped class for je instance logger.
     */
    private static class WrappedLogger implements InstanceLogger {

        private final Logger logger;

        WrappedLogger(Logger logger) {
            this.logger = logger;
        }

        @Override
        public void log(Level logLevel, String msg) {
            logger.log(logLevel, msg);
        }

        @Override
        public void log(Level logLevel, Supplier<String> supplier) {
            logger.log(logLevel, supplier);
        }

        @Override
        public boolean isLoggable(Level logLevel) {
            return logger.isLoggable(logLevel);
        }
    }
}
