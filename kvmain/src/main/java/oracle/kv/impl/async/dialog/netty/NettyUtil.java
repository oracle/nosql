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

package oracle.kv.impl.async.dialog.netty;

import static oracle.kv.impl.util.ObjectUtil.checkNull;
import static oracle.kv.impl.async.FutureUtils.handleFutureGetException;

import java.io.IOException;
import java.net.BindException;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;
import javax.net.ssl.SSLEngine;

import oracle.kv.impl.async.AsyncOption;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.ListenerConfig;
import oracle.kv.impl.async.ListenerPortRange;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.security.ssl.SSLControl;
import oracle.kv.impl.util.HostPort;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.AbstractNioChannel;

/**
 * Utils for netty.
 */
class NettyUtil {

    /**
     * A mapping from async options to netty options.
     *
     * The mapping includes all the async options that has a netty
     * correspondence. When configuring a netty channel all proper async
     * options are translated to the netty ones.
     */
    public static Map<AsyncOption<?>, ChannelOption<?>> optionMapping;
    static {
        Map<AsyncOption<?>, ChannelOption<?>> mapping =
            new HashMap<AsyncOption<?>, ChannelOption<?>>();
        mapping.put(AsyncOption.SO_KEEPALIVE,
                ChannelOption.SO_KEEPALIVE);
        mapping.put(AsyncOption.SO_LINGER,
                ChannelOption.SO_LINGER);
        mapping.put(AsyncOption.SO_RCVBUF,
                ChannelOption.SO_RCVBUF);
        mapping.put(AsyncOption.SO_REUSEADDR,
                ChannelOption.SO_REUSEADDR);
        mapping.put(AsyncOption.SO_SNDBUF,
                ChannelOption.SO_SNDBUF);
        mapping.put(AsyncOption.TCP_NODELAY,
                ChannelOption.TCP_NODELAY);
        mapping.put(AsyncOption.SSO_BACKLOG,
                ChannelOption.SO_BACKLOG);
        optionMapping = Collections.unmodifiableMap(mapping);
    }

    /**
     * Client supported options.
     */
    public static AsyncOption<?>[] clientSupportedOptions =
        new AsyncOption<?>[] {
            AsyncOption.SO_KEEPALIVE,
            AsyncOption.SO_LINGER,
            AsyncOption.SO_RCVBUF,
            AsyncOption.SO_REUSEADDR,
            AsyncOption.SO_SNDBUF,
            AsyncOption.TCP_NODELAY,
        };

    /**
     * Server supported options.
     */
    public static AsyncOption<?>[] serverSupportedOptions =
        new AsyncOption<?>[] {
            AsyncOption.SO_RCVBUF,
            AsyncOption.SO_REUSEADDR,
            AsyncOption.SSO_BACKLOG,
        };

    /**
     * Configure the bootstrap and connect.
     */
    public static ChannelFuture connect(final Bootstrap bootstrap,
                                        final EndpointConfig endpointConfig,
                                        NetworkAddress address) {
        /* Set options */
        for (AsyncOption<?> clientOption : clientSupportedOptions) {
            new OptionSetter() {
                @Override
                @SuppressWarnings("unchecked")
                public <T> void set(AsyncOption<T> ao) {
                    ChannelOption<T> co =
                        (ChannelOption<T>) optionMapping.get(ao);
                    final T val = endpointConfig.getOption(ao);
                    if (val != null) {
                        bootstrap.option(co, val);
                    }
                }
            }.set(clientOption);
        }

        return bootstrap.connect(address.getHostName(), address.getPort());
    }

    /**
     * Configure the server bootstrap and listen.
     */
    public static Channel listen(final ServerBootstrap serverBootstrap,
                                 final ListenerConfig listenerConfig)
        throws IOException {

        /* Set options */
        for (AsyncOption<?> serverOption : serverSupportedOptions) {
            new OptionSetter() {
                @Override
                @SuppressWarnings("unchecked")
                public <T> void set(AsyncOption<T> ao) {
                    ChannelOption<T> co =
                        (ChannelOption<T>) optionMapping.get(ao);
                    final T val = listenerConfig.getOption(ao);
                    if (val != null) {
                        serverBootstrap.option(co, val);
                    }
                }
            }.set(serverOption);
        }

        final ListenerPortRange portRange = listenerConfig.getPortRange();
        final String hostName = listenerConfig.getHostName();
        final int portStart = portRange.getPortStart();
        final int portEnd = portRange.getPortEnd();
        IOException lastException = null;
        for (int port = portStart; port <= portEnd; ++port) {
            final NetworkAddress networkAddress =
                NetworkAddress.createNetworkAddress(hostName, port);
            try {
                /*
                 * TODO: we invoke a blocking Future#get here where if we
                 * strictly follows the paradigm for netty, we sould not do
                 * that. However, this is a listen call which will only be on a
                 * synchronous code path and this is netty code which is not
                 * used for production. Therefore, for simplicity, I will just
                 * invoke this in a blocking manner.
                 */
                final SocketAddress addr =
                    networkAddress.resolveSocketAddress().get();
                final ChannelFuture future = serverBootstrap.bind(addr);
                future.sync();
                if (future.isSuccess()) {
                    return future.channel();
                }
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

    /**
     * Returns the local address of the netty channel.
     */
    public static HostPort getLocalAddress(Channel channel) {
        SocketAddress socketAddress = channel.localAddress();
        return HostPort.convert(socketAddress);
    }

    /**
     * Returns the nio socket channel of the netty channel.
     */
    public static SocketChannel getSocketChannel(Channel channel) {
        AbstractNioChannel nioChannel = (AbstractNioChannel) channel;
        AbstractNioChannel.NioUnsafe nioUnsafe = nioChannel.unsafe();
        return (SocketChannel) nioUnsafe.ch();
    }

    /**
     * Returns an VerifyingSSLHandler.
     */
    public static VerifyingSSLHandler newSSLHandler(
            String endpointId,
            SSLControl sslControl,
            NetworkAddress remoteAddress,
            boolean isClient,
            Logger logger) {

        checkNull("sslControl", sslControl);

        final String hostname = remoteAddress.getHostName();
        final int port = remoteAddress.getPort();
        final SSLEngine engine =
            sslControl.sslContext().createSSLEngine(hostname, port);
        engine.setSSLParameters(sslControl.sslParameters());
        engine.setUseClientMode(isClient);
        if (!isClient) {
            if (sslControl.peerAuthenticator() != null) {
                engine.setWantClientAuth(true);
            }
        }
        final String targetHost = isClient ? hostname : null;
        return new VerifyingSSLHandler(
                logger, endpointId, engine, targetHost,
                sslControl.hostVerifier(), sslControl.peerAuthenticator());
    }

    /**
     * Helper class for setting a socket channel option.
     */
    private interface OptionSetter {
        <T> void set(AsyncOption<T> option);
    }

}
