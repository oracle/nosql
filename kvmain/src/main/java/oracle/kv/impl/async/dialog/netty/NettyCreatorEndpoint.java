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

import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

import oracle.kv.impl.async.AbstractCreatorEndpoint;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.EndpointHandler;
import oracle.kv.impl.async.FutureUtils;
import oracle.kv.impl.async.NetworkAddress;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Netty creator endpoint.
 */
class NettyCreatorEndpoint extends AbstractCreatorEndpoint {

    private final NettyEndpointGroup endpointGroup;
    private final EventLoopGroup eventLoopGroup;


    NettyCreatorEndpoint(NettyEndpointGroup endpointGroup,
                         EventLoopGroup eventLoopGroup,
                         String perfName,
                         NetworkAddress remoteAddress,
                         NetworkAddress localAddress,
                         EndpointConfig endpointConfig) {
        super(endpointGroup, perfName,
              remoteAddress, localAddress, endpointConfig);
        this.endpointGroup = endpointGroup;
        this.eventLoopGroup = eventLoopGroup;
    }

    @Override
    protected CompletableFuture<EndpointHandler> newEndpointHandler() {
        final NetworkAddress address = getRemoteAddress();

        final PreWriteWrappedEndpointHandler handler =
            new PreWriteWrappedEndpointHandler(
                NettyCreatorEndpoint.this, endpointConfig,
                perfName, address);
        try {
            final CompletableFuture<EndpointHandler> result = new CompletableFuture<>();
            final Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .handler(new Initializer(handler));

            NettyUtil.connect(bootstrap, endpointConfig, address)
                .addListener((future) -> {
                    if (future.isSuccess()) {
                        result.complete(handler);
                    } else {
                        final Throwable cause = future.cause();
                        handler.shutdown(
                            String.format(
                                "Error initializing endpoint handler",
                                cause),
                            true);
                        result.completeExceptionally(cause);
                    }
            });
            return result;
        } catch (Throwable t) {
            return FutureUtils.failedFuture(t);
        }
    }

    Logger getLogger() {
        return endpointGroup.getLogger();
    }

    private class Initializer extends ChannelInitializer<SocketChannel> {

        private final PreWriteWrappedEndpointHandler handler;

        Initializer(PreWriteWrappedEndpointHandler handler) {
            this.handler = handler;
        }

        @Override
        public void initChannel(SocketChannel channel) {
            ChannelPipeline pipeline = channel.pipeline();
            pipeline.addLast(handler.getInboundHandler());
        }
    }
}
