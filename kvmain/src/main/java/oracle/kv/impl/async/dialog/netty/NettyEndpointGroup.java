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

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.SocketAddress;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.impl.async.AbstractEndpointGroup;
import oracle.kv.impl.async.AbstractListener;
import oracle.kv.impl.async.DialogHandlerFactoryMap;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.ListenerConfig;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.test.TestIOHook;
import oracle.kv.impl.util.EventTrackersManager;
import oracle.kv.impl.util.HostPort;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;

public class NettyEndpointGroup extends AbstractEndpointGroup {

    public static volatile TestIOHook<Channel> acceptTestHook = null;

    private static boolean logHandlerEnabled = false;

    /* Use the jdk logging. */
    static {
        InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
    }

    private final EventLoopGroup eventLoopGroup;
    private final EventTrackersManager trackersManager;


    public NettyEndpointGroup(Logger logger,
                              boolean forClientOnly,
                              int nthreads,
                              int numPermits,
                              UncaughtExceptionHandler uncaughtExceptionHandler)
        throws Exception
    {
        super(logger, forClientOnly, numPermits, uncaughtExceptionHandler);
        this.eventLoopGroup = new NioEventLoopGroup(nthreads);
        this.trackersManager = new EventTrackersManager(logger, eventLoopGroup);
    }

    /* For testing */
    public NettyEndpointGroup(Logger logger,
                              int nthreads) throws Exception {
        this(logger, false /* forClientOnly */, nthreads, Integer.MAX_VALUE,
             (t, e) -> { } /* uncaught exception handler */);
    }

    /**
     * Enables the netty log handler for debugging.
     *
     * This will add a log handler to the pipeline.
     */
    public static void enableLogHandler() {
        logHandlerEnabled = true;
    }

    /**
     * Disables the netty log handler.
     */
    public static void disableLogHandler() {
        logHandlerEnabled = false;
    }

    /**
     * Returns {@code true} if log handler is enabled.
     */
    public static boolean logHandlerEnabled() {
        return logHandlerEnabled;
    }

    @Override
    public ScheduledExecutorService getSchedExecService() {
        return eventLoopGroup;
    }

    @Override
    public EventTrackersManager getEventTrackersManager() {
        return trackersManager;
    }

    @Override
    protected NettyCreatorEndpoint
        newCreatorEndpoint(String perfName,
                           NetworkAddress remoteAddress,
                           NetworkAddress localAddress,
                           EndpointConfig endpointConfig) {

        return new NettyCreatorEndpoint(
            this, eventLoopGroup, perfName,
            remoteAddress, localAddress, endpointConfig);
    }

    @Override
    protected NettyListener newListener(AbstractEndpointGroup endpointGroup,
                                        ListenerConfig listenerConfig,
                                        DialogHandlerFactoryMap
                                        dialogHandlerFactories) {

        return new NettyListener(
            endpointGroup, listenerConfig, dialogHandlerFactories);
    }

    @Override
    protected void shutdownInternal(boolean force) {
        if (force) {
            eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
        } else {
            eventLoopGroup.shutdownGracefully();
        }
    }


    class NettyListener extends AbstractListener {

        /*
         * Assigned inside the parent listening channel lock. Volatile for
         * read.
         */
        private volatile Channel listeningChannel = null;

        NettyListener(AbstractEndpointGroup endpointGroup,
                      ListenerConfig listenerConfig,
                      DialogHandlerFactoryMap
                      dialogHandlerFactories) {
            super(endpointGroup, listenerConfig, dialogHandlerFactories);
        }

        @Override
        public HostPort getLocalAddressInternal() {
            ensureListeningChannelLockHeld();
            return NettyUtil.getLocalAddress(listeningChannel);
        }

        /**
         * Creates the listening channel if not existing yet.
         *
         * The method is called inside a synchronization block of the parent
         * endpoint group.
         */
        @Override
        protected void createChannelInternal() throws IOException {
            ensureListeningChannelLockHeld();
            if (listeningChannel == null) {
                ServerBootstrap serverBootstrap = new ServerBootstrap();
                serverBootstrap.group(eventLoopGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new ChannelAcceptHandler())
                    .childHandler(new Initializer());
                listeningChannel =
                    NettyUtil.listen(serverBootstrap, listenerConfig);
            }
        }

        /**
         * Close the created listening channel.
         *
         * The method is called inside a synchronization block of the parent
         * endpoint group.
         */
        @Override
        protected void closeChannel() {
            if (listeningChannel == null) {
                return;
            }
            listeningChannel.close();
            listeningChannel = null;
        }

        @Override
        protected boolean tryAccept() {
            /*
             * TODO: I currently don't know how to reach inside netty to
             * attempt an accept, but we are not actively using a netty
             * endpoint group right now and this method is just for an
             * optimization. So future task if really necessary.
             */
            return false;
        }

        @Override
        protected void registerAccept() throws IOException {
            final Channel ch = listeningChannel;
            if (ch != null) {
                eventLoopGroup.register(ch);
            }
        }

        @Override
        protected void deregisterAccept() throws IOException {
            final Channel ch = listeningChannel;
            if (ch != null) {
                ch.deregister();
            }
        }

        private class Initializer extends ChannelInitializer<SocketChannel> {

            @Override
            public void initChannel(SocketChannel channel) {
                SocketAddress addr = channel.remoteAddress();
                try {
                    /*
                     * TODO: We are not supposed to do a blocking operation of
                     * address resolution here. We are supposed to schedule a
                     * task after resolution is done and then add the handler
                     * to the pipeline. However, it seems netty will discard
                     * the messge if we do not add the handler right now when
                     * there is a possibility that there are further messages.
                     * Perhaps I need to add a buffering handler here before
                     * the resultion and switch the real handler when it is
                     * done. We are not really using netty in our production,
                     * and so this seems not worth the effort.
                     */
                    NetworkAddress remoteAddress =
                        NetworkAddress.convert(addr).get();
                    NettyResponderEndpoint endpoint = new NettyResponderEndpoint(
                        NettyEndpointGroup.this,
                        remoteAddress,
                        listenerConfig,
                        NettyListener.this,
                        endpointConfig);
                    cacheResponderEndpoint(endpoint);
                    acceptResponderEndpoint(endpoint);
                    ChannelPipeline pipeline = channel.pipeline();
                    pipeline.addLast(endpoint.getHandler().getInboundHandler());
                } catch (Throwable t) {
                    throw new RuntimeException(t);
                }
            }
        }

        private class ChannelAcceptHandler
                extends ChannelInboundHandlerAdapter {

            @Override
            public void
                channelRead(ChannelHandlerContext ctx,
                            Object msg) throws Exception {
                TestHookExecute.doIOHookIfSet(acceptTestHook, ctx.channel());
                super.channelRead(ctx, msg);
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx,
                                        Throwable cause)
                throws Exception {
                onChannelError(cause, !ctx.channel().isOpen());
            }
        }
    }

}
