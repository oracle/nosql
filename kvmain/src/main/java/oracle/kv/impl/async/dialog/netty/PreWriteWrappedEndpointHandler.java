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

import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.dialog.PreReadWriteWrappedEndpointHandler;
import oracle.kv.impl.async.dialog.ProtocolMesg;
import oracle.kv.impl.security.ssl.SSLControl;
import oracle.kv.impl.util.CommonLoggerUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.logging.LoggingHandler;

/**
 * Pre-writes the async magic number and add the real handlers afterward.
 */
class PreWriteWrappedEndpointHandler
    extends PreReadWriteWrappedEndpointHandler<NettyEndpointHandler> {

    private final Logger logger;
    private volatile boolean preWriteDone = false;
    private final NettyEndpointHandler innerEndpointHandler;
    private final InboundHandler inboundHandler = new InboundHandler();

    PreWriteWrappedEndpointHandler(NettyCreatorEndpoint creatorEndpoint,
                                   EndpointConfig endpointConfig,
                                   String perfName,
                                   NetworkAddress remoteAddress) {
        super(creatorEndpoint, endpointConfig, remoteAddress);
        this.logger = creatorEndpoint.getLogger();
        this.innerEndpointHandler = new NettyEndpointHandler(
            logger,
            this, endpointConfig, true,
            String.format("%s(%s)", perfName, remoteAddress.toString()),
            remoteAddress, creatorEndpoint.getDialogHandlerFactoryMap(),
            creatorEndpoint.getEndpointGroup().
            getDialogResourceManager());
    }

    @Override
    public NettyEndpointHandler getInnerEndpointHandler() {
        return innerEndpointHandler;
    }

    public InboundHandler getInboundHandler() {
        return inboundHandler;
    }

    private class InboundHandler extends SimpleChannelInboundHandler<ByteBuf> {
        @Override
        public void handlerAdded(ChannelHandlerContext context) {
            ChannelPipeline pipeline = context.pipeline();
            if (NettyEndpointGroup.logHandlerEnabled()) {
                pipeline.addFirst(new LoggingHandler());
            }
            SSLControl sslControl = endpointConfig.getSSLControl();
            if (sslControl != null) {
                VerifyingSSLHandler handler = NettyUtil.newSSLHandler(
                    getStringID(), sslControl, remoteAddress, true, logger);
                pipeline.addLast(handler.sslHandler());
                pipeline.addLast(handler);
            }
            pipeline.addLast(innerEndpointHandler.decoder());
        }

        @Override
        public void channelActive(ChannelHandlerContext context) {
            final ByteBuf preWriteBytes =
                Unpooled.wrappedBuffer(ProtocolMesg.MAGIC_NUMBER);
            context.writeAndFlush(preWriteBytes).addListener(
                new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) {
                        preWriteDone = true;
                    }
                });
            innerEndpointHandler.onChannelReady();
        }

        @Override
        public void channelRead0(ChannelHandlerContext context, ByteBuf msg)
            throws Exception {
            if (!preWriteDone) {
                throw new IllegalStateException(String.format(
                    "Creator endpoint received bytes " +
                    "before pre-write is done, bytes=%s",
                    ByteBufUtil.hexDump(
                        msg, 0, Math.min(16, msg.capacity()))));
            }
            /*
             * Retain the message once since it will be released in the
             * decoder.
             */
            msg.retain();
            context.fireChannelRead(msg);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context,
                                    Throwable cause) {

            if (logger.isLoggable(Level.INFO)) {
                logger.log(Level.INFO,
                           "{0} got exception, preWriteDone={1}, " +
                           "endpointHandler={2}, cause={3}",
                           new Object[] {
                           getClass().getSimpleName(),
                           preWriteDone,
                           innerEndpointHandler,
                           CommonLoggerUtils.getStackTrace(cause) });
            }
            context.fireExceptionCaught(cause);
        }
    }
}
