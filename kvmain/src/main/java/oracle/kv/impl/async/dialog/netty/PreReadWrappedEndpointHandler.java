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
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.async.AbstractResponderEndpoint;
import oracle.kv.impl.async.BytesUtil;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.SocketPrepared;
import oracle.kv.impl.async.dialog.PreReadWriteWrappedEndpointHandler;
import oracle.kv.impl.async.dialog.ProtocolMesg;
import oracle.kv.impl.security.ssl.SSLControl;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.RateLimitingLogger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.logging.LoggingHandler;

/**
 * Pre-reads some bytes from the channel and see if the channel is of async.
 */
class PreReadWrappedEndpointHandler
    extends PreReadWriteWrappedEndpointHandler<NettyEndpointHandler> {

    private final Logger logger;
    private final RateLimitingLogger<String> rateLimitingLogger;
    private final NettyEndpointGroup.NettyListener listener;
    private volatile boolean preReadDone = false;
    private final NettyEndpointHandler innerEndpointHandler;
    private final InboundHandler inboundHandler = new InboundHandler();

    private final byte[] magicNumber =
        new byte[ProtocolMesg.MAGIC_NUMBER.length];
    private final ByteBuffer magicNumberBuf = ByteBuffer.wrap(magicNumber);

    PreReadWrappedEndpointHandler(AbstractResponderEndpoint responderEndpoint,
                                  EndpointConfig endpointConfig,
                                  NetworkAddress remoteAddress,
                                  NettyEndpointGroup.NettyListener listener) {
        super(responderEndpoint, endpointConfig, remoteAddress);
        this.listener = listener;
        this.logger = responderEndpoint.getLogger();
        this.rateLimitingLogger =
            new RateLimitingLogger<>(10 * 60 * 1000 /* logSamplePeriodMs */,
                                     20 /* maxObjects */,
                                     logger);
        this.innerEndpointHandler = new NettyEndpointHandler(
            logger,
            this, endpointConfig, false, listener.getLocalAddress().toString(),
            remoteAddress, listener.getDialogHandlerFactoryMap(),
            responderEndpoint.getEndpointGroup().
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
                    getStringID(), sslControl, remoteAddress, false, logger);
                pipeline.addLast(handler.sslHandler());
                pipeline.addLast(handler);
            }
            pipeline.addLast(innerEndpointHandler.decoder());
        }

        @Override
        public void channelRead0(ChannelHandlerContext context, ByteBuf msg)
            throws Exception {

            if (!preReadDone) {
                msg.readBytes(magicNumberBuf);
                if (magicNumberBuf.remaining() != 0) {
                    return;
                }

                preReadDone = true;
                logger.log(Level.FINEST, () -> String.format(
                    "Got connection with magic number: %s",
                    BytesUtil.toString(magicNumber, 0, magicNumber.length)));

                if (!Arrays.equals(magicNumber, ProtocolMesg.MAGIC_NUMBER)) {
                    handleSyncChannel(context, msg);
                    return;
                }

                if (!listener.getDialogHandlerFactoryMap().hasActiveDialogTypes()) {
                    rateLimitingLogger.log(
                        "no async service",
                        Level.INFO, () -> getNoServiceMessage(true));
                    shutdown("No factory for async connection", true);
                    return;
                }

                innerEndpointHandler.onChannelReady();
            }

            /*
             * Retain the message once since it will be released in the
             * decoder.
             */
            msg.retain();
            context.fireChannelRead(msg);
        }

        private void handleSyncChannel(ChannelHandlerContext context,
                                       ByteBuf msg) throws Exception {
            try {
                /* Not something for our nio async */
                final ByteBuffer preReadBytes = ByteBuffer.allocate(
                    magicNumber.length + msg.readableBytes());
                magicNumberBuf.clear();
                preReadBytes.put(magicNumberBuf);
                msg.getBytes(msg.readerIndex(), preReadBytes);
                preReadBytes.clear();

                innerEndpointHandler.handedOffToSync();
                context.deregister().addListener(
                    new SocketInitializer(preReadBytes, context));
            } finally {
                shutdown("Got non-async connection", true);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context,
                                    Throwable cause) {

            logger.log(
                (cause instanceof IOException) ? Level.FINE : Level.INFO,
                () -> String.format(
                    "%s got exception, preReadDone=%s, " +
                    "endpointHandler=%s, cause=%s",
                    getClass().getSimpleName(), preReadDone,
                    innerEndpointHandler,
                    ((logger.isLoggable(Level.FINE) &&
                      !(cause instanceof IOException)) ?
                     CommonLoggerUtils.getStackTrace(cause) : cause)));
            context.fireExceptionCaught(cause);
        }

    }

    private String getNoServiceMessage(boolean async) {
        final String service = async ? "async" : "RMI";
        return String.format(
            "Connection failed to server address %s "
            + "from client address %s: "
            + "no %s services were found. "
            + "Services may have been unavailable "
            + "because the node was shutting down "
            + "or because of some other problem.",
            listener.getLocalAddress(), remoteAddress,
            service);
    }

    private class SocketInitializer implements ChannelFutureListener {

        private final ByteBuffer preReadBytes;
        private final ChannelHandlerContext context;

        SocketInitializer(ByteBuffer preReadBytes,
                          ChannelHandlerContext context) {
            this.preReadBytes = preReadBytes;
            this.context = context;
        }

        @Override
        public void operationComplete(ChannelFuture future) {

            if (!future.isSuccess()) {
                logger.log(Level.FINE, () -> String.format(
                    "Error deregistering socket: %s", future.cause()));
                future.channel().close();
                return;
            }

            final SocketPrepared socketPrepared = listener.getSocketPrepared();
            if (socketPrepared == null) {
                rateLimitingLogger.log(
                    "no sync service",
                    Level.INFO, () -> getNoServiceMessage(false));
                return;
            }
            SocketChannel socketChannel =
                NettyUtil.getSocketChannel(context.channel());
            try {
                socketChannel.configureBlocking(true);
                logger.log(Level.FINEST, () -> String.format(
                    "Handing off the connection " +
                    "to the non-async handler: handler=%s, buf=%s, socket=%s",
                    socketPrepared,
                    BytesUtil.toString(
                        preReadBytes,
                        preReadBytes.limit()),
                    socketChannel.socket() ));
                socketPrepared.onPrepared(
                    preReadBytes, socketChannel.socket());
            } catch (IOException e) {
                logger.log(Level.INFO, () -> String.format(
                    "Error handing off the channel " +
                    "to non-async handler: %s", e));
            }
        }
    }
}
