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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.async.AbstractResponderEndpoint;
import oracle.kv.impl.async.BytesUtil;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.SocketPrepared;
import oracle.kv.impl.async.dialog.PreReadWriteWrappedEndpointHandler;
import oracle.kv.impl.async.dialog.ProtocolMesg;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.RateLimitingLogger;

class PreReadWrappedEndpointHandler
    extends PreReadWriteWrappedEndpointHandler<NioEndpointHandler>
    implements ChannelHandler {

    /** The magic number at the start of an async connection. */
    private static final int ASYNC_MAGIC_NUMBER =
        BytesUtil.bytesToInt(ProtocolMesg.MAGIC_NUMBER, 0);

    /** The magic number at the start of an RMI connection. */
    private static final int RMI_MAGIC_NUMBER = 0x4a524d49;

    /**
     * The magic number at the start of most TLS v1.2 connections. I think the
     * 0 in the 4th byte could be different for longer messages, but I don't
     * think we've seen that in practice.
     */
    private static final int TLS12_MAGIC_NUMBER = 0x16030300;

    private final Logger logger;
    private final RateLimitingLogger<String> rateLimitingLogger;
    private final NioChannelExecutor channelExecutor;
    private final NioEndpointHandler innerEndpointHandler;
    private final SocketChannel socketChannel;
    private final NioEndpointGroup.NioListener listener;
    private final byte[] magicNumber =
        new byte[ProtocolMesg.MAGIC_NUMBER.length];
    private final ByteBuffer magicNumberBuf = ByteBuffer.wrap(magicNumber);

    PreReadWrappedEndpointHandler(AbstractResponderEndpoint responderEndpoint,
                                  EndpointConfig endpointConfig,
                                  NetworkAddress remoteAddress,
                                  NioChannelExecutor channelExecutor,
                                  ScheduledExecutorService backupExecutor,
                                  NioEndpointGroup.NioListener listener,
                                  SocketChannel socketChannel) {
        super(responderEndpoint, endpointConfig, remoteAddress);
        this.logger = responderEndpoint.getLogger();
        this.rateLimitingLogger =
            new RateLimitingLogger<>(10 * 60 * 1000 /* logSamplePeriodMs */,
                                     20 /* maxObjects */,
                                     logger);
        this.channelExecutor = channelExecutor;
        this.socketChannel = socketChannel;
        this.listener = listener;
        this.innerEndpointHandler = new NioEndpointHandler(
            logger, this, endpointConfig, false,
            listener.getLocalAddress().toString(),
            remoteAddress, channelExecutor, backupExecutor,
            listener.getDialogHandlerFactoryMap(),
            socketChannel,
            responderEndpoint.getEndpointGroup().getDialogResourceManager(),
            responderEndpoint.getEndpointGroup().getEventTrackersManager());
    }

    @Override
    public NioEndpointHandler getInnerEndpointHandler() {
        return innerEndpointHandler;
    }

    @Override
    public void onConnected() {
        try {
            if (!onChannelPreRead()) {
                channelExecutor.setReadInterest(socketChannel, this, true);
            }
        } catch (Throwable t) {
            handleError(t);
        }
    }

    private void handleError(Throwable t) {
        logger.log(
            (t instanceof IOException) ? Level.FINE : Level.INFO,
            () -> String.format(
                "Error while doing pre-read, " +
                "remoteAddress=%s, localAddress=%s, handlerid=%s: %s",
                remoteAddress, listener.getLocalAddress(), getStringID(),
                ((logger.isLoggable(Level.FINE) &&
                  !(t instanceof IOException)) ?
                 CommonLoggerUtils.getStackTrace(t) : t)));
        cancel(t);
        rethrowUnhandledError(t);
    }

    @Override
    public void onRead() {
        try {
            onChannelPreRead();
        } catch (Throwable t) {
            handleError(t);
        }
    }

    private boolean onChannelPreRead() throws IOException {
        /*
         * Read with SocketChannel instead of DataChannel. This implements
         * the async initiation handshake protocol that the magic number is
         * determined before ssl handshake. This is critical to avoid an
         * extra ssl handshake because if the connection is not async, the
         * connection is hand-off to an RMI socket implementation which
         * will do its own handshake.
         */
        socketChannel.read(magicNumberBuf);
        if (magicNumberBuf.remaining() != 0) {
            return false;
        }
        logMagicNumber();

        if (!Arrays.equals(magicNumber, ProtocolMesg.MAGIC_NUMBER)) {
            /* Not something for our nio async */
            handleSyncChannel();
            return true;
        }

        if (!listener.getDialogHandlerFactoryMap().hasActiveDialogTypes()) {
            rateLimitingLogger.log(
                "no async service",
                Level.INFO, () -> getNoServiceMessage(true));
            shutdown("No factory for async connection", true);
            return true;
        }

        logger.log(Level.FINEST, () -> String.format(
            "Async endpoint handler enabled for a new connection: %s",
            innerEndpointHandler));

        channelExecutor.setReadInterest(
            socketChannel, innerEndpointHandler, true);
        innerEndpointHandler.onChannelReady();
        innerEndpointHandler.onRead();
        return true;
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

    private void logMagicNumber() {
        final int magicInt = BytesUtil.bytesToInt(magicNumber, 0);
        logger.log(Level.FINEST, () -> String.format(
            "Done pre-reading: magic number=%s%s, " +
            "remoteAddress=%s, localAddress=%s, handlerid=%s",
            BytesUtil.toString(
                magicNumber, 0, magicNumber.length),
            ((magicInt == ASYNC_MAGIC_NUMBER) ?
             " (Async)" :
             (magicInt == RMI_MAGIC_NUMBER) ?
             " (RMI)" :
             (magicInt == TLS12_MAGIC_NUMBER) ?
             " (TLSv1.2)" :
             ""),
            remoteAddress, listener.getLocalAddress(),
            getStringID()));
    }

    private void handleSyncChannel() throws IOException {
        try {
            final SocketPrepared socketPrepared =
                listener.getSocketPrepared();
            if (socketPrepared == null) {
                rateLimitingLogger.log(
                    "no sync service",
                    Level.INFO, () -> getNoServiceMessage(false));
            } else {
                innerEndpointHandler.handedOffToSync();
                channelExecutor.deregister(socketChannel);
                socketChannel.configureBlocking(true);
                /*
                 * Clear buffer so that SocketPrepared#onPrepared will
                 * get a cleared buffer.
                 */
                magicNumberBuf.clear();
                logger.log(Level.FINEST, () -> String.format(
                    "Handing off the connection " +
                    "to the non-async handler: " +
                    "socketPrepared=%s, buf=%s, socket=%s",
                    socketPrepared,
                    BytesUtil.toString(
                        magicNumberBuf,
                        magicNumberBuf.limit()),
                    socketChannel.socket()));
                socketPrepared.onPrepared(
                    magicNumberBuf, socketChannel.socket());
            }
        } finally {
            shutdown("Got non-async connection", true);
        }
    }

    @Override
    public void onWrite() {
        /*
         * We should not be writing before the pre-read is done. After it is
         * done, this handler should not be notified.
         */
        throw new IllegalStateException();
    }

    @Override
    public void cancel(Throwable t) {
        getInnerEndpointHandler().cancel(t);
    }
}
