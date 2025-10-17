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
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.async.DialogHandlerFactoryMap;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.dialog.PreReadWriteWrappedEndpointHandler;
import oracle.kv.impl.async.dialog.ProtocolMesg;
import oracle.kv.impl.util.CommonLoggerUtils;

class PreWriteWrappedEndpointHandler
    extends PreReadWriteWrappedEndpointHandler<NioEndpointHandler>
    implements ChannelHandler {

    private final Logger logger;
    private final NioChannelExecutor channelExecutor;
    private final NioEndpointHandler innerEndpointHandler;
    private final SocketChannel socketChannel;
    private final ByteBuffer preWriteBytes =
        ByteBuffer.wrap(ProtocolMesg.MAGIC_NUMBER);

    PreWriteWrappedEndpointHandler(NioCreatorEndpoint creatorEndpoint,
                                   EndpointConfig endpointConfig,
                                   String perfName,
                                   NetworkAddress remoteAddress,
                                   NioChannelExecutor channelExecutor,
                                   ExecutorService backupExecutor,
                                   ScheduledExecutorService backupSchedExecutor,
                                   DialogHandlerFactoryMap
                                   dialogHandlerFactories,
                                   SocketChannel socketChannel) {
        super(creatorEndpoint, endpointConfig, remoteAddress);
        this.logger = creatorEndpoint.getLogger();
        this.channelExecutor = channelExecutor;
        this.socketChannel = socketChannel;
        this.innerEndpointHandler = new NioEndpointHandler(
            logger, this, endpointConfig, true,
            String.format("%s(%s)", perfName, remoteAddress.toString()),
            remoteAddress, channelExecutor, backupExecutor, backupSchedExecutor,
            dialogHandlerFactories, socketChannel,
            creatorEndpoint.getEndpointGroup().getDialogResourceManager(),
            creatorEndpoint.getEndpointGroup().getEventTrackersManager());
    }

    @Override
    public NioEndpointHandler getInnerEndpointHandler() {
        return innerEndpointHandler;
    }

    @Override
    public void onConnected() {
        logger.log(Level.FINE, () -> String.format(
            "Connected and pre-writing, channel=%s, handlerid=%s",
            socketChannel,
            getStringID()));
        try {
            if (!onChannelPreWrite()) {
                channelExecutor.setInterest(
                    socketChannel, this,
                    SelectionKey.OP_READ | SelectionKey.OP_WRITE);
            }
        } catch (Throwable t) {
            handleError(t);
        }
    }

    private void handleError(Throwable t) {
        logger.log(
            (t instanceof IOException) ? Level.FINE : Level.INFO,
            () -> String.format(
                "Error while doing pre-write, " +
                "remoteAddress=%s, handlerid=%s: %s",
                remoteAddress, getStringID(),
                ((logger.isLoggable(Level.FINE) &&
                  !(t instanceof IOException)) ?
                 CommonLoggerUtils.getStackTrace(t) : t)));
        cancel(t);
        rethrowUnhandledError(t);
    }

    @Override
    public void onRead() {
        /*
         * It should be an error that the remote send something  before we
         * pre-write.
         */
        throw new IllegalStateException();
    }

    @Override
    public void onWrite() {
        onChannelPreWrite();
    }

    private boolean onChannelPreWrite() {
        try {
            /*
             * Write with SocketChannel instead of DataChannel. This implements
             * the async initiation handshake protocol that the magic number is
             * determined before ssl handshake. This is critical to avoid an
             * extra ssl handshake because if the connection is not async, the
             * connection is hand-off to an RMI socket implementation which
             * will do its own handshake.
             */
            socketChannel.write(preWriteBytes);
            if (preWriteBytes.remaining() != 0) {
                return false;
            }
            channelExecutor.setInterest(
                socketChannel, innerEndpointHandler, SelectionKey.OP_READ);
            innerEndpointHandler.onChannelReady();
            innerEndpointHandler.onRead();
            return true;
        } catch (Throwable t) {
            handleError(t);
            return true;
        }
    }

    @Override
    public void cancel(Throwable t) {
        getInnerEndpointHandler().cancel(t);
    }

    @Override
    public String toString() {
        return String.format(
            "%s(%s)",
            getClass().getSimpleName(),
            innerEndpointHandler.getStringID());
    }
}
