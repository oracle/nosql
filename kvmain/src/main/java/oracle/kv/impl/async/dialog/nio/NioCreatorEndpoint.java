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

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Logger;

import oracle.kv.impl.async.AbstractCreatorEndpoint;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.EndpointHandler;
import oracle.kv.impl.async.NetworkAddress;

/**
 * Nio creator endpoint.
 */
class NioCreatorEndpoint extends AbstractCreatorEndpoint {

    private final NioEndpointGroup endpointGroup;
    private final NioChannelThreadPool channelThreadPool;

    NioCreatorEndpoint(NioEndpointGroup endpointGroup,
                       NioChannelThreadPool channelThreadPool,
                       String perfName,
                       NetworkAddress remoteAddress,
                       NetworkAddress localAddress,
                       EndpointConfig endpointConfig) {
        super(endpointGroup, perfName,
              remoteAddress, localAddress, endpointConfig);
        this.endpointGroup = endpointGroup;
        this.channelThreadPool = channelThreadPool;
    }

    @Override
    protected CompletableFuture<EndpointHandler> newEndpointHandler() {
        final EndpointConfig config = getEndpointConfig();
        return NioUtil.getSocketChannel(remoteAddress, localAddress, config)
            .thenApply(checked((ch) -> setupHandler(ch)));
    }

    /*
     * Sets up the endpoint handler and assign it to an executor. Retries if
     * the executor happens to be shutting itself down.
     *
     * TODO: there might be a common pattern here, consider move the logic of
     * dealing with RejectedExecutionException to the NioChannelThreadPool for
     * code sharing.
     */
    private EndpointHandler setupHandler(SocketChannel socketChannel)
        throws IOException
    {
        while (true) {
            final NioChannelExecutor executor = channelThreadPool.next();
            final PreWriteWrappedEndpointHandler handler =
                new PreWriteWrappedEndpointHandler(
                    this,
                    endpointConfig,
                    perfName,
                    remoteAddress,
                    executor,
                    endpointGroup.getBackupSchedExecService(),
                    getDialogHandlerFactoryMap(),
                    socketChannel);
            try {
                if (socketChannel.isConnected()) {
                    /* The socket channel may be established immediately. */
                    executor.submit(() -> handler.onConnected());
                } else {
                    executor.registerConnectInterest(socketChannel, handler);
                }
                return handler;
            } catch (RejectedExecutionException e) {
                if (!channelThreadPool.isShutdown()) {
                    /*
                     * It is possible the executor is shutting down, just try
                     * again.
                     */
                    continue;
                }
                handler.shutdown(
                    "Error initializing endpoint handler "
                    + "due to channelThreadPool shutdown",
                    true);
                throw e;
            } catch (Throwable t) {
                handler.shutdown(
                    String.format(
                        "Error initializing endpoint handler: %s", t),
                    true);
                throw t;
            }
        }
    }

    Logger getLogger() {
        return endpointGroup.getLogger();
    }
}

