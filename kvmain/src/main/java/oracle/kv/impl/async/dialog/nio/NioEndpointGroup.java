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
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.async.AbstractEndpointGroup;
import oracle.kv.impl.async.AbstractListener;
import oracle.kv.impl.async.AsyncOption;
import oracle.kv.impl.async.DialogHandlerFactoryMap;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.FutureUtils;
import oracle.kv.impl.async.ListenerConfig;
import oracle.kv.impl.async.InetNetworkAddress;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.perf.DialogEndpointGroupPerf;
import oracle.kv.impl.async.perf.NioChannelThreadPoolPerf;
import oracle.kv.impl.fault.AsyncEndpointGroupFaultHandler;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.EventTrackersManager;
import oracle.kv.impl.util.HostPort;


public class NioEndpointGroup extends AbstractEndpointGroup {

    private final NioChannelThreadPool channelThreadPool;

    /* The event trackers manager */
    private final EventTrackersManager trackersManager;

    public NioEndpointGroup(Logger logger,
                            boolean forClientOnly,
                            int nthreads,
                            int maxQuiescentSeconds,
                            int numPermits,
                            AsyncEndpointGroupFaultHandler faultHandler,
                            UncaughtExceptionHandler uncaughtExceptionHandler)
        throws Exception
    {
        super(logger, forClientOnly, numPermits, uncaughtExceptionHandler);
        this.channelThreadPool =
            new NioChannelThreadPool(
                logger, nthreads, maxQuiescentSeconds, faultHandler,
                getBackupSchedExecService());
        this.trackersManager =
            new EventTrackersManager(logger, channelThreadPool.next());
    }

    /* For testing */
    public NioEndpointGroup(Logger logger,
                            int nthreads)
        throws Exception {
        this(logger, false /* forClientOnly */, nthreads,
             ParameterState
             .SN_ENDPOINT_GROUP_MAX_QUIESCENT_SECONDS_VALUE_DEFAULT,
             Integer.MAX_VALUE,
             AsyncEndpointGroupFaultHandler.DEFAULT,
             (t, e) -> {} /* uncaught exception handler */);
    }

    @Override
    public ScheduledExecutorService getSchedExecService() {
        return channelThreadPool.next();
    }

    @Override
    public EventTrackersManager getEventTrackersManager() {
        return trackersManager;
    }

    @Override
    protected NioCreatorEndpoint
        newCreatorEndpoint(String perfName,
                           NetworkAddress remoteAddress,
                           NetworkAddress localAddress,
                           EndpointConfig endpointConfig) {

        return new NioCreatorEndpoint(
            this, channelThreadPool, perfName,
            remoteAddress, localAddress, endpointConfig);
    }

    @Override
    protected NioListener newListener(AbstractEndpointGroup endpointGroup,
                                      ListenerConfig listenerConfig,
                                      DialogHandlerFactoryMap
                                      dialogHandlerFactories) {

        return new NioListener(
                endpointGroup, listenerConfig, dialogHandlerFactories);
    }

    @Override
    protected void shutdownInternal(boolean force) {
        channelThreadPool.shutdown(force);
    }

    class NioListener extends AbstractListener {

        /*
         * Assigned inside the parent listening channel lock. Volatile for
         * read.
         */
        private volatile ServerSocketChannel listeningChannel = null;
        /* Assigned inside synchronization block. Volatile for read.  */
        private volatile NioChannelExecutor channelExecutor = null;
        private final NioChannelAccepter channelAccepter =
            new NioChannelAccepter();
        /*
         * A thread-local to prevent onAccept recursion. The recursion occurs
         * when a new endpoint assigning to a channel executor is rejected and
         * being retried during which clean-up occurs and triggers an onAccept.
         */
        private final ThreadLocal<Boolean> insideOnAccept =
            ThreadLocal.withInitial(() -> false);

        /*
         * A clean-up task to deal with a live-lock issue caused by the server
         * backlog and connection timeout.
         *
         * We de-register the server channel when there are too many
         * connections. Even if de-registered, there are still client endpoints
         * establishing connections which are queued up in the server backlog.
         * These connections may be timed-out before the server channel is
         * registered again. If these connections are later accepted but are
         * already timed-out at the client side, the server side may still need
         * certain amount of time to discover the client is disconnected during
         * which time newly queued-up connections may be timed out again in the
         * backlog. Hence resulting in a live-lock situation. We allieviate
         * this issue by periodically clean up the backlog if the server
         * channel is de-registered.
         */
        private final Runnable clearBacklogTask = new Runnable() {
            @Override
            public void run() {
                final ServerSocketChannel serverChannel = listeningChannel;
                if (serverChannel == null) {
                    return;
                }
                while (true) {
                    try {
                        final SocketChannel clientChannel =
                            serverChannel.accept();
                        if (clientChannel == null) {
                            break;
                        }
                        clientChannel.close();
                    } catch (IOException e) {
                        break;
                    }
                }
            }
        };
        /* Accessed by synchronized on clearBacklogTask */
        private Future<?> clearBacklogFuture = null;

        NioListener(AbstractEndpointGroup endpointGroup,
                    ListenerConfig listenerConfig,
                    DialogHandlerFactoryMap
                    dialogHandlerFactories) {
            super(endpointGroup, listenerConfig, dialogHandlerFactories);
        }

        @Override
        public HostPort getLocalAddressInternal() {
            ensureListeningChannelLockHeld();
            final ServerSocketChannel ch = listeningChannel;
            if (ch == null) {
                /*
                 * This method will not be called before the listening channel
                 * is created, so if the channel is null, then this means we are
                 * shutting down. Returns the any local address.
                 */
                return HostPort.convert(
                    InetNetworkAddress.ANY_LOCAL);
            }
            return NioUtil.getLocalAddress(listeningChannel);
        }

        /**
         * Creates the listening channel if not existing yet.
         *
         * The method is called inside a synchronization block of the parent
         * endpoint group.
         *
         * This is a blocking method as it calls NioUtil#listen.
         */
        @Override
        protected void createChannelInternal() throws IOException {
            ensureListeningChannelLockHeld();
            if (listeningChannel == null) {
                listeningChannel = NioUtil.listen(listenerConfig);
                try {
                    listeningChannel.configureBlocking(false);
                    channelExecutor = channelThreadPool.next();
                    channelExecutor.registerAcceptInterest(
                        listeningChannel, channelAccepter);
                } catch (IOException e) {
                    NetworkAddress.closeServerSocketChannel(listeningChannel);
                    /*
                     * Something wrong with the channel so that we cannot
                     * register. Wake up the executor to be safe.
                     */
                    channelExecutor.wakeup();
                    listeningChannel = null;
                    throw e;
                }
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
            synchronized(clearBacklogTask) {
                if (clearBacklogFuture != null) {
                    clearBacklogFuture.cancel(false);
                }
            }
            try {
                NetworkAddress.closeServerSocketChannel(listeningChannel);
                channelExecutor.wakeup();
            } catch (IOException e) {
                getLogger().log(Level.INFO,
                        "Error closing server channel: {0}", e);
            }
            listeningChannel = null;
            channelExecutor = null;
        }

        @Override
        public String toString() {
            return String.format(
                    "NioListener[listeningChannel=%s]", listeningChannel);
        }

        @Override
        protected boolean tryAccept() throws IOException {
            if (insideOnAccept.get()) {
                return false;
            }
            final ServerSocketChannel serverChannel = listeningChannel;
            if (serverChannel == null) {
                return false;
            }
            final SocketChannel clientChannel = serverChannel.accept();
            if (clientChannel == null) {
                return false;
            }
            channelAccepter.onAccept(clientChannel);
            return true;
        }

        @Override
        protected void registerAccept() throws IOException {
            synchronized(clearBacklogTask) {
                if (clearBacklogFuture != null) {
                    clearBacklogFuture.cancel(false);
                    clearBacklogFuture = null;
                }
            }
            final ServerSocketChannel ch = listeningChannel;
            if (ch != null) {
                channelExecutor.registerAcceptInterest(ch, channelAccepter);
            }
        }

        @Override
        protected void deregisterAccept() throws IOException {
            final NioChannelExecutor executor = channelExecutor;
            if (executor == null) {
                return;
            }
            final ServerSocketChannel ch = listeningChannel;
            if (ch != null) {
                executor.deregister(ch);
            }
            synchronized(clearBacklogTask) {
                final int interval =
                    getListenerConfig().
                    getOption(AsyncOption.DLG_CLEAR_BACKLOG_INTERVAL);
                clearBacklogFuture =
                    executor.scheduleWithFixedDelay(
                        clearBacklogTask, interval, interval,
                        TimeUnit.MILLISECONDS);
            }
        }

        /**
         * Accepter for the listener.
         */
        class NioChannelAccepter implements ChannelAccepter {

            @Override
            public void onAccept(final SocketChannel socketChannel) {

                insideOnAccept.set(true);
                try {
                    onAcceptInternal(socketChannel);
                } catch (IOException e) {
                    /*
                     * Cannot create a new endpoint for the channel. This is
                     * expected since the client side can close the channel at
                     * any time. Simply close the socket channel.
                     */
                    try {
                        socketChannel.close();
                    } catch (Throwable t) {
                        getLogger().log(Level.INFO, () -> String.format(
                            "Error closing channel after IO Exception: %s", t));
                    }
                } catch (Throwable t) {
                    /*
                     * Other kind of exceptions are fatal, we need to propagate
                     * the exception down to the channel executor which will
                     * restart the process. Call shutdown just to be clean.
                     */
                    cancel(t);
                    rethrowUnhandledError(t);
                } finally {
                    insideOnAccept.set(false);
                }
            }

            private void onAcceptInternal(final SocketChannel socketChannel)
                throws IOException {

                getLogger().log(Level.FINEST, () -> String.format(
                    "%s accepting a connection: %s",
                    getClass().getSimpleName(), socketChannel));

                NioUtil.configureSocketChannel(socketChannel, endpointConfig);
                final NetworkAddress remoteAddress =
                    NioUtil.getRemoteAddressNow(socketChannel);
                try {
                    startEndpoint(remoteAddress, socketChannel);
                } catch (Throwable t) {
                    handleAcceptException(t, socketChannel);
                }
            }

            private void startEndpoint(NetworkAddress remoteAddress,
                                       SocketChannel socketChannel)
            {
                final Logger logger = getLogger();
                /*
                 * Creates the endpoint handler and assign it to an executor.
                 * Retries if the executor happens to be shutting itself down.
                 *
                 * TODO: there might be a common pattern here, consider move the
                 * logic of dealing with RejectedExecutionException to the
                 * NioChannelThreadPool for code sharing.
                 */
                while (true) {
                    final NioChannelExecutor executor =
                        channelThreadPool.next();
                    try {
                        startEndpointOnce(executor, remoteAddress, socketChannel);
                        break;
                    } catch (RejectedExecutionException e) {
                        if (channelThreadPool.isShutdown()) {
                            /*
                             * Shutting down, throw the exception so that the
                             * socket channel can be closed.
                             */
                            logger.log(Level.FINE, () -> String.format(
                                "%s cannot accept a connection " +
                                "due to shutdown: %s",
                                getClass().getSimpleName(),
                                socketChannel));
                            throw e;
                        }
                        if (!executor.isShutdownOrAfter()) {
                            logger.log(Level.WARNING, () -> String.format(
                                "Executor rejects execution " +
                                "but is not shut down\n" +
                                "%s\nerror=%s\n",
                                executor,
                                CommonLoggerUtils.getStackTrace(e)));
                            throw e;
                        }
                        /*
                         * If the thread pool is not shut down, then it so
                         * happens that the executor is shutting down itself.
                         * Just try another executor.
                         */
                        logger.log(Level.FINE, () -> String.format(
                            "%s cannot accept a connection " +
                            "due to executor %s being shutdown: %s",
                            getClass().getSimpleName(), executor.getId(),
                            socketChannel));
                        continue;
                    }
                }
            }

            private void startEndpointOnce(NioChannelExecutor executor,
                                           NetworkAddress remoteAddress,
                                           SocketChannel socketChannel)
            {
                final NioResponderEndpoint endpoint =
                    new NioResponderEndpoint(
                        NioEndpointGroup.this, remoteAddress,
                        listenerConfig, NioListener.this,
                        endpointConfig, executor, socketChannel);
                cacheResponderEndpoint(endpoint);
                acceptResponderEndpoint(endpoint);
                try {
                    executor.execute(
                        () -> endpoint.getHandler().onConnected());
                    getLogger().log(
                        Level.FINEST,
                        () ->
                        String.format(
                            "Scheduled onConnected for "
                            + "handler=%s, channel=%s",
                            endpoint.getHandler(),
                            socketChannel));
                } catch (RejectedExecutionException e) {
                    /*
                     * Shuts down the endpoint which will shuts down the
                     * handler and clears the references to this endpoint in
                     * the endpoint group.
                     */
                    endpoint.shutdown(
                        String.format(
                            "Rejected when scheduling " +
                            "handler onConnected: %s", e),
                        true);
                    throw e;
                } catch (Throwable t) {
                    final String msg = String.format(
                        "Unexpected exception " +
                        "when start responder endpoint: %s", t);
                    endpoint.shutdown(msg, true);
                    throw new IllegalStateException(msg, t);
                }
            }

            /**
             * Handles the asynchronous execution of configure and schedule the
             * new endpoint.
             *
             * An exception may be thrown from
             * NetworkAddress#resolveSocketAddress which could be IOException,
             * IllegalArgumentException (if address type mismatch) or some kind
             * of undocumented exception during IP-hostname lookup. The
             * IOException thrown there is handled immediately to use the
             * ANY_LOCAL_ADDRESS.
             *
             * Exceptions are not expected to be thrown from startEndpoint
             * call, which throws IllegalStateException when it happens.
             *
             * For now simply close the socketChannel and log. We may want to
             * throw the exception up and restart the process for unexpected
             * exceptions when the uncaught exception handler of the executing
             * thread support it.
             */
            private Void handleAcceptException(Throwable t,
                                               SocketChannel socketChannel) {
                if (t == null) {
                    return null;
                }
                final Throwable unwrapped = FutureUtils.unwrapException(t);
                getRateLimitingLogger().log(
                    "handleAcceptException",
                    Level.INFO, unwrapped,
                    () -> String.format(
                        "Encountered exception during accept "
                        + "and schedule for execution on a new connection "
                        + "(%s): %s", socketChannel, unwrapped));
                try {
                    socketChannel.close();
                } catch (Throwable e) {
                    /* ignore */
                }
                return null;
            }

            @Override
            public void cancel(Throwable t) {
                /* Notify the handlers about the error. */
                onChannelError(t, true /* mark as closed */);
            }
        }

    }

    /**
     * Returns the optional {@link NioChannelThreadPoolPerf}.
     */
    public Optional<NioChannelThreadPoolPerf> obtainChannelThreadPoolPerf(
        String watcherName,
        boolean clear)
    {
        return channelThreadPool.getPerfTracker()
            .obtain(watcherName, clear);
    }

    /**
     * Returns the optional {@link DialogEndpointGroupPerf}.
     */
    public Optional<DialogEndpointGroupPerf> obtainDialogEndpointGroupPerf(
        String watcherName,
        boolean clear)
    {
        return getDialogEndpointGroupPerfTracker()
            .obtain(watcherName, clear);
    }
}
