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

package oracle.kv.impl.async;

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.impl.async.perf.DialogEndpointGroupPerfTracker;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.RateLimitingLogger;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Abstract class for the endpoint group.
 */
public abstract class AbstractEndpointGroup implements EndpointGroup {

    /**
     * The maximum thread size of the back up executor.
     */
    private static final int BACKUP_EXECUTOR_NUM_CORE_THREADS = 1024;

    private final Logger logger;
    private final RateLimitingLogger<String> rateLimitingLogger;
    /**
     * A backup executor standing by for when the nio endpoint group executors
     * are not available or for tasks that might block.
     *
     * <p>The nio endpoint group manages its own thread pool to support all
     * asynchronous execution. Most tasks can be executed in the thread pool.
     * This backup executor is used in cases when the thread pool executors is
     * not available or the tasks might block. The following cases use this
     * backup executor:
     * <ul>
     * <li>The thread pool is getting shutdown with the endpoint group and some
     * clean-up tasks need to be finished in an asynchronous manner.</li>
     * <li>To actively reclaim resources, the thread pool shuts down executors
     * if it is idle for a long period. Race can happen when the executor is
     * being shut down and some tasks appear.</li>
     * <li>Heartbeat check tasks for thread pool executors are executed in the
     * backup executors.</li>
     * <li>DNS look up tasks which are blocking.</li>
     * </ul>
     *
     * <p>Tasks submitted to the backup executor should handle their
     * exceptions. Any exception thrown to the executor thread is handled by
     * the default exception handler which simply logs the issue.
     *
     * TODO: In the future when we provide thread-pool service for each
     * process, this backup executor should be replaced.
     */
    private final ScheduledThreadPoolExecutor backupExecutor;
    /*
     * Creator and responder endpoints. Use ConcurrentHashMap for thread-safety
     * and high concurrency. Entries are removed when the underlying
     * connections are closing to ensure we do not have many endpoint
     * references lying around. The endpoints cannot create new connections
     * once removed so that the upper layer will notice the removal, otherwise,
     * they can still create new connection with the removed endpoints and
     * hence creating too many connections.
     */
    private final
        ConcurrentHashMap<CreatorKey, AbstractCreatorEndpoint> creators =
        new ConcurrentHashMap<>();
    private final
        ConcurrentHashMap<ResponderKey, AbstractResponderEndpoint> responders =
        new ConcurrentHashMap<>();
    /*
     * Listeners and dialog factory maps. All access should be inside a
     * synchronization block of this endpoint group. We expect listen
     * operations are operated with low frequency.
     */
    private final HashMap<ListenerConfig, DialogHandlerFactoryMap>
        listenerDialogHandlerFactoriesMap =
        new HashMap<>();
    private final HashMap<ListenerConfig, AbstractListener> listeners =
        new HashMap<>();

    /**
     * The number of active dialog handler factories across all listeners. If
     * the count is greater than one, the endpoint group leaves a
     * KeepAliveThread running so that the JVM stays alive while async servers
     * are active. Note that RMI has its own keepalive mechanism and also does
     * not necessarily close server sockets even when all servers are closed,
     * so we should not include the RMI/sync server count when managing the
     * keepalive thread. Only access this field while synchronized on this
     * endpoint group.
     */
    private int activeDialogHandlerFactoriesCount;

    /**
     * If non-null, a non-daemon thread that keeps the JVM alive while there
     * are active dialog handler factories. Only access this field while
     * synchronized on this endpoint group.
     */
    private @Nullable KeepAliveThread keepAliveThread = null;

    /*
     * Whether the endpoint group is shut down. Once it is set to true, it will
     * never be false again. Use volatile for thread-safety.
     */
    private volatile boolean isShutdown = false;

    /* Perf tracker */
    private final DialogEndpointGroupPerfTracker perfTracker;

    /* Resource manager */
    private final DialogResourceManager dialogResourceManager;

    protected AbstractEndpointGroup(Logger logger,
                                    boolean forClientOnly,
                                    int numPermits,
                                    UncaughtExceptionHandler exceptionHandler)
    {
        this.logger = logger;
        this.perfTracker =
            new DialogEndpointGroupPerfTracker(logger, forClientOnly);
        this.rateLimitingLogger =
            new RateLimitingLogger<>(60 * 1000 /* logSamplePeriodMs */,
                                     20 /* maxObjects */,
                                     logger);
        this.dialogResourceManager = new DialogResourceManager(
            numPermits, perfTracker.getDialogResourceManagerPerfTracker());
        /*
         * Creates a ScheduledThreadPoolExecutor with
         * BACKUP_EXECUTOR_NUM_CORE_THREADS core threads. Although a subclass of
         * ThreadPoolExecutor, the ScheduledThreadPoolExecutor is a bit special.
         * The executor has unbounded queue and its max thread size cannot be
         * configured. Therefore the executor will create new threads when there
         * are more tasks than the number of threads until the thread count
         * reaches BACKUP_EXECUTOR_NUM_CORE_THREADS (i.e., 1024). After that, if
         * more tasks are scheduled, they are enqueued until OOME happens.
         *
         * We also configure keep-alive time and allow core thread to timeout.
         * This is because the back up executor is only used occasionally for
         * various tasks that should not block the nio channel executor and
         * therefore it seems undesirable to keep a lot of unused the threads
         * around. The javadoc of ScheduledThreadPoolExecutor warns about such
         * configuration. However, I think the reason for those warnings is for
         * timely execution of the tasks: we need to spin out core threads for
         * new tasks if they can be timed out. Since we do not expect back up
         * executor to be used to its full capacity with timing sensitive tasks,
         * such configuration should be OK.
         *
         * TODO: Currently using ScheduledThreadPoolExecutor but it is not
         * satisfactory since its max thread number cannot be configured
         * properly and its queue is unbounded. We want an executor that has a
         * max thread size and bounded queue size with reject policy of
         * restarting the process [KVSTORE-2265].
         */
        this.backupExecutor =
            new ScheduledThreadPoolExecutor(
                BACKUP_EXECUTOR_NUM_CORE_THREADS,
                new KVThreadFactory(
                    String.format("%s.backup", getClass().getSimpleName()),
                    logger)
                {
                    @Override
                    public UncaughtExceptionHandler
                        makeUncaughtExceptionHandler()
                    {
                        return exceptionHandler;
                    }
                });
        backupExecutor.setKeepAliveTime(1, TimeUnit.MINUTES);
        backupExecutor.allowCoreThreadTimeOut(true);
    }

    public Logger getLogger() {
        return logger;
    }

    @Override
    public DialogEndpointGroupPerfTracker getDialogEndpointGroupPerfTracker() {
        return perfTracker;
    }

    @Override
    public DialogResourceManager getDialogResourceManager() {
        return dialogResourceManager;
    }

    public RateLimitingLogger<String> getRateLimitingLogger() {
        return rateLimitingLogger;
    }

    /**
     * Returns a creator endpoint.
     *
     * Gets the endpoint if exists, creates and adds the endpoint otherwise.
     */
    @Override
    public AbstractCreatorEndpoint
        getCreatorEndpoint(String perfName,
                           NetworkAddress remoteAddress,
                           @Nullable NetworkAddress localAddress,
                           EndpointConfig endpointConfig) {

        checkNull("perfName", perfName);
        checkNull("remoteAddress", remoteAddress);
        checkNull("endpointConfig", endpointConfig);
        CreatorKey key = new CreatorKey(remoteAddress, endpointConfig);
        AbstractCreatorEndpoint endpoint = creators.get(key);
        if (endpoint == null) {
            endpoint = newCreatorEndpoint(
                perfName, remoteAddress, localAddress, endpointConfig);
            final AbstractCreatorEndpoint existingEndpoint =
                creators.putIfAbsent(key, endpoint);
            if (existingEndpoint != null) {
                endpoint.shutdown("Concurrent creation of endpoint", true);
                endpoint = existingEndpoint;
            }
        }
        if (isShutdown) {
            endpoint.shutdown("Endpoint group is shutdown", true);
            throw new IllegalStateException(
                    "Endpoint group is already shut down");
        }
        return endpoint;
    }

    /**
     * Removes the creator endpoint from our map.
     */
    public void invalidateCreatorEndpoint(AbstractCreatorEndpoint endpoint) {
        checkNull("endpoint", endpoint);
        CreatorKey key = new CreatorKey(
                endpoint.getRemoteAddress(), endpoint.getEndpointConfig());
        creators.remove(key);
    }

    /**
     * Returns a responder endpoint.
     */
    @Override
    public AbstractResponderEndpoint
        getResponderEndpoint(NetworkAddress remoteAddress,
                             ListenerConfig listenerConfig) {

        checkNull("remoteAddress", remoteAddress);
        checkNull("listenerConfig", listenerConfig);
        ResponderKey key = new ResponderKey(remoteAddress, listenerConfig);
        AbstractResponderEndpoint endpoint = responders.get(key);
        if (endpoint != null) {
            return endpoint;
        }
        return new AbstractResponderEndpoint.NullEndpoint(
                this, remoteAddress, listenerConfig);
    }

    /**
     * Adds the responder endpoint to our map.
     *
     * The method is called when a new connection is accepted.
     */
    public void cacheResponderEndpoint(AbstractResponderEndpoint endpoint) {

        checkNull("endpoint", endpoint);
        final NetworkAddress remoteAddress = endpoint.getRemoteAddress();

        /*
         * Only cache responder endpoints for TCP addresses. Incoming Unix
         * domain socket connections don't have unique remote addresses, so
         * there is no point in caching them because they don't identify the
         * initiator and can't be used to contact it.
         */
        if (!remoteAddress.isInetAddress()) {
            return;
        }

        final ResponderKey key =
            new ResponderKey(remoteAddress, endpoint.getListenerConfig());
        final AbstractResponderEndpoint existingEndpoint =
            responders.put(key, endpoint);
        if (existingEndpoint != null) {
            /*
             * This is possible when the following events happen:
             * (1) a client connecting with port p on connection c1.
             * (2) c1 terminated, the responder handler terminates and should
             * remove the endpoint from the map, but got delayed.
             * (3) the client connecting with the same port p on a new
             * connection c2, trying to cache the endpoint, but sees the old
             * yet-to-be-removed endpoint.
             *
             * Close c1 again just to make sure we close it properly.
             */
            existingEndpoint.shutdown(
                "A rare race occured that " +
                "a connection is reconnected with the same port " +
                "before the previous connection cleans up", true);
        }
    }

    /**
     * Removes the responder endpoint from our map.
     */
    public void invalidateResponderEndpoint(
                    AbstractResponderEndpoint endpoint) {

        checkNull("endpoint", endpoint);
        final NetworkAddress remoteAddress = endpoint.getRemoteAddress();

        /* Only cache TCP addresses */
        if (!remoteAddress.isInetAddress()) {
            return;
        }

        final ResponderKey key =
            new ResponderKey(remoteAddress, endpoint.getListenerConfig());
        /*
         * It is possible that we are doing a delayed removal of an endpoint.
         * See the events described in #cacheResponderEndpoint. Therefore,
         * remove only if it is the endpoint in the map.
         */
        responders.remove(key, endpoint);
    }

    /**
     * Listens for incoming async connections to respond to the specified type
     * of dialogs.
     *
     * This is a blocking method as it calls AbstractListener#newListenHandle.
     */
    @Override
    public synchronized
        ListenHandle listen(ListenerConfig listenerConfig,
                            int dialogType,
                            DialogHandlerFactory handlerFactory)
        throws IOException {

        checkNull("listenerConfig", listenerConfig);
        checkNull("handlerFactory", handlerFactory);
        final AbstractListener listener = getListener(listenerConfig);
        final ListenHandle listenHandle =
            listener.newListenHandle(dialogType, handlerFactory);
        activeDialogHandlerFactoriesCount++;
        if (keepAliveThread == null) {
            keepAliveThread = new KeepAliveThread();
            keepAliveThread.start();
        }
        return listenHandle;
    }

    /**
     * Decrement the activeDialogHandlerFactoriesCount when a listener is
     * shutdown, and shutdown the keep alive thread when the count reaches
     * zero.
     */
    void decrementActiveDialogHandlerFactoriesCount() {
        assert Thread.holdsLock(this);
        if (activeDialogHandlerFactoriesCount == 0) {
            return;
        }
        activeDialogHandlerFactoriesCount--;
        if ((activeDialogHandlerFactoriesCount == 0) &&
            (keepAliveThread != null)) {
            keepAliveThread.shutdown();
            keepAliveThread = null;
        }
    }

    /**
     * Listens for incoming sync connections.
     *
     * The method is synchronized on this class, therefore, it should not be
     * called in a critical path that requires high concurrency.
     *
     * This is a blocking method as it calls AbstractListener#newListenHandle.
     */
    @Override
    public synchronized EndpointGroup.ListenHandle
        listen(ListenerConfig listenerConfig,
               SocketPrepared socketPrepared)
        throws IOException {

        checkNull("listenerConfig", listenerConfig);
        checkNull("socketPrepared", socketPrepared);
        AbstractListener listener = getListener(listenerConfig);
        return listener.newListenHandle(socketPrepared);
    }

    /**
     * Shuts down all endpoints and listening channels in the group.
     */
    @Override
    public void shutdown(boolean force) {
        isShutdown = true;
        /*
         * We want to make sure all creator endpoints are shut down.
         *
         * Since isShutdown is volatile, the above set statement creates a
         * memory barrier. After each put of the endpoints, there is a read on
         * isShutdown. If isShutdown is false, the put will be visible by the
         * following enumeration and the endpoint/listener is shutdown in the
         * enumeration. Otherwise, the endpoint is shut down in the get
         * methods.
         */
        while (!creators.isEmpty()) {
            final Iterator<AbstractCreatorEndpoint> iter =
                creators.values().iterator();
            if (iter.hasNext()) {
                final AbstractCreatorEndpoint endpoint = iter.next();
                iter.remove();
                endpoint.shutdown("Endpoint group is shut down", force);
            }
        }
        /*
         * Synchronization block for listeners operation. We do not need to
         * shut down responder endpoints, they are shut down by the listeners.
         */
        synchronized(this) {
            for (AbstractListener listener :
                    new ArrayList<>(listeners.values())) {
                listener.shutdown(force);
            }
            listeners.clear();

            /*
             * Shutting down all listeners should have shut down the keepalive
             * thread, but just in case
             */
            if (keepAliveThread != null) {
                keepAliveThread.shutdown();
                keepAliveThread = null;
            }
        }

        shutdownInternal(force);
        /*
         * Do not shut down the backupExecutor. Let GC do the job. This is
         * because we want the backupExecutor live a bit longer for clean-up
         * tasks. If we shut it down here, the clean-up tasks might get a
         * RejectedExecutionException.
         */
    }

    @Override
    public boolean getIsShutdown() {
        return isShutdown;
    }

    @Override
    public ScheduledExecutorService getBackupSchedExecService() {
        return backupExecutor;
    }

    /**
     * Removes a listener.
     */
    void removeListener(AbstractListener listener) {
        assert Thread.holdsLock(this);
        listeners.remove(listener.getListenerConfig());
    }

    /**
     * Get current listeners, for testing.
     */
    public Map<ListenerConfig, AbstractListener> getListeners() {
        return listeners;
    }

    /* Abstract methods. */

    /**
     * Creates a creator endpoint.
     */
    protected abstract AbstractCreatorEndpoint newCreatorEndpoint(
        String perfName,
        NetworkAddress remoteAddress,
        @Nullable NetworkAddress localAddress,
        EndpointConfig endpointConfig);

    /**
     * Creates a listener.
     */
    protected abstract AbstractListener newListener(
        AbstractEndpointGroup endpointGroup,
        ListenerConfig listenerConfig,
        DialogHandlerFactoryMap listenerDialogHandlerFactories);

    /**
     * Shuts down the actual implementation and cleans up
     * implementation-dependent resources (e.g., the executor thread pool).
     */
    protected abstract void shutdownInternal(boolean force);

    /* Private implementation methods */

    /**
     * Gets a listener if exists, creates and adds otherwise.
     */
    private AbstractListener getListener(ListenerConfig listenerConfig) {
        assert Thread.holdsLock(this);
        if (isShutdown) {
            throw new IllegalStateException(
                    "Endpoint group is already shut down");
        }
        DialogHandlerFactoryMap listenerDialogHandlerFactories =
            listenerDialogHandlerFactoriesMap.get(listenerConfig);
        if (listenerDialogHandlerFactories == null) {
            listenerDialogHandlerFactories = new DialogHandlerFactoryMap();
        }
        AbstractListener listener = listeners.get(listenerConfig);
        if (listener == null) {
            listener = newListener(
                    this, listenerConfig, listenerDialogHandlerFactories);
            listeners.put(listenerConfig, listener);
        }
        return listener;
    }

    /* Key classes */

    /*
     * Note CreatorKey does not embody localAddress, since we would not use
     * different local addresses to talk to the same remoteAddress endpoint.
     * The KVStoreConfig allows for exactly one localAddr specification for all
     * KV traffic.
     */
    private class CreatorKey {

        private final NetworkAddress remoteAddress;
        private final EndpointConfig endpointConfig;

        public CreatorKey(NetworkAddress remoteAddress,
                          EndpointConfig endpointConfig) {
            this.remoteAddress = remoteAddress;
            this.endpointConfig = endpointConfig;
        }

        /**
         * Compares endpoints based on peer address and channel factory.
         */
        @Override
        public boolean equals(@Nullable Object obj) {
            if (!(obj instanceof CreatorKey)) {
                return false;
            }
            CreatorKey that = (CreatorKey) obj;
            return (this.remoteAddress.equals(that.remoteAddress) &&
                    this.endpointConfig.equals(that.endpointConfig));
        }

        /**
         * Returns hashcode for the endpoint based on peer address and channel
         * factory.
         */
        @Override
        public int hashCode() {
            return 37 * remoteAddress.hashCode() +
                endpointConfig.hashCode();
        }
    }

    private class ResponderKey {

        private final NetworkAddress remoteAddress;
        private final ListenerConfig listenerConfig;

        public ResponderKey(NetworkAddress remoteAddress,
                            ListenerConfig listenerConfig) {
            this.remoteAddress = remoteAddress;
            this.listenerConfig = listenerConfig;
        }

        /**
         * Compares endpoints based on peer address and channel factory.
         */
        @Override
        public boolean equals(@Nullable Object obj) {
            if (!(obj instanceof ResponderKey)) {
                return false;
            }
            ResponderKey that = (ResponderKey) obj;
            return (this.remoteAddress.equals(that.remoteAddress) &&
                    this.listenerConfig.equals(that.listenerConfig));
        }

        /**
         * Returns hashcode for the endpoint based on peer address and channel
         * factory.
         */
        @Override
        public int hashCode() {
            return 37 * remoteAddress.hashCode() +
                listenerConfig.hashCode();
        }
    }

    /**
     * Shuts down all creator endpoint handlers for testing.
     */
    public void shutdownCreatorEndpointHandlers(String detail, boolean force) {
        creators.values().forEach((c) -> c.shutdownHandler(detail, force));
    }

    /**
     * A non-daemon thread that will keep the JVM alive until the shutdown
     * method is called. Used to stay alive while there are open listeners.
     */
    private class KeepAliveThread extends Thread {
        private boolean shutdown;
        KeepAliveThread() {
            super("KV-EndpointGroup-KeepAliveThread");

            /*
             * Make sure this is a non-daemon thread: don't inherit this
             * behavior from the calling thread
             */
            setDaemon(false);
        }
        @Override
        public void run() {
            logger.fine("Starting keep alive thread");
            synchronized (this) {
                while (!shutdown) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                    }
                }
            }

            /*
             * If shutdown is true, we know that all async dialogs have
             * completed, but we don't know that the response has been sent out
             * on the network yet. If the keep alive thread exits too quickly
             * (causing the JVM to exit), that might happen before the response
             * is sent to the caller. I think RMI deals with this issue with
             * it's Distributed Garbage Collector (DGC), which keeps servers
             * alive as long as there are clients with references to them. We
             * don't have that complication in async, so just try waiting a
             * short time in hopes that it is long enough for the client to
             * receive the response before the server JVM shuts down and closes
             * the server side of the socket.
             */
            logger.finest("Start wait before exiting keep alive thread");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
            }
            logger.fine("Exiting keep alive thread");
        }
        synchronized void shutdown() {
            shutdown = true;
            notifyAll();
        }
    }

    /* For testing */

    /**
     * Shuts down the backup executor, waits for termination.
     */
    public boolean awaitBackupExecutorQuiescence(long timeout, TimeUnit unit)
        throws InterruptedException {

        backupExecutor.shutdown();
        return backupExecutor.awaitTermination(timeout, unit);
    }
}
