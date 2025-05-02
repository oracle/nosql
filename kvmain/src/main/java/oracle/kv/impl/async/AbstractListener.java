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

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.util.HostPort;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Abstract class for an endpoint group listener.
 */
public abstract class AbstractListener implements EndpointListener {

    /* The parent endpoint group. */
    private final AbstractEndpointGroup endpointGroup;
    /* The configuration for creating the listening channel. */
    protected final ListenerConfig listenerConfig;
    /* The configuration for creating the channels of accepted connections. */
    protected final EndpointConfig endpointConfig;
    /*
     * Indicates if the listener has created a channel and bound to a local
     * address. Access inside the listeningChannelLock.
     */
    private boolean listeningChannelCreated = false;
    /* A lock for createChannel and getLocalAddress */
    private final ReentrantLock listeningChannelLock = new ReentrantLock();
    /*
     * The set of responder endpoints that are accepted by this listener. It is
     * used for shut down responder endpoints when the listener is shut down.
     * Use concurrent hash map for thread-safety and high concurrency.
     */
    private final Set<AbstractResponderEndpoint> acceptedEndpoints =
        Collections.newSetFromMap(
                new ConcurrentHashMap<AbstractResponderEndpoint, Boolean>());
    /*
     * The dialog types enabled for responding. Passed down from EndpointGroup.
     * All listeners of the same ListenerConfig share this map. Endpoint
     * handlers of this listener (e.g., future handlers of newly accepted
     * responder endpoints) share this map as well. It should be a concurrent
     * hash map for thread-safety and high concurrency.
     */
    private final DialogHandlerFactoryMap dialogHandlerFactories;
    /*
     * The reference to socket prepared object. Endpoint handlers of this
     * listener share this reference. Use atomic reference for thread-safety.
     */
    private final AtomicReference<SocketPrepared> socketPreparedRef =
        new AtomicReference<SocketPrepared>(null);
    /*
     * Indicates whether the listener is shut down. The listener is shut down
     * because it is not listening. Once set to true, it will never be false
     * again. Use volatile for write visibility among threads.
     */
    private volatile boolean isShutdown = false;

    /* The allowed number of connection for throttling */
    private volatile int acceptMaxActiveConnections = Integer.MAX_VALUE;
    private final AtomicInteger numConnections = new AtomicInteger(0);
    private final Object registerLock = new Object();
    private volatile boolean registeredAccept = true;


    protected AbstractListener(AbstractEndpointGroup endpointGroup,
                               ListenerConfig listenerConfig,
                               DialogHandlerFactoryMap
                               dialogHandlerFactories) {
        this.endpointGroup = endpointGroup;
        this.listenerConfig = listenerConfig;
        this.dialogHandlerFactories = dialogHandlerFactories;
        this.endpointConfig = listenerConfig.getEndpointConfig();
        this.acceptMaxActiveConnections =
            listenerConfig.getOption(AsyncOption.DLG_ACCEPT_MAX_ACTIVE_CONNS);
    }

    /**
     * Returns the listener config.
     */
    protected ListenerConfig getListenerConfig() {
        return listenerConfig;
    }

    /**
     * Starts listening for an async connection and returns a handle.
     *
     * This is a blocking method as it calls createChannel.
     */
    AsyncHandle newListenHandle(int dialogType,
                                DialogHandlerFactory handlerFactory)
        throws IOException {

        assert Thread.holdsLock(endpointGroup);

        if (dialogHandlerFactories.isActive(dialogType)) {
            throw new IllegalStateException(
                    String.format(
                        "Already start listening for dialogType=%s with %s",
                        dialogType, dialogHandlerFactories.get(dialogType)));
        }

        /* Add the factory first to avoid a race of a concurrent connection */
        dialogHandlerFactories.put(dialogType, handlerFactory);
        createChannel();
        return new AsyncHandle(dialogType);
    }

    /**
     * Starts listening for a sync connection and returns a handle.
     *
     * This is a blocking method as it calls createChannel.
     */
    SyncHandle newListenHandle(SocketPrepared socketPrepared)
        throws IOException{

        assert Thread.holdsLock(endpointGroup);

        SocketPrepared existing = socketPreparedRef.get();
        if (existing != null) {
            throw new IllegalStateException(
                    String.format("Already start listening with %s",
                        existing));
        }

        /* Add the callback first to avoid a race of a concurrent connection */
        socketPreparedRef.set(socketPrepared);
        createChannel();
        return new SyncHandle(socketPrepared);
    }

    @Override
    public void shutdown(boolean force) {
        assert Thread.holdsLock(endpointGroup);

        final Logger logger = endpointGroup.getLogger();
        logger.info(
            () -> String.format("Listener shutting down: localAddress=%s",
                                getLocalAddress()));

        isShutdown = true;
        endpointGroup.removeListener(this);
        closeChannel();
        shutdownAcceptedEndpoints(force);
    }

    private void shutdownAcceptedEndpoints(boolean force) {
        assert Thread.holdsLock(endpointGroup);

        /*
         * Since the method is called inside the synchronization block of
         * the parent endpoint group, no new listener can be created (which
         * is also inside the synchronization block). Furthermore, we have
         * closed our listening channel, therefore no new connections will
         * be accepted. That is, no new endpoint will be added to the set
         * during the following iteration. On the other hand, endpoints are
         * removed either because of iter.remove or the endpoint fails by
         * itself.
         */
        while (!acceptedEndpoints.isEmpty()) {
            Iterator<AbstractResponderEndpoint> iter =
                acceptedEndpoints.iterator();
            while (iter.hasNext()) {
                AbstractResponderEndpoint endpoint = iter.next();
                /*
                 * Do iter.remove first, since endpoint.shutdown will try to
                 * remove itself.
                 */
                iter.remove();
                endpoint.shutdown("Stop listening", force);
            }
        }
    }

    @Override
    public boolean isShutdown() {
        return isShutdown;
    }

    @Override
    public DialogHandlerFactoryMap getDialogHandlerFactoryMap() {
        return dialogHandlerFactories;
    }

    /**
     * Ensures the listeningChannelLock is locked.
     */
    protected void ensureListeningChannelLockHeld() {
        if (!listeningChannelLock.isHeldByCurrentThread()) {
            throw new IllegalStateException("listening channel lock not held");
        }
    }

    /**
     * Creates the listening channel.
     *
     * This is a blocking method as it calls createChannelInternal.
     */
    public void createChannel() throws IOException {
        listeningChannelLock.lock();
        try {
            createChannelInternal();
            listeningChannelCreated = true;
        } finally {
            listeningChannelLock.unlock();
        }
    }

    @Override
    public HostPort getLocalAddress() {
        listeningChannelLock.lock();
        try {
            if (!listeningChannelCreated) {
                throw new IllegalStateException(
                    "Listening channel not created yet. " +
                    "This method should not be called before createChannel.");
            }
            return getLocalAddressInternal();
        } finally {
            listeningChannelLock.unlock();
        }
    }

    @Override
    public @Nullable SocketPrepared getSocketPrepared() {
        return socketPreparedRef.get();
    }

    @Override
    public void onChannelError(Throwable t, boolean channelClosed) {

        /*
         * Acquire endpoint group lock for thread-safety with adding new
         * handlers.
         */
        synchronized(endpointGroup) {
            shutdown(true /* force */);

            for (DialogHandlerFactory factory :
                    dialogHandlerFactories.getActiveFactories()) {
                factory.onChannelError(listenerConfig, t, channelClosed);
            }
            final SocketPrepared socketPrepared =
                socketPreparedRef.get();
            if (socketPrepared != null) {
                socketPrepared.onChannelError(
                        listenerConfig, t, channelClosed);
            }
        }
    }

    @Override
    public void acceptResponderEndpoint(AbstractResponderEndpoint endpoint) {
        acceptedEndpoints.add(endpoint);
        final int nconn = numConnections.incrementAndGet();
        if (nconn < acceptMaxActiveConnections) {
            return;
        }
        if (nconn > acceptMaxActiveConnections) {
            /*
             * Shuts down this endpoint forcefully. We are here because
             * multilpe connections has been accepted before we can
             * de-register. And it is possible there are many of them. To
             * prevent these endpoints from taking too much resource, just shut
             * them down immediately. The shutdown method will call
             * removeResponderEndpoint which will remove the endpoint and
             * decrement the counter, which will also do register operation if
             * needed.
             */
            endpoint.shutdown(
                String.format(
                    "Exceeded dialog accept max active connections, " +
                    "total number of connections: %d", nconn),
                true /* force */);
        } else {
            doRegister();
        }
    }

    /**
     * Re-register or de-register based on the number of connections.
     */
    private void doRegister() {
        synchronized(registerLock) {
            final int nconn = numConnections.get();
            final boolean actual = registeredAccept;
            final boolean expected =
                nconn < acceptMaxActiveConnections;
            if (actual != expected) {
                try {
                    if (expected) {
                        registerAccept();
                        registeredAccept = true;
                    } else {
                        deregisterAccept();
                        endpointGroup.getLogger().finest(
                            "Deregistered accept");
                        registeredAccept = false;
                    }
                } catch (IOException ioe) {
                    endpointGroup.getRateLimitingLogger().
                        log("listener accept register operation error",
                            Level.INFO, ioe,
                            () ->
                            String.format(
                                "Error do register accept operation for %s: " +
                                "currently %s, expect to %s, nconn=%s" +
                                getLocalAddress(),
                                actual ? "registered" : "deregistered",
                                expected ? "register" : "deregister",
                                nconn));
                }
            }
        }
    }

    @Override
    public void removeResponderEndpoint(AbstractResponderEndpoint endpoint) {
        acceptedEndpoints.remove(endpoint);
        final int nconn = numConnections.decrementAndGet();
        if (nconn >= acceptMaxActiveConnections) {
            return;
        }
        try {
            if (tryAccept()) {
                return;
            }
        } catch (IOException ioe) {
            endpointGroup.getRateLimitingLogger().
                log("listener accept error",
                    Level.INFO, ioe,
                    () ->
                    String.format("Error accept when connection re-enabled " +
                                  "for listener on %s: %s",
                                  getLocalAddress(), ioe));
        }
        doRegister();
    }

    /**
     * Abstract handle class.
     */
    abstract class Handle implements EndpointGroup.ListenHandle {

        private final HostPort localAddress;

        Handle() {
            this.localAddress =
                AbstractListener.this.getLocalAddress();
        }

        @Override
        public ListenerConfig getListenerConfig() {
            return listenerConfig;
        }

        @Override
        public HostPort getLocalAddress() {
            return localAddress;
        }

        @Override
        public void setAcceptMaxActiveConnections(int nconn) {
            acceptMaxActiveConnections = nconn;
        }

        /**
         * Shuts down the listener and shuts down accepted responder endpoint
         * handlers if all handles have been shut down.
         */
        protected void shutdownIfInactive(boolean force) {
            assert Thread.holdsLock(endpointGroup);

            if ((dialogHandlerFactories.hasActiveDialogTypes())
                || (socketPreparedRef.get() != null)) {
                return;
            }

            AbstractListener.this.shutdown(force);
            shutdownAcceptedEndpoints(force);
        }
    }

    /**
     * An async handle.
     */
    class AsyncHandle extends Handle {

        private final int dialogType;

        AsyncHandle(int dialogType) {
            this.dialogType = dialogType;
        }

        @Override
        public void shutdown(boolean force) {
            synchronized(endpointGroup) {
                dialogHandlerFactories.remove(dialogType);
                endpointGroup.decrementActiveDialogHandlerFactoriesCount();
                shutdownIfInactive(force);
            }
        }

        @Override
        public String toString() {
            return String.format(
                    "AsyncHandle[" +
                    " listener=%s" +
                    " dialogType=%d" +
                    " dialogHandlerFactory=%s ]",
                    AbstractListener.this,
                    dialogType,
                    dialogHandlerFactories.get(dialogType));
        }
    }

    /**
     * A sync handle.
     */
    class SyncHandle extends Handle {

        private final SocketPrepared socketPrepared;

        SyncHandle(SocketPrepared socketPrepared) {
            this.socketPrepared = socketPrepared;
        }

        @Override
        public void shutdown(boolean force) {
            synchronized(endpointGroup) {
                if (socketPreparedRef.compareAndSet(socketPrepared, null)) {
                    /*
                     * This can happen when the shutdown method is called
                     * multiple times and after the first shut down a new
                     * SocketPrepared is added. There is nothing we need to do
                     * in this case.
                     */
                }
                shutdownIfInactive(force);
            }
        }

        @Override
        public String toString() {
            return String.format(
                    "SyncHandle[" +
                    " listener=%s" +
                    " socketPrepared=%s ]",
                    AbstractListener.this,
                    socketPrepared);
        }
    }

    /**
     * Returns the local address.
     */
    protected abstract HostPort getLocalAddressInternal();

    /**
     * Creates the listening channel if not existing yet.
     */
    protected abstract void createChannelInternal() throws IOException;

    /**
     * Closes the created listening channel.
     */
    protected abstract void closeChannel();

    /**
     * Tries to accept a channel if there is any.
     *
     * @return {@code true} if accepted a channel
     */
    protected abstract boolean tryAccept() throws IOException;

    /**
     * Registers for accept.
     */
    protected abstract void registerAccept() throws IOException;

    /**
     * Deregisters for accept.
     */
    protected abstract void deregisterAccept() throws IOException;

}
