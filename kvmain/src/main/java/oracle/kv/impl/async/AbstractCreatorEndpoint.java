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
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import oracle.kv.impl.async.dialog.ChannelDescription.NoChannelDescription;
import oracle.kv.impl.async.exception.ConnectionEndpointShutdownException;
import oracle.kv.impl.async.exception.ConnectionException;
import oracle.kv.impl.async.exception.ConnectionUnknownException;
import oracle.kv.impl.async.exception.InitialConnectIOException;
import oracle.kv.impl.async.perf.DialogEndpointGroupPerfTracker;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Abstract class for a creator endpoint.
 */
public abstract class AbstractCreatorEndpoint
    implements CreatorEndpoint, EndpointHandlerManager {

    /**
     * If set, called on entrance to startDialog with the dialog type as an
     * argument.
     */
    public static volatile @Nullable TestHook<Integer> startDialogHook;
    public static volatile @Nullable TestHook<Void> connectHook;

    private final AbstractEndpointGroup endpointGroup;
    protected final String perfName;
    protected final NetworkAddress remoteAddress;
    protected final @Nullable NetworkAddress localAddress;
    protected final EndpointConfig endpointConfig;
    /**
     * The dialog types enabled for responding. Endpoint handlers of this
     * endpoint (e.g., future handlers) share this map. Use concurrent hash map
     * for thread-safety.
     */
    private final DialogHandlerFactoryMap dialogHandlerFactories =
        new DialogHandlerFactoryMap();
    /**
     * The reference to the completable future of the endpoint handler
     * resolution. Synchronize on the endpoint instance when setting this
     * field.
     *
     * Currently only support one handler for the endpoint. In the future, we
     * may create more handlers to exploit more network bandwidth.
     */
    private volatile @Nullable CompletableFuture<EndpointHandler>
        handlerRef = null;
    /**
     * The state of this creator endpoint. Access must be under the
     * synchronization block of this object.
     */
    private State state = State.NONE;
    /** The stat enum. */
    private enum State {
        /**
         * The creator endpoint is just initialized. No handler available and
         * no resolution attempt has been made. The handlerRef holds a
         * null value. Transit to RESOLVING or SHUTDOWN.
         */
        NONE,
        /**
         * The handler is under resolution. The value of handlerRef is
         * non-null. Transit to READY or SHUTDOWN.
         */
        RESOLVING,
        /**
         * The handler is ready to use. The value of handlerRef is
         * non-null and is completed successfully. Transit to SHUTDOWN.
         */
        READY,
        /**
         * The handler is shut down and therefore so is the creator endpoint.
         * The value of handlerRef is null. This is the terminal state.
         *
         * The handler is shutdown because either the handler is shut down or
         * the endpoint group is shut down. We shut down the creator endpoint
         * when the handler is shut down so that we do not have lingering
         * objects in the endpoint group cache. This, however, along with the
         * non-blocking design of synchronization created a complication where
         * some dialogs see ConnectionEndpointShutdownException when the
         * creator and the handler is shutting down before it is removed from
         * the cache. Currently this is handled by
         * AsyncRegistryUtils#WrappedDialogHandler, see [KVSTORE-1242].
         */
        SHUTDOWN,
    }

    protected AbstractCreatorEndpoint(AbstractEndpointGroup endpointGroup,
                                      String perfName,
                                      NetworkAddress remoteAddress,
                                      @Nullable NetworkAddress localAddress,
                                      EndpointConfig endpointConfig) {
        checkNull("endpointGroup", endpointGroup);
        checkNull("perfName", perfName);
        checkNull("remoteAddress", remoteAddress);
        checkNull("endpointConfig", endpointConfig);
        this.endpointGroup = endpointGroup;
        this.perfName = perfName;
        this.remoteAddress = remoteAddress;
        this.localAddress = localAddress;
        this.endpointConfig = endpointConfig;
    }

    /**
     * Starts a dialog.
     */
    @Override
    public void startDialog(int dialogType,
                            DialogHandler dialogHandler,
                            long timeoutMillis) {

        assert TestHookExecute.doHookIfSet(startDialogHook, dialogType);

        checkNull("dialogHandler", dialogHandler);
        EndpointHandler handler = getEndpointHandler();
        if (handler != null) {
            handler.startDialog(
                dialogType, dialogHandler, timeoutMillis);
            return;
        }
        synchronized(this) {
            if (state.equals(State.SHUTDOWN)) {
                NullDialogStart.fail(
                    dialogHandler,
                    (new ConnectionEndpointShutdownException(
                        false /* local */, false /* handshake not done */,
                        new NoChannelDescription(remoteAddress),
                        "endpoint already shutdown")).
                    getDialogException(false),
                    endpointGroup.getSchedExecService());
                return;
            }
        }
        try {
            assert TestHookExecute.doHookIfSet(connectHook, null);
            getOrConnect()
                .thenAccept(
                    (h) ->
                    h.startDialog(dialogType, dialogHandler, timeoutMillis))
                .exceptionally(
                    (e) -> {
                        notifyDialogStartException(e, dialogHandler);
                        return null;
                    });
        } catch (Throwable t) {
            notifyDialogStartException(t, dialogHandler);
        }
    }

    private void notifyDialogStartException(Throwable e,
                                            DialogHandler dialogHandler) {
        final Throwable cause = FutureUtils.unwrapException(e);
        if (cause instanceof IOException) {
            NullDialogStart.fail(
                dialogHandler,
                new InitialConnectIOException(
                    new NoChannelDescription(remoteAddress),
                    (IOException) cause).
                getDialogException(false),
                NullDialogStart.IN_PLACE_EXECUTE_EXECUTOR);
        } else if (cause instanceof ConnectionException) {
            NullDialogStart.fail(
                dialogHandler,
                ((ConnectionException) cause)
                .getDialogException(false),
                NullDialogStart.IN_PLACE_EXECUTE_EXECUTOR);
        } else {
            NullDialogStart.fail(
                dialogHandler,
                new ConnectionUnknownException(
                    false /* handshake not done */,
                    new NoChannelDescription(remoteAddress),
                    cause)
                .getDialogException(false),
                NullDialogStart.IN_PLACE_EXECUTE_EXECUTOR);
        }
    }

    /**
     * Returns the endpoint handler if the future is completed without
     * exception or {@code null} if it is not completed or encountered
     * exception.
     *
     * We return {@code null} under exception case instead of throw. This is
     * because the uses of this method does not care whether we have
     * encountered exception before. We do not care either because (1) we
     * obtain the endpoint handler here only for information or for clean up,
     * or (2) we had delivered the exception before and we are just checking
     * here as an optimization to see if a new endpoint handler resolution is
     * needed.
     */
    public @Nullable EndpointHandler getEndpointHandler() {
        final CompletableFuture<EndpointHandler> ref = handlerRef;
        if (ref == null) {
            return null;
        }
        if (!ref.isDone()) {
            return null;
        }
        if (ref.isCompletedExceptionally()) {
            return null;
        }
        if (ref.isCancelled()) {
            return null;
        }
        try {
            /*
             * Use try catch to avoid the rare race condition that the future
             * result is changed by the CompletableFuture#obstrudeException in
             * between the above checks and the following call.
             */
            return ref.getNow(null);
        } catch (CancellationException|CompletionException e) {
            return null;
        }
    }

    /**
     * Enables responding to a dialog type.
     */
    @Override
    public void enableResponding(int dialogType,
                                 DialogHandlerFactory factory) {
        checkNull("factory", factory);
        dialogHandlerFactories.put(dialogType, factory);
    }

    /**
     * Disables responding to a dialog type.
     */
    @Override
    public void disableResponding(int dialogType) {
        dialogHandlerFactories.remove(dialogType);
    }

    /**
     * Returns the endpoint group.
     */
    @Override
    public EndpointGroup getEndpointGroup() {
        return endpointGroup;
    }


    /**
     * Returns the network address of the remote endpoint.
     */
    @Override
    public NetworkAddress getRemoteAddress() {
        return remoteAddress;
    }

    /**
     * Returns the channel factory of the endpoint.
     */
    @Override
    public EndpointConfig getEndpointConfig() {
        return endpointConfig;
    }

    /**
     * Returns the limit on the number of dialogs this endpoint can
     * concurrently start.
     */
    @Override
    public int getNumDialogsLimit() {
        final EndpointHandler handler = getEndpointHandler();
        if (handler == null) {
            return -1;
        }
        return handler.getNumDialogsLimit();
    }

    /**
     * Cleans up when the handler is shutdown.
     *
     * Note that this method may be called more than once.
     */
    @Override
    public void onHandlerShutdown(EndpointHandler handler) {
        /*
         * Note that we do not need to verify that the handler passed in is the
         * same with that in handlerRef. Currently, it is designed that the
         * endpoint and its handler has a one-to-one mapping.
         */
        transitToShutdown();
    }

    private void transitToShutdown() {
        synchronized(this) {
            if (state.equals(State.SHUTDOWN)) {
                return;
            }
            /*
             * Mark the endpoint shut down, so that no one can create new
             * handlers.
             */
            transit(State.SHUTDOWN);
            handlerRef = null;
        }
        endpointGroup.invalidateCreatorEndpoint(this);
    }


    @Override
    public DialogEndpointGroupPerfTracker getEndpointGroupPerfTracker() {
        return endpointGroup.getDialogEndpointGroupPerfTracker();
    }

    @Override
    public void shutdown(String detail, boolean force) {
        synchronized(this) {
            if (state.equals(State.SHUTDOWN)) {
                return;
            }
            final CompletableFuture<EndpointHandler> ref = handlerRef;
            /*
             * Make sure the handler is scheduled to be shut down.
             */
            if (ref != null) {
                ref.thenAccept((h) -> {
                    h.shutdown(detail, force);
                });
            }
            /*
             * Transits to shudown for the endpoint. The handler being shut
             * down will call this method as well and therefore, in the cases
             * where there is a handler, this call is redundant. But it is
             * harmless since the transitToShutdown method is a no-op after
             * shutdown.
             */
            transitToShutdown();
        }
    }

    /**
     * Returns the dialog handler factories.
     */
    public DialogHandlerFactoryMap getDialogHandlerFactoryMap() {
        return dialogHandlerFactories;
    }

    /**
     * Creates the endpoint handler asynchronously and returns the completable
     * future. The completable future completes with a non-null endpoint
     * handler or completes exceptionally. The method itself is not expected to
     * throw any exceptions.
     */
    protected abstract
        CompletableFuture<EndpointHandler> newEndpointHandler();

    /**
     * Returns the completable future of obtaining the endpoint handler. The
     * completable future completes with a non-null endpoint handler or
     * completes exceptionally.
     *
     * Note that if the future completes exceptionally, the handler and the
     * creator endpoint are shut down or being shut down. The upper layer will
     * be notified of this exception through dialog layer exception handling.
     */
    private CompletableFuture<EndpointHandler> getOrConnect() {
        final CompletableFuture<EndpointHandler> ref = handlerRef;
        if (ref != null) {
            return ref;
        }
        synchronized (this) {
            if (handlerRef != null) {
                return handlerRef;
            }
            if (state.equals(State.SHUTDOWN)) {
                throw new ConnectionEndpointShutdownException(
                    false /* local */,
                    false /* handshake not done */,
                    new NoChannelDescription(remoteAddress),
                    "endpoint already shutdown");
            }
            transit(State.RESOLVING);
            final CompletableFuture<EndpointHandler> future =
                newEndpointHandler()
                .thenApply((h) -> {
                    transit(State.READY);
                    return h;
                });
            /*
             * Adds the exception handling stage, but do not return it
             * since the additional stage returns a null endpoint handler.
             */
            future.exceptionally((t) -> {
                /*
                 * Shuts down this endpoint as we failed to create a handler
                 * for it.
                 */
                transitToShutdown();
                return null;
            });
            handlerRef = future;
            return future;
        }
    }

    /**
     * Shuts down the handler for testing.
     */
    public void shutdownHandler(String detail, boolean force) {
        final EndpointHandler handler = getEndpointHandler();
        if (handler != null) {
            handler.shutdown(detail, force);
        }
    }

    @Override
    public String toString() {
        return String.format("%s[remoteAddress=%s, endpointConfig=%s]",
                             getClass().getSimpleName(),
                             remoteAddress,
                             endpointConfig);
    }

    /**
     * Returns the hash code of the underlying address, since that is most
     * likely to be different across multiple instances.
     */
    @Override
    public int hashCode() {
        return remoteAddress.hashCode();
    }

    /**
     * Returns true if the argument is an instance of the same class with the
     * equal field values. The perfName field is ignored because it is just for
     * debugging.
     */
    @Override
    public boolean equals(@Nullable Object object) {
        if ((object == null) || !getClass().equals(object.getClass())) {
            return false;
        }
        final AbstractCreatorEndpoint other = (AbstractCreatorEndpoint) object;
        return endpointGroup.equals(other.endpointGroup) &&
            remoteAddress.equals(other.remoteAddress) &&
            Objects.equals(localAddress, other.localAddress) &&
            endpointConfig.equals(other.endpointConfig);
    }

    /** Transits the state. */
    private synchronized void transit(State newState) {
        switch (state) {
        case NONE:
            if (!newState.equals(State.SHUTDOWN)) {
                ensureState(State.RESOLVING, newState);
            }
            break;
        case RESOLVING:
            if (!newState.equals(State.SHUTDOWN)) {
                ensureState(State.READY, newState);
            }
            break;
        case READY:
            ensureState(State.SHUTDOWN, newState);
            break;
        case SHUTDOWN:
            break;
        default:
            throw new IllegalStateException(
                String.format("Unknown state: %s", state));
        }
        state = newState;
    }

    private void ensureState(State expected, State actual) {
        if (!expected.equals(actual)) {
            throw new IllegalStateException(
                String.format(
                    "Expected %s, but got %s", expected, actual));
        }
    }
}
