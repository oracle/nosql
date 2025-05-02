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

import java.util.Objects;
import java.util.function.Supplier;
import java.util.logging.Logger;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An implementation of {@link Publisher} that creates a single subscription
 * using an {@link AsyncIterationHandleImpl}. Calls {@link Subscriber#onError}
 * with an {@link IllegalStateException} if {@link #subscribe} is called more
 * than once.
 *
 * @param <E> the element type
 */
public class AsyncPublisherImpl<E> implements Publisher<E> {

    private final Supplier<AsyncIterationHandleImpl<E>> handleSupplier;

    private final Logger logger;

    /* Synchronize on this instance when accessing these fields */

    private @Nullable Subscriber<? super E> subscriber;

    private boolean calledOnSubscribe;

    private AsyncPublisherImpl(
        Supplier<AsyncIterationHandleImpl<E>> handleSupplier,
        Logger logger) {

        this.handleSupplier = handleSupplier;
        this.logger = logger;
    }

    /**
     * Creates an instance that calls the argument to create the iteration
     * handle.
     *
     * @param <E> the element type
     * @param handleSupplier for creating the iteration handle
     * @param logger for logging messages
     * @return the publisher
     */
    public static <E> Publisher<E> newInstance(
        Supplier<AsyncIterationHandleImpl<E>> handleSupplier, Logger logger) {

        return new AsyncPublisherImpl<>(handleSupplier, logger);
    }

    @Override
    public void subscribe(@Nullable Subscriber<? super E> s) {
        Objects.requireNonNull(s, "The subscriber must not be null");
        boolean callDummyOnSubscribe = false;
        boolean callOnError = false;

        synchronized (this) {
            final Subscriber<? super E> currentSubscriber = subscriber;
            if (currentSubscriber == null) {
                subscriber = s;
            } else {

                /*
                 * Reactive Streams Subscriber Rule 1.10 says subscribe must
                 * not be called more than once with the same subscriber. Given
                 * that, it seems simplest to only support a single call to
                 * subscribe so we don't have to track multiple subscribers.
                 */
                callOnError = true;
                if (!currentSubscriber.equals(s)) {
                    callDummyOnSubscribe = true;
                } else if (!calledOnSubscribe) {
                    calledOnSubscribe = true;
                    callDummyOnSubscribe = true;
                }
            }
        }
        if (callDummyOnSubscribe) {
            callDummyOnSubscribe(s);
        }
        if (callOnError) {
            callOnError(s,
                        new IllegalStateException(
                            "Calling subscribe multiple times on this" +
                            " publisher is not supported"));
            return;
        }
        final AsyncIterationHandleImpl<E> handle;
        try {
            handle = handleSupplier.get();
        } catch (Throwable e) {
            synchronized (this) {
                if (!calledOnSubscribe) {
                    calledOnSubscribe = true;
                    callDummyOnSubscribe = true;
                }
            }
            if (callDummyOnSubscribe) {
                callDummyOnSubscribe(s);
            }
            callOnError(s, e);
            return;
        }
        synchronized (this) {

            /* Iteration handle will call onSubscribe */
            calledOnSubscribe = true;
        }
        handle.subscribe(s);
    }

    private void callDummyOnSubscribe(Subscriber<? super E> s) {
        try {
            s.onSubscribe(new DummySubscription());
        } catch (Throwable t) {
            logger.fine(() ->
                        "Exception thrown when calling onSubscribe" +
                        " on subscriber: " + s +
                        ", exception: " + t);
        }
    }

    private void callOnError(Subscriber<? super E> s, Throwable exception) {
        try {
            s.onError(exception);
        } catch (Throwable t) {
            logger.fine(() ->
                        "Exception thrown when calling onError" +
                        " on subscriber: " + s +
                        ", exception: " + t);
        }
    }

    /**
     * A dummy subscription for use in satisfying Rule 1.9: Call onSubscribe
     * before onError.
     */
    static class DummySubscription implements Subscription {
        @Override public void cancel() { }
        @Override public void request(long n) { }
    }
}
