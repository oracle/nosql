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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.IterationSubscription;
import oracle.kv.stats.DetailedMetrics;
import oracle.kv.impl.api.query.QueryStatementResultImpl.QueryResultIterator;
import oracle.kv.impl.query.QueryException;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A class that generates subscriptions, to implement {@link Publisher} for
 * async iterations.
 *
 * @param <E> the type of the iteration element
 */
public class AsyncIterationHandleImpl<E> implements IterationHandleNotifier {

    /** The number of currently open subscriptions. */
    private static final AtomicInteger openSubscriptions = new AtomicInteger();

    /**
     * A thread local variable that records whether a call to the request
     * method is already underway in the current thread.
     */
    private final ThreadLocal<Boolean> inRequest = new ThreadLocal<Boolean>();

    /**
     * The logger.
     */
    private final Logger logger;

    /**
     * The object used for locking, to avoid interference from application code
     * that could lock this instance.  Non-final fields should only be accessed
     * by using synchronization on this lock, either from within a synchronized
     * block, in a code block that is running exclusively because it set
     * notifyingSubscriber while synchronized, or, for fields that are only set
     * once to a non-null value, after checking for a non-null value while
     * synchronized.  The newNotify field is special since it is set by
     * non-notifying threads: that field should only be read while in a
     * synchronized block.
     */
    private final Object lock = new Object();

    /**
     * The async iterator that supplies locally available values and transmits
     * close requests, or null if not initialized.  Set only once.
     */
    private @Nullable AsyncTableIterator<E> asyncIterator;

    /** The subscriber, or null if either not initialized or complete. */
    private @Nullable Subscriber<? super E> subscriber;

    /**
     * If the subscriber is newly supplied, meaning that onSubscribe should be
     * called.
     */
    private boolean newSubscriber;

    /** The number of iteration items requested. */
    private long requests;

    /**
     * Used to make sure only one thread makes calls to the subscriber at a
     * time.
     */
    private boolean notifyingSubscriber;

    /**
     * Set when notifyNext is called while another thread is notifying the
     * subscriber.  If true, the notifying thread will check again for a next
     * element or whether the iteration is closed before exiting.
     */
    private boolean newNotify;

    /** Whether Subscriber.onSubscribe has been called. */
    private boolean onSubscribeCalled;

    /** Whether Subscriber.onComplete or onError has been called. */
    private boolean complete;

    /** Whether cancel has been called.  Set only once. */
    private boolean cancelCalled;

    /** If non-null, a throwable to deliver to Subscriber.onError. */
    private @Nullable Throwable deliverException;

    /**
     * Creates an instance of this class.
     */
    public AsyncIterationHandleImpl(Logger logger) {
        this.logger = checkNull("logger", logger);
    }

    /**
     * Sets the iterator that the handle should use to obtain elements.  This
     * method is typically called in the iterator constructor, and must be
     * called before the iterate method is called.
     *
     * @param asyncIterator the iterator
     * @throws IllegalStateException if the iterator has already been specified
     */
    public void setIterator(AsyncTableIterator<E> asyncIterator) {
        synchronized (lock) {
            if (this.asyncIterator != null) {
                throw new AssertionError(
                    "Iterator has already been specified");
            }
            this.asyncIterator = checkNull("asyncIterator", asyncIterator);
        }
    }

    /**
     * Register a subscriber. This method should only be called once and does
     * not throw any exceptions.
     */
    public void subscribe(Subscriber<? super E> s) {
        assert s != null : "Caller should have checked for null";
        try {
            synchronized (lock) {
                if (subscriber != null) {
                    throw new AssertionError(
                        "Subscribe has already been called");
                }
                subscriber = s;
                if (asyncIterator == null) {
                    throw new AssertionError("No iterator");
                }
                newSubscriber = true;
            }
            notifyNext();
        } catch (Throwable t) {

            /* Report exceptions to the subscriber if we can */
            boolean callDummyOnSubscribe = false;
            boolean callOnError = false;
            synchronized (lock) {
                if (s != subscriber) {
                    callDummyOnSubscribe = true;
                    callOnError = true;
                } else {
                    if (!onSubscribeCalled) {
                        callDummyOnSubscribe = true;
                        onSubscribeCalled = true;
                    }
                    if (!complete) {
                        callOnError = true;
                        complete = true;
                    }
                }
            }
            if (callDummyOnSubscribe) {
                try {
                    s.onSubscribe(new AsyncPublisherImpl.DummySubscription());
                } catch (Throwable t2) {
                    logger.fine(() ->
                                "Unexpected exception calling onSubscribe" +
                                " on subscriber: " + s +
                                ", exception: " + t2);
                }
            }
            if (callOnError) {
                try {
                    s.onError(t);
                } catch (Throwable t2) {
                    logger.fine(() ->
                                "Unexpected exception calling onError" +
                                " on subscriber: " + s +
                                ", exception: " + t2);
                }
            } else {

                /* If subscriber is already complete, then just log */
                logger.log(Level.WARNING, "Unexpected exception: " + t, t);
            }
        }
    }

    void request(long n) {
        synchronized (lock) {
            if (asyncIterator == null) {
                throw new AssertionError("No iterator");
            }

            if (n < 1) {
                deliverException = new IllegalArgumentException(
                    "Request value must be greater than zero");
            } else {
                requests += n;

                /* Check for overflow */
                if (requests <= 0) {
                    requests = Long.MAX_VALUE;
                }
            }
        }

        /*
         * If we are already within a nested call to the request method, then
         * the top level call will deliver the results.  A recursive call can
         * happen if the application calls request, request calls notifyNext,
         * that calls the subscriber's onNext method, and that method calls
         * request again.
         */
        if (inRequest.get() != null) {
            return;
        }
        inRequest.set(Boolean.TRUE);
        try {
            notifyNext();
        } finally {
            inRequest.remove();
        }
    }

    void cancel() {
        synchronized (lock) {
            if (asyncIterator == null) {
                throw new AssertionError("No iterator");
            }
            if (cancelCalled) {
                return;
            }
            cancelCalled = true;
        }
        notifyNext();
    }

    List<DetailedMetrics> getPartitionMetrics() {
        final AsyncTableIterator<E> ai;
        synchronized (lock) {
            ai = asyncIterator;
            if (ai == null) {
                return Collections.emptyList();
            }
        }
        return ai.getPartitionMetrics();
    }

    List<DetailedMetrics> getShardMetrics() {
        final AsyncTableIterator<E> ai;
        synchronized (lock) {
            ai = asyncIterator;
            if (ai == null) {
                return Collections.emptyList();
            }
        }
        return ai.getShardMetrics();
    }

    /* -- Implement IterationHandleNotifier -- */

    @Override
    public void notifyNext() {
        if (Thread.holdsLock(lock)) {
            throw new AssertionError(
                "Already holding lock in call to notifyNext");
        }
        final Subscriber<? super E> s;
        final boolean doOnSubscribe;

        synchronized (lock) {

            /* Note new notify so notify thread checks again */
            if (notifyingSubscriber) {
                newNotify = true;
                logger.finest("notifyNext newNotify=true");
                return;
            }

            /* Check if subscriber isn't initialized yet */
            final @Nullable Subscriber<? super E> subscriberCheckNull =
                subscriber;
            if (subscriberCheckNull == null) {
                return;
            }
            s = subscriberCheckNull;

            /* Subscriber was already notified that the iteration is done */
            if (complete) {
                return;
            }

            /* Note if we should call onSubscribe, then clear */
            doOnSubscribe = newSubscriber;
            newSubscriber = false;

            /*
             * Mark that we are delivering notifications and clear newNotify so
             * that we notice newer changes
             */
            notifyingSubscriber = true;
            newNotify = false;
            logger.finest("notifyNext");
        }

        if (doOnSubscribe) {
            final Subscription subscription = createSubscription();
            try {
                synchronized (lock) {
                    onSubscribeCalled = true;
                }
                s.onSubscribe(subscription);
            } catch (Throwable e) {

                /*
                 * Reactive Streams Publisher Rule 2.13 requires that the
                 * subscription be canceled if onSubscribe fails. It also
                 * recommends not bothering to notify the subscriber after a
                 * failure like this, but makes it easier to test, and seems
                 * harmless, if we attempt to notify the subscriber.
                 */
                logger.fine(() ->
                            "Unexpected exception calling onSubscribe" +
                            " on subscriber: " + subscriber +
                            ", exception: " + e);
                synchronized (lock) {

                    /* Done if cancel was already called */
                    if (cancelCalled) {
                        notifyingSubscriber = false;
                        return;
                    }

                    cancelCalled = true;
                    deliverException = (e instanceof Error) ?
                        e :
                        new IllegalStateException(
                            "Unexpected exception calling onSubscribe" +
                            " on subscriber: " + subscriber +
                            ", exception: " + e,
                            e);
                }
            }
            noteOpeningSubscription();
        }

        Throwable exception = null;
        try {

            /*
             * Return local results to the app until:
             * - There are no more local results
             * - We have returned all the results requested by the app
             * - The iteration was canceled by the app
             * - The iteration was closed due to an error
             */
            while (true) {
                if (notifyOneNext()) {
                    break;
                }
            }
        } catch (RuntimeException e) {
            exception = e;
            throw e;
        } catch (Error e) {
            exception = e;
            throw e;
        } finally {

            /*
             * Make certain that notifyingSubscriber is cleared if there is an
             * unexpected exception -- it will have already been cleared
             * otherwise
             */
            if (exception != null) {
                synchronized (lock) {
                    notifyingSubscriber = false;
                }
                logger.log(Level.WARNING, "Unexpected exception: " + exception,
                           exception);
            }
        }
    }

    protected Subscription createSubscription() {
        return new IterationSubscriptionImpl();
    }

    /*
     * IterationSubscriptionImpl is public because it is accessed by proxy code.
     */
    public class IterationSubscriptionImpl
            implements IterationSubscription {
        @Override
        public void cancel() {
            AsyncIterationHandleImpl.this.cancel();
        }
        @Override
        public void request(long n) {
            AsyncIterationHandleImpl.this.request(n);
        }
        @Override
        public List<DetailedMetrics> getPartitionMetrics() {
            return AsyncIterationHandleImpl.this.getPartitionMetrics();
        }
        @Override
        public List<DetailedMetrics> getShardMetrics() {
            return AsyncIterationHandleImpl.this.getShardMetrics();
        }

        public @Nullable AsyncTableIterator<E> getAsyncIterator() {
            return AsyncIterationHandleImpl.this.asyncIterator;
        }
    }

    private void noteOpeningSubscription() {
        openSubscriptions.incrementAndGet();
        if (logger.isLoggable(Level.FINEST)) {
            logger.finest("Opening subscription, subscriber: " + subscriber +
                          ", count: " + openSubscriptions.get());
        }
    }

    private void noteClosingSubscription(Subscriber<?> forSubscriber) {
        openSubscriptions.decrementAndGet();
        if (logger.isLoggable(Level.FINEST)) {
            logger.finest("Closing subscription," +
                          " subscriber: " + forSubscriber +
                          ", count: " + openSubscriptions.get());
        }
    }

    /**
     * Check for a single next element, a closed iterator, a cancel call, or an
     * exception to deliver, returning true if notifications are done because
     * there are no more elements or the iterator is closed. This method is
     * called after setting notifyingSubscriber with the lock held, so it can
     * read most fields without synchronization, but still needs to synchronize
     * for updates.
     */
    private boolean notifyOneNext() {

        assert !complete;

        /* Get next element and closed status */
        @Nullable E next = null;
        Throwable closeException = null;

        final AsyncTableIterator<E> ai =
            checkNull("asyncIterator", asyncIterator);

        if (cancelCalled || (deliverException != null)) {
            closeException = deliverException;
            ai.close();
        } else if (ai.isClosed()) {
            /*
             * Notice if the iterator was closed due to an exception thrown by
             * the remote request.
             */
            closeException = ai.getCloseException();
        } else if (requests > 0) {
            try {
                next = ai.nextLocal();
            } catch (QueryException qe) {
                closeException = qe.getIllegalArgument();
            } catch (Throwable e) {
                closeException = e;
            }
        }

        if (next != null) {
            closeException = onNext(next);

            /* Delivering next failed: terminate the iteration */
            if (closeException != null) {
                ai.close();
                next = null;
            }
        }

        final boolean makeComplete =
            (closeException != null) ? true :
            (next != null) ? false :
            ai.isClosed();

        final Subscriber<? super E> savedSubscriber =
            checkNull("subscriber", subscriber);

        final long originalRequests = requests;
        final boolean done;

        synchronized (lock) {

            /* Decrement requests if we delivered a next element */
            if (next != null) {
                assert requests > 0;
                requests--;
            }

            if (makeComplete) {

                /* Iteration is complete */
                complete = true;

                /*
                 * Reactive Streams Subscription Rule 3.13 says that canceling
                 * a subscription must result in dropping references to the
                 * subscriber.
                 */
                subscriber = null;
                notifyingSubscriber = false;
                done = true;
            } else if (newNotify) {

                /* Clear and try again */
                newNotify = false;
                done = false;
            } else if (next == null) {

                /* No next and no new notifications, so we're done */
                notifyingSubscriber = false;
                done = true;
            } else {

                /* Check for more elements */
                done = false;
            }
        }

        if (logger.isLoggable(Level.FINEST)) {
            logger.finest("notifyNext next=" + next +
                          " makeComplete=" + makeComplete +
                          " newNotify=" + newNotify +
                          " originalRequests=" + originalRequests +
                          (closeException != null ?
                           " closeException=" + closeException :
                           "") +
                          " done=" + done);
        }

        if (makeComplete) {
            if (closeException == null) {
                closeException = ai.getCloseException();
            }
            onComplete(savedSubscriber, closeException);

            /*
             * In case ai is a QueryResultIterator, ai.isClosed() returns true
             * if the iterator is "done", i.e., it does not have any more
             * results to produce. However, in the "done" state, resources
             * used by the query execution plan are not released, because
             * the plan can be re-opened (reset). So, close() must be called
             * here to release query resources.
             */
            ai.close();
        }
        return done;
    }

    /**
     * Deliver a next iteration result, returning any exception thrown during
     * delivery, or null if the call completes normally.
     */
    private @Nullable Throwable onNext(E next) {

        final Subscriber<? super E> s = checkNull("subscriber", subscriber);

        try {
            s.onNext(next);

            if (asyncIterator instanceof QueryResultIterator) {
                ((QueryResultIterator)asyncIterator).refreshEndTime();
            }

            return null;
        } catch (Throwable t) {
            logger.fine(() ->
                        "Unexpected exception calling onNext" +
                        " on subscriber: " + s +
                        ", exception: " + t);
            return (t instanceof Error) ?
                t :
                new IllegalStateException(
                    "Unexpected exception calling onNext" +
                    " on subscriber: " + s +
                    ", exception: " + t,
                    t);
        }
    }

    /**
     * Deliver the completion result.
     */
    private void onComplete(Subscriber<?> forSubscriber,
                            @Nullable Throwable exception) {
        noteClosingSubscription(forSubscriber);
        try {
            if (exception == null) {
                forSubscriber.onComplete();
            } else {
                forSubscriber.onError(exception);
            }
        } catch (Throwable t) {
            logger.fine(() ->
                        "Unexpected exception calling " +
                        ((exception != null) ? "onError" : "onComplete") +
                        " on subscriber: " + forSubscriber +
                        ((exception != null) ?
                         ", exception being delivered: " + exception :
                         "") +
                        ", exception from subscriber: " + t);
        }
    }

    /**
     * Close the iteration if the handle is garbage collected without being
     * closed.
     */
    @Deprecated
    @Override
    protected void finalize() {
        cancel();
    }
}
