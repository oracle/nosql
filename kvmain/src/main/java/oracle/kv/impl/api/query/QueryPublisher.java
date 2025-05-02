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

package oracle.kv.impl.api.query;

import static oracle.kv.impl.util.ObjectUtil.checkNull;
import static oracle.kv.impl.util.ThreadUtils.threadId;

import java.io.PrintStream;
import java.util.Objects;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.ExecutionSubscription;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.query.QueryStatementResultImpl.QueryResultIterator;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.Statement;
import oracle.kv.stats.DetailedMetrics;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import org.checkerframework.checker.nullness.qual.Nullable;

/*
 * An implementation of Publisher that creates a single subscription
 * using an {@link AsyncIterationHandleImpl}. Calls {@link Subscriber#onError}
 * with an {@link IllegalStateException} if {@link #subscribe} is called more
 * than once.
 */
public class QueryPublisher implements Publisher<RecordValue> {

    private final KVStoreImpl theStore;

    private final Logger theLogger;

    private final ExecuteOptions theOptions;

    private final Set<RepGroupId> theShards;

    private final InternalStatement theStatement;

    /* Synchronize on this instance when accessing these fields */

    private Subscriber<? super RecordValue> theSubscriber;

    private QueryStatementResultImpl theQueryResult;

    private QueryResultIterator theQueryResultIter;

    /* The number of iteration items requested. */
    private long theRequests;

    /* Whether Subscriber.onSubscribe has been called. */
    private boolean theCalledOnSubscribe;

    /* Whether Subscriber.onComplete or onError has been called. */
    private boolean theComplete;

    /* Whether cancel has been called. Set only once. */
    private volatile boolean theCancelCalled;

    /* If non-null, a throwable to deliver to Subscriber.onError. */
    private @Nullable Throwable theDeliverException;

    /* Although the notifySubscriber() method is synchronized (so only one
     * thread at a time may be executing the query), recursive calls (by the
     * same thread) to notifySubscriber() are possible, for 2 reasons:
     * (a) notifySubscriber() calls the subscriber's "on" methods, which
     * may invoke request() or cancel(), which then invoke notifySubscriber()
     * recursively.
     * (b) A thread that sends a remote request may be also the thread that
     * receives and processes the response. This can happen if between the
     * time the thread sends the request and the time it calls whenComplete()
     * on the CompletableFuture, the response has arrived so that the
     * CompletableFuture is alreasy completed.
     *
     * theNotifyingSubscriber is used to make sure that such recursive calls
     * are (almost) noops. It is set to true at the start of the outer
     * notifySubscriber() invocation and to false at the end of that invocation.
     * Recursive calls find theNotifyingSubscriber set to true and exit
     * immediatelly as a result. */
    private boolean theNotifyingSubscriber;

    /* Extra care needs to be taken for recursive calls of the (b) kind.
     * In this case, the qri.nextLocal() in the outer notifySubscriber() call
     * will return null (because the fact that a remote request was needed
     * means that there were no more local results). If no care is taken,
     * this will cause the outer notifySubscriber() call to exit, thus failing
     * to generate new local results that may now be available due to the
     * arrival of the remote response. This will cause the query to hung.
     * theHaveNewRemoteResults is used to avoid this problem. It is set to
     * true when a recursive call of the (b) kind occurs. Then, in the outer
     * notifySubscriber(), if nextLocal() returns null but theHaveNewRemoteResults
     * is true, notifySubscriber() won't exit but call nextLocal() again,
     * and it will thus "see" the new batch of results and start generating
     * local results again. */
    private boolean theHaveNewRemoteResults;

    private Object theLock;

    public QueryPublisher(
        KVStoreImpl store,
        ExecuteOptions options,
        InternalStatement statement,
        Set<RepGroupId> shards) {

        if (statement instanceof PreparedDdlStatementImpl) {
            throw new UnsupportedOperationException(
                "Asynchronous execution of DDL statements is not supported");
        }

        if (options == null) {
            options = new ExecuteOptions();
        }

        theStore = store;
        theLogger = store.getLogger();
        theOptions = options;
        theStatement = statement;
        theShards = shards;
        theLock = new Object();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("QueryPublisher :");
        sb.append("\ntheNotifyingSubscriber = ").append(theNotifyingSubscriber);
        sb.append("\ntheHaveNewRemoteResults = ").append(theHaveNewRemoteResults);
        sb.append("\ntheComplete = ").append(theComplete);
        sb.append("\ntheRequests = ").append(theRequests);
        sb.append("\ntheDeliverException = ").append(theDeliverException);
        return sb.toString();
    }

    public void printTrace(PrintStream out) {
        if (theQueryResult != null) {
            theQueryResult.printTrace(out);
        }
    }

    /*
     * QuerySubscription is public because it is accessed by proxy code.
     */
    public class QuerySubscription implements ExecutionSubscription {

        @Override
        public void cancel() {
            QueryPublisher.this.cancel();
        }

        @Override
        public void request(long n) {
            QueryPublisher.this.request(n);
        }

        @Override
        public Statement getStatement() {
            return QueryPublisher.this.theQueryResult.getStatement();
        }

        @Override
        public RecordDef getResultDef() {
            return QueryPublisher.this.theQueryResult.getResultDef();
        }

        @Override
        public List<DetailedMetrics> getPartitionMetrics() {
            return QueryPublisher.this.theQueryResultIter.getPartitionMetrics();
        }

        @Override
        public List<DetailedMetrics> getShardMetrics() {
            return QueryPublisher.this.theQueryResultIter.getShardMetrics();
        }

        public QueryPublisher getPublisher() {
            return QueryPublisher.this;
        }

        public @Nullable QueryResultIterator getAsyncIterator() {
            return QueryPublisher.this.theQueryResultIter;
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

    public Object getLock() {
        return theLock;
    }

    @Override
    public synchronized void subscribe(Subscriber<? super RecordValue> s) {

        Objects.requireNonNull(s, "The subscriber must not be null");
        boolean callDummyOnSubscribe = false;
        boolean callOnError = false;

        if (theSubscriber == null) {
            theSubscriber = s;
        } else {
            /*
             * Reactive Streams Subscriber Rule 1.10 says subscribe must
             * not be called more than once with the same subscriber. Given
             * that, it seems simplest to only support a single call to
             * subscribe so we don't have to track multiple subscribers.
             */
            callOnError = true;
            if (!theSubscriber.equals(s) || !theCalledOnSubscribe) {
                callDummyOnSubscribe = true;
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

        try {
            theQueryResult =
                new QueryStatementResultImpl(theStore.getTableAPIImpl(),
                                             theOptions,
                                             theStatement,
                                             this,
                                             null, /* partitions */
                                             theShards);
            theQueryResultIter = theQueryResult.getIterator();

            callOnSubscribe();

        } catch (Throwable t) {
            if (!theCalledOnSubscribe) {
                callDummyOnSubscribe = true;
             }

            if (!theComplete) {
                callOnError = true;
            }

            if (callDummyOnSubscribe) {
                callDummyOnSubscribe(s);
            }

            if (callOnError) {
                callOnError(theSubscriber, t);
            }
        }
    }

    private void callOnSubscribe() {

        try {
            theCalledOnSubscribe = true;
            Subscription subscription = new QuerySubscription();
            theSubscriber.onSubscribe(subscription);
        } catch (Throwable e) {
            theLogger.fine(() -> "Unexpected exception calling onSubscribe" +
                           " on subscriber: " + theSubscriber +
                           ", exception: " + e);

            /*
             * Reactive Streams Publisher Rule 2.13 requires that the
             * subscription be canceled if onSubscribe fails. It also
             * recommends not bothering to notify the subscriber after a
             * failure like this, but makes it easier to test, and seems
             * harmless, if we attempt to notify the subscriber by calling
             * onError() on it.
             */
            if (!theCancelCalled) {

                theDeliverException =
                    (e instanceof Error ?
                     e :
                     new IllegalStateException(
                         "Unexpected exception calling onSubscribe" +
                         " on subscriber: " + theSubscriber +
                         ", exception: " + e,
                         e));
                cancel();
            }
        }
    }

    private void callDummyOnSubscribe(Subscriber<? super RecordValue> subscriber) {
        try {
            theCalledOnSubscribe = true;
            subscriber.onSubscribe(new DummySubscription());
        } catch (Throwable t) {
            theLogger.fine(() -> "Exception thrown when calling onSubscribe" +
                           " on subscriber: " + subscriber + ", exception: " + t);
        }
    }

    void cancel() {
        assert(theQueryResultIter != null);

        if (theCancelCalled) {
            return;
        }
        theCancelCalled = true;

        synchronized(theLock) {
            notifySubscriber(false);
        }
    }

    void request(long n) {

        synchronized (theLock) {

            if (n < 1) {
                theDeliverException = new IllegalArgumentException(
                    "Request value must be greater than zero");
            } else {
                theRequests += n;

                /* Check for overflow */
                if (theRequests <= 0) {
                    theRequests = Long.MAX_VALUE;
                }
            }

            notifySubscriber(false);
        }
    }

    /*
     * This method drives query execution by generating local results, when they
     * are available, and calling the subscriber onNext() method. It also calls
     * the subsriber onComplete() or onError() methods when all the query results
     * have be generated, or the subscription is cancelled, or an exception is
     * raised. It is called by the subscription request() and cancel() methods,
     * as well as by the ReceiveIter when it receives a new batch of results
     * from an RN.
     */
    public void notifySubscriber(boolean haveNewRemoteResults) {

        assert(Thread.holdsLock(theLock));

        if (theOptions.getTraceLevel() >= 4) {
            theQueryResult.getRCB().trace(
                "QUERY(" + threadId(Thread.currentThread()) + ") : " +
                "notifySubscriber :\n" + "haveNewRemoteResults = " +
                haveNewRemoteResults + "\n" + this);
        }

        if (theComplete) {
            return;
        }

        if (theNotifyingSubscriber) {
            if (haveNewRemoteResults) {
                theHaveNewRemoteResults = true;
            }
            return;
        }

        theNotifyingSubscriber = true;

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
                if (notifyOnce()) {
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
            /* Make certain that notifyingSubscriber is cleared if there is an
             * unexpected exception -- it will have already been cleared
             * otherwise */
            if (exception != null) {
                theNotifyingSubscriber = false;
                theLogger.log(Level.WARNING, "Unexpected exception: " + exception,
                              exception);
            }
        }
    }

    private boolean notifyOnce() {

        assert(!theComplete);

        /* Get next element and closed status */
        @Nullable RecordValue next = null;
        Throwable closeException = null;

        final QueryResultIterator qri =
            checkNull("theQueryResultIter", theQueryResultIter);

        if (theCancelCalled || theDeliverException != null) {
            closeException = theDeliverException;
            qri.close();

        } else if (qri.isClosed()) {
            /*
             * Notice if the iterator was closed due to an exception thrown by
             * the remote request.
             */
            closeException = qri.getCloseException();

        } else if (theRequests > 0) {
            try {
                next = qri.nextLocal();
            } catch (QueryException qe) {
                closeException = qe.getIllegalArgument();
            } catch (Throwable e) {
                closeException = e;
            }
        }

        if (next != null) {
            closeException = callOnNext(next);

            /* Delivering next failed: terminate the iteration */
            if (closeException != null) {
                qri.close();
                next = null;
            } else {
                assert(theRequests > 0);
                theRequests--;
            }
        }

        boolean makeComplete =
            (closeException != null ?
             true :
             (next != null ? false : qri.isClosed()));

        boolean done;

        if (makeComplete) {
            if (closeException == null) {
                closeException = qri.getCloseException();
            }

            if (closeException == null) {
                callOnComplete(theSubscriber);
            } else {
                callOnError(theSubscriber, closeException);
            }

            /* qri.isClosed() returns true if the iterator is "done", i.e., it
             * does not have any more results to produce. However, in the "done"
             * state, resources used by the query execution plan are not released,
             * because the plan can be re-opened (reset). So, close() must be called
             * here to release query resources. */
            qri.close();

            /* Reactive Streams Subscription Rule 3.13 says that canceling
             * a subscription must result in dropping references to the
             * subscriber. */
            theSubscriber = null;

            theNotifyingSubscriber = false;
            done = true;

        } else if (theHaveNewRemoteResults) {
            theHaveNewRemoteResults = false;
            done = false;

        } else if (next == null) {
            /* no more local results */
            theNotifyingSubscriber = false;
            done = true;
        } else {
            /* produce more local results, if possible */
            done = false;
        }

        return done;
    }

    private @Nullable Throwable callOnNext(RecordValue next) {

        try {
            theSubscriber.onNext(next);
            theQueryResultIter.refreshEndTime();
            return null;
        } catch (Throwable t) {
            theLogger.fine(() ->
                           "Unexpected exception calling onNext" +
                           " on subscriber: " + theSubscriber +
                           ", exception: " + t);
            return (t instanceof Error ?
                    t :
                    new IllegalStateException(
                        "Unexpected exception calling onNext" +
                        " on subscriber: " + theSubscriber +
                        ", exception: " + t,
                        t));
        }
    }

    private void callOnComplete(Subscriber<?> subscriber) {

        try {
            theComplete = true;
            subscriber.onComplete();
        } catch (Throwable t) {
            theLogger.fine(() ->
                           "Unexpected exception calling onComplete" +
                           " on subscriber: " + subscriber +
                           ", exception from subscriber: " + t);
        }
    }

    private void callOnError(
        Subscriber<? super RecordValue> subscriber,
        Throwable exception) {
        try {
            theComplete = true;
            subscriber.onError(exception);
        } catch (Throwable t) {
            theLogger.fine(() -> "Exception thrown when calling onError" +
                           " on subscriber: " + subscriber + ", exception: " + t);
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
