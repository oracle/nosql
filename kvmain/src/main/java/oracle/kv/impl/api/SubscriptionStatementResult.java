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

package oracle.kv.impl.api;

import java.io.PrintStream;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import oracle.kv.StatementResult;
import oracle.kv.StoreIteratorException;
import oracle.kv.impl.api.query.QueryPublisher.QuerySubscription;
import oracle.kv.impl.test.ExceptionTestHook;
import oracle.kv.impl.test.ExceptionTestHookExecute;
import oracle.kv.stats.DetailedMetrics;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;
import oracle.kv.table.TableIterator;

/**
 * Implementation of the StatementResult when the statement is a query and
 * needs to be executed in the asynchronous mode.
 *
 * It acts as a subscriber to the executeAsync and fetch the results and
 * delivers to the client. it works similiar to the QueryStatementResultImpl.
 *
 * It works as below
 * 1. The client waits for the first result to be populated.
 * 2. when the client iterated over the first result, the subscriber collects
 *    the remaining results in the batch and repeats the step 2 for the new
 *    batch.
 * 3. onCompletion or onError while fetching the results, the subscriber's
 *    iterator will be done.
 */
class SubscriptionStatementResult implements StatementResult {

    private final QuerySubscription currentSub;
    private final ResultListIterator iterator;
    private volatile boolean closed;
    private final LinkedBlockingQueue <RecordValue> resList;
    private final long batchSize;
    /**
     * Test hook called by ResultListIterator.hasNext immediately before
     * calling take on the result list, to permit throwing an
     * InterruptedException at that point, so we can test that case.
     */
    private ExceptionTestHook<Void, InterruptedException> iteratorNextHook;

    /**
     * Represents the completion state of the iteration. Callers may complete
     * the future separately as a way to signal that the iteration is done.
     */
    private final CompletableFuture<Boolean> iterFuture;

    SubscriptionStatementResult(
        LinkedBlockingQueue<RecordValue> resList,
        QuerySubscription sub,
        long batchSize,
        CompletableFuture<Boolean> iterFuture) {

        this.currentSub = sub;
        this.batchSize = batchSize;
        this.resList = resList;
        this.iterator = new ResultListIterator();
        this.iterFuture = iterFuture;
        closed = false;
    }

    /**
     * Set test hook for iterator.next().
     */
    public void setIteratorNextHook(
            ExceptionTestHook<Void, InterruptedException> testHook) {
        iteratorNextHook = testHook;
    }

    @Override
    public boolean isSuccessful() {
        if (closed) {
            return true;
        }
        return !iterFuture.isCompletedExceptionally();
    }

    @Override
    public int getPlanId() {
        return 0;
    }

    @Override
    public String getInfo() {
        return null;
    }

    @Override
    public String getInfoAsJson() {
        return null;
    }

    @Override
    public String getErrorMessage() {
        return null;
    }

    @Override
    public String toString() {
       return "SubscriptionStatementResult[" +
           "success=" + isSuccessful() +
           " planId=" + getPlanId() +
           " jsonInfo=" + getInfoAsJson() +
           " isDone=" + isDone() +
           " isCancelled=" + isCancelled() +
           "]";
    }

    @Override
    public void printTrace(PrintStream out) {
        currentSub.getPublisher().printTrace(out);
    }

    @Override
    public boolean isDone() {
         return !iterator.hasNext();
    }

    @Override
    public boolean isCancelled() {
        if (closed) {
            return false;
        }
        return iterFuture.isCancelled();
    }

    @Override
    public String getResult() {
        return null;
    }

    @Override
    public Kind getKind() {
        return Kind.QUERY;
    }

    @Override
    public void close() {
        iterator.close();
    }

    @Override
    public RecordDef getResultDef() {
        return currentSub.getResultDef();
    }

    @Override
    public TableIterator<RecordValue> iterator() {
        if (closed) {
            throw new IllegalStateException("Statement result already closed.");
        }
        return iterator;
    }

    private class ResultListIterator implements TableIterator<RecordValue> {

        RecordValue nextElem = null;

        ResultListIterator() { }

        @Override
        public void close() {

            if (!closed) {
                closed = true;
                currentSub.cancel();
                iterFuture.cancel(false);
                resList.offer(KVStoreImpl.EMPTY_RECORD);
            }
        }

        @Override
        public List<DetailedMetrics> getPartitionMetrics() {
            return currentSub.getPartitionMetrics();
        }

        @Override
        public List<DetailedMetrics> getShardMetrics() {
            return currentSub.getShardMetrics();
        }

        /*
         * Return true when there is an exception so the caller sees the
         * exception when they call next
         */
        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            if (nextElem == null) {
                nextElem = resList.poll();
            }

            /*
             * if the exception happens in the previous request causing
             * the race to happens between iterFuture and resList.offer
             * due to context switch it will fall through and create
             * one more subscription request, as
             * https://github.com/reactive-streams/reactive-streams-jvm
             * #publisher#rule6 and #subscription#rule6 it will be noop
             * and barricaded by reslist.take()
             */

            if (nextElem != null) {
                /* If the element is empty, it is the done marker */
                if (nextElem.isEmpty()) {
                    if (iterFuture.isCompletedExceptionally()) {
                        return true;
                    }
                    return false;
                }
                return true;
            }

            currentSub.request(batchSize);
            try {
                assert ExceptionTestHookExecute.doHookIfSet(iteratorNextHook,
                                                            null);
                nextElem = resList.take();
            } catch (InterruptedException e) {
                iterFuture.completeExceptionally(e);
            }

            /*
             * nextElem can be in one to the three states
             *  1. null (this happens when client interrupts after
             *           currentSub.request during take()) -> return true
             *
             *  2. recordvalue
             *  3. Empty (during exception -> return true
             *            completed normally -> return false
             *  no race happens between iterFuture and resList
             *  here because nextElem will be one of the
             *  above state (take acts as a barrier to a publisher and client)
             *
            */

            if (nextElem != null && nextElem.isEmpty()) {
                if (iterFuture.isCompletedExceptionally()) {
                    return true;
                }
                return false;
            }
            return true;
        }

        @Override
        public RecordValue next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            if (iterFuture.isCompletedExceptionally() && !closed) {
                try {
                    iterFuture.get();
                } catch (Exception ex) {
                    final Throwable cause;
                    /*
                     * Use the cause for an ExecutionException, if any, since
                     * that represents the exception thrown by the operation.
                     */
                    if ((ex instanceof ExecutionException) &&
                        (ex.getCause() != null)) {
                        cause = ex.getCause();
                    } else {
                        cause = ex;
                    }
                    throw new StoreIteratorException(cause, null);
                }
            }
            RecordValue tmpElem = nextElem;
            nextElem = null;
            return tmpElem;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
