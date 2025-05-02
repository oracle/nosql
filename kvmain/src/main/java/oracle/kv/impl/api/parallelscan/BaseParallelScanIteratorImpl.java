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

package oracle.kv.impl.api.parallelscan;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static oracle.kv.impl.async.FutureUtils.unwrapExceptionVoid;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.Direction;
import oracle.kv.RequestTimeoutException;
import oracle.kv.StoreIteratorException;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.KVStoreImpl.TaskExecutor;
import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.parallelscan.ParallelScanHook.HookType;
import oracle.kv.impl.api.query.QueryPublisher;
import oracle.kv.impl.async.AsyncIterationHandleImpl;
import oracle.kv.impl.async.AsyncTableIterator;
import oracle.kv.impl.async.IterationHandleNotifier;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.util.contextlogger.LogContext;

/**
 * Implementation of a scatter-gather iterator. The iterator will access the
 * store, possibly in parallel, and sort the values before returning them
 * through the next() method.
 * <p>
 * Theory of Operation
 * <p>
 * When the iterator is created, one {@code Stream} instance is created
 * for each single range, such as a shard or partition. {@code Stream} maintains
 * state required for reading from the store, and the returned data. The
 * instances are kept in a {@code TreeSet} and remain until there are no more
 * records available from the range. How the streams are sorted is described
 * below.
 * <p>
 * When the {@code Stream} is first created it is submitted to the thread pool
 * executor. When run, it will attempt to use makeReadRequest() method to read a
 * block of records from the store. The returned data is placed on the
 * {@code blocks} queue. If there are more records in the store, it will use
 * setResumeKey method to save resume key and if there is room on the queue, the
 * stream re-submits itself to eventually read another block. There is locking
 * which will prevent the stream from being submitted more than once. There is
 * more on threads and reading below.
 * <p>
 * Sorting
 * <p>
 * {@code Stream} implements {@code Comparable}. (Note that {@code Stream} has a
 * natural ordering that is inconsistent with {@code equals}.) When a stream is
 * inserted into the {@code TreeSet} it will sort
 * (using {@code Comparable.compareTo}) on the next element in the stream. If
 * the stream does not have a next element (because the stream has been drained
 * and the read from the store has not yet returned)
 * {@code Comparable.compareTo()} will return -1 causing the stream to sort
 * to the beginning of the set. This means that the first stream in the
 * {@code TreeSet} has the overall next element or is empty waiting on data.
 * See the section below on Direction for an exception to behavior.
 * <p>
 * To get the next overall element the basic steps are as follows:
 * <p>
 * 1. Remove the first stream from the {@code TreeSet} <br>
 * 2. Remove the next element from the stream  <br>
 * 3. Re-insert the stream into the {@code TreeSet}  <br>
 * 4. If the element is not null, return  <br>
 * 5. If the element is null, wait and go back to 1  <br>
 * <p>
 * Removing the element at #2 will cause the stream to update the next
 * element with the element next in line (if any). This will cause the
 * stream to sort based on the new element when re-inserted at #3.
 * <p>
 * There is an optimization at step #5 which will skip the wait if
 * removing an element (#2) resulting in the stream having a non-null
 * next element.
 * <p>
 * Reading
 * <p>
 * Initially, each {@code Stream} is submitted to the executor. As mentioned
 * above the {@code Stream} will read a block and if there is more data
 * available and space in the queue, will re-submit itself to read more data.
 * This will result in reading {@code QUEUE_SIZE} blocks for each range. The
 * {@code ShardStream} is not re-submitted once the queue is filled. If reading
 * from the store results in an end-of-data, a flag is set preventing the stream
 * from being submitted.
 * <p>
 * Once elements are removed from the iterator and a block is removed from the
 * queue, an attempt is made to submit a stream for reading. This happens
 * at step #2 above in {@code Stream.removeNext()}. The method
 * {@code removeNext()} will first attempt to remove an element from the
 * current block. If that block is empty it removes the next block on the
 * queue, making that the current block. At this point the stream is
 * submitted to the executor.
 * <p>
 * Thread Safety
 * <p>
 * The implementations of the {@link Iterator} interface used by
 * synchronous/blocking clients are not thread-safe.  Callers are expected to
 * provide their own synchronization.  Synchronous clients do not call the
 * asynchronous API.
 * <p>
 * The implementations of the {@link AsyncTableIterator} interface used by
 * asynchronous clients need to provide a somewhat restricted form of thread
 * safety.  The callers of the API are required to provide external
 * synchronization: {@link AsyncIterationHandleImpl} is the caller and it
 * provides the needed synchronization.  Implementations need to make sure that
 * the interaction between those API calls and the processing of new data
 * received from the server is handled safely.  In this class, that only
 * requires the use of volatile (read) and synchronization (write) for access
 * to the closed and closeException fields, since safe access to element data
 * is already provided.
 * <p>
 * The {@link Stream} nested class documents its own synchronization, which is
 * part of the support for safe access to element data.
 * <p>
 * Subclasses of this class and the Stream class need to provide thread safety
 * between processing results returned from the server and access via calls to
 * AsyncTableIterator API.
 *
 * @param <K> the element type
 */
public abstract class BaseParallelScanIteratorImpl<K>
        implements AsyncTableIterator<K> {

    /*
     * Use to convert, instead of TimeUnit.toMillis(), when you don't want
     * a negative result.
     */
    private static final long NANOS_TO_MILLIS = 1000000L;

    /* Time to wait for data to be returned from the store */
    private static final long WAIT_TIME_MS = 100L;

    /*
     * The default size of the queue of blocks in each stream. Note that the
     * maximum number of blocks maintained by the stream is QUEUE_SIZE + 1.
     * (The queue plus the current block).
     */
    private static final int QUEUE_SIZE = 3;

    protected final KVStoreImpl storeImpl;

    protected final Logger logger;

    protected final long requestTimeoutMs;

    /**
     * The amount of time in milliseconds to allow for roundtrip network
     * communication with the server. Wait this additional amount of time, if
     * any, before timing out a request, so that async calls can provide better
     * exceptions.
     */
    private final long networkRoundtripTimeoutMs;

    protected final Direction itrDirection;

    /**
     * This field is set in the constructor, but is not final to allow
     * subclasses more flexibility when initializing it.  Synchronized when
     * setting to make sure it is only set once.
     */
    private volatile TaskExecutor taskExecutor;

    /*
     * The sorted set of streams. Only streams that have elements or are
     * waiting on reads are in the set. Streams that have exhausted the store
     * are discarded. Access to the set must be synchronized, because is is
     * read/modified by the producer/iterator thread and may also be modified
     * by the stream threads if an elasticity operation is going on concurrently
     * (to add new streams for virtual shard scans).
     */
    protected final TreeSet<Stream> streams = new TreeSet<Stream>();

    /*
     * True if the iterator has been closed.  Synchronize when setting this
     * field and only set it after setting closeException so that the correct
     * close exception is always supplied if the iterator is marked closed.
     */
    protected volatile boolean closed = false;

    /*
     * The exception passed to close(Throwable).  Synchronize when setting this
     * field and always set it before setting closed so that the correct close
     * exception is always supplied if the iterator is marked closed.
     */
    private volatile Throwable closeException = null;

    /*
     * The next element to be returned from this iterator. This may be null if
     * waiting for reads to complete.  The field is volatile so that async
     * calls to close from different threads will clear it properly.
     */
    private volatile K next = null;

    /*
     * The size of queue of blocks in each stream, defaults to QUEUE_SIZE.
     */
    final int maxResultsBatches;

    /**
     * The object to notify when an async iterator has new results or is
     * closed, or null if synchronous or this BaseParallelScanIteratorImpl
     * is used by a query.
     */
    final IterationHandleNotifier iterHandleNotifier;

    /*
     * Like iterHandleNotifier, but used by async queries only.
     */
    final QueryPublisher theQueryPublisher;

    /**
     * A semaphore that controls the number of currently executing async tasks.
     */
    private final Semaphore asyncTaskPermits = new Semaphore(0);

    /**
     * A queue of pending async tasks that are waiting for an async task permit
     * in order to run.
     */
    private final BlockingQueue<Stream> pendingAsyncTasks =
        new LinkedBlockingQueue<>();

    private final boolean prefetch;

    protected final LogContext lc;
    protected final AuthContext auth;
    protected final boolean noCharge;

    /**
     * Creates an instance of this class.
     */
    protected BaseParallelScanIteratorImpl(
        KVStoreImpl storeImpl,
        Logger logger,
        long requestTimeoutMs,
        Direction itrDirection,
        int maxResultsBatches,
        boolean prefetch,
        IterationHandleNotifier iterHandleNotifier,
        QueryPublisher publisher,
        LogContext lc,
        AuthContext auth,
        boolean noCharge) {

        assert(iterHandleNotifier == null || publisher == null);

        this.storeImpl = storeImpl;
        this.logger = logger;
        this.requestTimeoutMs = requestTimeoutMs;
        networkRoundtripTimeoutMs =
            storeImpl.getDispatcher().getNetworkRoundtripTimeoutMs();
        this.itrDirection = itrDirection;
        this.maxResultsBatches =
            (maxResultsBatches > 0 ? maxResultsBatches : QUEUE_SIZE);
        this.prefetch = prefetch;
        this.iterHandleNotifier = iterHandleNotifier;
        theQueryPublisher = publisher;
        this.lc = lc;
        this.auth = auth;
        this.noCharge = noCharge;
    }

    /*
    public void trace(String msg) {
        System.out.println(Clock.systemUTC().instant().
                           truncatedTo(ChronoUnit.MILLIS) +
                           " PARALLEL SCAN" +
                           "(" + Thread.currentThread().getId() +
                           ") : " + msg);
    }
    */

    boolean isAsync() {
        return iterHandleNotifier != null || theQueryPublisher != null;
    }

    /* -- From Iterator -- */

    @Override
    public boolean hasNext() {
        if (next == null) {
            next = getNext();
        }
        return (next != null);
    }

    @Override
    public K next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final K lastReturned = next;
        next = null;
        return lastReturned;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    /* -- From ParallelScanIterator -- */

    @Override
    public void close() {
        close(null);
    }

    /* -- From AsyncIteration -- */

    @Override
    public K nextLocal() {

        /*
         * In the async case, hasNext doesn't block, so it amounts to
         * "hasNextLocal", but kicks off an asynchronous request for more items
         * if needed
         */
        if (!hasNext()) {
            return null;
        }
        final K lastReturned = next;
        next = null;
        return lastReturned;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public StoreIteratorException getCloseException() {
        return (closed && (closeException != null)) ?
            new StoreIteratorException(closeException, null) :
            null;
    }


    /* -- Other methods -- */

    protected long getTimeout() {
        return requestTimeoutMs;
    }

    /**
     * Gets the next value in sorted order. If no more values remain or the
     * iterator is canceled, null is returned. In synchronous operation, this
     * method will block waiting for data from the store. For asynchronous
     * operation, returns null if there are no more elements available locally,
     * notifying the iteration handle when more results become available, and
     * closes the iteration when all results have been returned.
     *
     * @return the next value in sorted order or null
     */
    private K getNext() {

        final long timeout = getTimeout();

        final long limitNs = System.nanoTime() +
                             MILLISECONDS.toNanos(timeout +
                                                  networkRoundtripTimeoutMs);

        while (!isClosed()) {

            /*
             * The first stream in the set will contain the next
             * element in order.
             */
            final Stream stream;
            synchronized (streams) {
                stream = streams.pollFirst();
            }

            /* If there are no more streams we are done. */
            if (stream == null) {
                close();
                return null;
            }

            /*
             * If the stream is empty, the removeNext() call will submit the
             * stream for execution, so that it will (when actually executed)
             * send a request to an RN for more results.
             */
            final K entry = stream.removeNext();

            /*
             * Return the stream back to the set where it will sort on the
             * next element.
             */
            if (!stream.isDone()) {
                synchronized (streams) {
                    streams.add(stream);
                }
            }

            /*
             * removeNext() may have waited or thrown an exception so check
             * for closed.
             */
            if (isClosed()) {
                break;
            }

            if (entry != null) {
                return entry;
            }

            /* If the current stream is done, try the next one */
            if (stream.isDone()) {
                continue;
            }

            /*
             * If the stream is no longer empty, see if we are ready to select
             * the next element
             */
            if (stream.hasNextElem()) {
                continue;
            }

            long waitMs =
                   Math.min((limitNs - System.nanoTime()) / NANOS_TO_MILLIS,
                            WAIT_TIME_MS);

            if (waitMs <= 0) {
                throw new RequestTimeoutException
                    ((int)timeout,
                     ("Operation timed out on shard: " + stream),
                     getCloseException(), false);
            }

            /*
             * In the async case, if the current stream is not done but is
             * currently empty, then we have submitted another request for it
             * and will be called back when more items are available.
             */
            if (isAsync()) {
                return null;
            }

            /*
             * Put the consumer thread to sleep for 100ms (WAIT_TIME_MS) or
             * less if the remaing time until the timeout is < 100ms. When
             * the thread wakes up it will check the streams again for new
             * results.
             */
            stream.waitForNext(waitMs);
        }
        /* Closed */
        final StoreIteratorException ce = getCloseException();
        if (ce != null) {
            throw ce;
        }
        return null;
    }

    /**
     * Close the iterator, recording the specified remote exception. If
     * the reason is not null, the exception is thrown from the hasNext()
     * or next() methods.
     *
     * <p>This method sets the closed field and closeReason if closed isn't
     * already set
     *
     * @param reason the exception causing the close or null
     * @return whether the iterator was closed by this call; returns false if
     * it was already closed
     */
    protected boolean close(Throwable reason) {
        synchronized (this) {
            if (closed) {
                return false;
            }

            /*
             * Set closeException first so we are sure it is set if something
             * reads it (without synchronization) after finding closed==true.
             */
            closeException = reason;
            closed = true;
        }
        next = null;
        if (isAsync()) {
            if (iterHandleNotifier != null) {
                iterHandleNotifier.notifyNext();
            } else {
                synchronized (theQueryPublisher.getLock()) {
                    theQueryPublisher.notifySubscriber(false);
                }
            }
        }
        return true;
    }

    /**
     * Compares the two elements. Returns a negative integer, zero, or a
     * positive integer if object one is less than, equal to, or greater
     * than object two.
     *
     * @return a negative integer, zero, or a positive integer
     */
    protected abstract int compare(K one, K two);

    /**
     * Convert the given Result obj (storing results received from the server)
     * into list of elements (the client version of the same results).
     *
     * If a RuntimeException is thrown, the iterator will be closed and the
     * exception return to the application.
     *
     * NOTE: If a server result (in Result) fails to convert to a client result,
     * this method MUST put a null in the elementList. This is important because
     * Stream.currentBlock and Stream.currentResultSet MUST have the same number
     * of results. A server result may fail to convert to a client result
     * because the server may return records that do not actually belong to the
     * table being scanned.
     *
     * @param result result object
     * @param elementList list to place the converted elements
     */
    protected abstract void convertResult(
        Result result,
        List<K> elementList);

    /**
     * Returns the task executor.
     */
    protected TaskExecutor getTaskExecutor() {
        if (taskExecutor == null) {
            throw new IllegalStateException("No task executor");
        }
        return taskExecutor;
    }

    /**
     * Sets the task executor to allow the specified number of concurrent
     * tasks.  This method must be called before any iterations are performed
     * and will throw an exception if called more than once.
     *
     * @param maxConcurrentTasks the maximum number of concurrent tasks
     */
    protected void setTaskExecutor(int maxConcurrentTasks) {
        synchronized (this) {
            if (taskExecutor != null) {
                throw new IllegalStateException(
                    "Task executor has already been set");
            }
            taskExecutor = storeImpl.getTaskExecutor(maxConcurrentTasks);
            asyncTaskPermits.release(maxConcurrentTasks);
        }
    }

    /**
     * Runs the specified asynchronous task, running it immediately if doing so
     * would not exceed the maximum concurrent tasks limit, or else queuing it
     * to run later.
     */
    void runAsync(final Stream task) {
        if (asyncTaskPermits.tryAcquire()) {
            task.run();
            return;
        }

        pendingAsyncTasks.add(task);

        /*
         * See if we can run a task now in case there was a race between adding
         * the task to the queue and a completed task releasing a permit.  Note
         * that the queued task might not be the one we just added in some
         * cases.
         */
        if (asyncTaskPermits.tryAcquire()) {
            final Stream t = pendingAsyncTasks.poll();
            if (t == null) {
                asyncTaskPermits.release();
            } else {
                t.run();
            }
        }
    }

    /**
     * After a task is done, try running another task if one is available in
     * the queue, or else release the permit.  We submit the task rather than
     * running it in place to keep the call stack from getting too deep.
     */
    void asyncTaskDone() {
        final Stream t = pendingAsyncTasks.poll();
        if (t == null) {
            asyncTaskPermits.release();
        } else {
            try {
                taskExecutor.submit(t);
            } catch (RejectedExecutionException ree) {
                /* Probably already closed, but just in case */
                close(ree);
            }
        }
    }

    /**
     * Object that encapsulates the activity around reading records of a single
     * range, such as a shard or partition.
     *
     * Note: this class has a natural ordering that is inconsistent with
     * equals.
     *
     * blocks :
     * The queue of unconverted server results (queue of Result objs) waiting to
     * be consumed). NOTE: blocks is always accessed in synchronized code
     * (except in assertions).
     *
     * currentResultSet :
     * The latest block of server results that have been converted to client
     * results (which are stored in this.currentBlock). currentResultSet is
     * not in this.blocks. NOTE: currentResultSet is accessed only by the
     * consumer/iterator thread.
     *
     * currentBlock :
     * The block of elements (converted server results) currently being consumed.
     * NOTE: currentBlock is accessed only by the consumer/iterator thread.
     *
     * currentResultPos :
     * Serves as an index into currentResultSet, pointing to the server version
     * of the client result pointed by nextElem.
     *
     * nextElem :
     * The next element to be given by this stream to the consuming iterator.
     * NOTE: nextElem is accessed only by the consumer/iterator thread.
     *
     * NOTE: nextElem or currentResultPos are used to sort the streams in the
     * "streams" TreeSet of the containing parallel iterator obj. As a result,
     * nextElem and currentResultPos must (and are indeed) updated only when
     * "this" is not a member of the "streams" TreeSet.
     *
     * doneReading :
     * True if there are no more results to be fetched from the server by this
     * stream. The stream may still have locally cached results, so this.done
     * may be false while this.doneReading is true.
     * NOTE: doneReading is always accessed in synchronized code (except in
     * assertions)
     *
     * done :
     * True if this stream has no more results to return.
     * NOTE: done is always set in synchronized code. However, it is read in
     * non-synchronized code via the isDone() method. This should be OK because
     * it is accessed by the consumer/iterator thread.
     *
     * active :
     * True if this stream has been submitted to fetch more results from the
     * server.
     */
    public abstract class Stream implements Comparable<Stream>, Runnable {

        /* The block of values being drained */
        private final BlockingQueue<Result> blocks =
            new LinkedBlockingQueue<Result>(maxResultsBatches);

        /* The block of server results being drained */
        protected Result currentResultSet;

        /* The block of values being drained */
        private List<K> currentBlock;

        /* Position of the "current" server result inside currentResultSet */
        protected int currentResultPos = -1;

        /* The last element removed, used for sorting */
        private K nextElem = null;

        /* True if nothing left to read from the server */
        protected boolean doneReading = false;

        /* True if there are no more values */
        private boolean done = false;

        /* True if this stream is active */
        protected boolean active = false;

        /**
         * Remove the next element from this stream and return it. If no
         * elements are available null is returned.
         *
         * Method run by the consumer thread only.
         *
         * @return the next element from this stream or null
         */
        K removeNext() {

            assert !done;

            final K ret = nextElem;

            if (currentBlock != null && !currentBlock.isEmpty()) {
                do {
                    nextElem = currentBlock.remove(0);
                    ++currentResultPos;
                    /*
                     * nextElem may be null here because a server result failed
                     * to convert to a client result (see comment on
                     * BaseParallelScanIteratorImpl.convertResult method above).
                     */
                } while (nextElem == null && !currentBlock.isEmpty());
            } else {
                nextElem = null;
            }

            /*
             * If there are no more results in the current block, attempt
             * to get a new block.
             */
            while (nextElem == null) {

                synchronized (this) {

                    /*
                     * Retrieve and remove the Result at the head of the queue,
                     * returning null if the queue is empty.
                     */
                    currentResultSet = blocks.poll();
                    currentBlock = null;
                    currentResultPos = -1;

                    /*
                     * If there are no more cached blocks break out of this loop
                     * and return null. The iterator will then put this stream
                     * back to the tree set and wait for more server results to
                     * arrive in this stream. However, if there are no more
                     * results to be fetched from the server (ie, doneReading is
                     * true), set done to true to signal that this stream is
                     * finished.
                     */
                    if (currentResultSet == null) {
                        submit();
                        done = doneReading;
                        break;
                    } else if (prefetch) {
                        submit();
                    }
                }

                int nRecords = currentResultSet.getNumRecords();

                /*
                 * Convert the Result into a list of elements ready to be
                 * returned from the iterator. If a server fails to convert,
                 * null is placed in the element list.
                 */
                if (nRecords > 0) {
                    currentBlock = new ArrayList<K>(nRecords);
                    convertResult(currentResultSet, currentBlock);
                    assert(nRecords == currentBlock.size());

                    do {
                        nextElem = currentBlock.remove(0);
                        ++currentResultPos;
                    } while (nextElem == null && !currentBlock.isEmpty());
                }
            }

            return ret;
        }

        /** Returns whether the stream current has a next element. */
        boolean hasNextElem() {
            return nextElem != null;
        }

        /**
         * Waits up to waitMs for the next element to be available.
         *
         * Method run by the consumer thread only, when it needs to get a
         * result from this stream in order to proceed, and this stream has
         * run out of cached results.
         *
         * @param waitMs the max time in ms to wait
         */
        void waitForNext(long waitMs) {

            assert(iterHandleNotifier == null && theQueryPublisher == null);

            try {
                synchronized (this) {
                    /* Wait if there is no data left and still reading */
                    if (blocks.isEmpty() && !doneReading) {
                        wait(waitMs);
                    }
                }
            } catch (InterruptedException ex) {
                if (!closed) {
                    logger.log(Level.WARNING, "Unexpected interrupt ", ex);
                }
            }
        }

        /**
         * Returns true if all of the elements have been removed from this
         * stream.
         *
         * Method run by the consumer thread only
         */
        boolean isDone() {
            return done;
        }

        /**
         * Submit this stream to request data if (a) it isn't already submitted
         * (active), and (b) there is more server data to read, and (c) there
         * is room for it.
         *
         * This method is called from both the consumer/iterator thread and
         * the producer thread:
         *
         *  - The producer thread resubmits the stream after fetching a block
         *    of results
         *  - The iterator thread does the initial submit on each stream, and
         *    then it submits a stream after its currentBlock becomes empty.
         */
        public void submit() {

            synchronized (this) {
                if (active ||
                    doneReading ||
                    blocks.remainingCapacity() == 0 ||
                    closed) {
                    return;
                }

                active = true;
            }

            if (isAsync()) {
                runAsync(this);
                return;
            }

            try{
                getTaskExecutor().submit(this);
            } catch (RejectedExecutionException ree) {
                setActive(false);
                close(ree);
            }
        }

        synchronized protected void setActive(boolean value) {
            active = value;
        }

        /**
         *  Method run by the producer thread only
         */
        @Override
        public final void run() {

            try {
                assert active;
                assert !doneReading;
                assert blocks.remainingCapacity() > 0;
                readBlock();
            } catch (Throwable e) {

                /*
                 * Close on all exceptions so we don't need to worry about
                 * maintaining asyncTaskPermits once an unexpected exception is
                 * thrown
                 */
                setActive(false);
                close(e);
            }
        }

        /**
         * Read a block of records from the store and updates metrics.
         *
         * Method run by the producer thread only
         */
        private void readBlock() {

            final long start = System.nanoTime();
            final Request req = makeReadRequest();

            if (req == null) {
                synchronized (this) {
                    /* About to exit, active may be reset in submit() */
                    active = false;
                    doneReading = true;
                    /* Wake up the iterator if it is waiting */
                    notifySync();
                }
                notifyAsync();
                submitDetailedMetrics(start, 0);
                if (isAsync()) {
                    asyncTaskDone();
                }
                return;
            }

            req.setLogContext(lc);
            req.setAuthContext(auth);
            req.setNoCharge(noCharge);

            assert storeImpl.getParallelScanHook() == null ?
                true :
                storeImpl.getParallelScanHook().
                callback(Thread.currentThread(),
                         HookType.BEFORE_EXECUTE_REQUEST, null);

            if (isAsync()) {

                /*
                 * Use the asynchronous backend to kick off the request and
                 * return. The handler will process the result or exception and
                 * submit the next task as necessary.
                 */
                storeImpl.executeRequestAsync(req)
                    .whenComplete(unwrapExceptionVoid((result, exception) ->
                    {
                        try {
                            asyncTaskDone();
                            if (exception != null) {
                                handleException(exception);
                            } else {
                                processResult(start, result);
                            }
                        } catch (Throwable t) {
                            storeImpl.getExceptionHandler()
                                .uncaughtException(Thread.currentThread(), t);
                        }
                    }));
            } else {
                try {
                    Result result = storeImpl.executeRequest(req);
                    processResult(start, result);
                } catch (Throwable e) {
                    handleException(e);
                }
            }
        }

        protected void handleException(Throwable e) {
            setActive(false);
            close(e);
        }

        /**
         * Notify the synchronous iterator that more results are available.
         * This method must be called while synchronized on the Stream
         * instance.
         */
        protected void notifySync() {
            assert Thread.holdsLock(this);
            if (!isAsync()) {
                notify();
            }
        }

        /**
         * Notify the asynchronous iteration handle that more results are
         * available.  This method should be called without holding the lock on
         * the Stream instance, to avoid deadlocks.
         */
        protected void notifyAsync() {
            assert !Thread.holdsLock(this);
            if (isAsync()) {
                if (iterHandleNotifier != null) {
                    iterHandleNotifier.notifyNext();
                } else {
                    synchronized (theQueryPublisher.getLock()) {
                        theQueryPublisher.notifySubscriber(true);
                    }
                }
            }
        }

        void processResult(long start, Result result) {

            final boolean hasMore = hasMoreElements(result);

            int nRecords = result.getNumRecords();

            setResumeKey(result);

            synchronized (this) {
                /* About to exit, active may be reset in submit() */
                active = false;
                doneReading = !hasMore;

                if (nRecords == 0) {
                    assert storeImpl.getParallelScanHook() == null ?
                           true :
                           storeImpl.getParallelScanHook().
                           callback(Thread.currentThread(),
                                    HookType.AFTER_PROCESSING_STREAM, null);
                } else {
                    blocks.add(result);
                }
                /* Wake up the iterator if it is waiting */
                notifySync();
            }
            notifyAsync();

            if (hasMore && prefetch) {
                submit();
            }

            submitDetailedMetrics(start, nRecords);
        }

        /**
         * Update resume key for next read request.
         * @param result result object
         */
        protected abstract void setResumeKey(Result result);

        /**
         * Create a request using resume key to get next block of records.
         * The read request may return null for bulk get operations when the
         * keys supplied by the application run out and no further requests to
         * the store are required.
         *
         * see KVStore.storeIterator(Iterator, int, KeyRange, Depth,
         *                           Consistency, long, TimeUnit,
         *                           StoreIteratorConfig)
         */
        protected abstract Request makeReadRequest();

        protected boolean hasMoreElements(Result result) {
            return result.hasMoreElements();
        }

        /**
         * Compares this object with the specified object for order for
         * determining the placement of the stream in the TreeSet. See
         * the IndexScan class doc a detailed description of its operation.
         */
        @Override
        public int compareTo(Stream other) {
            /*
             * The same stream is always equal. This is the only time that
             * 0 can be returned.
             */
            if (this == other) {
                return 0;
            }

            /*
             * If unordered, we skip comparing the streams and always sort
             * to the top if we have a next element. If no elements, sort
             * to the bottom. This will keep full streams at the top and
             * empty streams at the bottom.
             */
            if (itrDirection == Direction.UNORDERED) {
                return (nextElem == null) ? 1 : -1;
            }

            /* Forward or reverse */

            /* If we don't have a next, then sort to the top of the set. */
            if (nextElem == null) {
                return -1;
            }

            /* If the other next is null then keep them on top. */
            final K otherNext = other.nextElem;
            if (otherNext == null) {
                return 1;
            }

            /*
             * Finally, compare the elements, inverting the result
             * if necessary.
             */
            final int comp = compareInternal(other);

            /*
             * If the different stream elements are equal then there is
             * a duplicate entry, possibly due to partition migration. We
             * currently don't handle is case, so punt.
             */
            if (comp == 0) {
                close(new IllegalStateException("Detected an unexpected " +
                                                "duplicate record"));
            }

            return comp;
        }

        /**
         * Subclasses may override this
         */
        protected int compareInternal(Stream other) {
            int cmp = compare(nextElem, other.nextElem);
            return itrDirection == Direction.FORWARD ? cmp : (cmp * -1);
        }

        /**
         * Get stream status string.
         */
        public synchronized String getStatus() {
            return done + ", " + active + ", " + doneReading + ", "
                   + blocks.size();
        }

        /**
         * Update the metrics for this iterator.
         *
         * @param timeInMs the time spent reading
         * @param recordCount the number of records read
         */
        protected abstract void updateDetailedMetrics(
            final long timeInMs,
            final long recordCount);

        private void submitDetailedMetrics(long start, int nRecords) {
            final long end = System.nanoTime();
            final long thisTimeMs = (end - start) / NANOS_TO_MILLIS;
            updateDetailedMetrics(thisTimeMs, nRecords);
        }
    }
}
