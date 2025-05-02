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

package oracle.kv.impl.util;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A substitute for {@link ThreadPoolExecutor} that reuses existing core
 * threads rather than creating new ones. Only creates new threads if no free
 * threads are available.
 */
public class ReusingThreadPoolExecutor extends ThreadPoolExecutor {

    /** Queue of requests waiting for a free thread to be executed. */
    private final BlockingQueue<Runnable> queue;

    /** The queue capacity. */
    private final int queueCapacity;

    /**
     * The number of execute operations currently being performed. Use the
     * count to determine if there is a thread that will notice items added to
     * the queue.
     */
    private final AtomicInteger executingCount = new AtomicInteger();

    /**
     * The number of times an execute call needed to be retried because of the
     * timing window between when a task is done and when the executor is ready
     * for the next task.
     */
    private final AtomicInteger retryCount = new AtomicInteger();

    /**
     * Creates an instance of this class.
     *
     * @param corePoolSize the number of threads to keep in the pool, even if
     * they are idle
     * @param maxPoolSize the maximum number of threads to allow in the pool
     * @param keepAliveTime when the number of threads is greater than the
     * core, this is the maximum time that excess idle threads will wait for
     * new tasks before terminating.
     * @param unit the time unit for the {@code keepAliveTime} argument
     * @param queueCapacity the capacity of the work queue
     * @param threadFactory the factory to use when the executor creates a new
     * thread
     * @throws IllegalArgumentException if: <ul>
     * <li> {@code corePoolSize < 0}
     * <li> {@code maxPoolSize <= 0}
     * <li> {@code keepAliveTime < 0}
     * <li> {@code queueCapacity < 1}
     * <li> {@code maxPoolSize < corePoolSize}
     * </ul>
     */
    public ReusingThreadPoolExecutor(int corePoolSize,
                                     int maxPoolSize,
                                     long keepAliveTime,
                                     TimeUnit unit,
                                     int queueCapacity,
                                     ThreadFactory threadFactory) {
        super(corePoolSize, maxPoolSize, keepAliveTime, unit,

              /*
               * Use a synchronous queue so that the executor's queue blocks if
               * no more core threads are available, which causes new threads
               * to be created up to the max pool size.
               */
              new SynchronousQueue<>(), threadFactory);
        if (queueCapacity < 1) {
            throw new IllegalArgumentException(
                "queueCapacity must be greater than 0");
        }
        this.queueCapacity = queueCapacity;
        queue = new ArrayBlockingQueue<>(queueCapacity);
        setRejectedExecutionHandler(new AddToQueueHandler());
    }

    /**
     * Handle rejected execution by attempting to add the task to the queue.
     */
    private class AddToQueueHandler extends AbortPolicy {
        @Override
        public void rejectedExecution(Runnable r,
                                      ThreadPoolExecutor executor) {
            if (isShutdown()) {
                super.rejectedExecution(r, executor);
            }

            /*
             * If the queue is empty and there are no threads currently
             * executing, then there was a race between when the last task was
             * done and when the executor was ready for the next task. Try
             * again.
             */
            if (queue.isEmpty() && (executingCount.get() == 0)) {
                retryCount.incrementAndGet();
                throw new RetryExecutionException();
            }

            /* Reject if we can't add to the queue */
            if (!queue.offer(r)) {
                super.rejectedExecution(r, executor);
            }
        }
    }

    /** Private exception used to request retries. */
    private static class RetryExecutionException extends RuntimeException {
        private static final long serialVersionUID = 1;
    }

    /** Returns the capacity of the work queue. */
    public int getQueueCapacity() {
        return queueCapacity;
    }

    /**
     * Returns the number of times the execute operation was retried because
     * of a race between task completion and being ready for the next
     * operation.
     */
    int getRetryCount() {
        return retryCount.get();
    }

    /** Retry if RetryExecutionException is thrown. */
    @Override
    public void execute(Runnable command) {
        while (true) {
            try {
                super.execute(command);
                return;
            } catch (RetryExecutionException e) {

                /*
                 * Retry execution, giving a hint to the scheduler to allow
                 * other threads to run since we want the next executor thread
                 * to have a chance to get ready. This is a busy loop: we
                 * expect that the only reason we need to retry is because of a
                 * thread race, so the actual number of retries should be
                 * limited.
                 */
                Thread.yield();
            }
        }
    }

    /** Increment executingCount. */
    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        executingCount.incrementAndGet();
    }

    /** Try executing tasks in the queue when a previous task is done. */
    @Override
    protected void afterExecute(Runnable r, Throwable t) {

        /*
         * Check for tasks in a loop so that we notice additional task after
         * running the first one. Note that afterExecute will not be called for
         * tasks run here, so we need to check explicitly for more tasks after
         * running each one. Since we know afterExecute will not be called when
         * we run a task, we don't need to worry about recursion.
         */
        while (!isShutdown()) {

            /*
             * Decrement the executing count before polling so there is no race
             * between polling the queue and noting that this thread is no
             * longer checking the queue.
             *
             * If we didn't maintain and check the executing count, then a race
             * could occur where queue.poll() could return null but, before the
             * final executing thread either exited or began waiting on the
             * thread pool's underlying synchronous queue, a new item could
             * arrive. In this case, because the maximum number of threads is
             * currently running, no new thread will be launched, and no
             * existing thread will notice that there is an item in the queue.
             * This is mostly a problem if there is a pause in requests right
             * at this moment. Otherwise, after a new request is executed, and
             * the thread would notice the item in the queue before going back
             * to sleep.
             */
            executingCount.decrementAndGet();
            final Runnable next = queue.poll();
            if (next == null) {
                break;
            }
            executingCount.incrementAndGet();
            next.run();
        }
    }

    @Override
    public String toString() {
        return super.toString() +
            "[" + " queueCapacity=" + queueCapacity + "]";
    }
}
