/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An implmentation of the Java Executor interface that creates and
 * manages a fixed-size thread pool in a lightweight manner, with as
 * little locking as possible.
 * <p>
 * The implementation uses a non-blocking queue structure,
 * ConcurrentLinkedQueue, to hold Runnable tasks executed by threads
 * kept in a pool.
 * <p>
 * This makes insertion and removal of tasks very cheap with good
 * concurrency. If the queue is empty threads will wait on a separate
 * lock and condition that are only used when the queue size goes to
 * zero and increments from zero. This means that locking/waiting only
 * happens when the queue and Executor are fairly idle vs putting
 * locks in the path of the Executor when the queue is busy
 * <p>
 * Possible enhancements (be careful of performance impact)
 * o wait time statistics
 * o allowing for a dynamic vs fixed-size thread pool
 * o limit queue size. This would likely mean blocking in the execute() path
 * to slow down producers or rejecting execution
 *
 * TODO: consider making this a public, reusable class in this or other
 * common repository
 */
public class ProxyThreadPoolExecutor implements Executor {

    /* simple, concurrent FIFO queue for tasks */
    ConcurrentLinkedQueue<Runnable> taskQueue =
        new ConcurrentLinkedQueue<>();

    /* track the size of the queue as well as active threads */
    private final AtomicInteger queueSize = new AtomicInteger();
    private final AtomicInteger activeThreadCount = new AtomicInteger();

    /* stats for lifetime of queue */
    private final AtomicLong totalTasksProcessed = new AtomicLong();
    private final AtomicLong totalThreadWakeups = new AtomicLong();

    /* lock and condition used for waiting on the queue */
    private final ReentrantLock waitLock = new ReentrantLock();
    private final Condition available = waitLock.newCondition();

    /* used to shutdown the Executor */
    volatile private boolean active = true;
    /*
     * tracks queue size on last execute. This is volatile vs Atomic as it's
     * only used for either set or get
     */
    volatile private int lastQueueSize;

    /* the pool */
    private final Thread[] threadPool;
    private final int numThreads;

    /**
     * Create an Executor instance that uses a fixed-size thread pool to
     * execute the Runnable tasks. The pool must be shutdown using the
     * {@link #shutdown} method to clean up the queue and thread pool.
     *
     * @param numThreads the number of threads for the pool
     * @param namePrefix a string used to name the threads created for the
     * pool. The name of a thread is the prefix plus ".N" where N is an
     * integer. If this parameter is null a default prefix ("ProxyPool") is
     * used
     */
    public ProxyThreadPoolExecutor(int numThreads,
                                   String namePrefix) {
        this.numThreads = numThreads;
        if (namePrefix == null) {
            namePrefix = "ProxyPool";
        }
        if (numThreads <=0 ) {
            throw new IllegalArgumentException(
                "Pool size must be greater than zero");
        }
        threadPool = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            Thread th = new Thread(
                new ExecutorThread(), (namePrefix + "." + i));
            threadPool[i] = th;
            th.start();
        }
    }

    /**
     * Returns the current number of elements in the task queue
     * @return the size
     */
    public int getQueueSize() {
        return queueSize.get();
    }

    /**
     * Returns the number of threads actively handling a task
     * @return the number
     */
    public int getActiveThreadCount() {
        return activeThreadCount.get();
    }

    /**
     * Returns the number of queue tasks processed for the lifetime of
     * the executor
     * @return the number
     */
    public long getTotalTasksProcessed() {
        return totalTasksProcessed.get();
    }

    /**
     * Returns the number of thread wakeup calls done for the lifetime of
     * the executor
     * @return the number
     */
    public long getTotalThreadWakeups() {
        return totalThreadWakeups.get();
    }

    /**
     * Shutdown the Executor, including waiting for the threads in the
     * pool, optionally using graceful shutdown to ensure that all current
     * tasks in the queue are run. If graceful is false the threads still
     * exit but any tasks not yet run are removed from the queue and
     * ignored.
     *
     * @param graceful if true, shut down gracefully
     */
    public void shutdown(boolean graceful) {
        try {
            active = false;
            /*
             * graceful shutdown allows task queue to be emptied
             */
            if (graceful && !taskQueue.isEmpty()) {
                while (!taskQueue.isEmpty()) {
                    wakeupAll();
                }
            }
            /*
             * these are redundant for graceful shutdown but
             * do not hurt; efficiency doesn't matter in shutdown
             */
            taskQueue.clear();
            wakeupAll();

            /* wait for threads */
            for (int i = 0; i < numThreads; i++) {
                threadPool[i].join();
            }
        } catch (InterruptedException e) {
            /* ignore */
        }
    }

    /*
     * A note on synchronization between the task queue and threads that operate
     * on it. The goal is to have sufficient threads running to keep up with the
     * task queue and also avoid excessive await/signal calls that can be
     * concurrency hotspots.
     *
     * This is done by:
     * 1. tracking the "last" queue size each time a task is added
     * 2. if, on adding a task, the queue is larger than it was previously
     * wakeup a thread, but only if there aren't a number of threads already
     * running. That number is currently the size of the queue itself. This
     * number is perhaps subject to change.
     *
     * As the queue grows additional threads are adding to
     * hopefully enable the consumer threads to keep up with the producer
     * calls. At some point if the queue size grows beyond the thread pool
     * size the queue itself will keep growing, essentially without bound.
     * This means that the thread pool should be sized with the
     * producer/consumer paths in mind.
     */
    @Override
    public void execute(Runnable r) {
        if (!active) {
            throw new RejectedExecutionException(
                "Executor has been shut down");
        }
        taskQueue.add(r);
        int size = queueSize.getAndIncrement();
        totalTasksProcessed.getAndIncrement();
        int lqs = lastQueueSize;
        lastQueueSize = size;
        int activeThreads = activeThreadCount.get();

        /*
         * Wake up if any of these is true
         *  o there are no active threads
         *  o the queue is growing (i.e. number of threads isn't keeping up)
         *  o the number of active threads is <= queue size
         *
         * If all threads in the pool are active, don't bother with a wakeup
         */
        if (((activeThreads == 0 ||
              (size - lqs) > 0)) &&
            activeThreads <= size &&
            activeThreads < numThreads) {
            totalThreadWakeups.getAndIncrement();
            signalAvailable();
        }
    }

    /*
     * The wait-related methods are used when the queue is empty, allowing
     * threads to wait for a task. The lock taken in this path should not
     * be a concurrency hotspot because it's only used when the queue is
     * lightly used, indicating less load. In a busy system the queue will
     * have tasks available most of the time.
     */
    private void signalAvailable() {
        /*
         * there should be waiting threads, wake one up
         */
        try {
            waitLock.lock();
            available.signal();
        } finally {
            waitLock.unlock();
        }
    }

    private void waitForAvailable() {
        try {
            waitLock.lock();
            available.await();
        } catch (InterruptedException ie) {
            /* ignore */
        } finally {
            waitLock.unlock();
        }
    }

    /*
     * wakeup all waiting threads
     */
    private void wakeupAll() {
        try {
            waitLock.lock();
            available.signalAll();
        } finally {
            waitLock.unlock();
        }
    }

    /**
     * This is the class that is run by the pool threads. It looks for
     * Runnable tasks on the queue and runs them. If no tasks are available
     * it waits on a condition that is signaled when a task is available
     */
    private class ExecutorThread implements Runnable {
        @Override
        public void run() {
            /*
             * this conditional allows for graceful shutdown, handling
             * the queue before exiting. not-graceful shutdown is done
             * using the interrupted() status, leaving the queue with
             * entries if not empty
             */
            while (!Thread.currentThread().isInterrupted() &&
                   (active || !taskQueue.isEmpty())) {
                Runnable task = taskQueue.poll();
                if (task != null) {
                    queueSize.decrementAndGet();
                    activeThreadCount.incrementAndGet();
                    totalTasksProcessed.getAndIncrement();
                    task.run();
                    activeThreadCount.decrementAndGet();
                } else {
                    waitForAvailable();
                }
            }
        }
    }
}
