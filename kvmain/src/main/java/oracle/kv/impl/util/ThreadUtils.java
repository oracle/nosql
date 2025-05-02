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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Miscellaneous utilities for the {@link Thread} class. */
@NonNullByDefault
public class ThreadUtils {

    /** Prevent instantiation. */
    private ThreadUtils() { }

    /**
     * Returns the identifier for the thread. Use this method rather than
     * {@link Thread#getId} since that method is deprecated in Java 20.
     *
     * @param thread the thread
     * @return the thread ID
     */
    public static long threadId(Thread thread) {
        return threadIdInternal(thread);
    }

    /**
     * Call the deprecated Thread.getId method in a separate, private, method
     * that is itself deprecated to avoid warnings.
     */
    @Deprecated
    private static long threadIdInternal(Thread thread) {
        /* Use Thread.threadId when we switch to Java 19. */
        return thread.getId();
    }

    /**
     * A {@link ThreadPoolExecutor} that implements {@code close} and {@link
     * AutoCloseable}. That is what ThreadPoolExecutor does in Java 19, and
     * using a version that does so now makes it easier to write code that
     * compiles with previous Java versions but doesn't get resource leak
     * warnings with Java 20.
     */
    /*
     * Suppress all warnings so that Eclipse doesn't complain under Java 19 and
     * later that the AutoCloseable is redundant.
     */
    @SuppressWarnings("all")
    public static class ThreadPoolExecutorAutoClose
            extends ThreadPoolExecutor
            implements AutoCloseable {

        /**
         * Creates a new instance with the given initial parameters, the
         * default thread factory and the default rejected execution handler.
         *
         * @param corePoolSize the number of threads to keep in the pool, even
         * if they are idle, unless {@code allowCoreThreadTimeOut} is set
         * @param maximumPoolSize the maximum number of threads to allow in the
         * pool
         * @param keepAliveTime when the number of threads is greater than the
         * core, this is the maximum time that excess idle threads will wait
         * for new tasks before terminating.
         * @param unit the time unit for the {@code keepAliveTime} argument
         * @param workQueue the queue to use for holding tasks before they are
         * executed. This queue will hold only the {@code Runnable} tasks
         * submitted by the {@code execute} method.
         * @throws IllegalArgumentException if one of the following holds:<br>
         * {@code corePoolSize < 0}<br>
         * {@code keepAliveTime < 0}<br>
         * {@code maximumPoolSize <= 0}<br>
         * {@code maximumPoolSize < corePoolSize}
         * @throws NullPointerException if {@code workQueue} is null
         */
        public ThreadPoolExecutorAutoClose(int corePoolSize,
                                           int maximumPoolSize,
                                           long keepAliveTime,
                                           TimeUnit unit,
                                           BlockingQueue<Runnable> workQueue) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit,
                  workQueue);
        }

        /**
         * Creates a new instance with the given initial parameters, the
         * specified thread factory, and the default rejected execution
         * handler.
         *
         * @param corePoolSize the number of threads to keep in the pool, even
         * if they are idle, unless {@code allowCoreThreadTimeOut} is set
         * @param maximumPoolSize the maximum number of threads to allow in the
         * pool
         * @param keepAliveTime when the number of threads is greater than the
         * core, this is the maximum time that excess idle threads will wait
         * for new tasks before terminating.
         * @param unit the time unit for the {@code keepAliveTime} argument
         * @param workQueue the queue to use for holding tasks before they are
         * executed. This queue will hold only the {@code Runnable} tasks
         * submitted by the {@code execute} method.
         * @param threadFactory the factory to use when the executor creates a
         * new thread
         * @throws IllegalArgumentException if one of the following holds:<br>
         * {@code corePoolSize < 0}<br>
         * {@code keepAliveTime < 0}<br>
         * {@code maximumPoolSize <= 0}<br>
         * {@code maximumPoolSize < corePoolSize}
         * @throws NullPointerException if {@code workQueue} or {@code
         * threadFactory} is null
         */
        public ThreadPoolExecutorAutoClose(int corePoolSize,
                                           int maximumPoolSize,
                                           long keepAliveTime,
                                           TimeUnit unit,
                                           BlockingQueue<Runnable> workQueue,
                                           ThreadFactory threadFactory) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit,
                  workQueue, threadFactory);
        }

        /**
         * For now, just call shutdown. In Java 19, the real close waits for
         * the executor to terminate, but this seems good enough for now. In
         * fact, figure out if this is better: maybe we don't want to wait?
         */
        @Override
        public void close() {
            shutdown();
        }
    }
}
