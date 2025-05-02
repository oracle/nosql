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

package oracle.kv.impl.api.rgstate;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.RateLimitingLogger;

/**
 * A subclass of ThreadPoolExecutor used to manage tasks that need to
 * communicate with services such as RNs. The intent of this executor is to
 * ensure that no task is held up due to Head Of Line (HOL) blocking resulting
 * from one or more of the services being slow to respond, or from network
 * connection issues, like tasks being blocked on socket timeouts.
 *
 * HOL blocking is avoided by the following two mechanisms implemented by the
 * executor:
 *
 * 1) There is at most one task associated with each service in the pool so
 * that a network problem with a single service does not end up consuming all
 * threads, as successive threads all block trying to accomplish tasks with the
 * same servbice. This is accomplished via the new {@link #execute(UpdateTask)}
 * method.
 *
 * 2) The executor dynamically adjusts threads ({@link #tunePoolSize(int)}, so
 * that a minimal number of threads are used when all is normal and more
 * threads are made available for running tasks when the current configuration
 * of threads results in HOL blocking.
 */
public class UpdateThreadPoolExecutor extends ThreadPoolExecutor {

    /**
     * The default core pool size.
     *
     * A single thread should typically be sufficient to make a pass over all
     * the nodes in a store, since each request should take the order of 1-2ms.
     * So roughly 500-100 request/sec from the single core thread.
     */
    private static int CORE_POOL_SIZE = 1;

    /**
     * The size of the thread pool queue. To avoid task rejection it should
     * have an entry for each RN in the store. Unfortunately, the size cannot
     * be changed dynamically, and so we change maxPooolThreads when we detect
     * that tasks are being rejected.
     */
    private static int QUEUE_SIZE = 300;

    /**
     * The thread count at which we start using an incremental percentage to
     * increase the thread count.
     */
    private static int THREAD_INC_START_THRESHOLD = 16;

    /**
     * The thread limits for the core pool and the max are increased in small
     * steps. This constant determines the size of the step.
     */
    private static int THREAD_INC_PERCENT = 10;

    /**
     * Counts used to determine thread limit settings.
     */
    private volatile int rejectCount = 0;

    private volatile int executeCount = 0;

    /**
     * The map from services (e.g. RNs) to active tasks. It's used to ensure
     * that there is at most one task associated with each service. An entry is
     * added to this map when an execute() request is made and it's removed
     * when the task finishes, or is rejected.
     */
    private final Map<ResourceId, UpdateTask> activeUpdates =
        new ConcurrentHashMap<ResourceId, UpdateTask>();

    /* counts maintained for testing. */
    private volatile int maxCorePoolSize;

    private volatile int maxMaxPoolSize;

    private final RateLimitingLogger<String> logger;

    UpdateThreadPoolExecutor(Logger logger,
                             int keepAliveMs,
                             ThreadFactory updateThreadFactory) {
        super(1, /* core size -- adjusted dynamically */
              3, /* max size -- adjusted dynamically. */
              keepAliveMs,
              TimeUnit.MILLISECONDS,
              new LinkedBlockingQueue<Runnable>(QUEUE_SIZE),
              updateThreadFactory);

        this.logger = new RateLimitingLogger<String>(300 * 1000, 10, logger);
        setRejectedExecutionHandler(new UpdateRejectHandler());
    }

    /**
     * For testing only
     */
    int getMaxCorePoolSize() {
        return maxCorePoolSize;
    }

    /**
     * For testing only
     */
    int getMaxMaxPoolSize() {
        return maxMaxPoolSize;
    }

    /**
     * This method is invoked before each pass over the services. It adjusts
     * the core pool size and the max pool size depending upon the statistics
     * from the preceding pass. A single thread should typically be sufficient
     * to make a pass over all the nodes in a store, since each request should
     * take the order of 1-2ms. However, when threads are stalled due to
     * network issues more threads are brought into service on a temporary
     * basis to permit progress with other services.
     *
     * The thread pool configurations are changed as follows:
     *
     * If no progress was made during the previous pass, and tasks are queued
     * up, the executing threads may be stuck on the network. Increase the core
     * pool size, so that additional threads can immediately start working on
     * the tasks stalled in the queue.
     *
     * If tasks are being rejected, it means that the max pool size + the pool
     * queue is unable to accommodate all the tasks we wish to queue. So
     * increase the max number of threads to help make room in the queue in
     * this pass. Ideally, we should configure the queue length to be numRNs,
     * but since the thread pool mechanism does not allow for dynamic queue
     * size changes we vary max threads instead.
     *
     * @param numServices the total number of services that can be accessed by
     * the tasks via this executor
     */
    public void tunePoolSize(int numServices) {

        if (numServices == 0) {
            return;
        }

        int maxPoolSize = Math.max(3, numServices / 10);
        int corePoolSize = CORE_POOL_SIZE;

        final int currCorePoolSize = getCorePoolSize();
        final int currMaxPoolSize = getMaximumPoolSize();
        final int activeCount = getActiveCount();
        final int queueSize = getQueue().size() ;

        if ((executeCount == 0) && (activeCount > 0) && (queueSize > 0)) {

            /*
             * No executions since last pass and tasks are waiting in
             * the queue. Increase the core pool size, so that additional
             * threads can be set to work on the queue and it can be drained.
             */
            corePoolSize = Math.min(increaseThreads(currCorePoolSize), numServices);
        }

        if (rejectCount > 0) {
            /*
             * Did not manage to get through all the tasks. Some of the threads
             * may be stalled trying to do I/O. Increase the max number of
             * threads to accommodate all the tasks.
             */
            maxPoolSize = Math.min(increaseThreads(currMaxPoolSize), numServices);
        }

        maxPoolSize = Math.max(corePoolSize, maxPoolSize);

        /*
         * Change the maximum first if it is increasing because, starting with
         * Java 9, the maximum size must always be larger than the core size.
         */
        if (maxPoolSize > getMaximumPoolSize()) {
            setMaximumPoolSize(maxPoolSize);
            setCorePoolSize(corePoolSize);
        } else {
            setCorePoolSize(corePoolSize);
            setMaximumPoolSize(maxPoolSize);
        }

        /* update stats. */
        maxCorePoolSize = Math.max(maxCorePoolSize, corePoolSize);
        maxMaxPoolSize = Math.max(maxMaxPoolSize, maxPoolSize);

        if ((corePoolSize > 1) &&
            ((corePoolSize >= numServices) || (maxPoolSize >= numServices))) {
            final String msg =
                String.format("Update thread pool size at limit. " +
                              "Total rns:%d " +
                              "core pool size:%d max pool size:%d",
                              numServices, corePoolSize, maxPoolSize);
            logger.log(msg, Level.INFO, msg);
        }

        /* Reset the progress indicators for the next pass. */
        rejectCount = 0;
        executeCount = 0;
    }

    /**
     * Returns an incremented thread count for thread pool limits based on the
     * policy described below.
     *
     * When the executor thread limits are increased two policies are used:
     *
     * 1) At small pool sizes, thread pool sizes are quadrupled up to the
     * threshold value, so that there is a rapid deployment of thread
     * resources.
     *
     * 2) Beyond the threshold, thread pool size are increased more gradually,
     * on a percentage basis using THREAD_INC_PERCENT.
     */
    private int increaseThreads(int threads) {
        return (threads < THREAD_INC_START_THRESHOLD) ?
            4 * threads :
            threads + Math.min(threads / THREAD_INC_PERCENT, 1);
    }

    @Deprecated
    @Override
    /**
     * @deprecated Use UpdateThreadPoolExecutor#execute(UpdateTask) instead
     */
    public void execute(Runnable command) {
        throw new UnsupportedOperationException
            ("Please use the method: execute(UpdateTask), instead.");
    }

    /**
     * Executes the task, if one is not already in progress for the RN. It
     * returns true if the task was queued for execution and false if a task
     * was already executing or queued for execution. The caller is expected
     * to be resilient in this regard and retry the request, the next time
     * around.
     */
    public void execute(UpdateThreadPoolExecutor.UpdateTask task) {
        final ResourceId id = task.getResourceId();
        if ((id != null) && (activeUpdates.put(id, task) != null)) {
            /* Already present -- skip this task */
            logger.log(id.toString(), Level.FINE,
                       "Task " + task +
                       " execute() ignored for service " + id +
                       " since it already had an outstanding task: " +
                       activeUpdates.get(id));
            return;
        }
        super.execute(task);
        return;
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);

        executeCount++;

        final ResourceId id = ((UpdateTask)r).getResourceId();
        if (id != null) {
            activeUpdates.remove(id);
        }
    }

    /**
     * Action to be taken when an "execute" request is rejected by the
     * thread pool. It's invoked either because the queue is full, or because
     * the thread pool is being shutdown.
     */
    private class UpdateRejectHandler extends ThreadPoolExecutor.DiscardPolicy {
        @Override
        public void rejectedExecution(Runnable r,
                                      ThreadPoolExecutor e) {

            final int count = rejectCount++;

            final ResourceId id = ((UpdateTask)r).getResourceId();
            if (id != null) {
                activeUpdates.remove(id);
            }

            final Level level =
                ((count % 100) == 0) ? Level.INFO : Level.FINE;

            final String msg =
                String.format("RN state update thread pool rejected " +
                              "%d requests. Pool size:%d Max pool size:%d",
                              count, getPoolSize(),
                              getMaximumPoolSize());
            logger.log(msg, level, msg);
        }
    }

    /**
     * Interface that must be implemented by tasks to be run by this executor.
     * It provides a mechanism for returning the RN associated with the task.
     */
    public interface UpdateTask extends Runnable {

        /**
         * Returns the RepNodeId of the RN associated with the task, or null
         * if there is no specific RN associated with this task.
         */
        ResourceId getResourceId();
    }
}
