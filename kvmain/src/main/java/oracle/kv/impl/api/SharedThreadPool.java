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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.impl.api.KVStoreImpl.TaskExecutor;
import oracle.kv.impl.util.KVThreadFactory;

/**
 * A shared thread pool which supports the submission and canceling of
 * groups of tasks.
 */
public class SharedThreadPool {
    
    private static final int KEEP_ALIVE_SEC = 10;
    
    private final ThreadPoolExecutor threadPool;
    
    /**
     * Creates a shared thread pool.
     * 
     * @param logger a logger passed to the thread factory
     */
    public SharedThreadPool(Logger logger) {
        
        /*
         * With corePoolSize == 0 and maximumPoolSize == Integer.MAX_VALUE the
         * thread pool is unbounded and will reuse an idle thread before
         * creating a new one. Because the pool is unbounded a direct handoff
         * queue is used. (see doc for ThreadPoolExecutor)
         * 
         * Task submission will not be rejected unless the pool is shutdown.
         */
        threadPool =
                new ThreadPoolExecutor(0,                   // corePoolSize
                                       Integer.MAX_VALUE,   // maximumPoolSize
                                       KEEP_ALIVE_SEC, TimeUnit.SECONDS,
                                       new SynchronousQueue<Runnable>(),
                                       new KVThreadFactory(" shared thread",
                                                           logger));
    }
    
    /**
     * Gets a task executor instance. The returned executor will limit
     * the number of concurrent tasks submitted to the shared thread pool to
     * maxConcurrentTasks.
     * 
     * @param maxConcurrentTasks the maximum number of concurrent tasks
     * submitted to the thread pool
     * @return a task executor instance
     */
    public TaskExecutor getTaskExecutor(int maxConcurrentTasks) {
        if (threadPool.isShutdown()) {
            throw new IllegalStateException("The shared thread pool has been " +
                                            "shutdown");
        }
        return new TaskExecutorImpl(maxConcurrentTasks);
    }
    
    /**
     * Attempts to stop all actively executing tasks and halts the processing
     * of waiting tasks. Subsequent calls to TaskExecutor.submit() will throw
     * RejectedExecutionException.
     */
    void shutdownNow() {
        threadPool.shutdownNow();
    }

    /*  -- For unit tests -- */
    
    int getActiveCount() {
        return threadPool.getActiveCount();
    }

    int getPoolSize() {
        return threadPool.getPoolSize();
    }
    
    long getKeepAliveTime(TimeUnit timeUnit) {
        return threadPool.getKeepAliveTime(timeUnit);
    }

    /**
     * Implementation of a TaskExecutor which will submit tasks to the shared
     * thread pool. It will queue tasks if necessary and will maintain handles
     * to the tasks so that they may be canceled on shutdown.
     */
    private class TaskExecutorImpl implements TaskExecutor {
        private final int maxConcurrentTasks;
        private final Queue<WrappedTask> taskQueue;
        private final Set<WrappedTask> runningTasks;
        private volatile boolean closed = false;
        
        private TaskExecutorImpl(int maxConcurrentTasks) {
            this.maxConcurrentTasks = maxConcurrentTasks;
            taskQueue = new LinkedBlockingQueue<WrappedTask>();
            runningTasks = new HashSet<WrappedTask>(maxConcurrentTasks);
        }
                   
        @Override
        public synchronized Future<?> submit(Runnable r) {
            if (closed) {
                throw new RejectedExecutionException();
            }
            final WrappedTask task = new WrappedTask(r, this);
            
            if (runningTasks.size() >= maxConcurrentTasks) {
                taskQueue.add(task);
            } else {
                submitWrappedTask(task);
            }
            return task;
        }
        
        @Override
        public List<Runnable> shutdownNow() {
            closed = true;
            
            synchronized (this) {
                for (WrappedTask task : runningTasks) {
                    task.cancel(true);
                }
                ArrayList<Runnable> ret = new ArrayList<Runnable>(taskQueue);
                taskQueue.clear();
                return ret;
            }
        }
        
        /*
         * Processes a completed task. The specified task is removed from
         * the running task list and if allowed a new task is submitted
         * to the pool.
         */
        private synchronized void completed(WrappedTask task) {
            if (closed) {
                return;
            }
            runningTasks.remove(task);

            while (!closed && !taskQueue.isEmpty() &&
                   (runningTasks.size() < maxConcurrentTasks)) {
                submitWrappedTask(taskQueue.remove());
            }
        }
        
        /*
         * Submits a task to be executed. If the task cannot be immediately
         * added to the shared thread pool it is queued to be submitted
         * at a later time. 
         */
        private void submitWrappedTask(WrappedTask task) {
            assert Thread.holdsLock(this);
            try {
                threadPool.execute(task);
                runningTasks.add(task);
            } catch (RejectedExecutionException ree) {
                /* Should only happen if the pool is shutdown */
                if (threadPool.isShutdown()) {
                    closed = true;
                    throw ree;
                }
                throw new IllegalStateException("Unexpected task rejection",
                                                ree);
            }
        }
        
        @Override
        public String toString() {
            return "TaskExecutor[max=" + maxConcurrentTasks + ",running=" +
                   runningTasks.size() + ",queue=" + taskQueue.size() +
                   ",pool=" + threadPool.getPoolSize() + "]";
        }
    }
    
    /**
     * Wrapper for tasks submitted to the shared thread pool. This wrapper
     * allows the task executor to be notified when the task is done and also
     * serves as the task's future.
     */
    private static class WrappedTask extends FutureTask<Object> {
        
        /* The executor which is handling the task */
        private final TaskExecutorImpl executor;
        
        WrappedTask(Runnable runnable, TaskExecutorImpl executor) {
            super(runnable, null);
            this.executor = executor;
        }

        @Override
        protected void done() {
            executor.completed(this);
        }
    }
}
