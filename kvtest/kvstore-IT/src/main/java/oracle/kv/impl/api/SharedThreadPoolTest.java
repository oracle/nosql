/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import oracle.kv.TestBase;
import oracle.kv.impl.api.KVStoreImpl.TaskExecutor;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.client.ClientLoggerUtils;

import org.junit.Test;

/**
 * Unit test for the shared thread pool
 */
public class SharedThreadPoolTest extends TestBase {

    private SharedThreadPool stp = null;
    
    @Override
    public void setUp() throws Exception {
        super.setUp();
        stp = new SharedThreadPool(
                   ClientLoggerUtils.getLogger(SharedThreadPool.class, "test"));
    }
    
    @Override
    public void tearDown() throws Exception {
        if (stp != null) {
            stp.shutdownNow();
        }
        super.tearDown();
    }
    
    /*
     * Tests the basic pool functions. Creates two executors and sumbits
     * tasks from both. Then waits for all tasks to finish.
     */
    @Test
    public void testBasic() {
        final int N_TASKS = 10;
        
        final TaskExecutor te1 = stp.getTaskExecutor(100);
        final TaskExecutor te2 = stp.getTaskExecutor(100);
        
        final AtomicInteger counter1 = new AtomicInteger(0);
        final AtomicInteger counter2 = new AtomicInteger(0);
       
        // Submit a bunch of tasks from two executors
        for (int i = 0; i < N_TASKS; i++) {
            te1.submit(new WaitingTask(counter1, false));
            te2.submit(new WaitingTask(counter2, false));
        }
        waitForCounter(counter1, N_TASKS);
        waitForCounter(counter2, N_TASKS);
        waitForActive(0);
        te1.shutdownNow();
        te2.shutdownNow();
        
        // Wait for the pool threads to exit
        waitForRetire();
    }
    
    /*
     * Tests the task limit on the executor.
     */
    @Test
    public void testTaskLimit() {
        final int TASK_LIMIT = 10;
        
        final TaskExecutor te = stp.getTaskExecutor(TASK_LIMIT);
        final AtomicInteger counter = new AtomicInteger(0);
        final List<WaitingTask> tasks = new ArrayList<WaitingTask>(TASK_LIMIT);
        
        // Submit an initial batch, keeping track of the tasks
        for (int i = 0; i < TASK_LIMIT; i++) {
            WaitingTask task = new WaitingTask(counter, true);
            tasks.add(task);
            te.submit(task);
        }
        
        // Wait for taskLimit number of tasks to execute
        waitForCounter(counter, TASK_LIMIT);
        
        // Add another batch of tasks
        for (int i = 0; i < TASK_LIMIT; i++) {
            te.submit(new WaitingTask(counter, true));
        }
        // Should not change
        assertEquals(TASK_LIMIT, counter.get());
        
        // Cause the first batch to finish
        for (WaitingTask task : tasks) {
            task.finish();
        }
        
        // Wait for the rest of the tasks to execute
        waitForCounter(counter, TASK_LIMIT*2);
        
        te.shutdownNow();
    }
    
    /*
     * Tests shutting down an executor with tasks running and waiting.
     */
    @Test
    public void testShutdown() {
        final int TASK_LIMIT = 10;
        
        final TaskExecutor te = stp.getTaskExecutor(TASK_LIMIT);
        final AtomicInteger counter = new AtomicInteger(0);
        
        // Submit 3x the limit
        for (int i = 0; i < TASK_LIMIT*3; i++) {
            te.submit(new WaitingTask(counter, true));
        }
        
        // Wait for taskLimit number of tasks to execute
        waitForCounter(counter, TASK_LIMIT);
        
        // The rest should still be pending and will be returned on shutdown
        List<Runnable> leftover = te.shutdownNow();
        assertEquals(TASK_LIMIT*2, leftover.size());
        
        // Second call should return an empty list
        leftover = te.shutdownNow();
        assertEquals(0, leftover.size());
        
        // The running tasks should end
        waitForActive(0);
    }
    
    /*
     * Tests operations after the executor and the pool have been shutdown.
     */
    @Test
    public void testAfterShutdown() {
        final TaskExecutor te = stp.getTaskExecutor(100);
        
        te.shutdownNow();
        try {
            te.submit(new Runnable() {
                        @Override
                        public void run() {}
                      });
            fail("Expected RejectedExecutionException");
        } catch (RejectedExecutionException expected) {}
        
        stp.shutdownNow();
        
        try {
            te.submit(new Runnable() {
                        @Override
                        public void run() {}
                      });
            fail("Expected RejectedExecutionException");
        } catch (RejectedExecutionException expected) {}
        
        // Should be OK to call again
        te.shutdownNow();
        
        try {
            stp.getTaskExecutor(100);
            fail("Expected IllegalStateException");
        } catch (IllegalStateException expected) {}
    }
    
    /* Waits for the active thread count to reach the specified value */
    private void waitForActive(final int nThreads) {
        boolean success = new PollCondition(50, 10000) {
            @Override
            protected boolean condition() {
                return stp.getActiveCount() == nThreads;
            }
        }.await();
        assert(success);
    }
    
    /* Waits for the thread pool threads to retire */
    private void waitForRetire() {
        boolean success =
                new PollCondition(50, (int)stp.getKeepAliveTime(
                                                     TimeUnit.MILLISECONDS)*2) {
            @Override
            protected boolean condition() {
                return stp.getPoolSize() == 0;
            }
        }.await();
        assert(success);
    }
    
    /* Waits for the thread pool threads to retire */
    private void waitForCounter(final AtomicInteger counter, final int count) {
        boolean success = new PollCondition(50, 10000) {
            @Override
            protected boolean condition() {
                return counter.get() == count;
            }
        }.await();
        assert(success);
    }
    
    /* A task that increments a count then optionally waits */
    private static class WaitingTask implements Runnable {
        final AtomicInteger counter;
        final boolean wait;
        
        WaitingTask(AtomicInteger counter, boolean wait) {
            this.counter = counter;
            this.wait = wait;
        }
        
        @Override
        public void run() {
            counter.incrementAndGet();
            if (wait) {
                synchronized (this) {
                    try {
                        wait(20000);    // 20 sec ~= forever
                    } catch (InterruptedException ex) {}
                }
            }
        }
        
        synchronized void finish() {
            notify();
        }
    }
}
