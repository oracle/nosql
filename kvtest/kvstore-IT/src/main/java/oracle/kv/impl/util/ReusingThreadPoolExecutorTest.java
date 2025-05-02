/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;

import oracle.kv.UncaughtExceptionTestBase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ReusingThreadPoolExecutorTest
        extends UncaughtExceptionTestBase
{
    final ThreadFactory threadFactory = r -> {
        final Thread t = new Thread(r);
        t.setUncaughtExceptionHandler(this);
        return t;
    };

    ReusingThreadPoolExecutor executor;

    @Override
    @Before
    public void setUp()
        throws Exception
    {
        super.setUp();
    }

    @Override
    @After
    public void tearDown()
        throws Exception
    {
        super.tearDown();
        if (executor != null) {
            executor.shutdown();
        }
    }

    @Test
    public void testConstructorBadCorePoolSize() {
        checkException(
            () -> new ReusingThreadPoolExecutor(
                -1, 2, 2, MILLISECONDS, 2, threadFactory),
            IllegalArgumentException.class, null);
    }

    @Test
    public void testConstructorBadMaxPoolSize() {
        checkException(
            () -> new ReusingThreadPoolExecutor(
                0, 0, 2, MILLISECONDS, 2, threadFactory),
            IllegalArgumentException.class, null);
    }

    @Test
    public void testConstructorBadCoreMaxPoolSizeCombo() {
        checkException(
            () -> new ReusingThreadPoolExecutor(
                1, 0, 2, MILLISECONDS, 2, threadFactory),
            IllegalArgumentException.class, null);
    }

    @Test
    public void testConstructorBadKeepAliveTime() {
        checkException(
            () -> new ReusingThreadPoolExecutor(
                0, 1, -1, MILLISECONDS, 2, threadFactory),
            IllegalArgumentException.class, null);
    }

    @Test
    public void testConstructorBadQueueCapacity() {
        checkException(
            () -> new ReusingThreadPoolExecutor(
                0, 1, 2, MILLISECONDS, 0, threadFactory),
            IllegalArgumentException.class, "queueCapacity");
    }

    @Test
    public void testSimpleSizeAccessors() {
        executor = new ReusingThreadPoolExecutor(
            1, 2, 3, MILLISECONDS, 4, threadFactory);
        assertEquals(1, executor.getCorePoolSize());
        assertEquals(2, executor.getMaximumPoolSize());
        assertEquals(3, executor.getKeepAliveTime(MILLISECONDS));
        assertEquals(4, executor.getQueueCapacity());
    }

    @Test
    public void testQueueRace()
        throws Exception
    {
        /*
         * Create an executor with 2 threads and 3 slots in the queue. We will
         * create three tasks, so there will be one that is waiting to be
         * executed in some fashion, but there is space in the queue for all of
         * the tasks if needed. The problem we want to catch is where a race
         * between adding and completing tasks means that tasks stay in the
         * queue without anyone waking up to run them.
         */
        executor = new ReusingThreadPoolExecutor(
            0, 2, 3, MILLISECONDS, 3, threadFactory);

        /*
         * Keep two tasks busy, so the threads are busy, and arrange for a 3rd
         * task to be enqueued. Make sure that task always gets removed from
         * the queue. We want a race between a task completing and one being
         * added.
         */

        final Semaphore semaphore = new Semaphore(3);
        class Task implements Runnable {
            @Override
            public void run() {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                }
                semaphore.release();
            }
        }
        for (int i = 0; i < 1000; i++) {
            assertTrue("Didn't get semaphore",
                       semaphore.tryAcquire(5, SECONDS));
            executor.execute(new Task());
        }
    }

    /**
     * Another test for the race between when a thread is done and a new task
     * is added.
     */
    @Test
    public void testQueueRace2()
        throws Exception
    {
        final Semaphore doneCheckQueue = new Semaphore(0);
        final Semaphore waitAfterExecute = new Semaphore(0);

        /* Create an executor with 1 thread and 1 slot in the queue */
        executor = new ReusingThreadPoolExecutor(0, 1, 30, SECONDS, 1,
                                                 threadFactory) {
            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                super.afterExecute(r, t);

                /* We're done checking the queue */
                doneCheckQueue.release();

                try {

                    /*
                     * Wait in the bad period where we are done checking the
                     * queue but are not yet ready to take another task
                     */
                    assertTrue(waitAfterExecute.tryAcquire(10, SECONDS));
                } catch (InterruptedException e) {
                    throw new RuntimeException("Unexpected exception: " + e, e);
                }
            }
        };

        /* Release the thread for both tasks on failure */
        tearDowns.add(() -> waitAfterExecute.release(2));

        /* First task */
        final Semaphore firstTask = new Semaphore(0);
        executor.execute(firstTask::release);
        assertTrue("Didn't run first task", firstTask.tryAcquire(5, SECONDS));

        /* Wait for thread to be done checking the queue */
        assertTrue(doneCheckQueue.tryAcquire(5, SECONDS));

        /* Arrange to release the thread in 1 second */
        CompletableFuture.runAsync(() -> {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
                waitAfterExecute.release(2);
            });

        /* Second task */
        final Semaphore secondTask = new Semaphore(0);
        executor.execute(secondTask::release);
        assertTrue("Didn't run second task",
                   secondTask.tryAcquire(5, SECONDS));
    }
}
