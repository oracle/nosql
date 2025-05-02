/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import oracle.kv.TestBase;
import oracle.kv.impl.async.DialogResourceManager.Handle;
import oracle.kv.impl.util.CommonLoggerUtils;

import org.junit.Test;

/**
 * Tests the {@link DialogResourceManager}.
 */
public class DialogResourceManagerTest extends TestBase {

    private static int DEFAULT_AVG_TASK_RUN_TIME = 80;
    private static int DEFAULT_DELAY_TIME = 20;

    /**
     * Tests when the manager is under utilized.
     */
    @Test
    public void testUnderUtilized() throws Exception {
        final int concurrency = 100;
        final DialogResourceManager manager =
            new DialogResourceManager(concurrency);
        final Job job = new Job(
            "", manager, concurrency,
            DEFAULT_AVG_TASK_RUN_TIME, DEFAULT_DELAY_TIME);
        final float throughput = job.throughputEstimate();
        Thread.sleep(1000);
        verifyJobThroughput(Collections.singletonMap(job, throughput));
    }

    private void verifyJobThroughput(Map<Job, Float> expectedMinThroughput)
        throws Exception {
        for (Job job : expectedMinThroughput.keySet()) {
            job.stop();
        }
        Thread.sleep(100);
        Throwable error = null;
        for (Map.Entry<Job, Float> entry : expectedMinThroughput.entrySet()) {
            final Job job = entry.getKey();
            final Float expected = entry.getValue();
            try {
                assertTrue(
                    String.format(
                        "Job %s throughput %s " +
                        "should exceeds expected minimum %s, " +
                        "nblocked=%s",
                        job, job.throughput(), expected, job.nblocked()),
                    job.throughput() > expected);
            } catch (Throwable e) {
                if (error == null) {
                    error = e;
                }
            } finally {
                logger.log(Level.FINEST, String.format(
                    "Job %s has actual throughput %s, " +
                    "expected throughput %s, nblocked %s, " +
                    "avgRunTime %s ms, delay time %s ms, " +
                    "max throughput estimate %s",
                    job, job.throughput(), expected,
                    job.nblocked(),
                    job.avgRunTime, job.delayTime,
                    job.throughputEstimate()));
            }
        }
        if (error != null) {
            throw new RuntimeException(error);
        }
    }

    /**
     * Tests when the manager is slightly over utilized.
     */
    @Test
    public void testSlightlyOverUtilized() throws Exception {
        testOverUtilized(0.8f);
    }

    private void testOverUtilized(float overUtilizedFactor) throws Exception {
        final int concurrency = 100;
        final int maxConcurrency = (int) (concurrency * overUtilizedFactor);
        final DialogResourceManager manager =
            new DialogResourceManager(maxConcurrency);
        final Job job = new Job(
            "", manager, concurrency,
            DEFAULT_AVG_TASK_RUN_TIME, DEFAULT_DELAY_TIME);
        final float throughput =
            job.throughputEstimate() * overUtilizedFactor;
        Thread.sleep(1000);
        verifyJobThroughput(Collections.singletonMap(job, throughput));
    }

    /**
     * Tests when the manager is heavily over utilized.
     */
    @Test
    public void testHeavilyOverUtilized() throws Exception {
        testOverUtilized(0.2f);
    }

    /**
     * Tests when the manager is heavily over utilized with different run time.
     *
     * This tests the manager's throughput fairness property.
     */
    @Test
    public void testHeavilyOverUtilizedVariousRuntime() throws Exception {
        final int concurrency = 100;
        final DialogResourceManager manager =
            new DialogResourceManager(concurrency);
        final Job job1 = new Job(
            "1", manager, concurrency,
            DEFAULT_AVG_TASK_RUN_TIME, DEFAULT_DELAY_TIME);
        final float throughput1 =
            job1.throughputEstimate(concurrency / 2);
        final Job job2 = new Job(
            "2", manager, concurrency,
            2 * DEFAULT_AVG_TASK_RUN_TIME, 2 * DEFAULT_DELAY_TIME);
        final float throughput2 =
            job2.throughputEstimate(concurrency / 2);
        Thread.sleep(1000);
        verifyJobThroughput(
            Stream.of(
                new AbstractMap.SimpleImmutableEntry<>(job1, throughput1),
                new AbstractMap.SimpleImmutableEntry<>(job2, throughput2)).
            collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    /**
     * Tests with handle creation and stop reserving over time.
     */
    @Test
    public void testHandleAddRemove() throws Exception {
        final int concurrency = 100;
        final DialogResourceManager manager =
            new DialogResourceManager(concurrency);
        /* Job 1 runs the whole 2 seconds */
        final Job job1 = new Job(
            "all2", manager, concurrency,
            DEFAULT_AVG_TASK_RUN_TIME, DEFAULT_DELAY_TIME);
        final float throughput1 =
            job1.throughputEstimate(concurrency / 2);
        /* Job 2 runs the first 1 second */
        final Job job2 = new Job(
            "first1", manager, concurrency,
            DEFAULT_AVG_TASK_RUN_TIME, DEFAULT_DELAY_TIME);
        final float throughput2 = throughput1;
        Thread.sleep(1000);
        job2.stop();
        /* Job 3 runs the last 1 second */
        final Job job3 = new Job(
            "last1", manager, concurrency,
            DEFAULT_AVG_TASK_RUN_TIME, DEFAULT_DELAY_TIME);
        final float throughput3 = throughput1;
        Thread.sleep(1000);
        verifyJobThroughput(
            Stream.of(
                new AbstractMap.SimpleImmutableEntry<>(job1, throughput1),
                new AbstractMap.SimpleImmutableEntry<>(job2, throughput2),
                new AbstractMap.SimpleImmutableEntry<>(job3, throughput3)).
            collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    /**
     * Tests the permit change.
     */
    @Test
    public void testPermitsChange() throws Exception {
        final int concurrency = 100;
        final DialogResourceManager manager =
            new DialogResourceManager(concurrency);
        final Job job = new Job(
            "", manager, concurrency,
            DEFAULT_AVG_TASK_RUN_TIME, DEFAULT_DELAY_TIME);
        /* Half the time full throughput, half the time half throughput */
        final float throughput =
            job.throughputEstimate((1 + 0.5f) / 2 * concurrency);
        Thread.sleep(1000);
        final AtomicInteger adjusted = new AtomicInteger(0);
        manager.setNumPermits(concurrency / 2, (n) -> { adjusted.set(n); });
        Thread.sleep(1000);
        verifyJobThroughput(Collections.singletonMap(job, throughput));
        assertEquals(concurrency / 2, adjusted.get());
    }

    /**
     * A job that spawns tasks.
     */
    private class Job {

        private final ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor();
        private final String id;
        private final int ntasks;
        private final long avgRunTime;
        private final long delayTime;
        private final Handle handle;

        private volatile boolean stop = false;

        private final long ts = System.currentTimeMillis();
        private volatile long te = -1;
        private final AtomicLong nactive = new AtomicLong(0);
        private final AtomicLong nstarted = new AtomicLong(0);
        private final AtomicLong nblocked = new AtomicLong(0);

        private Job(String id,
                    DialogResourceManager manager,
                    int ntasks,
                    long avgRunTime,
                    long delayTime) {
            this.id = id;
            this.ntasks = ntasks;
            this.avgRunTime = avgRunTime;
            this.delayTime = delayTime;
            this.handle = manager.createHandle(id, (n) -> {
                executor.submit(() -> launchTasks(n));
            });
            executor.submit(() -> launchTasks());
        }

        private void launchTasks() {
            launchTasks(0);
        }

        /**
         * Launch tasks with available permits until reaching the required
         * number of tasks.
         */
        private void launchTasks(int availablePermits) {
            if (stop) {
                return;
            }
            try {
                /* Must use up all the permits */
                for (int i = 0; i < availablePermits; ++i) {
                    runTask();
                }
                /*
                 * Acquire permits and run tasks until we reach the number
                 * required, or blocked
                 */
                while ((nactive.get() < ntasks) &&
                       acquirePermitAndRunTask()) {
                }
            } catch (Throwable t) {
                logger.finest(() -> String.format(
                    "Encountered exception: %s",
                    CommonLoggerUtils.getStackTrace(t)));
            }
        }

        private void runTask() {
            nactive.getAndIncrement();
            nstarted.getAndIncrement();
            /* adds some randomness to the run time but ensure the average */
            final long runTime =
                (long) (avgRunTime  * 9.0f / 10 +
                        ThreadLocalRandom.current().nextLong(avgRunTime) *
                        2.0f / 10);
            logger.finest(() -> String.format(
                "%s start one, ntasks=%s, nactive=%s, runTime=%s",
                this, ntasks, nactive.get(), runTime));
            executor.schedule(this::delay, runTime, TimeUnit.MILLISECONDS);
        }

        private boolean acquirePermitAndRunTask() {
            if (handle.reserve()) {
                runTask();
                return true;
            }
            logger.finest(() -> String.format(
                "%s blocked, ntasks=%s, nactive=%s", this, ntasks, nactive));
            nblocked.getAndIncrement();
            return false;
        }

        private void delay() {
            try {
                nactive.getAndDecrement();
                logger.finest(() -> String.format(
                    "%s done one, ntasks=%s, nactive=%s",
                    this, ntasks, nactive.get()));
                handle.free();
                executor.schedule(() -> launchTasks(),
                                  delayTime, TimeUnit.MILLISECONDS);
            } catch (Throwable t) {
                logger.finest(() -> String.format(
                    "Encountered exception: %s",
                    CommonLoggerUtils.getStackTrace(t)));
            }
        }

        private void stop() {
            if (stop) {
                return;
            }
            stop = true;
            te = System.currentTimeMillis();
        }

        private float throughput() {
            final long n = nstarted.get();
            if (n == 0) {
                return 0;
            }
            if (te == -1) {
                throw new IllegalStateException("Job not stopped");
            }
            return nstarted.get() * 1.0e3f / (te - ts);
        }

        /**
         * Returns the throughput when the concurrency is ntasks.
         */
        private float throughputEstimate() {
            return throughputEstimate(ntasks);
        }

        /**
         * Returns the throughput given the estimated concurrency.
         */
        private float throughputEstimate(float concurrency) {
            /* Multiplies a factor in case of randomness */
            return concurrency * 1.0e3f / (avgRunTime + delayTime) * 0.8f;
        }

        private long nblocked() {
            return nblocked.get();
        }

        @Override
        public String toString() {
            return id;
        }
    }
}
