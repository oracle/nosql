/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.utilint;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.StoppableThreadFactory;

/**
 * Tracks the number of operations begun, as a way of measuring level of
 * activity. Is capable of displaying thread dumps if the activity level
 * reaches a specified ceiling
 */
public class ActivityCounter {

    private static final
        long DEFAULT_CORE_THREAD_KEEP_ALIVE_TIME_MILLIS = 1000;

    private final AtomicInteger activeCount;
    private final AtomicBoolean threadDumpInProgress;
    private volatile long lastThreadDumpTime;
    private volatile int numCompletedDumps;
    private final int activeThreshold;
    private final int  maxNumDumps;
    private final AtomicInteger maxActivity;

    /*
     * Thread dumps can only happen this many milliseconds apart, to avoid
     * overwhelming the system.
     */
    private final long requiredIntervalMillis;

    private final Logger logger;

    private final ThreadPoolExecutor dumper;

    /**
     * Constructs the activity counter.
     *
     * @param activeThreshold the thresold of active count which if exceeded
     * will trigger thread dump
     * @param requiredIntervalMillis the required interval between thread
     * dumps. A triggered thread dump will be skipped if duration between this
     * and the previous dump is less than the specified value. This argument
     * also controls the alive timeout of the dumping thread. The timeout is
     * ten times the requiredIntervalMillis if it is larger than zero.
     * Otherwise, the timeout value is set to one second.
     * @param maxNumDumps the max number of thread dumps
     * @param logger the logger
     */
    public ActivityCounter(int activeThreshold,
                           long requiredIntervalMillis,
                           int maxNumDumps,
                           Logger logger) {

        activeCount = new AtomicInteger(0);
        threadDumpInProgress = new AtomicBoolean(false);
        maxActivity = new AtomicInteger(0);

        this.activeThreshold = activeThreshold;
        this.requiredIntervalMillis = requiredIntervalMillis;
        this.maxNumDumps = maxNumDumps;
        this.logger = logger;

        /*
         * Specifies the thread keep-alive time which will affect the core
         * thread since we will set allowCoreThreadTimeOut.  The executor
         * is only active once every requiredIntervalMillis because this
         * class will dump at most once per this interval. Therefore, if we
         * set the keep-alive timeout smaller than this interval, thrashing
         * between core thread shutting down and creation will occurr. We
         * set the keep-alive timeout ten times the requiredIntervalMillis.
         * The multiplier ten is simply a heuristic value to further avoid
         * the thrashing when the active count constantly goes up and down
         * near the activeThreshold.
         */
        final long keepAliveMillis;
        if (requiredIntervalMillis <= 0) {
            keepAliveMillis = DEFAULT_CORE_THREAD_KEEP_ALIVE_TIME_MILLIS;
        } else if (requiredIntervalMillis < Long.MAX_VALUE / 10) {
            keepAliveMillis = 10 * requiredIntervalMillis;
        } else {
            keepAliveMillis = Long.MAX_VALUE;
        }

        dumper = new ThreadPoolExecutor(
            1, 1,
            keepAliveMillis, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new StoppableThreadFactory(
                ActivityCounter.class.getSimpleName(), logger));
        /*
         * Dump events should be rare, shut down the core thread if there is no
         * event.
         */
        dumper.allowCoreThreadTimeOut(true);
    }

    /* An operation has started. */
    public void start() {
        int numActive = activeCount.incrementAndGet();
        int max = maxActivity.get();
        if (numActive > max) {
            maxActivity.compareAndSet(max, numActive);
        }
        check(numActive);
    }

    /* An operation has finished. */
    public void finish() {
        activeCount.decrementAndGet();
    }

    public int getAndClearMaxActivity() {
        return maxActivity.getAndSet(0);
    }

    private boolean intervalIsTooShort() {
        /* Don't do a thread dump if the last dump was too recent */
        long interval = TimeSupplier.currentTimeMillis() - lastThreadDumpTime;
        return interval < requiredIntervalMillis;
    }

    /**
     * If the activity level is above a threshold, there is no other thread
     * that is dumping now, and a dump hasn't happened for a while, dump
     * thread stack traces.
     */
    private void check(int numActive) {

        /* Activity is low, no need to do any dumps. */
        if (numActive <= activeThreshold) {
            return;
        }

        if (numCompletedDumps >= maxNumDumps) {
            return;
        }

        /* Don't do a thread dump if the last dump was too recent */
        if (intervalIsTooShort()) {
            return;
        }

        /* There's one in progress. */
        if (threadDumpInProgress.get()) {
            return;
        }

        /*
         * Let's do a dump. The flag threadDumpInProgress guarantees that we do
         * one dump at a time.
         */
        dumper.execute(new GetStackTraces());
    }

    /**
     * For unit test support.
     */
    public int getNumCompletedDumps() {
        return numCompletedDumps;
    }

    private class GetStackTraces implements Runnable {

        public void run() {

            if (intervalIsTooShort()) {
                return;
            }

            if (!threadDumpInProgress.compareAndSet(false, true)) {
                logger.warning("Unexpected: ActivityCounter stack trace " +
                               "dumper saw threadDumpInProgress flag set.");
                return;
            }

            try {
                lastThreadDumpTime = TimeSupplier.currentTimeMillis();
                dumpThreads();
                numCompletedDumps++;
            } finally {
                boolean reset = threadDumpInProgress.compareAndSet(true, false);
                assert reset : "ThreadDump should have been in progress";
            }
        }

        private void dumpThreads() {

            int whichDump = numCompletedDumps;

            logger.info(() -> String.format(
                "[Dump %s for %s, numActive=%s, threshold=%s "
                + "--Dumping stack traces for all threads ]",
                whichDump, ActivityCounter.class.getSimpleName(),
                activeCount.get(), activeThreshold));

            Map<Thread, StackTraceElement[]> stackTraces =
                Thread.getAllStackTraces();

            for (Map.Entry<Thread, StackTraceElement[]> stme :
                     stackTraces.entrySet()) {
                if (stme.getKey() == Thread.currentThread()) {
                    continue;
                }
                logger.info(stme.getKey().toString());
                for (StackTraceElement ste : stme.getValue()) {
                    logger.info("     " + ste);
                }
            }

            logger.info("[Dump " + whichDump + " --Thread dump completed]");
        }
    }

    /**
     * Returns the active count for testing.
     */
    public int getActiveCount() {
        return activeCount.get();
    }
}

