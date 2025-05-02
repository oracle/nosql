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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import oracle.kv.impl.measurement.PerfStatType;
import oracle.nosql.common.sklogger.measure.LatencyElement;

import com.sleepycat.utilint.ActivityCounter;

/**
 * Tracks latency stats for a given set of operations.
 *
 * <p>
 * This class can be used to keep track of latency for a set of types of
 * operaitons. The {@link #markStart} method can be used to obtain the start
 * timestamp. The {@link #markFinish} method can be used, together with the
 * start timestamp, to update for the latency of the type of the operation. For
 * this purpose, the two methods do not need to be called in pairs and may set
 * the {@code isPairedActivity} to {@code false}. That is, the start timestamp
 * can be used for multiple operation-finish events such that the markFinish
 * method is called with more times than the markStart method. One such example
 * of this unpaired accouting is for types in {@link PerfStatType}: the latency
 * of GET_INT type is updated together with USER_SINGLE_OP_INT and
 * USER_SINGLE_READ_INT types for one get operation.
 *
 * <p>
 * For historic reasons, this class can also be used to keep track of paired
 * operation start and finish. This is useful to track a sudden throughput drop
 * and dump thread stack traces when that happens. For this purpose, the {@link
 * #markStart} and {@link #markFinish} should be called in pairs and set the
 * flag {@code isPairedActivity} to {@code true}.
 */
public class LatencyTracker<T> {

    /*
     * Operation latency map. We can use a thread-unsafe hash map here because
     * it is final and it is only written in the constructor so that only the
     * completely initialized map will be seen by other threads. See
     * https://docs.oracle.com/javase/specs/jls/se8/html/jls-17.html#jls-17.5.
     */
    private final Map<T, LatencyElement> latencyMap = new HashMap<>();
    /*
     * ActivityCounter tracks throughput and dumps thread stacktraces when
     * throughput drops.
     */
    private final ActivityCounter activityCounter;


    public LatencyTracker(T[] keys,
                          Logger stackTraceLogger,
                          int activeThreadThreshold,
                          long threadDumpIntervalMillis,
                          int threadDumpMax) {
        for (T key : keys) {
            latencyMap.put(key, new LatencyElement());
        }
        this.activityCounter =
            new ActivityCounter(activeThreadThreshold,
                                threadDumpIntervalMillis, threadDumpMax,
                                stackTraceLogger);
    }

    /**
     * Track the start of a operation.
     * @return the value of System.nanoTime, for passing to markFinish.
     */
    public long markStart() {
        return markStart(true);
    }

    /**
     * Track the start of a operation.
     * @param isPairedActivity {@code true} if a paired markFinish will be
     * called with the same flag set. Set this flag to {@code true} will
     * increment an outstanding activity count.  The thread stack traces are
     * dumped if the count reaches a threshold
     * @return the value of System.nanoTime, for passing to markFinish.
     */
    public long markStart(boolean isPairedActivity) {
        if (isPairedActivity) {
            activityCounter.start();
        }
        return System.nanoTime();
    }

    /**
     * Track the end of a request.
     * @param key the key of the operation
     * @param startTime should be the value returned by the corresponding call
     * to markStart
     */
    public void markFinish(T key, long startTime) {
        markFinish(key, startTime, 1);
    }

    /**
     * Track the end of a request.
     * @param key the key of the request
     * @param startTime should be the value returned by the corresponding call
     * to markStart
     * @param numOperations the number of operations for the request.
     */
    public void markFinish(T key, long startTime, int numOperations) {
        markFinish(key, startTime, numOperations, true);
    }

    /**
     * Track the end of a request.
     * @param key the key of the request
     * @param startTime should be the value returned by the corresponding call
     * to markStart
     * @param numOperations the number of operations for the request.
     * @param isPairedActivity {@code true} if a paired markStart was called
     * with the same flag set. Set this flag to {@code true} will decrement an
     * outstanding activity count. The thread stack traces are dumped if the
     * count reaches a threshold
     */
    public void markFinish(T key,
                           long startTime,
                           int numOperations,
                           boolean isPairedActivity) {
        if (isPairedActivity) {
            activityCounter.finish();
        }
        if (numOperations == 0) {
            return;
        }

        if (key != null) {
            final LatencyElement element = latencyMap.get(key);
            if (element != null) {
                long elapsed = System.nanoTime() - startTime;
                /*
                 * Estimates the operation latency with the average value.
                 *
                 * TODO: This might not be the most precise way to do it. We
                 * probably need to re-design latency stats collection for
                 * single-/multil-operations as well as query since the the
                 * latency values do not mean the same thing.
                 */
                element.observe(elapsed / numOperations, numOperations);
            }
        }
    }

    /**
     * Return all results for the specified watcher, clearing results for every
     * key.
     */
    public Map<T, LatencyElement.Result> obtain(String watcherName) {
        return obtain(watcherName, (k) -> true);
    }

    /**
     * Return all results for the specified watcher, calling the specified
     * function to determine if results should be cleared for a given key.
     */
    public Map<T, LatencyElement.Result> obtain(
        String watcherName,
        Predicate<T> clearForKey) {
        return Collections.unmodifiableMap(
            latencyMap.entrySet().
            stream().collect(
                Collectors.toMap(
                    e -> e.getKey(),
                    e ->
                    e.getValue().obtain(watcherName,
                                        clearForKey.test(e.getKey())))));
    }

    /**
     * Returns the activity counter for test.
     */
    public ActivityCounter getActivityCounter() {
        return activityCounter;
    }
}
