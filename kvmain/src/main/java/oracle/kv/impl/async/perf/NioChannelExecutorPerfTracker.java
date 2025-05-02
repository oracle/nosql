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

package oracle.kv.impl.async.perf;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import oracle.nosql.common.sklogger.measure.LongCounterElement;

/**
 * Tracks the performance of a NioChannelExecutor.
 */
public class NioChannelExecutorPerfTracker {

    /** The executor child ID.*/
    private final int childId;

    /** The executor child index. */
    private final int childIndex;

    /**
     * The total time in nanos when the executor is idle, waiting for the
     * select loop to return.
     */
    private final LongCounterElement idleTimeNanos =
        new LongCounterElement();
    /**
     * The total time in nanos when the executor is processing IO operations
     * and the associated callback.
     */
    private final LongCounterElement ioProcessTimeNanos =
        new LongCounterElement();
    /**
     * The total time in nanos when the executor is processing scheduled tasks.
     */
    private final LongCounterElement taskProcessTimeNanos =
        new LongCounterElement();
    /**
     * The total number of immediate and dalyed task executions.
     */
    private final LongCounterElement numTasksExecuted =
        new LongCounterElement();
    /**
     * The total number of submitted immediate tasks.
     */
    private final LongCounterElement numImmediateTasksSubmitted =
        new LongCounterElement();
    /**
     * The total number of submitted delayed tasks.
     */
    private final LongCounterElement numDelayedTasksSubmitted =
        new LongCounterElement();

    /** The last time the executor reported responsiveness. */
    private volatile long lastResponsivenessTimestamp = System.nanoTime();

    /** The start time of a quiescent period. */
    private volatile long quiescentPeriodTimestamp = -1;

    /**
     * The set of handler perf trackers registered to this channel executor.
     */
    private final Set<NioEndpointHandlerPerfTracker> handlerPerfTrackers =
        ConcurrentHashMap.newKeySet();

    /**
     * Construct the perf tracker.
     */
    public NioChannelExecutorPerfTracker(int childId, int childIndex) {
        this.childId = childId;
        this.childIndex = childIndex;
    }

    /**
     * Marks the start of executor an idle/select period. Returns the start
     * time in nanos.
     */
    public long markIdleStart() {
        return System.nanoTime();
    }

    /**
     * Marks the finish of executor an idle/select period.
     */
    public void markIdleFinish(long startTimeNanos) {
        idleTimeNanos.observe(System.nanoTime() - startTimeNanos);
    }

    /**
     * Marks the start of IO process period. Returns the start time in nanos.
     */
    public long markIOProcessStart() {
        return System.nanoTime();
    }

    /**
     * Marks the finish of IO process period. Returns the duration in nanos.
     */
    public long markIOProcessFinish(long startTimeNanos) {
        final long duration = System.nanoTime() - startTimeNanos;
        ioProcessTimeNanos.observe(duration);
        return duration;
    }

    /**
     * Marks the start of task process period. Returns the start time in nanos.
     */
    public long markTaskProcessStart() {
        return System.nanoTime();
    }

    /**
     * Marks the finish of task process period.
     */
    public void markTaskProcessFinish(long startTimeNanos) {
        taskProcessTimeNanos.observe(System.nanoTime() - startTimeNanos);
    }

    /**
     * Called when a task is executed.
     */
    public void onTaskExecuted() {
        numTasksExecuted.observe(1L);
    }

    /**
     * Called when a new immediate task is being submitted.
     */
    public void onImmediateTaskSubmitted() {
        numImmediateTasksSubmitted.observe(1L);
    }

    /**
     * Called when a new delayed task is being submitted.
     */
    public void onDelayedTaskSubmitted() {
        numDelayedTasksSubmitted.observe(1L);
    }

    /**
     * Called when the channel executor reports responsiveness to the thread pool.
     */
    public void onReportResponsiveness() {
        lastResponsivenessTimestamp = System.nanoTime();
    }

    /**
     * Called when the channel executor reports the number of quiescent periods.
     */
    public void onReportQuiescent(boolean isLastPeriodQuiescent) {
        if (!isLastPeriodQuiescent) {
            quiescentPeriodTimestamp = -1;
            return;
        }
        final long ts = quiescentPeriodTimestamp;
        if (ts == -1) {
            quiescentPeriodTimestamp = System.nanoTime();
        }
    }

    /**
     * Adds an endpoint handler perf tracker.
     */
    public void addEndpointHandlerPerfTracker(
        NioEndpointHandlerPerfTracker tracker)
    {
        handlerPerfTrackers.add(tracker);
    }

    /**
     * Removes an endpoint handler perf tracker.
     */
    public void removeEndpointHandlerPerfTracker(
        NioEndpointHandlerPerfTracker tracker)
    {
        handlerPerfTrackers.remove(tracker);
    }

    /**
     * Returns the channel executor perf since last time.
     */
    public NioChannelExecutorPerf obtain(String watcherName, boolean clear) {
        final long current = System.nanoTime();
        return new NioChannelExecutorPerf(
            childId, childIndex,
            idleTimeNanos.obtain(watcherName, clear),
            ioProcessTimeNanos.obtain(watcherName, clear),
            taskProcessTimeNanos.obtain(watcherName, clear),
            numTasksExecuted.obtain(watcherName, clear),
            numImmediateTasksSubmitted.obtain(watcherName, clear),
            numDelayedTasksSubmitted.obtain(watcherName, clear),
            current - lastResponsivenessTimestamp,
            getQuiescentDurationNanos(current),
            handlerPerfTrackers.stream()
            .map((t) -> t.obtain(watcherName, clear))
            .collect(Collectors.toList()));
    }

    /**
     * Returns the quiescent duration in nanos.
     */
    private long getQuiescentDurationNanos(long current) {
        final long ts = quiescentPeriodTimestamp;
        if (ts == -1) {
            /*
             * We use -1 to indicate there was no quiescent. It will be very
             * rare that by accident, the timestamp is set to -1.
             */
            return 0;
        }
        return current - ts;
    }
}
