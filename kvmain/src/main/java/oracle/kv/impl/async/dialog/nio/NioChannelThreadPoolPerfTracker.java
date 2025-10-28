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

package oracle.kv.impl.async.dialog.nio;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.async.perf.NioChannelExecutorPerf;
import oracle.kv.impl.async.perf.NioChannelThreadPoolPerf;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.nosql.common.sklogger.measure.LongPercentileElement;

/**
 * Tracks the performance metrics of the NioChannelThreadPool.
 */
public class NioChannelThreadPoolPerfTracker {

    /**
     * The threshold for heartbeat staleness. An executor with the most recent
     * heartbeat time value more stale than the theshold comparing to the
     * current time is consider not responsive. Default to 1 minute.
     */
    public static final String HEARTBEAT_INTERVAL_THRESHOLD_MILLIS =
        "oracle.kv.async.executor.heartbeat.interval.threshold.millis";
    public static final long DEFAULT_HEARTBEAT_INTERVAL_THRESHOLD_MILLIS =
        60 * 1000;
    public static volatile long heartbeatThresholdMillis = Long.getLong(
        HEARTBEAT_INTERVAL_THRESHOLD_MILLIS,
        DEFAULT_HEARTBEAT_INTERVAL_THRESHOLD_MILLIS);
    /**
     * The interval for checking for stuck executors. Default to 5 minutes.
     */
    public static final String HEARTBEAT_CHECK_INTERVAL_MILLIS =
        "oracle.kv.async.executor.heartbeat.check.interval.millis";
    public static final long DEFAULT_HEARTBEAT_CHECK_INTERVAL_MILLIS =
        5 *  60 * 1000;
    public static volatile long heartbeatCheckIntervalMillis = Long.getLong(
        HEARTBEAT_CHECK_INTERVAL_MILLIS,
        DEFAULT_HEARTBEAT_CHECK_INTERVAL_MILLIS);
    /**
     * The dump threshold. Do not dump thread stack trace if we have already
     * dumped one recently within the threshold. Default to 10 minutes.
     */
    public static final String STUCK_EXECUTOR_DUMP_THRESHOLD_MILLIS =
        "oracle.kv.async.executor.stuck.dump.threshold.millis";
    public static final long DEFAULT_STUCK_EXECUTOR_DUMP_THRESHOLD_MILLIS =
        10 *  60 * 1000;
    public static volatile long stuckExecutorDumpThresholdMillis =
        Long.getLong(STUCK_EXECUTOR_DUMP_THRESHOLD_MILLIS,
                     DEFAULT_STUCK_EXECUTOR_DUMP_THRESHOLD_MILLIS);

    // TODO: The following fields are used as a temporary fix for the issue
    // that the async data overflows the stats ([KVSTORE-1818]). Remove when
    // the more suitable solution is ready.
    /**
     * Whether to do rate limiting. Default to true. Can be set to false for
     * testing.
     */
    public static volatile boolean enableRateLimiting = true;
    /**
     * The duration in seconds that we want to keep for stats data.
     * Default to 7 days.
     */
    private static final int STATS_ROTATION_MAX_DURATION_SECS = 7 * 24 * 3600;
    /**
     * The number of bytes we can generate per second such that we can satisfy
     * STATS_ROTATION_MAX_DURATION_SECS with default settings.
     *
     * We use the default value of ParameterState.SN_LOG_FILE_COUNT_DEFAULT
     * (20) and ParameterState.SN_LOG_FILE_LIMIT_DEFAULT (5M), but we do not
     * want to import ParameterState since this class can be used in the
     * client.
     */
    private static final long MAX_OBTAIN_NUM_BYTES_PER_SEC =
        20 * 5 * 1024 * 1024
        / STATS_ROTATION_MAX_DURATION_SECS;
    /**
     * The approximate number of bytes per executor in DialogEndpointPerf. This
     * is estimated from release test data.
     */
    private static final long NUM_BYTES_PER_EXECUTOR = 300;
    /**
     * The approximate number of bytes per handler in DialogEndpointPerf. This
     * is estimated from release test data.
     */
    private static final long NUM_BYTES_PER_HANDLER = 2000;
    /**
     * The timestamp in nanos when we last obtain the perf.
     */
    private volatile long lastObtainTimeNanos = -1;
    /**
     * The duration after which we can obtain a new perf without vilating
     * STATS_ROTATION_MAX_DURATION_SECS.
     */
    private volatile long nextObtainDurationNanos = 0;

    /** The tracked thread pool. */
    private final NioChannelThreadPool pool;
    /** The logger. */
    private final RateLimitingLogger<String> rateLimitingLogger;
    /** executor heartbeat times. */
    private final AtomicLongArray heartbeatTimes;
    /** The current number of alive executors. */
    private final AtomicInteger numAliveExecutors = new AtomicInteger(0);
    /** The perf metric of alive executors. */
    private final LongPercentileElement aliveExecutorCount =
        new LongPercentileElement();
    /** The number of unresponsive executors. */
    private final AtomicInteger numUnresponsiveExecutors =
        new AtomicInteger(0);
    /** The interval start. */
    private volatile long currIntervalStart;

    public NioChannelThreadPoolPerfTracker(
        NioChannelThreadPool pool,
        Logger logger,
        ScheduledExecutorService backupSchedExecutor)
    {
        this.pool = pool;
        this.rateLimitingLogger = new RateLimitingLogger<String>(
            30 * 60 * 1000 /* 30 minutes */,
            10 /* types of objects */,
            logger);
        this.heartbeatTimes =
            new AtomicLongArray(pool.getExecutors().length());
        backupSchedExecutor.scheduleWithFixedDelay(
            new HeartbeatCheckTask(), 1,
            heartbeatCheckIntervalMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Called when a new executor is created.
     */
    public void onExecutorCreated() {
        final int n = numAliveExecutors.incrementAndGet();
        aliveExecutorCount.observe(n);
    }

    /**
     * Called when an executor is shut down.
     */
    public void onExecutorShutdown() {
        final int n = numAliveExecutors.decrementAndGet();
        aliveExecutorCount.observe(n);
    }

    /**
     * Reports that a channel executor is responsive.
     */
    void reportResponsive(int childIndex) {
        heartbeatTimes.set(
            childIndex, System.currentTimeMillis());
    }

    /**
     * Periodically checks if all the executors are responsive and dumps the
     * stack trace if the executor is stuck.
     */
    private class HeartbeatCheckTask implements Runnable {

        private final AtomicLongArray dumpTimes =
            new AtomicLongArray(heartbeatTimes.length());

        @Override
        public void run() {
            final long currentTimeMillis = System.currentTimeMillis();
            int unresponsiveCount = 0;
            for (int i = 0; i < heartbeatTimes.length(); ++i) {
                if (dumpIfExecutorNotResponsive(currentTimeMillis, i)) {
                    unresponsiveCount++;
                }
            }
            numUnresponsiveExecutors.set(unresponsiveCount);
        }

        /**
         * Dumps the executor stack trace if the executor is not responsive and
         * have not dumped for a while. Returns {@code true} if the executor is
         * not responsive.
         */
        private boolean dumpIfExecutorNotResponsive(long currentTimeMillis,
                                                 int childIndex) {
            final long heartbeatTimeMillis = heartbeatTimes.get(childIndex);
            final long duration = currentTimeMillis - heartbeatTimeMillis;
            if (duration < heartbeatThresholdMillis) {
                /*
                 * We have a heartbeat from the executor within the threshold.
                 */
                return false;
            }
            final NioChannelExecutor executor =
                pool.getExecutors().get(childIndex);
            if (executor == null) {
                /* The executor is not started. */
                return false;
            }
            if (currentTimeMillis - dumpTimes.get(childIndex)
                < stuckExecutorDumpThresholdMillis) {
                /* Already dumped recently. */
                return true;
            }
            final Thread thread = executor.getThread();
            final StringBuilder sb = new StringBuilder(
                String.format("%s not responsive for %s ms since %s. ",
                              NioChannelExecutor.class.getSimpleName(),
                              duration,
                              heartbeatTimeMillis));
            if (thread == null) {
                /* Somehow, the executor has not been initialized correctly. */
                sb.append(" ").append("There is no running thread.");
            } else {
                sb.append("\n");
                CommonLoggerUtils.getStackTrace(thread, sb);
            }
            pool.getLogger().log(Level.INFO, () -> sb.toString());
            dumpTimes.set(childIndex, currentTimeMillis);
            return true;
        }
    }

    /**
     * Returns the number of alive executors for testing.
     */
    public int getNumAliveExecutors() {
        return numAliveExecutors.get();
    }

    /**
     * Obtains an optional {@link NioChannelThreadPoolPerf}.
     */
    public Optional<NioChannelThreadPoolPerf> obtain(String watcherName,
                                                     boolean clear) {
        /*
         * Only return non-null if we are not doing rate limiting or passed the
         * next obtain timestamp.
         */
        final long currNanos = System.nanoTime();
        if (enableRateLimiting
            && (currNanos - lastObtainTimeNanos <= nextObtainDurationNanos)) {
            return Optional.empty();
        }
        final long ts = currIntervalStart;
        final long te = System.currentTimeMillis();
        currIntervalStart = te;
        final List<NioChannelExecutorPerf> executorPerfs = new ArrayList<>();
        final AtomicReferenceArray<NioChannelExecutor> executors =
            pool.getExecutors();
        for (int i = 0; i < executors.length(); ++i) {
            final NioChannelExecutor executor = executors.get(i);
            if (executor != null) {
                executorPerfs.add(
                    executor.getPerfTracker().obtain(watcherName, clear));
            }
        }
        final NioChannelThreadPoolPerf channelThreadPoolPerf =
            new NioChannelThreadPoolPerf(
                ts, te,
                aliveExecutorCount.obtain(watcherName, clear),
                numUnresponsiveExecutors.get(),
                executorPerfs);
        final int numExecutors = channelThreadPoolPerf.getNumExecutorPerfs();
        final int numHandlers = channelThreadPoolPerf.getNumHandlerPerfs();
        final long nbytes =
            numExecutors * NUM_BYTES_PER_EXECUTOR
            + numHandlers * NUM_BYTES_PER_HANDLER;
        final long delaySecs = nbytes / MAX_OBTAIN_NUM_BYTES_PER_SEC;
        final long delayNanos = TimeUnit.SECONDS.toNanos(delaySecs);
        lastObtainTimeNanos = currNanos;
        nextObtainDurationNanos = delayNanos;
        rateLimitingLogger.log(
            "obtain", Level.INFO,
            () -> String.format(
                "Obtained %s with %s executors, %s handlers "
                + "and estimated %s bytes; "
                + "next obtain after %s seconds",
                NioChannelThreadPoolPerf.class.getSimpleName(),
                numExecutors, numHandlers, nbytes, delaySecs));
        return Optional.of(channelThreadPoolPerf);
    }
}
