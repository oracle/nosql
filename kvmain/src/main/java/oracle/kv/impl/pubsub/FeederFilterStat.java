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

package oracle.kv.impl.pubsub;

import oracle.kv.impl.util.FormatUtils;
import oracle.nosql.common.sklogger.measure.LongPercentileElement;
import oracle.nosql.common.sklogger.measure.LongValueStats;

import com.sleepycat.je.rep.subscription.SubscriptionStat;

/**
 * Object represent the feeder filter metrics
 */
public class FeederFilterStat {

    /** last feeder commit timestamp */
    private volatile long lastCommitTimeMs;
    /** modification time of last processed operation in filter */
    private volatile long lastModTimeMs;
    /** last vlsn processed by filter */
    private volatile long lastFilterVLSN;
    /** last vlsn passing the filter */
    private volatile long lastPassVLSN;

    //TODO: using separate classes for raw stat and snapshot from the raw stat
    /** filter lagging metrics results, populated in snapshot */
    private final StreamOpMetrics laggingMs;
    /**
     * raw filter lagging metrics
     *
     * Using LongPercentileElement because the source store likely is in
     * another geographical region and it is unlikely to get sub ms latency
     * across regions.
     */
    private final LongPercentileElement rawLaggingMs;

    /**
     * Create a mutable feeder filter statistics
     */
    public FeederFilterStat() {
        this.lastCommitTimeMs = 0;
        this.lastModTimeMs = 0;
        this.lastFilterVLSN = 0;
        this.lastPassVLSN = 0;
        laggingMs = null;
        rawLaggingMs = new LongPercentileElement();
    }

    /**
     * Create a snapshot instance that is immutable
     */
    private FeederFilterStat(long lastCommitTimeMs,
                             long lastModTimeMs,
                             long lastFilterVLSN,
                             long lastPassVLSN,
                             StreamOpMetrics laggingMs) {
        this.lastCommitTimeMs = lastCommitTimeMs;
        this.lastModTimeMs = lastModTimeMs;
        this.lastFilterVLSN = lastFilterVLSN;
        this.lastPassVLSN = lastPassVLSN;
        this.laggingMs = laggingMs;
        rawLaggingMs = null;
    }

    /**
     * Gets an immutable snapshot from the stats by copying primitives and
     * obtaining snapshot of latency metrics
     *
     * @return snapshot of the statistics
     */
    public synchronized FeederFilterStat getSnapShot() {
        if (isSnapshot()) {
            /* already a snapshot */
            return this;
        }

        /* generate snapshot from raw percentile stats */
        final LongValueStats result = rawLaggingMs.obtain(false);
        return new FeederFilterStat(lastCommitTimeMs,
                                    lastModTimeMs,
                                    lastFilterVLSN,
                                    lastPassVLSN,
                                    new StreamOpMetrics(result));
    }

    public long getLastFilterVLSN() {
        return lastFilterVLSN;
    }

    public long getLastPassVLSN() {
        return lastPassVLSN;
    }

    /**
     * Unit test only
     */
    public long getLastCommitTimeMs() {
        return lastCommitTimeMs;
    }

    /**
     * Unit test only
     */
    public long getLastModTimeMs() {
        return lastModTimeMs;
    }

    public StreamOpMetrics getLaggingMs() {
        if (!isSnapshot()) {
            throw new IllegalStateException(
                "Cannot get raw statistics, please take a snapshot and use " +
                "the returned metrics");
        }
        return laggingMs;
    }

    /**
     * Sets the feeder filter stat from the subscription stat
     * @param stat subscription stat
     */
    synchronized void setFeederFilterStat(SubscriptionStat stat) {
        if (isSnapshot()) {
            throw new IllegalStateException("Cannot set feeder stats for " +
                                            "snapshot");
        }
        lastCommitTimeMs = stat.getLastCommitTimeMs();
        lastModTimeMs = stat.getLastModTimeMs();
        lastFilterVLSN = stat.getLastFilterVLSN();
        lastPassVLSN = stat.getLastPassVLSN();
        rawLaggingMs.observe(getLagging());
    }

    /**
     * Unit test only, normally it should be set via
     * {@code setFeederFilterStat(SubscriptionStat)}
     */
    public void setLaggingMs(long val) {
        if (isSnapshot()) {
            throw new IllegalStateException("Cannot set feeder stats for " +
                                            "snapshot");
        }
        rawLaggingMs.observe(val);
    }

    @Override
    public String toString() {
        return "last commit time=" +
               FormatUtils.formatTimeMillis(lastCommitTimeMs) + ", " +
               "modification time of filter last op=" +
               FormatUtils.formatTimeMillis(lastModTimeMs) + ", " +
               "last filter vlsn=" + lastFilterVLSN + ", " +
               "last pass vlsn=" + lastPassVLSN +
               ", snapshot=" + isSnapshot() +
               (isSnapshot() ? ", laggingMs=" + laggingMs : "");
    }

    private long getLagging() {
        return Math.max(0, lastCommitTimeMs - lastModTimeMs);
    }

    private boolean isSnapshot() {
        return rawLaggingMs == null;
    }
}
