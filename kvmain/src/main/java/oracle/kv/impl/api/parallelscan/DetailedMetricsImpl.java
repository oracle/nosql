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

package oracle.kv.impl.api.parallelscan;

import oracle.kv.stats.DetailedMetrics;

/**
 * Implementation of the per-partition and per-shard metrics returned by {@link
 * oracle.kv.ParallelScanIterator#getPartitionMetrics and
 * oracle.kv.ParallelScanIterator#getShardMetrics()}.
 */
public class DetailedMetricsImpl implements DetailedMetrics {

    private final String name;
    private long scanTime;
    private long scanRecordCount;

    public DetailedMetricsImpl(final String name,
                        final long scanTime,
                        final long scanRecordCount) {
        this.name = name;
        this.scanTime = scanTime;
        this.scanRecordCount = scanRecordCount;
    }

    /**
     * Return the name of the Shard or a Partition.
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * Return the total time for scanning the Shard or Partition.
     */
    @Override
    public long getScanTime() {
        return scanTime;
    }

    /**
     * Return the record count for the Shard or Partition.
     */
    @Override
    public long getScanRecordCount() {
        return scanRecordCount;
    }

    public synchronized void inc(long time, long count) {
        scanTime += time;
        scanRecordCount += count;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Detailed Metrics: ");
        sb.append(name).append(" records: ").append(scanRecordCount);
        sb.append(" scanTime: ").append(scanTime);
        return sb.toString();
    }
}
