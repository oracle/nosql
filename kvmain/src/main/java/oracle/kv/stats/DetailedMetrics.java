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

package oracle.kv.stats;

/**
 * Interface to the per-partition and per-shard metrics returned by
 * {@link oracle.kv.ParallelScanIterator#getPartitionMetrics and
 * oracle.kv.ParallelScanIterator#getShardMetrics()}.
 */
public interface DetailedMetrics {

    /**
     * Return the name of the Shard or a Partition.
     */
    public String getName();

    /**
     * Return the total time in Milli Seconds for scanning the Shard or
     * Partition.
     */
    public long getScanTime();

    /**
     * Return the record count for the Shard or Partition.
     */
    public long getScanRecordCount();
}
