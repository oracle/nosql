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

package oracle.kv;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import oracle.kv.stats.DetailedMetrics;

/**
 * Interface to the specialized Iterator type returned by the {@link
 * KVStore#storeIterator(Direction, int, Key, KeyRange, Depth, Consistency, long,
 * TimeUnit, StoreIteratorConfig) Parallel Scan version} of storeIterator().
 * <p>
 * This Iterator adds the ability to close (terminate) a ParallelScan as well
 * gather per-partition and per-shard statistics about the scan.
 * <p>
 * Iterators implementing this interface can only be used safely by one thread
 * at a time unless synchronized externally.
 */
public interface ParallelScanIterator<K> extends Iterator<K> {

    /**
     * Close (terminate) a Parallel Scan. This shutdowns down all related
     * threads and tasks, and awaits up to 60 seconds for threads to exit.
     */
    public void close();

    /**
     * Gets the per-partition metrics for this Parallel Scan. This may be
     * called at any time during the iteration in order to obtain metrics to
     * that point or it may be called at the end to obtain metrics for the
     * entire scan. If there are no metrics available yet for a particular
     * partition, then there will not be an entry in the list.
     *
     * @return the per-partition metrics for this Parallel Scan.
     */
    public List<DetailedMetrics> getPartitionMetrics();

    /**
     * Gets the per-shard metrics for this Parallel Scan. This may be called at
     * any time during the iteration in order to obtain metrics to that point
     * or it may be called at the end to obtain metrics for the entire scan.
     * If there are no metrics available yet for a particular shard, then there
     * will not be an entry in the list.
     *
     * @return the per-shard metrics for this Parallel Scan.
     */
    public List<DetailedMetrics> getShardMetrics();

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration.
     *
     * @throws NoSuchElementException - iteration has no more elements.
     *
     * @throws StoreIteratorException - an exception occurred during a
     * retrieval as part of a multi-record iteration method. This exception
     * does not necessarily close or invalidate the iterator. Repeated calls to
     * next() may or may not cause an exception to be thrown. It is incumbent
     * on the caller to determine the type of exception and act accordingly.
     */
    @Override
    public K next();
}
