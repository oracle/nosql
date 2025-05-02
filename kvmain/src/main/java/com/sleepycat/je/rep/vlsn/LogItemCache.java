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
package com.sleepycat.je.rep.vlsn;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import com.sleepycat.je.log.LogItem;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatGroup;

/**
 * A no-wait cache used to retain the most recent VLSNs. The Feeders check this
 * cache first for log entries to send out to the Replicas. Feeders that are
 * feeding at the most up to date portion of the replication stream will likely
 * hit in the cache, preventing a lookup in the log buffers or log files.
 *
 * The log item cache is made up of references so there is never any
 * guarantee that even the most recent 32 entries are in there.
 */
class LogItemCache {

    private final int sizeMask;
    private final AtomicReference<LogItem>[] items;
    private final LongStat nHits;
    private final LongStat nMisses;

    /**
     * Creates a log item size of the specified size.
     *
     * @param cacheSize it must be a power of two and small, typically around
     * 32 entries. Increasing sizes typically yield diminishing returns.
     * @param statGroup the statsGroup to which this cache adds its stats
     * @throws IllegalArgumentException via ReplicatedEnvironment ctor.
     */
    @SuppressWarnings("unchecked")
    LogItemCache(int cacheSize, StatGroup statGroup) {
        if (Integer.bitCount(cacheSize) != 1) {
            throw new IllegalArgumentException
                ("Bad cache size: " + cacheSize + "; it must be a power of 2");
        }
        sizeMask = cacheSize - 1;
        items = new AtomicReference[cacheSize];
        for (int i = 0; i < cacheSize; i += 1) {
            items[i] = new AtomicReference<>(null);
        }
        nHits = new LongStat(statGroup, VLSNIndexStatDefinition.N_HITS);
        nMisses = new LongStat(statGroup, VLSNIndexStatDefinition.N_MISSES);
    }

    void put(long vlsn, LogItem item) {

        final LogItem oldItem =
            items[(int)vlsn & sizeMask].getAndSet(item);
        /*
         * The pooled buffer must be deallocated only when changing the
         * reference to a different LogItem, to guarantee that deallocation
         * occurs only once.
         */
        if (oldItem != null) {
            oldItem.requestDeallocate();
        }
    }

    LogItem get(long vlsn) {

        final AtomicReference<LogItem> ref =
            items[(int)vlsn & sizeMask];

        final LogItem item = ref.get();

        if (item != null &&
            item.header.getVLSN() == vlsn &&
            item.incrementUse()) {
            /*
             * It's possible that the pooled buffer referenced by this LogItem
             * has been reused by a different LogItem while we were doing the
             * checks above. If this happened, the LogItem in this ref would
             * have been replaced by put() before the buffer was deallocated.
             * So we can compare the ref value to guard against this.
             */
            if (item == ref.get()) {
                nHits.increment();
                return item;
            }

            item.decrementUse();
        }

        nMisses.increment();
        return null;
    }

    /**
     * Clear cached entries with VLSNs matching the given predicate.
     */
    void clear(Predicate<Long> predicate) {

        for (final AtomicReference<LogItem> ref : items) {
            final LogItem item = ref.get();

            if (item != null &&
                predicate.test(item.header.getVLSN()) &&
                ref.compareAndSet(item, null)) {
                /*
                 * The pooled buffer must be deallocated only when changing
                 * the reference to a different LogItem, to guarantee that
                 * deallocation occurs only once.
                 */
                item.requestDeallocate();
            }
        }
    }
}
