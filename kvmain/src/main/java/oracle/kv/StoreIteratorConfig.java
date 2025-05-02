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

import java.util.concurrent.TimeUnit;

/**
 * The configuration object for {@link KVStore#storeIterator(Direction, int,
 * Key, KeyRange, Depth, Consistency, long, TimeUnit, StoreIteratorConfig)}.
 */
public class StoreIteratorConfig {
    private int maxConcurrentRequests;
    private int maxResultsBatches;

    /**
     * Sets the maximum degree of parallelism (in effect the maximum number of
     * client-side threads) to be used when running a parallel store iteration.
     * Setting maxConcurrentRequests to 1 causes the store iteration to be
     * performed using only the current thread. Setting it to 0 lets the KV
     * Client determine the number of threads based on topology information (up
     * to a maximum of the number of available processors as returned by
     * java.lang.Runtime.availableProcessors()). Values less than 0 are
     * reserved for some future use and cause an IllegalArgumentException to be
     * thrown.
     *
     * @param maxConcurrentRequests the maximum number of client-side threads.
     *
     * @return this
     *
     * @throws IllegalArgumentException if a value less than 0 is passed for
     * maxConcurrentRequests.
     */
    public StoreIteratorConfig
        setMaxConcurrentRequests(int maxConcurrentRequests) {

        if (maxConcurrentRequests < 0) {
            throw new IllegalArgumentException
                ("maxConcurrentRequests must be >= 0");
        }
        this.maxConcurrentRequests = maxConcurrentRequests;
        return this;
    }

    /**
     * Returns the maximum number of concurrent requests.
     *
     * @return the maximum number of concurrent requests
     */
    public int getMaxConcurrentRequests() {
        return maxConcurrentRequests;
    }

    /**
     * @deprecated since 3.4, no longer supported.
     *
     * @param maxResultsBatches the maximum number of results sets that can be
     * held on the client side before Replication Node processing pauses.
     *
     * @return this
     */
    @Deprecated
    public StoreIteratorConfig setMaxResultsBatches(int maxResultsBatches) {
        this.maxResultsBatches = maxResultsBatches;
        return this;
    }

    /**
     * @deprecated since 3.4, no longer supported.
     *
     * Returns the value set by setMaxResultsBatches().
     *
     * @return the value set by setMaxResultsBatches()
     */
    @Deprecated
    public int getMaxResultsBatches() {
        return maxResultsBatches;
    }

    @Override
    public String toString() {
        return String.format("maxConcurrentRequests=" + maxConcurrentRequests);
    }
}
