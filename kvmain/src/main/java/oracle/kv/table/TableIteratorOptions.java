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

package oracle.kv.table;

import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.KVStoreConfig;
import oracle.kv.impl.api.table.PrimaryKeyImpl;

/**
 * TableIteratorOptions extends ReadOptions and is passed to read-only store
 * operations that return iterators.  It is used to specify non-default
 * behavior.  Default behavior is configured when a store is opened using
 * {@link KVStoreConfig}.
 *
 * @since 3.0
 */
public class TableIteratorOptions extends ReadOptions {

    private final Direction direction;

    private final int maxConcurrentRequests;
    private final int batchResultsSize;
    /* The max number of KB read per request */
    private final int maxReadKB;
    private PrimaryKey resumePrimaryKey;
    /**
     * True if include tombstones in table iteration, false otherwise. This
     * parameter is for internal use and hidden from public.
     */
    private boolean includeTombstones = false;

    /**
     * Creates a {@code TableIteratorOptions} with the specified parameters.
     * Equivalent to
     * {@code TableIteratorOptions(direction, consistency, timeout,
     * timeoutUnit, 0, 0)}.
     *
     * @param direction a direction
     * @param consistency the read consistency or null
     * @param timeout the request timeout or 0
     * @param timeoutUnit the {@link TimeUnit} for the {@code timeout}
     * parameter or null
     *
     * @throws IllegalArgumentException if direction is null, the timeout
     * is negative, or timeout is &gt; 0 and timeoutUnit is null
     */
    public TableIteratorOptions(Direction direction,
                                Consistency consistency,
                                long timeout,
                                TimeUnit timeoutUnit) {
        this(direction, consistency, timeout, timeoutUnit, 0, 0);
    }

    /**
     * Creates a {@code TableIteratorOptions} with the specified parameters,
     * including {@code maxConcurrentRequests} and {@code batchResultsSize}
     * values.
     * <p>
     * If {@code consistency} is {@code null}, the
     * {@link KVStoreConfig#getConsistency default consistency}
     * is used. If {@code timeout} is zero the
     * {@link KVStoreConfig#getRequestTimeout default request timeout} is used.
     * <p>
     * {@code maxConcurrentRequests} specifies the maximum degree of
     * parallelism to be used when running an iteration. Setting {@code
     * maxConcurrentRequests} to 1 causes the iteration to be performed using
     * only a single thread. Setting it to 0 lets the KV Client determine the
     * degree of concurrency based on topology information (up to a maximum of
     * the number of available processors as returned by
     * java.lang.Runtime.availableProcessors()). Values less than 0 cause an
     * {@code IllegalArgumentException} to be thrown.
     *
     * @param direction a direction
     * @param consistency the read consistency or null
     * @param timeout the request timeout or 0
     * @param timeoutUnit the {@code TimeUnit} for the {@code timeout}
     * parameter or null
     * @param maxConcurrentRequests the maximum number of concurrent requests
     * @param batchResultsSize the number of results per request
     *
     * @throws IllegalArgumentException if direction is null, the timeout is
     * negative, timeout is &gt; 0 and timeoutUnit is null, or
     * maxConcurrentRequests or batchResultsSize is less than 0
     *
     * @since 3.4
     */
    public TableIteratorOptions(Direction direction,
                                Consistency consistency,
                                long timeout,
                                TimeUnit timeoutUnit,
                                int maxConcurrentRequests,
                                int batchResultsSize) {
        this(direction, 0, consistency, timeout, timeoutUnit,
             maxConcurrentRequests, batchResultsSize);
    }

    /**
     * @hidden
     */
    public TableIteratorOptions(Direction direction,
                                int maxReadKB,
                                Consistency consistency,
                                long timeout,
                                TimeUnit timeoutUnit,
                                int maxConcurrentRequests,
                                int batchResultsSize) {
        super(consistency, timeout, timeoutUnit);
        if (direction == null) {
            throw new IllegalArgumentException("direction must not be null");
        }
        if (maxConcurrentRequests < 0) {
            throw new IllegalArgumentException
                ("maxConcurrentRequests must be >= 0");
        }
        if (batchResultsSize < 0) {
            throw new IllegalArgumentException("batchResultsSize must be >= 0");
        }
        this.direction = direction;
        this.maxConcurrentRequests = maxConcurrentRequests;
        this.batchResultsSize = batchResultsSize;
        this.maxReadKB = maxReadKB;
        resumePrimaryKey = null;
    }

    /**
     * @deprecated since 3.4, no longer supported.
     *
     * replaced by {@link #TableIteratorOptions(Direction, Consistency, long,
     * TimeUnit, int, int)}
     */
    @Deprecated
    public TableIteratorOptions(Direction direction,
                                Consistency consistency,
                                long timeout,
                                TimeUnit timeoutUnit,
                                int maxConcurrentRequests,
                                int batchResultsSize,
                                @SuppressWarnings("unused")
                                int maxResultsBatches) {
        this(direction, consistency, timeout, timeoutUnit,
             maxConcurrentRequests, batchResultsSize);
    }

    /**
     * Returns the direction.
     *
     * @return the direction
     */
    public Direction getDirection() {
        return direction;
    }

    /**
     * Returns the maximum number of concurrent requests, or {@code 0} if no
     * maximum was specified.
     *
     * @return the maximum number of concurrent requests or {@code 0}
     */
    public int getMaxConcurrentRequests() {
        return maxConcurrentRequests;
    }

    /**
     * Returns the number of results per request, or {@code 0} if no value was
     * specified.
     *
     * @return the number of results or {@code 0}
     */
    public int getResultsBatchSize() {
        return batchResultsSize;
    }

    /**
     * @hidden
     *
     * Returns the max number of KB read per request.
     *
     * @return the max number of KB read.
     */
    public int getMaxReadKB() {
        return maxReadKB;
    }

    /**
     * @deprecated since 3.4, no longer supported.
     *
     * Returns zero.
     *
     * @return zero
     */
    @Deprecated
    public int getMaxResultsBatches() {
        return 0;
    }

    /**
     * For internal use only
     * @hidden
     *
     * Sets the resume primary key
     * <p>
     * The resume primary key is used to continue table iteration, it references
     * the position where the table iteration started.
     * <p>
     * The resume primary key is exclusive, the table iteration resumes after
     * the resume key and does not include the resume key if it is a complete
     * primary key.
     * <p>
     * It takes effect only in the these 2 table iteration APIs:
     * {@link TableAPI#tableIterator(PrimaryKey, MultiRowOptions,
     * TableIteratorOptions)} and
     * {@link TableAPI#tableKeysIterator(PrimaryKey, MultiRowOptions,
     * TableIteratorOptions)}.
     * If resumePrimaryKey specified, the parameter {@code PrimaryKey} of
     * table iterator APIs can be null.
     * <p>
     * It can not be used if {@link Direction#UNORDERED} specified, it can be
     * complete or partial but must contains all of the fields defined for the
     * target table's shard key.
     *
     * @param resumeKey the resume primary key
     *
     * @since 21.1
     */
    public void setResumePrimaryKey(PrimaryKey resumeKey) {
        /* Resume primary key */
        if (resumeKey != null) {
            /* The resume primary key is used with ordered scanning */
            if (direction == Direction.UNORDERED) {
                throw new IllegalArgumentException("The resumePrimaryKey can " +
                    "not be used with Direction.UNORDERED: " + resumeKey);
            }

            /* The resume primary key must be complete shard key */
            if (!((PrimaryKeyImpl)resumeKey).isCompleteShardKey()) {
                throw new IllegalArgumentException("The resumePrimaryKey " +
                    "must contain complete shard key: " + resumeKey);
            }
        }

        this.resumePrimaryKey = resumeKey;
    }

    /**
     * @hidden
     * Returns the resume primary key
     */
    public PrimaryKey getResumePrimaryKey() {
        return resumePrimaryKey;
    }

    /**
     * @hidden
     * Sets that the table iteration should include tombstones
     */
    public void setIncludeTombstones() {
        includeTombstones = true;
    }

    /**
     * @hidden
     * Returns true if the table iteration should include tombstones, false
     * otherwise.
     */
    public boolean getIncludeTombstones() {
        return includeTombstones;
    }
}
