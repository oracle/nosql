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

package oracle.kv.impl.api;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.KeyRange;

/**
 * StoreIteratorParams serves two purposes:
 *
 * (1) it is a struct to hold the arguments to KVStore.storeIterator() so that
 * they can be passed around as a single arg rather than the usual 9 args.
 *
 * (2) it isolates out the two operations that are different for storeIterator
 * and storeKeysIterator. generateGetterOp() generates either a StoreKeysIterate
 * or StoreIterate operation to be passed to the server side. Since these two
 * operations return different types to the client (byte[] and KeyValueVersion)
 * convertResults() makes the right conversions depending on the type of
 * operation.
 *
 * ConvertResultsReturnValue is a simple struct to hold multiple return values
 * from the convertResults method. If Java provided multi-value returns, then
 * we wouldn't need this.
 */
public class StoreIteratorParams {
    protected final byte[] parentKeyBytes;
    protected final Direction direction;
    protected int batchSize;
    /* The max number of KB read per request */
    protected int maxReadKB;
    protected final KeyRange subRange;
    protected final Depth depth;
    protected final Consistency consistency;
    protected final long timeout;
    protected final TimeUnit timeoutUnit;
    protected final boolean excludeTombstones;

    /*
     * The set of partition that the iterator should read from. If null,
     * then all partitions are accessed.
     */
    private final Set<Integer> partitions;

    public StoreIteratorParams(final Direction direction,
                               final int batchSize,
                               final byte[] parentKeyBytes,
                               final KeyRange subRange,
                               final Depth depth,
                               final Consistency consistency,
                               final long timeout,
                               final TimeUnit timeoutUnit,
                               final boolean excludeTombstones) {
        this(direction, batchSize, parentKeyBytes, subRange, depth,
             consistency, timeout, timeoutUnit, null, excludeTombstones);
    }

    public StoreIteratorParams(final Direction direction,
                               final int batchSize,
                               final byte[] parentKeyBytes,
                               final KeyRange subRange,
                               final Depth depth,
                               final Consistency consistency,
                               final long timeout,
                               final TimeUnit timeoutUnit,
                               final Set<Integer> partitions,
                               final boolean excludeTombstones) {

        this(direction, batchSize, 0, parentKeyBytes, subRange, depth,
             consistency, timeout, timeoutUnit, partitions, excludeTombstones);
    }

    public StoreIteratorParams(final Direction direction,
                               final int batchSize,
                               final int maxReadKB,
                               final byte[] parentKeyBytes,
                               final KeyRange subRange,
                               final Depth depth,
                               final Consistency consistency,
                               final long timeout,
                               final TimeUnit timeoutUnit,
                               final Set<Integer> partitions,
                               final boolean excludeTombstones) {
        /*
         * Set default values for batchSize and Depth if not set.
         */
        this.batchSize = (batchSize > 0 ? batchSize :
                          (maxReadKB == 0) ? KVStoreImpl.DEFAULT_ITERATOR_BATCH_SIZE : 0);
        this.depth = (depth != null ? depth : Depth.PARENT_AND_DESCENDANTS);

        this.maxReadKB = maxReadKB;
        this.direction = direction;
        this.parentKeyBytes = parentKeyBytes;
        this.subRange = subRange;
        this.consistency = consistency;
        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
        this.partitions = partitions;
        this.excludeTombstones = excludeTombstones;
    }

    public byte[] getParentKeyBytes() {
        return parentKeyBytes;
    }

    public Direction getDirection() {
        return direction;
    }

    public Direction getPartitionDirection() {
        /*
         * If it is UNORDERED, turn into FORWARD because that is what will be
         * sent to each partition.
         */
        return direction == Direction.UNORDERED ? Direction.FORWARD : direction;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setMaxReadKB(int maxReadKB) {
        this.maxReadKB = maxReadKB;
    }

    public int getMaxReadKB() {
        return maxReadKB;
    }

    public KeyRange getSubRange() {
        return subRange;
    }

    public Depth getDepth() {
        return depth;
    }

    public Consistency getConsistency() {
        return consistency;
    }

    public long getTimeout() {
        return timeout;
    }

    public TimeUnit getTimeoutUnit() {
        return timeoutUnit;
    }

    /**
     * Returns the set of partition that the iterator should access. A
     * null return indicates that all partitions should be read.
     *
     * @return the list of partitions to read from
     */
    public Set<Integer> getPartitions() {
        return partitions;
    }

    public boolean getExcludeTombstones() {
        return excludeTombstones;
    }
}
