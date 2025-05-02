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

import oracle.kv.table.WriteOptions;

/**
 * BulkWriteOptions is used to configure bulk write operations.
 *
 * The default values, documented in the setter methods, should be a good
 * choice for a wide range of conditions. If you should wish to fine tune the
 * bulk load operation further, you can use these values as a starting point
 * for a benchmark using your own application data and actual hardware.
 *
 * @since 4.0
 */
public class BulkWriteOptions extends WriteOptions {
   /*
    * TODO:
    *
    * 1) Allow for passing in (stream and shard) thread pools, so they can be
    * reused across batch loads?
    *
    * 2) Can such pools be shared across concurrent putBatch operations? The
    * current code relies on pool shutdown which requires exclusive use of
    * pools.
    */
    private final static int MIN_REQUEST_SIZE = 64 * 1024;

    private int bulkHeapPercent = 40;

    private int maxRequestSize = 512 * 1024;

    private int perShardParallelism = 3;

    private int streamParallelism = 1;

    private boolean overwrite = false;

    private boolean usePutResolve;

    /**
     * The options used to configure the bulk put operation.
     *
     * @param durability the durability to be used by the underlying
     * write operations that make up the bulk put.
     *
     * @param timeout the timeout associated with the underlying
     * write operations that make up the bulk put.
     *
     * @param timeoutUnit the units associated with the timeout
     */
    public BulkWriteOptions(Durability durability,
                            long timeout,
                            TimeUnit timeoutUnit) {
        super(durability, timeout, timeoutUnit);
    }

    /* Internal copy constructor. */
    public BulkWriteOptions(BulkWriteOptions options) {
        super(options);

        bulkHeapPercent = options.bulkHeapPercent;
        maxRequestSize = options.maxRequestSize;
        perShardParallelism = options.perShardParallelism;
        streamParallelism = options.streamParallelism;
    }

    /**
     * Create a {@code BulkWriteOptions} with default values.
     */
    public BulkWriteOptions() {
        super();
    }

    /**
     * Returns the percentage of Runtime.maxMemory() that can be used for
     * the operation.
     */
    public int getBulkHeapPercent() {
        return bulkHeapPercent;
    }

    /**
     * The percentage of Runtime.maxMemory() that can be used for the
     * operation. This heap is used to assemble batches of entries
     * associated with specific shards and partitions.
     * <p>
     * The default is 40%.
     * </p>
     */
    public void setBulkHeapPercent(int bulkHeapPercent) {

        if (bulkHeapPercent > 100) {
            throw new IllegalArgumentException
                ("Percentage:" + bulkHeapPercent + " cannot exceed 100");
        }

        if (bulkHeapPercent < 1 ) {
            throw new IllegalArgumentException
                ("Percentage:" + bulkHeapPercent + " cannot be less than 1");
        }
        this.bulkHeapPercent = bulkHeapPercent;
    }

    /**
     * Returns the max number of bytes of records in a single bulk put request.
     */
    public int getMaxRequestSize() {
        return maxRequestSize;
    }

    /**
     * The max request size is used to limit the total number of bytes of
     * records in a single bulk put request.
     * <p>
     * The default is 512K.
     * </p>
     */
    public void setMaxRequestSize(int maxRequestSize) {

        if (maxRequestSize < MIN_REQUEST_SIZE) {
            throw new IllegalArgumentException
                ("Max request size:" + maxRequestSize + " cannot be less " +
                 "than " + MIN_REQUEST_SIZE);
        }
        this.maxRequestSize = maxRequestSize;
    }

    /**
     * The maximum number of threads that can concurrently write a batch
     * of entries to a single shard in the store.
     */
    public int getPerShardParallelism() {
        return perShardParallelism;
    }

    /**
     * Sets the maximum number of threads that can concurrently write it's
     * batch of entries to a single shard in the store.
     * <p>
     * The default value is 3 and allows for overlapping the reading of the
     * next batch with processing of the current batch at a server node.
     * Higher capacity networks and and storage nodes can allow for
     * higher parallelism.
     * </p>
     */
    public void setPerShardParallelism(int perShardParallelism) {
        if (perShardParallelism < 1 ) {
            throw new IllegalArgumentException
                ("Maximum number of threads per shard:" + perShardParallelism +
                 " cannot be less than 1");
        }
        this.perShardParallelism = perShardParallelism;
    }

    /**
     * Returns the maximum number of streams that can be read concurrently.
     * Each stream is read by a dedicated thread from a thread pool. This
     * setting  determines the size of the thread pool used for
     * reading streams.
     */
    public int getStreamParallelism() {
        return streamParallelism;
    }

    /**
     * Sets the maximum number of streams that can be read concurrently.
     * Each stream is read by a dedicated thread from a thread pool. This
     * setting determines the size of the thread pool used for reading
     * streams.
     * <p>
     * The default parallelism is 1. For streams with high overheads, say
     * because the I/O device underlying the stream is slow and there are
     * different I/O devices underlying each stream, a higher value would
     * be appropriate.
     * </p>
     */
    public void setStreamParallelism(int streamParallelism) {
        if (streamParallelism < 1 ) {
            throw new IllegalArgumentException
                ("Maximum number of streams concurrently read:" +
                 streamParallelism + " cannot be less than 1");
        }
        this.streamParallelism = streamParallelism;
    }

    /**
     * @hidden
     * Use by data loader
     *
     * Returns whether overwrites the record if its key already present in the
     * store.
     *
     * @return true if overwrites the record if its key already present,
     * otherwise return false.
     */
    public boolean getOverwrite() {
        return overwrite;
    }

    /**
     * @hidden
     * Use by data loader
     *
     * Set whether overwrites the record when put an entry associated with a
     * key that's already present in the store.
     * <p>
     * Set to true to overwrite the existing record, otherwise the record will
     * be rejected and {@link EntryStream#keyExists(Object)} will be invoked,
     * the default value is false.
     * <p>
     * Insertion of entries containing the same key within a stream is strictly
     * ordered, the first entry will be inserted (if it's not already present)
     * and the second entry and subsequent entries will result in overwrite or
     * the invocation of {@link EntryStream#keyExists(Object)}.
     * <p>
     * The behavior of duplicate entries contained in different streams is thus
     * undefined, the first entry to win the race is inserted and subsequent
     * duplicates will result in overwrite or the invocation of
     * {@link EntryStream#keyExists(Object)}.
     *
     * @param value whether to overwrite the record
     */
    public void setOverwrite(boolean value) {
        overwrite = value;
    }

    /**
     * @hidden
     * Used by migrator and restore-oriented tools
     *
     * Instead of using Put semantics, use PutResolve semantics, allowing
     * the modification time and region id to be used to determine which
     * record to use as well as merging MR counters in the case of a multi-
     * region table with MR counters.
     * <p>
     * This flag implies overwrite because PutResolve semantics imply a
     * potential overwrite. Conflict resolution is done using the modification
     * time from the {@link EntryStream}. If the row modification time is not
     * set (is 0) and there is a conflict with an existing row the existing row
     * will always win the resolution and not be overwritten.
     * @param value whether to use PutResolve for all records where modification
     * time is available
     */
    public void setUsePutResolve(boolean value) {
        usePutResolve = value;
    }

    /**
     * @hidden
     * Used by migrator and restore-oriented tools
     *
     * Returns whether or not to use PutResolve
     * @return true if PutResolve is to be used.
     */
    public boolean getUsePutResolve() {
        return usePutResolve;
    }
}
