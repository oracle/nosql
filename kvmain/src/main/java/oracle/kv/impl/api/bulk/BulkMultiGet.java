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

package oracle.kv.impl.api.bulk;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import oracle.kv.Consistency;
import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.KeyValueVersion;
import oracle.kv.ParallelScanIterator;
import oracle.kv.StoreIteratorConfig;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.KeySerializer;
import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.StoreIteratorParams;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.ops.MultiGetBatchIterate;
import oracle.kv.impl.api.ops.MultiGetBatchKeysIterate;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.ops.ResultKey;
import oracle.kv.impl.api.ops.ResultKeyValueVersion;
import oracle.kv.impl.api.parallelscan.BaseParallelScanIteratorImpl;
import oracle.kv.impl.api.parallelscan.DetailedMetricsImpl;
import oracle.kv.impl.async.IterationHandleNotifier;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.TopologyUtil;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.contextlogger.LogContext;
import oracle.kv.stats.DetailedMetrics;

import com.sleepycat.je.utilint.PropUtil;

/**
 * Implementation of a bulk get storeIterator and storeKeysIterator.
 *
 * BulkMultiGet.BulkGetIterator&lt;K, V&gt;: Implements the bulk get
 * operation. It provides the common underpinnings for batching both rows and
 * KV pairs.
 */
public class BulkMultiGet {

    /**
     * Creates a bulk get iterator returning KeyValueVersion.
     */
    public static ParallelScanIterator<KeyValueVersion>
        createBulkMultiGetIterator(final KVStoreImpl storeImpl,
                                   final List<Iterator<Key>> parentKeyIterators,
                                   final int batchSize,
                                   final KeyRange subRange,
                                   final Depth depth,
                                   final Consistency consistency,
                                   final long timeout,
                                   final TimeUnit timeoutUnit,
                                   final StoreIteratorConfig config,
                                   final boolean excludeTombstones) {

        /* Prohibit iteration of internal keyspace (//). */
        final KeyRange useRange =
            storeImpl.getKeySerializer().restrictRange(null, subRange);

        final StoreIteratorParams params =
            new StoreIteratorParams(Direction.UNORDERED,
                                    batchSize,
                                    null,
                                    useRange,
                                    depth,
                                    consistency,
                                    timeout,
                                    timeoutUnit,
                                    excludeTombstones);

        return new BulkGetIterator<Key, KeyValueVersion>(
            storeImpl, parentKeyIterators, params, config,
            null /* iterationHandle */,
            null,
            null,
            false) {

            @Override
            public void validateKey(Key key) {
                return;
            }

            @Override
            public Key getKey(Key key) {
                return key;
            }

            @Override
            protected InternalOperation
                generateBulkGetOp(List<byte[]> parentKeys, byte[] resumeKey) {

                return new MultiGetBatchIterate(parentKeys,
                                                resumeKey,
                                                params.getSubRange(),
                                                params.getDepth(),
                                                params.getBatchSize(),
                                                params.getExcludeTombstones());
            }

            @Override
            protected void convertResult(Result result,
                                         List<KeyValueVersion> elementList) {
                final List<ResultKeyValueVersion> results =
                    result.getKeyValueVersionList();
                if (results.size() == 0) {
                    return;
                }
                for (ResultKeyValueVersion entry: results) {
                    final byte[] keyBytes = entry.getKeyBytes();
                    final Key key = keySerializer.fromByteArray(keyBytes);
                    final KeyValueVersion value =
                        KVStoreImpl.createKeyValueVersion(
                            key,
                            entry.getValue(),
                            entry.getVersion(),
                            entry.getExpirationTime(),
                            entry.getCreationTime(),
                            entry.getModificationTime());
                    elementList.add(value);
                }
            }
        };
    }

    /**
     * Creates a bulk get iterator returning Keys.
     */
    public static ParallelScanIterator<Key> createBulkMultiGetKeysIterator
        (final KVStoreImpl storeImpl,
         final List<Iterator<Key>> parentKeyiterators,
         final int batchSize,
         final KeyRange subRange,
         final Depth depth,
         final Consistency consistency,
         final long timeout,
         final TimeUnit timeoutUnit,
         final StoreIteratorConfig config,
         final boolean excludeTombstones) {

        /* Prohibit iteration of internal keyspace (//). */
        final KeyRange useRange =
            storeImpl.getKeySerializer().restrictRange(null, subRange);

        final StoreIteratorParams params =
            new StoreIteratorParams(Direction.UNORDERED,
                                    batchSize,
                                    null,
                                    useRange,
                                    depth,
                                    consistency,
                                    timeout,
                                    timeoutUnit,
                                    excludeTombstones);

        return new BulkGetIterator<Key, Key>(storeImpl, parentKeyiterators,
                                             params, config,
                                             null /* iterationHandle */,
                                             null,
                                             null,
                                             false) {
            @Override
            public void validateKey(Key key) {
                return;
            }

            @Override
            public Key getKey(Key key) {
                return key;
            }

            @Override
            protected InternalOperation
                generateBulkGetOp(List<byte[]> parentKeys, byte[] resumeKey) {

                return new MultiGetBatchKeysIterate(parentKeys,
                                                    resumeKey,
                                                    params.getSubRange(),
                                                    params.getDepth(),
                                                    params.getBatchSize(),
                                                    params.
                                                        getExcludeTombstones());
            }

            @Override
            protected void convertResult(Result result, List<Key> elementList) {
                final List<ResultKey> byteKeyResults = result.getKeyList();

                int cnt = byteKeyResults.size();
                if (cnt == 0) {
                    assert (!result.hasMoreElements());
                    return;
                }
                for (int i = 0; i < cnt; i += 1) {
                    final byte[] entry = byteKeyResults.get(i).getKeyBytes();
                    elementList.add(keySerializer.fromByteArray(entry));
                }
            }
        };
    }

    /**
     * This class implements the bulk get operation. It provides the common
     * underpinnings for batching both rows and KV pairs.
     *
     * In general, the bulk get operation works as follows:
     *
     * 1) Multiple reader tasks that read keys supplied by user supplied
     * iterator, and the keys are grouped by partition and sorted and wrapped
     * into batches.
     *
     * 2) Multiple bulk get tasks that take key batch and perform get batch
     * operation.
     *
     * 3) The get batch operation retrieved records associated with the keys,
     * and also may return its child or dependents depending on configuration
     * of Depth for KV API or child/ancestor table for Table API.
     *
     * The general flow of a key entry is as follows:
     *
     * 1) The key entries is supplied by user supplied iterator. Multiple reader
     * threads read the input keys in parallel.
     *
     * 2) Each reader accumulates the key entries, along with earlier entries,
     * in a sorted tree associated with each partition.
     *
     * 3) When the threshold associated with the partition is exceeded, the
     * leading elements in the sorted tree are assembled into a batch and placed
     * into a queue associated with the partition.
     *
     * 4) The ShardGetStream associated with the queue takes the batch and
     * retrieves the rows associated with the keys in the batch from store.
     *
     * 5) Multiple ShardGetStreams perform the bulk get operation in parallel,
     * the number of streams is configurable.
     *
     * 2 key parameters used in this operation:
     *
     * 1) Threshold for each partition batch = batchSize
     * 2) Number of shard get tasks =
     *      StoreIteratorConfig.maxCurrentRequests &gt; 0 ?
     *          StoreIteratorConfig.maxCurrentRequests :
     *          MIN(#available RNs, nProcessors).
     *
     * @param <K> must be a PrimaryKey or a Key
     * @param <V> must be a Row or a KeyValueVersion
     *
     */
    public static abstract class BulkGetIterator<K, V>
            extends BaseParallelScanIteratorImpl<V> {

        /**
         * The Key comparator used to group keys associated with a partition,
         * so that they can be sent as a contiguous batch.
         */
        private final static Comparator<byte[]> KEY_BYTES_COMPARATOR =
            new Key.BytesComparator();

        /**
         * Canonical PartitionBatch object to signify EOF in the partition
         * batch queue.
         */
        private final PartitionBatch partitionBatchEOF =
            new PartitionBatch(null, null);

        /**
         * The topology associated with the store.
         */
        private final Topology topology;

        /**
         * A map indexed by partition id which yields the keys that are being
         * aggregated for that partition.
         */
        private final PartitionValues pMap[];

        /**
         * Used to manage key reader threads.
         */
        private ExecutorService readerExecutor;
        private HashMap<Future<Integer>, KeysReader> readerTasks;
        private final AtomicInteger remainingReaders = new AtomicInteger();
        private final Set<ShardGetStream> getStreams =
            new HashSet<ShardGetStream>();

        private final Consistency consistency;
        private final TimeUnit timeoutUnit;
        private final long timeout;

        private final Map<RepGroupId, DetailedMetricsImpl> shardMetrics;

        /**
         * Used to hold the aggregate statistics associated with this operation
         */
        private final AggregateStatistics statistics;

        protected final KeySerializer keySerializer;

        public BulkGetIterator(KVStoreImpl store,
                               List<Iterator<K>> parentKeyIterators,
                               StoreIteratorParams params,
                               StoreIteratorConfig config,
                               IterationHandleNotifier iterHandleNotifier,
                               LogContext lc,
                               AuthContext auth,
                               boolean noCharge) {
            super(store, store.getLogger(), getRequestTimeoutMs(store, params),
                  params.getDirection(), 32 /* maxResultsBatches */,
                  true, /* prefetch */
                  iterHandleNotifier,
                  null, // QueryPublisher
                  lc,
                  auth,
                  noCharge);

            topology = storeImpl.getTopology();
            keySerializer = storeImpl.getKeySerializer();
            shardMetrics = new HashMap<RepGroupId, DetailedMetricsImpl>();
            statistics = new AggregateStatistics();

            /* Initialize parameters */
            consistency = params.getConsistency();
            timeout = params.getTimeout();
            timeoutUnit = params.getTimeoutUnit();

            /* Create partition values array */
            final int partitionThreshold = params.getBatchSize();
            final int nParts = topology.getPartitionMap().size();
            @SuppressWarnings({"unchecked", "rawtypes", "all"})
            PartitionValues[] partitionValues =
                new BulkGetIterator.PartitionValues[nParts+1];
            pMap = partitionValues;
            for (int i = 0 ; i <= nParts; i++) {
                pMap[i] = new PartitionValues(i, partitionThreshold);
            }

            /*
             * Create shard get tasks, the number of tasks is set to the value
             * of StoreIteratorConfig.getMaxConcurrentRequests() if it is set
             * and greater than 0, otherwise set to MIN(#available RNs,
             * nProcessors).  The shard get tasks need to be created before the
             * reader tasks start running so that the readers can insert stream
             * EOF markers.
             */
            final int maxConcurrentRequests =
                (config != null) ? config.getMaxConcurrentRequests() : 0;
            final int RNThreads = getNumOfShardTasks();
            final int maxShardTasks =
                    (maxConcurrentRequests > 0) ?
                                    Math.min(maxConcurrentRequests, RNThreads) :
                                    RNThreads;
            createShardExecutor(maxShardTasks);

            /* Start reader threads */
            startReaderExecutor(parentKeyIterators);

            /*
             * Start shard get tasks now that all reader threads are started so
             * that it is more likely that keys will be available for the get
             * operations without blocking.
             */
            for (final ShardGetStream task : getStreams) {
                task.submit();
            }

            /* Start a thread to monitor readers. */
            ExecutorService executor = Executors.newSingleThreadExecutor(
                new KVThreadFactory("BulkGetReadersMonitor", logger));
            executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            logReaderProgress(readerExecutor);
                            for (Future<Integer> f: readerTasks.keySet()) {
                                int nKeys = f.get();
                                statistics.aggregate(nKeys);
                            }
                        } catch (InterruptedException ie) {
                            logger.info(Thread.currentThread() +
                                " caught " + ie);
                        } catch (ExecutionException ee) {
                            logger.info(Thread.currentThread() +
                                " caught " + ee);
                        }
                    }
                });
            executor.shutdown();
        }

        /**
         * Abstract method to check the validation of key supplied by iterator.
         */
        protected abstract void validateKey(K key);

        /**
         * Abstract method to abstract how the key is obtained.
         */
        protected abstract Key getKey(K key);

        /**
         * Abstract method to create bulk get operation.
         */
        protected abstract InternalOperation generateBulkGetOp
            (List<byte[]> parentKeys, byte[] resumeKey);

        @Override
        protected boolean close(Throwable reason) {
            if (!super.close(reason)) {
                return false;
            }

            /* Cancel the reader tasks */
            for (Future<?> f : readerTasks.keySet()) {
                f.cancel(true);
            }

            List<Runnable> unfinishedBusiness = readerExecutor.shutdownNow();
            if (!unfinishedBusiness.isEmpty()) {
                final int nRemainingTasks = unfinishedBusiness.size();
                logger.log(Level.FINE,
                           "Bulk get reader executor didn't shutdown cleanly. "+
                           "{0} tasks remaining.", nRemainingTasks);
            }

            unfinishedBusiness = getTaskExecutor().shutdownNow();
            if (!unfinishedBusiness.isEmpty()) {
                final int nRemainingTasks = unfinishedBusiness.size();
                logger.log(Level.FINE,
                           "Bulk get shard executor didn't shutdown cleanly. "+
                           "{0} tasks remaining.", nRemainingTasks);
            }

            getStatInfo();
            logger.log(Level.INFO, statistics.toString());
            return true;
        }

        @Override
        public List<DetailedMetrics> getPartitionMetrics() {
            return Collections.emptyList();
        }

        @Override
        public List<DetailedMetrics> getShardMetrics() {
            synchronized (shardMetrics) {
                final ArrayList<DetailedMetrics> ret =
                    new ArrayList<DetailedMetrics>(shardMetrics.size());
                ret.addAll(shardMetrics.values());
                return ret;
            }
        }

        private static int getRequestTimeoutMs(KVStoreImpl storeImpl,
                                               StoreIteratorParams params) {
            final long timeOut = params.getTimeout();
            if (timeOut == 0) {
                return storeImpl.getDefaultRequestTimeoutMs();
            }
            final int requestTimeoutMs =
                PropUtil.durationToMillis(timeOut, params.getTimeoutUnit());
            if (!storeImpl.getDispatcher().isAsync() &&
                (requestTimeoutMs > storeImpl.getReadTimeoutMs())) {
                final String format = "Request timeout parameter: %,d ms " +
                    "exceeds socket read timeout: %,d ms";
                throw new IllegalArgumentException(
                    String.format(format, requestTimeoutMs,
                                  storeImpl.getReadTimeoutMs()));
            }
            return requestTimeoutMs;
        }

        /**
         * Create the tasks that will read batches of partition keys from
         * their respective shard. The number of tasks per shard is defined by
         * the configuration parameter: maxConcurrentRequest. There is at least
         * one task per shard to ensure that all shards are kept busy during
         * the load.
         *
         * The partitions are divided amongst each shard to ensure that no two
         * tasks ever access the same partition.
         */
        private void createShardExecutor(int maxShardTasks) {
            final String fmt = "createShardExecutor #ShardTasks:%d, " +
                "#Shards:%d, base parallelism per shard:%d, " +
                "residual parallelism per shard:%d";

            final int nShards = topology.getRepGroupMap().size();
            int basePerShardParallelism;
            int residualPerShardParallelism;
            if (maxShardTasks > nShards) {
                basePerShardParallelism = maxShardTasks / nShards;
                residualPerShardParallelism = maxShardTasks % nShards;
            } else {
                basePerShardParallelism = 1;
                residualPerShardParallelism = 0;
            }

            logger.info(String.format(fmt, maxShardTasks, nShards,
                                      basePerShardParallelism,
                                      residualPerShardParallelism));

            final Map<RepGroupId, List<PartitionId>> map =
                TopologyUtil.getRGIdPartMap(topology);

            setTaskExecutor(maxShardTasks);
            for (RepGroupId rgId : topology.getRepGroupIds()) {
                final List<PartitionId> list = map.get(rgId);
                final int nParts = list.size();
                int perShardParallelism = basePerShardParallelism;
                if (residualPerShardParallelism > 0) {
                    residualPerShardParallelism--;
                    perShardParallelism++;
                }

                /* Divide up the partitions amongst the tasks. */
                final int basePerTaskPartitions = nParts / perShardParallelism;
                int residualPerTaskPartitions = nParts % perShardParallelism;

                doneWithShard:
                for (int i = 0; i < nParts;  ) {
                    int perTaskPartitions = basePerTaskPartitions;
                    if (residualPerTaskPartitions > 0) {
                        perTaskPartitions++;
                        residualPerTaskPartitions--;
                    }

                    if (perTaskPartitions == 0) {
                        /* More parallelism than partitions in shard. */
                        break doneWithShard;
                    }

                    final List<PartitionId> taskPartitions =
                        list.subList(i, i + perTaskPartitions);

                    logger.info("Partitions:" +
                        Arrays.toString(taskPartitions.toArray()) +
                        " assigned to RG task");

                    final ShardGetStream task =
                        new ShardGetStream(rgId, taskPartitions.size());
                    for (PartitionId pid : taskPartitions) {
                        PartitionValues pv = pMap[pid.getPartitionId()];
                        pv.setShardTask(task);
                    }
                    streams.add(task);
                    getStreams.add(task);

                    i += perTaskPartitions;
                }
            }
        }

        /**
         * Returns the consistency used for this operation.
         */
        private Consistency getConsistency() {
            return (consistency != null) ?
                    consistency : storeImpl.getDefaultConsistency();
        }

        /**
         * Returns the total number of threads can be used for bulk get.
         */
        private int getNumOfShardTasks() {
            final int useNumRepNodes;
            if (getConsistency() == Consistency.ABSOLUTE) {
                useNumRepNodes = topology.getRepGroupMap().size();
            } else {
                final int[] readZoneIds =
                    storeImpl.getDispatcher().getReadZoneIds();
                useNumRepNodes =
                    TopologyUtil.getNumRepNodesForRead(topology, readZoneIds);
            }
            if (useNumRepNodes == 0) {
                throw new IllegalStateException("Store not yet initialized");
            }
            /* The 2x will keep all RNs busy, with a request in transit to/from
             * the RN and a request being processed.
             */
            return useNumRepNodes * 2;
        }

        /**
         * Start key reader tasks
         */
        private void startReaderExecutor(List<Iterator<K>> parentKeyIterators) {
            final ThreadFactory threadFactory =
                new KVThreadFactory("BulkGetReaders", logger);
            final int nReaders = parentKeyIterators.size();
            readerExecutor =
                Executors.newFixedThreadPool(nReaders, threadFactory);
            readerTasks = new HashMap<Future<Integer>, KeysReader>(nReaders);
            remainingReaders.set(nReaders);

            for (int i = 0; i < nReaders; i++) {
                final Iterator<K> keyIterator = parentKeyIterators.get(i);
                final KeysReader kr = new KeysReader(keyIterator);
                Future<Integer> future = null;
                try {
                    future = readerExecutor.submit(kr);
                } catch (RejectedExecutionException ree) {
                    close(ree);
                }
                readerTasks.put(future, kr);
            }
            readerExecutor.shutdown();
        }

        /**
         * Flush all residual values that were queued at their partitions to
         * their respective shards.
         */
        private void flushPartitions()
            throws InterruptedException {

            for (PartitionValues pv : pMap) {
               pv.flush(true);
            }
            logger.info("Flushed all partitions");
        }

        /**
         * Log progress at one minute intervals until all the readers have
         * reached EOF
         */
        private void logReaderProgress(final ExecutorService executor)
            throws InterruptedException {

            final long startMs = System.currentTimeMillis();
            long prevTotalRead = 0;

            while (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                final String fmt = "Reading continues. %,d values read. " +
                    "Throughput:%,d values/sec";
                final long totalRead = totalRead();
                final long throughput = (totalRead * 1000) /
                    (System.currentTimeMillis() - startMs);
                /*
                 * Log at warning level if there was no read progress and the
                 * operation appears to have stalled.
                 */
                logger.log((totalRead > prevTotalRead) ?
                    Level.INFO : Level.WARNING,
                    String.format(fmt, totalRead, throughput));
                prevTotalRead = totalRead;
            }
        }

        /**
         * Total entries read from all readers
         */
        private long totalRead() {
            long totalRead = 0;
            for (KeysReader kr: readerTasks.values()) {
                totalRead += kr.getReadCount();
            }
            return totalRead;
        }

        /**
         * Total aggregate statistics
         */
        private void getStatInfo() {
            for (ShardGetStream sgs : getStreams) {
                statistics.batchCount += sgs.getBatchCount();
                statistics.batchQueueUnderflow += sgs.getBatchQueueUnderflow();
                statistics.batchQueueOverflow += sgs.getBatchQueueOverflow();
                if (statistics.maxBatchRequestRepeated <
                        sgs.getMaxBatchRequestRepeated()) {
                    statistics.maxBatchRequestRepeated =
                        sgs.getMaxBatchRequestRepeated();
                }
            }
        }

        /**
         * The function can be ignored because bulk get doesn't support sorting
         */
        @Override
        protected int compare(V one, V two) {
            return 0;
        }

        /**
         * Dedicated task used to read a specific Key stream
         */
        private class KeysReader implements Callable<Integer> {

            /**
             * The KeyStream being read
             */
            private final Iterator<K> keyIterator;

            /**
             * The number of keys read by this stream reader.
             */
            private volatile int readCount = 0;

            KeysReader(Iterator<K> keyIterator) {
                super();
                this.keyIterator = keyIterator;
            }

            @Override
            public Integer call()
                throws Exception {

                final KeySerializer serializer = storeImpl.getKeySerializer();
                logger.info("Started keys reader");
                try {
                    K k;
                    while (keyIterator.hasNext()) {
                        k = keyIterator.next();
                        if (k == null) {
                            throw new IllegalArgumentException("The parent key" +
                                " should not be null");
                        }
                        /* Call validateKey() to check the key */
                        validateKey(k);
                        readCount++;

                        final Key key = getKey(k);
                        final byte[] bytes = serializer.toByteArray(key);
                        final PartitionId pid = topology.getPartitionId(bytes);
                        pMap[pid.getPartitionId()].put(bytes);
                    }

                    /*
                     * Flush partitions and add EOF markers only after all keys
                     * have been generated.  Otherwise, the iterator might
                     * think it was done if it encountered EOFs and some
                     * streams hadn't been added yet.
                     */
                    if (remainingReaders.decrementAndGet() <= 0) {
                        flushPartitions();
                        for (ShardGetStream sgs: getStreams) {
                            sgs.setEOFPartitionBatch();
                        }
                    }
                } catch (RuntimeException e) {
                    close(e);
                } catch (Error e) {
                    close(e);
                } finally {
                    logger.info("Finished keys reader");
                }
                return readCount;
            }

            public int getReadCount() {
                return readCount;
            }
        }

        /**
         * Used to hold a sorted list of key byte array. The sorted list
         * ensures locality of reference during reading on the server.
         */
        private class PartitionBatch {
            final PartitionId pid;
            final List<byte[]> entries;

            PartitionBatch(PartitionId pid, List<byte[]> entries) {
                super();
                this.pid = pid;
                this.entries = entries;
            }
        }

        /**
         * Reading records of a single partition.
         */
        public class ShardGetStream extends Stream {
            /**
             * The shard associated with this task
             */
            private final RepGroupId rgId;

            /**
             *  The queue of batches to be processed by this task.
             */
            private final ArrayBlockingQueue<PartitionBatch> queuedBatchs;

            /**
             * The number of batches processed by this task.
             */
            private long batchCount = 0 ;

            /**
             * The number of times this task was blocked because it did not have
             * a partition batch to execute. Large numbers of queue underflows
             * indicate that the user input streams are not providing data fast
             * enough and increasing stream parallelism could help.
             */
            private long batchQueueUnderflow = 0 ;

            /**
             * The number of times a batch could not be inserted because there
             * was no space in the queue. Large numbers of queue overflows
             * indicate that performance could benefit from increased shard
             * parallelism.
             */
            private long batchQueueOverflows = 0 ;

            /**
             * The maximum times a batch of parent keys was processed repeatedly
             * if the batch size is smaller than the total count of result set.
             */
            private int maxBatchRequestRepeated = 0;
            private int batchRequestRepeated = 0;

            private PartitionBatch currentBatch = null;
            private int resumeParentKeyIndex = -1;
            private byte[] resumeKey = null;

            ShardGetStream(RepGroupId rgId, int numTaskPartitions) {
                this.rgId = rgId;
                queuedBatchs = new ArrayBlockingQueue<PartitionBatch>(
                    numTaskPartitions * 2);
            }

            void add(PartitionBatch partBatch)
                throws InterruptedException {

                if (!queuedBatchs.offer(partBatch)) {
                    batchQueueOverflows++;
                    queuedBatchs.put(partBatch);
                }
            }

            @Override
            protected void updateDetailedMetrics(long timeInMs,
                                                 long recordCount) {

                final String shardName = rgId.toString();
                DetailedMetricsImpl dmi;

                /* Shard Metrics. */
                synchronized (shardMetrics) {
                    dmi = shardMetrics.get(rgId);
                    if (dmi == null) {
                        dmi = new DetailedMetricsImpl
                            (shardName, timeInMs, recordCount);
                        shardMetrics.put(rgId, dmi);
                        return;
                    }
                }
                dmi.inc(timeInMs, recordCount);
            }

            @Override
            protected void setResumeKey(Result result) {
                if (result.hasMoreElements()) {
                    if (resumeParentKeyIndex == -1) {
                        resumeParentKeyIndex = result.getResumeParentKeyIndex();
                    } else {
                        resumeParentKeyIndex +=result.getResumeParentKeyIndex();
                    }
                    resumeKey = result.getPrimaryResumeKey();
                } else {
                    resetResumeKey();
                }
            }

            @Override
            protected Request makeReadRequest() {
                final List<byte[]> keys;
                if (resumeParentKeyIndex == -1) {
                    if (currentBatch == null) {
                        currentBatch = getPartitionBatch();
                        if (currentBatch == null) {
                            return null;
                        }
                    }
                    keys = currentBatch.entries;

                    batchCount++;
                    logMaxBatchRequestRepeated();
                } else {
                    final List<byte[]> batchKeys = currentBatch.entries;
                    keys = batchKeys.subList(resumeParentKeyIndex,
                                             batchKeys.size());
                    batchRequestRepeated++;
                }

                final InternalOperation op = generateBulkGetOp(keys, resumeKey);
                return storeImpl.makeReadRequest(op, currentBatch.pid,
                                                 consistency, timeout,
                                                 timeoutUnit);
            }

            @Override
            protected boolean hasMoreElements(Result result) {
                if (result.hasMoreElements()) {
                    return true;
                }
                currentBatch = getPartitionBatch();
                resetResumeKey();
                return (currentBatch != null);
            }

            @Override
            public String toString() {
                return "ShardGetStream[" + rgId + "]";
            }

            private PartitionBatch getPartitionBatch() {
                try {
                    PartitionBatch pbatch = queuedBatchs.poll();
                    if (pbatch == null) {
                        batchQueueUnderflow++;

                        /*
                         * TODO: This method will block if the KeysReader
                         * threads have not be able to read enough keys from
                         * the iterators.  That blocking could end up blocking
                         * an async thread, which isn't good.  We should
                         * fix this. [#27557]
                         */
                        pbatch = queuedBatchs.take();
                    }
                    if (pbatch == partitionBatchEOF) {
                        return null;
                    }
                    return pbatch;
                } catch (InterruptedException ie) {
                    logger.info(Thread.currentThread() + " caught " + ie);
                    Thread.currentThread().interrupt();
                }
                return null;
            }

            private void resetResumeKey() {
                resumeParentKeyIndex = -1;
                resumeKey = null;
            }

            long getBatchCount() {
                return batchCount;
            }

            long getBatchQueueUnderflow() {
                return batchQueueUnderflow;
            }

            long getBatchQueueOverflow() {
                return batchQueueOverflows;
            }

            int getMaxBatchRequestRepeated() {
                logMaxBatchRequestRepeated();
                return maxBatchRequestRepeated;
            }

            void setEOFPartitionBatch()
                throws InterruptedException {

                add(partitionBatchEOF);
            }

            private void logMaxBatchRequestRepeated() {
                if (batchRequestRepeated == 0) {
                    return;
                }
                if (batchRequestRepeated > maxBatchRequestRepeated) {
                    maxBatchRequestRepeated = batchRequestRepeated;
                }
                batchRequestRepeated = 0;
            }
        }

        /**
         * The values associated with a specific partition.
         */
        private class PartitionValues {

            /**
             * The partition associated with the values.
             */
            private final int partitionId;

            /**
             * The task designated to write this partition's values to its
             * shard.
             */
            private ShardGetStream getStream;

            /**
             * The number of entries that were actually inserted into the
             * partition.
             */
            private long getCount = 0;

            /**
             * The number of duplicated entries that were already existed in
             * the partition.
             */
            private long dupCount = 0;

            /**
             * Holds the sorted values that are waiting to be written to the
             * shard. Tried ConcurrentSkipListMap to eliminate use of
             * synchronized methods but it resulted in lower perf on
             * Nashua machines.
             */
            private final Set<byte[]> keys =
                new TreeSet<byte[]>(KEY_BYTES_COMPARATOR);

            private final int threshold;

            PartitionValues(int pid, int partitionThreshold) {
                this.partitionId = pid;
                this.threshold = partitionThreshold;
            }

            void setShardTask(ShardGetStream stream) {
                getStream = stream;
            }

            synchronized void put(byte[] key)
                throws InterruptedException {

                if (keys.contains(key)) {
                    dupCount++;
                    return;
                }
                keys.add(key);
                flush(false);
            }

            /**
             * Flushes keys set to batch queue of shard task if needed.
             *
             * A flush is typically done if the number of the keys exceeds the
             * threshold number.
             *
             * @param force if true the partition is flushed even if the
             * threshold has not been reached
             *
             */
            void flush(boolean force)
                throws InterruptedException {

                final int maxRequestSize = 1024 * 1024;

                final String fmt =
                    "Queued Partition %d flushed. Batch size %,d; Total:%,d;" +
                        " Number of keys:%,d; request size:%,d" ;
                int numKeys = keys.size();
                while ((force && numKeys > 0) || (numKeys >= threshold)) {
                    int getBatchCount = 0;
                    int requestSize = 0;
                    final List<byte[]> le = new ArrayList<byte[]>();
                    synchronized (this) {
                        for (Iterator<byte[]> iter = keys.iterator();
                            iter.hasNext();) {

                            final byte[] kvBytes = iter.next();
                            iter.remove();
                            getBatchCount++;
                            numKeys--;
                            requestSize += kvBytes.length;
                            le.add(kvBytes);
                            if (requestSize > maxRequestSize) {
                                break;
                            }
                        }
                        getCount += getBatchCount;
                    }
                    /* Can block, do it outside sync block */
                    final PartitionBatch batch =
                        new PartitionBatch(new PartitionId(partitionId),le);
                    getStream.add(batch);
                    logger.fine(String.format(fmt, partitionId,
                                              getBatchCount, getCount,
                                              keys.size(), requestSize));
                }
            }
        }

        /**
         * Represents the aggregate statistics across all keyStreams.
         */
        private class AggregateStatistics {

            private long batchCount;
            private long batchQueueUnderflow;
            private long batchQueueOverflow;
            private int maxBatchRequestRepeated;

            /**
             * The total number of entries read from all the streams supplied
             * to the operation, it may be less than total number keys
             * supplied by iterator if contains duplicated keys
             */
            private long readCount ;

            public void aggregate(int entriesRead) {
                readCount += entriesRead;
            }

            /**
             * The total number of V actually retrieved from the store
             * as a result of the operation.
             */
            public long totalGetCount() {
                long total = 0;
                for (Entry<RepGroupId, DetailedMetricsImpl> entry:
                     shardMetrics.entrySet()) {
                    total += entry.getValue().getScanRecordCount();
                }
                return total;
            }

            private long getTotalDupCount() {
                long total = 0;
                for (PartitionValues pv: pMap)  {
                    total += pv.dupCount;
                }
                return total;
            }

            @Override
            public String toString() {
                final String fmt =
                    "%,d key streams; %,d shard streams; " +
                    "%,d keys read; %,d duplicated; " +
                    "%,d get; %,d batches; " +
                    "%,d batch queue underflows; " +
                    "%,d batch queue overflows; "  +
                    "%,d av batch size; " +
                    "%,d max batch request repeated;";
                final long getCount = totalGetCount();
                return String.format(fmt, readerTasks.size(), getStreams.size(),
                                     readCount, getTotalDupCount(),
                                     getCount, batchCount,
                                     batchQueueUnderflow,
                                     batchQueueOverflow,
                                     ((batchCount > 0) ?
                                         (getCount / batchCount) : 0),
                                      maxBatchRequestRepeated);
            }
        }
    }
}
