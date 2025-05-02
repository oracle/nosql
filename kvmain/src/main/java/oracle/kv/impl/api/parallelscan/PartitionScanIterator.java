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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import oracle.kv.Consistency;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.StoreIteratorParams;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.query.QueryPublisher;
import oracle.kv.impl.async.IterationHandleNotifier;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.TopologyUtil;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.stats.DetailedMetrics;

/**
 * Base class for parallel scan iterators.
 *
 * Both of the parallel scan methods (storeIterator(...,
 * StoreIteratorConfig) and storeKeysIterator(..., StoreIteratorConfig)
 * return an instance of a ParallelScanIterator (as opposed to plain old
 * Iterator) so that we can eventually use them in try-with-resources
 * constructs.
 */
public abstract class PartitionScanIterator<K>
        extends BaseParallelScanIteratorImpl<K> {

    /* Indexed by partition id. */
    private final Map<Integer, DetailedMetricsImpl> partitionMetrics;

    private final Map<RepGroupId, DetailedMetricsImpl> shardMetrics;

    protected final StoreIteratorParams storeIteratorParams;

    public PartitionScanIterator(KVStoreImpl store,
                                 ExecuteOptions options,
                                 StoreIteratorParams storeIteratorParams) {
        this(store, options, storeIteratorParams,
             (QueryPublisher)null);
    }

    public PartitionScanIterator(KVStoreImpl store,
                                 ExecuteOptions options,
                                 StoreIteratorParams storeIteratorParams,
                                 IterationHandleNotifier iterHandleNotifier) {
        super(store,
              store.getLogger(),
              store.getTimeoutMs(options.getTimeout(),
                                 options.getTimeoutUnit()),
              storeIteratorParams.getDirection(),
              0 /* default maxResultsBatches */,
              options.getDoPrefetching(),
              iterHandleNotifier,
              null, // QueryPublisher
              options.getLogContext(),
              options.getAuthContext(),
              options.getNoCharge());

        this.storeIteratorParams = storeIteratorParams;

        this.partitionMetrics = new HashMap<Integer, DetailedMetricsImpl>(
            storeImpl.getNPartitions());
        this.shardMetrics = new HashMap<RepGroupId, DetailedMetricsImpl>();

        createAndSubmitStreams(options);
    }

    public PartitionScanIterator(KVStoreImpl store,
                                 ExecuteOptions options,
                                 StoreIteratorParams storeIteratorParams,
                                 QueryPublisher publisher) {
        super(store,
              store.getLogger(),
              store.getTimeoutMs(options.getTimeout(),
                                 options.getTimeoutUnit()),
              storeIteratorParams.getDirection(),
              0 /* default maxResultsBatches */,
              options.getDoPrefetching(),
              null, // IterationHandleNotifier
              publisher,
              options.getLogContext(),
              options.getAuthContext(),
              options.getNoCharge());

        this.storeIteratorParams = storeIteratorParams;

        this.partitionMetrics = new HashMap<Integer, DetailedMetricsImpl>(
            storeImpl.getNPartitions());
        this.shardMetrics = new HashMap<RepGroupId, DetailedMetricsImpl>();

        createAndSubmitStreams(options);
    }

    /**
     * Returns the consistency used for this operation.
     */
    private Consistency getConsistency() {
        return (storeIteratorParams.getConsistency() != null) ?
            storeIteratorParams.getConsistency() :
            storeImpl.getDefaultConsistency();
    }

    /*
     * For each partition, create a stream and start reading.
     */
    private void createAndSubmitStreams(final ExecuteOptions options) {

        final Map<RepGroupId, Set<Integer>> partitionsByShard =
            getPartitionTopology(storeIteratorParams.getPartitions());

        int nShards = partitionsByShard.size();
        if (nShards < 1) {
            throw new IllegalStateException("partitionsByShard has no entries");
        }

        /*
         * Calculate n threads based on topology. The 2x will keep all
         * RNs busy, with a request in transit to/from the RN and a request
         * being processed
         */
        final int RNThreads = 2 *
            ((getConsistency() == Consistency.ABSOLUTE) ?
             nShards :
             TopologyUtil.getNumRepNodesForRead(
                 storeImpl.getTopology(),
                 storeImpl.getDispatcher().getReadZoneIds()));

        final int nThreads = options.getMaxConcurrentRequests();
        final int useNThreads =
            (nThreads == 0) ? RNThreads : Math.min(nThreads, RNThreads);

        setTaskExecutor(useNThreads);

        /*
         * Submit the partition streams in round robin order by shard to
         * achieve poor man's balancing across shards.
         */
        final Map<RepGroupId, List<PartitionStream>> streamsByShard =
            generatePartitionStreams(partitionsByShard,
                                     options.getContinuationKey());

        final Collection<List<PartitionStream>> streamsByShardColl =
            streamsByShard.values();

        @SuppressWarnings({"unchecked", "rawtypes", "all"})
        final List<PartitionStream>[] streamsByShardArr =
            streamsByShardColl.toArray(new List[0]);

        for (int idx = 0; idx < nShards; idx++) {
            List<PartitionStream> tasks = streamsByShardArr[idx];
            for (PartitionStream stream : tasks) {
                streams.add(stream);
            }
        }

        boolean didSomething;
        do {
            didSomething = false;

            for (int idx = 0; idx < nShards; idx++) {
                List<PartitionStream> tasks = streamsByShardArr[idx];
                if(tasks.size() > 0) {
                    PartitionStream task = tasks.get(0);
                    task.submit();
                    tasks.remove(0);
                    didSomething = true;
                }
            }
        } while (didSomething);
    }

    private Map<RepGroupId, List<PartitionStream>> generatePartitionStreams(
        final Map<RepGroupId,
        Set<Integer>> partitionsByShard,
        byte[] resumeKey) {

        logger.fine("Generating Partition Streams");
        final Map<RepGroupId, List<PartitionStream>> ret =
            new HashMap<RepGroupId, List<PartitionStream>>(
                partitionsByShard.size());

        for (Map.Entry<RepGroupId, Set<Integer>> ent :
                 partitionsByShard.entrySet()) {

            final RepGroupId rgid = ent.getKey();
            final Set<Integer> parts = ent.getValue();

            for (Integer part : parts) {
                final PartitionStream pis = createStream(rgid, part, resumeKey);

                List<PartitionStream> partitionStreams = ret.get(rgid);

                if (partitionStreams == null) {
                    partitionStreams = new ArrayList<PartitionStream>();
                    ret.put(rgid, partitionStreams);
                }

                partitionStreams.add(pis);
            }
        }

        return ret;
    }

    /*
     * Extracts the rep factor of the topology and creates a map of shard to
     * the set of partitions in the shard.
     */
    private Map<RepGroupId, Set<Integer>> getPartitionTopology(
        final Set<Integer> partitions) {

        final Topology topology =
            storeImpl.getDispatcher().getTopologyManager().getTopology();

        /* Determine Rep Factor. */
        Collection<Datacenter> datacenters =
            topology.getDatacenterMap().getAll();

        if (datacenters.size() < 1) {
            throw new IllegalStateException("No zones in topology?");
        }

        final Map<RepGroupId, Set<Integer>> shardPartitions =
            new HashMap<RepGroupId, Set<Integer>>();

        /*
         * If the set of partitions was specified, create a map using them
         * and return.
         */
        if (partitions != null) {
            for (Integer i : partitions) {
                PartitionId partId = new PartitionId(i);
                RepGroupId rgid = topology.getRepGroupId(partId);
                if (rgid == null) {
                    throw new IllegalStateException("Partition " + partId +
                                                    " not in topology?");
                }
                Set<Integer> parts = shardPartitions.get(rgid);
                if (parts == null) {
                    parts = new HashSet<Integer>();
                    shardPartitions.put(rgid, parts);
                }
                parts.add(i);
            }
            return shardPartitions;
        }

        for (int i = 1; i <= storeImpl.getNPartitions(); i++) {
            PartitionId partId = new PartitionId(i);
            RepGroupId rgid = topology.getRepGroupId(partId);
            if (rgid == null) {
                throw new IllegalStateException("Partition " + partId +
                                                " not in topology?");
            }
            Set<Integer> parts = shardPartitions.get(rgid);
            if (parts == null) {
                parts = new HashSet<Integer>();
                shardPartitions.put(rgid, parts);
            }
            parts.add(i);
        }

        return shardPartitions;
    }

    /*
     * May be overriden by sublasses.
     */
    protected PartitionStream createStream(
        RepGroupId groupId,
        int partitionId,
        byte[] resumeKey) {
        return new PartitionStream(groupId, partitionId, resumeKey);
    }

    /* -- Metrics from ParallelScanIterator -- */

    @Override
    public List<DetailedMetrics> getPartitionMetrics() {
        synchronized (partitionMetrics) {
            List<DetailedMetrics> l =
                new ArrayList<DetailedMetrics>(partitionMetrics.size());
            l.addAll(partitionMetrics.values());
            return Collections.unmodifiableList(l);
        }
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

    /**
     * Returns the generated op for this iterator.
     */
    protected abstract InternalOperation generateGetterOp(byte[] resumeKey);

    /**
     * Close the iterator, recording the specified remote exception. If
     * the reason is not null, the exception is thrown from the hasNext()
     * or next() methods.
     *
     * @param reason the exception causing the close or null
     * @return whether the iterator was closed by this call; returns false if
     * it was already closed
     */
    @Override
    protected boolean close(Throwable reason) {

        if (!super.close(reason)) {
            return false;
        }

        final List<Runnable> unfinishedBusiness =
            getTaskExecutor().shutdownNow();

        if (!unfinishedBusiness.isEmpty()) {
            logger.log(Level.FINE,
                       "ParallelScan executor didn''t shutdown cleanly. " +
                       "{0} tasks remaining.",
                       unfinishedBusiness.size());
        }
        return true;
    }


    /**
     * Reading records of a single partition.
     */
    protected class PartitionStream extends Stream {

        protected final RepGroupId groupId;

        protected final int partitionId;

        protected byte[] resumeKey = null;

        protected PartitionStream(RepGroupId rgi,
                                  int part,
                                  byte[] resumeKey) {
            this.groupId = rgi;
            this.partitionId = part;
            this.resumeKey = resumeKey;
        }

        @Override
        protected void updateDetailedMetrics(long timeInMs, long recordCount) {
            /* Partition Metrics. */
            final int partIdx = partitionId - 1;
            final String shardName = groupId.toString();
            DetailedMetricsImpl dmi;
            synchronized (partitionMetrics) {
                dmi = partitionMetrics.get(partIdx);
                if (dmi != null) {
                    dmi.inc(timeInMs, recordCount);
                } else {
                    final StringBuilder sb = new StringBuilder();
                    sb.append(partitionId).append(" (").append(shardName).
                        append(")");
                    dmi = new DetailedMetricsImpl(sb.toString(), timeInMs,
                                                  recordCount);
                    partitionMetrics.put(partIdx, dmi);
                }
            }

            synchronized (shardMetrics) {
                /* Shard Metrics. */
                dmi = shardMetrics.get(groupId);
                if (dmi == null) {
                    dmi = new DetailedMetricsImpl(
                        shardName, timeInMs, recordCount);
                    shardMetrics.put(groupId, dmi);
                    return;
                }
            }

            dmi.inc(timeInMs, recordCount);
        }

        @Override
        protected Request makeReadRequest() {
            return storeImpl.makeReadRequest(
                generateGetterOp(resumeKey),
                new PartitionId(partitionId),
                getConsistency(),
                requestTimeoutMs);
        }

        @Override
        protected void setResumeKey(Result result) {
            resumeKey = result.getPrimaryResumeKey();
        }

        @Override
        public String toString() {
            return "PartitionStream[" + groupId + ":"+ partitionId + ", "
                + getStatus() + "]";
        }
    }
}
