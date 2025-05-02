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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.concurrent.atomic.AtomicInteger;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.TopologyManager;
import oracle.kv.impl.api.TopologyManager.PostUpdateListener;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.query.QueryPublisher;
import oracle.kv.impl.async.IterationHandleNotifier;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.stats.DetailedMetrics;


/**
 * Base class for building shard iterators.
 *
 * @param <K> the type of elements returned by the iterator
 */
public abstract class ShardScanIterator<K>
    extends BaseParallelScanIteratorImpl<K>
    implements PostUpdateListener {

    protected final Consistency consistency;

    /*
     * The number of shards when the iterator was created. If this changes
     * we must abort the operation as data may have been missed between
     * the point that the new shard came online and when we noticed it.
     */
    private final int nGroups;

    /*
     * The hash code of the partition map when the iterator was created.
     * If the location of any partition changes we must abort the operation,
     * otherwise data may be lost or duplicate values can be returned.
     * The hash code is used as a poor man's check to see if the partitions
     * have changed location. We could copy the map and check each
     * partition's location but that could be costly when there are 1000s
     * of partitions. Note the only reason that the map should change is
     * due to a change in the group.
     */
    private final int partitionMapHashCode;

    protected final int batchSize;

    protected final AtomicInteger theVSIDCounter = new AtomicInteger(1);

    /* Per shard metrics provided through ParallelScanIterator */
    private final Map<RepGroupId, DetailedMetricsImpl> shardMetrics =
        new HashMap<RepGroupId, DetailedMetricsImpl>();

    public ShardScanIterator(
        KVStoreImpl store,
        Topology topology,
        ExecuteOptions options,
        Direction dir,
        Set<RepGroupId> shardSet,
        IterationHandleNotifier iterHandleNotifier,
        boolean isTopologyListner) {

        this(store, topology, options, dir, shardSet,
             null, iterHandleNotifier, isTopologyListner);
    }

    public ShardScanIterator(
        KVStoreImpl store,
        Topology topology,
        ExecuteOptions options,
        Direction dir,
        Set<RepGroupId> shardSet,
        QueryPublisher publisher) {

        this(store, topology, options, dir, shardSet,
             publisher, null, false);
    }

    public ShardScanIterator(
        KVStoreImpl store,
        Topology topology,
        ExecuteOptions options,
        Direction dir,
        Set<RepGroupId> shardSet,
        QueryPublisher publisher,
        IterationHandleNotifier iterHandleNotifier,
        boolean isTopologyListner) {

        super(store,
              store.getLogger(),
              store.getTimeoutMs(options.getTimeout(),
                                 options.getTimeoutUnit()),
              /*
               * BaseParallelScanIterator needs itrDirection to be set in order
               * to sort properly. If not set, index scans default to FORWARD.
               */
              dir,
              0 /* default maxResultsBatches */,
              options.getDoPrefetching(),
              iterHandleNotifier,
              publisher,
              options.getLogContext(),
              options.getAuthContext(),
              options.getNoCharge());

        consistency = options.getConsistency();

        batchSize = options.getResultsBatchSize();

        TopologyManager topoManager = store.getDispatcher().getTopologyManager();

        /* Collect group information from the current topology. */
        if (topology == null) {
            topology = topoManager.getTopology();
        }
        Set<RepGroupId> groups;
        if (shardSet == null) {
            groups = topology.getRepGroupIds();
        } else {
            groups = shardSet;
        }
        nGroups = groups.size();
        if (nGroups == 0) {
            throw new IllegalStateException("Store not yet initialized");
        }
        partitionMapHashCode = topology.getPartitionMap().hashCode();

        /*
         * The 2x will keep all RNs busy, with a request in transit to/from
         * the RN and a request being processed
         */
        setTaskExecutor(nGroups * 2);

        /* For each shard, create a stream and start reading.
         *
         * All the created streams are added to the this.streams TreeSet as well
         * as to a local ArrayList. Then, the streams are submitted for
         * execution.
         *
         * Submission has to be done after all streams are added to the TreeSet
         * because otherwise a response from a submitted stream may arrive and
         * be processed before all the streams have been added to the TreeSet,
         * which can create problems (for example, the iteration may terminate
         * wrongly due to the TreeSet becoming empty while there are still
         * unsubmitted streams to be inserted to the TreeSet).
         * 
         * The streams are submitted by iterating over the local ArrayList.
         * This is because iterating over the TreeSet directly can lead to
         * ConcurrentModificationException in the following scenario:
         * While iterating over the TreeSet, a response arrives to a
         * submitted stream and proceccesing this response results in new
         * streams being created and added to the TreeSet (which can happen
         * if the query is executing concurrently with an elasticity operation).
         * Notice that doing the iteration inside a synchronized block
         * (synchronizing over the TreeSet) does not help because the thread
         * performing the iteration here may be the same thread that processes
         * the response and adding the new streams. */
        List<Stream> newStreams = new ArrayList<Stream>(groups.size());

        for (RepGroupId groupId : groups) {
            final ShardStream stream = createStream(groupId);
            streams.add(stream);
            newStreams.add(stream);
        }

        for (Stream stream : newStreams) {
            stream.submit();
        }

        /*
         * Register a listener to detect changes in the groups (shards).
         * We register the lister weakly so that the listener will be
         * GCed in the event that the application does not close the
         * iterator.
         */
        if (isTopologyListner) {
            topoManager.addPostUpdateListener(this, true);
        }
    }

    /*
     * Sbclasses override this if they need to use a subclass of
     * ShardIndexStream in their implementation.
     */
    protected ShardStream createStream(RepGroupId groupId) {
        return new ShardStream(groupId, null, null);
    }

    /* -- Metrics from ParallelScanIterator -- */

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

    /**
     * Create an operation using the specified resume key. The resume key
     * parameters may be null.
     *
     * @param resumeSecondaryKey a resume key or null
     * @param resumePrimaryKey a resume key or null
     * @return an operation
     */
    protected abstract InternalOperation createOp(
        byte[] resumeSecondaryKey,
        byte[] resumePrimaryKey);

    /**
     * Returns a resume secondary key based on the specified element.
     *
     * @param result result object
     * @return a resume secondary key
     */
    protected byte[] extractResumeSecondaryKey(Result result) {
        return result.getSecondaryResumeKey();
    }

    @Override
    protected boolean close(Throwable reason) {
        return close(reason, true);
    }

    /**
     * Close the iterator, recording the specified remote exception. If
     * the reason is not null, the exception is thrown from the hasNext()
     * or next() methods.
     *
     * @param reason the exception causing the close or null
     * @param remove if true remove the topo listener
     * @return whether the iterator was closed by this call; returns false if
     * the iterator was already closed
     */
    private boolean close(Throwable reason, boolean remove) {
        if (!super.close(reason)) {
            return false;
        }

        if (remove) {
            storeImpl.getDispatcher().getTopologyManager().
                removePostUpdateListener(this);
        }

        final List<Runnable> unfinishedBusiness =
            getTaskExecutor().shutdownNow();

        if (!unfinishedBusiness.isEmpty()) {
            logger.log(Level.FINE,
                       "IndexScan executor didn''t shutdown cleanly. " +
                       "{0} tasks remaining.",
                       unfinishedBusiness.size());
        }
        return true;
    }

    /* -- From PostUpdateListener -- */

    /*
     * Checks to see if something in the new topology has changed which
     * would invalidate the iteration. In this case if a partition moves
     * we can no longer trust the results. We check for partitions moving
     * by a change in the number of shards or a change in the partition
     * map. If a change is detected the iterator is closed with a
     * UnsupportedOperationException describing the issue.
     */
    @Override
    public boolean postUpdate(Topology topology) {

        if (closed) {
            return true;
        }

        final int newGroupSize = topology.getRepGroupIds().size();

        /*
         * If the number of groups have changed this iterator needs to be
         * closed. The RE will be reported back to the application from
         * hasNext() or next().
         */
        if (nGroups > newGroupSize) {
            close(new UnsupportedOperationException("The number of shards "+
                                         "has decreased during the iteration"),
                  false);
        }

        /*
         * The number of groups has increased.
         */
        if (nGroups < newGroupSize) {
            close(new UnsupportedOperationException("The number of shards "+
                                         "has increased during the iteration"),
                  false);
        }

        /*
         * Check to see if the partition locations have changed (see
         * comment for partitionMapHashCode).
         */
        if (partitionMapHashCode != topology.getPartitionMap().hashCode()) {
            close(new UnsupportedOperationException("The location of " +
                                         "one or more partitions has changed " +
                                         "during the iteration"),
                  false);
        }
        return closed;
    }

    /**
     * Reading index records of a single shard.
     */
    protected class ShardStream extends Stream {

        protected final RepGroupId theGroupId;

        protected byte[] resumeSecondaryKey;

        protected byte[] resumePrimaryKey;

        protected ShardStream(RepGroupId groupId,
                              byte[] resumeSecondaryKey,
                              byte[] resumePrimaryKey) {
            theGroupId = groupId;
            this.resumeSecondaryKey = resumeSecondaryKey;
            this.resumePrimaryKey = resumePrimaryKey;
        }

        protected RepGroupId getGroupId() {
            return theGroupId;
        }

        @Override
        protected void updateDetailedMetrics(long timeInMs, long recordCount) {
            DetailedMetricsImpl dmi;
            synchronized (shardMetrics) {

                dmi = shardMetrics.get(theGroupId);
                if (dmi == null) {
                    dmi = new DetailedMetricsImpl(theGroupId.toString(),
                                                  timeInMs, recordCount);
                    shardMetrics.put(theGroupId, dmi);
                    return;
                }
            }
            dmi.inc(timeInMs, recordCount);
        }

        @Override
        protected Request makeReadRequest() {
            return storeImpl.makeReadRequest(
                    createOp(resumeSecondaryKey, resumePrimaryKey),
                    theGroupId,
                    consistency,
                    requestTimeoutMs,
                    MILLISECONDS);
        }

        @Override
        protected void setResumeKey(Result result) {
            resumeSecondaryKey = extractResumeSecondaryKey(result);
            resumePrimaryKey = result.getPrimaryResumeKey();
        }

        @Override
        public String toString() {
            return "ShardStream[" + theGroupId + ", " + getStatus() + "]";
        }
    }
}
