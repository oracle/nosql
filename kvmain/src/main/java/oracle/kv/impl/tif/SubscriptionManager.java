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

package oracle.kv.impl.tif;

import com.sleepycat.je.rep.GroupShutdownException;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.subscription.Subscription;
import com.sleepycat.je.rep.subscription.SubscriptionConfig;
import com.sleepycat.je.rep.subscription.SubscriptionStatus;
import com.sleepycat.je.utilint.InternalException;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.SharedThreadPool;
import oracle.kv.impl.rep.subscription.partreader.PartitionReader;
import oracle.kv.impl.rep.subscription.partreader.PartitionReaderCallBack;
import oracle.kv.impl.rep.subscription.partreader.PartitionReaderStatus.PartitionRepState;
import oracle.kv.impl.topo.PartitionId;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Object managing subscriptions for TextIndexFeeder (TIF). It creates
 * initial-state subscription over the partition migration stream and ongoing
 * state subscription using the subscription service in JE.
 *
 * The manager also maintains state of subscription, handles filtering and
 * state transition between two phases of subscriptions.
 */
class SubscriptionManager {

    /* max concurrent readers to stream data from migration service */
    public static final int MAX_CONCURRENT_PARTITION_READERS = 1;

    /* logger */
    private final Logger logger;
    /* source RN to stream data from*/
    private final SourceRepNode sourceRN;
    /* RN hosting tif and subscription manager */
    private final HostRepNode hostRN;
    /* configuration of subscription */
    private final SubscriptionConfig config;
    /* subscription of replication stream */
    private final Subscription repStreamConsumer;
    /* group of partition readers indexed by partition id */
    private final Map<PartitionId, PartitionReader> partitionReaderMap;
    /* degree of parallelism in terms of # of active partition readers */
    private final int dop;
    /* filtering entries from rep stream */
    private final SubscriptionFilter filter;
    /* schedule partition reader threads */
    private final KVStoreImpl.TaskExecutor executor;
    /*
     * map of client-defined non-default partition reader callback.
     *
     * User is able to specify callback for a partition. If user does not
     * provide callback, the default partition reader callback will be
     * used.
     */
    private final Map<PartitionId, PartitionReaderCallBack> partReaderCbkMap;
    /*
     * a FIFO queue to which items from rep stream are enqueued
     * by subscription callbacks, and dequeued by a TIF worker.
     */
    private final BlockingQueue<DataItem> inputQueueRepStream;
    /*
     * a FIFO queue to which items from partition transfer are enqueued
     * by subscription callbacks, and dequeued by multiple TIF workers.
     */
    private final BlockingQueue<DataItem> inputQueuePartReader;
    /* local copy of owned partitions, updated when topology changes */
    private final Set<PartitionId> managedPartitions;

    /* state of subscription */
    private SubscriptionState state;
    /* topology sequence number */
    private long topologySeq;

    SubscriptionManager(SourceRepNode sourceRN,
                        HostRepNode hostRN,
                        SubscriptionConfig config,
                        Logger logger) {
        this.sourceRN = sourceRN;
        this.hostRN = hostRN;
        this.config = config;
        this.logger = logger;

        partitionReaderMap = new HashMap<>();
        partReaderCbkMap = new HashMap<>();

        /*
         * At the beginning, the managed partitions and its topology sequence
         * number are both from source config. They would be updated when
         * partitions migrate later and topology updates.
         */
        managedPartitions = new HashSet<>(sourceRN.getPartitionIdSet());
        topologySeq = sourceRN.getTopoSequence();

        /* init scheduler */
        dop = computeDOP();
        executor = new SharedThreadPool(logger).getTaskExecutor(dop);

        /* input queue to queue data entry from replication stream */
        inputQueueRepStream =
            new ArrayBlockingQueue<>(config.getInputMessageQueueSize());

        /* input queue to queue data entry from partition transfer stream */
        inputQueuePartReader =
            new ArrayBlockingQueue<>(config.getOutputMessageQueueSize());

        /* create filter */
        filter = new SubscriptionFilter();

        /* set subscription callback used in TIF */
        config.setCallback
            (new FeederSubscriptionCbk(filter, inputQueueRepStream, logger));
        repStreamConsumer = new Subscription(config, logger);

        /* now subscription state is idle */
        state = SubscriptionState.READY;
    }

    /**
     * Starts streaming from specified VLSN
     *
     * @param vlsn start VLSN
     */
    void startStream(long vlsn) {

        /* start receiving rep stream from a vlsn */
        state = SubscriptionState.REPLICATION_STREAM;
        final String nodeName = sourceRN.getRepNodeId().getFullName();
        logger.log(Level.INFO,
                   "Start streaming from source node {0}, start vlsn: {1}",
                   new Object[]{nodeName, vlsn});
        try {
            repStreamConsumer.start(vlsn);
            logger.log(Level.INFO,
                       "Subscription succeeded, requested vlsn {0}" +
                       " is available at {1}.",
                       new Object[]{vlsn, nodeName});
        } catch (InsufficientLogException ile) {

            /* requested VLSN is not available, switch to partition transfer */
            logger.log(Level.INFO,
                       "Requested vlsn {0} is not available at {1}, switch to" +
                       " initial replication.",
                       new Object[]{vlsn, nodeName});

            /*
             * shut down this consumer, a new one will be created after
             * the first partition is done
             */
            repStreamConsumer.shutdown();
            startStream(managedPartitions);
        } catch (IllegalArgumentException | GroupShutdownException |
            InternalException | TimeoutException e) {
            logger.log(Level.WARNING,
                       "Unable to start replication due to error {0}",
                       e.getMessage());
            repStreamConsumer.shutdown();
            state = SubscriptionState.ERROR;
        }
    }

    /**
     * Starts streaming by transferring partitions from source.
     *
     * @param toTransfer  set of partitions to transfer
     */
    void startStream(Set<PartitionId> toTransfer) {

        state = SubscriptionState.PARTITION_TRANSFER;
        /* initialize readers and schedule transfer for each partition */
        for (PartitionId partitionId : toTransfer) {
            scheduleTransfer(partitionId);
        }
        logger.log(Level.INFO,
                   "All {0} partition receivers scheduled to transfer, with " +
                   "DOP {1}, partitions scheduled to transfer: {2}",
                   new Object[]{toTransfer.size(), dop,
                       partitionListToString(toTransfer)});
    }

    /**
     * Start a mixed mode streaming to stream data from a VLSN concurrently
     * with partition transfer. This is used in the scenario that after
     * recovery, by comparing the list of completed partitions in checkpoint
     * with the list of partitions hosted by source, TIF finds there are
     * partitions in source but not in checkpoint. These partitions could
     * be migrated into the source during the failure.
     *
     * @param vlsn   start VLSN
     * @param toTransfer   set of partitions to transfer
     */
    void startStream(long vlsn, Set<PartitionId> toTransfer) {

        assert (toTransfer != null && toTransfer.size() > 0);

        state = SubscriptionState.PARTITION_TRANSFER;
        filter.setCompletePartitions(toTransfer);
        logger.log(Level.INFO,
                   "All {0} partition receivers scheduled to transfer, with " +
                   "DOP {1}",
                   new Object[]{toTransfer.size(), dop});

        try {

            repStreamConsumer.start(vlsn);

            /* if no exception, subscription to rep stream is successful */
            assert (repStreamConsumer.getSubscriptionStatus()
                                     .equals(SubscriptionStatus.SUCCESS));
            logger.log(Level.INFO,
                       "Successfully start rep stream consumer from vlsn {0}.",
                       vlsn);

            /* initialize readers and schedule transfer for each partition */
            for (PartitionId partitionId : toTransfer) {
                scheduleTransfer(partitionId);
            }
            logger.log(Level.INFO,
                       "All {0} partitions scheduled to transfer with DOP " +
                       "{1}, list of partitions: {2}.",
                       new Object[]{toTransfer.size(), dop,
                           partitionListToString(toTransfer)});

        } catch (TimeoutException | IllegalArgumentException |
            GroupShutdownException | InternalException |
            InsufficientLogException e) {
            logger.log(Level.WARNING,
                       "Unable to subscribe from VLSN {0}, reason: {1}",
                       new Object[]{vlsn, e.getMessage()});
            shutdown(SubscriptionState.ERROR);
        }
    }

    /**
     * Shutdown all active partition readers and rep stream consumer
     *
     * @param exitCode  exit code to set after shutdown
     */
    void shutdown(SubscriptionState exitCode) {

        /* shut down all partition readers */
        for (Map.Entry<PartitionId, PartitionReader> entry :
            partitionReaderMap.entrySet()) {

            PartitionReader reader = entry.getValue();
            PartitionId partitionId = entry.getKey();
            logger.log(Level.INFO, "Shutdown receiver for {0} in state {1}",
                       new Object[]{partitionId,
                           reader.getStatus().getState()});
            reader.shutdown();
        }
        logger.log(Level.INFO, "All partition readers shut down.");

        /* shut down rep stream consumer */
        repStreamConsumer.shutdown();
        logger.log(Level.INFO,
                   "Rep stream consumer shut down, all subscription " +
                   "activities stopped.");

        state = exitCode;
    }

    /**
     * Gets replication stream consumer
     *
     * @return replication stream consumer
     */
    Subscription getRepStreamConsumer() {
        return repStreamConsumer;
    }

    /**
     * Gets DOP to concurrently transfer partitions from source
     *
     * @return DOP of partition readers
     */
    int getDOPForPartTransfer() {
        return dop;
    }

    /**
     * Gets subscription filter
     *
     * @return subscription filter
     */
    SubscriptionFilter getSubscriptionFilter() {
        return filter;
    }

    /**
     * Gets the state of subscription
     *
     * @return  state of subscription
     */
    SubscriptionState getState() {
        return state;
    }

    /**
     * Gets input queue for replication stream
     *
     * @return input queue for replication stream
     */
    BlockingQueue<DataItem> getInputQueueRepStream() {
        return inputQueueRepStream;
    }

    /**
     * Gets input queue for partition readers
     *
     * @return input queue for partition readers
     */
    BlockingQueue<DataItem> getInputQueuePartReader() {
        return inputQueuePartReader;
    }

    /**
     * Gets partition reader map
     *
     * @return partition reader map
     */
    Map<PartitionId, PartitionReader> getPartitionReaderMap() {
        return partitionReaderMap;
    }

    /**
     * Checks if a partition is managed by the TIF
     *
     *
     * @param pid  id of partition
     */
    boolean isManangedPartition(PartitionId pid) {
        return managedPartitions.contains(pid);

    }

    /**
     * Returns set of all managed partitions
     *
     * @return set of all managed partitions
     */
    Set<PartitionId> getManagedPartitions() {
        return managedPartitions;
    }

    /**
     * Returns topology sequence number
     *
     * @return topology sequence number
     */
    long getCurrentTopologySeq() {
        return topologySeq;
    }

    /**
     * Sets topology sequence number
     *
     * @param seq topology sequence number
     */
    void setCurrentTopologySeq(final long seq) {
        topologySeq = seq;
    }

    /**
     * For test use only.
     *
     * Set the partition reader callback
     *
     * @param pid id of partition for the callback
     * @param cbk partition reader callback from client
     */
    synchronized void setPartitionReaderCallBack(PartitionId pid,
                                                 PartitionReaderCallBack cbk) {
        partReaderCbkMap.put(pid, cbk);
    }

    /**
     * Sets subscription state
     *
     * @param s state of subscription
     */
    synchronized void setSubscriptionState(SubscriptionState s) {
        final SubscriptionState old = state;
        state = s;
        logger.log(Level.INFO,
                   "Subscription state is set from {0} to {1}",
                   new Object[]{old, state});
    }

    /**
     * Returns true if all partitions are done transfer
     */
    synchronized boolean allPartComplete() {

        /* check each partition from source to ensure transfer is done */
        for (PartitionId partitionId : managedPartitions) {

            if (!partitionReaderMap.containsKey(partitionId)) {
                /* reader not yet scheduled */
                return false;
            }

            if (partitionReaderMap.get(partitionId).getStatus().getState() !=
                PartitionRepState.DONE) {
                return false;
            }
        }

        return true;
    }

    /* add a new partition */
    synchronized void addPartition(PartitionId pid) {

        if (pid == null) {
            logger.log(Level.FINE, "Null partition, ignore");
            return;
        }

        if (managedPartitions.contains(pid)) {
            logger.log(Level.FINE, "Partition {0} already exist, ignore", pid);
            return;
        }

        managedPartitions.add(pid);
        scheduleTransfer(pid);
        logger.log(Level.INFO,
                   "Partition {0} added into managed partitions: {1}",
                   new Object[]{pid, partitionListToString(managedPartitions) });
    }

    /* remove a partition */
    synchronized void removePartition(PartitionId pid) {

        if (pid == null) {
            logger.log(Level.FINE, "Null partition, ignore");
            return;
        }

        if (!managedPartitions.contains(pid)) {
            logger.log(Level.FINE, "Partition {0} does not exist, ignore", pid);
            return;
        }

        final PartitionReader reader = partitionReaderMap.get(pid);
        if (reader != null) {
            logger.log(Level.FINE,
                       "Shutdown reader for {0} in state {1}",
                       new Object[]{pid, reader.getStatus().getState()});
            reader.shutdown();
            /* adjust filter */
            filter.removePartition(pid);
        } else {
            logger.log(Level.FINE, "No reader for {0}, ignore.", pid);
        }

        managedPartitions.remove(pid);
        logger.log(Level.INFO,
                   "Partition {0} removed from managed partitions: {1}",
                   new Object[]{pid, partitionListToString(managedPartitions)});
    }

    /* compute DOP to transfer partitions from source */
    private int computeDOP() {
        /*
         * DOP used partition transfer is minimum of
         *
         * 1. number of partitions hosted at RN
         * 2. max concurrent part transfers allowed in source PMS
         * 3. max concurrent readers allowed in the manager
         */
        return Math.min(Math.min(managedPartitions.size(),
                                 sourceRN.getConcurrentSourceLimit()),
                        MAX_CONCURRENT_PARTITION_READERS);
    }

    /* schedules a partition transfer */
    private synchronized void scheduleTransfer(PartitionId partitionId) {

        if (partitionReaderMap.containsKey(partitionId)) {
            /*
             * if partition is already scheduled to transfer, or in transfer,
             * no need to reschedule it, log and ignore the request.
             */
            final PartitionReader old = partitionReaderMap.get(partitionId);
            final PartitionRepState s = old.getStatus().getState();
            if (s == PartitionRepState.IDLE ||
                s == PartitionRepState.REPLICATING) {
                logger.log(Level.FINE,
                           "Partition {0} scheduled to transfer, or in " +
                           "transfer, ignore (state: {1})",
                           new Object[]{partitionId, s});
                return;
            }

            logger.log(Level.FINE, "Found existent reader for {0}, state: {1}.",
                       new Object[]{partitionId, s});

            /* for all other cases, shut off old reader */
            old.shutdown();
            partitionReaderMap.remove(partitionId);
        }

        /* schedule a new partition transfer */
        PartitionReaderCallBack cbk;
        /* use default cbk if not set in cbk map */
        if (partReaderCbkMap.containsKey(partitionId)) {
            cbk = partReaderCbkMap.get(partitionId);
            logger.log(Level.FINE, "Partition {0} uses client-defined cbk.", 
                       partitionId);
        } else {
            cbk = new FeederPartReaderCbk(this, partitionId, logger);
        }

        final PartitionReader reader = new PartitionReader(hostRN.getRepEnv(),
                                                           partitionId,
                                                           cbk,
                                                           config,
                                                           logger);

        partitionReaderMap.put(partitionId, reader);
        executor.submit(reader);
        logger.log(Level.FINE, "Partition {0} is scheduled to transfer.",
                   partitionId);
        if (!state.equals(SubscriptionState.PARTITION_TRANSFER)) {
            logger.log(Level.INFO,
                       "Subscription state changes from {0} to {1}.",
                       new Object[]{state,
                           SubscriptionState.PARTITION_TRANSFER});
            state = SubscriptionState.PARTITION_TRANSFER;
        }
    }

    /* convert a list of partition ids to string format */
    static String partitionListToString(Set<PartitionId> parts) {

        if (parts == null || parts.isEmpty()) {
            return "[]";
        }

        return "["+ Arrays.toString(parts.toArray())+ "]";
    }

    /**
     * Object to filter entry from replication stream based on the
     * partition that entry belongs to, in the case that partition
     * readers run concurrently with replication stream consumer.
     *
     * It tracks the list of partitions that have been transferred
     * completely and will filter out all entries that do not belong to
     * the list.
     */
    public class SubscriptionFilter {

        /* statistics: set of partitions that have been transferred */
        private Set<PartitionId> completePartitions;

        /* statistics tracking num of entries filtered */
        private AtomicLong numEntryFiltered;

        SubscriptionFilter() {
            completePartitions = new HashSet<>();
            numEntryFiltered = new AtomicLong(0);
        }

        /**
         * Returns list of completely transferred partitions
         *
         * @return list of completely transferred partitions
         */
        public Set<PartitionId> getCompletePartitions() {
            return completePartitions;
        }

        /**
         * Set list of completely transferred partitions
         *
         * @param complete list of completed partitions
         */
        public void setCompletePartitions(Set<PartitionId> complete) {
            completePartitions = complete;
        }

        /**
         * Returns number of entries filtered
         *
         * @return number of entries filtered
         */
        public long getNumEntryFiltered() {
            return numEntryFiltered.get();
        }

        /**
         * Adds a partition from complete list
         *
         * @param pid id of partition to add
         * @return    true if the set adds it successfully
         */
        public synchronized boolean addPartition(PartitionId pid) {
            return completePartitions.add(pid);
        }

        /**
         * Removes a partition from complete list
         *
         * @param pid id of partition to remove
         * @return    true if the set contains partition and removes it
         *            successfully
         */
        public synchronized boolean removePartition(PartitionId pid) {
            return completePartitions.remove(pid);
        }

        /**
         * Checks if an entry from replication stream need to be filtered
         *
         * @param entry entry from replication stream
         * @return null if this entry is filtered, otherwise return input entry
         */
        public DataItem filter(DataItem entry) {

            if (state != SubscriptionState.PARTITION_TRANSFER) {
                /* no filtering if not in partition transfer state */
                return entry;
            }

            if (entry.isTxnAbort() || entry.isTxnCommit()) {
                /* never filter an txn op */
                return entry;
            }

            /* determine which partition this entry belongs to */
            PartitionId pid = sourceRN.getPartitionId(entry.getKey());

            if (completePartitions.contains(pid)) {
                /* pass the entry if its partition is transferred */
                return entry;
            }

            /* block the entry if not */
            numEntryFiltered.incrementAndGet();
            return null;
        }
    }
}
