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

import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import oracle.kv.impl.rep.subscription.partreader.PartitionReader;
import oracle.kv.impl.rep.subscription.partreader.PartitionReaderCallBack;
import oracle.kv.impl.topo.PartitionId;

import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.rep.GroupShutdownException;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.subscription.Subscription;
import com.sleepycat.je.rep.subscription.SubscriptionStatus;
import com.sleepycat.je.utilint.InternalException;
import com.sleepycat.je.utilint.VLSN;

/**
 * Default callback to process each entry received from partition migration
 * stream.
 */
class FeederPartReaderCbk implements PartitionReaderCallBack {

    private final PartitionId partitionId;
    private final SubscriptionManager manager;
    private final BlockingQueue<DataItem> inputQueue;
    private final PartitionReader reader;
    private final Subscription repStreamConsumer;
    private final Logger logger;

    FeederPartReaderCbk(SubscriptionManager manager,
                        PartitionId partitionId,
                        Logger logger) {
        this.partitionId = partitionId;
        this.manager = manager;
        this.logger = logger;
        reader = manager.getPartitionReaderMap().get(partitionId);
        repStreamConsumer = manager.getRepStreamConsumer();
        inputQueue = manager.getInputQueuePartReader();
    }

    /**
     * callback function to process a COPY or PUT operation
     *
     * @param pid   id of partition
     * @param vlsn  vlsn of operation
     * @param exp   expiration time
     * @param key   key to copy or put
     * @param value value part of operation
     */
    @Override
    public void processCopy(PartitionId pid, long vlsn, long exp, byte[] key,
                            byte[] value) {
        final DataItem entry = new DataItem(vlsn,
                                            DataItem.TXN_ID_COPY_IN_PARTTRANS,
                                            key, value);
        entry.setPartitionId(pid);
        processEntry(entry);
    }

    /**
     * callback function to process a COPY or PUT operation
     *
     * @param pid   id of partition
     * @param vlsn  vlsn of operation
     * @param exp   expiration time
     * @param key   key to copy or put
     * @param value value part of operation
     * @param txnId id of transaction
     */
    @Override
    public void processPut(PartitionId pid, long vlsn, long exp, byte[] key,
                           byte[] value, long txnId) {
        final DataItem entry = new DataItem(vlsn, txnId, key, value);
        entry.setPartitionId(pid);
        processEntry(entry);
    }

    /**
     * callback function to process a DEL operation
     *
     * @param pid   id of partition
     * @param vlsn  vlsn of operation
     * @param key   key to delete
     * @param txnId id of transaction
     */
    @Override
    public void processDel(PartitionId pid,
                           long vlsn,
                           byte[] key,
                           long txnId) {
        final DataItem entry = new DataItem(vlsn, txnId, key);
        entry.setPartitionId(pid);
        processEntry(entry);
    }

    /**
     * callback function to process a PREPARE operation
     *
     * @param pid   id of partition
     * @param txnId id of transaction
     */
    @Override
    public void processPrepare(PartitionId pid, long txnId) {
        /* useless to TIF, skip */
    }

    /**
     * callback function to COMMIT a transaction
     *
     * @param pid   id of partition
     * @param txnId id of transaction
     */
    @Override
    public void processCommit(PartitionId pid, long txnId) {
        /*
         * current migration stream implementation does not send VLSN for
         * transaction resolution such as COMMIT and ABORT, we tentatively
         * use a NULL VLSN. TIF need take it in consideration when consuming
         * VLSN from entries from migration stream.
         */

        final DataItem entry = new DataItem(NULL_VLSN, txnId,
                                            LogEntryType.LOG_TXN_COMMIT);
        entry.setPartitionId(pid);
        processEntry(entry);
    }

    /**
     * callback function to ABORT a transaction
     *
     * @param pid   id of partition
     * @param txnId id of transaction
     */
    @Override
    public void processAbort(PartitionId pid, long txnId) {
        final DataItem entry = new DataItem(NULL_VLSN, txnId,
                                            LogEntryType.LOG_TXN_ABORT);
        entry.setPartitionId(pid);
        processEntry(entry);
    }

    /**
     * callback function to process EOD
     *
     * @param pid id of partition
     */
    @Override
    public synchronized void processEOD(PartitionId pid) {

        final SubscriptionManager.SubscriptionFilter
            filter = manager.getSubscriptionFilter();
        final long highVLSN = reader.getHighestVLSN();

        logger.info(partitionId + " transfer is done at high vlsn " + highVLSN);
        /* add this partition to completed partitions in filter */
        filter.addPartition(pid);
        try {

            /* the first partition done, start ongoing rep from high vlsn */
            if (filter.getCompletePartitions().size() == 1) {
                assert (repStreamConsumer != null);

                if (!repStreamConsumer.getSubscriptionStatus()
                                      .equals(SubscriptionStatus.SUCCESS)) {
                    /* start rep stream if not started */
                    repStreamConsumer.start(VLSN.getNext(highVLSN));

                    /* no exception, subscription is successful */
                    assert (repStreamConsumer.getSubscriptionStatus()
                                             .equals(
                                                 SubscriptionStatus.SUCCESS));

                    logger.info("successfully start rep stream consumer " +
                                "from VLSN " + VLSN.getNext(highVLSN) +
                                " after finishing " + partitionId);
                } else {
                    logger.info("the rep stream consumer has already started " +
                                "when " + partitionId + " is done.");

                }
            }

            /* the last partition done, change state to ongoing */
            if (manager.allPartComplete()) {
                manager.setSubscriptionState(SubscriptionState
                                                 .REPLICATION_STREAM);
            }

        } catch (InsufficientLogException | IllegalArgumentException |
            TimeoutException | GroupShutdownException | InternalException e) {
            logger.warning("Unable to subscribe from VLSN " +
                           VLSN.getNext(highVLSN) +
                           " after finishing " + partitionId + "" +
                           ", reason: " + e.getMessage());
            manager.shutdown(SubscriptionState.ERROR);
        }
    }

    /* action applied to each entry. user can override */
    private void processEntry(DataItem entry) {
        while (true) {
            try {
                inputQueue.put(entry);
                return;
            } catch (InterruptedException e) {
                /* This might have to get smarter. */
                logger.warning(partitionId + ": interrupted input " +
                               "queue operation put, retrying");
            }
        }
    }
}
