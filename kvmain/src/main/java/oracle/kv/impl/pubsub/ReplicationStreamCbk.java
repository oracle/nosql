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

package oracle.kv.impl.pubsub;

import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.rep.subscription.SubscriptionCallback;

/**
 * Default callback to process each entry received from replication stream.
 */
class ReplicationStreamCbk implements SubscriptionCallback {

    /* private logger */
    private final Logger logger;
    /* FIFO queue for entries from replication stream */
    private final BlockingQueue<? super DataEntry> queue;
    /* statistics */
    private final ReplicationStreamConsumerStat stat;
    /** shard id */
    private final int shard;

    ReplicationStreamCbk(BlockingQueue<? super DataEntry> queue,
                         ReplicationStreamConsumerStat stat,
                         Logger logger) {
        this.queue = queue;
        this.stat = stat;
        this.logger = logger;
        shard = stat.getParent().getRepGroupId().getGroupId();
    }

    public ReplicationStreamConsumerStat getStat() {
        return stat;
    }

    /**
     * Processes a put (insert or update) entry from stream
     *
     * @param vlsn  VLSN of the insert entry
     * @param key   key of the insert entry
     * @param value value of the insert entry
     * @param txnId id of txn the entry belongs to
     * @param dbId  id of database the entry belongs to
     * @param ts    timestamp of the last update
     * @param expMs expiration time in system time in ms
     */
    @Override
    public void processPut(long vlsn, byte[] key, byte[] value, long txnId,
                           DatabaseId dbId, long ts, long expMs,
                            boolean beforeImgEnabled,
                            byte[] valBeforeImg,
                            long tsBeforeImg,
                            long expBeforeImg) {
        processEntry(new DataEntry(DataEntry.Type.PUT,
                                   vlsn,
                                   txnId,
                                   key,
                                   value,
                                   dbId,
                                   ts,
                                   expMs));
        stat.incrNumPuts(vlsn);
    }

    /**
     * Processes a delete entry from stream
     *
     * @param vlsn  VLSN of the delete entry
     * @param key   key of the delete entry
     * @param val   val of tombstone if exists, null otherwise
     * @param txnId id of txn the entry belongs to
     * @param dbId  id of database the entry belongs to
     * @param ts    timestamp of the last update
     */
    @Override
    public void processDel(long vlsn, byte[] key, byte[] val, long txnId,
                           DatabaseId dbId,
                           long ts,  boolean beforeImgEnabled,
                           byte[] valBeforeImg,
                           long tsBeforeImg,
                           long expBeforeImg) {
        processEntry(new DataEntry(DataEntry.Type.DELETE,
                                   vlsn,
                                   txnId,
                                   key,
                                   val,
                                   dbId,
                                   ts));
        stat.incrNumDels(vlsn);
    }

    /**
     * Processes a commit entry from stream
     *
     * @param vlsn  VLSN of commit entry
     * @param txnId id of txn to commit
     * @param ts  timestamp of commit
     */
    @Override
    public void processCommit(long vlsn, long txnId, long ts) {
        processEntry(DataEntry.getCommitEntry(vlsn, txnId, ts));
        stat.incrNumCommits(vlsn);
    }

    /**
     * Processes an abort entry from stream
     *
     * @param vlsn  VLSN of abort entry
     * @param txnId id of txn to abort
     * @param ts  timestamp of abort
     */
    @Override
    public void processAbort(long vlsn, long txnId, long ts) {
        processEntry(DataEntry.getAbortEntry(vlsn, txnId, ts));
        stat.incrNumAborts(vlsn);
    }

    /**
     * Processes the exception from stream.
     *
     * @param exp exception raised in service and to be processed by
     *            client
     */
    @Override
    public void processException(final Exception exp) {
        stat.incrNumExceptions();

        /*
         * When receiving an exception msg from feeder, the JE client thread
         * will shutdown the stream  after calling this function. The
         * replication stream consumer which owns the JE client thread is
         * supposed to retry or terminate the subscription.
         */
        logger.warning(lm("Exception in replication stream, error=" + exp));
    }

    /* internal helper to process each entry */
    private void processEntry(DataEntry dataEntry) {
        try {

            queue.put(dataEntry);
            logger.finest(() -> lm("enqueued entry with " +
                                   "type=" + dataEntry.getType() +
                                   ", txn id=" + dataEntry.getTxnID() +
                                   ", key=" + dataEntry.getTxnID()));

        } catch (InterruptedException ie) {
            /* thread is shut down by others */
            logger.warning(lm("Interrupted queue operation for entry=" +
                              dataEntry));
        }
    }

    private String lm(String msg) {
        return "[RSCBK][shard=" + shard + "] " + msg;
    }
}
