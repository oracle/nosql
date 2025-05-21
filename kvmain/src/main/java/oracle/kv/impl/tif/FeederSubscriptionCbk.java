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


import static com.sleepycat.je.log.LogEntryType.LOG_TXN_ABORT;
import static com.sleepycat.je.log.LogEntryType.LOG_TXN_COMMIT;

import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.rep.subscription.SubscriptionCallback;

/**
 * Default subscription callback in TIF to process each entry received from
 * replication stream.
 */
class FeederSubscriptionCbk implements SubscriptionCallback {

    protected final Logger logger;
    protected final SubscriptionManager.SubscriptionFilter filter;
    protected final BlockingQueue<DataItem> inputQueueRepStream;

    FeederSubscriptionCbk(SubscriptionManager.SubscriptionFilter filter,
                          BlockingQueue<DataItem> inputQueueRepStream,
                          Logger logger) {
        this.filter = filter;
        this.inputQueueRepStream = inputQueueRepStream;
        this.logger = logger;

    }

    /**
     * Process a put (insert or update) entry from stream
     *
     * @param vlsn   VLSN of the insert entry
     * @param key    key of the insert entry
     * @param value  value of the insert entry
     * @param txnId  id of txn the entry belongs to
     * @param dbId   id of database the entry belongs to
     * @param ts     timestamp of the last update
     * @param exp    expiration time in ms, not used in TIF
     */
    @Override
    public void processPut(long vlsn, byte[] key, byte[] value, long txnId,
                           DatabaseId dbId, long ts, long exp,
                           boolean beforeImgEnabled, byte[] valBeforeImg,
                           long tsBeforeImg, long expBeforeImg) {
        processEntry(new DataItem(vlsn, txnId, key, value));
    }

    /**
     * Process a delete entry from stream
     *
     * @param vlsn   VLSN of the delete entry
     * @param key    key of the delete entry
     * @param val    val of tombstone if exists, null otherwise
     * @param txnId  id of txn the entry belongs to
     * @param dbId   id of database the entry belongs to
     * @param ts     timestamp of the last update
     */
    @Override
    public void processDel(long vlsn, byte[] key, byte[] val, long txnId,
                           DatabaseId dbId, long ts, boolean beforeImgEnabled,
                           byte[] valBeforeImg, long tsBeforeImg,
                           long expBeforeImg) {
        processEntry(new DataItem(vlsn, txnId, key, val, true));
    }

    /**
     * Process a commit entry from stream
     *
     * @param vlsn  VLSN of commit entry
     * @param txnId id of txn to commit
     */
    @Override
    public void processCommit(long vlsn, long txnId) {
        processEntry(new DataItem(vlsn, txnId, LOG_TXN_COMMIT));
    }

    /**
     * Process an abort entry from stream
     *
     * @param vlsn  VLSN of abort entry
     * @param txnId id of txn to abort
     */
    @Override
    public void processAbort(long vlsn, long txnId) {
        processEntry(new DataItem(vlsn, txnId, LOG_TXN_ABORT));
    }

    /**
     * Process the exception from stream.
     *
     * @param exp  exception raised in service and to be processed by
     *             client
     */
    @Override
    public void processException(final Exception exp) {
        logger.info("Exception in subscription of replication stream "  +
                    exp.getLocalizedMessage());

        processEntry(new DataItem(exp));
    }

    /* internal helper to process each operation */
    private void processEntry(DataItem entry) {

        entry = filter.filter(entry);
        if (entry == null) {
            return;
        }

        /* enqueue data entry to be processed by TIF worker */
        while (true) {
            try {
                inputQueueRepStream.put(entry);
                return;
            } catch (InterruptedException e) {
                /* might have to get smarter. */
                logger.warning("Interrupted input queue operation put, " +
                               "retrying");
            }
        }
    }
}
