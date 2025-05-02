/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.rep.subscription;

import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatGroup;

/**
 * Object to represent subscription statistics
 */
public class SubscriptionStat {

    /*
     * VLSN from which feeder agrees to stream log entries, it is returned from
     * the feeder and can be equal to or earlier than the VLSN requested by the
     * client, which is specified in subscription configuration.
     */
    private long startVLSN = NULL_VLSN;

    /* the last VLSN that has been processed */
    private long highVLSN = INVALID_VLSN;

    /* last feeder filter processed vlsn */
    private long lastFilterVLSN = NULL_VLSN;

    /* last vlsn passing the filter */
    private long lastPassVLSN = NULL_VLSN;

    /* modification time of last filter processed op, -1 if unspecified */
    private long lastModTimeMs = -1;

    /* last commit time stamp in feeder, -1 if unspecified */
    private long lastCommitTimeMs = -1;

    /* partition generation db id or null if not known */
    private DatabaseId partGenDBId;

    /* used by main thread: # of retries to insert msgs into input queue */
    private final LongStat nReplayQueueOverflow;
    /* used by main thread: # of msgs received from feeder */
    private final LongStat nMsgReceived;
    /* used by main thread: max # of items pending in input queue */
    private final LongStat maxPendingInput;
    /* used by output thread: # of acks sent to feeder */
    private final LongStat nMsgResponded;
    /* used by input thread: # of data ops processed */
    private final LongStat nOpsProcessed;
    /* used by input thread: # of txn aborted and committed */
    private final LongStat nTxnAborted;
    private final LongStat nTxnCommitted;
    private final LongStat nHeartbeatsSent;
    private final LongStat nHeartbeatsReceived;

    SubscriptionStat(StatGroup stats) {

        partGenDBId = null;

        nReplayQueueOverflow = new LongStat(stats,
                SubscriptionStatDefinition.SUB_N_REPLAY_QUEUE_OVERFLOW, 0L);
        nMsgReceived = new LongStat(stats,
                SubscriptionStatDefinition.SUB_MSG_RECEIVED, 0L);
        nMsgResponded = new LongStat(stats,
                SubscriptionStatDefinition.SUB_MSG_RESPONDED, 0L);
        maxPendingInput = new LongStat(stats,
                SubscriptionStatDefinition.SUB_MAX_PENDING_INPUT, 0L);

        nOpsProcessed = new LongStat(stats,
                SubscriptionStatDefinition.SUB_OPS_PROCESSED, 0L);
        nTxnAborted = new LongStat(stats,
                SubscriptionStatDefinition.SUB_TXN_ABORTED, 0L);
        nTxnCommitted = new LongStat(stats,
                SubscriptionStatDefinition.SUB_TXN_COMMITTED, 0L);
        nHeartbeatsSent = new LongStat(stats, 
        		SubscriptionStatDefinition.SUB_HEARTBEAT_SENT, 0L);
        nHeartbeatsReceived = new LongStat(stats, 
        		SubscriptionStatDefinition.SUB_HEARTBEAT_RECEIVED, 0L);

    }

    /*--------------*/
    /*-  Getters   -*/
    /*--------------*/
    public synchronized LongStat getNumReplayQueueOverflow() {
        return nReplayQueueOverflow;
    }
    
    public synchronized LongStat getMaxPendingInput() {
        return maxPendingInput;
    }
    
    public synchronized LongStat getNumMsgResponded() {
        return nMsgResponded;
    }

    public synchronized LongStat getNumMsgReceived() {
        return nMsgReceived;
    }

    public synchronized LongStat getNumOpsProcessed() {
        return nOpsProcessed;
    }

    public synchronized LongStat getNumTxnAborted() {
        return nTxnAborted;
    }

    public synchronized LongStat getNumTxnCommitted() {
        return nTxnCommitted;
    }

    public synchronized long getStartVLSN() {
        return startVLSN;
    }

    public synchronized long getLastFilterVLSN() {
        return lastFilterVLSN;
    }

    public synchronized long getLastPassVLSN() {
        return lastPassVLSN;
    }

    public synchronized long getLastModTimeMs() {
        return lastModTimeMs;
    }

    public synchronized long getLastCommitTimeMs() {
        return lastCommitTimeMs;
    }

    public synchronized long getHighVLSN() {
        return highVLSN;
    }

    public synchronized DatabaseId getPartGenDBId() {
        return partGenDBId;
    }
    
    public synchronized LongStat getNHeartbeatsSent() {
        return nHeartbeatsSent;
    }

    public synchronized LongStat getNHeartbeatsReceived() {
        return nHeartbeatsReceived;
    }

    /*--------------*/
    /*-  Setters   -*/
    /*--------------*/
    public synchronized void setStartVLSN(long vlsn) {
        startVLSN = vlsn;
    }

    public synchronized void setHighVLSN(long vlsn) {
        highVLSN = vlsn;
    }

    public synchronized void setPartGenDBId(DatabaseId dbId) {
        partGenDBId = dbId;
    }

    public synchronized void setLastFilterVLSN(long vlsn) {
        lastFilterVLSN = vlsn;
    }

    public synchronized void setLastPassVLSN(long vlsn) {
        lastPassVLSN = vlsn;
    }

    public synchronized void setLastModTimeMs(long ts) {
        lastModTimeMs = ts;
    }

    public synchronized void setLastCommitTimeMs(long ts) {
        lastCommitTimeMs = ts;
    }
}
