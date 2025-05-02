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

import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.util.concurrent.atomic.AtomicLong;

import oracle.kv.impl.pubsub.security.StreamClientAuthHandler;

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.rep.subscription.SubscriptionStat;

/**
 * Object represents the statistics of the replication stream consumer
 */
class ReplicationStreamConsumerStat {

    /** parent consumer */
    private final ReplicationStreamConsumer parent;

    /** last streamed VLSN */
    private volatile long lastStreamVLSN;
    /** the start VLSN requested by client */
    private volatile long reqStartVLSN;
    /** the start VLSN acked by Feeder */
    private volatile long ackedStartVLSN;
    /** the last time receiving a msg from feeder */
    private volatile long lastMsgTimeMs;
    /** last committed vlsn before reconnect, internally used only */
    private volatile long lastVLSNBeforeReconnect;

    /** feeder filter metrics */
    private final FeederFilterStat feederFilterStat;

    /*- counters -*/
    /** number of puts */
    private final AtomicLong numPuts;
    /** number of dels */
    private final AtomicLong numDels;
    /** number of commits */
    private final AtomicLong numCommits;
    /** number of aborts */
    private final AtomicLong numAborts;
    /** number of exceptions */
    private final AtomicLong numExceptions;
    /** number of successful reconnect */
    private final AtomicLong numSuccReconn;

    /** partition md db id */
    private volatile DatabaseId partGenDBId;

    ReplicationStreamConsumerStat(ReplicationStreamConsumer parent) {
        this.parent = parent;
        lastStreamVLSN = NULL_VLSN;
        reqStartVLSN = NULL_VLSN;
        ackedStartVLSN = NULL_VLSN;
        lastVLSNBeforeReconnect = NULL_VLSN;
        lastMsgTimeMs = 0;
        partGenDBId = null;

        numPuts = new AtomicLong(0);
        numDels = new AtomicLong(0);
        numCommits = new AtomicLong(0);
        numAborts = new AtomicLong(0);
        numExceptions = new AtomicLong(0);
        numSuccReconn = new AtomicLong(0);

        feederFilterStat = new FeederFilterStat();
    }

    /**
     * @return parent replication stream consumer
     */
    ReplicationStreamConsumer getParent() {
        return parent;
    }

    /**
     * Gets the partition md db id
     *
     * @return the partition md db id, null if not initialized
     */
    DatabaseId getPartGenDBId() {
        return partGenDBId;
    }

    /**
     * Gets the start VLSN requested by client
     *
     * @return start VLSN requested by client
     */
    long getReqStartVLSN() {
        return reqStartVLSN;
    }

    /**
     * Gets the start VLSN acked by server
     *
     * @return start VLSN acked by server
     */
    long getAckedStartVLSN() {
        return ackedStartVLSN;
    }

    /**
     * Gets the VLSN of last committed transaction
     *
     * @return last commit VLSN
     */
    long getLastCommitVLSN() {
        return parent.getTxnBuffer().getLastCommitVLSN();
    }

    /**
     * Gets VLSN of last aborted transaction
     *
     * @return last aborted VLSN
     */
    long getLastAbortVLSN() {
        return parent.getTxnBuffer().getLastAbortVLSN();
    }

    /**
     * Gets num of open txns
     *
     * @return num of open txns
     */
    long getOpenTxns() {
        return parent.getTxnBuffer().getOpenTxns();
    }

    /**
     * Gets num of committed txns
     *
     * @return num of committed txns
     */
    long getCommitTxns() {
        return parent.getTxnBuffer().getCommitTxns();
    }

    /**
     * Gets num of committed ops
     *
     * @return num of committed ops
     */
    long getCommitOps() {
        return parent.getTxnBuffer().getCommitOps();
    }

    /**
     * Gets num of abort txns
     *
     * @return num of abort txns
     */
    long getAbortTxns() {
        return parent.getTxnBuffer().getAbortTxns();
    }

    /**
     * Gets num of abort ops
     *
     * @return num of abort ops
     */
    long getAbortOps() {
        return parent.getTxnBuffer().getAbortOps();
    }

    /**
     * Gets the last time the consumer received a msg from feeder
     *
     * @return the last time the consumer received a msg from feeder
     */
    long getLastMsgTimeMs() {
        return lastMsgTimeMs;
    }

    /**
     * Gets the last streamed VLSN, or NULL_VLSN if not set.
     *
     * @return the last streamed VLSN or NULL_VLSN
     */
    long getLastStreamedVLSN() {
        return lastStreamVLSN;
    }

    /**
     * Gets the feeder filter stat
     * @return the feeder filter stat
     */
    FeederFilterStat getFeederFilterStat() {
        return feederFilterStat.getSnapShot();
    }

    /**
     * Sets the requested start vlsn
     *
     * @param vlsn  the requested start vlsn
     */
    void setReqStartVLSN(long vlsn) {
        reqStartVLSN = vlsn;
    }

    /**
     * Sets the acked start vlsn
     *
     * @param vlsn the acked start vlsn
     */
    void setAckedStartVLSN(long vlsn) {
        ackedStartVLSN = vlsn;
    }

    /**
     * Sets the ast time the consumer received a msg from feeder
     *
     * @param t timestamp of last msg
     */
    void setLastMsgTimeMs(long t) {
        lastMsgTimeMs = t;
    }

    /**
     * Sets the number of successful re-connections
     *
     * @param t  number of successful re-connections
     */
    void setNumSuccReconn(long t) {
        numSuccReconn.set(t);
    }

    /**
     * Sets the partition md db id
     *
     * @param dbId  the partition md db id
     */
    void setPartGenDBId(DatabaseId dbId) {
        partGenDBId = dbId;
    }

    /* getters and increments for counters */

    long getNumPuts() {
        return numPuts.get();
    }

    long getNumDels() {
        return numDels.get();
    }

    long getNumCommits() {
        return numCommits.get();
    }

    long getNumAborts() {
        return numAborts.get();
    }

    long getNumExceptions() {
        return numExceptions.get();
    }

    long getNumSuccReconn() {
        return numSuccReconn.get();
    }

    long getLastVLSNBeforeReconnect() {
        return lastVLSNBeforeReconnect;
    }

    void incrNumPuts(long vlsn) {
        numPuts.incrementAndGet();
        lastStreamVLSN = vlsn;
    }

    void incrNumDels(long vlsn) {
        numDels.incrementAndGet();
        lastStreamVLSN = vlsn;
    }

    void incrNumCommits(long vlsn) {
        numCommits.incrementAndGet();
        lastStreamVLSN = vlsn;
    }

    void incrNumAborts(long vlsn) {
        numAborts.incrementAndGet();
        lastStreamVLSN = vlsn;
    }

    void incrNumExceptions() {
        numExceptions.incrementAndGet();
    }

    void setLastVLSNBeforeReconnect(long vlsn) {
        lastVLSNBeforeReconnect = vlsn;
    }

    /**
     * Sets the feeder filter stat from the subscription stat
     * @param stat subscription stat
     */
    void setFeederFilterStat(SubscriptionStat stat) {
        feederFilterStat.setFeederFilterStat(stat);
    }

    /**
    * Gets the number of times that token has been refreshed
     *
     * @return the number of times that token has been refreshed
     */
     long getNumTokenRefreshed() {
         if (parent.getAuthHandler() == null) {
             return 0;
         }
         return ((StreamClientAuthHandler)parent.getAuthHandler())
             .getNumTokenRefreshed();
     }

    /**
     * Gets max num of open txns
     *
     * @return max num of open txns
     */
    private long getMaxOpenTxns(){
        return parent.getTxnBuffer().getMaxOpenTxns();
    }

    String dumpStat() {
        return "\nStatistics of RSC " + parent.getConsumerId() + ":\n" +
               "requested start VLSN: " + reqStartVLSN + "\n" +
               "acknowledged start VLSN: " + ackedStartVLSN + "\n" +
               "# committed ops: " + getCommitOps() + "\n" +
               "# aborted ops: " + getAbortOps() + "\n" +
               "# committed txns: " + getCommitTxns() + "\n" +
               "# aborted txns: " + getAbortTxns() + "\n" +
               "# open txns: " + getOpenTxns() + "\n" +
               "# max open txns: " + getMaxOpenTxns() + "\n" +
               "# successful reconnections: " + getNumSuccReconn() + "\n" +
               "# token refreshed: " + getNumTokenRefreshed() + "\n" +
               "last committed txn vlsn: " + getLastCommitVLSN() + "\n" +
               "last aborted txn vlsn: " + getLastAbortVLSN();
    }

}
