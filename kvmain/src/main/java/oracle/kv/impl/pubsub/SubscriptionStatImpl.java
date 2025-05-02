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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.pubsub.NoSQLSubscriberId;
import oracle.kv.stats.SubscriptionMetrics;

/**
 * An implementation of subscription metrics representing a bag of statistics
 * of a single subscription.
 */
public class SubscriptionStatImpl implements SubscriptionMetrics {

    /* sid of the parent subscriber */
    private final NoSQLSubscriberId sid;
    /* shards covered by the parent subscriber */
    private final Set<RepGroupId> shards;

    /* the start VLSN requested */
    private final Map<RepGroupId, Long> reqStartVLSN;
    /* the start VLSN acked by Feeder */
    private final Map<RepGroupId, Long> ackedStartVLSN;
    /* last committed VLSN */
    private final Map<RepGroupId, Long> lastCommitVLSN;
    /* last abortted VLSN */
    private final Map<RepGroupId, Long> lastAbortVLSN;
    /* the last time receiving a msg from feeder */
    private final Map<RepGroupId, Long> lastMsgTime;
    /* total open txns */
    private final Map<RepGroupId, Long> currOpenTxns;
    /* number of successful reconnections */
    private final Map<RepGroupId, Long> numSuccReconn;

    /* total ops that have been consumed by subscriber*/
    private final AtomicLong totalConsumedOps;
    /* total committed txns */
    private final AtomicLong totalCommitTxns;
    /* total aborted txns */
    private final AtomicLong totalAbortTxns;
    /* total committed ops */
    private final AtomicLong totalCommitOps;
    /* total abort ops */
    private final AtomicLong totalAbortOps;
    /* total token refreshed, including # renewal and re-authentication */
    private final AtomicLong totalTokenRefreshed;


    /**
     * Creates an stat instance for subscription
     *
     * @param sid     id of subscriber
     * @param shards  set of shards
     */
    SubscriptionStatImpl(NoSQLSubscriberId sid, Set<RepGroupId> shards) {

        this.sid = sid;
        this.shards = shards;

        reqStartVLSN = new ConcurrentHashMap<>();
        ackedStartVLSN = new ConcurrentHashMap<>();
        lastAbortVLSN = new ConcurrentHashMap<>();
        lastCommitVLSN = new ConcurrentHashMap<>();
        lastMsgTime = new ConcurrentHashMap<>();
        currOpenTxns = new ConcurrentHashMap<>();
        numSuccReconn = new ConcurrentHashMap<>();
        for (RepGroupId id : shards) {
            reqStartVLSN.put(id, NULL_VLSN);
            ackedStartVLSN.put(id, NULL_VLSN);
            lastAbortVLSN.put(id, NULL_VLSN);
            lastCommitVLSN.put(id, NULL_VLSN);
            lastMsgTime.put(id, (long)0);
            currOpenTxns.put(id, (long)0);
        }

        totalConsumedOps = new AtomicLong(0);
        totalCommitTxns = new AtomicLong(0);
        totalAbortTxns = new AtomicLong(0);
        totalCommitOps = new AtomicLong(0);
        totalAbortOps = new AtomicLong(0);
        totalTokenRefreshed = new AtomicLong(0);
    }

    @Override
    public synchronized Map<RepGroupId, Long> getReqStartVLSN() {
        return copy(reqStartVLSN);
    }

    @Override
    public synchronized Map<RepGroupId, Long> getAckStartVLSN() {
        return copy(ackedStartVLSN);
    }

    @Override
    public synchronized Map<RepGroupId, Long> getLastCommitVLSN() {
        return copy(lastCommitVLSN);
    }

    @Override
    public synchronized Map<RepGroupId, Long> getLastAbortVLSN() {
        return copy(lastAbortVLSN);
    }

    @Override
    public synchronized Map<RepGroupId, Long> getLastMsgTimeStamp() {
        return copy(lastMsgTime);
    }

    @Override
    public long getTotalConsumedOps(){
        return totalConsumedOps.get();
    }

    @Override
    public long getTotalCommitTxns() {
        return totalCommitTxns.get();
    }

    @Override
    public long getTotalAbortTxns() {
        return totalAbortTxns.get();
    }

    @Override
    public long getTotalCommitOps() {
        return totalCommitOps.get();
    }

    @Override
    public long getTotalAbortOps() {
        return totalAbortOps.get();
    }

    @Override
    public long getCurrentOpenTxn() {
        long ret  = 0;
        for (Long l : currOpenTxns.values()) {
            ret += l;
        }
        return ret;
    }

    @Override
    public long getTotalReconnect() {
        long ret = 0;
        for (Long l : numSuccReconn.values()) {
            ret += l;
        }
        return ret;
    }

    @Override
    public long getNumTokenRefreshed() {
        return totalTokenRefreshed.get();
    }

    void setTotalConsumedOps(long ops) {
        totalConsumedOps.getAndSet(ops);
    }

    void setReqStartVLSN(RepGroupId shard, long vlsn)
        throws IllegalArgumentException {

        checkShard(sid, shards, shard);

        reqStartVLSN.put(shard, vlsn);
    }

    void setAckStartVLSN(RepGroupId shard, long vlsn)
        throws IllegalArgumentException {

        checkShard(sid, shards, shard);

        ackedStartVLSN.put(shard, vlsn);
    }

    synchronized void updateShardStat(RepGroupId shard,
                                      ReplicationStreamConsumerStat stat)
        throws IllegalArgumentException {

        checkShard(sid, shards, shard);

        /* shard stat */
        lastCommitVLSN.put(shard, stat.getLastCommitVLSN());
        lastAbortVLSN.put(shard, stat.getLastAbortVLSN());
        lastMsgTime.put(shard, stat.getLastMsgTimeMs());
        currOpenTxns.put(shard, stat.getOpenTxns());
        numSuccReconn.put(shard, stat.getNumSuccReconn());

        /* accumulated stat */
        totalCommitTxns.addAndGet(stat.getCommitTxns());
        totalAbortTxns.addAndGet(stat.getAbortTxns());
        totalCommitOps.addAndGet(stat.getCommitOps());
        totalAbortOps.addAndGet(stat.getAbortOps());
        totalTokenRefreshed.addAndGet(stat.getNumTokenRefreshed());
    }

    private static <K, V> Map<K, V> copy(Map<K, V> stat) {
        return new HashMap<>(stat);
    }

    private static void checkShard(NoSQLSubscriberId sid,
                                   Set<RepGroupId> shards,
                                   RepGroupId shard)
        throws IllegalArgumentException {
        if (!shards.contains(shard)) {
            throw new IllegalArgumentException("Subscription stat does not " +
                                               "cover shard=" + shard + ", " +
                                               "subscription id=" + sid + ", " +
                                               "covered shards=" +
                                               Arrays.toString(
                                                   shards.toArray()));
        }
    }

    /**
     * Unit test only
     */
    public String dumpStat() {
        return "Subscriber=" + sid + "\n" +
               "requested start VLSN=" + getReqStartVLSN() + "\n" +
               "acknowledged start VLSN=" + getAckStartVLSN() + "\n" +
               "last committed txn vlsn=" + getLastCommitVLSN() + "\n" +
               "last aborted txn vlsn=" + getLastAbortVLSN() + "\n" +
               "# committed ops=" + getTotalCommitOps() + "\n" +
               "# aborted ops=" + getTotalAbortOps() + "\n" +
               "# committed txns=" + getTotalCommitTxns() + "\n" +
               "# aborted txns=" + getTotalAbortTxns() + "\n" +
               "# token refreshed=" + getNumTokenRefreshed();
    }
}
