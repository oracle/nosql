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

package oracle.kv.stats;

import java.util.Map;

import oracle.kv.impl.topo.RepGroupId;

/**
 * Object represents a set of statistics of a single subscription.
 */
public interface SubscriptionMetrics {

    /**
     * Returns the requested start vlsn for each subscribed shard
     * 
     * @return  the requested start vlsn for each subscribed shard
     */
    Map<RepGroupId, Long> getReqStartVLSN();

    /**
     * Returns the acknowledged start vlsn from NoSQL DB for each subscribed
     * shard
     *
     * @return the acknowledged start vlsn from NoSQL DB for each subscribed
     * shard
     */
    Map<RepGroupId, Long> getAckStartVLSN();

    /**
     * Returns the last committed vlsn for each subscribed shard
     *
     * @return the last committed vlsn for each subscribed shard
     */
    Map<RepGroupId, Long> getLastCommitVLSN();

    /**
     * Returns the last aborted vlsn for each subscribed shard
     *
     * @return the last aborted vlsn for each subscribed shard
     */
    Map<RepGroupId, Long> getLastAbortVLSN();

    /**
     * Returns the timestamp of last message from each subscribed shard
     *
     * @return the timestamp of last message from each subscribed shard
     */
    Map<RepGroupId, Long> getLastMsgTimeStamp();

    /**
     * Returns the total number of stream operations that have been consumed
     * by subscriber
     *
     * @return the total number of stream operations that have been consumed
     * by subscriber
     */
    long getTotalConsumedOps();

    /**
     * Returns the total committed transactions in the subscription
     *
     * @return the total committed transactions in the subscription
     */
    long getTotalCommitTxns();

    /**
     * Returns the total aborted transactions in the subscription
     *
     * @return the total aborted transactions in the subscription
     */
    long getTotalAbortTxns();

    /**
     * Returns the total number of operations that were part of committed
     * transactions in the subscription. It is a sum of operations of all
     * committed transactions.
     *
     * @return the total committed operations in the subscription
     */
    long getTotalCommitOps();

    /**
     * Returns the total number of operations that were part of aborted
     * transactions in the subscription. It is a sum of operations of all
     * aborted transactions.
     *
     * @return the total aborted operations in the subscription
     */
    long getTotalAbortOps();

    /**
     * Returns the current total number of open transactions in publisher
     * that have not yet committed or aborted.
     *
     * @return the current total open transactions in the subscription
     */
    long getCurrentOpenTxn();

    /**
     * Returns the total number of reconnections to shards in subscription.
     * The reconnection happens at level of shard when subscriber is unable to
     * stream from a node in that shard, e.g., the master node is done and a
     * new master is elected, and subscriber will reconnect to the new master
     * of the shard.
     *
     * @return the total number of reconnections
     */
    long getTotalReconnect();

    /**
     * Gets the number of times that token has been refreshed with the secure
     * store. Refreshing a token includes both renewing an existing token and
     * re-authenticating to obtain a new token if existing token cannot be
     * renewed. For non-secure store, it always returns 0.
     *
     * @return the number of times that token has been refreshed
     */
    long getNumTokenRefreshed();
}
