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

package oracle.kv.pubsub;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import oracle.kv.FaultException;
import oracle.kv.impl.topo.RepGroupId;

import org.reactivestreams.Subscriber;

/**
 * Exception used to indicate that the logs on at least one shard were
 * insufficient for the purposes of the stream. This exception cannot be
 * handled by retrying to establish a subscription with the same
 * StreamPosition.
 * <p>
 * Note that this exception is not actually thrown but is delivered to the
 * NoSQLSubscriber via the {@link Subscriber#onError} signal.
 */
public class SubscriptionInsufficientLogException extends FaultException {

    private static final long serialVersionUID = 1L;

    /* id of the failed subscriber */
    private final NoSQLSubscriberId subscriberId;

    /* map of requested vlsn for each shard */
    private final Map<RepGroupId, Long> requestedVLSN;

    /* subscribed tables */
    private final Set<String> tables;

    /**
     * @hidden
     *
     * Creates a SubscriptionInsufficientLogException instance
     *
     * @param subscriberId     id of subscriber
     * @param tables           subscribed tables
     * @param msg              error message
     */
    public SubscriptionInsufficientLogException(NoSQLSubscriberId subscriberId,
                                                Set<String> tables,
                                                String msg) {
        super(msg, true);
        this.subscriberId = subscriberId;
        this.tables = tables;
        requestedVLSN = new HashMap<>();
    }

    /**
     * Gets the subscriber ID.
     *
     * @return the subscriber ID
     */
    public NoSQLSubscriberId getSubscriberId() {
        return subscriberId;
    }

    /**
     * Gets all shards with insufficient logs to stream
     *
     * @return shards with insufficient logs to stream
     */
    public Set<RepGroupId> getInsufficientLogShards() {
        return requestedVLSN.keySet();
    }

    /**
     * Gets subscribed tables
     *
     * @return subscribed tables
     */
    public Set<String> getTables() {
        return tables;
    }

    /**
     * Gets requested VLSN to stream for a given shard
     *
     * @param id  id of the given shard
     *
     * @return  requested VLSN to stream
     */
    public long getReqVLSN(RepGroupId id) {
        return requestedVLSN.get(id);
    }

    /**
     * @hidden
     *
     * Adds a shard with insufficient log to service the request of
     * subscription from a given VLSN.
     *
     * @param id        shard id
     * @param reqVLSN   start VLSN requested by subscriber
     */
    public synchronized void addInsufficientLogShard(RepGroupId id,
                                                     long reqVLSN) {

        requestedVLSN.put(id, reqVLSN);
    }
}
