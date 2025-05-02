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

import java.util.concurrent.TimeoutException;

/**
 * Exception raised when publisher does not hear from source shard for a
 * given period of time. This exception is only used in calls to
 * {@link NoSQLSubscriber#onWarn(Throwable)}, which, unlike
 * {@link NoSQLSubscriber#onError(Throwable)}, intends to raise a
 * warning to subscriber without cancelling ongoing subscription.
 */
public class ShardTimeoutException extends TimeoutException {

    private static final long serialVersionUID = 1L;

    /* last time we heard from that given shard */
    private final long lastMsgTime;
    /* the max silence time in ms to trigger the exception */
    private final long timeoutMs;
    /* id of shard that we have not heard from a while */
    private final int shardId;

    /**
     * @hidden
     *
     * Builds a timeout exception instance
     *
     * @param shardId      id of shard
     * @param lastMsgTime  last time we heard from that given shard
     * @param timeoutMs        the max silence time to trigger the exception
     * @param msg          error message
     */
    public ShardTimeoutException(int shardId, long lastMsgTime, long timeoutMs,
                                 String msg) {
        super(msg);
        this.shardId = shardId;
        this.lastMsgTime = lastMsgTime;
        this.timeoutMs = timeoutMs;
    }

    /**
     * Returns the last time we heard from that given shard
     *
     * @return the timestamp of the last message
     */
    public long getLastMsgTime() {
        return lastMsgTime;
    }

    /**
     * Returns the time out of the period the given shard has not been heard.
     *
     * @return  time out limit
     */
    public long getTimeoutMs() {
        return timeoutMs;
    }

    /**
     * Returns Id of the shard that has not been heard from long enough
     *
     * @return  Id of the shard
     */
    public int getShardId() {
        return shardId;
    }
}
