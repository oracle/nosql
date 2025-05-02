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

import com.sleepycat.je.rep.stream.FeederFilterChangeResult;

/**
 * Object represents the shard result of subscription change.
 */
class ChangeStreamShardResult {

    /** for request that failed before request id is generated */
    private static final String NULL_REQ_ID = "NULL";

    /* name of table */
    private final String tableName;

    /* id of the shard */
    private final int shardId;

    /* result from feeder */
    private final FeederFilterChangeResult result;

    /* local cause if failed to apply the change */
    private final Throwable cause;

    /* get a result from feeder */
    ChangeStreamShardResult(String tableName, int shardId,
                            FeederFilterChangeResult result) {
        if (result == null) {
            throw new IllegalArgumentException("Change result cannot be null");
        }
        this.tableName = tableName;
        this.shardId = shardId;
        this.result = result;
        cause = null;
    }

    /* cannot get result from feeder with cause */
    ChangeStreamShardResult(String tableName, int shardId, Throwable cause) {
        if (cause == null) {
            throw new IllegalArgumentException("Cause cannot be null");
        }
        this.tableName = tableName;
        this.shardId = shardId;
        this.cause = cause;
        result = null;
    }

    int getShardId() {
        return shardId;
    }

    Throwable getLocalCause() {
        return cause;
    }

    String getTableName() {
        return tableName;
    }

    FeederFilterChangeResult getResult() {
        return result;
    }

    @Override
    public String toString() {
        final String reqId = (result == null) ? NULL_REQ_ID : result.getReqId();
        return "[reqId=" + reqId + "] shard=" + shardId +
               ", table=" + tableName +
               ", filter change result=" + result + ", cause=" + cause;
    }
}

