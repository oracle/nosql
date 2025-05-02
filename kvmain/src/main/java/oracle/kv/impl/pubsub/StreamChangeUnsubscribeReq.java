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

import java.util.logging.Logger;

/**
 * Objects represents a stream change request that removes a subscribed table
 * from the running stream.
 */
public final class StreamChangeUnsubscribeReq extends StreamChangeReq {

    private static final long serialVersionUID = 1L;

    /** Request to unsubscribe a table */
    StreamChangeUnsubscribeReq(String reqId,
                               String tableName,
                               String rootTableId,
                               String tableId,
                               Logger logger) {
        super(reqId, Type.REMOVE, tableName, rootTableId, tableId, logger);
    }

    @Override
    StreamChangeSubscribeReq asSubscribeReq() {
        throw new IllegalArgumentException("Not a subscribe request");
    }

    @Override
    StreamChangeUnsubscribeReq asUnsubscribeReq() {
        return this;
    }

    @Override
    public String toString() {
        return "[reqId=" + getReqId() +
               ", unsubscribe table=" + getTableName() +
               ", root table id=" + getRootTableId() +
               ", table id=" + getTableId() + "]";
    }
}
