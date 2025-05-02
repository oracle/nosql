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

import oracle.kv.impl.api.table.TableImpl;

/**
 * Objects represents a stream change request that adds a subscribed table to
 * the running stream.
 */
public final class StreamChangeSubscribeReq extends StreamChangeReq {

    private static final long serialVersionUID = 1L;

    /*
     * number of primary key components associated with just this
     * table, not parent tables. see {@link NoSQLStreamFeederFilter.MatchKey}
     */
    private final int keyCount;

    /*
     * number of key components to skip to find the table ID
     * see {@link NoSQLStreamFeederFilter.MatchKey}
     */
    private final int skipCount;

    /**
     * Request to subscribe a table, with all info to update the stream filter
     */
    StreamChangeSubscribeReq(String reqId,
                             String tableName,
                             String rootTableId,
                             String tableId,
                             int keyCount,
                             int skipCount,
                             Logger logger) {

        super(reqId, Type.ADD, tableName, rootTableId, tableId, logger);
        this.keyCount = keyCount;
        this.skipCount = skipCount;
    }

    @Override
    StreamChangeSubscribeReq asSubscribeReq() {
        return this;
    }

    @Override
    StreamChangeUnsubscribeReq asUnsubscribeReq() {
        throw new IllegalArgumentException("Not a Unsubscribe request");
    }

    @Override
    public String toString() {
        return "[reqId=" + getReqId() +
               " subscribe table=" + getTableName() +
               ", root table idStr=" + getRootTableId() +
               "(id=" + TableImpl.createIdFromIdStr(getRootTableId()) +
               "), table idStr=" + getTableId() +
               "(id=" + TableImpl.createIdFromIdStr(getTableId()) +
               "), key count=" + keyCount +
               ", skip count=" + skipCount +
               "]";
    }

    int getKeyCount() {
        return keyCount;
    }

    int getSkipCount() {
        return skipCount;
    }
}