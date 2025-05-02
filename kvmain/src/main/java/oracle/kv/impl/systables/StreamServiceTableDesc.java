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

package oracle.kv.impl.systables;

import oracle.kv.impl.api.table.TableBuilder;

/**
 * Base class defining the fields common to the streaming service tables.
 *
 *  Field       Type    Description
 *  -----       ----    -----------
 *  serviceType Integer Type of stream service (e.g. multi-region, point in time
 *                      recovery, etc.)
 *  requestId   Integer`Unique identifier of the request.
 *  timestamp   Long    Time value for debug purposes. Likely the time that the
 *                      message was created but is implementation dependent.
 *  payload     byte[]  (Optional) Message specific information.
 *
 * The primary key is: serviceType.requestId
 * The shard key is the serviceType
 */
public abstract class StreamServiceTableDesc extends SysTableDescriptor {

    /* All fields common to the stream service tables */
    public static final String COL_SERVICE_TYPE = "serviceType";
    public static final String COL_REQUEST_ID = "requestId";
    public static final String COL_TIMESTAMP = "timestamp";
    public static final String COL_PAYLOAD = "payload";

    @Override
    protected void buildTable(TableBuilder builder) {
        builder.addInteger(COL_SERVICE_TYPE);
        builder.addInteger(COL_REQUEST_ID);
        builder.addLong(COL_TIMESTAMP);
        builder.addBinary(COL_PAYLOAD, null, true, null);
        builder.primaryKey(COL_SERVICE_TYPE, COL_REQUEST_ID);
        builder.shardKey(COL_SERVICE_TYPE);
    }
}
