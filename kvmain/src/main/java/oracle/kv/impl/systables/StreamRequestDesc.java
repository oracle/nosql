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
 * Descriptor for the stream service command table. This class defines
 * request specific fields in addition to the base fields defined by
 * StreamServiceTableDesc.
 *
 *  Field       Type    Description
 *  -----       ----    -----------
 *  requestType Integer Request type, service specific.
 */
public class StreamRequestDesc extends StreamServiceTableDesc {

    public static final String TABLE_NAME =
            makeSystemTableName("StreamRequest");

    /* Fields specific to this table */

    /* Request type. The request types are specific to the service type */
    public static final String COL_REQUEST_TYPE = "requestType";

    /* Schema version of the table */
    private static final int TABLE_VERSION = 1;

    /**
     * Don't restore in a snapshot load: agent and admin can post entries
     * again to continue their conversation.
     */
    private static final boolean needToRestore = false;

    @Override
    public String getTableName() {
        return TABLE_NAME;
    }

    @Override
    protected int getCurrentSchemaVersion() {
        return TABLE_VERSION;
    }

    @Override
    protected void buildTable(TableBuilder builder) {
        super.buildTable(builder);
        builder.addInteger(COL_REQUEST_TYPE);
    }

    @Override
    public boolean isRestore() {
        return needToRestore;
    }
}
