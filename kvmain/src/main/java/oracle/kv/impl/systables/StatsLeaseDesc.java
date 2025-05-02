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
 * Base descriptor for the stats lease system tables.
 */
abstract public class StatsLeaseDesc extends SysTableDescriptor {

    /* Columns associated with lease management. */
    public static final String COL_NAME_LEASE_RN = "leasingRN";
    public static final String COL_NAME_LEASE_DATE = "leaseExpiry";
    public static final String COL_NAME_LAST_UPDATE = "lastUpdated";

    @Override
    protected void buildTable(TableBuilder builder) {
        builder.addString(COL_NAME_LEASE_RN);
        builder.addString(COL_NAME_LEASE_DATE);
        builder.addString(COL_NAME_LAST_UPDATE);
    }
}
