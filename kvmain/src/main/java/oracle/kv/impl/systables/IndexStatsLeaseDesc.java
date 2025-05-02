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
 * Descriptor for the index stats lease system table.
 */
public class IndexStatsLeaseDesc extends StatsLeaseDesc {

    public static final String TABLE_NAME =
            makeSystemTableName("IndexStatsLease");

    /* The index-specific columns in the lease table  */
    public static final String COL_NAME_TABLE_NAME = "tableName";
    public static final String COL_NAME_INDEX_NAME = "indexName";
    public static final String COL_NAME_SHARD_ID = "shardId";

    /** Schema version of the table */
    private static final int TABLE_VERSION = 1;

    /**
     * Don't restore index stats in a snapshot load: the stats are only
     * meaningful for the store in which they were collected. This information
     * will be collected and refreshed by target store.
     */
    private static final boolean needToRestore = false;

    IndexStatsLeaseDesc() { }

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
        builder.addString(COL_NAME_TABLE_NAME);
        builder.addString(COL_NAME_INDEX_NAME);
        builder.addInteger(COL_NAME_SHARD_ID);
        super.buildTable(builder);
        builder.primaryKey(COL_NAME_TABLE_NAME,
                           COL_NAME_INDEX_NAME,
                           COL_NAME_SHARD_ID);
        builder.shardKey(COL_NAME_TABLE_NAME,
                         COL_NAME_INDEX_NAME,
                         COL_NAME_SHARD_ID);
    }

    @Override
    public boolean isRestore() {
        return needToRestore;
    }
}
