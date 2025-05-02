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
import oracle.kv.impl.api.table.TableEvolver;

/**
 * Descriptor for the index stats system table.
 */
public class TableStatsIndexDesc extends SysTableDescriptor {

    public static final String TABLE_NAME =
            makeSystemTableName("TableStatsIndex");

    /* All fields within this table */
    public static final String COL_NAME_TABLE_NAME = "tableName";
    public static final String COL_NAME_INDEX_NAME = "indexName";
    public static final String COL_NAME_SHARD_ID = "shardId";
    public static final String COL_NAME_COUNT = "count";
    public static final String COL_NAME_AVG_KEY_SIZE = "avgKeySize";

    /* Field for index size added in version 2 */
    public static final String COL_NAME_INDEX_SIZE = "indexSize";
    private static final int INDEX_SIZE_VERSION = 2;

    /** Schema version of the table */
    private static final int TABLE_VERSION = 2;

    /**
     * Don't restore table stats in a snapshot load: the stats are only
     * meaningful for the store in which they were collected. This information
     * will be collected and refreshed by target store.
     */
    private static final boolean needToRestore = false;

    TableStatsIndexDesc() { }

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
        builder.addLong(COL_NAME_COUNT);
        builder.addInteger(COL_NAME_AVG_KEY_SIZE);
        /* Index size added in v2 */
        builder.addLong(COL_NAME_INDEX_SIZE);
        builder.primaryKey(COL_NAME_TABLE_NAME,
                           COL_NAME_INDEX_NAME,
                           COL_NAME_SHARD_ID);
        builder.shardKey(COL_NAME_TABLE_NAME,
                         COL_NAME_INDEX_NAME,
                         COL_NAME_SHARD_ID);
    }

    @Override
    protected int evolveTable(TableEvolver ev, int schemaVersion) {
        if (schemaVersion < INDEX_SIZE_VERSION) {
            ev.addLong(COL_NAME_INDEX_SIZE);
            schemaVersion = INDEX_SIZE_VERSION;
        }
        return schemaVersion;
    }

    @Override
    public boolean isRestore() {
        return needToRestore;
    }
}
