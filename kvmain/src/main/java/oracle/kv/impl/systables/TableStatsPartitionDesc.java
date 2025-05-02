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
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.table.FieldValue;
import oracle.kv.table.Row;

/**
 * Descriptor for the table stats system table.
 */
public class TableStatsPartitionDesc extends SysTableDescriptor {

    /**
     * Test hook to test in upgrade, the table size with tombstone column may
     * return null
     */
    public static volatile TestHook<Row> upgradeTestHook = null;

    public static final String TABLE_NAME =
            makeSystemTableName("TableStatsPartition");

    /* All fields within table TableStatsPartition */
    public static final String COL_NAME_TABLE_NAME = "tableName";
    public static final String COL_NAME_PARTITION_ID = "partitionId";
    public static final String COL_NAME_SHARD_ID = "shardId";
    public static final String COL_NAME_COUNT = "count";
    public static final String COL_NAME_AVG_KEY_SIZE = "avgKeySize";

    /* Field for table size without tombstone added in version 2 */
    public static final String COL_NAME_TABLE_SIZE = "tableSize";
    private static final int TABLE_SIZE_VERSION = 2;

    /* Field for table size with tombstone added in version 3 */
    /**
     * Please note after introducing the new stat column, the original stat
     * {@link #COL_NAME_TABLE_SIZE} does not change its semantics and still has
     * use cases. For example, when a user exports a table, the user may need to
     * know the size of the table (live data only, not including tombstones).
     * In addition, the other existing columns in this stat table (e.g.,
     * {@link #COL_NAME_COUNT}, {@link #COL_NAME_AVG_KEY_SIZE}) would have
     * their semantics unchanged, and they are for live data only.
     */
    public static final String COL_NAME_TABLE_SIZE_WITH_TOMBSTONES =
        "tableSizeWithTombstones";
    private static final int TABLE_SIZE_WITH_TOMBSTONE_VERSION = 3;


    /** Schema version of the table */
    private static final int TABLE_VERSION = 3;

    /**
     * Don't restore table stats in a snapshot load: the stats are only
     * meaningful for the store in which they were collected. This information
     * will be collected and refreshed by target store.
     */
    private static final boolean needToRestore = false;

    TableStatsPartitionDesc() { }

    /**
     * Gets the table size with tombstones from the stats row. In the cases
     * where the stat is not available, using the table size instead.
     * @param row a stats row from the system table
     * @return table size with tombstones
     */
    public static long getSizeValue(Row row) {
        /* sanity check */
        final String tb = row.getTable().getFullNamespaceName();
        if (!tb.equals(TABLE_NAME)) {
            throw new IllegalArgumentException(
                "Row must be from the stats table=" + TABLE_NAME +
                ", while it is from=" + tb);
        }

        assert TestHookExecute.doHookIfSet(upgradeTestHook, row);
        FieldValue sz = row.get(COL_NAME_TABLE_SIZE_WITH_TOMBSTONES);
        if (!sz.isNull()) {
            /* value is available */
            return sz.asLong().get();
        }

        /*
         * In some cases like upgrade from a version predating the column
         * COL_NAME_TABLE_SIZE_WITH_TOMBSTONES, its value could be null
         * before key scan completes, in such cases, we use the table size
         * without tombstone as the starting value. Once the key scan
         * completes, the column would have a valid value.
         */
        sz = row.get(COL_NAME_TABLE_SIZE);
        return sz.asLong().get();
    }

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
        builder.addInteger(COL_NAME_PARTITION_ID);
        builder.addInteger(COL_NAME_SHARD_ID);
        builder.addLong(COL_NAME_COUNT);
        builder.addInteger(COL_NAME_AVG_KEY_SIZE);

        /* Table size added in v2 */
        builder.addLong(COL_NAME_TABLE_SIZE);

        /* Table with tombstone size added in v3 */
        builder.addLong(COL_NAME_TABLE_SIZE_WITH_TOMBSTONES);

        /* primary key and shard key */
        builder.primaryKey(COL_NAME_TABLE_NAME, COL_NAME_PARTITION_ID);
        builder.shardKey(COL_NAME_TABLE_NAME, COL_NAME_PARTITION_ID);
    }

    @Override
    protected int evolveTable(TableEvolver ev, int schemaVersion) {
        if (schemaVersion < TABLE_SIZE_VERSION) {
            ev.addLong(COL_NAME_TABLE_SIZE);
            schemaVersion = TABLE_SIZE_VERSION;
        }
        if (schemaVersion < TABLE_SIZE_WITH_TOMBSTONE_VERSION) {
            ev.addLong(COL_NAME_TABLE_SIZE_WITH_TOMBSTONES);
            schemaVersion = TABLE_SIZE_WITH_TOMBSTONE_VERSION;
        }
        return schemaVersion;
    }

    @Override
    public boolean isRestore() {
        return needToRestore;
    }
}
