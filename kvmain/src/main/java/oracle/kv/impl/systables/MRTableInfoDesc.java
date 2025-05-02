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
 * Descriptor for the MRT info table.
 */
public class MRTableInfoDesc extends SysTableDescriptor {

    public static final String TABLE_NAME = makeSystemTableName("MRTableInfo");

    public static final String COL_TABLE_NAME = "tableName";
    public static final String COL_REGION_NAME = "regionName";
    public static final String COL_INFO = "info";
    private static final String COL_INFO_JSON_DESC = "JSON doc of table info";

    /** Schema version of the table */
    private static final int TABLE_VERSION = 1;

    /**
     * Restore MR Table info in a snapshot load: user may want to restore
     * table as the way it was, connected to other regions. This table record
     * state of table initialization. Without this info, the agent of the new
     * kvstore would have to re-initialize all MR tables from scratch in table
     * metadata.
     */
    private static final boolean needToRestore = true;

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
        builder.addString(COL_TABLE_NAME);
        builder.addString(COL_REGION_NAME);
        builder.addJson(COL_INFO, COL_INFO_JSON_DESC);
        builder.shardKey(COL_TABLE_NAME);
        builder.primaryKey(COL_TABLE_NAME, COL_REGION_NAME);
    }

    @Override
    public boolean isRestore() {
        return needToRestore;
    }
}
