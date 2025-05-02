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
 * Descriptor for a system table that stores the multi-region table
 * initialization checkpoint
 */
public class MRTableInitCkptDesc extends SysTableDescriptor {

    public static final String TABLE_NAME =
        makeSystemTableName("MRTableInitCheckpoint");

    /** Schema version of the table */
    public static final int TABLE_VERSION = 1;

    /** Fields of the table */
    public static final String COL_NAME_AGENT_ID = "agentID";
    public static final String COL_NAME_SOURCE_REGION = "sourceRegion";
    public static final String COL_NAME_TABLE_NAME = "tableName";
    public static final String[] PRIMARY_KEY_FIELDS =
        new String[] {COL_NAME_AGENT_ID, COL_NAME_SOURCE_REGION,
            COL_NAME_TABLE_NAME};
    public static final String[] SHARD_KEY_FIELDS =
        new String[] {COL_NAME_AGENT_ID, COL_NAME_SOURCE_REGION};

    /** a json column of checkpoint */
    public static final String COL_NAME_CHECKPOINT = "checkpoint";
    public static final String COL_CHECKPOINT_DESC = "TableInitCheckpoint";

    /**
     * Restore MR Table checkpoint info in a snapshot load: this table record
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
        builder.addString(COL_NAME_AGENT_ID);
        builder.addString(COL_NAME_TABLE_NAME);
        builder.addString(COL_NAME_SOURCE_REGION);
        builder.addJson(COL_NAME_CHECKPOINT, COL_CHECKPOINT_DESC);
        builder.shardKey(SHARD_KEY_FIELDS);
        builder.primaryKey(PRIMARY_KEY_FIELDS);
    }

    @Override
    public boolean isRestore() {
        return needToRestore;
    }
}
