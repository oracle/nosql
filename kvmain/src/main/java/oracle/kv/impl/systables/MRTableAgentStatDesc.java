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
import oracle.kv.table.TimeToLive;

/**
* Descriptor for the multi-region table agent statistics system table.
*/
public class MRTableAgentStatDesc extends SysTableDescriptor {

    public static final String TABLE_NAME =
        makeSystemTableName("MRTableAgentStat");

    /* All fields within this table */
    public static final String COL_NAME_AGENT_ID = "agentID";
    public static final String COL_NAME_TABLE_ID = "tableID";
    public static final String COL_NAME_TIMESTAMP = "timestamp";
    public static final String COL_NAME_STATISTICS = "statistics";
    public static final TimeToLive DEFAULT_TTL = TimeToLive.ofDays(3);

    /** Schema version of the table */
    private static final int TABLE_VERSION = 1;

    /**
     * Don't restore MR table agent stats in a snapshot load: agent stats
     * information will be collected and refreshed by target store.
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
        builder.addString(COL_NAME_AGENT_ID);
        builder.addLong(COL_NAME_TABLE_ID);
        builder.addLong(COL_NAME_TIMESTAMP);
        builder.addJson(COL_NAME_STATISTICS, "agent statistics");
        builder.shardKey(COL_NAME_TABLE_ID);
        builder.primaryKey(COL_NAME_TABLE_ID, COL_NAME_AGENT_ID,
                           COL_NAME_TIMESTAMP);
        builder.setDefaultTTL(DEFAULT_TTL);
    }

    @Override
    public boolean isRestore() {
        return needToRestore;
    }

}
