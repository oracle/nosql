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
 * Descriptor for the topology history system table.
 *
 * <p>
 * Currently this table is only used for query under elasticity: The query only
 * needs to maintain a base topology sequence number instead of the topology
 * itself. When the topology is not available, this history table can be used
 * to obtain it with the specified sequence number.
 *
 * <p>
 * TODO: This table, however, can be used to replace the existing topology
 * gossip mechanism in the future. The replacement design, however, has not
 * been figured out yet. Complications and trade-offs include but not limit to:
 * <ul>
 * <li>Whether this mechanism will replace the gossip mechanism fully, or
 * should they co-exist?</li>
 * <li>To obtain the latest topology, should we maintain the latest row or just
 * scan the table.</li>
 * <li>How to do garbage collection, currently each row has a TTL. We want to
 * keep the latest row always available, though.</li>
 * </ul>
 */
public class TopologyHistoryDesc extends SysTableDescriptor {

    public static final String TABLE_NAME =
        makeSystemTableName("TopologyHistory");

    /* All fields with table TopologyHistory. */

    /**
     * A constant shard key field so that all rows are on the same shard. Such
     * design will make scanning this table more efficient for, e.g., obtaining
     * the latest topology.
     */
    public static final String COL_SHARD_KEY = "shardKey";
    /** The sequence number. */
    public static final String COL_NAME_TOPOLOGY_SEQUENCE_NUMBER =
        "sequenceNumber";
    /** The serialized bytes. */
    public static final String COL_NAME_SERIALIZED_TOPOLOGY =
        "serializedTopology";

    /** Schema version of the table. */
    private static final int TABLE_VERSION = 1;

    /**
     * Don't restore topology history in snapshot load: topology history is
     * specific to the store it came from.
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
        builder.addString(COL_SHARD_KEY);
        builder.addInteger(COL_NAME_TOPOLOGY_SEQUENCE_NUMBER);
        builder.addBinary(COL_NAME_SERIALIZED_TOPOLOGY);
        builder.primaryKey(
            COL_SHARD_KEY,
            COL_NAME_TOPOLOGY_SEQUENCE_NUMBER);
        builder.shardKey(COL_SHARD_KEY);
    }

    @Override
    public boolean isRestore() {
        return needToRestore;
    }
}
