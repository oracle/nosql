/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

import oracle.kv.TestBase;

import org.junit.Test;

/**
 * Tests for the metadata object
 */
public class MetadataTest extends TestBase {

    private static final TableImpl USER_TABLE =
        TableBuilder.createTableBuilder("User")
        .addInteger("id")
        .addString("firstName")
        .addString("lastName")
        .addInteger("age")
        .primaryKey("id")
        .shardKey("id")
        .buildTable();

    @Test
    public void testChangeListPruning() {
        final TableMetadata md = new TableMetadata(true);

        for (int i = 0; i < 100; i++) {
            addTable(md, USER_TABLE, "_" + i);
        }
        assertEquals(100, md.getChangeHistorySize());
        assertEquals(1, md.getFirstChangeSeqNum());

        /* Check edge case */
        md.pruneChanges(0, Integer.MAX_VALUE);
        assertEquals(100, md.getChangeHistorySize());
        assertEquals(1, md.getFirstChangeSeqNum());

        /* Attempt to prune, but the seq # is the first change */
        md.pruneChanges(1, 50);
        assertEquals(100, md.getChangeHistorySize());
        assertEquals(1, md.getFirstChangeSeqNum());

        /* Prune down to 50 */
        md.pruneChanges(Integer.MAX_VALUE, 50);
        assertEquals(50, md.getChangeHistorySize());
        assertEquals(51, md.getFirstChangeSeqNum());

        /* Attempt to prune down to 10, but the seq # part way in */
        md.pruneChanges(75, 10);
        assertEquals(26, md.getChangeHistorySize());
        assertEquals(75, md.getFirstChangeSeqNum());

        /* Prune the rest */
        md.pruneChanges(Integer.MAX_VALUE, 0);
        assertEquals(0, md.getChangeHistorySize());
        /* An empty change list should return -1 */
        assertEquals(-1, md.getFirstChangeSeqNum());
    }

    /**
     * Tests the per-table sequence number.
     */
    @Test
    public void testTableSeqNum() {
        final TableMetadata md = new TableMetadata(true);

        TableImpl table = addTable(md, USER_TABLE, "");
        assertEquals(table.getSequenceNumber(), md.getSequenceNumber());

        md.setLimits(table, TableLimits.READ_ONLY);
        assertEquals(table.getSequenceNumber(), md.getSequenceNumber());

        md.evolveTable(table, table.numTableVersions(),
                       table.getFieldMap(), null, "DESCRIPTION",
                       false, null, null);
        assertEquals(table.getSequenceNumber(), md.getSequenceNumber());

        TableImpl childTable =
            TableBuilder.createTableBuilder("Child", "Child table", table)
            .addInteger("income")
            .primaryKey("income")
            .buildTable();

        childTable = addTable(md, childTable, "");
        assertEquals(childTable.getSequenceNumber(), md.getSequenceNumber());
        assertEquals(childTable.getSequenceNumber(), table.getSequenceNumber());

        md.addIndex(null, "INDEX",
                    table.getFullName(),
                    new ArrayList<>(Arrays.asList("firstName")),
                    null, true, false, null);
        assertEquals(table.getSequenceNumber(), md.getSequenceNumber());

        md.updateIndexStatus(null,
                             "INDEX",
                             table.getFullName(),
                             IndexImpl.IndexStatus.READY);
        assertEquals(table.getSequenceNumber(), md.getSequenceNumber());

        md.dropIndex(null, "INDEX", table.getFullNamespaceName());
        assertEquals(table.getSequenceNumber(), md.getSequenceNumber());

        md.dropTable(null, childTable.getFullName(), true);
        assertEquals(table.getSequenceNumber(), md.getSequenceNumber());

        md.dropTable(null, childTable.getFullName(), false);
        assertEquals(table.getSequenceNumber(), md.getSequenceNumber());

        md.dropTable(null, table.getName(), true);
        assertEquals(table.getSequenceNumber(), md.getSequenceNumber());
    }

    private TableImpl addTable(TableMetadata md, TableImpl table, String v) {
        return md.addTable(table.getInternalNamespace(),
                           table.getName() + v,
                           table.getParentName(),
                           table.getPrimaryKey(),
                           null, // primaryKeySizes
                           table.getShardKey(),
                           table.getFieldMap(),
                           null, // TTL
                           null, // limits
                           false, 0,
                           null, null);
    }
}
