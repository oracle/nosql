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
 * Table for table metadata.
 *
 * This table contains a row for each table hierarchy, namespace, and region.
 *
 * The primary key is the type and a key string. The shard key is a constant
 * in order to keep all of the metadata rows in the same partition to avoid
 * fan-out when scanning and to support transactions between rows.
 *
 * The index is on sequence number to support incremental updates.
 *
 * For more details, see {@link oracle.kv.impl.api.table.TableSysTableUtil}.
 */
public class TableMetadataDesc extends SysTableDescriptor {

    /**
     *  Table metadata table name
     */
    public static final String TABLE_NAME =
            makeSystemTableName("TableMetadata");

    /**
     *  The key of the index on the sequence number
     */
    public static final String SEQ_INDEX_NAME = "SeqIndex";

    /**
     * The special table ID. The store must be upgrade to
     * TABLE_MD_IN_STORE_VERSION before this table can be created.
     */
    public static final long METADATA_TABLE_ID = Long.MAX_VALUE;

    /* -- Fields -- */

    /**
     * Constant column to ensure that all rows are in the same partition. This
     * is needed to support transactions across rows.
     */
    public static final String COL_NAME_CONSTANT = "Constant";

    public static String ROW_CONSTANT = "A";

    /**
     * The type of object (table, namespace,...). See below for
     * defined types.
     */
    public static final String COL_NAME_TYPE = "Type";

    /**
     * The key of the object. See below on what is used for the key
     * for each type of object.
     */
    public static final String COL_NAME_KEY = "Key";

    /**
     *  Sequence number of the object. The sequence number column is
     *  indexed by SEQ_INDEX_NAME.
     */
    public static final String COL_NAME_SEQ_NUM = "SeqNum";

    /**
     * Deleted flag. If true the item the row represents has been
     * removed from the metadata. The deleted flag is to support
     * incremental update. Rows that have are deleted may be garbage
     * collected when they are not being retained for incremental updates.
     */
    public static final String COL_NAME_DELETED = "Deleted";

    /**
     * JSON description of the object or null
     * */
    public static final String COL_NAME_DESCRIPTION = "Description";

    /**
     *  Java serialized form of the object
     */
    public static final String COL_NAME_DATA = "Data";

    /* -- Row types -- */

    /* The values below are found in COL_NAME_TYPE */

    /**
     * Table row. There is one row per table hierarchy.
     *
     * Row Fields
     *
     * Type:    TABLE_TYPE
     * Key:     Full namespace table name (String returned by
     *          Table.getFullNamespaceName()) converted to lower case.
     * SeqNum:  The sequence number of the table (Value returned
     *          by TableImpl.getSequenceNumber()). The sequence number will
     *          be unique between table rows.
     * Description: The JSON representation of the table hierarchy (String
     *          returned by TableImpl.toJsonString() on the top level table)
     *          or null if the table is dropped.
     * Data:    If not a deleted row it is the Java serialized top level
     *          TableImpl of the hierarchy. If deleted the data is the
     *          Java serialized (Long) table ID.
     */
    public static final String TABLE_TYPE = "Table";

    /**
     * Namespace row. One row per namespace.
     *
     *  Row Fields
     *
     * Type:    NAMESPACE_TYPE
     * Key:     Namespace name (String returned by NamespaceImpl.getNamespace())
     *          converted to lower case.
     * SeqNum:  The table metadata sequence number at the time the
     *          row was written. The sequence number may not be unique
     *          between other rows of any type.
     * Description: The JSON representation of the namespace (String
     *          returned by NamespaceImpl.toJsonString()) or null if the
     *          namespace is removed.
     * Data:    If not a deleted row it is the Java serialized NamespaceImpl.
     *          If deleted the data is null.
     */
    public static final String NAMESPACE_TYPE = "Namespace";

    /**
     * Region row. One row per known region. Note that regions are
     * never removed so the deleted flag is never true.
     *
     * Row Fields
     *
     * Type:    REGION_TYPE
     * Key:     Region ID in string form (String returned by
     *          Integer.toString(Region.getName()))
     * SeqNum:  The general metadata sequence number at the time the
     *          row was written. The sequence number may not be unique
     *          between other rows of any type.
     * Description: The JSON representation of the region (String
     *          returned by Region.toJsonString()).
     * Data:    The Java serialized Region.
     */
    public static final String REGION_TYPE = "Region";

    /**
     *  Other non-table metadata information. The subtype of the row is
     *  determined by the key (COL_NAME_KEY). See below for defined other
     *  types.
     */
    public static final String OTHER_TYPE = "Other";

    /* -- Other type rows -- */

    /**
     * Other rows will have COL_NAME_TYPE set to OTHER_TYPE. The key
     * (COL_NAME_KEY) will be one of the following values.
     */

    /**
     * Garbage collection sequence number. Lowest sequence number for which
     * deleted table markers have been retained.
     *
     * Row Fields
     *
     * Type:    OTHER_TYPE
     * Key:     GC_SEQ_NUM
     * SeqNum:  A sequence number. The sequence number may not be unique
     *          between other rows of any type.
     * Description: The JSON representation of the sequence number.
     * Data:    Null
     *
     * This is the sequence number indicating the high mark
     * for purging deleted markers. Deleted markers with a lower
     * sequence number than this number can be removed. Partial
     * sequence number scans used for incremental metadata updates
     * should not start before this sequence number.
     *
     * <pre>{@literal
     * To purge delete markers:
     * 1) write the GCSN with a sequence number > the current
     *    GCSN and less than the max seq num.
     * 2) scan from 0 to GCSN removing any delete markers.
     *
     * The steps to perform an incremental update from
     * a last known sequence number (LKSN) are:
     * 1) read the GCSN
     * 2) if GCSN > LKSN abort
     * 3) start sequence number scan from LKSN
     * 4) when done, re-read GCSN
     * 5) if GCSN > LKSN abort
     * }</pre>
     *
     * If GCSN is greater than the start of the scan in either
     * step 2 or 5 an incremental update can not be done. A
     * full scan is required.
     */
    public static final String GC_SEQ_NUM = "GCSN";

    /**
     * Schema version of the table
     * <ul>
     * <li>1 - Initial version (23.2)
     * </li>
     */
    private static final int TABLE_VERSION = 1;

    /**
     * Restore table metadata in a snapshot load: RN can access the table
     * metadata directly.
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
        builder.addString(COL_NAME_CONSTANT);
        builder.addString(COL_NAME_TYPE);
        builder.addString(COL_NAME_KEY);
        builder.addInteger(COL_NAME_SEQ_NUM);
        builder.addBoolean(COL_NAME_DELETED, null, null, false);
        builder.addJson(COL_NAME_DESCRIPTION, null);
        builder.addBinary(COL_NAME_DATA, null, true, null);
        builder.primaryKey(COL_NAME_CONSTANT, COL_NAME_TYPE, COL_NAME_KEY);
        builder.shardKey(COL_NAME_CONSTANT);
    }

    @Override
    public IndexDescriptor[] getIndexDescriptors() {
        return new IndexDescriptor[]{new SeqIndexDesc()};
    }

    private static class SeqIndexDesc implements IndexDescriptor {

        @Override
        public String getIndexName() {
            return SEQ_INDEX_NAME;
        }

        @Override
        public String[] getIndexedFields() {
            return new String[]{COL_NAME_SEQ_NUM};
        }

        @Override
        public String getDescription() {
            return "Sequence number index";
        }
    }

    @Override
    public boolean isRestore() {
        return needToRestore;
    }
}
