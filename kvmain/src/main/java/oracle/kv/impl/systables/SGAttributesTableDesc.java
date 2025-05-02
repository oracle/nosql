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
 * Descriptor for the SGAttributesTable system table.
 */
public class SGAttributesTableDesc extends SysTableDescriptor {

    public static final String TABLE_NAME =
            makeSystemTableName("SGAttributesTable");

    /* All fields within this table */
    public static final String COL_NAME_SGTYPE = "SGType";
    public static final String COL_NAME_SGNAME = "SGName";
    public static final String COL_NAME_DATATYPE = "Datatype";
    public static final String COL_NAME_STARTWITH = "StartWith";
    public static final String COL_NAME_INCREMENTBY = "IncrementBy";
    public static final String COL_NAME_MINVALUE = "MinValue";
    public static final String COL_NAME_MAXVALUE = "MaxValue";
    public static final String COL_NAME_CACHE = "Cache";
    public static final String COL_NAME_CYCLE = "Cycle";
    public static final String COL_NAME_VERSION = "SGAttrVersion";
    public static final String COL_NAME_CURRENTVALUE = "CurrentValue";

    /** Schema version of the table */
    private static final int TABLE_VERSION = 1;

    /**
     * Restore SG attributes in a snapshot load: attributes will be used to
     * generate next identity value.
     */
    private static final boolean needToRestore = true;

    SGAttributesTableDesc() { }

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
        builder.addString(COL_NAME_SGTYPE);
        builder.addString(COL_NAME_SGNAME);

        builder.addString(COL_NAME_DATATYPE);
        builder.addNumber(COL_NAME_STARTWITH);
        builder.addLong(COL_NAME_INCREMENTBY);
        builder.addNumber(COL_NAME_MINVALUE);
        builder.addNumber(COL_NAME_MAXVALUE);
        builder.addLong(COL_NAME_CACHE);
        builder.addBoolean(COL_NAME_CYCLE);
        builder.addLong(COL_NAME_VERSION);
        builder.addNumber(COL_NAME_CURRENTVALUE);

        builder.primaryKey(COL_NAME_SGTYPE,
                           COL_NAME_SGNAME);
        builder.shardKey(COL_NAME_SGTYPE,
                         COL_NAME_SGNAME);
    }

    /* Types of the sequences */
    public static enum SGType {
        /* IDENTITY columns */
        INTERNAL,
        /* independent sequence generators - not yet implemented */
        EXTERNAL
    }

    @Override
    public boolean isRestore() {
        return needToRestore;
    }
}
