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

package oracle.kv.impl.api.table;

import static oracle.kv.impl.util.SerializationUtil.readNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullSequenceLength;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.table.Table;

/**
 * Encapsulates target tables for a table operation that involves multiple
 * tables.  All table operations require a target table.  They optionally
 * affect child and/or ancestor tables as well.  Additionally, there is state
 * that indicates that for certain operations the target table itself should
 * not be returned.  This last piece of information is not yet used and is
 * preparing for a future enhancement.  At this time target tables may not be
 * excluded from results.
 *
 * Internally the target table and child tables requested are kept in the
 * same array, with the target being the first entry.  This simplifies the
 * server side which wants that array intact.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class TargetTables implements FastExternalizable {

    /*
     * Includes the target table id and optional child ids.
     */
    private final long[] targetAndChildIds;

    /*
     * Ancestor table ids, may be empty.
     */
    private final long[] ancestorTableIds;

    /*
     * Include the target table in results, or not.
     * For now (R3) this is always true.
     */
    private final boolean includeTarget;

    /**
     * Creates a TargetTables object on the client side
     *
     * @param targetTable the target of the operation, which is the table
     * from which the operation's key was created
     *
     * @param childTables a list of child tables to include in the operation,
     * or null
     *
     * @param ancestorTables a list of ancestor tables to include in the
     * operation, or null
     *
     * @throws IllegalArgumentException if there is no target table
     */
    public TargetTables(Table targetTable,
                        List<Table> childTables,
                        List<Table> ancestorTables) {
        if (targetTable == null) {
            throw new IllegalArgumentException
                ("Missing target table");
        }

        /* target table plus child tables */
        int arraySize = childTables != null ? childTables.size() + 1 : 1;

        targetAndChildIds = new long[arraySize];
        targetAndChildIds[0] = ((TableImpl) targetTable).getId();
        if (childTables != null) {
            int i = 1;
            for (Table table : childTables) {
                targetAndChildIds[i++] = ((TableImpl)table).getId();
            }
        }

        ancestorTableIds = makeIdArray(ancestorTables);

        /*
         * This is not currently negotiable.
         */
        includeTarget = true;
    }

    /**
     * Constructor used at the servers by query (see TableScannerFactory)
     *
     * @param tables An array of all the relevant tables. The tables in the
     * array are ordered as ancestors first, followed by the target table,
     * followed by the descendants.
     *
     * @param numAncestors The number of ancestors
     */
    public TargetTables(TableImpl[] tables, int numAncestors) {

        includeTarget = true;

         ancestorTableIds = new long[numAncestors];

         for (int i = 0; i < numAncestors; ++i) {
             ancestorTableIds[i] = tables[i].getId();
         }

         targetAndChildIds = new long[tables.length - numAncestors];
         targetAndChildIds[0] = tables[numAncestors].getId();

         for (int i = numAncestors + 1; i < tables.length; ++ i) {
             targetAndChildIds[i - numAncestors] = tables[i].getId();
         }
    }

    /**
     * Internal constructor used only by MultiDeleteTables when called from
     * a RepNode that is deleting table data on table removal.
     */
    public TargetTables(long tableId) {
        targetAndChildIds = new long[1];
        targetAndChildIds[0] = tableId;
        ancestorTableIds = new long[0];
        includeTarget = true;
    }

    /**
     * Creates a TargetTables instance on the server side from a message
     */
    public TargetTables(DataInput in,
                        @SuppressWarnings("unused") short serialVersion)
        throws IOException {

        int len = readNonNullSequenceLength(in);
        targetAndChildIds = new long[len];
        for (int i = 0; i < len; i++) {
            targetAndChildIds[i] = in.readLong();
        }
        len = readNonNullSequenceLength(in);
        ancestorTableIds = new long[len];
        for (int i = 0; i < len; i++) {
            ancestorTableIds[i] = in.readLong();
        }

        includeTarget = in.readBoolean();
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link SerializationUtil#writeNonNullSequenceLength}) <i>number of
     *      targetAndChildIds</i>
     * <li> ({@link DataOutput#writeLong long}{@code []}) {@link
     *      #getTargetAndChildIds targetAndChildIds}
     * <li> ({@link SerializationUtil#writeNonNullSequenceLength}) <i>number of
     *      ancestorTableIds</i>
     * <li> ({@link DataOutput#writeLong long}{@code []}) {@link
     *      #getAncestorTableIds ancestorTableIds}
     * <li> ({@link DataOutput#writeBoolean boolean}) {@link #getIncludeTarget
     *      includeTarget}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        writeNonNullSequenceLength(out, targetAndChildIds.length);

        for (long l : targetAndChildIds) {
            out.writeLong(l);
        }
        writeNonNullSequenceLength(out, ancestorTableIds.length);

        for (long l : ancestorTableIds) {
            out.writeLong(l);
        }
        out.writeBoolean(includeTarget);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof TargetTables)) {
            return false;
        }
        final TargetTables other = (TargetTables) obj;
        return Arrays.equals(targetAndChildIds, other.targetAndChildIds) &&
            Arrays.equals(ancestorTableIds, other.ancestorTableIds) &&
            (includeTarget == other.includeTarget);
    }

    @Override
    public int hashCode() {
        return Objects.hash(targetAndChildIds, ancestorTableIds,
                            includeTarget);
    }

    /**
     * Returns the target table id.
     */
    public long getTargetTableId() {
        return targetAndChildIds[0];
    }

    /**
     * Returns the target table id in the array with child targets.
     */
    public long[] getTargetAndChildIds() {
        return targetAndChildIds;
    }

    /**
     * Returns the ancestor tables array. This is never null, but it may be
     * empty.
     */
    public long[] getAncestorTableIds() {
        return ancestorTableIds;
    }

    /**
     * Returns true if the target table is to be included in the results
     */
    public boolean getIncludeTarget() {
        return includeTarget;
    }

    /**
     * Returns true if there are ancestor tables.
     */
    public boolean hasAncestorTables() {
        return ancestorTableIds.length > 0;
    }

    /**
     * Returns true if there are child tables.
     */
    public boolean hasChildTables() {
        return targetAndChildIds.length > 1;
    }

    private long[] makeIdArray(List<Table> tables) {
        if (tables == null) {
            return new long[0];
        }
        final long[] ids = new long[tables.size()];
        int i = 0;
        for (Table table : tables) {
            ids[i++] = ((TableImpl)table).getId();
        }
        return ids;
    }
}
