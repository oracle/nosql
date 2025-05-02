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

import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.ReadFastExternal;

/**
 * The base class for all table changes. A sequence of changes can be
 * applied to a TableMetadata instance, via {@link TableMetadata#apply}
 * to make it more current.
 * <p>
 * Each TableChange represents a logical change entry in a logical log with
 * changes being applied in sequence via {@link TableMetadata#apply} to modify
 * the table metadata and bring it up to date.
 */
public abstract class TableChange implements FastExternalizable, Serializable {
    private static final long serialVersionUID = 1L;

    private static List<ChangeTypeFinder> changeTypeFinders =
        new ArrayList<>();
    static {
        addChangeTypeFinder(StandardChangeType::valueOf);
    }

    private final int sequenceNumber;

    /** Identifies the type of a TableChange subclass. */
    interface ChangeType {

        /** Returns the integer value associated with the type. */
        int getIntValue();

        /** Reads the associated table change. */
        TableChange readTableChange(DataInput in, short serialVersion)
            throws IOException;

        /** Reads the integer value of a TableChange type. */
        static int readIntValue(DataInput in,
                                @SuppressWarnings("unused")
                                short serialVersion)
            throws IOException
        {
            return readPackedInt(in);
        }

        /** Writes the integer value of this type. */
        default void writeIntValue(DataOutput out,
                                   @SuppressWarnings("unused") short sv)
            throws IOException
        {
            writePackedInt(out, getIntValue());
        }
    }

    /** Finds a ChangeType from the associated integer value. */
    interface ChangeTypeFinder {

        /**
         * Returns the ChangeType associated with the specified value, or null
         * if none is found.
         */
        ChangeType getChangeType(int intValue);
    }

    /** The change types for standard table changes. */
    enum StandardChangeType implements ChangeType {
        ADD_INDEX(0, AddIndex::new),
        ADD_NAMESPACE_CHANGE(1, AddNamespaceChange::new),
        ADD_REGION(2, AddRegion::new),
        ADD_TABLE(3, AddTable::new),
        DROP_INDEX(4, DropIndex::new),
        DROP_TABLE(5, DropTable::new),
        EVOLVE_TABLE(6, EvolveTable::new),
        REMOVE_NAMESPACE_CHANGE(7, RemoveNamespaceChange::new),
        REMOVE_REGION(8, RemoveRegion::new),
        TABLE_LIMIT(9, TableLimit::new),
        UPDATE_INDEX_STATUS(10, UpdateIndexStatus::new);

        private static final ChangeType[] VALUES = values();
        private final ReadFastExternal<TableChange> reader;

        private StandardChangeType(final int ordinal,
                                   final ReadFastExternal<TableChange> reader)
        {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
            this.reader = reader;
        }

        static ChangeType valueOf(int ordinal) {
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                return null;
            }
        }

        @Override
        public int getIntValue() {
            return ordinal();
        }

        @Override
        public TableChange readTableChange(DataInput in, short serialVersion)
            throws IOException
        {
            return reader.readFastExternal(in, serialVersion);
        }
    }

    protected TableChange(int sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    protected TableChange(DataInput in, @SuppressWarnings("unused") short sv)
        throws IOException
    {
        sequenceNumber = readPackedInt(in);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException
    {
        writePackedInt(out, sequenceNumber);
    }

    /** Deserializes an instance of a subclass of ChangeType. */
    public static TableChange readTableChange(DataInput in,
                                              short serialVersion)
        throws IOException
    {
        final ChangeType changeType =
            findChangeType(ChangeType.readIntValue(in, serialVersion));
        return changeType.readTableChange(in, serialVersion);
    }

    private static ChangeType findChangeType(int intValue) {
        for (final ChangeTypeFinder finder : changeTypeFinders) {
            final ChangeType changeType = finder.getChangeType(intValue);
            if (changeType != null) {
                return changeType;
            }
        }
        throw new IllegalArgumentException("Unknown ChangeType: " + intValue);
    }

    /**
     * Serializes an instance so that the correct subclass can be created when
     * it is deserialized.
     */
    public void writeTableChange(DataOutput out, short serialVersion)
        throws IOException
    {
        getChangeType().writeIntValue(out, serialVersion);
        writeFastExternal(out, serialVersion);
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    /**
     * Applies the change to the specified metadata. If the change modifies
     * a table, the modified table impl is returned, otherwise null is returned.
     *
     * @param md
     * @return the modified table or null
     */
    abstract TableImpl apply(TableMetadata md);

    /** Returns the change type of this instance. */
    abstract ChangeType getChangeType();

    /**
     * Registers a ChangeTypeFinder.
     */
    public static void addChangeTypeFinder(ChangeTypeFinder finder) {
        changeTypeFinders.add(finder);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + sequenceNumber + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof TableChange)) {
            return false;
        }
        final TableChange other = (TableChange) obj;
        return (sequenceNumber == other.sequenceNumber);
    }

    @Override
    public int hashCode() {
        return sequenceNumber;
    }
}
