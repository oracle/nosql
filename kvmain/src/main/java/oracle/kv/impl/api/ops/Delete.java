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

package oracle.kv.impl.api.ops;

import static oracle.kv.impl.util.SerialVersion.CLOUD_MR_TABLE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import oracle.kv.ReturnValueVersion.Choice;
import oracle.kv.impl.api.lob.KVLargeObjectImpl;

/**
 * The delete operation deletes the key/value pair associated with the key.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class Delete extends SingleKeyOperation {

    /**
     * Whether to return previous value/version.
     */
    private final Choice prevValChoice;

    /**
     * Table operations include the table id.  0 means no table.
     */
    private final long tableId;

    /**
     * Whether put tombstone instead of deleting record.
     */
    private final boolean doTombstone;

    /**
     * Constructs a delete operation.
     */
    public Delete(byte[] keyBytes, Choice prevValChoice) {
        this(keyBytes, prevValChoice, 0, false);
    }

    /**
     * Constructs a delete operation with a table id and doTombstone.
     */
    public Delete(byte[] keyBytes,
                  Choice prevValChoice,
                  long tableId,
                  boolean doTombstone) {
        this(OpCode.DELETE, keyBytes, prevValChoice, tableId, doTombstone);
    }

    /**
     * For subclasses, allows passing OpCode.
     */
    Delete(OpCode opCode,
           byte[] keyBytes,
           Choice prevValChoice,
           long tableId,
           boolean doTombstone) {
        super(opCode, keyBytes);
        this.prevValChoice = prevValChoice;
        this.tableId = tableId;
        this.doTombstone = doTombstone;
    }

    /** Constructor to implement deserializedForm */
    Delete(Delete other, short serialVersion) {
        super(other.getOpCode(), other.getKeyBytes());
        prevValChoice = other.prevValChoice;
        tableId = other.tableId;
        if (includeCloudMRTable(serialVersion)) {
            doTombstone = other.doTombstone;
        } else {
            if (other.doTombstone) {
                throw new IllegalStateException("Serial version " +
                    serialVersion + " does not support for external " +
                    "multi-region table, must be " + CLOUD_MR_TABLE +
                    " or greater");
            }
            doTombstone = false;
        }
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    Delete(DataInput in, short serialVersion)
        throws IOException {

        this(OpCode.DELETE, in, serialVersion);
    }

    /**
     * For subclasses, allows passing OpCode.
     */
    Delete(OpCode opCode, DataInput in, short serialVersion)
        throws IOException {

        super(opCode, in, serialVersion);
        prevValChoice = Choice.readFastExternal(in, serialVersion);

        /*
         * Read table id.  If there is no table the value is 0.
         */
        tableId = in.readLong();

        if (includeCloudMRTable(serialVersion)) {
            doTombstone = in.readBoolean();
        } else {
            doTombstone = false;
        }

    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link SingleKeyOperation}) {@code super}
     * <li> ({@link Choice}) {@link #getReturnValueVersionChoice prevValChoice}
     * <li> ({@link DataOutput#writeLong long}) {@link #getTableId tableId}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        prevValChoice.writeFastExternal(out, serialVersion);

        /*
         * Write the table id.  If this is not a table operation the id will be
         * 0.
         */
        out.writeLong(tableId);

        if (includeCloudMRTable(serialVersion)) {
            out.writeBoolean(doTombstone);
        } else {
            if (doTombstone) {
                throw new IllegalStateException("Serial version " +
                    serialVersion + " does not support setting doTombstone, " +
                    "must be " + CLOUD_MR_TABLE + " or greater");
            }
        }
    }

    public Choice getReturnValueVersionChoice() {
        return prevValChoice;
    }

    @Override
    public boolean performsWrite() {
        return true;
    }

    @Override
    public boolean isDelete() {
        return true;
    }

    /**
     * Returns the tableId, which is 0 if this is not a table operation.
     */
    @Override
    public long getTableId() {
        return tableId;
    }

    @Override
    public byte[] checkLOBSuffix(byte[] lobSuffixBytes) {
        return KVLargeObjectImpl.hasLOBSuffix(getKeyBytes(), lobSuffixBytes) ?
               getKeyBytes() :
               null;
    }

    public boolean doTombstone() {
        return doTombstone;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Delete ");
        if (doTombstone) {
            sb.append(" doTombstone=true ");
        }
        if (tableId != 0) {
            sb.append("Table Id ");
            sb.append(tableId);
            sb.append(" ");
        }
        sb.append(super.toString());
        return sb.toString();
    }

    private static boolean includeCloudMRTable(short serialVersion) {
        return serialVersion >= CLOUD_MR_TABLE;
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj) ||
            !(obj instanceof Delete)) {
            return false;
        }
        final Delete other = (Delete) obj;
        return (prevValChoice == other.prevValChoice) &&
            (tableId == other.tableId) &&
            (doTombstone == other.doTombstone);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), prevValChoice, tableId,
                            doTombstone);
    }

    @Override
    public Delete deserializedForm(short serialVersion) {
        return new Delete(this, serialVersion);
    }
}
