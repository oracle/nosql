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

package oracle.kv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The exception is thrown when an expected piece of metadata is not found.
 * This probably indicates an inconsistency between the client's metadata and
 * that on the server side.  This can happen if, for example, a table is
 * dropped then re-created using the same name.  In this case a client's
 * {@link oracle.kv.table.Table} instance may still represent the old table and it must be
 * refreshed using {@link oracle.kv.table.TableAPI#getTable}.
 *
 * @since 3.4
 * @hidden.see {@link #writeFastExternal FastExternalizable format}
 */
public class MetadataNotFoundException extends FastExternalizableException {

    private static final long serialVersionUID = 1L;

    /* The sequence number associated with TableMetadata */
    private final int tableMetadataSeqNum;

    /**
     * For internal use only.
     * @hidden
     */
    public MetadataNotFoundException(String msg, int tableMetadataSeqNum) {
        super(msg +
              ", Table ID metadata sequence number: " + tableMetadataSeqNum);
        this.tableMetadataSeqNum = tableMetadataSeqNum;
    }

    /**
     * Creates an instance from the input stream.
     *
     * @hidden For internal use only
     */
    public MetadataNotFoundException(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        tableMetadataSeqNum = in.readInt();
    }

    /**
     * Writes the fields of this object to the output stream.  Format:
     * <ol>
     * <li> ({@link FastExternalizableException}) {@code super}
     * </ol>
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeInt(tableMetadataSeqNum);
    }

    /**
     * For internal use only
     * @hidden
     *
     * Returns the TableMetadata sequence number of a RepNode.
     */
    public int getTableMetadataSeqNum() {
        return tableMetadataSeqNum;
    }
}
