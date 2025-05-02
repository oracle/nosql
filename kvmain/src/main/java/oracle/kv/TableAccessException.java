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
 * Thrown when an operation to a table cannot be completed due to limits on
 * write and possibly read access. When this exception is thrown, the caller
 * can assume that the associated operation was not performed and that there
 * were no side effects.
 * 
 * @hidden.see {@link #writeFastExternal FastExternalizable format}
 */
public class TableAccessException extends ResourceLimitException {

    private static final long serialVersionUID = 1L;

    private final boolean readAllowed;

    /**
     * Constructs an instance of <code>TableAccessException</code> with
     * the specified table name and detail message. If readAllowed is true
     * read operations to the table are permitted.
     *
     * @param tableName the table name
     * @param readAllowed true if the table is read-only
     * @param msg the detail message
     * 
     * @hidden For internal use only
     */
    public TableAccessException(String tableName,
                                boolean readAllowed,
                                String msg) {
        super(tableName, msg);
        assert tableName != null;
        this.readAllowed = readAllowed;
    }

    /**
     * Creates an instance from the input stream.
     *
     * @hidden For internal use only
     */
    public TableAccessException(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        readAllowed = in.readBoolean();
    }

    /**
     * Returns true is reads are allowed.
     *
     * @return true is reads are allowed
     * 
     * @hidden For internal use only
     */
    public boolean getReadAllowed() {
        return readAllowed;
    }

    /**
     * Writes the fields of this object to the output stream.  Format:
     * <ol>
     * <li> ({@link ResourceLimitException}) {@code super}
     * <li> ({@link DataOutput#writeBoolean writeBoolean})
     *      {@link #getReadAllowed readAllowed}
     * </ol>
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {
        super.writeFastExternal(out, serialVersion);
        out.writeBoolean(readAllowed);
    }
}
