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

import oracle.kv.FastExternalizableException;
import oracle.kv.impl.util.SerializationUtil;

/**
 * Exception to indicate that the table version of a serialized row is higher
 * than that available on the client or server.  It must be caught by internal
 * code to trigger a fetch of new metadata from the client.  In the case of the
 * server is needs to trigger an exception that will cause the client to retry
 * the operation, giving the server a chance to update its metadata.
 *
 * @hidden.see {@link #writeFastExternal FastExternalizable format}
 */
public class TableVersionException extends FastExternalizableException {

    private final int requiredVersion;

    private static final long serialVersionUID = 1L;

    /**
     * @hidden
     * Builds a TableVersionException instance
     *
     * @param requiredVersion required table version
     */
    public TableVersionException(int requiredVersion) {
        super("Requires table version " + requiredVersion);
        this.requiredVersion = requiredVersion;
    }

    /**
     * Returns the higher table version that is required.
     *
     * @return the table version
     */
    public int getRequiredVersion() {
        return requiredVersion;
    }

    /**
     * Creates an instance from the input stream.
     *
     * @hidden For internal use only
     */
    public TableVersionException(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        requiredVersion = readPackedInt(in);
    }

    /**
     * Writes the fields of this object to the output stream.  Format:
     * <ol>
     * <li> ({@link FastExternalizableException}) {@code super}
     * <li> ({@link SerializationUtil#writePackedInt packed int}) {@link
     *      #getRequiredVersion requiredVersion}
     * </ol>
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        writePackedInt(out, requiredVersion);
    }
}
