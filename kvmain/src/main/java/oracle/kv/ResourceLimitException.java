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

import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.util.SerializationUtil;

/**
 * A common exception base class for exceptions related to resource usage. In
 * general resource limits are associated with a table. In that case the table
 * name returned by getTableName will be non-null. When a subclass of this
 * exception is thrown, the caller can assume that the associated operation
 * was not performed and that there were no side effects.
 * 
 * @hidden.see {@link #writeFastExternal FastExternalizable format}
 */
public abstract class ResourceLimitException extends FaultException {

    private static final long serialVersionUID = 1L;

    private final String tableName;

    /**
     * Constructs an instance of <code>ResourceLimitException</code> with the
     * specified table name and detail message.
     *
     * @param tableName the table name or null
     * @param msg the detail message
     */
    protected ResourceLimitException(String tableName, String msg) {
        super(msg, true /* isRemote */);
        this.tableName = tableName;
    }

    /**
     * Creates an instance from the input stream.
     *
     * @hidden For internal use only
     */
    public ResourceLimitException(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        tableName = readString(in, serialVersion);
    }

    /**
     * Gets the name of the table who's resource limit was exceeded. If the
     * resource limit is not associated with a table null is returned.
     *
     * @return a table name or null
     * 
     * @hidden For internal use only
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Writes the fields of this object to the output stream.  Format:
     * <ol>
     * <li> ({@link FaultException}) {@code super}
     * <li> ({@link SerializationUtil#writeString writeString})
     *      {@link #getTableName tableName}
     * </ol>
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {
        super.writeFastExternal(out, serialVersion);
        writeString(out, serialVersion, tableName);
    }
}
