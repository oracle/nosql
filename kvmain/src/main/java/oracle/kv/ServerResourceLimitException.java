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
 * Thrown when the server is unable to handle a request because of resource
 * constraints. The caller should retry after a short wait in hopes that server
 * resource constraints will have eased by then. When this exception is thrown,
 * the caller can assume that the associated operation was not performed and
 * that there were no side effects.
 *
 * @since 19.5
 * @hidden.see {@link #writeFastExternal FastExternalizable format}
 */
public class ServerResourceLimitException extends ResourceLimitException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs an instance of this class with the specified detail message.
     *
     * @param msg the detail message
     * @hidden For internal use only
     */
    public ServerResourceLimitException(String msg) {
        super(null, msg);
    }

    /**
     * Creates an instance from the input stream.
     *
     * @hidden For internal use only
     */
    public ServerResourceLimitException(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
    }

    /**
     * Writes the fields of this object to the output stream.  Format:
     * <ol>
     * <li> ({@link ResourceLimitException}) {@code super}
     * </ol>
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
    }
}
