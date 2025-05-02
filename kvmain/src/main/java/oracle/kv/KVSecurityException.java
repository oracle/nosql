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
 * The common abstract base class of KVStore security-related exceptions. These
 * exceptions are produced only when accessing a secure KVStore instance.
 *
 * @since 3.0
 * @hidden.see {@link #writeFastExternal FastExternalizable format}
 */
public abstract class KVSecurityException extends FastExternalizableException {

    private static final long serialVersionUID = 1L;

    /**
     * For internal use only.
     * @hidden
     */
    protected KVSecurityException(String msg) {
        super(msg);
    }

    /**
     * For internal use only.
     * @hidden
     */
    protected KVSecurityException(Throwable cause) {
        super(cause);
    }

    /**
     * For internal use only.
     * @hidden
     */
    protected KVSecurityException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Creates an instance from the input stream.
     *
     * @hidden For internal use only
     */
    protected KVSecurityException(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
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
    }
}
