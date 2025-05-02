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
package oracle.kv.impl.security;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.FastExternalizableException;

/**
 * Exception thrown when session data is inaccessible. The session data may
 * be non-existent or it may exist but simply cannot be accessed due to an
 * operational failure.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class SessionAccessException extends FastExternalizableException {

    private static final long serialVersionUID = 1L;

    /*
     * When false, this indicates that the error applies to the credentials
     * provided by the authentication context that was current when an operation
     * was performed, and should be considered when the dispatch mechanism
     * decides whether to retry the operation. When true, this indicates that
     * the exception refers to authentication credentials passed explicitly in
     * operation arguments, and the exception is intended to signal a return
     * result directly to the caller. A value of true is normally specified
     * when the exception is thrown, and a new exception is thrown when caught
     * in the context of authentication credentials checking.
     */
    private final boolean isReturnSignal;

    public SessionAccessException(String message) {
        super(message);
        this.isReturnSignal = true;
    }

    public SessionAccessException(Throwable cause,
                                  boolean isReturnSignal) {
        super(cause);
        this.isReturnSignal = isReturnSignal;
    }

    /**
     * Creates an instance from the input stream.
     */
    public SessionAccessException(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        isReturnSignal = in.readBoolean();
    }

    /**
     * Writes the fields of this object to the output stream.  Format:
     * <ol>
     * <li> ({@link FastExternalizableException}) {@code super}
     * <li> ({@link DataOutput#writeBoolean boolean}) {@link #getIsReturnSignal
     *      isReturnSignal}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeBoolean(isReturnSignal);
    }

    /**
     * Indicates whether the SessionAccessException applies to the session
     * associated with the caller of a method or to a session argument to the
     * method call.
     * @return true if the exception applies to a method argument and false
     * otherwise.
     */
    public boolean getIsReturnSignal() {
        return isReturnSignal;
    }
}
