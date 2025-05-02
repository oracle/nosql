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
 * This exception is thrown when a secured operation is attempted and the
 * client is not currently authenticated. This can occur either if the client
 * did not supply login credentials either directly or by specifying a login
 * file, or it can occur if login credentials were specified, but the login
 * session has expired, requiring that the client reauthenticate itself.
 * The client application should reauthenticate before retrying the operation.
 *
 * @since 3.0
 * @hidden.see {@link #writeFastExternal FastExternalizable format}
 */
public class AuthenticationRequiredException extends KVSecurityException {

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

    /**
     * For internal use only.
     * @hidden
     */
    public AuthenticationRequiredException(String msg,
                                           boolean isReturnSignal) {
        super(msg);
        this.isReturnSignal = isReturnSignal;
    }

    /**
     * For internal use only.
     * @hidden
     */
    public AuthenticationRequiredException(Throwable cause,
                                           boolean isReturnSignal) {
        super(cause);
        this.isReturnSignal = isReturnSignal;
    }

    /**
     * Creates an instance from the input stream.
     *
     * @hidden For internal use only
     */
    public AuthenticationRequiredException(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        isReturnSignal = in.readBoolean();
    }

    /**
     * Writes the fields of this object to the output stream.  Format:
     * <ol>
     * <li> ({@link KVSecurityException}) {@code super}
     * <li> ({@link DataOutput#writeBoolean boolean}) {@link #getIsReturnSignal
     *      isReturnSignal}
     * </ol>
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeBoolean(isReturnSignal);
    }

    /**
     * For internal use only.
     * @hidden
     */
    public boolean getIsReturnSignal() {
        return isReturnSignal;
    }
}
