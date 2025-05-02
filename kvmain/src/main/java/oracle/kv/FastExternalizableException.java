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

import static oracle.kv.impl.util.CommonLoggerUtils.appendCurrentStack;
import static oracle.kv.impl.util.SerializationUtil.readString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.SerializeExceptionUtil;

/**
 * A common exception base class to support internal serialization facilities.
 *
 * @hidden.see {@link #writeFastExternal FastExternalizable format}
 * @hidden.see {@link SerializeExceptionUtil}
 */
public abstract class FastExternalizableException extends RuntimeException
        implements FastExternalizable {

    private static final long serialVersionUID = 1;

    /**
     * @hidden For internal use only
     */
    protected FastExternalizableException(String msg) {
        super(msg);
    }

    /**
     * @hidden For internal use only
     */
    protected FastExternalizableException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * @hidden For internal use only
     */
    protected FastExternalizableException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates an instance from the input stream.
     *
     * @hidden For internal use only
     */
    protected FastExternalizableException(DataInput in, short serialVersion)
        throws IOException {

        super(readString(in, serialVersion),
              (in.readBoolean() ?
               SerializeExceptionUtil.readException(in, serialVersion) :
               null));
        final StackTraceElement[] stackTrace =
            SerializeExceptionUtil.readStackTrace(in, serialVersion);
        if (stackTrace != null) {
            setStackTrace(stackTrace);
            appendCurrentStack(this);
        }
    }

    /**
     * Writes the fields of this object to the output stream.  Format:
     * <ol>
     * <li> ({@link SerializationUtil#writeString String}) {@link #getMessage()
     *      message}
     * <li> ({@link DataOutput#writeBoolean boolean}) <i>whether cause is
     *      present</i>
     * <li> <i>[Optional]</i> ({@link SerializeExceptionUtil#writeException
     *      exception}) {@link #getCause cause}
     * </ol>
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        writeFastExternal(out, serialVersion, getMessage());
    }

    /**
     * Writes the fields of this object to the output stream, but specifies the
     * message.
     *
     * @hidden For internal use only
     */
    protected void writeFastExternal(DataOutput out,
                                     short serialVersion,
                                     String message)
        throws IOException {

        SerializeExceptionUtil.writeExceptionContents(
            out, serialVersion, message, this);
    }
}
