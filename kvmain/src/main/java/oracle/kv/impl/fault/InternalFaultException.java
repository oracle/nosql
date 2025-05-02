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

package oracle.kv.impl.fault;

import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import oracle.kv.FastExternalizableException;
import oracle.kv.KVVersion;
import oracle.kv.impl.util.SerializationUtil;

/**
 * An exception wrapper used to indicate a fault encountered in a server side
 * process while servicing an internal, non-data operation request.
 * Application-specific subclasses of this class are typically used by the
 * ProcessFaultHandler to throw an exception when processing such a request.
 *
 * <p>
 * Given the distributed nature of the KVS, the client may not have access to
 * the class associated with the "cause" object created by the server, or the
 * class definition may represent a different and potentially incompatible
 * version. This wrapper class ensures that the stack trace and textual
 * information is preserved and communicated to the client.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public abstract class InternalFaultException
        extends FastExternalizableException {

    private static final long serialVersionUID = 1L;
    private final String faultClassName;
    private final String originalStackTrace;

    public InternalFaultException(Throwable cause) {
        super(cause.getClass().getName() + ":" + cause.getMessage() + " (" +
              KVVersion.CURRENT_VERSION.getNumericVersionString() + ")");
        /* Preserve the stack trace and the exception class name. */
        final StringWriter sw = new StringWriter(500);
        cause.printStackTrace(new PrintWriter(sw));
        originalStackTrace = "Fault StackTrace: " + sw.toString();

        faultClassName = cause.getClass().getName();
    }

    /**
     * Creates an instance from the input stream.
     */
    public InternalFaultException(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        originalStackTrace = readString(in, serialVersion);
        faultClassName = readString(in, serialVersion);
    }

    /**
     * Writes the fields of this object to the output stream.  Format:
     * <ol>
     * <li> ({@link FastExternalizableException}) {@code super}
     * <li> ({@link SerializationUtil#writeString String}) {@code
     *      originalStackTrace}
     * <li> ({@link SerializationUtil#writeString String}) {@link
     *      #getFaultClassName faultClassName}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        writeString(out, serialVersion, originalStackTrace);
        writeString(out, serialVersion, faultClassName);
    }

    /* The name of the original exception class, often used for testing. */
    public String getFaultClassName() {
        return faultClassName;
    }

    @Override
    public String toString() {
        return super.toString() + "\n" + originalStackTrace;
    }
}
