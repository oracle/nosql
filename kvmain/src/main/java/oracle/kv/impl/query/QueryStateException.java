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

package oracle.kv.impl.query;

import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import oracle.kv.impl.fault.WrappedClientException;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;

/**
 * An internal class that encapsulates illegal states in the query engine.
 * The query engine operates inside clients and servers and cannot safely
 * throw IllegalStateException as that can crash the server. This exception is
 * used to indicate problems in the engine that are most likely query engine
 * bugs but are not otherwise fatal to the system.
 *
 * On the server side this exception is caught and logged, then passed to
 * the client as a WrappedClientException, where it will be caught as
 * a simple IllegalStateException, and logged there as well, if possible.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class QueryStateException extends IllegalStateException
        implements FastExternalizable {

    private static final long serialVersionUID = 1L;
    private final String stackTrace;

    public QueryStateException(String message) {
        super("Unexpected state in query engine:\n" + message);

        final StringWriter sw = new StringWriter(500);
        new RuntimeException().printStackTrace(new PrintWriter(sw));
        stackTrace = sw.toString();
    }

    public QueryStateException(String message, Throwable cause) {
        super("Unexpected state in query engine:\n" + message);

        final StringWriter sw = new StringWriter(500);
        new RuntimeException(cause).printStackTrace(new PrintWriter(sw));
        stackTrace = sw.toString();
    }

    /**
     * Wrap this exception so it can be passed to the client.
     */
    public void throwClientException() {
        throw new WrappedClientException(this);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(1000);
        sb.append(super.toString());
        sb.append("\nStack trace: ");
        sb.append(stackTrace);
        return sb.toString();
    }

    /**
     * Creates an instance from the input stream.
     */
    public QueryStateException(DataInput in, short serialVersion)
        throws IOException {

        super(readString(in, serialVersion));
        stackTrace = readString(in, serialVersion);
    }

    /**
     * Writes the fields of this object to the output stream.  Format:
     * <ol>
     * <li> ({@link SerializationUtil#writeString String}) {@link #getMessage()
     *      message}
     * <li> ({@link SerializationUtil#writeString string}) <i>stack trace<i/>
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        writeString(out, serialVersion, getMessage());
        writeString(out, serialVersion, stackTrace);
    }
}
