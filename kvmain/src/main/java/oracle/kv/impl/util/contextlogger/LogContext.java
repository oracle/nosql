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

package oracle.kv.impl.util.contextlogger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;

/**
 * A LogContext establishes a logging context for a SkLogger.log call.
 * The logging context contains information about the original API request
 * that resulted in the execution of the code that produced a log record.
 * This allows the log records that are associated with the same request to
 * be grouped together during postprocessing of the log.
 *
 * The LogContext also establishes the current environmental log level for the
 * logging call, overriding the logger's current log level.
 * @see #writeFastExternal FastExternalizable format
 *
 */
public class LogContext extends oracle.nosql.common.contextlogger.LogContext
    implements FastExternalizable {

    private static final long serialVersionUID = 1;

    public LogContext() {
        super();
    }

    public LogContext(oracle.nosql.common.contextlogger.LogContext lc) {
        super(lc.getId(),
              lc.getEntry(),
              lc.getOrigin(),
              lc.getLogLevel(),
              lc.isLogged());
    }

    /* for FastExternalizable */
    public LogContext(DataInput in, short serialVersion)
        throws IOException {

        id = SerializationUtil.readString(in, serialVersion);
        entry = SerializationUtil.readString(in, serialVersion);
        origin = SerializationUtil.readString(in, serialVersion);
        logLevel = in.readInt();
        logged = in.readBoolean();
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link SerializationUtil#writeString String}) {@link #getId id}
     * <li> ({@link SerializationUtil#writeString String}) {@link #getEntry
     *      entry}
     * <li> ({@link SerializationUtil#writeString String}) {@link #getOrigin
     *      origin}
     * <li> ({@link DataOutput#writeInt int}) {@link #getLogLevel logLevel}
     * <li> ({@link DataOutput#writeBoolean boolean}) {@link #isLogged logged}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        SerializationUtil.writeString(out, serialVersion, id);
        SerializationUtil.writeString(out, serialVersion, entry);
        SerializationUtil.writeString(out, serialVersion, origin);
        out.writeInt(logLevel);
        out.writeBoolean(logged);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof LogContext)) {
            return false;
        }
        final LogContext other = (LogContext) obj;
        return Objects.equals(id, other.id) &&
            Objects.equals(entry, other.entry) &&
            Objects.equals(origin, other.origin) &&
            (logLevel == other.logLevel) &&
            (logged == other.logged);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, entry, origin, logLevel, logged);
    }
}
