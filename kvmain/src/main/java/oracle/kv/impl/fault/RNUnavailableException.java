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

import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_14;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.FastExternalizableException;

/**
 * Thrown when an RN could not be located to satisfy a Request. The exception
 * typically results in the operation being retried transparently at the
 * KVClient, until the request succeeds or the request times out.
 * <p>
 * These retries are typically done when a request needs a Master and an
 * election is in progress. If the election can be concluded before the request
 * timeouts, then the retry is largely transparent to the client.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class RNUnavailableException extends FastExternalizableException {

    private static final long serialVersionUID = 1L;

    /* See commnet for Request.needsMaster field */
    private final boolean needsMaster;

    public RNUnavailableException(String message) {
        super(message);
        needsMaster = false;
    }

    public RNUnavailableException(String message, boolean needsMaster) {
        super(message);
        this.needsMaster = needsMaster;
    }

    /**
     * Creates an instance from the input stream.
     */
    public RNUnavailableException(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        if (serialVersion >= QUERY_VERSION_14) {
            needsMaster = in.readBoolean();
        } else {
            needsMaster = false;
        }
    }

    /**
     * Writes the fields of this object to the output stream.  Format:
     * <ol>
     * <li> ({@link FastExternalizableException}) {@code super}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        if (serialVersion >= QUERY_VERSION_14) {
            out.writeBoolean(needsMaster);
        }
    }

    public boolean needsMaster() {
        return needsMaster;
    }
}
