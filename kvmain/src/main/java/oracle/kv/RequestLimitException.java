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

import oracle.kv.impl.topo.RepNodeId;

/**
 * Thrown when a request cannot be processed because it would exceed the
 * maximum number of active requests for a node as configured via
 * {@link KVStoreConfig#setRequestLimit}.
 *
 * This exception may simply indicate that the request limits are too strict
 * and need to be relaxed to more correctly reflect application behavior. It
 * may also indicate that there is a network issue with the communications path
 * to the node or that the node itself is having problems and needs attention.
 * In most circumstances, the KVS request dispatcher itself will handle node
 * failures automatically, so this exception should be pretty rare.
 * <p>
 * When encountering this exception it's best for the application to abandon
 * the request, and free up the thread, thus containing the failure and
 * allowing for more graceful service degradation. Freeing up the thread makes
 * it available for requests that can be processed by other healthy nodes.
 *
 * @see RequestLimitConfig
 * @hidden.see {@link #writeFastExternal FastExternalizable format}
 */
public class RequestLimitException extends FaultException {

    private static final long serialVersionUID = 1L;

    /**
     * For internal use only.
     * @hidden
     */
    private RequestLimitException(String msg, boolean isRemote) {
        super(msg, isRemote);
    }

    /**
     * Creates an instance from the input stream.
     *
     * @hidden For internal use only
     */
    public RequestLimitException(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
    }

    /**
     * Writes the fields of this object to the output stream.
     *
     * <p>Format:
     * <ol>
     * <li> ({@link FaultException}) {@code super}
     * </ol>
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
    }

    /**
     * For internal use only.
     * @hidden
     */
    public static RequestLimitException create(RequestLimitConfig config,
                                               RepNodeId rnId,
                                               int activeRequests,
                                               int nodeRequests,
                                               boolean isRemote) {
        assert config != null;

        String msg = "Node limit exceeded at:" + rnId +
            " Active requests at node:" + nodeRequests +
            " Total active requests:" + activeRequests +
            " Request limit configuration:" + config.toString();

        return new RequestLimitException(msg, isRemote);
    }
}
