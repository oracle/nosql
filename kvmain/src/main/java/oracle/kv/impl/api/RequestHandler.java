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

package oracle.kv.impl.api;

import java.rmi.RemoteException;

import oracle.kv.FaultException;
import oracle.kv.impl.util.registry.VersionedRemote;

/**
 * Handles requests that have been directed to this RN by some
 * {@link RequestDispatcher}. It's ultimately responsible for the
 * execution of a request that originated at a KV Client.
 */
public interface RequestHandler extends VersionedRemote {

    /**
     * Executes the request. It identifies the database that owns the keys
     * associated with the request and executes the request.
     *
     * <p>
     * The local request handler contains the retry logic for all failures that
     * can be handled locally. For example, a retry resulting from an
     * environment handle that was invalidated due to a hard recovery in the
     * midst of an operation. Exceptional situations that cannot be
     * handled internally are propagated back to the client.
     * <p>
     * It may not be possible to initiate execution of the request because the
     * request was misdirected and the RN does not own the key, or because the
     * request is for an update and the RN is not a master. In these cases,
     * it internally redirects the request to a more appropriate RN and returns
     * the response or exception as appropriate.
     *
     * @param request the request to be executed
     *
     * @return the response from the execution of the request
     */
    public Response execute(Request request)
        throws FaultException, RemoteException;
}
