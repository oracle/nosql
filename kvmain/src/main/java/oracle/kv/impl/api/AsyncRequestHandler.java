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

import java.io.DataInput;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import oracle.kv.impl.async.AsyncVersionedRemote;
import oracle.kv.impl.util.ReadFastExternal;

/**
 * Remote interface for handling requests asynchronously that have been
 * directed to this RN by a {@link RequestDispatcher}.  This class is
 * ultimately responsible for the execution of a request that originated at a
 * KV Client.
 */
public interface AsyncRequestHandler extends AsyncVersionedRemote {

    /**
     * The IDs for methods in this interface.
     */
    enum RequestMethodOp implements MethodOp {

        /**
         * The ID for the {@link AsyncVersionedRemote#getSerialVersion} method,
         * with ordinal 0.
         */
        GET_SERIAL_VERSION(0, GetSerialVersionCall::new),

        /**
         * The ID for the {@link AsyncRequestHandler#execute} method, with
         * ordinal 1.
         */
        EXECUTE(1, (in, serialVersion) -> new Request(in));

        private static final RequestMethodOp[] VALUES = values();

        private final ReadFastExternal<MethodCall<?>> reader;

        RequestMethodOp(final int ordinal,
                        final ReadFastExternal<MethodCall<?>> reader) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
            this.reader = reader;
        }

        /**
         * Returns the RequestMethodOp with the specified ordinal.
         *
         * @param ordinal the ordinal
         * @return the RequestMethodOp
         * @throws IllegalArgumentException if there is no associated value
         */
        public static RequestMethodOp valueOf(int ordinal) {
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "Wrong ordinal for RequestMethodOp: " + ordinal, e);
            }
        }

        @Override
        public int getValue() {
            return ordinal();
        }

        @Override
        public MethodCall<?> readRequest(final DataInput in,
                                         final short serialVersion)
            throws IOException
        {
            return reader.readFastExternal(in, serialVersion);
        }

        @Override
        public String toString() {
            return name() + '(' + ordinal() + ')';
        }
    }

    /** Method call for getSerialVersion */
    class GetSerialVersionCall extends AbstractGetSerialVersionCall {
        GetSerialVersionCall() { }
        @SuppressWarnings("unused")
        GetSerialVersionCall(DataInput in, short serialVersion) { }
        @Override
        public RequestMethodOp getMethodOp() {
            return RequestMethodOp.GET_SERIAL_VERSION;
        }
        @Override
        public String describeCall() {
            return "AsyncRequestHandler.GetSerialVersionCall";
        }
    }

    /**
     * Executes the request. It identifies the database that owns the keys
     * associated with the request and executes the request. <p>
     *
     * The caller should implement the retry logic for all failures that
     * can be handled locally. For example, a retry resulting from an
     * environment handle that was invalidated due to a hard recovery in the
     * midst of an operation. Exceptional situations that cannot be
     * handled internally are propagated back to the client. <p>
     *
     * It may not be possible to initiate execution of the request because the
     * request was misdirected and the RN does not own the key, or because the
     * request is for an update and the RN is not a master. In these cases,
     * it internally redirects the request to a more appropriate RN and returns
     * the response or exception as appropriate. <p>
     *
     * The caller should set the timeoutMillis parameter to a larger value than
     * the timeout in the request itself.  The additional time is needed to
     * report specific problems on the server side back to the client before
     * the remote call infrastructure makes a more generic decision that the
     * call has timed out.
     *
     * @param request the request to be executed
     * @param timeoutMillis the remote execution timeout in milliseconds
     * @return a future that returns the response
     */
    CompletableFuture<Response> execute(Request request, long timeoutMillis);
}
