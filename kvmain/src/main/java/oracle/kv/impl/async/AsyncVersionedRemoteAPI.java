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

package oracle.kv.impl.async;

import java.util.concurrent.CompletableFuture;

import oracle.kv.impl.rep.admin.AsyncClientRepNodeAdminAPI;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.registry.RemoteAPI;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Base class for API classes that wrap asynchronous remote interfaces to
 * implement an API called by clients of remote services.
 *
 * <p>This class is analogous to the {@link RemoteAPI} class used for
 * synchronous services and, like that class, negotiates the serial version to
 * use for communications.  Subclasses should provide a static method,
 * typically named "wrap", that creates an instance of the API class
 * asynchronously after determining the serial version by calling {@link
 * #computeSerialVersion computeSerialVersion}.
 *
 * <p>For an example of the layers used to implement an async API, see {@link
 * AsyncClientRepNodeAdminAPI}.
 *
 * @see AsyncVersionedRemote
 */
public abstract class AsyncVersionedRemoteAPI {

    /** The serial version used for communications. */
    private final short serialVersion;

    /**
     * Creates an instance of this class.  Subclasses should be designed to
     * create instances only after the serial version has been provided to the
     * future returned in a call to {@link #computeSerialVersion}.
     *
     * @param serialVersion the serial version used for communications
     */
    protected AsyncVersionedRemoteAPI(short serialVersion) {
        this.serialVersion = serialVersion;
    }

    /**
     * Returns the serial version used for communications.
     *
     * @return the serial version used for communications
     */
    public short getSerialVersion() {
        return serialVersion;
    }

    /**
     * Computes the serial version to use for communications, which is the
     * minimum of the serial versions for the initiator and the responder.
     * Makes a call through the initiator to the remote server to get its
     * serial version, and computes the result using the response and the local
     * value.  Subclasses, when implementing the "wrap" method, call this
     * method to decide what serial version to use.
     *
     * <p>This method will always supply a non-null serial version or a
     *  non-null exception.
     *
     * @param initiator the initiator-side stub for the remote server
     * @param timeoutMillis the timeout for the operation in milliseconds
     * @return the future
     */
    protected static CompletableFuture<Short>
        computeSerialVersion(AsyncVersionedRemote initiator,
                             long timeoutMillis) {
        return initiator.getSerialVersion(SerialVersion.CURRENT, timeoutMillis)
            .thenApply(
                AsyncVersionedRemoteAPI::computeSerialVersionHandleResult);
    }

    private static
        Short computeSerialVersionHandleResult(@Nullable Short serialVersion)
    {
        if (serialVersion == null) {
            throw new IllegalArgumentException("SerialVersion was null");
        }
        if (serialVersion < SerialVersion.MINIMUM) {
            throw SerialVersion.serverUnsupportedException(
                serialVersion, SerialVersion.MINIMUM);
        }
        return (short) Math.min(SerialVersion.CURRENT, serialVersion);
    }
}
