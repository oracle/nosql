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

/**
 * A subclass that provides a default timeout, which is typically derived from
 * the read timeout for the associated client socket factory.
 */
public abstract class AsyncVersionedRemoteAPIWithTimeout
        extends AsyncVersionedRemoteAPI {

    private final long defaultTimeoutMs;

    /**
     * Creates an instance of this class.  Subclasses should be designed to
     * create instances only after the serial version has been provided to the
     * future returned in a call to {@link #computeSerialVersion}.
     *
     * @param serialVersion the serial version used for communications
     * @param defaultTimeoutMs the default timeout
     * @throws IllegalArgumentException if defaultTimeoutMs is 0 or less
     */
    protected AsyncVersionedRemoteAPIWithTimeout(short serialVersion,
                                                 long defaultTimeoutMs) {
        super(serialVersion);
        if (defaultTimeoutMs <= 0) {
            throw new IllegalArgumentException(
                "The defaultTimeoutMs must be greater than 0, found " +
                defaultTimeoutMs);
        }
        this.defaultTimeoutMs = defaultTimeoutMs;
    }

    /**
     * Returns the default timeout in milliseconds.
     *
     * @return the default timeout in milliseconds
     */
    public long getDefaultTimeoutMs() {
        return defaultTimeoutMs;
    }
}
