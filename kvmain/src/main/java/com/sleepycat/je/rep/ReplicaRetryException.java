/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.rep;

/**
 * Thrown to indicate that the Replica must retry connecting to the same
 * master, after some period of time.
 */

@SuppressWarnings("serial")
public abstract class ReplicaRetryException extends Exception {
    final int nRetries;
    final int retrySleepMs;

    public ReplicaRetryException(String message,
            int nRetries,
            int retrySleepMs) {
        super(message);
        this.nRetries = nRetries;
        this.retrySleepMs = retrySleepMs;
    }

    /**
     * Get thread sleep time before retry
     *
     * @return sleep time in ms
     */
    public long getRetrySleepMs() {
        return retrySleepMs;
    }

    /**
     * Get number of nRetries
     *
     * @return number of nRetries
     */
    public int getNRetries() {
        return nRetries;
    }

    @Override
    public String getMessage() {
        return "Failed after nRetries: " + nRetries +
            " with retry interval: " + retrySleepMs + "ms. with message " + super.getMessage();
    }
}
