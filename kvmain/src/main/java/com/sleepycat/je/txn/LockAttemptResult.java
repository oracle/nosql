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

package com.sleepycat.je.txn;

/**
 * This is just a struct to hold a multi-value return.
 */
public class LockAttemptResult {
    public final boolean success;
    final Lock useLock;
    public final LockGrantType lockGrant;

    LockAttemptResult(Lock useLock,
                      LockGrantType lockGrant,
                      boolean success) {

        this.useLock = useLock;
        this.lockGrant = lockGrant;
        this.success = success;
    }
}
