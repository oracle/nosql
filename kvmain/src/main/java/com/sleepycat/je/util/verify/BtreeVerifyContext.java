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
package com.sleepycat.je.util.verify;

import com.sleepycat.je.VerifyError;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * Returned by {@link EnvironmentImpl#getBtreeVerifyContext} when the current
 * thread is running under the Btree verifier.
 */
public interface BtreeVerifyContext {

    /** Adds an error for an LN or IN. */
    void recordError(VerifyError error);
}
