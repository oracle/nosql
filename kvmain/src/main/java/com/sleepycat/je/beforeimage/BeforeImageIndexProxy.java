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

package com.sleepycat.je.beforeimage;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.txn.Locker;

/**
 * {@literal
 * This class mainly to disable the beforeimage feature 
 * }
 */
public class BeforeImageIndexProxy extends BeforeImageIndex {

    public BeforeImageIndexProxy(EnvironmentImpl envImpl)
            throws DatabaseException {
        super(envImpl);
    }

    @Override
    public boolean put(final DBEntry entry) {
        return false;
    }

    @Override
    public DatabaseEntry get(long abortLsn, Locker lck) {
        return null;
    }

    @Override
    public void close()
        throws DatabaseException {
        // No-op
    }
}
