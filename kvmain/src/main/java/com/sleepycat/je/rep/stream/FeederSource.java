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

package com.sleepycat.je.rep.stream;

import java.io.IOException;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * Provides the next log record, blocking if one is not available. It
 * encapsulates the source of the Log records, which can be a real Master or a
 * Replica in a Replica chain that is replaying log records it received from
 * some other source.
 */
public interface FeederSource {

    public void shutdown(EnvironmentImpl envImpl);

    /**
     * Must be called to allow deletion of files protected by this feeder.
     */
    public OutputWireRecord getWireRecord(
        long vlsn, long waitNs, boolean includeBeforeImage)
        throws DatabaseException, InterruptedException, IOException;

    public String dumpState();
}
