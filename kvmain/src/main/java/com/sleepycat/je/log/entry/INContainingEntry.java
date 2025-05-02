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

package com.sleepycat.je.log.entry;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.tree.IN;

/**
 * An INContainingEntry is a log entry that contains internal nodes.
 */
public interface INContainingEntry {
        
    /**
     * Currently used by recovery only. For a new BINDeltaLogEntry, 
     * it just returns the delta. And for an INLogEntry it just returns 
     * the (full) IN.
     */
    public IN getIN(DatabaseImpl dbImpl)
        throws DatabaseException;

    /*
     * A quick way to check whether this LogEntry reads/writes a BIN-Delta
     * logrec.
     */
    public boolean isBINDelta();
        
    /**
     * @return the database id held within this log entry.
     */
    public DatabaseId getDbId();

    /**
     * @return the LSN of the prior full version of this node, or NULL_LSN if
     * no prior full version. Used for counting the prior version as obsolete.
     * If the offset of the LSN is zero, only the file number is known because
     * we read a version 1 log entry.
     */
    public long getLastFullLsn();

    /**
     * @return the LSN of the prior delta version of this node, or NULL_LSN if
     * the prior version is a full version.  Used for counting the prior
     * version as obsolete.  If the offset of the LSN is zero, only the file
     * number is known because we read a version 1 log entry.
     */
    public long getLastDeltaLsn();
}
