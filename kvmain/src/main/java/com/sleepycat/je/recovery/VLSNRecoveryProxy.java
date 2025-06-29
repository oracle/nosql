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

package com.sleepycat.je.recovery;

import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.entry.LogEntry;

/**
 * The VLSNRecoveryProxy is a handle for invoking VLSN tracking at recovery
 * time.
 */
public interface VLSNRecoveryProxy {

    public void trackMapping(long lsn, 
                             LogEntryHeader currentEntryHeader,
                             LogEntry logEntry);
}
