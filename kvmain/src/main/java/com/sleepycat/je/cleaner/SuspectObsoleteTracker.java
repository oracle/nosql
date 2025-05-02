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
package com.sleepycat.je.cleaner;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.utilint.LoggerUtils;

public class SuspectObsoleteTracker extends LocalUtilizationTracker {
    private long suspectObsoleteCount;

    public SuspectObsoleteTracker(EnvironmentImpl env) {
        super(env);
        suspectObsoleteCount = 0;
    }

    public synchronized void transferToUtilizationTracker(UtilizationTracker tracker)
        throws DatabaseException {

        DataEraser eraser = env.getDataEraser();

        if (eraser == null || !eraser.isErasureEnabled()) {
            return;
        }

        super.transferToUtilizationTracker(tracker);
        fileSummaries.clear();

        /*
         * NOTE : Same offset may contribute multiple times to
         * suspectObsoleteCount when eraser period is small and eraser runs
         * multiple times between two checkpoints.
         */
        LoggerUtils.info(env.getLogger(), env,
            "ERASER transferred suspect obsolete offsets to global " +
                "tracker. Total offsets transferred: " + suspectObsoleteCount);

        suspectObsoleteCount = 0;
    }

    @Override
    public synchronized void countObsoleteNode(long lsn,
                                  LogEntryType type,
                                  int size) {

        countObsolete(lsn, type, size,
            true /*trackOffset*/,
            false /*checkDupOffsets*/);

        suspectObsoleteCount++;
    }
}
