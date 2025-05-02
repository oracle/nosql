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

import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryType;

/**
 * Accumulates changes to the utilization profile locally in a single thread.
 *
 * <p>The countNewLogEntry, countObsoleteNode and countObsoleteNodeInexact
 * methods may be called without taking the log write latch.  Totals and offset
 * are accumulated locally in this object only, not in DatabaseImpl
 * objects.</p>
 *
 * <p>When finished with this object, its information should be added to the
 * Environment's UtilizationTracker and DatabaseImpl objects by calling
 * transferToUtilizationTracker under the log write latch.  This is done in the
 * Checkpointer, Evictor and INCompressor by calling
 * UtilizationProfile.flushLocalTracker which calls
 * LogManager.transferToUtilizationTracker which calls
 * BaseLocalUtilizationTracker.transferToUtilizationTracker.</p>
 */
public class LocalUtilizationTracker extends BaseUtilizationTracker {

    public LocalUtilizationTracker(EnvironmentImpl env) {
        super(env);
    }

    /**
     * Counts a node that has become obsolete and tracks the LSN offset, if
     * non-zero, to avoid a lookup during cleaning.
     *
     * <p>A zero LSN offset is used as a special value when obsolete offset
     * tracking is not desired. [#15365]  The file header entry (at offset
     * zero) is never counted as obsolete, it is assumed to be obsolete by the
     * cleaner.</p>
     *
     * <p>This method should only be called for LNs and INs (i.e, only for
     * nodes).  If type is null we assume it is an LN.</p>
     */
    public void countObsoleteNode(long lsn,
                                  LogEntryType type,
                                  int size) {
        countObsolete
            (lsn, type, size,
             true,   // trackOffset
             true);  // checkDupOffsets
    }

    /**
     * Counts as countObsoleteNode does, but since the LSN may be inexact, does
     * not track the obsolete LSN offset.
     *
     * <p>This method should only be called for LNs and INs (i.e, only for
     * nodes).  If type is null we assume it is an LN.</p>
     */
    public void countObsoleteNodeInexact(long lsn,
                                         LogEntryType type,
                                         int size) {
        countObsolete
            (lsn, type, size,
             false,  // trackOffset
             false); // checkDupOffsets
    }
}
