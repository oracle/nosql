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

package com.sleepycat.je.rep.vlsn;

import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.log.entry.SingleItemEntry;
import com.sleepycat.je.recovery.VLSNRecoveryProxy;
import com.sleepycat.je.txn.RollbackStart;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.VLSN;

/**
 * {@literal
 * The VLSNRecoveryTracker is used as a transient tracker at recovery time.
 * It gathers up VLSN->LSN mappings that are in the log, but not persisted to
 * the mapping database. It has somewhat different preconditions from the
 * parent VLSNTracker which affect the semantics of tracking.
 *
 * Unlike VLSNTracker, the track() method is guaranteed to be executed in a
 * serial fashion. In addition, this tracker needs to "discover" where the
 * range for this tracker should start.  For example, suppose the on-disk
 * VLSNIndex covers VLSNs 25 -> 200. Also, suppose that the recovery portion of
 * the log holds VLSNs 190 -> 210 (an overlap of 190 -> 200)
 *
 * The VLSNIndex will be initialized with a range of 25 -> 200. We want the
 * recovery tracker to hold VLSN mappings from 190 -> 210. We don't want it to
 * just consult its range to determine where the next bucket starts.  If we did
 * that, the recovery tracker would start at VLSN 1.
 *
 * The VLSNRecoveryTracker must account for rollbacks and invisible log
 * entries. It has the authoritative view of what is in the recovery part of the
 * log and will override what is in the on-disk tracker.  At merge time, the
 * regular VLSNIndex must consult the VLSNRecoveryTracker's notion of what the
 * highest VLSN value is.
 *
 * If we see a RollbackStart, the end of range is abruptly reset back to the
 * matchpoint start. If we see non-invisible entries, the end of range may be
 * incrementing. For example, suppose the log has:recovery tracker
 *
 * VLSN 10                                tracks 10
 * VLSN 11 (invisible)                    skips
 * VLSN 12 (invisible)                    skips
 * VLSN 13 (invisible)                    skips
 * rollback start to VLSN 9               truncates to 9, clear everything
 * VLSN 10                                tracks 10
 * VLSN 11                                tracks 11
 * VLSN 12                                tracks 12
 * rollback start to VLSN 11              truncates to 11
 *
 * Suppose the on-disk VLSNIndex holds mappings for VLSN 1->13. A merge of the
 * VLSN index and the recovery tracker would
 *   1) truncate any VLSN > than the recovery tracker's high point -- so the
 *      VLSN index will drop mappings 12, 13
 *   2) will replace any VLSN index mappings with those held in the recovery
 *      tracker.
 * The VLSNIndex should map 1 -> 11, with the 10 and 11 mapping provided by the
 * recovery tracker.
 * }
 */
public final class VLSNRecoveryTracker
    extends VLSNTracker implements VLSNRecoveryProxy {

    private byte rollbackType;
    private long lastMatchpointVLSN = NULL_VLSN;
    private long lastMatchpointLsn = DbLsn.NULL_LSN;

    public VLSNRecoveryTracker(EnvironmentImpl envImpl,
                               int stride,
                               int maxMappings,
                               int maxDistance) {
        super(envImpl, stride, maxMappings, maxDistance);

        rollbackType = LogEntryType.LOG_ROLLBACK_START.getTypeNum();
    }

    /* VLSNRecoveryProxy.trackMapping */
    @Override
    public void trackMapping(long lsn,
                             LogEntryHeader currentEntryHeader,
                             LogEntry targetLogEntry) {

        if (currentEntryHeader.getReplicated() &&
            !currentEntryHeader.isInvisible()) {

            long vlsn = currentEntryHeader.getVLSN();
            track(vlsn, lsn,  currentEntryHeader.getType());
        } else if (currentEntryHeader.getType() == rollbackType) {
            RollbackStart rb = (RollbackStart)
                ((SingleItemEntry<?>) targetLogEntry).getMainItem();

            lastMatchpointVLSN = rb.getMatchpointVLSN();
            lastMatchpointLsn = rb.getMatchpoint();
            if (VLSN.isNull(range.getFirst())) {
                return;
            }

            if (range.getFirst() > lastMatchpointVLSN) {
                /* Throw away all mappings. */
                initEmpty();
                return;
            }

            if (range.getLast() <= lastMatchpointVLSN) {
                /* Nothing to truncate. */
                return;
            }

            truncateFromTail(VLSN.getNext(lastMatchpointVLSN),
                             lastMatchpointLsn);
        }
    }

    public boolean isEmpty() {
        return bucketCache.size() == 0;
    }

    public long getLastMatchpointVLSN() {
        return lastMatchpointVLSN;
    }

    public long getLastMatchpointLsn() {
        return lastMatchpointLsn;
    }
}
