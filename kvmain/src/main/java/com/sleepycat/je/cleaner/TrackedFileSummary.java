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

import com.sleepycat.je.dbi.MemoryBudget;

/**
 * Delta file summary info for a tracked file.  Tracked files are managed by
 * the UtilizationTracker.
 *
 * <p>The methods in this class for reading obsolete offsets may be used by
 * multiple threads without synchronization even while another thread is adding
 * offsets.  This is possible because elements are never deleted from the
 * lists.  The thread adding obsolete offsets does so under the log write
 * latch to prevent multiple threads from adding concurrently.</p>
 */
public class TrackedFileSummary extends FileSummary {

    private BaseUtilizationTracker tracker;
    private long fileNum;
    private OffsetList obsoleteOffsets;
    private int memSize;
    private boolean trackDetail;
    private boolean allowFlush = true;

    /**
     * Creates an empty tracked summary.
     */
    TrackedFileSummary(BaseUtilizationTracker tracker,
                       long fileNum,
                       boolean trackDetail) {
        this.tracker = tracker;
        this.fileNum = fileNum;
        this.trackDetail = trackDetail;
    }

    /**
     * Returns whether this summary is allowed or prohibited from being flushed
     * or evicted during cleaning.  By default, flushing is allowed.
     * <p>
     * Note that allowFlush is only used for summaries in the global tracker,
     * not in local trackers. The summary in a local tracker is never flushed to
     * the log directly, it is only added to the corresponding TFS in the global
     * tracker.
     */
    public boolean getAllowFlush() {
        return allowFlush;
    }

    /**
     * Allows or prohibits this summary from being flushed or evicted during
     * cleaning.  By default, flushing is allowed.
     * <p>
     * Note that allowFlush is only used for summaries in the global tracker,
     * not in local trackers. The summary in a local tracker is never flushed to
     * the log directly, it is only added to the corresponding TFS in the global
     * tracker.
     */
    void setAllowFlush(boolean allowFlush) {
        this.allowFlush = allowFlush;
    }

    /**
     * Returns the file number being tracked.
     */
    public long getFileNumber() {
        return fileNum;
    }

    /**
     * Return the total memory size for this object.  We only bother to budget
     * obsolete detail, not the overhead for this object, for two reasons:
     * 1) The number of these objects is very small, and 2) unit tests disable
     * detail tracking as a way to prevent budget adjustments here.
     */
    int getMemorySize() {
        return memSize;
    }

    /**
     * Overrides reset for a tracked file, and is called when a FileSummaryLN
     * is written to the log.
     *
     * <p>Must be called under the log write latch.</p>
     */
    @Override
    public void reset() {
        assert tracker != null;

        obsoleteOffsets = null;

        tracker.resetFile(this);

        if (memSize > 0) {
            updateMemorySize(0 - memSize);
        }

        super.reset();
    }

    /**
     * Tracks the given offset as obsolete or non-obsolete.
     *
     * <p>Must be called under the log write latch.</p>
     */
    void trackObsolete(long offset, boolean checkDupOffsets) {

        if (!trackDetail) {
            return;
        }

        int adjustMem = 0;
        if (obsoleteOffsets == null) {
            obsoleteOffsets = new OffsetList();
            adjustMem += MemoryBudget.TFS_LIST_INITIAL_OVERHEAD;
        }

        if (obsoleteOffsets.add(offset, checkDupOffsets)) {
            adjustMem += MemoryBudget.TFS_LIST_SEGMENT_OVERHEAD;
        }

        if (adjustMem != 0) {
            updateMemorySize(adjustMem);
        }
    }

    /**
     * Adds the obsolete offsets as well as the totals of the given object.
     */
    void addTrackedSummary(TrackedFileSummary other) {

        /* Add the totals. */
        add(other);

        /*
         * Add the offsets and the memory used [#15505] by the other tracker.
         * The memory budget has already been updated for the offsets to be
         * added, so we only need to account for a possible difference of one
         * segment when we merge them.
         */
        memSize += other.memSize;
        if (other.obsoleteOffsets != null) {
            if (obsoleteOffsets != null) {
                /* Merge the other offsets into our list. */
                if (obsoleteOffsets.merge(other.obsoleteOffsets)) {
                    /* There is one segment less as a result of the merge. */
                    updateMemorySize
                        (- MemoryBudget.TFS_LIST_SEGMENT_OVERHEAD);
                }
            } else {
                /* Adopt the other's offsets as our own. */
                obsoleteOffsets = other.obsoleteOffsets;
            }
        }
    }

    /**
     * Returns obsolete offsets as an array of longs, or null if none.
     */
    public long[] getObsoleteOffsets() {

        if (obsoleteOffsets != null) {
            return obsoleteOffsets.toArray();
        } else {
            return null;
        }
    }

    /**
     * Returns whether the given offset is present in the tracked offsets.
     * This does not indicate whether the offset is obsolete in general, but
     * only if it is known to be obsolete in this version of the tracked
     * information.
     */
    boolean containsObsoleteOffset(long offset) {

        if (obsoleteOffsets != null) {
            return obsoleteOffsets.contains(offset);
        } else {
            return false;
        }
    }

    private void updateMemorySize(int delta) {
        memSize += delta;
    }
}
