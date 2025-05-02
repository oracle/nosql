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

package com.sleepycat.je.tree;

import java.nio.ByteBuffer;

import com.sleepycat.je.cleaner.FileSummary;
import com.sleepycat.je.cleaner.PackedOffsets;
import com.sleepycat.je.cleaner.TrackedFileSummary;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.utilint.DbLsn;

/**
 * A FileSummaryLN represents a Leaf Node in the UtilizationProfile database.
 *
 * <p>The contents of the FileSummaryLN are not fixed until the moment at which
 * the LN is added to the log.  A base summary object contains the summary last
 * added to the log.  A tracked summary object contains live summary info being
 * updated in real time.  The tracked summary is added to the base summary just
 * before logging it, and then the tracked summary is reset.  This ensures that
 * the logged summary will accurately reflect the totals calculated at the
 * point in the log where the LN is added.</p>
 *
 * <p>This is all done in the writeToLog method, which operates under the log
 * write latch.  All utilization tracking must be done under the log write
 * latch.</p>
 *
 * <p>In record version 1, obsolete offset tracking was added and multiple
 * records are stored for a single file rather than a single record.  Each
 * record contains the offsets that were tracked since the last record was
 * written.
 *
 * <p>The key is 8 bytes: 4 bytes for the file number followed by 4 bytes for
 * the sequence number.  The lowest valued key for a given file contains the
 * most recent summary information, while to get a complete list of obsolete
 * offsets all records for the file must be read.  A range search using just
 * the first 4 bytes can be used to find the most recent record -- this is
 * possible because the sequence number values are decreasing over time for a
 * given file.  Here are example keys for three summary records in file 1:</p>
 *
 * <pre>
 * (file=1, sequence=Integer.MAX_VALUE - 300)
 * (file=1, sequence=Integer.MAX_VALUE - 200)
 * (file=1, sequence=Integer.MAX_VALUE - 100)
 * </pre>
 *
 * <p>The sequence number is the number of obsolete entries counted so far,
 * subtracted from Integer.MAX_VALUE to cause the latest written record to have
 * the lowest key.</p>
 *
 * @see com.sleepycat.je.cleaner.UtilizationProfile
 */
public final class FileSummaryLN extends LN {

    private static final String BEGIN_TAG = "<fileSummaryLN>";
    private static final String END_TAG = "</fileSummaryLN>";

    private int extraMarshaledMemorySize;
    private final FileSummary baseSummary;
    private TrackedFileSummary trackedSummary;
    private PackedOffsets obsoleteOffsets;
    private boolean needOffsets;
    private long anchorLsn;

    /**
     * Creates a new LN with a given base summary.
     */
    public FileSummaryLN(FileSummary baseSummary,
                         EnvironmentImpl env) {
        super(new byte[0]);
        assert baseSummary != null;
        this.baseSummary = baseSummary;
        obsoleteOffsets = new PackedOffsets();
        this.anchorLsn = env.getFileManager().getLastUsedLsn();
    }

    /**
     * Creates an empty LN to be filled in from the log.
     */
    public FileSummaryLN() {
        baseSummary = new FileSummary();
        obsoleteOffsets = new PackedOffsets();
        anchorLsn = DbLsn.NULL_LSN;
    }

    /**
     * Creates a deleted FileSummaryLN.
     *
     * @param deletedMarker makes this constructor signature unique, the value
     * passed doesn't matter.
     */
    private FileSummaryLN(boolean deletedMarker) {
        super((byte[]) null);
        baseSummary = new FileSummary();
        obsoleteOffsets = new PackedOffsets();
        anchorLsn = DbLsn.NULL_LSN;
    }

    /**
     * Creates a deleted FileSummaryLN.
     */
    public static LN makeDeletedLN() {
        return new FileSummaryLN(true /*deletedMarker*/);
    }

    /**
     * Sets the live summary object that will be added to the base summary at
     * the time the LN is logged.
     */
    public void setTrackedSummary(TrackedFileSummary trackedSummary) {
        this.trackedSummary = trackedSummary;
        needOffsets = true;
    }

    /**
     * Returns the base summary for the file that is stored in the LN.
     */
    public FileSummary getBaseSummary() {
        return baseSummary;
    }

    /**
     * Returns the obsolete offsets for the file.
     */
    public PackedOffsets getObsoleteOffsets() {
        return obsoleteOffsets;
    }

    /**
     * Returns the value of LastUsedLsn taken just prior to creating and
     * logging the FileSummaryLN.
     *
     * <p>Entries at the obsolete offsets in this FileSummaryLN should not be
     * erased or discarded by the cleaner until LastCheckpointFirstActiveLsn
     * has advanced past the anchorLsn, i.e., a full checkpoint has been
     * performed after the entry became obsolete.</p>
     *
     * <p>NULL_LSN is returned for FileSummaryLNs created prior to log
     * version 19.</p>
     */
    public long getAnchorLsn() {
        return anchorLsn;
    }

    /**
     * Convert a FileSummaryLN key from a byte array to a long.  The file
     * number is the first 4 bytes of the key.
     */
    public static long getFileNumber(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        return LogUtils.readIntMSB(buf) & 0xFFFFFFFFL;
    }

    /**
     * Get the sequence number from the byte array. The sequence number is the
     * last 4 bytes of the key.
     */
    private static long getSequence(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        LogUtils.readIntMSB(buf);
        return (Integer.MAX_VALUE - LogUtils.readIntMSB(buf)) & 0xFFFFFFFFL;
    }

    /**
     * Returns the first 4 bytes of the key for the given file number.  This
     * can be used to do a range search to find the first LN for the file.
     */
    public static byte[] makePartialKey(long fileNum) {

        byte[] bytes = new byte[4];
        ByteBuffer buf = ByteBuffer.wrap(bytes);

        LogUtils.writeIntMSB(buf, (int) fileNum);

        return bytes;
    }

    /**
     * Returns the full two-part key for a given file number and unique
     * sequence.  This can be used to insert a new LN.
     *
     * @param sequence is a unique identifier for the LN for the given file,
     * and must be greater than the last sequence.
     */
    public static byte[] makeFullKey(long fileNum, int sequence) {

        assert sequence >= 0;

        byte[] bytes = new byte[8];
        ByteBuffer buf = ByteBuffer.wrap(bytes);

        /*
         * The sequence is subtracted from MAX_VALUE so that increasing values
         * will be sorted first.  This allows a simple range search to find the
         * most recent value.
         */
        LogUtils.writeIntMSB(buf, (int) fileNum);
        LogUtils.writeIntMSB(buf, Integer.MAX_VALUE - sequence);

        return bytes;
    }

    /*
     * Dumping
     */

    @Override
    public String toString() {
        return dumpString(0, true);
    }

    @Override
    public String beginTag() {
        return BEGIN_TAG;
    }

    @Override
    public String endTag() {
        return END_TAG;
    }

    @Override
    public String dumpString(int nSpaces, boolean dumpTags) {
        StringBuilder sb = new StringBuilder();
        sb.append(super.dumpString(nSpaces, dumpTags));
        sb.append('\n');
        if (!isDeleted()) {
            sb.append(baseSummary.toString());
            sb.append(obsoleteOffsets.toString());
        }
        return sb.toString();
    }

    /**
     * Dump additional fields. Done this way so the additional info can
     * be within the XML tags defining the dumped log entry.
     */
    @Override
    protected void dumpLogAdditional(StringBuilder sb, boolean verbose) {
        if (!isDeleted()) {
            baseSummary.dumpLog(sb, true);
            if (verbose) {
                obsoleteOffsets.dumpLog(sb, true);
            }
            sb.append("<anchor lsn=\"");
            sb.append(DbLsn.toString(anchorLsn));
            sb.append("\"/>");
        }
    }

    /*
     * Logging
     */

    /**
     * Return the correct log type for a FileSummaryLN. 
     *
     * Note: FileSummaryLN will never be transactional.
     */
    @Override
    protected LogEntryType getLogType(boolean isInsert,
                                      boolean isTransactional,
                                      DatabaseImpl db) {
        assert !isTransactional : "Txnl access to UP db not allowed";

        return LogEntryType.LOG_FILESUMMARYLN;
    }

    /**
     * This log entry type is configured to perform marshaling (getLogSize and
     * writeToLog) under the write log mutex.  Otherwise, the size could change
     * in between calls to these two methods as the result of utilizaton
     * tracking.
     */
    @Override
    public int getLogSize(final int logVersion, final boolean forReplication) {
        int size = super.getLogSize(logVersion, forReplication);
        size += LogUtils.getPackedLongLogSize(anchorLsn);
        if (!isDeleted()) {
            size += baseSummary.getLogSize();
            getOffsets();
            size += obsoleteOffsets.getLogSize();
        }
        return size;
    }

    @Override
    public void writeToLog(final ByteBuffer logBuffer,
                           final int logVersion,
                           final boolean forReplication) {

        /*
         * Add the tracked (live) summary to the base summary before writing it
         * to the log, and reset the tracked summary.  When deleting the LN,
         * the tracked summary is cleared explicitly and will be null.
         */
        if (trackedSummary != null && !isDeleted()) {
            baseSummary.add(trackedSummary);
            getOffsets();
            /* Reset the totals to zero and clear the tracked offsets. */
            trackedSummary.reset();
        }

        super.writeToLog(logBuffer, logVersion, forReplication);

        LogUtils.writePackedLong(logBuffer, anchorLsn);

        if (!isDeleted()) {
            baseSummary.writeToLog(logBuffer);
            obsoleteOffsets.writeToLog(logBuffer);
        }
    }

    @Override
    public void readFromLog(EnvironmentImpl envImpl,
                            ByteBuffer itemBuffer,
                            int entryVersion) {

        super.readFromLog(envImpl, itemBuffer, entryVersion);

        anchorLsn = (entryVersion >= 19) ?
            LogUtils.readPackedLong(itemBuffer) :
            DbLsn.NULL_LSN;

        if (!isDeleted()) {
            baseSummary.readFromLog(envImpl, itemBuffer, entryVersion);
            if (entryVersion > 0) {
                obsoleteOffsets.readFromLog(envImpl, itemBuffer, entryVersion);
            }
        }
    }

    /**
     * Should never be replicated.
     */
    @Override
    public boolean logicalEquals(Loggable other) {
        return false;
    }

    /**
     * If tracked offsets may be present, get them so they are ready to be
     * written to the log.
     */
    private void getOffsets() {
        assert !isDeleted();
        if (needOffsets) {
            long[] offsets = trackedSummary.getObsoleteOffsets();
            if (offsets != null) {
                int oldSize = obsoleteOffsets.getExtraMemorySize();
                obsoleteOffsets.pack(offsets);
                int newSize = obsoleteOffsets.getExtraMemorySize();
                extraMarshaledMemorySize = newSize - oldSize;
            }
            needOffsets = false;
        }
    }

    /**
     * Overrides this method to add space occupied by this object's fields.
     */
    @Override
    public long getMemorySizeIncludedByParent() {
        return super.getMemorySizeIncludedByParent() +
               (MemoryBudget.FILESUMMARYLN_OVERHEAD -
                MemoryBudget.LN_OVERHEAD) +
               obsoleteOffsets.getExtraMemorySize();
    }

    /**
     * Clear out the obsoleteOffsets to save memory when the LN is deleted.
     */
    @Override
    void makeDeleted() {
        super.makeDeleted();
        obsoleteOffsets = new PackedOffsets();
    }

    /**
     * Adds the extra memory used by obsoleteOffsets to the parent BIN memory
     * size. Must be called after LN is inserted into the BIN and logged,
     * while the cursor is still positioned on the inserted LN.  The BIN must
     * be latched.  [#17462]
     *
     * <p>The obsoleteOffsets memory size is not intially budgeted in the usual
     * way because PackedOffsets.pack (which changes the memory size) is called
     * during marshalling (see getOffset).  This amount is not counted in the
     * parent IN size in the usual way, because LN logging / marshalling occurs
     * after the LN is inserted in the BIN and its memory size has been counted
     * (see CursorImpl.putInternal).</p>
     * 
     * <p>Note that the tree memory usage cannot be updated directly in
     * getOffsets because the tree memory usage must always be the sum of all
     * IN sizes, and it is reset to this sum each checkpoint.</p>
     */
    @Override
    public void addExtraMarshaledMemorySize(BIN parentBIN) {
        if (extraMarshaledMemorySize != 0) {
            assert trackedSummary != null; /* Must be set during the insert. */
            assert parentBIN.isLatchExclusiveOwner();
            parentBIN.updateMemorySize(0, extraMarshaledMemorySize);
            extraMarshaledMemorySize = 0;
        }
    }

    @Override
    public void dumpKey(StringBuilder sb, byte[] key) {
        sb.append("<fileSummaryLNKey fileNumber=\"0x" + 
                  Long.toHexString(getFileNumber(key)) + "\" ");
        sb.append("sequence=\"0x" + 
                  Long.toHexString(getSequence(key)) + "\"/>");
        super.dumpKey(sb, key);
    }
}
