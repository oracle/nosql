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

package com.sleepycat.je.log;

import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;

import java.nio.ByteBuffer;
import java.util.zip.Checksum;

import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.utilint.Adler32;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.VLSN;

/**
 * A LogEntryHeader embodies the header information at the beginning of each
 * log entry file.
 */
public class LogEntryHeader {

    /**
     * Persistent fields. Layout on disk is
     * (invariant) checksum - 4 bytes
     * (invariant) entry type - 1 byte
     * (invariant) entry flags - 1 byte
     * (invariant) offset of previous log entry - 4 bytes
     * (invariant) item size (not counting header size) - 4 bytes
     * (optional) vlsn - 8 bytes
     *
     * Flags:
     * The provisional bit can be set for any log type in the log. It's an
     * indication to recovery that the entry shouldn't be processed when
     * rebuilding the tree. See com.sleepycat.je.log.Provisional.java for
     * the reasons why it's set.
     *
     * The replicated bit is set when this particular log entry is
     * part of the replication stream and contains a VLSN in the header.
     *
     * The invisible bit is set when this log entry has been rolled back as
     * part of replication syncup. The ensuing log entry has not been
     * checksum-corrected, and to read it, the invisible bit must be cloaked.
     *
     * The VLSN_PRESENT bit is set when a VLSN is present, and is set when the
     * replicated bit is *not* set in the case of a cleaner migrated LN. If
     * the replicated bit is set then a VLSN is always present. [#19476]
     *
     *                    first version of        migrated LN
     *                    a replicated LN
     *                    ---------------         -----------
     *                    replicated = true       replicated = false
     * preserve record    vlsn present = true     vlsn present = false
     * version = false    vlsn exists in header   no vlsn in header
     *
     *                    replicated = true       replicated = false
     * preserve record    vlsn present = true     vlsn present = true
     * version = true     vlsn exists in header   vlsn exists in header
     */

    public static final int MIN_HEADER_SIZE = 14;
    public static final int MAX_HEADER_SIZE = MIN_HEADER_SIZE + VLSN.LOG_SIZE;

    public static final int CHECKSUM_BYTES = 4;

    public static final int ENTRYTYPE_OFFSET = 4;
    static final int FLAGS_OFFSET = 5;
    private static final int PREV_OFFSET = 6;
    private static final int ITEMSIZE_OFFSET = 10;
    public static final int VLSN_OFFSET = MIN_HEADER_SIZE;

    /*
     * Flags defined in the entry header.
     *
     * WARNING: Flags may not be defined or used in the entry header of the
     * FileHeader.  All flags defined here may only be used in log entries
     * other then the FileHeader. [#16939]
     */
    private static final byte PROVISIONAL_ALWAYS_MASK = (byte) 0x80;
    private static final byte PROVISIONAL_BEFORE_CKPT_END_MASK = (byte) 0x40;
    private static final byte REPLICATED_MASK = (byte) 0x20;
    private static final byte INVISIBLE = (byte) 0x10;
    private static final byte IGNORE_INVISIBLE = ~INVISIBLE;
    private static final byte VLSN_PRESENT = (byte) 0x08;
    /* Flag values 0x01, 0x02 and 0x04 are unused (currently zero). */

    private static final byte FILE_HEADER_TYPE_NUM =
        LogEntryType.LOG_FILE_HEADER.getTypeNum();

    private long checksumVal;   // stored in 4 bytes as an unsigned int
    private byte entryType;
    private long prevOffset;
    private int itemSize;
    private long vlsn;

    /*
     * The version is stored only in the FileHeader.  The version is not
     * stored in each entry; however, the version is maintained in this
     * in-memory object for convenience of access to the version when
     * processing log entries. [#16939]
     */
    private int entryVersion;

    /* Version flag fields */
    private Provisional provisional;
    private boolean replicated;
    private boolean invisible;
    private boolean vlsnPresent;

    /**
     * For reading a log entry.
     *
     * @param entryBuffer the buffer containing at least the first
     * MIN_HEADER_SIZE bytes of the entry header.
     *
     * @param logVersion is the log version of the file that contains the given
     * buffer, and is obtained from the file header.  Note that for the file
     * header entry itself, UNKNOWN_FILE_HEADER_VERSION may be passed.
     *
     * @param lsn is the LSN of the entry, for exception reporting.
     */
    public LogEntryHeader(ByteBuffer entryBuffer, int logVersion, long lsn)
        throws ChecksumException {

        assert logVersion == LogEntryType.UNKNOWN_FILE_HEADER_VERSION ||
            (logVersion >= LogEntryType.FIRST_LOG_VERSION &&
             logVersion <= LogEntryType.LOG_VERSION) : logVersion;

        checksumVal = LogUtils.readUnsignedInt(entryBuffer);
        entryType = entryBuffer.get();
        if (!LogEntryType.isValidType(entryType)) {
            throw new ChecksumException(
                "Invalid log entry type: " + entryType +
                " lsn=" + DbLsn.getNoFormatString(lsn) +
                " bufPosition=" + entryBuffer.position() +
                " bufRemaining=" + entryBuffer.remaining());
        }

        if (entryType == FILE_HEADER_TYPE_NUM) {
            /* Actual version will be set by setFileHeaderVersion. */
            entryVersion = LogEntryType.UNKNOWN_FILE_HEADER_VERSION;
            /* Discard flags byte: none are allowed for the file header. */
            entryBuffer.get();
            initFlags(0);
        } else {
            if (logVersion == LogEntryType.UNKNOWN_FILE_HEADER_VERSION ) {
                /*
                 * If we are reading a log header the type should be
                 * FILE_HEADER_TYPE_NUM.
                 */
                throw new ChecksumException(
                    "Wrong entry type for header: " + entryType +
                    " lsn=" + DbLsn.getNoFormatString(lsn) +
                    " bufPosition=" + entryBuffer.position() +
                    " bufRemaining=" + entryBuffer.remaining());
            } else {
                entryVersion = logVersion;
                initFlags(entryBuffer.get());
            }
        }
        prevOffset = LogUtils.readUnsignedInt(entryBuffer);
        itemSize = LogUtils.readInt(entryBuffer);
        if (itemSize < 0) {
            throw new ChecksumException(
                "Invalid log entry size: " + itemSize +
                " lsn=" + DbLsn.getNoFormatString(lsn) +
                " bufPosition=" + entryBuffer.position() +
                " bufRemaining=" + entryBuffer.remaining());
        }
    }

    /**
     * For writing a log header. Must call {@link #initForWrite} and
     * {@link #initEntrySize}.
     */
    public LogEntryHeader() {
    }

    /**
     * For writing a log header.
     * {@link #initEntrySize(LogEntry)} must be called to complete the header.
     */
    public void initForWrite(LogEntry entry,
                             Provisional provisional,
                             ReplicationContext repContext) {

        LogEntryType logEntryType = entry.getLogType();
        entryType = logEntryType.getTypeNum();
        entryVersion = LogEntryType.LOG_VERSION;
        this.provisional = provisional;

        assert (!((!logEntryType.isReplicationPossible()) &&
                  repContext.inReplicationStream())) :
               logEntryType + " should never be replicated.";

        if (logEntryType.isReplicationPossible()) {
            this.replicated = repContext.inReplicationStream();
        } else {
            this.replicated = false;
        }
        invisible = false;

        /*
         * If we about to write a new replicated entry, the VLSN will be null
         * and mustGenerateVLSN will return true.  For a cleaner migrated LN
         * that was replicated, the VLSN will be non-null and mustGenerateVLSN
         * will return false.  [#19476]
         */
        vlsnPresent = repContext.getClientVLSN() != INVALID_VLSN ||
            repContext.mustGenerateVLSN();

        /* Init for object reuse. */
        checksumVal = 0;
        prevOffset = 0;
        itemSize = 0;
        vlsn = INVALID_VLSN;
    }

    public void initEntrySize(LogEntry entry) {
        itemSize = entry.getSize();
    }

    /**
     * For reading a replication message. The node-specific parts of the header
     * are not needed.
     */
    public LogEntryHeader(byte entryType,
                          int entryVersion,
                          int itemSize,
                          long vlsn) {

        assert ((vlsn != INVALID_VLSN) && !VLSN.isNull(vlsn)) :
               "vlsn = " + vlsn;

        this.entryType = entryType;
        this.entryVersion = entryVersion;
        this.itemSize = itemSize;
        this.vlsn = vlsn;
        replicated = true;
        vlsnPresent = true;
        provisional = Provisional.NO;
    }

    private void initFlags(int entryFlags) {
        if ((entryFlags & PROVISIONAL_ALWAYS_MASK) != 0) {
            provisional = Provisional.YES;
        } else if ((entryFlags & PROVISIONAL_BEFORE_CKPT_END_MASK) != 0) {
            provisional = Provisional.BEFORE_CKPT_END;
        } else {
            provisional = Provisional.NO;
        }
        replicated = ((entryFlags & REPLICATED_MASK) != 0);
        invisible = ((entryFlags & INVISIBLE) != 0);
        vlsnPresent = ((entryFlags & VLSN_PRESENT) != 0) || replicated;
    }

    /**
     * Called to set the version for a file header entry after reading the
     * version from the item data.  See FileHeaderEntry.readEntry.  [#16939]
     */
    public void setFileHeaderVersion(final int logVersion) {
        entryVersion = logVersion;
    }

    public long getChecksum() {
        return checksumVal;
    }

    public byte getType() {
        return entryType;
    }

    public int getVersion() {
        return entryVersion;
    }

    public long getPrevOffset() {
        return prevOffset;
    }

    public int getItemSize() {
        return itemSize;
    }

    public int getEntrySize() {
        return getSize() + getItemSize();
    }

    public long getVLSN() {
        return vlsn;
    }

    public boolean getReplicated() {
        return replicated;
    }

    public Provisional getProvisional() {
        return provisional;
    }

    public boolean isInvisible() {
        return invisible;
    }

    public boolean isTxnEnd() {
        return LogEntryType.LOG_TXN_COMMIT.getTypeNum() == entryType ||
               LogEntryType.LOG_TXN_ABORT.getTypeNum() == entryType;
    }

    /**
     * Returns whether the type of this entry has been changed to LOG_ERASED.
     *
     * @see com.sleepycat.je.log.entry.ErasedLogEntry
     * @see com.sleepycat.je.cleaner.DataEraser
     */
    public boolean isErased() {
        return entryType == LogEntryType.LOG_ERASED.getTypeNum();
    }

    public boolean hasChecksum() {
        return !isErased();
    }

    public int getVariablePortionSize() {
        return VLSN.LOG_SIZE;
    }

    /**
     * @return number of bytes used to store this header
     */
    public int getSize() {
        if (vlsnPresent) {
            return MIN_HEADER_SIZE + VLSN.LOG_SIZE;
        }
        return MIN_HEADER_SIZE;
    }

    /**
     * @return the number of bytes used to store the header, excepting
     * the checksum field.
     */
    int getSizeMinusChecksum() {
        return getSize()- CHECKSUM_BYTES;
    }

    /**
     * @return the number of bytes used to store the header, excepting
     * the checksum field.
     */
    int getInvariantSizeMinusChecksum() {
        return MIN_HEADER_SIZE - CHECKSUM_BYTES;
    }

    /**
     * Assumes this is called directly after the constructor, and that the
     * entryBuffer is positioned right before the VLSN.
     */
    public void readVariablePortion(EnvironmentImpl envImpl,
                                    ByteBuffer entryBuffer) {
        if (vlsnPresent) {
            vlsn = LogUtils.readLong(entryBuffer);
        }
    }

    /**
     * Serialize this object into the buffer and leave the buffer positioned in
     * the right place to write the following item.  The checksum, prevEntry,
     * and vlsn values will filled in later on.
     *
     * public for unit tests.
     */
    public void writeToLog(ByteBuffer entryBuffer) {

        /* Skip over the checksumVal, proceed to the entry type. */
        entryBuffer.position(ENTRYTYPE_OFFSET);
        entryBuffer.put(entryType);

        /* Flags */
        byte flags = 0;
        if (provisional == Provisional.YES) {
            flags |= PROVISIONAL_ALWAYS_MASK;
        } else if (provisional == Provisional.BEFORE_CKPT_END) {
            flags |= PROVISIONAL_BEFORE_CKPT_END_MASK;
        }
        if (replicated) {
            flags |= REPLICATED_MASK;
        }
        if (vlsnPresent) {
            flags |= VLSN_PRESENT;
        }
        entryBuffer.put(flags);

        /*
         * Leave room for the prev offset, which must be added under
         * the log write latch. Proceed to write the item size.
         */
        entryBuffer.position(ITEMSIZE_OFFSET);
        LogUtils.writeInt(entryBuffer, itemSize);

        /*
         * Leave room for a VLSN if needed, must also be generated
         * under the log write latch.
         */
        if (vlsnPresent) {
            entryBuffer.position(entryBuffer.position() + VLSN.LOG_SIZE);
        }
    }

    /**
     * Add those parts of the header that must be calculated later to the
     * entryBuffer, and also assign the fields in this class.
     * That's
     * - the prev offset, which must be done within the log write latch to
     *   be sure what that lsn is
     * - the VLSN, for the same reason
     * - the checksumVal, which must be added last, after all other
     *   fields are marshalled.
     * (public for unit tests)
     */
    public void addPostMarshallingInfo(ByteBuffer entryBuffer,
                                       long lastOffset,
                                       long vlsn) {

        /* Add the prev pointer */
        prevOffset = lastOffset;
        entryBuffer.position(PREV_OFFSET);
        LogUtils.writeUnsignedInt(entryBuffer, prevOffset);

        if (vlsn != INVALID_VLSN) {
            this.vlsn = vlsn;
            entryBuffer.position(VLSN_OFFSET);

            LogUtils.writeLong(entryBuffer, vlsn);
        }

        /*
         * Now calculate the checksumVal and write it into the buffer.  Be sure
         * to set the field in this instance, for use later when printing or
         * debugging the header.
         */
        Checksum checksum = Adler32.makeChecksum();
        checksum.update(entryBuffer.array(),
                        entryBuffer.arrayOffset() + CHECKSUM_BYTES,
                        entryBuffer.limit() - CHECKSUM_BYTES);
        entryBuffer.position(0);
        checksumVal = checksum.getValue();
        LogUtils.writeUnsignedInt(entryBuffer, checksumVal);

        /* Leave this buffer ready for copying into another buffer. */
        entryBuffer.position(0);
    }

    /**
     * @param sb destination string buffer
     * @param verbose if true, dump the full, verbose version
     */
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append("<hdr ");
        dumpLogNoTag(sb, verbose);
        sb.append("\"/>");
    }

    /**
     * Dump the header without enclosing <header> tags. Used for
     * DbPrintLog, to make the header attributes in the <entry> tag, for
     * a more compact rendering.
     * @param sb destination string buffer
     * @param verbose if true, dump the full, verbose version
     */
    void dumpLogNoTag(StringBuilder sb, boolean verbose) {
        LogEntryType lastEntryType = LogEntryType.findType(entryType);

        sb.append("type=\"");
        if (lastEntryType != null) {
            sb.append(lastEntryType.toStringNoVersion()).
                append("/").append(entryVersion);
        }

        if (provisional != Provisional.NO) {
            sb.append("\" prov=\"");
            sb.append(provisional);
        }

        if (vlsn != INVALID_VLSN) {
            sb.append("\" ");
            sb.append("<vlsn v=\"").append(vlsn).append("\">");
        } else {
            sb.append("\"");
        }

        if (getReplicated()) {
            sb.append(" isReplicated=\"1\"");
        }

        if (isInvisible()) {
            sb.append(" isInvisible=\"1\"");
        }

        sb.append(" prev=\"0x").append(Long.toHexString(prevOffset));
        if (verbose) {
            sb.append("\" size=\"").append(itemSize);
            sb.append("\" cksum=\"").append(checksumVal);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        dumpLog(sb, true /* verbose */);
        return sb.toString();
    }

    /*
     * Dump only the parts of the header that apply for replicated entries.
     */
    public void dumpRep(StringBuilder sb) {

        LogEntryType lastEntryType = LogEntryType.findType(entryType);

        sb.append(lastEntryType.toStringNoVersion()).
            append("/").append(entryVersion);

        if (vlsn != INVALID_VLSN) {
            sb.append(" vlsn=" ).append(vlsn);
        } else {
            sb.append("\"");
        }

        if (getReplicated()) {
            sb.append(" isReplicated=\"1\"");
        }

        if (isInvisible()) {
            sb.append(" isInvisible=\"1\"");
        }
    }

    /**
     * @return true if two log headers are logically the same. This check will
     * ignore the log version.
     *
     * Used by replication.
     */
    public boolean logicalEqualsIgnoreVersion(LogEntryHeader other) {

        /*
         * Note that item size is not part of the logical equality, because
         * on-disk compression can make itemSize vary if the entry has VLSNs
         * that were packed differently.
         */
        return ((getVLSN() == other.getVLSN()) &&
                (getReplicated() == other.getReplicated()) &&
                (isInvisible() == other.isInvisible()) &&
                (getType() == other.getType()));
    }

    /**
     * May be called after reading MIN_HEADER_SIZE bytes to determine
     * whether more bytes (getVariablePortionSize) should be read.
     */
    public boolean isVariableLength() {
        /* Currently only entries with VLSNs are variable length. */
        return vlsnPresent;
    }

    /**
     * Set the invisible bit in the given log entry flags.
     */
    static byte makeInvisible(byte flags) {
        return (byte) (flags | INVISIBLE);
    }

    /**
     * Turn off the invisible bit in the byte buffer which backs this log entry
     * header.
     * @param logHeaderStartPosition the byte position of the start of the log
     * entry header.
     */
    public static void turnOffInvisible(ByteBuffer buffer,
                                        int logHeaderStartPosition) {

        int flagsPosition = logHeaderStartPosition + FLAGS_OFFSET;
        byte flags = buffer.get(flagsPosition);
        flags &= IGNORE_INVISIBLE;
        buffer.put(flagsPosition, flags);
    }
}
