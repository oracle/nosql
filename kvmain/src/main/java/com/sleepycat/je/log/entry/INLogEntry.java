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

import java.nio.ByteBuffer;

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.utilint.DbLsn;

/**
 * - INLogEntry is used to read/write full-version IN logrecs.
 *
 * - BINDeltaLogEntry subclasses INLogEntry and is used to read/write
 *   BIN-delta logrecs for log versions 9 or later.
 *
 * On disk, a full IN logrec contains:
 *
 * <pre>
 * {@literal
 * (8 <= version)
 *        database id
 *        lastFullLsn
 *        lastFullLogSize (version >= 21)
 *        lastDeltaLsn
 *        lastDeltaLogSize (version >= 21)
 *        IN
 * }
 * </pre>
 *
 *  On disk, a BIN-delta logrec written via the BINDeltaLogEntry contains:
 *
 * <pre>
 * {@literal
 * (version == 9)
 *        database id
 *        lastFullLsn  -- always NULL
 *        lastDeltaLsn
 *        BIN (dirty slots only)
 *        lastFullLsn
 *
 * (version >= 10)
 *        database id
 *        lastFullLsn
 *        lastFullLogSize (version >= 21)
 *        lastDeltaLsn
 *        lastDeltaLogSize (version >= 21)
 *        BIN (dirty slots only and including the new fullBinNEntries and
 *             fullBinMaxEntries fields)
 * }
 * </pre>
 */
public class INLogEntry<T extends IN> extends BaseEntry<T>
    implements LogEntry, INContainingEntry {

    /*
     * Persistent fields in an IN entry.
     */

    private DatabaseId dbId;

    /*
     * this.in may be a (a) UIN, (b) full BIN, or (c) BIN delta.
     * In case (a), "this" is a INLogEntry
     * In case (c), "this" is a BINDeltaLogEntry instance.
     * In case (b), "this" may be either a INLogEntry or a BINDeltaLogEntry
     * instance. It will be a BINDeltaLogEntry instance, if "this" is used
     * to log a full in-memory BIN as a BIN-delta.
     */
    private T in;

    /*
     * lastFullLsn is the lsn of the previous full-version logrec for the same
     * IN, or null if this is only version logged. lastFullLogSize is the size
     * of that logrec, or zero if lastFullLsn is null.
     *
     * See comment above about the evolution of this field.
     */
    private long lastFullLsn;
    private int lastFullLogSize;

    /*
     * If this is a BIN logrec and the previous logrec for the same BIN was
     * a BIN-delta, lastDeltaLsn is the lsn of that previous logrec. Otherwise,
     * lastDeltaLsn is null. lastDeltaLogSize is the size of the logrec, or
     * zero if lastDeltaLsn is null.
     *
     * See comment above about the evolution of this field.
     */
    private long lastDeltaLsn;
    private int lastDeltaLogSize;

    /**
     * Construct a log entry for reading.
     */
    public static <T extends IN> INLogEntry<T> create(final Class<T> INClass) {
        return new INLogEntry<T>(INClass);
    }

    INLogEntry(Class<T> INClass) {
        super(INClass);
    }

    /**
     * Construct an INLogEntry for writing to the log.
     */
    public INLogEntry(T in) {
        this(in, false /*isBINDelta*/);
    }

    /*
     * Used by both INLogEntry and BINDeltaLogEntry for writing to the log.
     */
    INLogEntry(T in, boolean isBINDelta) {

        setLogType(isBINDelta ? LogEntryType.LOG_BIN_DELTA : in.getLogType());

        dbId = in.getDatabase().getId();

        this.in = in;

        lastFullLsn = in.getLastFullLsn();
        lastFullLogSize = in.getLastFullLogSize();
        lastDeltaLsn = in.getLastDeltaLsn();
        lastDeltaLogSize = in.getLastDeltaLogSize();
    }

    /*
     * Whether this LogEntry reads/writes a BIN-Delta logrec.
     * Overriden by the BINDeltaLogEntry subclass.
     */
    @Override
    public boolean isBINDelta() {
        return false;
    }

    @Override
    public DatabaseId getDbId() {
        return dbId;
    }

    @Override
    public long getLastFullLsn() {
        return lastFullLsn;
    }

    public int getLastFullLogSize() {
        return lastFullLogSize;
    }

    @Override
    public long getLastDeltaLsn() {
        return lastDeltaLsn;
    }

    @Override
    public T getMainItem() {
        return in;
    }

    @Override
    public IN getIN(DatabaseImpl dbImpl) {
        return in;
    }

    public long getNodeId() {
        return in.getNodeId();
    }

    /*
     * Read support
     */

    @Override
    public void readEntry(
        EnvironmentImpl envImpl,
        LogEntryHeader header,
        ByteBuffer entryBuffer) {

        int logVersion = header.getVersion();

        dbId = new DatabaseId();
        dbId.readFromLog(envImpl, entryBuffer, logVersion);

        lastFullLsn = LogUtils.readPackedLong(entryBuffer);
        lastFullLogSize = (logVersion >= 21) ?
            LogUtils.readPackedInt(entryBuffer) : 0;
        lastDeltaLsn = (logVersion >= 8) ?
            LogUtils.readPackedLong(entryBuffer) : DbLsn.NULL_LSN;
        lastDeltaLogSize = (logVersion >= 21) ?
            LogUtils.readPackedInt(entryBuffer) : 0;

        /* Read IN. */
        in = newInstanceOfType();
        readMainItem(envImpl, in, entryBuffer, logVersion);
    }

    private void readMainItem(EnvironmentImpl envImpl,
                              T in,
                              ByteBuffer entryBuffer,
                              int logVersion) {
        if (isBINDelta()) {
            assert(logVersion >= 9);

            in.readFromLog(
                envImpl, entryBuffer, logVersion, true /*deltasOnly*/);

            if (logVersion == 9) {
                /* Odd case where lastFullLsn follows the IN. */
                lastFullLsn = LogUtils.readPackedLong(entryBuffer);
            }
        } else {
            in.readFromLog(envImpl, entryBuffer, logVersion);
        }
    }

    /*
     * Writing support
     */
    @Override
    public int getSize() {
        return (in.getLogSize(isBINDelta()) +
                dbId.getLogSize() +
                LogUtils.getPackedLongLogSize(lastFullLsn) +
                LogUtils.getPackedIntLogSize(lastFullLogSize) +
                LogUtils.getPackedLongLogSize(lastDeltaLsn) +
                LogUtils.getPackedIntLogSize(lastDeltaLogSize));
    }

    @Override
    public void writeEntry(ByteBuffer destBuffer) {

        dbId.writeToLog(destBuffer);

        LogUtils.writePackedLong(destBuffer, lastFullLsn);
        LogUtils.writePackedInt(destBuffer, lastFullLogSize);
        LogUtils.writePackedLong(destBuffer, lastDeltaLsn);
        LogUtils.writePackedInt(destBuffer, lastDeltaLogSize);
        in.writeToLog(destBuffer, isBINDelta());
    }

    @Override
    public long getTransactionId() {
        return 0;
    }

    /**
     * INs from two different environments are never considered equal,
     * because they have lsns that are environment-specific.
     */
    @Override
    public boolean logicalEquals(@SuppressWarnings("unused") LogEntry other) {
        return false;
    }

    @Override
    public StringBuilder dumpEntry(StringBuilder sb, boolean verbose) {

        dbId.dumpLog(sb, verbose);
        in.dumpLog(sb, verbose);

        sb.append("<lastFullVersion lsn=\"");
        sb.append(DbLsn.getNoFormatString(lastFullLsn));
        sb.append("\" size=\"");
        sb.append(lastFullLogSize);
        sb.append("\"/>");

        sb.append("<lastDeltaVersion lsn=\"");
        sb.append(DbLsn.getNoFormatString(lastDeltaLsn));
        sb.append("\" size=\"");
        sb.append(lastDeltaLogSize);
        sb.append("\"/>");

        return sb;
    }

    /** Never replicated. */
    public void dumpRep(@SuppressWarnings("unused") StringBuilder sb) {
    }
}
