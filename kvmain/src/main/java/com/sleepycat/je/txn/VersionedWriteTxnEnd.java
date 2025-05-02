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

package com.sleepycat.je.txn;

import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;
import static com.sleepycat.je.utilint.VLSN.UNINITIALIZED_VLSN;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.VersionedWriteLoggable;
import com.sleepycat.je.rep.impl.node.MasterIdTerm;
import com.sleepycat.je.rep.impl.node.MasterTerm;
import com.sleepycat.je.statcap.StatUtils;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.util.PackedInteger;

/**
 * Based class for commit and abort records, which are replicated.
 * The log formats for commit and abort are identical.
 */
public abstract class VersionedWriteTxnEnd
    extends TxnEnd implements VersionedWriteLoggable {

    /**
     * The log version of the most recent format change for this loggable.
     *
     * @see #getLastFormatChange
     */
    private static final int LAST_FORMAT_CHANGE =
        LogEntryType.LOG_VERSION_MASTER_TERM;

    VersionedWriteTxnEnd(long id,
                         long lastLsn,
                         Supplier<MasterIdTerm> masterIdTermSupplier,
                         long dtvlsn) {
        super(id, lastLsn, masterIdTermSupplier, dtvlsn);
    }

    /**
     * For constructing from the log during tests.
     */
    public VersionedWriteTxnEnd() {
    }

    /*
     * Log support for writing.
     */

    @Override
    public int getLastFormatChange() {
        return LAST_FORMAT_CHANGE;
    }

    @Override
    public Collection<VersionedWriteLoggable> getEmbeddedLoggables() {
        return Collections.emptyList();
    }

    @Override
    public int getLogSize() {
        return getLogSize(LogEntryType.LOG_VERSION, false /*forReplication*/);
    }

    @Override
    public void writeToLog(final ByteBuffer logBuffer) {
        writeToLog(
            logBuffer, LogEntryType.LOG_VERSION, false /*forReplication*/);
    }

    @Override
    public int getLogSize(final int logVersion, final boolean forReplication) {

        if (dtvlsn == NULL_VLSN) {
            throw new IllegalStateException("DTVLSN is null");
        }

        return LogUtils.getPackedLongLogSize(id) +
            LogUtils.getTimestampLogSize(time) +
            LogUtils.getPackedLongLogSize(
                forReplication ? DbLsn.NULL_LSN : lastLsn) +
            LogUtils.getPackedIntLogSize(masterId) +
            ((logVersion >= LogEntryType.LOG_VERSION_DURABLE_VLSN) ?
             LogUtils.getPackedLongLogSize(dtvlsn) : 0) +
            ((logVersion >= LogEntryType.LOG_VERSION_MASTER_TERM) ?
                LogUtils.getPackedLongLogSize(checkMasterTerm()) : 0);
    }

    @Override
    public void writeToLog(final ByteBuffer logBuffer,
                           final int entryVersion,
                           final boolean forReplication) {

        if (entryVersion >= 12) {
            LogUtils.writePackedLong(logBuffer,
                forReplication ? DbLsn.NULL_LSN : lastLsn);
        }
        LogUtils.writePackedLong(logBuffer, id);
        LogUtils.writeTimestamp(logBuffer, time);
        if (entryVersion < 12) {
            LogUtils.writePackedLong(logBuffer,
                forReplication ? DbLsn.NULL_LSN : lastLsn);
        }
        LogUtils.writePackedInt(logBuffer, masterId);

        if (entryVersion >= LogEntryType.LOG_VERSION_DURABLE_VLSN) {
            if (dtvlsn == NULL_VLSN) {
                throw new IllegalStateException("Unexpected null dtvlsn");
            }
            LogUtils.writePackedLong(logBuffer, dtvlsn);

            if (entryVersion >= LogEntryType.LOG_VERSION_MASTER_TERM) {
                LogUtils.writePackedLong(logBuffer, checkMasterTerm());
            }
        }
    }

    /**
     * Check the master term and return it if it's a valid value for this
     * log entry. This check must be invoked before the entry is written
     * to the log or after it has been read from the log.
     *
     * @throws IllegalStateException if the term value is invalid
     */
    private long checkMasterTerm() {
        if (!TxnManager.isReplicatedTxn(id)) {
            if (masterTerm != MasterTerm.NULL) {
                throw new IllegalStateException("Expected NULL term");
            }
            return masterTerm;
        }

        return MasterTerm.check(masterTerm);
    }

    @Override
    public void readFromLog(EnvironmentImpl envImpl,
                            ByteBuffer logBuffer,
                            int entryVersion) {

        if (entryVersion >= 12) {
            lastLsn = LogUtils.readPackedLong(logBuffer);
        }
        id = LogUtils.readPackedLong(logBuffer);
        time = LogUtils.readTimestamp(logBuffer);
        if (entryVersion < 12) {
            lastLsn = LogUtils.readPackedLong(logBuffer);
        }
        masterId = LogUtils.readPackedInt(logBuffer);

        /* The default value if one is not available in the entry */
        masterTerm = MasterTerm.PRETERM_TERM;

        if (entryVersion >= LogEntryType.LOG_VERSION_DURABLE_VLSN) {
            dtvlsn = LogUtils.readPackedLong(logBuffer);
            if (dtvlsn == NULL_VLSN) {
                throw new IllegalStateException("Unexpected null dtvlsn");
            }
            if (entryVersion >= LogEntryType.LOG_VERSION_MASTER_TERM) {
                masterTerm = LogUtils.readPackedLong(logBuffer);
                checkMasterTerm();
            }
        } else {
            /*
             * Distinguished value to make it clear that the value was derived
             * from an old log entry.
             */
            dtvlsn = UNINITIALIZED_VLSN;
        }
    }

    @Override
    public boolean hasReplicationFormat() {
        return true;
    }

    @Override
    public boolean isReplicationFormatWorthwhile(final ByteBuffer logBuffer,
                                                 final int srcVersion,
                                                 final int destVersion) {
        /*
         * It is too much trouble to parse versions older than 12, because the
         * lastLsn is not at the front in older versions.
         */
        if (srcVersion < 12) {
            return false;
        }

        /*
         * If the size of lastLsn is greater than one (meaning it is not
         * NULL_LSN), then we should re-serialize.
         */
        return PackedInteger.getReadLongLength(
            logBuffer.array(),
            logBuffer.arrayOffset() + logBuffer.position()) > 1;
    }

    public boolean logicalEquals(TxnEnd other) {
        return  (id == other.id) &&
                (masterId == other.masterId) &&
                /* Ignore term comparison if any one of the records is pre term
                 * as can happen if one of the nodes is pre term */
                ((MasterTerm.isPreTerm(masterTerm) ||
                  MasterTerm.isPreTerm(other.masterTerm)) ||
                 (masterTerm == other.masterTerm));
    }

    @Override
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append("<").append(getTagName());
        sb.append(" id=\"").append(id);
        sb.append("\" time=\"").append(StatUtils.getDate(time));
        sb.append("\" master=\"").append(masterId);
        sb.append("\" masterTerm=\"").append(masterTerm);
        sb.append("\" dtvlsn=\"").append(dtvlsn);
        sb.append("\">");
        sb.append(DbLsn.toString(lastLsn));
        sb.append("</").append(getTagName()).append(">");
    }
}
