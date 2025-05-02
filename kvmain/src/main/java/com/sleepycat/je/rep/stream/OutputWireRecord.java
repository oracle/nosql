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

package com.sleepycat.je.rep.stream;

import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogItem;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.entry.LNEntryInfo;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.log.entry.ReplicableLogEntry;
import com.sleepycat.je.tree.NameLN;
import com.sleepycat.je.txn.TxnCommit;
import com.sleepycat.je.txn.TxnEnd;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.VLSN;

/**
 * Format for log entries sent across the wire for replication. In most
 * cases, the bytes are read directly from the log and never need to be
 * serialized into the backing object.
 *
 * Note that the ByteBuffer held within the OutputWireRecord has a limited
 * lifetime. Often it's just sliced, rather than copied from the underlying
 * buffer.
 */
public class OutputWireRecord extends WireRecord {

    protected final ByteBuffer entryBuffer;
    protected final EnvironmentImpl envImpl;
    protected final LogItem logItem;

    /** A shared entry of the type specified by the header, or null */
    private ReplicableLogEntry sharedEntry = null;

    /** A log entry created from the data in the entry buffer, or null */
    protected ReplicableLogEntry logEntry = null;

    /*
     * TODO: add a way to get DB ID, key, and array/offset of LN data from a
     *  wire record _without_ instantiating the log entry. Feeder filters
     *  (stream subscriptions) need this info, and if they instantiate the log
     *  entry, this will negate the optimization of the LogItem.cachedBuffer.
     */

    /** Whether the log entry will be re-serialized, or null if unknown. */
    private Boolean reserialize = null;

    /** Size of re-serialized log entry, or -1 if reserialize != true. */
    protected int reSerializedSize = -1;

    /** Whether an old log format must be used, or null if unknown. */
    private Boolean oldFormatRequired = null;

    /**
     * Make a OutputWireRecord from FileReader output for sending out.
     *
     * @param entryBuffer is a new slice of the reader buffer.
     */
    protected OutputWireRecord(final EnvironmentImpl envImpl,
                     final LogEntryHeader header,
                     final ByteBuffer entryBuffer) {
        super(header);
        this.envImpl = envImpl;
        this.logItem = null;
        this.entryBuffer = entryBuffer;

        assert entryBuffer.remaining() == header.getItemSize() :
            "remaining:" + entryBuffer.remaining() +
            " itemSize:" + header.getItemSize();
    }

    /**
     * Creates an OutputWireRecord from a log item. This constructor is used
     * when a Feeder can bypass access to the log because the log item is
     * available in the log item cache associated with the VLSNIndex.
     */
    protected OutputWireRecord(final EnvironmentImpl envImpl,
                               final LogItem logItem) {
        super(logItem.header);
        this.envImpl = envImpl;
        this.logItem = logItem;

        final ByteBuffer buffer = logItem.getBuffer();
        if (buffer != null) {
            /* Create a slice because LogItem is shared by multiple feeders. */
            entryBuffer = buffer.slice();

            assert entryBuffer.remaining() == header.getItemSize() :
                "remaining:" + entryBuffer.remaining() +
                    " itemSize:" + header.getItemSize();
        } else {
            assert logItem.cachedEntry != null;
            logEntry = logItem.cachedEntry;
            entryBuffer = null;
        }
    }

    /* For unit test support. */
    protected OutputWireRecord(final EnvironmentImpl envImpl,
                     final InputWireRecord input) {
        super(input.header);
        this.envImpl = envImpl;
        this.logItem = null;
        final LogEntry entry = input.getLogEntry();
        this.entryBuffer = ByteBuffer.allocate(entry.getSize());
        entry.writeEntry(entryBuffer);
        entryBuffer.flip();
    }

    public void decrementUse() {
        if (logItem != null) {
            logItem.decrementUse();
        }
    }

    /**
     * Returns the shared replicable log entry associated with the log entry
     * header.
     */
    private synchronized ReplicableLogEntry getSharedEntry()
        throws DatabaseException {

        if (sharedEntry == null) {
            final LogEntryType entryType = getLogEntryType();
            if (!entryType.isReplicationPossible()) {
                throw EnvironmentFailureException.unexpectedState(
                    "Log entry type does not support replication: " + entryType);
            }
            sharedEntry = (ReplicableLogEntry) entryType.getSharedLogEntry();
        }
        return sharedEntry;
    }

    /**
     * Returns a log entry corresponding to the entry buffer. Note that the log
     * entry will only be created once, at most.
     * <p>
     * When a LogItem from the LogItemCache was used to construct this record,
     * we cache the materialized entry in LogItem to try to avoid redundant
     * materialization in multiple feeders.
     * <p>
     * To avoid allocating memory unnecessarily and defeating the optimization
     * provided by the LogItemCache, in general we should avoid calling
     * instantiateEntry. For LNs in particular (which are heavy weight), call
     * {@link #getLNEntryInfo} instead whenever possible.
     * <p>
     * However, when LogItem.cachedEntry is initially set (see also
     * LogParams.immutableLogEntry), the entry is pre-instantiated so there is
     * no cost to calling instantiateEntry. This is true for commits and
     * aborts, which also means that {@link #getCommitAbortTxnId()} and
     * {@link #getCommitTimeStamp} use a pre-instantiated entry.
     */
    public synchronized ReplicableLogEntry instantiateEntry()
        throws DatabaseException {

        if (logEntry != null) {
            return logEntry;
        }
        if (logItem != null) {
//            logItem.verifyBufferInUse();
            logEntry = logItem.cachedEntry;
            if (logEntry != null) {
                return logEntry;
            }
        }
        final LogEntry entry = instantiateEntry(envImpl, entryBuffer);
        if (!(entry instanceof ReplicableLogEntry)) {
            throw EnvironmentFailureException.unexpectedState(
                "Log entry type does not support replication: " +
                entry.getClass().getName());
        }
        logEntry = (ReplicableLogEntry) entry;
        if (logItem != null) {
            logItem.cachedEntry = logEntry;
        }
        return logEntry;
    }

    /**
     * @return the log entry type for this record.
     */
    public byte getEntryType() {
        return header.getType();
    }

    /**
     * Used at syncup, when comparing records received from the feeder against
     * local records.
     *
     * @return true if this OutputWireRecord has the same logical contents as
     * the InputWireRecord.  The comparison will disregard portions of the
     * logEntry that may be different, such at timestamps on a Commit
     * entry. Must be called before the entryBuffer that backs this
     * OutputWireRecord is reused.
     * @throws DatabaseException
     */
    public boolean match(final InputWireRecord input)
        throws DatabaseException {

        /*
         * Ignore the log version check if the log versions on the feeder and
         * replica don't match. This would happen if the group is doing an
         * upgrade that requires a log version change.
         */
        if (!header.logicalEqualsIgnoreVersion(input.header)) {
            return false;
        }

        final LogEntry entry = instantiateEntry();
        return entry.logicalEquals(input.getLogEntry());
    }

    /**
     * For unit tests.
     * @return true if this OutputWireRecord has the same logical contents as
     * "other".
     * @throws DatabaseException
     */
    public boolean match(final OutputWireRecord otherRecord)
        throws DatabaseException {

        if (!header.logicalEqualsIgnoreVersion(otherRecord.header)) {
            return false;
        }

        final LogEntry entry = instantiateEntry();
        final LogEntry otherEntry = otherRecord.instantiateEntry();
        return entry.logicalEquals(otherEntry);
    }

    /**
     * Returns lsn if this entry was created from the log item cache, and
     * NULL_LSN otherwise.
     */
    long getLogItemLSN() {
       return (logItem != null) ? logItem.lsn : DbLsn.NULL_LSN;
    }

    public long getVLSN() {
        return header.getVLSN();
    }

    /**
     * Dump the contents.
     * @throws DatabaseException
     */
    public String dump()
        throws DatabaseException {

        final StringBuilder sb = new StringBuilder();
        header.dumpRep(sb);
        final LogEntry entry = instantiateEntry();
        entry.dumpRep(sb);
        return sb.toString();
    }

    @Override
    public String toString() {
        try {
           return dump();
        } catch (DatabaseException e) {
            LoggerUtils.warning(envImpl.getLogger(),envImpl,e.getMessage());
           return "";
        }
    }

    /**
     * Returns the number of bytes needed to represent the message data for this
     * record for the specified log version.
     */
    protected int getWireSize(final int logVersion) {
        return 1 + 4 + 4 + VLSN.LOG_SIZE + getEntrySize(logVersion);
    }

    /**
     * Returns the number of bytes needed to represent the entry portion of the
     * message data for this record for the specified log version.
     */
    protected int getEntrySize(final int logVersion) {
        return willReSerialize(logVersion) ?
            reSerializedSize : header.getItemSize();
    }

    /**
     * Returns whether the log entry will be re-serialized when written, due to
     * a required format change, an optimized replication format should be
     * used, or no entry buffer (only a cached entry) is available.
     *
     * This method caches its result, so that it can be called twice (from
     * getEntrySize and writeToWire) and will not repeat the calculation or
     * return different results. If true is returned, reSerializedSize will
     * also be set.
     */
    protected boolean willReSerialize(final int logVersion) {

        if (reserialize != null) {
            return reserialize;
        }

        int newSize = -1;

        if (entryBuffer == null) {
            /* Only a cached entry was supplied via LogItem. */
            reserialize = true;
            assert logEntry != null;

        } else if (isOldFormatRequired(logVersion)) {
            /* Re-serialization is mandatory. */
            reserialize = true;

        } else {
            /* Determine whether re-serialization is worthwhile. */
            if (logEntry != null) {
                /*
                 * If we have the entry, then using the optimized replication
                 * format is worthwhile simply if the entry has an optimized
                 * format and its size is smaller, since the cost of
                 * re-serialization is fairly low.
                 */
                if (logEntry.hasReplicationFormat()) {
                    newSize = logEntry.getSize(
                        logVersion, true /*forReplication*/);
                    reserialize = header.getItemSize() > newSize;
                } else {
                    reserialize = false;
                }
            } else {
                /*
                 * If we must materialize the entry in order to re-serialize
                 * it, then we must make a best guess about whether this is
                 * worthwhile by examining the entry in serialized format.
                 */
                if (envImpl != null && envImpl.optimizeRepStreamForSize()) {
                    reserialize =
                        getSharedEntry().isReplicationFormatWorthwhile(
                            entryBuffer, header.getVersion(), logVersion);
                } else {
                    reserialize = false;
                }
            }
        }

        assert reserialize != null;

        if (reserialize) {
            if (newSize == -1) {
                newSize = instantiateEntry().getSize(
                    logVersion, true /*forReplication*/);
            }
            reSerializedSize = newSize;
        }

        return reserialize;
    }

    /**
     * Returns whether the format of the entry needs to be changed in order to
     * be read by a replica that only understands versions no later than {@code
     * logVersion}.
     */
    protected boolean isOldFormatRequired(final int logVersion) {

        if (oldFormatRequired != null) {
            return oldFormatRequired;
        }

        oldFormatRequired =
            /* The requested version is older than the current version, */
            logVersion < LogEntryType.LOG_VERSION &&
            /* it is older than the entry version, */
            logVersion < header.getVersion() &&
            /* and it is older than the entry class's last format change */
            logVersion < (getSharedEntry().getLastFormatChange());

        return oldFormatRequired;
    }

    /**
     * Write the log header and entry associated with this instance to the
     * specified buffer using the format for the specified log version.
     *
     * @param messageBuffer the destination buffer
     * @param logVersion the log version of the format
     * @return whether the data format was changed to support an old version.
     */
    protected boolean writeToWire(final ByteBuffer messageBuffer,
                        final int logVersion) {

        messageBuffer.put(header.getType());
        if (willReSerialize(logVersion)) {
            final ReplicableLogEntry entry = instantiateEntry();
            LogUtils.writeInt(messageBuffer, logVersion);
            LogUtils.writeInt(messageBuffer, reSerializedSize);
            LogUtils.writeLong(messageBuffer, header.getVLSN());
            entry.writeEntry(messageBuffer, logVersion, true /*forReplication*/);
        } else {
            LogUtils.writeInt(messageBuffer, header.getVersion());
            LogUtils.writeInt(messageBuffer, header.getItemSize());
            LogUtils.writeLong(messageBuffer, header.getVLSN());
            entryBuffer.mark();
            messageBuffer.put(entryBuffer);
            entryBuffer.reset();
        }
        return isOldFormatRequired(logVersion);
    }

    /**
     * Returns the transaction id associated with a commit/abort log entry.
     * <p>
     * LogItem.cachedEntry is used for commits and aborts, so this method
     * always uses pre-instantiated entry, i.e., it is low cost.
     *
     * @return the transaction id, if it's a commit/abort record, zero
     * otherwise.
     */
    public long getCommitAbortTxnId() throws DatabaseException {

        if (!LogEntryType.LOG_TXN_COMMIT.equalsType(header.getType()) &&
            !LogEntryType.LOG_TXN_ABORT.equalsType(header.getType())) {
            return 0;
        }

        final LogEntry commitEntry = instantiateEntry();
        return commitEntry.getTransactionId();
    }

    /**
     * Returns the timestamp associated with a commit log entry, or 0.
     * <p>
     * LogItem.cachedEntry is used for commits and aborts, so this method
     * always uses pre-instantiated entry, i.e., it is low cost.
     *
     * @return the commit timestamp or 0
     */
    public long getCommitTimeStamp()
        throws DatabaseException {

        if (!LogEntryType.LOG_TXN_COMMIT.equalsType(header.getType())) {
            return 0;
        }

        final TxnCommit txnCommit =
            (TxnCommit) instantiateEntry().getMainItem();
        return txnCommit.getTime().getTime();
    }

    /**
     * Returns the timestamp associated with transaction ending log entry, or
     * zero if doesn't end a transaction.
     *
     * @return the timestamp or zero
     */
    public long getTimeStamp()
        throws DatabaseException {

        final byte type = header.getType();
        if (!LogEntryType.LOG_TXN_COMMIT.equalsType(type) &&
            !LogEntryType.LOG_TXN_ABORT.equalsType(type)) {
            return 0;
        }

        final TxnEnd txnEnd = (TxnEnd) instantiateEntry().getMainItem();
        return txnEnd.getTime().getTime();
    }

    /**
     * Returns information about an LNLogEntry wire record.
     *
     * <p>Used to obtain information about an LNLogEntry without having to
     * incur the costs (mainly memory allocations) of fully instantiating it
     * from serialized form. Note that the wire record may already be
     * instantiated, in which case the LNLogEntry fields are returned without
     * having to parse the serialized entry.</p>
     *
     * @param lnInfo structure used to return LN log entry information.
     *
     * @return true if the wire record is an LNLogEntry, and its information is
     * returned in lnInfo; otherwise, returns false and lnInfo is unchanged.
     *
     * @since 19.5
     */
    public boolean getLNEntryInfo(final LNEntryInfo lnInfo) {

        if (!getLogEntryType().isLNType()) {
            return false;
        }

        LNLogEntry.parseEntry(entryBuffer, header, lnInfo);
        return true;
    }

    /*
     * Unit test and assertion support: Transaction, database and node IDs in
     * the replication sequences are supposed to occupy the negative
     * numberspace.
     *
     * It seems a little fragile to test this here, using instanceof to decide
     * what to test. It would be cleaner to put this validity check as part of
     * the implementation of a log entry. But this is a HA related check, and
     * we want to keep the core code as independent as possible. The check is
     * here rather than in some other test code because it needs to access the
     * deserialized log entry. We don't want to provide a method which returns
     * a logEntry, because in general an OutputWireRecord should not
     * instantiate the log entry.
     *
     * @throws RuntimeException if there are any sequences that are not
     * negative.
     */
    public boolean verifyNegativeSequences(final String debugTag) {

        LogEntry entry = null;
        try {
            entry = instantiateEntry();
        } catch (DatabaseException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }

        if (entry.getTransactionId() >= 0) {
            throw EnvironmentFailureException.unexpectedState
                (debugTag + " txn id should be negative: " + entry);
        }

        if (entry instanceof LNLogEntry) {
            if (LogEntryType.LOG_NAMELN_TRANSACTIONAL.equalsType
                (getEntryType())) {
                final LNLogEntry<?> lnEntry = (LNLogEntry<?>) entry;
                lnEntry.postFetchInit(false /*isDupDb*/);
                final NameLN nameLN = (NameLN) lnEntry.getLN();
                if (nameLN.getId().getId() >= 0) {
                    throw EnvironmentFailureException.unexpectedState
                        (debugTag + " db id should be negative: " + entry);
                }
            } else {
                if (entry.getDbId().getId() >= 0) {
                    throw EnvironmentFailureException.unexpectedState
                        (debugTag + " db id should be negative: " + entry);
                }
            }
        }

        return true;
    }
}
