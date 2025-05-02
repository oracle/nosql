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

import static com.sleepycat.je.EnvironmentFailureException.unexpectedState;
import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DupKeyData;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.VersionedWriteLoggable;
import com.sleepycat.je.statcap.StatUtils;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.VLSN;

/**
 * An LNLogEntry is the in-memory image of an LN logrec describing a write op
 * (insertion, update, or deletion) performed by a locker T on a record R.
 * T always locks R in exclusive (WRITE or WRITE_RANGE) mode before performing
 * any write ops on it, and it retains its exclusive lock on R until it
 * terminates (commits or aborts). (Non-transactional lockers can be viewed as
 * "simple" transactions that perform at most one write op, and then
 * immediately commit).
 *
 * On disk, an LN logrec contains :
 *
 * {@literal
 * 6 <= versions <= 10 :
 *
 *   databaseid
 *   abortLsn             -- if transactional
 *   abortKnownDeleted    -- if transactional
 *   txn id               -- if transactional
 *   prev LSN of same txn -- if transactional
 *   data
 *   key
 *
 * 11 == version :
 *
 *   databaseid
 *   abortLsn               -- if transactional
 *   1-byte flags
 *     abortKnownDeleted
 *     embeddedLN
 *     haveAbortKey
 *     haveAbortData
 *     haveAbortVLSN
 *   txn id                 -- if transactional
 *   prev LSN of same txn   -- if transactional
 *   abort key              -- if haveAbortKey
 *   abort data             -- if haveAbortData
 *   abort vlsn             -- if haveAbortVLSN
 *   data
 *   key
 *
 *   In forReplication mode, these flags and fields are omitted:
 *     embeddedLN, haveAbortKey, haveAbortData, haveAbortVLSN,
 *     abort key, abort data, abort vlsn
 *
 * 12 <= version :
 *
 *   1-byte flags
 *     abortKnownDeleted
 *     embeddedLN
 *     haveAbortKey
 *     haveAbortData
 *     haveAbortVLSN
 *     haveAbortLSN
 *     haveAbortExpiration
 *     haveExpiration
 *   databaseid
 *   abortLsn                -- if transactional and haveAbortLSN
 *   txn id                  -- if transactional
 *   prev LSN of same txn    -- if transactional
 *   abort key               -- if haveAbortKey
 *   abort data              -- if haveAbortData
 *   abort vlsn              -- if haveAbortVLSN
 *   abort expiration        -- if haveAbortExpiration
 *   expiration              -- if haveExpiration
 *   data
 *   key
 *
 *   In forReplication mode, these flags and fields are omitted:
 *     abortKnownDeleted, embeddedLN, haveAbortKey, haveAbortData,
 *     haveAbortVLSN, abort key, abort data, abort vlsn,
 *     haveAbortLSN, abortLsn, haveAbortExpiration, abort expiration
 *
 * 16 <= version :
 *
 *   1-byte flags
 *     abortKnownDeleted
 *     embeddedLN
 *     haveAbortKey
 *     haveAbortData
 *     haveAbortVLSN
 *     haveAbortLSN
 *     haveAbortExpiration
 *     haveExpiration
 *   1-byte flags2
 *     havePriorSize
 *     havePriorFile
 *   databaseid
 *   abortLsn                -- if transactional and haveAbortLSN
 *   txn id                  -- if transactional
 *   prev LSN of same txn    -- if transactional
 *   abort key               -- if haveAbortKey
 *   abort data              -- if haveAbortData
 *   abort vlsn              -- if haveAbortVLSN
 *   abort expiration        -- if haveAbortExpiration
 *   expiration              -- if haveExpiration
 *   priorSize               -- if havePriorSize
 *   priorFile               -- if havePriorFile
 *   data
 *   key
 * }
 *
 *   In forReplication mode, these flags and fields are omitted:
 *     abortKnownDeleted, embeddedLN, haveAbortKey, haveAbortData,
 *     haveAbortVLSN, abort key, abort data, abort vlsn,
 *     haveAbortLSN, abortLsn, haveAbortExpiration, abort expiration,
 *     havePriorSize, priorSize, havePriorFile, priorFile
 *
 * {@literal
 * 19 <= version :
 *
 *   1-byte flags
 *     abortKnownDeleted
 *     embeddedLN
 *     haveAbortKey
 *     haveAbortData
 *     haveAbortVLSN
 *     haveAbortLSN
 *     haveAbortExpiration
 *     haveExpiration
 *   1-byte flags2
 *     havePriorSize
 *     havePriorFile
 *     tombstone                                              << new
 *     abortTombstone                                         << new
 *     haveAbortModificationTime                              << new
 *   databaseid
 *   expiration              -- if haveExpiration
 *   modificationTime                                         << new
 *   key
 *   data
 *   abortLsn                -- if transactional and haveAbortLSN
 *   txn id                  -- if transactional
 *   prev LSN of same txn    -- if transactional
 *   abort key               -- if haveAbortKey
 *   abort data              -- if haveAbortData
 *   abort vlsn              -- if haveAbortVLSN
 *   abort expiration        -- if haveAbortExpiration
 *   abort modification time -- if haveAbortModificationTime  << new
 *   priorSize               -- if havePriorSize
 *   priorFile               -- if havePriorFile
 * }
 *
 *   Expiration, key and data fields were moved up to support partial reads.
 *
 *   In forReplication mode, these flags and fields are omitted:
 *     abortKnownDeleted, embeddedLN, haveAbortKey, haveAbortData,
 *     haveAbortVLSN, abort key, abort data, abort vlsn,
 *     haveAbortLSN, abortLsn, haveAbortExpiration, abort expiration,
 *     havePriorSize, priorSize, havePriorFile, priorFile,
 *     haveAbortModificationTime, abortModificationTime, abortTombstone.
 *
 * {@literal
 * 25 <= version :
 *
 *   1-byte flags
 *     abortKnownDeleted
 *     embeddedLN
 *     haveAbortKey
 *     haveAbortData
 *     haveAbortVLSN
 *     haveAbortLSN
 *     haveAbortExpiration
 *     haveExpiration
 *   1-byte flags2
 *     havePriorSize
 *     havePriorFile
 *     enableBeforeImage                                        << new
 *     tombstone
 *     abortTombstone
 *     haveAbortModificationTime
 *   databaseid
 *   expiration              -- if haveExpiration
 *   modificationTime
 *   key
 *   data
 *   abortLsn                -- if transactional and haveAbortLSN
 *   txn id                  -- if transactional
 *   prev LSN of same txn    -- if transactional
 *   abort key               -- if haveAbortKey
 *   abort data              -- if haveAbortData
 *   abort vlsn              -- if haveAbortVLSN
 *   abort expiration        -- if haveAbortExpiration
 *   abort modification time -- if haveAbortModificationTime
 *   priorSize               -- if havePriorSize
 *   priorFile               -- if havePriorFile
 * }
 *
 *   In forReplication mode, these flags and fields are omitted:
 *     abortKnownDeleted, embeddedLN, haveAbortKey, haveAbortData,
 *     haveAbortVLSN, abort key, abort data, abort vlsn,
 *     haveAbortLSN, abortLsn, haveAbortExpiration, abort expiration,
 *     havePriorSize, priorSize, havePriorFile, priorFile,
 *     haveAbortModificationTime, abortModificationTime, abortTombstone.
 *
 * NOTE: LNLogEntry is sub-classed by NameLNLogEntry, which adds some extra
 * fields after the fields shown above.  NameLNLogEntry never has a
 * Before Image.
 */
public class LNLogEntry<T extends LN> extends BaseReplicableEntry<T> {

    /* flags */
    private static final byte ABORT_KD_MASK = 0x1;
    private static final byte EMBEDDED_LN_MASK = 0x2;
    private static final byte HAVE_ABORT_KEY_MASK = 0x4;
    private static final byte HAVE_ABORT_DATA_MASK = 0x8;
    private static final byte HAVE_ABORT_VLSN_MASK = 0x10;
    private static final byte HAVE_ABORT_LSN_MASK = 0x20;
    private static final byte HAVE_ABORT_EXPIRATION_MASK = 0x40;
    private static final byte HAVE_EXPIRATION_MASK = (byte) 0x80;
    /* flags2 */
    private static final byte HAVE_PRIOR_SIZE_MASK = 0x1;
    private static final byte HAVE_PRIOR_FILE_MASK = 0x2;
    private static final byte TOMBSTONE_MASK = 0x4;
    private static final byte ABORT_TOMBSTONE_MASK = 0x8;
    private static final byte HAVE_ABORT_MODIFICATION_TIME_MASK = 0x10;
    private static final byte BLIND_DELETION_MASK = 0x20;
    private static final byte ENABLE_BEFORE_IMAGE_MASK = 0x40;

    /**
     * Used for computing the minimum log space used by an LNLogEntry.
     */
    public static final int MIN_LOG_SIZE = 2 + // Flags
                                           1 + // DatabaseId
                                           5 + // ModificationTime
                                           1 + // LN with zero-length data
                                           LogEntryHeader.MIN_HEADER_SIZE;

    /**
     * The log version when the most recent format change for this entry was
     * made (including any changes to the format of the underlying LN and other
     * loggables).
     *
     * @see #getLastFormatChange
     */
    private static final int LAST_FORMAT_CHANGE = 19;

    /*
     * Persistent fields.
     */

    /*
     * The id of the DB containing the record.
     */
    private DatabaseId dbId;

    /*
     * The Txn performing the write op. It is null for non-transactional DBs.
     * On disk we store only the txn id and the LSN of the previous logrec
     * (if any) generated by this txn.
     */
    private Txn txn;

    /*
     * The LSN of the record's "abort" version, i.e., the version to revert to
     * if this logrec must be undone as a result of a txn abort. It is set to
     * the most recent version before the record was locked by the locker T
     * associated with this logrec. Because T locks R before it writes it, the
     * abort version is always a committed version.
     *
     * It is null for non-transactional lockers, because such lockers never
     * abort.
     */
    private long abortLsn = DbLsn.NULL_LSN;

    /*
     * Whether the record's abort version was a deleted version or not.
     */
    private boolean abortKnownDeleted;

    /*
     * The key of the record's abort version, if haveAbortKey is true;
     * null otherwise.
     */
    private byte[] abortKey = null;

    /*
     * The data portion of the record's abort version, if haveAbortData is
     * true; null otherwise.
     */
    private byte[] abortData = null;

    /*
     * The VLSN of the record's abort version, if haveAbortVLSN is true;
     * NULL_VLSN otherwise.
     */
    private long abortVLSN = NULL_VLSN;

    /* Abort expiration time in days or hours. */
    private int abortExpiration = 0;
    private boolean abortExpirationInHours = false;

    /* Abort modification time in millis. */
    private long abortModificationTime = 0;

    /* Abort tombstone property. */
    private boolean abortTombstone = false;

    /*
     * True if the logrec stores an abort LSN, which is the case only if
     * (a) this is a transactional logrec (b) the abort LSN is non-null.
     */
    private boolean haveAbortLSN;

    /*
     * True if the logrec stores an abort key, which is the case only if
     * (a) this is a transactional logrec, (b) the record's abort version
     * was embedded in the BIN, and (c) the DB allows key updates.
     */
    private boolean haveAbortKey;

    /*
     * True if the logrec stores abort data, which is the case only if
     * (a) this is a transactional logrec and (b) the record's abort
     * version was embedded in the BIN.
     */
    private boolean haveAbortData;

    /*
     * True if the logrec stores an abort VLSN, which is the case only if
     * (a) this is a transactional logrec (b) the record's abort version
     * was embedded in the BIN, and (c) VLSN caching is enabled.
     */
    private boolean haveAbortVLSN;

    /*
     * True if the logrec stores an abort expiration, which is the case only if
     * (a) this is a transactional logrec (b) the record's abort version has a
     * non-zero expiration.
     */
    private boolean haveAbortExpiration;

    /*
     * True if the logrec stores an abort modification time, which is the case
     * only if (a) this is a transactional logrec, and (b) the record's abort
     * version was embedded in the BIN.
     */
    private boolean haveAbortModificationTime;
    
    /*
     * True if the logrec stores the before image which would be empty for 
     * inserts but non-empty for updates and deletes
     */
    private boolean enableBeforeImage;

    /*
     * True if the logrec stores a non-zero expiration.
     */
    private boolean haveExpiration;

    /*
     * True if the logrec stores the size of the obsolete prior version.
     * Used to count the prior version obsolete during recovery.
     *
     * The size is stored if has the LN has prior version (it is not a pure
     * insertion) and the prior version is not immediately obsolete.
     */
    private boolean havePriorSize;

    /*
     * True if the logrec stores the file of the obsolete prior version.
     * Used to count the prior version obsolete during recovery. If false and
     * havePriorSize is true, the file of the abortLsn is used.
     *
     * The file number is stored only if havePriorSize is true. And it is
     * stored only if the abortLsn is absent (because the LN is not txnal) or
     * the abortLsn not the prior prior version (because the prior version is
     * part of the same txn).
     */

    private boolean havePriorFile;

    /*
     * Whether, after the write op described by this logrec, the record is
     * embedded in the BIN or not.
     */
    private boolean embeddedLN;

    /*
     * The LN storing the record's data, after the write op described by this
     * logrec. The ln has a null data value if the write op is a deletion. For
     * replicated DBs, the ln contains the record's VLSN as well.
     */
    private LN ln;

    /*
     * The value of the record's key, after the write op described by this
     * logrec.
     */
    private byte[] key;

    /* Expiration time in days or hours. */
    private int expiration;
    private boolean expirationInHours;

    /* Modification time as Java time in millis. */
    private long modificationTime;

    /* Whether the record is a tombstone. */
    private boolean tombstone;

    /* Whether a deletion was done blindly (secondary DBs only). */
    private boolean blindDeletion = false;

    /* Use for obsolete counting during recovery. */
    private int priorSize;
    private long priorFile = DbLsn.MAX_FILE_NUM;

    /* Transient field for getUserKeyData. Is null if status is unknown. */
    private Boolean dupStatus;

    /**
     * Creates an instance to read an entry.
     *
     * @param <T> the type of the contained LN
     * @param cls the class of the contained LN
     * @return the log entry
     */
    public static <T extends LN> LNLogEntry<T> create(final Class<T> cls) {
        return new LNLogEntry<>(cls);
    }

    /* Constructor to read an entry. */
    protected LNLogEntry(final Class<T> cls) {
        super(cls);
    }

    /* Constructor to write an entry. */
    public LNLogEntry(
        LogEntryType entryType,
        DatabaseId dbId,
        Txn txn,
        long abortLsn,
        boolean abortKD,
        byte[] abortKey,
        byte[] abortData,
        long abortVLSN,
        int abortExpiration,
        boolean abortExpirationInHours,
        long abortModificationTime,
        boolean abortTombstone,
        byte[] key,
        T ln,
        boolean embeddedLN,
        int expiration,
        boolean expirationInHours,
        long modificationTime,
        boolean tombstone,
        boolean blindDeletion,
        int priorSize,
        long priorLsn,
        boolean enableBeforeImage) {

        setLogType(entryType);
        this.dbId = dbId;
        this.txn = txn;
        this.abortLsn = abortLsn;
        this.abortKnownDeleted = abortKD;
        this.abortKey = abortKey;
        this.abortData = abortData;
        this.abortVLSN = abortVLSN;
        this.abortExpiration = abortExpiration;
        this.abortExpirationInHours = abortExpirationInHours;
        this.abortModificationTime = abortModificationTime;
        this.abortTombstone = abortTombstone;
        this.enableBeforeImage = enableBeforeImage;

        haveAbortLSN = (abortLsn != DbLsn.NULL_LSN);
        haveAbortKey = (abortKey != null);
        haveAbortData = (abortData != null);
        haveAbortVLSN = !VLSN.isNull(abortVLSN);
        haveAbortExpiration = (abortExpiration != 0);
        haveExpiration = (expiration != 0);
        haveAbortModificationTime = (abortModificationTime != 0);

        this.embeddedLN = embeddedLN;
        this.key = key;
        this.ln = ln;
        this.expiration = expiration;
        this.expirationInHours = expirationInHours;
        this.modificationTime = modificationTime;
        this.tombstone = tombstone;
        this.blindDeletion = blindDeletion;

        this.priorSize = priorSize;
        havePriorSize = (priorSize != 0);

        if (havePriorSize == (priorLsn == DbLsn.NULL_LSN)) {
            throw EnvironmentFailureException.unexpectedState(
                "priorSize=" + priorSize +
                " priorLsn=" + DbLsn.getNoFormatString(priorLsn));
        }

        priorFile = (!havePriorSize || priorLsn == abortLsn) ?
            DbLsn.MAX_FILE_NUM : DbLsn.getFileNumber(priorLsn);

        havePriorFile = (priorFile != DbLsn.MAX_FILE_NUM);

        /* A txn should only be provided for transactional entry types. */
        assert(entryType.isTransactional() == (txn != null));
    }

    protected void reset() {
        dbId = null;
        txn = null;
        abortLsn = DbLsn.NULL_LSN;
        abortKnownDeleted = false;
        abortKey = null;
        abortData = null;
        abortVLSN = NULL_VLSN;
        abortExpiration = 0;
        abortExpirationInHours = false;
        abortModificationTime = 0;
        abortTombstone = false;

        haveAbortLSN = false;
        haveAbortKey = false;
        haveAbortData = false;
        haveAbortVLSN = false;
        haveAbortExpiration = false;
        haveExpiration = false;
        haveAbortModificationTime = false;
        havePriorSize = false;
        havePriorFile = false;

        embeddedLN = false;
        key = null;
        ln = null;
        expiration = 0;
        expirationInHours = false;
        modificationTime = 0;
        tombstone = false;
        blindDeletion = false;
        priorSize = 0;
        priorFile = DbLsn.MAX_FILE_NUM;

        dupStatus = null;
    }

    /**
     * Utility to populate a LNEntryInfo structure from the properties of an
     * LNLogEntry.
     *
     * TODO: Doesn't work for dup dbs because postFetchInit changes the LN and
     *  key fields for dup dbs. It is OK for the moment because it is only
     *  called for non-dup dbs. Remove this method after implementing
     *  parseEntry. This implementation is meant to be temporary.
     */
    public void getLNEntryInfo(final LNEntryInfo lnInfo) {
        assert dbId != null;
        assert key != null;

        lnInfo.databaseId = getDbId().getId();
        lnInfo.transactionId = (txn != null) ? txn.getId() : -1;
        lnInfo.modificationTime = getModificationTime();
        lnInfo.tombstone = isTombstone();
        lnInfo.data = ln.getData();
        lnInfo.dataOffset = 0;
        lnInfo.dataLength = (lnInfo.data != null) ? lnInfo.data.length : 0;
        lnInfo.key = key;
        lnInfo.keyOffset = 0;
        lnInfo.keyLength = lnInfo.key.length;
    }

    /**
     * Parses a log entry in a given buffer and returns information in the
     * LNEntryInfo structure.
     *
     * <p>Used to obtain information about an LNLogEntry without having to
     * incur the costs (mainly memory allocations) of fully instantiating
     * it.</p>
     *
     * <p>Note that {@link LNEntryInfo#key} and {@link LNEntryInfo#data}
     * are both set to the {@link ByteBuffer#array()} of the {@code buffer}
     * parameter value.</p>
     *
     * @since 19.5
     */
    public static void parseEntry(final ByteBuffer buffer,
                                  final LogEntryHeader header,
                                  final LNEntryInfo lnInfo) {

        Flags flag = new Flags();
        final long logVersion = header.getVersion();
        int recStartPosition = buffer.position();

        if (logVersion >= 12) {
            byte flags = buffer.get();
            byte flags2 = (logVersion >= 16) ? buffer.get() : (byte) 0;
            flag.setFlags(flags,flags2);
        }

        lnInfo.databaseId = LogUtils.readPackedLong(buffer);
        lnInfo.transactionId = -1;

        if (logVersion >= 19) {
            if (flag.haveExpiration) {
                LogUtils.readPackedInt(buffer);
            }
            lnInfo.modificationTime = LogUtils.readPackedLong(buffer);
            lnInfo.key = buffer.array();

            /*
             * read keyLength first so the position is set correctly
             */
            lnInfo.keyLength = LogUtils.readPackedInt(buffer);
            lnInfo.keyOffset = buffer.position() + buffer.arrayOffset();

            /*
             * set the buffer.position() manually
             */
            buffer.position(buffer.position() + lnInfo.keyLength);

            int size = LogUtils.readPackedInt(buffer);
            if (size >= 0) {
                lnInfo.data = buffer.array();
                lnInfo.dataLength = size;
                lnInfo.dataOffset = buffer.position() + buffer.arrayOffset();
                buffer.position(buffer.position() + size);
            }
        }

        LogEntryType type = LogEntryType.findType(header.getType());
        if (type.isTransactional()) {
            if (flag.haveAbortLSN || logVersion < 12) {
                LogUtils.readPackedLong(buffer);
            }

            if (logVersion < 12) {
                /*
                 * flag.haveAbortLSN is not used from here,
                 * so no need to change it as readBaseLNEntry does
                 */
                flag.setFlags(buffer.get(), (byte) 0);
            }

            lnInfo.transactionId = LogUtils.readPackedLong(buffer);
            LogUtils.readPackedLong(buffer);
        } else if (logVersion == 11) {
            flag.setFlags(buffer.get(), (byte) 0);
        }

        if (logVersion >= 11) {
            if (flag.haveAbortKey) {
                LogUtils.readByteArray(buffer);
            }
            if (flag.haveAbortData) {
                LogUtils.readByteArray(buffer);
            }
            if (flag.haveAbortVLSN) {
                LogUtils.readPackedLong(buffer);
            }
        }

        if (logVersion >= 12) {
            if (flag.haveAbortExpiration) {
                LogUtils.readPackedInt(buffer);
            }
        }

        if (logVersion >= 19) {
            if (flag.haveAbortModificationTime) {
                LogUtils.readPackedLong(buffer);
            }
        }

        if (logVersion >= 12 && logVersion < 19) {
            if (flag.haveExpiration) {
                LogUtils.readPackedInt(buffer);
            }
        }

        if (logVersion >= 16) {
            if (flag.havePriorSize) {
                LogUtils.readPackedInt(buffer);
            }
            if (flag.havePriorFile) {
                LogUtils.readPackedLong(buffer);
            }
        }

        if (logVersion < 19) {
            int size = LogUtils.readPackedInt(buffer);
            if (size >= 0) {
                lnInfo.data = buffer.array();
                lnInfo.dataLength = size;
                lnInfo.dataOffset = buffer.position() + buffer.arrayOffset();
                buffer.position(buffer.position() + size);
            }
            int keySize;
            boolean keyIsLastSerializedField =
                    logVersion >= 8 || type.isUserLNType();
            if (keyIsLastSerializedField) {
                int bytesWritten = buffer.position() - recStartPosition;
                keySize = header.getItemSize() - bytesWritten;
            } else {
                keySize = LogUtils.readPackedInt(buffer);
            }
            lnInfo.key = buffer.array();
            lnInfo.keyLength = keySize;
            lnInfo.keyOffset = buffer.position() + buffer.arrayOffset();
            buffer.position(buffer.position() + lnInfo.keyLength);
        }

        lnInfo.tombstone = flag.tombstone;

        buffer.position(recStartPosition);
    }

    static class Flags {

        boolean haveExpiration = false;
        boolean haveAbortLSN = false;
        boolean haveAbortKey = false;
        boolean haveAbortData = false;
        boolean haveAbortVLSN = false;
        boolean haveAbortExpiration = false;
        boolean haveAbortModificationTime = false;
        boolean enableBeforeImage = false;
        boolean havePriorSize = false;
        boolean havePriorFile = false;
        boolean tombstone = false;

        public void setFlags(byte flags, byte flags2) {
            haveExpiration = ((flags & HAVE_EXPIRATION_MASK) != 0);
            haveAbortLSN = ((flags & HAVE_ABORT_LSN_MASK) != 0);
            haveAbortKey = ((flags & HAVE_ABORT_KEY_MASK) != 0);
            haveAbortData = ((flags & HAVE_ABORT_DATA_MASK) != 0);
            haveAbortVLSN = ((flags & HAVE_ABORT_VLSN_MASK) != 0);
            haveAbortExpiration = ((flags & HAVE_ABORT_EXPIRATION_MASK) != 0);

            havePriorSize = ((flags2 & HAVE_PRIOR_SIZE_MASK) != 0);
            havePriorFile = ((flags2 & HAVE_PRIOR_FILE_MASK) != 0);
            haveAbortModificationTime =
                    ((flags2 & HAVE_ABORT_MODIFICATION_TIME_MASK) != 0);
            enableBeforeImage =
                    ((flags2 & ENABLE_BEFORE_IMAGE_MASK) != 0);
            tombstone = ((flags2 & TOMBSTONE_MASK) != 0);
        }
    }

    @Override
    public void readEntry(
        EnvironmentImpl envImpl,
        LogEntryHeader header,
        ByteBuffer entryBuffer) {

        /* Subclasses must call readBaseLNEntry. */
        assert getClass() == LNLogEntry.class;

        /*
         * Prior to version 8, the optimization to omit the key size was
         * mistakenly not applied to internal LN types such as FileSummaryLN
         * and MapLN, and was only applied to user LN types.  The optimization
         * should be applicable whenever LNLogEntry is not subclassed to add
         * additional fields. [#18055]
         */
        final boolean keyIsLastSerializedField =
            header.getVersion() >= 8 || entryType.isUserLNType();

        readBaseLNEntry(envImpl, header, entryBuffer,
                        keyIsLastSerializedField);
    }

    /**
     * Method shared by LNLogEntry subclasses.
     *
     * @param noMoreLastSerializedFields is true when the key length can be
     * omitted if the key is the last field of the base LN.This should be
     * false when an LNLogEntry subclass adds fields to the serialized format.
     */
    protected final void readBaseLNEntry(
        EnvironmentImpl envImpl,
        LogEntryHeader header,
        ByteBuffer entryBuffer,
        boolean noMoreLastSerializedFields) {

        reset();

        int logVersion = header.getVersion();
        int recStartPosition = entryBuffer.position();

        if (logVersion >= 12) {
            byte flags = entryBuffer.get();
            byte flags2 = (logVersion >= 16) ? entryBuffer.get() : (byte) 0;
            setFlags(flags, flags2);
        }

        ln = newInstanceOfType();

        dbId = new DatabaseId();
        dbId.readFromLog(envImpl, entryBuffer, logVersion);

        if (logVersion >= 19) {
            if (haveExpiration) {
                expiration = LogUtils.readPackedInt(entryBuffer);
                if (expiration < 0) {
                    expiration = (- expiration);
                    expirationInHours = true;
                }
            }
            modificationTime = LogUtils.readPackedLong(entryBuffer);
            key = LogUtils.readByteArray(entryBuffer);
            ln.readFromLog(envImpl, entryBuffer, logVersion);
        }

        if (entryType.isTransactional()) {

            /*
             * AbortLsn. If it was a marker LSN that was used to fill in a
             * create, mark it null.
             */
            if (haveAbortLSN || logVersion < 12) {
                abortLsn = LogUtils.readPackedLong(entryBuffer);
                if (DbLsn.getFileNumber(abortLsn) ==
                    DbLsn.getFileNumber(DbLsn.NULL_LSN)) {
                    abortLsn = DbLsn.NULL_LSN;
                }
            }

            if (logVersion < 12) {
                setFlags(entryBuffer.get(), (byte) 0);
                haveAbortLSN = (abortLsn != DbLsn.NULL_LSN);
            }

            txn = new Txn();
            txn.readFromLog(envImpl, entryBuffer, logVersion);

        } else if (logVersion == 11) {
            setFlags(entryBuffer.get(), (byte) 0);
        }

        if (logVersion >= 11) {
            if (haveAbortKey) {
                abortKey = LogUtils.readByteArray(entryBuffer);
            }
            if (haveAbortData) {
                abortData = LogUtils.readByteArray(entryBuffer);
            }
            if (haveAbortVLSN) {
                abortVLSN = LogUtils.readPackedLong(entryBuffer);
            }
        }

        if (logVersion >= 12) {
            if (haveAbortExpiration) {
                abortExpiration = LogUtils.readPackedInt(entryBuffer);
                if (abortExpiration < 0) {
                    abortExpiration = (- abortExpiration);
                    abortExpirationInHours = true;
                }
            }
        }

        if (logVersion >= 19) {
            if (haveAbortModificationTime) {
                abortModificationTime = LogUtils.readPackedLong(entryBuffer);
            }
        }

        if (logVersion >= 12 && logVersion < 19) {
            if (haveExpiration) {
                expiration = LogUtils.readPackedInt(entryBuffer);
                if (expiration < 0) {
                    expiration = (- expiration);
                    expirationInHours = true;
                }
            }
        }

        if (logVersion >= 16) {
            if (havePriorSize) {
                priorSize = LogUtils.readPackedInt(entryBuffer);
            }
            if (havePriorFile) {
                priorFile = LogUtils.readPackedLong(entryBuffer);
            }
        }

        if (logVersion < 19) {
            ln.readFromLog(envImpl, entryBuffer, logVersion);
            int keySize;
            if (noMoreLastSerializedFields) {
                int bytesWritten = entryBuffer.position() - recStartPosition;
                keySize = header.getItemSize() - bytesWritten;
            } else {
                keySize = LogUtils.readPackedInt(entryBuffer);
            }
            key = LogUtils.readBytesNoLength(entryBuffer, keySize);
        }

        /* Save cached values after read. */
        ln.setModificationTime(modificationTime);
        ln.setVLSNSequence(
            (header.getVLSN() != INVALID_VLSN) ?
                header.getVLSN() : NULL_VLSN);

        dupStatus = null;
    }

    private void setFlags(final byte flags, final byte flags2) {

        /* First flags byte. */
        embeddedLN = ((flags & EMBEDDED_LN_MASK) != 0);
        abortKnownDeleted = ((flags & ABORT_KD_MASK) != 0);
        haveAbortLSN = ((flags & HAVE_ABORT_LSN_MASK) != 0);
        haveAbortKey = ((flags & HAVE_ABORT_KEY_MASK) != 0);
        haveAbortData = ((flags & HAVE_ABORT_DATA_MASK) != 0);
        haveAbortVLSN = ((flags & HAVE_ABORT_VLSN_MASK) != 0);
        haveAbortExpiration = ((flags & HAVE_ABORT_EXPIRATION_MASK) != 0);
        haveExpiration = ((flags & HAVE_EXPIRATION_MASK) != 0);

        /* Second flags byte. */
        havePriorSize = ((flags2 & HAVE_PRIOR_SIZE_MASK) != 0);
        havePriorFile = ((flags2 & HAVE_PRIOR_FILE_MASK) != 0);
        tombstone = ((flags2 & TOMBSTONE_MASK) != 0);
        blindDeletion = ((flags2 & BLIND_DELETION_MASK) != 0);
        abortTombstone = ((flags2 & ABORT_TOMBSTONE_MASK) != 0);
        haveAbortModificationTime =
            ((flags2 & HAVE_ABORT_MODIFICATION_TIME_MASK) != 0);
        enableBeforeImage =
                ((flags2 & ENABLE_BEFORE_IMAGE_MASK) != 0);
    }

    @Override
    public boolean hasReplicationFormat() {
        return true;
    }

    @Override
    public boolean isReplicationFormatWorthwhile(final ByteBuffer logBuffer,
                                                 final int srcVersion,
                                                 final int destVersion) {

        /* The replication format is optimized only in versions >= 11. */
        if (destVersion < 11) {
            return false;
        }

        /*
         * It is too much trouble to parse versions older than 12, because the
         * flags are not at the front in older versions.
         */
        if (srcVersion < 12) {
            return false;
        }

        final byte flags = logBuffer.get(0);

        /*
         * If we have an abort key or data, assume that the savings is
         * substantial enough to be worthwhile.
         *
         * The abort key is unusual and implies that data is hidden in the key
         * using a partial comparator, so we assume it is probably large,
         * relative to the total size.
         *
         * If there is abort data, it may be small. However, because the
         * presence of abort data implies that this is an update or deletion,
         * there will also be an abort LSN and an abort VLSN (with HA). Plus,
         * abort data is likely to be around the same size as the non-abort
         * data, and keys are normally smallish, meaning that the abort data is
         * largish relative to the total record size. So we assume the savings
         * are substantial enough.
         */
        return (flags &
            (HAVE_ABORT_KEY_MASK | HAVE_ABORT_DATA_MASK)) != 0;
    }

    @Override
    public StringBuilder dumpEntry(StringBuilder sb, boolean verbose) {

        dbId.dumpLog(sb, verbose);

        ln.dumpKey(sb, key);
        ln.dumpLog(sb, verbose);

        sb.append("<embeddedLN val=\"");
        sb.append(embeddedLN);
        sb.append("\"/>");

        if (tombstone) {
            sb.append("<tombstone/>");
        }

        if (blindDeletion) {
            sb.append("<blindDeletion/>");
        }

        if (modificationTime != 0) {
            sb.append("<modTime val=\"");
            sb.append(StatUtils.getDate(modificationTime));
            sb.append("\"/>");
        }

        if (haveExpiration) {
            sb.append("<expires val=\"");
            sb.append(TTL.formatExpiration(expiration, expirationInHours));
            sb.append(" val:").append(expiration);
            sb.append(expirationInHours ? " hours" : " days");
            sb.append("\"/>");
        } else {
            sb.append("<expires/>");
        }

        if (havePriorSize || havePriorFile) {
            sb.append("<prior size=\"");
            sb.append(priorSize);
            sb.append("\" file=\"");
            sb.append(priorFile);
            sb.append("\"/>");
        }

        if (entryType.isTransactional()) {

            txn.dumpLog(sb, verbose);

            sb.append("<abortLSN val=\"");
            sb.append(DbLsn.getNoFormatString(abortLsn));
            sb.append("\"/>");

            sb.append("<abortKD val=\"");
            sb.append(abortKnownDeleted ? "true" : "false");
            sb.append("\"/>");

            sb.append("<abortTombstone val=\"");
            sb.append(abortTombstone ? "true" : "false");
            sb.append("\"/>");

            if (haveAbortKey) {
                sb.append(Key.dumpString(abortKey, "abortKey", 0));
            }
            if (haveAbortData) {
                sb.append(Key.dumpString(abortData, "abortData", 0));
            }
            if (haveAbortVLSN) {
                sb.append("<abortVLSN v=\"");
                sb.append(abortVLSN);
                sb.append("\"/>");
            }
            if (haveAbortExpiration) {
                sb.append("<abortExpires val=\"");
                sb.append(TTL.formatExpiration(
                    abortExpiration, abortExpirationInHours));
                sb.append(" val:").append(abortExpiration);
                sb.append(abortExpirationInHours ? " hours" : " days");
                sb.append("\"/>");
            } else {
                sb.append("<abortExpires/>");
            }
            if (haveAbortModificationTime) {
                sb.append("<abortModTime val=\"");
                sb.append(StatUtils.getDate(abortModificationTime));
                sb.append("\"/>");
            }
            
            if (enableBeforeImage) {
                sb.append("<beforeImageEnabled=\"");
                sb.append(enableBeforeImage);
                sb.append("\"/>");
            }
        }

        return sb;
    }

    @Override
    public void dumpRep(StringBuilder sb) {
        if (entryType.isTransactional()) {
            sb.append(" txn=").append(txn.getId());
        }
    }

    @Override
    public LN getMainItem() {
        return ln;
    }

    @Override
    public long getTransactionId() {
        if (entryType.isTransactional()) {
            return txn.getId();
        }
        return 0;
    }

    /*
     * Writing support.
     */

    @Override
    public int getLastFormatChange() {
        return LAST_FORMAT_CHANGE;
    }

    @Override
    public Collection<VersionedWriteLoggable> getEmbeddedLoggables() {
        return Arrays.asList(new LN(), new DatabaseId(), new Txn());
    }

    @Override
    public int getSize(final int logVersion, final boolean forReplication) {

        assert getClass() == LNLogEntry.class;

        return getBaseLNEntrySize(
            logVersion, true /*keyIsLastSerializedField*/, forReplication);
    }

    /**
     * Method shared by LNLogEntry subclasses.
     *
     * @param keyIsLastSerializedField specifies whether the key length can be
     * omitted because the key is the last field.  This should be false when
     * an LNLogEntry subclass adds fields to the serialized format.
     */
    protected final int getBaseLNEntrySize(
        final int logVersion,
        final boolean keyIsLastSerializedField,
        final boolean forReplication) {

        int size = ln.getLogSize(logVersion, forReplication) +
            dbId.getLogSize(logVersion, forReplication) +
            key.length;

        if (logVersion >= 19 || !keyIsLastSerializedField) {
            size += LogUtils.getPackedIntLogSize(key.length);
        }

        if (logVersion >= 19) {
            size += LogUtils.getPackedLongLogSize(modificationTime);
        }

        if (entryType.isTransactional() || logVersion >= 11) {
            size += 1;   // flags
        }

        if (logVersion >= 16) {
            size += 1;   // flags2
        }

        if (entryType.isTransactional()) {
            if (logVersion < 12 || (haveAbortLSN && !forReplication)) {
                size += LogUtils.getPackedLongLogSize(abortLsn);
            }
            size += txn.getLogSize(logVersion, forReplication);
        }

        if (!forReplication) {
            if (logVersion >= 11) {
                if (haveAbortKey) {
                    size += LogUtils.getByteArrayLogSize(abortKey);
                }
                if (haveAbortData) {
                    size += LogUtils.getByteArrayLogSize(abortData);
                }
                if (haveAbortVLSN) {
                    size += LogUtils.getPackedLongLogSize(abortVLSN);
                }
            }
            if (logVersion >= 12) {
                if (haveAbortExpiration) {
                    size += LogUtils.getPackedIntLogSize(
                        abortExpirationInHours ?
                            (-abortExpiration) : abortExpiration);
                }
            }
            if (logVersion >= 16) {
                if (havePriorSize) {
                    size += LogUtils.getPackedIntLogSize(priorSize);
                }
                if (havePriorFile) {
                    size += LogUtils.getPackedLongLogSize(priorFile);
                }
            }
            if (logVersion >= 19) {
                if (haveAbortModificationTime) {
                    size += LogUtils.getPackedLongLogSize(
                        abortModificationTime);
                }
            }
        }

        if (logVersion >= 12) {
            if (haveExpiration) {
                size += LogUtils.getPackedIntLogSize(
                    expirationInHours ? (- expiration) : expiration);
            }
        }

        return size;
    }

    @Override
    public void writeEntry(final ByteBuffer destBuffer,
                           final int logVersion,
                           final boolean forReplication) {

        /* Subclasses must call writeBaseLNEntry. */
        assert getClass() == LNLogEntry.class;

        writeBaseLNEntry(
            destBuffer, logVersion, true /*keyIsLastSerializedField*/,
            forReplication);
    }

    /**
     * Method shared by LNLogEntry subclasses.
     *
     * @param keyIsLastSerializedField specifies whether the key length can be
     * omitted because the key is the last field.  This should be false when
     * an LNLogEntry subclass adds fields to the serialized format.
     */
    protected final void writeBaseLNEntry(
        final ByteBuffer destBuffer,
        final int logVersion,
        final boolean keyIsLastSerializedField,
        final boolean forReplication) {

        /*
         * Note this method (unlike readFromLog) only works for log versions
         * GTE 9 (LogEntryType.LOG_VERSION_REPLICATE_OLDER). Writing earlier
         * versions is unsupported.
         */
        byte flags = 0;
        byte flags2 = 0;

        if (entryType.isTransactional() &&
            (logVersion < 12 || !forReplication)) {

            if (abortKnownDeleted) {
                flags |= ABORT_KD_MASK;
            }
            if (haveAbortLSN) {
                flags |= HAVE_ABORT_LSN_MASK;
            }
        }

        if (!forReplication) {
            if (logVersion >= 11) {
                if (embeddedLN) {
                    flags |= EMBEDDED_LN_MASK;
                }
                if (haveAbortKey) {
                    flags |= HAVE_ABORT_KEY_MASK;
                }
                if (haveAbortData) {
                    flags |= HAVE_ABORT_DATA_MASK;
                }
                if (haveAbortVLSN) {
                    flags |= HAVE_ABORT_VLSN_MASK;
                }
            }
            if (logVersion >= 12) {
                if (haveAbortExpiration) {
                    flags |= HAVE_ABORT_EXPIRATION_MASK;
                }
            }
            if (logVersion >= 16) {
                if (havePriorSize) {
                    flags2 |= HAVE_PRIOR_SIZE_MASK;
                }
                if (havePriorFile) {
                    flags2 |= HAVE_PRIOR_FILE_MASK;
                }
            }
            if (logVersion >= 19) {
                if (haveAbortModificationTime) {
                    flags2 |= HAVE_ABORT_MODIFICATION_TIME_MASK;
                }
                if (abortTombstone) {
                    flags2 |= ABORT_TOMBSTONE_MASK;
                }
            }
        }

        if (logVersion >= 19) {
            if (tombstone) {
                flags2 |= TOMBSTONE_MASK;
            }
        }

        if (logVersion >= 20) {
            if (blindDeletion) {
                flags2 |= BLIND_DELETION_MASK;
            }
        }
        
        if (logVersion >= 25) {
            if (enableBeforeImage) {
                flags2 |= ENABLE_BEFORE_IMAGE_MASK;
            }
        }

        if (logVersion >= 12) {
            if (haveExpiration) {
                flags |= HAVE_EXPIRATION_MASK;
            }
            destBuffer.put(flags);
        }

        if (logVersion >= 16) {
            destBuffer.put(flags2);
        }

        dbId.writeToLog(destBuffer, logVersion, forReplication);

        if (logVersion >= 19) {
            if (haveExpiration) {
                LogUtils.writePackedInt(
                    destBuffer,
                    expirationInHours ? (-expiration) : expiration);
            }
            LogUtils.writePackedLong(destBuffer, modificationTime);
            LogUtils.writeByteArray(destBuffer, key);
            ln.writeToLog(destBuffer, logVersion, forReplication);
        }

        if (entryType.isTransactional()) {

            if (logVersion < 12 || (haveAbortLSN && !forReplication)) {
                LogUtils.writePackedLong(destBuffer, abortLsn);
            }

            if (logVersion < 12) {
                destBuffer.put(flags);
            }

            txn.writeToLog(destBuffer, logVersion, forReplication);

        } else if (logVersion == 11) {
            destBuffer.put(flags);
        }

        if (!forReplication) {
            if (logVersion >= 11) {
                if (haveAbortKey) {
                    LogUtils.writeByteArray(destBuffer, abortKey);
                }
                if (haveAbortData) {
                    LogUtils.writeByteArray(destBuffer, abortData);
                }
                if (haveAbortVLSN) {
                    LogUtils.writePackedLong(destBuffer, abortVLSN);
                }
            }
            if (logVersion >= 12) {
                if (haveAbortExpiration) {
                    LogUtils.writePackedInt(
                        destBuffer,
                        abortExpirationInHours ?
                            (-abortExpiration) : abortExpiration);
                }
            }
            if (logVersion >= 19) {
                if (haveAbortModificationTime) {
                    LogUtils.writePackedLong(
                        destBuffer, abortModificationTime);
                }
            }
        }

        if (logVersion >= 12 && logVersion < 19) {
            if (haveExpiration) {
                LogUtils.writePackedInt(
                    destBuffer,
                    expirationInHours ? (-expiration) : expiration);
            }
        }

        if (!forReplication) {
            if (logVersion >= 16) {
                if (havePriorSize) {
                    LogUtils.writePackedInt(destBuffer, priorSize);
                }
                if (havePriorFile) {
                    LogUtils.writePackedLong(destBuffer, priorFile);
                }
            }
        }

        if (logVersion < 19) {
            ln.writeToLog(destBuffer, logVersion, forReplication);
            if (!keyIsLastSerializedField) {
                LogUtils.writePackedInt(destBuffer, key.length);
            }
            LogUtils.writeBytesNoLength(destBuffer, key);
        }

    }

    @Override
    public boolean isImmediatelyObsolete(DatabaseImpl dbImpl) {
        return (ln.isDeleted() ||
                embeddedLN ||
                dbImpl.isLNImmediatelyObsolete());
    }

    @Override
    public boolean isDeleted() {
        return ln.isDeleted();
    }

    /**
     * For LN entries, we need to record the latest LSN for that node with the
     * owning transaction, within the protection of the log latch. This is a
     * callback for the log manager to do that recording.
     */
    @Override
    public void postLogWork(
        LogEntryHeader header,
        long justLoggedLsn,
        long vlsn) {

        if (entryType.isTransactional()) {
            txn.addLogInfo(justLoggedLsn);
        }

        /* Save cached values after write. */
        ln.setModificationTime(modificationTime);
        ln.setVLSNSequence(
            (vlsn != INVALID_VLSN) ? vlsn : NULL_VLSN);
    }

    @Override
    public void postFetchInit(DatabaseImpl dbImpl) {
        postFetchInit(dbImpl.getSortedDuplicates());
    }

    /**
     * Converts the key/data for old format LNs in a duplicates DB.
     *
     * This method MUST be called before calling any of the following methods:
     *  getLN
     *  getKey
     *  getUserKeyData
     *
     * TODO:
     * This method is not called by the HA feeder when materializing entries.
     * This is OK because entries with log version 7 and below are never
     * materialized. But we may want to rename this method to make it clear
     * that it only is, and only must be, called for the log versions &lt; 8.
     */
    public void postFetchInit(boolean isDupDb) {
        dupStatus = isDupDb;
    }

    /**
     * Translates two-part keys in duplicate DBs back to the original user
     * operation params.  postFetchInit must be called before calling this
     * method.
     */
    public void getUserKeyData(
        DatabaseEntry keyParam,
        DatabaseEntry dataParam) {

        if (dupStatus == null) {
            throw unexpectedState(
                "postFetchInit was not called");
        }

        if (dupStatus) {
            DupKeyData.split(key, key.length, keyParam, dataParam);
        } else {
            if (keyParam != null) {
                keyParam.setData(key);
            }
            if (dataParam != null) {
                dataParam.setData(ln.getData());
            }
        }
    }

    /*
     * Accessors.
     */
    public boolean isEmbeddedLN() {
        return embeddedLN;
    }

    public LN getLN() {
        return ln;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getData() {
        return ln.getData();
    }

    public byte[] getEmbeddedData() {

        if (!isEmbeddedLN()) {
            return null;
        }

        if (ln.isDeleted()) {
            return Key.EMPTY_KEY;
        }

        return ln.getData();
    }

    /**
     * Returns the expiration time in ms
     * @return expiration time
     */
    public long getExpirationTime() {
        return TTL.expirationToSystemTime(getExpiration(),
                                          isExpirationInHours());
    }
    
    public int getExpiration() {
        return expiration;
    }

    public boolean isExpirationInHours() {
        return expirationInHours;
    }

    /**
     * Returns the LN's modification time, or zero if the LN belongs to a
     * secondary (duplicates) database or was originally written using JE 19.3
     * or earlier.
     *
     * @since 19.5
     */
    public long getModificationTime() {
        return modificationTime;
    }

    /**
     * Returns the tombstone property of the record.
     *
     * @see <a href="../../WriteOptions.html#tombstones">Tombstones</a>
     * @since 19.5
     */
    public boolean isTombstone() {
        return tombstone;
    }

    public int getDataLength() {
        return ln.getData().length;
    }

    public int getKeyLength() {
        return key.length;
    }

    @Override
    public DatabaseId getDbId() {
        return dbId;
    }

    public long getAbortLsn() {
        return abortLsn;
    }

    public boolean getAbortKnownDeleted() {
        return abortKnownDeleted;
    }

    public byte[] getAbortKey() {
        return abortKey;
    }

    public byte[] getAbortData() {
        return abortData;
    }

    public long getAbortVLSN() {
        return abortVLSN;
    }

    public boolean isBlindDeletion() {
        return blindDeletion;
    }
    
    public boolean isBeforeImageEnabled() {
        return enableBeforeImage;
    }

    /**
     * Returns true if recovery should count the prior version obsolete using
     * {@link #getPriorVersionSize()} and {@link #getPriorVersionLsn()} ()}.
     * True is returned if there is a prior version of this LN that is not
     * immediately obsolete.
     */
    public boolean countPriorVersionObsolete() {
        return havePriorSize;
    }

    /**
     * Returns the log size of the prior version of this LN.
     *
     * Must not be called if {@link #countPriorVersionObsolete()} returns
     * false.
     */
    public int getPriorVersionSize() {
        if (!havePriorSize) {
            throw EnvironmentFailureException.unexpectedState();
        }
        return priorSize;
    }

    /**
     * Returns the LSN of the prior version of this LN, for purposes of
     * obsolete counting -- the LSN offset may be incorrect, but the LSN file
     * is correct. If the prior version LSN is the abortLsn, then the abortLsn
     * (including its true offset) is returned by this method.
     *
     * Must not be called if {@link #countPriorVersionObsolete()} returns
     * false.
     */
    public long getPriorVersionLsn() {
        if (!havePriorSize) {
            throw EnvironmentFailureException.unexpectedState();
        }
        if (!havePriorFile && !haveAbortLSN) {
            throw EnvironmentFailureException.unexpectedState();
        }
        return havePriorFile ? DbLsn.makeLsn(priorFile, 0) : abortLsn;
    }

    public int getAbortExpiration() {
        return abortExpiration;
    }

    public boolean isAbortExpirationInHours() {
        return abortExpirationInHours;
    }

    public long getAbortModificationTime() {
        return abortModificationTime;
    }

    public boolean getAbortTombstone() {
        return abortTombstone;
    }

    public Long getTxnId() {
        if (entryType.isTransactional()) {
            return txn.getId();
        }
        return null;
    }

    public Txn getUserTxn() {
        if (entryType.isTransactional()) {
            return txn;
        }
        return null;
    }

    @Override
    public boolean logicalEquals(LogEntry other) {
        if (!(other instanceof LNLogEntry)) {
            return false;
        }

        LNLogEntry<?> otherEntry = (LNLogEntry<?>) other;

        if (!dbId.logicalEquals(otherEntry.dbId)) {
            return false;
        }

        if (txn != null) {
            if (!txn.logicalEquals(otherEntry.txn)) {
                return false;
            }
        } else {
            if (otherEntry.txn != null) {
                return false;
            }
        }

        if (!Arrays.equals(key, otherEntry.key)) {
            return false;
        }

        if (!ln.logicalEquals(otherEntry.ln)) {
            return false;
        }

        return true;
    }
}
