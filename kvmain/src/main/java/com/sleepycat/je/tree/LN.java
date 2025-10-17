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

import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import com.sleepycat.je.beforeimage.BeforeImageContext;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogItem;
import com.sleepycat.je.log.LogParams;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.log.Provisional;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.log.VersionedWriteLoggable;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.txn.LockGrantType;
import com.sleepycat.je.txn.LockResult;
import com.sleepycat.je.txn.LockType;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.txn.WriteLockInfo;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.SizeofMarker;
import com.sleepycat.je.utilint.VLSN;

/**
 * An LN represents a Leaf Node in the JE tree.
 */
public class LN extends Node implements VersionedWriteLoggable {

    private static final String BEGIN_TAG = "<ln>";
    private static final String END_TAG = "</ln>";

    /**
     * The log version of the most recent format change for this loggable.
     *
     * @see #getLastFormatChange
     */
    private static final int LAST_FORMAT_CHANGE = 8;

    private byte[] data;

    /*
     * Flags: bit fields
     */
    private static final int FETCHED_COLD_BIT = 0x80000000;
    private int flags; // not persistent

    /**
     * A cached version of the VLSN and modification time that are stored in
     * the LNLogEntry. These are used to cache these values in the Btree (BIN)
     * when the LN is resident.
     */
    private long vlsnSequence = NULL_VLSN;
    private long modificationTime = 0;
    private long creationTime = 0;

    /**
     * Create an empty LN, to be filled in from the log.
     */
    public LN() {
        this.data = null;
    }

    /**
     * Create a new LN from a byte array.  Pass a null byte array to create a
     * deleted LN.
     *
     * Does NOT copy the byte array, so after calling this method the array is
     * "owned" by the Btree and should not be modified.
     *
     * The envImpl param may be used in the future to create LN subclasses.
     */
    public static LN makeLN(
        @SuppressWarnings("unused")
        EnvironmentImpl envImpl,
        byte[] dataParam) {

        return new LN(dataParam);
    }

    /**
     * Create a new LN from a DatabaseEntry. Makes a copy of the byte array.
     *
     * The envImpl param may be used in the future to create LN subclasses.
     */
    public static LN makeLN(
        @SuppressWarnings("unused")
        EnvironmentImpl envImpl,
        DatabaseEntry dbt) {

        return new LN(dbt);
    }

    /**
     * Does NOT copy the byte array, so after calling this method the array is
     * "owned" by the Btree and should not be modified.
     */
    LN(final byte[] data) {

        if (data == null) {
            this.data = null;
        } else if (data.length == 0) {
            this.data = LogUtils.ZERO_LENGTH_BYTE_ARRAY;
        } else {
            this.data = data;
        }
    }

    /**
     * Makes a copy of the byte array.
     */
    LN(DatabaseEntry dbt) {
        byte[] dat = dbt.getData();
        if (dat == null) {
            data = null;
        } else if (dbt.getPartial()) {
            init(dat,
                 dbt.getOffset(),
                 dbt.getPartialOffset() + dbt.getSize(),
                 dbt.getPartialOffset(),
                 dbt.getSize());
        } else {
            init(dat, dbt.getOffset(), dbt.getSize());
        }
    }

    /** For Sizeof. */
    public LN(@SuppressWarnings("unused") SizeofMarker marker,
              DatabaseEntry dbt) {
        this(dbt);
    }

    private void init(byte[] data, int off, int len, int doff, int dlen) {
        if (len == 0) {
            this.data = LogUtils.ZERO_LENGTH_BYTE_ARRAY;
        } else {
            this.data = new byte[len];
            System.arraycopy(data, off, this.data, doff, dlen);
        }
    }

    private void init(byte[] data, int off, int len) {
        init(data, off, len, 0, len);
    }

    public byte[] getData() {
        return data;
    }

    public int getDataOffset() {
        assert data != null : "Not allowed for deleted LN";
        return 0;
    }

    public int getDataSize() {
        assert data != null : "Not allowed for deleted LN";
        return data.length;
    }

    public boolean isDeleted() {
        return (data == null);
    }

    @Override
    public boolean isLN() {
        return true;
    }

    void makeDeleted() {
        data = null;
    }

    public boolean getFetchedCold() {
        return ((flags & FETCHED_COLD_BIT) != 0);
    }

    public void setFetchedCold(boolean val) {
        if (val) {
            flags |= FETCHED_COLD_BIT;
        } else {
            flags &= ~FETCHED_COLD_BIT;
        }
    }

    /**
     * Initialize LN after fetching from disk.
     */
    public void initialize(DatabaseImpl db) {
        /*
         * This flag is initially true for a fetched LN, and will be set to
         * false if the LN is accessed with any CacheMode other than UNCHANGED.
         */
        setFetchedCold(true);
    }

    /**
     * Returns the cached VLSN that was copied from the LNLogEntry, or
     * {@link VLSN#NULL_VLSN} if the log entry has no VLSN.
     * @see #vlsnSequence
     */
    public long getVLSNSequence() {
        return vlsnSequence;
    }

    /**
     * Caches a copy of the VLSN from the LNLogEntry after a read or write.
     * @see #vlsnSequence
     */
    public void setVLSNSequence(long seq) {
        vlsnSequence = seq;
    }

    /**
     * Returns the cached modification time that was copied from the
     * LNLogEntry.
     * @see #modificationTime
     */
    public long getModificationTime() {
        return modificationTime;
    }

    /**
     * Caches a copy of the modification time from the LNLogEntry after a read
     * or write.
     * @see #modificationTime
     */
    public void setModificationTime(long time) {
        modificationTime = time;
    }

    /**
     * Returns the cached creation time that was copied from the
     * LNLogEntry.
     * @see #creationTime
     */
    public long getCreationTime() {
        return creationTime;
    }

    /**
     * Caches a copy of the creation time from the LNLogEntry after a read
     * or write.
     * @see #creationTime
     */
    public void setCreationTime(long time) {
        creationTime = time;
    }

    /*
     * If you get to an LN, this subtree isn't valid for delete. True, the LN
     * may have been deleted, but you can't be sure without taking a lock, and
     * the validate -subtree-for-delete process assumes that bin compressing
     * has happened and there are no committed, deleted LNS hanging off the
     * BIN.
     */
    @Override
    boolean isValidForDelete() {
        return false;
    }

    /**
     * Returns true by default, but is overridden by MapLN to prevent eviction
     * of open databases.  This method is meant to be a guaranteed check and is
     * used after a BIN has been selected for LN stripping but before actually
     * stripping an LN. [#13415]
     * @throws DatabaseException from subclasses.
     */
    boolean isEvictable()
        throws DatabaseException {

        return true;
    }

    public void delete() {
        makeDeleted();
    }

    public void modify(byte[] newData) {
        data = newData;
    }

    /**
     * Sets data to empty and returns old data.  Called when converting an old
     * format LN in a duplicates DB.
     */
    public byte[] setEmpty() {
        final byte[] retVal = data;
        data = Key.EMPTY_KEY;
        return retVal;
    }

    /**
     * Add yourself to the in memory list if you're a type of node that should
     * belong.
     */
    @Override
    void rebuildINList(INList inList) {
        /*
         * Don't add, LNs don't belong on the list.
         */
    }

    /**
     * Compute the approximate size of this node in memory for evictor
     * invocation purposes.
     */
    @Override
    public long getMemorySizeIncludedByParent() {
        int size = MemoryBudget.LN_OVERHEAD;
        if (data != null) {
            size += MemoryBudget.byteArraySize(getDataSize());
        }
        return size;
    }

    /*
     * Dumping
     */

    public String beginTag() {
        return BEGIN_TAG;
    }

    public String endTag() {
        return END_TAG;
    }

    @Override
    public String dumpString(int nSpaces, boolean dumpTags) {
        StringBuilder self = new StringBuilder();
        if (dumpTags) {
            self.append(TreeUtils.indent(nSpaces));
            self.append(beginTag());
            self.append('\n');
        }

        self.append(super.dumpString(nSpaces + 2, true));
        self.append('\n');
        if (data != null) {
            self.append(TreeUtils.indent(nSpaces+2));
            self.append("<data>");
            self.append(Key.DUMP_TYPE.dumpByteArray(
                data, getDataOffset(), getDataSize()));
            self.append("</data>");
            self.append('\n');
        }
        if (dumpTags) {
            self.append(TreeUtils.indent(nSpaces));
            self.append(endTag());
        }
        return self.toString();
    }

    /*
     * Logging Support
     */

    /*
     * Lock the new LSN immediately after logging, with the BIN latched.
     * Lock non-blocking, since no contention is possible on the new LSN.
     * If the locker is transactional, a new WriteLockInfo is created for
     * the new LSN and stored in the locker. lockResult points to that
     * WriteLockInfo. Since this new WriteLockInfo and the WriteLockInfo
     * given as input to this method refer to the same logical record,
     * the info from the given WriteLockInfo is copied to the new one.
     */
    private void getPostLogLock(
    	final Locker locker,
    	final LogItem item,
    	final DatabaseImpl dbImpl,
    	final WriteLockInfo writeLockInfo
    ) {
    	final long newLsn = item.lsn;

        /* Null cursor param is OK because this is a not a read lock. */
        final LockResult lockResult = locker.postLogNonBlockingLock(
            newLsn, LockType.WRITE, false /*jumpAheadOfWaiters*/, dbImpl,
            null);

        assert lockResult.getLockGrant() != LockGrantType.DENIED :
               DbLsn.getNoFormatString(newLsn);

        lockResult.copyWriteLockInfo(writeLockInfo);
    }

    /**
     * Generate and write to the log a logrec describing an operation O that
     * is being performed on a record R with key K. O may be an insertion,
     * update, deletion, migration.
     *
     * Let T be the locker performing O. T is null in case of DW eviction/ckpt.
     * Otherwise, T holds a lock on R and it will keep that lock until it
     * terminates. In case of a CUD op, the lock is an exclusive one; in
     * case of LN migration, it's a shared one (and T is non-transactional).
     *
     * - Let Rc be the current version of R (before O). The absence of R from
     *   the DB is considered as a special "deleted" version. Rc may be the
     *   deleted version.
     * - If T is a Txn, let Ra be the version of R before T write-locked R. Ra
     *   may be the deleted version. Ra and Rc will be the same if O is the
     *   very 1st op on R by T.
     * - Let Rn be R's new version (after O). Rc and Rn will be the same if O
     *   is migration or DW eviction/ckpt.
     *
     * - Let Ln be the LSN of the logrec that will be generated here to
     *   describe O.
     * - Let Lc be the current LSN value in R's slot, or NULL if no such slot
     *   exists currently. If an R slot exists, then Lc points to Rc, or may be
     *   NULL if Rc is the deleted version.
     * - If T is a Txn, let La be the LSN value in R's slot at the time T
     *   write-locked R, or NULL if no such slot existed at that time.
     *
     * @param locker The locker T. If non-null, a write lock will be acquired
     * by T on Ln's LSN.
     *
     * WARNING: Be sure to pass null for the locker param if the new LSN should
     * not be locked.
     *
     * @param writeLockInfo It is non-null if and only if T is a Txn. It
     * contains info that must be included in Ln to make it undoable if T
     * aborts. Specifically, it contains:
     *
     * - abortKD   : True if Ra is the deleted version; false otherwise.
     * - abortLSN  : The La LSN as defined above.
     * - abortKey  : The key of Ra, if Ra was embedded in the parent BIN and
     *               the containing DB allows key updates.
     * - abortData : The data of Ra, if Ra was embedded in the parent BIN.
     *
     * When the new LSN is write-locked, a new WriteLockInfo is created and
     * the above info is copied into it. Normally this parameter should be
     * obtained from the prepareForInsert or prepareForUpdate method of
     * CursorImpl.LockStanding.
     *
     * @param newEmbeddedLN Whether Rn will be embedded into the parent BIN.
     * If true, Ln will be counted as an "immediately obsolete" logrec.
     *
     * @param newKey Rn's key. Note: Rn's data is not passed as a parameter to
     * this method because it is stored in this LN. Rn (key and data) will be
     * stored in Ln. Rn's key will also be stored in the parent BIN, and if
     * newEmbeddedLN is true, Rn's data too will be stored there.
     *
     * @param newExpiration the new expiration time in days or hours.
     *
     * @param newExpirationInHours whether the new expiration time is in hours.
     *
     * @param currEmbeddedLN Whether Rc's data is embedded into the parent
     * BIN. If true, Lc has already been counted obsolete.
     *
     * @param currLsn The Lc LSN as defined above. Is given as a param to this
     * method to count the associated logrec as obsolete (which must done under
     * the LWL), if it has not been counted already.
     *
     * @param currSize The size of Lc (needed for obsolete counting).
     *
     * @param isInsertion True if the operation is an insertion (including
     * slot reuse). False otherwise.
     */
    public LogItem log(
        final EnvironmentImpl envImpl,
        final DatabaseImpl dbImpl,
        final Locker locker,
        final WriteLockInfo writeLockInfo,
        final boolean newEmbeddedLN,
        final byte[] newKey,
        final int newExpiration,
        final boolean newExpirationInHours,
        final long creationTime,
        final long newModificationTime,
        final boolean newTombstone,
        final boolean newBlindDeletion,
        final boolean currEmbeddedLN,
        final long currLsn,
        final int currSize,
        final boolean isInsertion,
        final boolean backgroundIO,
        ReplicationContext repContext,
        final BeforeImageContext bImgCtx)
        throws DatabaseException {

        assert !(dbImpl.getDbType().isInternal() && newEmbeddedLN);
        assert !(dbImpl.getSortedDuplicates() && newModificationTime != 0);

        if (envImpl.isReadOnly()) {
            
            /* Returning a NULL_LSN will not allow locking. */
            throw EnvironmentFailureException.unexpectedState(
                "Cannot log LNs in read-only env.");
        }

        /*
         * Check that a replicated txn is used for writing to a replicated DB,
         * and a non-replicated locker is used for writing to a
         * non-replicated DB. This is critical for avoiding corruption when HA
         * failover occurs [#23234] [#23330].
         *
         * Two cases are exempt from this rule:
         *
         *  - The locker is null only when performing internal logging (not a
         *    user operation), such as cleaner migration This is always
         *    non-transactional and non-replicated, so we can skip this check.
         *    Note that the cleaner may migrate an LN in a replicated DB, but
         *    this is not part ot the rep stream.
         *
         *  - Some DBs contain a mixture of replicated and non-replicated
         *    records. For example, only NameLNs that identify replicated DBs
         *    are replicated, not all NameLNs in the naming DB, so the naming
         *    DB is exempt.
         *
         * This guard should never fire because of two checks made prior to
         * logging:
         *
         *  - When a user txn in a replicated environment is not configured for
         *    local-write and a write operation is attempted (or when the
         *    opposite is true), the Cursor class will throw
         *    UnsupportedOperationException. See Locker.isLocalWrite.
         *
         *  - On a replica, writes to replicated DBs are disallowed even when
         *    local-write is false.  This is enforced by the ReadonlyTxn class
         *    which throws ReplicaWriteException in this case.
         */
        final boolean isMixedRepDB = dbImpl.getDbType().isMixedReplication() ||
                                     dbImpl.getDbType().isMixedTransactional();

        if (!isMixedRepDB &&
            envImpl.isReplicated() &&
            locker != null &&
            dbImpl.isReplicated() != locker.isReplicated()) {

            throw EnvironmentFailureException.unexpectedState(
                (locker.isReplicated() ?
                    "Rep txn used to write to non-rep DB" :
                    "Non-rep txn used to write to rep DB") +
                ", class = " + locker.getClass().getName() +
                ", txnId = " + locker.getId() +
                ", dbName = " + dbImpl.getName());
        }

        /*
         * As an additional safeguard, check that a replicated txn is used when
         * the operation is part of the rep stream, and that the inverse is
         * also true. Mixed rep DBs are exempt for the same reason as above.
         */
        if (!isMixedRepDB) {

            boolean isRepLocker = (locker != null) && locker.isReplicated();

            if (repContext.inReplicationStream() != isRepLocker) {
                throw EnvironmentFailureException.unexpectedState(
                    (isRepLocker ?
                        "Rep txn used to write outside of rep stream" :
                        "Non-rep txn used to write in rep stream") +
                    ((locker != null) ?
                        (", class = " + locker.getClass().getName() +
                         ", txnId = " + locker.getId()) :
                        ", null locker") +
                    ", dbName = " + dbImpl.getName());
            }
        }

        LogEntryType entryType;
        Txn txn = null;
        long abortLsn = DbLsn.NULL_LSN;
        boolean abortKD = false;
        byte[] abortKey = null;
        byte[] abortData = null;
        long abortVLSN = NULL_VLSN;
        int abortExpiration = 0;
        boolean abortExpirationInHours = false;
        long abortModificationTime = 0;
        long abortCreationTime = 0;
        boolean abortTombstone = false;

        LogParams params = new LogParams();

        if (locker != null && locker.isTransactional()) {

            entryType = getLogType(isInsertion, true, dbImpl);

            txn = locker.getTxnLocker();
            assert(txn != null);

            abortLsn = writeLockInfo.getAbortLsn();
            abortKD = writeLockInfo.getAbortKnownDeleted();
            abortKey = writeLockInfo.getAbortKey();
            abortData = writeLockInfo.getAbortData();
            abortVLSN = writeLockInfo.getAbortVLSN();
            abortExpiration = writeLockInfo.getAbortExpiration();
            abortExpirationInHours = writeLockInfo.isAbortExpirationInHours();
            abortModificationTime = writeLockInfo.getAbortModificationTime();
            abortCreationTime = writeLockInfo.getAbortCreationTime();
            abortTombstone = writeLockInfo.getAbortTombstone();

            params.obsoleteDupsAllowed = locker.isRolledBack();

        } else {
            entryType = getLogType(isInsertion, false, dbImpl);
        }

        /*
         * Determine whether the prior version Rc was counted earlier as an
         * "immediately obsolete" logrec. This includes the cases where the
         * DB is a dups DB, or the current op is an insertion (which implies
         * Rc is a deletion and as such has been counted already) or Rc is
         * embedded.
         */
        boolean currImmediatelyObsolete =
            dbImpl.isLNImmediatelyObsolete() ||
            isInsertion ||
            currEmbeddedLN;

        int priorSize = currImmediatelyObsolete ? 0 : currSize;
        long priorLsn = (priorSize == 0) ? DbLsn.NULL_LSN : currLsn;

        /*
         * If currImmediatelyObsolete, pass zero/NULL_LSN for the prior
         * size/lsn. Recovery uses these values to count prior versions
         * obsolete, but only when they are not immediately obsolete.
         */
        params.entry = createLogEntry(
            entryType, dbImpl, txn,
            abortLsn, abortKD, abortKey, abortData, abortVLSN,
            abortExpiration, abortExpirationInHours,
            abortModificationTime, abortCreationTime, abortTombstone,
            newKey, newEmbeddedLN, newExpiration, newExpirationInHours,
            creationTime, newModificationTime, newTombstone, newBlindDeletion,
            priorSize, priorLsn, repContext, bImgCtx);

        /* LNs are never provisional. */
        params.provisional = Provisional.NO;

        /*
         * Decide whether to count the current record version as obsolete
         * during logging. Rc should not be counted as obsolete if:
         * (a) Rc == Ra; Ra (i.e. abortLsn) will be counted obsolete during
         * commit, or
         * (b) Rc was counted earlier as an "immediately obsolete" logrec.
         */
        if (currLsn != abortLsn && !currImmediatelyObsolete) {
            params.oldLsn = currLsn;
            params.oldSize = currSize;
        }

        params.repContext = repContext;
        params.backgroundIO = backgroundIO;
        params.nodeDb = dbImpl;

        /* Save obsolete size information to be used during commit. */
        if (txn != null && currLsn == abortLsn) {
            writeLockInfo.setAbortLogSize(currSize);
        }

        /*
         * TODO: We could set LogParams.immutableLogEntry to true when the LN
         *  will be evicted immediately after logging (it is not cached) and
         *  is replicated and immutable, e.g., index records and deletions.
         *  This would reduce memory allocation since a cached buffer and its
         *  associated queue entry would not be needed.
         */

        LogItem item;
        try {
            if (txn != null) {

                /*
                 * Writing an LN_TX entry requires looking at the Txn's
                 * lastLoggedLsn.  The Txn may be used by multiple threads so
                 * ensure that the view we get is consistent. [#17204]
                 * The lock on the log has to be acquired in the synchronized
                 * because other internal threads may try to access the
                 * lastLoggedLsn and the lock on it, such as
                 * MasterTxn.convertToReplayTxnAndClose.
                 */
                synchronized (txn) {
                    item = envImpl.getLogManager().log(params);
                    getPostLogLock(locker, item, dbImpl, writeLockInfo);
                }
            } else {
                item = envImpl.getLogManager().log(params);
            }
        } catch (Throwable e) {
            /*
             * If any exception occurs while logging an LN, ensure that the
             * environment is invalidated. This will also ensure that the txn
             * cannot be committed.
             */
            if (envImpl.isValid()) {
                throw new EnvironmentFailureException(
                    envImpl, EnvironmentFailureReason.LOG_INCOMPLETE,
                    "LN could not be logged", e);
            } else {
                throw e;
            }
        }

        if (txn == null && locker != null) {
            getPostLogLock(locker, item, dbImpl, writeLockInfo);
        }

        /* In a dup DB, do not expect embedded LNs or non-empty data. */
        if (dbImpl.getSortedDuplicates() &&
            (newEmbeddedLN || (data != null && getDataSize() > 0))) {

            throw EnvironmentFailureException.unexpectedState(
                envImpl,
                "[#25288] emb=" + newEmbeddedLN +
                " key=" + Key.getNoFormatString(newKey) +
                " data=" + Key.getNoFormatString(
                    data, getDataOffset(), getDataSize()) +
                " lsn=" + DbLsn.getNoFormatString(currLsn));
        }

        return item;
    }

    public LogItem log(
        final EnvironmentImpl envImpl,
        final DatabaseImpl dbImpl,
        final Locker locker,
        final WriteLockInfo writeLockInfo,
        final boolean newEmbeddedLN,
        final byte[] newKey,
        final int newExpiration,
        final boolean newExpirationInHours,
        final long creationTime,
        final long newModificationTime,
        final boolean newTombstone,
        final boolean newBlindDeletion,
        final boolean currEmbeddedLN,
        final long currLsn,
        final int currSize,
        final boolean isInsertion,
        final boolean backgroundIO,
        ReplicationContext repContext)
        throws DatabaseException {
          return log(envImpl, dbImpl, locker, writeLockInfo, newEmbeddedLN, 
                     newKey, newExpiration, newExpirationInHours, creationTime, 
                     newModificationTime, newTombstone, newBlindDeletion, 
                     currEmbeddedLN, currLsn, currSize, isInsertion, 
                     backgroundIO, repContext, null);

    }

    /*
     * Each LN knows what kind of log entry it uses to log itself. Overridden
     * by subclasses.
     */
    protected LNLogEntry<?> createLogEntry(
        LogEntryType entryType,
        DatabaseImpl dbImpl,
        Txn txn,
        long abortLsn,
        boolean abortKD,
        byte[] abortKey,
        byte[] abortData,
        long abortVLSN,
        int abortExpiration,
        boolean abortExpirationInHours,
        long abortModificationTime,
        long abortCreationTime,
        boolean abortTombstone,
        byte[] newKey,
        boolean newEmbeddedLN,
        int newExpiration,
        boolean newExpirationInHours,
        long creationTime,
        long newModificationTime,
        boolean newTombstone,
        boolean newBlindDeletion,
        int priorSize,
        long priorLsn,
        ReplicationContext repContext,
        BeforeImageContext bImgCtx) {

        return new LNLogEntry<>(
            entryType, dbImpl.getId(), txn,
            abortLsn, abortKD, abortKey, abortData, abortVLSN,
            abortExpiration, abortExpirationInHours, abortModificationTime,
            abortCreationTime,
            abortTombstone, newKey, this, newEmbeddedLN,
            newExpiration, newExpirationInHours, creationTime,
            newModificationTime, newTombstone, newBlindDeletion,
            priorSize, priorLsn, (bImgCtx != null));
    }

    /**
     * @see Node#incFetchStats
     */
    @Override
    void incFetchStats(EnvironmentImpl envImpl, boolean isMiss) {
        envImpl.getEvictor().incLNFetchStats(isMiss);
    }

    /**
     * @see Node#getGenericLogType
     */
    @Override
    public LogEntryType getGenericLogType() {
        return getLogType(true, false, null);
    }

    protected LogEntryType getLogType(
        boolean isInsert,
        boolean isTransactional,
        DatabaseImpl db) {

        if (db != null) {
            LogEntryType type = db.getDbType().getLogType(isTransactional);
            if (type != null) {
                return type;
            }
        }

        if (isDeleted()) {
            assert !isInsert;
            return isTransactional ?
                   LogEntryType.LOG_DEL_LN_TRANSACTIONAL :
                   LogEntryType.LOG_DEL_LN;
        }

        if (isInsert) {
            return isTransactional ?
                LogEntryType.LOG_INS_LN_TRANSACTIONAL :
                LogEntryType.LOG_INS_LN;
        }

        return isTransactional ?
            LogEntryType.LOG_UPD_LN_TRANSACTIONAL :
            LogEntryType.LOG_UPD_LN;
    }

    /**
     * @see VersionedWriteLoggable#getLastFormatChange
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
        return calcLogSize(isDeleted() ? -1 : getDataSize());
    }

    /**
     * Calculates log size based on given dataLen, which is negative to
     * calculate the size of a deleted LN.
     */
    private int calcLogSize(int dataLen) {

        int size = 0;

        if (dataLen < 0) {
            size += LogUtils.getPackedIntLogSize(-1);
        } else {
            size += LogUtils.getPackedIntLogSize(dataLen);
            size += dataLen;
        }

        return size;
    }

    @Override
    public void writeToLog(final ByteBuffer logBuffer,
                           final int logVersion,
                           final boolean forReplication) {

        if (isDeleted()) {
            LogUtils.writePackedInt(logBuffer, -1);
        } else {
            assert data != null;
            LogUtils.writeByteArray(logBuffer,
                data, getDataOffset(), getDataSize());
        }
    }

    @Override
    public void readFromLog(EnvironmentImpl envImpl,
                            ByteBuffer itemBuffer,
                            int entryVersion) {

        int size = LogUtils.readPackedInt(itemBuffer);
        if (size >= 0) {
            data = LogUtils.readBytesNoLength(itemBuffer, size);
        }
    }

    @Override
    public boolean hasReplicationFormat() {
        return false;
    }

    @Override
    public boolean isReplicationFormatWorthwhile(final ByteBuffer logBuffer,
                                                 final int srcVersion,
                                                 final int destVersion) {
        return false;
    }

    @Override
    public boolean logicalEquals(Loggable other) {

        if (!(other instanceof LN)) {
            return false;
        }

        LN otherLN = (LN) other;

        /*
         * This includes a comparison of deletedness, since the data is null
         * for a deleted LN.
         */
        return Arrays.equals(getData(), otherLN.getData());
    }

    @Override
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append(beginTag());

        if (data != null) {
            sb.append("<data>");
            if (verbose) {
                sb.append(Key.DUMP_TYPE.dumpByteArray(
                    data, getDataOffset(), getDataSize()));
            } else {
                sb.append("hidden");
            }
            sb.append("</data>");
        }

        dumpLogAdditional(sb, verbose);

        sb.append(endTag());
    }

    public void dumpKey(StringBuilder sb, byte[] key) {
        sb.append(Key.dumpString(key, 0));
    }

    /*
     * Allows subclasses to add additional fields before the end tag.
     */
    protected void dumpLogAdditional(StringBuilder sb,
                                     @SuppressWarnings("unused")
                                     boolean verbose) {
    }

    /**
     * Account for FileSummaryLN's extra marshaled memory. [#17462]
     */
    public void addExtraMarshaledMemorySize(BIN parentBIN) {
        /* Do nothing here.  Overwridden in FileSummaryLN. */
    }

    /*
     * DatabaseEntry utilities
     */

    /**
     * Copies the non-deleted LN's byte array to the entry.  Does not support
     * partial data.
     */
    public void setEntry(DatabaseEntry entry) {
        assert !isDeleted();
        int len = getDataSize();
        byte[] bytes = new byte[len];
        System.arraycopy(data, getDataOffset(), bytes, 0, len);
        entry.setData(bytes);
    }

    /**
     * Copies the given byte array to the given destination entry, copying only
     * partial data if the entry is specified to be partial.  If the byte array
     * is null, clears the entry.
     */
    public static void setEntry(DatabaseEntry dest, byte[] bytes) {

        if (bytes != null) {
            boolean partial = dest.getPartial();
            int off = partial ? dest.getPartialOffset() : 0;
            int len = partial ? dest.getPartialLength() : bytes.length;
            if (off + len > bytes.length) {
                len = (off > bytes.length) ? 0 : bytes.length  - off;
            }

            byte[] newdata;
            if (len == 0) {
                newdata = LogUtils.ZERO_LENGTH_BYTE_ARRAY;
            } else {
                newdata = new byte[len];
                System.arraycopy(bytes, off, newdata, 0, len);
            }
            dest.setData(newdata);
            dest.setOffset(0);
            dest.setSize(len);
        } else {
            dest.setData(null);
            dest.setOffset(0);
            dest.setSize(0);
        }
    }


    /**
     * Copies the given source entry to the given destination entry, copying
     * only partial data if the destination entry is specified to be partial.
     */
    public static void setEntry(DatabaseEntry dest, DatabaseEntry src) {

        if (src.getData() != null) {
            byte[] srcBytes = src.getData();
            boolean partial = dest.getPartial();
            int off = partial ? dest.getPartialOffset() : 0;
            int len = partial ? dest.getPartialLength() : srcBytes.length;
            if (off + len > srcBytes.length) {
                len = (off > srcBytes.length) ? 0 : srcBytes.length  - off;
            }

            byte[] newdata;
            if (len == 0) {
                newdata = LogUtils.ZERO_LENGTH_BYTE_ARRAY;
            } else {
                newdata = new byte[len];
                System.arraycopy(srcBytes, off, newdata, 0, len);
            }
            dest.setData(newdata);
            dest.setOffset(0);
            dest.setSize(len);
        } else {
            dest.setData(null);
            dest.setOffset(0);
            dest.setSize(0);
        }
    }

    /**
     * Returns a byte array that is a complete copy of the data in a
     * non-partial entry.
     */
    public static byte[] copyEntryData(DatabaseEntry entry) {
        assert !entry.getPartial();
        int len = entry.getSize();
        final byte[] newData =
            (len == 0) ? LogUtils.ZERO_LENGTH_BYTE_ARRAY : (new byte[len]);
        System.arraycopy(entry.getData(), entry.getOffset(),
                         newData, 0, len);
        return newData;
    }

    /**
     * Merges the partial entry with the given byte array, effectively applying
     * a partial entry to an existing record, and returns a enw byte array.
     */
    public static byte[] resolvePartialEntry(DatabaseEntry entry,
                                             byte[] foundDataBytes ) {
        assert foundDataBytes != null;
        final int dlen = entry.getPartialLength();
        final int doff = entry.getPartialOffset();
        final int origlen = foundDataBytes.length;
        final int oldlen = (doff + dlen > origlen) ? (doff + dlen) : origlen;
        final int len = oldlen - dlen + entry.getSize();

        final byte[] newData;
        if (len == 0) {
            newData = LogUtils.ZERO_LENGTH_BYTE_ARRAY;
        } else {
            newData = new byte[len];
        }
        int pos = 0;

        /* Keep 0..doff of the old data (truncating if doff > length). */
        int slicelen = (doff < origlen) ? doff : origlen;
        if (slicelen > 0) {
            System.arraycopy(foundDataBytes, 0, newData, pos, slicelen);
        }
        pos += doff;

        /* Copy in the new data. */
        slicelen = entry.getSize();
        System.arraycopy(entry.getData(), entry.getOffset(), newData, pos,
                         slicelen);
        pos += slicelen;

        /* Append the rest of the old data (if any). */
        slicelen = origlen - (doff + dlen);
        if (slicelen > 0) {
            System.arraycopy(foundDataBytes, doff + dlen, newData, pos,
                             slicelen);
        }

        return newData;
    }

    public static void outputBytes(final DatabaseEntry outputEntry,
                                   final byte[] bytes,
                                   final int offset,
                                   final int size) {

        /*
         * TODO: Add reusable buffer support, something like this:
         *
        if (outputEntry.getReusable() &&
            outputEntry.getData().length >= size) {

            System.arraycopy(bytes, offset, outputEntry.getData(), 0, size);
            outputEntry.setOffset(0);
            outputEntry.setSize(size);
            return;
        }
        */

        final byte[] buf = new byte[size];
        System.arraycopy(bytes, offset, buf, 0, size);
        outputEntry.setData(buf);
    }
}
