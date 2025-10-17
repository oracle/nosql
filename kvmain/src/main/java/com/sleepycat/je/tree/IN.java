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

import static com.sleepycat.je.EnvironmentFailureException.unexpectedState;
import static com.sleepycat.je.ExtinctionFilter.ExtinctionStatus.EXTINCT;
import static com.sleepycat.je.ExtinctionFilter.ExtinctionStatus.NOT_EXTINCT;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.VerifyError;
import com.sleepycat.je.WriteOptions;
import com.sleepycat.je.cleaner.PackedObsoleteInfo;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.dbi.InternalComparator;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.evictor.Evictor;
import com.sleepycat.je.latch.LatchContext;
import com.sleepycat.je.latch.LatchFactory;
import com.sleepycat.je.latch.LatchSupport;
import com.sleepycat.je.latch.LatchTable;
import com.sleepycat.je.latch.SharedLatch;
import com.sleepycat.je.log.ErasedException;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogItem;
import com.sleepycat.je.log.LogParams;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.log.Provisional;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.log.WholeEntry;
import com.sleepycat.je.log.entry.BINDeltaLogEntry;
import com.sleepycat.je.log.entry.INLogEntry;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.statcap.StatUtils;
import com.sleepycat.je.util.verify.BtreeVerifier;
import com.sleepycat.je.util.verify.BtreeVerifyContext;
import com.sleepycat.je.util.verify.OperationVerifyException;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.SizeofMarker;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;
import com.sleepycat.je.utilint.VLSN;

/**
 * An IN represents an Internal Node in the JE tree.
 *
 * Explanation of KD (KnownDeleted) and PD (PendingDelete) entry flags
 * ===================================================================
 *
 * PD: set for all LN entries that are deleted, even before the LN is
 * committed.  Is used as an authoritative (transactionally correct) indication
 * that an LN is deleted. PD will be cleared if the txn for the deleted LN is
 * aborted.
 *
 * KD: set under special conditions for entries containing LNs which are known
 * to be obsolete.  Not used for entries in an active/uncommitted transaction.
 *
 * First notice that IN.fetchLN will allow a FileNotFoundException when the
 * PD or KD flag is set on the entry.  And it will allow a NULL_LSN when the KD
 * flag is set.
 *
 * KD was implemented first, and was originally used when the cleaner attempts
 * to migrate an LN and discovers it is deleted (see Cleaner.migrateLN). We
 * need KD because the INCompressor may not have run, and may not have
 * compressed the BIN. There's the danger that we'll try to fetch that entry,
 * and that the file was deleted by the cleaner.
 *
 * KD was used more recently when an unexpected exception occurs while logging
 * an LN, after inserting the entry.  Rather than delete the entry to clean up,
 * we mark the entry KD so it won't cause a fetch error later.  In this case
 * the entry LSN is NULL_LSN. See Tree.insertNewSlot.
 *
 * PD is closely related to the first use of KD above (for cleaned deleted LNs)
 * and came about because of a cleaner optimization we make. The cleaner
 * considers all deleted LN log entries to be obsolete, without doing a tree
 * lookup, and without any record of an obsolete offset.  This makes the cost
 * of cleaning of deleted LNs very low.  For example, if the log looks like
 * this:
 *
 * 100  LNA
 * 200  delete of LNA
 *
 * then LSN 200 will be considered obsolete when this file is processed by the
 * cleaner. After all, only two things can happen: (1) the txn commits, and we
 * don't need LSN 200, because we can wipe this LN out of the tree, or (2) the
 * txn aborts, and we don't need LSN 200, because we are going to revert to LSN
 * 100/LNA.
 *
 * We set PD for the entry of a deleted LN at the time of the operation, and we
 * clear PD if the transaction aborts.  There is no real danger that this log
 * entry will be processed by the cleaner before it's committed, because
 * cleaning can only happen after the first active LSN.
 *
 * Just as in the first use of KD above, setting PD is necessary to avoid a
 * fetch error, when the file is deleted by the cleaner but the entry
 * containing the deleted LN has not been deleted by the INCompressor.
 *
 * PD is also set in replication rollback, when LNs are marked as
 * invisible.
 *
 * When LSN locking was implemented (see CursorImpl.lockLN), the PD flag took
 * on additional meaning.  PD is used to determine whether an LN is deleted
 * without fetching it, and therefore is relied on to be transactionally
 * correct.
 *
 * In addition to the setting and use of the KD/PD flags described above, the
 * situation is complicated by the fact that we must restore the state of these
 * flags during abort, recovery, and set them properly during slot reuse.
 *
 * We have been meaning to consider whether PD and KD can be consolidated into
 * one flag: simply the Deleted flag.  The Deleted flag would be set in the
 * same way as PD is currently set, as well as the second use of KD described
 * above (when the LSN is NULL_LSN after an insertion error).  The use of KD
 * and PD for invisible entries and recovery rollback should also be
 * considered.
 *
 * If we consolidate the two flags and set the Deleted flag during a delete
 * operation (like PD), we'll have to remove optimizations (in CursorImpl for
 * example) that consider a slot deleted when KD is set.  Since KD is rarely
 * set currently, this shouldn't have a noticeable performance impact.
 */
public class IN extends Node implements Comparable<IN>, LatchContext {

    private static final String BEGIN_TAG = "<in>";
    private static final String END_TAG = "</in>";
    private static final String TRACE_SPLIT = "Split:";
    private static final String TRACE_DELETE = "Delete:";

    /* Used for defunct compact LSN representation. */
    private static final int THREE_BYTE_NEGATIVE_ONE = 0xffffff;

    /**
     * Used as the "empty rep" for the INLongRep lastLoggedSizes field.
     *
     * minLength is 1 because log sizes are unpredictable.
     *
     * allowSparseRep is false because all slots have log sizes and less
     * mutation is better.
     */
    static final INLongRep.EmptyRep EMPTY_LAST_LOGGED_SIZES =
        new INLongRep.EmptyRep(1, false);

    /*
     * Levels:
     * The mapping tree has levels in the 0x20000 -> 0x2ffff number space.
     * The main tree has levels in the 0x10000 -> 0x1ffff number space.
     * The duplicate tree levels are in 0-> 0xffff number space.
     */
    public static final int DBMAP_LEVEL = 0x20000;
    public static final int MAIN_LEVEL = 0x10000;
    public static final int LEVEL_MASK = 0x0ffff;
    public static final int MIN_LEVEL = -1;
    public static final int BIN_LEVEL = MAIN_LEVEL | 1;

    /* Used to indicate that an exact match was found in findEntry. */
    public static final int EXACT_MATCH = (1 << 16);

    /* Used to indicate that an insert was successful. */
    public static final int INSERT_SUCCESS = (1 << 17);

    /*
     * Boolean properties of an IN, encoded as bits inside the flags
     * data member. These flag values are not persistent, although some of
     * them are translated to other persistent flags by serialize().
     */
    private static final int IN_DIRTY_BIT = 0x1;
    private static final int IN_RECALC_TOGGLE_BIT = 0x2;
    private static final int IN_IS_ROOT_BIT = 0x4;
    private static final int IN_HAS_CACHED_CHILDREN_BIT = 0x8;
    private static final int IN_PRI2_LRU_BIT = 0x10;
    private static final int IN_DELTA_BIT = 0x20;
    private static final int IN_FETCHED_COLD_BIT = 0x40;
    private static final int IN_RESIDENT_BIT = 0x80;
    private static final int IN_PROHIBIT_NEXT_DELTA_BIT = 0x100;
    private static final int IN_EXPIRATION_IN_HOURS = 0x200;
    private static final int IN_SUBTREE_SLOTS_REFLECT_LEAST_VALUE = 0x400;

    /* Tracing for LRU-related ops */
    private static final boolean traceLRU = false;
    private static final boolean traceDeltas = false;
    private static final Level traceLevel = Level.INFO;

    private static final LN EMPTY_LN_FOR_VERIFIER = new LN();
    static {
        EMPTY_LN_FOR_VERIFIER.setEmpty();
    }

    DatabaseImpl databaseImpl;

    private int level;

    /* The unique id of this node. */
    long nodeId;

    /* Some bits are persistent and some are not, see serialize. */
    int flags;

    /*
     * The identifier key is a key that can be used used to search for this IN.
     * Initially it is the key of the zeroth slot, but insertions prior to slot
     * zero make this no longer true.  It is always equal to some key in the
     * IN, and therefore it is changed by BIN.compress when removing slots.
     */
    private byte[] identifierKey;

    int nEntries;

    byte[] entryStates;

    /*
     * entryKeys contains the keys in their entirety if key prefixing is not
     * being used. If prefixing is enabled, then keyPrefix contains the prefix
     * and entryKeys contains the suffixes. Records with small enough data
     * (smaller than the value je.tree.maxEmbeddedLN param) are stored in
     * their entirety (both key (or key suffix) and data) inside BINs. This is
     * done by combining the record key and data as a two-part key (see the
     * dbi/DupKeyData class) and storing the resulting array in entryKeys.
     * A special case is when the record to be embedded has no data. Then,
     * the two-part key format is not used, but instead the NO_DATA_LN_BIT
     * is turned on in the slot's state. This saves the space overhead of
     * using the two-part key format.
     */
    INKeyRep entryKeys;
    byte[] keyPrefix;
    long[] entryLsns;

    /*
     * The children of this IN. Only the ones that are actually in the cache
     * have non-null entries. Specialized sparse array represents are used to
     * represent the entries. The representation can mutate as modifications
     * are made to it.
     */
    INTargetRep entryTargets;

    /**
     * Stores the size of the most recently written logrec of each child LN
     * or IN, or zero if the size is unknown.
     *
     * We use INLongRep in spite of the fact that sizes are int not long;
     * INLongRep will store the minimum number of bytes. An EMPTY_REP is
     * used initially until the need arises to add a non-zero value.
     */
    INLongRep lastLoggedSizes = EMPTY_LAST_LOGGED_SIZES;

    long inMemorySize;

    /*
     * Accumulated memory budget delta. Once this exceeds
     * MemoryBudget.ACCUMULATED_LIMIT we inform the MemoryBudget that a change
     * has occurred.  See SR 12273.
     */
    private int accumulatedDelta = 0;

    /*
     * Max allowable accumulation of memory budget changes before MemoryBudget
     * should be updated. This allows for consolidating multiple calls to
     * updateXXXMemoryBudget() into one call.  Not declared final so that the
     * unit tests can modify it.  See SR 12273.
     */
    public static final int ACCUMULATED_LIMIT_DEFAULT = 1000;
    public static int ACCUMULATED_LIMIT = ACCUMULATED_LIMIT_DEFAULT;

    /**
     * References to the next and previous nodes in an LRU list. If the node
     * is not in any LRUList, both of these will be null. If the node is at
     * the front/back of an LRUList, prevLRUNode/nextLRUNode will point to
     * the node itself.
     */
    private IN nextLRUNode = null;
    private IN prevLRUNode = null;

    /*
     * Let L be the most recently written logrec for this IN instance.
     * (a) If this is a UIN, lastFullVersion is the lsn of L.
     * (b) If this is a BIN instance and L is a full-version logrec,
     *     lastFullVersion is the lsn of L.
     * (c) If this is a BIN instance and L is a delta logrec, lastFullVersion
     *     is the lsn of the most recently written full-version logrec for the
     *     same BIN.
     *
     * It is set in 2 cases:
     *
     * (a) after "this" is created via reading a logrec L, lastFullVersion is
     * set to L's lsn, if L is a UIN or a full BIN. (this is done in
     * IN.postFetch/RecoveryInit(), via IN.setLastLoggedLsn()). If L is a BIN
     * delta, lastFullVersion is set by BINDeltaLogEntry.readEntry() to
     * L.prevFullLsn.
     *
     * (b) After logging a UIN or a full-BIN logrec, it is set to the LSN of
     * the logrec written. This is done in IN.afterLog().
     *
     * Notice that this is a persistent field, but except from case (c), when
     * reading a logrec L, it is set not to the value found in L, but to the
     * lsn of L. This is why its read/write is managed by the INLogEntry class
     * rather than the IN readFromLog/writeFromLog methods.
     *
     * lastFullLogSize is the size of that logrec, or zero if lastFullLsn is
     * null.
     */
    private long lastFullLsn = DbLsn.NULL_LSN;
    private int lastFullLogSize = 0;

    /*
     * BINs have a lastDeltaVersion data field as well, which is defined as
     * follows. For UINs, this field is always NULL_LSN.
     *
     * Let L be the most recently written logrec for this BIN instance. If
     * L is a full-version logrec, lastDeltaVersion is NULL; otherwise it
     * is the lsn of L.
     *
     * It is used for obsolete tracking.
     *
     * It is set in 2 cases:
     *
     * (a) after "this" is created via reading a logrec L, lastDeltaVersion
     * is set to L's lsn, if L is a BIN-delta logrec, or to NULL if L is a
     * full-BIN logrec (this is done in IN.postFetch/RecoveryInit(), via
     * BIN.setLastLoggedLsn()).
     *
     * (b) After we write a logrec L for this BIN instance, lastDeltaVersion
     * is set to NULL if L is a full-BIN logrec, or to L's lsn, if L is a
     * BIN-delta logrec (this is done in BIN.afterLog()).
     *
     * Notice that this is a persistent field, but when reading a logrec L,
     * it is set not to the value found in L, but to the lsn of L. This is why
     * its read/write is managed by the INLogEntry class rather than the IN
     * readFromLog/writeFromLog methods.
     *
     * lastDeltaLogSize is the size of the logrec, or zero if lastDeltaLsn is
     * null.
     *
     * Note that the lastDeltaLogSize is not currently used. It was added for
     * completeness (to go along with the size of the last full BIN) and with
     * the expectation that it may be used for utilization counting.
     */
    private long lastDeltaLsn = DbLsn.NULL_LSN;
    private int lastDeltaLogSize = 0;

    /*
     * A sequence of obsolete info that cannot be counted as obsolete until an
     * ancestor IN is logged non-provisionally.
     */
    private PackedObsoleteInfo provisionalObsolete;

    private int pinCount = 0;

    private SharedLatch latch;

    private IN parent;

    private TestHook<?> fetchINHook;

    private static TestHook<IN> latchParentHook;

    private static boolean disableIdKeyUpdateForTesting = false;

    /**
     * Create an empty IN, with no node ID, to be filled in from the log.
     */
    public IN() {
        init(null, Key.EMPTY_KEY, -1, -1, 0);
    }

    /**
     * Create a new IN.
     */
    public IN(
        DatabaseImpl dbImpl,
        byte[] identifierKey,
        int maxCompactKeySize,
        int capacity,
        int level) {

        nodeId = dbImpl.getEnv().getNodeSequence().getNextLocalNodeId();

        init(dbImpl, identifierKey, maxCompactKeySize, capacity,
            generateLevel(dbImpl.getId(), level));

        initMemorySize();
    }

    /**
     * For Sizeof.
     */
    public IN(@SuppressWarnings("unused") SizeofMarker marker) {

        /*
         * Set all variable fields to null, since they are not part of the
         * fixed overhead.
         */
        entryTargets = null;
        entryKeys = null;
        keyPrefix = null;
        entryLsns = null;
        entryStates = null;

        latch = LatchFactory.createSharedLatch(
            LatchSupport.DUMMY_LATCH_CONTEXT, isAlwaysLatchedExclusively());

        /*
         * Use the latch to force it to grow to "runtime size".
         */
        latch.acquireExclusive();
        latch.release();
        latch.acquireExclusive();
        latch.release();
    }

    /**
     * Create a new IN.  Need this because we can't call newInstance() without
     * getting a 0 for nodeId.
     */
    IN createNewInstance(
        byte[] identifierKey,
        int maxCompactKeySize,
        int maxEntries,
        int level) {
        return new IN(
            databaseImpl, identifierKey, maxCompactKeySize, maxEntries, level);
    }

    /**
     * Initialize IN object.
     *
     * @param initialCapacity is -1 when creating an empty IN to be
     * materialized after fetching it from the log.
     */
    protected void init(
        DatabaseImpl db,
        @SuppressWarnings("hiding")
        byte[] identifierKey,
        int maxCompactKeySize,
        int initialCapacity,
        @SuppressWarnings("hiding")
        int level) {

        setDatabase(db);
        latch = LatchFactory.createSharedLatch(
            this, isAlwaysLatchedExclusively());
        flags = 0;
        nEntries = 0;
        this.identifierKey = identifierKey;
        entryTargets = INTargetRep.NONE;
        if (initialCapacity >= 0) {
            entryKeys = INKeyRep.create(
                initialCapacity, maxCompactKeySize, db.getEnv());
            entryLsns = new long[initialCapacity];
            entryStates = new byte[initialCapacity];
        }
        keyPrefix = null;
        this.level = level;
    }

    @Override
    public final boolean isIN() {
        return true;
    }

    @Override
    public final boolean isUpperIN() {
        return !isBIN();
    }

    @Override
    public final String getLatchName() {
        return shortClassName() + getNodeId();
    }

    @Override
    public final int getLatchTimeoutMs() {
        return databaseImpl.getEnv().getLatchTimeoutMs();
    }

    @Override
    public final LatchTable getLatchTable() {
        return LatchSupport.btreeLatchTable;
    }

    /*
     * Return whether the shared latch for this kind of node should be of the
     * "always exclusive" variety.  Presently, only IN's are actually latched
     * shared.  BINs are latched exclusive only.
     */
    boolean isAlwaysLatchedExclusively() {
        return false;
    }

    /**
     * Latch this node if it is not latched by another thread. Update the LRU
     * using the given cacheMode if the latch succeeds.
     */
    public final boolean latchNoWait(CacheMode cacheMode) {
        if (!latch.acquireExclusiveNoWait()) {
            return false;
        }
        updateLRU(cacheMode);
        return true;
    }

    /**
     * Latch this node exclusive and update the LRU using the given cacheMode.
     */
    public void latch(CacheMode cacheMode) {
        latch.acquireExclusive();
        updateLRU(cacheMode);
    }

    /**
     * Latch this node exclusive and update the LRU using the default cacheMode.
     */
    @Override
    public final void latch() {
        latch(CacheMode.DEFAULT);
    }

    /**
     * Latch this node shared and update the LRU using the given cacheMode.
     */
    @Override
    public void latchShared(CacheMode cacheMode) {
        latch.acquireShared();
        updateLRU(cacheMode);
    }

    /**
     * Latch this node shared and update the LRU using the default cacheMode.
     */
    @Override
    public final void latchShared() {
        latchShared(CacheMode.DEFAULT);
    }

    /**
     * Latch this node exclusive and do not update the LRU or cause other
     * related side effects.
     *
     * @param db is passed in order to initialize the database for an
     * uninitialized node, which is necessary in order to latch it.
     */
    public final void latchNoUpdateLRU(DatabaseImpl db) {
        if (databaseImpl == null) {
            databaseImpl = db;
        }
        latch.acquireExclusive();
    }

    /**
     * Latch this node exclusive and do not update the LRU or cause other
     * related side effects.
     */
    public final void latchNoUpdateLRU() {
        assert databaseImpl != null;
        latch.acquireExclusive();
    }

    /**
     * Latch this node shared and do not update the LRU or cause other
     * related side effects.
     */
    public final void latchSharedNoUpdateLRU() {
        assert databaseImpl != null;
        latch.acquireShared();
    }

    /**
     * Release the latch on this node.
     */
    @Override
    public final void releaseLatch() {
        latch.release();
    }

    /**
     * Release the latch on this node if it is owned.
     */
    public final void releaseLatchIfOwner() {
        latch.releaseIfOwner();
    }

    /**
     * @return true if this thread holds the IN's latch
     */
    public final boolean isLatchOwner() {
        return latch.isOwner();
    }

    public final boolean isLatchExclusiveOwner() {
        return latch.isExclusiveOwner();
    }

    /* For unit testing. */
    public final int getLatchNWaiters() {
        return latch.getNWaiters();
    }

    @SuppressWarnings("deprecation")
    public final void updateLRU(CacheMode cacheMode) {

        if (!getInListResident()) {
            return;
        }

        switch (cacheMode) {
        case UNCHANGED:
            break;
        case EVICT_LN:
        case DEFAULT:
            setFetchedCold(false);

            if (isBIN() || !hasCachedChildrenFlag()) {
                assert(isBIN() || !hasCachedChildren());
                getEvictor().moveBack(this);
            }
            break;
        case EVICT_BIN:
            setFetchedCold(false);
            break;
        default:
            assert false;
        }
    }

    /**
     * This method should be used carefully. Unless this node and the parent
     * are already known to be latched, call latchParent instead to access the
     * parent safely.
     */
    public IN getParent() {
        return parent;
    }

    public void setParent(IN in) {
        assert in != null;

        /*
         * Must hold EX-latch when changing a non-null parent. But when setting
         * the parent initially (it is currently null), we assume it is being
         * attached and no other threads have access to it.
         */
        if (parent != null && !isLatchExclusiveOwner()) {
            throw unexpectedState();
        }

        parent = in;
    }

    /**
     * Latches the parent exclusively, leaving this node latched. The parent
     * must not already be latched.
     *
     * This node must be latched on entry and will be latched on exit. This
     * node's latch may be released temporarily, in which case it will be
     * ex-latched (since the parent is ex-latched, this isn't a drawback).
     *
     * Does not perform cache mode processing, since this node is already
     * latched.
     *
     * @return the ex-latched parent, for which calling getKnownChildIndex with
     * this node is guaranteed to succeed.
     *
     * @throws EnvironmentFailureException (fatal) if the parent latch is
     * already held.
     */
    public final IN latchParent() {

        assert latch.isOwner();
        assert !isRoot();
        assert getParent() != null;

        final Tree tree = getDatabase().getTree();

        /*
         * We must release the child latch in order to acquire the parent
         * latch, to follow the latch order rules. If after latching the parent
         * pointer and relatching the child, the parent pointer is still the
         * same, then the parent did not change while unlatched.
         *
         * When the parent is the root we must follow the rule that the
         * Tree.rootRatch is acquired before the root IN latch. Unfortunately
         * we don't know for certain that the parent is the root unless we
         * latch it first. We attempt to avoid latching the parent twice by
         * checking the root property before latching even though this is not
         * guaranteed to be accurate, and then check again after latching in
         * case a stale value was read.
         *
         * If the parent is not the root and we can acquire the root IN latch
         * using a no-wait latch, this is done to avoid unlatching and
         * relatching the child. However, this is not possible if the parent is
         * the root.
         */
        while (true) {
            IN p = getParent();

            if (latchParentHook == null) {
                if (!p.isRoot() && p.latch.acquireExclusiveNoWait()) {
                    if (p.isRoot()) {
                        p.latch.release();
                    } else {
                        return p;
                    }
                }
            }

            pin();
            try {
                latch.release();

                if (latchParentHook != null) {
                    latchParentHook.doHook(this);
                }

                if (p.isRoot()) {
                    p = tree.getRootINLatchedExclusive(CacheMode.UNCHANGED);
                } else {
                    p.latch.acquireExclusive();
                    if (p.isRoot()) {
                        p.latch.release();
                        p = tree.getRootINLatchedExclusive(CacheMode.UNCHANGED);
                    }
                }

                latch.acquireExclusive();
            } finally {
                unpin();
            }

            if (getParent() == p) {
                return p;
            }

            p.latch.release();
        }
    }

    /**
     * Sets a hook to be called while latching the parent during the window
     * when no latches are held.
     *
     * Setting this hook also disables the optimization where we avoid
     * unlatching by using a no-wait latch. This allows reliable testing of the
     * case where no latches are held.
     */
    public static void setLatchParentHook(TestHook<IN> hook) {
        latchParentHook = hook;
    }

    /**
     * Returns the index of the given child. Should only be called when the
     * caller knows that the given child is resident.
     */
    public int getKnownChildIndex(final Node child) {

        for (int i = 0; i < nEntries; i += 1) {
            if (getTarget(i) == child) {
                return i;
            }
        }

        throw unexpectedState();
    }

    public final synchronized void pin() {
        assert(isLatchOwner());
        assert(pinCount >= 0);
        ++pinCount;
    }

    public final synchronized void unpin() {
        assert(pinCount > 0);
        --pinCount;
    }

    public final synchronized boolean isPinned() {
        assert(isLatchExclusiveOwner());
        assert(pinCount >= 0);
        return pinCount > 0;
    }

    /**
     * Get the database for this IN.
     */
    public final DatabaseImpl getDatabase() {
        return databaseImpl;
    }

    /**
     * Set the database reference for this node.
     */
    public final void setDatabase(DatabaseImpl db) {
        databaseImpl = db;
    }

    /*
     * Get the database id for this node.
     */
    public final DatabaseId getDatabaseId() {
        return databaseImpl.getId();
    }

    @Override
    public final EnvironmentImpl getEnvImplForFatalException() {
        return databaseImpl.getEnv();
    }

    public final EnvironmentImpl getEnv() {
        return databaseImpl.getEnv();
    }

    final Evictor getEvictor() {
        return databaseImpl.getEnv().getEvictor();
    }

    /**
     * Convenience method to return the database key comparator.
     */
    public final InternalComparator getKeyComparator() {
        return databaseImpl.getKeyComparator();
    }

    @Override
    public final int getLevel() {
        return level;
    }

    public final int getNormalizedLevel() {
        return level & LEVEL_MASK;
    }

    private static int generateLevel(DatabaseId dbId, int newLevel) {
        if (dbId.equals(DbTree.ID_DB_ID)) {
            return newLevel | DBMAP_LEVEL;
        } else {
            return newLevel | MAIN_LEVEL;
        }
    }

    public final long getNodeId() {
        return nodeId;
    }

    /* For unit tests only. */
    final void setNodeId(long nid) {
        nodeId = nid;
    }

    /**
     * We would like as even a hash distribution as possible so that the
     * Evictor's LRU is as accurate as possible.  ConcurrentHashMap takes the
     * value returned by this method and runs its own hash algorithm on it.
     * So a bit complement of the node ID is sufficient as the return value and
     * is a little better than returning just the node ID.  If we use a
     * different container in the future that does not re-hash the return
     * value, we should probably implement the Wang-Jenkins hash function here.
     */
    @Override
    public final int hashCode() {
        return (int) ~getNodeId();
    }

    @Override
    public final boolean equals(Object obj) {
        if (!(obj instanceof IN)) {
            return false;
        }
        IN in = (IN) obj;
        return (this.getNodeId() == in.getNodeId());
    }

    /**
     * Sort based on equality key.
     */
    @Override
    public final int compareTo(IN argIN) {
        long argNodeId = argIN.getNodeId();
        long myNodeId = getNodeId();

        if (myNodeId < argNodeId) {
            return -1;
        } else if (myNodeId > argNodeId) {
            return 1;
        } else {
            return 0;
        }
    }

    public final boolean getDirty() {
        return (flags & IN_DIRTY_BIT) != 0;
    }

    public final void setDirty(boolean dirty) {
        if (dirty) {
            flags |= IN_DIRTY_BIT;
        } else {
            flags &= ~IN_DIRTY_BIT;
        }
    }

    @Override
    public final boolean isBINDelta() {
        assert(isUpperIN() || isLatchOwner());
        return (flags & IN_DELTA_BIT) != 0;
    }

    /*
     * This version of isBINDelta() takes a checkLatched param to allow
     * for cases where it is ok to call the method without holding the
     * BIN latch (e.g. in single-threaded tests, or when the BIN is not
     * attached to the tree (and thus inaccessible from other threads)).
     */
    @Override
    public final boolean isBINDelta(boolean checkLatched) {
        assert(!checkLatched || isUpperIN() || isLatchOwner());
        return (flags & IN_DELTA_BIT) != 0;
    }

    final void setBINDelta(boolean delta) {
        if (delta) {
            flags |= IN_DELTA_BIT;
        } else {
            flags &= ~IN_DELTA_BIT;
        }
    }

    /**
     * Indicates that the BIN was fetched from disk using CacheMode.UNCHANGED,
     * and has not been accessed with another CacheMode. BINs in this state
     * should be evicted from cache as soon as they are no longer referenced by
     * a cursor.
     */
    public final boolean getFetchedCold() {
        return (flags & IN_FETCHED_COLD_BIT) != 0;
    }

    /** @see #getFetchedCold() */
    public final void setFetchedCold(boolean val) {
        if (val) {
            flags |= IN_FETCHED_COLD_BIT;
        } else {
            flags &= ~IN_FETCHED_COLD_BIT;
        }
    }

    public final boolean getRecalcToggle() {
        return (flags & IN_RECALC_TOGGLE_BIT) != 0;
    }

    public final void setRecalcToggle(boolean toggle) {
        if (toggle) {
            flags |= IN_RECALC_TOGGLE_BIT;
        } else {
            flags &= ~IN_RECALC_TOGGLE_BIT;
        }
    }

    public final boolean isRoot() {
        return (flags & IN_IS_ROOT_BIT) != 0;
    }

    final void setIsRoot(boolean isRoot) {
        setIsRootFlag(isRoot);
        setDirty(true);
    }

    private void setIsRootFlag(boolean isRoot) {
        if (isRoot) {
            flags |= IN_IS_ROOT_BIT;
        } else {
            flags &= ~IN_IS_ROOT_BIT;
        }
    }

    /**
     * @return true if, for the subtree with the current node as root, the
     * least key in subtree is in the 0th key slot of all nodes in the subtree.
     */
    public final boolean doSubtreeSlotsReflectLeastValue() {
        return (flags & IN_SUBTREE_SLOTS_REFLECT_LEAST_VALUE) != 0;
    }

    /**
     * This flag is set if, for the subtree with the current node as root, the
     * least key in subtree is in the 0th key slot of all nodes in the subtree.
     *
     * @param value
     */
    public void setFlagSubtreeSlotsReflectLeastValue(boolean value) {
        if (value) {
            flags |= IN_SUBTREE_SLOTS_REFLECT_LEAST_VALUE;
        } else {
            flags &= ~IN_SUBTREE_SLOTS_REFLECT_LEAST_VALUE;
        }
    }

    public final boolean hasCachedChildrenFlag() {
        return (flags & IN_HAS_CACHED_CHILDREN_BIT) != 0;
    }

    private void setHasCachedChildrenFlag(boolean value) {
        if (value) {
            flags |= IN_HAS_CACHED_CHILDREN_BIT;
        } else {
            flags &= ~IN_HAS_CACHED_CHILDREN_BIT;
        }
    }

    public final boolean isInPri2LRU() {
        return (flags & IN_PRI2_LRU_BIT) != 0;
    }

    /* public for unit tests */
    public final void setInPri2LRU(boolean value) {
        if (value) {
            flags |= IN_PRI2_LRU_BIT;
        } else {
            flags &= ~IN_PRI2_LRU_BIT;
        }
    }

    public boolean isExpirationInHours() {
        return (flags & IN_EXPIRATION_IN_HOURS) != 0;
    }

    void setExpirationInHours(boolean value) {
        if (value) {
            flags |= IN_EXPIRATION_IN_HOURS;
        } else {
            flags &= ~IN_EXPIRATION_IN_HOURS;
        }
    }

    /**
     * @return the identifier key for this node.
     */
    public final byte[] getIdentifierKey() {
        return identifierKey;
    }

    /**
     * Set the identifier key for this node.
     *
     * @param key - the new identifier key for this node.
     *
     * @param makeDirty should normally be true, but may be false when an
     * expired slot containing the identifier key has been deleted.
     */
    public final void setIdentifierKey(byte[] key, boolean makeDirty) {

        assert(!isBINDelta());

        /*
         * The identifierKey is "intentionally" not kept track of in the
         * memory budget.  If we did, then it would look like this:

         int oldIDKeySz = (identifierKey == null) ?
                           0 :
                           MemoryBudget.byteArraySize(identifierKey.length);

         int newIDKeySz = (key == null) ?
                           0 :
                           MemoryBudget.byteArraySize(key.length);
         updateMemorySize(newIDKeySz - oldIDKeySz);

         */
        identifierKey = key;

        if (makeDirty) {
            setDirty(true);
        }
    }

    /**
     * @return the number of entries in this node.
     */
    public final int getNEntries() {
        return nEntries;
    }

    /**
     * @return the maximum number of entries in this node.
     *
     * Overriden by TestIN in INEntryTestBase.java
     */
    public int getMaxEntries() {
        return entryStates.length;
    }

    public final byte getState(int idx) {
        return entryStates[idx];
    }

    /**
     * @return true if the object is dirty.
     */
    public final boolean isDirty(int idx) {
        return ((entryStates[idx] & EntryStates.DIRTY_BIT) != 0);
    }

    public final boolean isUpdateKeyWhenLogged(int idx) {
        return ((entryStates[idx] & EntryStates.UPDATE_KEY_WHEN_LOGGED) != 0);
    }

    /**
     * @return true if the idx'th entry has been deleted, although the
     * transaction that performed the deletion may not be committed.
     */
    public final boolean isEntryPendingDeleted(int idx) {
        return ((entryStates[idx] & EntryStates.PENDING_DELETED_BIT) != 0);
    }

    /**
     * Set pendingDeleted to true.
     */
    public final void setPendingDeleted(int idx) {

        entryStates[idx] |= EntryStates.PENDING_DELETED_BIT;
        entryStates[idx] |= EntryStates.DIRTY_BIT;
        setDirty(true);
    }

    /**
     * Set pendingDeleted to false.
     */
    final void clearPendingDeleted(int idx) {

        entryStates[idx] &= EntryStates.CLEAR_PENDING_DELETED_BIT;
        entryStates[idx] |= EntryStates.DIRTY_BIT;
        setDirty(true);
    }

    /**
     * @return true if the idx'th entry is deleted for sure.  If a transaction
     * performed the deletion, it has been committed.
     */
    public final boolean isEntryKnownDeleted(int idx) {
        return ((entryStates[idx] & EntryStates.KNOWN_DELETED_BIT) != 0);
    }

    /**
     * Set KD flag to true and clear the PD flag (PD does not need to be on
     * if KD is on).
     */
    public final void setKnownDeleted(int idx) {

        assert(isBIN());

        entryStates[idx] |= EntryStates.KNOWN_DELETED_BIT;
        entryStates[idx] &= EntryStates.CLEAR_PENDING_DELETED_BIT;
        entryStates[idx] |= EntryStates.DIRTY_BIT;
        setDirty(true);
    }

    /**
     * Set knownDeleted flag to true and evict the child LN if cached. The
     * child LN is evicted to save memory, since it will never be fetched
     * again.
     */
    public final void setKnownDeletedAndEvictLN(int index) {

        assert(isBIN());

        setKnownDeleted(index);

        LN oldLN = (LN) getTarget(index);
        if (oldLN != null) {
            updateMemorySize(oldLN, null /* newNode */);
        }
        setTarget(index, null);
    }

    /**
     * Set knownDeleted to false.
     */
    final void clearKnownDeleted(int idx) {

        assert(isBIN());

        entryStates[idx] &= EntryStates.CLEAR_KNOWN_DELETED_BIT;
        entryStates[idx] |= EntryStates.DIRTY_BIT;
        setDirty(true);
    }

    /**
     * Returns tombstone property for a slot.
     */
    public boolean isTombstone(int idx) {
        return (entryStates[idx] & EntryStates.TOMBSTONE_BIT) != 0;
    }

    /**
     * Sets tombstone property for a slot.
     */
    public void setTombstone(int idx, boolean tombstone) {
        if (tombstone) {
            entryStates[idx] |= EntryStates.TOMBSTONE_BIT;
        } else {
            entryStates[idx] &= EntryStates.CLEAR_TOMBSTONE_BIT;
        }
        entryStates[idx] |= EntryStates.DIRTY_BIT;
        setDirty(true);
    }

    /*
     * In the future we may want to move the following static methods to an
     * EntryState utility class and share all state bit twidling among IN,
     * ChildReference, and DeltaInfo.
     */

    /**
     * Returns true if the given state is known deleted.
     */
    static boolean isStateKnownDeleted(byte state) {
        return ((state & EntryStates.KNOWN_DELETED_BIT) != 0);
    }

    /**
     * Returns true if the given state is pending deleted.
     */
    static boolean isStatePendingDeleted(byte state) {
        return ((state & EntryStates.PENDING_DELETED_BIT) != 0);
    }

    /**
     * Return true if the LN at the given slot is embedded.
     */
    public final boolean isEmbeddedLN(int idx) {
        return ((entryStates[idx] & EntryStates.EMBEDDED_LN_BIT) != 0);
    }

    public static boolean isEmbeddedLN(byte state) {
        return ((state & EntryStates.EMBEDDED_LN_BIT) != 0);
    }

    /**
     * Set embeddedLN to true.
     */
    private void setEmbeddedLN(int idx) {

        entryStates[idx] |= EntryStates.EMBEDDED_LN_BIT;
        entryStates[idx] |= EntryStates.DIRTY_BIT;
        setDirty(true);
    }

    /**
     * Set embeddedLN to false.
     */
    private void clearEmbeddedLN(int idx) {

        entryStates[idx] &= EntryStates.CLEAR_EMBEDDED_LN_BIT;
        entryStates[idx] |= EntryStates.DIRTY_BIT;
        setDirty(true);
    }

    /**
     * Return true if the LN at the given slot is an embedded LN with no data.
     */
    public final boolean isNoDataLN(int idx) {
        return ((entryStates[idx] & EntryStates.NO_DATA_LN_BIT) != 0);
    }

    public static boolean isNoDataLN(byte state) {
        return ((state & EntryStates.NO_DATA_LN_BIT) != 0);
    }

    /**
     * Set noDataLN to true.
     */
    void setNoDataLN(int idx) {

        entryStates[idx] |= EntryStates.NO_DATA_LN_BIT;
        entryStates[idx] |= EntryStates.DIRTY_BIT;
        setDirty(true);
    }

    /**
     * Set noDataLN to false.
     */
    private void clearNoDataLN(int idx) {

        entryStates[idx] &= EntryStates.CLEAR_NO_DATA_LN_BIT;
        entryStates[idx] |= EntryStates.DIRTY_BIT;
        setDirty(true);
    }

    /**
     * Returns whether slot has non-empty embedded data, meaning that the data
     * is embedded in the key value. Returns false if the data is zero length,
     * even though isEmbedded(idx) is true in that case.
     */
    final boolean haveEmbeddedData(int idx) {
        return (isEmbeddedLN(idx) && !isNoDataLN(idx));
    }

    /* For unit testing */
    public final int getNumEmbeddedLNs() {
        int res = 0;
        for (int i = 0; i < getNEntries(); ++i) {
            if (isEmbeddedLN(i)) {
                ++res;
            }
        }

        return res;
    }

    /* For unit testing */
    public final INKeyRep getKeyVals() {
        return entryKeys;
    }

    public final byte[] getKeyPrefix() {
        return keyPrefix;
    }

    /*
     * For unit testing only
     */
    public final boolean hasKeyPrefix() {
        return keyPrefix != null;
    }

    /* This has default protection for access by the unit tests. */
    final void setKeyPrefix(byte[] keyPrefix) {

        assert databaseImpl != null;

        int prevLength = (this.keyPrefix == null) ? 0 : this.keyPrefix.length;
        this.keyPrefix = keyPrefix;
        /* Update the memory budgeting to reflect changes in the key prefix. */
        int currLength = (keyPrefix == null) ? 0 : keyPrefix.length;
        updateMemorySize(prevLength, currLength);
    }

    /**
     * Return the idx'th key. If prefixing is enabled, construct a new byte[]
     * containing the prefix and suffix. If prefixing is not enabled, just
     * return the current byte[] in entryKeys.
     */
    public final byte[] getKey(int idx) {
        assert idx < nEntries;

        byte[] key = entryKeys.getFullKey(
            keyPrefix, idx, haveEmbeddedData(idx));

        assert(key != null);

        return key;
    }

    public final byte[] getEmbeddedData(int idx) {

        if (haveEmbeddedData(idx)) {
            return entryKeys.getData(idx);
        }

        if (isNoDataLN(idx)) {
            return Key.EMPTY_KEY;
        }

        return null;
    }

    /*
     * TODO: Call this method from CursorImpl insert/update methods to avoid
     *  unnecessary allocations.
     */
    public final boolean getEmbeddedData(int idx, DatabaseEntry data) {

        if (haveEmbeddedData(idx)) {
            entryKeys.getData(idx, data);
            return true;
        }

        if (isNoDataLN(idx)) {
            data.setSize(0);
            return true;
        }

        return false;
    }

    /**
     * Returns the size of the key that is stored persistently, which will be
     * the combined key-data for an embedded LN or duplicated DB record.
     */
    int getStoredKeySize(int idx) {
        return entryKeys.size(idx)
            + (keyPrefix == null ? 0 : keyPrefix.length);
    }

    /**
     * Updates the key in the idx-th slot of this BIN, if the DB allows key
     * updates and the new key is not identical to the current key in the slot.
     * It also updates the data (if any) that is embedded with the key in the
     * idx-slot, or embeds new data in that slot, is the "data" param is
     * non-null, or removes embedded data, if "data" is null. Finally, it
     * sets the EMBEDDED_LN_BIT and NO_DATA_LN_BIT flags in the slot's state.
     *
     * @param key is the key to set in the slot and is the LN key.
     *
     * @param embData If the data portion of a record must be embedded in this
     * BIN, "data" stores the record's data. Null otherwise. See also comment
     * for the keyEntries field.
     *
     * @return true if a multi-slot change was made and the complete IN memory
     * size must be updated.
     */
    private boolean updateLNSlotKey(int idx, byte[] key, byte[] embData) {

        assert(isBIN());

        boolean haveEmbeddedData = haveEmbeddedData(idx);

        if (embData == null) {
            if (isEmbeddedLN(idx)) {
                clearEmbeddedLN(idx);
                clearNoDataLN(idx);
            }
        } else {
            if (!isEmbeddedLN(idx)) {
                setEmbeddedLN(idx);
            }
            if (embData.length == 0) {
                setNoDataLN(idx);
            } else {
                clearNoDataLN(idx);
            }
        }

        /*
         * The new key may be null if a dup LN was deleted, in which case there
         * is no need to update it.  There is no need to compare keys if there
         * is no comparator configured, since a key cannot be changed when the
         * default comparator is used.
         */
        if (key != null &&
            databaseImpl.allowsKeyUpdates() &&
            !Arrays.equals(key, getKey(idx))) {

            setDirty(true);
            return setKey(idx, key, embData, false);

        } else if (haveEmbeddedData) {

            /*
             * The key does not change, but the slot contains embedded data,
             * which must now either be removed (if data == null or
             * data.length == 0) or updated.
             * TODO #21488: update the data only if it actually changes.
             */
            setDirty(true);
            entryStates[idx] |= EntryStates.DIRTY_BIT;

            INKeyRep.Type oldRepType = entryKeys.getType();
            entryKeys = entryKeys.setData(idx, embData, this);
            return oldRepType != entryKeys.getType();

        } else if (embData != null && embData.length != 0) {

            /*
             * The key does not change, but we now have to embed data in a slot
             * that does not currently have embedded data.
             */
            setDirty(true);
            entryStates[idx] |= EntryStates.DIRTY_BIT;

            key = entryKeys.getKey(idx, false);
            INKeyRep.Type oldRepType = entryKeys.getType();
            entryKeys = entryKeys.set(idx, key, embData, this);
            return oldRepType != entryKeys.getType();

        } else {
            return false;
        }
    }

    /*
     * Convenience wrapper for setKey() method below
     */
    private boolean insertKey(
        int idx,
        byte[] key,
        byte[] embData) {

        /*
         * Set the id key when inserting the first entry. This is important
         * when compression removes all entries from a BIN, and then an entry
         * is inserted before the empty BIN is purged.
         */
        if (nEntries == 1 && !isBINDelta()) {
            setIdentifierKey(key, true /*makeDirty*/);
        }

        return setKey(idx, key, embData, true);
    }

    // TODO re-enable this and figure out why it is firing

    private boolean idKeyIsSlotKey() {

        final boolean DISABLED = true;
        if (DISABLED) {
            return true;
        }

        if (!isBIN() || nEntries == 0) {
            return true;
        }

        for (int i = 0; i < nEntries; i += 1) {

            if (entryKeys.compareKeys(
                identifierKey,  keyPrefix, i, haveEmbeddedData(i),
                databaseImpl.getKeyComparator()) == 0) {

                return true;
            }
        }

        return false;
    }

    /*
     * Convenience wrapper for setKey() method below. It is used for
     * upper INs only, so no need to worry about the EMBEDDED_LN_BIT
     * and NO_DATA_LN_BIT flags.
     */
    private boolean updateKey(
        int idx,
        byte[] key,
        byte[] embData) {
        return setKey(idx, key, embData, false);
    }

    /**
     * This method inserts or updates a key at a given slot. In either case,
     * the associated embedded data (if any) is inserted or updated as well,
     * and the key prefix is adjusted, if necessary.
     *
     * In case of insertion (indicated by a true value for the isInsertion
     * param), it is assumed that the idx slot does not store any valid info,
     * so any change to the key prefix (if any) is due to the insertion of
     * this new new key and not to the removal of the current key at the idx
     * slot.
     *
     * In case of update, the method does not check if the current key is
     * indeed different from the new key; it just updates the key
     * unconditionally. If the slot has embedded data, that data will also
     * be updated (if the data param is not null), or be removed (if the data
     * param is null). If the slot does not have embedded data and the data
     * param is not null, the given data will be embedded.
     *
     * Note: For BINs, the maintenance of the EMBEDDED_LN_BIT andNO_DATA_LN_BIT
     * is done by the callers of this method.
     *
     * @param embData If the data portion of a record must be embedded in this
     * BIN, "data" stores the record's data. Null otherwise. See also comment
     * for the keyEntries field.
     *
     * @return true if a multi-slot change was made and the complete IN memory
     * size must be updated.
     */
    public boolean setKey(
        int idx,
        byte[] key,
        byte[] embData,
        boolean isInsertion) {

        entryStates[idx] |= EntryStates.DIRTY_BIT;
        setDirty(true);

        /*
         * Only compute key prefix if prefixing is enabled and there's an
         * existing prefix.
         */
        if (databaseImpl.getKeyPrefixing() && keyPrefix != null) {

            int newPrefixLen = Key.getKeyPrefixLength(
                keyPrefix, keyPrefix.length, key);

            if (newPrefixLen < keyPrefix.length) {

                /*
                 * The new key doesn't share the current prefix, so recompute
                 * the prefix and readjust all the existing suffixes.
                 */
                byte[] newPrefix = (isInsertion ?
                                    Key.createKeyPrefix(keyPrefix, key) :
                                    computeKeyPrefix(idx));

                if (newPrefix != null) {
                    /* Take the new key into consideration for new prefix. */
                    newPrefix = Key.createKeyPrefix(newPrefix, key);
                }

                recalcSuffixes(newPrefix, key, embData, idx);
                return true;

            } else {

                INKeyRep.Type prevRepType = entryKeys.getType();

                byte[] suffix = computeKeySuffix(keyPrefix, key);
                entryKeys = entryKeys.set(idx, suffix, embData, this);

                return prevRepType != entryKeys.getType();
            }

        } else if (keyPrefix != null) {

            /*
             * Key prefixing has been turned off on this database, but there
             * are existing prefixes. Remove prefixes for this IN.
             */
            recalcSuffixes(null, key, embData, idx);
            return true;

        } else {
            INKeyRep.Type oldRepType = entryKeys.getType();
            entryKeys = entryKeys.set(idx, key, embData, this);
            return oldRepType != entryKeys.getType();
        }
    }

    /**
     * Given 2 byte arrays, "prefix" and "key", where "prefix" is or stores
     * a prefix of "key", allocate and return another byte array that stores
     * the suffix of "key" w.r.t. "prefix".
     */
    private static byte[] computeKeySuffix(byte[] prefix, byte[] key) {

        int prefixLen = (prefix == null ? 0 : prefix.length);

        if (prefixLen == 0) {
            return key;
        }

        int suffixLen = key.length - prefixLen;
        byte[] ret = new byte[suffixLen];
        System.arraycopy(key, prefixLen, ret, 0, suffixLen);
        return ret;
    }

    /**
     * Iterate over all keys in this IN and recalculate their suffixes based on
     * newPrefix.  If keyVal and idx are supplied, it means that entry[idx] is
     * about to be changed to keyVal so use that instead of
     * entryKeys.get(idx) when computing the new suffixes. If idx is < 0,
     * and keyVal is null, then recalculate suffixes for all entries in this.
     */
    private void recalcSuffixes(
        byte[] newPrefix,
        byte[] key,
        byte[] embData,
        int idx) {

        if (newPrefix == null && keyPrefix == null) {
            return;
        }

        for (int i = 0; i < nEntries; i++) {

            byte[] curKey = (i == idx ? key : getKey(i));

            byte[] curEmbData = null;

            if (i == idx) {
                curEmbData = embData;
            } else if (haveEmbeddedData(i)) {
                curEmbData = entryKeys.getData(i);
            }

            byte[] suffix = computeKeySuffix(newPrefix, curKey);

            entryKeys = entryKeys.set(i, suffix, curEmbData, this);
        }

        setKeyPrefix(newPrefix);
    }

    /**
     * Forces computation of the key prefix, without requiring a split.
     * Is public for use by DbCacheSize.
     */
    public final void recalcKeyPrefix() {

        assert(!isBINDelta());

        recalcSuffixes(computeKeyPrefix(-1), null, null, -1);
    }

    /**
     * Computes a key prefix based on all the keys in 'this'.  Return null if
     * the IN is empty or prefixing is not enabled or there is no common
     * prefix for the keys.
     */
    private byte[] computeKeyPrefix(int excludeIdx) {

        if (!databaseImpl.getKeyPrefixing() || nEntries <= 1) {
            return null;
        }

        int firstIdx = (excludeIdx == 0) ? 1 : 0;
        byte[] curPrefixKey = getKey(firstIdx);
        int prefixLen = curPrefixKey.length;

        /*
         * Only need to look at first and last entries when keys are ordered
         * byte-by-byte.  But when there is a comparator, keys are not
         * necessarily ordered byte-by-byte.  [#21328]
         */
        boolean byteOrdered;
        final boolean OPTIMIZATION_DISABLED = true;
        if (OPTIMIZATION_DISABLED) {
            /* Disable optimization for now.  Needs testing. */
            byteOrdered = false;
        } else {
            byteOrdered = (databaseImpl.getKeyComparator() == null);
        }

        if (byteOrdered) {
            int lastIdx = nEntries - 1;
            if (lastIdx == excludeIdx) {
                lastIdx -= 1;
            }
            if (lastIdx > firstIdx) {
                byte[] lastKey = getKey(lastIdx);
                int newPrefixLen = Key.getKeyPrefixLength(
                    curPrefixKey, prefixLen, lastKey);

                if (newPrefixLen < prefixLen) {
                    curPrefixKey = lastKey;
                    prefixLen = newPrefixLen;
                }
            }
        } else {
            for (int i = firstIdx + 1; i < nEntries; i++) {

                if (i == excludeIdx) {
                    continue;
                }

                byte[] curKey = getKey(i);

                int newPrefixLen = Key.getKeyPrefixLength(
                    curPrefixKey, prefixLen, curKey);

                if (newPrefixLen < prefixLen) {
                    curPrefixKey = curKey;
                    prefixLen = newPrefixLen;
                }
            }
        }

        byte[] ret = new byte[prefixLen];
        System.arraycopy(curPrefixKey, 0, ret, 0, prefixLen);

        return ret;
    }

    /**
     * For debugging.
     */
    final boolean verifyKeyPrefix() {

        byte[] computedKeyPrefix = computeKeyPrefix(-1);
        if (keyPrefix == null) {
            return computedKeyPrefix == null;
        }

        if (computedKeyPrefix == null ||
            computedKeyPrefix.length < keyPrefix.length) {
            System.out.println("VerifyKeyPrefix failed");
            System.out.println(dumpString(0, false));
            return false;
        }
        for (int i = 0; i < keyPrefix.length; i++) {
            if (keyPrefix[i] != computedKeyPrefix[i]) {
                System.out.println("VerifyKeyPrefix failed");
                System.out.println(dumpString(0, false));
                return false;
            }
        }
        return true;
    }

    /**
     * Returns whether the given key is greater than or equal to the first key
     * in the IN and less than or equal to the last key in the IN.  This method
     * is used to determine whether a key to be searched for or inserted
     * belongs in this IN, without doing a tree search.  If false is returned
     * it is still possible that the key belongs in this IN, but a tree search
     * must be performed to find out.
     */
    public final boolean isKeyInBounds(byte[] key) {

        if (nEntries < 2) {
            return false;
        }

        InternalComparator comparator = getKeyComparator();
        int cmp;

        /* Compare key given to my first key. */
        cmp = entryKeys.compareKeys(
            key, keyPrefix, 0, haveEmbeddedData(0), comparator);

        if (cmp < 0) {
            return false;
        }

        /* Compare key given to my last key. */
        int idx =  nEntries - 1;
        cmp = entryKeys.compareKeys(
            key, keyPrefix, idx, haveEmbeddedData(idx), comparator);

        return cmp <= 0;
    }

    /**
     * This method checks if the identifier key matches the value in the 0th
     * index slot. If not, it updates the identifier key to the value in the
     * 0th slot. Note: This method must be called while holding a latch on
     * the corresponding IN.
     *
     * @return Latest value of Identifier Key
     */
    byte[] getUpdatedIdKey() {
        int cmp = entryKeys
            .compareKeys(getIdentifierKey(), keyPrefix, 0, haveEmbeddedData(0),
                getKeyComparator());

        if (cmp != 0) {
            setIdentifierKey(getKey(0), true);
        }

        return getIdentifierKey();
    }

    private void setUpdateKeyWhenLogged(int idx) {
        /*
         * No dirty bit needed, UPDATE_KEY_WHEN_LOGGED only applies to upper INs.
         */
        entryStates[idx] |= EntryStates.UPDATE_KEY_WHEN_LOGGED;
    }

    /**
     * This method checks if the supplied key matches the key corresponding to
     * the supplied index. If not then the slot is marked to be updated when
     * the IN is logged.
     */
    public void updateChildKeyWhenLogged(int idx, IN child) {
        assert !isBIN();

        if (disableIdKeyUpdateForTesting) {
            return;
        }

        if (child.getNEntries() == 0 || child.isBINDelta()) {
            return;
        }

        byte[] key = child.getUpdatedIdKey();

        assert idx > -1;
        assert key != null;

        int cmp = entryKeys.compareKeys(
            key, keyPrefix, idx, haveEmbeddedData(idx), getKeyComparator());

        if (cmp == 0) {
            return;
        }

        if (!child.doSubtreeSlotsReflectLeastValue()) {
            /*
             * Child hasn't reconciled least value in tree with id key.
             */
            return;
        }

        setUpdateKeyWhenLogged(idx);
    }

    private boolean checkKeyOrder(byte[] key, int idx) {
        if (idx > 0) {
            if (entryKeys.compareKeys(key, keyPrefix, idx - 1,
                                      haveEmbeddedData(idx - 1),
                                      getKeyComparator()) <= 0) {
                return false;
            }
        }
        if (nEntries > (idx + 1)) {
            if (entryKeys.compareKeys(key, keyPrefix, idx + 1,
                                      haveEmbeddedData(idx + 1),
                                      getKeyComparator()) >= 0) {
                return false;
            }
        }
        return true;
    }

    /**
     *
     * See KVSTORE-1323.
     */
    private void updateChildKeys() {
        assert(!isBIN());
        for (int idx = 0; idx < nEntries; idx++) {
            if (isUpdateKeyWhenLogged(idx)) {
                IN child = fetchIN(idx, CacheMode.UNCHANGED);

                /*
                 * No need to worry about embedded data since this is not
                 * called for BINs.
                 */
                final boolean latchHere = !child.isLatchOwner();

                if (latchHere) {
                    child.latchShared(CacheMode.UNCHANGED);
                }
                byte[] childIdKey;
                try {
                    childIdKey = child.getIdentifierKey();
                } finally {
                    if (latchHere) {
                        child.releaseLatch();
                    }
                }
                updateKey(idx, childIdKey, null/*data*/);
                assert(checkKeyOrder(childIdKey, idx));

                updateMemorySize(inMemorySize, computeMemorySize());

                entryStates[idx] &= EntryStates.CLEAR_UPDATE_KEY_WHEN_LOGGED;
                if (idx == 0) {
                    setFlagSubtreeSlotsReflectLeastValue(true);
                }
            }
            if (nEntries > 0) {
                getUpdatedIdKey();
            }
        }
    }

    public static void disableIDKeyUpdateForTesting(boolean value) {
        disableIdKeyUpdateForTesting = value;
    }

    public static boolean isIDKeyUpdateDisabledForTesting() {
        return disableIdKeyUpdateForTesting;
    }

    /**
     * Return the idx'th LSN for this entry.
     *
     * @return the idx'th LSN for this entry.
     */
    public final long getLsn(int idx) {
        return entryLsns[idx];
    }

    /**
     * Set the LSN of the idx'th slot, mark the slot dirty, and update
     * memory consuption. Throw exception if the update is not legitimate.
     */
    public void setLsn(int idx, long lsn) {
        setLsn(idx, lsn, true);
    }

    /**
     * Set the LSN of the idx'th slot, mark the slot dirty, and update
     * memory consuption. If "check" is true, throw exception if the
     * update is not legitimate.
     */
    private void setLsn(int idx, long lsn, boolean check) {

        if (!check || shouldUpdateLsn(getLsn(idx), lsn)) {

            int oldSize = computeLsnOverhead();

            /* setLsnInternal can mutate to an array of longs. */
            setLsnInternal(idx, lsn);

            updateMemorySize(computeLsnOverhead() - oldSize);
            entryStates[idx] |= EntryStates.DIRTY_BIT;
            setDirty(true);
        }
    }

    /**
     * Set the LSN of the idx'th slot. If the current storage for LSNs is the
     * compact one, mutate it to the the non-compact, if necessary.
     */
    private void setLsnInternal(int idx, long value) {
        entryLsns[idx] = value;
    }

    /**
     * Ensures that a null LSN is not accidentally assigned when the env is
     * not read-only.
     */
    private boolean shouldUpdateLsn(long oldLsn, long newLsn) {

        /* Save a little computation in packing/updating an unchanged LSN. */
        if (oldLsn == newLsn) {
            return false;
        }
        /* The rules for a new null LSN can be broken in a read-only env. */
        if (newLsn != DbLsn.NULL_LSN || getEnv().isReadOnly()) {
            return true;
        }
        throw unexpectedState(
            "Assigning a NULL_LSN is not allowed" +
            " oldLsn = " + DbLsn.getNoFormatString(oldLsn) +
            " newLsn = " + DbLsn.getNoFormatString(newLsn));
    }

    /**
     * The last logged size is never needed when the LN is counted obsolete
     * immediately, since it is only needed for counting an LN obsolete
     * during an update or deletion.
     *
     * This method may not be called until after the database is initialized,
     * i,e., it may not be called during readFromLog.
     */
    boolean isLastLoggedSizeStored(int idx) {
        return mayHaveLastLoggedSizeStored() && !isEmbeddedLN(idx);
    }

    boolean mayHaveLastLoggedSizeStored() {
        return isUpperIN() || !databaseImpl.isLNImmediatelyObsolete();
    }

    /**
     * Sets last logged size if necessary.
     *
     * This method does not dirty the IN because the caller methods dirty it,
     * for example, when setting the LSN, key, or node.
     *
     * This method is sometimes called to add the logged size for a pre log
     * version 9 BIN, for example, during fetchTarget and preload.  This makes
     * the logged size available for obsolete counting but does not dirty the
     * IN, since that could cause an unexpected write of the IN being read.
     *
     * @param lastLoggedSize is positive if the size is known, zero if the size
     * is unknown, or -1 if the size should not be changed because logging of
     * the LN was deferred.
     */
    public void setLastLoggedSize(int idx, int lastLoggedSize) {
        if ((lastLoggedSize < 0) || !isLastLoggedSizeStored(idx)) {
            return;
        }
        setLastLoggedSizeUnconditional(idx, lastLoggedSize);
    }

    public void clearLastLoggedSize(int idx) {
        setLastLoggedSizeUnconditional(idx, 0);
    }

    /**
     * Sets the size without checking whether it is appropriate.
     *
     * This method is used when reading from the log because the databaseImpl
     * is not yet initialized and isLastLoggedSizeStored cannot be called.
     * It is also called for efficiency reasons when it is known that storing
     * the logged size is necessary, for example, when copying values between
     * slots.
     */
    void setLastLoggedSizeUnconditional(int idx, int lastLoggedSize) {
        lastLoggedSizes = lastLoggedSizes.set(idx, lastLoggedSize, this);
    }

    /**
     * @return a positive value if the size is known, or zero if unknown.
     */
    public int getLastLoggedSize(int idx) {
        if (isLastLoggedSizeStored(idx)) {
            return (int) lastLoggedSizes.get(idx);
        }
        return 0;
    }

    private int getLastLoggedSizeUnconditional(int idx) {
        return (int) lastLoggedSizes.get(idx);
    }

    public final INTargetRep getTargets() {
        return entryTargets;
    }

    /**
     * Sets the idx'th target. No need to make dirty, that state only applies
     * to key and LSN.
     *
     * <p>WARNING: This method does not update the memory budget.  The caller
     * must update the budget.</p>
     */
    void setTarget(int idx, Node target) {

        assert isLatchExclusiveOwner() :
            "Not latched for write " + getClass().getName() +
            " id=" + getNodeId();

        final Node curChild = entryTargets.get(idx);

        entryTargets = entryTargets.set(idx, target, this);

        if (target != null && target.isIN()) {
            ((IN) target).setParent(this);
        }

        if (isUpperIN()) {
            if (target == null) {

                /*
                 * If this UIN has just lost its last cached child, set its
                 * hasCachedChildren flag to false and put it back to the
                 * LRU list.
                 */
                if (curChild != null &&
                    hasCachedChildrenFlag() &&
                    !hasCachedChildren()) {

                    setHasCachedChildrenFlag(false);

                    if (!isDIN()) {
                        if (traceLRU) {
                            LoggerUtils.envLogMsg(
                                traceLevel, getEnv(),
                                Thread.currentThread().getId() + "-" +
                                    Thread.currentThread().getName() +
                                    "-" + getEnv().getName() +
                                    " setTarget(): " +
                                    " Adding UIN " + getNodeId() +
                                    " to LRU after detaching child " +
                                    ((IN) curChild).getNodeId());
                        }
                        getEvictor().addBack(this);
                    }
                }
            } else {
                if (curChild == null &&
                    !hasCachedChildrenFlag()) {

                    setHasCachedChildrenFlag(true);

                    if (traceLRU) {
                        LoggerUtils.envLogMsg(
                            traceLevel, getEnv(),
                            Thread.currentThread().getId() + "-" +
                                Thread.currentThread().getName() +
                                "-" + getEnv().getName() +
                                " setTarget(): " +
                                " Removing UIN " + getNodeId() +
                                " after attaching child " +
                                ((IN) target).getNodeId());
                    }
                    getEvictor().remove(this);
                }
            }
        }
    }

    /**
     * Return the idx'th target.
     */
    public final Node getTarget(int idx) {
        return entryTargets.get(idx);
    }

    /**
     * Returns the idx-th child of "this" upper IN, fetching the child from
     * the log and attaching it to its parent if it is not already attached.
     * This method is used during tree searches.
     *
     * On entry, the parent must be latched already.
     *
     * If the child must be fetched from the log, the parent is unlatched.
     * After the disk read is done, the parent is relatched. However, due to
     * splits, it may not be the correct parent anymore. If so, the method
     * will return null, and the caller is expected to restart the tree search.
     *
     * On return, the parent will be latched, unless null is returned or an
     * exception is thrown.
     *
     * The "searchKey" param is the key that the caller is looking for. It is
     * used by this method in determining if, after a disk read, "this" is
     * still the correct parent for the child. "searchKey" may be null if the
     * caller is doing a LEFT or RIGHT search.
     */
    final IN fetchINWithNoLatch(int idx,
                                byte [] searchKey) {
        return fetchINWithNoLatch(idx, searchKey, null);
    }

    /**
     * This variant of fetchIN() takes a SearchResult as a param, instead of
     * an idx (it is used by Tree.getParentINForChildIN()). The ordinal number
     * of the child to fetch is specified by result.index. result.index will
     * be adjusted by this method if, after a disk read, the ordinal number
     * of the child changes due to insertions, deletions or splits in the
     * parent.
     */
    final IN fetchINWithNoLatch(SearchResult result,
                                byte [] searchKey) {
        return fetchINWithNoLatch(result.index, searchKey, result);
    }

    /**
     * Provides the implementation of the above two methods.
     */
    private IN fetchINWithNoLatch(
        int idx,
        byte [] searchKey,
        SearchResult result) {

        assert(isUpperIN());
        assert(isLatchOwner());

        final EnvironmentImpl envImpl = getEnv();
        final long lsn = getLsn(idx);
        checkLsnInActiveFile("fetchIN", envImpl, lsn, idx);

        /* Get everything we need from IN before releasing latch. */
        int lastLoggedSize = getLastLoggedSize(idx);

        boolean isMiss = false;
        boolean success = false;

        IN child = (IN)entryTargets.get(idx);
        if (child == null) {
            if (lsn == DbLsn.NULL_LSN) {
                throw unexpectedState(makeFetchErrorMsg(
                    "fetchIN", "NULL_LSN in upper IN", lsn, idx));
            }

            pin();
            try {
                releaseLatch();

                TestHookExecute.doHookIfSet(fetchINHook);

                final WholeEntry wholeEntry = envImpl.getLogManager().
                    getLogEntryAllowInvisibleAtRecovery(
                        lsn, lastLoggedSize);

                lastLoggedSize = wholeEntry.getHeader().getEntrySize();
                final LogEntry logEntry = wholeEntry.getEntry();

                child = (IN) logEntry.getResolvedItem(databaseImpl);

                isMiss = true;

                latch(CacheMode.UNCHANGED);

                /*
                 * The following if statement relies on splits being logged
                 * immediately, or more precisely, the split node and its
                 * new sibling being logged immediately, while both siblings
                 * and their parent are latched exclusively. The reason for
                 * this is as follows:
                 *
                 * Let K be the search key. If we are doing a left-deep or
                 * right-deep search, K is -INF or +INF respectively.
                 *
                 * Let P be the parent IN (i.e., "this") and S be the slot at
                 * the idx position before P was unlatched above. Here, we
                 * view slots as independent objects, not identified by their
                 * position in an IN but by some unique (imaginary) and
                 * immutable id assigned to the slot when it is first inserted
                 * into an IN.
                 *
                 * Before unlatching P, S was the correct slot to follow down
                 * the tree looking for K. After P is unlatched and then
                 * relatched, let S' be the slot at the idx position, if P
                 * still has an idx position. We consider the following 2 cases:
                 *
                 * 1. S' exists and S'.LSN == S.LSN. Then S and S' are the same
                 * (logical) slot (because two different slots can never cover
                 * overlapping ranges of keys, and as a result, can never point
                 * to the same LSN). Then, S is still the correct slot to take
                 * down the tree, unless the range of keys covered by S has
                 * shrunk while P was unlatched. But the only way for S's key
                 * range to shrink is for its child IN to split, which could
                 * not have happened because if it did, the before and after
                 * LSNs of S would be different, given that splits are logged
                 * immediately. We conclude that the set of keys covered by
                 * S after P is relatched is the same or a superset of the keys
                 * covered by S before P was unlatched, and thus S (at the idx
                 * position) is still the correct slot to follow.
                 *
                 * 2. There is no idx position in P or S'.LSN != S.LSN. In
                 * this case we cannot be sure if S' (if it exists) is the
                 * the correct slot to follow. So, we (re)search for K in P
                 * to check if P is still the correct parent and find the
                 * correct slot to follow. If this search lands on the 1st or
                 * last slot in P, we may return null because using the key
                 * info contained in P only, we do not know the full range of
                 * keys covered by those two slots. If null is returned, the
                 * caller is expected to restart the tree search from the root.
                 *
                 * Notice that the if conditions are necessary before calling
                 * findEntry(). Without them, we could get into an infinite
                 * loop of search re-tries in the scenario where nothing changes
                 * in the tree and findEntry always lands on the 1st or last
                 * slot in P. The conditions guarantee that we may restart the
                 * tree search only if something changes with S while P is
                 * unlatched (S moves to a different position or a different
                 * IN or it points to a different LSN).
                 *
                 * Notice also that if P does not split while it is unlatched,
                 * the range of keys covered by P does not change either. This
                 * implies that the correct slot to follow *must* be inside P,
                 * and as a result, the 1st and last slots in P can be trusted.
                 * Unfortunately, however, we have no way to detecting reliably
                 * whether P splits or not.
                 */
                if (idx >= nEntries || getLsn(idx) != lsn) {

                    if (searchKey == null) {
                        return null;
                    }

                    idx = findEntry(searchKey, false, false);

                    if ((idx == 0 || idx == nEntries - 1) &&
                        !isKeyInBounds(searchKey)) {
                        return null;
                    }
                }

                if (result != null) {
                    result.index = idx;
                }

                /*
                 * "this" is still the correct parent and "idx" points to the
                 * correct slot to follow for the search down the tree. But
                 * what we fetched from the log may be out-of-date by now
                 * (because it was fetched and then updated by other threads)
                 * or it may not be the correct child anymore ("idx" was
                 * changed by the findEntry() call above). We check 5 cases:
                 *
                 * (a) There is already a cached child at the "idx" position.
                 * In this case, we return whatever is there because it has to
                 * be the most recent version of the appropriate child node.
                 * This is true even when a split or reverse split occurred.
                 * The check for isKeyInBounds above is critical in that case.
                 *
                 * (b) There is no cached child at the "idx" slot, but the slot
                 * LSN is not the same as the LSN we read from the log. This is
                 * the case if "idx" was changed by findEntry() or other
                 * threads fetched the same child as this thread, updated it,
                 * and then evicted it. The child we fetched is obsolete and
                 * should not be attached. For simplicity, we just return null
                 * in this (quite rare) case.
                 *
                 * (c) Otherwise, we attach the fetched/loaded child to the
                 * parent.
                 */
                if (entryTargets.get(idx) != null) {
                    child = (IN) entryTargets.get(idx);

                } else if (getLsn(idx) != lsn) {
                    return null;

                } else {
                    child.latchNoUpdateLRU(databaseImpl);

                    child.postFetchInit(databaseImpl, lsn, wholeEntry);
                    /* Cache size in case it is missing or inaccurate. */
                    setLastLoggedSize(idx, lastLoggedSize);

                    attachNode(idx, child, null);

                    child.releaseLatch();
                }

                success = true;

            } catch (FileNotFoundException e) {
                /* Always throws an exception. */
                handleCleanedIN(e, lsn, -1);

            } catch (ErasedException e) {
                /* Always throws an exception. */
                handleErasedIN(e, lsn, -1);

            } catch (EnvironmentFailureException e) {
                e.addErrorMessage(makeFetchErrorMsg(
                    "fetchIN", null, lsn, -1));
                throw e;

            } catch (RuntimeException e) {
                throw new EnvironmentFailureException(
                    envImpl, EnvironmentFailureReason.LOG_INTEGRITY,
                    makeFetchErrorMsg("fetchIN", e.toString(), lsn, -1), e);
            } finally {
                /*
                 * Release the parent latch if null is being returned. In this
                 * case, the parent was unlatched earlier during the disk read,
                 * and as a result, the caller cannot make any assumptions
                 * about the state of the parent.
                 *
                 * If we are returning or throwing out of this try block, the
                 * parent may or may not be latched. So, only release the latch
                 * if it is currently held.
                 */
                if (!success) {
                    if (child != null) {
                        child.incFetchStats(envImpl, isMiss);
                    }
                    releaseLatchIfOwner();
                }

                unpin();
            }
        }

        assert(hasCachedChildren() == hasCachedChildrenFlag());

        child.incFetchStats(envImpl, isMiss);

        return child;
    }

    /**
     * Returns the idx-th child of "this" upper IN, fetching the child from
     * the log and attaching it to its parent if it is not already attached.
     *
     * On entry, the parent must be EX-latched already and it stays EX-latched
     * for the duration of this method and on return (even in case of
     * exceptions).
     *
     * @param idx The slot of the child to fetch.
     */
    public IN fetchIN(int idx, CacheMode cacheMode) {

        assert(isUpperIN());
        if (!isLatchExclusiveOwner()) {
            throw unexpectedState("EX-latch not held before fetch");
        }

        final EnvironmentImpl envImpl = getEnv();
        boolean isMiss = false;
        final long lsn = getLsn(idx);
        checkLsnInActiveFile("fetchIN", envImpl, lsn, idx);

        IN child = (IN) entryTargets.get(idx);
        if (child == null) {
            if (lsn == DbLsn.NULL_LSN) {
                throw unexpectedState(makeFetchErrorMsg(
                    "fetchIN", "NULL_LSN in upper IN", lsn, idx));
            }

            try {
                final WholeEntry wholeEntry = envImpl.getLogManager().
                    getLogEntryAllowInvisibleAtRecovery(
                        lsn, getLastLoggedSize(idx));

                /* Cache size in case it is missing or inaccurate. */
                setLastLoggedSize(
                    idx, wholeEntry.getHeader().getEntrySize());

                final LogEntry logEntry = wholeEntry.getEntry();
                child = (IN) logEntry.getResolvedItem(databaseImpl);

                isMiss = true;

                child.latchNoUpdateLRU(databaseImpl);
                child.postFetchInit(databaseImpl, lsn, wholeEntry);
                attachNode(idx, child, null);
                child.releaseLatch();

            } catch (FileNotFoundException e) {
                /* Always throws an exception. */
                handleCleanedIN(e, lsn, idx);

            } catch (ErasedException e) {
                /* Always throws an exception. */
                handleErasedIN(e, lsn, idx);

            } catch (EnvironmentFailureException e) {
                e.addErrorMessage(makeFetchErrorMsg(
                    "fetchIN", null, lsn, idx));
                throw e;

            } catch (RuntimeException e) {
                throw new EnvironmentFailureException(
                    envImpl, EnvironmentFailureReason.LOG_INTEGRITY,
                    makeFetchErrorMsg("fetchIN", e.toString(), lsn, idx), e);
            }
        }

        assert(hasCachedChildren() == hasCachedChildrenFlag());

        child.incFetchStats(envImpl, isMiss);

        return child;
    }

    /**
     * Returns the target of the idx'th entry, fetching from disk if necessary.
     *
     * Null is returned in the following cases:
     *
     * 1. if the LSN is null and the KnownDeleted flag is set; or
     * 2. if the LSN's file has been cleaned and:
     *    a. the PendingDeleted or KnownDeleted flag is set, or
     *    b. the entry is "probably expired".
     *
     * Note that checking for PD/KD before calling this method is not
     * sufficient to ensure that null is not returned, because null is also
     * returned for expired records.
     *
     * When null is returned, the caller must treat the record as deleted.
     *
     * Note that null can only be returned for a slot that could contain an LN,
     * not other node types and not a DupCountLN since DupCountLNs are never
     * deleted or expired.
     *
     * An exclusive latch must be held on this BIN.
     *
     * @return the LN or null.
     */
    @SuppressWarnings("deprecation")
    public final LN fetchLN(int idx, CacheMode cacheMode) {

        assert(isBIN());

        if (!isLatchExclusiveOwner()) {
            throw unexpectedState("EX-latch not held before fetch");
        }

        if (isEntryKnownDeleted(idx)) {
            return null;
        }

        final BIN bin = (BIN) this;
        final EnvironmentImpl envImpl = getEnv();
        boolean isMiss = false;
        final long lsn = getLsn(idx);
        final boolean embedded =
            isEmbeddedLN(idx) || databaseImpl.isLNImmediatelyObsolete();
        if (!embedded) {
            checkLsnInActiveFile("fetchLN", envImpl, lsn, idx);
        }

        LN ln = (LN) entryTargets.get(idx);
        if (ln == null) {
            if (lsn == DbLsn.NULL_LSN) {
                throw unexpectedState(makeFetchErrorMsg(
                    "fetchLN", "NULL_LSN without KnownDeleted", lsn, idx));
            }
            if (embedded) {
                throw unexpectedState("May not fetch immediately obsolete LN");
            }
            try {
                final WholeEntry wholeEntry = envImpl.getLogManager().
                    getLogEntryAllowInvisibleAtRecovery(
                        lsn, getLastLoggedSize(idx));

                /* Last logged size is not present before log version 9. */
                setLastLoggedSize(
                    idx, wholeEntry.getHeader().getEntrySize());

                final LogEntry logEntry = wholeEntry.getEntry();

                final LNLogEntry<?> lnEntry =
                    (LNLogEntry<?>) wholeEntry.getEntry();

                lnEntry.postFetchInit(databaseImpl);
                BtreeVerifier.verifyDataRecord(lnEntry, bin, idx);

                final byte[] lnSlotKey = lnEntry.getKey();

                /*
                 * BINs with resident LNs shouldn't be in the dirty
                 * LRU.
                 */
                final Evictor evictor = getEvictor();
                if (evictor.useDirtyLRUSet() &&
                    cacheMode != CacheMode.EVICT_LN &&
                    cacheMode != CacheMode.EVICT_BIN &&
                    cacheMode != CacheMode.UNCHANGED) {
                    getEvictor().moveToPri1LRU(this);
                }

                ln = (LN) logEntry.getResolvedItem(databaseImpl);

                isMiss = true;

                ln.initialize(databaseImpl);
                attachNode(idx, ln, lnSlotKey);

            } catch (FileNotFoundException e) {
                return handleCleanedLN(e, lsn, idx);

            } catch (ErasedException e) {
                return handleErasedLN(e, lsn, idx);

            } catch (OperationVerifyException e) {
                /* Checksum error when verifier is running. */
                return EMPTY_LN_FOR_VERIFIER;

            } catch (EnvironmentFailureException e) {
                e.addErrorMessage(makeFetchErrorMsg(
                    "fetchLN", null, lsn, idx));
                throw e;

            } catch (RuntimeException e) {
                throw new EnvironmentFailureException(
                    envImpl, EnvironmentFailureReason.LOG_INTEGRITY,
                    makeFetchErrorMsg("fetchLN", e.toString(), lsn, idx), e);
            }
        }

        if (cacheMode != CacheMode.UNCHANGED) {
            ln.setFetchedCold(false);
        }

        ln.incFetchStats(envImpl, isMiss);

        return ln;
    }

    /**
     * Return the idx'th LN target, enforcing rules defined by the cache modes
     * for the LN. This method should be called instead of getTarget when a
     * cache mode applies to user operations such as reads and updates.
     */
    @SuppressWarnings("deprecation")
    public final LN getLN(int idx, CacheMode cacheMode) {
        assert isBIN();

        final LN ln = (LN) entryTargets.get(idx);

        if (ln == null) {
            return null;
        }

        if (cacheMode != CacheMode.UNCHANGED) {
            ln.setFetchedCold(false);
        }

        return ln;
    }

    /**
     * Initialize a node that has been read in from the log.
     */
    public final void postFetchInit(DatabaseImpl db,
                                    long fetchedLsn,
                                    WholeEntry wholeEntry) {
        assert isLatchExclusiveOwner();

        commonInit(db);
        setLastLsnsAndSizes(fetchedLsn, wholeEntry);
        addToCache();

        if (isBIN()) {
            setFetchedCold(true);
        }
    }

    /**
     * Initialize a node read in during recovery.
     */
    public final void postRecoveryInit(DatabaseImpl db,
                                       long lastLoggedLsn,
                                       WholeEntry wholeEntry) {
        commonInit(db);
        setLastLsnsAndSizes(lastLoggedLsn, wholeEntry);
    }

    /**
     * Common actions of postFetchInit and postRecoveryInit.
     */
    private void commonInit(DatabaseImpl db) {
        setDatabase(db);
        initMemorySize(); // compute before adding to IN list
    }

    /**
     * Add to INList and perform eviction related initialization.
     */
    private void addToCache() {

        getEnv().getInMemoryINs().add(this);

        if (!isDIN() && !isDBIN()) {
            if (traceLRU) {
                if (isUpperIN()) {
                    LoggerUtils.envLogMsg(
                        traceLevel, getEnv(),
                        Thread.currentThread().getId() + "-" +
                        Thread.currentThread().getName() +
                        "-" + getEnv().getName() +
                        " postFetchInit(): " +
                        " Adding UIN to LRU: " + getNodeId());
                }
            }
            getEvictor().addBack(this);
            /* Compress full BINs after fetching or loading. */
            getEnv().lazyCompress(this);
        }
    }

    /**
     * @see Node#incFetchStats
     */
    @Override
    final void incFetchStats(EnvironmentImpl envImpl, boolean isMiss) {
        Evictor e = envImpl.getEvictor();
        if (isBIN()) {
            e.incBINFetchStats(isMiss, isBINDelta(false/*checLatched*/));
        } else {
            e.incUINFetchStats(isMiss);
        }
    }

    /**
     * Construct exception message for fetch errors.
     *
     * <p>In the case of calls from fetchINWithNoLatch this IN will not be
     * latched. We tolerate possible inaccuracies in that case. However,
     * the idx param should be negative to prevent referencing the wrong slot
     * or, worse, index out of bounds.</p>
     */
    public String makeFetchErrorMsg(
        final String label,
        final String msg,
        final long lsn,
        final int idx) {

        final byte state = idx >= 0 ? entryStates[idx] : 0;

        final long expirationTime;

        if (isBIN() && idx >= 0) {

            final BIN bin = (BIN) this;

            expirationTime = TTL.expirationToSystemTime(
                bin.getExpiration(idx), isExpirationInHours());

        } else {
            expirationTime = 0;
        }

        final byte[] key = (idx >= 0) ? getKey(idx) : null;

        return makeFetchErrorMsg(
            getEnv(), label, msg, this, key, lsn, state, expirationTime);
    }

    /**
     * @param in parent IN.  Is null when root is fetched.
     */
    static String makeFetchErrorMsg(
        EnvironmentImpl envImpl,
        String label,
        String msg,
        IN in,
        byte[] key,
        long lsn,
        byte state,
        long expirationTime) {

        /*
         * Bolster the exception with the LSN, which is critical for
         * debugging. Otherwise, the exception propagates upward and loses the
         * problem LSN.
         */
        StringBuilder sb = new StringBuilder(label);

        if (lsn == DbLsn.NULL_LSN) {
            sb.append(" null lsn");
        } else {
            sb.append(" ");
            sb.append(DbLsn.getNoFormatString(lsn));
        }

        if (in != null) {
            sb.append(" parent IN=").append(in.getNodeId());
            sb.append(" IN class=").append(in.getClass().getName());
            sb.append(" lastFullLsn=");
            sb.append(DbLsn.getNoFormatString(in.getLastFullLsn()));
            sb.append(" lastLoggedLsn=");
            sb.append(DbLsn.getNoFormatString(in.getLastLoggedLsn()));
            sb.append(" parent.getDirty()=").append(in.getDirty());
        }

        if (envImpl.getExposeUserData()) {
            if (key != null) {
                sb.append(" key=");
            } else {
                sb.append(" idKey=");
                if (in != null) {
                    key = in.getIdentifierKey();
                }
            }
            if (key != null) {
                sb.append('[').append(Key.getNoFormatString(key)).append(']');
            } else {
                sb.append("[unknown]");
            }
        }

        sb.append(" state=").append(state);

        sb.append(" expires=");

        if (expirationTime != 0) {
            sb.append(TTL.formatExpirationTime(expirationTime));
        } else {
            sb.append("never");
        }

        if (msg != null) {
            sb.append(" ").append(msg);
        }

        return sb.toString();
    }

    /**
     * Checks whether the given LSN (referenced by the active Btree), is
     * located in an active file. If not (it is in a reserved file), then
     * the logic is similar to handleCorruption(), except that the env is
     * invalidated (and an exception is thrown) _only_ in test mode.
     * <p>
     * Uses {@link #FileProtector#isReservedFile} to avoid blocking when there
     * is no error. However, a Long object is created and there is a HashMap
     * lookup on each call.
     */
    void checkLsnInActiveFile(
        final String label,
        final EnvironmentImpl envImpl,
        final long lsn,
        final int idx) {

        /* NULL_LSN is checked by caller for non-resident nodes only. */
        if (lsn == DbLsn.NULL_LSN ||
            !envImpl.getFileProtector().isReservedFile(
                DbLsn.getFileNumber(lsn))) {
            return;
        }

        /* Extinct LNs may validly be present in reserved files. */
        if (isBIN() && idx >= 0) {
            if (isValidCleanedLN(idx)) {
                return;
            }
        }

        final String logMsg = makeFetchErrorMsg(
            label, "Active LSN refers to reserved file.", lsn, idx);

        final BtreeVerifyContext verifyContext =
            getEnv().getBtreeVerifyContext();

        if (verifyContext == null) {
            if (envImpl.isTestMode()) {
                final EnvironmentFailureException e =
                    new EnvironmentFailureException(envImpl,
                        EnvironmentFailureReason.LOG_FILE_NOT_FOUND, logMsg);

                LoggerUtils.envLogMsg(Level.SEVERE,
                    envImpl, LoggerUtils.getStackTraceForSevereLog(e));

                throw e;
            } else {
                envImpl.getReservedFileRefLogger().log(envImpl, Level.SEVERE, logMsg);
            }
        } else {
            verifyContext.recordError(new VerifyError(
                VerifyError.Problem.LSN_IN_RESERVED_FILE, logMsg, lsn));
        }
    }

    /**
     * Called when a corruption is discovered and we cannot continue.
     * <ul>
     *   <li>When in verify mode and not in test mode, a verify error is
     *   recorded and no exception is thrown or logged here. The verifier logs
     *   errors using a rate-limited logger.</li>
     *
     *   <li>When in verify mode and test mode, a verify error is recorded
     *   here, but the verifier immediately invalidates the environment and the
     *   invalidating exception is logged and thrown.</li>
     *
     *   <li>Otherwise the env is invalidated here and the invalidating
     *   exception is logged and thrown.</li>
     * </ul>
     */
    private void handleCorruption(
        final String label,
        final EnvironmentImpl envImpl,
        final long lsn,
        final int idx,
        final String msg,
        final VerifyError.Problem problem,
        final EnvironmentFailureReason reason) {

        final String logMsg = makeFetchErrorMsg(label, msg, lsn, idx);

        final BtreeVerifyContext verifyContext =
            getEnv().getBtreeVerifyContext();

        if (verifyContext == null) {
            final EnvironmentFailureException e =
                new EnvironmentFailureException(envImpl, reason, logMsg);

            LoggerUtils.envLogMsg(Level.SEVERE,
                envImpl, LoggerUtils.getStackTraceForSevereLog(e));

            throw e;
        } else {
            verifyContext.recordError(new VerifyError(problem, logMsg, lsn));
        }
    }

    /**
     * Returns true if an erased LN should be skipped rather than being
     * considered an integrity error.
     *
     * <p>This is the case when the LN is EXTINCT or MAYBE_EXTINCT. Currently
     * only extinct LNs are erased but in the future perhaps deleted and
     * expired LNs will be also.</p>
     */
    private boolean isValidErasedLN(final int idx) {
        return getEnv().getExtinctionStatus(
            databaseImpl, getKey(idx)) != NOT_EXTINCT;
    }

    /**
     * Returns true if a cleaned LN should be skipped rather than being
     * considered an integrity error.
     *
     * <p>This is the case when the LN is deleted, likely expired, EXTINCT
     * or MAYBE_EXTINCT. It is safe to ignore a cleaned file for a KD or PD
     * entry because files with active txns will not be cleaned.</p>
     */
    private boolean isValidCleanedLN(final int idx) {
        final BIN bin = (BIN) this;

        return bin.isDeleted(idx) ||
            bin.isProbablyExpired(idx) ||
            getEnv().getExtinctionStatus(
                databaseImpl, getKey(idx)) != NOT_EXTINCT;
    }

    /**
     * Returns a null LN, returns a special empty LN, or throws a fatal EFE.
     * <ul>
     *   <li>If {@link #isValidCleanedLN} returns true, null is returned and
     *   the caller should skip over the LN.</li>
     *
     *   <li>Else handleCorruption() is called to invalidate the env or record
     *   a verify error, as needed.</li>
     *
     *   <li>When a verify error is recorded and no exception is thrown, an
     *   empty LN is returned to allow the verifier to process the error. A
     *   message will be logged by the verifier using a rate-limited
     *   logger.</li>
     * </ul>
     */
    private LN handleCleanedLN(
        final FileNotFoundException e,
        final long lsn,
        final int idx) {

        if (isValidCleanedLN(idx)) {
            return null;
        }

        handleCorruption("fetchLN", getEnv(), lsn, idx,
            null, getCleanedNodeProblem(lsn),
            EnvironmentFailureReason.LOG_FILE_NOT_FOUND);

        /* Continue operation in verification mode. */
        return EMPTY_LN_FOR_VERIFIER;
    }

    /**
     * Returns a null LN, returns a special empty LN, or throws a fatal EFE.
     * <ul>
     *   <li>If {@link #isValidErasedLN} returns true, null is returned and the
     *   caller should skip over the LN.</li>
     *
     *   <li>Else handleCorruption() is called to invalidate the env or record
     *   a verify error, as needed.</li>
     *
     *   <li>When a verify error is recorded and no exception is thrown, an
     *   empty LN is returned to allow the verifier to process the error. A
     *   message will be logged by the verifier using a rate-limited
     *   logger.</li>
     * </ul>
     */
    private LN handleErasedLN(
        final ErasedException e,
        final long lsn,
        final int idx) {

        if (isValidErasedLN(idx)) {
            return null;
        }

        handleCorruption("fetchLN", getEnv(), lsn, idx,
            "LN is erased unexpectedly, implied corruption.",
            VerifyError.Problem.ENTRY_ERASED,
            EnvironmentFailureReason.LOG_CHECKSUM);

        /* Continue operation in verification mode. */
        return EMPTY_LN_FOR_VERIFIER;
    }

    /**
     * Throws an exception to abort the operation.
     * <ul>
     *   <li>handleCorruption() is called to invalidate the env or record a
     *   verify error, as needed.</li>
     *
     *   <li>When a verify error is recorded and no exception is thrown by
     *   handleCorruption(), a non-fatal exception is thrown here. Unlike LNs,
     *   the caller does not have the ability to skip over INs. The exception
     *   will cause the verifier to move to the next Database. A message will
     *   be logged by the verifier using a rate-limited logger.</li>
     * </ul>
     */
    void handleCleanedIN(
        final FileNotFoundException e,
        final long lsn,
        final int idx) {

        handleCorruption("fetchIN", getEnv(), lsn, idx,
            null, getCleanedNodeProblem(lsn),
            EnvironmentFailureReason.LOG_FILE_NOT_FOUND);

        /* Abort operation in verification mode. */
        throw new OperationVerifyException();
    }

    private VerifyError.Problem getCleanedNodeProblem(final long lsn) {
        final FileManager fileManager = getEnv().getFileManager();

        return (DbLsn.getFileNumber(lsn) >
            DbLsn.getFileNumber(fileManager.getNextLsn())) ?
            VerifyError.Problem.LSN_OUT_OF_BOUNDS :
            VerifyError.Problem.LSN_FILE_MISSING;
    }

    /**
     * Throws an exception to abort the operation.
     * <ul>
     *   <li>handleCorruption() is called to invalidate the env or record a
     *   verify error, as needed.</li>
     *
     *   <li>When a verify error is recorded and no exception is thrown by
     *   handleCorruption(), a non-fatal exception is thrown here. Unlike LNs,
     *   the caller does not have the ability to skip over INs. The exception
     *   will cause the verifier to move to the next Database. A message will
     *   be logged by the verifier using a rate-limited logger.</li>
     * </ul>
     */
    void handleErasedIN(
        final ErasedException e,
        final long lsn,
        final int idx) {

        handleCorruption("fetchIN", getEnv(), lsn, idx,
            "IN is erased unexpectedly, implied corruption.",
            VerifyError.Problem.ENTRY_ERASED,
            EnvironmentFailureReason.LOG_CHECKSUM);

        /* Abort operation in verification mode. */
        throw new OperationVerifyException();
    }

    public final int findEntry(
        byte[] key,
        boolean indicateIfDuplicate,
        boolean exact) {

        return findEntry(key, indicateIfDuplicate, exact, null /*Comparator*/);
    }

    /**
     * Find the entry in this IN for which key is LTE the key arg.
     *
     * Currently uses a binary search, but eventually, this may use binary or
     * linear search depending on key size, number of entries, etc.
     *
     * This method guarantees that the key parameter, which is the user's key
     * parameter in user-initiated search operations, is always the left hand
     * parameter to the Comparator.compare method.  This allows a comparator
     * to perform specialized searches, when passed down from upper layers.
     *
     * This is public so that DbCursorTest can access it.
     *
     * Note that the 0'th entry's key is treated specially in an IN.  It always
     * compares lower than any other key.
     *
     * @param key - the key to search for.
     * @param indicateIfDuplicate - true if EXACT_MATCH should
     * be or'd onto the return value if key is already present in this node.
     * @param exact - true if an exact match must be found.
     * @return offset for the entry that has a key LTE the arg.  0 if key
     * is less than the 1st entry.  -1 if exact is true and no exact match
     * is found.  If indicateIfDuplicate is true and an exact match was found
     * then EXACT_MATCH is or'd onto the return value.
     */
    public final int findEntry(
        byte[] key,
        boolean indicateIfDuplicate,
        boolean exact,
        InternalComparator comparator) {

        assert idKeyIsSlotKey();

        int high = nEntries - 1;
        int low = 0;
        int middle = 0;

        if (comparator == null) {
            comparator = databaseImpl.getKeyComparator();
        }

        /*
         * Special Treatment of 0th Entry
         * ------------------------------
         * IN's are special in that they have a entry[0] where the key is a
         * virtual key in that it always compares lower than any other key.
         * BIN's don't treat key[0] specially.  But if the caller asked for an
         * exact match or to indicate duplicates, then use the key[0] and
         * forget about the special entry zero comparison.
         *
         * We always use inexact searching to get down to the BIN, and then
         * call findEntry separately on the BIN if necessary.  So the behavior
         * of findEntry is different for BINs and INs, because it's used in
         * different ways.
         *
         * Consider a tree where the lowest key is "b" and we want to insert
         * "a".  If we did the comparison (with exact == false), we wouldn't
         * find the correct (i.e.  the left) path down the tree.  So the
         * virtual key ensures that "a" gets inserted down the left path.
         *
         * The insertion case is a good specific example.  findBinForInsert
         * does inexact searching in the INs only, not the BIN.
         *
         * There's nothing special about the 0th key itself, only the use of
         * the 0th key in the comparison algorithm.
         */
        boolean entryZeroSpecialCompare =
            isUpperIN() && !exact && !indicateIfDuplicate;

        assert nEntries >= 0;

        while (low <= high) {

            middle = (high + low) / 2;
            int s;

            if (middle == 0 && entryZeroSpecialCompare) {
                s = 1;
            } else {
                s = entryKeys.compareKeys(
                    key,  keyPrefix, middle,
                    haveEmbeddedData(middle), comparator);
            }

            if (s < 0) {
                high = middle - 1;
            } else if (s > 0) {
                low = middle + 1;
            } else {
                int ret;
                if (indicateIfDuplicate) {
                    ret = middle | EXACT_MATCH;
                } else {
                    ret = middle;
                }

                if ((ret >= 0) && exact && isEntryKnownDeleted(ret & 0xffff)) {
                    return -1;
                } else {
                    return ret;
                }
            }
        }

        /*
         * No match found.  Either return -1 if caller wanted exact matches
         * only, or return entry whose key is < search key.
         */
        if (exact) {
            return -1;
        } else {
            return high;
        }
    }

    /**
     * Inserts a slot with the given key, lsn and child node into this IN, if
     * a slot with the same key does not exist already. The state of the new
     * slot is set to DIRTY. Assumes this node is already latched by the
     * caller.
     *
     * @return true if the entry was successfully inserted, false
     * if it was a duplicate.
     *
     * @throws EnvironmentFailureException if the node is full
     * (it should have been split earlier).
     */
    public final boolean insertEntry(
        Node child,
        byte[] key,
        long childLsn)
        throws DatabaseException {

        assert(!isBINDelta());

        int res = insertEntryInternal(
             child, key, null, childLsn, EntryStates.DIRTY_BIT, false);

        return (res & INSERT_SUCCESS) != 0;
    }

    /**
     * Inserts a slot with the given key, lsn and child node into this IN, if
     * a slot with the same key does not exist already. The state of the new
     * slot is set to DIRTY. Assumes this node is already latched by the
     * caller.
     *
     * @param embData If the data portion of a record must be embedded in this
     * BIN, "data" stores the record's data. Null otherwise. See also comment
     * for the keyEntries field.
     *
     * @return either (1) the index of location in the IN where the entry was
     * inserted |'d with INSERT_SUCCESS, or (2) the index of the duplicate in
     * the IN if the entry was found to be a duplicate.
     *
     * @throws EnvironmentFailureException if the node is full (it should have
     * been split earlier).
     */
    public final int insertEntry1(
        Node child,
        byte[] key,
        byte[] embData,
        long childLsn,
        boolean blindInsertion) {

        return insertEntryInternal(
            child, key, embData, childLsn, EntryStates.DIRTY_BIT,
            blindInsertion);
    }

    /**
     * Inserts a slot with the given key, lsn, state, and child node into this
     * IN, if a slot with the same key does not exist already. Assumes this
     * node is already latched by the caller.
     *
     * This returns a failure if there's a duplicate match. The caller must do
     * the processing to check if the entry is actually deleted and can be
     * overwritten. This is foisted upon the caller rather than handled in this
     * object because there may be some latch releasing/retaking in order to
     * check a child LN.
     *
     * @param embData If the data portion of a record must be embedded in this
     * BIN, "data" stores the record's data. Null otherwise. See also comment
     * for the keyEntries field.
     *
     * @return either (1) the index of location in the IN where the entry was
     * inserted |'d with INSERT_SUCCESS, or (2) the index of the duplicate in
     * the IN if the entry was found to be a duplicate.
     *
     * @throws EnvironmentFailureException if the node is full (it should have
     * been split earlier).
     */
    int insertEntryInternal(
        Node child,
        byte[] key,
        byte[] embData,
        long childLsn,
        byte state,
        boolean blindInsertion) {

        /*
         * Search without requiring an exact match, but do let us know the
         * index of the match if there is one.
         */
        int index = findEntry(key, true, false);

        if (index >= 0 && (index & EXACT_MATCH) != 0) {

            /*
             * There is an exact match.  Don't insert; let the caller decide
             * what to do with this duplicate.
             */
            return index & ~IN.EXACT_MATCH;
        }

        /*
         * There was no key match, but if this is a bin delta, there may be an
         * exact match in the full bin. Mutate to full bin and search again.
         * However, if we know for sure that the key does not exist in the full
         * BIN, then don't mutate, unless there is no space in the delta to do
         * the insertion.
         */
        if (isBINDelta()) {

            BIN bin = (BIN)this;

            if (insertMustMutateToFullBIN(key, blindInsertion)) {

                mutateToFullBIN(true /*leaveFreeSlot*/);

                index = findEntry(key, true, false);

                if (index >= 0 && (index & EXACT_MATCH) != 0) {
                    return index & ~IN.EXACT_MATCH;
                }
            } else {
                getEvictor().incBinDeltaBlindOps();

                if (traceDeltas) {
                    LoggerUtils.envLogMsg(
                        traceLevel, getEnv(),
                        Thread.currentThread().getId() + "-" +
                        Thread.currentThread().getName() +
                        "-" + getEnv().getName() +
                        (blindInsertion ?
                         " Blind insertion in BIN-delta " :
                         " Blind put in BIN-delta ") +
                        getNodeId() + " nEntries = " +
                        nEntries + " max entries = " +
                        getMaxEntries() +
                        " full BIN entries = " +
                        bin.getFullBinNEntries() +
                        " full BIN max entries = " +
                        bin.getFullBinMaxEntries());
                }
            }
        }

        if (nEntries >= getMaxEntries()) {
            throw unexpectedState(
                getEnv(),
                "Node " + getNodeId() +
                " should have been split before calling insertEntry" +
                " is BIN-delta: " + isBINDelta() +
                " num entries: " + nEntries +
                " max entries: " + getMaxEntries());
        }

        /* There was no key match, so insert to the right of this entry. */
        index++;

        /* We found a spot for insert, shift entries as needed. */
        if (index < nEntries) {
            int oldSize = computeLsnOverhead();

            /* Adding elements to the LSN array can change the space used. */
            shiftEntriesRight(index);

            updateMemorySize(computeLsnOverhead() - oldSize);
        } else {
            nEntries++;
        }

        if (isBINDelta()) {
            ((BIN)this).incFullBinNEntries();
        }

        int oldSize = computeLsnOverhead();

        if (embData == null) {
            setTarget(index, child);
        }

        setLsnInternal(index, childLsn);

        boolean multiSlotChange = insertKey(index, key, embData);

        /*
         * Do this after calling insert key to overwrite whatever state changes
         * were done by the insertEntry() call.
         */
        entryStates[index] = state;

        if (embData != null) {
            setEmbeddedLN(index);
            if (embData.length == 0) {
                setNoDataLN(index);
            }
        }

        adjustCursorsForInsert(index);

        updateMemorySize(oldSize,
                         getEntryInMemorySize(index) +
                         computeLsnOverhead());

        if (multiSlotChange) {
            updateMemorySize(inMemorySize, computeMemorySize());
        }

        setDirty(true);

        assert(isBIN() || hasCachedChildren() == hasCachedChildrenFlag());

        return (index | INSERT_SUCCESS);
    }

    public boolean insertMustMutateToFullBIN(
        final byte[] key,
        final boolean blindInsertion) {

        if (nEntries >= getMaxEntries()) {
            return true;
        }

        return !blindInsertion && ((BIN) this).mayHaveKeyInFullBin(key);
    }

    /**
     * Removes the slot at index from this IN.  Assumes this node is already
     * latched by the caller.
     *
     * @param index The index of the entry to delete from the IN.
     */
    public void deleteEntry(int index) {
        deleteEntry(index, true /*makeDirty*/, true /*validate*/);
    }

    /**
     * Variant that allows specifying whether the IN is dirtied and whether
     * validation takes place. 'validate' should be false only in tests.
     *
     * See BIN.compress and INCompressor for a discussion about why slots can
     * be deleted without dirtying the BIN, and why the next delta is
     * prohibited when the slot is dirty.
     */
    void deleteEntry(int index, boolean makeDirty, boolean validate) {

        assert !isBINDelta();
        assert index >= 0 && index < nEntries;
        assert !validate || validateSubtreeBeforeDelete(index);

        if (makeDirty) {
            setDirty(true);
        }

        if (isDirty(index)) {
            setProhibitNextDelta(true);
        }

        Node child = getTarget(index);
        if (child != null && child.isIN()) {
            IN childIN = (IN)child;
            getEnv().getInMemoryINs().remove(childIN);
        }

        updateMemorySize(getEntryInMemorySize(index), 0);
        int oldLSNArraySize = computeLsnOverhead();

        /*
         * Do the actual deletion. Note: setTarget() must be called before
         * copyEntries() so that the hasCachedChildrenFlag will be properly
         * maintained.
         */
        setTarget(index, null);
        copyEntries(index + 1, index, nEntries - index - 1);
        nEntries--;

        /* cleanup what used to be the last entry */
        clearEntry(nEntries);

        /* setLsnInternal can mutate to an array of longs. */
        updateMemorySize(oldLSNArraySize, computeLsnOverhead());

        assert(isBIN() || hasCachedChildrenFlag() == hasCachedChildren());

        /*
         * Note that we don't have to adjust cursors for delete, since
         * there should be nothing pointing at this record.
         */
        traceDelete(Level.FINEST, index);
    }

    /**
     * WARNING: clearEntry() calls entryTargets.set() directly, instead of
     * setTarget(). As a result, the hasCachedChildren flag of the IN is not
     * updated here. The caller is responsible for updating this flag, if
     * needed.
     */
    void clearEntry(int idx) {

        entryTargets = entryTargets.set(idx, null, this);
        entryKeys = entryKeys.set(idx, null, this);
        setLsnInternal(idx, DbLsn.NULL_LSN);
        entryStates[idx] = 0;
        setLastLoggedSizeUnconditional(idx, 0);
    }

    /**
     * This method is called after the idx'th child of this node gets logged,
     * and changes position as a result.
     *
     * @param newLSN The new on-disk position of the child.
     *
     * @param newVLSN The VLSN of the logrec at the new position.
     * For LN children only.
     *
     * @param newSize The size of the logrec at the new position.
     * For LN children only.
     */
    public final void updateEntry(
        int idx,
        long newLSN,
        long newVLSN,
        int newSize) {

        setLsn(idx, newLSN);
        setLastLoggedSize(idx, newSize);

        if (isBIN()) {
            /* When the LN is evicted its VLSN will be cached. */
            ((BIN)this).setCachedVLSN(idx,
                (getTarget(idx) == null) ? newVLSN : NULL_VLSN);
        }

        setDirty(true);
    }

    /**
     * This method is called only from BIN.applyDelta(). It applies the info
     * extracted from a delta slot to the corresponding slot in the full BIN.
     *
     * Unlike other update methods, the LSN may be NULL_LSN if the KD flag is
     * set. This allows applying a BIN-delta with a NULL_LSN and KD, for an
     * invisible log entry for example.
     *
     * No need to do memory counting in this method because the BIN is not
     * yet attached to the tree.
     */
    final void applyDeltaSlot(
        int idx,
        Node node,
        long lsn,
        int lastLoggedSize,
        byte state,
        byte[] key,
        byte[] embData) {

        assert(isBIN());
        assert(!isBINDelta());
        assert(lsn != DbLsn.NULL_LSN ||
               (state & EntryStates.KNOWN_DELETED_BIT) != 0);
        assert(node == null || embData == null);
        assert(!getInListResident());

        setLsn(idx, lsn, false/*check*/);
        setLastLoggedSize(idx, lastLoggedSize);
        setTarget(idx, node);

        updateLNSlotKey(idx, key, embData);

        assert(isEmbeddedLN(idx) == isEmbeddedLN(state));
        assert(isNoDataLN(idx) == isNoDataLN(state));

        entryStates[idx] = state;

        setDirty(true);
    }

    /**
     * Update the idx slot of this BIN to reflect a record insertion in an
     * existing KD slot. It is called from CursorImpl.insertRecordInternal(),
     * after logging the insertion op.
     *
     * @param newLN The LN associated with the new record. Should be null if
     * the LN should not be cached.
     *
     * @param newLSN The LSN of the insertion logrec.
     *
     * @param newSize The size of the insertion logrec.
     *
     * @param newKey The value for the record's key. It is equal to the current
     * key value in the slot, but may not be identical to that value if a
     * custom comparator is used.
     *
     * @param newData If the record's data must be embedded in this BIN, "data"
     * stores the record's data. Null otherwise. See also comment for the
     * keyEntries field.
     */
    public final void insertRecord(
        int idx,
        LN newLN,
        long newLSN,
        long newVLSN,
        int newSize,
        byte[] newKey,
        byte[] newData,
        int expiration,
        boolean expirationInHours,
        long creationTime,
        long modificationTime,
        boolean tombstone) {

        assert(isBIN());

        final BIN bin = (BIN) this;

        /*
         * TODO: Seems wrong that mutations that adjust memory size are
         *  between the getEntryInMemorySize and updateMemorySize.
         */
        long oldSlotSize = getEntryInMemorySize(idx);

        setLsn(idx, newLSN);

        boolean multiSlotChange = updateLNSlotKey(idx, newKey, newData);

        boolean embeddedData = isEmbeddedLN(idx);

        if (embeddedData) {
            clearLastLoggedSize(idx);
        } else {
            setLastLoggedSize(idx, newSize);
        }

        if (newLN != null) {
            setTarget(idx, newLN);
        }

        /* When the LN is evicted its VLSN will be cached. */
        bin.setCachedVLSN(idx,
            (newLN == null) ? newVLSN : NULL_VLSN);

        bin.setExpiration(idx, expiration, expirationInHours);
        bin.setModificationTime(idx, embeddedData ? modificationTime : 0);
        bin.setCreationTime(idx, embeddedData ? creationTime : 0);

        bin.setTombstone(idx, tombstone);

        if (multiSlotChange) {
            updateMemorySize(inMemorySize, computeMemorySize());
        } else {
            long newSlotSize = getEntryInMemorySize(idx);
            updateMemorySize(oldSlotSize, newSlotSize);
        }

        clearKnownDeleted(idx);
        clearPendingDeleted(idx);
        setDirty(true);

        assert(isBIN() || hasCachedChildren() == hasCachedChildrenFlag());
    }

    /**
     * Update the idx slot of this BIN to reflect an update of the associated
     * record. It is called from CursorImpl.updateRecordInternal(), after
     * logging the update op.
     *
     * @param oldMemSize If the child LN was cached before the update op and
     * is cached when this method is called, it has already been updated
     * in-place by the caller. In this case, oldMemSize stores the size of the
     * child LN before the update, and it is used to do memory counting.
     * Otherwise oldMemSize is 0 and the newly created LN is not attached to
     * the tree.
     *
     * @param newLSN The LSN of the update logrec.
     *
     * @param newVLSN The VLSN of the update logrec.
     *
     * @param newSize The on-disk size of the update logrec.
     *
     * @param newKey The new value for the record's key. It is equal to the
     * current value, but may not be identical to the current value if a
     * custom comparator is used. It may be null, if the caller knows for
     * sure that the key does not change.
     *
     * @param newEmbData If the record's data must be embedded in this BIN,
     * "data" stores the record's data. Null otherwise. See also comment for
     * the keyEntries field.
     */
    public final void updateRecord(
        int idx,
        long oldMemSize,
        long newLSN,
        long newVLSN,
        int newSize,
        byte[] newKey,
        byte[] newEmbData,
        int expiration,
        boolean expirationInHours,
        long creationTime,
        long modificationTime,
        boolean tombstone) {

        assert(isBIN());

        final BIN bin = (BIN) this;

        long oldSlotSize = getEntryInMemorySize(idx);

        /*
         * TODO: Seems wrong that mutations that adjust memory size are
         *  between the getEntryInMemorySize and updateMemorySize.
         */
        setLsn(idx, newLSN);

        boolean multiSlotChange = updateLNSlotKey(idx, newKey, newEmbData);

        if (isEmbeddedLN(idx)) {
            clearLastLoggedSize(idx);
        } else {
            setLastLoggedSize(idx, newSize);
        }

        /* When the LN is evicted its VLSN will be cached. */
        bin.setCachedVLSN(idx,
            (getTarget(idx) == null) ? newVLSN : NULL_VLSN);

        bin.setExpiration(idx, expiration, expirationInHours);

        bin.setModificationTime(
            idx, (newEmbData != null) ? modificationTime : 0);

        bin.setCreationTime(
            idx, (newEmbData != null) ? creationTime : 0);

        bin.setTombstone(idx, tombstone);

        if (multiSlotChange) {
            updateMemorySize(inMemorySize, computeMemorySize());
        } else {
            /* Update mem size for key change. */
            long newSlotSize = getEntryInMemorySize(idx);
            updateMemorySize(oldSlotSize, newSlotSize);

            /* Update mem size for node change. */
            if (oldMemSize != 0) {
                Node newLN = entryTargets.get(idx);
                long newMemSize = (newLN != null) ?
                    newLN.getMemorySizeIncludedByParent() : 0;
                updateMemorySize(oldMemSize, newMemSize);
            }
        }

        setDirty(true);
    }

    /**
     * Update the idx slot slot of this BIN to reflect a deletion of the
     * associated record. It is called from CursorImpl.deleteCurrentRecord(),
     * after logging the deletion op.
     *
     * @param oldMemSize If the child LN was cached before the deletion, it
     * has already been updated in-place by the caller (the ln contents have
     * been deleted). In this case, oldMemSize stores the in-memory size of
     * the child LN before the update, and it is used to do memory counting.
     * Otherwise oldMemSize is 0 and the newly created LN has not been attached
     * to the tree; it will be attached later by the caller, if needed.
     *
     * @param newLSN The LSN of the deletion logrec.
     *
     * @param newVLSN The VLSN of the deletion logrec.
     *
     * @param newSize The on-disk size of the deletion logrec.
     */
    public final void deleteRecord(
        int idx,
        long oldMemSize,
        long newLSN,
        long newVLSN,
        int newSize) {

        assert(isBIN());

        final BIN bin = (BIN) this;

        setLsn(idx, newLSN);

        if (isEmbeddedLN(idx)) {
            clearLastLoggedSize(idx);
        } else {
            setLastLoggedSize(idx, newSize);
        }

        bin.setModificationTime(idx, 0);
        bin.setCreationTime(idx, 0);
        bin.setTombstone(idx, false);

        Node newLN = entryTargets.get(idx);

        if (newLN != null) {
            /* Update mem size for node change. */
            assert(oldMemSize != 0);
            long newMemSize = newLN.getMemorySizeIncludedByParent();
            updateMemorySize(oldMemSize, newMemSize);
        } else {
            assert(oldMemSize == 0);
        }

        /* When the LN is evicted its VLSN will be cached. */
        bin.setCachedVLSN(idx,
            (newLN == null) ? newVLSN : NULL_VLSN);

        setPendingDeleted(idx);
        setDirty(true);
    }

    /**
     * This method is used by the RecoveryManager to change the current version
     * of a record, either to a later version (in case of redo), or to an
     * earlier version (in case of undo). The current version may or may not be
     * cached as a child LN of this BIN (it may be only in case of txn abort
     * during normal processing). If it is, it is evicted. The new version is
     * not attached to the in-memory tree, to save memory during crash
     * recovery.
     *
     * @param idx The BIN slot for the record.
     *
     * @param lsn The LSN of the new record version. It may be null in case of
     * undo, if the logrec that is being undone is an insertion and the record
     * did not exist at all in the DB before that insertion.
     *
     * @param knownDeleted True if the new version is a committed deletion.
     *
     * @param pendingDeleted True if the new version is a deletion, which
     * may or may not be committed.
     *
     * @param key The key of the new version. It is null only if we are undoing
     * and the revert-to version was not embedded (in this case the key of the
     * revert-to version is not stored in the logrec). If it is null and the
     * DB allows key updates, the new record version is fetched from disk to
     * retrieve its key, so that the key values stored in the BIN slots are
     * always transactionally correct.
     *
     * @param embData The data of the new version. It is non-null if and only if
     * the new version must be embedded in the BIN.
     *
     * @param vlsn The VLSN of the new version.
     *
     * @param logrecSize The on-disk size of the logrec corresponding to the
     * new version. It may be 0 (i.e. unknown) in case of undo.
     */
    public final void recoverRecord(
        int idx,
        long lsn,
        boolean knownDeleted,
        boolean pendingDeleted,
        byte[] key,
        byte[] embData,
        long vlsn,
        int logrecSize,
        int expiration,
        boolean expirationInHours,
        long creationTime,
        long modificationTime,
        boolean tombstone) {

        assert(isBIN());

        BIN bin = (BIN) this;

        if (lsn == DbLsn.NULL_LSN) {

            /*
             * A NULL lsn means that we are undoing an insertion that was done
             * without slot reuse. To undo such an insertion we evict the
             * current version (it may cached only in case of normal txn abort)
             * and set the KD flag in the slot. We also set the LSN to null to
             * ensure that the slot does not point to a logrec that does not
             * reflect the slot's current state. The slot can then be put on
             * the compressor for complete removal.
             */
            setKnownDeletedAndEvictLN(idx);

            setLsnInternal(idx, DbLsn.NULL_LSN);

            bin.queueSlotDeletion(idx);

            /*
             * TODO: clear other properties to reclaim memory and leave slot
             *  in a clean state.
             *  - cached VLSN
             *  - lastLoggedSize
             *  - expiration
             *  - modificationTime
             *  - tombstone flag
             *  Also clear the last three during deletions.
             */
            return;
        }

        if (key == null &&
            databaseImpl.allowsKeyUpdates() &&
            !knownDeleted) {

            try {
                WholeEntry wholeEntry = getEnv().getLogManager().
                    getLogEntryAllowInvisibleAtRecovery(
                        lsn, getLastLoggedSize(idx));

                LNLogEntry<?> logrec = (LNLogEntry<?>) wholeEntry.getEntry();
                logrec.postFetchInit(getDatabase());

                key = logrec.getKey();
                logrecSize = wholeEntry.getHeader().getEntrySize();

            } catch (FileNotFoundException e) {
                throw new EnvironmentFailureException(
                    getEnv(), EnvironmentFailureReason.LOG_FILE_NOT_FOUND,
                    makeFetchErrorMsg("recoverRecord", null, lsn, idx), e);

            } catch (ErasedException e) {
                throw new EnvironmentFailureException(
                    getEnv(), EnvironmentFailureReason.LOG_CHECKSUM,
                    "LN is erased unexpectedly, implied corruption. " +
                    makeFetchErrorMsg("recoverRecord", null, lsn, idx), e);
            }
        }

        long oldSlotSize = getEntryInMemorySize(idx);

        setLsn(idx, lsn);
        setTarget(idx, null);

        boolean multiSlotChange = updateLNSlotKey(idx, key, embData);

        if (isEmbeddedLN(idx)) {
            clearLastLoggedSize(idx);
        } else {
            setLastLoggedSize(idx, logrecSize);
        }

        bin.setCachedVLSN(idx, vlsn);

        if (knownDeleted) {
            assert(!pendingDeleted);
            setKnownDeleted(idx);
            bin.queueSlotDeletion(idx);
        } else {
            clearKnownDeleted(idx);
            if (pendingDeleted) {
                setPendingDeleted(idx);
                bin.queueSlotDeletion(idx);
            } else {
                clearPendingDeleted(idx);
            }
        }

        bin.setExpiration(idx, expiration, expirationInHours);
        bin.setModificationTime(idx, (embData != null) ? modificationTime : 0);
        bin.setCreationTime(idx, (embData != null) ? creationTime : 0);
        bin.setTombstone(idx, tombstone);

        if (multiSlotChange) {
            updateMemorySize(inMemorySize, computeMemorySize());
        } else {
            long newSlotSize = getEntryInMemorySize(idx);
            updateMemorySize(oldSlotSize, newSlotSize);
        }

        setDirty(true);
    }

    /**
     * Update the cached-child and LSN properties of the idx-th slot. This
     * method is used by the RecoveryManager.recoverChildIN() to change the
     * version of a child IN, a later version The child IN may or may not be
     * already attached to the tree.
     */
    public final void recoverIN(
        int idx,
        Node node,
        long lsn,
        int lastLoggedSize) {

        long oldSlotSize = getEntryInMemorySize(idx);

        /*
         * If we are about to detach a cached child IN, make sure that it is
         * not in the INList. This is correct, because this method is called
         * during the recovery phase where the INList is disabled,
         */
        Node child = getTarget(idx);
        assert(child == null ||
               !((IN)child).getInListResident() ||
               child == node/* this is needed by a unit test*/);

        setLsn(idx, lsn);
        setLastLoggedSize(idx, lastLoggedSize);
        setTarget(idx, node);

        long newSlotSize = getEntryInMemorySize(idx);
        updateMemorySize(oldSlotSize, newSlotSize);

        setDirty(true);

        assert(isBIN() || hasCachedChildren() == hasCachedChildrenFlag());
    }

    /**
     * Attach the given node as the idx-th child of "this" node. If the child
     * node is an LN, update the key of the parent slot to the given key value,
     * if that value is non-null and an update is indeed necessary.
     *
     * This method is called after the child node has been either (a) fetched
     * in from disk and is not dirty, or (b) is a newly created instance that
     * will be written out later by something like a checkpoint. In either
     * case, the slot LSN does not need to be updated.
     *
     * Note: does not dirty the node unless the LN slot key is changed.
     */
    public final void attachNode(int idx, Node node, byte[] newKey) {

        assert !(node instanceof IN) || ((IN) node).isLatchExclusiveOwner();

        long oldSlotSize = getEntryInMemorySize(idx);

        /* Make sure we are not using this method to detach a cached child */
        assert(getTarget(idx) == null);

        setTarget(idx, node);

        boolean multiSlotChange = false;

        if (isBIN() && newKey != null) {
            assert(!haveEmbeddedData(idx));
            multiSlotChange = updateLNSlotKey(idx, newKey, null);
        }

        if (multiSlotChange) {
            updateMemorySize(inMemorySize, computeMemorySize());
        } else {
            long newSlotSize = getEntryInMemorySize(idx);
            updateMemorySize(oldSlotSize, newSlotSize);
        }

        assert(isBIN() || hasCachedChildren() == hasCachedChildrenFlag());
    }

    /*
     * Detach from the tree the child node at the idx-th slot.
     *
     * The most common caller of this method is the evictor. If the child
     * being evicted was dirty, it has just been logged and the lsn of the
     * slot must be updated.
     */
    public final void detachNode(int idx, boolean updateLsn, long newLsn) {

        long oldSlotSize = getEntryInMemorySize(idx);

        Node child = getTarget(idx);

        if (updateLsn) {
            setLsn(idx, newLsn);
            setDirty(true);
        }
        setTarget(idx, null);

        long newSlotSize = getEntryInMemorySize(idx);
        updateMemorySize(oldSlotSize, newSlotSize);

        if (child != null && child.isIN()) {
            getEnv().getInMemoryINs().remove((IN) child);
        }

        assert(isBIN() || hasCachedChildren() == hasCachedChildrenFlag());
    }

    void copyEntries(final int from, final int to, final int n) {

        entryTargets = entryTargets.copy(from, to, n, this);
        entryKeys = entryKeys.copy(from, to, n, this);
        lastLoggedSizes = lastLoggedSizes.copy(from, to, n, this);

        System.arraycopy(entryStates, from, entryStates, to, n);
        System.arraycopy(entryLsns, from, entryLsns, to, n);
    }

    /**
     * Return true if this node needs splitting.  For the moment, needing to be
     * split is defined by there being no free entries available.
     */
    public final boolean needsSplitting() {

        if (isBINDelta()) {
            BIN bin = (BIN)this;
            int fullBinNEntries = bin.getFullBinNEntries();
            int fullBinMaxEntries = bin.getFullBinMaxEntries();

            if (fullBinNEntries < 0) {
                /* fullBinNEntries is unknown in logVersions < 10 */
                mutateToFullBIN(false /*leaveFreeSlot*/);
            } else {
                assert(fullBinNEntries > 0);
                return ((fullBinMaxEntries - fullBinNEntries) < 1);
            }
        }

        return ((getMaxEntries() - nEntries) < 1);
    }

    /**
     * Split this into two nodes.  Parent IN is passed in parent and should be
     * latched by the caller.
     *
     * childIndex is the index in parent of where "this" can be found.
     *
     * @return EX-latched new sibling.
     */
    public final IN split(
        IN parent,
        int childIndex,
        IN grandParent,
        int parentIndex,
        int maxEntries) {

        return splitInternal(
            parent, childIndex, grandParent, parentIndex, maxEntries, -1);
    }

    /**
     * Called when we know we are about to split on behalf of a key that is the
     * minimum (leftSide) or maximum (!leftSide) of this node.  This is
     * achieved by just forcing the split to occur either one element in from
     * the left or the right (i.e. splitIndex is 1 or nEntries - 1).
     *
     * @return EX-latched new sibling.
     */
    IN splitSpecial(
        IN parent,
        int childIndex,
        IN grandParent,
        int parentIndex,
        int maxEntriesPerNode,
        byte[] key,
        boolean leftSide) {

        int index = findEntry(key, false, false);

        if (leftSide && index == 0) {
            return splitInternal(
                parent, childIndex, grandParent, parentIndex,
                maxEntriesPerNode, 1);

        } else if (!leftSide && index == (nEntries - 1)) {
            return splitInternal(
                parent, childIndex, grandParent, parentIndex,
                maxEntriesPerNode, nEntries - 1);

        } else {
            return split(
                parent, childIndex, grandParent, parentIndex,
                maxEntriesPerNode);
        }
    }

    final IN splitInternal(
        final IN parent,
        final int childIndex,
        final IN grandParent,
        final int parentIndex,
        final int maxEntries,
        int splitIndex)
        throws DatabaseException {

        assert(!isBINDelta());

        /*
         * Find the index of the existing identifierKey so we know which IN
         * (new or old) to put it in.
         */
        if (identifierKey == null) {
            throw unexpectedState();
        }

        final int idKeyIndex = findEntry(identifierKey, false, false);

        if (splitIndex < 0) {
            splitIndex = nEntries / 2;
        }

        /* Range of entries to copy to new sibling. */
        final int low, high;

        if (idKeyIndex < splitIndex) {

            /*
             * Current node (this) keeps left half entries.  Right half entries
             * will go in the new node.
             */
            low = splitIndex;
            high = nEntries;
        } else {

            /*
             * Current node (this) keeps right half entries.  Left half entries
             * will go in the new node.
             */
            low = 0;
            high = splitIndex;
        }

        final byte[] newIdKey = getKey(low);

        /*
         * Ensure that max entries is large enough to hold the slots being
         * moved to the new sibling, with one spare slot for insertions. This
         * is important when the maxEntries param is less than nEntries in this
         * node, which can occur when the user reduces the fanout or when this
         * node has temporarily grown beyond its original fanout.
         */
        final IN newSibling = createNewInstance(
            newIdKey, entryKeys.getCompactMaxKeySize(),
            Math.max(maxEntries, high - low + 1),
            level);

        newSibling.latch(CacheMode.UNCHANGED);

        try {
            boolean addedNewSiblingToCompressorQueue = false;
            final int newSiblingNEntries = (high - low);
            final boolean haveCachedChildren = hasCachedChildrenFlag();

            assert(isBIN() || haveCachedChildren == hasCachedChildren());

            final BIN bin = isBIN() ? (BIN) this : null;

            /*
             * Distribute entries among the split node and the new sibling.
             */
            for (int i = low; i < high; i++) {

                if (!addedNewSiblingToCompressorQueue &&
                    bin != null &&
                    bin.isDefunct(i)) {

                    addedNewSiblingToCompressorQueue = true;
                    getEnv().addToCompressorQueue((BIN) newSibling);
                }

                newSibling.appendEntryFromOtherNode(this, i);
                clearEntry(i);
            }

            if (low == 0) {
                shiftEntriesLeft(newSiblingNEntries);
                setIdentifierKey(getKey(0), true);
            }

            nEntries -= newSiblingNEntries;
            setDirty(true);

            if (isUpperIN() && haveCachedChildren) {
                setHasCachedChildrenFlag(hasCachedChildren());
            }

            assert(isBIN() ||
                   hasCachedChildrenFlag() == hasCachedChildren());
            assert(isBIN() ||
                   newSibling.hasCachedChildrenFlag() ==
                   newSibling.hasCachedChildren());

            adjustCursors(newSibling, low, high);

            /*
             * If this node has no key prefix, calculate it now that it has
             * been split.  This must be done before logging, to ensure the
             * prefix information is made persistent [#20799].
             */
            byte[] newKeyPrefix = computeKeyPrefix(-1);
            recalcSuffixes(newKeyPrefix, null, null, -1);

            /* Apply compaction after prefixing [#20799]. */
            entryKeys = entryKeys.compact(this);

            /* Only recalc if there are multiple entries in newSibling. */
            if (newSibling.getNEntries() > 1) {
                byte[] newSiblingPrefix = newSibling.computeKeyPrefix(-1);
                newSibling.recalcSuffixes(newSiblingPrefix, null, null, -1);
                /* initMemorySize calls entryKeys.compact. */
                newSibling.initMemorySize();
            }

            assert idKeyIsSlotKey();
            assert newSibling.idKeyIsSlotKey();

            /*
             * Update size. newSibling and parent are correct, but this IN has
             * had its entries shifted and is not correct.
             *
             * Also, inMemorySize does not reflect changes that may have
             * resulted from key prefixing related changes, it needs to be
             * brought up to date, so update it appropriately for this and the
             * above reason.
             */
            EnvironmentImpl env = getEnv();
            INList inMemoryINs = env.getInMemoryINs();
            long oldMemorySize = inMemorySize;
            long newSize = computeMemorySize();
            updateMemorySize(oldMemorySize, newSize);

            /*
             * Parent refers to child through an element of the entries array.
             * Depending on which half of the BIN we copied keys from, we
             * either have to adjust one pointer and add a new one, or we have
             * to just add a new pointer to the new sibling.
             *
             * We must use the provisional logging for two reasons:
             *
             *   1) All three log entries must be read atomically. The parent
             *   must get logged last, as all referred-to children must precede
             *   it. Provisional entries guarantee that all three are processed
             *   as a unit. Recovery skips provisional entries, so the changed
             *   children are only used if the parent makes it out to the log.
             *
             *   2) We log all they way to the root to avoid the "great aunt"
             *   problem (see RecoveryManager.buildINs), and provisional
             *   logging is necessary during a checkpoint for levels less than
             *   maxFlushLevel.
             *
             * Compression is not necessary here because the old sibling is
             * compressed beforehand to determine whether a split is necessary.
             * We prohibit compression for simplicity to maintain at least one
             * entry in each IN. Note the use of getKey(0) below.
             */
            LogItem newSiblingItem =
                newSibling.logProvisionalNoCompress(parent);

            LogItem myNewItem = logProvisionalNoCompress(parent);

            assert nEntries > 0;

            /*
             * When we update the parent entry, we make sure that we don't
             * replace the parent's key that points at 'this' with a key that
             * is > than the existing one.  Replacing the parent's key with
             * something > would effectively render a piece of the subtree
             * inaccessible.  So only replace the parent key with something
             * <= the existing one.  See tree/SplitTest.java for more details
             * on the scenario.
             */
            if (low == 0) {

                /*
                 * Change the original entry to point to the new child and add
                 * an entry to point to the newly logged version of this
                 * existing child.
                 */
                parent.updateSplitSlot(
                    childIndex, newSibling, newSiblingItem.lsn, newIdKey);

                parent.setLastLoggedSize(childIndex, newSiblingItem.size);

                int inserted = parent.insertEntry1(
                    this, getKey(0), null, myNewItem.lsn, false);

                assert (inserted & INSERT_SUCCESS) != 0 :
                    "Did not insert this node," +
                    " childNodeId: " + getNodeId() +
                    " key: " + Key.getNoFormatString(getKey(0)) +
                    " parent: " + parent +
                    " newSib: " + newSibling +
                    " this: " + this;

                parent.setLastLoggedSize(
                    inserted & ~INSERT_SUCCESS, myNewItem.size);
            } else {

                /*
                 * Update the existing child's LSN to reflect the newly logged
                 * version and insert new child into parent.
                 */
                parent.updateSplitSlot(
                    childIndex, this, myNewItem.lsn, getKey(0));

                parent.setLastLoggedSize(childIndex, myNewItem.size);

                int inserted = parent.insertEntry1(
                    newSibling, newIdKey, null, newSiblingItem.lsn, false);

                assert (inserted & INSERT_SUCCESS) != 0 :
                    "Did not insert new sibling," +
                    " childNodeId: " + getNodeId() +
                    " key: " + Key.getNoFormatString(newIdKey) +
                    " parent: " + parent +
                    " newSib: " + newSibling +
                    " this: " + this;

                parent.setLastLoggedSize(
                    inserted & ~INSERT_SUCCESS, newSiblingItem.size);
            }

            /*
             * TODO FUTURE CLEANUP: call updateEntry instead of
             *  setLastLoggedSize above, and don't pass LSN to insertEntry.
             */

            inMemoryINs.add(newSibling);

            /*
             * Log the parent. Note that the root slot or grandparent slot is
             * not updated with the parent's LSN here; this is done by
             * Tree.forceSplit.
             */
            long parentLsn;
            if (!Tree.LOG_SPLITS) {
                parentLsn = parent.getLastLoggedLsn();
            } else if (parent.isRoot()) {
                parentLsn = parent.log().lsn;
            } else {
                LogItem parentItem =
                    parent.logProvisional(grandParent);
                grandParent.updateEntry(
                    parentIndex, parentItem.lsn, NULL_VLSN, parentItem.size);
                parentLsn = parentItem.lsn;
            }

            /* Coordinate the split with an in-progress checkpoint. */
            env.getCheckpointer().coordinateSplitWithCheckpoint(newSibling);

            /*
             * Check whether either the old or the new sibling must be added
             * to the LRU (priority-1 LRUSet).
             */
            assert(!isDIN() && !isDBIN());

            if (isBIN() || !newSibling.hasCachedChildrenFlag()) {
                if (traceLRU) {
                    if (isUpperIN()) {
                        LoggerUtils.envLogMsg(
                            traceLevel, getEnv(),
                            "split-newSibling " +
                                Thread.currentThread().getId() + "-" +
                                Thread.currentThread().getName() +
                                "-" + getEnv().getName() +
                                " Adding UIN to LRU: " +
                                newSibling.getNodeId());
                    }
                }
                getEvictor().addBack(newSibling);
            }

            if (isUpperIN() &&
                haveCachedChildren &&
                !hasCachedChildrenFlag()) {
                if (traceLRU) {
                    LoggerUtils.envLogMsg(
                        traceLevel, getEnv(),
                        "split-oldSibling " +
                        Thread.currentThread().getId() + "-" +
                        Thread.currentThread().getName() +
                        "-" + getEnv().getName() +
                        " Adding UIN to LRU: " + getNodeId());
                }
                getEvictor().addBack(this);
            }

            /* Debug log this information. */
            traceSplit(
                Level.FINE, parent, newSibling, parentLsn, myNewItem.lsn,
                newSiblingItem.lsn, splitIndex, idKeyIndex, childIndex);
        } catch (Throwable e) {
            newSibling.releaseLatchIfOwner();
            throw e;
        }

        return newSibling;
    }

    /**
     * Used for moving entries between BINs during splits.
     */
    void appendEntryFromOtherNode(IN from, int fromIdx) {

        assert(!isBINDelta());

        final Node target = from.entryTargets.get(fromIdx);
        final long lsn = from.getLsn(fromIdx);
        final byte state = from.entryStates[fromIdx];
        final byte[] key = from.getKey(fromIdx);
        final byte[] embData = (from.haveEmbeddedData(fromIdx) ?
                             from.getEmbeddedData(fromIdx) : null);

        long oldSize = computeLsnOverhead();

        ++nEntries;

        int idx = nEntries - 1;

        /*
         * When calling setTarget for an IN child we must latch it, because
         * setTarget sets the parent. The child latch may already be held
         * when a split is performed at level 2, since BIN latches are not
         * released until the end of the split.
         */
        final IN targetIN =
            (target != null && target.isIN()) ? (IN) target : null;

        if (targetIN != null && !targetIN.isLatchExclusiveOwner()) {
            targetIN.latchNoUpdateLRU(databaseImpl);
            setTarget(idx, target);
            targetIN.releaseLatch();
        } else {
            setTarget(idx, target);
        }

        boolean multiSlotChange = insertKey(idx, key, embData);

        /* setLsnInternal can mutate to an array of longs. */
        setLsnInternal(idx, lsn);

        entryStates[idx] = state;

        if (multiSlotChange) {
            updateMemorySize(inMemorySize, computeMemorySize());
        } else {
            long newSize = getEntryInMemorySize(idx) + computeLsnOverhead();
            updateMemorySize(oldSize, newSize);
        }

        setLastLoggedSizeUnconditional(idx, from.getLastLoggedSize(fromIdx));

        setDirty(true);
    }

    /**
     * Update a slot that is being split. The slot to be updated here is the
     * one that existed before the split.
     *
     * @param child The new child to be placed under the slot. May be the
     * newly created sibling or the pre-existing sibling.
     * @param lsn The new lsn of the child (the child was logged just before
     * calling this method, so its slot lsn must be updated)
     * @param key The new key for the slot. We should not actually update the
     * slot key, because its value is the lower bound of the key range covered
     * by the slot, and this lower bound does not change as a result of the
     * split (the new slot created as a result of the split is placed to the
     * right of the pre-existing slot). There is however one exception: the
     * key can be updated if "idx" is the 0-slot. The 0-slot key is not a true
     * lower bound; the actual lower bound for the 0-slot is the key in the
     * parent slot for this IN. So, in this case, if the given key is less
     * than the current one, it is better to update the key in order to better
     * approximate the real lower bound (and thus make the isKeyInBounds()
     * method more effective).
     */
    private void updateSplitSlot(
        int idx,
        IN child,
        long lsn,
        byte[] key) {

        assert(isUpperIN());

        long oldSize = getEntryInMemorySize(idx);

        setLsn(idx, lsn);
        setTarget(idx, child);

        if (idx == 0) {
            int s = entryKeys.compareKeys(
                key, keyPrefix, idx, haveEmbeddedData(idx),
                getKeyComparator());

            boolean multiSlotChange = false;
            if (s < 0) {
                multiSlotChange = updateKey(idx, key, null/*data*/);
            }

            if (multiSlotChange) {
                updateMemorySize(inMemorySize, computeMemorySize());
            } else {
                long newSize = getEntryInMemorySize(idx);
                updateMemorySize(oldSize, newSize);
            }
        } else {
            long newSize = getEntryInMemorySize(idx);
            updateMemorySize(oldSize, newSize);
        }

        setDirty(true);

        assert(hasCachedChildren() == hasCachedChildrenFlag());
    }

    /**
     * Shift entries to the right by one position, starting with (and
     * including) the entry at index. Increment nEntries by 1. Called
     * in insertEntry1()
     *
     * @param index - The position to start shifting from.
     */
    private void shiftEntriesRight(int index) {
        copyEntries(index, index + 1, nEntries - index);
        clearEntry(index);
        nEntries++;
        setDirty(true);
    }

    /**
     * Shift entries starting at the byHowMuch'th element to the left, thus
     * removing the first byHowMuch'th elements of the entries array.  This
     * always starts at the 0th entry. Caller is responsible for decrementing
     * nEntries.
     *
     * @param byHowMuch - The number of entries to remove from the left side
     * of the entries array.
     */
    private void shiftEntriesLeft(int byHowMuch) {
        copyEntries(byHowMuch, 0, nEntries - byHowMuch);
        for (int i = nEntries - byHowMuch; i < nEntries; i++) {
            clearEntry(i);
        }
        setDirty(true);
    }

    void adjustCursors(
        IN newSibling,
        int newSiblingLow,
        int newSiblingHigh) {
        /* Cursors never refer to IN's. */
    }

    void adjustCursorsForInsert(int insertIndex) {
        /* Cursors never refer to IN's. */
    }

    /*
     * Get the current memory consumption of this node
     */
    public long getInMemorySize() {
        return inMemorySize;
    }

    /**
     * Compute the current memory consumption of this node, after putting
     * its keys in their compact representation, if possible.
     */
    private void initMemorySize() {
        entryKeys = entryKeys.compact(this);
        inMemorySize = computeMemorySize();
    }

    /**
     * Count up the memory usage attributable to this node alone. LNs children
     * are counted by their BIN parents, but INs are not counted by their
     * parents because they are resident on the IN list.  The identifierKey is
     * "intentionally" not kept track of in the memory budget.
     */
    public long computeMemorySize() {

        long calcMemorySize = getFixedMemoryOverhead();

        calcMemorySize += MemoryBudget.byteArraySize(entryStates.length);

        calcMemorySize += computeLsnOverhead();

        for (int i = 0; i < nEntries; i++) {
            calcMemorySize += getEntryInMemorySize(i);
        }

        if (keyPrefix != null) {
            calcMemorySize += MemoryBudget.byteArraySize(keyPrefix.length);
        }

        if (provisionalObsolete != null) {
            calcMemorySize += provisionalObsolete.getMemorySize();
        }

        calcMemorySize += entryTargets.calculateMemorySize();
        calcMemorySize += entryKeys.calculateMemorySize();

        if (lastLoggedSizes != null) {
            calcMemorySize += lastLoggedSizes.getMemorySize();
        }

        return calcMemorySize;
    }

    /*
     * Overridden by subclasses.
     */
    protected long getFixedMemoryOverhead() {
        return MemoryBudget.IN_FIXED_OVERHEAD;
    }

    /*
     * Compute the memory consumption for storing this node's LSNs
     */
    private int computeLsnOverhead() {
        return MemoryBudget.ARRAY_OVERHEAD +
                (entryLsns.length *
                 MemoryBudget.PRIMITIVE_LONG_ARRAY_ITEM_OVERHEAD);
    }

    private long getEntryInMemorySize(int idx) {

        /*
         * Do not count state size here, since it is counted as overhead
         * during initialization.
         */
        long ret = 0;

        /*
         * Don't count the key size if the representation has already
         * accounted for it.
         */
        if (!entryKeys.accountsForKeyByteMemUsage()) {

            /*
             * Materialize the key object only if needed, thus avoiding the
             * object allocation cost when possible.
             */
            final byte[] key = entryKeys.get(idx);
            if (key != null) {
                ret += MemoryBudget.byteArraySize(key.length);
            }
        }

        final Node target = entryTargets.get(idx);
        if (target != null) {
            ret += target.getMemorySizeIncludedByParent();
        }
        return ret;
    }

    /**
     * Compacts the representation of the IN, if possible.
     *
     * Called by the evictor to reduce memory usage. Should not be called too
     * often (e.g., every CRUD operation), since this could cause lots of
     * memory allocations as the representations contract and expend, resulting
     * in expensive GC.
     *
     * @return number of bytes reclaimed.
     */
    public long compactMemory() {

        final long oldSize = inMemorySize;
        final INKeyRep oldKeyRep = entryKeys;

        entryTargets = entryTargets.compact(this);
        entryKeys = entryKeys.compact(this);

        /*
         * Note that we only need to account for mem usage changes in the key
         * rep here, not the target rep.  The target rep, unlike the key rep,
         * updates its mem usage internally, and the responsibility for mem
         * usage of contained nodes is fixed -- it is always managed by the IN.
         *
         * When the key rep changes, the accountsForKeyByteMemUsage property
         * also changes. Recalc the size of the entire IN, because
         * responsibility for managing contained key byte mem usage has shifted
         * between the key rep and the IN parent.
         */
        if (entryKeys != oldKeyRep) {
            updateMemorySize(inMemorySize, computeMemorySize());
        }

        return oldSize - inMemorySize;
    }

    /**
     * Returns the amount of memory currently budgeted for this IN.
     */
    public long getBudgetedMemorySize() {
        return inMemorySize - accumulatedDelta;
    }

    /**
     * Called as part of a memory budget reset (during a checkpoint) to clear
     * the accumulated delta and return the total memory size.
     */
    public long resetAndGetMemorySize() {
        accumulatedDelta = 0;
        return inMemorySize;
    }

    protected void updateMemorySize(long oldSize, long newSize) {
        long delta = newSize - oldSize;
        updateMemorySize(delta);
    }

    /*
     * Called when a cached child is replaced by another cached child.
     */
    void updateMemorySize(Node oldNode, Node newNode) {
        long delta = 0;
        if (newNode != null) {
            delta = newNode.getMemorySizeIncludedByParent();
        }

        if (oldNode != null) {
            delta -= oldNode.getMemorySizeIncludedByParent();
        }
        updateMemorySize(delta);
    }

    /*
     * Change this.onMemorySize by the given delta and update the memory
     * budget for the cache, but only if the accummulated delta for this
     * node exceeds the ACCUMULATED_LIMIT threshold and this IN is actually
     * on the IN list. (For example, when we create new INs, they are
     * manipulated off the IN list before being added; if we updated the
     * environment wide cache then, we'd end up double counting.)
     */
    void updateMemorySize(long delta) {

        if (delta == 0) {
            return;
        }

        inMemorySize += delta;

        if (getInListResident()) {

            /*
             * This assertion is disabled if the environment is invalid to
             * avoid spurious assertions during testing of IO errors.  If the
             * environment is invalid, memory budgeting errors are irrelevant.
             * [#21929]
             */
            assert
                inMemorySize >= getFixedMemoryOverhead() ||
                !getEnv().isValid():
                "delta: " + delta + " inMemorySize: " + inMemorySize +
                " overhead: " + getFixedMemoryOverhead() +
                " computed: " + computeMemorySize() +
                " dump: " + toString() + assertPrintMemorySize();

            accumulatedDelta += delta;
            if (accumulatedDelta > ACCUMULATED_LIMIT ||
                accumulatedDelta < -ACCUMULATED_LIMIT) {
                updateMemoryBudget();
            }
        }
    }

    /**
     * Move the accumulated delta to the memory budget.
     */
    public void updateMemoryBudget() {
        final EnvironmentImpl env = getEnv();
        env.getInMemoryINs().memRecalcUpdate(this, accumulatedDelta);
        env.getMemoryBudget().updateTreeMemoryUsage(accumulatedDelta);
        accumulatedDelta = 0;
    }

    /*
     *  Utility method used during unit testing.
     */
    protected long printMemorySize() {

        final long inOverhead = getFixedMemoryOverhead();

        final long statesOverhead =
            MemoryBudget.byteArraySize(entryStates.length);

        final int lsnOverhead =  computeLsnOverhead();

        int entryOverhead = 0;
        for (int i = 0; i < nEntries; i++) {
            entryOverhead += getEntryInMemorySize(i);
        }

        final int keyPrefixOverhead =  (keyPrefix != null) ?
            MemoryBudget.byteArraySize(keyPrefix.length) : 0;

        final int provisionalOverhead = (provisionalObsolete != null) ?
            provisionalObsolete.getMemorySize() : 0;

        final long targetRepOverhead = entryTargets.calculateMemorySize();
        final long keyRepOverhead = entryKeys.calculateMemorySize();
        final long logSizesOverhead = lastLoggedSizes.getMemorySize();

        final long total = inOverhead + statesOverhead + lsnOverhead +
            entryOverhead + keyPrefixOverhead +  provisionalOverhead +
            targetRepOverhead + keyRepOverhead + logSizesOverhead;

        System.out.println(" nEntries:" + nEntries +
                           "/" + entryStates.length +
                           " in: " + inOverhead +
                           " states: " + statesOverhead +
                           " entry: " + entryOverhead +
                           " lsn: " + lsnOverhead +
                           " keyPrefix: " + keyPrefixOverhead +
                           " provisional: " + provisionalOverhead +
                           " targetRep(" + entryTargets.getType() + "): " +
                           targetRepOverhead +
                           " keyRep(" + entryKeys.getType() +"): " +
                           keyRepOverhead +
                           " logSizesOverhead: " + logSizesOverhead +
                           " Total: " + total +
                           " inMemorySize: " + inMemorySize);
        return total;
    }

    /* Utility method used to print memory size in an assertion. */
    private boolean assertPrintMemorySize() {
        printMemorySize();
        return true;
    }

    public boolean verifyMemorySize() {

        long calcMemorySize = computeMemorySize();
        if (calcMemorySize != inMemorySize) {

            String msg = "-Warning: Out of sync. Should be " +
                calcMemorySize + " / actual: " + inMemorySize +
                " node: " + getNodeId();
            LoggerUtils.envLogMsg(Level.INFO, getEnv(), msg);

            System.out.println(msg);
            printMemorySize();

            return false;
        }
        return true;
    }

    /**
     * Adds (increments) or removes (decrements) the cache stats for the key
     * and target representations.  Used when rep objects are being replaced
     * with a new instance, rather than by calling their mutator methods.
     * Specifically, it is called when mutating from full bin to bin delta
     * or vice-versa.
     */
    protected void updateRepCacheStats(boolean increment) {
        assert(isBIN());
        entryKeys.updateCacheStats(increment, this);
        entryTargets.updateCacheStats(increment, this);
    }

    protected int getCompactMaxKeyLength() {
        return getEnv().getCompactMaxKeyLength();
    }

    /**
     * Called when adding/removing this IN to/from the INList.
     */
    public void setInListResident(boolean resident) {

        if (!resident) {
            /* Decrement the stats before clearing its residency */
            entryTargets.updateCacheStats(false, this);
            entryKeys.updateCacheStats(false, this);
        }

        if (resident) {
            flags |= IN_RESIDENT_BIT;
        } else {
            flags &= ~IN_RESIDENT_BIT;
        }

        if (resident) {
            /* Increment the stats after setting its residency. */
            entryTargets.updateCacheStats(true, this);
            entryKeys.updateCacheStats(true, this);
        }
    }

    /**
     * Returns whether this IN is on the INList.
     */
    public boolean getInListResident() {
        return (flags & IN_RESIDENT_BIT) != 0;
    }

    public IN getPrevLRUNode() {
    	return prevLRUNode;
    }

    public void setPrevLRUNode(IN node) {
    	prevLRUNode = node;
    }

    public IN getNextLRUNode() {
    	return nextLRUNode;
    }

    public void setNextLRUNode(IN node) {
    	nextLRUNode = node;
    }

    /**
     * @see BIN#partialEviction
     */
    public long partialEviction() {
        return 0;
    }

    /**
     * @see BIN#isEvictable
     */
    public boolean isEvictable() {
        return true;
    }

    /**
     * Returns whether any child is non-null in the cache.
     */
    public boolean hasCachedChildren() {
        assert isLatchOwner();

        for (int i = 0; i < getNEntries(); i++) {
            if (entryTargets.get(i) != null) {
                return true;
            }
        }

        return false;
    }

    /**
     * Disallow delta on next log. Set to true (a) when we we delete a slot
     * from a BIN, (b) when the cleaner marks a BIN as dirty so that it will
     * be migrated during the next checkpoint.
     */
    public void setProhibitNextDelta(boolean val) {

        if (!isBIN()) {
            return;
        }

        if (val) {
            flags |= IN_PROHIBIT_NEXT_DELTA_BIT;
        } else {
            flags &= ~IN_PROHIBIT_NEXT_DELTA_BIT;
        }
    }

    public boolean getProhibitNextDelta() {
        return (flags & IN_PROHIBIT_NEXT_DELTA_BIT) != 0;
    }

    /*
     * Validate the subtree that we're about to delete.  Make sure there aren't
     * more than one valid entry on each IN and that the last level of the tree
     * is empty. Also check that there are no cursors on any bins in this
     * subtree. Assumes caller is holding the latch on this parent node.
     *
     * While we could latch couple down the tree, rather than hold latches as
     * we descend, we are presumably about to delete this subtree so
     * concurrency shouldn't be an issue.
     *
     * @return true if the subtree rooted at the entry specified by "index" is
     * ok to delete.
     *
     * Overriden by BIN class.
     */
    boolean validateSubtreeBeforeDelete(int index)
        throws DatabaseException {

        if (index >= nEntries) {

            /*
             * There's no entry here, so of course this entry is deletable.
             */
            return true;
        } else {
            IN child = fetchIN(index, CacheMode.UNCHANGED);

            boolean needToLatch = !child.isLatchExclusiveOwner();

            try {
                if (needToLatch) {
                    child.latch(CacheMode.UNCHANGED);
                }
                return child.isValidForDelete();
            } finally {
                if (needToLatch && isLatchOwner()) {
                    child.releaseLatch();
                }
            }
        }
    }

    /**
     * Check if this node fits the qualifications for being part of a deletable
     * subtree. It can only have one IN child and no LN children.
     *
     * Note: the method is overwritten by BIN and LN.
     * BIN.isValidForDelete() will not fetch any child LNs.
     * LN.isValidForDelete() simply returns false.
     *
     * We assume that this is only called under an assert.
     */
    @Override
    boolean isValidForDelete()
        throws DatabaseException {

        assert(!isBINDelta());

        /*
         * Can only have one valid child, and that child should be
         * deletable.
         */
        if (nEntries > 1) {            // more than 1 entry.
            return false;

        } else if (nEntries == 1) {    // 1 entry, check child
            IN child = fetchIN(0, CacheMode.UNCHANGED);
            boolean needToLatch = !child.isLatchExclusiveOwner();
            if (needToLatch) {
                child.latch(CacheMode.UNCHANGED);
            }

            boolean ret = false;
            try {
                if (child.isBINDelta()) {
                    return false;
                }

                ret = child.isValidForDelete();

            } finally {
                if (needToLatch) {
                    child.releaseLatch();
                }
            }
            return ret;
        } else {                       // 0 entries.
            return true;
        }
    }

    /**
     * Add self and children to this in-memory IN list. Called by recovery, can
     * run with no latching.
     */
    @Override
    final void rebuildINList(INList inList)
        throws DatabaseException {

        /*
         * Recompute your in memory size first and then add yourself to the
         * list.
         */
        initMemorySize();

        inList.add(this);

        boolean hasCachedChildren = false;

        /*
         * Add your children if they're resident. (LNs know how to stop the
         * flow).
         */
        for (int i = 0; i < nEntries; i++) {
            Node n = getTarget(i);
            if (n != null) {
                n.rebuildINList(inList);
                hasCachedChildren = true;
            }
        }

        if (isUpperIN()) {
            if (hasCachedChildren) {
                setHasCachedChildrenFlag(true);
            } else {
                setHasCachedChildrenFlag(false);
                if (!isDIN()) {
                    if (traceLRU) {
                        LoggerUtils.envLogMsg(
                            traceLevel, getEnv(),
                            "rebuildINList " +
                            Thread.currentThread().getId() +
                            "-" +
                            Thread.currentThread().getName() +
                            "-" + getEnv().getName() +
                            " Adding UIN to LRU: " +
                            getNodeId());
                    }
                    getEvictor().addBack(this);
                }
            }
        } else if (isBIN() && !isDBIN()) {
            getEvictor().addBack(this);
        }
    }

    /*
     * DbStat support.
     */
    void accumulateStats(TreeWalkerStatsAccumulator acc) {
        acc.processIN(this, getNodeId(), getLevel());
    }

    /**
     * Sets the last logged LSN and size, as appropriate for INs, full BINs
     * and BIN-deltas.
     */
    private void setLastLsnsAndSizes(long lsn, WholeEntry wholeEntry) {
        final int logSize = wholeEntry.getHeader().getEntrySize();

        if (isBINDelta()) {
            final LogEntry entry = wholeEntry.getEntry();
            final long lastFullLsn;
            final int lastFullLogSize;

            if (entry instanceof INLogEntry<?>) {
                final INLogEntry<?> inEntry = (INLogEntry<?>) entry;
                lastFullLsn = inEntry.getLastFullLsn();
                lastFullLogSize = inEntry.getLastFullLogSize();
            } else {
                throw unexpectedState();
            }

            setLastFullLsnAndSize(lastFullLsn, lastFullLogSize);
            setLastDeltaLsnAndSize(lsn, logSize);
        } else {
            setLastFullLsnAndSize(lsn, logSize);
            setLastDeltaLsnAndSize(DbLsn.NULL_LSN, 0);
        }
    }

    /**
     * Returns the LSN of the last last logged version of this IN, or NULL_LSN
     * if never logged.
     */
    public final long getLastLoggedLsn() {
        if (isBIN()) {
            return (getLastDeltaLsn() != DbLsn.NULL_LSN ?
                    getLastDeltaLsn() :
                    getLastFullLsn());
        }

        return getLastFullLsn();
    }

    public final void setLastFullLsnAndSize(long lsn, int logSize) {
        lastFullLsn = lsn;
        lastFullLogSize = logSize;
    }

    public void setLastDeltaLsnAndSize(long lsn, int logSize) {
        lastDeltaLsn = lsn;
        lastDeltaLogSize = logSize;
    }

    /**
     * Returns the last full version LSN, or NULL_LSN if this IN has not yet
     * been logged.
     */
    public final long getLastFullLsn() {
        return lastFullLsn;
    }

    /**
     * Returns the size of the {@link #getLastFullLsn()} log entry. Returns
     * zero if this IN has not yet been logged or the logged version is 20 or
     * earlier.
     */
    public int getLastFullLogSize() {
        return lastFullLogSize;
    }

    /**
     * Returns the last delta version LSN, or NULL_LSN if the last logged
     * entry was not a delta.
     */
    public long getLastDeltaLsn() {
        return lastDeltaLsn;
    }

    /**
     * Returns the size of the {@link #getLastDeltaLsn()} log entry. Returns
     * zero if the last logged entry was not a delta or the logged version is
     * 20 or earlier.
     */
    public int getLastDeltaLogSize() {
        return lastDeltaLogSize;
    }

    /*
     * Logging support
     */

    public final LogItem log() {
        return logInternal(
            false /*allowDeltas*/, true /*allowCompress*/,
            Provisional.NO, false /*backgroundIO*/, null /*parent*/);
    }

    public final LogItem log(
        boolean allowDeltas,
        boolean isProvisional,
        boolean backgroundIO,
        IN parent) {

        return logInternal(
            allowDeltas, true /*allowCompress*/,
            isProvisional ? Provisional.YES : Provisional.NO,
            backgroundIO, parent);
    }

    public final LogItem log(
        boolean allowDeltas,
        Provisional provisional,
        boolean backgroundIO,
        IN parent) {

        return logInternal(
            allowDeltas, true /*allowCompress*/, provisional,
            backgroundIO, parent);
    }

    LogItem logProvisional(IN parent) {
        return logProvisional(parent, true /*allowCompress*/);
    }

    LogItem logProvisionalNoCompress(IN parent) {
        return logProvisional(parent, false /*allowCompress*/);
    }

    private LogItem logProvisional(IN parent, boolean allowCompress) {
        return logInternal(
            false /*allowDeltas*/, allowCompress,
            Provisional.YES, false /*backgroundIO*/, parent);
    }

    /**
     * Bottleneck method for all IN logging.
     *
     * If 'node' is non-null, 'logEntry' must be null.
     * If 'node' is null, 'logEntry' and 'parent' must be non-null.
     *
     * 'parent' may or may not be null. It must be non-null when logging
     * provisionally, since obsolete LSNs are added to the parent's collection.
     */
    private LogItem logInternal(
        final boolean allowDeltas,
        final boolean allowCompress,
        final Provisional provisional,
        final boolean backgroundIO,
        final IN parent) {

        assert isLatchExclusiveOwner();
        assert parent == null || parent.isLatchExclusiveOwner();

        final DatabaseImpl dbImpl = getDatabase();
        final EnvironmentImpl envImpl = dbImpl.getEnv();

        final boolean isBin = isBIN();
        final BIN bin = isBin ? ((BIN) this) : null;

        final boolean isDelta;
        final INLogEntry<?> logEntry;

        if (isBin) {
            /* Compress non-dirty slots before determining delta status. */
            if (allowCompress) {
                envImpl.lazyCompress(bin, false /*compressDirtySlots*/);
            }

            isDelta = bin.isBINDelta() ||
                (allowDeltas && bin.shouldLogDelta());

            /* Be sure that we didn't illegally mutate to a delta. */
            assert (!(isDelta && bin.isDeltaProhibited()));

            /* Also compress dirty slots, if we will not log a delta. */
            if (allowCompress && !isDelta) {
                envImpl.lazyCompress(bin, true /*compressDirtySlots*/);
            }

            logEntry = isDelta ?
                (new BINDeltaLogEntry(bin)) :
                (new INLogEntry<>(bin));
        } else {
            updateChildKeys();
            isDelta = false;
            logEntry = new INLogEntry<>(this);
        }

        final LogParams params = new LogParams();
        params.entry = logEntry;
        params.provisional = provisional;
        params.repContext = ReplicationContext.NO_REPLICATE;
        params.nodeDb = dbImpl;
        params.backgroundIO = backgroundIO;

        /*
         * For delta logging:
         *  + Count lastDeltaVersion obsolete, if non-null.
         *  + Set lastDeltaVersion to newly logged LSN.
         *  + Leave lastFullVersion unchanged.
         *
         * For full version logging:
         *  + Count lastFullVersion and lastDeltaVersion obsolete, if non-null.
         *  + Set lastFullVersion to newly logged LSN.
         *  + Set lastDeltaVersion to null.
         */
        final long oldLsn =
            isDelta ? DbLsn.NULL_LSN : logEntry.getLastFullLsn();

        final long auxOldLsn = logEntry.getLastDeltaLsn();

        /*
         * Determine whether to count the prior version of an IN (as well as
         * accumulated provisionally obsolete LSNs for child nodes) obsolete
         * when logging the new version.
         *
         * True is set if we are logging the IN non-provisionally, since the
         * non-provisional version durably replaces the prior version and
         * causes all provisional children to also become durable.
         */
        final boolean countObsoleteNow = provisional != Provisional.YES;

        if (countObsoleteNow) {
            params.oldLsn = oldLsn;
            params.auxOldLsn = auxOldLsn;
            params.packedObsoleteInfo = provisionalObsolete;
            discardProvisionalObsolete();
        }

        /* Log it. */
        final LogItem item = envImpl.getLogManager().log(params);

        item.deltaBinLogged = isDelta;

        /*
         * If a BIN split is performed during recovery in a read-only
         * environment then the logging operation for the new sibling
         * is a no-op, and as such should remain dirty so it is never
         * evicted from the cache.  See [KVSTORE-706].
         */
        if (!envImpl.isReadOnly()) {
            setDirty(false);
        }

        if (!countObsoleteNow && parent != null) {
            parent.trackProvisionalObsolete(this, oldLsn);
            parent.trackProvisionalObsolete(this, auxOldLsn);
        }

        if (bin != null) {
            if (isDelta) {
                bin.setLastDeltaLsnAndSize(item.lsn, item.size);
            } else {
                bin.setLastFullLsnAndSize(item.lsn, item.size);
                bin.setLastDeltaLsnAndSize(DbLsn.NULL_LSN, 0);
            }

            bin.setProhibitNextDelta(false);

        } else {
            setLastFullLsnAndSize(item.lsn, item.size);
        }

        final Evictor evictor = envImpl.getEvictor();

        if (evictor.useDirtyLRUSet()) {

            /*
             * To capture all cases where a node needs to be moved to the
             * priority-1 LRUSet after being cleaned, we invoke moveToPri1LRU()
             * after logging. This includes the case where the node is being
             * logged as part of being evicted, in which case we don't really
             * want it to go back to the LRU. However, this is ok because
             * moveToPri1LRU() checks whether the node is actually in the
             * priority-2 LRUSet before moving it to the priority-1 LRUSet.
             */
            if (traceLRU) {
                if (isUpperIN()) {
                    LoggerUtils.envLogMsg(
                        traceLevel, envImpl,
                        Thread.currentThread().getId() + "-" +
                            Thread.currentThread().getName() +
                            "-" + envImpl.getName() +
                            " afterLogCommon(): " +
                            " Moving UIN to mixed LRU: " + getNodeId());
                }
            }
            evictor.moveToPri1LRU(this);
        }

        return item;
    }

    /**
     * Adds the given obsolete LSN and any tracked obsolete LSNs for the given
     * child IN to this IN's tracking list.  This method is called to track
     * obsolete LSNs when a child IN is logged provisionally.  Such LSNs
     * cannot be considered obsolete until an ancestor IN is logged
     * non-provisionally.
     */
    void trackProvisionalObsolete(final IN childIN, final long obsoleteLsn) {

        final boolean moveChildInfo =
            (childIN != null && childIN.provisionalObsolete != null);

        final boolean addChildLsn = (obsoleteLsn != DbLsn.NULL_LSN);

        if (!moveChildInfo && !addChildLsn) {
            return;
        }

        final int oldMemSize = (provisionalObsolete != null) ?
             provisionalObsolete.getMemorySize() : 0;

        if (moveChildInfo) {
            if (provisionalObsolete != null) {
                /* Append child info to parent info. */
                provisionalObsolete.copyObsoleteInfo
                    (childIN.provisionalObsolete);
            } else {
                /* Move reference from child to parent. */
                provisionalObsolete = childIN.provisionalObsolete;
            }
            childIN.updateMemorySize(
                0 - childIN.provisionalObsolete.getMemorySize());
            childIN.provisionalObsolete = null;
        }

        if (addChildLsn) {
            if (provisionalObsolete == null) {
                provisionalObsolete = new PackedObsoleteInfo();
            }
            provisionalObsolete.addObsoleteInfo(obsoleteLsn);
        }

        updateMemorySize(oldMemSize,
                         (provisionalObsolete != null) ?
                         provisionalObsolete.getMemorySize() :
                         0);
    }

    /**
     * Discards the provisional obsolete tracking information in this node
     * after it has been counted in the live tracker.  This method is called
     * after this node is logged non-provisionally.
     */
    private void discardProvisionalObsolete()
        throws DatabaseException {

        if (provisionalObsolete != null) {
            updateMemorySize(0 - provisionalObsolete.getMemorySize());
            provisionalObsolete = null;
        }
    }

    /*
     * NOOP for upper INs. Overriden by BIN class.
     */
    public void mutateToFullBIN(boolean leaveFreeSlot) {
    }

    public void mutateToFullBIN(boolean leaveFreeSlot, CacheMode cacheMode) {
    }

    /**
     * Only write dirty slots when logging a BIN-delta.
     *
     * Don't write slots that are extinct when logging a full BIN, to support
     * erasure of extinct records. Skipping the extinct slots during logging
     * is necessary because extinct slots cannot be deleted by BIN.compress
     * when a cursor is present.
     *
     * Otherwise, when logging an IN, write all slots.
     */
    private int getNEntriesToWrite(boolean deltasOnly) {
        if (deltasOnly) {
            return getNDeltas();
        }
        if (!isBIN()) {
            return nEntries;
        }
        int n = 0;
        for (int i = 0; i < nEntries; i++) {
            if (canDeleteExtinctSlot(i)) {
                continue;
            }
            n += 1;
        }
        return n;
    }

    /**
     * Returns whether the given slot is extinct and can be deleted during
     * logging.
     *
     * Check KnownDeleted before checking for an extinct slot, to
     * reduce calls to the more expensive getExtinctionStatus method.
     * KnownDeleted is set on extinct slots by the ExtinctionScanner.
     */
    private boolean canDeleteExtinctSlot(int idx) {
        assert isBIN();

        final EnvironmentImpl envImpl = getEnv();

        /*
         * The check for isValid is necessary because we cannot call the
         * extinction filter until recovery is complete and we have the DB
         * name and type [#27022]. In the future we may wish to implement a
         * more comprehensive check in getExtinctionStatus.
         */
        return envImpl.isValid() &&
            isEntryKnownDeleted(idx) &&
            envImpl.getExtinctionStatus(databaseImpl, getKey(idx)) == EXTINCT;
    }

    public final int getNDeltas() {
        int n = 0;
        for (int i = 0; i < nEntries; i++) {
            if (!isDirty(i)) {
                continue;
            }
            n += 1;
        }
        return n;
    }

    /**
     * @see Node#getGenericLogType
     */
    @Override
    public final LogEntryType getGenericLogType() {
        return getLogType();
    }

    /**
     * Get the log type of this node.
     */
    public LogEntryType getLogType() {
        return LogEntryType.LOG_IN;
    }

    @Override
    public int getLogSize() {
        return getLogSize(false);
    }

    public final int getLogSize(boolean deltasOnly) {

        BIN bin = (isBIN() ? (BIN)this : null);

        boolean haveVLSNCache = (bin != null && bin.isVLSNCachingEnabled());

        int size = 0;

        boolean haveExpiration = false;

        if (bin != null) {
            final int expBase = bin.getExpirationBase();
            haveExpiration = (expBase != -1);
            size += LogUtils.getPackedIntLogSize(expBase);
            final long createTimesBase = bin.getCreationTimesBase();
            size += LogUtils.getPackedLongLogSize(createTimesBase);
            final long modTimesBase = bin.getModificationTimesBase();
            size += LogUtils.getPackedLongLogSize(modTimesBase);
        }

        size += LogUtils.getPackedLongLogSize(nodeId);
        size += LogUtils.getByteArrayLogSize(identifierKey); // identifier key

        if (keyPrefix != null) {
            size += LogUtils.getByteArrayLogSize(keyPrefix);
        }

        size += LogUtils.getPackedIntLogSize(entryKeys.getCompactMaxKeySize());

        size += 1; // one byte for boolean flags

        final int nEntriesToWrite = getNEntriesToWrite(deltasOnly);

        final int maxEntriesToWrite =
            (!deltasOnly ?
             getMaxEntries() :
             bin.getDeltaCapacity(nEntriesToWrite));

        size += LogUtils.getPackedIntLogSize(nEntriesToWrite);
        size += LogUtils.getPackedIntLogSize(level);
        size += LogUtils.getPackedIntLogSize(maxEntriesToWrite);

        for (int i = 0; i < nEntries; i++) {    // entries

            /* See getNEntriesToWrite and canDeleteExtinctSlot. */
            if (deltasOnly) {
                if (!isDirty(i)) {
                    continue;
                }
            } else {
                if (bin != null && canDeleteExtinctSlot(i)) {
                    continue;
                }
            }

            size += LogUtils.getByteArrayLogSize(entryKeys.size(i)) +
                LogUtils.getLongLogSize() +  // LSN
                1;                           // state

            if (isLastLoggedSizeStored(i)) {
                size += LogUtils.getPackedIntLogSize(getLastLoggedSize(i));
            }

            final boolean isEmbedded = isEmbeddedLN(i);

            if (haveVLSNCache && isEmbedded) {
                size += LogUtils.getPackedLongLogSize(bin.getCachedVLSN(i));
            }

            if (haveExpiration) {
                size +=
                    LogUtils.getPackedIntLogSize(bin.getExpirationOffset(i));
            }

            if (isEmbedded) {
                size += LogUtils.getPackedLongLogSize(
                    bin.getCreationTimeOffset(i));
                size += LogUtils.getPackedLongLogSize(
                    bin.getModificationTimeOffset(i));
            }
        }

        if (deltasOnly) {
            size += LogUtils.getPackedIntLogSize(bin.getFullBinNEntries());
            size += LogUtils.getPackedIntLogSize(bin.getFullBinMaxEntries());

            size += bin.getBloomFilterLogSize();
        }

        return size;
    }

    @Override
    public void writeToLog(ByteBuffer logBuffer) {

        serialize(logBuffer, false /*deltasOnly*/, true /*clearDirtyBits*/);
    }

    public void writeToLog(ByteBuffer logBuffer, boolean deltasOnly) {

        serialize(logBuffer, deltasOnly, !deltasOnly /*clearDirtyBits*/);
    }

    /**
     * This method has no side effects unless the clearDirtyBits param is true.
     */
    public final void serialize(ByteBuffer logBuffer,
                                boolean deltasOnly,
                                boolean clearDirtyBits) {

        assert(!deltasOnly || isBIN());

        final BIN bin = (isBIN() ? (BIN)this : null);

        final byte[] bloomFilter =
            (deltasOnly ? bin.createBloomFilter() : null);

        final boolean haveExpiration;

        if (bin != null) {
            final int expBase = bin.getExpirationBase();
            haveExpiration = (expBase != -1);
            LogUtils.writePackedInt(logBuffer, expBase);
            LogUtils.writePackedLong(
                logBuffer, bin.getCreationTimesBase());
            LogUtils.writePackedLong(
                logBuffer, bin.getModificationTimesBase());
        } else {
            haveExpiration = false;
        }

        LogUtils.writePackedLong(logBuffer, nodeId);

        LogUtils.writeByteArray(logBuffer, identifierKey);

        final boolean hasKeyPrefix = (keyPrefix != null);
        final boolean mayHaveLastLoggedSize = mayHaveLastLoggedSizeStored();
        final boolean haveVLSNCache =
            (bin != null && bin.isVLSNCachingEnabled());

        byte booleans = (byte) (isRoot() ? 1 : 0);
        booleans |= (hasKeyPrefix ? 2 : 0);
        booleans |= (mayHaveLastLoggedSize ? 4 : 0);
        booleans |= (bloomFilter != null ? 8 : 0);
        booleans |= (haveVLSNCache ? 16 : 0);
        booleans |= (isExpirationInHours() ? 32 : 0);

        logBuffer.put(booleans);

        if (hasKeyPrefix) {
            LogUtils.writeByteArray(logBuffer, keyPrefix);
        }

        LogUtils.writePackedInt(logBuffer, entryKeys.getCompactMaxKeySize());

        final int nEntriesToWrite = getNEntriesToWrite(deltasOnly);

        final int maxEntriesToWrite =
            (!deltasOnly ?
             getMaxEntries() :
             bin.getDeltaCapacity(nEntriesToWrite));
        /*
        if (deltasOnly) {
            BIN bin = (BIN)this;
            System.out.println(
                "Logging BIN-delta: " + getNodeId() +
                " is delta = " + isBINDelta() +
                " nEntries = " + nEntriesToWrite +
                " max entries = " + maxEntriesToWrite +
                " full BIN entries = " + bin.getFullBinNEntries() +
                " full BIN max entries = " + bin.getFullBinMaxEntries());
        }
        */
        LogUtils.writePackedInt(logBuffer, nEntriesToWrite);
        LogUtils.writePackedInt(logBuffer, level);
        LogUtils.writePackedInt(logBuffer, maxEntriesToWrite);

        for (int i = 0; i < nEntries; i++) {

            /* See getNEntriesToWrite and canDeleteExtinctSlot. */
            if (deltasOnly) {
                if (!isDirty(i)) {
                    continue;
                }
            } else {
                if (bin != null && canDeleteExtinctSlot(i)) {
                    continue;
                }
            }

            entryKeys.write(i, logBuffer);

            /*
             * A NULL_LSN may be stored when an incomplete insertion occurs,
             * but in that case the KnownDeleted flag must be set. See
             * Tree.insert.  [#13126]
             */
            assert checkForNullLSN(i) :
                "logging IN " + getNodeId() + " with null lsn child " +
                " db=" + databaseImpl.getName();

            LogUtils.writeLong(logBuffer, entryLsns[i]);

            logBuffer.put(
                (byte) (entryStates[i] & EntryStates.CLEAR_TRANSIENT_BITS));

            if (clearDirtyBits) {
                entryStates[i] &= EntryStates.CLEAR_DIRTY_BIT;
            }

            if (isLastLoggedSizeStored(i)) {
                LogUtils.writePackedInt(logBuffer, getLastLoggedSize(i));
            }

            final boolean isEmbedded = isEmbeddedLN(i);

            if (haveVLSNCache && isEmbedded) {
                LogUtils.writePackedLong(logBuffer, bin.getCachedVLSN(i));
            }

            if (haveExpiration) {
                LogUtils.writePackedInt(
                    logBuffer, bin.getExpirationOffset(i));
            }

            if (isEmbedded) {
                LogUtils.writePackedLong(
                    logBuffer, bin.getCreationTimeOffset(i));
                LogUtils.writePackedLong(
                    logBuffer, bin.getModificationTimeOffset(i));
            }
        }

        if (deltasOnly) {
            LogUtils.writePackedInt(logBuffer, bin.getFullBinNEntries());
            LogUtils.writePackedInt(logBuffer, bin.getFullBinMaxEntries());

            if (bloomFilter != null) {
                BINDeltaBloomFilter.writeToLog(bloomFilter, logBuffer);
            }
        }
    }

    /*
     * Used for assertion to prevent writing a null lsn to the log.
     */
    private boolean checkForNullLSN(int index) {
        boolean ok;
        if (isBIN()) {
            ok = !(getLsn(index) == DbLsn.NULL_LSN &&
                   (entryStates[index] & EntryStates.KNOWN_DELETED_BIT) == 0);
        } else {
            ok = (getLsn(index) != DbLsn.NULL_LSN);
        }
        return ok;
    }

    /**
     * Returns whether the given serialized IN is a BIN that may have
     * expiration values.
     */
    public boolean mayHaveExpirationValues(
        ByteBuffer itemBuffer,
        int entryVersion) {

        if (!isBIN() || entryVersion < 12) {
            return false;
        }

        itemBuffer.mark();
        int expirationBase = LogUtils.readPackedInt(itemBuffer);
        itemBuffer.reset();

        return (expirationBase != -1);
    }

    @Override
    public void readFromLog(
        EnvironmentImpl envImpl,
        ByteBuffer itemBuffer,
        int entryVersion) {

        materialize(
            envImpl, itemBuffer, entryVersion,
            false /*deltasOnly*/, true /*clearDirtyBits*/);
    }

    public void readFromLog(
        EnvironmentImpl envImpl,
        ByteBuffer itemBuffer,
        int entryVersion,
        boolean deltasOnly) {

        materialize(
            envImpl, itemBuffer, entryVersion,
            deltasOnly, !deltasOnly /*clearDirtyBits*/);
    }

    /**
     * This method has no side effects unless the clearDirtyBits param is true.
     */
    public final void materialize(
        EnvironmentImpl envImpl,
        ByteBuffer itemBuffer,
        int entryVersion,
        boolean deltasOnly,
        boolean clearDirtyBits) {

        assert(!deltasOnly || isBIN());

        final BIN bin = (isBIN() ? (BIN)this : null);

        final boolean haveExpiration;

        if (bin != null && entryVersion >= 12) {
            int base = LogUtils.readPackedInt(itemBuffer);
            haveExpiration = (base != -1);
            bin.setExpirationBase(base);
        } else {
            haveExpiration = false;
        }

        if (bin != null && entryVersion >= 19) {
            if (entryVersion >= LogEntryType.LOG_VERSION_CREATION_TIME) {
                long base = LogUtils.readPackedLong(itemBuffer);
                bin.setCreationTimesBase(base);
            }
            long base = LogUtils.readPackedLong(itemBuffer);
            bin.setModificationTimesBase(base);
            if (entryVersion < LogEntryType.LOG_VERSION_CREATION_TIME) {
                bin.setCreationTimesBase(base);
            }
        }

        nodeId = LogUtils.readPackedLong(itemBuffer);
        identifierKey = LogUtils.readByteArray(itemBuffer);

        final byte booleans = itemBuffer.get();

        setIsRootFlag((booleans & 1) != 0);

        if ((booleans & 2) != 0) {
            keyPrefix = LogUtils.readByteArray(itemBuffer);
        }

        final int maxCompactKeySize = (entryVersion >= 18) ?
            LogUtils.readPackedInt(itemBuffer) : -1;

        final boolean mayHaveLastLoggedSize = ((booleans & 4) != 0);
        assert !(mayHaveLastLoggedSize && (entryVersion < 9));

        final boolean hasBloomFilter = ((booleans & 8) != 0);
        assert(!hasBloomFilter || (entryVersion >= 10 && deltasOnly));

        final boolean haveVLSNCache = ((booleans & 16) != 0);
        assert !(haveVLSNCache && (entryVersion < 11));

        setExpirationInHours((booleans & 32) != 0);

        nEntries = LogUtils.readPackedInt(itemBuffer);
        level = LogUtils.readPackedInt(itemBuffer);

        final int length = LogUtils.readPackedInt(itemBuffer);

        entryTargets = INTargetRep.NONE;
        entryKeys = INKeyRep.create(length, maxCompactKeySize, envImpl);
        entryLsns = new long[length];
        entryStates = new byte[length];

        final boolean defunctCompactLsnsRep =
            (entryVersion > 1 && entryVersion < 18) &&
            LogUtils.readBoolean(itemBuffer);

        final long defunctBaseFileNumber =
            defunctCompactLsnsRep ? LogUtils.readInt(itemBuffer) : -1;

        for (int i = 0; i < nEntries; i++) {
            entryKeys = entryKeys.read(i, itemBuffer, this);

            final long lsn;
            if (defunctCompactLsnsRep) {
                /* LSNs in compact form that is no longer used. */
                byte fileNumberOffset = itemBuffer.get();
                int fileOffset = (itemBuffer.get() & 0xff);
                fileOffset |= ((itemBuffer.get() & 0xff) << 8);
                fileOffset |= ((itemBuffer.get() & 0xff) << 16);
                if (fileOffset == THREE_BYTE_NEGATIVE_ONE) {
                    lsn = DbLsn.NULL_LSN;
                } else {
                    lsn = DbLsn.makeLsn(
                        defunctBaseFileNumber + fileNumberOffset, fileOffset);
                }
            } else {
                /* LSNs in long form. */
                lsn = LogUtils.readLong(itemBuffer);
            }

            setLsnInternal(i, lsn);

            byte entryState = itemBuffer.get();

            if (clearDirtyBits) {
                entryState &= EntryStates.CLEAR_DIRTY_BIT;
            }

            /*
             * The old MIGRATE_BIT (0x4) was accidentally written in a pre-JE 6
             * log version.
             */
            if (entryVersion < 9) {
                entryState &= EntryStates.CLEAR_TRANSIENT_BITS;
            }

            /*
             * A NULL_LSN is the remnant of an incomplete insertion and the
             * KnownDeleted flag should be set.  But because of bugs in prior
             * releases, the KnownDeleted flag may not be set.  So set it here.
             * See Tree.insert.  [#13126]
             */
            if (entryVersion < 9 && lsn == DbLsn.NULL_LSN) {
                entryState |= EntryStates.KNOWN_DELETED_BIT;
            }

            entryStates[i] = entryState;

            if (mayHaveLastLoggedSize && !isEmbeddedLN(i)) {
                setLastLoggedSizeUnconditional(
                    i, LogUtils.readPackedInt(itemBuffer));
            }

            final boolean isEmbedded = isEmbeddedLN(i);

            if (haveVLSNCache && isEmbedded) {
                bin.setCachedVLSNUnconditional(
                    i, LogUtils.readPackedLong(itemBuffer));
            }

            if (haveExpiration) {
                bin.setExpirationOffset(i, LogUtils.readPackedInt(itemBuffer));
            }

            if (entryVersion >= 19 && isEmbedded) {
            	if (entryVersion >= LogEntryType.LOG_VERSION_CREATION_TIME) {
                    bin.setCreationTimeOffset(
                        i, LogUtils.readPackedLong(itemBuffer));
            	}
                bin.setModificationTimeOffset(
                    i, LogUtils.readPackedLong(itemBuffer));
                if (entryVersion < LogEntryType.LOG_VERSION_CREATION_TIME) {
                    bin.setCreationTimeOffset(
                        i , WriteOptions.CREATION_TIME_NOT_SET);
                }
            }
        }

        if (deltasOnly) {
            setBINDelta(true);

            if (entryVersion >= 10) {
                bin.setFullBinNEntries(LogUtils.readPackedInt(itemBuffer));
                bin.setFullBinMaxEntries(LogUtils.readPackedInt(itemBuffer));

                if (hasBloomFilter) {
                    bin.bloomFilter =
                        BINDeltaBloomFilter.readFromLog(itemBuffer);
                }
            }
        }
    }

    /**
     * @see Loggable#logicalEquals
     * Always return false, this item should never be compared.
     */
    @Override
    public final boolean logicalEquals(Loggable other) {
        return false;
    }

    /**
     * @see Loggable#dumpLog
     */
    @Override
    public final void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append(beginTag());

        sb.append("<nodeId val=\"");
        sb.append(nodeId);
        sb.append("\"/>");

        sb.append(Key.dumpString(identifierKey, "idKey", 0));

        // isRoot
        sb.append("<isRoot val=\"");
        sb.append(isRoot());
        sb.append("\"/>");

        // level
        sb.append("<level val=\"");
        sb.append(Integer.toHexString(level));
        sb.append("\"/>");

        if (keyPrefix != null) {
            sb.append(Key.dumpString(keyPrefix, "keyPrefix", 0));
        }

        // nEntries, length of entries array
        sb.append("<entries numEntries=\"");
        sb.append(nEntries);

        sb.append("\" length=\"");
        sb.append(getMaxEntries());

        final BIN bin = isBIN() ? (BIN) this : null;

        if (isBINDelta(false)) {
            sb.append("\" numFullBinEntries=\"");
            sb.append(bin.getFullBinNEntries());
            sb.append("\" maxFullBinEntries=\"");
            sb.append(bin.getFullBinMaxEntries());
        }

        if (bin != null) {
            sb.append("\" baseExpiration=\"");
            sb.append(bin.getExpirationBase());
            sb.append("\" expirationIn=\"");
            sb.append(bin.isExpirationInHours() ? "hours" : "days");
        }
        if (bin != null && bin.getCreationTimesBase() != -1) {
            sb.append("\" baseCreateTime=\"");
            sb.append(bin.getCreationTimesBase());
        }
        if (bin != null && bin.getModificationTimesBase() != -1) {
            sb.append("\" baseModTime=\"");
            sb.append(bin.getModificationTimesBase());
        }
        sb.append("\">");

        if (verbose) {
            for (int i = 0; i < nEntries; i++) {
                sb.append("<ref");
                dumpSlotState(sb, i, bin);
                sb.append(">");
                sb.append(Key.dumpString(getKey(i), 0));
                if (isEmbeddedLN(i)) {
                    sb.append(Key.dumpString(getEmbeddedData(i), "data", 0));
                }
                sb.append(DbLsn.toString(getLsn(i)));
                sb.append("</ref>");
            }
        }

        sb.append("</entries>");

        if (isBINDelta(false)) {
            if (bin.bloomFilter != null) {
                BINDeltaBloomFilter.dumpLog(bin.bloomFilter, sb, verbose);
            }
        }

        /* Add on any additional items from subclasses before the end tag. */
        dumpLogAdditional(sb);

        sb.append(endTag());
    }

    /**
     * Allows subclasses to add additional fields before the end tag. If they
     * just overload dumpLog, the xml isn't nested.
     */
    protected void dumpLogAdditional(StringBuilder sb) {
    }

    public String beginTag() {
        return BEGIN_TAG;
    }

    public String endTag() {
        return END_TAG;
    }

    /**
     * For unit test support:
     * @return a string that dumps information about this IN, without
     */
    @Override
    public String dumpString(int nSpaces, boolean dumpTags) {
        StringBuilder sb = new StringBuilder();
        if (dumpTags) {
            sb.append(TreeUtils.indent(nSpaces));
            sb.append(beginTag());
            sb.append('\n');
        }

        if (dumpTags) {
            sb.append(TreeUtils.indent(nSpaces));
            sb.append("<nodeId val=\"");
            sb.append(nodeId);
            sb.append("\"/>");
        } else {
            sb.append(nodeId);
        }
        sb.append('\n');

        BIN bin = null;
        if (isBIN()) {
            bin = (BIN) this;
        }

        sb.append(TreeUtils.indent(nSpaces + 2));
        sb.append("<idkey>");
        sb.append(identifierKey == null ?
                  "" :
                  Key.dumpString(identifierKey, 0));
        sb.append("</idkey>");
        sb.append('\n');
        sb.append(TreeUtils.indent(nSpaces + 2));
        sb.append("<prefix>");
        sb.append(keyPrefix == null ? "" : Key.dumpString(keyPrefix, 0));
        sb.append("</prefix>\n");
        sb.append(TreeUtils.indent(nSpaces + 2));
        sb.append("<dirty val=\"").append(getDirty()).append("\"/>");
        sb.append('\n');
        sb.append(TreeUtils.indent(nSpaces + 2));
        sb.append("<level val=\"");
        sb.append(Integer.toHexString(level)).append("\"/>");
        sb.append('\n');
        sb.append(TreeUtils.indent(nSpaces + 2));
        sb.append("<isRoot val=\"").append(isRoot()).append("\"/>");
        sb.append('\n');
        sb.append(TreeUtils.indent(nSpaces + 2));
        sb.append("<isBINDelta val=\"").append(isBINDelta(false)).append("\"/>");
        sb.append(TreeUtils.indent(nSpaces + 2));
        sb.append(
            "<prohibitNextDelta val=\"").
            append(getProhibitNextDelta()).append("\"/>");
        if (bin != null) {
            sb.append(TreeUtils.indent(nSpaces + 2));
            sb.append("<cursors val=\"").append(bin.nCursors()).append("\"/>");
            sb.append(TreeUtils.indent(nSpaces + 2));
            sb.append("<deltas val=\"").append(bin.getNDeltas()).append("\"/>");
        }
        sb.append('\n');

        sb.append(TreeUtils.indent(nSpaces + 2));
        sb.append("<entries nEntries=\"");
        sb.append(nEntries);
        sb.append("\">");
        sb.append('\n');

        for (int i = 0; i < nEntries; i++) {
            sb.append(TreeUtils.indent(nSpaces + 4));
            sb.append("<entry id=\"").append(i).append("\"");
            dumpSlotState(sb, i, bin);
            sb.append(">\n");
            if (getLsn(i) == DbLsn.NULL_LSN) {
                sb.append(TreeUtils.indent(nSpaces + 6));
                sb.append("<lsn/>");
            } else {
                sb.append(DbLsn.dumpString(getLsn(i), nSpaces + 6));
            }
            sb.append('\n');
            if (entryKeys.get(i) == null) {
                sb.append(TreeUtils.indent(nSpaces + 6));
                sb.append("<key/>");
            } else {
                sb.append(Key.dumpString(entryKeys.get(i), (nSpaces + 6)));
            }
            sb.append('\n');
            if (entryTargets.get(i) == null) {
                sb.append(TreeUtils.indent(nSpaces + 6));
                sb.append("<target/>");
            } else {
                sb.append(entryTargets.get(i).dumpString(nSpaces + 6, true));
            }
            sb.append('\n');
            sb.append(TreeUtils.indent(nSpaces + 4));
            sb.append("</entry>");
            sb.append('\n');
        }

        sb.append(TreeUtils.indent(nSpaces + 2));
        sb.append("</entries>");
        sb.append('\n');
        if (dumpTags) {
            sb.append(TreeUtils.indent(nSpaces));
            sb.append(endTag());
        }
        return sb.toString();
    }

    private void dumpSlotState(StringBuilder sb, int i, BIN bin) {
        sb.append(" kd=\"").append(isEntryKnownDeleted(i));
        sb.append("\" pd=\"").append(isEntryPendingDeleted(i));
        sb.append("\" dirty=\"").append(isDirty(i));
        sb.append("\" embedded=\"").append(isEmbeddedLN(i));
        sb.append("\" noData=\"").append(isNoDataLN(i));
        sb.append("\" logSize=\"");
        sb.append(getLastLoggedSizeUnconditional(i));
        if (bin != null) {
            long vlsn = bin.getCachedVLSN(i);
            if (!VLSN.isNull(vlsn)) {
                sb.append("\" vlsn=\"").append(vlsn);
            }
            sb.append("\" expires=\"");
            if (bin.getExpiration(i) != 0) {
                sb.append(TTL.formatExpiration(
                    bin.getExpiration(i), bin.isExpirationInHours()));
                sb.append(" val:").append(bin.getExpiration(i));
                sb.append(" off:").append(bin.getExpirationOffset(i));
            }
            sb.append("\" tombstone=\"").append(bin.isTombstone(i));
            long createTime = bin.getCreationTime(i);
            if (createTime != 0) {
                sb.append("\" creationTime=\"")
                  .append(StatUtils.getDate(createTime));
            }
            long modTime = bin.getModificationTime(i);
            if (modTime != 0) {
                sb.append("\" modTime=\"").append(StatUtils.getDate(modTime));
            }
        }
        sb.append("\"");
    }

    /**
     * Converts to an identifying string that is safe to output in a log.
     * Keys are excluded for security/privacy reasons unless
     * {@link EnvironmentImpl#getExposeUserData()} is true.
     */
    public String toSafeString(final int... slotIndexes) {

        final boolean exposeKeys = getEnv().getExposeUserData();
        final BIN bin = isBIN() ? (BIN) this : null;
        final StringBuilder sb = new StringBuilder();

        sb.append("IN nodeId=").append(getNodeId());
        sb.append(" lastLoggedLSN=");
        sb.append(DbLsn.getNoFormatString(getLastLoggedLsn()));
        sb.append(" lastFulLSN=");
        sb.append(DbLsn.getNoFormatString(getLastFullLsn()));
        sb.append(" level=").append(Integer.toHexString(getLevel()));
        sb.append(" flags=").append(Integer.toHexString(flags));
        sb.append(" isBINDelta=").append(isBINDelta());
        sb.append(" nSlots=").append(getNEntries());

        if (slotIndexes != null) {
            for (final int i : slotIndexes) {
                sb.append(" slot-").append(i).append(":[");
                sb.append("lsn=");
                sb.append(DbLsn.getNoFormatString(getLsn(i)));
                sb.append(" offset=");
                sb.append(DbLsn.getFileOffset(getLsn(i)));
                sb.append(" offset+logSize=");
                sb.append(DbLsn.getFileOffset(getLsn(i)) +
                    getLastLoggedSizeUnconditional(i));
                if (exposeKeys) {
                    sb.append(" key='");
                    sb.append(Key.getNoFormatString(entryKeys.get(i)));
                    sb.append("'");
                }
                dumpSlotState(sb, i, bin);
                sb.append("]");
            }
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return dumpString(0, true);
    }

    public String shortClassName() {
        return "IN";
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    private void traceSplit(Level level,
                            IN parent,
                            IN newSibling,
                            long parentLsn,
                            long myNewLsn,
                            long newSiblingLsn,
                            int splitIndex,
                            int idKeyIndex,
                            int childIndex) {
        Logger logger = getEnv().getLogger();
        if (logger.isLoggable(level)) {
            StringBuilder sb = new StringBuilder();
            sb.append(TRACE_SPLIT);
            sb.append(" parent=");
            sb.append(parent.getNodeId());
            sb.append(" child=");
            sb.append(getNodeId());
            sb.append(" newSibling=");
            sb.append(newSibling.getNodeId());
            sb.append(" parentLsn = ");
            sb.append(DbLsn.getNoFormatString(parentLsn));
            sb.append(" childLsn = ");
            sb.append(DbLsn.getNoFormatString(myNewLsn));
            sb.append(" newSiblingLsn = ");
            sb.append(DbLsn.getNoFormatString(newSiblingLsn));
            sb.append(" splitIdx=");
            sb.append(splitIndex);
            sb.append(" idKeyIdx=");
            sb.append(idKeyIndex);
            sb.append(" childIdx=");
            sb.append(childIndex);
            LoggerUtils.logMsg(logger,
                               databaseImpl.getEnv(),
                               level,
                               sb.toString());
        }
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    private void traceDelete(Level level, int index) {
        Logger logger = databaseImpl.getEnv().getLogger();
        if (logger.isLoggable(level)) {
            StringBuilder sb = new StringBuilder();
            sb.append(TRACE_DELETE);
            sb.append(" in=").append(getNodeId());
            sb.append(" index=");
            sb.append(index);
            LoggerUtils.logMsg(logger,
                               databaseImpl.getEnv(),
                               level,
                               sb.toString());
        }
    }

    public final void setFetchINHook(TestHook<?> hook) {
        fetchINHook = hook;
    }
}
