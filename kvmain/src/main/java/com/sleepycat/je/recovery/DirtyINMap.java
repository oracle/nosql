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

package com.sleepycat.je.recovery;

import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Level;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.log.Provisional;
import com.sleepycat.je.recovery.Checkpointer.CheckpointReference;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.MapLN;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 * Manages the by-level map of checkpoint references that are to be flushed by
 * a checkpoint or Database.sync, the MapLNs to be flushed, the highest level
 * by database to be flushed, and the state of the checkpoint.
 *
 * An single instance of this class is used for checkpoints and has the same
 * lifetime as the checkpointer and environment.  An instance per Database.sync
 * is created as needed.  Only one checkpoint can occur at a time, but multiple
 * syncs may occur concurrently with each other and with the checkpoint.
 *
 * The methods in this class are synchronized to protect internal state from
 * concurrent access by the checkpointer, compression, splits and eviction, and
 * to coordinate state changes between them.  See coordinateXxxWithCheckpoint
 * methods.
 *
 * When INs are latched along with synchronization on a DirtyINMap, the order
 * must be: 1) IN latches and 2) synchronize on DirtyINMap.  For example,
 * the evictor latches the parent and child IN before calling the synchronized
 * method coordinateEvictionWithCheckpoint, and selectDirtyINsForCheckpoint
 * latches the IN before calling the synchronized method selectForCheckpoint.
 */
class DirtyINMap {

    static final boolean DIRTY_SET_DEBUG_TRACE = false;

    private final EnvironmentImpl envImpl;
    private final SortedMap<Integer, Map<Long, CheckpointReference>> levelMap;
    private int numEntries;
    private final Set<DatabaseId> mapLNsToFlush;
    private final Map<DatabaseImpl, Integer> highestFlushLevels;

    enum CkptState {
        /** No checkpoint in progress, or is used for Database.sync. */
        NONE,
        /** Checkpoint started but dirty map is not yet complete. */
        DIRTY_MAP_INCOMPLETE,
        /** Checkpoint in progress and dirty map is complete. */
        DIRTY_MAP_COMPLETE,
    }

    private CkptState ckptState;
    private boolean ckptFlushAll;
    private boolean ckptFlushExtraLevel;

    DirtyINMap(EnvironmentImpl envImpl) {
        this.envImpl = envImpl;
        levelMap = new TreeMap<>();
        numEntries = 0;
        mapLNsToFlush = new HashSet<>();
        highestFlushLevels = new IdentityHashMap<>();
        ckptState = CkptState.NONE;
    }

    /**
     * Coordinates an eviction with an in-progress checkpoint and returns
     * whether or not provisional logging is needed.
     *
     * @return the provisional status to use for logging the target.
     */
    synchronized Provisional coordinateEvictionWithCheckpoint(
        final DatabaseImpl db,
        final int targetLevel,
        final IN parent) {

        /*
         * If the checkpoint is in-progress and has not finished dirty map
         * construction, we must add the parent to the dirty map.  That way the
         * dirtiness and logging will cascade up in the same way as if the
         * target were not evicted, and instead were encountered during dirty
         * map construction.  We don't want the evictor's actions to introduce
         * an IN in the log that has not cascaded up properly.
         *
         * Note that we add the parent even if it is not dirty here.  It will
         * become dirty after the target child is logged, but that hasn't
         * happened yet.
         *
         * We do not add the parent if it is null, which is the case when the
         * root is being evicted.
         */
        if (ckptState == CkptState.DIRTY_MAP_INCOMPLETE &&
            parent != null) {

            /* Add latched parent IN to dirty map. */
            selectForCheckpoint(parent);

            /* Save dirty DBs for later. */
            saveMapLNsToFlush(parent);
        }

        /*
         * The evictor has to log provisionally in two cases:
         *
         * 1 - The checkpoint is in-progress and has not finished dirty map
         *     construction, and the target is not the root. The parent IN has
         *     been added to the dirty map, so we know the child IN is at a
         *     level below the max flush level.
         */
        if (ckptState == CkptState.DIRTY_MAP_INCOMPLETE &&
            parent != null) {
            return Provisional.YES;
        }

        /*
         * 2 - The checkpoint is in-progress and has finished dirty map
         *     construction, and is at a level above the eviction target.
         */
        if (ckptState == CkptState.DIRTY_MAP_COMPLETE &&
            targetLevel < getHighestFlushLevel(db)) {
            return Provisional.YES;
        }

        /*
         * Otherwise, log non-provisionally if splits are logged, and
         * provisionally otherwise.
         *
         * TODO: When LOG_SPLITS is removed, if splits are never logged then
         *  a lot of the above logic can be removed because YES will always
         *  be returned. The Provisional value need not be returned at all.
         */
        return Tree.LOG_SPLITS ? Provisional.NO : Provisional.YES;
    }

    /**
     * Coordinates a split with an in-progress checkpoint.
     *
     * TODO:
     * Is it necessary to perform MapLN flushing for nodes logged by a split
     * (and not just the new sibling)?
     *
     * @param newSibling the sibling IN created by the split.
     */
    void coordinateSplitWithCheckpoint(final IN newSibling) {

        assert newSibling.isLatchExclusiveOwner();

        /*
         * If the checkpoint is in-progress and has not finished dirty map
         * construction, we must add the BIN children of the new sibling to the
         * dirty map. The new sibling will be added to the INList but it may or
         * may not be seen by the in-progress INList iteration, and we must
         * ensure that its dirty BIN children are logged by the checkpoint.
         *
         * Note that we cannot synchronize on 'this' before calling
         * selectDirtyBINChildrenForCheckpoint, since it latches BIN children.
         * IN latching must come before synchronization on 'this'. Eventually
         * after latching the IN, selectForCheckpoint is called , which  is
         * synchronized and checks for ckptState == DIRTY_MAP_INCOMPLETE.
         */
        selectDirtyBINChildrenForCheckpoint(newSibling);
    }

    /**
     * Coordinates compression pruning with an in-progress checkpoint.
     * <p>
     * When compression pruning occurs during/after dirty IN map construction
     * and before flushing is complete, the parent of the pruned branch must be
     * flushed by the checkpoint. This is critical because:
     * <pre>
     * 1. The pruned branch itself will not be flushed by the checkpoint since
     *    it is no longer present in the tree, and the branch parent may not
     *    have been placed in the dirty map when it was originally constructed.
     * 2. The action that caused the compression (LN deletion or extinction
     *    scan) may have been logged prior to CkptStart and therefore will not
     *    be replayed by recovery, if we crash after CkptEnd.
     * </pre>
     * How do we know that pruning, resulting from a deletion/extinction that
     * was logged prior to CkptStart, is made durable by the checkpoint?
     * <pre>
     * - If pruning occurs early enough, the branch parent will be placed in
     *   the dirty IN map when it is originally constructed, and it will be
     *   flushed as usual by the checkpoint.
     * - If pruning occurs too late for the branch parent to be included in the
     *   dirty IN map initially, but prior to flushing all INs at the same
     *   level, then it will be added to the map by this method and then
     *   flushed by the checkpoint.
     * - If pruning occurs after all INs at the same level have been flushed,
     *   then the pruned branch (with empty BINs) has already been flushed by
     *   the checkpoint. If we crash and the branch parent is not made durable
     *   by the next checkpoint, then pruning will be done opportunistically
     *   after recovery. Lazy compression is performed by recovery and after
     *   loading INs into cache.
     * </pre>
     * See [KVSTORE-642], ExtinctionTest.testCompressDeletedDuringCheckpoint
     * and ExtinctionTest.testCompressExtinctDuringCheckpoint.
     *
     * @param branchParent the node from which the pruned branch was deleted.
     */
    synchronized void coordinatePruningWithCheckpoint(final IN branchParent) {

        assert branchParent.isLatchExclusiveOwner();
        assert branchParent.getDirty();

        /*
         * Add the node to the dirty map even if dirty map construction is
         * complete, updating the max flush level if necessary.
         */
        if (ckptState != CkptState.NONE) {
            selectUnconditionally(branchParent);
        }
    }

    /**
     * Must be called before starting a checkpoint, and must not be called for
     * Database.sync.  Updates memory budget and sets checkpoint state.
     */
    synchronized void beginCheckpoint(boolean flushAll,
                                      boolean flushExtraLevel) {
        assert levelMap.isEmpty();
        assert mapLNsToFlush.isEmpty();
        assert highestFlushLevels.isEmpty();
        assert numEntries == 0;
        assert ckptState == CkptState.NONE;
        ckptState = CkptState.DIRTY_MAP_INCOMPLETE;
        ckptFlushAll = flushAll;
        ckptFlushExtraLevel = flushExtraLevel;
    }

    /**
     * Must be called after a checkpoint or Database.sync is complete.  Updates
     * memory budget and clears checkpoint state.
     */
    synchronized void reset() {
        levelMap.clear();
        mapLNsToFlush.clear();
        highestFlushLevels.clear();
        numEntries = 0;
        ckptState = CkptState.NONE;
    }

    /**
     * Scan the INList for all dirty INs.  Save them in a tree-level ordered
     * map for level ordered flushing.
     *
     * Take this opportunity to recalculate the memory budget tree usage.
     *
     * This method itself is not synchronized to allow concurrent eviction.
     * Synchronization is performed on a per-IN basis to protect the data
     * structures here, and eviction can occur in between INs.
     */
    void selectDirtyINsForCheckpoint() {
        assert ckptState == CkptState.DIRTY_MAP_INCOMPLETE;

        /*
         * Opportunistically recalculate the INList memory budget while
         * traversing the entire INList.
         */
        final INList inMemINs = envImpl.getInMemoryINs();
        inMemINs.memRecalcBegin();

        boolean completed = false;
        try {
            for (IN in : inMemINs) {
                in.latchShared(CacheMode.UNCHANGED);
                try {
                    if (!in.getInListResident()) {
                        continue;
                    }

                    inMemINs.memRecalcIterate(in);

                    /* Add dirty UIN to dirty map. */
                    if (in.getDirty() && !in.isBIN()) {
                        selectForCheckpoint(in);
                    }

                    /* Add dirty level 2 children to dirty map. */
                    selectDirtyBINChildrenForCheckpoint(in);

                    /* Save dirty DBs for later. */
                    saveMapLNsToFlush(in);
                } finally {
                    in.releaseLatch();
                }

                /* Call test hook after releasing latch. */
                TestHookExecute.doHookIfSet(
                    Checkpointer.examineINForCheckpointHook, in);
            }
            completed = true;
        } finally {
            inMemINs.memRecalcEnd(completed);
        }

        /*
         * Finish filling out the highestFlushLevels map. For each entry in
         * highestFlushLevels that has a null level Integer value (set by
         * selectForCheckpoint), we call DbTree.getHighestLevel and replace the
         * null level. We must call DbTree.getHighestLevel, which latches the
         * root, only when not synchronized, to avoid breaking the
         * synchronization rules described in the class comment.  This must be
         * done in several steps to follow the synchronization rules, yet
         * protect the highestFlushLevels using synchronization.
         */
        final Map<DatabaseImpl, Integer> maxFlushDbs = new HashMap<>();

        /* Copy entries with a null level. */
        synchronized (this) {
            for (DatabaseImpl db : highestFlushLevels.keySet()) {

                if (highestFlushLevels.get(db) == null) {
                    maxFlushDbs.put(db, null);
                }
            }
        }

        /* Call getHighestLevel without synchronization. */
        final DbTree dbTree = envImpl.getDbTree();

        for (Map.Entry<DatabaseImpl, Integer> entry : maxFlushDbs.entrySet()) {

            entry.setValue(dbTree.getHighestLevel(entry.getKey()));
        }

        /* Fill in levels in highestFlushLevels. */
        synchronized (this) {

            for (Map.Entry<DatabaseImpl, Integer> entry :
                 maxFlushDbs.entrySet()) {

                highestFlushLevels.put(entry.getKey(), entry.getValue());
            }
        }

        /* Complete this phase of the checkpoint. */
        synchronized (this) {
            ckptState = CkptState.DIRTY_MAP_COMPLETE;
        }

        if (DIRTY_SET_DEBUG_TRACE) {
            traceDirtySet();
        }
    }

    /**
     * Add the IN to the dirty map if dirty map construction is in progress.
     * If added, the highest flush level map is also updated.
     */
    private synchronized void selectForCheckpoint(final IN in) {

        /*
         * Must check state while synchronized. The state may not be
         * DIRTY_MAP_INCOMPLETE when called from eviction or a split.
         */
        if (ckptState != CkptState.DIRTY_MAP_INCOMPLETE) {
            return;
        }

        selectUnconditionally(in);
    }

    /**
     * Add the IN to the dirty map regardless of the state of dirty map
     * construction. If added, the highest flush level map is also updated.
     */
    private synchronized void selectUnconditionally(final IN in) {
        addIN(in, true /*updateFlushLevels*/);
    }

    /**
     * Adds the the dirty child BINs of the 'in' if dirty map construction is
     * in progress.
     *
     * Resident BINs are added when their parent is encountered in
     * the INList iteration, rather than when the BIN is encountered in the
     * iteration. This was originally done to account for off-heap BINs, but
     * was left in place (when the off-heap cache was removed) since we've come
     * to rely on the algorithm over time and it would be risky to change.
     *
     * Note that this method is not synchronized because it latches the BIN
     * children. IN latching must come before synchronizing on 'this'. The
     * selectForCheckpoint method, which is called after latching the BIN, is
     * synchronized.
     */
    private void selectDirtyBINChildrenForCheckpoint(final IN in) {

        if (in.getNormalizedLevel() != 2) {
            return;
        }

        for (int i = 0; i < in.getNEntries(); i += 1) {

            final IN bin = (IN) in.getTarget(i);

            if (bin != null) {

                /* When called via split a child may already be latched. */
                final boolean latchBinHere = !bin.isLatchOwner();

                if (latchBinHere) {
                    bin.latchShared(CacheMode.UNCHANGED);
                }

                try {
                    if (bin.getDirty()) {
                        selectForCheckpoint(bin);
                    }
                } finally {
                    if (latchBinHere) {
                        bin.releaseLatch();
                    }
                }
            }
        }
    }

    private void updateFlushLevels(Integer level,
                                   final DatabaseImpl db,
                                   final boolean isBIN,
                                   final boolean isRoot) {

        /*
         * IN was added to the dirty map.  Update the highest level seen
         * for the database.  Use one level higher when ckptFlushExtraLevel
         * is set.  When ckptFlushAll is set, use the maximum level for the
         * database.
         *
         * Always flush at least one level above the bottom-most BIN level so
         * that the BIN level is logged provisionally and the expense of
         * processing BINs during recovery is avoided.
         */
        if (ckptFlushAll) {
            if (!highestFlushLevels.containsKey(db)) {

                /*
                 * Null is used as an indicator that getHighestLevel should be
                 * called in selectDirtyINsForCheckpoint, when not
                 * synchronized.
                 */
                highestFlushLevels.put(db, null);
            }
        } else {
            if ((ckptFlushExtraLevel || isBIN) && !isRoot) {
                /* Next level up in the same tree. */
                level += 1;
            }

            final Integer highestLevelSeen = highestFlushLevels.get(db);

            if (highestLevelSeen == null || level > highestLevelSeen) {
                highestFlushLevels.put(db, level);
            }
        }
    }

    /**
     * Scan the INList for all dirty INs for a given database.  Arrange them in
     * level sorted map for level ordered flushing.
     *
     * This method is not synchronized to allow concurrent eviction.
     * Coordination between eviction and Database.sync is not required.
     */
    void selectDirtyINsForDbSync(DatabaseImpl dbImpl) {

        assert ckptState == CkptState.NONE;

        final DatabaseId dbId = dbImpl.getId();

        for (IN in : envImpl.getInMemoryINs()) {
            if (in.getDatabaseId().equals(dbId)) {
                in.latch(CacheMode.UNCHANGED);
                try {
                    if (in.getInListResident() && in.getDirty()) {
                        addIN(in, false /*updateFlushLevels*/);
                    }
                } finally {
                    in.releaseLatch();
                }
            }
        }

        /*
         * Create a single entry map that forces all levels of this DB to
         * be flushed.
         */
        highestFlushLevels.put(
            dbImpl, envImpl.getDbTree().getHighestLevel(dbImpl));
    }

    synchronized int getHighestFlushLevel(DatabaseImpl db) {

        assert ckptState != CkptState.DIRTY_MAP_INCOMPLETE;

        /*
         * This method is only called while flushing dirty nodes for a
         * checkpoint or Database.sync, not for an eviction, so an entry for
         * this database should normally exist.  However, if the DB root (and
         * DatabaseImpl) have been evicted since the highestFlushLevels was
         * constructed, the new DatabaseImpl instance will not be present in
         * the map.  In this case, we do not need to checkpoint the IN and
         * eviction should be non-provisional.
         */
        Integer val = highestFlushLevels.get(db);
        return (val != null) ? val : IN.MIN_LEVEL;
    }

    synchronized int getNumLevels() {
        return levelMap.size();
    }

    /**
     * Add a node unconditionally to the dirty map.
     *
     * @param in is the IN to add.
     *
     * @param updateMemoryBudget if true then update the memory budget as the
     * map is changed; if false then addCostToMemoryBudget must be called
     * later.
     */
    synchronized void addIN(final IN in, final boolean updateFlushLevels) {
        final Integer level = in.getLevel();
        final long lsn = in.getLastLoggedLsn();
        final long nodeId = in.getNodeId();
        final boolean isRoot = in.isRoot();
        final boolean isBin = in.isBIN();

        Map<Long, CheckpointReference> lsnMap = levelMap.get(level);
        if (lsnMap == null) {
            /*
             * We use TreeMap rather than HashMap because HashMap.iterator() is
             * a slow way of getting the first element (see removeNextNode).
             */
            lsnMap = new TreeMap<>();
            levelMap.put(level, lsnMap);
        }

        final DatabaseImpl db = in.getDatabase();

        /*
         * TODO Use the zero'th slot rather than the idKey here. We would like
         * to phase out the use of the identifier key, since it is not updated
         * immediately to reflect changes in the IN slots. However, in this
         * case getKey(0) would add significantly to the amount of memory used
         * in the dirty map. getKey normally allocates a new byte array to
         * combine the key prefix and suffix. A future solution is to store the
         * IN itself in the dirty map and use latchParent when logging.
         */
        final CheckpointReference ref = new CheckpointReference(
            db.getId(), nodeId, level, isRoot, in.getIdentifierKey(), lsn);

        final boolean added;

        assert lsn != DbLsn.NULL_LSN;
        added = lsnMap.put(lsn, ref) == null;

        if (!added) {
            return;
        }

        numEntries++;

        if (updateFlushLevels) {
            updateFlushLevels(level, db, isBin, isRoot);
        }
    }

    /**
     * Get the lowest level currently stored in the map.
     */
    synchronized Integer getLowestLevelSet() {
        return levelMap.firstKey();
    }

    /**
     * Removes the set corresponding to the given level.
     */
    synchronized void removeLevel(Integer level) {
        levelMap.remove(level);
    }

    synchronized CheckpointReference removeNode(final int level,
                                                final long lsn) {

        if (lsn == DbLsn.NULL_LSN) {
            return null;
        }

        final Map<Long, CheckpointReference> lsnMap = levelMap.get(level);
        if (lsnMap == null) {
            return null;
        }

        return lsnMap.remove(lsn);
    }

    synchronized CheckpointReference removeNextNode(Integer level) {

        final Map<Long, CheckpointReference> lsnMap = levelMap.get(level);

        if (lsnMap == null || lsnMap.isEmpty()) {
            return null;
        }

        final Iterator<Map.Entry<Long, CheckpointReference>> iter =
            lsnMap.entrySet().iterator();

        assert iter.hasNext();
        final CheckpointReference ref = iter.next().getValue();
        iter.remove();
        return ref;
    }

    /**
     * If the given IN is a BIN for the ID mapping database, saves all
     * dirty/temp MapLNs contained in it.
     */
    private synchronized void saveMapLNsToFlush(IN in) {

        if (in.isBIN() &&
            in.getDatabase().getId().equals(DbTree.ID_DB_ID)) {

            for (int i = 0; i < in.getNEntries(); i += 1) {
                final MapLN ln = (MapLN) in.getTarget(i);

                if (ln != null && ln.getDatabase().isCheckpointNeeded()) {
                    mapLNsToFlush.add(ln.getDatabase().getId());
                }
            }
        }
    }

    /**
     * Flushes all saved dirty/temp MapLNs and clears the saved set.
     *
     * <p>If dirty, a MapLN must be flushed at each checkpoint to record
     * updated utilization info in the checkpoint interval.</p>
     *
     * This method is not synchronized because it takes the Btree root latch,
     * and we must never latch something in the Btree after synchronizing on
     * DirtyINMap; see class comments.  Special synchronization is performed
     * for accessing internal state; see below.
     *
     * @param checkpointStart start LSN of the checkpoint in progress.  To
     * reduce unnecessary logging, the MapLN is only flushed if it has not been
     * written since that LSN.
     */
    void flushMapLNs(long checkpointStart) {

        /*
         * This method is called only while flushing dirty nodes for a
         * checkpoint or Database.sync, not for an eviction, and mapLNsToFlush
         * is not changed during the flushing phase.  So we don't strictly need
         * to synchronize while accessing mapLNsToFlush.  However, for
         * consistency and extra safety we always synchronize while accessing
         * internal state.
         */
        final Set<DatabaseId> mapLNsCopy;

        synchronized (this) {
            assert ckptState != CkptState.DIRTY_MAP_INCOMPLETE;

            if (mapLNsToFlush.isEmpty()) {
                mapLNsCopy = null;
            } else {
                mapLNsCopy = new HashSet<>(mapLNsToFlush);
                mapLNsToFlush.clear();
            }
        }

        if (mapLNsCopy != null) {
            final DbTree dbTree = envImpl.getDbTree();

            for (DatabaseId dbId : mapLNsCopy) {
                envImpl.checkDiskLimitViolation();
                final DatabaseImpl db = dbTree.getDb(dbId);
                try {
                    if (db != null && db.isCheckpointNeeded()) {

                        dbTree.modifyDbRoot(
                            db, checkpointStart /*ifBeforeLsn*/,
                            true /*mustExist*/);
                    }
                } finally {
                    dbTree.releaseDb(db);
                }
            }
        }
    }

    /**
     * Flushes the DB mapping tree root at the end of the checkpoint, if either
     * mapping DB is dirty and the root was not flushed previously during the
     * checkpoint.
     *
     * This method is not synchronized because it does not access internal
     * state.  Also, it takes the DbTree root latch and although this latch
     * should never be held by eviction, for consistency we should not latch
     * something related to the Btree after synchronizing on DirtyINMap; see
     * class comments.
     *
     * @param checkpointStart start LSN of the checkpoint in progress.  To
     * reduce unnecessary logging, the Root is only flushed if it has not been
     * written since that LSN.
     */
    void flushRoot(long checkpointStart) {

        final DbTree dbTree = envImpl.getDbTree();

        if (dbTree.getDb(DbTree.ID_DB_ID).isCheckpointNeeded() ||
            dbTree.getDb(DbTree.NAME_DB_ID).isCheckpointNeeded()) {

            envImpl.logMapTreeRoot(checkpointStart);
        }
    }

    synchronized int getNumEntries() {
        return numEntries;
    }

    private void traceDirtySet() {
        assert DIRTY_SET_DEBUG_TRACE;

        final StringBuilder sb = new StringBuilder();
        sb.append("Ckpt dirty set");

        for (final Integer level : levelMap.keySet()) {

            final Map<Long, CheckpointReference> lsnMap =
                levelMap.get(level);

            sb.append("\nlevel = 0x").append(Integer.toHexString(level));
            sb.append(" lsnMap = ");
            sb.append((lsnMap != null) ? lsnMap.size() : 0);
        }

        sb.append("\ndbId:highestFlushLevel");

        for (final DatabaseImpl db : highestFlushLevels.keySet()) {
            sb.append(' ').append(db.getId()).append(':');
            sb.append(highestFlushLevels.get(db) & IN.LEVEL_MASK);
        }

        LoggerUtils.logMsg(
            envImpl.getLogger(), envImpl, Level.INFO, sb.toString());
    }
}
