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

package com.sleepycat.je.dbi;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.log.ErasedException;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.WholeEntry;
import com.sleepycat.je.log.entry.BINDeltaLogEntry;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.SizeofMarker;

/**
 * SortedLSNTreeWalker uses ordered disk access rather than random access to
 * iterate over a database tree. Faulting in data records by on-disk order can
 * provide much improved performance over faulting in by key order, since the
 * latter may require random access.  SortedLSN walking does not obey cursor
 * and locking constraints, and therefore can only be guaranteed consistent for
 * a quiescent tree which is not being modified by user or daemon threads.
 *
 * The class walks over the tree using sorted LSN fetching for parts of the
 * tree that are not in memory. It returns LSNs for each node in the tree,
 * <b>except</b> the root IN, in an arbitrary order (i.e. not key
 * order). The caller is responsible for getting the root IN's LSN explicitly.
 * <p>
 * A callback function specified in the constructor is executed for each LSN
 * found.
 * <p>
 * The walker works in two phases.  The first phase is to gather and return all
 * the resident INs using the roots that were specified when the SLTW was
 * constructed.  For each child of each root, if the child is resident it is
 * passed to the callback method (processLSN).  If the child was not in memory,
 * it is added to a list of LSNs to read.  When all of the in-memory INs have
 * been passed to the callback for all LSNs collected, phase 1 is complete.
 * <p>
 * In phase 2, for each of the sorted LSNs, the target is fetched, the type
 * determined, and the LSN and type passed to the callback method for
 * processing.  LSNs of the children of those nodes are retrieved and the
 * process repeated until there are no more nodes to be fetched for this
 * database's tree.  LSNs are accumulated in batches in this phase so that
 * memory consumption is not excessive.  For instance, if batches were not used
 * then the LSNs of all of the BINs would need to be held in memory.
 */
public class SortedLSNTreeWalker {

    /*
     * The interface for calling back to the user with each LSN.
     */
    public interface TreeNodeProcessor {
        void processLSN(long childLSN,
                        LogEntryType childType,
                        Node theNode,
                        byte[] lnKey,
                        int lastLoggedSize,
                        boolean isEmbedded)
            throws FileNotFoundException;

        /* Called when the internal memory limit is exceeded. */
        void noteMemoryExceeded();
    }

    /*
     * Optionally passed to the SortedLSNTreeWalker to be called when an
     * exception occurs.
     */
    interface ExceptionPredicate {
        /* Return true if the exception can be ignored. */
        boolean ignoreException(Exception e);
    }

    final DatabaseImpl[] dbImpls;
    protected final EnvironmentImpl envImpl;

    /*
     * Save the root LSN at construction time, because the root may be
     * nulled out before walk() executes.
     */
    private final long[] rootLsns;

    /* The limit on memory to be used for internal structures during SLTW. */
    private long internalMemoryLimit = Long.MAX_VALUE;

    /* The current memory usage by internal SLTW structures. */
    private long internalMemoryUsage;

    private final TreeNodeProcessor callback;

    /*
     * If true, then walker should fetch LNs and pass them to the
     * TreeNodeProcessor callback method.  Even if true, dup LNs are not
     * fetched because they are normally never used (see accumulateDupLNs).
     */
    public boolean accumulateLNs = false;

    /*
     * If true, fetch LNs in a dup DB.  Since LNs in a dup DB are not used by
     * cursor operations, fetching dup LNs should only be needed in very
     * exceptional situations.  Currently this field is never set to true.
     */
    boolean accumulateDupLNs = false;

    /*
     * If non-null, save any exceptions encountered while traversing nodes into
     * this savedException list, in order to walk as much of the tree as
     * possible. The caller of the tree walker will handle the exceptions.
     */
    private final List<DatabaseException> savedExceptions;

    private final ExceptionPredicate excPredicate;

    /*
     * The batch size of LSNs which will be sorted.
     */
    private long lsnBatchSize = Long.MAX_VALUE;

    /* Holder for returning LN key from fetchLSN. */
    private final DatabaseEntry lnKeyEntry = new DatabaseEntry();

    /*
     * This map provides an LSN to IN/index. When an LSN is processed by the
     * tree walker, the map is used to lookup the parent IN and child entry
     * index of each LSN processed by the tree walker.
     * TODO: This map may no longer be necessary. But this entire class may
     *  be removed anyway in the future.
     *
     * LSN -> INEntry
     */
    /* struct to hold IN/entry-index pair. */
    public static class INEntry {
        final IN in;
        final int index;

        INEntry(IN in, int index) {
            assert in != null;
            assert in.getDatabase() != null;
            this.in = in;
            this.index = index;
        }

        public INEntry(@SuppressWarnings("unused") SizeofMarker marker) {
            this.in = null;
            this.index = 0;
        }

        Object getDelta() {
            return null;
        }

        long getDeltaLsn() {
            return DbLsn.NULL_LSN;
        }

        long getMemorySize() {
            return MemoryBudget.HASHMAP_ENTRY_OVERHEAD +
                   MemoryBudget.INENTRY_OVERHEAD;
        }
    }

    /**
     * Supplements INEntry with BIN-delta information.  When a BIN-delta is
     * encountered during the fetching process, we cannot immediately place it
     * in the tree.  Instead we queue a DeltaINEntry for fetching the full BIN,
     * in LSN order as usual.  When the full BIN is fetched, the DeltaINEntry
     * is used to apply the delta and place the result in the tree.
     */
    public static class DeltaINEntry extends INEntry {
        private final Object delta;
        private final long deltaLsn;

        DeltaINEntry(IN in, int index, Object delta, long deltaLsn) {
            super(in, index);
            assert (delta != null);
            assert (deltaLsn != DbLsn.NULL_LSN);
            this.delta = delta;
            this.deltaLsn = deltaLsn;
        }

        public DeltaINEntry(@SuppressWarnings("unused") SizeofMarker marker) {
            super(marker);
            this.delta = null;
            this.deltaLsn = 0;
        }

        @Override
        Object getDelta() {
            return delta;
        }

        @Override
        long getDeltaLsn() {
            return deltaLsn;
        }

        @Override
        long getMemorySize() {
            final long deltaSize = ((BIN) delta).getInMemorySize();
            return deltaSize +
                MemoryBudget.HASHMAP_ENTRY_OVERHEAD +
                MemoryBudget.DELTAINENTRY_OVERHEAD;
        }
    }

    private final Map<Long, INEntry> lsnINMap = new HashMap<>();

    /*
     * @param dbImpls an array of DatabaseImpls which should be walked over
     * in disk order.  This array must be parallel to the rootLsns array in
     * that rootLsns[i] must be the root LSN for dbImpls[i].
     *
     * @param rootLsns is passed in addition to the dbImpls, because the
     * root may be nulled out on the dbImpl before walk() is called.
     *
     * @param callback the callback instance
     *
     * @param savedExceptions a List of DatabaseExceptions encountered during
     * the tree walk.
     *
     * @param excPredicate a predicate to determine whether a given exception
     * should be ignored.
     */
    public SortedLSNTreeWalker(DatabaseImpl[] dbImpls,
                               long[] rootLsns,
                               TreeNodeProcessor callback,
                               List<DatabaseException> savedExceptions,
                               ExceptionPredicate excPredicate) {

        if (dbImpls == null || dbImpls.length < 1) {
            throw EnvironmentFailureException.unexpectedState
                ("DatabaseImpls array is null or 0-length for " +
                 "SortedLSNTreeWalker");
        }

        this.dbImpls = dbImpls;
        this.envImpl = dbImpls[0].getEnv();
        /* Make sure all databases are from the same environment. */
        for (DatabaseImpl di : dbImpls) {
            EnvironmentImpl ei = di.getEnv();
            if (ei == null) {
                throw EnvironmentFailureException.unexpectedState
                    ("environmentImpl is null for target db " +
                     di.getName());
            }

            if (ei != this.envImpl) {
                throw new IllegalArgumentException
                    ("Must be called with Databases which are all in the " +
                        "same Environment. (" + di.getName() + ")");
            }
        }

        this.rootLsns = rootLsns;
        this.callback = callback;
        this.savedExceptions = savedExceptions;
        this.excPredicate = excPredicate;
    }

    void setLSNBatchSize(long lsnBatchSize) {
        this.lsnBatchSize = lsnBatchSize;
    }

    public void setInternalMemoryLimit(long internalMemoryLimit) {
        this.internalMemoryLimit = internalMemoryLimit;
    }

    private void incInternalMemoryUsage(long increment) {
        internalMemoryUsage += increment;
    }

    private LSNAccumulator createLSNAccumulator() {
        return new LSNAccumulator() {
            @Override
            void noteMemUsage(long increment) {
                incInternalMemoryUsage(increment);
            }
        };
    }

    /**
     * Find all non-resident nodes, and execute the callback.  The root IN's
     * LSN is not returned to the callback.
     */
    public void walk() {
        walkInternal();
    }

    void walkInternal() {

        /*
         * Phase 1: seed the SLTW with all of the roots of the DatabaseImpl[].
         * For each root, look for all in-memory child nodes and process them
         * (i.e. invoke the callback on those LSNs).  For child nodes which are
         * not in-memory (i.e. they are LSNs only and no Node references),
         * accumulate their LSNs to be later sorted and processed during phase
         * 2.
         */
        LSNAccumulator pendingLSNs = createLSNAccumulator();
        for (int i = 0; i < dbImpls.length; i += 1) {
            processRootLSN(dbImpls[i], pendingLSNs, rootLsns[i]);
        }

        /*
         * Phase 2: Sort and process any LSNs we've gathered so far. For each
         * LSN, fetch the target record and process it as in Phase 1 (i.e.
         * in-memory children get passed to the callback, not in-memory children
         * have their LSN accumulated for later sorting, fetching, and
         * processing.
         */
        processAccumulatedLSNs(pendingLSNs);
    }

    /*
     * Retrieve the root for the given DatabaseImpl and then process its
     * children.
     */
    private void processRootLSN(DatabaseImpl dbImpl,
                                LSNAccumulator pendingLSNs,
                                long rootLsn) {
        IN root = getOrFetchRootIN(dbImpl, rootLsn);
        if (root != null) {
            try {
                accumulateLSNs(root, pendingLSNs);
            } finally {
                releaseRootIN(root);
            }
        }
    }

    /*
     * Traverse the in-memory tree rooted at "parent". For each visited node N
     * call the callback method on N and put in pendingLSNs the LSNs of N's
     * non-resident children.
     *
     * On entering this method, parent is latched and remains latched on exit.
     */
    private void accumulateLSNs(final IN parent,
                                final LSNAccumulator pendingLSNs) {
        envImpl.checkOpen();

        final DatabaseImpl db = parent.getDatabase();
        final boolean dups = db.getSortedDuplicates();
        final boolean allChildrenAreLNs = parent.isBIN();

        /*
         * If LNs are not needed, there is no need to accumulate the child LSNs
         * when all children are LNs.
         */
        final boolean accumulateChildren =
            !allChildrenAreLNs || (dups ? accumulateDupLNs : accumulateLNs);

        final BIN parentBin = parent.isBIN() ? ((BIN) parent) : null;

        /*
         * Process all children, but only accumulate LSNs for children that are
         * not in memory.
         */
        for (int i = 0; i < parent.getNEntries(); i += 1) {

            final long lsn = parent.getLsn(i);
            Node child = parent.getTarget(i);
            final boolean childCached = child != null;
            final boolean isEmbedded = parent.isEmbeddedLN(i);

            final byte[] lnKey =
                (allChildrenAreLNs || (childCached && child.isLN())) ?
                parent.getKey(i) : null;

            if (parentBin != null && parentBin.isDefunct(i)) {
                /* continue; */

            } else if (accumulateChildren &&
                       !childCached &&
                       lsn != DbLsn.NULL_LSN) {

                /*
                 * Child is not in cache. Put its LSN in the current batch of
                 * LSNs to be sorted and fetched in phase 2. But don't do
                 * this if the child is an embedded LN.
                 */
                if (!isEmbedded) {
                    pendingLSNs.add(lsn);
                    addToLsnINMap(lsn, parent, i);
                } else {
                    processChild(
                        DbLsn.NULL_LSN, null /*child*/, lnKey,
                        0 /*lastLoggedSize*/, true /*isEmbedded*/,
                        pendingLSNs);
                }

            } else if (childCached) {

                child.latchShared();
                boolean isLatched = true;

                try {
                    if (child.isBINDelta()) {
                        assert (lsn != DbLsn.NULL_LSN);

                        final BIN delta = (BIN) child;
                        final long fullLsn = delta.getLastFullLsn();
                        pendingLSNs.add(fullLsn);
                        addToLsnINMap(fullLsn, parent, i, delta, lsn);

                    } else {

                        child.releaseLatch();
                        isLatched = false;

                        processChild(
                            lsn, child, lnKey, parent.getLastLoggedSize(i),
                            isEmbedded, pendingLSNs);
                    }
                } finally {
                    if (isLatched) {
                        child.releaseLatch();
                    }
                }

            } else {
                /*
                 * We are here because the child was not cached and was not
                 * accumulated either (because it was an LN and LN accumulation
                 * is turned off or its LSN was NULL).
                 */
                processChild(
                    lsn, null /*child*/, lnKey, parent.getLastLoggedSize(i),
                    isEmbedded, pendingLSNs);
            }

            /*
             * If we've exceeded the batch size then process the current
             * batch and start a new one.
             */
            final boolean internalMemoryExceeded =
                internalMemoryUsage > internalMemoryLimit;

            if (pendingLSNs.getNTotalEntries() > lsnBatchSize ||
                internalMemoryExceeded) {
                if (internalMemoryExceeded) {
                    callback.noteMemoryExceeded();
                }
                processAccumulatedLSNs(pendingLSNs);
                pendingLSNs.clear();
            }
        }
    }

    private void processChild(
        final long lsn,
        final Node child,
        final byte[] lnKey,
        final int lastLoggedSize,
        final boolean isEmbedded,
        final LSNAccumulator pendingLSNs) {

        final boolean childCached = (child != null);

        /*
         * If the child is resident, use its log type, else it must be an LN.
         */
        callProcessLSNHandleExceptions(
            lsn,
            (!childCached ?
             LogEntryType.LOG_INS_LN /* Any LN type will do */ :
             child.getGenericLogType()),
            child, lnKey, lastLoggedSize, isEmbedded);

        if (childCached && child.isIN()) {
            final IN nodeAsIN = (IN) child;
            try {
                nodeAsIN.latch(CacheMode.UNCHANGED);
                accumulateLSNs(nodeAsIN, pendingLSNs);
            } finally {
                nodeAsIN.releaseLatch();
            }
        }
    }

    /*
     * Process a batch of LSNs by sorting and fetching each of them.
     */
    private void processAccumulatedLSNs(LSNAccumulator pendingLSNs) {

        while (!pendingLSNs.isEmpty()) {
            final long[] currentLSNs = pendingLSNs.getAndSortPendingLSNs();
            pendingLSNs = createLSNAccumulator();
            for (long lsn : currentLSNs) {
                fetchAndProcessLSN(lsn, pendingLSNs);
            }
        }
    }

    /*
     * Fetch the node at 'lsn' and callback to let the invoker process it.  If
     * it is an IN, accumulate LSNs for it.
     */
    private void fetchAndProcessLSN(long lsn, LSNAccumulator pendingLSNs) {

        lnKeyEntry.setData(null);

        final FetchResult result = fetchLSNHandleExceptions(
            lsn, lnKeyEntry, pendingLSNs);

        if (result == null) {
            return;
        }

        final boolean isIN = result.node.isIN();
        final IN in;
        if (isIN) {
            in = (IN) result.node;
            in.latch(CacheMode.UNCHANGED);
        } else {
            in = null;
        }

        try {
            callProcessLSNHandleExceptions(
                lsn, result.node.getGenericLogType(), result.node,
                lnKeyEntry.getData(), result.lastLoggedSize,
                false /*isEmbedded*/);

            if (isIN) {
                accumulateLSNs(in, pendingLSNs);
            }
        } finally {
            if (isIN) {
                in.releaseLatch();
            }
        }
    }

    private FetchResult fetchLSNHandleExceptions(
        long lsn,
        DatabaseEntry lnKeyEntry,
        LSNAccumulator pendingLSNs) {

        DatabaseException dbe = null;

        try {
            return fetchLSN(lsn, lnKeyEntry, pendingLSNs);

        } catch (DatabaseException e) {
            if (excPredicate == null ||
                !excPredicate.ignoreException(e)) {
                dbe = e;
            }
        }

        if (dbe != null) {
            if (savedExceptions != null) {

                /*
                 * This LSN fetch hit a failure. Do as much of the rest of
                 * the tree as possible.
                 */
                savedExceptions.add(dbe);
            } else {
                throw dbe;
            }
        }

        return null;
    }

    private void callProcessLSNHandleExceptions(long childLSN,
                                                LogEntryType childType,
                                                Node theNode,
                                                byte[] lnKey,
                                                int lastLoggedSize,
                                                boolean isEmbedded) {
        DatabaseException dbe = null;

        try {
            callback.processLSN(
                childLSN, childType, theNode, lnKey, lastLoggedSize,
                isEmbedded);

        } catch (FileNotFoundException e) {
            if (excPredicate == null ||
                !excPredicate.ignoreException(e)) {
                dbe = new EnvironmentFailureException(
                    envImpl, EnvironmentFailureReason.LOG_FILE_NOT_FOUND, e);
            }

        } catch (DatabaseException e) {
            if (excPredicate == null ||
                !excPredicate.ignoreException(e)) {
                dbe = e;
            }
        }

        if (dbe != null) {
            if (savedExceptions != null) {

                /*
                 * This LSN fetch hit a failure. Do as much of the rest of
                 * the tree as possible.
                 */
                savedExceptions.add(dbe);
            } else {
                throw dbe;
            }
        }
    }

    /**
     * Returns the root IN, latched shared.  Allows subclasses to override
     * getResidentRootIN and/or getRootIN to modify behavior.
     * getResidentRootIN is called first,
     */
    private IN getOrFetchRootIN(DatabaseImpl dbImpl, long rootLsn) {
        final IN root = getResidentRootIN(dbImpl);
        if (root != null) {
            return root;
        }
        if (rootLsn == DbLsn.NULL_LSN) {
            return null;
        }
        return getRootIN(dbImpl, rootLsn);
    }

    /**
     * The default behavior fetches the rootIN from the log and latches it
     * shared. Classes extending this may fetch (and latch) the root from the
     * tree.
     */
    IN getRootIN(DatabaseImpl dbImpl, long rootLsn) {
        final IN root = (IN)
            envImpl.getLogManager().getEntryHandleNotFound(rootLsn, -1);
        if (root == null) {
            return null;
        }
        root.setDatabase(dbImpl);
        root.latchShared(CacheMode.DEFAULT);
        return root;
    }

    /**
     * The default behavior returns (and latches shared) the IN if it is
     * resident in the Btree, or null otherwise.  Classes extending this may
     * return (and latch) a known IN object.
     */
    public IN getResidentRootIN(DatabaseImpl dbImpl) {
        return dbImpl.getTree().getResidentRootIN(true /*latched*/);
    }

    /**
     * Release the latch.  Overriding this method should not be necessary.
     */
    private void releaseRootIN(IN root) {
        root.releaseLatch();
    }

    /**
     * Add an LSN-IN/index entry to the map.
     */
    private void addToLsnINMap(long lsn, IN in, int index) {
        addEntryToLsnMap(lsn, new INEntry(in, index));
    }

    /**
     * Add an LSN-IN/index entry, along with a delta and delta LSN, to the map.
     */
    private void addToLsnINMap(long lsn,
                               IN in,
                               int index,
                               Object delta,
                               long deltaLsn) {
        addEntryToLsnMap(lsn, new DeltaINEntry(in, index, delta, deltaLsn));
    }

    private void addEntryToLsnMap(long lsn, INEntry inEntry) {
        if (lsnINMap.put(lsn, inEntry) == null) {
            incInternalMemoryUsage(inEntry.getMemorySize());
        }
    }

    private static class FetchResult {
        final Node node;
        final int lastLoggedSize;

        FetchResult(final Node node,
                    final int lastLoggedSize) {
            this.node = node;
            this.lastLoggedSize = lastLoggedSize;
        }
    }

    /*
     * Process an LSN.  Get & remove its INEntry from the map, then fetch the
     * target at the INEntry's IN/index pair.  This method will be called in
     * sorted LSN order.
     */
    private FetchResult fetchLSN(
        long lsn,
        DatabaseEntry lnKeyEntry,
        LSNAccumulator pendingLSNs) {

        final LogManager logManager = envImpl.getLogManager();

        final INEntry inEntry = lsnINMap.remove(lsn);
        assert (inEntry != null) : DbLsn.getNoFormatString(lsn);

        incInternalMemoryUsage(- inEntry.getMemorySize());

        IN in = inEntry.in;
        int index = inEntry.index;

        IN in1ToUnlatch = null;
        IN in2ToUnlatch = null;

        if (!in.isLatchExclusiveOwner()) {
            in.latch();
            in1ToUnlatch = in;
        }

        final DatabaseImpl dbImpl = in.getDatabase();
        byte[] lnKey = null;

        Node residentNode = in.getTarget(index);
        if (residentNode != null) {
            residentNode.latch();
        }

        try {
            Object deltaObject = inEntry.getDelta();

            /*
             * Concurrent activity (e.g., log cleaning) that was active before
             * we took the root latch may have changed the state of a slot.
             * Repeat check for LN deletion/expiration and check that the LSN
             * has not changed.
             */
            if (in.isBIN() && ((BIN) in).isDefunct(index)) {
                return null;
            }

            if (deltaObject == null) {
                if (in.getLsn(index) != lsn) {
                    return null;
                }
            } else {
                if (in.getLsn(index) != inEntry.getDeltaLsn()) {
                    return null;
                }
            }

            boolean mutateResidentDeltaToFullBIN = false;

            if (residentNode != null) {
                /*
                 * If the resident node is not a delta then concurrent
                 * activity (e.g., log cleaning) must have loaded the node.
                 * Just return it and continue.
                 */
                if (!residentNode.isBINDelta()) {
                    if (residentNode.isLN()) {
                        lnKeyEntry.setData(in.getKey(index));
                    }
                    return new FetchResult(
                        residentNode, in.getLastLoggedSize(index));
                }

                /* The resident node is a delta. */
                if (((BIN) residentNode).getLastFullLsn() != lsn) {
                    return null; // See note on concurrent activity above.
                }
                mutateResidentDeltaToFullBIN = true;
            }

            /* Fetch log entry. */
            final WholeEntry wholeEntry;
            try {
                /*
                 * LastLoggedSize param is -1 meaning we'll have to do two
                 * fetches. The best solution to this is to get rid of
                 * SortedLSNTreeWalker entirely.
                 */
                wholeEntry = logManager.getWholeLogEntry(lsn, -1);

            } catch (FileNotFoundException|ErasedException e) {
                final String msg =
                    "SortedLSNTreeWalker failed" +
                    " dbId=" + dbImpl.getId() +
                    " deltaObject=" + (deltaObject != null) +
                    " residentNode=" + (residentNode != null);

                throw new EnvironmentFailureException(
                    envImpl,
                    (e instanceof FileNotFoundException) ?
                        EnvironmentFailureReason.LOG_FILE_NOT_FOUND :
                        EnvironmentFailureReason.LOG_CHECKSUM,
                    in.makeFetchErrorMsg(
                        "SortedLSNTreeWalker", msg, lsn, index), e);
            }

            final LogEntry entry = wholeEntry.getEntry();
            final int lastLoggedSize = wholeEntry.getHeader().getEntrySize();

            /*
             * For a BIN delta, queue fetching of the full BIN and combine the
             * full BIN with the delta when it is processed later (see below).
             */
            if (entry instanceof BINDeltaLogEntry) {
                final BINDeltaLogEntry deltaEntry = (BINDeltaLogEntry) entry;
                final long fullLsn = deltaEntry.getLastFullLsn();
                final BIN delta = deltaEntry.getMainItem();
                pendingLSNs.add(fullLsn);
                addToLsnINMap(fullLsn, in, index, delta, lsn);
                return null;
            }

            /* For an LNLogEntry, call postFetchInit and get the lnKey. */
            if (entry instanceof LNLogEntry) {
                final LNLogEntry<?> lnEntry = (LNLogEntry<?>) entry;
                lnEntry.postFetchInit(dbImpl);
                lnKey = lnEntry.getKey();
                lnKeyEntry.setData(lnKey);
            }

            /* Get the Node from the LogEntry. */
            final Node ret = (Node) entry.getResolvedItem(dbImpl);

            /*
             * For an IN Node, set the database so it will be passed down to
             * nested fetches.
             */
            if (ret.isIN()) {
                final IN retIn = (IN) ret;
                retIn.setDatabase(dbImpl);
            }

            /*
             * If there is a delta, then this is the full BIN to which the
             * delta must be applied. The delta LSN is the last logged LSN.
             */
            if (mutateResidentDeltaToFullBIN) {
                final BIN fullBIN = (BIN) ret;
                BIN delta = (BIN) residentNode;
                delta.reconstituteBIN(
                    dbImpl, fullBIN, false /*leaveFreeSlot*/);

                return new FetchResult(ret, lastLoggedSize);
            }

            if (deltaObject != null) {
                final BIN fullBIN = (BIN) ret;
                final BIN delta = (BIN) deltaObject;
                delta.reconstituteBIN(
                    dbImpl, fullBIN, false /*leaveFreeSlot*/);
            }

            assert !ret.isBINDelta(false);

            return new FetchResult(ret, lastLoggedSize);

        } finally {
            if (residentNode != null) {
                residentNode.releaseLatch();
            }
            if (in1ToUnlatch != null) {
                in1ToUnlatch.releaseLatch();
            }
            if (in2ToUnlatch != null) {
                in2ToUnlatch.releaseLatch();
            }
        }
    }
}
