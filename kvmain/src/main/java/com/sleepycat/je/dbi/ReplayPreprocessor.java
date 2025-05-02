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

import static com.sleepycat.je.EnvironmentFailureException.assertState;
import static com.sleepycat.je.log.LogEntryType.LOG_DEL_LN_TRANSACTIONAL;
import static com.sleepycat.je.log.LogEntryType.LOG_INS_LN_TRANSACTIONAL;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.tree.BIN;

/**
 * Performs the following work in preprocessor threads, in order to off-load
 * this work from the replay thread.
 * <ul>
 *     <li>
 *     Does the Btree search for all operations, which may perform a
 *     fetch of the BIN. For inserts, if necessary, splits will be performed
 *     during the search and BIN-deltas will be mutated to full BINs. The
 *     resulting BIN is pinned to prevent eviction prior to performing the
 *     replay. The pinned BIN is passed to replay cursor ops and used whenever
 *     possible.
 *     <p>
 *     Unfortunately, for both splits and mutations to a full BIN, a
 *     series of sequential-key insertions could easily cause the split or
 *     mutation to occur in the replay thread. If multiple insertions are
 *     preprocessed for the same BIN prior to any of them being replayed,
 *     the conditions needed for the split or mutation won't occur until
 *     during the replay. For BIN-delta mutations, it is better not to
 *     unconditionally mutate to a full BIN because that would prevent all
 *     insertions into a BIN-delta; instead we simply tolerate the mutation
 *     costs during replay in this situation. Perhaps in the future we'll
 *     think of a simple way to predict in advance whether a split is needed
 *     in this situation, perhaps using a pending insertion count per BIN.
 *     <p>
 *     A related problematic case is when records are inserted in key order
 *     and each inserted record is the highest key in the Database. The proper
 *     BIN will be found by the preprocessor but it may be rejected by
 *     {@link BIN#isKeyInBounds} (called by {@link #getPinnedBIN}) because
 *     the target key is greater than the last key in the BIN. This may be
 *     infrequent in KVS because multiple tables are stored per Database,
 *     making insertions at the end of a Database less frequent.
 *     <p>
 *     A possibility for addressing both of the problems above is to find a
 *     more accurate way to validate a BIN for an operation than calling
 *     {@link BIN#isKeyInBounds}, perhaps by checking the parent slots.
 *     </li>
 *
 *     <li>If the DB has any triggers (which are used for some KVS metadata
 *     DBs), the LN is fetched here to avoid this overhead in the replay
 *     thread.</li>
 * </ul>
 */
public class ReplayPreprocessor implements Runnable {

    private final EnvironmentImpl envImpl;
    private final LNLogEntry<?> lnEntry;
    private BIN pinnedBIN;

    /**
     * True if preprocessing is complete, or is handled by replay because
     * replay processing occurred before preprocessing (should be rare).
     */
    private volatile boolean done;

    /**
     * True if {@link #close} was called, or close is unnecessary because
     * pinnedBIN is null and therefore there are no resources to release.
     */
    private boolean closed;

    /**
     * The {@link #close} method must be called after creating this object
     * to release resources, if there is any chance that the {@link #run}
     * method may have been called.
     */
    public ReplayPreprocessor(final EnvironmentImpl envImpl,
                              final LNLogEntry<?> lnEntry) {
        this.envImpl = envImpl;
        this.lnEntry = lnEntry;
    }

    /**
     * Called by replay to get the preprocessed result. This should be the
     * first method called after creating the object (normally, after
     * queuing it for preprocessing).
     *
     * <p>If null is returned, the preprocessor did not yet run or could not
     * obtain a result (this is unexpected but possible), and the replayer
     * must do without the result. If non-null is returned, the processor
     * did run and a pinned BIN is available for replay.</p>
     *
     * <p>This method may block while the preprocessor finishes running. No
     * work is wasted, but a thread context switch must occur for the replayer
     * to continue. Even if we somehow allowed both threads to process the
     * operation concurrently, they would block on the BIN latch, so blocking
     * is unavoidable. This is not expected to be a common case because the
     * preprocessor threads should stay ahead of the replay thread.</p>
     */
    public synchronized ReplayPreprocessor takeForReplay() {
        if (closed){
            return null;
        }
        if (done) {
            if (pinnedBIN != null) {
                return this;
            } else {
                closed = true;
                return null;
            }
        } else {
            done = true;
            closed = true;
            return null;
        }
    }

    /**
     * Returns the BIN resulting from the Btree search during preprocessing,
     * if the BIN still qualifies for use by the operation; otherwise null.
     *
     * @param key is the key to be inserted, updated or fetched.
     *
     * @param forInsert is true if an insert will be performed and therefore
     * the BIN must not need splitting.
     *
     * @return the latched BIN or null.
     */
    synchronized BIN getPinnedBIN(final byte[] key, final boolean forInsert) {
        assertState(done);
        assertState(!closed);

        if (pinnedBIN != null) {
            pinnedBIN.latchNoUpdateLRU();
            if (pinnedBIN.isKeyInBounds(key) &&
                !(forInsert && pinnedBIN.needsSplitting())) {
                return pinnedBIN;
            } else {
                pinnedBIN.releaseLatch();
                envImpl.incReplayPreprocessMiss();
            }
        }
        return null;
    }

    /**
     * If not previously closed, releases any resources acquired during
     * execution and not used by replay. This includes unpinning the BIN.
     *
     * <p>Must be called if a ReplayProcessor was executed.</p>
     */
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        done = true;
        if (pinnedBIN != null) {
            pinnedBIN.unpin();
        }
    }

    /**
     * Performs preprocessing if {@link #takeForReplay} has not yet been
     * called.
     */
    @Override
    public synchronized void run() {
        if (done) {
            return;
        }
        done = true;

        final DatabaseId dbId = lnEntry.getDbId();

        final DatabaseImpl dbImpl = envImpl.getDbTree().getDb(dbId);
        if (dbImpl == null) {
            return;
        }

        BIN bin = null;

        try {
            lnEntry.postFetchInit(dbImpl);

            final boolean isInsertion =
                LOG_INS_LN_TRANSACTIONAL.equals(lnEntry.getLogType());

            final boolean isDeletion =
                LOG_DEL_LN_TRANSACTIONAL.equals(lnEntry.getLogType());

            final CacheMode cacheMode = dbImpl.getDefaultCacheMode();
            final byte[] key = lnEntry.getKey();

            if (isInsertion) {
                bin = dbImpl.getTree().searchSplitsAllowed(key, cacheMode);

                if (bin == null) {
                    return;
                }

                if (bin.isBINDelta() &&
                    bin.insertMustMutateToFullBIN(key, false)) {

                    bin.mutateToFullBIN(true);
                }
            } else {
                bin = dbImpl.getTree().search(key, cacheMode);

                if (bin == null) {
                    return;
                }

                int idx = bin.findEntry(key, false, true);

                if (idx < 0 &&
                    bin.isBINDelta() &&
                    !(isDeletion && dbImpl.allowBlindDeletions()) &&
                    bin.mayHaveKeyInFullBin(key)) {

                    bin.mutateToFullBIN(false);
                    idx = bin.findEntry(key, false, true);
                }

                if (idx >= 0) {
                    final boolean hasSeparateLN =
                        !dbImpl.isLNImmediatelyObsolete() &&
                        !bin.isEmbeddedLN(idx);
                    /*
                     * Add old LN to cache to avoid a fetch from disk
                     * during the replay put. The old data is needed for
                     * triggers but not for secondaries, since secondary LNs
                     * are currently replicated independently.
                     */
                    if (hasSeparateLN && dbImpl.hasUserTriggers()) {
                        bin.fetchLN(idx, cacheMode);
                    }
                }
            }

            bin.pin();
            pinnedBIN = bin;
        } catch (Throwable e) {
            close();
            throw e;
        } finally {
            if (bin != null) {
                bin.releaseLatch();
            }
            envImpl.getDbTree().releaseDb(dbImpl);
        }
    }
}
