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
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.log.ErasedException;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogItem;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.log.WholeEntry;
import com.sleepycat.je.txn.LockManager;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.SizeofMarker;
import com.sleepycat.je.utilint.TinyHashSet;
import com.sleepycat.je.utilint.VLSN;

/**
 * A BIN represents a Bottom Internal Node in the JE tree.
 *
 * BIN-deltas
 * ==========
 * A BIN-delta is a BIN with the non-dirty slots omitted. A "full BIN", OTOH
 * contains all slots.  On disk and in memory, the format of a BIN-delta is the
 * same as that of a BIN.  In memory, a BIN object is actually a BIN-delta when
 * the BIN-delta flag is set (IN.isBINDelta).  On disk, the NewBINDelta log
 * entry type (class BINDeltaLogEntry) is the only thing that distinguishes it
 * from a full BIN, which has the BIN log entry type.
 *
 * BIN-deltas provides two benefits: Reduced writing and reduced memory usage.
 *
 * Reduced Writing
 * ---------------
 * Logging a BIN-delta rather a full BIN reduces writing significantly.  The
 * cost, however, is that two reads are necessary to reconstruct a full BIN
 * from scratch.  The reduced writing is worth this cost, particularly because
 * less writing means less log cleaning.
 *
 * A BIN-delta is logged when 25% or less (configured with EnvironmentConfig
 * TREE_BIN_DELTA) of the slots in a BIN are dirty. When a BIN-delta is logged,
 * the dirty flag is cleared on the the BIN in cache.  If more slots are
 * dirtied and another BIN-delta is logged, it will contain all entries dirtied
 * since the last full BIN was logged.  In other words, BIN-deltas are
 * cumulative and not chained, to avoid reading many (more than two) log
 * entries to reconstruct a full BIN.  The dirty flag on each slot is cleared
 * only when a full BIN is logged.
 *
 * In addition to the cost of fetching two entries on a BIN cache miss, another
 * drawback of the current approach is that dirtiness propagates upward in the
 * Btree due to BIN-delta logging, causing repeated logging of upper INs.  The
 * slot of the parent IN contains the LSN of the most recent BIN-delta or full
 * BIN that was logged.  A BINDeltaLogEntry in turn contains the LSN of the
 * last full BIN logged.
 *
 *   In JE 6, deltas were also maintained in the Btree cache.  This was done to
 *   provide the reduced memory benefits described in the next section.  The
 *   log format for a delta was also changed.  The OldBINDelta log format used 
 *   to be different. Its log entry type name is still BINDelta, 
 *   which is why the new type is named NewBINDelta (for backward compatibility,
 *   log entry type names cannot be changed). This is also why the spelling 
 *   "BIN-delta" is used to refer to deltas in the new approach. The old 
 *   BINDelta class was renamed to OldBINDelta and there is no longer a class
 *   named BINDelta.
 *
 * Reduced Memory Usage
 * --------------------
 * In the Btree cache, a BIN may be represented as a full BIN or a BIN-delta.
 * Eviction will mutate a full BIN to a BIN-delta in preference to discarding
 * the entire BIN. A BIN-delta in cache occupies less memory than a full BIN,
 * and can be exploited as follows:
 *
 *  - When a full BIN is needed, it can be constructed with only one fetch
 *    rather than two, reducing IO overall.  IN.fetchIN implements this
 *    optimization.
 *
 *  - Certain operations can sometimes be performed using the BIN-delta alone,
 *    allowing such operations on a given data set to take place using less
 *    less IO (for a given cache size).
 *
 * The latter benefit is not yet implemented.   No user CRUD operations are
 * currently implemented using BIN-deltas. In the future we plan to implement
 * the following operations using the BIN-delta alone.
 *
 *  - Consider recording deletions in a BIN-delta.  Currently, slot deletion
 *    prohibits a BIN-delta from being logged.  To record deletion in
 *    BIN-deltas, slot deletion will have to be deferred until a full BIN is
 *    logged.
 *
 *  - User reads by key, updates and deletions can be implemented if the key
 *    happens to appear in the BIN-delta.
 *
 *  - The Cleaner can migrate an LN if its key happens to appear in the
 *    BIN-delta.  This is similar to a user update operation, but in a
 *    different code path.
 *
 *  - Insertions, deletions and updates can always be performed in a BIN-delta
 *    during replica replay, since the Master operation has already determined
 *    whether the key exists.
 *
 *  - Recovery LN redo could also apply insertions, updates and inserts in the
 *    manner described.
 *
 *  - Add idempotent put/delete operations, which can always be applied in a
 *    BIN-delta.
 *
 *  - Store a hash of the keys in the full BIN in the BIN-delta and use it to
 *    perform the following in the delta:
 *    - putIfAbsent (true insertion)
 *    - get/delete/putIfPresent operations that return NOTFOUND
 *    - to avoid accumulating unnecessary deletions
 *
 * However, some internal operations do currently exploit BIN-deltas to avoid
 * unnecessary IO.  The following are currently implemented.
 *
 *  - The Evictor and Checkpointer log a BIN-delta that is present in the
 *    cache, without having to fetch the full BIN.
 *
 *  - The Cleaner can use the BIN-delta to avoid fetching when processing a BIN
 *    log entry (delta or full) and the BIN is not present in cache,
 *
 * To support BIB-delta-aware operations, the IN.fetchIN() and IN.getTarget()
 * methods may return a BIN delta. IN.getTarget() will return whatever object
 * is cached under the parent IN, and IN.fetchIN() will do a single I/O to
 * fetch the most recently log record for the requested BIN, which may be a
 * full BIN or a delta. Callers of these methods must be prepared to handle
 * a BIN delta; either doing their operation directly on the delta, if
 * possible, or mutating the delta to a full BIN by calling
 * BIN.mutateToFullBIN().
 */
public class BIN extends IN {

    private static final String BEGIN_TAG = "<bin>";
    private static final String END_TAG = "</bin>";

    /**
     * Used as the "empty rep" for the INLongRep vlsnCache field.
     *
     * minLength is 5 because VLSNS grow that large fairly quickly, and less
     * mutation is better. The value 5 accommodates data set sizes up to 100
     * billion. If we want to improve memory utilization for smaller data sets
     * or reduce mutation for larger data sets, we could dynamically determine
     * a value based on the last assigned VLSN.
     *
     * allowSparseRep is false because either all slots typically have VLSNs,
     * or none do, and less mutation is better.
     */
    private static final INLongRep.EmptyRep EMPTY_VLSNS =
        new INLongRep.EmptyRep(5, false);

    /**
     * Used as the "empty rep" for the INLongRep expirationValues field.
     *
     * minLength is 1 because we expect most expiration values, which are an
     * offset from a base day/hour, to fit in one byte.
     *
     * allowSparseRep is true because some workloads only set TTLs on some of
     * the LNs in a BIN.
     */
    private static final INLongRep.EmptyRep EMPTY_EXPIRATION =
        new INLongRep.EmptyRep(1, true);

    /**
     * Used as the "empty rep" for the INLongRep modificationTimes field.
     * Modification times are stored in the BIN for embedded data records.
     *
     * minLength is 3 because this allows around 1 day to be represented.
     * 2 bytes would only allow represent around half a minute. This value is
     * the delta between records in a single BIN.
     *
     * allowSparseRep is true because only some of the LNs in a BIN may have
     * embedded data.
     */
    private static final INLongRep.EmptyRep EMPTY_MODIFICATION_TIMES =
        new INLongRep.EmptyRep(3, true);

    private static final INLongRep.EmptyRep EMPTY_CREATION_TIMES =
        new INLongRep.EmptyRep(3, true);

    /**
     * The set of cursors that are currently referring to this BIN.
     * This field is set to null when there are no cursors on this BIN.
     */
    private TinyHashSet<CursorImpl> cursorSet;

    /*
     * Support for logging BIN deltas. (Partial BIN logging)
     */

    /**
     * If this is a delta, fullBinNEntries stores the number of entries
     * in the full version of the BIN. This is a persistent field for
     * BIN-delta logrecs only, and for log versions >= 10.
     */
    private int fullBinNEntries = -1;

    /**
     * If this is a delta, fullBinMaxEntries stores the max number of
     * entries (capacity) in the full version of the BIN. This is a
     * persistent field for BIN-delta logrecs only, and for log versions >= 10.
     */
    private int fullBinMaxEntries = -1;

    /**
     * If "this" is a BIN-delta, bloomFilter is a bloom-filter representation
     * of the set of keys in the clean slots of the full version of the same
     * BIN. It is used to allow blind put operations in deltas, by answering
     * the question whether the put key is in the full BIN or not. See the
     * javadoc of the  TREE_BIN_DELTA_BLIND_PUTS config param for more info.
     * This is a persistent field for BIN-delta logrecs only, and for log
     * versions >= 10.
     */
    byte[] bloomFilter;

    /**
     * Caches the VLSN sequence for the LN entries in a BIN, when VLSN
     * preservation and caching are configured.
     *
     * A VLSN is added to the cache when an LN is evicted from a BIN. When the
     * LN is resident, there is no need for caching because the LN contains the
     * VLSN. See BIN.setTarget.  This strategy works because an LN is always
     * cached during a read or write operation, and only evicted after that,
     * based on eviction policies.
     *
     * For embedded LNs a VLSN is added to the cache every time the record is
     * logged. Furthermore, the vlsn cache is made persistent for such LNs.
     *
     * An EMPTY_REP is used initially until the need arises to add a non-zero
     * value.  The cache will remain empty if LNs are never evicted or version
     * caching is not configured, which is always the case for standalone JE.
     */
    private INLongRep vlsnCache = EMPTY_VLSNS;

    /**
     * An expirationValues slot value is one more than the number of days/hours
     * to add to the expirationBase to get the true expiration days/hours. A
     * slot value of zero means no expiration, and a non-zero slot value is one
     * greater than the actual offset to be added. The base is the smallest
     * non-zero offset that has been encountered.
     */
    private INLongRep expirationValues = EMPTY_EXPIRATION;
    private int expirationBase = -1;

    /**
     * A modificationTimes slot value is one more than the number of millis
     * to add to the modificationTimesBase to get the true modificationTime. A
     * slot value of zero means no modificationTime, and a non-zero slot value
     * is one greater than the actual offset to be added. The base is the
     * smallest non-zero offset that has been encountered.
     */
    private INLongRep modificationTimes = EMPTY_MODIFICATION_TIMES;
    private long modificationTimesBase = -1;

    private INLongRep creationTimes = EMPTY_CREATION_TIMES;
    private long creationTimesBase = -1;

    public BIN() {
    }

    public BIN(
        DatabaseImpl db,
        byte[] identifierKey,
        int maxCompactKeySize,
        int capacity,
        int level) {

        super(db, identifierKey, maxCompactKeySize, capacity, level);
    }

    /**
     * For Sizeof.
     */
    public BIN(@SuppressWarnings("unused") SizeofMarker marker) {
        super(marker);
    }

    /**
     * Create a new BIN.  Need this because we can't call newInstance()
     * without getting a 0 for nodeId.
     */
    @Override
    protected IN createNewInstance(
        byte[] identifierKey,
        int maxCompactKeySize,
        int maxEntries,
        int level) {

        return new BIN(
            getDatabase(), identifierKey, maxCompactKeySize, maxEntries, level);
    }

    public BINReference createReference() {
      return new BINReference(
          getNodeId(), getDatabase().getId(), getIdentifierKey());
    }

    @Override
    public boolean isBIN() {
        return true;
    }

    /*
     * Return whether the shared latch for this kind of node should be of the
     * "always exclusive" variety.  Presently, only IN's are actually latched
     * shared.  BINs are latched exclusive only.
     */
    @Override
    boolean isAlwaysLatchedExclusively() {
        return true;
    }

    @Override
    public String shortClassName() {
        return "BIN";
    }

    @Override
    public String beginTag() {
        return BEGIN_TAG;
    }

    @Override
    public String endTag() {
        return END_TAG;
    }

    boolean isVLSNCachingEnabled() {
        return (!databaseImpl.getSortedDuplicates() && getEnv().getCacheVLSN());
    }

    public void setCachedVLSN(int idx, long vlsn) {

        /*
         * We do not cache the VLSN for dup DBs, because dup DBs are typically
         * used only for indexes, and the overhead of VLSN maintenance would be
         * wasted.  Plus, although technically VLSN preservation might apply to
         * dup DBs, the VLSNs are not reliably available since the LNs are
         * immediately obsolete.
         */
        if (!isVLSNCachingEnabled()) {
            return;
        }
        setCachedVLSNUnconditional(idx, vlsn);
    }

    void setCachedVLSNUnconditional(int idx, long vlsn) {
        vlsnCache = vlsnCache.set(
            idx,
            (vlsn == NULL_VLSN ? 0 : vlsn),
            this);
    }

    long getCachedVLSN(int idx) {
        final long vlsn = vlsnCache.get(idx);
        return (vlsn == 0 ? NULL_VLSN : vlsn);
    }

    /**
     * Returns the VLSN.  NULL_VLSN (-1) is returned in two
     * cases:
     * 1) This is a standalone environment.
     * 2) The VLSN is not cached (perhaps VLSN caching is not configured), and
     *    the allowFetch param is false.
     *
     * WARNING: Because the vlsnCache is only updated when an LN is evicted, it
     * is critical that getVLSN returns the VLSN for a resident LN before
     * getting the VLSN from the cache.
     */
    public long getVLSN(int idx, boolean allowFetch, CacheMode cacheMode) {

        /* Must return the VLSN from the LN, if it is resident. */
        LN ln = (LN) getTarget(idx);
        if (ln != null) {
            return ln.getVLSNSequence();
        }

        /* Next try the vlsnCache. */
        long vlsn = getCachedVLSN(idx);
        if (!VLSN.isNull(vlsn)) {
            return vlsn;
        }

        /* As the last resort, fetch the LN if fetching is allowed. */
        if (!allowFetch || isEmbeddedLN(idx)) {
            return vlsn;
        }

        ln = fetchLN(idx, cacheMode);
        if (ln != null) {
            return ln.getVLSNSequence();
        }

        return NULL_VLSN;
    }

    /** For unit testing. */
    public INLongRep getVLSNCache() {
        return vlsnCache;
    }

    /**
     * Sets the expiration time for a slot in days or hours.
     */
    public void setExpiration(final int idx, int value, final boolean hours) {

        /* This slot has no expiration. */
        if (value == 0) {
            expirationValues = expirationValues.set(idx, 0, this);
            return;
        }

        /*
         * If this is the first slot with an expiration, initialize the base to
         * the value and set the offset (slot value) to one.
         */
        if (expirationBase == -1 || nEntries == 1) {
            expirationBase = value;
            setExpirationOffset(idx, 1);
            setExpirationInHours(hours);
            return;
        }

        if (hours) {
            /* Convert existing values to hours if necessary. */
            if (!isExpirationInHours()) {

                expirationBase *= 24;
                setExpirationInHours(true);

                for (int i = 0; i < nEntries; i += 1) {

                    if (i == idx) {
                        continue;
                    }

                    final int offset = (int) expirationValues.get(i);

                    if (offset == 0) {
                        continue;
                    }

                    setExpirationOffset(i, ((offset - 1) * 24) + 1);
                }
            }
        } else {
            /* If values are stored in hours, convert days to hours. */
            if (isExpirationInHours()) {
                value *= 24;
            }
        }

        /*
         * Slot's expiration must not be less than the base. If it is, decrease
         * the base and increase the offset in other slots accordingly.
         */
        if (value < expirationBase) {

            final int adjustment = expirationBase - value;
            expirationBase = value;

            for (int i = 0; i < nEntries; i += 1) {

                if (i == idx) {
                    continue;
                }

                final int offset = (int) expirationValues.get(i);

                if (offset == 0) {
                    continue;
                }

                setExpirationOffset(i, offset + adjustment);
            }
        }

        setExpirationOffset(idx, value - expirationBase + 1);
    }

    public boolean hasExpirationValues() {

        return !expirationValues.isEmpty();
    }

    /**
     * Returns the expiration time for a slot. The return value is in days or
     * hours, depending on isExpirationTimeInHours.
     */
    public int getExpiration(int idx) {

        final int offset = (int) expirationValues.get(idx);

        if (offset == 0) {
            return 0;
        }

        return offset - 1 + expirationBase;
    }

    public int getExpirationBase() {
        return expirationBase;
    }

    int getExpirationOffset(int idx) {
        return (int) expirationValues.get(idx);
    }

    void setExpirationBase(int base) {
        expirationBase = base;
    }

    void setExpirationOffset(int idx, int offset) {
        expirationValues = expirationValues.set(idx, offset, this);
    }

    /**
     * Sets the modification time for an embedded record.
     *
     * @param value Java time in millis.
     */
    public void setModificationTime(int idx, long value) {

        if (value == 0) {
            modificationTimes = modificationTimes.set(idx, 0, this);
            return;
        }

        /*
         * If this is the first slot with an expiration, initialize the base to
         * the value and set the offset (slot value) to one.
         */
        if (modificationTimesBase == -1 || nEntries == 1) {
            modificationTimesBase = value;
            setModificationTimeOffset(idx, 1);
            return;
        }

        /*
         * Slot's modification time must not be less than the base. If it is,
         * decrease the base and increase the offset in other slots
         * accordingly.
         */
        if (value < modificationTimesBase) {
            final long adjustment = modificationTimesBase - value;
            modificationTimesBase = value;

            for (int i = 0; i < nEntries; ++i) {
                if (i == idx) {
                    continue;
                }
                final long offset = modificationTimes.get(i);
                if (offset == 0) {
                    continue;
                }
                setModificationTimeOffset(i, offset + adjustment);
            }
        }

        setModificationTimeOffset(idx, value - modificationTimesBase + 1);
    }
      
    /**
     * Returns the modification time for an embedded record. Returns zero if
     * the record is not embedded or the record was written with JE 19.3 or
     * earlier.
     */
    public long getModificationTime(int idx) {
        final long offset = modificationTimes.get(idx);
        if (offset == 0) {
            return 0;
        }
        return offset - 1 + modificationTimesBase;
    }

    long getModificationTimesBase() {
        return modificationTimesBase;
    }

    long getModificationTimeOffset(int idx) {
        return modificationTimes.get(idx);
    }

    void setModificationTimesBase(long base) {
        modificationTimesBase = base;
    }

    void setModificationTimeOffset(int idx, long offset) {
        modificationTimes = modificationTimes.set(idx, offset, this);
    }

    public void setCreationTime(int idx, long value) {

        if (value == 0) {
            creationTimes = creationTimes.set(idx, 0, this);
            return;
        }

        /*
         * If this is the first slot with an expiration, initialize the base to
         * the value and set the offset (slot value) to one.
         */
        if (creationTimesBase == -1 || nEntries == 1) {
            creationTimesBase = value;
            setCreationTimeOffset(idx, 1);
            return;
        }

        /*
         * Slot's creation time must not be less than the base. If it is,
         * decrease the base and increase the offset in other slots
         * accordingly.
         */
        if (value < creationTimesBase) {
            final long adjustment = creationTimesBase - value;
            creationTimesBase = value;

            for (int i = 0; i < nEntries; ++i) {
                if (i == idx) {
                    continue;
                }
                final long offset = creationTimes.get(i);
                if (offset == 0) {
                    continue;
                }
                setCreationTimeOffset(i, offset + adjustment);
            }
        }

        setCreationTimeOffset(idx, value - creationTimesBase + 1);
    }

    public long getCreationTime(int idx) {
        final long offset = creationTimes.get(idx);

        if (offset == 0) {
            return 0;
        }
        return offset - 1 + creationTimesBase;
    }

    long getCreationTimesBase() {
        return creationTimesBase;
    }

    long getCreationTimeOffset(int idx) {
        return creationTimes.get(idx);
    }

    void setCreationTimesBase(long base) {
        creationTimesBase = base;
    }

    void setCreationTimeOffset(int idx, long offset) {
        creationTimes = creationTimes.set(idx, offset, this);
    }

    /**
     * Returns whether the slot is known-deleted, pending-deleted, or expired.
     */
    public boolean isDefunct(int idx) {
        return isDeleted(idx) || isExpired(idx);
    }

    /**
     * Same as {@link #isDefunct(int)} but optionally considers tombstones
     * defunct as well.
     */
    public boolean isDefunct(int idx, boolean excludeTombstones) {
        return isDefunct(idx) || (excludeTombstones && isTombstone(idx));
    }

    /**
     * Returns whether the slot is known-deleted or pending-deleted.
     */
    public boolean isDeleted(int idx) {
        return isEntryKnownDeleted(idx) || isEntryPendingDeleted(idx);
    }

    /**
     * Same as {@link #isDeleted(int)} but optionally considers tombstones
     * deleted as well.
     */
    public boolean isDeleted(int idx, boolean excludeTombstones) {
        return isDeleted(idx) || (excludeTombstones && isTombstone(idx));
    }

    /**
     * Returns whether the slot is expired.
     */
    public boolean isExpired(int idx) {
        return getEnv().isExpired(getExpiration(idx), isExpirationInHours());
    }

    public boolean isProbablyExpired(int idx) {

        return getEnv().expiresWithin(
            getExpiration(idx), isExpirationInHours(),
            getEnv().getTtlClockTolerance());
    }

    /**
     * Updates the vlsnCache when an LN target is evicted.  See vlsnCache.
     */
    @Override
    void setTarget(int idx, Node target) {

        if (target == null) {
            final Node oldTarget = getTarget(idx);
            if (oldTarget instanceof LN) {
                setCachedVLSN(idx, ((LN) oldTarget).getVLSNSequence());
            }
        }
        super.setTarget(idx, target);
    }

    /**
     * Overridden to account for BIN-specific slot info.
     */
    @Override
    void appendEntryFromOtherNode(IN from, int fromIdx) {

        super.appendEntryFromOtherNode(from, fromIdx);

        final BIN fromBin = (BIN) from;
        final int idx = nEntries - 1;

        setCachedVLSNUnconditional(idx, fromBin.getCachedVLSN(fromIdx));

        setExpiration(
            idx, fromBin.getExpiration(fromIdx),
            fromBin.isExpirationInHours());

        setModificationTime(idx, fromBin.getModificationTime(fromIdx));
        setCreationTime(idx, fromBin.getCreationTime(fromIdx));
    }

    /**
     * Overridden to account for BIN-specific slot info.
     */
    @Override
    void copyEntries(int from, int to, int n) {
        super.copyEntries(from, to, n);
        vlsnCache = vlsnCache.copy(from, to, n, this);
        expirationValues = expirationValues.copy(from, to, n, this);
        creationTimes = creationTimes.copy(from, to, n, this);
        modificationTimes = modificationTimes.copy(from, to, n, this);
    }

    /**
     * Overridden to account for BIN-specific slot info.
     */
    @Override
    void clearEntry(int idx) {
        super.clearEntry(idx);
        setCachedVLSNUnconditional(idx, NULL_VLSN);
        setExpiration(idx, 0, false);
        setModificationTime(idx, 0);
        setCreationTime(idx, 0);
    }

    /*
     * Cursors
     */

    /* public for the test suite. */
    public Set<CursorImpl> getCursorSet() {
       if (cursorSet == null) {
           return Collections.emptySet();
       }
       return cursorSet.copy();
    }

    /**
     * Register a cursor with this BIN.  Caller has this BIN already latched.
     * @param cursor Cursor to register.
     */
    public void addCursor(CursorImpl cursor) {
        assert isLatchExclusiveOwner();
        if (cursorSet == null) {
            cursorSet = new TinyHashSet<>();
        }
        cursorSet.add(cursor);
    }

    /**
     * Unregister a cursor with this bin.  Caller has this BIN already
     * latched.
     *
     * @param cursor Cursor to unregister.
     */
    public void removeCursor(CursorImpl cursor) {
        assert isLatchExclusiveOwner();
        if (cursorSet == null) {
            return;
        }
        cursorSet.remove(cursor);
        if (cursorSet.size() == 0) {
            cursorSet = null;
        }
    }

    /**
     * @return the number of cursors currently referring to this BIN.
     */
    public int nCursors() {

        /*
         * Use a local var to concurrent assignment to the cursorSet field by
         * another thread. This method is called via eviction without latching.
         * LRU-TODO: with the new evictor this method is called with the node
         * EX-latched. So, cleanup after the old evictor is scrapped.
         */
        final TinyHashSet<CursorImpl> cursors = cursorSet;
        if (cursors == null) {
            return 0;
        }
        return cursors.size();
    }

    /**
     * Adjust any cursors that are referring to this BIN.  This method is
     * called during a split operation.  "this" is the BIN being split.
     * newSibling is the new BIN into which the entries from "this" between
     * newSiblingLow and newSiblingHigh have been copied.
     *
     * @param newSibling the newSibling into which "this" has been split.
     * @param newSiblingLow the low entry of "this" that were moved into
     * newSibling.
     * @param newSiblingHigh the high entry of "this" that were moved into
     * newSibling.
     */
    @Override
    void adjustCursors(
        IN newSibling,
        int newSiblingLow,
        int newSiblingHigh)
    {
        assert newSibling.isLatchExclusiveOwner();
        assert this.isLatchExclusiveOwner();
        if (cursorSet == null) {
            return;
        }
        int adjustmentDelta = (newSiblingHigh - newSiblingLow);
        Iterator<CursorImpl> iter = cursorSet.iterator();

        while (iter.hasNext()) {
            CursorImpl cursor = iter.next();
            int cIdx = cursor.getIndex();
            cursor.assertBIN(this);
            assert newSibling instanceof BIN;

            /*
             * There are four cases to consider for cursor adjustments,
             * depending on (1) how the existing node gets split, and (2) where
             * the cursor points to currently.  In cases 1 and 2, the id key of
             * the node being split is to the right of the splitindex so the
             * new sibling gets the node entries to the left of that index.
             * This is indicated by "new sibling" to the left of the vertical
             * split line below.  The right side of the node contains entries
             * that will remain in the existing node (although they've been
             * shifted to the left).  The vertical bar (^) indicates where the
             * cursor currently points.
             *
             * case 1:
             *
             *   We need to set the cursor's "bin" reference to point at the
             *   new sibling, but we don't need to adjust its index since that
             *   continues to be correct post-split.
             *
             *   +=======================================+
             *   |  new sibling        |  existing node  |
             *   +=======================================+
             *         cursor ^
             *
             * case 2:
             *
             *   We only need to adjust the cursor's index since it continues
             *   to point to the current BIN post-split.
             *
             *   +=======================================+
             *   |  new sibling        |  existing node  |
             *   +=======================================+
             *                              cursor ^
             *
             * case 3:
             *
             *   Do nothing.  The cursor continues to point at the correct BIN
             *   and index.
             *
             *   +=======================================+
             *   |  existing Node        |  new sibling  |
             *   +=======================================+
             *         cursor ^
             *
             * case 4:
             *
             *   Adjust the "bin" pointer to point at the new sibling BIN and
             *   also adjust the index.
             *
             *   +=======================================+
             *   |  existing Node        |  new sibling  |
             *   +=======================================+
             *                                 cursor ^
             */
            BIN ns = (BIN) newSibling;
            if (newSiblingLow == 0) {
                if (cIdx < newSiblingHigh) {
                    /* case 1 */
                    iter.remove();
                    cursor.setBIN(ns);
                    ns.addCursor(cursor);
                } else {
                    /* case 2 */
                    cursor.setIndex(cIdx - adjustmentDelta);
                }
            } else {
                if (cIdx >= newSiblingLow) {
                    /* case 4 */
                    cursor.setIndex(cIdx - newSiblingLow);
                    iter.remove();
                    cursor.setBIN(ns);
                    ns.addCursor(cursor);
                }
            }
        }
    }

    /**
     * For each cursor in this BIN's cursor set, ensure that the cursor is
     * actually referring to this BIN.
     */
    public void verifyCursors() {
        if (cursorSet == null) {
            return;
        }
        for (CursorImpl cursor : cursorSet) {
            cursor.assertBIN(this);
        }
    }

    /**
     * Adjust cursors referring to this BIN following an insert.
     *
     * @param insertIndex - The index of the new entry.
     */
    @Override
    void adjustCursorsForInsert(int insertIndex) {

        assert this.isLatchExclusiveOwner();
        if (cursorSet == null) {
            return;
        }

        for (CursorImpl cursor : cursorSet) {
            int cIdx = cursor.getIndex();
            if (insertIndex <= cIdx) {
                cursor.setIndex(cIdx + 1);
            }
        }
    }

    /**
     * Called when we know we are about to split on behalf of a key that is the
     * minimum (leftSide) or maximum (!leftSide) of this node.  This is
     * achieved by just forcing the split to occur either one element in from
     * the left or the right (i.e. splitIndex is 1 or nEntries - 1).
     */
    @Override
    IN splitSpecial(
        IN parent,
        int childIndex,
        IN grandParent,
        int parentIndex,
        int maxEntriesPerNode,
        byte[] key,
        boolean leftSide)
        throws DatabaseException {

        int nEntries = getNEntries();

        int index = findEntry(key, true, false);

        boolean exact = (index & IN.EXACT_MATCH) != 0;
        index &= ~IN.EXACT_MATCH;

        if (leftSide && index < 0) {
            return splitInternal(
                parent, childIndex, grandParent, parentIndex,
                maxEntriesPerNode, 1);

        } else if (!leftSide && !exact && index == (nEntries - 1)) {
            return splitInternal(
                parent, childIndex, grandParent, parentIndex,
                maxEntriesPerNode, nEntries - 1);

        } else {
            return split(
                parent, childIndex, grandParent, parentIndex,
                maxEntriesPerNode);
        }
    }

    /**
     * Check if a bin should be compressed to remove (effectively to erase)
     * obsolete slots or keys. If true is returned, the node should be dirtied
     * and flushed, which will perform compression and update the parent's copy
     * of the identifier key (if necessary).
     * <p>
     * Note we don't check for extinction because it's an expensive check and
     * should be taken care of by the extinction scan. We also don't make this
     * decision based on presence of entries marked as deleting. Such entries
     * will be taken care of in the next cycle.
     *
     * @return Returns true if this bin can be compressed.
     */
    public boolean shouldCompressObsoleteKeys() {
        if (isBINDelta()) {
            return false;
        }

        if (getNEntries() <= 0) {
            return false;
        }

        if (entryKeys
            .compareKeys(getIdentifierKey(), keyPrefix, 0, haveEmbeddedData(0),
                getKeyComparator()) != 0) {
            return true;
        }

        for (int i = 0; i < getNEntries(); i++) {
            final boolean expired = isExpired(i);
            final boolean deleted = isEntryKnownDeleted(i);

            if (deleted || expired) {
                return true;
            }
        }

        return false;
    }

    /**
     * Compress a full BIN by removing any slots that are deleted or expired.
     *
     * This must not be a BIN-delta. No cursors can be present on the BIN,
     * unless the env is closing. Caller is responsible for latching and
     * unlatching this node.
     *
     * If the slot containing the identifier is removed or a new key with a
     * smaller value is added, the identifier key will be changed to the key
     * in the first remaining slot.
     *
     * Normally when a slot is removed, the IN is dirtied. However, during
     * compression the BIN is not dirtied when a slot is removed. This is safe
     * for the reasons described below. Note that the BIN being compressed is
     * always a full BIN, not a delta.
     *
     *  + If the BIN is not dirty and it does not become dirty before shutdown,
     *  i.e., it is not logged, then it is possible that this compression will
     *  be "lost". However, the state of the slot on disk is expired/deleted,
     *  and when the BIN is later fetched from disk, this state will be
     *  restored and the compression will be performed again.
     *
     *  + If the slot is dirty, the BIN may also be dirty or may become dirty
     *  later, and be logged. Logging a delta would cause the information in
     *  the dirty slot to be lost. Therefore, when a dirty slot is removed, we
     *  set a flag that prohibits the next BIN logged from being a delta.
     *
     * This optimization (that we don't dirty the BIN and we allow logging a
     * delta after removing a non-dirty slot) has one minor and one major
     * impact:
     *
     * 1. When a slot is removed for a deleted record, normally the slot and
     * the BIN will be dirty. Although it is unusual, we may encounter a
     * non-dirty slot for a deleted record. This happens if the slot could not
     * be removed by this method when a full BIN is logged, due to a lock or a
     * cursor, and we compress the full BIN later.
     *
     * 2. When a slot is removed for an expired record, it is common that the
     * slot will not be be dirty. In this case, without the optimization, the
     * removal of expired slots would cause more logging and less deltas would
     * be logged.
     *
     * @return true if all deleted and expired slots were compressed, or false
     * if one or more slots could not be compressed because we were unable to
     * obtain a lock. A false return value means "try again later".
     */
    public boolean compress(boolean compressDirtySlots) {

        /*
         * If the environment is not yet recovered we can't rely on locks
         * being set up to safeguard active data and so we can't compress
         * safely.
         */
        if (!databaseImpl.getEnv().isValid()) {
            return true;
        }

        if (nCursors() > 0) {
            throw unexpectedState();
        }

        if (isBINDelta()) {
            throw unexpectedState();
        }

        final DatabaseImpl db = getDatabase();
        final EnvironmentImpl envImpl = db.getEnv();

        final LockManager lockManager =
            envImpl.getTxnManager().getLockManager();

        boolean anyLocked = false;

        for (int i = 0; i < getNEntries(); i++) {

            if (!compressDirtySlots && isDirty(i)) {
                continue;
            }

            final boolean expired = isExpired(i);
            final boolean deleted = isDeleted(i);

            if (!deleted && !expired) {
                continue;
            }

            /*
             * We have to be able to lock the LN before we can compress the
             * entry. If we can't, then skip over it. For a deleted record, a
             * read lock is sufficient because it means the deletion has been
             * committed, and other lockers don't hold read locks on a deleted
             * record. For an expired record, a write lock is needed to prevent
             * removal of a slot for a record that is read-locked elsewhere.
             * In both cases it is more efficient to call isLockUncontended
             * than to actually lock the LN, since we would release the lock
             * immediately.
             *
             * We must be able to lock the LN even if isKnownDeleted is true,
             * because locks protect the aborts. (Aborts may execute multiple
             * operations, where each operation latches and unlatches. It's the
             * LN lock that protects the integrity of the whole multi-step
             * process.)
             *
             * For example, during abort, there may be cases where we have
             * deleted and then added an LN during the same txn.  This means
             * that to undo/abort it, we first delete the LN (leaving
             * knownDeleted set), and then add it back into the tree.  We want
             * to make sure the entry is in the BIN when we do the insert back
             * in.
             */
            final long lsn = getLsn(i);

            /* Can discard a NULL_LSN entry without locking. */
            if (lsn != DbLsn.NULL_LSN &&
                !lockManager.isLockUncontended(lsn)) {

                /*
                 * Check whether the extinct scanner has set the KD flag. If
                 * so, it is OK to delete a locked slot (since no cursor is
                 * positioned on it). getExtinctionStatus has overhead, but a
                 * KD slot should be locked infrequently.
                 */
                if (!isEntryKnownDeleted(i) ||
                    envImpl.getExtinctionStatus(db, getKey(i)) != EXTINCT) {
                    /* Try again later. */
                    anyLocked = true;
                    continue;
                }
            }

            deleteEntry(i, false /*makeDirty*/, true /*validate*/);

            /* Since we're deleting the current entry, decrement the index. */
            i--;
        }

        /*
         * If identifier key has changed, we want the bin to be marked dirty,
         * so that the change can be passed on to the parent whenever the bin
         * is flushed. getUpdatedIdKey() marks the IN/BIN dirty if the id key
         * is updated.
         */
        if (getNEntries() != 0) {
            if (IN.isIDKeyUpdateDisabledForTesting() == false) {
                getUpdatedIdKey();
            }
        }

        if (getNEntries() == 0) {
            /* This BIN is empty and expendable. */
            updateLRU(CacheMode.UNCHANGED); // TODO actually make cold
        }

        /*
         * Reduce capacity if this BIN is larger than the configured capacity,
         * and has less entries then the configured capacity. This could be due
         * to enlarging the BIN during recovery (see reconstituteBIN) or
         * because the configured capacity was changed.
         */
        final int configuredCapacity = databaseImpl.getNodeMaxTreeEntries();
        if (getMaxEntries() > configuredCapacity &&
            getNEntries() < configuredCapacity) {
            resize(configuredCapacity);
        }

        return !anyLocked;
    }

    /**
     * This method is called opportunistically at certain places where a
     * deleted slot is observed (when the slot's PendingDeleted or KnownDeleted
     * flag is set), to ensure that the slot is compressed away. This is an
     * attempt to process slots that were not compressed during the mainstream
     * record deletion process because of cursors on the BIN during compress,
     * or a crash prior to compression.
     */
    public void queueSlotDeletion(final int idx) {

        /*
         * If the next logrec for this BIN should be a BIN-delta, don't queue
         * the BIN if the deleted slot is dirty, because removing dirty BIN
         * slots prevents logging a delta.
         */
        if (isDirty(idx) && shouldLogDelta()) {
            return;
        }

        getEnv().addToCompressorQueue(this);
    }

    /* For debugging.  Overrides method in IN. */
    @Override
    boolean validateSubtreeBeforeDelete(int index) {

        assert(!isBINDelta());

        return true;
    }

    /**
     * Check if this node fits the qualifications for being part of a deletable
     * subtree. It may not have any LN children.
     *
     * We assume that this is only called under an assert.
     */
    @Override
    boolean isValidForDelete()
        throws DatabaseException {

        assert(isLatchExclusiveOwner());

        if (isBINDelta()) {
            return false;
        }

        int numValidEntries = 0;

        for (int i = 0; i < getNEntries(); i++) {
            if (!isEntryKnownDeleted(i)) {
                numValidEntries++;
            }
        }

        if (numValidEntries > 0) { // any valid entries, not eligible
            return false;
        }
        if (nCursors() > 0) {      // cursors on BIN, not eligible
            return false;
        }
        return true;               // 0 entries, no cursors
    }

    @Override
    public long compactMemory() {
        final long oldSize = inMemorySize;
        super.compactMemory();
        expirationValues = expirationValues.compact(this, EMPTY_EXPIRATION);
        creationTimes =
            creationTimes.compact(this, EMPTY_CREATION_TIMES);
        modificationTimes =
            modificationTimes.compact(this, EMPTY_MODIFICATION_TIMES);
        return oldSize - inMemorySize;
    }

    /**
     * Adds vlsnCache size to computed memory size.
     */
    @Override
    public long computeMemorySize() {

        long size = super.computeMemorySize();

        /*
         * vlsnCache, lastLoggedSizes, etc, are null only when this method is
         * called by the superclass constructor, i.e., before this class
         * constructor has run. Luckily the initial representations have a
         * memory size of zero, so we can ignore them in this case.
         */
        if (vlsnCache != null) {
            size += vlsnCache.getMemorySize();
        }

        if (expirationValues != null) {
            size += expirationValues.getMemorySize();
        }

        if (creationTimes != null) {
            size += creationTimes.getMemorySize();
        }

        if (modificationTimes != null) {
            size += modificationTimes.getMemorySize();
        }

        if (bloomFilter != null) {
            size += BINDeltaBloomFilter.getMemorySize(bloomFilter);
        }

        return size;
    }

    /* Utility method used during unit testing. */
    @Override
    protected long printMemorySize() {
        final long inTotal = super.printMemorySize();
        final long vlsnCacheOverhead = vlsnCache.getMemorySize();
        final long expirationOverhead = expirationValues.getMemorySize();
        final long createTimeOverhead = creationTimes.getMemorySize();
        final long modTimeOverhead = modificationTimes.getMemorySize();

        final long binTotal = inTotal +
            vlsnCacheOverhead + expirationOverhead + modTimeOverhead
            + createTimeOverhead;

        System.out.format("BIN: %d vlsns: %d expiration: %d"
                              + " createTimeOverhead: %d"
                              + " modTimeOverhead: %d %n",
                          binTotal, vlsnCacheOverhead, expirationOverhead,
                          createTimeOverhead, modTimeOverhead);
        return binTotal;
    }

    @Override
    protected long getFixedMemoryOverhead() {
        return MemoryBudget.BIN_FIXED_OVERHEAD;
    }

    /**
     * Try to compact or otherwise reclaim memory in this IN and return the
     * number of bytes reclaimed. For example, a BIN should evict LNs, if
     * possible.
     *
     * Used by the evictor to reclaim memory by some means short of evicting
     * the entire node.  If a positive value is returned, the evictor will
     * normally postpone full eviction of this node.
     *
     * The BIN should be latched by the caller.
     *
     * @return the number of evicted bytes.
     */
    @Override
    public long partialEviction() {

        /* Try compressing non-dirty slots. */
        final long oldMemSize = inMemorySize;
        getEnv().lazyCompress(this);
        if (oldMemSize > inMemorySize) {
            return oldMemSize - inMemorySize;
        }

        /* Try LN eviction. Return if any were evicted. */
        final long lnBytesAndStatus = evictLNs();
        if (lnBytesAndStatus > 0) {
            return lnBytesAndStatus;
        }

        /*
         * Try compacting, since evictLNs does not compact when no LNs are
         * evicted, and additionally try discarding the VLSNCache.
         */
        return (compactMemory() + discardVLSNCache());
    }

    public long discardVLSNCache() {

        final long vlsnBytes = vlsnCache.getMemorySize();

        if (vlsnBytes > 0) {

            int numEntries = getNEntries();
            for (int i = 0; i < numEntries; ++i) {
                if (isEmbeddedLN(i)) {
                    return 0;
                }
            }

            vlsnCache = EMPTY_VLSNS;
            updateMemorySize(0 - vlsnBytes);
        }

        return vlsnBytes;
    }

    /**
     * Determines whether the BIN is evictable or not. It is non-evictable if
     * it (a) has cursors registered on it, or (b) has a resident
     * non-evictable LN, which can happen only for MapLNs (see
     * MapLN.isEvictable()), or (c) is a DB allowing blind deletions and at
     * least one record is locked.
     */
    @Override
    public boolean isEvictable() {

        if (nCursors() > 0) {
            return false;
        }

        final boolean checkLocks = getDatabase().allowBlindDeletions();
        final LockManager lockManager =
            getEnv().getTxnManager().getLockManager();

        for (int i = 0; i < getNEntries(); i++) {

            final LN ln = (LN) getTarget(i);
            if (ln != null && !ln.isEvictable()) {
                return false;
            }
            if (checkLocks) {
                final long lsn = getLsn(i);
                if (lsn != DbLsn.NULL_LSN &&
                    !lockManager.isLockUncontended(lsn)) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Reduce memory consumption by evicting all LN targets. Note that this may
     * cause LNs to be logged, which will mark this BIN dirty.
     *
     * The BIN should be latched by the caller.
     *
     * @return a long number encoding (a) the number of evicted bytes, and
     * (b) whether this BIN  is evictable. (b) will be false if the BIN has
     * any cursors on it, or has any non-evictable children.
     */
    public long evictLNs()
        throws DatabaseException {

        assert isLatchExclusiveOwner() :
            "BIN must be latched before evicting LNs";

        /*
         * We can't evict an LN which is pointed to by a cursor, in case that
         * cursor has a reference to the LN object. We'll take the cheap choice
         * and avoid evicting any LNs if there are cursors on this BIN. We
         * could do a more expensive, precise check to see entries have which
         * cursors. This is something we might move to later.
         */
        if (nCursors() > 0) {
            return 0;
        }

        /* Try to evict each child LN. */
        long totalRemoved = 0;
        long numLNsEvicted = 0;

        for (int i = 0; i < getNEntries(); i++) {

            if (getTarget(i) == null) {
                continue;
            }

            long lnRemoved = evictLNInternal(i, false /*ifFetchedCold*/);

            if (lnRemoved > 0) {
                totalRemoved += lnRemoved;
                ++numLNsEvicted;
            }
        }

        /*
         * compactMemory() may decrease the memory footprint by mutating the
         * representations of the target and key sets.
         */
        if (totalRemoved > 0) {
            updateMemorySize(totalRemoved, 0);
            totalRemoved += compactMemory();
        }

        getEvictor().incNumLNsEvicted(numLNsEvicted);

        return totalRemoved;
    }

    public void evictLN(int index) {
        evictLN(index, false /*ifFetchedCold*/);
    }

    public void evictLN(int index, boolean ifFetchedCold)
        throws DatabaseException {

        final long removed = evictLNInternal(index, ifFetchedCold);

        /* May decrease the memory footprint by changing the INTargetRep. */
        if (removed > 0) {
            updateMemorySize(removed, 0);
            compactMemory();
        }
    }

    /**
     * Evict a single LN if allowed. The amount of memory freed is returned
     * and must be subtracted from the memory budget by the caller.
     *
     * @param ifFetchedCold If true, evict the LN only if it has the
     * FetchedCold flag set.
     *
     * @return number of evicted bytes or zero.
     */
    private long evictLNInternal(int index, boolean ifFetchedCold)
        throws DatabaseException {

        final Node n = getTarget(index);

        assert(n == null || n instanceof LN);

        if (n == null) {
            return 0;
        }

        final LN ln = (LN) n;

        if (ifFetchedCold && !ln.getFetchedCold()) {
            return 0;
        }

        /*
         * Don't evict MapLNs for open databases (LN.isEvictable) [#13415].
         */
        if (!ln.isEvictable()) {
            return 0;
        }

        /* Log LN if necessary. */
        logEvictedLN(index, ln);

        /* Clear target. */
        setTarget(index, null);

        return n.getMemorySizeIncludedByParent();
    }

    /**
     * Logs the LN at the given index if necessary. Currently only MapLNs are
     * logged when isCheckpointNeeded returns true, meaning that the root LSN
     * of the contained Btree has changed.
     */
    private void logEvictedLN(final int idx, final LN ln) {
        if (!(ln instanceof MapLN)) {
            return;
        }
        final MapLN mapLN = (MapLN) ln;
        if (!mapLN.getDatabase().isCheckpointNeeded()) {
            return;
        }
        final long currLsn = getLsn(idx);
        final DatabaseImpl dbImpl = getDatabase();
        final EnvironmentImpl envImpl = dbImpl.getEnv();
        /*
         * Do not lock while logging.  Locking of new LSN is performed by
         * lockAfterLsnChange. Note that MapLNs are not replicated.
         */
        final LogItem logItem = ln.log(
            envImpl, dbImpl, null /*locker*/, null /*writeLockInfo*/,
            isEmbeddedLN(idx), getKey(idx),
            getExpiration(idx), isExpirationInHours(), ln.getCreationTime(),
            ln.getModificationTime(), isTombstone(idx),
            false /*newBlindDeletion*/, isEmbeddedLN(idx),
            currLsn, getLastLoggedSize(idx),
            false /*isInsertion*/, true /*backgroundIO*/,
            ReplicationContext.NO_REPLICATE);

        /* MapLNs have no VLSN but getVLSNSequence used for consistency. */
        updateEntry(idx, logItem.lsn, ln.getVLSNSequence(), logItem.size);

        /* Lock new LSN on behalf of existing lockers. */
        CursorImpl.lockAfterLsnChange(
            dbImpl, currLsn, logItem.lsn, null /*excludeLocker*/);
    }

    /*
     * Logging support
     */

    /**
     * @see IN#getLogType
     */
    @Override
    public LogEntryType getLogType() {
        return LogEntryType.LOG_BIN;
    }

    /*
     * BIN delta support
     */

    public int getFullBinNEntries() {
        if (isBINDelta()) {
            return fullBinNEntries;
        } else {
            return nEntries;
        }
    }

    public void setFullBinNEntries(int n) {
        assert(isBINDelta(false));
        fullBinNEntries = n;
    }

    void incFullBinNEntries() {
        assert(isBINDelta());
        ++fullBinNEntries;
    }

    public int getFullBinMaxEntries() {
        if (isBINDelta()) {
            return fullBinMaxEntries;
        } else {
            return getMaxEntries();
        }
    }

    public void setFullBinMaxEntries(int n) {
        assert(isBINDelta(false));
        fullBinMaxEntries = n;
    }

    int getDeltaCapacity(int numDirtyEntries) {

        boolean blindOps =
            (getEnv().allowBlindOps() || getEnv().allowBlindPuts());

        if (isBINDelta()) {
            return getMaxEntries();
        }

        if (blindOps) {
            return (getNEntries() * databaseImpl.getBinDeltaPercent()) / 100;
        }

        return numDirtyEntries;
    }

    boolean allowBlindPuts() {
        boolean res = getEnv().allowBlindPuts();

        if (res) {
            res = res && databaseImpl.hasBtreeBinaryEqualityComparator();
            res = res && databaseImpl.hasDuplicateBinaryEqualityComparator();
        }

        return res;
    }

    /*
     * It is called in 3 cases listed below. In all cases, if blind puts are
     * not allowed, the method returns null.
     *
     * 1. A full BIN is being mutated to an in-memory delta. A new filter will
     *    be created here and will be stored in the delta by the caller.
     * 2. A full BIN is being logged as a delta. A new filter will be created
     *    here and will be written in the delta logrec by the caller.
     * 3. An in-memory BIN-delta is being logged. If the delta has a bloom
     *    filter already, that filter will be returned and written into the
     *    logrec. The delta may not have a filter already because it was read
     *    from an older-version logfile; in this case we return null.
     */
    byte[] createBloomFilter() {

        assert(bloomFilter == null || isBINDelta());

        boolean blindPuts = allowBlindPuts();

        if (!blindPuts) {
            assert(bloomFilter == null);
            return null;
        }

        if (bloomFilter != null) {
            /*
             * We are here because we are logging a delta that has a filter
             * already. We just need to log the existing filter.
             */
            return bloomFilter;
        }

        if (isBINDelta()) {
            return null;
        }

        int numKeys = getNEntries() - getNDeltas();
        int nbytes = BINDeltaBloomFilter.getByteSize(numKeys);

        byte[] bf = new byte[nbytes];

        BINDeltaBloomFilter.HashContext hc =
            new BINDeltaBloomFilter.HashContext();

        if (keyPrefix != null) {
            hc.hashKeyPrefix(keyPrefix);
        }

        for (int i = 0; i < getNEntries(); ++i) {

            if (isDirty(i)) {
                continue;
            }

            entryKeys.addToBloomFilter(i, haveEmbeddedData(i), bf, hc);
        }

        return bf;
    }

    public boolean mayHaveKeyInFullBin(byte[] key) {

        assert(isBINDelta());

        if (bloomFilter == null) {
            return true;
        }

        return BINDeltaBloomFilter.contains(bloomFilter, key);
    }

    /*
     * Used in IN.getLogSize() only
     */
    int getBloomFilterLogSize() {

        if (!allowBlindPuts()) {
            return 0;
        }

        if (isBINDelta()) {
            if (bloomFilter != null) {
                return BINDeltaBloomFilter.getLogSize(bloomFilter);
            }

            return 0;

        } else {
            assert(bloomFilter == null);
            int numKeys = getNEntries() - getNDeltas();
            return BINDeltaBloomFilter.getLogSize(numKeys);
        }
    }

    boolean isDeltaProhibited() {
        return (getProhibitNextDelta() ||
            getLastFullLsn() == DbLsn.NULL_LSN);
    }

    /**
     * Decide whether to log a full or partial BIN, depending on the ratio of
     * the delta size to full BIN size.
     *
     * Other factors are taken into account:
     * + a delta cannot be logged if the BIN has never been logged before
     * + this particular delta may have been prohibited because the cleaner is
     *   migrating the BIN or a dirty slot has been removed
     * + if there are no dirty slots, we might as well log a full BIN
     *
     * @return true if we should log the deltas of this BIN
     */
    public boolean shouldLogDelta() {

        if (isBINDelta()) {
            assert !getProhibitNextDelta();
            assert getLastFullLsn() != DbLsn.NULL_LSN;
            return true;
        }

        /* Cheapest checks first. */
        if (isDeltaProhibited()) {
            return false;
        }

        /* Must count deltas to check further. */
        final int numDeltas = getNDeltas();

        /* A delta with zero items is not valid. */
        if (numDeltas <= 0) {
            return false;
        }

        /* Check the configured BinDeltaPercent. */
        final int deltaLimit =
            (getNEntries() * databaseImpl.getBinDeltaPercent()) / 100;

        return numDeltas <= deltaLimit;
    }

    /**
     * Returns whether mutateToBINDelta can be called.
     */
    public boolean canMutateToBINDelta() {
        /*
         * The most expensive check, isEvictable, is performed last.
         *
         * TODO: If we determine it is worthwhile, we could use a specialized
         *  isEvictable method that only checks whether non-dirty slots (those
         *  that will be removed by mutation) are non-evictable or locked.
         */
        return (!isBINDelta() &&
                shouldLogDelta() &&
                isEvictable());
    }

    /**
     * Mutate to a delta (discard non-dirty entries and resize arrays).
     *
     * This method must be called with this node latched exclusively, and
     * canMutateToBINDelta must return true.
     *
     * @return the number of bytes freed.
     */
    public long mutateToBINDelta() {

        assert isLatchExclusiveOwner();
        assert canMutateToBINDelta();

        if (getInListResident()) {
            getEnv().getInMemoryINs().updateBINDeltaStat(1);
        }

        final long oldSize = getInMemorySize();
        final int nDeltas = getNDeltas();
        final int capacity = getDeltaCapacity(nDeltas);

        bloomFilter = createBloomFilter();

        initBINDelta(this, nDeltas, capacity, true);

        return oldSize - getInMemorySize();
    }

    /**
     * Replaces the contents of destBIN with the deltas in this BIN.
     */
    private void initBINDelta(
        final BIN destBIN,
        final int nDeltas,
        final int capacity,
        final boolean copyTargets) {

        long[] lsns= new long[nDeltas];
        final long[] vlsns = new long[nDeltas];
        final int[] sizes = new int[nDeltas];
        final byte[][] keys = new byte[nDeltas][];
        final byte[] states = new byte[nDeltas];
        Node[] targets = null;
        int[] expiration = null;
        long[] modTimes = null;
        long[] createTimes = null;

        if (copyTargets) {
            targets = new Node[nDeltas];
        }

        if (expirationBase != -1) {
            expiration = new int[nDeltas];
        }

        if (modificationTimesBase != -1) {
            modTimes = new long[nDeltas];
        }

        if (creationTimesBase != -1) {
            createTimes = new long[nDeltas];
        }

        int j = 0;
        for (int i = 0; i < getNEntries(); i += 1) {

            if (!isDirty(i)) {
                continue;
            }

            lsns[j] = getLsn(i);
            keys[j] = entryKeys.get(i);
            states[j] = getState(i);

            if (targets != null) {
                targets[j] = getTarget(i);
            }

            vlsns[j] = getCachedVLSN(i);
            sizes[j] = getLastLoggedSize(i);

            if (expiration != null) {
                expiration[j] = getExpiration(i);
            }

            if (modTimes != null) {
                modTimes[j] = getModificationTime(i);
            }

            if (createTimes != null) {
                createTimes[j] = getCreationTime(i);
            }
            j += 1;
        }

        /*
         * Do this before resetContent() because destBIN and "this" may be the
         * same java obj
         */
        destBIN.fullBinNEntries = getFullBinNEntries();
        destBIN.fullBinMaxEntries = getFullBinMaxEntries();

        destBIN.resetContent(
            capacity, nDeltas, lsns,
            states, keyPrefix, keys, targets,
            sizes, vlsns,
            expiration, isExpirationInHours(),
            createTimes,
            modTimes);

        destBIN.setBINDelta(true);

        destBIN.compactMemory();
    }

    /**
     * Replaces the contents of this BIN with the given contents.
     * Used in mutating a full BIN to a BIN-delta or for creating
     * a new BIN delta with the given content.
     */
    private void resetContent(
        final int capacity,
        final int newNEntries,
        final long[] lsns,
        final byte[] states,
        final byte[] keyPrefix,
        final byte[][] keys,
        final Node[] targets,
        final int[] loggedSizes,
        final long[] vlsns,
        final int[] expiration,
        final boolean expirationInHours,
        final long[] createTimes,
        final long[] modTimes) {

        updateRepCacheStats(false);

        nEntries = newNEntries;
        entryLsns = new long[capacity];
        this.keyPrefix = keyPrefix;
        entryKeys = new INKeyRep.Default(capacity);
        entryTargets = INTargetRep.NONE;
        vlsnCache = EMPTY_VLSNS;
        lastLoggedSizes = EMPTY_LAST_LOGGED_SIZES;
        expirationValues = EMPTY_EXPIRATION;
        expirationBase = -1;
        modificationTimes = EMPTY_MODIFICATION_TIMES;
        modificationTimesBase = -1;
        creationTimes = EMPTY_CREATION_TIMES;
        creationTimesBase = 1;
        updateRepCacheStats(true);

        entryStates = new byte[capacity];

        for (int i = 0; i < newNEntries; i += 1) {

            entryLsns[i] = lsns[i];
            entryKeys = entryKeys.set(i, keys[i], this);
            entryStates[i] = states[i];

            if (targets != null) {
                entryTargets = entryTargets.set(i, targets[i], this);
            }

            if (expiration != null) {
                setExpiration(i, expiration[i], expirationInHours);
            }

            if (modTimes != null) {
                setModificationTime(i, modTimes[i]);
            }

            if (createTimes != null) {
                setCreationTime(i, createTimes[i]);
            }
            setLastLoggedSizeUnconditional(i, loggedSizes[i]);
            setCachedVLSNUnconditional(i, vlsns[i]);
        }

        updateMemorySize(inMemorySize, computeMemorySize());
    }

    /**
     * Fetch the full BIN and apply the deltas in this BIN to it, then use the
     * merged result to replace the contents of this BIN.
     *
     * This method must be called with this node latched exclusively. If 'this'
     * is not a delta, this method does nothing.
     */
    @Override
    public void mutateToFullBIN(boolean leaveFreeSlot) {

        if (!isBINDelta()) {
            return;
        }

        final BIN fullBIN = fetchFullBIN(databaseImpl);

        mutateToFullBIN(fullBIN, leaveFreeSlot);

        getEvictor().incFullBINMissStats();
    }

    /**
     * Used for mutating to a full BIN when passing a fullBIN that was read
     * using a FileReader. We must be sure to fill in the full lsn/size info,
     * since it used by BIN.resetContent.
     */
    public void mutateToFullBIN(BIN fullBIN,
                                boolean leaveFreeSlot,
                                long logLsn,
                                WholeEntry wholeEntry) {

        fullBIN.setLastFullLsnAndSize(
            logLsn, wholeEntry.getHeader().getEntrySize());

        mutateToFullBIN(fullBIN, leaveFreeSlot);
    }

    @Override
    public void mutateToFullBIN(boolean leaveFreeSlot,
                                CacheMode cacheMode) {

        if (!isBINDelta()) {
            return;
        }
        mutateToFullBIN(leaveFreeSlot);
        
        if (cacheMode == CacheMode.UNCHANGED) {
             setFetchedCold(true);
        }
    }

    /**
     * Mutates this delta to a full BIN by applying this delta to the fullBIN
     * param and then replacing this BIN's contents with it.
     *
     * This method must be called with this node latched exclusively. 'this'
     * must be a delta.
     *
     * After mutation, the full BIN is compressed and compacted. The
     * compression is particularly important, since BIN-deltas in cache cannot
     * be compressed.
     *
     * The method is public because it is called directly from FileProcessor
     * when it finds a BIN that must be migrated. In that case, fullBIN is a
     * full BIN that has just been read from the log, and it is not part of
     * the memory-resident tree.
     */
    public void mutateToFullBIN(BIN fullBIN, boolean leaveFreeSlot) {

        assert isLatchExclusiveOwner();
        assert isBINDelta() : this;

        byte[][] keys = null;
        int i = 0;

        if (cursorSet != null) {
            keys = new byte[cursorSet.size()][];

            for (CursorImpl cursor : cursorSet) {
                final int index = cursor.getIndex();
                if (index >= 0 && index < getNEntries()) {
                    keys[i] = cursor.getCurrentKey(true/*isLatched*/);
                }
                ++i;
            }
        }

        reconstituteBIN(databaseImpl, fullBIN, leaveFreeSlot);

        resetContent(fullBIN);

        setBINDelta(false);

        /*
         * The fullBIN identifierKey may have changed when reconstituteBIN
         * called BIN.compress. We cannot call setIdentifierKey in resetContent
         * because assert(!isBINDelta()) will fail, so call it here.
         */
        setIdentifierKey(fullBIN.getIdentifierKey(), false);

        if (cursorSet != null) {

            i = 0;
            for (CursorImpl cursor : cursorSet) {

                if (keys[i] != null) {
                    /*
                     * Do not ask for an exact match from findEntry because if
                     * the cursor was on a KD slot, findEntry would return -1.
                     */
                    int index = findEntry(keys[i], true, false);

                    if ((index & IN.EXACT_MATCH) == 0) {
                        throw unexpectedState(
                            getEnv(), "Failed to reposition cursor during " +
                            "mutation of a BIN delta to a full BIN");
                    }

                    index &= ~IN.EXACT_MATCH;

                    assert(index >= 0 && index < getNEntries());
                    cursor.setIndex(index);
                }
                ++i;
            }
        }

        getEnv().lazyCompress(this);
        compactMemory();

        if (getInListResident()) {
            getEnv().getInMemoryINs().updateBINDeltaStat(-1);
        }
    }

    private BIN fetchFullBIN(DatabaseImpl dbImpl) {

        final EnvironmentImpl envImpl = dbImpl.getEnv();
        final long lsn = getLastFullLsn();

        if (lsn == DbLsn.NULL_LSN) {
            throw unexpectedState(makeFetchErrorMsg(
                "fetchFullBIN", "NULL_LSN as LastFullLsn", lsn, -1));
        }
        checkLsnInActiveFile("fetchFullBIN", envImpl, lsn, -1);

        try {
            final WholeEntry wholeEntry = envImpl
                .getLogManager()
                .getLogEntryAllowInvisibleAtRecovery(
                    lsn, getLastFullLogSize());

            final BIN bin = (BIN) wholeEntry.getEntry().getMainItem();

            /* Fill in full lsn/size info, used by resetContent. */
            bin.setLastFullLsnAndSize(
                lsn, wholeEntry.getHeader().getEntrySize());

            return bin;

        } catch (FileNotFoundException e) {
            /* Always throws an exception. */
            handleCleanedIN(e, lsn, -1);
            return null;

        } catch (ErasedException e) {
            /* Always throws an exception. */
            handleErasedIN(e, lsn, -1);
            return null;

        } catch (EnvironmentFailureException e) {
            e.addErrorMessage(makeFetchErrorMsg(
                "fetchFullBIN", null, lsn, -1));
            throw e;

        } catch (RuntimeException e) {
            throw new EnvironmentFailureException(
                envImpl, EnvironmentFailureReason.LOG_INTEGRITY,
                makeFetchErrorMsg("fetchFullBIN", e.toString(), lsn, -1), e);
        }
    }

    /**
     * Replaces the contents of this BIN with the contents of the given BIN,
     * including lsns, states, keys and targets.  Key prefixing and key/target
     * representations will also be those of the given BIN.
     */
    private void resetContent(final BIN other) {

        updateRepCacheStats(false);

        nEntries = other.nEntries;

        entryLsns = other.entryLsns;

        keyPrefix = other.keyPrefix;
        entryKeys = other.entryKeys;

        entryTargets = other.entryTargets;

        entryStates = other.entryStates;

        lastLoggedSizes = other.lastLoggedSizes;

        expirationValues = other.expirationValues;
        expirationBase = other.expirationBase;
        setExpirationInHours(other.isExpirationInHours());

        creationTimes = other.creationTimes;
        creationTimesBase = other.creationTimesBase;

        modificationTimes = other.modificationTimes;
        modificationTimesBase = other.modificationTimesBase;

        vlsnCache = other.vlsnCache;

        bloomFilter = null;

        /*
         * Cache size in case it is missing or inaccurate. Do this
         * conditionally in case we neglect to set the last full lsn/size on
         * the source BIN.
         */
        if (other.getLastFullLsn() != DbLsn.NULL_LSN &&
            other.getLastFullLsn() != 0) {
            setLastFullLsnAndSize(
                other.getLastFullLsn(), other.getLastFullLogSize());
        }

        updateMemorySize(inMemorySize, computeMemorySize());

        updateRepCacheStats(true);
    }

    private void resize(final int newCapacity) {

        assert newCapacity >= getNEntries();

        updateRepCacheStats(false);

        if (entryLsns != null) {
            entryLsns = Arrays.copyOfRange(entryLsns, 0, newCapacity);
        }
        if (entryStates != null) {
            entryStates = Arrays.copyOfRange(entryStates, 0, newCapacity);
        }

        entryKeys = entryKeys.resize(newCapacity);
        entryTargets = entryTargets.resize(newCapacity);
        lastLoggedSizes = lastLoggedSizes.resize(newCapacity);
        expirationValues = expirationValues.resize(newCapacity);
        modificationTimes = modificationTimes.resize(newCapacity);
        creationTimes = creationTimes.resize(newCapacity);

        vlsnCache = vlsnCache.resize(newCapacity);

        updateMemorySize(inMemorySize, computeMemorySize());

        updateRepCacheStats(true);
    }

    /**
     * Create a BIN by fetching its most recent full version from the log and
     * applying to it the deltas in this BIN delta. The new BIN is not added
     * to the INList or the BTree.
     *
     * @return the full BIN with deltas applied.
     */
    public BIN reconstituteBIN(DatabaseImpl dbImpl) {
        final BIN fullBIN = fetchFullBIN(dbImpl);
        reconstituteBIN(dbImpl, fullBIN, false /*leaveFreeSlot*/);
        return fullBIN;
    }

    /**
     * Given a full version BIN, apply to it the deltas in this BIN delta. The
     * fullBIN will then be complete, but its memory will not be compacted.
     *
     * Called from mutateToFullBIN() above and from SortedLSNTreewalker.
     *
     * @param leaveFreeSlot should be true if a slot will be inserted into the
     * resulting full BIN, without first checking whether the full BIN must be
     * split, and performing the split if necessary. If this param is true, the
     * returned BIN will contain at least one free slot. If this param is
     * false, a BIN with no free slots may be returned. For example, it is
     * important that false is passed when a split will be performed, since if
     * true were passed, the BIN would grow beyond its bounds unnecessarily.
     */
    public void reconstituteBIN(
        DatabaseImpl dbImpl,
        BIN fullBIN,
        boolean leaveFreeSlot) {

        fullBIN.setDatabase(dbImpl);
        fullBIN.latch(CacheMode.UNCHANGED);

        try {
            if (databaseImpl == null) {
                setDatabase(dbImpl);
            }

            /*
             * Compress the full BIN before applying deltas, to handle the
             * following scenario: Non-dirty slots were compressed away
             * earlier, leaving room for inserted records, and a delta was
             * logged with the inserted records. The full version of the BIN
             * (after compression) was not logged, because the BIN is not
             * dirtied when non-dirty slots were compressed away. If we don't
             * compress here, there may not be room in the original BIN for the
             * slots inserted when applying the deltas.
             *
             * However, during recovery we can't compress because locking is
             * not used during recovery, and the compressor may delete a slot
             * for a record that is part of an active transaction. In addition,
             * even when compression is performed here, it is possible that it
             * doesn't compress all deleted/expired slots that were compressed
             * originally in the scenario described, for one of the following
             * reasons:
             *
             *  + The record is locked temporarily by a read operation that
             *    will skip the record. Note that the compressor uses
             *    non-blocking locking.
             *
             *  + If expiration has been disabled, or the system clock has been
             *    changed, slots that were expired originally may not be
             *    expired now.
             *
             * Therefore, in all cases we enlarge the BIN if necessary to hold
             * all slots to be inserted when applying the delta. An extra slot
             * is added if leaveFreeSlot is true, to handle cases
             * where mutation to a full BIN is performed after calling
             * Tree.searchSplitsAllowed, or one of the methods that calls it
             * such as Tree.findBinForInsert and Tree.getParentBINForChildLN.
             * If the search returns a BIN-delta without splitting, and then we
             * must mutate to full BIN in order to insert, because blind
             * insertions do not apply, then the scenario described can occur.
             *
             * If the BIN is enlarged, we add it to the compressor queue so it
             * will be shrunk back down to the Database's configured maxEntries
             * during normal compression.
             */
            if (!dbImpl.getEnv().isInInit()) {
                fullBIN.compress(false /*compressDirtySlots*/);
            }
            int nInsertions = leaveFreeSlot ? 1 : 0;
            for (int i = 0; i < getNEntries(); i += 1) {
                final int foundIndex = fullBIN.findEntry(
                    getKey(i), true, false);
                if (foundIndex < 0 || (foundIndex & IN.EXACT_MATCH) == 0) {
                    nInsertions += 1;
                }
            }
            final int maxEntries = nInsertions + fullBIN.getNEntries();
            if (maxEntries > fullBIN.getMaxEntries()) {
                fullBIN.resize(maxEntries);
                dbImpl.getEnv().addToCompressorQueue(fullBIN);
            }

            /* Process each delta. */
            for (int i = 0; i < getNEntries(); i++) {

                assert isDirty(i) : this;

                fullBIN.applyDelta(
                    getKey(i), getEmbeddedData(i), getLsn(i), getState(i),
                    getLastLoggedSize(i),
                    getCachedVLSN(i), getTarget(i),
                    getExpiration(i), isExpirationInHours(),
                    getCreationTime(i),
                    getModificationTime(i));
            }

            /*
             * The applied deltas will leave some slots dirty, which is
             * necessary as a record of changes that will be included in the
             * next delta.  However, the BIN itself should not be dirty,
             * because this delta is a persistent record of those changes.
             */
            fullBIN.setDirty(false);
        } finally {
            fullBIN.releaseLatch();
        }
    }

    /**
     * Apply (insert, update) a given delta slot in this full BIN.
     */
    void applyDelta(
        final byte[] key,
        final byte[] embData,
        final long lsn,
        final byte state,
        final int lastLoggedSize,
        final long vlsn,
        final Node child,
        final int expiration,
        final boolean expirationInHours,
        final long creationTime,
        final long modificationTime) {

        /*
         * The delta is the authoritative version of the entry. In all cases,
         * it should supersede the entry in the full BIN.  This is true even if
         * the BIN Delta's entry is knownDeleted or if the full BIN's version
         * is knownDeleted. Therefore we use the flavor of findEntry that will
         * return a knownDeleted entry if the entry key matches (i.e. true,
         * false) but still indicates exact matches with the return index.
         * findEntry only returns deleted entries if third arg is false, but we
         * still need to know if it's an exact match or not so indicateExact is
         * true.
         */
        int foundIndex = findEntry(key, true, false);

        if (foundIndex >= 0 && (foundIndex & IN.EXACT_MATCH) != 0) {

            foundIndex &= ~IN.EXACT_MATCH;

            /*
             * The entry exists in the full version, update it with the delta
             * info.  Note that all state flags should be restored [#22848].
             */
            applyDeltaSlot(
                foundIndex, child, lsn, lastLoggedSize, state, key, embData);

        } else {

            /*
             * The entry doesn't exist, insert the delta entry. We insert the
             * entry even when it is known or pending deleted, since the
             * deleted (and dirty) entry will be needed to log the next delta.
             * [#20737]
             */
            final int result = insertEntryInternal(
                child, key, embData, lsn, state, false /*blindInsertion*/);

            assert (result & INSERT_SUCCESS) != 0;
            foundIndex = result & ~IN.INSERT_SUCCESS;

            setLastLoggedSizeUnconditional(foundIndex, lastLoggedSize);
        }

        setCachedVLSNUnconditional(foundIndex, vlsn);
        setExpiration(foundIndex, expiration, expirationInHours);
        setCreationTime(foundIndex, creationTime);
        setModificationTime(foundIndex, modificationTime);
    }

    /*
     * DbStat support.
     */
    @Override
    void accumulateStats(TreeWalkerStatsAccumulator acc) {
        acc.processBIN(this, getNodeId(), getLevel());
    }
}
