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

package com.sleepycat.je.rep.vlsn;

import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.utilint.VLSN;

public class VLSNRange {

    /* On-disk version. */
    private static final int VERSION = 1;
    public static final long RANGE_KEY = -1L;
    static final VLSNRange EMPTY = new VLSNRange(NULL_VLSN,
                                                 NULL_VLSN,
                                                 NULL_VLSN,
                                                 NULL_VLSN);

    /*
     * Information about the range of contiguous VLSN entries on this node.
     * All the range values must be viewed together, to ensure a consistent set
     * of values.
     */
    private final long first;
    private final long last;
    private final byte commitType = LogEntryType.LOG_TXN_COMMIT.getTypeNum();
    private final byte abortType = LogEntryType.LOG_TXN_ABORT.getTypeNum();

    /*
     * VLSN of the last log entry in our VLSN range that can serve as a sync
     * matchpoint.
     *
     * Currently lastSync and lastTxnEnd are the same value because a
     * sync point is defined as a commit or abort; however, in the future the
     * Matchpoint log entry may also be used for sync points and and then
     * lastSync may be ahead of lastTxnEnd.
     */
    private final long lastSync;
    private final long lastTxnEnd;

    private VLSNRange(long first,
                      long last,
                      long lastSync,
                      long lastTxnEnd) {
        this.first = first;
        this.last = last;
        this.lastSync = lastSync;
        this.lastTxnEnd = lastTxnEnd;
    }

    /**
     * When the range is written out by the VLSNTracker, we must always be sure
     * to update the tracker's lastVSLNOnDisk field. Return the last VLSN in 
     * the range as part of this method, to help ensure that update.
     * @param envImpl
     * @param dbImpl
     * @param txn
     */
    long writeToDatabase(final EnvironmentImpl envImpl,
                         final DatabaseImpl dbImpl,
                         Txn txn) {

        VLSNRangeBinding binding = new VLSNRangeBinding();
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        LongBinding.longToEntry(RANGE_KEY, key);
        binding.objectToEntry(this, data);

        Cursor c = null;
        try {
            c = DbInternal.makeCursor(dbImpl, 
                                      txn,
                                      CursorConfig.DEFAULT);
            DbInternal.getCursorImpl(c).setAllowEviction(false);

            OperationStatus status = c.put(key, data);
            if (status != OperationStatus.SUCCESS) {
                throw EnvironmentFailureException.unexpectedState
                    (envImpl, "Unable to write VLSNRange, status = " + status,
                    ((RepImpl)envImpl).getVLSNIndex());
            }
        } finally {
            if (c != null) {
                c.close();
            }
        }
        return last;
    }

    public static VLSNRange readFromDatabase(DatabaseEntry data) {
        VLSNRangeBinding binding = new VLSNRangeBinding();
        VLSNRange range = binding.entryToObject(data);

        return range;
    }

    public long getFirst() {
        return first;
    }

    public long getLast() {
        return last;
    }

    public long getLastSync() {
        return lastSync;
    }

    public long getLastTxnEnd() {
        return lastTxnEnd;
    }

    /**
     * Return the VLSN that should come after the lastVLSN.
     */
    long getUpcomingVLSN() {
        return VLSN.getNext(last);
    }

    /**
     * @return true if this VLSN is within the range described by this class.
     */
    public boolean contains(final long vlsn) {
        if (first == NULL_VLSN) {
            return false;
        }

        if ((first <= vlsn) && (last >= vlsn)) {
            return true;
        }

        return false;
    }

    /**
     * A new VLSN->LSN mapping has been registered in a bucket. Update the
     * range accordingly.
     */
    VLSNRange getUpdateForNewMapping(final long newValue,
                                     final byte entryTypeNum) {
        long newFirst = first;
        long newLast = last;
        long newLastSync = lastSync;
        long newLastTxnEnd = lastTxnEnd;

        if (first == NULL_VLSN || first > newValue) {
            newFirst = newValue;
        }

        if (last < newValue) {
            newLast = newValue;
        }

        if (LogEntryType.isSyncPoint(entryTypeNum)) {
            if (lastSync < newValue) {
                newLastSync = newValue;
            }
        }

        if ((entryTypeNum == commitType) || (entryTypeNum == abortType)) {
            if (lastTxnEnd < newValue) {
                newLastTxnEnd = newValue;
            }
        }

        return new VLSNRange(newFirst, newLast, newLastSync, newLastTxnEnd);
    }

    /**
     * Incorporate the information in "other" in this range.
     */
    VLSNRange getUpdate(final VLSNRange other) {
        long newFirst = getComparison(first, other.first,
                                      other.first < first);
        long newLast = getComparison(last, other.last,
                                     other.last > last);
        long newLastSync =
            getComparison(lastSync, other.lastSync,
                          other.lastSync > lastSync);
        long newLastTxnEnd =
            getComparison(lastTxnEnd, other.lastTxnEnd,
                          other.lastTxnEnd > lastTxnEnd);
        return new VLSNRange(newFirst, newLast, newLastSync, newLastTxnEnd);
    }

    /**
     * The "other" range is going to be appended to this range.
     */
    VLSNRange merge(final VLSNRange other) {
        long newLast = getComparison(last, other.last, true);
        long newLastSync = getComparison(lastSync, other.lastSync, true);
        long newLastTxnEnd = getComparison(lastTxnEnd, other.lastTxnEnd, true);
        return new VLSNRange(first, newLast, newLastSync, newLastTxnEnd);
    }

    /*
     * We can assume that deleteStart.getPrev() is either NULL_VLSN or is
     * on a sync-able boundary. We can also assume that lastTxnEnd has not
     * been changed. And lastly, we can assume that this range is not empty,
     * since that was checked earlier on.
     */
    VLSNRange shortenFromEnd(final long deleteStart) {
        long newLast = VLSN.getPrev(deleteStart);

        assert newLast >= lastTxnEnd :
        "Can't truncate at " + newLast +
            " because it overwrites a commit at " +  lastTxnEnd;

        if (newLast == NULL_VLSN) {
            return new VLSNRange(NULL_VLSN, NULL_VLSN,
                                 NULL_VLSN, NULL_VLSN);
        }
        return new VLSNRange(first, newLast, newLast, lastTxnEnd);
    }

    /*
     * @return an new VLSNRange which starts at deleteEnd.getNext()
     */
    VLSNRange shortenFromHead(final long deleteEnd) {

        long newFirst = INVALID_VLSN;
        long newLast = last;
        if (deleteEnd == last) {
            newFirst = NULL_VLSN;
            newLast = NULL_VLSN;
        } else {
            newFirst = VLSN.getNext(deleteEnd);
        }

        /* We shouldn't be truncating the last sync */
        assert (lastSync == NULL_VLSN ||
                lastSync >= newFirst) :
            "Can't truncate lastSync= " + lastSync + " deleteEnd=" + deleteEnd;

        long newTxnEnd = lastTxnEnd > newFirst ?
            lastTxnEnd : NULL_VLSN;

        return new VLSNRange(newFirst, newLast, lastSync, newTxnEnd);
    }

    /**
     * Compare two VLSNs, normalizing for NULL_VLSN. If one of them is
     * NULL_VLSN, return the other one. If neither are NULL_VLSN, use the
     * result of the comparison, expressed as the value of "better" to decide
     * which one to return. If "better" is true, return "otherVLSN".
     */
    private long getComparison(final long thisVLSN,
                               final long otherVLSN,
                               final boolean better) {
        if (thisVLSN == NULL_VLSN) {
            return otherVLSN;
        }

        if (otherVLSN == NULL_VLSN) {
            return thisVLSN;
        }

        if (better) {
            return otherVLSN;
        }

        return thisVLSN;
    }

    boolean isEmpty() {
        return first == NULL_VLSN;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("first=").append(first);
        sb.append(" last=").append(last);
        sb.append(" sync=").append(lastSync);
        sb.append(" txnEnd=").append(lastTxnEnd);
        return sb.toString();
    }

    /**
     * Marshals a VLSNRange to a byte buffer to store in the database.
     */
    static class VLSNRangeBinding extends TupleBinding<VLSNRange> {

        @Override
        public VLSNRange entryToObject(final TupleInput ti) {
            int onDiskVersion = ti.readPackedInt();
            if (onDiskVersion != VERSION) {
                throw EnvironmentFailureException.unexpectedState
                    ("Don't expect version diff " +
                     "on_disk=" + onDiskVersion +
                     " source=" +
                     VERSION);
            }

            VLSNRange range =
                new VLSNRange(ti.readPackedLong(), // first
                              ti.readPackedLong(), // last
                              ti.readPackedLong(), // lastSync
                              ti.readPackedLong()); // lastTxnEnd
            return range;
        }

        @Override
        public void objectToEntry(final VLSNRange range,
                                  final TupleOutput to) {
            /* No need to store the file number -- that's the key */
            to.writePackedInt(VERSION);
            to.writePackedLong(range.getFirst());
            to.writePackedLong(range.getLast());
            to.writePackedLong(range.getLastSync());
            to.writePackedLong(range.getLastTxnEnd());
        }
    }

    boolean verify(final boolean verbose) {
        if (first == NULL_VLSN) {
            if (!(last == NULL_VLSN &&
                  (lastSync == NULL_VLSN) &&
                  (lastTxnEnd == NULL_VLSN))) {
                if (verbose) {
                    System.out.println("Range: All need to be NULL_VLSN " +
                                       this);
                }
                return false;
            }
        } else {
            if (first > last) {
                if (verbose) {
                    System.out.println("Range: first > last " + this);
                }
                return false;
            }

            if (lastSync != NULL_VLSN) {
                if (lastSync > last) {
                    if (verbose) {
                        System.out.println("Range: lastSync > last " + this);
                    }
                    return false;
                }
            }

            if (lastTxnEnd != NULL_VLSN) {
                if (lastTxnEnd > last) {
                    if (verbose) {
                        System.out.println("Range: lastTxnEnd > last " + this);
                    }
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * @return true if subsetRange is a subset of this range.
     */
    boolean verifySubset(final boolean verbose, final VLSNRange subsetRange) {
        if (subsetRange == null) {
            return true;
        }

        if ((subsetRange.getFirst() == NULL_VLSN) &&
            (subsetRange.getLast() == NULL_VLSN) &&
            (subsetRange.getLastSync() == NULL_VLSN) &&
            (subsetRange.getLastTxnEnd() == NULL_VLSN)) {
            return true;
        }
    
        if (first > subsetRange.getFirst()) {
            if (verbose) {
                System.out.println("Range: subset must be LTE: this=" + this +
                                   " subset=" + subsetRange);
            }
            return false;
        }

        if (first == NULL_VLSN) {
            return true;
        }

        if (last < subsetRange.getLast()) {
            if (verbose) {
                System.out.println("Range: last must be GTE: this=" + this +
                                   " subsetRange=" + subsetRange);
            }
            return false;
        }
        return true;
    }
}
