/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.kv.impl.api.ops;

import static oracle.kv.impl.api.ops.InternalOperationHandler.MIN_READ;
import static oracle.kv.impl.api.ops.InternalOperationHandler.getStorageSize;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;

import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.TxnUtil;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.DbInternal.Search;
import com.sleepycat.je.Environment;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.ReadOptions;
import com.sleepycat.je.ScanFilter;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.dbi.RangeConstraint;

/**
 * A class to encapsulate iteration over JE records for KV methods that
 * require iteration.
 *
 * The usage pattern is:  <pre>
 *   Scanner scanner = new Scanner(...);
 *   DatabaseEntry keyEntry = scanner.getKey();
 *   DatabaseEntry dataEntry = scanner.getData();
 *   try {
 *     while (scanner.next()) {
 *        // do things with keyEntry, dataEntry
 *     }
 *   } finally {
 *     scanner.close();  // closes cursor
 *   }                    </pre>
 *
 * Any security-relevant operations are performed by callers. Batching is
 * also handled by callers.
 *
 * See below for more information on state.
 */
public class Scanner {

    /** Lowest possible value for a serialized key character. */
    private static final byte MIN_VALUE_BYTE = ((byte) 1);
    /** Lowest possible value for a serialized key character. */
    private static final char MIN_VALUE_CHAR = ((char) 1);

    /**
     * The max value for a serialized byte is 0xFE and the min value is
     * 0xFF (it sorts before zero). See comments at the top of Key.java.
     */
    private static final int MIN_KEY_BYTE = 0xFF;
    private static final int MAX_KEY_BYTE = 0xFE;

    /** Minimum possible key. */
    private static final byte[] MIN_KEY = new byte[0];

    /** Same key comparator as used for KV keys */
    static final Comparator<byte[]> KEY_BYTES_COMPARATOR =
        new Key.BytesComparator();

    private final InternalOperation op;
    private final byte[] parentKey;
    private final boolean majorPathComplete;
    private final KeyRange subRange;
    private final Direction direction;
    private final byte[] resumeKey;

    /*
     * If false, resume will start at the resume key rather than moving to
     * the 1st key after the resume key. It may be false only in the case of
     * a query containing a NESTED TABLES clause, or inner joins, or if a
     * SizeLimitException is thrown during query processing.
     */
    private final boolean moveAfterResumeKey;

    private final LockMode lockMode;
    private final boolean includeParent;
    private final boolean allDescendants;
    private final Database db;
    private final Cursor cursor;
    private final int nChildComponents;

    /*
     * These could be passed in by callers if that makes more sense. It'd avoid
     * the need for getters to access the keys and data.
     */
    private final DatabaseEntry keyEntry;
    private final DatabaseEntry dataEntry;

    /*
     * Members that are relatively static, but will change during the first
     * couple of calls to next().
     *
     * The State is used by next() to handle the transitions from a
     * not-yet-started scan to looking for the parent, to the steady-state
     * iteration loop. These states are:
     * INIT -- no calls to next() have been made. When called in this state the
     * Scanner is initialized and the parent search is done if necessary.
     * FIRST_DESC -- this state indicates that the parent search has been done
     * and the iteration is ready to find the first descendant (either forward
     * or reverse)
     * ALL -- this is the steady state for the iteration where most calls are
     * JE getNext()/getPrev().
     */
    private enum State { INIT, FIRST_DESC, ALL }
    private State state;

    private boolean parentFound;
    private byte[] keyPrefix;
    private byte[] searchInitKey;
    private RangeConstraint rangeConstraint;
    private OperationResult result;

    /* Set to false if disable charging cost for reading key, default is true */
    private boolean chargeKeyRead;

    /*
     * If true, subtracts key read cost(MIN_READ) when charge the read cost of
     * current record.
     */
    private boolean subtractKeyReadCost;

    /*
     * Construction.
     *
     * @param txn is the current transaction. It is used to create the cursor.
     *
     * @param partitionId the id of the partition being iterated. This
     * corresponds to a JE database.
     *
     * @param repNode the current RepNode; used to get the database.
     *
     * @param parentKey if not null, all keys retrieved by this scanner must have
     * the given byte array as a prefix.
     *
     * @param majorPathComplete is true if the parentKey's major path is
     * complete and therefore the subRange applies to the next minor component,
     * or false if the major path is incomplete (there is no minor path) and
     * therefore the subRange applies to the next major component.  Must be
     * true for multiGet iteration and false for store iteration.
     *
     * @param subRange used for ranged iteration. May be null.
     *
     * @param the depth to use for iteration. For tables this is always
     * Depth.PARENT_AND_CHILDREN.
     *
     * @param direction is the direction of the scan. This must always be
     * Direction.FORWARD or Direction.REVERSE.
     *
     * @param resumeKey is the key after which to resume the iteration of
     * descendants, or null to start at the parent.  To resume the iteration
     * where it left off, pass the last key returned.
     *
     * @param cursorConfig the CursorConfig to use for the scan.  Should be
     * DEFAULT for a non-iterator operation and READ_COMMITTED for an iterator
     * operation.
     *
     * @param lockMode the LockMode to use for the iteration.
     *
     * A Scanner instance must be closed using Scanner.close() or the cursor
     * that is created on construction will remain open.
     */
    public Scanner(InternalOperation op,
                   Transaction txn,
                   PartitionId partitionId,
                   RepNode repNode,
                   byte[] parentKey,
                   boolean majorPathComplete,
                   KeyRange subRange,
                   Depth depth,
                   Direction direction,
                   byte[] resumeKey,
                   boolean moveAfterResumeKey,
                   CursorConfig cursorConfig,
                   LockMode lockMode,
                   boolean keyOnly) {

        this.op = op;
        this.parentKey = parentKey;
        this.majorPathComplete = majorPathComplete;
        this.direction = direction;
        this.resumeKey = resumeKey;
        this.moveAfterResumeKey = moveAfterResumeKey;
        this.lockMode = lockMode;

        if (subRange != null &&
            subRange.isPrefix() &&
            subRange.getStart().length() == 0) {
            this.subRange = null;
        } else {
            this.subRange = subRange;
        }

        includeParent = (resumeKey == null &&
                         (depth == Depth.PARENT_AND_CHILDREN ||
                          depth == Depth.PARENT_AND_DESCENDANTS));

        allDescendants = (depth == Depth.DESCENDANTS_ONLY ||
                          depth == Depth.PARENT_AND_DESCENDANTS);

        assert (direction == Direction.FORWARD ||
                direction == Direction.REVERSE);

        db = repNode.getPartitionDB(partitionId);

        keyEntry = new DatabaseEntry();
        dataEntry = new DatabaseEntry();
        if (keyOnly) {
            dataEntry.setPartial(0, 0, true);
        }

        cursor = db.openCursor(txn, cursorConfig);

        state = State.INIT;
        parentFound = false;
        nChildComponents = (parentKey != null ?
                            (Key.countComponents(parentKey) + 1) : 1);

        /* Charge the cost of reading key */
        chargeKeyRead = true;
    }

    /**
     * Returns the key
     */
    public final DatabaseEntry getKey() {
        return keyEntry;
    }

    /**
     * Returns the data
     */
    public final DatabaseEntry getData() {
        return dataEntry;
    }

    /**
     * Returns the cursor
     */
    public final Cursor getCursor() {
        return cursor;
    }

    public final LockMode getLockMode() {
        return lockMode;
    }

    /**
     * This is called to lock the current data entry for key-only scans.
     *
     * Note that tombstones are excluded.
     *
     * This method is called after getNext() method, because the data entry is
     * skipped to read in this call, and the read cost of reading key was
     * recorded in getNext() method, so no read cost is needed to record here.
     */
    public boolean getCurrent() {
        assert dataEntry.getPartial();
        ReadOptions jeOptions =
            InternalOperationHandler.DEFAULT_EXCLUDE_TOMBSTONES;
        result = cursor.get(keyEntry, dataEntry, Get.CURRENT, jeOptions);
        return (result != null);
    }

    /**
     * This is called if the dataEntry is partial and the data needs to be
     * locked and fetched.
     *
     */
    public boolean getLockedData(DatabaseEntry newEntry,
                                 boolean includeTombstones,
                                 boolean chargeRowRead) {
        assert !newEntry.getPartial();
        final ReadOptions jeOpt = includeTombstones ?
            InternalOperationHandler.DEFAULT_INCLUDE_TOMBSTONES :
            InternalOperationHandler.DEFAULT_EXCLUDE_TOMBSTONES;
        result = cursor.get(keyEntry, newEntry, Get.CURRENT, jeOpt);
        return processResult(result, newEntry, chargeRowRead, false);
    }

    /**
     * Returns the JE database used for the Scanner.
     */
    public Database getDatabase() {
        return db;
    }

    /**
     * Returns the expiration time of the current valid result if non-null,
     * otherwise 0.
     *
     * This means that the caller must have received a true result from
     * one of the navigational interfaces indicating there's a current
     * record.
     */
    public long getExpirationTime() {
        return (result != null ? result.getExpirationTime() : 0);
    }

    /**
     * Returns the creation time of the current valid result if non-null,
     * otherwise 0.
     */
    public long getCreationTime() {
        return (result != null ? result.getCreationTime() : 0);
    }

    public long getModificationTime() {
        return (result != null ? result.getModificationTime() : 0);
    }

    /**
     * Returns tombstone property of the record in result.
     */
    public boolean isTombstone() {
        return (result != null && result.isTombstone());
    }

    /**
     * Returns the current OperationResult. If the most recent operation failed
     * to find a record this will be null.
     */
    public OperationResult getResult() {
        return result;
    }

    void setResult(OperationResult r) {
        result = r;
    }
    /**
     * Closes the scanner.
     */
    public void close() {
        TxnUtil.close(cursor);
    }

    /**
     * Returns true if there is a "next" record, regardless of direction.
     * The key and data for the record are in the key and data members.
     */
    public boolean next() {
        return next(false);
    }

    /**
     * Returns true if there is a "next" record, regardless of direction.
     * The key and data for the record are in the key and data members. If
     * noChargeEmpty == true no read charges are added if the first call to
     * next returns false.
     *
     * TODO - The noChargeEmpty flag is targeted for secondary
     * database population. This may require further refinement.
     */
    public boolean next(boolean noChargeEmpty) {

        if (direction == Direction.FORWARD) {
            return getNext(noChargeEmpty);
        }
        return getPrev();
    }

    public int getCurrentStorageSize() {
        return getStorageSize(cursor);
    }

    /*
     * Forward iteration
     */
    private boolean getNext(boolean noChargeEmpty) {

        if (state == State.INIT) {
            initForward();
            /* Do separate search for parent. */
            if (includeParent && parentKey != null) {

                keyEntry.setData(parentKey);
                result = cursor.get(keyEntry, dataEntry,
                                    Get.SEARCH, lockMode.toReadOptions());
                /*
                 * Don't charge the empty read if parent key is not found, since
                 * continue to read its first descendant.
                 */
                parentFound = processResult(result, dataEntry,
                                            false, /*chargeRowRead*/
                                            true /*noChargeEmpty*/);
            }
            if (parentFound) {
                return true;
            }
        }

        if (state == State.FIRST_DESC) {
            if (!getFirstDescendant(noChargeEmpty)) {
                return false;
            }

            /*
             * In this case we are at the first descendant now.
             */
            if (allDescendants) {
                return true;
            }
            return handleImmediateChildren();
        }

        assert(state == State.ALL);
        result = cursor.get(keyEntry, dataEntry,
                            Get.NEXT, lockMode.toReadOptions());
        processResult(result, dataEntry);
        if (result == null) {
            return false;
        }

        /* Simple case: collect all descendants. */
        if (allDescendants) {
            return true;
        }

        return handleImmediateChildren();
    }

    /*
     * Determine the search and scan constraints.  The searchInitKey is
     * used to start the scan for descendents, and is used when we do not
     * first position on the parent. The rangeConstraint is used to end the
     * scan and prevents scanning outside the parent key descendents (or
     * sub-range of child keys), which is particularly important to avoid
     * deadlocks with other transactions.
     */
    private void initForward() {
        state = State.FIRST_DESC;
        /*
         * Determine the search and scan constraints.  The searchInitKey is
         * used to start the scan for descendents, and is used when we do not
         * first position on the parent. The rangeConstraint is used to end the
         * scan and prevents scanning outside the parent key descendents (or
         * sub-range of child keys), which is particularly important to avoid
         * deadlocks with other transactions.
         */
        if (subRange == null) {

            /*
             * Case 1: No KeyRange.  Start the scan at first descendant and
             * stop it at a key that doesn't start with the parent key prefix.
             * Scan entire key set when parentKey is null.
             */
            if (parentKey != null) {
                searchInitKey = Key.addComponent(parentKey, majorPathComplete, "");
                rangeConstraint = getPrefixConstraint(searchInitKey);
            } else {
                searchInitKey = MIN_KEY;
                rangeConstraint = null;
            }

        } else if (subRange.isPrefix()) {

            /*
             * Case 2: KeyRange is a prefix.  Start the scan at the first child
             * key with the given KeyRange prefix and stop it at a key that
             * doesn't start with that prefix.
             */
            searchInitKey = Key.addComponent(parentKey,
                                             majorPathComplete,
                                             subRange.getStart());
            rangeConstraint = getPrefixConstraint(searchInitKey);

        } else {

            /*
             * Case 3: KeyRange has different start/end points.  Start the scan
             * at the child key with the inclusive KeyRange start key.
             */
            if (subRange.getStart() != null) {

                String rangeStart =
                    (subRange.getStartInclusive() ?
                     subRange.getStart() :
                     getPathComponentSuccessor(subRange.getStart()));

                searchInitKey = Key.addComponent(parentKey,
                                                 majorPathComplete,
                                                 rangeStart);
            } else if (parentKey != null) {
                searchInitKey = Key.addComponent(parentKey,
                                                 majorPathComplete,
                                                 "");
            } else {
                searchInitKey = MIN_KEY;
            }

            /* Stop the scan at the exclusive KeyRange end key. */
            if (subRange.getEnd() != null) {

                String rangeEnd =
                    (subRange.getEndInclusive() ?
                     getPathComponentSuccessor(subRange.getEnd()) :
                     subRange.getEnd());

                rangeConstraint = getRangeEndConstraint
                    (Key.addComponent(parentKey, majorPathComplete, rangeEnd));

            } else if (parentKey != null) {
                rangeConstraint = getPrefixConstraint
                    (Key.addComponent(parentKey, majorPathComplete, ""));
            } else {
                rangeConstraint = null;
            }
        }
        assert (searchInitKey != null);
    }

    private boolean getFirstDescendant(boolean noChargeEmpty) {
        state = State.ALL;
        /*
         * For a non-prefix range there is a possibility that the range
         * start (or MIN_KEY) exceeds the range end. The rangeConstraint
         * won't detect this if the searchInitKey (range start or MIN_KEY)
         * matches a record exactly.
         */
        if (subRange != null &&
            rangeConstraint != null &&
            !subRange.isPrefix() &&
            !rangeConstraint.inBounds(searchInitKey)) {
            result = null;
            return false;
        }

        /* Move to first descendant. */
        cursor.setRangeConstraint(rangeConstraint);

        if (parentFound && subRange == null) {
            result = cursor.get(keyEntry, dataEntry,
                                Get.NEXT, lockMode.toReadOptions());
            processResult(result, dataEntry);
        } else if (resumeKey != null) {
            keyEntry.setData(resumeKey);
            result = cursor.get(keyEntry, dataEntry,
                                Get.SEARCH_GTE, lockMode.toReadOptions());
            if (result != null &&
                moveAfterResumeKey &&
                Arrays.equals(resumeKey, keyEntry.getData())) {
                result = cursor.get(keyEntry, dataEntry,
                                    Get.NEXT, lockMode.toReadOptions());
                processResult(result, dataEntry);
            } else {
                /*
                 * Only record the read cost if resume key is not found. If
                 * found, continue to read the next of resume key.
                 */
                processResult(result, dataEntry);
            }
        } else {
            keyEntry.setData(searchInitKey);
            result = cursor.get(keyEntry, dataEntry,
                                Get.SEARCH_GTE, lockMode.toReadOptions());
            processResult(result, dataEntry, true, noChargeEmpty);
        }
        return (result != null);
    }

    private boolean handleImmediateChildren() {

        /*
         * To collect immediate children only, we position at the child's
         * successor key when we encounter a descendant that is not an
         * immediate child.
         */
        while (result != null) {
            final int nComponents =
                Key.countComponents(keyEntry.getData());
            if (nComponents == nChildComponents) {
                /* This is an immediate child. */
                return true;
            }
            /* Not an immediate child.  Move to child's successor. */
            assert nComponents > nChildComponents;
            getChildKeySuccessor(keyEntry, parentKey);
            result = cursor.get(keyEntry, dataEntry,
                                Get.SEARCH_GTE, lockMode.toReadOptions());
            processResult(result, dataEntry);
        }
        return false;
    }


    /*
     * Reverse iteration
     */

    /*
     * The keyPrefix is used both to create a rangeConstraint and to find the
     * first record following the range as the starting point. The
     * searchInitKey is only used as a starting point when a KeyRange end point
     * is given. The rangeConstraint is used to end the scan.
     */
    private void initReverse() {

        state = State.FIRST_DESC;

        /*
         * For a reverse scan, to avoid deadlocks with transactions that move
         * the cursor forward we must use read-committed (which is always used
         * for an iteration and in this method) and a non-sticky cursor.
         * Read-committed ensures that we release locks as we move the cursor.
         * The non-sticky cursor ensures that we release the lock at the
         * current position before getting the lock at the next position, as
         * opposed to overlapping the locks as occurs with a default, sticky
         * cursor.
         */
        assert DbInternal.getCursorImpl(cursor)
            .getLocker()
            .isReadCommittedIsolation();

        if (subRange == null) {

            /*
             * Case 1: No KeyRange.
             */
            if (parentKey != null) {

                /*
                 * Use the parent key prefix as the range constraint and
                 * starting point.
                 */
                keyPrefix = Key.addComponent(parentKey, majorPathComplete, "");
                searchInitKey = null;
                rangeConstraint = getPrefixConstraint(keyPrefix);
            } else {
                /* Scan all keys, no constraints. */
                keyPrefix = null;
                searchInitKey = null;
                rangeConstraint = null;
            }

        } else if (subRange.isPrefix()) {

            /*
             * Case 2: KeyRange is a prefix.  Same as case 1 but using the
             * KeyRange prefix appended to the parent key.
             */
            keyPrefix = Key.addComponent(parentKey, majorPathComplete,
                                         subRange.getStart());
            searchInitKey = null;
            rangeConstraint = getPrefixConstraint(keyPrefix);

        } else {

            /*
             * Case 3: KeyRange has different start/end points.  The key prefix
             * may be used in several different ways below.
             */
            if (parentKey != null) {
                keyPrefix = Key.addComponent(parentKey, majorPathComplete, "");
            } else {
                keyPrefix = null;
            }

            if (subRange.getEnd() != null) {

                /*
                 * Start the scan at the child key with the exclusive KeyRange
                 * end key.
                 */
                final String rangeEnd = subRange.getEndInclusive() ?
                    getPathComponentSuccessor(subRange.getEnd()) :
                    subRange.getEnd();

                searchInitKey = Key.addComponent
                    (parentKey, majorPathComplete, rangeEnd);
            } else {
                /* Use the keyPrefix, if any, to start the scan. */
                searchInitKey = null;
            }

            if (subRange.getStart() != null) {

                /*
                 * Stop the scan at the inclusive KeyRange start key.
                 */
                final String rangeStart = subRange.getStartInclusive() ?
                    subRange.getStart() :
                    getPathComponentSuccessor(subRange.getStart());

                rangeConstraint = getRangeStartConstraint(
                    Key.addComponent(
                        parentKey, majorPathComplete, rangeStart));

            } else if (keyPrefix != null) {
                /* Use the keyPrefix to stop the scan. */
                rangeConstraint = getPrefixConstraint(keyPrefix);
            } else {
                /* Scan all keys, no constraints. */
                rangeConstraint = null;
            }
        }
    }

    private boolean getPrev() {

        if (state == State.INIT) {
            initReverse();

            /* Do separate search for parent. */
            state = State.FIRST_DESC;
            if (includeParent && parentKey != null) {
                keyEntry.setData(parentKey);
                result = cursor.get(keyEntry, dataEntry,
                                    Get.SEARCH, lockMode.toReadOptions());
                /*
                 * Don't charge the empty read if parent key is not found, since
                 * continue to read its first descendant.
                 */
                processResult(result, dataEntry, true, true /* noChargeEmpty */);
                if (result != null) {
                    return true;
                }
            }
        }

        if (state == State.FIRST_DESC) {
            state = State.ALL;
            /*
             * Move to the desired key using an exclusive end point. Do not set
             * the rangeConstraint at first, because we may temporarily move
             * outside of the range during the DbInternal.search call.
             */
            final byte[] exclusiveInitKey;
            Search search = Search.LT;

            if (resumeKey != null) {
                exclusiveInitKey = resumeKey;
                if (!moveAfterResumeKey) {
                    search = Search.LTE;
                }
            } else if (searchInitKey != null) {
                exclusiveInitKey = searchInitKey;
            } else if (keyPrefix != null) {
                final byte[] nextPrefix = getPrefixSuccessor(keyPrefix);
                exclusiveInitKey = (nextPrefix != null) ? nextPrefix : null;
            } else {
                exclusiveInitKey = null;
            }

            if (exclusiveInitKey != null) {
                keyEntry.setData(exclusiveInitKey);
                result = DbInternal.search(
                    cursor, keyEntry, null, dataEntry,
                    search, lockMode.toReadOptions());
            } else {
                result = cursor.get(keyEntry, dataEntry,
                                    Get.LAST, lockMode.toReadOptions());
            }
            processResult(result, dataEntry);
            if (result == null) {
                return false;
            }

            /*
             * Because we did not set the rangeConstraint above, we must check
             * it explicitly here for the first record to be returned.
             */
            if (rangeConstraint != null &&
                !rangeConstraint.inBounds(keyEntry.getData())) {
                result = null;
            }

            /* Now use the rangeConstraint to stop the scan. */
            cursor.setRangeConstraint(rangeConstraint);

            if (allDescendants) {
                return (result != null);
            }
            return handleImmediateChildrenReverse();
        }

        assert(state == State.ALL);
        result = cursor.get(keyEntry, dataEntry,
                            Get.PREV, lockMode.toReadOptions());
        processResult(result, dataEntry);
        if (result == null) {
            return false;
        }

        /* Simple case: collect all descendants. */
        if (allDescendants) {
            return true;
        }

        return handleImmediateChildrenReverse();
    }

    private boolean handleImmediateChildrenReverse() {

        /*
         * To collect immediate children only, we position at the child's
         * predecessor key when we encounter a descendant that is not an
         * immediate child.
         */
        while (result != null) {
            final int nComponents =
                Key.countComponents(keyEntry.getData());
            if (nComponents == nChildComponents) {
                /* This is an immediate child. */
                return true;
            }
            /*
             * Not an immediate child.  Move back/up to the immediate
             * child, which has a key that is a prefix of the key we
             * found but with nChildComponents.
             */
            assert nComponents > nChildComponents;
            final byte[] nonChildKey = keyEntry.getData();
            keyEntry.setData
                (Key.getPrefixKey(nonChildKey, nChildComponents));
            result = cursor.get(keyEntry, dataEntry,
                                Get.SEARCH_GTE, lockMode.toReadOptions());
            processResult(result, dataEntry);
            if (result != null &&
                Arrays.equals(nonChildKey, keyEntry.getData())) {

                /*
                 * Arrived back at the same record, meaning that there
                 * is no key with nChildComponents for this child and
                 * we are at the first descendent for this child.  Move
                 * to the previous child.
                 */
                result = cursor.get(keyEntry, dataEntry,
                                    Get.PREV, lockMode.toReadOptions());
                processResult(result, dataEntry);
            }
        }
        return false;
    }

    /*
     * Utility functions.
     */

    /**
     * Returns the first key that sorts higher than a child key, if we were to
     * remove all components following the child key in the given full key.
     *
     * The character with value 1 has the lowest sort value of any character in
     * the "Modified UTF-8" encoding.
     */
    private void getChildKeySuccessor(DatabaseEntry key, byte[] parent) {
        final byte[] bytes = key.getData();

        /* Child begins after the parent's delimiter byte. */
        final int childOff = (parent != null) ? (parent.length + 1) : 0;
        final int childLen = Key.getComponentLength(bytes, childOff);

        /* Successor key is child key plus a byte '1' value. */
        final int newLen = childOff + childLen + 1;
        assert (newLen <= bytes.length);
        bytes[newLen - 1] = MIN_VALUE_BYTE;
        key.setSize(newLen);
    }

    /**
     * Returns the first key that sorts higher than the given prefix, with the
     * same number of bytes.  Searching for a record LT than the return value
     * will find the last (maximum) key having the given prefix.
     *
     * The algorithm increments the least significant byte value. If adding one
     * reaches the max value, set that byte to the min value and adds one to
     * the next most significant byte, etc. If the most significant byte would
     * wrap, then the original prefix consists of all max value bytes, and null
     * is returned. In that case, the keys having the prefix are the last keys
     * in the DB, and Cursor.getLast will land on the last key with the prefix.
     *
     * Note that this method produces a key that may be invalid, i.e., may not
     * be deserializable. This is because its only requirement is to produce a
     * key that compares greater than all keys with the given prefix, without
     * regard to whether the byte values in the result are valid UTF8.
     */
    private static byte[] getPrefixSuccessor(byte[] prefix) {

        final byte[] bytes = new byte[prefix.length];
        System.arraycopy(prefix, 0, bytes, 0, bytes.length);

        for (int i = bytes.length - 1; i >= 0; i -= 1) {
            final int b = bytes[i] & 0xFF;
            if (b == MAX_KEY_BYTE) {
                bytes[i] = (byte) MIN_KEY_BYTE;
                continue;
            }
            bytes[i] = (byte) ((b == MIN_KEY_BYTE) ? 0 : (b + 1));
            return bytes;
        }

        return null;
    }

    /**
     * Returns the smallest possible key that sorts higher than the given key.
     *
     * This is similar to calling
     *  Key.addComponent(key, majorPathComplete, "")
     * But knowing majorPathComplete is unnecessary.
     *
     * Note that this method produces a key that may be invalid in the sense
     * that a major path separator is added, which may be inappropriate for
     * the given key. The result should normally not be be de-serialized, and
     * is intended only for comparisons.
     */
    private static byte[] getKeySuccessor(byte[] key) {

        final byte[] bytes = new byte[key.length + 1];
        System.arraycopy(key, 0, bytes, 0, key.length);
        bytes[key.length] = (byte) MIN_KEY_BYTE;

        return bytes;
    }

    /**
     * Returns a RangeConstraint that prevents the cursor from moving past
     * the descendants of the prefix key.
     */
    private RangeConstraint getPrefixConstraint(final byte[] prefixKey) {

        final int prefixLen = prefixKey.length;

        return new RangeConstraint() {
            @Override
            public boolean inBounds(byte[] checkKey) {
                if (checkKey.length < prefixLen) {
                    return false;
                }
                for (int i = 0; i < prefixLen; i += 1) {
                    if (prefixKey[i] != checkKey[i]) {
                        return false;
                    }
                }
                return true;
            }
        };
    }

    /**
     * Returns a RangeConstraint that prevents the cursor from moving backward
     * past an inclusive start point.
     */
    private RangeConstraint
        getRangeStartConstraint(final byte[] startKeyInclusive) {

        return new RangeConstraint() {
            @Override
            public boolean inBounds(byte[] checkKey) {
                return KEY_BYTES_COMPARATOR.compare(checkKey,
                                                    startKeyInclusive) >= 0;
            }
        };
    }

    /**
     * Returns the first string that sorts higher than the given string, if
     * the strings were converted to UTF-8.
     *
     * The character with value 1 has the lowest sort value of any character in
     * the "Modified UTF-8" encoding.
     */
    private String getPathComponentSuccessor(String comp) {
        return comp + MIN_VALUE_CHAR;
    }

    /**
     * Returns a RangeConstraint that prevents the cursor from moving forward
     * past an exclusive end point.
     */
    private RangeConstraint
        getRangeEndConstraint(final byte[] endKeyExclusive) {

        return new RangeConstraint() {
            @Override
            public boolean inBounds(byte[] checkKey) {
                return KEY_BYTES_COMPARATOR.compare(checkKey,
                                                    endKeyExclusive) < 0;
            }
        };
    }

    /**
     * Processes the result of a operation, recording the read cost depending
     * on the success/failure and the data returned. Returns true if the
     * operation was successful (r != null).
     */
    private boolean processResult(OperationResult r, DatabaseEntry e) {
        return processResult(r, e, true, false);
    }

    private boolean processResult(OperationResult r,
                                  DatabaseEntry e,
                                  boolean chargeRowRead,
                                  boolean noChargeEmpty) {
        /* If failure charge the min. read */
        if (r == null) {
            /* If noChargeEmpty is true, ignore charging the min. read */
            if (!noChargeEmpty) {
                op.addEmptyReadCharge();
            }
            return false;
        }

        /*
         * If no data returned, charge min. read if chargeKeyRead is true.
         * Otherwise, get the size of the record.
         */
        if (e == null || e.getPartial()) {
            if (chargeKeyRead) {
                op.addReadBytes(MIN_READ);
            }
        } else if (chargeRowRead) {
            int storageSize = getStorageSize(cursor);
            if (subtractKeyReadCost) {
                storageSize -= MIN_READ;
            }
            if (storageSize > 0) {
                op.addReadBytes(storageSize);
            }
        }
        return true;
    }

    public void setChargeKeyRead(boolean enable) {
        chargeKeyRead = enable;
    }

    public void setSubtractKeyReadCost(boolean value) {
        subtractKeyReadCost = value;
    }

    /**
     * Initiates asynchronous discarding of records from the specified table.
     *
     * @throws DatabaseNotFoundException if any name in dbNames does not
     * refer to an existing DB
     */
    public static void discardTableRecords(final Environment env,
                                           final Transaction txn,
                                           final Set<String> dbNames,
                                           final TableImpl table) {
        final byte[] topIdBytes = table.getTopLevelTable().getIDBytes();
        final byte[] startKey = getKeySuccessor(topIdBytes);
        final byte[] endKey = getPrefixSuccessor(topIdBytes);

        /*
         * A table pattern is needed to match child tables, since the key
         * range includes all records in the top-level table.
         */
        final byte[][] tablePattern = (table.getParent() != null) ?
            Scanner.createTablePattern(table) : null;

        /*
         * In the unusual case that endKey is null (see getPrefixSuccessor)
         * we must use a key prefix to terminate the scan.
         */
        final byte[] prefix = (endKey == null) ? topIdBytes : null;

        /*
         * Avoiding a scan filter is desirable, since it adds some storage and
         * processing overhead. But we need a filter if we have to check the
         * key prefix or select child table records.
         */
        final ScanFilter filter = (prefix != null || tablePattern != null) ?
                                new TableFilter(prefix, tablePattern) : null;

        env.discardExtinctRecords(txn, dbNames, new DatabaseEntry(startKey),
                                  (endKey != null) ? new DatabaseEntry(endKey) :
                                                     null,
                                  filter, "DropTable:" + table.getFullName());
    }

    /**
     * Creates a pattern for the given table that can be used to determine
     * whether a key is in the table.
     *
     * The pattern is an array with an element for each key component. The
     * array positions corresponding to table IDs contain the table ID itself
     * as a byte array. All other array positions contain null.
     *
     * @see #matchTablePattern
     */
    private static byte[][] createTablePattern(TableImpl table) {
        final byte[][] pattern = new byte[table.getNumKeyComponents()][];
        do {
            final TableImpl parent = (TableImpl) table.getParent();
            final int i = (parent != null) ? parent.getNumKeyComponents() : 0;
            pattern[i] = table.getIDBytes();
            table = parent;
        } while (table != null);

        return pattern;
    }

    /**
     * Returns whether the given key matches the given pattern, which
     * determines whether the key belongs to the table from which the pattern
     * was created.
     *
     * To match, the key's number of components must equal the number of
     * elements in the pattern. The non-null components in the pattern are
     * matched against table IDs, while the null components are wildcards.
     *
     * @see #createTablePattern
     */
    private static boolean matchTablePattern(byte[] key, byte[][] pattern) {
        int prevOff = 0;

        for (final byte[] comp : pattern) {
            final int nextOff = Key.findNextComponent(key, prevOff);

            if (nextOff < 0) {
                /* Key has less components than the pattern. */
                return false;
            }

            if (comp != null) {
                final int len = nextOff - prevOff;
                if (len != comp.length) {
                    return false;
                }
                for (int i = 0; i < len; i += 1) {
                    if (key[prevOff + i] != comp[i]) {
                        return false;
                    }
                }
            }
            prevOff = nextOff + 1;
        }

        /* Check that there are no extra key components. */
        return Key.findNextComponent(key, prevOff) < 0;
    }

    /**
     * A JE ScanFilter for matching keys in a given table.
     */
    private static class TableFilter implements ScanFilter, Serializable {

        private static final long serialVersionUID = 1L;

        private final byte[] keyPrefix;
        private final byte[][] tablePattern;

        /**
         * @param keyPrefix is the prefix for all keys in the table (the ID
         * ID bytes of the top level table), and is used to stop the scan
         * when a key in a different top-level table is encountered. It may be
         * null if an end key is used to terminate the scan.
         *
         * @param tablePattern is a pattern for matching a particular parent
         * or child table (created by {@link #createTablePattern}, or null
         * to match all records of the top-level table and its child tables.
         */
        TableFilter(byte[] keyPrefix, byte[][] tablePattern) {
            this.keyPrefix = keyPrefix;
            this.tablePattern = tablePattern;
        }

        /**
         * Returns EXCLUDE_STOP to stop the scan when the given key does not
         * have the given keyPrefix (if any).
         *
         * Otherwise, returns EXCLUDE when the key does not match the
         * tablePattern (if any).
         *
         * Otherwise, returns INCLUDE.
         */
        @Override
        public ScanFilter.ScanResult checkKey(byte[] key) {
            if (keyPrefix != null) {
                if (key.length < keyPrefix.length) {
                    return ScanFilter.ScanResult.EXCLUDE_STOP;
                }
                for (int i = 0; i < keyPrefix.length; i += 1) {
                    if (key[i] != keyPrefix[i]) {
                        return ScanFilter.ScanResult.EXCLUDE_STOP;
                    }
                }
            }
            if (tablePattern != null) {
                if (!matchTablePattern(key, tablePattern)) {
                    return ScanFilter.ScanResult.EXCLUDE;
                }
            }
            return ScanFilter.ScanResult.INCLUDE;
        }
    }
}
