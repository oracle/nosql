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

import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.io.PrintStream;
import java.util.ArrayList;

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
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.NotSerializable;
import com.sleepycat.je.utilint.VLSN;

/**
 * {@literal
 * A VLSNBucket instance represents a set of VLSN->LSN mappings. Buckets are
 * usually not updated, except at times when the replication stream may have
 * been reduced in size, by log cleaning or syncup. The VLSNBuckets in the
 * VLSNIndex's VLSNTracker are written to disk and are persistent. There are
 * also VLSNBuckets in the temporary recovery-time tracker that are used for
 * collecting mappings found in the log during recovery.
 *
 * VLSNBuckets only hold mappings from a single log file. A single log file
 * may be mapped by multiple VLSNBuckets though.
 *
 * As a tradeoff in space vs time, a VLSNBucket only stores a sparse set of
 * mappings and the caller must use a VLSNReader to scan the log file and
 * find any log entries not mapped directly by the bucket. In addition,
 * the VLSN is not actually stored. Only the offset portion of the LSN is
 * stored, and the VLSN is intuited by a stride field. Each VLSNBucket
 * only maps a single file, though a single file may be mapped by several
 * VLSNBuckets.
 *
 * For example, suppose a node had these VLSN->LSN mappings:
 *
 * VLSN            LSN (file/offset)
 * 9               10/100
 * 10              10/110
 * 11              10/120
 * 12              10/130
 * 13              10/140
 * 14              11/100
 * 15              11/120
 *
 * The mappings in file 10 could be represented by a VLSNBucket with
 * a stride of 4. That means the bucket would hold the mappings for
 *  9              10/100,
 * 13              10/140
 *
 * And since the target log file number and the stride is known, the mappings
 * can be represented in by the offset alone in this array: {100, 140}, rather
 * than storing the whole lsn.
 *
 * Each bucket can also provide the mapping for the first and last VLSN it
 * covers, even if the lastVLSN is not divisible by the stride. This is done to
 * support forward and backward scanning. From the example above, the completed
 * bucket can provide 9->10/100, 13->10/140, 15 -> 10/160 even though 15 is not
 * a stride's worth away from 13.
 *
 * In order to reduce synchronization overhead, we provide two classes:
 * VLSNBucket: It is a read-only class, representing the old buckets inside
 *             which the mappings will not change, and it has no synchronized
 *             functions.
 * VLSNBucketCurrent:It inherits VLSNBucket, representing current bucket
 *                   accepting read/write access. It overrides the read
 *                   functions with 'synchronized', and provides the write
 *                   function.
 * When current bucket is filled, a new VLSNBucket will be created based on
 * VLSNBucketBucket's data, and replaces the corresponding items in tracker's
 * bucket cache.
 *
 * Because registering a VLSN->LSN mapping is done outside the log write latch,
 * any inserts into the VLSNBucket may not be in order. However, when any
 * VLSN is registered, we can assume that all VLSNs < that value do exist in
 * the log. It's just an accident of timing that they haven't yet been
 * registered. Note that out of order inserts into the buckets can create holes
 * in the bucket's offset array, or cause the array to be shorter than
 * anticipated.
 *
 * For example, if the insertion order into the bucket is vlsns 9, 15, we'll
 * actually only keep an offset array of size 1. We have to be able to handle
 * holes in the bucket, and can't count on filling them in when the lagging
 * vlsn arrives, because it is possible that a reading thread will access the
 * bucket before the laggard inserter arrives, or that the bucket might be
 * flushed to disk, and become immutable.
 * }
 */
public class VLSNBucket {

    /* On-disk version. */
    private static final int VERSION = 1;

    /* File number for target file. */
    protected final long fileNumber;

    /* Interval between VLSN values that are mapped. */
    protected final int stride;

    protected long firstVLSN = NULL_VLSN;
    protected long lastVLSN = NULL_VLSN;
    protected long lastLsn = DbLsn.NULL_LSN;

    /*
     * The file offsets are really unsigned ints. The calls to put() are
     * implemented to let us assume that the list is fully populated.  A
     * subclass of truncateableList has been used in order to provide access to
     * the ArrayList.removeFromRange method.
     */
    protected TruncateableList<Integer> fileOffsets;

    protected static final int NO_OFFSET = 0;

    /* True if there are changes to the bucket that are not on disk. */
    boolean dirty;

    VLSNBucket(long fileNumber,
               int stride,
               long firstVLSN) {
        this.fileNumber = fileNumber;
        this.stride = stride;
        this.firstVLSN = firstVLSN;

        fileOffsets = new TruncateableList<Integer>();
        fileOffsets.add(0, NO_OFFSET);
    }

    VLSNBucket(VLSNBucketCurrent current) {
        this.fileNumber = current.fileNumber;
        this.stride = current.stride;
        this.firstVLSN = current.firstVLSN;
        this.lastVLSN = current.lastVLSN;
        this.lastLsn = current.lastLsn;
        this.fileOffsets = current.fileOffsets;
        this.dirty = current.dirty;
    }

    /* For reading from disk. */
    private VLSNBucket(TupleInput ti) {
        fileNumber = ti.readPackedLong();
        stride = ti.readPackedInt();
        firstVLSN = ti.readPackedLong();
        lastVLSN = ti.readPackedLong();
        lastLsn = ti.readPackedLong();
        int size = ti.readPackedInt();
        fileOffsets = new TruncateableList<Integer>(size);
        for (int i = 0; i < size; i++) {
            fileOffsets.add(i, DbLsn.getFileOffsetAsInt(ti.readUnsignedInt()));
        }
    }

    /*
     * Return true if this VLSN is on a stride boundary.  Assumes
     * !firstVLSN.isNull()
     */
    protected boolean isModulo(long vlsn) {
        return (((vlsn - firstVLSN) % stride) ==
                0);
    }

    protected int getIndex(long vlsn) {
        assert isModulo(vlsn) : "Don't call getIndex on non-modulo VLSN " +
            vlsn + " bucket=" + this;

        return (int) ((vlsn - firstVLSN) / stride);
    }

    /**
     * @return true if this bucket contains this mapping.
     */
    boolean owns(long vlsn) {
        if (vlsn == NULL_VLSN) {
            return false;
        } else if (firstVLSN == NULL_VLSN) {
            return false;
        } else {
            return (firstVLSN <= vlsn) &&
                (lastVLSN >= vlsn);
        }
    }

    long getFirst() {
        return firstVLSN;
    }

    long getLast() {
        return lastVLSN;
    }

    /**
     * Return a file number that is less or equal to the first lsn mapped
     * by this bucket. In standard VLSNBuckets, only one file is covered, so
     * there is only one possible value. In GhostBuckets, multiple files could
     * be covered.
     * @return
     */
    long getLTEFileNumber() {
        return fileNumber;
    }

    /*
     * Similar to getLTEFileNumber, for this implementation there's only one
     * possible file.
     */
    long getGTEFileNumber() {
        return fileNumber;
    }

    boolean empty() {
        return emptyInternal();
    }

    protected boolean emptyInternal() {
        return ((firstVLSN == lastVLSN) &&
                (lastLsn == DbLsn.NULL_LSN));
    }

    boolean follows(long vlsn) {
        return (firstVLSN > vlsn);
    }

    boolean precedes(long vlsn) {
        return ((lastVLSN != NULL_VLSN) &&
                (lastVLSN < vlsn));
    }

    /**
     * Returns the mapping whose VLSN is {@literal >=} the VLSN parameter. For
     * example, if the bucket holds mappings for vlsn 10, 13, 16,
     *
     *  - the greater than or equal mapping for VLSN 10 is 10/lsn
     *  - the greater than or equal mapping for VLSN 11 is 13/lsn
     *  - the greater than or equal mapping for VLSN 13 is 13/lsn
     *
     * File offsets may be null in the middle of the file offsets array because
     * of out of order mappings. This method must return a non-null lsn, and
     * must account for null offsets.
     *
     * @return the mapping whose VLSN is {@literal >=} the VLSN parameter. Will
     * never return NULL_LSN, because the VLSNRange begin and end point are
     * always mapped.
     */
    public long getGTELsn(long vlsn) {

        if (lastVLSN == vlsn) {
            return lastLsn;
        }

        int index;
        if (firstVLSN >= vlsn) {
            
            /* 
             * It's possible for vlsn to be < the firstVLSN if vlsn
             * falls between two buckets. For example, if the buckets are:
             *    bucketA = vlsn 10-> 20
             *    bucketB = vlsn 22->30
             * then vlsn 21 will fall between two buckets, and will get bucketB
             */
            index = 0;
        } else {
            index = getGTEIndex(vlsn);
        }

        /* 
         * This should never happen. Throw this exception to make debugging
         * info available.
         */
        if (index < 0) {
            throw EnvironmentFailureException.unexpectedState
                ("index=" + index +
                 " vlsn=" + vlsn +
                 " bucket=" + this);
        }

        if (index >= fileOffsets.size()) {
            return lastLsn;
        }
        int useIndex = findPopulatedIndex(index, true /* forward */);
        int offset = fileOffsets.get(useIndex);
        return offset == NO_OFFSET ?
            lastLsn : DbLsn.makeLsn(fileNumber, offset);
    }

    /**
     * Return the index for the mapping >= this VLSN. Note that this is just
     * a stride calculation, and a non-existent file offset index might be
     * returned.
     */
    private int getGTEIndex(long vlsn) {
        long diff = vlsn - firstVLSN;
        return (int) ((diff + (stride - 1)) / stride);
    }

    /**
     * We'd like to return the mapping at startIndex for the get{LTE, GTE}
     * Mapping methods, but the offsets may not be populated if put() calls
     * have come out of order. Search for the next populated offset.
     */
    protected int findPopulatedIndex(int startIndex, boolean forward) {
        if (forward) {
            for (int i = startIndex; i < fileOffsets.size(); i++) {
                if (fileOffsets.get(i) != NO_OFFSET) {
                    return i;
                }
            }
        } else {
            for (int i = startIndex; i >= 0; i--) {
                if (fileOffsets.get(i) != NO_OFFSET) {
                    return i;
                }
            }
        }
        return startIndex;
    }

    /**
     * Returns the lsn whose VLSN is <= the VLSN parameter. For example, if
     * the bucket holds mappings for vlsn 10, 13, 16,
     *
     *  - the less than or equal mapping for VLSN 10 is 10/lsn
     *  - the less than or equal mapping for VLSN 11 is 10/lsn
     *  - the less than or equal mapping for VLSN 13 is 13/lsn
     *
     * File offsets may be null in the middle of the file offsets array because
     * of out of order mappings. This method must return a non-null lsn, and
     * must account for null offsets.
     *
     * @return the lsn whose VLSN is <= the VLSN parameter. Will never return
     * NULL_LSN, because the VLSNRange begin and end points are always mapped.
     */
    long getLTELsn(long vlsn) {

        /*
         * It's possible for vlsn to be greater than lastVLSN if vlsn falls
         * between two buckets.
         * For example, if the buckets are:
         *    bucketA = vlsn 10-> 20
         *    bucketB = vlsn 22->30
         * then vlsn 21 will fall between two buckets, and will get bucketA
         */
        if (lastVLSN <= vlsn) {
            return lastLsn;
        }

        long diff = vlsn - firstVLSN;

        /*
         * Make sure that the file offset array isn't unexpectedly short due to
         * out of order inserts.
         */
        int index = (int)(diff / stride);
        if (index >= fileOffsets.size()) {
            index = fileOffsets.size() - 1;
        }

        int useIndex = findPopulatedIndex(index, false /* forward */);
        int offset = fileOffsets.get(useIndex);

        assert offset != NO_OFFSET : "bucket should always have a non-null " +
            "first offset. vlsn= " + vlsn + " bucket=" + this;

        return (DbLsn.makeLsn(fileNumber, offset));
    }

    /**
     * @return the lsn whose VLSN is == the VLSN parameter or DbLsn.NULL_LSN if
     * there is no mapping. Note that because of out of order puts, there may
     * be missing mappings that appear later on.
     */
    public long getLsn(long vlsn) {
        assert owns(vlsn) : "vlsn=" + vlsn + " " + this;

        if (lastVLSN == vlsn) {
            return lastLsn;
        }

        if (!isModulo(vlsn)) {
            return DbLsn.NULL_LSN;
        }

        int index = getIndex(vlsn);
        if (index >= fileOffsets.size()) {
            return DbLsn.NULL_LSN;
        }

        int offset = fileOffsets.get(index);
        if (offset == NO_OFFSET) {
            return DbLsn.NULL_LSN;
        }

        return DbLsn.makeLsn(fileNumber, offset);
    }

    long getLastLsn() {
        return lastLsn;
    }


    /**
     * Remove the mappings from this bucket that are for VLSNs >=
     * startOfDelete.  Unlike removing from the head, we need not worry about
     * breaking a bucket stride interval.
     * 
     * The buckets can be updated here because the removal can only happen
     * during recovery or replica side syncup, and they have no readers at
     * these times.
     *
     * If prevLsn is NULL_VLSN, we don't have a good value to cap the bucket.
     * Instead, we'll have to delete the bucket back to whatever was the next
     * available lsn. For example, suppose the bucket has these mappings.  This
     * strange bucket (stride 25 is missing) is possible if vlsn 26 arrived
     * early, out of order.
     * 
     *  in fileOffset: 10 -> 101
     *  in fileOffset: 15 -> no offset
     *  in fileOffset: 20 -> 201
     *  lastVLSN->lastnLsn mapping  26 -> 250 
     *
     * If we have a prevLsn and the startOfDelete is 17, then we can create
     * a new mapping
     *  in fileOffset: 10 -> 101
     *  in fileOffset: 15 -> no offset
     *  lastVLSN->lastnLsn mapping  17 -> 190
     *
     * If we don't have a prevLsn, then we know that we have to cut the bucket
     * back to the largest known mapping, losing many mappings along the way.
     *  in fileOffset: 10 -> 101
     *  lastVLSN->lastnLsn mapping  10 -> 101
     *
     * If we are deleting in the vlsn area between the last stride and the
     * last offset, (i.e. vlsn 23 is the startOfDelete) the with and without
     * prevLSn cases would look like this:
     *
     * (there is a prevLsn, and 23 is startDelete. No need to truncate
     * anything) 
     *  in fileOffset: 10 -> 101
     *  in fileOffset: 15 -> no offset
     *  in fileOffset: 20 -> 201
     *  lastVLSN->lastnLsn mapping  23 -> prevLsn
     *
     * (there is no prevLsn, and 23 is startDelete) 
     *  in fileOffset: 10 -> 101
     *  in fileOffset: 15 -> no offset
     *  in fileOffset: 20 -> 201
     *  lastVLSN->lastnLsn mapping  20 -> 201
     *
     * @param startOfDelete is the VLSN that begins the range to delete,
     *        inclusive
     * @param prevLsn is the lsn of startOfDelete.getPrev(). We'll be using it
     * to cap off the end of the bucket, by assigning it to the lastLsn field.
     */
    void removeFromTail(long startOfDelete, long prevLsn) {

        if (empty()) {
            return;
        }

        if (lastVLSN < startOfDelete) {
            return;
        }

        /* Delete all the mappings. */
        if (firstVLSN >= startOfDelete) {
            lastVLSN = firstVLSN;
            lastLsn = DbLsn.NULL_LSN;
            fileOffsets.clear();
            return;
        }

        /* Delete some of the mappings. */
        int deleteIndex = getGTEIndex(startOfDelete);

        /* 
         * This should never happen, because the startOfDelete should be a vlsn
         * that is >= the first vlsn and we handled the case where
         * startOfDelete == firstVLSN already.)  Throw this exception to make
         * debugging info available.
         */
        if (deleteIndex <= 0) {
            throw EnvironmentFailureException.unexpectedState
                ("deleteIndex=" + deleteIndex +
                 " startOfDelete=" + startOfDelete +
                 " bucket=" + this);
        }

        /* See if there are any fileoffsets to prune off. */
        if (deleteIndex < fileOffsets.size()) {

            /*
             * The startOfDeleteVLSN is a value between the firstVLSN and
             * the last file offset.
             */
            if (prevLsn == DbLsn.NULL_LSN) {
                int lastPopulatedIndex = 
                    findPopulatedIndex(deleteIndex-1, false);
                if (lastPopulatedIndex != (deleteIndex -1)) {
                    deleteIndex = lastPopulatedIndex + 1;
                }
            }
            fileOffsets.truncate(deleteIndex);
        } else {
            /* 
             * The startOfDelete vlsn is somewhere between the last file offset
             * and the lastVLSN.
             */
            if (prevLsn == DbLsn.NULL_LSN) {
                int lastIndex = fileOffsets.size() - 1;
                int lastPopulatedIndex = findPopulatedIndex(lastIndex, false);
                if (lastPopulatedIndex < lastIndex) {
                    fileOffsets.truncate(lastPopulatedIndex);
                }
            }
        }

        /* Now set the lastVLSN -> lastLSN mapping. */
        if (prevLsn == DbLsn.NULL_LSN) {
            lastVLSN = ((fileOffsets.size()-1) * stride) + firstVLSN;
            Integer lastOffset = fileOffsets.get(fileOffsets.size() - 1);
            assert lastOffset != null;
            lastLsn = DbLsn.makeLsn(fileNumber,  lastOffset);
        } else {
            lastVLSN = VLSN.getPrev(startOfDelete);
            lastLsn = prevLsn;
        }
        dirty = true;
    }

    /* For unit tests */
    int getNumOffsets() {
        return fileOffsets.size();
    }


    /**
     * Write this bucket to the mapping database. 
     */
    void writeToDatabase(EnvironmentImpl envImpl,
                         DatabaseImpl bucketDbImpl,
                         Txn txn) {

        if (!dirty) {
            return;
        }

        Cursor c = null;
        try {
            c = DbInternal.makeCursor(bucketDbImpl,
                                      txn,
                                      CursorConfig.DEFAULT);
            writeToDatabase(envImpl, c);
        } finally {
            if (c != null) {
                c.close();
            }
        }
    }

    /**
     * Write this bucket to the mapping database using a cursor.  Note that
     * this method must disable critical eviction. Critical eviction makes the
     * calling thread search for a target IN node to evict. That target IN node
     * may or may not be in the internal VLSN db.
     * 
     * For example, when a new, replicated LN is inserted or modified, a
     * new VLSN is allocated. To do so, the app thread that is executing the
     * operation 
     *  A1. Takes a BIN latch on a BIN in a replicated db
     *  A2. Takes the VLSNINdex mutex
     *
     * Anyone calling writeDatabase() has to take these steps:
     *  B1. Take the VLSNIndex mutex
     *  B2. Get a BIN latch for a BIN in the internal vlsn db.
     *
     * This difference in locking hierarchy could cause a deadlock except for
     * the fact that A1 and B2 are guaranteed to be in different databases.  If
     * writeDatabase() also did critical eviction, it would have a step where
     * it tried to get a BIN latch on a replicated db, and we'd have a
     * deadlock. [#18475]
     */
    void writeToDatabase(EnvironmentImpl envImpl, Cursor cursor) {

        if (!dirty) {
            return;
        }

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        LongBinding.longToEntry(firstVLSN, key);
        VLSNBucketBinding bucketBinding = new VLSNBucketBinding();
        bucketBinding.objectToEntry(this, data);

        DbInternal.getCursorImpl(cursor).setAllowEviction(false);
        OperationStatus status = cursor.put(key, data);

        if (status != OperationStatus.SUCCESS) {
            throw EnvironmentFailureException.unexpectedState
                 (envImpl, "Unable to write VLSNBucket for file " +
                 fileNumber + " status=" + status,
                 ((RepImpl)envImpl).getVLSNIndex());
        }
        dirty = false;
    }

    /**
     * Instantiate this from the database. Assumes that this bucket will not be
     * used for insertion in the future.
     */
    public static VLSNBucket readFromDatabase(DatabaseEntry data) {

        VLSNBucketBinding mapperBinding = new VLSNBucketBinding();
        VLSNBucket bucket = mapperBinding.entryToObject(data);
        return bucket;
    }

    void fillDataEntry(DatabaseEntry data) {
        VLSNBucketBinding binding = new VLSNBucketBinding();
        binding.objectToEntry(this, data);
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return String.format("<VLSNBucket fileNum=%d(0x%x) numOffsets=%d " +
                             "stride=%d firstVLSN=%s lastVLSN=%s lastLsn=%s/>",
                             fileNumber, fileNumber,
                             (fileOffsets == null) ? 0 : fileOffsets.size(),
                             stride, firstVLSN, lastVLSN,
                             DbLsn.getNoFormatString(lastLsn));
    }

    /**
     * For debugging and tracing.
     */
    public void dump(PrintStream out) {
        if (fileOffsets == null) {
            return;
        }

        out.printf("fileNumber: 0x%x \n", fileNumber);
        long vlsnVal = firstVLSN;
        int newlineCounter = 0;
        for (Integer offset : fileOffsets) {
            out.printf(" [%d 0x%x]", vlsnVal, 
                       DbLsn.convertIntFileOffsetToLong(offset));

            vlsnVal += stride;
            if (++newlineCounter > 6) {
                out.println("\n");
                newlineCounter = 0;
            }
        }

        out.printf("\n---------Last: VLSN=%s LSN=%s", lastVLSN,
                   DbLsn.getNoFormatString(lastLsn));
    }

    public String dumpToLog() {
        if (fileOffsets == null) {
            return "";
        }  

        StringBuffer sb = new StringBuffer();
        sb.append(String.format("fileNumber: 0x%x \n", fileNumber));
        long vlsnVal = firstVLSN;
        int newlineCounter = 0;
        for (Integer offset : fileOffsets) {
            sb.append(String.format("[%d 0x%x]", vlsnVal,
                       DbLsn.convertIntFileOffsetToLong(offset)));
            vlsnVal += stride;
            if (++newlineCounter > 6) {
                sb.append("\n");
                newlineCounter = 0;
            }
        }
        sb.append(String.format("\n---------Last: VLSN=%s LSN=%s \n", lastVLSN,
                    DbLsn.getNoFormatString(lastLsn)));
        return sb.toString();
    }


    boolean isGhost() {
        return false;
    }

    void writeToTupleOutput(TupleOutput to) {

        to.writePackedLong(fileNumber);
        to.writePackedInt(stride);
        to.writePackedLong(firstVLSN);
        to.writePackedLong(lastVLSN);
        to.writePackedLong(lastLsn);
        to.writePackedInt(fileOffsets.size());
        for (Integer offset: fileOffsets) {
            to.writeUnsignedInt(DbLsn.convertIntFileOffsetToLong(offset));
        }
    }

    /**
     * Marshals a VLSNBucket to a byte buffer to store in the database.
     * Doesn't persist the file number, because that's the key of the database.
     * A number of the fields are transient and are also not stored.
     */
    private static class VLSNBucketBinding extends TupleBinding<VLSNBucket> {

        @Override
        public VLSNBucket entryToObject(TupleInput ti) {

            int onDiskVersion = ti.readPackedInt();
            if (onDiskVersion != VLSNBucket.VERSION) {
                throw EnvironmentFailureException.unexpectedState
                    ("Don't expect version diff on_disk=" + onDiskVersion +
                     " source=" + VLSNBucket.VERSION);
            }
            boolean isGhost = ti.readBoolean();
            VLSNBucket bucket = null;
            if (isGhost) {
                bucket = GhostBucket.makeNewInstance(ti);
            } else {
                bucket = new VLSNBucket(ti);
            }
            return bucket;
        }

        @Override
        public void objectToEntry(VLSNBucket bucket, TupleOutput to) {
            to.writePackedInt(VLSNBucket.VERSION);
            to.writeBoolean(bucket.isGhost());
            bucket.writeToTupleOutput(to);
        }
    }

    @SuppressWarnings("serial")
    protected static class TruncateableList<T> extends ArrayList<T>
        implements NotSerializable {

        TruncateableList() {
            super();
        }

        TruncateableList(int capacity) {
            super(capacity);
        }

        void truncate(int fromIndex) {
            removeRange(fromIndex, size());
        }
    }
}
