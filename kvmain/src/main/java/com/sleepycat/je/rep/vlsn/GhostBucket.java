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

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.utilint.DbLsn;

/**
 * A ghost bucket stands in as a placeholder for a set of vlsns that are 
 * unknown. This kind of bucket can only be present at the very beginning of 
 * the vlsn range. 
 *
 * This fulfills an edge case that can arise when vlsns are inserted out of 
 * order, and log cleaner truncation lops off the leading edge of the index. 
 * For example, suppose vlsns were inserted in this order:

 * vlsnIndex.put(vlsn=2, lsn=1/2)
 * vlsnIndex.put(vlsn=1, lsn=1/0)
 * vlsnIndex.put(vlsn=3, lsn=1/3)
 * ...
 * vlsnIndex.put(vlsn=5, lsn=2/9)
 * vlsnIndex.put(vlsn=4, lsn=2/0)
 * vlsnIndex.put(vlsn=6, lsn=2/10)
 * ..
 * This results in an index that has two buckets. Bucket 1 = {vlsn 2,3} and
 * bucket 2 = {vlsn 5,6}. If we log clean file 1, we will truncate log at vlsn
 * 3, and the new range will be vlsn 4-> vlsn 6. But the beginning and end of
 * each range needs to have a valid bucket, and there is no bucket to represent
 * vlsn 4. A GhostBucket is added to the head of the bucket set.
 */
class GhostBucket extends VLSNBucket {
    private long firstPossibleLsn;
    private long lastPossibleLsn;
    
    GhostBucket(long ghostVLSN,
                long firstPossibleLsn,
                long lastPossibleLsn) {
        /* 
         * Use ghostVLSN for the firstVLSN, which will make the own(),
         * getFirst, getLast() methods work.
         */
        super(DbLsn.getFileNumber(firstPossibleLsn), // fileNumber
              1, // maxDistance,
              ghostVLSN); // firstVLSN
        this.lastVLSN = ghostVLSN;
        this.firstPossibleLsn = firstPossibleLsn;
        this.lastPossibleLsn = lastPossibleLsn;
        dirty = true;
    }

    /**
     * Ideally, this would be a constructor, but we have to read several
     * items off the tuple input first before calling super();
     */
    static GhostBucket makeNewInstance(TupleInput ti) {
        long ghostVLSN = ti.readPackedLong();
        long firstLsn = ti.readPackedLong();
        long lastLsn = ti.readPackedLong();
        return new GhostBucket(ghostVLSN, firstLsn, lastLsn);
    }

    @Override
    boolean isGhost() {
        return true;
    }

    @Override
    void writeToTupleOutput(TupleOutput to) {
        to.writePackedLong(firstVLSN);
        to.writePackedLong(firstPossibleLsn);
        to.writePackedLong(lastPossibleLsn);
    }

    /**
     * Return a lsn as a starting point for a backward scan.
     */
    @Override
    public synchronized long getGTELsn(long vlsn) { 
        return lastPossibleLsn;
    }

    /**
     * Return a lsn as a starting point for a forward scan.
     */
    @Override
    synchronized long getLTELsn(long vlsn) { 
        return firstPossibleLsn;
    }

    /**
     * There is no mapping for this VLSN, so always return NULL_LSN.
     */
    @Override
    public synchronized long getLsn(long vlsn) { 
        return DbLsn.NULL_LSN;            
    }

    /**
     * Return a file number that is less or equal to the first mapped vlsn.
     */
    @Override 
    long getLTEFileNumber() {
        return DbLsn.getFileNumber(firstPossibleLsn);
    }

    @Override
    long getGTEFileNumber() {
        return DbLsn.getFileNumber(lastPossibleLsn);
    }

    @Override
    void removeFromTail(long startOfDelete, long prevLsn) {
    
        throw EnvironmentFailureException.unexpectedState
            ("Shouldn't be called");
    }

    @Override
    int getNumOffsets() {
        return 0;
    }

    @Override 
    public String toString() {
        StringBuilder sb = new StringBuilder("<GhostBucket vlsn=");
        sb.append(firstVLSN);
        sb.append(" firstLsn=");
        sb.append(DbLsn.getNoFormatString(firstPossibleLsn));
        sb.append(" lastLsn=").append(DbLsn.getNoFormatString(lastPossibleLsn));
        sb.append("/>");
        return sb.toString();
    }
}

