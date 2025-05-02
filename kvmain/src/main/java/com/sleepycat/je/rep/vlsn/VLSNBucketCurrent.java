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

import com.sleepycat.je.utilint.DbLsn;

/**
 * This class denotes the current read/write bucket in tracker's cache. See
 * VLSNBucket for more information about the VLSN->LSN mappings design.
 *
 * @see VLSNBucket
 */
public class VLSNBucketCurrent extends VLSNBucket{

    /*
     * The max number of offsets and maxDistance help guide when to close the
     * bucket and start a new one. Not persistent.
     */
    private int maxMappings;
    private int maxDistance;

    /*
     * True if the VLSNBucket will not accept any more modifications; used to
     * safeguard the bucket while the index is being written to disk.
     */
    private boolean closed = false;

    VLSNBucketCurrent(long fileNumber,
               int stride,
               int maxMappings,
               int maxDistance,
               long firstVLSN) {

        super(fileNumber, stride, firstVLSN);

        this.maxMappings = maxMappings;
        this.maxDistance = maxDistance;

        /*
         * The VLSNs in the bucket are initialized to indicate what range
         * should be covered by this bucket. But there may not be any offsets
         * recorded either in the lastLsn or the fileOffset.
         */
        this.lastVLSN = firstVLSN;
    }

    /**
     * Record the LSN location for this VLSN.
     *
     * One key issue is that puts() are not synchronized, and the VLSNs may
     * arrive out of order. If an out of order VLSN does arrive, we can still
     * assume that the earlier VLSNs have been successfully logged. If a VLSN
     * arrives that is divisible by the stride, and should be recorded in the
     * fileOffsets, but is not the next VLSN that should be recorded, we'll pad
     * out the fileOffsets list with placeholders.
     *
     * For example, suppose the stride is 3, and the first VLSN is 2. Then this
     * bucket should record VLSN 2, 5, 8, ... etc.  If VLSN 8 arrives before
     * VLSN 5, VLSN 8 will be recorded, and VLSN 5 will have an offset
     * placeholder of NO_OFFSET. It is a non-issue if VLSNs 3, 4, 6, 7 arrive
     * out of order, because they would not have been recorded anyway. This
     * should not happen often, because the stride should be fairly large, and
     * the calls to put() should be close together. If the insertion order is
     * vlsn 2, 5, 9, then the file offsets array will be a little short, and
     * will only have 2 elements, instead of 3.
     *
     * We follow this policy because we must always have a valid begin and end
     * point for the range. We must handle placeholders in all cases, and can't
     * count of later vlsn inserts, because a bucket can become immutable at
     * any time if it is flushed to disk.
     *
     * @return false if this bucket will not accept this VLSN. Generally, a
     * refusal might happen because the bucket was full or the mapping is too
     * large a distance away from the previous mapping. In that case, the
     * tracker will start another bucket.
     */
    synchronized boolean put(long vlsn, long lsn) {

        if (closed) {
            return false;
        }

        if (!belongs(vlsn, lsn)) {
            return false;
        }

        /*
         * Add it to the fileOffset if it's on a stride boundary and is the
         * next mapping in the fileOffset list.
         */
        if (isModulo(vlsn)) {
            int index = getIndex(vlsn);
            int listLen = fileOffsets.size();
            if (index < listLen) {
                fileOffsets.set(index, DbLsn.getFileOffsetAsInt(lsn));
            } else if (index == listLen) {
                fileOffsets.add(DbLsn.getFileOffsetAsInt(lsn));
            } else {
                for (int i = listLen; i < index; i++) {
                    fileOffsets.add(NO_OFFSET);
                }
                fileOffsets.add(DbLsn.getFileOffsetAsInt(lsn));
            }
            dirty = true;
        }

        /* If the lastLsn is less, or not initialized, set it to this VLSN. */
        if ((lastVLSN < vlsn) ||
            (lastLsn == DbLsn.NULL_LSN)) {
            lastVLSN = vlsn;
            lastLsn = lsn;
            dirty = true;
        }

        return true;
    }

    /**
     * @return true if this VLSN->LSN mapping should go into this bucket.
     */
    private boolean belongs(long vlsn, long lsn) {
        assert vlsn >= firstVLSN:
            "firstVLSN = " + firstVLSN + " should not be greater than " + vlsn;

        if (DbLsn.getFileNumber(lsn) != fileNumber) {
            /* Mappings must be for same file. */
            return false;
        }

        if (emptyInternal()) {
            return true;
        }

        /*
         * Some other thread beat us to the put() call and inserted a later
         * mapping, so we know for sure that we fit in this bucket
         */
        if (lastVLSN > vlsn) {
            return true;
        }

        boolean onStrideBoundary = isModulo(vlsn);
        if (onStrideBoundary && (fileOffsets.size() >= maxMappings)) {
            /* Too full. */
            return false;
        }

        /*
         * Will this VLSN be next one recorded in the fileOffsets? If so,
         * calculate the scan distance.
         */
        if ((onStrideBoundary && (getIndex(vlsn) == fileOffsets.size())) ||
            lastVLSN < vlsn) {
            /* This VLSN is going in at the tail of the bucket. */
            int lastOffset = fileOffsets.get(fileOffsets.size() - 1);
            if ((DbLsn.getFileOffset(lsn) -
                 DbLsn.convertIntFileOffsetToLong(lastOffset)) >
                maxDistance) {
                /* The scan distance is exceeded. */
                return false;
            }
        }

        return true;
    }

    @Override
    synchronized boolean owns(long vlsn) {
        return super.owns(vlsn);
    }

    @Override
    synchronized long getFirst() {
        return super.getFirst();
    }

    @Override
    synchronized long getLast() {
        return super.getLast();
    }

    @Override
    synchronized boolean empty() {
        return super.empty();
    }

    @Override
    public synchronized long getGTELsn(long vlsn) {
        return super.getGTELsn(vlsn);
    }

    @Override
    synchronized long getLTELsn(long vlsn) {
        return super.getLTELsn(vlsn);
    }

    @Override
    public synchronized long getLsn(long vlsn) {
        return super.getLsn(vlsn);
    }

    @Override
    synchronized long getLastLsn() {
        return super.getLastLsn();
    }

    void close() {
        closed = true;
    }
}
