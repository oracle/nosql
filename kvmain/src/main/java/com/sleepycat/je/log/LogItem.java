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

package com.sleepycat.je.log;

import java.nio.ByteBuffer;

import com.sleepycat.je.beforeimage.BeforeImageContext;
import com.sleepycat.je.log.entry.ReplicableLogEntry;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Values returned when a item is logged.
 *
 * This class is used as a simple struct for returning multiple values, and
 * does not need getters and setters.
 */
public class LogItem {

    static final LogItem EMPTY_ITEM = new LogItem();

    /**
     * LSN of the new log entry.  Is NULL_LSN if a BIN-delta is logged.  If
     * not NULL_LSN for a tree node, is typically used to update the slot in
     * the parent IN.
     */
    public long lsn = DbLsn.NULL_LSN;

    /**
     * Size of the new log entry. Is used to update the slot in the parent IN.
     */
    public int size = 0;

    /**
     * The header of the new log entry. Used by HA to do VLSN tracking and
     * implement a tip cache.
     */
    public LogEntryHeader header = null;

    /**
     * The bytes of the new log entry for a cached buffer. Used by HA to
     * implement a tip cache.
     *
     * For a replicated item, exactly one of cachedBuffer and nonCachedBuffer
     * is non-null. For a non-replicated item, both are null.
     */
    CachedLogItemBuffer cachedBuffer = null;

    /**
     * The bytes of the new log entry for a non-cached buffer. Used by HA to
     * implement a tip cache.
     *
     * For a replicated item, at most one of cachedBuffer and nonCachedBuffer
     * is non-null, and if both are null then cachedEntry is non-null. For a
     * non-replicated item, all three fields are null.
     */
    ByteBuffer nonCachedBuffer = null;

    byte[] bImgData = null;
    BeforeImageContext bImgCtx = null;

    public ByteBuffer getBuffer() {
        return (cachedBuffer != null) ?
            cachedBuffer.getBuffer() : nonCachedBuffer;
    }

    public void setBeforeImageData(byte[] bImgData) {
        this.bImgData = bImgData;
    }

    public void setBeforeImageCtx(BeforeImageContext bImgCtx) {
        this.bImgCtx = bImgCtx;
    }

    public BeforeImageContext getBeforeImageCtx() {
        return bImgCtx;
    }

    public byte[] getBeforeImageData() {
        return bImgData;
    }

    public boolean incrementUse() {
        return (cachedBuffer == null) || cachedBuffer.incrementUse();
    }

    public void decrementUse() {
        if (cachedBuffer != null) {
            cachedBuffer.decrementUse();
        }
    }

    public void requestDeallocate() {
        if (cachedBuffer != null) {
            cachedBuffer.requestDeallocate();
        }
    }

    public String getBufferState() {
        return (cachedBuffer != null) ?
            cachedBuffer.toString() : "[not cached]";
    }

    public void verifyBufferInUse() {
        if (cachedBuffer != null) {
            cachedBuffer.verifyResourceInUse();
        }
    }

    /**
     * Used for saving the materialized form of the buffer in LogItemCache.
     * Also used for serializing over the wire when both cachedBuffer and
     * nonCachedBuffer are null (when LogParams.immutableLogEntry is true).
     */
    public volatile ReplicableLogEntry cachedEntry = null;

    public boolean deltaBinLogged = false;
}
