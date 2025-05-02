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

import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors; 


import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.statcap.StatUtils;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.Timestamp;

/**
 * CheckpointEnd encapsulates the information needed by a checkpoint end log
 * entry.
 */
public class CheckpointEnd implements Loggable {

    private static final byte ROOT_LSN_MASK = (byte) 0x1;
    private static final byte CLEANED_FILES_MASK = (byte) 0x2;

    /*
     * invoker is just a way to tag each checkpoint in the log for easier log
     * based debugging. It will tell us whether the checkpoint was invoked by
     * recovery, the daemon, the api, or the cleaner.
     */
    private String invoker;

    private Timestamp endTime;
    private long checkpointStartLsn;
    private boolean rootLsnExists;
    private long rootLsn;
    private long firstActiveLsn;
    private long lastLocalNodeId;
    private long lastReplicatedNodeId;
    private long lastLocalDbId;
    private long lastReplicatedDbId;
    private long lastLocalTxnId;
    private long lastReplicatedTxnId;
    private long lastLocalExtinctionId;
    private long lastReplicatedExtinctionId;
    private long id;
    private Set<Long> completedScanIds;

    /*
     * True if there were cleaned files to delete after this checkpoint.
     * Used to govern HA recovery truncation. Defaults to true.
     */
    private boolean cleanedFilesToDelete;

    public CheckpointEnd(String invoker,
                         long checkpointStartLsn,
                         long rootLsn,
                         long firstActiveLsn,
                         long lastLocalNodeId,
                         long lastReplicatedNodeId,
                         long lastLocalDbId,
                         long lastReplicatedDbId,
                         long lastLocalTxnId,
                         long lastReplicatedTxnId,
                         long lastLocalExtinctionId,
                         long lastReplicatedExtinctionId,
                         long id,
                         boolean cleanedFilesToDelete,
                         Set<Long> completedScanIds) {
        if (invoker == null) {
            this.invoker = "";
        } else {
            this.invoker = invoker;
        }

        Calendar cal = Calendar.getInstance();
        this.endTime = new Timestamp(cal.getTime().getTime());
        this.checkpointStartLsn = checkpointStartLsn;
        this.rootLsn = rootLsn;
        if (rootLsn == DbLsn.NULL_LSN) {
            rootLsnExists = false;
        } else {
            rootLsnExists = true;
        }
        if (firstActiveLsn == DbLsn.NULL_LSN) {
            this.firstActiveLsn = checkpointStartLsn;
        } else {
            this.firstActiveLsn = firstActiveLsn;
        }
        this.lastLocalNodeId = lastLocalNodeId;
        this.lastReplicatedNodeId = lastReplicatedNodeId;
        this.lastLocalDbId = lastLocalDbId;
        this.lastReplicatedDbId = lastReplicatedDbId;
        this.lastLocalTxnId = lastLocalTxnId;
        this.lastReplicatedTxnId = lastReplicatedTxnId;
        this.lastLocalExtinctionId = lastLocalExtinctionId;
        this.lastReplicatedExtinctionId = lastReplicatedExtinctionId;
        this.id = id;
        this.cleanedFilesToDelete = cleanedFilesToDelete;
        this.completedScanIds = completedScanIds;
    }

    /* For logging only */
    public CheckpointEnd() {
        checkpointStartLsn = DbLsn.NULL_LSN;
        rootLsn = DbLsn.NULL_LSN;
        firstActiveLsn = DbLsn.NULL_LSN;
    }

    public String getInvoker() {
        return invoker;
    }

    /*
     * Logging support for writing to the log
     */

    /**
     * @see Loggable#getLogSize
     */
    public int getLogSize() {
        int size =
            LogUtils.getStringLogSize(invoker) +    // invoker
            LogUtils.getTimestampLogSize(endTime) + // endTime
            LogUtils.getPackedLongLogSize(checkpointStartLsn) +
            1 +           // flags: rootLsnExists, cleanedFilesToDelete
            LogUtils.getPackedLongLogSize(firstActiveLsn) +
            LogUtils.getPackedLongLogSize(lastLocalNodeId) +
            LogUtils.getPackedLongLogSize(lastReplicatedNodeId) +
            LogUtils.getPackedLongLogSize(lastLocalDbId) +
            LogUtils.getPackedLongLogSize(lastReplicatedDbId) +
            LogUtils.getPackedLongLogSize(lastLocalTxnId) +
            LogUtils.getPackedLongLogSize(lastReplicatedTxnId) +
            LogUtils.getPackedLongLogSize(lastLocalExtinctionId) +
            LogUtils.getPackedLongLogSize(lastReplicatedExtinctionId) +
            LogUtils.getPackedLongLogSize(id);

        if (rootLsnExists) {
            size += LogUtils.getPackedLongLogSize(rootLsn);
        }
        if (completedScanIds != null) {
            size += LogUtils.getPackedLongLogSize(completedScanIds.size());
            for (Long scanId : completedScanIds) {
                size += LogUtils.getPackedLongLogSize(scanId.longValue());
            }
        } else {
            size += LogUtils.getPackedLongLogSize(0);
        }
        return size;
    }

    /**
     * @see Loggable#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {
        LogUtils.writeString(logBuffer, invoker);
        LogUtils.writeTimestamp(logBuffer, endTime);
        LogUtils.writePackedLong(logBuffer, checkpointStartLsn);

        byte flags = 0;
        if (rootLsnExists) {
            flags |= ROOT_LSN_MASK;
        }

        if (cleanedFilesToDelete) {
            flags |= CLEANED_FILES_MASK;
        }

        logBuffer.put(flags);

        if (rootLsnExists) {
            LogUtils.writePackedLong(logBuffer, rootLsn);
        }
        LogUtils.writePackedLong(logBuffer, firstActiveLsn);

        LogUtils.writePackedLong(logBuffer, lastLocalNodeId);
        LogUtils.writePackedLong(logBuffer, lastReplicatedNodeId);

        LogUtils.writePackedLong(logBuffer, lastLocalDbId);
        LogUtils.writePackedLong(logBuffer, lastReplicatedDbId);

        LogUtils.writePackedLong(logBuffer, lastLocalTxnId);
        LogUtils.writePackedLong(logBuffer, lastReplicatedTxnId);

        LogUtils.writePackedLong(logBuffer, lastLocalExtinctionId);
        LogUtils.writePackedLong(logBuffer, lastReplicatedExtinctionId);

        LogUtils.writePackedLong(logBuffer, id);
        if (completedScanIds != null) {
            LogUtils.writePackedLong(logBuffer, completedScanIds.size());
            for (Long scanId : completedScanIds) {
                LogUtils.writePackedLong(logBuffer, scanId.longValue());
            }
        } else {
            LogUtils.writePackedLong(logBuffer, 0);
        }
    }

    /**
     * @see Loggable#readFromLog
     */
    public void readFromLog(EnvironmentImpl envImpl,
                            ByteBuffer logBuffer,
                            int entryVersion) {
        invoker = LogUtils.readString(logBuffer,
            entryVersion);
        endTime = LogUtils.readTimestamp(logBuffer);
        checkpointStartLsn = LogUtils.readPackedLong(logBuffer);
        byte flags = logBuffer.get();
        rootLsnExists = (flags & ROOT_LSN_MASK) != 0;

        if (rootLsnExists) {
            rootLsn = LogUtils.readPackedLong(logBuffer);
        }

        cleanedFilesToDelete = ((flags & CLEANED_FILES_MASK) != 0);

        firstActiveLsn = LogUtils.readPackedLong(logBuffer);

        lastLocalNodeId = LogUtils.readPackedLong(logBuffer);
        lastReplicatedNodeId = LogUtils.readPackedLong(logBuffer);

        lastLocalDbId = LogUtils.readPackedLong(logBuffer);
        lastReplicatedDbId = LogUtils.readPackedLong(logBuffer);

        lastLocalTxnId = LogUtils.readPackedLong(logBuffer);
        lastReplicatedTxnId = LogUtils.readPackedLong(logBuffer);

        if (entryVersion >= 17) {
            lastLocalExtinctionId = LogUtils.readPackedLong(logBuffer);
            lastReplicatedExtinctionId = LogUtils.readPackedLong(logBuffer);
        }

        id = LogUtils.readPackedLong(logBuffer);

        if (entryVersion <= 10) {
            /* Read defunct CleanerLogSummary. */
            LogUtils.readPackedLong(logBuffer);
            LogUtils.readPackedInt(logBuffer);
            final int nAvgLNSizes = LogUtils.readPackedInt(logBuffer);
            for (int i = 0; i < nAvgLNSizes; i += 1) {
                LogUtils.readPackedInt(logBuffer);
                LogUtils.readPackedInt(logBuffer);
                LogUtils.readPackedInt(logBuffer);
                LogUtils.readPackedInt(logBuffer);
            }
        }
        if (entryVersion >= LogEntryType.LOG_VERSION_CKPT_SCAN_IDS) {
            completedScanIds = new HashSet<>();     
            long size = LogUtils.readPackedLong(logBuffer); 
            for (int i =0; i < size; i++) {
                completedScanIds.add(LogUtils.readPackedLong(logBuffer));
            }
        }
    }

    /**
     * @see Loggable#dumpLog
     */
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append("<CkptEnd invoker=\"").append(invoker);
        sb.append("\" time=\"").append(StatUtils.getDate(endTime));
        sb.append("\" lastLocalNodeId=\"").append(lastLocalNodeId);
        sb.append("\" lastReplicatedNodeId=\"").append(lastReplicatedNodeId);
        sb.append("\" lastLocalDbId=\"").append(lastLocalDbId);
        sb.append("\" lastReplicatedDbId=\"").append(lastReplicatedDbId);
        sb.append("\" lastLocalTxnId=\"").append(lastLocalTxnId);
        sb.append("\" lastReplicatedTxnId=\"").append(lastReplicatedTxnId);
        sb.append("\" lastLocalExtinctionId=\"").append(lastLocalExtinctionId);
        sb.append("\" lastReplicatedExtinctionId=\"").
            append(lastReplicatedExtinctionId);
        sb.append("\" id=\"").append(id);
        sb.append("\" rootExists=\"").append(rootLsnExists);
        sb.append("\" cleanedFilesToDelete=\"").append(cleanedFilesToDelete);
        if (completedScanIds != null) {
            sb.append("\" completedScanIds=\"").append(completedScanIds.
                    stream().map(Object::toString).collect(
                        Collectors.joining(",")));
        }
        sb.append("\">");
        sb.append("<ckptStart>");
        sb.append(DbLsn.toString(checkpointStartLsn));
        sb.append("</ckptStart>");

        if (rootLsnExists) {
            sb.append("<root>");
            sb.append(DbLsn.toString(rootLsn));
            sb.append("</root>");
        }
        sb.append("<firstActive>");
        sb.append(DbLsn.toString(firstActiveLsn));
        sb.append("</firstActive>");
        sb.append("</CkptEnd>");
    }

    /**
     * @see Loggable#getTransactionId
     */
    public long getTransactionId() {
        return 0;
    }

    /**
     * @see Loggable#logicalEquals
     * Always return false, this item should never be compared.
     */
    public boolean logicalEquals(Loggable other) {
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("time=").append(endTime);
        sb.append(" lastLocalNodeId=").append(lastLocalNodeId);
        sb.append(" lastReplicatedNodeId=").append(lastReplicatedNodeId);
        sb.append(" lastLocalDbId=").append(lastLocalDbId);
        sb.append(" lastReplicatedDbId=").append(lastReplicatedDbId);
        sb.append(" lastLocalTxnId=").append(lastLocalTxnId);
        sb.append(" lastReplicatedTxnId=").append(lastReplicatedTxnId);
        sb.append(" lastLocalExtinctionId=").append(lastLocalExtinctionId);
        sb.append(" lastReplicatedExtinctionId=").
            append(lastReplicatedExtinctionId);
        sb.append(" id=").append(id);
        sb.append(" rootExists=").append(rootLsnExists);
        sb.append(" ckptStartLsn=").append
            (DbLsn.getNoFormatString(checkpointStartLsn));
        if (rootLsnExists) {
            sb.append(" root=").append(DbLsn.getNoFormatString(rootLsn));
        }
        sb.append(" firstActive=").
           append(DbLsn.getNoFormatString(firstActiveLsn));
        sb.append(" cleanedFilesToDelete=").append(cleanedFilesToDelete);
        if (completedScanIds != null) {
            sb.append("completedScanIds=").append(completedScanIds.stream().
                    map(Object::toString).collect(Collectors.joining(",")));
        }
        return sb.toString();
    }

    /*
     * Accessors
     */
    long getCheckpointStartLsn() {
        return checkpointStartLsn;
    }

    long getRootLsn() {
        return rootLsn;
    }

    public long getFirstActiveLsn() {
        return firstActiveLsn;
    }

    long getLastLocalNodeId() {
        return lastLocalNodeId;
    }

    long getLastReplicatedNodeId() {
        return lastReplicatedNodeId;
    }

    long getLastLocalDbId() {
        return lastLocalDbId;
    }

    long getLastReplicatedDbId() {
        return lastReplicatedDbId;
    }

    long getLastLocalTxnId() {
        return lastLocalTxnId;
    }

    long getLastReplicatedTxnId() {
        return lastReplicatedTxnId;
    }

    long getLastLocalExtinctionId() {
        return lastLocalExtinctionId;
    }

    long getLastReplicatedExtinctionId() {
        return lastReplicatedExtinctionId;
    }

    public long getId() {
        return id;
    }

    public boolean getCleanedFilesToDelete() {
        return cleanedFilesToDelete;
    }

    public Set<Long> getCompletedScanIds() {
        return completedScanIds;
    }
}
