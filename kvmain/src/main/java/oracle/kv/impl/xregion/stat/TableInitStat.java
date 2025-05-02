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

package oracle.kv.impl.xregion.stat;

import java.io.Serializable;
import java.util.Objects;

import oracle.kv.impl.util.NonNullByDefault;
import oracle.nosql.common.json.JsonUtils;

/**
 * Class including table initialization statistics
 */
public class TableInitStat implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * time in ms representing start time while transfer not started
     */
    private static final long NOT_START_TIME_MS = 0;
    /**
     * time in ms representing complete time while transfer incomplete
     */
    static final long NOT_COMPLETE_TIME_MS = 0;

    /**
     * source region name
     */
    private final String region;
    /**
     * timestamp in ms when transfer starts, Long.MAX if not start
     */
    private volatile long transferStartMs = NOT_START_TIME_MS;
    /**
     * timestamp in ms when transfer completes, 0 if not complete
     */
    private volatile long transferCompleteMs = NOT_COMPLETE_TIME_MS;
    /**
     * table transfer elapsed time, -1 if not complete
     */
    private volatile long elapsedMs = -1;
    /**
     * # of rows transferred from source in initialization, excluding tombstones
     */
    private volatile long transferRows = 0;
    /**
     * # of tombstones transferred from source in initialization
     */
    private volatile long transferTombstones = 0;
    /**
     * # of rows winning conf. res. and persisted in initialization,
     * excluding tombstones
     */
    private volatile long persistRows = 0;
    /**
     * # of tombstones winning conf. res. and persisted in initialization
     */
    private volatile long persistTombstones = 0;
    /**
     * # of rows transferred from source but expired
     */
    private volatile long expireRows = 0;
    /**
     * # of bytes transferred from source in initialization
     */
    private volatile long transferBytes = 0;
    /**
     * # of bytes persisted in initialization
     */
    private volatile long persistBytes = 0;
    /**
     * table initialization state
     */
    private volatile TableInitState state = TableInitState.NOT_START;
    /**
     * Sync locker to ensure callers see consistent values of
     * {@link #transferStartMs}, {@link #transferCompleteMs}, {@link #state}
     * and {@link #elapsedMs}
     */
    private final String transStateLock = "transferStateLock";

    @NonNullByDefault
    public TableInitStat(String region) {
        this.region = region;
    }

    @NonNullByDefault
    public TableInitStat(TableInitStat other) {
        this.region = other.getRegion();
        this.transferStartMs = other.getTransferStartMs();
        this.transferCompleteMs = other.getTransferCompleteMs();
        this.elapsedMs = other.getElapsedMs();
        this.transferRows = other.getTransferRows();
        this.transferTombstones = other.getTransferTombstones();
        this.persistRows = other.getPersistRows();
        this.persistTombstones = other.getPersistTombstones();
        this.expireRows = other.getExpireRows();
        this.transferBytes = other.getTransferBytes();
        this.persistBytes = other.getPersistBytes();
        this.state = other.state;
    }

    /* getter internally used but need public access to create JSON */
    public long getTransferStartMs() {
        synchronized (transStateLock) {
            return transferStartMs;
        }
    }

    public long getTransferCompleteMs() {
        synchronized (transStateLock) {
            return transferCompleteMs;
        }
    }

    public long getElapsedMs() {
        synchronized (transStateLock) {
            return elapsedMs;
        }
    }

    public long getTransferRows() {
        return transferRows;
    }

    public long getTransferTombstones() {
        return transferTombstones;
    }

    public long getPersistRows() {
        return persistRows;
    }

    public long getPersistTombstones() {
        return persistTombstones;
    }

    public long getExpireRows() {
        return expireRows;
    }

    long getTransferBytes() {
        return transferBytes;
    }

    long getPersistBytes() {
        return persistBytes;
    }

    public TableInitState getState() {
        synchronized (transStateLock) {
            return state;
        }
    }

    public String getRegion() {
        return region;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TableInitStat)) {
            return false;
        }
        final TableInitStat other = (TableInitStat) obj;
        final boolean equalRegion = Objects.equals(region, other.region);
        return equalRegion &&
               transferStartMs == other.transferStartMs &&
               transferCompleteMs == other.transferCompleteMs &&
               elapsedMs == other.elapsedMs &&
               transferRows == other.transferRows &&
               transferTombstones == other.transferTombstones &&
               persistRows == other.persistRows &&
               persistTombstones == other.persistTombstones &&
               expireRows == other.expireRows &&
               transferBytes == other.transferBytes &&
               persistBytes == other.persistBytes &&
               state.equals(other.state);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(region) +
               Long.hashCode(transferStartMs) +
               Long.hashCode(transferCompleteMs) +
               Long.hashCode(elapsedMs) +
               Long.hashCode(transferRows) +
               Long.hashCode(transferTombstones) +
               Long.hashCode(persistRows) +
               Long.hashCode(persistTombstones) +
               Long.hashCode(expireRows) +
               Long.hashCode(transferBytes) +
               Long.hashCode(persistBytes) +
               state.hashCode();
    }

    @Override
    public String toString() {
        return JsonUtils.prettyPrint(this);
    }

    public void setState(TableInitState st) {
        synchronized (transStateLock) {
            state = st;
        }
    }

    public void setTransferStartMs(long ts) {
        synchronized (transStateLock) {
            transferStartMs = ts;
            state = TableInitState.IN_PROGRESS;
        }
    }

    public void setTransferCompleteMs(long ts) {
        synchronized (transStateLock) {
            transferCompleteMs = ts;
            elapsedMs = (transferCompleteMs == NOT_COMPLETE_TIME_MS) ? -1 :
                (transferCompleteMs - transferStartMs);
            state = TableInitState.COMPLETE;
        }
    }

    public synchronized void incrTransferredRows(long delta) {
        transferRows += delta;
    }

    public synchronized void incrTransTombstones(long delta) {
        transferTombstones += delta;
    }

    public synchronized void incrPersistedRows(long delta) {
        persistRows += delta;
    }

    public synchronized void incrPersistedTombstones(long delta) {
        persistTombstones += delta;
    }

    public synchronized void incrExpired(long delta) {
        expireRows += delta;
    }

    public synchronized void incrTransBytes(long delta) {
        transferBytes += delta;
    }

    public synchronized void incrPersistBytes(long delta) {
        persistBytes += delta;
    }

    /**
     * Table initialization state
     */
    public enum TableInitState {
        /** Initialization needed but not yet started */
        NOT_START {
            @Override
            public String toString() {
                return "not started";
            }
        },

        /** initialization in progress */
        IN_PROGRESS {
            @Override
            public String toString() {
                return "in progress";
            }
        },

        /** initialization complete and all rows transferred */
        COMPLETE {
            @Override
            public String toString() {
                return "complete";
            }
        },

        /** initialization failed */
        ERROR {
            @Override
            public String toString() {
                return "error";
            }
        },

        /** initialization incomplete because of shutdown */
        SHUTDOWN {
            @Override
            public String toString() {
                return "shutdown";
            }
        }
    }
}
