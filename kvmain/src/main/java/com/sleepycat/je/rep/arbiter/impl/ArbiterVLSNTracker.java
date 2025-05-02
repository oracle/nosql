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

package com.sleepycat.je.rep.arbiter.impl;

import static com.sleepycat.je.rep.arbiter.impl.ArbiterStatDefinition.ARB_DTVLSN;
import static com.sleepycat.je.rep.arbiter.impl.ArbiterStatDefinition.ARB_N_FSYNCS;
import static com.sleepycat.je.rep.arbiter.impl.ArbiterStatDefinition.ARB_N_WRITES;
import static com.sleepycat.je.rep.arbiter.impl.ArbiterStatDefinition.ARB_VLSN;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.rep.impl.node.MasterTerm;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatGroup;

/**
 * This class is used to maintain two pieces of persistent state. The
 * replication group node identifier of the Arbiter and a VLSN value that
 * represents the highest commit record VLSN the Arbiter has acknowledged.
 */
class ArbiterVLSNTracker {
    /* Added masterTerm */
    private static final int VERSION_3 = 3;
    /* Added lastAckNodeId */
    private static final int VERSION_2 = 2;
    private static final int VERSION_1 = 1;

    private static final int VERSION = VERSION_3;
    /*
     * Version 1 format
     * int version
     * int nodeid
     * VLSN data
     *
     * Version 2 format
     * same as above
     * int nodeid of last acked transaction
     *
     * Version 3 format
     * long masterTerm of last acked transaction
     */

    private RandomAccessFile raf;
    private final File dataFile;
    private long currentVLSN = NULL_VLSN;
    private volatile long dtvlsn = NULL_VLSN;
    private final int VERSION_OFFSET = 0;
    private final int NODEID_OFFSET = Integer.SIZE + VERSION_OFFSET;
    private final int DATA_OFFSET = Integer.SIZE + NODEID_OFFSET;
    private int nodeId = NameIdPair.NULL_NODE_ID;
    private volatile long masterTerm = MasterTerm.MIN_TERM;
    private final StatGroup stats;
    private final LongStat nWrites;
    private final LongStat nFSyncs;
    private final LongStat vlsnStat;
    private final LongStat dtVlsnStat;
    private int lastAckNodeId = NameIdPair.NULL.getId();
    private long lastMasterTerm = MasterTerm.MIN_TERM;

    ArbiterVLSNTracker(File file) {
        dataFile = file;
        boolean fileExists = dataFile.exists();

        stats = new StatGroup(ArbiterStatDefinition.ARBIO_GROUP_NAME,
                              ArbiterStatDefinition.ARBIO_GROUP_DESC);
        nFSyncs = new LongStat(stats, ARB_N_FSYNCS);
        nWrites = new LongStat(stats, ARB_N_WRITES);
        vlsnStat = new LongStat(stats, ARB_VLSN);
        dtVlsnStat = new LongStat(stats, ARB_DTVLSN);
        try {
            raf = new RandomAccessFile(dataFile, "rw");
            if (fileExists) {
                final int readVersion = readVersion();
                if (readVersion > VERSION) {
                    throw new RuntimeException(
                        "Arbiter data file does not have a supported " +
                        "version field " +
                        dataFile.getAbsolutePath());
                }
                nodeId = readNodeId();
                if (raf.length() > DATA_OFFSET) {
                    raf.seek(DATA_OFFSET);
                    currentVLSN = raf.readLong();
                    dtvlsn = raf.readLong();
                    if (readVersion > VERSION_1) {
                        lastAckNodeId = raf.readInt();
                    }
                    if (readVersion >= VERSION_3) {
                        masterTerm = raf.readLong();
                    }
                }
            } else {
                writeVersion(VERSION);
                writeNodeIdInternal(nodeId);
            }
        } catch (IOException e) {
            throw new RuntimeException(
                "Unable to read the Arbiter data file " +
                dataFile.getAbsolutePath());
        }
        catch (Exception e) {
            throw new RuntimeException(
                "Unable to open the Arbiter data file " +
                dataFile.getAbsolutePath() + " exception " + e.getMessage());
        }
    }

    public StatGroup loadStats(StatsConfig config) {
        vlsnStat.set(get());
        dtVlsnStat.set(getDTVLSN());
        return stats.cloneGroup(config.getClear());
    }

    public synchronized void writeNodeId(int id) {
        if (nodeId == id) {
            return;
        }
        writeNodeIdInternal(id);
    }

    public synchronized int getCachedNodeId() {
        return nodeId;
    }

    private void writeNodeIdInternal(int id) {
        if (raf == null) {
            throw new RuntimeException(
                "Internal error: Unable to write the Arbiter data file " +
                " because the file is not open." +
                dataFile.getAbsolutePath());
        }
        try {
            raf.seek(NODEID_OFFSET);
            raf.writeInt(id);
            nWrites.increment();
            doFSync();
        } catch (IOException e) {
            throw new RuntimeException(
                "Unable to write the Arbiter data file " +
                dataFile.getAbsolutePath());
        }
    }

    private int readNodeId() {
        if (raf == null) {
            throw new RuntimeException(
                "Internal error: Unable to read the Arbiter data file " +
                " because the file is not open." +
                dataFile.getAbsolutePath());
        }
        try {
            raf.seek(NODEID_OFFSET);
            return raf.readInt();
        } catch (IOException e) {
            throw new RuntimeException(
                "Unable to read the Arbiter data file " +
                dataFile.getAbsolutePath());
        }
    }

    private void writeVersion(int id) {
        if (raf == null) {
            throw new RuntimeException(
                "Internal error: Unable to write the Arbiter data file " +
                " because the file is not open." +
                dataFile.getAbsolutePath());
        }

        try {
            raf.seek(VERSION_OFFSET);
            raf.writeInt(id);
            nWrites.increment();
            doFSync();
        } catch (IOException e) {
            throw new RuntimeException(
                "Unable to write the Arbiter data file " +
                dataFile.getAbsolutePath());
        }
    }

    private int readVersion() {
        if (raf == null) {
            throw new RuntimeException(
                "Internal error: Unable to read the Arbiter data file " +
                " because the file is not open." +
                dataFile.getAbsolutePath());
        }
        try {
            raf.seek(VERSION_OFFSET);
            return raf.readInt();
        } catch (IOException e) {
            throw new RuntimeException(
                "Unable to write the Arbiter data file " +
                dataFile.getAbsolutePath());
        }
    }

    public synchronized void write(long nextCurrentVLSN,
                                   long nextDTVLSN,
                                   int masterId,
                                   long masterTerm,
                                   boolean doFSync) {
        if (raf == null) {
            throw new RuntimeException(
                "Internal error: Unable to write the Arbiter data file " +
                " because the file is not open." +
                dataFile.getAbsolutePath());
        }
        if (nextCurrentVLSN > currentVLSN) {
            this.currentVLSN = nextCurrentVLSN;
            this.dtvlsn = nextDTVLSN;
            this.masterTerm = masterTerm;
            try {
                raf.seek(DATA_OFFSET);
                raf.writeLong(nextCurrentVLSN);
                raf.writeLong(nextDTVLSN);
                if (lastAckNodeId == NameIdPair.NULL.getId() ||
                    lastAckNodeId != masterId  ||
                    lastMasterTerm != masterTerm) {
                    raf.writeInt(masterId);
                    lastAckNodeId = masterId;
                    raf.writeLong(masterTerm);
                    lastMasterTerm = masterTerm;
                    nWrites.add(3);
                } else  {
                   nWrites.add(2);
                }
                if (doFSync) {
                    doFSync();
                }
            } catch (IOException e) {
                throw new RuntimeException(
                    "Unable to write the Arbiter data file " +
                    dataFile.getAbsolutePath());
            }
        }
    }

    public synchronized void close() {
        if (raf != null) {
            try {
                doFSync();
                raf.close();
            } catch (IOException ignore) {
            } finally {
                raf = null;
            }
        }
    }

    public long get() {
        return currentVLSN;
    }

    public long getMasterTerm() {
        return masterTerm;
    }

    public long getDTVLSN() {
        return dtvlsn;
    }

    public int getLastAckNodeId() {
        return lastAckNodeId;
    }

    public static StatGroup loadEmptyStats() {
        StatGroup tmpStats =
            new StatGroup(ArbiterStatDefinition.ARBIO_GROUP_NAME,
                          ArbiterStatDefinition.ARBIO_GROUP_DESC);
        new LongStat(tmpStats, ARB_N_FSYNCS);
        new LongStat(tmpStats, ARB_N_WRITES);
        new LongStat(tmpStats, ARB_VLSN);
        new LongStat(tmpStats, ARB_DTVLSN);
        return tmpStats;
    }

    private void doFSync() throws IOException {
        if (raf == null) {
            return;
        }
        raf.getFD().sync();
        nFSyncs.increment();

    }
}
