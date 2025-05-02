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
package com.sleepycat.je.rep.stream;

import static com.sleepycat.je.utilint.DbLsn.NULL_LSN;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.ChecksumException;
import com.sleepycat.je.rep.SyncUpFailedException;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.stream.ReplicaSyncupReader.SkipGapException;
import com.sleepycat.je.rep.vlsn.VLSNIndex;
import com.sleepycat.je.rep.vlsn.VLSNIndex.BackwardVLSNScanner;
import com.sleepycat.je.rep.vlsn.VLSNRange;
import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.TestHook;

/**
 * The FeederSyncupReader scans the log backwards for requested log entries.
 * It uses the vlsnIndex to optimize its search, repositioning when a concrete
 * {@literal vlsn->lsn} mapping is available.
 *
 * The FeederSyncupReader is not thread safe, and can only be used serially.
 */
public class FeederSyncupReader extends VLSNReader {
    /* The scanner is a cursor over the VLSNIndex. */
    private final BackwardVLSNScanner scanner;

    private  FeederReplicaSyncup syncup;

    private final int CHECK_CHANNEL_INTERVAL;

    private int timesChecked;

    private long lastTimeChecked;

    /*
     * Ping message frequency, will be 1/5 of RepParams.PRE_HEARTBEAT_TIMEOUT
     */
    private final int PING_INTERVAL;

    private static TestHook<Object> detectChannelHook;

    public FeederSyncupReader(EnvironmentImpl envImpl,
                              VLSNIndex vlsnIndex,
                              long endOfLogLsn,
                              int readBufferSize,
                              long startVLSN,
                              long finishLsn,
                              FeederReplicaSyncup syncup)
            throws IOException, DatabaseException {

        /*
         * If we go backwards, endOfFileLsn and startLsn must not be null.
         * Make them the same, so we always start at the same very end.
         */
        this(envImpl,
             vlsnIndex,
             endOfLogLsn,
             readBufferSize,
             startVLSN,
             finishLsn);
        this.syncup = syncup;
        lastTimeChecked = TimeSupplier.currentTimeMillis();
    }

    public FeederSyncupReader(EnvironmentImpl envImpl,
                              VLSNIndex vlsnIndex,
                              long endOfLogLsn,
                              int readBufferSize,
                              long startVLSN,
                              long finishLsn)
        throws IOException, DatabaseException {

        /*
         * If we go backwards, endOfFileLsn and startLsn must not be null.
         * Make them the same, so we always start at the same very end.
         */
        super(envImpl,
              vlsnIndex,
              false,           // forward
              endOfLogLsn,
              readBufferSize,
              finishLsn);
        scanner = new BackwardVLSNScanner(vlsnIndex);
        CHECK_CHANNEL_INTERVAL = 100;
        timesChecked = 0;
        PING_INTERVAL = envImpl.getConfigManager().getDuration(
                RepParams.PRE_HEARTBEAT_TIMEOUT) / 5;
        initScan(startVLSN);
    }

    /**
     * Set up the FeederSyncupReader to start scanning from this VLSN. If we
     * find a mapping for this VLSN, we'll start precisely at its LSN, else
     * we'll have to start from an earlier location.
     *
     * @throws InterruptedException
     * @throws IOException
     * @throws DatabaseException
     */
    private void initScan(long startVLSN)
        throws DatabaseException, IOException {

        if (startVLSN == NULL_VLSN) {
            throw EnvironmentFailureException.unexpectedState
                ("FeederSyncupReader start can't be NULL_VLSN");
        }

        long startPoint = startVLSN;
        startLsn = scanner.getStartingLsn(startPoint);
        assert startLsn != NULL_LSN;

        /*
         * Flush the log so that syncup can assume that all log entries that
         * are represented in the VLSNIndex  are safely out of the log buffers
         * and on disk. Simplifies this reader, so it can use the regular
         * ReadWindow, which only works on a file.
         */
        envImpl.getLogManager().flushNoSync();

        window.initAtFileStart(startLsn);
        currentEntryPrevOffset = window.getEndOffset();
        currentEntryOffset = window.getEndOffset();
        currentVLSN = startVLSN;
    }

    /**
     * Backward scanning for records for the feeder's part in syncup.
     * @throws ChecksumException 
     * @throws FileNotFoundException 
     */
    public OutputWireRecord scanBackwards(long vlsn)
        throws FileNotFoundException, ChecksumException {

        VLSNRange range = vlsnIndex.getRange();
        if (vlsn < range.getFirst()) {
            /*
             * The requested VLSN is before the start of our range, we don't
             * have this record.
             */
            return null;
        }

        currentVLSN = vlsn;

        /*
         * If repositionLsn is not NULL_LSN, the reader will seek to that
         * position when calling readNextEntry instead of scanning.
         * setPosition() is a noop if repositionLsn is null.
         */
        long repositionLsn = scanner.getPreciseLsn(vlsn);
        setPosition(repositionLsn);

        if (readNextEntry()) {
            return currentFeedRecord;
        }

        return null;
    }

    @Override
    protected void handleGapInBackwardsScan(long prevFileNum) {
        SkipGapException e = new SkipGapException(window.currentFileNum(),
                                                  prevFileNum,
                                                  currentVLSN);
        LoggerUtils.warning(logger, envImpl, e.toString());
        throw e;
    }

    /**
     * @throw an EnvironmentFailureException if we were scanning for a
     * particular VLSN and we have passed it by.
     */
    private void checkForPassingTarget(long compareResult) {

        if (compareResult < 0) {
            /* Hey, we passed the VLSN we wanted. */
            throw EnvironmentFailureException.unexpectedState
                ("want to read " + currentVLSN + " but reader at " +
                 currentEntryHeader.getVLSN());
        }
    }

    @Override
    protected boolean isTargetEntry()
        throws DatabaseException {

        nScanned++;
        if(syncup != null) {
            timesChecked ++;
            if(timesChecked >= CHECK_CHANNEL_INTERVAL) {
                timesChecked = 0;
                if(!syncup.isNamedChannelOpen()) {
                    doDetectChannelHook("Caught channel is down.");
                    /*
                     * This exception will be thrown all the way up to
                     * InputThread.run() in Feeder.java, an IOException will be
                     * handled there, causing the syncup process to fail.
                     */
                    throw new SyncUpFailedException
                            ("Feeder-Replica Syncup Failed. Feeder-replica" +
                             " channel has been shut down while feeder scans" +
                             " log files, syncup is terminated consequently");
                }
                sendPingMessage();
            }
        }

        /* Skip invisible entries. */
        if (currentEntryHeader.isInvisible()) {
            return false;
        }

        /* 
         * Return true if this entry is replicated and its VLSN is currentVLSN.
         */
        if (entryIsReplicated()) {
            long entryVLSN = currentEntryHeader.getVLSN();
            checkForPassingTarget(entryVLSN - currentVLSN);

            /* return true if this is the entry we want. */
            return (entryVLSN == currentVLSN);
        }

        return false;
    }

    private void sendPingMessage() {

        long currentTime = TimeSupplier.currentTimeMillis();
        if(currentTime - lastTimeChecked >= PING_INTERVAL) {
            try {
                syncup.sendPingMessage();
            } catch (IOException e) {
                throw new SyncUpFailedException
                        ("Feeder-Replica Syncup Failed. Feeder " +
                         "IOException: " + e.getMessage() +
                         " Failed to keep ping messages ongoing between" +
                         " Feeder and Replica, syncup is terminated" +
                         " consequently");
            }
            lastTimeChecked = TimeSupplier.currentTimeMillis();
        }
    }

    /**
     * For test, so after the pause, FeederSyncupReader will
     * immediately check the connectivity of the channel.
     */
    public void setTimesChecked(int timesChecked) {
        this.timesChecked = timesChecked;
    }

    /**
     * Instantiate a WireRecord to house this log entry.
     */
    @Override
    protected boolean processEntry(ByteBuffer entryBuffer) {

        ByteBuffer buffer = entryBuffer.slice();
        buffer.limit(currentEntryHeader.getItemSize());
        currentFeedRecord =
            new OutputWireRecord(envImpl, currentEntryHeader, buffer);

        entryBuffer.position(entryBuffer.position() +
                             currentEntryHeader.getItemSize());
        return true;
    }

    public static void setDetectChannelHook(TestHook<Object> hook) {
        FeederSyncupReader.detectChannelHook = hook;
    }

    public void doDetectChannelHook(Object T)
    {
        if (detectChannelHook != null) {
            detectChannelHook.doHook(T);
        }
    }
}
