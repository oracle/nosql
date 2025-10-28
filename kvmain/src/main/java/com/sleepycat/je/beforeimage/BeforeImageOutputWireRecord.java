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

package com.sleepycat.je.beforeimage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogItem;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.entry.ReplicableLogEntry;
import com.sleepycat.je.rep.stream.OutputWireRecord;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;
import com.sleepycat.je.rep.stream.InputWireRecord;

/**
 * Format for log entries sent across the wire while replicating records with
 * before images. While streamed, we instantiate the outputwirerecord and 
 * append the before image data as a byte array.
 *
 */
public class BeforeImageOutputWireRecord extends OutputWireRecord {

    private byte[] bImgData;
    private static volatile TestHook<Boolean> verifyHook;

    private synchronized ReplicableLogEntry 
        instantiateEntryWithBeforeImage(LogEntryHeader currentEntryHeader) {

        if (logEntry != null) {
            return logEntry;
        }
        if (logItem != null) {
            logEntry = logItem.cachedEntry;
            if (logEntry != null) {
                if (logItem.getBeforeImageCtx() != null) {
                    if (!TTL.isExpired(logItem.getBeforeImageCtx().getExpTime(),
                            logItem.getBeforeImageCtx().isExpTimeInHrs())) {
                        bImgData = logItem.getBeforeImageData();
                    }
                    if (bImgData != null) {
                        return logEntry;
                    }
                }
            }
        }
        
        if (!LogEntryType.isBeforeImageType(currentEntryHeader.getType())) {
            throw EnvironmentFailureException.unexpectedState(
                "Log entry type does not support beforeimage: ");
        }

        final BeforeImageLNLogEntry entry =
            (BeforeImageLNLogEntry)(LogEntryType
                    .findType(currentEntryHeader.getType())
                    .getNewLogEntry());
        entryBuffer.mark();
        entry.readEntry(envImpl, currentEntryHeader, entryBuffer);
        entryBuffer.reset();
        
        logEntry = entry;
        if (logItem != null) {
            logItem.cachedEntry = logEntry;
        }
        if (envImpl.getBeforeImageIndex() != null) {
            DatabaseEntry bImgEntry = envImpl.getBeforeImageIndex()
                    .get(entry.getAbortLsn(), null);
            if (bImgEntry != null) {
                bImgData = bImgEntry.getData();
                if (logItem != null) {
                    logItem.setBeforeImageData(bImgData);
                    BeforeImageContext bImgCtx = new BeforeImageContext(
                            entry.getBeforeImageExpiration(),
                            entry.isBeforeImageExpirationInHours());
                    logItem.setBeforeImageCtx(bImgCtx);
                }
            }
        }
        return logEntry;
    }

    public BeforeImageOutputWireRecord(final EnvironmentImpl envImpl,
        final LogEntryHeader header,
        final ByteBuffer entryBuffer) {
        super(envImpl, header, entryBuffer);
        // Todo check we need to do it here because we want beforeimage size to
        // create
        // message buffer
        instantiateEntryWithBeforeImage(header);

        assert entryBuffer.remaining() == header.getItemSize() : "remaining:"
            + entryBuffer.remaining() + " itemSize:"
            + header.getItemSize();
    }

    /**
	 * Creates an BeforeImageOutputWireRecord from a log item. This constructor
	 * is used when a Feeder can bypass access to the log because the log item
	 * is available in the log item cache associated with the VLSNIndex.
	 */
    public BeforeImageOutputWireRecord(final EnvironmentImpl envImpl,
                                final LogItem logItem) {
        super(envImpl, logItem);
        if (logItem.getBeforeImageCtx() != null) {
            if (!TTL.isExpired(logItem.getBeforeImageCtx().getExpTime(),
                    logItem.getBeforeImageCtx().isExpTimeInHrs())) {
                this.bImgData = logItem.getBeforeImageData();
            } else {
                TestHookExecute.doHookIfSet(verifyHook, true);
            }
        }
    }

    public static void setInputWireRecordHook(TestHook<Boolean> hook) {
        verifyHook = hook;
    }

    /* For unit test support. */
    public BeforeImageOutputWireRecord(final EnvironmentImpl envImpl,
                     final InputWireRecord input) {
        super(envImpl, input);
    }

    /**
     * This shouldn't be used as the matchpoint applies to the 
     * replica vlsn and this subclass of outputwirerecord don't play a role in
     * the matchpoint.
     * @throws DatabaseException
     */
    @Override
    public boolean match(final InputWireRecord input)
        throws DatabaseException {
        return super.match(input) &&
               Arrays.equals(bImgData, input.getBeforeImageData());
    }

    /**
	 * For unit tests.
	 * 
	 * @return true if this BeforeImageOutputWireRecord has the same logical
	 *         contents as "other".
	 * @throws DatabaseException
	 */
    public boolean match(final BeforeImageOutputWireRecord otherRecord)
        throws DatabaseException {
        return super.match(otherRecord) &&
               Arrays.equals(bImgData, otherRecord.bImgData);
    }

    /**
     * Dump the contents.
     * @throws DatabaseException
     */
    @Override
    public String dump()
        throws DatabaseException {
		String dumpData = super.dump() + " bImgData: [";
		dumpData += (bImgData != null)
				? new String(bImgData, StandardCharsets.UTF_8)
				: "null" + "]";
		return dumpData;
    }

    public byte[] getBImgData() {
         return bImgData;
    }

    /**
     * Returns the number of bytes needed to represent the message data for
     * this record for the specified log version.
     */
    @Override
    protected int getWireSize(final int logVersion) {
        // assert bImgData != null;
        int baseSize = super.getWireSize(logVersion);
        if (bImgData != null) {
            baseSize += LogUtils.getByteArrayLogSize(bImgData);
        }
        return baseSize;
    }

    /**
     * Write the log header and entry associated with this instance to the
     * specified buffer using the format for the specified log version.
     *
     * @param messageBuffer the destination buffer
     * @param logVersion the log version of the format
     * @return whether the data format was changed to support an old version.
     */
    @Override
    protected boolean writeToWire(final ByteBuffer messageBuffer,
                        final int logVersion) {

        //TODO check willreserialize optimizations
        messageBuffer.put(header.getType());
        assert logEntry != null;
        LogUtils.writeInt(messageBuffer, logVersion);
        LogUtils.writeInt(messageBuffer, header.getItemSize());
        LogUtils.writeLong(messageBuffer, header.getVLSN());
        logEntry.writeEntry(messageBuffer, logVersion, true /*forReplication*/);
        if (bImgData != null) {
            LogUtils.writeByteArray(messageBuffer, bImgData);
        }
        return isOldFormatRequired(logVersion);
    }
}
