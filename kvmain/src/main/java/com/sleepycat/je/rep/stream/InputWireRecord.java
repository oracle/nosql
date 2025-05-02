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

import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.beforeimage.BeforeImageIndex;
import com.sleepycat.je.beforeimage.BeforeImageIndex.BeforeImagePayLoad;
import com.sleepycat.je.beforeimage.BeforeImageLNLogEntry;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.ReplayPreprocessor;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;

/**
 * Format for messages received at across the wire for replication. Instead of
 * sending a direct copy of the log entry as it is stored on the JE log files
 * (LogEntryHeader + LogEntry), select parts of the header are sent.
 *
 * An InputWireRecord de-serializes the logEntry from the message bytes and
 * releases any claim on the backing ByteBuffer.
 */
public class InputWireRecord extends WireRecord {

    private final LogEntry logEntry;
    private ReplayPreprocessor preprocessor;
    private boolean hasBeforeImage;
    private boolean enabledBeforeImage;
    private BeforeImageIndex.BeforeImagePayLoad beforeImage;
    private int beforeImageExp;


    /**
     * Make a InputWireRecord from an incoming replication message buffer for
     * applying at a replica.
     * @throws DatabaseException
     */
    InputWireRecord(final EnvironmentImpl envImpl,
                    final ByteBuffer msgBuffer,
                    final BaseProtocol protocol)
        throws DatabaseException {
        this(envImpl, msgBuffer, protocol, false);
    }

    InputWireRecord(final EnvironmentImpl envImpl,
                    final ByteBuffer msgBuffer,
                    final BaseProtocol protocol,
                    final boolean isBeforeImageEntry)
        throws DatabaseException {
        super(createLogEntryHeader(msgBuffer, protocol));
        
        /*
         * we need this to differentiate between the replica and the stream
         * so we cannot use the header types
         */
        this.enabledBeforeImage = isBeforeImageEntry;

        if (enabledBeforeImage) {
            final LogEntry entry = getLogEntryType().getNewLogEntry();
            msgBuffer.mark();
            entry.readEntry(envImpl, header, msgBuffer);

            if (entry instanceof BeforeImageLNLogEntry) {
                beforeImageExp = ((BeforeImageLNLogEntry) entry)
                    .getBeforeImageExpiration();
            }

            if (msgBuffer.hasRemaining()) {
                this.hasBeforeImage = true;
                this.beforeImage = BeforeImageIndex.BeforeImagePayLoad
                    .unMarshalData(LogUtils.readByteArray(msgBuffer));
            }
            msgBuffer.reset();
            logEntry = entry;
        } else {
            logEntry = instantiateEntry(envImpl, msgBuffer);
            if (logEntry instanceof LNLogEntry) {
                // this is insert but we enabled before image
                enabledBeforeImage = ((LNLogEntry<?>) logEntry)
                    .isBeforeImageEnabled();
            }
        }
    }

    private static LogEntryHeader createLogEntryHeader(
        final ByteBuffer msgBuffer, final BaseProtocol protocol) {

        final byte entryType = msgBuffer.get();
        int entryVersion = LogUtils.readInt(msgBuffer);
        final int itemSize = LogUtils.readInt(msgBuffer);
        final long vlsn = LogUtils.readLong(msgBuffer);

        /*
         * Check to see if we need to fix the entry's log version to work
         * around [#25222].
         */
        if ((entryVersion > LogEntryType.LOG_VERSION_EXPIRE_INFO)
            && protocol.getFixLogVersion12Entries()) {
            entryVersion = LogEntryType.LOG_VERSION_EXPIRE_INFO;
        }

        return new LogEntryHeader(entryType, entryVersion, itemSize, vlsn);
    }

    public long getVLSN() {
        return header.getVLSN();
    }

    public byte getEntryType() {
        return header.getType();
    }

    public void setPreprocessor(ReplayPreprocessor preprocessor) {
        this.preprocessor = preprocessor;
    }

    public ReplayPreprocessor getPreprocessor() {
        return preprocessor;
    }

    public LogEntry getLogEntry() {
        return logEntry;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        header.dumpRep(sb);
        sb.append(" ");
        logEntry.dumpRep(sb);
        sb.append(" enabledBeforeImage:").append(enabledBeforeImage);
        if (enabledBeforeImage) {
            sb.append(" hasBeforeImage:").append(hasBeforeImage);
            sb.append(" BeforeImageExpiration:")
              .append(getBeforeImageExpiration());
            sb.append(" getBeforeImageModificationTime:")
              .append(getBeforeImageModificationTime());
        }
        return sb.toString();
    }

    /**
     * Convert the full version of the log entry to a string.
     */
    public String dumpLogEntry() {
        StringBuilder sb = new StringBuilder();
        sb.append(header);
        sb.append(" ").append(logEntry);
        return sb.toString();
    }

    /**
     * Returns true if the LogEntry is an LNLogEntry with a Before Image.
     */
    public boolean hasBeforeImage() {
        return hasBeforeImage;
    }

    /**
     * Returns true if a Before Image was enabled for this entry, regardless of
     * whether it has one. For insertions this will always be false , but 
     * for updates and deletes with before image , this is true.
     * basically  
     * a update or delete ln  with before image enabled might still not have the 
     * beforeimage as it was expired. this api is to distinguish this 
     */
    public boolean enabledBeforeImage() {
        return enabledBeforeImage;
    }

    /**
     * Returns the LNLogEntry of the original record that makes up
     * the Before Image if it exists.  This is a materialization of the
     * original log, so some information, such as the expiration, does
     * not apply to the Before Image, but the original log.  For the
     * Before Image expiration use {@link #getBeforeImageExpiration()}
     */
    public byte[] getBeforeImageData() {
        if (!hasBeforeImage()) {
            return null;
        }
        return beforeImage.getbImgData();
    }

    public void setBeforeImageData(BeforeImagePayLoad bImgData) {
        beforeImage = bImgData;
    }

    /**
     * Returns the expiration time of the Before Image.
     */
    public long getBeforeImageExpiration() {
        return beforeImageExp;
    }
    
    /**
     * Returns the time when the before image was inserted in 
     * the primary database.
     */
    public long getBeforeImageModificationTime() {
        if (!hasBeforeImage()) {
            return 0;
        }
        return beforeImage.getModTime();
    }
}
