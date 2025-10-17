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
import java.util.ArrayList;
import java.util.Collection;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.VersionedWriteLoggable;
import com.sleepycat.je.txn.Txn;

/**
 * BeforeImageLNLogEntry contains all the regular LNLogEntry fields and
 * additional information about the before image.
 * This additional information is used to support replication
 * of beforeimage information to other replicas.
 *
 * The extra fields which follow the usual {@link
 * com.sleepycat.je.log.entry.LNLogEntry} fields introduced in version 25 are:
 *
 * beforeImageExpiration - beforeImage Expiration time
 * beforeImageExpirationInHours - beforeImage Expiration time in days or hours.
 *
 */
public class BeforeImageLNLogEntry extends LNLogEntry<BeforeImageLN> {

    /**
     * The log version of the most recent format change for this entry,
     * including the superclass and any changes to the format of referenced
     * loggables.
     *
     * @see #getLastFormatChange
     */
    static final int LAST_FORMAT_CHANGE = 25;

    /* beforeImage Expiration time in days or hours. */
    private int beforeImageExpiration;
    private boolean beforeImageExpirationInHours;

    /**
     * Constructor to read an entry.
     */
    public BeforeImageLNLogEntry() {
        super(com.sleepycat.je.beforeimage.BeforeImageLN.class);
    }

    /**
     * Constructor to write this entry.
     */
    public BeforeImageLNLogEntry(
        LogEntryType entryType,
        DatabaseId dbId,
        Txn txn,
        long abortLsn,
        boolean abortKD,
        byte[] abortKey,
        byte[] abortData,
        long abortVLSN,
        int abortExpiration,
        boolean abortExpirationInHours,
        long abortModificationTime,
        long abortCreationTime,
        boolean abortTombstone,
        byte[] key,
        BeforeImageLN ln,
        boolean embeddedLN,
        int expiration,
        boolean expirationInHours,
        long creationTime,
        long modificationTime,
        boolean tombstone,
        boolean blindDeletion,
        int priorSize,
        long priorLsn,
        BeforeImageContext befImageContext) {

        super(entryType, dbId, txn, abortLsn, abortKD, abortKey, abortData,
              abortVLSN, abortExpiration, abortExpirationInHours,
              abortModificationTime, abortCreationTime,
              abortTombstone, key, ln, embeddedLN,
              expiration, expirationInHours, creationTime, modificationTime,
              tombstone, blindDeletion, priorSize, priorLsn, true);

        if (befImageContext != null) {
            beforeImageExpiration = befImageContext.getExpTime();
            beforeImageExpirationInHours = befImageContext.isExpTimeInHrs();
        }
    }

    @Override
    protected void reset() {
    	super.reset();
    	beforeImageExpiration = 0;
    	beforeImageExpirationInHours = false;
    }

    /**
     * Extends its super class to read in database operation information.
     */
    @Override
    public void readEntry(EnvironmentImpl envImpl,
                          LogEntryHeader header,
                          ByteBuffer entryBuffer) {

        readBaseLNEntry(envImpl, header, entryBuffer,
                        false /*keyIsLastSerializedField*/);

        /*
         * The BeforeImageLNLogEntry was introduced in version LAST_FORMAT_CHANGE.
         */
        int version = header.getVersion();
        if (version >= LAST_FORMAT_CHANGE) {
            beforeImageExpiration = LogUtils.readPackedInt(entryBuffer);
            if (beforeImageExpiration < 0) {
                beforeImageExpiration = (- beforeImageExpiration);
                beforeImageExpirationInHours = true;
            }
        }
    }

    /**
     * Extends its super class to dump database operation information.
     */
    @Override
    public StringBuilder dumpEntry(StringBuilder sb, boolean verbose) {

        super.dumpEntry(sb, verbose);
        sb.append("<BeforeImage expires=\"");
        sb.append(TTL.formatExpiration(beforeImageExpiration,
                    beforeImageExpirationInHours));
        sb.append(" val:").append(beforeImageExpiration);
        sb.append(beforeImageExpirationInHours ? " hours" : " days");
        sb.append("\"/>");
        return sb;
    }

    @Override
    public int getLastFormatChange() {
        return LAST_FORMAT_CHANGE;
    }

    @Override
    public Collection<VersionedWriteLoggable> getEmbeddedLoggables() {
        final Collection<VersionedWriteLoggable> list =
                new ArrayList<>(super.getEmbeddedLoggables());
        list.add(new BeforeImageLN());
        return list;
    }

    /**
     * returns the before image expiration as user specified
     */
    public int getBeforeImageExpiration() {
        return beforeImageExpiration;
    }

    public long getBeforeImageExpirationTime() {
        return TTL.expirationToSystemTime(getBeforeImageExpiration(),
				beforeImageExpirationInHours);
    }

    /**
     * returns the before image expiration as stored in log
     */
    public String getBeforeImageStoredExpiration() {
        return TTL.formatExpiration(beforeImageExpiration,
                    beforeImageExpirationInHours);
    }

    /**
     * returns true if before image expired to the record
     */
    public boolean isBeforeImageExpired() {
        return TTL.isExpired(beforeImageExpiration,
                    beforeImageExpirationInHours);
    }

    public boolean isBeforeImageExpirationInHours() {
        return beforeImageExpirationInHours;
    }

    @Override
    public int getSize(final int logVersion, final boolean forReplication) {

        int size = getBaseLNEntrySize(
            logVersion, false /*keyIsLastSerializedField*/,
            forReplication);

        if (logVersion >= LAST_FORMAT_CHANGE) {
            size += LogUtils.getPackedIntLogSize(
                    beforeImageExpirationInHours ?
                    (-beforeImageExpiration) : beforeImageExpiration);
        }
        return size;
    }

    @Override
    public void writeEntry(final ByteBuffer destBuffer,
                           final int logVersion,
                           final boolean forReplication) {

        writeBaseLNEntry(destBuffer, logVersion,
                false /*keyIsLastSerializedField*/, forReplication);
        if (logVersion >= LAST_FORMAT_CHANGE) {
            LogUtils.writePackedInt(destBuffer,
                    beforeImageExpirationInHours ?
                    (-beforeImageExpiration) : beforeImageExpiration);
        }
    }

    @Override
    public void dumpRep(StringBuilder sb) {
        super.dumpRep(sb);
    }
}
