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
package oracle.kv.impl.api.lob;

import java.io.IOException;
import java.io.InputStream;
import java.util.ConcurrentModificationException;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.Key;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.util.UserDataControl;
import oracle.kv.lob.KVLargeObject.LOBState;

/**
 * Implements the LOB append functionality that permits extending an existing
 * LOB.
 *
 * At the representation level, if a LOB currently consists of three chunks:
 * two of which are complete chunks, of size CHUNK_SIZE, and a third partial
 * chunk, it will continue appending to the LOB by replacing the third
 * (partial) chunk with the pre-existing + appended contents to make it a
 * CHUNK_SIZE chunk and then continue with more chunks as required based upon
 * size of the lobAppendStream.
 *
 * This method shared much of the implementation with the "resumption" support
 * for a put operations, via the common WriteOperation super class.
 *
 */
public class AppendOperation extends WriteOperation {

    AppendOperation(KVStoreImpl kvsImpl,
                    Key appLobKey,
                    InputStream lobAppendStream,
                    Durability durability,
                    long chunkTimeout,
                    TimeUnit timeoutUnit) {

        super(kvsImpl, appLobKey, lobAppendStream, durability, chunkTimeout,
              timeoutUnit);
    }

    /**
     * Appends to an existing LOB, or resumes an append of a partially appended
     * LOB. This involves the following steps:
     *
     * 1) Use the ALK to lookup the ILK.
     *
     * 2) Lookup the metadata using the ILK.
     *
     * 3) Update the metadata to indicate that an append is in progress: - move
     * LOB_SIZE to APPEND_LOB_SIZE
     *
     * APPEND_LOB_SIZE can be used to reset the lob stream and resume the
     * append should it become necessary to retry the append operation.
     *
     * 4) Position the chunk keys iterator at the last (typically partial)
     * chunk.
     *
     * 5) Replace the last chunk with a concatenation of old and newly appended
     * bytes and follow up by creating chunks from the append lob stream, as is
     * done by a put operation. The append operation maintains the
     * representation invariant that all chunks preceding the last chunk must
     * contain exactly chunkSize bytes; the last chunk can be any size &gt; 0
     * and {@literal <=} chunkSize.
     *
     * 6) Update the LOB metadata at the end to: - Create a new entry for
     * LOB_SIZE - Remove the APPEND_LOB_SIZE value to indicate completion of
     * the append operation.
     *
     * Any failures after step 3) result in the creation of a partial appended
     * LOB that can be deleted, or the append operation can be resumed.
     */
    Version execute()
        throws IOException {

        final ValueVersion appValueVersion =
                kvsImpl.get(appLOBKey, Consistency.ABSOLUTE,
                            chunkTimeoutMs, TimeUnit.MILLISECONDS);

        if (appValueVersion == null) {
            throw new IllegalArgumentException
                ("LOB not present. Key:" + appLOBKey +
                 "The key/value pair must be created " +
                 "using putLOB() before invoking appendLOB(). ");
        }

        internalLOBKey = valueToILK(appValueVersion.getValue());

        final ValueVersion metadataVV =
            initMetadata(Consistency.ABSOLUTE, LOBState.PARTIAL_APPEND);

        if (lobProps.getVersion() == 1) {
            String msg = "appendLOB is only supported for LOBs " +
                "created by KVS version 2.1.55 or later." ;
            throw new UnsupportedOperationException(msg);
        }

        Version mdVersion = metadataVV.getVersion();
        final ValueVersion lastChunkVV;
        if (lobProps.isPartiallyAppended()) {
            /*
             * Append metadata is already setup, we don't know the true end of
             * the partial lob. Initialize transient state for resumption and
             * verify any trailing bytes.
             */
            lastChunkVV = setupForResume(mdVersion);
        } else {
            lastChunkVV = getLastChunk();

            /* Initialize the metadata to indicate an append is in progress. */
            lobProps.startAppend();

            /* Save the updated metadata */
            mdVersion = updateMetadata(mdVersion);
        }

        final byte prefixChunk[];
        if ((lastChunkVV == null) ||
            (lastChunkVV.getValue().getValue().length == chunkSize)) {
            prefixChunk = null;
        } else {
            /* Continue rewriting the last partial chunk. */
            prefixChunk = lastChunkVV.getValue().getValue();
        }
        mdVersion = putChunks(lobSize, prefixChunk, mdVersion);

        lobProps.endAppend();

        updateMetadata(mdVersion);

        /*
         * Rewrite the ALK key/(ILK)value pair. The ILK value itself is
         * unchanged, but we need to establish a new version number as a result
         * of the successful append to note the fact that the object has
         * changed.
         */
        final Version appLobVersion =
            kvsImpl.putIfVersion(appLOBKey,
                                 appValueVersion.getValue(),
                                 appValueVersion.getVersion(),
                                 null,
                                 CHUNK_DURABILITY,
                                 chunkTimeoutMs, TimeUnit.MILLISECONDS);

        if (appLobVersion == null) {
            final String msg = "Concurrent LOB operation detected " +
                "for key: " + UserDataControl.displayKey(appLOBKey) +
                " Internal LOB key:" + internalLOBKey;
            throw new ConcurrentModificationException(msg);
        }

        return appLobVersion;
    }

    @Override
    protected long positionLobStream(final long chunkStreamPos)
        throws IOException {

        final long appendStartPos = lobProps.getAppendLobSize();
        final long lobStreamPos = chunkStreamPos - appendStartPos;

        final long bytesSkipped = skipInput(lobStream, lobStreamPos);
        if (lobStreamPos != bytesSkipped) {
            throw new IllegalArgumentException(
                "The LOB input stream did not skip the requested number of" +
                " bytes." +
                " Bytes skipped:" + bytesSkipped +
                " Requested:" + lobStreamPos +
                " absolute lob pos:" + chunkStreamPos +
                " append start pos:" + appendStartPos);
        }

        return lobStreamPos;
    }

    @Override
    protected long getVerificationByteCount() {
        /* Get initial limit based upon stored LOB bytes. */
        final long verificationByteCount = super.getVerificationByteCount();

        /*
         * Lower the limit based upon the bytes that can be supplied by
         * just the LOB stream.
         */
        final long appendStartPos = lobProps.getAppendLobSize();
        final long availVerificationBytes = (lobSize - appendStartPos);

        return Math.min(availVerificationBytes, verificationByteCount);
    }
}
