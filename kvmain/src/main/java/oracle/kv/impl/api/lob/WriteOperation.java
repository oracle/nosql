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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.Durability.ReplicaAckPolicy;
import oracle.kv.Durability.SyncPolicy;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.ReturnValueVersion;
import oracle.kv.ReturnValueVersion.Choice;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.impl.api.KVStoreImpl;

/**
 * Superclass for all LOB write operations.
 */
public abstract class WriteOperation extends Operation {

    /**
     * The durability associated with the storage of individual chunks. Chunk
     * durability favors performance. LOB durability is defined as part of the
     * application request.
     */
    protected static final Durability CHUNK_DURABILITY =
        new Durability(SyncPolicy.NO_SYNC, SyncPolicy.NO_SYNC,
                       ReplicaAckPolicy.NONE);

    /**
     * The durability as specified by the application request.
     */
    protected final Durability lobDurability;

    /*
     * The stream producing the bytes for the write (put, append) operations.
     */
    protected final InputStream lobStream;

    /*
     * Used for testing
     */
    private static long testVerificationByteCount = -1;

    WriteOperation(KVStoreImpl kvsImpl,
                   Key appLobKey,
                   InputStream lobStream,
                   Durability durability,
                   long chunkTimeout,
                   TimeUnit timeoutUnit) {

        super(kvsImpl, appLobKey, chunkTimeout, timeoutUnit);

        if (lobStream instanceof BufferedInputStream) {
            this.lobStream = lobStream;
        } else if (lobStream != null) {
            this.lobStream = new BufferedInputStream(lobStream);
        } else {
            if (! (this instanceof DeleteOperation)) {
                throw new IllegalArgumentException
                    ("expected non-null lobStream argument");
            }
            this.lobStream = null;
        }
        this.lobDurability = (durability == null) ?
            kvsImpl.getDefaultDurability() : durability;
    }

    /**
     * Updates the metadata associated with the internalLobKey (ILK) upon
     * completion, or during intermediate metadata checkpoints. Note that it
     * uses the specified durability instead of chunk durability.
     *
     * @param version if null new metadata otherwise update existing metadata.
     *
     * @return the version associated with the updated metadata
     */
    protected Version updateMetadata(Version version) {

        final Value propsArray = lobProps.serialize();

        final ReturnValueVersion prevValueVersion =
            new ReturnValueVersion(Choice.VERSION);

        final Version storedVersion =
            kvsImpl.putIfVersion(internalLOBKey, propsArray,
                                 version,
                                 prevValueVersion,
                                 lobDurability,
                                 chunkTimeoutMs, TimeUnit.MILLISECONDS);

        /*
         * Take this opportunity to perform checks for any concurrent
         * modifications.
         */
        if (storedVersion == null) {
            /* Update failed. */
            if (prevValueVersion.getVersion() == null) {
                throw new ConcurrentModificationException("LOB was deleted: " +
                                                          internalLOBKey);
            }
            throw new ConcurrentModificationException
                ("LOB was updated concurrently");
        }

        return storedVersion;
    }

    /**
     * Stores a single chunk using the specified durability.
     *
     * @param chunkKey the key to be used for writing the chunk
     *
     * @param chunk the array holding the chunk bytes
     *
     * @param actualSize the number of actual bytes in the chunk
     *
     * @param replace determines whether the put operation replaces the chunk
     * associated with the key, or creates a brand new key value pair
     *
     * @param chunkDurability the durability associated with the put operation
     */
    private void putChunk(Key chunkKey,
                             byte[] chunk,
                             int actualSize,
                             boolean replace,
                             Durability chunkDurability) {

        if (actualSize != chunkSize) {
            final byte[] smallerChunk = new byte[actualSize];
            System.arraycopy(chunk, 0, smallerChunk, 0, actualSize);
            chunk = smallerChunk;
        }

        final Value chunkValue =  Value.createValue(chunk);
        final Version version;

        /* Write out the chunk. */
        if (replace) {
            version = kvsImpl.putIfPresent(chunkKey, chunkValue, null,
                                           chunkDurability,
                                           chunkTimeoutMs, TimeUnit.MILLISECONDS);
            if (version == null) {
                throw new ConcurrentModificationException
                ("Expected  to find chunk " + chunkKey +
                 " but it was missing. ");
            }
        } else {
            version = kvsImpl.putIfAbsent(chunkKey, chunkValue, null,
                                          chunkDurability,
                                          chunkTimeoutMs, TimeUnit.MILLISECONDS);
            if (version == null) {
                throw new ConcurrentModificationException
                ("Chunk " + chunkKey +
                 " was already associated with the key: " +
                 chunkKey.toString());

            }
        }
    }

    /**
     * Reads the lob stream creating fixed size chunks until EOF is reached.
     * Updates the metadata with the LOB size and the max chunk id. Note that
     * the put operation can start at an existing partial chunk, in which case,
     * the chunkPrefix contains the pre-existing contents and the lob stream
     * supplies the additional contents. The lob stream must be correctly
     * positioned (to read the next byte that must be written to a chunk) upon
     * entry to this method.
     *
     * There are two different durabilities at play:
     * <p>
     * 1) LOB durability: The application supplied durability, that must hold
     * after the LOB has been created.
     *
     * 2) Chunk durability: which is internal and more relaxed, so that we get
     * better performance.
     *
     * Within a full superchunk of say size 10, chunks 1..9 are written with
     * Chunk durability, with the last chunk in the superchunk being written
     * at LOB durability, thus effectively ensuring that all the chunks are
     * written at LOB durability without the latency of having had to wait for
     * each individual chunk 1..9.
     *
     * @param startByte the byte boundary at which to start adding chunks
     *
     * @param chunkPrefix the array used to hold the prefix chunk bytes that
     * will be used to build up to the first complete chunk. If null, it means
     * the put is starting at a chunk boundary.
     *
     * @param metadataVersion the version associated with the metadata
     *
     * @return the updated metadata resulting from the checkpointing of new
     * super chunk id values in the metadata as new chunks are added to the
     * LOB.
     *
     */
    protected Version putChunks(long startByte,
                                byte chunkPrefix[],
                                Version metadataVersion)
        throws IOException {

        if (((chunkPrefix == null) != ((startByte % chunkSize) == 0))) {
            throw new IllegalStateException("start byte:" + startByte +
                                            " chunk size:" + chunkSize +
                                            " inconsistent with prefix chunk:" +
                                            chunkPrefix);
        }

        if ((lobSize < 0) || (numChunks < 0)) {
            throw new IllegalStateException("lobSize:" + lobSize +
                                            " numChunks:" + numChunks);
        }

        byte chunk[] = new byte[chunkSize];
        boolean initialReplacePut = false;

        int currentSize = 0;
        if (chunkPrefix != null) {
            currentSize = chunkPrefix.length;
            lobSize -= currentSize;
            /* Back up over the chunk, so we can overwrite it. */
            numChunks--;
            System.arraycopy(chunkPrefix, 0, chunk, 0, currentSize);
            initialReplacePut = true;
        }

        final ChunkKeysIterator chunkKeys =
            getChunkKeysByteRangeIterator(startByte, Long.MAX_VALUE);

        readLoop:
            while (true) {
                int readBytes = -1;

                /* Fill up a chunk. */
                while (currentSize < chunkSize) {
                    readBytes = lobStream.read(chunk, currentSize,
                                               (chunkSize - currentSize));
                    if (readBytes == -1) {
                        if (currentSize > 0) {
                            break;
                        }
                        break readLoop;
                    }
                    currentSize += readBytes;
                }

                final Key chunkKey = chunkKeys.next();

                /* Write it, vary the durability based upon the chunk. */
                final Durability chunkDurability = (readBytes == -1) ||
                    (chunkKeys.getChunkId() == chunksPerPartition) ?
                     lobDurability : CHUNK_DURABILITY;

                putChunk(chunkKey, chunk, currentSize,
                         initialReplacePut, chunkDurability);
                numChunks++;
                if (chunkKeys.getChunkId() == 1) {
                    metadataVersion =
                        checkpointSuperChunkId(chunkKeys.getSuperChunkId(),
                                               metadataVersion);
                }
                lobSize += currentSize;
                currentSize = 0;
                initialReplacePut = false;
            }

        return metadataVersion;
    }

    /**
     * Persists the new super chunk id in the metadata. Note that this method
     * must be invoked after the chunk associated with this superchunk has
     * been written.
     */
    private Version checkpointSuperChunkId(long superChunkId,
                                           Version metadataVersion) {
        if ((lobProps.getNumChunks() != null) ||
            (lobProps.getLOBSize() != null)) {
            throw new IllegalStateException("Inconsistent lob props for "
                + "metadata checkpoint:" + lobProps.toString());
        }
        lobProps.setLastSuperChunkId(superChunkId);
        return updateMetadata(metadataVersion);
    }

    /**
     * Setup for resuming a put operation that needs to be resumed, or an
     * append operation (either a new one, or one that needs to be resumed.)
     *
     * It determines the last chunk that was persistently stored and verifies
     * that the trailing bytes in the LOB stored so far match the supplied
     * stream. Note that "put" operations are always resumed on an integral
     * chunk boundary, but append operations may need to be initiated or
     * resumed from unaligned boundaries. This method does not make any
     * distinction between the two cases.
     *
     * The method initializes the in-memory values for numChunks and lobSize,
     * so that any subsequent writing can proceed from that point onwards.
     *
     * @param metadataVersion the version associated with the partial metadata
     *
     * @return the ValueVersion associated with the last chunk or null, if
     * there are no chunks associated with the LOB. The parameter is used to
     * detect any concurrent LOB modifications.
     *
     * @throws IOException if the lobStream generated one when verifying the
     * trailing bytes
     */
    protected ValueVersion setupForResume(Version metadataVersion)
        throws IOException {

        numChunks = computeNumChunks();

        if (numChunks == 0) {
            lobSize = 0;
            return null;
        }

        final ValueVersion lastChunkVV = getLastChunk();
        final int lastChunkLength =
            (lastChunkVV == null) ? 0 :
             lastChunkVV.getValue().getValue().length;

        lobSize = ((numChunks - 1) * chunkSize) + lastChunkLength;
        verifyTrailingBytes(metadataVersion);

        return lastChunkVV;
    }

    /**
     * Use the last superchunk as the starting point for determining the number
     * of chunks that were written when the operation failed, by searching
     * efficiently within it. The order of writing in the putChunks() method
     * ensures that the last chunk must either be in the last recorded super
     * chunk, or it is the first chunk in the next, as yet unrecorded super
     * chunk, if the failure was between the writing of that chunk and the
     * update of the super chunk id in the metadata.
     */
    private long computeNumChunks() {
        Long storedLastSCId = lobProps.getLastSuperChunkId();

        if (storedLastSCId == null) {
            /* Failed between write of first chunk and metadata. */
            storedLastSCId = 1l;
        }

        /* Determine the last recorded saved super chunk. */
        long lastSCId = storedLastSCId + 1;
        long lastCId = findLastChunkInSuperChunk(lastSCId);
        if (lastCId == -1) {
            /* Look in the previous super chunk. */
            lastSCId--;
            lastCId = findLastChunkInSuperChunk(lastSCId);
            if (lastCId == -1) {
                if (lastSCId == 1) {
                    /* At start, no chunks were written. */
                    return 0;
                }
                final String msg = "Expected at least one chunk for " +
                    lastSCId + " Delete the LOB and retry.";
                throw new IllegalStateException(msg);

            }
        }

        /* Have the last chunk. */
        return ((lastSCId - 1) * chunksPerPartition) + lastCId;
    }

    /**
     * Find the last chunk in a super chunk, by finding the key with the
     * largest chunkId.
     */
    private long findLastChunkInSuperChunk(long superChunkId) {
        final Key lastSCKey =
            chunkKeyFactory.createSuperChunkKey(internalLOBKey, superChunkId);

        long chunkId = -1;
        /*
         * Use absolute consistency reads since it's effectively a metadata read
         * and required to ensure we are not getting fewer keys than are present
         * from a lagging replica.
         */
        for (Iterator<Key> i =
            kvsImpl.multiGetKeysIterator(Direction.FORWARD,
                                         chunksPerPartition,
                                         lastSCKey,
                                         new KeyRange("", false, null, false),
                                         Depth.CHILDREN_ONLY,
                                         Consistency.ABSOLUTE,
                                         chunkTimeoutMs,
                                         TimeUnit.MILLISECONDS);
                i.hasNext();) {

           chunkId = Math.max(chunkId, chunkKeyFactory.getChunkId(i.next()));
        }
        return chunkId;
    }

    /**
     * Reads in the last chunk as identified by the numChunks value.
     *
     * @return the ValueVersion for the last chunk, or null if the LOB
     * has no chunks
     */
    protected ValueVersion getLastChunk() {

        if (numChunks < 0) {
            throw new IllegalStateException("Chunk count is unknown:" +
                                             numChunks);
        }

        if (numChunks == 0) {
            return null;
        }

        final long superChunkId = ((numChunks - 1) / chunksPerPartition) + 1;
        final long chunkId = ((numChunks - 1) % chunksPerPartition) + 1;

        final Key lastChunkKey =
            chunkKeyFactory.create(internalLOBKey, superChunkId, chunkId);

        return kvsImpl.get(lastChunkKey, Consistency.ABSOLUTE,
                           chunkTimeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Verifies the trailing LOB bytes. Note that the user supplied lob stream
     * is advanced as a result of the verification and is positioned at the
     * first byte to be appended to the partial LOB upon exit.
     */
    private void verifyTrailingBytes(Version metadataVersion)
        throws IOException {

        final long verificationByteCount = getVerificationByteCount();

        long chunkStreamPos = lobSize - verificationByteCount;

        /* Position the lobStream at the first byte to be verified.  */
        long lobStreamPos = positionLobStream(chunkStreamPos);

        if (verificationByteCount == 0) {
            return;
        }

        /* Verify all the bytes starting with this last chunk. */
        final ChunkEncapsulatingInputStream verStream =
            new ChunkEncapsulatingInputStream(this,
                                              lobSize,
                                              metadataVersion);
        try {
            /*
             * Position the chunk stream at the first byte to be verified.
             */
            final long skipBytes = verStream.skip(chunkStreamPos);

            if (skipBytes != chunkStreamPos) {
                throw new IllegalStateException("Requested skip bytes: " +
                                                chunkStreamPos +
                                                " actual skip bytes:" +
                                                skipBytes);
            }

            /* Now compare the two streams until the chunk stream is exhausted. */
            int chunkStreamByte;
            while ((chunkStreamByte = verStream.read()) != -1) {
                lobStreamPos++; chunkStreamPos++;
                final int lobStreamByte = lobStream.read();
                if (lobStreamByte == -1) {
                    final String msg =
                        "Premature EOF on LOB stream at byte: " +
                            lobStreamPos + " chunk stream at:" + verStream;
                    throw new IllegalArgumentException(msg);
                }

                if (chunkStreamByte != lobStreamByte) {
                    final String msg =
                        "LOB stream inconsistent with stored LOB contents. " +
                            " Byte mismatch." +
                            " Stream byte position: " + lobStreamPos +
                            " LOB byte position:" + chunkStreamPos +
                            " app stream byte: " + lobStreamByte +
                            " lob byte: " + chunkStreamByte +
                            " lob stream: " + verStream;
                    throw new IllegalArgumentException(msg);
                }
            }
        } finally {
            verStream.close();
        }
    }

    /**
     * Position the lobStream at the byte position relative to the LOB. This
     * method is overridden by the append operation, so that it can position
     * the lobStream relative to the point where this instance of the append
     * operation was initiated.
     *
     * @param chunkStreamPos the LOB-relative byte position.
     *
     * @return the stream byte position
     */
    protected long positionLobStream(long chunkStreamPos)
        throws IOException {

        final long bytesSkipped = skipInput(lobStream, chunkStreamPos);
        if (chunkStreamPos != bytesSkipped) {
            throw new IllegalArgumentException(
                "The LOB input stream did not skip the requested number of" +
                " bytes." +
                " Bytes skipped:" + bytesSkipped +
                " Requested:" + chunkStreamPos);
        }

        return chunkStreamPos;
    }

    /**
     * Returns an adjusted verification byte count that accounts for the actual
     * number of bytes that were written for the LOB.
     *
     * The implementation of the Append operation overrides this method to
     * establish a lower min value by accounting for the append stream as well.
     */
    protected long getVerificationByteCount() {
        final long verificationByteCount = testVerificationByteCount >= 0 ?
            testVerificationByteCount :
            kvsImpl.getDefaultLOBVerificationBytes();

        return Math.min(lobSize, verificationByteCount);
    }

    public static void
        setTestVerificationByteCount(long testVerificationByteCount) {

        WriteOperation.testVerificationByteCount = testVerificationByteCount;
    }

    public static void
        revertTestVerificationByteCount() {

        WriteOperation.testVerificationByteCount = -1;
    }
}
