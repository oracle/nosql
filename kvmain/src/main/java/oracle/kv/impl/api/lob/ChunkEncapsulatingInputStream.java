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
import java.nio.ByteBuffer;
import java.util.ConcurrentModificationException;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.ConsistencyException;
import oracle.kv.FaultException;
import oracle.kv.KVSecurityException;
import oracle.kv.Key;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.impl.api.KVStoreImpl;

/**
 * The subclass of InputStream that's used by the application read a LOB
 * whose underlying representation consists of RMI retrievable fixed size
 * chunks.
 */
public class ChunkEncapsulatingInputStream extends InputStream {

    private final Version metadataVersion;

    private boolean closed = false;

    private ByteBuffer chunkBuffer = null;
    private final long lobSize;

    private final KVStoreImpl kvsImpl;
    private final Key internalLobKey;

    private final Consistency consistency;
    private final long timeoutMs;

    /* The chunk size */
    private final int chunkSize;

    /* The current chunk providing bytes. */
    private Key chunkKey = null;
    private final ChunkKeysIterator chunkKeys;

    /**
     * The information resulting from a mark operation for use during a
     * subsequent reset.
     */
    private Mark mark;

    /**
     * Creates the LOB input stream to be handed over to the application. This
     * stream supplies all the bytes in the LOB.
     *
     * @param readOp the LOB read operation
     *
     * @param metadataVersion used to verify that the LOB did not change
     * while it was being read
     */
    ChunkEncapsulatingInputStream(ReadOperation readOp,
                                  Version metadataVersion) {

        synchronized (this) /* flush processor cache */ {

            kvsImpl = readOp.getKvsImpl();
            internalLobKey = readOp.getInternalLOBKey();
            consistency = readOp.getConsistency();
            timeoutMs = readOp.getChunkTimeoutMs();
            Operation.LOBProps lobProps = readOp.getLOBProps();
            final long numChunks = lobProps.getNumChunks();
            chunkKeys = readOp.getChunkKeysNumChunksIterator(numChunks);
            chunkSize = readOp.getChunkSize();

            lobSize = lobProps.getLOBSize();
            chunkBuffer = ByteBuffer.allocate(0);

            this.metadataVersion = metadataVersion;
        }
    }

    /**
     * Creates a LOB input stream for reading internally when resuming a
     * partial LOB. The reading is done for the purposes of verification to
     * ensure that the partial LOB is consistent with the user supplied stream.
     *
     * The stream is positioned to start reading the next chunk after the one
     * at which the iterator is currently positioned.
     *
     * @param writeOp the operation initiating the stream creation
     *
     * @param partialLobSize the partial LOB size
     *
     * @param metadataVersion used to verify that the LOB did not change
     * while it was being read
     */
    ChunkEncapsulatingInputStream(WriteOperation writeOp,
                                  long partialLobSize,
                                  Version metadataVersion) {

        synchronized (this) /* flush processor cache */ {
            kvsImpl = writeOp.getKvsImpl();
            internalLobKey = writeOp.getInternalLOBKey();
            consistency = Consistency.ABSOLUTE;
            timeoutMs = writeOp.getChunkTimeoutMs();
            this.chunkKeys =
                writeOp.getChunkKeysByteRangeIterator(0, partialLobSize);
            chunkSize = writeOp.getChunkSize();

            lobSize = partialLobSize;
            chunkBuffer = ByteBuffer.allocate(0);

            this.metadataVersion = metadataVersion;
        }
    }

    @Override
    public synchronized int read()
        throws IOException {

        checkForClosedStream();

        return (ensureBuffer() == -1) ? -1 : (0xff & chunkBuffer.get());
    }

    @Override
    public synchronized int read(byte b[], int off, int len)
        throws IOException {

        checkForClosedStream();

        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException("buffer length: " + b.length +
                                                " offset: " + off +
                                                " len: " + len);
        }

        if (len == 0) {
            return 0;
        }

        final int remainingBytes = ensureBuffer();

        if (remainingBytes == -1) {
            return -1;
        }

        final int getBytes = Math.min(len, remainingBytes);
        chunkBuffer.get(b, off, getBytes);
        return getBytes;
    }

    /**
     * Ensures that there are bytes to be read in the chunkBuffer. If the
     * buffer is empty, it will read the next chunk and use it to initialize
     * the buffer.
     *
     * @return the number of bytes available in the chunkBuffer or -1 if the
     * chunkBuffer is empty and there are no more chunks.
     */
    private int ensureBuffer()
        throws IOException {

        if (atEOS()) {
            return -1;
        }

        final int remainingBytes = chunkBuffer.remaining();
        if (remainingBytes > 0) {
            return remainingBytes;
        }

        /* Chunk exhausted, try next chunk. */
        chunkBuffer = getNextChunk();

        if (chunkBuffer != null) {
            return ensureBuffer();
        }

        /* No more chunks. Verify that metadata was stable. */
        final ValueVersion metadata = getWithFallback(internalLobKey);
        if (metadata == null) {
            throw wrapIOE(new ConcurrentModificationException
                          ("LOB metadata deleted."));
        }

        if (!metadata.getVersion().equals(metadataVersion)) {
            final String msg = "LOB metadata changed." +
                " Version at start: " + metadataVersion +
                " Version at end: " + metadata.getVersion();
            throw wrapIOE(new ConcurrentModificationException(msg));
        }
        /* EOS */
        return -1;
    }

    /**
     * Skips the requested number of bytes efficiently by skipping over entire
     * chunks if the skip can be done more efficiently.
     */
    @Override
    public long skip(long n) throws IOException {
        checkForClosedStream();

        if (n <= 0) {
            return 0;
        }

        if (atEOS()) {
            /* at EOS, nothing to skip */
            return 0;
        }

        final int leadBytes = chunkBuffer.remaining();
        if (n <= leadBytes) {
            /* Skip within current chunk */
            chunkBuffer.position(chunkBuffer.position() + (int)n);
            return n;
        }

        final long startIndex = currentByteIndex();
        if ((startIndex + n) > lobSize) {
            /* Skip the max amount we can, adjust n downwards */
            n = lobSize - startIndex;
        }

        /* Skip the bytes currently in the buffer */
        chunkBuffer.position(chunkBuffer.limit());
        final long skipChunkBytes = n - leadBytes;

        /* Skip intervening chunks */
        final int interveningChunks = (int)(skipChunkBytes / chunkSize);
        final long skippedChunks = chunkKeys.skip(interveningChunks);

        if (skippedChunks != interveningChunks) {
            throw new IllegalStateException("Requested skip chunks: " +
                                            interveningChunks +
                                            " actual bytes: " + skippedChunks);
        }

        /*
         * Ensure that the buffer has been initialized to reflect the new
         * position.
         */
        ensureBuffer();

        /* Skip the trailing bytes in the current chunk */
        final int trailBytesRequest = (int)(skipChunkBytes % chunkSize);
        final long trailBytes = super.skip(trailBytesRequest);

        if (trailBytesRequest != trailBytes) {
            throw new IllegalStateException("Requested skip bytes:" +
                                            trailBytesRequest +
                                            "actual bytes: " +  trailBytes);
        }

        final long endIndex = currentByteIndex();
        if ((endIndex - startIndex) != n) {
            throw new IllegalStateException("End index: " + endIndex +
                                            " <> startIndex: " + startIndex +
                                            " + n: " + n);
        }

        return leadBytes + (skippedChunks * chunkSize) + trailBytes;
    }

    /**
     * The one based byte index in the range:
     *
     * 1 <= i <= lobSize
     *
     * indicating the last byte that was "read" from the stream. A read
     * operation will return the next byte.
     *
     * The value zero is used to denote that it's at the start of the stream
     * and no bytes have been read.
     */
    private long currentByteIndex() {
        if (chunkKeys.currentChunkIndex() == 0) {
            return 0;
        }

        if (atEOS()) {
            return lobSize;
        }

        return (chunkKeys.currentChunkIndex() - 1) * chunkSize +
               chunkBuffer.position();
    }

    /**
     * Utility to wrap any encountered exception in an IO exception, so
     * that any standard user IOException handler can deal with it.
     */
    private IOException wrapIOE(RuntimeException rte) {
        return new IOException("Exception in LOB input stream", rte);
    }

    /**
     * Gets the value associated with the key. It first tries with the supplied
     * consistency, but then falls back to using absolute consistency. It
     * resorts to this fallback if there was a consistency timeout, or if the
     * key was not found, perhaps due to a lagging replica. The fallback makes
     * even the NO_CONSISTENCY policy useful by keeping the read load off the
     * master when a replica has the data but falling back to the master only
     * when necessary.
     */
    private ValueVersion getWithFallback(Key key)
        throws IOException {

        try {
            ValueVersion valueVersion = null;

            try {
                valueVersion = kvsImpl.get(key, consistency, timeoutMs,
                                           TimeUnit.MILLISECONDS);
            } catch (ConsistencyException ce) {

                /*
                 * May be a lagging replica, retry below with ABSOLUTE
                 * consistency.
                 */
            }

            if (valueVersion != null) {
                return valueVersion;
            }

            if (!Consistency.ABSOLUTE.equals(consistency)) {
                /* Fallback retry. */
                return kvsImpl.get(chunkKey, Consistency.ABSOLUTE,
                                   timeoutMs, TimeUnit.MILLISECONDS);
            }

            return valueVersion;
        } catch (FaultException fe) {
            throw wrapIOE(fe);
        } catch (KVSecurityException kse) {
            throw wrapIOE(kse);
        }
    }

    /*
     * Retrieves the next chunk in the LOB
     */
    private ByteBuffer getNextChunk()
        throws IOException {

        if (!chunkKeys.hasNext()) {
            return null;
        }

        chunkKey = chunkKeys.next();

        final ValueVersion valueVersion = getWithFallback(chunkKey);

        if (valueVersion == null) {
            throw wrapIOE(new ConcurrentModificationException
                          ("Missing LOB  chunk key: " +
                           chunkKey +
                           " key iterator: " + chunkKeys.toString() +
                           " LOB size: " + lobSize));
        }
        return ByteBuffer.wrap(valueVersion.getValue().getValue());
    }

    @Override
    public synchronized void mark(int readlimit) {
        if (isClosed()) {
            /* mark on a closed stream has no effect. */
            return;
        }

        mark = new Mark();
    }

    @Override
    public synchronized void reset()
       throws IOException {

       checkForClosedStream();

       if (mark == null) {
           throw new IOException("No preceding call to mark");
       }

       if (mark.pos == -1) {
           /* EOS */
           chunkBuffer = null;
           return;
       }

       chunkKeys.reset(mark.savedIterator);
       chunkKeys.backup(); /* So we can get the current buffer. */
       chunkBuffer = ByteBuffer.allocate(0);
       if (ensureBuffer() != -1) {
           /* position within the buffer to read the next byte. */
           chunkBuffer.position(mark.pos);
       }
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void close()
        throws IOException {

        closed = true;

        /* Free up buffer space. */
        chunkBuffer = null;
    }

    private boolean atEOS() {
        return chunkBuffer == null;
    }

    private boolean isClosed() {
        return closed;
    }

    private void checkForClosedStream()
        throws IOException {

        if (isClosed()) {
            throw new IOException("Stream has been closed");
        }
    }

    /**
     * Utility class to capture the stream position associated with the mark()
     * operation.
     */
    private class Mark {

        /*
         * The chunk iterator at the time of the mark operation. It effectively
         * identifies the current key.
         */
        private final ChunkKeysIterator savedIterator;

        /* The position in the buffer associated with the above chunk key. */
        private final int pos;

        Mark() {
            this((ChunkKeysIterator) chunkKeys.clone(),
                 (chunkBuffer != null) ?
                  chunkBuffer.position() :
                  -1 /* At EOS */);
        }

        Mark(ChunkKeysIterator savedIterator, int pos) {
            super();
            this.savedIterator = savedIterator;
            this.pos = pos;
        }
    }

    @Override
    public synchronized String toString() {
        return "<Chunk Encapsulating stream. " +
                 "chunk key: " + chunkKey +
                 ((chunkBuffer == null) ?
                 " EOS " :
                 (" remaining bytes: " + chunkBuffer.remaining() +
                 " chunk keys: " + chunkKeys.toString())) + ">";
    }
}
