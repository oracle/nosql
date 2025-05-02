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

package oracle.kv.impl.async.dialog;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;

import oracle.kv.impl.async.IOBufPoolTrackers;
import oracle.kv.impl.async.IOBufSliceImpl;
import oracle.kv.impl.async.IOBufSliceList;
import oracle.kv.impl.async.IOBufferPool;
import oracle.kv.impl.util.EventTrackersManager;

import com.sleepycat.util.PackedInteger;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An output class used for writing protocol messages.
 *
 * The class implements the funcitonality of writing protocol messages chunks.
 * Inheriting will implement the flushing funcitonality.
 *
 * Each message must be written to the channel output atomically, i.e., bytes
 * cannot be interleaved for different messages. Therefore, the channel output
 * is operated with chunks. To write, start with beginChunk and finish with
 * Chunk#done. The implementation of Chunk#done should be atomic.
 *
 * Note that there is no atomicity support in this class among chunks.
 * Therefore, the happen-before relationship between two chunks cannot be
 * defined using only methods from this class, for example, to achieve the
 * requirement of no DialogFrame after DialogAbort. Extra locking is needed for
 * that purpose.
 */
public class ChannelOutput {

    /* TODO: Use StandardCharsets version in Java 8 */
    private static final Charset utf8 = Charset.forName("UTF-8");

    private static final String ADD_TO_CHUNK_OUTPUTS = "addToChunkOutputs";
    /*
     * The buffer pool used to allocate buffer slices for non-array-like
     * scatter data
     */
    private final IOBufferPool pool;
    /* Manager to track pool buffer usage. */
    private final @Nullable EventTrackersManager trackersManager;
    /* The endpoint handler */
    private final @Nullable AbstractDialogEndpointHandler endpointHandler;
    /*
     * A slice list staging the written chunk buf slices which will be flushed
     * to the transport. Access to the slices are inside its synchronization
     * block.
     */
    private final IOBufSliceList slices = new IOBufSliceList();
    /*
     * Whether last chunk done. Access to the slices are inside the
     * synchronization block of slices.
     */
    private boolean lastChunkDone = false;
    /*
     * Whether close called. Access should be inside the synchronization block
     * of slices.
     */
    private boolean closed = false;

    public ChannelOutput(IOBufferPool pool) {
        this(pool, null, null);
    }

    protected ChannelOutput(IOBufferPool pool,
                            @Nullable EventTrackersManager trackersManager,
                            @Nullable
                            AbstractDialogEndpointHandler endpointHandler) {
        this.pool = pool;
        this.trackersManager = trackersManager;
        this.endpointHandler = endpointHandler;
    }

    /**
     * Moves all of the slices representing chunks written to this output
     * channel to the provided list.
     */
    public void fetch(IOBufSliceList toExtend) {
        synchronized(slices) {
            if (closed) {
                return;
            }
            toExtend.extend(
                slices,
                (s) ->
                IOBufPoolTrackers.addStringInfo(s, "fetchFromChannelOutput"));
        }
    }

    /**
     * Begin a message chunk.
     *
     * Must call {@link Chunk#done} after finished writing to the chunk.
     *
     * @param maxScatterSize the maximum size of data that is not held
     * inside byte buffers.
     * @param last {@code true} if the chunk is the last valid chunk. Chunks
     * after the last will not be written to the transport.
     *
     * @return the chunk
     */
    public Chunk beginChunk(int maxScatterSize, boolean last) {
        return new Chunk(maxScatterSize, last);
    }

    /**
     * Close the output and release the resources.
     */
    public void close() {
        synchronized(slices) {
            closed = true;
            slices.freeEntries();
        }
    }

    /**
     * A chunk for writing message fields that writes to a list of byte
     * buffers.
     *
     * The class is not thread safe. All operations to the chunk should appear
     * in one thread.
     *
     * A caller should call the write*() methods and call done(). No methods of
     * this should be called after done() is called, otherwise, leak may occur.
     */
    public class Chunk {

        /* A buffer slice used for non-array-like scatter data. */
        private final IOBufSliceImpl sliceForScatter;
        /* The underlying byte buffer of sliceForScatter */
        private final ByteBuffer bufForScatter;
        /* The underlying byte array of bufForScatter */
        private final byte[] bytesForScatter;
        /* Position in bufForScatter to add to outputs */
        private int addBufPos = 0;
        /* The list of buffers holding all byte buffers of this chunk */
        private final IOBufSliceList outputs;
        /* Whether the chunk is the last of a dialog */
        private final boolean last;

        public Chunk(int maxScatterSize, boolean last) {
            this.sliceForScatter =
                IOBufSliceImpl.OutputPoolBufSlice.createFromPool(
                    pool, IOBufPoolTrackers.TrackerType.CHANNEL_OUTPUT,
                    trackersManager);
            IOBufPoolTrackers.addEndpointHandlerInfo(
                sliceForScatter, endpointHandler);
            this.bufForScatter = sliceForScatter.buf();
            this.bytesForScatter = bufForScatter.array();
            this.outputs = new IOBufSliceList();
            this.last = last;
            if (bytesForScatter.length < maxScatterSize) {
                throw new IllegalStateException(
                              String.format(
                                  "Chunk scatter data size %d " +
                                  "is larger than pool buffer size %d",
                                  maxScatterSize, bytesForScatter.length));
            }
        }

        /**
         * Writes a byte.
         *
         * @param v value to write
         */
        public void writeByte(byte v) {
            bufForScatter.put(v);
        }

        /**
         * Writes a packed integer.
         *
         * The format follows com.sleepycat.util.PackedInteger#writeLong
         *
         * @param v value to write
         */
        public void writePackedLong(long v) {
            int pos = bufForScatter.position();
            int newPos = PackedInteger.writeLong(bytesForScatter, pos, v);
            bufForScatter.position(newPos);
        }

        /**
         * Writes a string with standard UTF-8 encoding, truncating the output
         * if it exceeds the specified maximum length.
         *
         * @param s the string
         * @param maxLength the max length of the encoded string
         */
        public void writeUTF8(String s, int maxLength) {
            CharBuffer chbuf = CharBuffer.wrap(s);
            final ByteBuffer buf = utf8.encode(chbuf);
            int length = buf.limit();
            /* Truncate the output if it is too large. */
            if (length > maxLength) {
                buf.limit(maxLength);
                length = buf.limit();
            }
            writePackedLong(length);
            addBuf();
            outputs.add(new IOBufSliceImpl.HeapBufSlice(buf));
        }

        /**
         * Writes an array of byte buffers.
         *
         * @param bytes the buffer slice list
         */
        public void writeBytes(IOBufSliceList bytes) {
            addBuf();
            outputs.extend(
                bytes,
                (s) -> {
                    IOBufPoolTrackers.addStringInfo(
                        s, ADD_TO_CHUNK_OUTPUTS);
                    IOBufPoolTrackers.addEndpointHandlerInfo(
                        s, endpointHandler);
                });
        }

        /**
         * Mark the writing chunk done.
         */
        public void done() {
            addBuf();
            synchronized(slices) {
                /*
                 * Only add the chunk if not closed and we have not seen last
                 * chunk.
                 */
                if ((!closed) && (!lastChunkDone)) {
                    slices.extend(
                        outputs,
                        (s) ->
                        IOBufPoolTrackers.addStringInfo(
                            s, "chunkAddToChnlOutput"));
                    if (last) {
                        lastChunkDone = true;
                    }
                } else {
                    /*
                     * We do not need to worry about adding slices after
                     * outputs entries are freed causing leak since our code
                     * always have the done() method as the last method called
                     * for a Chunk. See methods of ProtocolWriter.
                     */
                    outputs.freeEntries();
                }
            }
            sliceForScatter.markFree();
        }

        /**
         * Returns if the chunk is the last.
         */
        public boolean last() {
            return last;
        }

        private void addBuf() {
            int pos = bufForScatter.position();
            if (addBufPos > pos) {
                throw new IllegalStateException(
                              String.format(
                                  "Wrong position for scatter buffer " +
                                  "pos=%d, addBufPos=%d",
                                  pos, addBufPos));
            }
            if (addBufPos == pos) {
                return;
            }
            final IOBufSliceImpl forked = sliceForScatter.forkBackwards(
                pos - addBufPos, ADD_TO_CHUNK_OUTPUTS);
            IOBufPoolTrackers.addStringInfo(forked, ADD_TO_CHUNK_OUTPUTS);
            outputs.add(forked);
            addBufPos = pos;
        }
    }

}

