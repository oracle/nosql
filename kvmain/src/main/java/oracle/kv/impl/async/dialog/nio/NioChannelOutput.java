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

package oracle.kv.impl.async.dialog.nio;

import java.nio.ByteBuffer;

import oracle.kv.impl.async.BytesUtil;
import oracle.kv.impl.async.IOBufPoolTrackers;
import oracle.kv.impl.async.IOBufSliceImpl;
import oracle.kv.impl.async.IOBufSliceList;
import oracle.kv.impl.async.IOBufferPool;
import oracle.kv.impl.async.dialog.AbstractDialogEndpointHandler;
import oracle.kv.impl.async.dialog.ChannelOutput;
import oracle.kv.impl.util.EventTrackersManager;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An output used for writing protocol messages and flushing to an NIO channel.
 *
 * The output is used as follows:
 *
 * NioChannelOutput.Bufs bufs = output.getBufs();
 * socketChannel.write(bufs.array(), bufs.offset(), bufs.length());
 * if (output.hasRemaining()) {
 *     // register for WRITE_INTEREST if necessary
 * }
 *
 * The above methods for flushing are not thread safe. Callers should acquire a
 * flush lock while calling these methods.
 */
public class NioChannelOutput extends ChannelOutput {

    private static final int ARRAY_LENGTH = 128;

    private final Bufs bufsToFlush;
    private final int arrayLength;

    public NioChannelOutput(EventTrackersManager trackersManager,
                            AbstractDialogEndpointHandler endpointHandler) {
        this(ARRAY_LENGTH, IOBufferPool.CHNL_OUT_POOL, trackersManager,
            endpointHandler);
    }

    /* For testing */
    public NioChannelOutput(int arrayLength) {
        this(arrayLength, IOBufferPool.CHNL_OUT_POOL, null, null);
    }

    public NioChannelOutput(
        int arrayLength,
        IOBufferPool pool,
        @Nullable EventTrackersManager trackersManager,
        @Nullable AbstractDialogEndpointHandler endpointHandler)
    {
        super(pool, trackersManager, endpointHandler);
        this.arrayLength = arrayLength;
        this.bufsToFlush = new Bufs();
    }

    /**
     * Wrapper class for an array of buffers.
     *
     * The object is fed to the nio socket channel.
     *
     * We try to do a bit optimization and trade-off here. We feed the socket
     * channel with an array of buffers. We do not want this array to have too
     * few data since socket channel writes are costly. We do not want to fill
     * the array too often as well since we need to do array copy to fill.
     * Hence the complication. The strategy is we allocate a larger array and
     * fill when it is half full.
     */
    public class Bufs {
        private ByteBuffer[] array = new ByteBuffer[arrayLength];
        /* The position of the first buffer in the array that has data */
        private int offset = 0;
        /*
         * The list of slices that have the buffers in the array. Used to free
         * the slices when the buffers have no remaining data.
         */
        private final IOBufSliceList freelist = new IOBufSliceList();
        /*
         * The list of slices fetched but not yet put into the array.
         */
        private final IOBufSliceList fetchlist = new IOBufSliceList();

        /**
         * Returns the byte buffer array.
         */
        public ByteBuffer[] array() {
            return array;
        }

        /**
         * Returns the position of the first buffer in the array that has data
         */
        public int offset() {
            return offset;
        }

        /**
         * Returns the number of consecutive buffers in the array that has data
         */
        public int length() {
            return freelist.size();
        }

        /**
         * Updates the Bufs.
         */
        public void update() {
            /* Free up done buffers */
            free();

            if (length() < arrayLength / 2) {
                fill();
            }
        }

        /**
         * Frees the slices that has no remaining data and updates offset and
         * length.
         */
        public void free() {
            final int off = offset;
            final int len = length();
            for (int i = off; i < off + len; ++i) {
                if (array[i].remaining() != 0) {
                    offset = i;
                    break;
                }
                final IOBufSliceList.Entry entry = freelist.poll();
                if (entry == null) {
                    throw new IllegalStateException(
                        "Buffer management code error, seeing null entry");
                }
                entry.markFree();
            }
            if (freelist.isEmpty()) {
                offset = 0;
            }
        }

        /**
         * Fills the array.
         */
        public void fill() {
            /* Save the length since we will modify the freelist */
            final int length = length();
            /* Not fetch if we have enough data to fill the whole array */
            if (fetchlist.size() + length < arrayLength) {
                fetch(fetchlist);
            }
            /* No need to fill if no more data */
            if (fetchlist.isEmpty()) {
                return;
            }

            /* Move all the remaining buffers to the front of the array */
            System.arraycopy(array, offset, array, 0, length);
            offset = 0;

            int inc = Math.min(fetchlist.size(), arrayLength - length);
            for (int i = 0; i < inc; i++) {
                IOBufSliceList.Entry slice = fetchlist.poll();
                if (slice == null) {
                    throw new IllegalStateException(
                        "Buffer management code error, seeing null entry");
                }
                IOBufPoolTrackers.addStringInfo(
                    (IOBufSliceImpl) slice, "addToBufForSocket");
                freelist.add(slice);
                array[length + i] = slice.buf();
            }
        }
    }

    /**
     * Returns the buffers to flush to the channel.
     */
    public Bufs getBufs() {
        bufsToFlush.update();
        return bufsToFlush;
    }

    /**
     * Returns {@code true} if there is remaining data to flush.
     */
    public boolean hasRemaining() {
        return (!bufsToFlush.fetchlist.isEmpty()) || (bufsToFlush.length() != 0);
    }

    @Override
    public void close() {
        super.close();
        bufsToFlush.fetchlist.freeEntries();
        bufsToFlush.freelist.freeEntries();
    }

    /**
     * Return the string representation.
     *
     * The operation is not thread safe.
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("ChannelOutput");
        builder.append(":Array");
        builder.append("(").append(bufsToFlush.offset()).append(",").
            append(bufsToFlush.length()).append(")");
        builder.append(BytesUtil.toString(
                    bufsToFlush.array(), bufsToFlush.offset(),
                    bufsToFlush.length(), 32));
        builder.append("free").append(bufsToFlush.freelist);
        builder.append("fetch").append(bufsToFlush.fetchlist);
        return builder.toString();
    }

}

