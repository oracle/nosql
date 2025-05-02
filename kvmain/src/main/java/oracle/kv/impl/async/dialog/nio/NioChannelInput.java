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
import java.nio.charset.Charset;

import oracle.kv.impl.async.BytesInput;
import oracle.kv.impl.async.IOBufPoolTrackers;
import oracle.kv.impl.async.IOBufSliceImpl;
import oracle.kv.impl.async.IOBufSliceList;
import oracle.kv.impl.async.IOBufferPool;
import oracle.kv.impl.async.dialog.AbstractDialogEndpointHandler;
import oracle.kv.impl.async.dialog.ChannelInput;
import oracle.kv.impl.util.EventTrackersManager;

import com.sleepycat.util.PackedInteger;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Input used for reading from an NIO channel and feeding to protocol read.
 *
 * This input provides buffer management for both socket channel reads and
 * protocol reader. For socket channel reads, to fully utilize the gathering
 * read feature and avoid reading into a byte buffer that has few remaining
 * space, we always prepare two buffers. For protocol reads, we must support
 * the mark-and-reset functionality as the current data may not be complete for
 * a protocol message.
 *
 * The data structure is as follows:
 * |------------------------------------------------------------|
 * |                           inputSliceList                   |
 * |------------------------------------------------------------|
 * | ...| protoSlice | ............. | chnlSlice0 | chnlSlice1  |
 * |------------------------------------------------------------|
 *         ^  ^                                ^
 *         |  |                                |
 *         |  |                                |
 *         |  markPos, where protocol reader   |
 *         |           marks for a later reset |
 *         |                                   |
 *         |                              chnlPos, where socket reads start
 *     protoPos, where protocol read starts
 *
 * The instances of this class is intended to be used inside the single-thread
 * channel exector and is not intended to be thread safe.
 *
 * The input is used as follows:
 *
 * while (true) {
 *     ByteBuffer[] bufs = input.flipToChannelRead()
 *     socketChannel.read(bufs);
 *     input.flipToProtocolRead();
 *     while (true) {
 *         // ...
 *         input.mark();
 *         if (notEnoughData(input)) {
 *             input.reset();
 *             break;
 *         }
 *         // read data from input
 *     }
 * }
 */
public class NioChannelInput implements ChannelInput {

    /* TODO: Use StandardCharsets version in Java 8 */
    private static final Charset utf8 = Charset.forName("UTF-8");

    private static final ByteBuffer emptyBuf = ByteBuffer.allocate(0);
    private static final ByteBuffer[] emptyBufs = new ByteBuffer[0];
    private static final IOBufSliceImpl emptySlice =
        new IOBufSliceImpl.HeapBufSlice(emptyBuf);

    private static final String READ_BYTES_INPUT = "readBytesInput";

    /* A pool for allocating byte buffers */
    private final IOBufferPool pool;
    /* The event trackers manager */
    private final @Nullable EventTrackersManager trackersManager;
    /*
     * The endpoint handler, the handler is used for tracking to describe the
     * states of the endpoint handler associated to the channel input buffer
     * slices.
     */
    private final @Nullable AbstractDialogEndpointHandler endpointHandler;
    /*
     * List of buffer slices including channel read slices and protocol read
     * slices.
     */
    private final IOBufSliceList inputSliceList;
    /*
     * Two buffer slices for channel read, and their underlying byte buffers.
     * When flipping to the protocol-read mode, if chnlBuf0 is filled with
     * protocol-ready data, chnlBuf1 becomes chnlBuf0 while a new chnllBuf1 is
     * allocated.
     */
    private IOBufSliceImpl chnlSlice0;
    private ByteBuffer chnlBuf0;
    private IOBufSliceImpl chnlSlice1;
    private ByteBuffer chnlBuf1;
    /* Byte buffer array for channel read. */
    private final ByteBuffer[] chnlArray = new ByteBuffer[2];
    /*
     * The pos in chnlBuf0 for a socket channel read. Data before chnlPos is
     * put during previos channel read and is ready for protocol read.
     */
    private int chnlPos;
    /* The slice for protocol read */
    private IOBufSliceImpl protoSlice;
    /* The underlying byte buffer of protoSlice */
    private ByteBuffer protoBuf;
    /*
     * The pos of the protoBuf to start read. Used the save the position of
     * protoBuf when flipping to the channel read mode. This is necessary
     * because chnlBuf0 and protoBuf can be the same byte buffer.
     */
    private int protoPos;
    /*
     * The marked pos that may be reset later. The marked buffer is always the
     * first slice of inputSliceList, i.e., buffers before it are always
     * considered consumed and thrown away since the caller will have no way to
     * refer to them again.
     */
    private int markPos;
    /*
     * Number of bytes readable. Readable bytes includes the bytes after
     * protoBuf.position(), plus all the bytes in the slices after protoSlice
     * in inputSliceList until the bytes before chnlBuf0.chnlPos.
     */
    private int readableBytes = 0;

    /* Byte array used for reading packed long. */
    private final byte[] packedLongBytes =
        new byte[PackedInteger.MAX_LONG_LENGTH];

    /* Whether close called. */
    private boolean closed = false;

    public NioChannelInput(EventTrackersManager trackersManager,
                           AbstractDialogEndpointHandler endpointHandler) {
        this(IOBufferPool.CHNL_IN_POOL, trackersManager, endpointHandler);
    }

    /* For testing */
    public NioChannelInput(IOBufferPool pool) {
        this(pool, null, null);
    }

    public NioChannelInput(IOBufferPool pool,
                           @Nullable EventTrackersManager trackersManager,
                           @Nullable AbstractDialogEndpointHandler handler) {
        this.pool = pool;
        this.trackersManager = trackersManager;
        this.endpointHandler = handler;
        this.chnlSlice0 =
            IOBufSliceImpl.InputPoolBufSlice.createFromPool(
                pool, trackersManager);
        IOBufPoolTrackers.addEndpointHandlerInfo(chnlSlice0, endpointHandler);
        this.chnlBuf0 = chnlSlice0.buf();
        this.chnlSlice1 =
            IOBufSliceImpl.InputPoolBufSlice.createFromPool(
                pool, trackersManager);
        IOBufPoolTrackers.addEndpointHandlerInfo(chnlSlice1, endpointHandler);
        this.chnlBuf1 = chnlSlice1.buf();
        this.chnlArray[0] = chnlBuf0;
        this.chnlArray[1] = chnlBuf1;
        this.chnlPos = 0;
        this.inputSliceList = new IOBufSliceList();
        this.inputSliceList.add(chnlSlice0);
        this.inputSliceList.add(chnlSlice1);
        this.protoSlice = chnlSlice0;
        this.protoBuf = chnlBuf0;
        this.protoPos = 0;
        this.markPos = 0;
    }

    /**
     * Sets the mark at the current position.
     */
    @Override
    public void mark() {
        /* Since we mark again, everything until protoSlice can be removed. */
        pollUntilProtoSlice();
        markPos = protoBuf.position();
    }

    /**
     * Resets the position to a previously marked position.
     */
    @Override
    public void reset() {
        /*
         * Resets the protocol-ready buffers to the state of when we mark,
         * i.e., all buffers between marked slice and the current protoSlice
         * should reset their position to 0 and the marked slice reset its
         * position to the markPos. We only need to reset until the current
         * protoSlice because the slices after are not read yet for the
         * protocol and should have their positions set to 0.
         */
        for (IOBufSliceList.Entry slice = inputSliceList.head();
             slice != null;
             slice = slice.next()) {
            final ByteBuffer buf = slice.buf();
            readableBytes += buf.position();
            buf.position(0);
            if (slice == protoSlice) {
                break;
            }
        }

        /* Reset the pointers to the protoSlice */
        protoSlice = (IOBufSliceImpl) inputSliceList.head();
        protoBuf = protoSlice.buf();
        protoBuf.position(markPos);

        readableBytes -= markPos;
    }

    /**
     * Gets the number of readable bytes in the input.
     */
    @Override
    public int readableBytes() {
        return readableBytes;
    }

    /**
     * Reads a byte from the input.
     */
    @Override
    public byte readByte() {
        ensureProtoSliceNotConsumed();
        byte b = protoBuf.get();
        readableBytes --;
        return b;
    }

    /**
     * Reads {@code len} bytes from the input.
     */
    @Override
    public BytesInput readBytes(int len) {
        if (len == 0) {
            return new NioBytesInput(0, null);
        }
        IOBufSliceList bufs = new IOBufSliceList();
        int n = len;
        while (true) {
            int chunkLen = Math.min(n, protoBuf.remaining());
            if (chunkLen != 0) {
                final IOBufSliceImpl forked =
                    protoSlice.forkAndAdvance(chunkLen, READ_BYTES_INPUT);
                IOBufPoolTrackers.addStringInfo(forked, READ_BYTES_INPUT);
                bufs.add(forked);
                n -= chunkLen;
            }
            if (n == 0) {
                break;
            }
            ensureProtoSliceNotConsumed();
        }
        readableBytes -= len;
        return new NioBytesInput(len, bufs);
    }

    /**
     * Peeks at the input to see if there is enough data for {@code
     * readPackedLong}.
     */
    @Override
    public boolean canReadPackedLong() {
        if (readableBytes == 0) {
            return false;
        }
        return (readableBytes >= peekPackedLongLength());
    }

    /**
     * Reads a packed long.
     */
    @Override
    public long readPackedLong() {
        /* Get the length. */
        final int len = peekPackedLongLength();
        /* Get the bytes from current protoBuf and the next if necessary */
        final int rest = protoBuf.remaining();
        if (len <= rest) {
            protoBuf.get(packedLongBytes, 0, len);
        } else {
            protoBuf.get(packedLongBytes, 0, rest);
            ensureProtoSliceNotConsumed();
            protoBuf.get(packedLongBytes, rest, len - rest);
        }
        readableBytes -= len;
        return PackedInteger.readLong(packedLongBytes, 0);
    }

    private int peekPackedLongLength() {
        ensureProtoSliceNotConsumed();
        packedLongBytes[0] = protoBuf.get(protoBuf.position());
        return PackedInteger.getReadLongLength(packedLongBytes, 0);
    }

    /**
     * Reads a {@code String} in standard UTF8 format.
     */
    @Override
    public String readUTF8(int length) {
        if (readableBytes < length) {
            return null;
        }
        /* Read the UTF8 bytes */
        final int len = length;
        final byte[] bytes = new byte[length];
        int offset = 0;
        while (true) {
            int n = Math.min(length, protoBuf.remaining());
            protoBuf.get(bytes, offset, n);
            length -= n;
            offset += n;
            if (length == 0) {
                break;
            }
            ensureProtoSliceNotConsumed();
        }
        readableBytes -= len;
        return utf8.decode(ByteBuffer.wrap(bytes)).toString();
    }

    /**
     * Close the input and release resources.
     */
    @Override
    public void close() {
        closed = true;
        inputSliceList.freeEntries();
        chnlBuf0 = chnlBuf1 = protoBuf = emptyBuf;
        chnlSlice0 = chnlSlice1 = protoSlice = emptySlice;
    }

    /**
     * Return the string representation.
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("ChannelInput");
        builder.append(" chnlPos=").append(chnlPos);
        builder.append(" markPos=").append(markPos);
        builder.append(" protoPos=").append(protoPos);
        builder.append(" readable=").append(readableBytes);
        builder.append(" bufs={");
        for (IOBufSliceList.Entry slice = inputSliceList.head();
             slice != null;
             slice = slice.next()) {
            if (slice == protoSlice) {
                builder.append("Proto");
            }
            if (slice == chnlSlice0) {
                builder.append("Chnl0");
            }
            if (slice == chnlSlice1) {
                builder.append("Chnl1");
            }
            builder.append(slice);
            builder.append(" ");
        }
        builder.append("}");
        return builder.toString();
    }

    /**
     * Flip to the mode of reading from channel.
     *
     * Gets the input ready for a socket channel read after we have completed a
     * protocol read. See the class description for how the method is used.
     *
     * @return byte array for channel reading.
     */
    public ByteBuffer[] flipToChannelRead() {
        if (closed) {
            return emptyBufs;
        }
        /*
         * Clean up and save the protocol read states.
         * - Reset the mark position
         * - Remove buffers until protoBuf
         * - Set the protoPos
         */
        markPos = 0;
        pollUntilProtoSlice();
        protoPos = protoBuf.position();
        /* Make chnlBuf0 ready for channel read. */
        chnlBuf0.position(chnlPos);
        chnlBuf0.limit(chnlBuf0.capacity());
        return chnlArray;
    }

    /**
     * Flip to the mode of reading by the protocol.
     *
     * Gets the input ready for protocol reader after we have completed a
     * socket channel read. See the class description for how the method is
     * used.
     */
    public void flipToProtocolRead() {
        if (closed) {
            return;
        }
        /* Do accounting first */
        readableBytes += chnlBuf0.position() - chnlPos;
        readableBytes += chnlBuf1.position();
        /* Step1, allocate new buffer if necessary. */
        while (chnlBuf0.remaining() == 0) {
            /*
             * Make all the data in chnlBuf0 readable for protocol, if it is
             * already half-read, then it is also the protoBuf, the position
             * will be set in Step 3.
             */
            chnlBuf0.limit(chnlBuf0.capacity());
            chnlBuf0.position(0);
            /* Allocate a new buffer. */
            chnlSlice0 = chnlSlice1;
            chnlSlice1 = closed ? emptySlice :
                IOBufSliceImpl.InputPoolBufSlice.createFromPool(
                    pool, trackersManager);
            IOBufPoolTrackers.addEndpointHandlerInfo(
                chnlSlice1, endpointHandler);

            chnlBuf0 = chnlSlice0.buf();
            chnlBuf1 = chnlSlice1.buf();
            chnlArray[0] = chnlBuf0;
            chnlArray[1] = chnlBuf1;
            inputSliceList.add(chnlSlice1);
        }
        /*
         * Step2, save channel read postion and Make chnlBuf0 ready for
         * protocol read.
         */
        chnlPos = chnlBuf0.position();
        chnlBuf0.limit(chnlPos);
        chnlBuf0.position(0);
        /*
         * Step3, set up the protocol read buffers. This will also set chnlBuf0
         * to the correct position if protoBuf == chnlBuf0.
         */
        pollUntilProtoSlice();
        protoBuf.position(protoPos);
    }

    /**
     * Removes everything until protoSlice.
     */
    private void pollUntilProtoSlice() {
        while (true) {
            IOBufSliceList.Entry slice = inputSliceList.head();
            if (slice == null) {
                /*
                 * This may be possible when the method is called after the
                 * input has closed
                 */
                break;
            }
            if (slice == protoSlice) {
                break;
            }
            inputSliceList.poll();
            slice.markFree();
        }
    }

    /**
     * Ensures the protoBuf has remaining.
     */
    private void ensureProtoSliceNotConsumed() {
        IOBufSliceList.Entry curr = protoSlice;
        while (true) {
            final ByteBuffer buf = curr.buf();
            if (buf.remaining() > 0) {
                break;
            }
            curr = curr.next();
            if (curr == null) {
                throw new IllegalStateException(
                        String.format(
                            "There is not enough data, " +
                            "should check readableBytes before read, " +
                            "input=%s",
                            toString()));
            }
        }
        protoSlice = (IOBufSliceImpl) curr;
        protoBuf = protoSlice.buf();
    }
}
