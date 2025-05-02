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

package oracle.kv.impl.async;

import java.io.DataOutput;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import oracle.kv.impl.async.dialog.DialogContextImpl;
import oracle.kv.impl.util.EventTrackersManager;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@code DataOutput} stream for output dialog messages.
 *
 * <p>Methods in this class are not thread-safe. We expect the message output
 * is written from only one thread.
 *
 * <p>If the output will never been written to a dialog, i.e., {@link
 * DialogContext#write} will never be called with this message output as input,
 * {@link MessageOutput#discard} must be called. Otherwise, there will be leak
 * on {@link IOBufferPool}.
 *
 * @see java.io.DataOutput
 */
public class MessageOutput implements DataOutput {

    /**
     * The largest number of bytes that will be copied when writing a byte
     * array. If the array is larger than this size, then the array will be
     * retained in order to copy its value later. In that case, the caller must
     * not modify the array contents.
     */
    public static final int MAX_COPY_BYTES_SIZE = 16;

    private static final String FRAME_POLLED_MESSAGE =
        "The frames of this message output have already been polled. " +
        "That is, the output has already either been " +
        "written to the channel or discarded";

    private static final String ADD_TO_FRAME_TO_POLL = "addToFrameToPoll";
    private static final String APPEND_BUFFERED_TO_OUTPUT =
        "appendBufferedToOutput";

    /* The buffer pool used to allocate */
    private final IOBufferPool pool;
    /* The trackers manager */
    private final @Nullable EventTrackersManager trackersManager;
    /* A buffer slice used for non-array-like scatter data */
    private IOBufSliceImpl sliceForScatter;
    /* The underlying byte buffer for the sliceForScatter */
    private ByteBuffer bufForScatter;
    /*
     * The position in buffer representing the start of new data that needs to
     * be added to outputs.
     */
    private int pos = 0;
    /* The output data in the form of a buffer slice list */
    private final IOBufSliceList outputs = new IOBufSliceList();
    /* Size of the output */
    private volatile int nbytesTotal = 0;
    /*
     * Indicates whether the output frames are already polled. The frames are
     * only allowed to be polled once and if polled, no write can be performed.
     * This is used to make sure that the caller should not write to the
     * message output after it calls DialogContext#write with the message.
     * There is no need to discard the message output after frames are polled,
     * but it is also harmless.
     */
    private final AtomicBoolean framesPolled = new AtomicBoolean(false);

    public MessageOutput() {
        this(IOBufferPool.MESG_OUT_POOL, getEventTrackersManagerOrNull());
    }

    private static @Nullable
        EventTrackersManager getEventTrackersManagerOrNull() {

        final EndpointGroup group = AsyncRegistryUtils.getEndpointGroupOrNull();
        return (group == null) ? null : group.getEventTrackersManager();
    }

    /* For testing */
    public MessageOutput(IOBufferPool pool) {
        this(pool, null);
    }

    /* For testing */
    public MessageOutput(EventTrackersManager trackersManager) {
        this(IOBufferPool.MESG_OUT_POOL, trackersManager);
    }

    private MessageOutput(IOBufferPool pool,
                          @Nullable EventTrackersManager trackersManager) {
        this.pool = pool;
        this.trackersManager = trackersManager;
        this.sliceForScatter =
            IOBufSliceImpl.OutputPoolBufSlice.createFromPool(
                pool, IOBufPoolTrackers.TrackerType.MESSAGE_OUTPUT,
                trackersManager);
        this.bufForScatter = sliceForScatter.buf();
    }

    /**
     * Writes to the output stream the eight low-order bits of the argument
     * <code>b</code>.
     */
    @Override
    public void write(int b) {
        ensureFramesNotPolled();
        allocIfLessThan(1);
        bufForScatter.put((byte) b);
        nbytesTotal ++;
    }

    /**
     * Writes to the output stream all the bytes in array <code>b</code>.
     */
    @Override
    public void write(byte @Nullable[] b) {
        if (b == null) {
            throw new IllegalArgumentException("Bytes should not be null");
        }
        write(b, 0, b.length);
    }

    /**
     * Writes <code>len</code> bytes from array <code>b</code>, in order,  to
     * the output stream.
     */
    @Override
    public void write(byte @Nullable[] b, int off, int len) {
        if (b == null) {
            throw new IllegalArgumentException("Bytes should not be null");
        }
        ensureFramesNotPolled();

        /*
         * Check for zero length: the ByteBuffer.put or wrap methods will do
         * the rest of argument checks.
         */
        if (len == 0) {
            return;
        }
        if ((len > 0) && (len <= MAX_COPY_BYTES_SIZE)) {
            allocIfLessThan(len);
            bufForScatter.put(b, off, len);
        } else {
            appendBufferedToOutput();
            outputs.add(new IOBufSliceImpl.HeapBufSlice(
                            ByteBuffer.wrap(b, off, len)));
        }
        nbytesTotal += len;
    }

    /**
     * Writes a <code>boolean</code> value to this output stream.
     */
    @Override
    public void writeBoolean(boolean v) {
        write(v ? 1 : 0);
    }

    /**
     * Writes to the output stream the eight low- order bits of the argument
     * <code>v</code>.
     */
    @Override
    public void writeByte(int v) {
        write(v);
    }

    /**
     * Writes two bytes to the output stream to represent the value of the
     * argument.
     */
    @Override
    public void writeShort(int v) {
        ensureFramesNotPolled();
        allocIfLessThan(2);
        bufForScatter.putShort((short) v);
        nbytesTotal += 2;
    }

    /**
     * Writes a <code>char</code> value, which is comprised of two bytes, to
     * the output stream.
     */
    @Override
    public void writeChar(int v) {
        ensureFramesNotPolled();
        allocIfLessThan(2);
        bufForScatter.putChar((char) v);
        nbytesTotal += 2;
    }

    /**
     * Writes an <code>int</code> value, which is comprised of four bytes, to
     * the output stream.
     */
    @Override
    public void writeInt(int v) {
        ensureFramesNotPolled();
        allocIfLessThan(4);
        bufForScatter.putInt(v);
        nbytesTotal += 4;
    }

    /**
     * Writes a <code>long</code> value, which is comprised of eight bytes, to
     * the output stream.
     */
    @Override
    public void writeLong(long v) {
        ensureFramesNotPolled();
        allocIfLessThan(8);
        bufForScatter.putLong(v);
        nbytesTotal += 8;
    }

    /**
     * Writes a <code>float</code> value, which is comprised of four bytes, to
     * the output stream.
     */
    @Override
    public void writeFloat(float v) {
        ensureFramesNotPolled();
        allocIfLessThan(4);
        bufForScatter.putFloat(v);
        nbytesTotal += 4;
    }

    /**
     * Writes a <code>double</code> value, which is comprised of eight bytes,
     * to the output stream.
     */
    @Override
    public void writeDouble(double v) {
        ensureFramesNotPolled();
        allocIfLessThan(8);
        bufForScatter.putDouble(v);
        nbytesTotal += 8;
    }

    /**
     * Writes a string to the output stream.
     */
    @Override
    public void writeBytes(@Nullable String s) {
        throw new UnsupportedOperationException();
    }

    /**
     * Writes every character in the string <code>s</code>, to the output
     * stream, in order, two bytes per character.
     */
    @Override
    public void writeChars(@Nullable String s) {
        throw new UnsupportedOperationException();
    }

    /**
     * Writes two bytes of length information to the output stream, followed by
     * the modified UTF-8 representation of  every character in the string
     * <code>s</code>.
     */
    @Override
    public void writeUTF(@Nullable String s) throws UTFDataFormatException {
        throw new UnsupportedOperationException(
                "Use oracle.kv.impl.util.SerializationUtil.writeString");
    }

    /**
     * Gets the size of this output.
     *
     * @return the size
     */
    public int size() {
        return nbytesTotal;
    }

    /**
     * Discards the message output when the dialog is aborted.
     *
     * The method must be called if {@link DialogContext#write} will never be
     * called for this message output. Otherwise, leak would occurr.
     *
     * If this method is called after a {@link DialogContext#write}, then it
     * does nothing.
     *
     * This method may also be called after {@link DialogContext#write} due to
     * thread concurrency. The upper layer should ensure that this method is
     * only called after
     */
    public void discard() {
        if (framesPolled.compareAndSet(false, true)) {
            sliceForScatter.markFree();
            outputs.freeEntries();
        }
    }

    /**
     * Creates frames according to frameSize and returns the frames.
     *
     * After the method is called, all the methods that writes more bytes to
     * this output will throw {@link IllegalStateException}.
     *
     * This method will return the list of frames only once. After this method
     * is called once, future invocation will return {@code null}. The method
     * may be called multiple times, however, due to concurrency between
     * obtaining the frame list and {@link #discard()}.
     *
     * @param frameSize the frame size
     * @return the list of frames, or {@code null} if the method is called
     *         before.
     */
    public @Nullable Queue<IOBufSliceList> pollFrames(
        int frameSize,
        @Nullable DialogContextImpl dialog)
    {
        if (frameSize <= 0) {
            throw new IllegalArgumentException();
        }

        if (!framesPolled.compareAndSet(false, true)) {
            return null;
        }

        appendBufferedToOutput();

        /* Set the dialog info for output slices */
        outputs.setTrackerInfo(
            (s) -> IOBufPoolTrackers.addDialogInfo(s, dialog));

        final LinkedList<IOBufSliceList> frames =
            new LinkedList<IOBufSliceList>();

        if (outputs.isEmpty()) {
            frames.add(new IOBufSliceList());
            return frames;
        }

        /*
         * Current frame that needs to be filled with output slices until its
         * size reaching the frame size
         */
        IOBufSliceList frame = new IOBufSliceList();
        /* The size of the frame */
        int size = 0;
        /* Add the current frame to the list of frames before we fill it */
        frames.add(frame);

        while (true) {
            final IOBufSliceList.Entry slice = outputs.poll();
            if (slice == null) {
                break;
            }
            final ByteBuffer buf = slice.buf();
            /*
             * Keep filling the current frame with chunks of the buf until we
             * consume all.
             */
            while (true) {
                int remaining = buf.remaining();
                if (remaining == 0) {
                    break;
                }

                if (size == frameSize) {
                    /*
                     * The current frame has enough data. Create a new frame,
                     * add it to the frame list and make that the new "current
                     * frame".
                     */
                    frame = new IOBufSliceList();
                    size = 0;
                    frames.add(frame);
                }

                int inc = Math.min(remaining, frameSize - size);
                final IOBufSliceImpl forked =
                    slice.forkAndAdvance(inc, ADD_TO_FRAME_TO_POLL);
                IOBufPoolTrackers.addStringInfo(forked, ADD_TO_FRAME_TO_POLL);
                IOBufPoolTrackers.addDialogInfo(forked, dialog);
                frame.add(forked);
                size += inc;
            }
            slice.markFree();
        }
        /*
         * This method is the last we will call on the object, free the scatter
         * slice.
         */
        sliceForScatter.markFree();
        return frames;
    }

    /**
     * Allocate a new buffer if the remaining of current is less than a certain
     * value. Add frames of the current buffer before allocate the new.
     */
    private void allocIfLessThan(int val) {
        if (bufForScatter.remaining() >= val) {
            return;
        }
        appendBufferedToOutput();
        sliceForScatter.markFree();
        sliceForScatter =
            IOBufSliceImpl.OutputPoolBufSlice.createFromPool(
                pool, IOBufPoolTrackers.TrackerType.MESSAGE_OUTPUT,
                trackersManager);
        bufForScatter = sliceForScatter.buf();
        if (bufForScatter.remaining() < val) {
            throw new IllegalStateException(
                "Scatter-data buffer is too small: requested: " + val +
                " buffer: " + bufForScatter);
        }
        pos = 0;
    }

    /**
     * Append a part between pos and the current position of buffer as a chunk
     * to the frame.
     */
    private void appendBufferedToOutput() {
        int currpos = bufForScatter.position();
        if ((bufForScatter.limit() != bufForScatter.capacity()) ||
            (pos > currpos)) {
            throw new IllegalStateException(
                          String.format("Wrong buffer and position states: " +
                                        "limit=%d, capacity=%d, " +
                                        "pos=%d, currpos=%d",
                                        bufForScatter.limit(),
                                        bufForScatter.capacity(),
                                        pos, currpos));
        }
        if (pos == currpos) {
            return;
        }

        final IOBufSliceImpl forked = sliceForScatter.forkBackwards(
            currpos - pos, APPEND_BUFFERED_TO_OUTPUT);
        IOBufPoolTrackers.addStringInfo(forked, APPEND_BUFFERED_TO_OUTPUT);
        outputs.add(forked);

        pos = currpos;
    }

    /**
     * Ensure the frames are not polled.
     */
    private void ensureFramesNotPolled() {
        if (framesPolled.get()) {
            throw new IllegalStateException(FRAME_POLLED_MESSAGE);
        }
    }
}

