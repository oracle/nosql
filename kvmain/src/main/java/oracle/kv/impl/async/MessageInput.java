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

import java.io.DataInput;
import java.io.EOFException;
import java.util.LinkedList;
import java.util.Queue;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@code DataInput} stream for input dialog messages.
 *
 * <p>Methods in this class are not thread-safe. We expect the message input is
 * read from only one thread.
 *
 * <p>Note that it is the callers responsibility to fully consume the returned
 * message input, otherwise leak may happen. The caller can use {@link
 * MessageInput#discard} if not interested in the message input anymore.
 */
public class MessageInput implements DataInput {

    private final Queue<BytesInput> inputs = new LinkedList<BytesInput>();

    /**
     * Creates an instance of this class.
     */
    public MessageInput() {

    }

    /**
     * Reads some bytes from an input stream and stores them into the buffer
     * array {@code b}.
     */
    @Override
    public void readFully(byte @Nullable[] b) throws EOFException {
        if (b == null) {
            throw new IllegalArgumentException("Bytes should not be null");
        }
        readFully(b, 0, b.length);
    }

    /**
     *
     * Reads {@code len} bytes from an input stream.
     */
    @Override
    public void readFully(byte @Nullable[] b, int off, int len)
        throws EOFException {

        if (b == null) {
            throw new IllegalArgumentException("Bytes should not be null");
        }
        if (len == 0) {
            return;
        }
        BytesInput input;
        int remaining;
        while (true) {
            input = inputs.peek();
            if (input == null) {
                throw new EOFException();
            }
            remaining = input.remaining();
            int n = Math.min(len, remaining);
            input.readFully(b, off, n);
            off += n;
            len -= n;
            pollIfConsumed(input);
            if (len == 0) {
                break;
            }
        }
    }

    /**
     * Makes an attempt to skip over {@code n} bytes of data from the input
     * stream, discarding the skipped bytes.
     */
    @Override
    public int skipBytes(int len) {
        int skipped = 0;
        BytesInput input;
        int remaining;
        while (true) {
            input = inputs.peek();
            if (input == null) {
                return skipped;
            }
            remaining = input.remaining();
            int n = Math.min(remaining, len);
            try {
                input.skipBytes(n);
            } catch (EOFException e) {
                /* We already checked the input has this many data. */
                throw new AssertionError();
            }
            len -= n;
            skipped += n;
            pollIfConsumed(input);
            if (len == 0) {
                return skipped;
            }
        }
    }

    /**
     * Reads one input byte and returns {@code true} if that byte is nonzero,
     * {@code false} if that byte is zero.
     */
    @Override
    public boolean readBoolean() throws EOFException {
         byte b = readByte();
         return (b != 0);
    }

    /**
     * Reads and returns one input byte.
     */
    @Override
    public byte readByte() throws EOFException {
        final BytesInput input = inputs.peek();
        if (input == null) {
            throw new EOFException();
        }
        byte b = input.readByte();
        pollIfConsumed(input);
        return b;
    }

    /**
     * Reads one input byte, zero-extends it to type {@code int}, and returns
     * the result, which is therefore in the range {@code 0} through {@code
     * 255}.
     */
    @Override
    public int readUnsignedByte() throws EOFException {
        return readByte() & 0xff;
    }

    /**
     * Reads two input bytes and returns a {@code short} value.
     */
    @Override
    public short readShort() throws EOFException {
        byte b0 = readByte();
        byte b1 = readByte();
        return (short) ((b0 << 8) | (b1 & 0xff));
    }

    /**
     * Reads two input bytes and returns an {@code int} value in the range
     * {@code 0} through {@code 65535}.
     */
    @Override
    public int readUnsignedShort() throws EOFException {
        byte b0 = readByte();
        byte b1 = readByte();
        return (((b0 & 0xff) << 8) | (b1 & 0xff));
    }

    /**
     * Reads two input bytes and returns a {@code char} value.
     */
    @Override
    public char readChar() throws EOFException {
        byte b0 = readByte();
        byte b1 = readByte();
        return (char) ((b0 << 8) | (b1 & 0xff));
    }

    /**
     * Reads four input bytes and returns an {@code int} value.
     */
    @Override
    public int readInt() throws EOFException {
        int result = 0;
        BytesInput input = inputs.peek();
        for (int i = 0; i < 4; i++) {
            if (input == null) {
                throw new EOFException();
            }
            byte b = input.readByte();
            result = (result << 8) + (b & 0xff);
            if (pollIfConsumed(input)) {
                input = inputs.peek();
            }
        }
        return result;
    }

    /**
     * Reads eight input bytes and returns a {@code long} value.
     */
    @Override
    public long readLong() throws EOFException {
        long result = 0;
        BytesInput input = inputs.peek();
        for (int i = 0; i < 8; i++) {
            if (input == null) {
                throw new EOFException();
            }
            byte b = input.readByte();
            result = (result << 8) + (b & 0xff);
            if (pollIfConsumed(input)) {
                input = inputs.peek();
            }
        }
        return result;
    }

    /**
     * Reads four input bytes and returns a {@code float} value.
     */
    @Override
    public float readFloat() throws EOFException {
        return Float.intBitsToFloat(readInt());
    }

    /**
     * Reads eight input bytes and returns a {@code double} value.
     */
    @Override
    public double readDouble() throws EOFException {
        return Double.longBitsToDouble(readLong());
    }

    /**
     * Reads the next line of text from the input stream.
     */
    @Override
    public String readLine() {
        throw new UnsupportedOperationException();
    }

    /**
     * Reads in a string that has been encoded using a modified UTF-8 format.
     */
    @Override
    public String readUTF() {
        throw new UnsupportedOperationException(
                "Use oracle.kv.impl.util.SerializationUtil.readString");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getClass().getSimpleName());
        sb.append("[");
        for (BytesInput input : inputs) {
            sb.append(input.toString());
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * Adds a {@link BytesInput} to the message input.
     *
     * @param input the bytes input
     */
    public void add(BytesInput input) {
        if (input.remaining() == 0) {
            return;
        }
        inputs.add(input);
    }

    /**
     * Discards the message input and cleans up.
     */
    public void discard() {
        while (true) {
            final BytesInput input = inputs.poll();
            if (input == null) {
                break;
            }
            input.discard();
        }
    }

    /**
     * Check if the argument, which should represent the top of the inputs
     * queue, is empty, removing it if it is and returning true.
     */
    private boolean pollIfConsumed(BytesInput input) {
        if (input.remaining() > 0) {
            return false;
        }
        inputs.poll();
        /*
         * Do not need to discard here since we have already consumed
         * everything.
         */
        return true;
    }

    /**
     * Marks that this message input has been polled from the dialog layer by
     * the upper layer, for tracking.
     */
    public void markPolled() {
        inputs.forEach((i) -> i.markMessageInputPolled());
    }
}
