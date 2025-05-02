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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Utility tools for a collection of bytes.
 */
public class BytesUtil {

    private final static int NUM_BYTES_PER_LINE = 16;
    private final static int LINE_NUM_WIDTH = 8 + 4;
    private final static int BYTE_ASCII_SEPARATE_WIDTH = 4;
    private final static int BYTE_REPR_WIDTH = 2;
    private final static int BYTE_SEPARATOR_WIDTH = 1;
    private final static int BYTE_REPR_SEP_WIDTH =
        BYTE_REPR_WIDTH + BYTE_SEPARATOR_WIDTH;
    private final static int ASCII_CHAR_WIDTH = 1;
    private final static int LINE_WIDTH =
        LINE_NUM_WIDTH + NUM_BYTES_PER_LINE * BYTE_REPR_SEP_WIDTH +
        BYTE_ASCII_SEPARATE_WIDTH + NUM_BYTES_PER_LINE * ASCII_CHAR_WIDTH
        + 1;


    /**
     * Returns an integer from four bytes in a byte array.
     *
     * @param array the byte array
     * @param offset the offset to start
     *
     * @return the integer
     */
    public static int bytesToInt(byte[] array, int offset) {
        return (array[offset] << 24) +
            ((array[offset + 1] & 0xff) << 16) +
            ((array[offset + 2] & 0xff) << 8) +
            (array[offset + 3] & 0xff);
    }

    /**
     * Returns the hex string representation of a byte array.
     *
     * @param array the byte array
     *
     * @return the string
     */
    public static String toString(byte @Nullable[] array) {
        if (array == null) {
            return "[/]";
        }
        return toString(array, 0, array.length);
    }

    /**
     * Returns the hex string representation of a byte array.
     *
     * @param array the byte array
     * @param offset the offset to start
     * @param len the number of bytes
     *
     * @return the string
     */
    public static String toString(byte @Nullable[] array,
                                  int offset,
                                  int len) {
        if (array == null) {
            return "[/]";
        }
        len = Math.min(len, array.length - offset);
        final char lineEnd =
            (offset % NUM_BYTES_PER_LINE + len <= NUM_BYTES_PER_LINE) ?
            ' ' : '\n';
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        builder.append("offset=").append(offset).
            append(", len=").append(len).append(lineEnd);
        builder.append(bytesToHex(array, offset, len, lineEnd));
        builder.append("]");
        return builder.toString();
    }

    /**
     * Returns a string of hex bytes for a byte array.
     *
     * @param array the byte array
     * @param offset the offset to start
     * @param len the number of bytes
     *
     * @return the string
     */
    private static String bytesToHex(byte[] array,
                                     int offset,
                                     int len,
                                     char lineEnd) {
        return bytesToHex((i) -> array[i], offset, len, lineEnd);
    }

    private static String bytesToHex(Function<Integer, Byte> func,
                                     int offset,
                                     int len,
                                     char lineEnd) {
        final int nlines =
            (offset % NUM_BYTES_PER_LINE + len + NUM_BYTES_PER_LINE - 1) /
            NUM_BYTES_PER_LINE;
        final char[] hexArray = new char[nlines * LINE_WIDTH];
        Arrays.fill(hexArray, ' ');
        for (int i = 0; i < nlines; ++i) {
            hexArray[(i + 1) * LINE_WIDTH - 1] = lineEnd;
        }
        fillLineNumbers(hexArray, nlines, offset);
        fillLines(hexArray, func, offset, len);
        return new String(hexArray);
    }

    /**
     * Fills the line number chars of the hex array.
     */
    private static void fillLineNumbers(char[] hexArray,
                                        int nlines,
                                        int offset) {
        int curr = offset / NUM_BYTES_PER_LINE * NUM_BYTES_PER_LINE;
        for (int i = 0; i < nlines; ++i) {
            hexArray[i * LINE_WIDTH + 0] = '0';
            hexArray[i * LINE_WIDTH + 1] = 'x';
            int shift = curr;
            for (int j = 0; j < 8; j++) {
                hexArray[i * LINE_WIDTH + 1 + (8 - j)] =
                    Character.forDigit(shift & 0x0F, 16);
                shift = shift >>> 4;
            }
            hexArray[i * LINE_WIDTH + LINE_NUM_WIDTH - 2] = ':';
            hexArray[i * LINE_WIDTH + LINE_NUM_WIDTH - 1] = ' ';
            curr += NUM_BYTES_PER_LINE;
        }
    }

    /**
     * Fills the line chars of the hex array.
     */
    private static void fillLines(char[] hexArray,
                                  Function<Integer, Byte> func,
                                  int offset,
                                  int len) {
        final int padding = offset -
            offset / NUM_BYTES_PER_LINE * NUM_BYTES_PER_LINE;
        int idx = LINE_NUM_WIDTH + padding * BYTE_REPR_SEP_WIDTH;
        for (int i = 0; i < len; ++i) {
            final int v = func.apply(i + offset) & 0xFF;
            hexArray[idx + i * BYTE_REPR_SEP_WIDTH] =
                Character.toUpperCase(Character.forDigit(v >>> 4, 16));
            hexArray[idx + i * BYTE_REPR_SEP_WIDTH + 1] =
                Character.toUpperCase(Character.forDigit(v & 0x0F, 16));
            if ((offset + i + 1) % NUM_BYTES_PER_LINE == 0) {
                idx += LINE_NUM_WIDTH + BYTE_ASCII_SEPARATE_WIDTH +
                    NUM_BYTES_PER_LINE * ASCII_CHAR_WIDTH + 1;
            }
        }
        idx = LINE_NUM_WIDTH + BYTE_ASCII_SEPARATE_WIDTH +
            BYTE_REPR_SEP_WIDTH * NUM_BYTES_PER_LINE +
            padding * ASCII_CHAR_WIDTH ;
        for (int i = 0; i < len; ++i) {
            final int v = func.apply(i + offset) & 0xFF;
            final int val;
            if ((v <= 0x1F) || (v >= 0x7F)) {
                val = 0x2E;
            } else {
                val = v;
            }
            hexArray[idx + i * ASCII_CHAR_WIDTH] = (char) val;
            if ((offset + i + 1) % NUM_BYTES_PER_LINE == 0) {
                idx += LINE_NUM_WIDTH + BYTE_ASCII_SEPARATE_WIDTH +
                    NUM_BYTES_PER_LINE * BYTE_REPR_SEP_WIDTH + 1;
            }
        }
    }

    /**
     * Returns the hex string representation of a byte array.
     *
     * @param buf the byte buffer
     * @param len the number of bytes to show from position
     *
     * @return the string
     */
    public static String toString(@Nullable ByteBuffer buf, int len) {
        if (buf == null) {
            return "[/]";
        }
        final StringBuilder builder = new StringBuilder();
        final int pos = buf.position();
        final int lim = buf.limit();
        final int cap = buf.capacity();
        len = Math.min(len, lim - pos);
        final char lineEnd = (len < NUM_BYTES_PER_LINE) ? ' ' : '\n';
        builder.append("[").append("pos=").append(pos).
            append(",lim=").append(lim).
            append(",cap=").append(cap).append(lineEnd);
        builder.append(bytesToHex(buf, pos, len, lineEnd)).append(lineEnd);
        builder.append("]");
        return builder.toString();
    }

    /**
     * Returns a string of hex bytes for a byte buffer.
     *
     * @param buf the byte buffer
     * @param offset the offset to start
     * @param len the number of bytes
     *
     * @return the string
     */
    private static String bytesToHex(ByteBuffer buf,
                                     int offset,
                                     int len,
                                     char lineEnd) {
        return bytesToHex((i) -> buf.get(i), offset, len, lineEnd);
    }

    /**
     * Returns the string representation of an array of byte buffers.
     *
     * @param buffers the byte buffers
     * @param offset the offset of buffers
     * @param nbufs the number of buffers
     * @param len the number of bytes to show for each buf
     *
     * @return the string
     */
    public static String toString(ByteBuffer @Nullable[] buffers,
                                  int offset,
                                  int nbufs,
                                  int len) {
        if (buffers == null) {
            return "{/}";
        }
        final StringBuilder builder = new StringBuilder();
        builder.append("{");
        builder.append("byte buffer offset=").append(offset).append("\n");
        builder.append((offset > 0) ? "...\n" : "\n");
        final int end = Math.min(buffers.length, offset + nbufs);
        for (int i = offset; i < end; ++i) {
            builder.append(toString(buffers[i], len)).append("\n");
        }
        builder.append((end < buffers.length) ? "...\n" : "\n");
        builder.append("}");
        return builder.toString();
    }

    /**
     * Returns the string representation of a collection of byte buffers.
     *
     * @param bufs the byte buffers
     * @param offset the offset of buffers
     * @param nbufs the number of buffers
     * @param len the number of bytes to show for each buf
     *
     * @return the string
     */
    public static String toString(@Nullable Collection<ByteBuffer> bufs,
                                  int offset,
                                  int nbufs,
                                  int len) {
        if (bufs == null) {
            return "{/}";
        }
        int cnt0 = offset;
        int cnt1 = Math.min(bufs.size(), nbufs);
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        builder.append("byte buffer offset=").append(offset).append("\n");
        builder.append((offset > 0) ? "...\n" : "\n");
        for (ByteBuffer buf : bufs) {
            if ((--cnt0) > 0) {
                continue;
            }
            builder.append(toString(buf, len)).append("\n");
            if ((--cnt1) <= 0) {
                break;
            }
        }
        builder.append((offset + nbufs < bufs.size()) ? "...\n" : "\n");
        builder.append("}");
        return builder.toString();
    }
}

