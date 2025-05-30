/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle Berkeley
 * DB Java Edition made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/berkeleydb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle Berkeley DB Java Edition for a copy of the
 * license and additional information.
 */

package oracle.nosql.proxy.protocol;

/**
 * NOTE: this is cut/paste/edited from JE's class of the same name. An
 * important difference is that the packed values returned are ALWAYS
 * sorted. The unsorted methods supported by JE for compatibility are not
 * included.
 *
 * Static methods for reading and writing packed integers.
 */
public class PackedInteger {

    /**
     * The maximum number of bytes needed to store an int value (5).
     */
    public static final int MAX_LENGTH = 5;

    /**
     * The maximum number of bytes needed to store a long value (9).
     */
    public static final int MAX_LONG_LENGTH = 9;

    /**
     * Reads a sorted packed integer at the given buffer offset and returns it.
     *
     * @param buf the buffer to read from.
     *
     * @param off the offset in the buffer at which to start reading.
     *
     * @return the integer that was read.
     */
    public static int readSortedInt(byte[] buf, int off) {

        int byteLen;
        boolean negative;

        /* The first byte of the buf stores the length of the value part. */
        int b1 = buf[off++] & 0xff;
        /* Adjust the byteLen to the real length of the value part. */
        if (b1 < 0x08) {
            byteLen = 0x08 - b1;
            negative = true;
        } else if (b1 > 0xf7) {
            byteLen = b1 - 0xf7;
            negative = false;
        } else {
            return b1 - 127;
        }

        /*
         * The following bytes on the buf store the value as a big endian
         * integer. We extract the significant bytes from the buf and put them
         * into the value in big endian order.
         */
        int value;
        if (negative) {
            value = 0xFFFFFFFF;
        } else {
            value = 0;
        }
        if (byteLen > 3) {
            value = (value << 8) | (buf[off++] & 0xFF);
        }
        if (byteLen > 2) {
            value = (value << 8) | (buf[off++] & 0xFF);
        }
        if (byteLen > 1) {
            value = (value << 8) | (buf[off++] & 0xFF);
        }
        value = (value << 8) | (buf[off++] & 0xFF);

        /*
         * After get the adjusted value, we have to adjust it back to the
         * original value.
         */
        if (negative) {
            value -= 119;
        } else {
            value += 121;
        }
        return value;
    }

    /**
     * Reads a sorted packed long integer at the given buffer offset and
     * returns it.
     *
     * @param buf the buffer to read from.
     *
     * @param off the offset in the buffer at which to start reading.
     *
     * @return the long integer that was read.
     */
    public static long readSortedLong(byte[] buf, int off) {

        int byteLen;
        boolean negative;

        /* The first byte of the buf stores the length of the value part. */
        int b1 = buf[off++] & 0xff;
        /* Adjust the byteLen to the real length of the value part. */
        if (b1 < 0x08) {
            byteLen = 0x08 - b1;
            negative = true;
        } else if (b1 > 0xf7) {
            byteLen = b1 - 0xf7;
            negative = false;
        } else {
            return b1 - 127;
        }

        /*
         * The following bytes on the buf store the value as a big endian
         * integer. We extract the significant bytes from the buf and put them
         * into the value in big endian order.
         */
        long value;
        if (negative) {
            value = 0xFFFFFFFFFFFFFFFFL;
        } else {
            value = 0;
        }
        if (byteLen > 7) {
            value = (value << 8) | (buf[off++] & 0xFF);
        }
        if (byteLen > 6) {
            value = (value << 8) | (buf[off++] & 0xFF);
        }
        if (byteLen > 5) {
            value = (value << 8) | (buf[off++] & 0xFF);
        }
        if (byteLen > 4) {
            value = (value << 8) | (buf[off++] & 0xFF);
        }
        if (byteLen > 3) {
            value = (value << 8) | (buf[off++] & 0xFF);
        }
        if (byteLen > 2) {
            value = (value << 8) | (buf[off++] & 0xFF);
        }
        if (byteLen > 1) {
            value = (value << 8) | (buf[off++] & 0xFF);
        }
        value = (value << 8) | (buf[off++] & 0xFF);

        /*
         * After obtaining the adjusted value, we have to adjust it back to the
         * original value.
         */
        if (negative) {
            value -= 119;
        } else {
            value += 121;
        }
        return value;
    }

    /**
     * Returns the number of bytes that would be read by {@link
     * #readSortedInt}.
     *
     * <p>Because the length is stored in the first byte, this method may be
     * called with only the first byte of the packed integer in the given
     * buffer.  This method only accesses one byte at the given offset.</p>
     *
     * @param buf the buffer to read from.
     *
     * @param off the offset in the buffer at which to start reading.
     *
     * @return the number of bytes that would be read.
     */
    public static int getReadSortedIntLength(byte[] buf, int off) {

        /* The first byte of the buf stores the length of the value part. */
        int b1 = buf[off] & 0xff;
        if (b1 < 0x08) {
            return 1 + 0x08 - b1;
        }
        if (b1 > 0xf7) {
            return 1 + b1 - 0xf7;
        }
        return 1;
    }

    /**
     * Returns the number of bytes that would be read by {@link
     * #readSortedLong}.
     *
     * <p>Because the length is stored in the first byte, this method may be
     * called with only the first byte of the packed integer in the given
     * buffer.  This method only accesses one byte at the given offset.</p>
     *
     * @param buf the buffer to read from.
     *
     * @param off the offset in the buffer at which to start reading.
     *
     * @return the number of bytes that would be read.
     */
    public static int getReadSortedLongLength(byte[] buf, int off) {

        /* The length is stored in the same way for int and long. */
        return getReadSortedIntLength(buf, off);
    }

    /**
     * Writes a packed sorted integer starting at the given buffer offset and
     * returns the next offset to be written.
     *
     * @param buf the buffer to write to.
     *
     * @param offset the offset in the buffer at which to start writing.
     *
     * @param value the integer to be written.
     *
     * @return the offset past the bytes written.
     */
    public static int writeSortedInt(byte[] buf, int offset, int value) {

        /*
         * Values in the inclusive range [-119,120] are stored in a single
         * byte. For values outside that range, the first byte stores the
         * number of additional bytes. The additional bytes store
         * (value + 119 for negative and value - 121 for positive) as an
         * unsigned big endian integer.
         */
        int byte1Off = offset;
        offset++;
        if (value < -119) {

            /*
             * If the value < -119, then first adjust the value by adding 119.
             * Then the adjusted value is stored as an unsigned big endian
             * integer.
             */
            value += 119;

            /*
             * Store the adjusted value as an unsigned big endian integer.
             * For an negative integer, from left to right, the first
             * significant byte is the byte which is not equal to 0xFF. Also
             * please note that, because the adjusted value is stored in big
             * endian integer, we extract the significant byte from left to
             * right.
             *
             * In the left to right order, if the first byte of the adjusted
             * value is a significant byte, it will be stored in the 2nd byte
             * of the buf. Then we will look at the 2nd byte of the adjusted
             * value to see if this byte is the significant byte, if yes, this
             * byte will be stored in the 3rd byte of the buf, and the like.
             */
            if ((value | 0x00FFFFFF) != 0xFFFFFFFF) {
                buf[offset++] = (byte) (value >> 24);
            }
            if ((value | 0x0000FFFF) != 0xFFFFFFFF) {
                buf[offset++] = (byte) (value >> 16);
            }
            if ((value | 0x000000FF) != 0xFFFFFFFF) {
                buf[offset++] = (byte) (value >> 8);
            }
            buf[offset++] = (byte) value;

            /*
             * valueLen is the length of the value part stored in buf. Because
             * the first byte of buf is used to stored the length, we need
             * to subtract one.
             */
            int valueLen = offset - byte1Off - 1;

            /*
             * The first byte stores the number of additional bytes. Here we
             * store the result of 0x08 - valueLen, rather than directly store
             * valueLen. The reason is to implement natural sort order for
             * byte-by-byte comparison.
             */
            buf[byte1Off] = (byte) (0x08 - valueLen);
        } else if (value > 120) {

            /*
             * If the value > 120, then first adjust the value by subtracting
             * 121. Then the adjusted value is stored as an unsigned big endian
             * integer.
             */
            value -= 121;

            /*
             * Store the adjusted value as an unsigned big endian integer.
             * For a positive integer, from left to right, the first
             * significant byte is the byte which is not equal to 0x00.
             *
             * In the left to right order, if the first byte of the adjusted
             * value is a significant byte, it will be stored in the 2nd byte
             * of the buf. Then we will look at the 2nd byte of the adjusted
             * value to see if this byte is the significant byte, if yes, this
             * byte will be stored in the 3rd byte of the buf, and the like.
             */
            if ((value & 0xFF000000) != 0) {
                buf[offset++] = (byte) (value >> 24);
            }
            if ((value & 0xFFFF0000) != 0) {
                buf[offset++] = (byte) (value >> 16);
            }
            if ((value & 0xFFFFFF00) != 0) {
                buf[offset++] = (byte) (value >> 8);
            }
            buf[offset++] = (byte) value;

            /*
             * valueLen is the length of the value part stored in buf. Because
             * the first byte of buf is used to stored the length, we need to
             * subtract one.
             */
            int valueLen = offset - byte1Off - 1;

            /*
             * The first byte stores the number of additional bytes. Here we
             * store the result of 0xF7 + valueLen, rather than directly store
             * valueLen. The reason is to implement natural sort order for
             * byte-by-byte comparison.
             */
            buf[byte1Off] = (byte) (0xF7 + valueLen);
        } else {

            /*
             * If -119 <= value <= 120, only one byte is needed to store the
             * value. The stored value is the original value plus 127.
             */
            buf[byte1Off] = (byte) (value + 127);
        }
        return offset;
    }

    /**
     * Writes a packed sorted long integer starting at the given buffer offset
     * and returns the next offset to be written.
     *
     * @param buf the buffer to write to.
     *
     * @param offset the offset in the buffer at which to start writing.
     *
     * @param value the long integer to be written.
     *
     * @return the offset past the bytes written.
     */
    public static int writeSortedLong(byte[] buf, int offset, long value) {

        /*
         * Values in the inclusive range [-119,120] are stored in a single
         * byte. For values outside that range, the first byte stores the
         * number of additional bytes. The additional bytes store
         * (value + 119 for negative and value - 121 for positive) as an
         * unsigned big endian integer.
         */
        int byte1Off = offset;
        offset++;
        if (value < -119) {

            /*
             * If the value < -119, then first adjust the value by adding 119.
             * Then the adjusted value is stored as an unsigned big endian
             * integer.
             */
            value += 119;

            /*
             * Store the adjusted value as an unsigned big endian integer.
             * For an negative integer, from left to right, the first
             * significant byte is the byte which is not equal to 0xFF. Also
             * please note that, because the adjusted value is stored in big
             * endian integer, we extract the significant byte from left to
             * right.
             *
             * In the left to right order, if the first byte of the adjusted
             * value is a significant byte, it will be stored in the 2nd byte
             * of the buf. Then we will look at the 2nd byte of the adjusted
             * value to see if this byte is the significant byte, if yes, this
             * byte will be stored in the 3rd byte of the buf, and the like.
             */
            if ((value | 0x00FFFFFFFFFFFFFFL) != 0xFFFFFFFFFFFFFFFFL) {
                buf[offset++] = (byte) (value >> 56);
            }
            if ((value | 0x0000FFFFFFFFFFFFL) != 0xFFFFFFFFFFFFFFFFL) {
                buf[offset++] = (byte) (value >> 48);
            }
            if ((value | 0x000000FFFFFFFFFFL) != 0xFFFFFFFFFFFFFFFFL) {
                buf[offset++] = (byte) (value >> 40);
            }
            if ((value | 0x00000000FFFFFFFFL) != 0xFFFFFFFFFFFFFFFFL) {
                buf[offset++] = (byte) (value >> 32);
            }
            if ((value | 0x0000000000FFFFFFL) != 0xFFFFFFFFFFFFFFFFL) {
                buf[offset++] = (byte) (value >> 24);
            }
            if ((value | 0x000000000000FFFFL) != 0xFFFFFFFFFFFFFFFFL) {
                buf[offset++] = (byte) (value >> 16);
            }
            if ((value | 0x00000000000000FFL) != 0xFFFFFFFFFFFFFFFFL) {
                buf[offset++] = (byte) (value >> 8);
            }
            buf[offset++] = (byte) value;

            /*
             * valueLen is the length of the value part stored in buf. Because
             * the first byte of buf is used to stored the length, so we need
             * to minus one.
             */
            int valueLen = offset - byte1Off - 1;

            /*
             * The first byte stores the number of additional bytes. Here we
             * store the result of 0x08 - valueLen, rather than directly store
             * valueLen. The reason is to implement nature sort order for
             * byte-by-byte comparison.
             */
            buf[byte1Off] = (byte) (0x08 - valueLen);
        } else if (value > 120) {

            /*
             * If the value > 120, then first adjust the value by subtracting
             * 119. Then the adjusted value is stored as an unsigned big endian
             * integer.
             */
            value -= 121;

            /*
             * Store the adjusted value as an unsigned big endian integer.
             * For a positive integer, from left to right, the first
             * significant byte is the byte which is not equal to 0x00.
             *
             * In the left to right order, if the first byte of the adjusted
             * value is a significant byte, it will be stored in the 2nd byte
             * of the buf. Then we will look at the 2nd byte of the adjusted
             * value to see if this byte is the significant byte, if yes, this
             * byte will be stored in the 3rd byte of the buf, and the like.
             */
            if ((value & 0xFF00000000000000L) != 0L) {
                buf[offset++] = (byte) (value >> 56);
            }
            if ((value & 0xFFFF000000000000L) != 0L) {
                buf[offset++] = (byte) (value >> 48);
            }
            if ((value & 0xFFFFFF0000000000L) != 0L) {
                buf[offset++] = (byte) (value >> 40);
            }
            if ((value & 0xFFFFFFFF00000000L) != 0L) {
                buf[offset++] = (byte) (value >> 32);
            }
            if ((value & 0xFFFFFFFFFF000000L) != 0L) {
                buf[offset++] = (byte) (value >> 24);
            }
            if ((value & 0xFFFFFFFFFFFF0000L) != 0L) {
                buf[offset++] = (byte) (value >> 16);
            }
            if ((value & 0xFFFFFFFFFFFFFF00L) != 0L) {
                buf[offset++] = (byte) (value >> 8);
            }
            buf[offset++] = (byte) value;

            /*
             * valueLen is the length of the value part stored in buf. Because
             * the first byte of buf is used to stored the length, so we need
             * to minus one.
             */
            int valueLen = offset - byte1Off - 1;

            /*
             * The first byte stores the number of additional bytes. Here we
             * store the result of 0xF7 + valueLen, rather than directly store
             * valueLen. The reason is to implement nature sort order for
             * byte-by-byte comparison.
             */
            buf[byte1Off] = (byte) (0xF7 + valueLen);
        } else {

            /*
             * If -119 <= value <= 120, only one byte is needed to store the
             * value. The stored value is the original value adds 127.
             */
            buf[byte1Off] = (byte) (value + 127);
        }
        return offset;
    }

    /**
     * Returns the number of bytes that would be written by {@link
     * #writeSortedInt}.
     *
     * @param value the integer to be written.
     *
     * @return the number of bytes that would be used to write the given
     * integer.
     */
    public static int getWriteSortedIntLength(int value) {

        if (value < -119) {
            /* Adjust the value. */
            value += 119;

            /*
             * Find the left most significant byte of the adjusted value, and
             * return the length accordingly.
             */
            if ((value | 0x000000FF) == 0xFFFFFFFF) {
                return 2;
            }
            if ((value | 0x0000FFFF) == 0xFFFFFFFF) {
                return 3;
            }
            if ((value | 0x00FFFFFF) == 0xFFFFFFFF) {
                return 4;
            }
        } else if (value > 120) {
            /* Adjust the value. */
            value -= 121;

            /*
             * Find the left most significant byte of the adjusted value, and
             * return the length accordingly.
             */
            if ((value & 0xFFFFFF00) == 0) {
                return 2;
            }
            if ((value & 0xFFFF0000) == 0) {
                return 3;
            }
            if ((value & 0xFF000000) == 0) {
                return 4;
            }
        } else {

            /*
             * If -119 <= value <= 120, only one byte is needed to store the
             * value.
             */
            return 1;
        }
        return 5;
    }

    /**
     * Returns the number of bytes that would be written by {@link
     * #writeSortedLong}.
     *
     * @param value the long integer to be written.
     *
     * @return the number of bytes that would be used to write the given long
     * integer.
     */
    public static int getWriteSortedLongLength(long value) {

        if (value < -119) {
            /* Adjust the value. */
            value += 119;

            /*
             * Find the left most significant byte of the adjusted value, and
             * return the length accordingly.
             */
            if ((value | 0x00000000000000FFL) == 0xFFFFFFFFFFFFFFFFL) {
                return 2;
            }
            if ((value | 0x000000000000FFFFL) == 0xFFFFFFFFFFFFFFFFL) {
                return 3;
            }
            if ((value | 0x0000000000FFFFFFL) == 0xFFFFFFFFFFFFFFFFL) {
                return 4;
            }
            if ((value | 0x00000000FFFFFFFFL) == 0xFFFFFFFFFFFFFFFFL) {
                return 5;
            }
            if ((value | 0x000000FFFFFFFFFFL) == 0xFFFFFFFFFFFFFFFFL) {
                return 6;
            }
            if ((value | 0x0000FFFFFFFFFFFFL) == 0xFFFFFFFFFFFFFFFFL) {
                return 7;
            }
            if ((value | 0x00FFFFFFFFFFFFFFL) == 0xFFFFFFFFFFFFFFFFL) {
                return 8;
            }
        } else if (value > 120) {
            /* Adjust the value. */
            value -= 121;

            /*
             * Find the left most significant byte of the adjusted value, and
             * return the length accordingly.
             */
            if ((value & 0xFFFFFFFFFFFFFF00L) == 0L) {
                return 2;
            }
            if ((value & 0xFFFFFFFFFFFF0000L) == 0L) {
                return 3;
            }
            if ((value & 0xFFFFFFFFFF000000L) == 0L) {
                return 4;
            }
            if ((value & 0xFFFFFFFF00000000L) == 0L) {
                return 5;
            }
            if ((value & 0xFFFFFF0000000000L) == 0L) {
                return 6;
            }
            if ((value & 0xFFFF000000000000L) == 0L) {
                return 7;
            }
            if ((value & 0xFF00000000000000L) == 0L) {
                return 8;
            }
        } else {

            /*
             * If -119 <= value <= 120, only one byte is needed to store the
             * value.
             */
            return 1;
        }
        return 9;
    }
}
