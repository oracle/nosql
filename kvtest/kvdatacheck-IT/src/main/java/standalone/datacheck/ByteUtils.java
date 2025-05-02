/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

/** Utilities for converting byte arrays. */
class ByteUtils {

    /** 48-bit mask representing a six byte integer. */
    static final long SIX_BYTES_MASK = 0xffffffffffffL;

    /** 40-bit mask representing a five byte integer. */
    static final long FIVE_BYTES_MASK = 0xffffffffffL;

    /** Convert a long to an array of eight bytes. */
    static byte[] longToBytes(long l) {
        return new byte[] { (byte) (l >>> 56),
                            (byte) (l >>> 48),
                            (byte) (l >>> 40),
                            (byte) (l >>> 32),
                            (byte) (l >>> 24),
                            (byte) (l >>> 16),
                            (byte) (l >>> 8),
                            (byte) l };
    }

    /** Convert an array of eight bytes to a long. */
    static long bytesToLong(byte[] bytes) {
        assert bytes.length == 8;
        return ((long) bytes[0] << 56) +
            ((long) (bytes[1] & 0xff) << 48) +
            ((long) (bytes[2] & 0xff) << 40) +
            ((long) (bytes[3] & 0xff) << 32) +
            ((long) (bytes[4] & 0xff) << 24) +
            ((long) (bytes[5] & 0xff) << 16) +
            ((long) (bytes[6] & 0xff) << 8) +
            (bytes[7] & 0xff);
    }

    /** Convert a 48-bit long to an array of six bytes. */
    static byte[] longToSixBytes(long l) {
        if ((l & ~SIX_BYTES_MASK) != 0) {
            throw new IllegalArgumentException
                ("Argument is more than 48 bits: " + l);
        }
        return new byte[] { (byte) (l >>> 40),
                            (byte) (l >>> 32),
                            (byte) (l >>> 24),
                            (byte) (l >>> 16),
                            (byte) (l >>> 8),
                            (byte) l };
    }

    /** Convert an array of six bytes to a 48-bit long. */
    static long sixBytesToLong(byte[] bytes) {
        assert bytes.length == 6;
        return ((long) (bytes[0] & 0xff) << 40) +
            ((long) (bytes[1] & 0xff) << 32) +
            ((long) (bytes[2] & 0xff) << 24) +
            ((long) (bytes[3] & 0xff) << 16) +
            ((long) (bytes[4] & 0xff) << 8) +
            (bytes[5] & 0xff);
    }

    /** Convert a 40-bit long to an array of five bytes. */
    static byte[] longToFiveBytes(long l) {
        if ((l & ~FIVE_BYTES_MASK) != 0) {
            throw new IllegalArgumentException
                ("Argument is more than 40 bits: " + l);
        }
        return new byte[] { (byte) (l >>> 32),
                            (byte) (l >>> 24),
                            (byte) (l >>> 16),
                            (byte) (l >>> 8),
                            (byte) l };
    }

    /** Convert an array of five bytes to a 40-bit long. */
    static long fiveBytesToLong(byte[] bytes) {
        assert bytes.length == 5;
        return ((long) (bytes[0] & 0xff) << 32) +
            ((long) (bytes[1] & 0xff) << 24) +
            ((long) (bytes[2] & 0xff) << 16) +
            ((long) (bytes[3] & 0xff) << 8) +
            (bytes[4] & 0xff);
    }

    /** Convert an int to an array of four bytes. */
    static byte[] intToBytes(int i) {
        return new byte[] { (byte) (i >>> 24),
                            (byte) (i >>> 16),
                            (byte) (i >>> 8),
                            (byte) i };
    }

    /** Convert an array of four bytes to an int. */
    static int bytesToInt(byte[] bytes) {
        assert bytes.length == 4;
        return (bytes[0] << 24) +
            ((bytes[1] & 0xff) << 16) +
            ((bytes[2] & 0xff) << 8) +
            (bytes[3] & 0xff);
    }

    /** Convert a short to an array of two bytes. */
    static byte[] shortToBytes(short s) {
        return new byte[] { (byte) (s >>> 8), (byte) s };
    }

    /** Convert an array of two bytes to a short. */
    static short bytesToShort(byte[] bytes) {
        assert bytes.length == 2;
        return (short) ((bytes[0] << 8) + (bytes[1] & 0xff));
    }
}
