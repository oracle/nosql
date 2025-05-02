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

package oracle.kv.impl.util;

import java.util.BitSet;

/**
 * A utility class used to convert numeric types into a String format that
 * sorts correctly based on the original numeric format.  It works on 32-bit
 * integers (int), 32-bit floating point (float), 64-bit integers (long), and
 * 64-bit double (double).  The format is an encoded representation of the bit
 * value of the number.  It accounts for positive and negative numbers by
 * flipping the sign bit to cause negative numbers to sort smaller than
 * positive.
 * <p>
 * A base 128 encoding of the bits is used to compresses the string size from
 * one character per bit to one character per 7 bits.  Integer and Long share
 * some basic encoding/decoding code, which works on long.  They differ in
 * how bits are masked and in dealing with the sign bit.  Using this encoding
 * an integer takes no more than 5 characters and long takes no more than 10,
 * but both could be representing in as little as one character.
 * <p>
 * Originally the algorithm used base 64, which added a byte to each of the
 * maximum lengths.  Use 128 results in non-printable characters in the
 * encoding but as long as the encoded values do not need to be printed that
 * is fine.  Base 64 characters are printable but not particularly readable.
 * The vestiges of base 64 are kept here in case that version is ever wanted
 * as an option.
 * <p>
 * In order to use a smaller representation the caller needs to specify the
 * number of characters to use in the <code>stringLen</code>
 * argument to {@link #toSortable(int, int)} and
 * {@link #toSortable(long, int)}.  The length for a given value can be found
 * by calling {@link #encodingLength(Integer)} or {@link #encodingLength(Long)}
 * for a single value.  {@link #encodingLength(Integer, Integer)} or
 * {@link #encodingLength(Long, Long)} can be used for ranges.
 * <p>
 * Using constrained strings can bring the required storage down to a single
 * character in a String.
 */
public class SortableString {

    /**
     * Lookup table for bytes to base 64 characters.
     *
     * Base 64, using +, - for the first 2 values, then 0-9, A-Z, and
     * a-z.  This is not a true Base64 encoding, which will not sort
     * correctly as a string.  It just uses 64 printable characters, in
     * increasing order of byte value.  Characters that might be part of
     * the URI encoding of a Key (e.g. '/') are avoided.
     */
    private final static char[] BASE_64_DIGITS = {
        '+', '-',
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
        'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
        'U', 'V', 'W', 'X', 'Y', 'Z',
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
        'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
        'u', 'v', 'w', 'x', 'y', 'z'
    };

    /**
     * Lookup table for base 64 characters to bytes
     *
     * Initialize the byte lookup array for converting base 64
     * characters back to bit values.  This array is sparse.  The only
     * significant values are those indexed by the digits above.
     */
    private final static byte[] LOOKUP_BYTE_64 = new byte['z' + 1];
    static {
        for (int i = 0; i < BASE_64_DIGITS.length; i++) {
            LOOKUP_BYTE_64[BASE_64_DIGITS[i]] = (byte) i;
        }
    }

    /**
     * Lookup table for base 128.  Don't worry about using printable
     * characters, although starting at 32 will make many of them printable.
     */
    private final static int BASE_128_DIGITS_OFFSET = 48;
    private final static char[] BASE_128_DIGITS = new char[128];
    private final static short[] LOOKUP_BYTE_128 = new short[256];

    static {
        for (int i = 0; i < BASE_128_DIGITS.length; i++) {
            BASE_128_DIGITS[i] = (char) (i + BASE_128_DIGITS_OFFSET);
            LOOKUP_BYTE_128[(short)BASE_128_DIGITS[i]] = (short) i;
        }
    }

    /**
     * Masks used to zero out unnecessary bits when encoding an integer
     * into fewer than 6 characters.  It only exists to handle negative
     * numbers, which are sign extended.  It's used to both mask off those bits
     * and to replace them on conversion back to integer.
     *
     * The mask for base 64 increases with a 6-bit shift while the base 128
     * values increase with a 7-bit shift.
     *
     * The last entry in each array is a bit assymetrical because 32 does not
     * divide evenly by 6 or 7.
     */
    @SuppressWarnings("unused")
	private final static int[] INTEGER_MASK_64 = {
        0xffffffff, /* default -- 6 characters */
        0x3f,       /* 1 character */
        0xfff,      /* 2 characters */
        0x3ffff,    /* 3 characters */
        0xffffff,   /* 4 characters */
        0x3fffffff, /* 5 characters */
        0xffffffff  /* 6 characters */
    };

    private final static int[] INTEGER_MASK_128 = {
        0xffffffff, /* default -- 5 characters */
        0x7f,       /* 1 character */
        0x3fff,     /* 2 characters */
        0x1fffff,   /* 3 characters */
        0xfffffff,  /* 4 characters */
        0xffffffff  /* 5 characters */
    };

    /**
     * These bit values are used to represent the sign bit for strings of
     * various lengths.  The important property is that this bits must not
     * overlap with any bits of significance in the numbers themselves.
     */
    @SuppressWarnings("unused")
	private final static int[] INTEGER_SIGN_BIT_64 = {
        0x80000000, /* default -- 6 characters */
        0x20,       /* 1 character */
        0x800,      /* 2 characters */
        0x20000,    /* 3 characters */
        0x800000,   /* 4 characters */
        0x20000000, /* 5 characters */
        0x80000000  /* 6 characters */
    };

    private final static int[] INTEGER_SIGN_BIT_128 = {
        0x80000000, /* default -- 5 characters */
        0x40,       /* 1 character */
        0x2000,     /* 2 characters */
        0x100000,   /* 3 characters */
        0x8000000,  /* 4 characters */
        0x80000000  /* 5 characters */
    };

    /**
     * Masks and sign bits for long vs integer.  The last entry in each array
     * is a bit assymetrical because 64 does not divide evenly by 6 or 7.  See
     * comments above for INTEGER_MASK* about the purpose.
     */
    @SuppressWarnings("unused")
	private final static long[] LONG_MASK_64 = {
        0xffffffffffffffffL, /* default */
        0x3fL,                /* 1 character */
        0xfffL,               /* 2 */
        0x3ffffL,             /* 3 */
        0xffffffL,            /* 4 */
        0x3fffffffL,          /* 5 */
        0xfffffffffL,         /* 6 */
        0x3ffffffffffL,       /* 7 */
        0xffffffffffffL,      /* 8 */
        0x3fffffffffffffL,    /* 9 */
        0xfffffffffffffffL,   /* 10 */
        0xffffffffffffffffL   /* 11 */
    };

    private final static long[] LONG_MASK_128 = {
        0xffffffffffffffffL, /* default */
        0x7f,                /* 1 character */
        0x3fff,              /* 2 */
        0x1fffff,            /* 3 */
        0xfffffff,           /* 4 */
        0xffffffff,          /* 5 */
        0x3ffffffffffL,      /* 6 */
        0x1ffffffffffffL,    /* 7 */
        0xffffffffffffffL,   /* 8 */
        0x7fffffffffffffffL, /* 9 */
        0xffffffffffffffffL  /* 10 */
    };

    @SuppressWarnings("unused")
	private final static long[] LONG_SIGN_BIT_64 = {
        0x8000000000000000L, /* default */
        0x20L,
        0x800L,
        0x20000L,
        0x800000L,
        0x20000000L,
        0x800000000L,
        0x20000000000L,
        0x800000000000L,
        0x20000000000000L,
        0x800000000000000L,
        0x8000000000000000L
    };

    private final static long[] LONG_SIGN_BIT_128 = {
        0x8000000000000000L, /* default */
        0x40L,
        0x2000L,
        0x100000L,
        0x8000000L,
        0x400000000L,
        0x20000000000L,
        0x1000000000000L,
        0x80000000000000L,
        0x4000000000000000L,
        0x8000000000000000L
    };

    /*
     * These are the sign bits for full-length long and integer values.
     */
    private static final long SIGN_BIT = 0x8000000000000000L;
    private static final int SIGN_BIT_32 = 0x80000000;

    /*
     * Masks for 64 and 128 bit single character values
     */
	@SuppressWarnings("unused")
	private static final int MASK_64 = 0x3f;
    private static final int MASK_128 = 0x7f;

    /*
     * Each Base64 character represents 6 bits of information.
     */
    @SuppressWarnings("unused")
	private static final int BASE_64_SHIFT = 6;

    /*
     * Each Base128 character represents 7 bits of information.
     */
    private static final int BASE_128_SHIFT = 7;

    /*
     * Base64 string to represent 64-bits (long, double) is 11 bytes long.
     */
    @SuppressWarnings("unused")
	private static final int LONG_STRING_LEN_64 = 11;

    /*
     * Base128 string to represent 64-bits (long, double) is 10 bytes long.
     */
    private static final int LONG_STRING_LEN_128 = 10;

    /*
     * Base64 string to represent 32-bits is 6 bytes long.  This is used
     * for float as well as int.
     */
    @SuppressWarnings("unused")
	private static final int INT_STRING_LEN_64 = 6;

    /*
     * Base128 string to represent 32-bits is 5 bytes long.  This is used
     * for float as well as int.
     */
    private static final int INT_STRING_LEN_128 = 5;

    /*
     * Utility method to convert a long to a Base128 string.  The sign bit is
     * handled in the caller before calling this method.
     */
    private static String toString(long l, int stringLen) {
        final char[] buf = new char[stringLen];
        do {
            buf[--stringLen] = BASE_128_DIGITS[(int) (l & MASK_128)];
            l >>>= BASE_128_SHIFT;
        } while (stringLen > 0);
        return new String(buf);
    }

    /*
     * Utility method to convert a Base128 string to a long.  The sign bit is
     * handled in the caller on return.  This used to use a char[] but it is
     * more efficient to leave the String intact and just extract the char
     * members one at a time.
     */
    private static long fromString(final String s) {
        long out = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < BASE_128_DIGITS_OFFSET ||
                c > LOOKUP_BYTE_128.length + BASE_128_DIGITS_OFFSET - 1) {
                throw new IllegalArgumentException(
                    "Unexpected character found: " + c);
            }
            out <<= BASE_128_SHIFT;
            out ^= LOOKUP_BYTE_128[(short)c];
        }
        return out;
    }

    /*
     * The public interface
     */

    /**
     * Returns a string s for long l such that for any long l' where l &lt; l',
     * s.compareTo(toSortable(l')) &lt; 0.
     *
     * @param l the value to represent as a sortable String.
     *
     * @return the sortable string representation of the input value.
     */
    public static String toSortable(final long l) {
        /*
         * Toggle the sign bit and dump out as Base128 string
         */
        return toString(l ^ SIGN_BIT, LONG_STRING_LEN_128);
    }

    /**
     * Returns a string s for long l such that for any long l' where l &lt; l',
     * s.compareTo(toSortable(l')) &lt; 0, using <code>stringLen</code>
     * characters for the encoding.
     *
     * @param l the value to represent as a sortable String.  0 means use the
     * full length string encoding.
     *
     * @return the sortable string representation of the input value.
     * <p>
     * The value for the input parameter <code>l</code> must fit into the
     * expected length.  This is not validated in this function.  Use {@link
     * #encodingLength(Long)} to determine the appropriate length for a long.
     * <p>
     * If a number of string encodings are to be compared to one another they
     * must all use the same <code>stringLen</code>.  To do this calculate the
     * maximum length for any value in the set of values and use that length
     * for all.
     */
    public static String toSortable(final long l, final int stringLen) {
        return toString((l & LONG_MASK_128[stringLen]) ^ LONG_SIGN_BIT_128[stringLen],
                        stringLen != 0 ? stringLen : LONG_STRING_LEN_128 );
    }

    /**
     * Returns a long such that longFromSortable(toSortable(l)) == l.
     *
     * @param s the string to convert back to long.
     *
     * @return the long value encoded by the input string.
     */
    public static long longFromSortable(final String s) {
        if (s.length() > LONG_STRING_LEN_128) {
            throw new IllegalArgumentException(
                "Invalid sortable string, length " + s.length());
        }

        long out = fromString(s);

        /*
         * If the sign bit is 0 the number was negative in the first place and
         * needs to be sign extended.  Do this by OR'ing with the complement of
         * the mask for the string length.  After that the sign bit is flipped
         * with an XOR of the bit mask.
         */
        long mask = LONG_MASK_128[s.length()];
        long signBit = LONG_SIGN_BIT_128[s.length()];
        if ((out & signBit) == 0) {
            out |= ~mask;
        }
        return (out ^ signBit);
    }

    /**
     * Returns a string s for double d such that for any double d'
     * where d &lt; d', s.compareTo(toSortable(d')) &lt; 0.
     *
     * @param d the value to represent as a sortable String.
     *
     * @return the sortable string representation of the input value.
     */
    public static String toSortable(final double d) {
        long tmp = Double.doubleToRawLongBits(d);
        return toString((tmp < 0) ? ~tmp : (tmp ^ SIGN_BIT),
                        LONG_STRING_LEN_128);
    }

    /**
     * Returns a double such that doubleFromSortable(toSortable(d)) == d.
     *
     * @param s the string to convert back to double.
     *
     * @return the double value encoded by the input string.
     */
    public static double doubleFromSortable(final String s) {
        long tmp = fromString(s);
        tmp = (tmp < 0) ? (tmp ^ SIGN_BIT) : ~tmp;
        return Double.longBitsToDouble(tmp);
    }

    /**
     * Returns a string s for float f such that for any float f'
     * where f &lt; f', s.compareTo(toSortable(f')) &lt; 0.
     *
     * @param f the value to represent as a sortable String.
     *
     * @return the sortable string representation of the input value.
     */
    public static String toSortable(final float f) {
        int tmp = Float.floatToRawIntBits(f);
        return toString((tmp < 0) ? ~tmp : (tmp ^ SIGN_BIT_32),
                        INT_STRING_LEN_128);
    }

    /**
     * Returns a float such that floatFromSortable(toSortable(f)) == f.
     *
     * @param s the string to convert back to float.
     *
     * @return the float value encoded by the input string.
     */
    public static float floatFromSortable(final String s) {
        int tmp = (int) fromString(s);
        tmp = (tmp < 0) ? (tmp ^ SIGN_BIT_32) : ~tmp;
        return Float.intBitsToFloat(tmp);
    }

    /**
     * Returns a string s for int i such that for any int i' where i &lt; i',
     * s.compareTo(toSortable(i')) &lt; 0.  This version uses the full 6
     * characters to store any integer.
     *
     * @param i the value to represent as a sortable String.
     *
     * @return the sortable string representation of the input value.
     */
    public static String toSortable(final int i) {
        /*
         * Toggle the sign bit and dump out as Base128 string
         */
        return toString(i ^ SIGN_BIT_32, INT_STRING_LEN_128);
    }

    /**
     * Returns a string s for int i such that for any int i' where i &lt; i',
     * s.compareTo(toSortable(i')) &lt; 0, using <code>stringLen</code>
     * characters for the encoding.
     *
     * @param i the value to represent as a sortable String.  0 means use the
     * full length string encoding.
     *
     * @return the sortable string representation of the input value.
     * <p>
     * The value for the input integer <code>i</code> must fit into the
     * expected length.  This is not validated in this function.  Use {@link
     * #encodingLength} to determine the appropriate length for an integer.
     * <p>
     * If a number of string encodings are to be compared to one another they
     * must all use the same <code>stringLen</code>.  To do this calculate the
     * maximum length for any value in the set of integers and use that length
     * for all.
     */
    public static String toSortable(final int i, final int stringLen) {
        /*
         * Mask the value, toggle the sign bit and dump out as Base128 string
         */
        return toString((i & INTEGER_MASK_128[stringLen]) ^
                        INTEGER_SIGN_BIT_128[stringLen],
                        stringLen != 0 ? stringLen : INT_STRING_LEN_128 );
    }

    /**
     * Returns an int such that intFromSortable(toSortable(i)) == i.
     *
     * @param s the string to convert back to int.
     *
     * @return the int value encoded by the input string.
     */
    public static int intFromSortable(final String s) {
        /*
         * Get the int representation
         */
        int out = (int) fromString(s);

        /*
         * If the sign bit is 0 the number was negative in the first place and
         * needs to be sign extended.  Do this by OR'ing with the complement of
         * the mask for the byte length.  After that the sign bit is flipped
         * with an XOR of the bit mask.
         */
        int mask = INTEGER_MASK_128[s.length()];
        int signBit = INTEGER_SIGN_BIT_128[s.length()];
        if ((out & signBit) == 0) {
            out |= ~mask;
        }
        return (out ^ signBit);
    }

   /**
    * Returns a sortable string for a byte array using 128 based encoding, use
    * 1 char for every 7 bits.
    *
    * @param bytes the value to represent as a sortable String.
    *
    * @return the sortable string representation of the input value.
    */
   public static String toSortable(final byte[] bytes) {
       final int nBits = bytes.length * 8;
       final int stringLen = (nBits + BASE_128_SHIFT - 1) / BASE_128_SHIFT;
       final char[] buf = new char[stringLen];
       final BitSet bs = new BitSet(nBits);
       int bitIdx = 0;
       for (int i = 0; i < bytes.length; i++) {
           final byte b = bytes[i];
           for (int j = 0; j < 8; j++, bitIdx++) {
               bs.set(bitIdx, (b & (1 << (7 - j))) > 0);
           }
       }

       int bufIdx = 0;
       for (int i = 0; i < bs.length(); bufIdx++) {
           byte b = 0;
           for (int j = 0; j < 7; j++) {
               if (bs.get(i++)) {
                   b |= (byte) (1 << 6 - j);
               }
           }
           buf[bufIdx] = BASE_128_DIGITS[(b & MASK_128)];
       }
       while (bufIdx < buf.length) {
           buf[bufIdx++] = BASE_128_DIGITS[0];
       }
       return new String(buf);
   }

   /**
    * Decodes a byte array from a sortable string.
    *
    * @param s the string to convert back to byte array.
    *
    * @return the byte array encoded by the input string.
    */
   public static byte[] bytesFromSortable(final String s) {

       final int nBits = s.length() * BASE_128_SHIFT;
       final int nBytes = nBits / 8;
       final byte[] out = new byte[nBytes];
       final BitSet bs = new BitSet(nBits);

       int bitIdx = 0;
       for (int i = 0; i < s.length(); i++) {
           char c = s.charAt(i);
           byte b = (byte)LOOKUP_BYTE_128[(short)c];
           for (int j = 1; j < 8; j++) {
               bs.set(bitIdx, (b & (1 << (7 - j))) > 0);
               bitIdx++;
           }
       }

       int bufIdx = 0;
       for (int i = 0; i < bs.length() && bufIdx < out.length; bufIdx++) {
           byte b = 0;
           for (int j = 0; j < 8; j++) {
               if (bs.get(i++)) {
                   b |= (byte) (1 << 7 - j);
               }
           }
           out[bufIdx] = b;
       }
       return out;
   }

    /**
     * Returns the number of characters needed to store this value.
     *
     * @param value the value to use for the calculation
     *
     * @return the number of characters required to store the string
     * encoding of value.  This can be used as input to
     * {@link #toSortable(int, int)}.
     */
    public static int encodingLength(Integer value) {
        if (value == null) {
            return INT_STRING_LEN_128;
        }
        if (value == 0) {
            return 1;  /* treat 0 as 1 */
        }
        int ret = 0;

        /*
         * Turn negative numbers to positive with the exception of the special
         * case of Integer.MIN_VALUE which will not convert to a valid positive
         * integer.
         */
        if (value < 0) {
            if (value == Integer.MIN_VALUE) {
                //                return INT_STRING_LEN;
                return INT_STRING_LEN_128;
            }
            value = -value;
        }

        /*
         * Add a bit, but only if it remains a valid integer.  If the value is
         * too large it requires the maximum length.
         */
        if (value < Integer.MAX_VALUE/2) {
            value <<= 1;
        } else {
            return INT_STRING_LEN_128;
        }

        /*
         * Calculate -- shift until there are no more non-zero bits.
         */
        while (value != 0) {
            value >>= BASE_128_SHIFT;
            ++ret;
        }
        return ret;
    }

    /**
     * Returns the number of characters needed to store values in the
     * specified integer range.
     *
     * @param min the smaller value in the range
     *
     * @param max the larger value in the range
     *
     * @return the number of characters required to store all values in the
     * range. This can be used as input to {@link #toSortable(int, int)}.
     */
    public static int encodingLength(Integer min, Integer max) {
        if (min == null) {
            min = Integer.MIN_VALUE;
        }
        if (max == null) {
            max = Integer.MAX_VALUE;
        }
        /*
         * Use the "larger" of the two numbers in terms of encoding
         * requirement.
         */
        int larger = max;
        if (min < 0) {
            if (max <= 0) {
                larger = min;
            } else {
                larger = (max > -min ? max : min);
            }
        }
        return encodingLength(larger);
    }

    /**
     * Returns the number of characters needed to store this value.
     *
     * @param value the value to use for the calculation
     *
     * @return the number of characters required to store the string
     * encoding of value.  This can be used as input to
     * {@link #toSortable(long, int)}.
     */
    public static int encodingLength(Long value) {
        if (value == null) {
            return LONG_STRING_LEN_128;
        }
        if (value == 0) {
            return 1;  /* treat 0 as 1 */
        }
        int ret = 0;

        /*
         * Turn negative numbers to positive with the exception of the special
         * case of Long.MIN_VALUE which will not convert to a valid positive
         * integer.
         */
        if (value < 0) {
            if (value == Long.MIN_VALUE) {
                return LONG_STRING_LEN_128;
            }
            value = -value;
        }

        /*
         * Add a bit, but only if it remains a valid long.  If the value is
         * too large it requires the maximum length.
         */
        if (value < Long.MAX_VALUE/2) {
            value <<= 1;
        } else {
            return LONG_STRING_LEN_128;
        }

        /*
         * Calculate -- shift until there are no more non-zero bits.
         */
        while (value != 0) {
            value >>= BASE_128_SHIFT;
            ++ret;
        }
        return ret;
    }

    /**
     * Returns the number of characters needed to store values in the
     * specified range.
     *
     * @param min the smaller value in the range
     *
     * @param max the larger value in the range
     *
     * @return the number of characters required to store all values in the
     * range. This can be used as input to {@link #toSortable(long, int)}.
     */
    public static int encodingLength(Long min, Long max) {
        if (min == null) {
            min = Long.MIN_VALUE;
        }
        if (max == null) {
            max = Long.MAX_VALUE;
        }

        /*
         * Use the "larger" of the two numbers in terms of encoding
         * requirement.
         */
        long larger = max;
        if (min < 0) {
            if (max <= 0) {
                larger = min;
            } else {
                larger = (max > -min ? max : min);
            }
        }
        return encodingLength(larger);
    }
}
