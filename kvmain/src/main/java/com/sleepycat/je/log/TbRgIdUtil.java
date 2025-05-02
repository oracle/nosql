/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.log;

import java.nio.ByteBuffer;

import com.sleepycat.util.PackedInteger;
import com.sleepycat.util.UtfOps;

/**
 * Convenient method for printing out tableID and regionID
 */
public class TbRgIdUtil {

    public enum Format {

        NONE(0),

        AVRO(1),

        TABLE(2),

        TABLE_V1(3),

        MULTI_REGION_TABLE(4);

        private static final Format[] VALUES = values();
        public static Format valueOf(int ordinal) {
            return VALUES[ordinal];
        }

        private Format(int ordinal) {
            if (ordinal != ordinal()) {
                throw new IllegalStateException("Wrong ordinal");
            }
        }

        /**
         * For internal use only.
         * @hidden
         */
        public static Format fromFirstByte(int firstByte) {

            /*
             * Avro schema IDs are positive, which means the first byte of the
             * package sorted integer is negative.
             */
            if (firstByte < 0) {
                return Format.AVRO;
            }

            /* Zero means no format. */
            if (firstByte == 0) {
                return Format.NONE;
            }

            /* Table formats. */
            if (isTableFormat(firstByte)) {
                return valueOf(firstByte + 1);
            }

            /* Other values are not yet assigned. */
            throw new IllegalStateException
                    ("Value has unknown format discriminator: " + firstByte);
        }

        /**
         * Returns true if the value format is for table.
         */
        public static boolean isTableFormat(Format format) {
            int ordinal = format.ordinal();
            return ordinal >= Format.TABLE.ordinal() &&
                    ordinal <= Format.MULTI_REGION_TABLE.ordinal();
        }

        public static boolean isTableFormat(int firstByte) {
            int ordinal = firstByte + 1;
            return ordinal >= Format.TABLE.ordinal() &&
                    ordinal <= Format.MULTI_REGION_TABLE.ordinal();
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

    /*
     * Each Base128 character represents 7 bits of information.
     */
    private static final int BASE_128_SHIFT = 7;

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

    private static long fromString(final String s) {
        long out = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < BASE_128_DIGITS_OFFSET ||
                    c > LOOKUP_BYTE_128.length + BASE_128_DIGITS_OFFSET - 1) {
                return -1;
            }
            out <<= BASE_128_SHIFT;
            out ^= LOOKUP_BYTE_128[(short)c];
        }
        return out;
    }

    public static long longFromSortable(final String s) {
        /*
         * Base128 string to represent 64-bits (long, double) is 10 bytes long.
         */
        int LONG_STRING_LEN_128 = 10;

        if (s.length() > LONG_STRING_LEN_128) {
            return -1;
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

    public static long getTableId(byte[] key) {
        final int BINARY_COMP_DELIM = 0;
        int offSet = 0;
        boolean foundDelim = false;
        /*
         * In KV, Key is concatenated of major path and minor path.
         * The Major Path must have at least one component, and
         * the first component of the major path must be a non-empty String.
         * Get the first component of major path, it is always the tableId,
         * within major/minor path, BINARY_COMP_DELIM is used to separate
         * components.
         */
        for (int i = 0; i < key.length; i += 1) {
            final int b = (key[i] & 0xff);
            if (b == BINARY_COMP_DELIM) {
                foundDelim = true;
                break;
            }
            offSet += 1;
        }
        if (!foundDelim) {
            /*
             * No tableId attached,
             */
            return -1;
        }
        String tableIdString = UtfOps.bytesToString(key, 0, offSet);
        return TbRgIdUtil.longFromSortable(tableIdString);
    }

    public static int getRegionId(byte[] data) {

        Format format = Format.fromFirstByte(data[0]);
        if (format == Format.NONE || Format.isTableFormat(format)) {
            if (format == Format.MULTI_REGION_TABLE) {
                /* read compressed region id. */

                final int len = PackedInteger.getReadIntLength(data, 1);
                if (len > PackedInteger.MAX_LENGTH) {
                    /*
                     * We don't throw an exception here, we treat this as this
                     * entry doesn't have a regionId
                     */
                    return -1;
                }
                return PackedInteger.readInt(data,1);
            }
        }
        return -1;
    }

    public static boolean isUserDatabase(String dbName) {
        if (dbName == null || !dbName.startsWith("p") || dbName.length() < 2) {
            return false;
        }

        /*
         * Below performs the functionality of checking the substring
         * following the first character p is an integer or not.
         * Previously it was done by Integer.parseInt(name.substring(1)),
         * but calling String.substring() creates garbage.
         * Code below is equivalent to Integer.parseInt() and checks the
         * substring in place, avoids creating garbage.
         * Note that the digits could lead with a "-" sign.
         */
        int i = 1;
        int len = dbName.length();
        int minValue = -Integer.MAX_VALUE;

        char sign = dbName.charAt(1);
        if (sign == '-' || sign == '+') {
            i ++;
            minValue = sign == '-' ? Integer.MIN_VALUE : minValue;

            if (len == 2) {
                return false;
            }
        }

        int minLimit = minValue / 10;
        int remainder = -(minValue % 10);
        int result = 0;
        while (i < len) {
            char c = dbName.charAt(i++);
            if (c < '0' || c > '9') {
                return false;
            }
            int digit = c - '0';
            if (result < minLimit ||
                    (result == minLimit && digit > remainder)) {
                return false;
            }
            result = result * 10 - digit;
        }
        return true;
    }

    public static boolean keySpaceIsInternal(byte[] key) {
        /*
         * Code copied from KV, returns true if the key belongs the
         * internal key space.
         */
        int BINARY_COMP_DELIM = 0;
        return (key.length == 0) /* Just '/' */||
                ((key.length > 0) &&
                        (key[0] == BINARY_COMP_DELIM));
    }

}
