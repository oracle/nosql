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

package oracle.kv.impl.api.table;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

import com.sleepycat.bind.tuple.TupleInput;

/**
 * This class encapsulates methods to serialize/deserialize BigDecimal value
 * to/from a byte array.
 */
public class NumberUtils {

    /* The terminator byte */
    private static final byte TERMINATOR = (byte)-1;

    /* The leading byte of INFINITY */
    private static final byte POSI_INFINITY = (byte)0xFF;

    /* The leading byte of -INFINITY */
    private static final byte NEG_INFINITY = (byte)0;

    /* The leading byte of ZERO */
    private static final byte ZERO = excess128(0);

    /* The byte array that represents ZERO */
    private static final byte[] BYTES_ZERO = new byte[]{ ZERO };

    /* The number of digits of Long.MAX_VALUE */
    private static final int LONG_MAX_DIGITS_NUM =
        String.valueOf(Long.MAX_VALUE).length();

    /**
     * Serializes a BigDecimal value to a byte array that supports byte-to-byte
     * comparison.
     *
     * First, we need to do the normalization, which means we normalize a
     * given BigDecimal into two parts: exponent and mantissa.
     *
     * The decimal part contains 1 integer(non zero). For example,
     *      1234.56 will be normalized to 1.23456E3;
     *      123.4E100 will be normalized to 1.234E102;
     *      -1234.56E-100 will be normalized to -1.23456E-97.
     *
     * The byte format is:
     *     Byte 0 ~ m: the leading bytes that represents the combination of sign
     *                 and exponent, m is from 1 to 5.
     *     Byte m ~ n: the mantissa part with sign, each byte represents every
     *                 2 digits, a terminator byte (-1) is appended at the end.
     */
    public static byte[] serialize(BigDecimal value) {
        if (value.compareTo(BigDecimal.ZERO) == 0) {
            return BYTES_ZERO;
        }

        BigDecimal decimal = value.stripTrailingZeros();
        int sign = decimal.signum();
        String mantissa = decimal.unscaledValue().abs().toString();
        int exponent = mantissa.length() - 1 - decimal.scale();
        return writeBytes(sign, exponent, mantissa);
    }

    /**
     * Serializes a long to a byte array that supports byte-to-byte comparison.
     *
     * The byte format is:
     *     Byte 0 ~ m: the leading bytes that represents the combination of
     *                 sign and exponent, m is from 1 to 2.
     *     Byte m ~ n: the mantissa part with sign, each byte represents every
     *                 2 digits, a terminator byte (-1) is appended at the end.
     */
    public static byte[] serialize(long value) {
        if (value == 0) {
            return BYTES_ZERO;
        }

        char[] digits = Long.toString(value).toCharArray();
        int mlen = digits.length;
        /* Remove tailing zeros */
        while (mlen > 0 && digits[mlen - 1] == '0') {
            mlen--;
        }
        int sign = value > 0 ? 1 : -1;
        int start = sign > 0 ? 0 : 1;
        int exponent = digits.length - 1 - start;
        return writeBytes(sign, exponent, digits, start, mlen - start);
    }

    /**
     * Serializes the exponent and mantissa to a byte array and return it
     */
    private static byte[] writeBytes(int sign, int exponent, String digits) {
        return writeBytes(sign, exponent, digits.toCharArray(),
                          0, digits.length());
    }

    private static byte[] writeBytes(int sign, int exponent,
                                     char[] digits, int from, int len) {

        int nLeadingBytes = getNumBytesExponent(sign, exponent);
        /*
         * Required byte # =
         *  leading byte # + mantissa byte # + 1 terminator byte
         */
        int size = nLeadingBytes + (len + 1) / 2 + 1;
        byte[] bytes = new byte[size];
        writeExponent(bytes, 0, sign, exponent);
        writeMantissa(bytes, nLeadingBytes, sign, digits, from, len);
        return bytes;
    }

    /**
     * Write the leading bytes that combines the sign and exponent to
     * the given buffer, the leading bytes format as below:
     *
     *  ------------- ------- ---------------------------------------------
     *  Leading bytes  Sign                  Exponent
     *  ------------- ------- ---------------------------------------------
     *  0xFF                  infinity
     *  0xFC - 0xFE    &gt; 0 unused/reserved
     *  0xFB           &gt; 0 reserved for up to Long.MAX_VALUE
     *  0xF7 - 0xFA    &gt; 0 exponent from 4097 up to Integer.MAX_VALUE,
     *                        1 ~ 5 exponent bytes
     *  0xE7 - 0xF6    &gt; 0 exponent from 64 up to 4096, 1 exponent byte
     *  0x97 - 0xE6    &gt; 0 exponent from -16 up to 63, 0 exponent bytes
     *  0x87 - 0x96    &gt; 0 exponent from -4096 up to -17, 1 exponent byte
     *  0x83 - 0x86    &gt; 0 exponent from Integer.MIN_VALUE up to -4097,
     *                        1 ~ 5 exponent bytes
     *  0x82           &gt; 0 reserved for down to Long.MIN_VALUE
     *  0x81           &gt; 0 reserved for later expansion
     *  0x80           = 0
     *  0x7F           &lt; 0 reserved for later expansion
     *  0x7E           &lt; 0 reserved for down to Long.MIN_VALUE
     *  0x7A - 0x7D    &lt; 0 exponent from -4097 down to Integer.MIN_VALUE,
     *                        1 ~ 5 exponent bytes
     *  0x6A - 0x79    &lt; 0 exponent from -17 down to -4096, 1 exponent byte
     *  0x1A - 0x69    &lt; 0 exponent from 63 down to -16, 0 exponent bytes
     *  0x0A - 0x19    &lt; 0 exponent from 4096 down to 64, 1 exponent byte
     *  0x06 - 0x09    &lt; 0 exponent from Integer.MAX_VALUE down to 4097,
     *                        1 ~ 5 exponent bytes
     *  0x05           &lt; 0 reserved for up to Long.MAX_VALUE
     *  0x01 - 0x04    &lt; 0 unused/reserved
     *  0x00                  -infinity
     */
    static void writeExponent(byte[] buffer, int offset, int sign, int value) {

        if (sign == 0) {
            buffer[offset] = (byte)0x80;
        } else if (sign > 0) {
            if (value >= 4097) {
                writeExponentMultiBytes(buffer, offset, (byte)0xF7,
                                        (value - 4097));
            } else if (value >= 64) {
                writeExponentTwoBytes(buffer, offset,
                                      (byte)0xE7, (value - 64));
            } else if (value >= -16) {
                buffer[offset] = (byte)(0x97 + (value - (-16)));
            } else if (value >= -4096) {
                writeExponentTwoBytes(buffer, offset, (byte)0x87,
                                      (value - (-4096)));
            } else {
                writeExponentMultiBytes(buffer, offset, (byte)0x83,
                                        (value - Integer.MIN_VALUE));
            }
        } else {
            /* Sign < 0 */
            if (value <= -4097) {
                writeExponentMultiBytes(buffer, offset, (byte)0x7A,
                                        (-4097 - value));
            } else if (value <= -17){
                writeExponentTwoBytes(buffer, offset, (byte)0x6A,
                                      (-17 - value));
            } else if (value <= 63) {
                buffer[offset] = (byte)(0x1A + (63 - value));
            } else if (value <= 4096) {
                writeExponentTwoBytes(buffer, offset, (byte)0x0A,
                                      (4096 - value));
            } else {
                writeExponentMultiBytes(buffer, offset, (byte)0x06,
                                        (Integer.MAX_VALUE - value));
            }
        }
    }

    /**
     * Writes the leading bytes that represents the exponent with 2 bytes to
     * buffer.
     */
    private static void writeExponentTwoBytes(byte[] buffer,
                                              int offset,
                                              byte byte0,
                                              int exponent) {
        buffer[offset++] = (byte)(byte0 + (exponent >> 8 & 0xF));
        buffer[offset] = (byte)(exponent & 0xFF);
    }

    /**
     * Writes the leading bytes that represents the exponent with variable
     * length bytes to the buffer.
     */
    private static void writeExponentMultiBytes(byte[] buffer,
                                                int offset,
                                                byte byte0,
                                                int exponent) {

        int size = getNumBytesExponentVarLen(exponent);
        buffer[offset++] = (byte)(byte0 + (size - 2));
        if (size > 4) {
            buffer[offset++] = (byte)(exponent >>> 24);
        }
        if (size > 3) {
            buffer[offset++] = (byte)(exponent >>> 16);
        }
        if (size > 2) {
            buffer[offset++] = (byte)(exponent >>> 8);
        }
        buffer[offset] = (byte)exponent;
    }

    /**
     * Returns the number of bytes used to store the exponent value.
     */
    static int getNumBytesExponent(int sign, int value) {
        if (sign == 0) {
            return 1;
        } else if (sign > 0) {
            if (value >= 4097) {
                return getNumBytesExponentVarLen(value - 4097);
            } else if (value >= 64) {
                return 2;
            } else if (value >= -16) {
                return 1;
            } else if (value >= -4096) {
                return 2;
            } else {
                return getNumBytesExponentVarLen(value - Integer.MIN_VALUE);
            }
        } else {
            if (value <= -4097) {
                return getNumBytesExponentVarLen(-4097 - value);
            } else if (value <= -17){
                return 2;
            } else if (value <= 63) {
                return 1;
            } else if (value <= 4096) {
                return 2;
            } else {
                return getNumBytesExponentVarLen(Integer.MAX_VALUE - value);
            }
        }
    }

    /**
     * Returns the number bytes that used to store the exponent value with
     * variable length.
     */
    private static int getNumBytesExponentVarLen(int exponent) {
        if ((exponent & 0xFF000000) != 0) {
            return 5;
        } else if ((exponent & 0xFF0000) != 0) {
            return 4;
        } else if ((exponent & 0xFF00) != 0) {
            return 3;
        } else {
            return 2;
        }
    }

    /**
     * Write mantissa digits to the given buffer.
     *
     * The digits are stored as a byte array with a terminator byte (-1):
     * - All digits are stored with sign, 1 byte for every 2 digits.
     * - Pad a zero if the number of digits is odd.
     * - The byte is represented in excess-128.
     * - The terminator used is -1. For negative value, subtract additional 2
     *   to leave -1 as a special byte.
     */
    private static void writeMantissa(byte[] buffer, int offset, int sign,
                                      char[] digits, int start, int len) {
        for (int ind = 0; ind < len / 2; ind++) {
            writeByte(buffer, offset++, sign, digits, start + ind * 2);
        }

        /* Pad a zero if left 1 digit only. */
        if (len % 2 == 1) {
            int last = start + len - 1;
            writeByte(buffer, offset++, sign, new char[]{digits[last], '0'}, 0);
        }

        /* Writes terminator byte. */
        buffer[offset] = excess128(TERMINATOR);
    }

    /**
     * Parses 2 digits to a byte and write to the buffer on the given position.
     */
    private static void writeByte(byte[] buffer, int offset, int sign,
                                  char[] digits, int index) {

        assert(digits.length > index + 1);
        int value = (digits[index] - '0') * 10 + (digits[index + 1] - '0');
        if (sign < 0) {
            value = -1 * value;
        }
        buffer[offset] = toUnsignedByte(value);
    }

    /**
     * Converts the value with sign to a unsigned byte. If the value is
     * negative, subtract 2.
     */
    private static byte toUnsignedByte(int value) {
        if (value < 0) {
            value -= 2;
        }
        return excess128(value);
    }

    /**
     * Deserializes to a numeric object from a byte array, the numeric object
     * can be a Integer, Long or BigDecimal based on its value.
     *
     * New users should use deserialize(byte[], boolean). This is for
     * compatibility with previous callers.
     */
    public static Object deserialize(byte[] bytes) {
        return deserialize(bytes, true, false);
    }

    /**
     * Deserializes to a numeric object from a byte array, the numeric object
     * can be a Integer, Long or BigDecimal based on its value.
     */
    public static Object deserialize(
        byte[] bytes,
        boolean returnBigDecimalOnly) {
        return deserialize(bytes, returnBigDecimalOnly, false);
    }

    static void validateValue(byte[] bytes) {
        deserialize(bytes, false, true);
    }

    private static Object deserialize(
        byte[] bytes,
        boolean returnBigDecimalOnly,
        boolean validate) {

        if (bytes == null || bytes.length == 0) {
            throw new IllegalArgumentException(
                "Invalid bytes for Number value, it must not be null or empty");
        }

        int offset = 0;
        byte byte0 = bytes[offset++];

        if (!isValidLeadingByte(byte0)) {
            throw new IllegalArgumentException(
                "Invalid leading byte: " + byte0);
        }

        int sign = readSign(byte0);
        if (sign == 0) {
            /* Sign of 0 represents ZERO, no more byte to read */
            if (offset != bytes.length) {
                throw new IllegalArgumentException(
                    "Invalid bytes for Number value");
            }
            return (returnBigDecimalOnly ?
                    new BigDecimal(0) :
                    Integer.valueOf(0));
        }

        ReadBuffer in = new ReadBuffer(bytes, offset);
        int exponent = readExponent(in, byte0, sign);

        /* At least 2 more bytes left: mantissa part and TERMINATOR */
        if (in.available() < 2) {
            throw new IllegalArgumentException(
                "Invalid bytes for Number value");
        }

        if (validate) {
            return null;
        }

        return createNumericObject(in, returnBigDecimalOnly, sign, exponent);
    }

    /**
     * Reads and returns the exponent value from the given TupleInput.
     */
    static int readExponent(ReadBuffer in, byte byte0, int sign) {

        if (sign == 0) {
            return 0;
        }

        if (sign > 0) {
            if (byte0 >= (byte)0xF7) {
                return readExponentMultiBytes(in, byte0, (byte)0xF7) + 4097;
            } else if (byte0 >= (byte)0xE7) {
                return readExponentTwoBytes(in, byte0, (byte)0xE7) + 64;
            } else if (byte0 >= (byte)0x97) {
                return byte0 - (byte)0x97 + (-16);
            } else if (byte0 >= (byte)0x87) {
                return readExponentTwoBytes(in, byte0, (byte)0x87) + (-4096);
            } else {
                return readExponentMultiBytes(in, byte0, (byte)0x83) +
                       Integer.MIN_VALUE;
            }
        }

        /* Sign < 0 */
        if (byte0 >= (byte)0x7A) {
            return -4097 - readExponentMultiBytes(in, byte0, (byte)0x7A);
        } else if (byte0 >= (byte)0x6A) {
            return -17 - readExponentTwoBytes(in, byte0, (byte)0x6A);
        } else if (byte0 >= (byte)0x1A) {
            return 63 - (byte0 - (byte)0x1A);
        } else if (byte0 >= (byte)0x0A) {
            return 4096 - readExponentTwoBytes(in, byte0, (byte)0x0A);
        } else {
            return Integer.MAX_VALUE -
                    readExponentMultiBytes(in, byte0, (byte)0x06);
        }
    }

    /**
     * Returns true if the leading byte is valid, false for unused.
     */
    private static boolean isValidLeadingByte(byte byte0) {
        return (byte0 >= (byte)0x83 && byte0 <= (byte)0xFA) ||
               (byte0 >= (byte)0x06 && byte0 <= (byte)0x7D) ||
               byte0 == (byte)0x80 || byte0 == (byte)0x00 ||
               byte0 == (byte)0xFF;
    }

    /**
     * Reads the exponent value represented with 2 bytes from the given
     * TupleInput and returns it
     */
    private static int readExponentTwoBytes(ReadBuffer in,
                                            byte byte0,
                                            byte base) {
        if (in.available() == 0) {
            throw new IllegalArgumentException(
                "Invalid exponent value for Number");
        }
        return (byte0 - base) << 8 | (in.read() & 0xFF);
    }

    /**
     * Reads the exponent value represented with multiple bytes from the
     * given TupleInput and returns it
     */
    private static int readExponentMultiBytes(ReadBuffer in,
                                              byte byte0,
                                              byte base){
        int len = (byte0 - base) + 1;
        int exp = 0;
        if (in.available() < len) {
            throw new IllegalArgumentException(
                "Invalid exponent value for Number");
        }
        while (len-- > 0) {
            exp = exp << 8 | (in.read() & 0xFF);
        }
        return exp;
    }


    /**
     * Parses the digits string and constructs a numeric object, the numeric
     * object can be any of Integer, Long or BigDecimal according to the value.
     */
    private static Object createNumericObject(ReadBuffer in,
                                              boolean returnBigDecimalOnly,
                                              int sign,
                                              int expo) {

        int size = (in.available() - 1) * 2;
        char[] buf = new char[size];
        int ind = 0, val;

        /* Read from byte array and store them into char array*/
        while((val = excess128(in.read())) != TERMINATOR) {
            if (val < 0) {
                val = val + 2;
            }
            String group = Integer.toString(Math.abs(val));
            if (group.length() == 1) {
                buf[ind++] = '0';
                buf[ind++] = group.charAt(0);
            } else {
                buf[ind++] = group.charAt(0);
                buf[ind++] = group.charAt(1);
            }
        }

        /* Remove the tailing zero in mantissa */
        if (buf[ind - 1] == '0') {
            ind--;
        }
        String digits = new String(buf, 0, ind);

        /*
         * If digits number is less than that of Long.MAX_VALUE, try to
         * construct a Integer or Long.
         */
        if (!returnBigDecimalOnly &&
            expo >= 0 &&
            expo < LONG_MAX_DIGITS_NUM &&
            digits.length() <= expo + 1) {

            /* Append tailing zeros if number of digits < exponent + 1 */
            int num = expo + 1;
            if (digits.length() < num) {
                digits = String.format("%-" + num + "s", digits)
                            .replace(' ', '0');
            }
            String signedDigits = (sign < 0) ? "-" + digits : digits;

            /* Try to parse the string as long */
            try {
                long lval = Long.parseLong(signedDigits);
                /* Returns as a int if the value is in range of int */
                if (lval >= Integer.MIN_VALUE && lval <= Integer.MAX_VALUE) {
                    return Integer.valueOf((int)lval);
                }
                return Long.valueOf(lval);
            } catch (NumberFormatException ignored) {
                /*
                 * Out of Long's range, ignore the error and construct a
                 * BigDecimal object.
                 */
            }
        }

        /* Construct BigDecimal value */
        BigInteger unscaled = new BigInteger(digits);
        if (sign < 0) {
            unscaled = unscaled.negate();
        }
        return new BigDecimal(unscaled, digits.length() - 1)
                .scaleByPowerOfTen(expo);
    }

    /**
     * Reads a tuple for a BigDecimal value from the given TupleInput.
     */
    static byte[] readTuple(TupleInput in) {

        byte byte0 = (byte)in.read();
        int sign = readSign(byte0);
        if (sign == 0) {
            return new byte[]{byte0};
        }

        WriteBuffer buffer = new WriteBuffer();

        /* Read the leading bytes */
        readLeadingBytes(in, byte0, sign, buffer);

        /* Read the bytes of mantissa */
        int end = excess128(TERMINATOR);
        byte b;
        while ((b = (byte)in.read()) != end) {
            buffer.write(b);
        }
        buffer.write((byte)end);

        return buffer.toByteArray();
    }

    /**
     * Read the leading bytes from the specified TupleInput.
     */
    private static void readLeadingBytes(TupleInput in,
                                         byte byte0,
                                         int sign,
                                         WriteBuffer buffer) {

        buffer.write(byte0);
        if (sign == 0) {
            return;
        }

        if (in.available() == 0) {
            throw new IllegalStateException("Failed to read leading bytes");
        }

        if (sign > 0) {
            if (byte0 >= (byte)0xF7) {
                readLeadingMultiBytes(in, byte0, (byte)0xF7, buffer);
            } else if (byte0 >= (byte)0xE7) {
                buffer.write((byte)in.read());
            } else if (byte0 >= (byte)0x97) {
                return;
            } else if (byte0 >= (byte)0x87) {
                buffer.write((byte)in.read());
            } else {
                readLeadingMultiBytes(in, byte0, (byte)0x83, buffer);
            }
        } else {
            /* Sign < 0 */
            if (byte0 >= (byte)0x7A) {
                readLeadingMultiBytes(in, byte0, (byte)0x7A, buffer);
            } else if (byte0 >= (byte)0x6A) {
                buffer.write((byte)in.read());
            } else if (byte0 >= (byte)0x1A) {
                return;
            } else if (byte0 >= (byte)0x0A) {
                buffer.write((byte)in.read());
            } else {
                readLeadingMultiBytes(in, byte0, (byte)0x06, buffer);
            }
        }
    }

    /**
     * Reads the leading bytes with variable length from the TupleInput
     */
    private static void readLeadingMultiBytes(TupleInput in,
                                              byte byte0,
                                              byte base,
                                              WriteBuffer buffer) {
        int len = byte0 - base + 1;
        if (in.available() < len) {
            throw new IllegalStateException("Failed to read leading bytes");
        }
        for (int i = 0; i < len; i++) {
            buffer.write((byte)in.read());
        }
    }

    /**
     * Returns the byte array that represents the next value of the BigDecimal
     * value represented with the given byte array.
     *
     * It is used by NumberValueImpl.getNextValue(), the returning bytes is not
     * tge actual next value of the given BigDeicimal value, but add a special
     * byte to end of manitissa to make sure the bytes &lt; nextUp() and there
     * is no legal value such that bytes &lt; cantHappen &lt; nextUp().
     *
     * If sign == 0, return bytes of 0x81.
     *
     * If sign &gt; 0, add 0x80 (unsigned byte representation for 0) to end of
     * manitissa.
     * e.g.
     *      1234's byte[]:     AA 8C A2 7F
     *      1234's next value: AA 8C A2 80
     *
     * If sign &lt; 0, increments 1 on the last byte of manitissa and pad 0x1A
     * (unsigned byte representation for -100).
     * e.g.
     *     -1234's byte[]:     56 72 5C 7F
     *     -1234's next value: 56 72 5C 80
     *
     * Note that this function is for internal use for bytes's comparison only,
     * the deserializing of next value returned from nextUp() will causes the
     * problem.
     */
    static byte[] nextUp(byte[] bytes) {
        if (readSign(bytes[0]) == 0) {
            return new byte[]{(byte)(ZERO + 1)};
        }
        byte[] next = Arrays.copyOf(bytes, bytes.length);
        next[next.length - 1] = ZERO;
        return next;
    }

    /**
     * Returns the sign number that the specified byte stand for.
     */
    private static int readSign(byte byte0) {
        return (byte0 == (byte)0x80) ? 0 : (((byte0 & 0x80) > 0) ? 1 : -1);
    }

    /**
     * Returns the excess128 representation of a byte.
     */
    private static byte excess128(int b) {
        return (byte)(b ^ 0x80);
    }

    /**
     * Returns the bytes that represents the negative infinity.
     */
    static byte[] getNegativeInfinity() {
        return new byte[] {NEG_INFINITY};
    }

    /**
     * Returns the bytes that represents the positive infinity.
     */
    static byte[] getPositiveInfinity() {
        return new byte[] {POSI_INFINITY};
    }

    /**
     * A simple read byte buffer.
     */
    static class ReadBuffer {
        private final byte[] bytes;
        private int offset;

        ReadBuffer(byte[] bytes, int offset) {
            this.bytes = bytes;
            this.offset = offset;
        }

        /**
         * Reads the byte at this buffer's current position, and then
         * increments the position. Return -1 if the position is out of range
         * of the buffer.
         */
        byte read() {
            if (offset < bytes.length) {
                return bytes[offset++];
            }
            return -1;
        }

        /**
         * Returns the number of remaining bytes that can be read from this
         * buffer.
         */
        int available() {
            return bytes.length - offset;
        }
    }

    /**
     * A simple write byte buffer.
     */
    static class WriteBuffer {
        private final static int INIT_SIZE = 32;
        private final static int BUMP_SIZE = 32;
        private byte[] bytes;
        private int offset;

        WriteBuffer() {
            this(INIT_SIZE);
        }

        WriteBuffer(int size) {
            bytes = new byte[size];
            offset = 0;
        }

        /**
         * Writes the given byte into this buffer at the current position,
         * and then increments the position. If there is no space left,
         * increase the buffer size.
         */
        void write(byte val) {
            if (offset == bytes.length) {
                increaseBytes();
            }
            bytes[offset++] = val;
        }

        /**
         * Creates a newly allocated byte array. Its size is the current number
         * of bytes written and the valid contents of the buffer have been
         * copied into it.
         */
        byte[] toByteArray() {
            return Arrays.copyOf(bytes, offset);
        }

        private void increaseBytes() {
            offset = bytes.length;
            bytes = Arrays.copyOf(bytes, bytes.length + BUMP_SIZE);
        }
    }
}
