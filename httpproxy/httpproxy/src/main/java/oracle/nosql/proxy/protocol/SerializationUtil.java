/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;


/**
 * Utility methods to facilitate serialization/deserialization
 *
 * The numeric methods use PackedInteger class which uses a format
 * that is always sorted.
 */
public class SerializationUtil {

    /* TODO: Use StandardCharsets version in Java 8 */
    private static final Charset utf8 = Charset.forName("UTF-8");

    public static final String EMPTY_STRING = new String();

    public static final byte[] EMPTY_BYTES = { };

    /**
     * Reads a packed integer from the input and returns it.
     *
     * @param in the data input
     * @return the integer that was read
     */
    public static int readPackedInt(DataInput in) throws IOException {

        final byte[] bytes = new byte[PackedInteger.MAX_LENGTH];
        in.readFully(bytes, 0, 1);
        final int len = PackedInteger.getReadSortedIntLength(bytes, 0);
        try {
            in.readFully(bytes, 1, len - 1);
        } catch (IndexOutOfBoundsException e) {
            throw new IOException("Invalid packed int", e);
        }
        return PackedInteger.readSortedInt(bytes, 0);
    }

    /**
     * Skips over a packed integer from this input stream.
     *
     * @param in the data input
     * @return the number of bytes skipped.
     */
    public static int skipPackedInt(ByteInputStream in)
        throws IOException {

        byte b = in.readByte();
        int len = PackedInteger.getReadSortedIntLength(new byte[]{b}, 0);
        if (len > 1) {
            in.skip(len - 1);
        }
        return len;
    }

    /**
     * Writes a packed integer to the output.
     *
     * @param out the data output
     * @param value the integer to be written
     * @return the length of bytes written
     */
    public static int writePackedInt(DataOutput out, int value)
            throws IOException {
        final byte[] buf = new byte[PackedInteger.MAX_LENGTH];
        final int offset = PackedInteger.writeSortedInt(buf, 0, value);
        out.write(buf, 0, offset);
        return offset;
    }

    /**
     * Reads a packed long from the input and returns it.
     *
     * @param in the data input
     * @return the long that was read
     */
    public static long readPackedLong(ByteInputStream in) throws IOException {
        final byte[] bytes = new byte[PackedInteger.MAX_LONG_LENGTH];
        in.readFully(bytes, 0, 1);
        final int len = PackedInteger.getReadSortedLongLength(bytes, 0);
        try {
            in.readFully(bytes, 1, len - 1);
        } catch (IndexOutOfBoundsException e) {
            throw new IOException("Invalid packed long", e);
        }
        return PackedInteger.readSortedLong(bytes, 0);
    }

    /**
     * Skips over a packed long from this input stream.
     *
     * @param in the data input
     * @return the number of bytes skipped.
     */
    public static int skipPackedLong(ByteInputStream in) throws IOException {
        byte b = in.readByte();
        int len = PackedInteger.getReadSortedLongLength(new byte[]{b}, 0);
        if (len > 1) {
            in.skip(len - 1);
        }
        return len;
    }

    /**
     * Writes a packed long to the output.
     *
     * @param out the data output
     * @param value the long to be written
     */
    public static void writePackedLong(DataOutput out, long value)
            throws IOException {
        final byte[] buf = new byte[PackedInteger.MAX_LONG_LENGTH];
        final int offset = PackedInteger.writeSortedLong(buf, 0, value);
        out.write(buf, 0, offset);
    }

    /**
     * Reads a string written by {@link #writeString}, using standard UTF-8
     *
     * <p>First reads a {@link #readSortedInt packedInt} representing the
     * length of the UTF-8 encoding of the string, or a negative value for
     * null, followed by the string contents in UTF-8 format for a non-empty
     * string, if any.
     *
     * @param in the input stream
     * @return a string or null
     * @throws IOException if an I/O error occurs or if the input UTF-8
     * encoding is invalid
     */
    public static String readString(ByteInputStream in)
        throws IOException {

        final int length = readPackedInt(in);
        if (length < -1) {
            throw new IOException("Invalid length of String: " + length);
        }
        if (length == -1) {
            return null;
        }
        if (length == 0) {
            return EMPTY_STRING;
        }
        final byte[] bytes = new byte[length];
        in.readFully(bytes);
        return utf8.decode(ByteBuffer.wrap(bytes)).toString();
    }

    /**
     * Skips over a string from this input stream.
     *
     * @param in the data input
     * @return the number of bytes skipped.
     */
    public static int skipString(ByteInputStream in)
        throws IOException {

        int start = in.getOffset();
        int len = readPackedInt(in);
        if (len > 0) {
            in.skip(len);
        }
        return in.getOffset() - start;
    }

    /**
     * Reads a non-null string written by {@link #writeNonNullString}, using
     * standard UTF-8 or Java's modified UTF-8 format, depending on the serial
     * version.
     *
     * @param in the input stream
     * @return a string
     * @throws IOException if an I/O error occurs, if the input represents a
     * null value, or if the input UTF-8 encoding is invalid
     */
    public static String readNonNullString(ByteInputStream in)
        throws IOException {
        final String result = readString(in);
        if (result == null) {
            throw new IOException("Found null value for non-null string");
        }
        return result;
    }

    /**
     * Reads a byte array that is a UTF-8 encoded string into a
     * char array for use by DDL queries.
     */
    public static char[] readUTF8ToCharArray(ByteInputStream in)
        throws IOException {

        return UTF8ToCharArray(readByteArray(in));
    }

    /**
     * Translates a byte array that is a UTF-8 encoded string into a
     * char array for use by DDL queries.
     */
    public static char[] UTF8ToCharArray(byte[] bytes)
        throws IOException {

        if (bytes == EMPTY_BYTES) {
            throw new IOException("Empty bytes for char[]");
        }

        ByteBuffer buf = ByteBuffer.wrap(bytes);
        CharBuffer cb = utf8.decode(buf);
        return cb.array();
    }

    /**
     * Writes a string for reading by {@link #readString}, using standard UTF-8
     * format. The string may be null or empty.  This code differentiates
     * between the two, maintaining the ability to round-trip null and empty
     * string values.
     *
     * <p>This format is used rather than that of
     * {@link ByteOutputStream#writeUTF} to allow packing of the size
     * of the string. For shorter strings this size savings is a
     * significant percentage of the space used.
     *
     * The format is the standard UTF-8 format documented by <a
     * href="http://www.ietf.org/rfc/rfc2279.txt">RFC 2279</a> and implemented
     * by the {@link Charset} class using the "UTF-8" standard charset.
     *
     * <p>Format:
     * <ol>
     * <li> ({@link #writePackedInt packed int}) <i>string length, or -1
     * for null</i>
     * <li> <i>[Optional]</i> ({@code byte[]}) <i>UTF-8 bytes</i>
     * </ol>
     *
     * @param out the output stream
     * @param value the string or null
     * @throws IOException if an I/O error occurs
     */
    public static int writeString(DataOutput out, String value)
        throws IOException {

        return writeStdUTF8String(out, value);
    }

    /**
     * Writes a non-null string for reading by {@link #readNonNullString},
     * using the same format as {@link #writeString}, but not permitting a null
     * value to be written.
     *
     * @param out the output stream
     * @param value the string
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if {@code value} is {@code null}
     */
    public static void writeNonNullString(DataOutput out, String value)
        throws IOException {

        checkNull("value", value);
        writeString(out, value);
    }

    /**
     * Writes a possibly null or empty string to an output stream using
     * standard UTF-8 format.
     *
     * <p>First writes a {@link #writeSortedInt packedInt} representing the
     * length of the UTF-8 encoding of the string, or {@code -1} if the string
     * is null, followed by the UTF-8 encoding for non-empty strings.
     *
     * @param out the output stream
     * @param value the string or null
     * @throws IOException if an I/O error occurs
     */
    private static int writeStdUTF8String(DataOutput out, String value)
        throws IOException {

        if (value == null) {
            return writePackedInt(out, -1);
        }
        final ByteBuffer buffer = utf8.encode(value);
        final int length = buffer.limit();
        int len = writePackedInt(out, length);
        if (length > 0) {
            out.write(buffer.array(), 0, length);
        }
        return len + length;
    }

    /**
     * Reads the length of a possibly null sequence.  The length is represented
     * as a {@link #readPackedInt packed int}, with -1 interpreted as meaning
     * null, and other negative values not permitted.  Although we don't
     * enforce maximum sequence lengths yet, this entrypoint provides a place
     * to do that.
     *
     * @param in the input stream
     * @return the sequence length or -1 for null
     * @throws IOException if an I/O error occurs or the input format is
     * invalid
     */
    public static int readSequenceLength(ByteInputStream in)
        throws IOException {

        final int result = readPackedInt(in);
        if (result < -1) {
            throw new IOException("Invalid sequence length: " + result);
        }
        return result;
    }

    /**
     * Reads the length of a non-null sequence.  The length is represented as a
     * non-negative {@link #readPackedInt packed int}.  Although we don't
     * enforce maximum sequence lengths yet, this entrypoint provides a place
     * to do that.
     *
     * @param in the input stream
     * @return the sequence length
     * @throws IOException if an I/O error occurs, if the input represents a
     * null sequence, or if the input format is invalid
     */
    public static int readNonNullSequenceLength(ByteInputStream in)
        throws IOException {

        final int length = readSequenceLength(in);
        if (length == -1) {
            throw new IOException("Read null length for non-null sequence");
        }
        return length;
    }

    /**
     * Writes a sequence length.  The length is represented as a {@link
     * #readPackedInt packed int}, with -1 representing null.  Although we
     * don't enforce maximum sequence lengths yet, this entrypoint provides a
     * place to do that.
     *
     * @param out the output stream
     * @param length the sequence length or -1
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if length is less than -1
     */
    public static void writeSequenceLength(DataOutput out, int length)
        throws IOException {

        if (length < -1) {
            throw new IllegalArgumentException(
                "Invalid sequence length: " + length);
        }
        writePackedInt(out, length);
    }

    /**
     * Writes the length of a non-null sequence.  The length is represented as
     * a non-negative {@link #readPackedInt packed int}.  Although we don't
     * enforce maximum sequence lengths yet, this entrypoint provides a place
     * to do that.
     *
     * @param out the output stream
     * @param length the sequence length
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if length is less than 0
     */
    public static void writeNonNullSequenceLength(DataOutput out, int length)
        throws IOException {

        if (length < 0) {
            throw new IllegalArgumentException(
                "Invalid non-null sequence length: " + length);
        }
        writePackedInt(out, length);
    }

    /**
     * Reads a possibly null byte array as a {@link #readSequenceLength
     * sequence length} followed by the array contents.
     *
     * @param in the input stream
     * @return array the array or null
     * @throws IOException if an I/O error occurs or if the input format is
     * invalid
     */
    public static byte[] readByteArray(ByteInputStream in)
        throws IOException {

        final int len = readSequenceLength(in);
        if (len < -1) {
            throw new IOException("Invalid length of byte array: " + len);
        }
        if (len == -1) {
            return null;
        }
        if (len == 0) {
            return EMPTY_BYTES;
        }
        final byte[] array = new byte[len];
        in.readFully(array);
        return array;
    }

    /**
     * Skips over a byte array from this input stream.
     *
     * @param in the data input
     * @return the number of bytes skipped.
     */
    public static int skipByteArray(ByteInputStream in)
        throws IOException {

        int start = in.getOffset();
        final int len = readSequenceLength(in);
        if (len > 0) {
            in.skip(len);
        }
        return in.getOffset() - start;
    }

    /**
     * Writes a possibly null byte array as a {@link #writeSequenceLength
     * sequence length} followed by the array contents.
     *
     * @param out the output stream
     * @param array the byte array or null
     * @throws IOException if an I/O error occurs
     */
    public static void writeByteArray(DataOutput out, byte[] array)
        throws IOException {

        final int length = (array == null) ? -1 : Array.getLength(array);
        writeSequenceLength(out, length);
        if (length > 0) {
            out.write(array);
        }
    }

    /**
     * Reads a non-null byte array as a {@link #readNonNullSequenceLength
     * non-null sequence length} followed by the array contents.
     *
     * @param in the input stream
     * @return array the array
     * @throws IOException if an I/O error occurs, if the input represents a
     * null array, or if the input format is invalid
     */
    public static byte[] readNonNullByteArray(ByteInputStream in)
        throws IOException {

        final byte[] array = readByteArray(in);
        if (array == null) {
            throw new IOException("Read unexpected null array");
        }
        return array;
    }

    /**
     * Writes a non-null byte array as a {@link #writeNonNullSequenceLength
     * non-null sequence length} followed by the array contents.
     *
     * @param out the output stream
     * @param array the byte array
     * @throws IOException if an I/O error occurs
     */
    public static void writeNonNullByteArray(DataOutput out, byte[] array)
        throws IOException {

        checkNull("array", array);
        writeByteArray(out, array);
    }

    /**
     * Writes a possibly null int array as a {@link #writeSequenceLength
     * sequence length} followed by the array contents.
     *
     * @param out the output stream
     * @param array the int array or null
     * @throws IOException if an I/O error occurs
     */
    public static void writePackedIntArray(DataOutput out, int[] array)
        throws IOException {

        final int len = (array == null ? -1 : array.length);

        writeSequenceLength(out, len);

        if (array != null) {
            for (int v : array) {
                writePackedInt(out, v);
            }
        }
    }

    /**
     * Reads a possibly null int array as a {@link #readSequenceLength
     * sequence length} followed by the array contents.
     *
     * @param in the input stream
     * @return array the array or null
     * @throws IOException if an I/O error occurs or if the input format is
     * invalid
     */
    public static int[] readPackedIntArray(ByteInputStream in)
        throws IOException {

        final int len = readSequenceLength(in);
        if (len < -1) {
            throw new IOException("Invalid length of byte array: " + len);
        }
        if (len == -1) {
            return null;
        }

        final int[] array = new int[len];

        for (int i = 0; i < len; ++i) {
            array[i] = readPackedInt(in);
        }

        return array;
    }

    /**
     * Writes a possibly null int array as a {@link #writeSequenceLength
     * sequence length} followed by the array contents.
     *
     * @param out the output stream
     * @param array the int array or null
     * @throws IOException if an I/O error occurs
     */
    public static void writeIntArray(DataOutput out, int[] array)
        throws IOException {

        final int len = (array == null ? -1 : array.length);

        writeSequenceLength(out, len);

        if (array != null) {
            for (int v : array) {
                out.writeInt(v);
            }
        }
    }

    /**
     * Reads a possibly null int array as a {@link #readSequenceLength
     * sequence length} followed by the array contents.
     *
     * @param in the input stream
     * @return array the array or null
     * @throws IOException if an I/O error occurs or if the input format is
     * invalid
     */
    public static int[] readIntArray(ByteInputStream in)
        throws IOException {

        final int len = readSequenceLength(in);
        if (len < -1) {
            throw new IOException("Invalid length of byte array: " + len);
        }
        if (len == -1) {
            return null;
        }

        final int[] array = new int[len];

        for (int i = 0; i < len; ++i) {
            array[i] = in.readInt();
        }

        return array;
    }

    public static String[] readStringArray(ByteInputStream in)
        throws IOException {

        final int len = readSequenceLength(in);
        if (len < -1) {
            throw new IOException("Invalid length of byte array: " + len);
        }
        if (len == -1) {
            return null;
        }

        final String[] array = new String[len];

        for (int i = 0; i < len; ++i) {
            array[i] = readString(in);
        }

        return array;
    }

    public static void writeMathContext(
        MathContext mathContext,
        DataOutput out)
        throws IOException {

        if (mathContext == null) {
            out.writeByte(0);
        } else if (MathContext.DECIMAL32.equals(mathContext)) {
            out.writeByte(1);
        } else if (MathContext.DECIMAL64.equals(mathContext)) {
            out.writeByte(2);
        } else if (MathContext.DECIMAL128.equals(mathContext)) {
            out.writeByte(3);
        } else if (MathContext.UNLIMITED.equals(mathContext)) {
            out.writeByte(4);
        } else {
            out.writeByte(5);
            out.writeInt(mathContext.getPrecision());
            out.writeInt(mathContext.getRoundingMode().ordinal());
        }
    }

    public static MathContext readMathContext(DataInput in)
        throws IOException {

        int code = in.readByte();

        switch (code) {
        case 0:
            return null;
        case 1:
            return MathContext.DECIMAL32;
        case 2:
            return MathContext.DECIMAL64;
        case 3:
            return MathContext.DECIMAL128;
        case 4:
            return MathContext.UNLIMITED;
        case 5:
            int precision = in.readInt();
            int roundingMode = in.readInt();
            return
                new MathContext(precision, RoundingMode.valueOf(roundingMode));
        default:
            throw new IOException("Unknown MathContext code.");
        }
    }

    private static void checkNull(final String variableName,
                                  final Object value) {
        if (value == null) {
            throw new IllegalArgumentException(
                "The value of " + variableName + " must not be null");
        }
    }
}
