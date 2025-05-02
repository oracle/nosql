/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Stack;

import oracle.nosql.nson.util.ByteInputStream;
import oracle.nosql.nson.util.ByteOutputStream;
import oracle.nosql.nson.util.NettyByteInputStream;
import oracle.nosql.nson.util.NettyByteOutputStream;
import oracle.nosql.nson.util.SerializationUtil;
import oracle.nosql.nson.values.FieldValue;
import oracle.nosql.nson.values.FieldValueCreator;
import oracle.nosql.nson.values.FieldValueEventHandler;
import oracle.nosql.nson.values.JsonOptions;
import oracle.nosql.nson.values.JsonPrettySerializer;
import oracle.nosql.nson.values.JsonSerializer;
import oracle.nosql.nson.values.MapValue;
import oracle.nosql.nson.values.TimestampValue;

/**
 * This class implements the NSON format including serialization to the
 * format using a {@link FieldValueEventHandler} instance and deserialization
 * from the format by generating {@link FieldValueEventHandler} events from
 * the binary format.
 * <p>
 * The class also has methods to serialize and deserialize primitive values
 * according to the format. Serialization is done by using the
 * {@link NsonSerializer} class and passing it events. Deserialization is
 * performed by passing NSON and a {@link FieldValueEventHandler} to
 * {@link #generateEventsFromNson}
 * <p>
 * NSON is a simple tagged (typed) value format:
 * <pre>
 * &lt;byte&gt; field type
 * &lt;variable&gt; field value
 * </pre>
 * Primitive/atomic types have well-known formats and lengths. An NSON "row"
 * can be as simple as a single primitive. For example a single byte
 * representing the JSON null value is the smallest possible NSON document.
 * Complex types (map and array) include their total size in bytes, allowing
 * navigational code to skip them entirely if they are not of interest.
 * Details of the format of each data type is below.
 * <p>
 * Data Types
 * <ul>
 * <li> Array
 * <pre>
 *  Tag value: 0
 *  Value:
 *    4-byte (unpacked int) - size of the entire array in bytes
 *    4-byte (unpacked int) - number of elements in the array
 *  The array fields, which are NSON values. Because NSON is schemaless the
 *  array elements may be of any type
 * </pre>
 * </li>
 * <li> Binary
 * <pre>
 *  Tag value: 1
 *  Value:
 *    size of array in bytes (can be 0)
 *    the bytes
 * </pre>
 * </li>
 * <li> Boolean
 * <pre>
 *  Tag value: 2
 *  Value (byte): 0 (false) or non-zero (true)
 * </pre>
 * </li>
 * <li> Double
 * <pre>
 *  Tag value: 3
 *  Value: double value as written by Java's DataOutput class (TBD: describe this
 *  in more detail)
 * </pre>
 * </li>
 * <li> Integer
 * <pre>
 *  Tag value: 4
 *  Value: packed int (see below)
 * </pre>
 * </li>
 * <li> Long
 * <pre>
 *  Tag value: 5
 *  Value: packed long (see below)
 * </pre>
 * </li>
 * <li> Map
 * <pre>
 *  Tag value: 6
 *  Value:
 *    4-byte (unpacked int) - size of the entire map in bytes (value as written
 *      by Java's DataOutput.writeInt)
 *    4-byte (unpacked int) - number of elements in the map (value as written
 *      by Java's DataOutput.writeInt)
 *    The map fields, which are:
 *      key - String (see String below), cannot be null
 *      value - an NSON value. Because NSON is schemaless the map values may be
 *              of any type
 * </pre>
 * </li>
 * <li> String
 * <pre>
 *  Tag value: 7
 *  Value: UTF-8 encoded string, as bytes
 *    The first byte is packed integer and is the length of the string. If
 *    if the string is null a length of -1 is used
 *    the rest of the bytes are the string encoded as utf-8
 * </pre>
 * </li>
 * <li> Timestamp
 * <pre>
 *  Tag value: 8
 *  Value: ISO 8601 formatted string (see String above)
 * </pre>
 * </li>
 * <li> Number
 * <pre>
 *  Tag value: 9
 *  Value: A string (see String above) representing a BigDecimal (Java) value.
 *         TBD: describe in detail
 * </pre>
 * </li>
 * <li> Json Null
 * <pre>
 *  Tag value: 10
 *  Value: none
 * </pre>
 * </li>
 * <li> Null
 * <pre>
 *  Tag value: 11
 *  Value: none
 * </pre>
 * </li>
 * <li> Packed formats (TBD)
 * <pre>
 *   packed int, long -- - describe algorithm
 *   unpacked int - endian?
 *   Java BigDecimal/number format
 * </pre>
 * </li>
 * </ul>
 * @hide-in-sdk
 */
public class Nson {

    /*
     * Data types
     */
    public static final int TYPE_ARRAY = 0;
    public static final int TYPE_BINARY = 1;
    public static final int TYPE_BOOLEAN = 2;
    public static final int TYPE_DOUBLE = 3;
    public static final int TYPE_INTEGER = 4;
    public static final int TYPE_LONG = 5;
    public static final int TYPE_MAP = 6;
    public static final int TYPE_STRING = 7;
    public static final int TYPE_TIMESTAMP = 8;
    public static final int TYPE_NUMBER = 9;
    public static final int TYPE_JSON_NULL = 10;
    public static final int TYPE_NULL = 11;
    public static final int TYPE_EMPTY = 12;

    private static String[] NsonTypes = {"Array", "Binary", "Boolean", "Double",
                                         "Integer", "Long", "Map", "String",
                                         "Timestamp", "Number", "JsonNull",
                                         "Null", "Empty"};
    /*
     * Serialization methods
     *  int - packed int
     *  long - packed long
     *  double - DataOutput double format
     *  timestamp - ISO 8601 string
     *  string - utf-8 encoded byte[]
     */

    /*
     * Primitives
     */

    /**
     * Writes an integer to the output stream
     *
     * @param out the output stream
     * @param value the value
     * @throws IOException if there is a problem with the stream
     */
    static public void writeInt(ByteOutputStream out,
                                int value) throws IOException {
        SerializationUtil.writePackedInt(out, value);
    }

    /**
     * Writes a long to the output stream
     *
     * @param out the output stream
     * @param value the value
     * @throws IOException if there is a problem with the stream
     */
    static public void writeLong(ByteOutputStream out,
                                 long value) throws IOException {
        SerializationUtil.writePackedLong(out, value);
    }

    /**
     * Writes a double to the output stream
     *
     * @param out the output stream
     * @param value the value
     * @throws IOException if there is a problem with the stream
     */
    static public void writeDouble(ByteOutputStream out,
                                   double value) throws IOException {
        out.writeDouble(value);
    }

    /**
     * Writes the String value of a Timestamp to the output stream
     *
     * @param out the output stream
     * @param value the timestamp
     * @throws IOException if there is a problem with the stream
     */
    static public void writeTimestamp(ByteOutputStream out,
                                      TimestampValue value)
        throws IOException {
        writeString(out, value.getString());
    }

    /**
     * Writes a String to the output stream
     *
     * @param out the output stream
     * @param s the string
     * @throws IOException if there is a problem with the stream
     */
    static public void writeString(ByteOutputStream out,
                                   String s) throws IOException {

        SerializationUtil.writeString(out, s);
    }

    /**
     * Writes a char array as a UTF8 byte array. This is used for
     * system queries that may contain a password.
     * @param out the output stream
     * @param chars the characters to write
     * @throws IOException if there is a problem with the stream
     */
    static public void writeCharArrayAsUTF8(ByteOutputStream out,
                                            char [] chars)
        throws IOException {
        writeByteArray(out, getCharArrayAsUTF8(chars));
    }

    /**
     * Returns a char array as a UTF8 byte array. This is used for
     * system queries that may contain a password.
     * @param chars the characters to write
     * @return the array
     * @throws IOException if there is a problem with the stream
     */
    static public byte[] getCharArrayAsUTF8(char [] chars)
        throws IOException {
        ByteBuffer buf = StandardCharsets.UTF_8.encode(CharBuffer.wrap(chars));
        byte[] array = new byte[buf.limit()];
        buf.get(array);
        return array;
    }

    /**
     * Returns the serialized (as NSON) size of a FieldValue
     * @param value the value
     * @return the size in bytes
     */
    public static int getSerializedSize(FieldValue value) {

        try (ByteOutputStream out =
             NettyByteOutputStream.createNettyByteOutputStream()) {
            writeFieldValue(out, value);
            int ret = out.getOffset();
            return ret;
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Can't serialize field value: " + ioe.getMessage());
        }
    }

    /**
     * Serializes a generic FieldValue into the output stream
     * @param out the output stream
     * @param value the value
     * @throws IOException if there is a problem
     */
    public static void writeFieldValue(ByteOutputStream out,
                                       FieldValue value)
        throws IOException {

        NsonSerializer ns = new NsonSerializer(out);
        FieldValueEventHandler.generate(value, ns);
    }

    /**
     * Serializes a byte array into the NSON output stream
     * @param out the output stream
     * @param array the byte array
     * @throws IOException if there is a problem
     */
    public static void writeByteArray(ByteOutputStream out,
                                      byte[] array) throws IOException {
        SerializationUtil.writeByteArray(out, array);
    }

    /**
     * Serializes a byte array into the NSON output stream using a specified
     * offset and length into the byte array
     * @param out the output stream
     * @param array the byte array
     * @param offset the byte offset into the array to use to start
     * @param length the number of bytes to serialize, 0 means no limit
     * @throws IOException if there is a problem
     */
    public static void writeByteArray(ByteOutputStream out,
                                      byte[] array,
                                      int offset,
                                      int length) throws IOException {
        SerializationUtil.writeByteArray(out, array, offset, length);
    }

    /*
     * Writes a byte array with a full 4-byte int length
     */
    public static void writeByteArrayWithInt(ByteOutputStream out,
                                             byte[] array)
        throws IOException {
        out.writeInt(array.length);
        out.write(array);
    }

    /*
     * Deserialization
     * These are public to allow direct use by tests and specialized
     * applications.
     */

    /*
     * Primitives
     */
    static public int readInt(ByteInputStream in) throws IOException {
        return SerializationUtil.readPackedInt(in);
    }

    static public long readLong(ByteInputStream in) throws IOException {
        return SerializationUtil.readPackedLong(in);
    }

    static public double readDouble(ByteInputStream in) throws IOException {
        return in.readDouble();
    }

    static public String readString(ByteInputStream in) throws IOException {
        return SerializationUtil.readString(in);
    }

    static public byte[] readByteArray(ByteInputStream in)
        throws IOException {
        return SerializationUtil.readByteArray(in);
    }

    static public byte[] readByteArray(ByteInputStream in, boolean skip)
        throws IOException {
        return SerializationUtil.readByteArray(in, skip);
    }

    static public int[] readIntArray(ByteInputStream in)
        throws IOException {
        return SerializationUtil.readPackedIntArray(in);
    }

    /**
     * Reads a byte array that has a not-packed integer size
     *
     * @param in the input stream
     * @return a new byte array
     * @throws IOException if there's a problem with the stream
     */
    static public byte[] readByteArrayWithInt(ByteInputStream in)
        throws IOException {
        int length = in.readInt();
        if (length <= 0) {
            throw new IOException(
                "Invalid length for byte array: " + length);
        }
        byte[] query = new byte[length];
        in.readFully(query);
        return query;
    }

    /**
     * Returns the JSON string of given bytes in NSON with default
     * serialization behavior.
     *
     * @param nson bytes in NSON
     * @return the JSON string
     * @throws IllegalArgumentException if there's a problem with serializing
     * NSON to JSON string
     */
    public static String toJsonString(byte[] nson) {
        return toJsonString(nson, null);
    }

    /**
     * Returns the JSON string of given bytes in NSON.
     *
     * @param nson bytes in NSON
     * @param options {@link JsonOptions} to use for the serialization, or
     * null for default behavior.
     * @return the JSON string
     * @throws IllegalArgumentException if there's a problem with serializing
     * NSON to JSON string
     */
    public static String toJsonString(byte[] nson, JsonOptions options) {
        if (nson == null) {
            return null;
        }
        JsonSerializer jsonSerializer = new JsonSerializer(options);
        try (NettyByteInputStream bis =
             NettyByteInputStream.createFromBytes(nson)) {
            Nson.generateEventsFromNson(jsonSerializer, bis, false);
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Error serializing NSON to JSON: " + ioe.getMessage());
        }
        return jsonSerializer.toString();
    }

    /**
     * Returns the JSON string of given input stream of NSON bytes with default
     * serialization behavior. The input stream will be closed after serializing
     * NSON to JSON string.
     *
     * @param in the input stream of NSON bytes
     * @return the JSON string
     * @throws IllegalArgumentException if there's a problem with serializing
     * NSON to JSON string
     */
    public static String toJsonString(ByteInputStream in) {
        return toJsonString(in, null);
    }

    /**
     * Returns the JSON string of given input stream of NSON bytes. The input
     * stream will be closed after serializing NSON to JSON string.
     *
     * @param in the input stream of NSON bytes
     * @param options {@link JsonOptions} to use for the serialization, or
     * null for default behavior.
     * @return the JSON string
     * @throws IllegalArgumentException if there's a problem with serializing
     * NSON to JSON string
     */
    public static String toJsonString(ByteInputStream in,
                                      JsonOptions options) {
        if (in == null) {
            return null;
        }
        JsonSerializer jsonSerializer = (options != null &&
                                         options.getPrettyPrint()) ?
            new JsonPrettySerializer(options) : new JsonSerializer(options);
        try {
            Nson.generateEventsFromNson(jsonSerializer, in, false);
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Error serializing NSON to JSON: " + ioe.getMessage());
        } finally {
            in.close();
        }
        return jsonSerializer.toString();
    }

    /**
     * An instance of {@link FieldValueEventHandler} that accepts events
     * and adds them to a {@link ByteOutputStream} in NSON format.
     * @hide-in-sdk
     */
    public static class NsonSerializer implements FieldValueEventHandler {

        private final ByteOutputStream out;

        /*
         * Stack used to store offsets for maps and arrays for tracking
         * number of bytes used for a map or array.
         */
        private final Stack<Integer> offsetStack;
        /*
         * Stack used to store sizes for maps and arrays for tracking number of
         * elements in a map or array.
         */
        private final Stack<Integer> sizeStack;

        public NsonSerializer(ByteOutputStream out) {
            this.out = out;
            offsetStack = new Stack<Integer>();
            sizeStack = new Stack<Integer>();
        }

        /**
         * Returns the underlying stream
         * @return the stream
         */
        public ByteOutputStream getStream() {
            return out;
        }

        /*
         * Maps and Arrays. These objects start with their total length,
         * allowing them to be optionally skipped on deserialization.
         *  1. start:
         *    make a 4-byte space for the ultimate length of the serialized
         *    object.
         *  2. save the offset on a stack
         *  3. start counting elements on a stack
         *  4. ... entries are written
         *  5. end:
         *    a. pop the offset stack to get the original length offset
         *    write the real length into the spot that was held
         *    b. pop the size stack to get the number of elements
         *    write the real number of elements the spot that was held
         * NOTE: a full 4-byte integer is used to avoid the variable-length
         * encoding used by compressed integers.
         *
         * It would be more efficient and avoid an extra stack with pop/push
         * for each map/array element to rely on the size from the caller
         * but counting elements here is safer and doesn't rely on the caller
         * having access to the size information. For example, a caller may be
         * turning a List (via iterator) into an array. That is less likely
         * for a Map but it's simplest to keep them the same. Alternatively
         * the caller could track number of elements and send it correctly in
         * the end* calls but again, that relies on the caller.
         */
        @Override
        public void startMap(int size) throws IOException {
            out.writeByte(TYPE_MAP);
            int lengthOffset = out.getOffset();
            out.writeInt(0); // size in bytes
            out.writeInt(0); // number of elements
            offsetStack.push(lengthOffset);
            sizeStack.push(0);
        }

        @Override
        public void startArray(int size) throws IOException {
            out.writeByte(TYPE_ARRAY);
            int lengthOffset = out.getOffset();
            out.writeInt(0); // size in bytes
            out.writeInt(0); // number of elements
            offsetStack.push(lengthOffset);
            sizeStack.push(0);
        }

        @Override
        public void endMap(int size) throws IOException {
            int lengthOffset = offsetStack.pop();
            int numElems = sizeStack.pop();
            int start = lengthOffset + 4;
            /*
             * write size in bytes, then number of elements into the space
             * reserved
             */
            out.writeIntAtOffset(lengthOffset, out.getOffset() - start);
            out.writeIntAtOffset(lengthOffset + 4, numElems);
        }

        @Override
        public void endArray(int size) throws IOException {
            int lengthOffset = offsetStack.pop();
            int numElems = sizeStack.pop();
            int start = lengthOffset + 4;
            /*
             * write size in bytes, then number of elements into the space
             * reserved
             */
            out.writeIntAtOffset(lengthOffset, out.getOffset() - start);
            out.writeIntAtOffset(lengthOffset + 4, numElems);
        }

        @Override
        public boolean startMapField(String key) throws IOException {
            writeString(out, key);
            return false; /* don't skip */
        }

        @Override
        public void endMapField(String key) throws IOException {
            /* add one to number of elements */
            incrSize();
        }

        @Override
        public void endArrayField(int index) throws IOException {
            /* add one to number of elements */
            incrSize();
        }

        @Override
        public void booleanValue(boolean value) throws IOException {
            out.writeByte(TYPE_BOOLEAN);
            out.writeBoolean(value);
        }

        @Override
        public void binaryValue(byte[] byteArray) throws IOException {
            out.writeByte(TYPE_BINARY);
            writeByteArray(out, byteArray);
        }

        @Override
        public void binaryValue(byte[] byteArray,
                                int offset,
                                int length) throws IOException {
            out.writeByte(TYPE_BINARY);
            writeByteArray(out, byteArray, offset, length);
        }

        @Override
        public void stringValue(String value) throws IOException {
            out.writeByte(TYPE_STRING);
            writeString(out, value);
        }

        @Override
        public void integerValue(int value) throws IOException {
            out.writeByte(TYPE_INTEGER);
            writeInt(out, value);
        }

        @Override
        public void longValue(long value) throws IOException {
            out.writeByte(TYPE_LONG);
            writeLong(out, value);
        }

        @Override
        public void doubleValue(double value) throws IOException {
            out.writeByte(TYPE_DOUBLE);
            writeDouble(out, value);
        }

        @Override
        public void numberValue(BigDecimal value) throws IOException {
            out.writeByte(TYPE_NUMBER);
            writeString(out, value.toString());
        }

        @Override
        public void timestampValue(TimestampValue timestamp)
            throws IOException {

            out.writeByte(TYPE_TIMESTAMP);
            writeTimestamp(out, timestamp);
        }

        @Override
        public void jsonNullValue() throws IOException {
            out.writeByte(TYPE_JSON_NULL);
        }

        @Override
        public void nullValue() throws IOException {
            out.writeByte(TYPE_NULL);
        }

        @Override
        public void emptyValue() throws IOException {
            out.writeByte(TYPE_EMPTY);
        }

        private void incrSize() {
            int value = sizeStack.pop();
            sizeStack.push(value + 1);
        }

        /**
         * Increase current size for map or array with given delta. It
         * can be used after adding elements to output stream directly
         * without generating events.
         * @param delta size of delta to increase
         */
        public void incrSize(int delta) {
            int value = sizeStack.pop();
            sizeStack.push(value + delta);
        }
    }

    /**
     * Read the protocol input stream and send events to a handler that
     * creates a FieldValue.
     * @param in the input stream
     * @return the value
     * @throws IOException if there is a problem
     */
    public static FieldValue readFieldValue(ByteInputStream in)
        throws IOException {
        FieldValueCreator handler = new FieldValueCreator();
        generateEventsFromNson(handler, in, false);
        /*
         * Results accumulated in the handler
         */
        return handler.getCurrentValue();
    }

    /**
     * Reads NSON from the input stream and generates events calling the
     * provided {@link FieldValueEventHandler} instance.
     *
     * @param handler the event handler. This can be null if skip is true
     * and the intent is to skip the current field
     * @param in the input stream that holds the NSON
     * @param skip true if the next field should be skipped
     * @throws IOException if there is a problem reading the input
     */
    public static void generateEventsFromNson(
        FieldValueEventHandler handler, ByteInputStream in, boolean skip)
        throws IOException {

        if (handler == null && !skip) {
            throw new IllegalArgumentException(
                "Handler must be non-null if not skipping");
        }

        if (handler != null && handler.stop()) {
            return;
        }
        int t = in.readByte();
        switch (t) {
        case TYPE_ARRAY:
            int length = in.readInt(); // length of serialized bytes
            if (skip) {
                in.skip(length);
            } else {
                int numElements = in.readInt();
                handler.startArray(numElements);
                if (handler.stop()) {
                    return;
                }
                for (int i = 0; i < numElements; i++) {
                    skip = handler.startArrayField(i);
                    if (handler.stop()) {
                        return;
                    }
                    generateEventsFromNson(handler, in, skip);
                    if (handler.stop()) {
                        return;
                    }
                    handler.endArrayField(i);
                    if (handler.stop()) {
                        return;
                    }
                }
                /* always call endArray, even if stopping */
                handler.endArray(numElements);
            }
            break;
        case TYPE_BINARY:
            byte[] byteVal = readByteArray(in, skip);
            if (!skip) {
                handler.binaryValue(byteVal);
            }
            break;
        case TYPE_BOOLEAN:
            boolean bval = in.readBoolean();
            if (!skip) {
                handler.booleanValue(bval);
            }
            break;
        case TYPE_DOUBLE:
            double dval = readDouble(in);
            if (!skip) {
                handler.doubleValue(dval);
            }
            break;
        case TYPE_INTEGER:
            int ival = readInt(in);
            if (!skip) {
                handler.integerValue(ival);
            }
            break;
        case TYPE_LONG:
            long lval = readLong(in);
            if (!skip) {
                handler.longValue(lval);
            }
            break;
        case TYPE_MAP:
            /* NOTE: length in bytes includes the 4 bytes for numelements */
            length = in.readInt(); // length of serialized bytes
            if (skip) {
                in.skip(length);
            } else {
                int numElements = in.readInt(); // size of map
                handler.startMap(numElements);
                if (handler.stop()) {
                    return;
                }
                for (int i = 0; i < numElements; i++) {
                    String key = readString(in);
                    boolean skipField = handler.startMapField(key);
                    if (handler.stop()) {
                        return;
                    }
                    generateEventsFromNson(handler, in, skipField);
                    if (handler.stop()) {
                        return;
                    }
                    handler.endMapField(key);
                    if (handler.stop()) {
                        return;
                    }
                }
                handler.endMap(numElements);
            }
            break;
        case TYPE_STRING:
            String sval = readString(in);
            if (!skip) {
                handler.stringValue(sval);
            }
            break;
        case TYPE_TIMESTAMP:
            String tval = readString(in);
            if (!skip) {
                handler.timestampValue(new TimestampValue(tval, 1));
            }
            break;
        case TYPE_NUMBER:
            String nval = readString(in);
            if (!skip) {
                handler.numberValue(new BigDecimal(nval));
            }
            break;
        case TYPE_JSON_NULL:
            if (!skip) {
                handler.jsonNullValue();
            }
            break;
        case TYPE_NULL:
            if (!skip) {
                handler.nullValue();
            }
            break;
        case TYPE_EMPTY :
            if (!skip) {
                handler.emptyValue();
            }
            break;
        default:
            throw new IllegalStateException("Unknown value type code: " + t);
        }
    }

    /*
     * Methods to read atomic types from the current position in an NSON
     * stream. If types do not match an exception is thrown.
     */

    /**
     * Reads an integer from the {@link ByteInputStream}
     * @param in the input stream
     * @return the value
     * @throws IllegalArgumentException if the type at the stream is not
     * the one expected
     * @throws IOException if there are problems reading the stream
     */
    public static int readNsonInt(ByteInputStream in) throws IOException {
        readType(in, TYPE_INTEGER);
        return readInt(in);
    }

    /**
     * Reads a long from the {@link ByteInputStream}
     * @param in the input stream
     * @return the value
     * @throws IllegalArgumentException if the type at the stream is not
     * the one expected
     * @throws IOException if there are problems reading the stream
     */
    public static long readNsonLong(ByteInputStream in) throws IOException {
        readType(in, TYPE_LONG);
        return readLong(in);
    }

    /**
     * Reads a double from the {@link ByteInputStream}
     * @param in the input stream
     * @return the value
     * @throws IllegalArgumentException if the type at the stream is not
     * the one expected
     * @throws IOException if there are problems reading the stream
     */
    public static double readNsonDouble(ByteInputStream in) throws IOException {
        readType(in, TYPE_DOUBLE);
        return readDouble(in);
    }

    /**
     * Reads a Number as BigDecimal from the {@link ByteInputStream}
     * @param in the input stream
     * @return the value
     * @throws IllegalArgumentException if the type at the stream is not
     * the one expected
     * @throws IOException if there are problems reading the stream
     */
    public static BigDecimal readNsonNumber(ByteInputStream in)
        throws IOException {

        readType(in, TYPE_NUMBER);
        return new BigDecimal(Nson.readString(in));
    }

    /**
     * Reads a boolean from the {@link ByteInputStream}
     * @param in the input stream
     * @return the value
     * @throws IllegalArgumentException if the type at the stream is not
     * the one expected
     * @throws IOException if there are problems reading the stream
     */
    public static boolean readNsonBoolean(ByteInputStream in)
        throws IOException {

        readType(in, TYPE_BOOLEAN);
        return in.readBoolean();
    }

    /**
     * Reads a string from the {@link ByteInputStream}
     * @param in the input stream
     * @return the value
     * @throws IllegalArgumentException if the type at the stream is not
     * the one expected
     * @throws IOException if there are problems reading the stream
     */
    public static String readNsonString(ByteInputStream in)
        throws IOException {

        readType(in, TYPE_STRING);
        return readString(in);
    }

    /**
     * Reads a byte[] from the {@link ByteInputStream}
     * @param in the input stream
     * @return the value
     * @throws IllegalArgumentException if the type at the stream is not
     * the one expected
     * @throws IOException if there are problems reading the stream
     */
    public static byte[] readNsonBinary(ByteInputStream in)
        throws IOException {

        readType(in, TYPE_BINARY);
        return readByteArray(in);
    }

    /**
     * Reads a MapValue from the {@link ByteInputStream}
     * @param in the input stream
     * @return the value
     * @throws IllegalArgumentException if the type at the stream is not
     * the one expected
     * @throws IOException if there are problems reading the stream
     */
    public static MapValue readNsonMap(ByteInputStream in)
        throws IOException {

        /* cast must work */
        return (MapValue) readFieldValue(in);
    }

    private static void readType(ByteInputStream in, int expected)
        throws IOException {

        int type = in.readByte();
        if (type != expected) {
            throwTypeMismatch(expected, type);
        }
    }

    private static void throwTypeMismatch(int expected, int found) {
        throw new IllegalArgumentException(
            "Expected type not found, expected type: " + typeString(expected) +
            ", found type: " + typeString(found));
    }

    public static String typeString(int type) {
        if (type < 0 || type >= (NsonTypes.length - 1)) {
            return ("Unknown type: " + type);
        }
        return NsonTypes[type];
    }
}
