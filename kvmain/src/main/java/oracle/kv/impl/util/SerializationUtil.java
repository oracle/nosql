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

import static java.nio.charset.StandardCharsets.UTF_8;

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.sleepycat.util.PackedInteger;

/**
 * Utility methods to facilitate serialization/deserialization
 */
public class SerializationUtil {

    /**
     * The name of the system property that controls the maximum sequence
     * length.  The value of the system property should be a valid positive
     * integer.  The methods in this file for reading sequences, whether
     * arrays, collections, or strings, will throw an IOException if the
     * serialized input attempts to create an object with more elements than
     * the maximum value.
     */
    public static final String MAX_SEQUENCE_LENGTH_PROPERTY =
        "oracle.kv.serial.max.sequence.length";

    /** The default maximum sequence length. */
    private static final int DEFAULT_MAX_SEQUENCE_LENGTH = Integer.MAX_VALUE;

    /** The maximum sequence length. */
    public static volatile int maxSequenceLength = Integer.getInteger(
        MAX_SEQUENCE_LENGTH_PROPERTY, DEFAULT_MAX_SEQUENCE_LENGTH);

    public static final String EMPTY_STRING = new String();

    public static final byte[] EMPTY_BYTES = { };

    private static final char[] EMPTY_CHARS = { };

    /** The TimeUnit values, for read-only use. */
    private static final TimeUnit[] TIMEUNIT_VALUES = TimeUnit.values();

    /**
     * The size of cached thread-local byte arrays used to avoid unnecessary
     * allocation, at least 8 to accommodate a long.
     */
    public static final int LOCAL_BUFFER_SIZE = 2048;

    /**
     * A thread-local byte array to use as a temporary buffer. Only pass the
     * buffer to other calls if you know for sure that the value will not be
     * used after the call returns.
     */
    private static final ThreadLocal<byte[]> localBuffer =
        ThreadLocal.withInitial(() -> new byte[LOCAL_BUFFER_SIZE]);

    /**
     * Returns the object de-serialized from the specified byte array.
     * The object is de-serialized using Java serialization.
     *
     * @param <T> the type of the deserialized object
     *
     * @param bytes the serialized bytes
     * @param oclass the class associated with the deserialized object
     *
     * @return the deserialized object
     */
    public static <T> T getObject(byte[] bytes, Class<T> oclass) {

        if (bytes == null) {
            return null;
        }

        try (final ObjectInputStream ois =
             new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return oclass.cast(ois.readObject());
        } catch (IOException | ClassNotFoundException ioe) {
            throw new IllegalStateException("Exception deserializing object: " +
                                            oclass.getName(), ioe);
        }
    }

    /**
     * Returns a byte array containing the serialized form of this object.
     * The object is serialized using Java serialization.
     *
     * @param object the object to be serialized
     *
     * @return the serialized bytes
     */
    public static byte[] getBytes(Object object) {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(object);
            return baos.toByteArray();
        } catch (IOException ioe) {
            throw new IllegalStateException("Exception serializing object: " +
                                            object.getClass().getName(),
                                            ioe);
        }
    }

    /**
     * Reads a Java serialized object, which may be null, from the input
     * stream.
     *
     * @param <T> the type of the deserialized object
     * @param in the input stream
     * @param oclass the class of the deserialized object
     * @return the deserialized object
     * @throws ClassCastException if the serialized object is an instance of a
     * different specified class
     * @throws IllegalStateException if the class of a serialized object
     * cannot be found or there are problems with the serialized form
     * @throws IOException if an I/O error occurs
     */
    public static <T> T readJavaSerial(DataInput in, Class<T> oclass)
        throws IOException
    {
        return getObject(readByteArray(in), oclass);
    }

    /**
     * Writes an object in Java serialization format to the output stream.
     *
     * @param out the output stream
     * @param object the object
     * @throws IllegalStateException if there are problems with converting the
     * object to Java serialized form
     * @throws IOException if an I/O error occurs
     */
    public static void writeJavaSerial(DataOutput out, Object object)
        throws IOException
    {
        writeByteArray(out, getBytes(object));
    }

    /**
     * Returns a byte array containing the specified serial version and the
     * serialized form of the FastExternalizable object.
     *
     * <p>Format:
     * <ol>
     * <li> ({@code short}) <i>serial version</i>
     * <li> ({@link FastExternalizable#writeFastExternal}) <i>object</i>
     * </ol>
     *
     * @param object the object to be serialized
     * @param serialVersion the version to be used for serialization
     *
     * @return the serialized bytes
     */
    public static byte[] getBytes(FastExternalizable object,
                                  short serialVersion) {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeShort(serialVersion);
            object.writeFastExternal(dos, serialVersion);
            return baos.toByteArray();
        } catch (IOException ioe) {
            throw new IllegalStateException("Exception serializing object: " +
                                            object.getClass().getName(), ioe);
        }
    }

    /**
     * Reads a packed integer from the input and returns it.
     *
     * @param in the data input
     * @return the integer that was read
     */
    public static int readPackedInt(DataInput in) throws IOException {
        final byte[] bytes = localBuffer.get();
        bytes[0] = in.readByte();
        final int len = PackedInteger.getReadIntLength(bytes, 0);
        if (len > PackedInteger.MAX_LENGTH) {
            throw new IOException("Invalid packed int length: " + len);
        }
        try {
            in.readFully(bytes, 1, len - 1);
        } catch (IndexOutOfBoundsException e) {
            throw new IOException("Invalid packed int", e);
        }
        return PackedInteger.readInt(bytes, 0);
    }

    /**
     * Writes a packed integer to the output.
     *
     * @param out the data output
     * @param value the integer to be written
     */
    public static void writePackedInt(DataOutput out, int value)
            throws IOException {
        final byte[] buf = localBuffer.get();
        final int offset = PackedInteger.writeInt(buf, 0, value);
        out.write(buf, 0, offset);
    }

    /**
     * Reads a packed long from the input and returns it.
     *
     * @param in the data input
     * @return the long that was read
     */
    public static long readPackedLong(DataInput in) throws IOException {
        final byte[] bytes = localBuffer.get();
        bytes[0] = in.readByte();
        final int len = PackedInteger.getReadLongLength(bytes, 0);
        if (len > PackedInteger.MAX_LONG_LENGTH) {
            throw new IOException("Invalid packed long length: " + len);
        }
        try {
            in.readFully(bytes, 1, len - 1);
        } catch (IndexOutOfBoundsException e) {
            throw new IOException("Invalid packed long", e);
        }
        return PackedInteger.readLong(bytes, 0);
    }

    /**
     * Writes a packed long to the output.
     *
     * @param out the data output
     * @param value the long to be written
     */
    public static void writePackedLong(DataOutput out, long value)
            throws IOException {
        final byte[] buf = localBuffer.get();
        final int offset = PackedInteger.writeLong(buf, 0, value);
        out.write(buf, 0, offset);
    }

    /**
     * Reads a string written by {@link #writeString}, using standard UTF-8.
     *
     * @param in the input stream
     * @param serialVersion the version of the serialization format
     * @return a string or null
     * @throws IOException if an I/O error occurs, if the input UTF-8 encoding
     * is invalid, or if the String length is too large
     */
    public static String readString(DataInput in, short serialVersion)
        throws IOException {
        return readStdUTF8String(in);
    }

    /**
     * Reads a non-null string written by {@link #writeNonNullString}, using
     * standard UTF-8 or Java's modified UTF-8 format, depending on the serial
     * version.
     *
     * @param in the input stream
     * @param serialVersion the version of the serialization format
     * @return a string
     * @throws IOException if an I/O error occurs, if the input represents a
     * null value, if the input UTF-8 encoding is invalid, or if the String
     * length is too large
     */
    public static String readNonNullString(DataInput in, short serialVersion)
        throws IOException {

        final String result = readString(in, serialVersion);
        if (result == null) {
            throw new IOException("Found null value for non-null string");
        }
        return result;
    }

    /**
     * Writes a string for reading by {@link #readString}, using standard
     * UTF-8. The string may be null or empty. This code differentiates between
     * the two, maintaining the ability to round-trip null and empty string
     * values.
     *
     * <p>This format is used rather than that of {@link DataOutput#writeUTF}
     * to allow packing of the size of the string. For shorter strings this
     * size savings is a significant percentage of the space used.
     *
     * <p>To support non-Java interoperability, the UTF-8 bytes are written
     * using the standard UTF-8 format documented by <a
     * href="http://www.ietf.org/rfc/rfc2279.txt">RFC 2279</a> and implemented
     * by the {@link Charset} class using the "UTF-8" standard charset.
     *
     * <p>Format:
     * <ol>
     * <li> ({@link #writePackedInt packed int}) <i>string length, or -1 for
     *      null</i>
     * <li> <i>[Optional]</i> ({@code byte[]}) <i>UTF-8 bytes</i>
     * </ol>
     *
     * @param out the output stream
     * @param serialVersion the version of the serialization format
     * @param value the string or null
     * @throws IOException if an I/O error occurs
     */
    public static void writeString(DataOutput out,
                                   short serialVersion,
                                   String value)
        throws IOException {

        writeStdUTF8String(out, value);
    }

    /**
     * Writes a non-null string for reading by {@link #readNonNullString},
     * using the same format as {@link #writeString}, but not permitting a null
     * value to be written.
     *
     * @param out the output stream
     * @param serialVersion the version of the serialization format
     * @param value the string
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if {@code value} is {@code null}
     */
    public static void writeNonNullString(DataOutput out,
                                          short serialVersion,
                                          String value)
        throws IOException {

        ObjectUtil.checkNull("value", value);
        writeString(out, serialVersion, value);
    }

    /** Throws an IOException if length is greater than maxSequenceLength. */
    private static void checkMaxSequenceLength(int length) throws IOException {
        if (length > maxSequenceLength) {
            throw new IOException("Sequence length " + length +
                                  " is greater than maximum sequence length " +
                                  maxSequenceLength);
        }
    }

    /**
     * Returns a byte buffer of at least the requested length, possibly a
     * shared one. Only use this method if the byte buffer will not be
     * retained, or if the buffer will only be retained if its length is
     * greater than LOCAL_BUFFER_SIZE.
     */
    private static byte[] getByteBuffer(int len) {
        if (len > LOCAL_BUFFER_SIZE) {
            return new byte[len];
        }
        return localBuffer.get();
    }

    /* -- The following methods are cribbed from JE's UtfOps and the JDK -- */

    /**
     * Writes a possibly null or empty string to an output stream using
     * standard UTF-8 format.
     *
     * <p>First writes a {@link #writePackedInt packedInt} representing the
     * length of the UTF-8 encoding of the string, or {@code -1} if the string
     * is null, followed by the UTF-8 encoding for non-empty strings.
     *
     * @param out the output stream
     * @param value the string or null
     * @throws IOException if an I/O error occurs
     */
    private static void writeStdUTF8String(DataOutput out, String value)
        throws IOException {

        if (value == null) {
            writePackedInt(out, -1);
            return;
        }
        final ByteBuffer buffer = UTF_8.encode(value);
        final int length = buffer.limit();
        writePackedInt(out, length);
        if (length > 0) {
            out.write(buffer.array(), 0, length);
        }
    }

    /**
     * Reads a possibly null string from an input stream in standard UTF-8
     * format.
     *
     * <p>First reads a {@link #readPackedInt packedInt} representing the
     * length of the UTF-8 encoding of the string, or a negative value for
     * null, followed by the string contents in UTF-8 format for a non-empty
     * string, if any.
     *
     * @param in the input stream
     * @return the string
     * @throws IOException if an I/O error occurs, if the input UTF-8 encoding
     * is invalid, or if the String length is too large
     */
    private static String readStdUTF8String(DataInput in)
        throws IOException {

        final int length = readPackedInt(in);
        if (length < 0) {
            return null;
        }
        if (length == 0) {
            return EMPTY_STRING;
        }
        checkMaxSequenceLength(length);
        final byte[] bytes = getByteBuffer(length);
        in.readFully(bytes, 0, length);
        return UTF_8.decode(ByteBuffer.wrap(bytes, 0, length)).toString();
    }

    /**
     * Reads the length of a possibly null sequence.  The length is represented
     * as a {@link #readPackedInt packed int}, with -1 interpreted as meaning
     * null, and other negative values not permitted.
     *
     * @param in the input stream
     * @return the sequence length or -1 for null
     * @throws IOException if an I/O error occurs, if the input format is
     * invalid, or if the sequence length is too large
     */
    public static int readSequenceLength(DataInput in)
        throws IOException {

        final int result = readPackedInt(in);
        if (result < -1) {
            throw new IOException("Invalid sequence length: " + result);
        }
        checkMaxSequenceLength(result);
        return result;
    }

    /**
     * Reads the length of a non-null sequence.  The length is represented as a
     * non-negative {@link #readPackedInt packed int}.
     *
     * @param in the input stream
     * @return the sequence length
     * @throws IOException if an I/O error occurs, if the input represents a
     * null sequence, if the input format is invalid, or if the sequence length
     * is too large
     */
    public static int readNonNullSequenceLength(DataInput in)
        throws IOException {

        final int length = readSequenceLength(in);
        if (length == -1) {
            throw new IOException("Read null length for non-null sequence");
        }
        return length;
    }

    /**
     * Writes the size of a possibly null collection.  The length is
     * represented as a {@link #readPackedInt packed int}, with -1 representing
     * null.  Although we don't enforce maximum sequence lengths yet, this
     * entrypoint provides a place to do that.
     *
     * @param out the output stream
     * @param collection the collection or null
     * @throws IOException if an I/O error occurs
     */
    public static void writeCollectionLength(DataOutput out,
                                             Collection<?> collection)
        throws IOException {

        writeSequenceLength(
            out, (collection != null) ? collection.size() : -1);
    }

    /**
     * Writes the size of a possibly null map.  The length is
     * represented as a {@link #readPackedInt packed int}, with -1 representing
     * null. Although we don't enforce maximum sequence lengths yet, this
     * entrypoint provides a place to do that.
     *
     * @param out the output stream
     * @param map the map or null
     * @throws IOException if an I/O error occurs
     */
    public static void writeMapLength(DataOutput out, Map<?, ?> map)
        throws IOException {

        writeSequenceLength(out, (map != null) ? map.size() : -1);
    }

    /**
     * Writes the length of a possibly null array.  The length is represented
     * as a {@link #readPackedInt packed int}, with -1 representing null.
     * Although we don't enforce maximum sequence lengths yet, this entrypoint
     * provides a place to do that.
     *
     * @param out the output stream
     * @param array the array or null
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if {@code array} is not an array or
     * {@code null}
     */
    public static void writeArrayLength(DataOutput out, Object array)
        throws IOException {

        writeSequenceLength(
            out, (array != null) ? Array.getLength(array) : -1);
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
     * @return the array or null
     * @throws IOException if an I/O error occurs, if the input format is
     * invalid, or if the array length is too large
     */
    public static byte[] readByteArray(DataInput in)
        throws IOException {

        final int len = readSequenceLength(in);
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
        writeArrayLength(out, array);
        if (length > 0) {
            out.write(array);
        }
    }

    /**
     * Reads a non-null byte array as a {@link #readNonNullSequenceLength
     * non-null sequence length} followed by the array contents.
     *
     * @param in the input stream
     * @return the array
     * @throws IOException if an I/O error occurs, if the input represents a
     * null array, if the input format is invalid, or if the array length is
     * too large
     */
    public static byte[] readNonNullByteArray(DataInput in)
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
     * Serializes a non-null collection of non-null FastExternalizable elements
     * as a {@link #writeNonNullSequenceLength non-null sequence length}
     * followed by the serialized forms of the elements.
     *
     * @param out the output stream
     * @param serialVersion the version of the serialization format
     * @param collection the collection
     * @throws IOException if an I/O error occurs
     */
    public static void writeNonNullCollection(
        DataOutput out,
        short serialVersion,
        Collection<? extends FastExternalizable> collection)
        throws IOException {

        checkNull("collection", collection);
        writeNonNullSequenceLength(out, collection.size());
        for (final FastExternalizable e : collection) {
            checkNull("collection element", e);
            e.writeFastExternal(out, serialVersion);
        }
    }

    /**
     * Writes a possibly null collection of possibly null {@link
     * FastExternalizable} elements as a {@link #writeSequenceLength collection
     * size} followed by the collection elements written by {@link
     * #writeFastExternalOrNull}, if any.
     *
     * @param out the output stream
     * @param serialVersion the version of the serialization format
     * @param collection the collection or null
     * @throws IOException if an I/O error occurs
     */
    public static void
        writeCollection(DataOutput out,
                        short serialVersion,
                        Collection<? extends FastExternalizable> collection)
        throws IOException
    {
        writeCollection(out, serialVersion, collection,
                        FastExternalizable::writeFastExternal);
    }

    /**
     * Writes a possibly null collection of arbitrary elements as a {@link
     * #writeSequenceLength collection size} followed by the collection
     * contents, if any, calling {@link #writeFastExternalOrNull(DataOutput,
     * short, Object, WriteFastExternal)} using {@code elementWriter} to write
     * each element.
     *
     * @param <E> the type of collection elements
     * @param out the output stream
     * @param serialVersion the version of the serialization format
     * @param collection the collection or null
     * @param elementWriter called to write each non-null element
     * @throws IOException if an I/O error occurs
     */
    public static <E> void writeCollection(DataOutput out,
                                           short serialVersion,
                                           Collection<E> collection,
                                           WriteFastExternal<E> elementWriter)
        throws IOException
    {
        writeCollectionLength(out, collection);
        if (collection != null) {
            for (final E element : collection) {
                writeFastExternalOrNull(out, serialVersion, element,
                                        elementWriter);
            }
        }
    }

    /**
     * Reads a possibly null collection of arbitrary elements as a {@link
     * #readSequenceLength collection size} followed by the collection
     * contents, if any. Calls {@code createCollection} to create the
     * collection, and calls {@link #readFastExternalOrNull} with {@code
     * elementReader} to read each element.
     *
     * @param <E> the type of the collection elements
     * @param <C> the type of the collection
     * @param in the input stream
     * @param serialVersion the serial version
     * @param createCollection called to create the collection
     * @param elementReader called to read a non-null collection element
     * @return the collection or null
     * @throws IOException if an I/O error occurs, if the input format is
     * invalid, or if the collection size is too large
     */
    public static <E, C extends Collection<E>> C
        readCollection(DataInput in,
                       short serialVersion,
                       Supplier<C> createCollection,
                       ReadFastExternal<E> elementReader)
        throws IOException
    {
        final int length = readSequenceLength(in);
        if (length < 0) {
            return null;
        }
        final C collection = createCollection.get();
        for (int i = 0; i < length; i++) {
            collection.add(readFastExternalOrNull(in, serialVersion,
                                                  elementReader));
        }
        return collection;
    }

    /**
     * Writes a possibly null map with possibly null {@link FastExternalizable}
     * values as a {@link #writeSequenceLength map size} followed by pairs of
     * keys and values, if any. Uses {@code keyWriter} to write each key and
     * {@link #writeFastExternalOrNull} to write each value.
     *
     * @param out the output stream
     * @param serialVersion the version of the serialization format
     * @param map the map or null
     * @param keyWriter called to write each key
     * @throws IOException if an I/O error occurs
     */
    public static <K, V extends FastExternalizable>
        void writeMap(DataOutput out,
                      short serialVersion,
                      Map<? extends K, ? extends V> map,
                      WriteFastExternal<K> keyWriter)
        throws IOException
    {
        writeMap(out, serialVersion, map, keyWriter,
                 FastExternalizable::writeFastExternal);
    }

    /**
     * Writes a possibly null map with arbitrary values as a {@link
     * #writeSequenceLength map size} followed by pairs of keys and values, if
     * any. Uses {@code keyWriter} to write each key and {@link
     * #writeFastExternalOrNull(DataOutput, short, Object, WriteFastExternal)}
     * with {@code valueWriter} to write each value.
     *
     * @param out the output stream
     * @param serialVersion the version of the serialization format
     * @param map the map or null
     * @param keyWriter called to write each key
     * @param valueWriter called to write each value
     * @throws IOException if an I/O error occurs
     */
    public static <K, V> void writeMap(DataOutput out,
                                       short serialVersion,
                                       Map<? extends K, ? extends V> map,
                                       WriteFastExternal<K> keyWriter,
                                       WriteFastExternal<V> valueWriter)
        throws IOException
    {
        if (map == null) {
            writeCollectionLength(out, null);
            return;
        }
        writeSequenceLength(out, map.size());
        for (final Entry<? extends K, ? extends V> entry : map.entrySet()) {
            keyWriter.writeFastExternal(entry.getKey(), out, serialVersion);
            writeFastExternalOrNull(out, serialVersion, entry.getValue(),
                                    valueWriter);
        }
    }

    /**
     * Reads a possibly null map with arbitrary elements as a {@link
     * #readSequenceLength map size} followed by pairs of keys and values, if
     * any. Calls {@code createMap} to create the map that should be populated,
     * calls {@code keyReader} to read each key, and calls {@link
     * #readFastExternalOrNull} with {@code valueReader} to read each value.
     *
     * @param <K> the type of map keys
     * @param <V> the type of map values
     * @param <M> the type of the map
     * @param in the input stream
     * @param serialVersion the serial version
     * @param createMap called to create the map
     * @param keyReader called to read each map key
     * @param valueReader called to read each map value
     * @return the map or null
     * @throws IOException if an I/O error occurs, if the input format is
     * invalid, or if the map size is too large
     */
    public static <K, V, M extends Map<K, V>>
        M readMap(DataInput in,
                  short serialVersion,
                  Supplier<M> createMap,
                  ReadFastExternal<K> keyReader,
                  ReadFastExternal<V> valueReader)
        throws IOException
    {
        final int length = readSequenceLength(in);
        if (length < 0) {
            return null;
        }
        final M map = createMap.get();
        for (int i = 0; i < length; i++) {
            final K key = keyReader.readFastExternal(in, serialVersion);
            final V value = readFastExternalOrNull(in, serialVersion,
                                                   valueReader);
            map.put(key, value);
        }
        return map;
    }

    /**
     * Reads a Properties instance as a {@link #readSequenceLength entry count}
     * followed by entries as pairs of key and value strings, if any.
     *
     * @param in the input stream
     * @param serialVersion the serial version
     * @return the properties or null
     * @throws IOException if an I/O error occurs
     */
    public static Properties readProperties(final DataInput in,
                                            final short serialVersion)
        throws IOException
    {
        final int len = readSequenceLength(in);
        if (len < 0) {
            return null;
        }
        final Properties props = new Properties();
        for (int i = 0; i < len; i++) {
            final String key = readNonNullString(in, serialVersion);
            final String value = readNonNullString(in, serialVersion);
            props.setProperty(key, value);
        }
        return props;
    }

    /**
     * Writes a Properties instance as a {@link #readSequenceLength entry
     * count} followed by entries as pairs of key and value strings, if any.
     * Like the {@link Properties#store} method, throws {@link
     * ClassCastException} if the properties object contains any keys or values
     * that are not {@link String}s. Entries from the defaults for the
     * Properties instance are not written.
     *
     * @param out the output stream
     * @param serialVersion the version of the serialization format
     * @param props the properties or null
     * @throws ClassCastException if properties contains any keys or values
     * that are not Strings
     * @throws IOException if an I/O error occurs
     */
    public static void writeProperties(final DataOutput out,
                                       final short serialVersion,
                                       final Properties props)
        throws IOException
    {
        if (props == null) {
            writeSequenceLength(out, -1);
            return;
        }
        final int len = props.size();
        writeSequenceLength(out, len);
        for (final Entry<Object, Object> entry : props.entrySet()) {
            writeNonNullString(out, serialVersion, (String) entry.getKey());
            writeNonNullString(out, serialVersion, (String) entry.getValue());
        }
    }

    /**
     * Serializes a non-null array of non-null {@link FastExternalizable}
     * elements as a {@link #writeNonNullSequenceLength non-null sequence
     * length} followed by the serialized forms of the elements.
     *
     * @param out the output stream
     * @param serialVersion the version of the serialization format
     * @param array the array
     * @throws IOException if an I/O error occurs
     */
    public static void writeNonNullArray(DataOutput out,
                                         short serialVersion,
                                         FastExternalizable[] array)
        throws IOException {

        checkNull("array", array);
        writeNonNullSequenceLength(out, array.length);
        for (final FastExternalizable e : array) {
            e.writeFastExternal(out, serialVersion);
        }
    }

    /**
     * Serializes a FastExternalizable object which may be null.  First stores
     * a boolean, which represents if the object is present, followed, if true,
     * by the data for the object.
     *
     * @param <T> the type of the object
     * @param out the output stream
     * @param serialVersion the version of the serialization format
     * @param object the object or null
     * @throws IOException if there is an I/O error or a problem writing the
     * object
     */
    public static <T extends FastExternalizable>
        void writeFastExternalOrNull(DataOutput out,
                                     short serialVersion,
                                     T object)
        throws IOException {

        if (object != null) {
            out.writeBoolean(true);
            object.writeFastExternal(out, serialVersion);
        } else {
            out.writeBoolean(false);
        }
    }

    /**
     * Serializes an arbitrary object which may be null.  First stores
     * a boolean, which represents if the object is present, followed, if true,
     * by the data for the object provided by the writer.
     *
     * @param <T> the type of the object
     * @param out the output stream
     * @param serialVersion the version of the serialization format
     * @param object the object or null
     * @param writer the method to write the object
     * @throws IOException if there is an I/O error or a problem writing the
     * object
     */
    public static <T>
        void writeFastExternalOrNull(DataOutput out,
                                     short serialVersion,
                                     T object,
                                     WriteFastExternal<T> writer)
        throws IOException {

        if (object != null) {
            out.writeBoolean(true);
            writer.writeFastExternal(object, out, serialVersion);
        } else {
            out.writeBoolean(false);
        }
    }

    /**
     * Deserializes an arbitrary object which may be null. First reads a
     * boolean, which represents if the object is present and, if true, uses
     * the {@link ReadFastExternal} to read the object.
     *
     * @param <T> the type of the object
     * @param in the input stream
     * @param serialVersion the version of the serialization format
     * @param reader the method to read the object
     * @throws IOException if there is an I/O error or a problem reading the
     * object
     */
    public static <T>
        T readFastExternalOrNull(final DataInput in,
                                 final short serialVersion,
                                 final ReadFastExternal<T> reader)
        throws IOException
    {
        return in.readBoolean() ?
            reader.readFastExternal(in, serialVersion) :
            null;
    }

    /**
     * Writes a non-null {@link TimeUnit} to the output stream as a {@link
     * #writePackedInt packed int} using the ordinal value.
     *
     * @param out the output stream
     * @param timeUnit the time unit
     * @throws IOException if there is an I/O error
     */
    public static void writeTimeUnit(DataOutput out,
                                     TimeUnit timeUnit)
        throws IOException {

        checkNull("timeUnit", timeUnit);
        writePackedInt(out, timeUnit.ordinal());
    }

    /**
     * Reads a {@link TimeUnit} from the input stream as a {@link
     * #readPackedInt packed int} using the ordinal value.
     *
     * @param in the input stream
     * @throws IOException if there is an I/O error or a problem with the input
     * format
     */
    public static TimeUnit readTimeUnit(DataInput in)
        throws IOException {

        final int ordinal = readPackedInt(in);
        try {
            return TIMEUNIT_VALUES[ordinal];
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new IOException("Illegal TimeUnit ordinal: " + ordinal);
        }
    }

    /**
     * Reads a char array as a {@link #readSequenceLength sequence length}
     * followed by the array contents. Characters are written as two bytes
     * each, with the high order byte first.
     *
     * @param in the input stream
     * @return the array
     * @throws IOException if an I/O error occurs, if the input format is
     * invalid, or if the array length is too large
     */
    public static char[] readCharArray(final DataInput in) throws IOException {
        final int len = readSequenceLength(in);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return EMPTY_CHARS;
        }
        final char[] array = new char[len];
        for (int i = 0; i < len; i++) {
            array[i] = in.readChar();
        }
        return array;
    }

    /**
     * Writes a char array as a {@link #writeSequenceLength sequence length}
     * followed by the array contents. Characters are written as two bytes
     * each, with the high order byte first.
     *
     * @param out the output stream
     * @param chars the array
     * @throws IOException if an I/O error occurs
     */
    public static void writeCharArray(final DataOutput out,
                                      final char[] chars) throws IOException {
        writeArrayLength(out, chars);
        if (chars != null) {
            for (final char c : chars) {
                out.writeChar(c);
            }
        }
    }

    /**
     * Convenience method to convert an object to its deserialized form and
     * confirm that it has the same type. Just returns the object if it does
     * not implement {@link FastExternalizable}.
     *
     * @param <T> the type of the object
     * @param obj the object
     * @param serialVersion the serial version for serialization and
     * deserialization
     * @return an object of the same type representing the deserialized form
     */
    public static <T> T toDeserializedForm(T obj, short serialVersion) {
        if (!(obj instanceof FastExternalizable)) {
            return obj;
        }

        /*
         * A non-null object of type T has a class of type or extending type
         * Class<T>
         */
        @SuppressWarnings("unchecked")
        final Class<T> cl = (Class<T>) obj.getClass();
        return cl.cast(((FastExternalizable) obj).deserializedForm(
                           serialVersion));
    }
}
