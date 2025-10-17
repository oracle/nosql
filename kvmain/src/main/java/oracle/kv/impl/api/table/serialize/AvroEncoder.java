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
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package oracle.kv.impl.api.table.serialize;

import static oracle.kv.impl.util.SerializationUtil.LOCAL_BUFFER_SIZE;

import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;

import oracle.kv.impl.api.table.serialize.util.Utf8;

/**
 * This is adapted from what was an Avro library abstraction for
 * an Encoder. NoSQL only uses what used to be a BufferedBinaryEncoder
 * so the implementation was simplified and collapsed into a single
 * concrete class.
 */
public class AvroEncoder implements Flushable {
    private byte[] buf;
    private int pos;
    private OutputStreamSink sink;
    private int bulkLimit;
    private int regionId;

    private static final int DEFAULT_BUFFER_SIZE = 2048;
    /** A thread-local byte buffer used by writeString. */
    private static final ThreadLocal<ByteBuffer> stringByteBuffer =
        ThreadLocal.withInitial(() -> ByteBuffer.allocate(LOCAL_BUFFER_SIZE));

    /** A thread-local UTF8 CharsetEncoder used by writeString. */
    private static final ThreadLocal<CharsetEncoder> utf8Encoder =
        ThreadLocal.withInitial(() -> StandardCharsets.UTF_8.newEncoder());

    /**
     * Creates an instance using the default buffer size
     *
     * @param out writes to this output stream
     * @param buffer if non-null, the buffer to use if its size matches the
     * requested buffer size
     */
    public AvroEncoder(OutputStream out,  byte[] buffer) {
        this(out, DEFAULT_BUFFER_SIZE, buffer);
    }

    /**
     * Creates an instance.
     *
     * @param out writes to this output stream
     * @parem bufferSize the buffer size
     * @param buffer if non-null, the buffer to use if its size matches the
     * requested buffer size
     */
    public AvroEncoder(OutputStream out, int bufferSize, byte[] buffer) {
        if (null == out)
            throw new NullPointerException("OutputStream cannot be null!");
        if (null != this.sink) {
            if (pos > 0) {
                try {
                    flushBuffer();
                } catch (IOException e) {
                    throw new RuntimeException("Failure flushing old output",
                            e);
                }
            }
        }
        this.sink = new OutputStreamSink(out);
        pos = 0;
        if (buffer != null) {
            buf = buffer;
        }
        if (null == buf || buf.length != bufferSize) {
            buf = new byte[bufferSize];
        }
        bulkLimit = buf.length >>> 1;
        if (bulkLimit > 512) {
            bulkLimit = 512;
        }
    }

    public static int getDefaultBufferSize() {
        return DEFAULT_BUFFER_SIZE;
    }

    public void setRegionId(int regionId) {
        this.regionId = regionId;
    }

    public int getRegionId() {
        return regionId;
    }

    public boolean createCRDTMap() {
        return regionId != 0;
    }

    @Override
    public void flush() throws IOException {
        flushBuffer();
        sink.innerFlush();
    }

    /**
     * Flushes the internal buffer to the underlying output. Does not flush the
     * underlying output.
     */
    private void flushBuffer() throws IOException {
        if (pos > 0) {
            sink.innerWrite(buf, 0, pos);
            pos = 0;
        }
    }

    /**
     * Ensures that the buffer has at least num bytes free to write to between
     * its current position and the end. This will not expand the buffer larger
     * than its current size, for writes larger than or near to the size of the
     * buffer, we flush the buffer and write directly to the output, bypassing
     * the buffer.
     *
     * @param num
     * @throws IOException
     */
    private void ensureBounds(int num) throws IOException {
        int remaining = buf.length - pos;
        if (remaining < num) {
            flushBuffer();
        }
    }


    /**
     * Write a boolean value.
     *
     * @throws IOException
     *             If this is a stateful writer and a boolean is not expected
     */
    public void writeBoolean(boolean b) throws IOException {
        // inlined, shorter version of ensureBounds
        if (buf.length == pos) {
            flushBuffer();
        }
        pos += BinaryData.encodeBoolean(b, buf, pos);
    }

    /**
     * Writes a 32-bit integer.
     *
     * @throws IOException
     *             If this is a stateful writer and an integer is not expected
     */
    public void writeInt(int n) throws IOException {
        ensureBounds(5);
        pos += BinaryData.encodeInt(n, buf, pos);
    }

    /**
     * Write a 64-bit integer.
     *
     * @throws IOException
     *             If this is a stateful writer and a long is not expected
     */
    public void writeLong(long n) throws IOException {
        ensureBounds(10);
        pos += BinaryData.encodeLong(n, buf, pos);
    }

    /**
     * Write a float.
     *
     * @throws IOException
     *             If this is a stateful writer and a float is not expected
     */
    public void writeFloat(float f) throws IOException {
        ensureBounds(4);
        pos += BinaryData.encodeFloat(f, buf, pos);
    }

    /**
     * Write a double.
     *
     * @throws IOException
     *             If this is a stateful writer and a double is not expected
     */
    public void writeDouble(double d) throws IOException {
        ensureBounds(8);
        pos += BinaryData.encodeDouble(d, buf, pos);
    }

    /**
     * Writes a fixed size binary object.
     *
     * @param bytes
     *            The contents to write
     * @param start
     *            The position within <tt>bytes</tt> where the contents start.
     * @param len
     *            The number of bytes to write.
     * @throws IOException
     *             If this is a stateful writer and a byte-string is not
     *             expected
     * @throws IOException
     */
    public void writeFixed(byte[] bytes, int start, int len)
            throws IOException {
        if (len > bulkLimit) {
            // too big, write direct
            flushBuffer();
            sink.innerWrite(bytes, start, len);
            return;
        }
        ensureBounds(len);
        System.arraycopy(bytes, start, buf, pos, len);
        pos += len;
    }

    /** Writes a fixed from a ByteBuffer. */
    public void writeFixed(ByteBuffer bytes) throws IOException {
        if (!bytes.hasArray() && bytes.remaining() > bulkLimit) {
            flushBuffer();
            sink.innerWrite(bytes); // bypass the buffer
        } else {
            writeFixedInternal(bytes);
        }
    }

    private void writeFixedInternal(ByteBuffer bytes) throws IOException {
        int curPos = bytes.position();
        int len = bytes.limit() - curPos;
        if (bytes.hasArray()) {
            writeFixed(bytes.array(), bytes.arrayOffset() + curPos, len);
        } else {
            byte[] b = new byte[len];
            bytes.duplicate().get(b, 0, len);
            writeFixed(b, 0, len);
        }
    }

    /** Write a zero byte to the underlying output. **/
    void writeZero() throws IOException {
        writeByte(0);
    }

    private void writeByte(int b) throws IOException {
        if (pos == buf.length) {
            flushBuffer();
        }
        buf[pos++] = (byte) (b & 0xFF);
    }

    /**
     * Returns the number of bytes currently buffered by this encoder. If this
     * Encoder does not buffer, this will always return zero.
     * <p/>
     * Call {@link #flush()} to empty the buffer to the underlying output.
     */
    public int bytesBuffered() {
        return pos;
    }

    /**
     * "Writes" a null value. (Doesn't actually write anything, but advances the
     * state of the parser if this class is stateful.)
     *
     */
    public void writeNull() {
    }

    /**
     * Write a Unicode character string.
     *
     * @throws IOException
     *             If this is a stateful writer and a char-string is not
     *             expected
     */
    public void writeString(Utf8 utf8) throws IOException {
        this.writeBytes(utf8.getBytes(), 0, utf8.getByteLength());
    }


    /**
     * Write a Unicode character string. The default implementation converts the
     * String to a {@link oracle.kv.impl.api.table.serialize.util.Utf8}. Some
     * Encoder implementations may want to do something different as a
     * performance optimization.
     *
     * @throws IOException
     *             If this is a stateful writer and a char-string is not
     *             expected
     */
    public void writeString(String string) throws IOException {
        final int charLength = string != null ? string.length() : 0;
        if (0 == charLength) {
            writeZero();
            return;
        }
        final ByteBuffer bytes = getUTF8Bytes(string);
        final int utf8Length = bytes.remaining();
        writeInt(utf8Length);
        writeFixed(bytes.array(), 0, utf8Length);
    }

    /**
     * Write a Unicode character string. If the CharSequence is an
     * {@link oracle.kv.impl.api.table.serialize.util.Utf8} it writes this
     * directly, otherwise the CharSequence is converted to a String via
     * toString() and written.
     *
     * @throws IOException
     *             If this is a stateful writer and a char-string is not
     *             expected
     */
    public void writeString(CharSequence charSequence) throws IOException {
        if (charSequence instanceof Utf8)
            writeString((Utf8) charSequence);
        else
            writeString(charSequence.toString());
    }

    /**
     * Gets the encoded version of the String.
     *
     * @see CharsetEncoder#encode(CharBuffer)
     */
    private ByteBuffer getUTF8Bytes(String string)
        throws CharacterCodingException {

        /* Convert String to CharBuffer */
        final CharBuffer chars = CharBuffer.wrap(string);

        /* Use thread local encoder */
        final CharsetEncoder encoder = utf8Encoder.get().reset();

        /* Get result buffer, using local cache if possible */
        int bufferSize =
            (int) (chars.remaining() * encoder.averageBytesPerChar());
        ByteBuffer bytes;
        if (bufferSize <= LOCAL_BUFFER_SIZE) {
            bufferSize = LOCAL_BUFFER_SIZE;
            bytes = stringByteBuffer.get();
            bytes.clear();
        } else {
            bytes = ByteBuffer.allocate(bufferSize);
        }

        /* Try encoding, increasing buffer size as needed */
        while (true) {
            CoderResult result = chars.hasRemaining() ?
                encoder.encode(chars, bytes, true) :
                CoderResult.UNDERFLOW;
            if (result.isUnderflow()) {
                result = encoder.flush(bytes);
            }
            if (result.isUnderflow()) {
                break;
            }
            if (result.isOverflow()) {

                /* Double size. Note size was at least LOCAL_BUFFER_SIZE. */
                bufferSize = 2 * bufferSize;
                final ByteBuffer newBytes = ByteBuffer.allocate(bufferSize);
                bytes.flip();
                newBytes.put(bytes);
                bytes = newBytes;
            } else {
                result.throwException();
            }
        }

        /* Done */
        bytes.flip();
        return bytes;
    }

    /**
     * A shorthand for <tt>writeFixed(bytes, 0, bytes.length)</tt>
     *
     * @param bytes
     */
    public void writeFixed(byte[] bytes) throws IOException {
        writeFixed(bytes, 0, bytes.length);
    }

    /**
     * Writes a byte string. Equivalent to
     * <tt>writeBytes(bytes, 0, bytes.length)</tt>
     *
     * @throws IOException
     * @throws IOException
     *             If this is a stateful writer and a byte-string is not
     *             expected
     */
    public void writeBytes(byte[] bytes) throws IOException {
        writeBytes(bytes, 0, bytes.length);
    }

    /**
     * Write a byte string.
     *
     * @throws IOException
     *             If this is a stateful writer and a byte-string is not
     *             expected
     */
    public void writeBytes(ByteBuffer bytes) throws IOException {
        int len = bytes.limit() - bytes.position();
        if (0 == len) {
            writeZero();
        } else {
            writeInt(len);
            writeFixed(bytes);
        }
    }

    /**
     * Write a byte string.
     *
     * @throws IOException
     *             If this is a stateful writer and a byte-string is not
     *             expected
     */
    public void writeBytes(byte[] bytes, int start, int len)
            throws IOException {
        if (0 == len) {
            writeZero();
            return;
        }
        this.writeInt(len);
        this.writeFixed(bytes, start, len);
    }

    /**
     * Writes an enumeration.
     *
     * @param e
     * @throws IOException
     *             If this is a stateful writer and an enumeration is not
     *             expected or the <tt>e</tt> is out of range.
     * @throws IOException
     */
    public void writeEnum(int e) throws IOException {
        this.writeInt(e);
    }

    /**
     * Call this method to start writing an array.
     *
     * When starting to serialize an array, call {@link #writeArrayStart}. Then,
     * before writing any data for any item call {@link #setItemCount} followed
     * by a sequence of {@link #startItem()} and the item itself. The number of
     * {@link #startItem()} should match the number specified in
     * {@link #setItemCount}. When actually writing the data of the item, you
     * can call any {@link AvroEncoder} method (e.g., {@link #writeLong}).
     * When all items of the array have been written,
     * call {@link #writeArrayEnd}.
     *
     * As an example, let's say you want to write an array of records, the
     * record consisting of an Long field and a Boolean field. Your code would
     * look something like this:
     *
     * <pre>
     * out.writeArrayStart();
     * out.setItemCount(list.size());
     * for (Record r : list) {
     *     out.startItem();
     *     out.writeLong(r.longField);
     *     out.writeBoolean(r.boolField);
     * }
     * out.writeArrayEnd();
     * </pre>
     *
     */
    public void writeArrayStart() {
    }

    /**
     * Call this method before writing a batch of items in an array or a map.
     * Then for each item, call {@link #startItem()} followed by any of the
     * other write methods of {@link AvroEncoder}. The number of calls to
     * {@link #startItem()} must be equal to the count specified in
     * {@link #setItemCount}. Once a batch is completed you can start another
     * batch with {@link #setItemCount}.
     *
     * @param itemCount
     *            The number of {@link #startItem()} calls to follow.
     * @throws IOException
     */
    public void setItemCount(long itemCount) throws IOException {
        if (itemCount > 0) {
            this.writeLong(itemCount);
        }
    }

    /**
     * Start a new item of an array or map. See {@link #writeArrayStart} for
     * usage information.
     *
     */
    public void startItem() {
    }

    /**
     * Call this method to finish writing an array. See {@link #writeArrayStart}
     * for usage information.
     *
     * @throws IOException
     *             If items written does not match count provided to
     *             {@link #writeArrayStart}
     * @throws IOException
     *             If not currently inside an array
     */
    public void writeArrayEnd() throws IOException {
        writeZero();
    }

    /**
     * Call this to start a new map. See {@link #writeArrayStart} for details on
     * usage.
     *
     * As an example of usage, let's say you want to write a map of records, the
     * record consisting of an Long field and a Boolean field. Your code would
     * look something like this:
     *
     * {@literal
     * <pre>
     * out.writeMapStart();
     * out.setItemCount(list.size());
     * for (Map.Entry<String, Record> entry : map.entrySet()) {
     *     out.startItem();
     *     out.writeString(entry.getKey());
     *     out.writeLong(entry.getValue().longField);
     *     out.writeBoolean(entry.getValue().boolField);
     * }
     * out.writeMapEnd();
     * </pre>
     * }
     *
     */
    public void writeMapStart() {
    }

    /**
     * Call this method to terminate the inner-most, currently-opened map. See
     * {@link #writeArrayStart} for more details.
     *
     * @throws IOException
     *             If items written does not match count provided to
     *             {@link #writeMapStart}
     * @throws IOException
     *             If not currently inside a map
     */
    public void writeMapEnd() throws IOException {
        writeZero();
    }


    /**
     * Call this method to write the tag of a union.
     *
     * As an example of usage, let's say you want to write a union, whose second
     * branch is a record consisting of an Long field and a Boolean field. Your
     * code would look something like this:
     *
     * <pre>
     * out.writeIndex(1);
     * out.writeLong(record.longField);
     * out.writeBoolean(record.boolField);
     * </pre>
     *
     * @throws IOException
     *             If this is a stateful writer and a map is not expected
     */
    public void writeIndex(int unionIndex) throws IOException {
        writeInt(unionIndex);
    }

    /**
     * Call this to start a new CRDT. See {@link #writeArrayStart} for details
     * on usage.
     *
     * As an example of usage, let's say you want to write a long CRDT, which
     * internally contains (Integer(region id), Long(count)) pairs. Your code
     * would look something like this:
     *
     * {@literal
     * <pre>
     * out.writeCRDTStart();
     * out.setItemCount(crdt.getCRDTMap().size());
     * for (Map.Entry<String, Record> entry : crdt.getCRDTMap().entrySet()) {
     *     out.startItem();
     *     out.writeInt(entry.getKey());
     *     out.writeLong(entry.getValue());
     * }
     * out.writeCRDTEnd();
     * </pre>
     * }
     *
     */
    public void writeCRDTStart() {
    }

    /**
     * Call this method to terminate a CRDT. See {@link #writeArrayStart} for
     * more details.
     *
     * @throws IOException
     *             If items written does not match count provided to
     *             {@link #writeCRDTStart}
     * @throws IOException
     *             If not currently inside a CRDT
     */
    public void writeCRDTEnd() throws IOException {
        writeZero();
    }

    /**
     * This was originally an abstracted interface in Avro
     */
    static class OutputStreamSink {
        private final OutputStream out;
        private final WritableByteChannel channel;

        private OutputStreamSink(OutputStream out) {
            this.out = out;
            channel = Channels.newChannel(out);
        }

        void innerWrite(byte[] bytes, int off, int len)
                throws IOException {
            out.write(bytes, off, len);
        }

        void innerFlush() throws IOException {
            out.flush();
        }

        void innerWrite(ByteBuffer buff) throws IOException {
            channel.write(buff);
        }
    }
}
