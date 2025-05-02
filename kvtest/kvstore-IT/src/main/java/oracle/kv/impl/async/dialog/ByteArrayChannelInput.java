/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async.dialog;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import com.sleepycat.util.PackedInteger;

import oracle.kv.impl.async.CopyingBytesInput;

/**
 * A channel input that reads from a byte array.
 *
 * Methods are not thread safe and therefore is expected to be called within
 * one thread.
 */
public class ByteArrayChannelInput implements ChannelInput {

    private static final Charset utf8 = Charset.forName("UTF-8");

    /* The byte array that holds the data. */
    private final byte[] bytes;
    /* The byte buffer that wraps the byte array. */
    private final ByteBuffer inbuf;

    ByteArrayChannelInput(byte[] bytes) {
        this.bytes = bytes;
        this.inbuf = ByteBuffer.wrap(bytes);
        inbuf.limit(0);
    }

    @Override
    public void mark() {
        inbuf.mark();
    }

    @Override
    public void reset() {
        inbuf.reset();
    }

    @Override
    public int readableBytes() {
        return inbuf.remaining();
    }

    @Override
    public byte readByte() {
        return inbuf.get();
    }

    @Override
    public CopyingBytesInput readBytes(int len) {
        int oldLim = inbuf.limit();
        int newPos = inbuf.position() + len;
        inbuf.limit(newPos);
        CopyingBytesInput binput =
            new CopyingBytesInput(inbuf.slice());
        inbuf.limit(oldLim);
        inbuf.position(newPos);
        return binput;
    }

    @Override
    public boolean canReadPackedLong() {
        return (inbuf.remaining() >=
                PackedInteger.getReadLongLength(
                    bytes, inbuf.position()));
    }

    @Override
    public long readPackedLong() {
        int len = PackedInteger.getReadLongLength(
                bytes, inbuf.position());
        long val = PackedInteger.readLong(bytes, inbuf.position());
        inbuf.position(inbuf.position() + len);
        return val;
    }

    @Override
    public String readUTF8(int length) {
        if (inbuf.remaining() < length) {
            return null;
        }
        byte[] encoded = new byte[length];
        inbuf.get(encoded);
        return utf8.decode(ByteBuffer.wrap(encoded)).toString();
    }

    @Override
    public void close() {

    }

    void limit(int val) {
        inbuf.limit(val);
    }

    int limit() {
        return inbuf.limit();
    }

    int position() {
        return inbuf.position();
    }
}

