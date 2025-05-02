/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import oracle.kv.impl.async.dialog.DialogContextImpl;

import org.checkerframework.checker.nullness.qual.Nullable;

public class CopyingBytesInput implements BytesInput {

    private final byte[] bytes;
    private final ByteBuffer buffer;

    public CopyingBytesInput(List<IOBufSlice> buflist) {
        this(buflist,
             buflist.stream().mapToInt(s -> s.buf().remaining()).sum());
    }

    public CopyingBytesInput(List<IOBufSlice> buflist, int size) {
        this(buflist.stream().map(s -> s.buf()).
             collect(Collectors.toList()).toArray(new ByteBuffer[0]),
             size);
    }

    public CopyingBytesInput(ByteBuffer[] bufarray) {
        this(bufarray,
             Arrays.stream(bufarray).mapToInt(b -> b.remaining()).sum());
    }

    public CopyingBytesInput(ByteBuffer[] bufarray, int size) {
        this.bytes = new byte[size];
        this.buffer = ByteBuffer.wrap(bytes);
        for (ByteBuffer buf : bufarray) {
            int oldPos = buf.position();
            buffer.put(buf);
            buf.position(oldPos);
        }
        buffer.flip();
    }

    public CopyingBytesInput(ByteBuffer buf) {
        this.bytes = new byte[buf.remaining()];
        this.buffer = ByteBuffer.wrap(bytes);
        int oldPos = buf.position();
        buffer.put(buf);
        buf.position(oldPos);
        buffer.flip();
    }

    public CopyingBytesInput(byte[] bytes) {
        this.bytes = Arrays.copyOf(bytes, bytes.length);
        this.buffer = ByteBuffer.wrap(this.bytes);
    }

    /**
     * Fills the byte array with {@code len} bytes.
     */
    @Override
    public void readFully(byte[] b, int off, int len) {
        buffer.get(b, off, len);
    }

    /**
     * Skips {@code len} bytes.
     */
    @Override
    public void skipBytes(int n) {
        buffer.position(buffer.position() + n);
    }

    /**
     * Reads a byte.
     */
    @Override
    public byte readByte() {
        return buffer.get();
    }

    /**
     * Returns the number of remaining bytes.
     */
    @Override
    public int remaining() {
        return buffer.remaining();
    }

    /**
     * Discards the buffers.
     */
    @Override
    public void discard() {
        /* nothing */
    }

    @Override
    public void trackDialog(@Nullable DialogContextImpl dialog) {
    }

    @Override
    public void markMessageInputPolled() {
    }

    @Override
    public String toString() {
        return BytesUtil.toString(buffer, buffer.capacity());
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        if (!(obj instanceof CopyingBytesInput)) {
            return false;
        }
        CopyingBytesInput that = (CopyingBytesInput) obj;
        return Arrays.equals(this.bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }
}
