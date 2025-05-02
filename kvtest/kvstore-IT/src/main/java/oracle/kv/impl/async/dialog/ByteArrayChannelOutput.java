/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async.dialog;

import java.nio.ByteBuffer;

import oracle.kv.impl.async.BytesUtil;
import oracle.kv.impl.async.HeapIOBufferPool;
import oracle.kv.impl.async.IOBufSlice;
import oracle.kv.impl.async.IOBufSliceList;

/**
 * A channel output that flushes to a byte array.
 */
public class ByteArrayChannelOutput extends ChannelOutput {

    /* The byte buffer that wraps the byte array. */
    private final ByteBuffer outbuf;

    ByteArrayChannelOutput(byte[] array) {
        super(new HeapIOBufferPool(64));
        outbuf = ByteBuffer.wrap(array);
    }

    /**
     * Flushes the chunks to the byte array.
     */
    public synchronized void flush() {
        IOBufSliceList slices = new IOBufSliceList();
        fetch(slices);
        while (true) {
            IOBufSlice slice = slices.poll();
            if (slice == null) {
                break;
            }
            outbuf.put(slice.buf().duplicate());
            slice.markFree();
        }
    }

    /**
     * The position of the array that is filled with flushed data.
     */
    public synchronized int position() {
        return outbuf.position();
    }

    @Override
    public synchronized String toString() {
        return BytesUtil.toString(outbuf.array(), 0, outbuf.position());
    }
}

