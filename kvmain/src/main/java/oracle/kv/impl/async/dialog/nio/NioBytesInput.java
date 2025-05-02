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

package oracle.kv.impl.async.dialog.nio;

import java.io.EOFException;
import java.nio.ByteBuffer;

import oracle.kv.impl.async.BytesInput;
import oracle.kv.impl.async.IOBufPoolTrackers;
import oracle.kv.impl.async.IOBufSlice;
import oracle.kv.impl.async.IOBufSliceImpl;
import oracle.kv.impl.async.IOBufSliceList;
import oracle.kv.impl.async.dialog.DialogContextImpl;

/**
 * An input that consumes chunks of bytes.
 *
 * The instances of this class is intended to be created and called inside the
 * single-thread channel executor and thus is not intended to be thread safe.
 */
class NioBytesInput implements BytesInput {

    private static final String HEAD_OF_MESSAGE_INPUT = "headOfMessageInput";
    private static final String MESSAGE_INPUT_POLLED = "messageInputPolled";

    private int remaining;
    private final IOBufSliceList buflist;

    NioBytesInput(int remaining, IOBufSliceList buflist) {
        this.remaining = remaining;
        this.buflist = buflist;
        trackHead();
    }

    /**
     * Adds an info to the head of the list. This info will indicate that the
     * slice has been moved to the head and will be consumed next. Missing such
     * info on a leaked slice means the bytes input is not consumed properly.
     */
    private void trackHead() {
        if (buflist == null) {
            return;
        }
        IOBufSliceList.Entry curr = buflist.head();
        if (curr != null) {
            IOBufPoolTrackers.addStringInfo(
                (IOBufSliceImpl) curr, HEAD_OF_MESSAGE_INPUT);
        }
    }

    /**
     * Fills the byte array with {@code len} bytes.
     */
    @Override
    public void readFully(byte[] b, int off, int len) throws EOFException {
        if (len == 0) {
            return;
        }
        if (remaining < len) {
            throw new EOFException();
        }
        while (true) {
            IOBufSlice slice = buflist.head();
            if (slice == null) {
                throw new IllegalStateException(
                              "Incorrect buffer accounting");
            }
            ByteBuffer buf = slice.buf();
            int n = Math.min(len, buf.remaining());
            buf.get(b, off, n);
            len -= n;
            off += n;
            remaining -= n;
            if (buf.remaining() == 0) {
                pollHead();
                slice.markFree();
            }
            if (len == 0) {
                break;
            }
        }
    }

    private void pollHead() {
        buflist.poll();
        trackHead();
    }

    /**
     * Skips {@code len} bytes.
     */
    @Override
    public void skipBytes(int n) throws EOFException {
        if (remaining < n) {
            throw new EOFException();
        }
        while (true) {
            IOBufSlice slice = buflist.head();
            if (slice == null) {
                throw new IllegalStateException(
                              "Incorrect buffer accounting");
            }
            ByteBuffer buf = slice.buf();
            int toskip = Math.min(n, buf.remaining());
            buf.position(buf.position() + toskip);
            n -= toskip;
            remaining -= toskip;
            if (buf.remaining() == 0) {
                pollHead();
                slice.markFree();
            }
            if (n == 0) {
                break;
            }
        }
    }

    /**
     * Reads a byte.
     */
    @Override
    public byte readByte() throws EOFException {
        if (remaining == 0) {
            throw new EOFException();
        }
        while (true) {
            IOBufSlice slice = buflist.head();
            if (slice == null) {
                throw new IllegalStateException(
                              "Incorrect buffer accounting");
            }
            ByteBuffer buf = slice.buf();
            if (buf.remaining() != 0) {
                remaining --;
                final byte ret = buf.get();
                if (buf.remaining() == 0) {
                    pollHead();
                    slice.markFree();
                }
                return ret;
            }

            pollHead();
            slice.markFree();
        }
    }

    /**
     * Returns the number of remaining bytes.
     */
    @Override
    public int remaining() {
        return remaining;
    }

    /**
     * Discards the bytes input and does clean up.
     */
    @Override
    public void discard() {
        if (buflist == null) {
            return;
        }
        while (true) {
            IOBufSlice slice = buflist.poll();
            if (slice == null) {
                return;
            }
            slice.markFree();
        }
    }

    @Override
    public void trackDialog(DialogContextImpl dialog) {
        if (buflist == null) {
            return;
        }
        IOBufSliceList.Entry curr = buflist.head();
        while (curr != null) {
            IOBufPoolTrackers.addDialogInfo((IOBufSliceImpl) curr, dialog);
            curr = curr.next();
        }
    }

    @Override
    public void markMessageInputPolled() {
        if (buflist == null) {
            return;
        }
        IOBufSliceList.Entry curr = buflist.head();
        while (curr != null) {
            IOBufPoolTrackers.addStringInfo(
                (IOBufSliceImpl) curr, MESSAGE_INPUT_POLLED);
            curr = curr.next();
        }
    }

    @Override
    public String toString() {
        return String.format("%s %s", getClass().getSimpleName(), buflist);
    }
}
