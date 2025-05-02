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

package com.sleepycat.je.rep.utilint.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;


/**
 * A basic concrete extension of DataChannel.
 * This simply delegates operations directly to the underlying SocketChannel
 */
public class SimpleDataChannel extends AbstractDataChannel {

    /**
     * Test hook executed before channel read and write.
     */
    public static volatile TestHook<SimpleDataChannel> ioHook = null;

    /**
     * A wrapped socket channel so that read can be timed out.
     *
     * SocketChannel#read does not time out with the channel configured
     * blocking and the underlying socket having SO_TIMEOUT set. A work-around
     * channel is needed.
     *
     * The channel points to the work-around channel to read for blocking
     * channels. It points to the normal socket channel otherwise.
     */
    private volatile ReadableByteChannel wrappedReadChannel;

    /**
     * Constructor for general use.
     *
     * @param socketChannel A SocketChannel, which should be connected.
     */
    public SimpleDataChannel(SocketChannel socketChannel) {
        super(socketChannel);
        try {
            if (isBlocking() && socketChannel.isConnected()) {
                this.wrappedReadChannel =
                    Channels.newChannel(
                            socketChannel.socket().getInputStream());
            } else {
                this.wrappedReadChannel = socketChannel;
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                "Cannot get stream from connected socket " + socketChannel, e);
        }
    }

    @Override
    public boolean isOpen() {
        return socketChannel.isOpen();
    }

    @Override
    public boolean isSecure() {
        return false;
    }

    @Override
    public boolean isTrusted() {
        return false;
    }

    @Override
    public boolean isTrustCapable() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void configureBlocking(boolean block)
        throws IOException {
            if (block == isBlocking()) {
                return;
            }

            socketChannel.configureBlocking(block);

            synchronized(wrappedReadChannel) {
                if (block) {
                    wrappedReadChannel =
                        Channels.newChannel(
                            socketChannel.socket().getInputStream());
                } else {
                    wrappedReadChannel = socketChannel;
                }
            }
            configuredBlocking = block;
    }

    /*
     * The following ByteChannel implementation methods delegate to the wrapped
     * channel object.
     */

    @Override
    public int read(ByteBuffer dst) throws IOException {
        assert TestHookExecute.doIOHookIfSet(ioHook, this);

        synchronized(wrappedReadChannel) {
            return wrappedReadChannel.read(dst);
        }
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        return read(dsts, 0, dsts.length);
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length)
        throws IOException
    {
        assert TestHookExecute.doIOHookIfSet(ioHook, this);

        SSLDataChannel.checkParams(dsts, offset, length);
        long nbytes = 0;
        synchronized(wrappedReadChannel) {
            boolean done = false;
            for (int i = offset; i < offset + length; i++) {
                final ByteBuffer dst = dsts[i];
                while (dst.remaining() > 0) {
                    final int n = wrappedReadChannel.read(dst);
                    if ((n == -1) && (nbytes == 0)) {
                        return -1;
                    }
                    if (n <= 0) {
                        done = true;
                        break;
                    }
                    nbytes += n;
                }
                if (done) {
                    break;
                }
            }
        }
        return nbytes;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        assert TestHookExecute.doIOHookIfSet(ioHook, this);
        return socketChannel.write(src);
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        assert TestHookExecute.doIOHookIfSet(ioHook, this);
        return socketChannel.write(srcs);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length)
        throws IOException {

        assert TestHookExecute.doIOHookIfSet(ioHook, this);
        return socketChannel.write(srcs, offset, length);
    }

    @Override
    public boolean flush() {
        return true;
    }

    @Override
    public void close() throws IOException {
        try {
            ensureCloseForBlocking();
        } finally {
            socketChannel.close();
        }
    }

    @Override
    public boolean closeAsync() throws IOException {
        try {
            ensureCloseAsyncForNonBlocking();
        } finally {
            socketChannel.close();
        }
        return true;
    }

    @Override
    public void closeForcefully() throws IOException {
        socketChannel.close();
    }

    @Override
    public AsyncIO.ContinueAction getAsyncIOContinueAction(AsyncIO.Type type) {
        switch(type) {
        case READ:
            return AsyncIO.ContinueAction.WAIT_FOR_CHNL_READ;
        case WRITE:
            return AsyncIO.ContinueAction.WAIT_FOR_CHNL_WRITE_THEN_FLUSH;
        case FLUSH:
        case CLOSE:
            return AsyncIO.ContinueAction.RETRY_NOW;
        default:
            throw new IllegalStateException(
                          String.format(
                              "Unknown state for async IO operation: %s",
                              type));
        }
    }

    @Override
    public CompletableFuture<Void> executeTasks(ExecutorService executor) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public String toString() {
        return String.format(
                   "%s%s", addressPair, isBlocking() ? "BLK" : "NBL");
    }
}

