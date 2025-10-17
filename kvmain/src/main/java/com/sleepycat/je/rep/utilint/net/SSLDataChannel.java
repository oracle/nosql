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

import static java.util.logging.Level.FINE;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.INFO;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;

import com.sleepycat.je.rep.net.InstanceLogger;
import com.sleepycat.je.rep.net.SSLAuthenticator;
import com.sleepycat.je.utilint.CommonLoggerUtils;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;
import com.sleepycat.json_simple.JsonObject;

/**
 * SSLDataChannel provides SSL-based communications on top of a SocketChannel.
 * We attempt to maintain a degree of compatibility with SocketChannel
 * in terms of request completion semantics.  In particular,
 *    If in blocking mode:
 *       read() will return at least one byte if the buffer has room
 *       write() will write the entire buffer
 *    If in non-blocking mode:
 *       read() and write are not guaranteed to consume or produce anything.
 */
public class SSLDataChannel extends AbstractDataChannel {

    private static final int CHANNEL_TASK_BUSY_LOOP_COUNT = 5;

    /* For test only */
    /**
     * A hook when the channel is created. Deprecated by startupHook. TODO:
     * remove.
     */
    public static volatile TestHook<SSLSession> creationHook = null;
    /** A hook when the channel is created.  */
    public static volatile TestHook<SSLDataChannel> startupHook = null;
    /** A hook when before locks are grabbed. */
    public static volatile TestHook<SSLDataChannel> preLockHook = null;
    /** A hook when after locks are grabbed */
    public static volatile TestHook<SSLDataChannel> postLockHook = null;
    /** A hook when doing read and write */
    public static volatile TestHook<SSLDataChannel> ioHook = null;
    /** A hook before doing task execution. */
    public static volatile TestHook<SSLDataChannel> taskHook = null;

    /**
     * A null read channel.
     *
     * The channel is used as a sentinel to detect the cases when the caller is
     * switching between blocking and non-blocking channel and doing a read
     * operation at the same time. We do not support such cases.
     */
    private static final ReadableByteChannel NULL_READ_CHANNEL =
        new ReadableByteChannel() {
            /**
             * A null read channel that should not be used.
             */
            @Override
            public int read(ByteBuffer dst) {
                throw new IllegalStateException(
                              "Reading from a channel that " +
                              "should not be used. " +
                              "This indicates that a channel is switching " +
                              "between blocking and non-blocking mode " +
                              "while a concurrent read happens. " +
                              "We do not support such behavior");
            }

            @Override
            public void close() {

            }

            @Override
            public boolean isOpen() {
                return true;
            }
        };

    /**
     * A wrapped socket channel so that read can be timed out.
     *
     * SocketChannel#read does not time out with the channel configured
     * blocking and the underlying socket having SO_TIMEOUT set. A work-around
     * channel is needed.
     *
     * The channel points to the work-around channel to read for blocking
     * channels. It points to the normal socket channel otherwise.
     *
     * Write to the field is either at construction time or in a synchronized
     * block. Volatile for read thread-safety.
     */
    private volatile ReadableByteChannel wrappedReadChannel;

    /**
     * The SSLSession.
     */
    private final SSLSession sslSession;

    /**
     * The SSLEngine that will manage the secure operations.
     */
    private final SSLEngine sslEngine;

    /**
     * The lock for netRecvBuffer.
     *
     * Synchronize on this lock when accessing the buffer for unwrap and socket
     * channel read. If synchronizing on both netRecvLock and appRecvLock,
     * synchronize on netRecvLock first.
     */
    private final Object netRecvLock = new Object();
    /**
     * Raw bytes received from the SocketChannel - not yet unwrapped.
     *
     * Set up for channel read. Need to flip to transfer data to
     * appRecvBuffer and then compact.
     */
    private ByteBuffer netRecvBuffer;

    /**
     * The lock for netXmitBuffer.
     *
     * Synchronize on this lock when accessing netXmitBuffer for wrap and
     * socket channel write. It must be synchronized on as the innermost block.
     */
    private final Object netXmitLock = new Object();
    /**
     * Raw bytes to be sent to the wire - already wrapped.
     *
     * Set up is for wrap. Need to flip to write data to channel and then
     * compact.
     */
    private ByteBuffer netXmitBuffer;

    /**
     * The lock for appRecvBuffer.
     *
     * Synchronize on this lock when accessing appRecvBuffer for unwrap and
     * application read. If synchronizing on both appRecvLock and netRecvLock,
     * netRecvLock should be synchronized first.
     */
    private final Object appRecvLock = new Object();
    /**
     * Bytes unwrapped and ready for application consumption.
     *
     * Set up for unwrap. Need to flip for application read and then compact.
     */
    private ByteBuffer appRecvBuffer;

    /**
     * A dummy buffer array used during handshake operations.
     */
    private final ByteBuffer[] emptyAppXmitBuffers;

    /**
     * The channel write task.
     */
    private final ChannelWriteTask channelWriteTask = new ChannelWriteTask();

    /**
     * The channel read task.
     */
    private final ChannelReadTask channelReadTask = new ChannelReadTask();

    /**
     * The close task.
     */
    private final CloseTask closeTask = new CloseTask();

    /**
     * The thread local marking we are inside a close method.
     */
    private final ThreadLocal<Boolean> insideCloseMethod =
        ThreadLocal.withInitial(() -> false);

    /**
     * The String identifying the target host that we are connecting to, if
     * this channel was created in client context.
     */
    private final String targetHost;

    /**
     * Possibly null authenticator object used for checking whether the
     * peer for the negotiated session should be trusted.
     */
    private final SSLAuthenticator authenticator;

    /**
     * Possibly null host verifier object used for checking whether the
     * peer for the negotiated session is correct based on the connection
     * target.
     */
    private final HostnameVerifier hostVerifier;

    /**
     * Set to true when a handshake completes and a non-null authenticator
     * acknowledges the session as trusted.
     */
    private volatile boolean peerTrusted = false;

    private final InstanceLogger logger;

    /**
     * Async IO operation incompletion.
     */
    private final AsyncIOIncompletion asyncIOIncompletion =
        new AsyncIOIncompletion();

    /**
     * Construct an SSLDataChannel given a SocketChannel and an SSLEngine
     *
     * @param socketChannel a SocketChannel over which SSL communcation will
     *     occur.  This should generally be connected, but that is not
     *     absolutely required until the first read/write operation.
     * @param sslEngine an SSLEngine instance that will control the SSL
     *     interaction with the peer.
     */
    public SSLDataChannel(SocketChannel socketChannel,
                          SSLEngine sslEngine,
                          String targetHost,
                          HostnameVerifier hostVerifier,
                          SSLAuthenticator authenticator,
                          InstanceLogger logger) {

        super(socketChannel);
        this.sslSession = sslEngine.getSession();
        this.sslEngine = sslEngine;
        this.targetHost = targetHost;
        this.authenticator = authenticator;
        this.hostVerifier = hostVerifier;
        this.logger = logger;

        /* Determine the required buffer sizes */
        int netBufferSize = sslSession.getPacketBufferSize();
        int appBufferSize = sslSession.getApplicationBufferSize();

        /* allocate the buffers */
        this.emptyAppXmitBuffers = new ByteBuffer[] { ByteBuffer.allocate(0) };
        this.netXmitBuffer = ByteBuffer.allocate(netBufferSize);
        this.appRecvBuffer = ByteBuffer.allocate(appBufferSize);
        this.netRecvBuffer = ByteBuffer.allocate(netBufferSize);

        assert TestHookExecute.doHookIfSet(creationHook, sslSession);
        assert TestHookExecute.doHookIfSet(startupHook, this);

        try {
            if (isBlocking()) {
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
        logger.log(FINE,
                   () -> String.format("%s data channel created",
                                       toShortString()));
    }

    public InstanceLogger getLogger() {
        return logger;
    }

    /** Returns the ssl engine. */
    public SSLEngine getSSLEngine() {
        return sslEngine;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configureBlocking(boolean block)
        throws IOException {

        synchronized(this) {
            if (block == isBlocking()) {
                return;
            }

            /*
             * Sets the channel to a null channel so that we can detect
             * concurrent reads that we do not support.
             */
            wrappedReadChannel = NULL_READ_CHANNEL;

            /*
             * We rely on the thread-safety of the SocketChannel here. The read
             * and configureBlocking methods are atomic w.r.t. each other.
             */
            socketChannel.configureBlocking(block);
            /*
             * It is possible a concurrent read (happens after the above
             * statement) sees the NULL_READ_CHANNEL. ChannelReadTask#run does
             * an additional synchronization if that happens.
             */
            if (block) {
                wrappedReadChannel =
                    Channels.newChannel(
                        socketChannel.socket().getInputStream());
            } else {
                wrappedReadChannel = socketChannel;
            }
            configuredBlocking = block;
        }

        /*
         * Need to flush since non-blocking channel does not necessarily flush.
         */
        if (block) {
            flush();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isSecure() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isTrustCapable() {
        return authenticator != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isTrusted() {
        return peerTrusted;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isOpen() {
        if (!socketChannel.isOpen()) {
            return false;
        }
        return closeTask.isStandby();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AsyncIO.ContinueAction getAsyncIOContinueAction(AsyncIO.Type type) {
        return asyncIOIncompletion.nextAction(type);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int read(ByteBuffer toFill) throws IOException, SSLException {
        return (int) read(new ByteBuffer[] { toFill }, 0, 1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long read(ByteBuffer[] toFill) throws IOException, SSLException {
        return read(toFill, 0, toFill.length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long read(ByteBuffer toFill[], int offset, int length)
        throws IOException {
        logger.log(FINEST, () -> String.format(
            "%s data channel reading", toShortString()));
        /*
         * Short-circuit if there's no work to be done at this time.  This
         * avoids an unnecessary read() operation from blocking.
         */
        if ((countRemaining(toFill, offset, length)) <= 0) {
            return 0;
        }

        crossCloseQuiescentBarrier();

        int readCount = 0;
        while (true) {
            /*
             * If we have data that is already unwrapped and ready to transfer,
             * do it now
             */
            final int n = transferAppData(toFill, offset, length);
            readCount += n;
            if (n != 0) {
                logger.log(FINEST,
                           () -> String.format(
                               "%s transferred %s bytes", toShortString(), n));
                return readCount;
            }
            if (closeTask.isChannelReadDone() && (readCount == 0)) {
                logger.log(FINEST,
                           () -> String.format(
                               "%s read closed", toShortString()));
                return -1;
            }

            TransitionStopCause cause = unwrap();
            logger.log(FINEST,
                       () ->
                       String.format(
                           "%s data channel read "
                           + "unwrap stop cause is %s",
                           toShortString(), cause));
            switch(cause) {
            case NB_IS_CHNL_WRITE_BUSY:
            case NB_NEEDS_CHNL_READ_DATA:
            case NB_NEEDS_CHNL_READ_HANDSHAKE:
            case NEEDS_HANDSHAKE_TASKS:
            case NEEDS_APP_READ:
                logger.log(FINEST,
                           () ->
                           String.format(
                               "%s data channel read stopped, "
                               + "cause is %s",
                               toShortString(), cause));
                return readCount;
            case DONE:
                continue;
            default:
                throw new IllegalStateException(
                              String.format(
                                  "Invalid stop cause from unwrap in read: %s",
                                  cause));
            }
        }
    }

    public static int countRemaining(ByteBuffer[] bufs, int offset, int length) {
        checkParams(bufs, offset, length);
        int ret = 0;
        for (int i = offset; i < offset + length; ++i) {
            ret += bufs[i].remaining();
        }
        return ret;
    }

    public static void checkParams(ByteBuffer[] bufs, int offset, int length) {

        if (bufs == null) {
            throw new IllegalArgumentException("buffer is null");
        }

        if ((offset < 0) || (length < 0) ||
                (offset > bufs.length - length)) {
            throw new IndexOutOfBoundsException(
                    "index out of bound of the buffers");
        }

        for (int i = offset; i < offset + length; i++) {
            if (bufs[i] == null) {
                throw new IllegalArgumentException(
                        "buffer[" + i + "] == null");
            }
        }
    }

    /*
     * Once any close task is started, we want the channel to enter a quiescent
     * period in which only read and write tasks issued by a close task can
     * proceed. We achieve creating such quiescent period by letting every read
     * and write task call this method. When any close task is started, any
     * thread with insideCloseMethod set to false will throw exception when
     * calling this method.
     */
    private void crossCloseQuiescentBarrier() throws IOException {
        if (insideCloseMethod.get()) {
            /* Quiescent barrier does not apply to us */
            return;
        }
        if (closeTask.isRunning()) {
            throw new AsynchronousCloseException();
        }
        if (closeTask.isDone()) {
            throw new ClosedChannelException();
        }
    }

    private int transferAppData(ByteBuffer toFill[],
                                 int offset,
                                 int length) {
        assert TestHookExecute.doHookIfSet(preLockHook, this);
        synchronized(appRecvLock) {
            assert TestHookExecute.doHookIfSet(postLockHook, this);
            if (appRecvBuffer.position() <= 0) {
                asyncIOIncompletion.needsAppRead(false);
                return 0;
            }
            appRecvBuffer.flip();
            final int count =
                transfer(appRecvBuffer, toFill, offset, length);
            appRecvBuffer.compact();
            if (count > 0) {
                asyncIOIncompletion.needsAppRead(false);
            }
            return count;
        }
    }

    /**
     * Transfer as much data as possible from the src buffer to the dst
     * buffers.
     *
     * @param src the source ByteBuffer - it is expected to be ready for a get.
     * @param dsts the destination array of ByteBuffers, each of which is
     * expected to be ready for a put.
     * @param offset the offset within the buffer array of the first buffer
     * into which bytes are to be transferred.
     * @param length the maximum number of buffers to be accessed
     * @return The number of bytes transfered from src to dst
     */
    private int transfer(ByteBuffer src,
                         ByteBuffer[] dsts,
                         int offset,
                         int length) {

        int transferred = 0;
        for (int i = offset; i < offset + length; ++i) {
            final ByteBuffer dst = dsts[i];
            final int space = dst.remaining();

            if (src.remaining() > space) {
                /* not enough room for it all */
                final int oldLimit = src.limit();
                src.limit(src.position() + space);
                dst.put(src);
                src.position(src.limit());
                src.limit(oldLimit);
                transferred += space;
            } else {
                transferred += src.remaining();
                dst.put(src);
                break;
            }
        }
        return transferred;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int write(ByteBuffer toSend) throws IOException, SSLException {
        return (int) write(new ByteBuffer[] { toSend }, 0, 1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long write(ByteBuffer[] toSend) throws IOException, SSLException {
        return write(toSend, 0, toSend.length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long write(ByteBuffer[] toSend, int offset, int length)
        throws IOException {

        logger.log(FINEST,
                   () -> String.format(
                       "%s data channel writing", toShortString()));
        crossCloseQuiescentBarrier();

        final WrapStatus wrapStatus = wrap(toSend, offset, length);
        logger.log(FINEST,
                   () ->
                   String.format("%s data channel write " +
                                 "wrap stop cause is %s, consumed %s bytes",
                                 toShortString(),
                                 wrapStatus.cause, wrapStatus.consumed));
        if (wrapStatus.cause == TransitionStopCause.PAUSED) {
            throw new IllegalStateException(
                          "Invalid stop cause for wrap: PAUSED");
        }
        if (isBlocking()) {
            flush();
        }
        return wrapStatus.consumed;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean flush() throws IOException {

        logger.log(FINEST,
                   () ->
                   String.format(
                       "%s data channel flushing", toShortString()));
        final TransitionStopCause cause = channelWriteTask.runOrAwait();
        if (cause.equals(TransitionStopCause.DONE)) {
            return true;
        }
        return false;
    }

    /**
     * The causes that a transition is stopped.
     */
    enum TransitionStopCause {
        /* Another channel write task was in progress. */
        CONCURRENT_CHANNEL_WRITE_TASK,

        /* Another channel read task was in progress. */
        CONCURRENT_CHANNEL_READ_TASK,

        /* Nonblocking channel cannot write */
        NB_IS_CHNL_WRITE_BUSY,

        /* Nonblocking channel no app data to read */
        NB_NEEDS_CHNL_READ_DATA,

        /* Nonblocking channel no handshake data to read */
        NB_NEEDS_CHNL_READ_HANDSHAKE,

        /* The appRecvBuffer is full */
        NEEDS_APP_READ,

        /* Task execution is needed for handshake */
        NEEDS_HANDSHAKE_TASKS,

        /* Paused after one step, may call the step again */
        PAUSED,

        /*
         * Transition is done, should not call the step again without new
         * progress
         */
        DONE,
    }

    /**
     * Wraps for handshake.
     */
    private WrapStatus wrap() throws IOException {
        return wrap(emptyAppXmitBuffers, 0, 1);
    }

    /**
     * Wraps for application data, as much as we can.
     *
     * TransitionStopCause return:
     * - NB_IS_CHNL_WRITE_BUSY
     * - NB_NEEDS_CHNL_READ_DATA
     * - NB_NEEDS_CHNL_READ_HANDSHAKE
     * - NEEDS_APP_READ
     * - NEEDS_HANDSHAKE_TASKS
     * - DONE
     */
    private WrapStatus wrap(ByteBuffer[] bufs, int offset, int length)
        throws IOException {

        /*
         * The outbound is usually closed by application when it calls a close
         * method. Before TLS1.3, however, the outbound may be automatically
         * closed after receiving a close_notify. In that case, it is important
         * to do a check here. Otherwise, the application may repeatedly call
         * write and busy looping.
         */
        if (sslEngine.isOutboundDone()) {
            throw new SSLException("SSL outbound already closed");
        }

        final long total = countRemaining(bufs, offset, length);
        logger.log(FINEST,
                   () ->
                   String.format("%s wrapping, total to wrap is %s bytes",
                                 toShortString(), total));
        long consumed = 0;
        while (true) {
            final TransitionStopCause hsCause = handshake();
            logger.log(FINEST,
                       () ->
                       String.format("%s handshake stop cause is %s",
                                     toShortString(), hsCause));
            if (hsCause != TransitionStopCause.DONE) {
                return new WrapStatus(consumed, hsCause);
            }
            final WrapStatus wrapStatus = doWrap(bufs, offset, length);
            consumed += wrapStatus.consumed;
            final long remaining = total - consumed;
            logger.log(FINEST,
                       () ->
                       String.format("%s wrap stop cause is %s, " +
                                     "consumed %s bytes, " +
                                     "remaining is %s bytes",
                                     toShortString(),
                                     wrapStatus.cause, wrapStatus.consumed,
                                     remaining));
            final TransitionStopCause cause;
            if (sslEngine.getHandshakeStatus().
                equals(SSLEngineResult.HandshakeStatus.NEED_UNWRAP)) {
                /*
                 * An optimization here. We only flush the data when sslEngine
                 * needs the peer to respond for us to unwrap. Upper layer
                 * should call flush() when they really want to flush.
                 */
                cause = flushAfterDoWrap(wrapStatus.cause);
            } else {
                cause = wrapStatus.cause;
            }
            switch(cause) {
            case CONCURRENT_CHANNEL_WRITE_TASK:
                channelWriteTask.await();
                break;
            case NB_IS_CHNL_WRITE_BUSY:
            case DONE:
                return new WrapStatus(consumed, cause);
            case PAUSED:
                if (total - consumed == 0) {
                    return new WrapStatus(consumed, TransitionStopCause.DONE);
                }
                break;
            default:
                throw new IllegalStateException(
                              String.format("Invalid state from doWrap: %s",
                                            wrapStatus.cause));
            }
        }
    }

    private class WrapStatus {
        private final long consumed;
        private final TransitionStopCause cause;
        private WrapStatus(long consumed, TransitionStopCause cause) {
            this.consumed = consumed;
            this.cause = cause;
        }
    }

    /**
     * Do a flush after doWrap.
     *
     * TransitionStopCause return:
     * - NB_IS_CHNL_WRITE_BUSY
     * - PAUSED: doWrap returns PAUSED and flushed
     * - DONE: doWrap returns DONE and flushed
     */
    private TransitionStopCause flushAfterDoWrap(TransitionStopCause cause)
        throws IOException {

        if (!(cause.equals(TransitionStopCause.CONCURRENT_CHANNEL_WRITE_TASK)
              || cause.equals(TransitionStopCause.NB_IS_CHNL_WRITE_BUSY)
              || cause.equals(TransitionStopCause.PAUSED)
              || cause.equals(TransitionStopCause.DONE))) {
            throw new IllegalStateException(
                          "Invalid stop cause for doWrap: " + cause);
        }

        /*
         * If we successfully wrapped something, flush it so that the other
         * side can see. This ensures that we will not be blocked during unwrap
         * due to the other side not seeing the wrapped data and responds.
         */
        if ((cause.equals(TransitionStopCause.PAUSED) ||
             cause.equals(TransitionStopCause.DONE)) &&
            (!channelWriteTask.runOrAwait()
             .equals(TransitionStopCause.DONE))) {
            /* Notify the caller we need to write again. */
            return TransitionStopCause.NB_IS_CHNL_WRITE_BUSY;
        }
        return cause;
    }

    /**
     * Unwraps for raw data, as much as we can.
     *
     * TransitionStopCause return:
     * - NB_IS_CHNL_WRITE_BUSY
     * - NB_NEEDS_CHNL_READ_DATA
     * - NB_NEEDS_CHNL_READ_HANDSHAKE
     * - NEEDS_APP_READ
     * - NEEDS_HANDSHAKE_TASKS
     * - DONE
     */
    private TransitionStopCause unwrap() throws IOException {
        logger.log(FINEST,
                   () -> String.format("%s unwrapping", toShortString()));
        while (true) {
            /* Do handshake first in case we need it */
            final TransitionStopCause hsCause = handshake();
            logger.log(FINEST,
                       () ->
                       String.format("%s handshake stop cause is %s",
                                     toShortString(), hsCause));
            if (hsCause != TransitionStopCause.DONE) {
                return hsCause;
            }
            final UnwrapStatus unwrapStatus = doUnwrap();
            logger.log(FINEST,
                       () ->
                       String.format("%s unwrap stop cause is %s, " +
                                     "produced %s bytes",
                                     toShortString(),
                                     unwrapStatus.cause,
                                     unwrapStatus.produced));
            /*
             * Check if we already have some app data available to prevent
             * unnecessary blocking read from socket channel.
             */
            assert TestHookExecute.doHookIfSet(preLockHook, this);
            synchronized(appRecvLock) {
                assert TestHookExecute.doHookIfSet(postLockHook, this);
                if (appRecvBuffer.position() > 0) {
                    return TransitionStopCause.DONE;
                }
            }
            switch(unwrapStatus.cause) {
            case CONCURRENT_CHANNEL_READ_TASK:
                channelReadTask.await();
                break;
            case NB_NEEDS_CHNL_READ_DATA:
            case NB_NEEDS_CHNL_READ_HANDSHAKE:
            case NEEDS_APP_READ:
            case DONE:
                return unwrapStatus.cause;
            case PAUSED:
                break;
            default:
                throw new IllegalStateException(
                              String.format(
                                  "Invalid stop cause from " +
                                  "doUnwrap in wrap: %s",
                                  unwrapStatus.cause));
            }
        }
    }

    private class UnwrapStatus {
        private final long produced;
        private final TransitionStopCause cause;
        private UnwrapStatus(long produced, TransitionStopCause cause) {
            this.produced = produced;
            this.cause = cause;
        }
    }

    /**
     * Runs the engine for handshake until it stuck.
     *
     * TransitionStopCause return:
     * - NB_IS_CHNL_WRITE_BUSY
     * - NB_NEEDS_CHNL_READ_DATA
     * - NB_NEEDS_CHNL_READ_HANDSHAKE
     * - NEEDS_APP_READ
     * - NEEDS_HANDSHAKE_TASKS
     * - DONE
     */
    private TransitionStopCause handshake() throws IOException {
        logger.log(FINEST,
                   () -> String.format(
                       "%s handshaking", toShortString()));
        while (true) {
            final SSLEngineResult.HandshakeStatus hsStatus =
                sslEngine.getHandshakeStatus();
            logger.log(FINEST,
                       () ->
                       String.format("%s handshaking status is %s",
                                     toShortString(), hsStatus));
            final TransitionStopCause cause;
            switch(hsStatus) {
            case NEED_WRAP:
                TransitionStopCause wrapCause =
                    doWrap(emptyAppXmitBuffers, 0, 1).cause;
                /*
                 * The following is a work-around for a bug/mismatch between
                 * the documented behavior for NEED_WRAP and the underlying
                 * implementation for TLS1.3 (Java 11.0.2), see JDK-8220703.
                 * Before TLS1.3, the engine will immediately close the
                 * outbound after inbound is done. After TLS1.3, when inbound
                 * is done, the ssl engine will always be in the state of
                 * NEED_WRAP until the outbound is done. We need to return DONE
                 * here or otherwise we will busy loop.
                 */
                if (sslEngine.getHandshakeStatus().
                    equals(SSLEngineResult.HandshakeStatus.NEED_WRAP) &&
                    sslEngine.isInboundDone()) {
                    wrapCause = TransitionStopCause.DONE;
                }
                cause = flushAfterDoWrap(wrapCause);
                break;
            case NEED_UNWRAP:
                cause = doUnwrap().cause;
                break;
            case NEED_TASK:
                if (isBlocking()) {
                    executeTasks();
                    cause = TransitionStopCause.PAUSED;
                } else {
                    cause = TransitionStopCause.NEEDS_HANDSHAKE_TASKS;
                }
                break;
            case NOT_HANDSHAKING:
                return TransitionStopCause.DONE;
            default:
                throw new IllegalStateException(
                              String.format(
                                  "Invalid status when calling " +
                                  "SSLEngine.getHandshakeStatus: %s",
                                  hsStatus));
            }
            if (cause.equals(TransitionStopCause.PAUSED)) {
                continue;
            }
            if (cause.equals(
                TransitionStopCause.CONCURRENT_CHANNEL_WRITE_TASK)) {
                channelWriteTask.await();
                continue;
            }
            if (cause.equals(
                TransitionStopCause.CONCURRENT_CHANNEL_READ_TASK)) {
                channelReadTask.await();
                continue;
            }
            return cause;
        }
    }

    /**
     * Wraps once and handle the result.
     *
     * TransitionStopCause:
     * - CONCURRENT_CHANNEL_WRITE_TASK
     * - NB_IS_CHNL_WRITE_BUSY
     * - PAUSED: wrapped once
     * - DONE: wrap returned CLOSED
     */
    private WrapStatus doWrap(ByteBuffer[] bufs, int offset, int length)
        throws IOException {

        assert TestHookExecute.doHookIfSet(preLockHook, this);
        synchronized(netXmitLock) {
            assert TestHookExecute.doHookIfSet(postLockHook, this);
            final SSLEngineResult result =
                sslEngine.wrap(bufs, offset, length, netXmitBuffer);
            final int consumed = result.bytesConsumed();
            final SSLEngineResult.Status status = result.getStatus();
            switch(status) {
            case BUFFER_OVERFLOW:
                if (expandNetXmitBuffer()) {
                    return new WrapStatus(consumed, TransitionStopCause.PAUSED);
                }
                final TransitionStopCause fcause = channelWriteTask.run();
                if (fcause.equals(TransitionStopCause.DONE)) {
                    return new WrapStatus(
                                   consumed,
                                   TransitionStopCause.PAUSED);
                }
                return new WrapStatus(consumed, fcause);
            case CLOSED:
                /* The closeTask will take over from here, in WRAP_DONE */
                closeTask.setOutStatus(OutStatus.WRAP_DONE);
                return new WrapStatus(consumed,
                                      TransitionStopCause.DONE);
            case OK:
                /* Wrapped or not our turn to wrap */
                if (result.getHandshakeStatus() ==
                    SSLEngineResult.HandshakeStatus.FINISHED) {
                    validateCredentials();
                }
                return new WrapStatus(consumed, TransitionStopCause.PAUSED);
            default:
                throw new IllegalStateException(
                              String.format(
                                  "Invalid status for wrap: %s", status));
            }
        }
    }

    /**
     * Expands the netXmitBuffer if necessary.
     *
     * @return {@code true} if actually expanded
     */
    private boolean expandNetXmitBuffer() {
        if (!Thread.holdsLock(netXmitLock)) {
            throw new IllegalStateException("Must hold netXmitLock");
        }
        final int required = sslSession.getPacketBufferSize();
        if (required <= netXmitBuffer.capacity()) {
            return false;
        }
        netXmitBuffer = expand(netXmitBuffer, required);
        return true;
    }

    private ByteBuffer expand(ByteBuffer original, int required) {
        final ByteBuffer replacement = ByteBuffer.allocate(required);
        original.flip();
        replacement.put(original);
        return replacement;
    }

    /**
     * Unwraps once and handle the result.
     *
     * TransitionStopCause:
     * - CONCURRENT_CHANNEL_READ_TASK
     * - NB_NEEDS_CHNL_READ_DATA
     * - NB_NEEDS_CHNL_READ_HANDSHAKE
     * - NEEDS_APP_READ
     * - PAUSED: unwrapped
     * - DONE: unwrap returned CLOSED
     */
    private UnwrapStatus doUnwrap() throws IOException {
        if (Thread.holdsLock(appRecvLock)) {
            throw new IllegalStateException(
                          "Wrong lock order in doUnwrap: " +
                          "appRecvBuffer held when acquiring netRecvBuffer");
        }
        assert TestHookExecute.doHookIfSet(preLockHook, this);
        synchronized(netRecvLock) {
            synchronized(appRecvLock) {
                assert TestHookExecute.doHookIfSet(postLockHook, this);
                final SSLEngineResult result;
                try {
                    netRecvBuffer.flip();
                    result =
                        sslEngine.unwrap(netRecvBuffer, appRecvBuffer);
                } finally {
                    netRecvBuffer.compact();
                }
                final SSLEngineResult.Status status = result.getStatus();
                switch(status) {
                case BUFFER_UNDERFLOW:
                    if (expandNetRecvBuffer()) {
                        return new UnwrapStatus(result.bytesProduced(),
                                                TransitionStopCause.PAUSED);
                    }
                    return new UnwrapStatus(
                                   result.bytesProduced(),
                                   readChannelForBufUnderflow());
                case BUFFER_OVERFLOW:
                    if (expandAppRecvBuffer()) {
                        return new UnwrapStatus(result.bytesProduced(),
                                                TransitionStopCause.PAUSED);
                    }
                    asyncIOIncompletion.needsAppRead(true);
                    return new UnwrapStatus(
                                   result.bytesProduced(),
                                   TransitionStopCause.NEEDS_APP_READ);
                case CLOSED:
                    /*
                     * Close the inbound. The close task should do this also,
                     * but just in case.
                     */
                    closeTask.closeInbound();
                    return new UnwrapStatus(
                                   result.bytesProduced(),
                                   TransitionStopCause.DONE);
                case OK:
                    /* Unwrapped or not our turn to unwrap */
                    if (result.getHandshakeStatus() ==
                        SSLEngineResult.HandshakeStatus.FINISHED) {
                        validateCredentials();
                    }
                    return new UnwrapStatus(result.bytesProduced(),
                                            TransitionStopCause.PAUSED);
                default:
                    throw new IllegalStateException(
                                  String.format(
                                      "Invalid status for unwrap: %s",
                                      status));
                }
            }
        }
    }

    /**
     * Expands the netRecvBuffer if necessary.
     *
     * @return {@code true} if actually expanded
     */
    private boolean expandNetRecvBuffer() {
        if (!Thread.holdsLock(netRecvLock)) {
            throw new IllegalStateException("Must hold netRecvLock");
        }
        final int required = sslSession.getPacketBufferSize();
        if (required <= netRecvBuffer.capacity()) {
            return false;
        }
        netRecvBuffer = expand(netRecvBuffer, required);
        return true;
    }

    /**
     * Expands the appRecvBuffer if necessary.
     *
     * @return {@code true} if actually expanded
     */
    private boolean expandAppRecvBuffer() {
        if (!Thread.holdsLock(appRecvLock)) {
            throw new IllegalStateException("Must hold appRecvLock");
        }
        final int required = sslSession.getPacketBufferSize();
        if (required <= appRecvBuffer.capacity()) {
            return false;
        }
        appRecvBuffer = expand(appRecvBuffer, required);
        return true;
    }

    /**
     * Read channel after unwrap returns BUFFER_UNDERFLOW.
     *
     * TransitionStopCause:
     * - CONCURRENT_CHANNEL_READ_TASK
     * - NB_NEEDS_CHNL_READ_DATA
     * - NB_NEEDS_CHNL_READ_HANDSHAKE
     * - PAUSED: unwrapped
     */
    private TransitionStopCause readChannelForBufUnderflow()
        throws IOException {
        if (!Thread.holdsLock(netRecvLock)) {
            throw new IllegalStateException(
                          "readChannelForBufUnderflow " +
                          "must hold netRecvBuffer lock");
        }
        final SSLEngineResult.HandshakeStatus hsStatus =
            sslEngine.getHandshakeStatus();
        final ChannelReadTaskStatus readStatus = channelReadTask.run();
        if (readStatus.equals(ChannelReadTaskStatus.CONCURRENT_READ)) {
            return TransitionStopCause.CONCURRENT_CHANNEL_READ_TASK;
        }
        if (readStatus.equals(ChannelReadTaskStatus.READ_DATA)) {
            asyncIOIncompletion.needsChannelReadData(false);
            asyncIOIncompletion.needsChannelReadHandshake(false);
            return TransitionStopCause.PAUSED;
        }
        if (hsStatus.equals(HandshakeStatus.NOT_HANDSHAKING)) {
            asyncIOIncompletion.needsChannelReadData(true);
            return TransitionStopCause.NB_NEEDS_CHNL_READ_DATA;
        }
        asyncIOIncompletion.needsChannelReadHandshake(true);
        return TransitionStopCause.NB_NEEDS_CHNL_READ_HANDSHAKE;
    }

    /**
     * Validates the credentials.
     */
    private void validateCredentials() throws SSLException {
        final SSLSession session = sslEngine.getSession();
        logger.log(FINE,
                   () ->
                   String.format("%s SSL protocol is %s, " +
                                 "cipher suite is %s, " +
                                 "local certs are %s, " +
                                 "hostVerifier is %s, " +
                                 "authenticator is %s, " +
                                 "validating credentials",
                                 toShortString(),
                                 session.getProtocol(),
                                 session.getCipherSuite(),
                                 Arrays.toString(
                                     session.getLocalCertificates()),
                                 hostVerifier, authenticator));
        if (sslEngine.getUseClientMode()) {
            if (hostVerifier != null) {
                peerTrusted =
                    hostVerifier.verify(targetHost,
                                        sslEngine.getSession());
                if (peerTrusted) {
                    logger.log(FINE,
                               () ->
                               String.format(
                                   "%s SSL host verifier reports that " +
                                   "connection target is valid",
                                   toShortString()));
                } else {
                    logger.log(INFO,
                               () ->
                               String.format(
                                   "%s SSL host verifier reports that " +
                                   "connection target is NOT valid",
                                   toShortString()));
                    throw new SSLException(
                                  "Server identity could not be verified");
                }
            }
        } else {
            if (authenticator != null) {
                peerTrusted =
                    authenticator.isTrusted(sslEngine.getSession());
                if (peerTrusted) {
                    logger.log(FINE,
                               () ->
                               String.format(
                                   "%s SSL authenticator reports that " +
                                   "channel is trusted",
                                   toShortString()));
                } else {
                    logger.log(INFO,
                               () ->
                               String.format(
                                   "%s SSL authenticator reports that " +
                                   "channel is NOT trusted",
                                   toShortString()));
                    /*
                     * If client authentication is required, then fail if the
                     * client identity was not verified. If client
                     * authentication is optional (getWantClientAuth) then
                     * allow the connection to complete. Whoever is using the
                     * connection should check if the connection is trusted if
                     * it wants to perform an operation on the connection that
                     * requires client authentication. [KVSTORE-2344]
                     */
                    if (sslEngine.getNeedClientAuth()) {
                        throw new SSLException(
                            "Client identity could not be verified");
                    }
                }
            }
        }
    }

    /**
     * Manages the causes of incomplete async IO operations (read, write, flush
     * and close) and actions to continue.
     */
    public class AsyncIOIncompletion {

        /*
         * The flag to indicate the channel is busy writing. Must be modified
         * inside the netXmitBuffer synchronization block. This guarantees the
         * atomicity of the socket channel write and the flag modification such
         * that the flag always reflects the status of last socket channel
         * write. Volatile for read thread-safety.
         */
        private volatile boolean isChannelWriteBusy = false;
        /*
         * The flag to indicate the last IO operation stopped because it needs
         * to read more data. Must be modified inside the netRecvBuffer
         * synchronization block to guarantee atomicity. Similar to above.
         */
        private volatile boolean needsChannelReadData = false;
        /*
         * The flag to indicate the last IO operation stopped because it needs
         * to read more data for a handshake. Thread-safety consideration the
         * same as above.
         */
        private volatile boolean needsChannelReadHandshake = false;
        /*
         * The flag to indicate the last IO operation stopped because it needs
         * to read and can't until space is freed in the read buffer by
         * transferring data to the application. Must be modified inside the
         * appRecvBuffer synchronization block to guarantee atomicity. Similar
         * to above.
         */
        private volatile boolean needsAppRead = false;
        /*
         * There is no flag to indicate that we need to execute tasks. This is
         * because we cannot synchronize for atomicity since that will block.
         * And this non-atomicity can cause deadlock. For example, if we use a
         * flag
         *
         * T1                       T2                      T3
         *
         *                          executeTasks()
         *                                                  call read()
         *                                                  set flag for read
         *                                                  channel
         * call read()
         * case NEED_TASK:
         *     set flag for task
         *                          clear flag for task
         *
         *
         * At this point, the application needs to execute tasks, but the flags
         * reflect that it needs to wait for read. The application will wait
         * for read causing deadlock.
         *
         * So we always query the sslEngine for task execution status.
         */

        private void isChannelWriteBusy(boolean val) {
            if (!Thread.holdsLock(netXmitLock)) {
                throw new IllegalStateException(
                    "Modify isChannelWriteBusy must hold netXmitBuffer lock");
            }
            isChannelWriteBusy = val;
        }

        private void needsChannelReadData(boolean val) {
            if (!Thread.holdsLock(netRecvLock)) {
                throw new IllegalStateException(
                    "Modify needsChannelReadData must hold netRecvBuffer lock");
            }
            needsChannelReadData = val;
        }

        private void needsChannelReadHandshake(boolean val) {
            if (!Thread.holdsLock(netRecvLock)) {
                throw new IllegalStateException(
                    "Modify needsChannelReadHandshake must hold" +
                    " netRecvBuffer lock");
            }
            needsChannelReadHandshake = val;
        }

        private void needsAppRead(boolean val) {
            if (!Thread.holdsLock(appRecvLock)) {
                throw new IllegalStateException(
                    "Modify needsAppRead must hold appRecvBuffer lock");
            }
            needsAppRead = val;
        }

        /**
         * Returns the next action to take for an incomplete operation.
         *
         * Notes for thread safety:
         * - Thread safety issue is complicated because the non-atomicity of
         *   the operation and the action-query method.
         * - We should be cautious not to return an action that waits for an
         *   event that never happens.
         * - We are allowed to return an action that will not make any
         *   progress, but should be cautious not to busy wait also.
         * - Since the flag modification is synchronized inside the operation,
         *   we are guaranteed that when we see a incompletion cause, one of
         *   the preceding operation is stopped by that cause.
         * - Our principle here is we return an action that will solve the
         *   problem for someone, not necessarily ourselves. We will be making
         *   progress and evetually it will be our turn to retry the operation.
         */
        private AsyncIO.ContinueAction nextAction(AsyncIO.Type type) {
            /* Flush does not involve handshake at all. */
            if (type.equals(AsyncIO.Type.FLUSH)) {
                if (isChannelWriteBusy) {
                    return AsyncIO.ContinueAction.
                        WAIT_FOR_CHNL_WRITE_THEN_FLUSH;
                }
                return AsyncIO.ContinueAction.RETRY_NOW;
            }

            /*
             * Do actions that can just happen first. The application can just
             * take these action and retry the IO operation.
             */
            /* Read from sslEngine to query if we need tasks. */
            if (sslEngine.getHandshakeStatus().
                equals(HandshakeStatus.NEED_TASK)) {
                return AsyncIO.ContinueAction.WAIT_FOR_TASKS_EXECUTION;
            }
            if (needsAppRead) {
                return AsyncIO.ContinueAction.APP_READ;
            }
            /*
             * Check channel busy first since the reason we cannot read may
             * resulted from not being able to write. However, if the actual
             * incompletion cause for the IO is not channel busy, we might have
             * a busy loop: a read registers for channel write, gets called
             * back, reads again and exits incompleted, without any channel
             * write, asked to register for channel write again. The solution
             * is the application must flush before retries the IO operation.
             */
            if (isChannelWriteBusy) {
                return AsyncIO.ContinueAction.WAIT_FOR_CHNL_WRITE_THEN_FLUSH;
            }
            /*
             * We don't have a similar problem as above with this case. First,
             * if the cause is channel write busy but not waiting for channel
             * read, we would have returned already. Moreover, write will
             * unwrap if needed so we are making progress.
             */
            if (needsChannelReadHandshake) {
                return AsyncIO.ContinueAction.WAIT_FOR_CHNL_READ;
            }
            /*
             * If we are writing, we should not be waiting for a channel read
             * data. Otherwise, we might have a deadlock, since there might not
             * be any more data.
             */
            if (type.equals(AsyncIO.Type.WRITE)) {
                return AsyncIO.ContinueAction.RETRY_NOW;
            }
            if (needsChannelReadData) {
                return AsyncIO.ContinueAction.WAIT_FOR_CHNL_READ;
            }
            return AsyncIO.ContinueAction.RETRY_NOW;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Void> executeTasks(ExecutorService executor) {
        final CompletableFuture<Void> cf = new CompletableFuture<>();
        executor.submit(() -> {
            try {
                executeTasks();
                cf.complete(null);
            } catch (Throwable t) {
                cf.completeExceptionally(t);
            }
        });
        return cf;
    }

    private void executeTasks() {
        assert TestHookExecute.doHookIfSet(taskHook, this);
        final long ts = System.nanoTime();
        Runnable task;
        while ((task = sslEngine.getDelegatedTask()) != null) {
            task.run();
        }
        logger.log(FINE,
                   () ->
                   String.format("%s executed tasks in %s ms",
                                 toShortString(),
                                 (System.nanoTime() - ts) / 1.0e6));
    }

    /**
     * A socket channel task.
     *
     * The task enables the following features:
     * (1) Only one thread can be running, other threads will wait
     * (2) The running thread can be interrupted
     * (3) ensures a close quiescent transition
     *
     * To achieve this, subclass must call transitToInProgress before running
     * and call transitToIdle after.
     */
    private abstract class ChannelTask {

        /*
         * A lock to ensure the task is run by only one thread. Only used in
         * this class.
         */
        protected final Object taskLock = new Object();
        /*
         * The thread running the task. Acquire taskLock when access.
         */
        protected Thread runningThread = null;
        /*
         * An optimization so that we do not need to call Thread.interrupted()
         * all the time.
         */
        private boolean interrupted = false;

        /**
         * Sets the runningThread if it is null.
         *
         * @return {@code true} if successful, otherwise there is another
         * thread running the task
         */
        protected boolean transitToInProgress() throws IOException {
            assert TestHookExecute.doHookIfSet(
                preLockHook, SSLDataChannel.this);
            synchronized(taskLock) {
                assert TestHookExecute.doHookIfSet(
                    postLockHook, SSLDataChannel.this);
                if (runningThread != null) {
                    return false;
                }
                crossCloseQuiescentBarrier();
                runningThread = Thread.currentThread();
                return true;
            }
        }

        /**
         * Sets the runningThread to null and notifies others.
         */
        protected void transitToIdle() throws IOException {
            assert TestHookExecute.doHookIfSet(
                preLockHook, SSLDataChannel.this);
            synchronized(taskLock) {
                assert TestHookExecute.doHookIfSet(
                    postLockHook, SSLDataChannel.this);
                runningThread = null;
                taskLock.notifyAll();
                /*
                 * It is possible that the close procedure interrupted the
                 * runningThread after we exit from the IO operation, but
                 * before we flip the runningThread to null. Clear the
                 * interrupted status with Thread.interrupted (note, not
                 * thread.isInterrupted) so that we do not leak this
                 * InterruptedException.
                 */
                if (interrupted && Thread.interrupted()) {
                    interrupted = false;
                    throw new InterruptedIOException();
                }
            }
        }

        /**
         * Interrupts the runningThread if it is not null, waits until it is
         * null.
         */
        void interruptAndAwaitQuiescent() throws IOException {
            assert TestHookExecute.doHookIfSet(
                preLockHook, SSLDataChannel.this);
            synchronized(taskLock) {
                assert TestHookExecute.doHookIfSet(
                    postLockHook, SSLDataChannel.this);
                if (runningThread != null) {
                    runningThread.interrupt();
                    interrupted = true;
                    logger.log(FINE,
                               () ->
                               String.format("%s interrupted %s",
                                             toShortString(),
                                             getClass().getSimpleName()));
                }
                while (runningThread != null) {
                    try {
                        taskLock.wait();
                    } catch (InterruptedException ie) {
                        throw new InterruptedIOException(
                                      "Interrupted when waiting for " +
                                      "an IO operation to finish " +
                                      "during close");
                    }
                }
            }
        }

        /**
         * Awaits for the currently running channel task to be done.
         *
         * Awaits until the runningThread is null. Though waiting, we are not
         * waiting for a blocking event for non-blocking channels. If the
         * underlying SocketChannel is non-blocking, the inherited channel
         * tasks (ChannelWriteTask and ChannelReadTask) are local network data
         * transfers for when there is enough space (for SocketChannel#write)
         * or data already available (for SocketChannel#read). Hence we are
         * waiting for a non-blocking event to be done. Implementations should
         * ensure not holding locks held in the run methods to avoid deadlock,
         * [KVSTORE-1021].
         */
        abstract void await() throws IOException;

        protected void awaitInternal() throws IOException {
            int count = 0;
            while (true) {
                synchronized(taskLock) {
                    if (runningThread == null) {
                        return;
                    }
                    if (count++ < CHANNEL_TASK_BUSY_LOOP_COUNT) {
                        continue;
                    }
                    try {
                        taskLock.wait();
                        count = 0;
                    } catch (InterruptedException ie) {
                        /*
                         * Should throw InterruptedIOException since,
                         * otherwise, the fact that this thread has been
                         * interrupted (say by StoppableThread.shutdownThread)
                         * might be forgotten.
                         */
                        throw new InterruptedIOException(
                            "Interrupted when waiting for " +
                            "an IO operation to finish");
                    }
                }
            }
        }
    }

    /**
     * Encapsulates the procedure of writing to socket channel.
     *
     * Write must synchronize on netXmitBuffer, i.e., for exclusive access
     * against sslEngine wrap
     *
     * Immediate returns when another thread already running the task.
     */
    private class ChannelWriteTask extends ChannelTask {

        private TransitionStopCause lastStatus = null;

        /**
         * Runs the task.
         *
         * TransitionStopCause
         * - CONCURRENT_CHANNEL_WRITE_TASK
         * - NB_IS_CHNL_WRITE_BUSY
         * - DONE
         */
        private TransitionStopCause run() throws IOException {
            if (!transitToInProgress()) {
                lastStatus = TransitionStopCause.CONCURRENT_CHANNEL_WRITE_TASK;
                return lastStatus;
            }
            try {
                assert TestHookExecute.doHookIfSet(
                    preLockHook, SSLDataChannel.this);
                synchronized(netXmitLock) {
                    assert TestHookExecute.doHookIfSet(
                        postLockHook, SSLDataChannel.this);
                    logger.log(FINEST,
                               () ->
                               String.format("%s socket channel writing, " +
                                             "%s bytes to write",
                                             toShortString(),
                                             netXmitBuffer.position()));
                    if (netXmitBuffer.position() == 0) {
                        /* We do not have any data to flush. */
                        asyncIOIncompletion.isChannelWriteBusy(false);
                        lastStatus = TransitionStopCause.DONE;
                        return lastStatus;
                    }
                    try {
                        netXmitBuffer.flip();
                        lastStatus = writeChannel();
                        return lastStatus;
                    } finally {
                        netXmitBuffer.compact();
                    }
                }
            } finally {
                transitToIdle();
            }
        }

        /**
         * Runs the task, awaits if CONCURRENT_CHANNEL_TASK.
         *
         * TransitionStopCause
         * - NB_IS_CHNL_WRITE_BUSY
         * - DONE
         */
        private TransitionStopCause runOrAwait() throws IOException {
            while (true) {
                final TransitionStopCause cause = run();
                if (!cause.equals(
                    TransitionStopCause.CONCURRENT_CHANNEL_WRITE_TASK)) {
                    return cause;
                }
                await();
            }
        }

        /**
         * Writes to the channel.
         *
         * Writes all netXmitBuffer or blocks if blocking channel. Writes all
         * netXmitBuffer or return channel busy status if non-blocking.
         *
         * TransitionStopCause:
         * - NB_IS_CHNL_WRITE_BUSY
         * - DONE
         */
        private TransitionStopCause writeChannel() throws IOException {
            if (!Thread.holdsLock(netXmitLock)) {
                throw new IllegalStateException(
                              "Write channel without locking " +
                              "on the netXmitBuffer");
            }
            while (true) {
                assert TestHookExecute.doHookIfSet(
                    ioHook, SSLDataChannel.this);
                final int count = socketChannel.write(netXmitBuffer);
                logger.log(FINEST,
                           () ->
                           String.format(
                               "%s socket channel wrote %s bytes, " +
                               "netXmitBuffer remaining %s bytes",
                               toShortString(),
                               count, netXmitBuffer.remaining()));
                if (netXmitBuffer.remaining() == 0) {
                    asyncIOIncompletion.isChannelWriteBusy(false);
                    return TransitionStopCause.DONE;
                }
                if (isBlocking()) {
                    continue;
                }
                /* non-blocking */
                if (count != 0) {
                    continue;
                }
                asyncIOIncompletion.isChannelWriteBusy(true);
                return TransitionStopCause.NB_IS_CHNL_WRITE_BUSY;
            }
        }

        @Override
        void await() throws IOException {
            if (Thread.holdsLock(netXmitLock)) {
                throw new IllegalStateException(String.format(
                    "Holding netXmitBuffer while waiting"));
            }
            awaitInternal();
        }

        public JsonObject toJson() {
            final Thread runningThreadValue;
            final TransitionStopCause lastStatusValue;
            synchronized(taskLock) {
                runningThreadValue = runningThread;
                lastStatusValue = lastStatus;
            }
            final Map<String, Object> ret = new HashMap<>();
            ret.put("runningThread", Objects.toString(runningThreadValue));
            ret.put("lastStatus", Objects.toString(lastStatusValue));
            return new JsonObject(ret);
        }
    }

    /**
     * Encapsulates the procedure of reading from socket channel.
     *
     * Read must synchronize on netRecvBuffer, i.e., for exclusive access
     * against other reads as well as sslEngine unwrap.
     */
    private class ChannelReadTask extends ChannelTask {

        /*
         * The status of the last run method. No need for synchronization:
         * writes to this field are done inside run which is surrounded by
         * transitToInProgress and transitToIdle which in turn crosses the
         * taskLock barriers;  reads are within the taskLock.
         */
        private ChannelReadTaskStatus lastStatus = null;

        private ChannelReadTaskStatus run() throws IOException {
            if (!Thread.holdsLock(netRecvLock)) {
                throw new IllegalStateException(
                              "Channel read task must be " +
                              "synchronized on netRecvBuffer");
            }
            if (!transitToInProgress()) {
                lastStatus = ChannelReadTaskStatus.CONCURRENT_READ;
                return lastStatus;
            }
            try {
                logger.log(FINEST,
                           () ->
                           String.format("%s socket channel reading " +
                                         "netRecvBuffer.pos=%s, lim=%s",
                                         toShortString(),
                                         netRecvBuffer.position(),
                                         netRecvBuffer.limit()));
                ReadableByteChannel rchnl = wrappedReadChannel;
                /*
                 * Possibly seeing NULL_READ_CHANNEL when a concurrent thread
                 * is configureBlocking.
                 */
                if (rchnl == NULL_READ_CHANNEL) {
                    synchronized(SSLDataChannel.this) {
                        rchnl = wrappedReadChannel;
                    }
                }
                assert TestHookExecute.doHookIfSet(
                    ioHook, SSLDataChannel.this);
                final int n = rchnl.read(netRecvBuffer);
                logger.log(FINEST,
                           () ->
                           String.format(
                               "%s socket channel read %s bytes, " +
                               "netRecvBuffer.pos=%s, lim=%s",
                               toShortString(), n,
                               netRecvBuffer.position(),
                               netRecvBuffer.limit()));
                if (n < 0) {
                    closeTask.onChannelReadDone();
                }
                lastStatus = (n <= 0)
                    ? ChannelReadTaskStatus.READ_NO_DATA
                    : ChannelReadTaskStatus.READ_DATA;
                return lastStatus;
            } finally {
                transitToIdle();
            }
        }

        @Override
        void await() throws IOException {
            if (Thread.holdsLock(netRecvLock)) {
                throw new IllegalStateException(String.format(
                    "Holding netRecvBuffer while waiting"));
            }
            awaitInternal();
        }

        public JsonObject toJson() {
            final Thread runningThreadValue;
            final ChannelReadTaskStatus lastStatusValue;
            synchronized(taskLock) {
                runningThreadValue = runningThread;
                lastStatusValue = lastStatus;
            }
            final Map<String, Object> ret = new HashMap<>();
            ret.put("runningThread", Objects.toString(runningThreadValue));
            ret.put("lastStatus", Objects.toString(lastStatusValue));
            return new JsonObject(ret);
        }
    }

    private enum ChannelReadTaskStatus {
        /* Concurrent read task in progress */
        CONCURRENT_READ,

        /* Channel read was attempted but no data was returned */
        READ_NO_DATA,

        /* Channel read some data */
        READ_DATA,
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        logger.log(FINEST,
                   () ->
                   String.format("%s data channel sync closing",
                                 toShortString()));
        try {
            ensureCloseForBlocking();
            closeTask.run();
        } catch (Throwable t) {
            closeForcefullyInternal(t);
        }
    }

    private void closeForcefullyInternal(Throwable t) throws IOException {
        try {
            try {
                sslEngine.closeOutbound();
                sslEngine.closeInbound();
            } finally {
                closeTask.finish();
            }
        } catch (SSLException ssle) {
            /* Ignore SSL exceptions since we are closing forcefully */
        } catch (IOException ioe) {
            if (t == null) {
                throw ioe;
            }
            t.addSuppressed(ioe);
        }
        if (t == null) {
            return;
        }
        if (t instanceof IOException) {
            throw (IOException) t;
        } else if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        } else if (t instanceof Error) {
            throw (Error) t;
        }
        throw new RuntimeException(t);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean closeAsync() throws IOException {
        logger.log(FINEST,
                   () -> String.format("%s data channel async closing",
                                       toShortString()));
        try {
            ensureCloseAsyncForNonBlocking();
            return closeTask.run();
        } catch (Throwable t) {
            closeForcefullyInternal(t);
            return true;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void closeForcefully() throws IOException {
        logger.log(FINEST,
                   () ->
                   String.format("%s data channel closing forcefully",
                                 toShortString()));
        closeForcefullyInternal(null);
    }

    /**
     * Encapsulates the close procedure.
     */

    /* Close task statuses. */
    private enum CloseTaskStatus {
        /* Initial status */
        STANDBY,
        /*
         * First close method is called, crossCloseQuiescentBarrier should
         * throw AsynchronousCloseException.
         */
        RUNNING,
        /*
         * Close is done, crossCloseQuiescentBarrier should throw
         * ClosedChannelException.
         */
        DONE,
    }

    private enum OutStatus {
        /* Initial status */
        STANDBY,
        /*
         * SSLEngine.closeOutbound() has been called, but still need to
         * call wrap() until it returns close.
         */
        NEED_LAST_WRAP,
        /*
         * SslEngine.isOutboundDone(), equivalently, sslEngine.wrap()
         * returned CLOSED
         */
        WRAP_DONE,
        /* WRAP_DONE and netXmitBuffer is empty */
        CHNL_WRITE_DONE,
    }

    private enum InStatus {
        /* Initial status */
        STANDBY,
        /*
         * Received eof, after which we should drain netRecvBuffer and call
         * sslEngine.closeInbound()
         */
        CHNL_READ_DONE,
        /*
         * SslEngine.isInboundDone(), equivalently,
         * received peer's close_notify and unwrap() will return CLOSED,
         * though we might not yet received eof, we can consider we have
         * already reached that
         */
        UNWRAP_DONE,
    }

    private class CloseTask {


        /*
         * Ensure only one thread is doing the close work. No method other than
         * of this object should grab this lock.
         */
        private final ReentrantLock closeLock = new ReentrantLock();

        /*
         * Fields are modified inside synchronization block of this object.
         * Volatile for read thread-safety.
         */

        private volatile CloseTaskStatus taskStatus = CloseTaskStatus.STANDBY;
        private volatile InStatus inStatus = InStatus.STANDBY;
        private volatile OutStatus outStatus = OutStatus.STANDBY;

        private boolean isStandby() {
            return taskStatus.equals(CloseTaskStatus.STANDBY);
        }

        private boolean isRunning() {
            return taskStatus.equals(CloseTaskStatus.RUNNING);
        }

        private boolean isDone() {
            return taskStatus.equals(CloseTaskStatus.DONE);
        }

        private synchronized void setTaskStatus(CloseTaskStatus s) {
            if (taskStatus.compareTo(s) < 0) {
                taskStatus = s;
            }
        }

        private synchronized void setInStatus(InStatus s) {
            if (inStatus.compareTo(s) < 0) {
                inStatus = s;
            }
        }

        private synchronized void setOutStatus(OutStatus s) {
            if (outStatus.compareTo(s) < 0) {
                outStatus = s;
            }
        }

        private void onChannelReadDone() {
            setInStatus(InStatus.CHNL_READ_DONE);
        }

        private boolean isChannelReadDone() {
            return inStatus.compareTo(InStatus.CHNL_READ_DONE) >= 0;
        }

        private synchronized void closeInbound() {
            setInStatus(InStatus.UNWRAP_DONE);
            try {
                sslEngine.closeInbound();
            } catch (SSLException ssle) {
                /*
                 * A truncation exception may be thrown because we close the
                 * engine before we got the close notify. There is nothing we
                 * need to do about this. The only reason a truncation attack
                 * is interesting, that we can think of, is if the application
                 * gives semantic meaning to an EOF. In other words, there
                 * could be a protocol which reads until EOF as the only way of
                 * knowing that it has received everything. In that case, a
                 * truncation could make it think that it received everything
                 * when it hadn't. All of our application messages include
                 * information about how long they are, so this really isn't
                 * any issue.
                 */
            }
        }

        /**
         * Runs the close procedure.
         *
         * For blocking channel, the first close will transit through the close
         * procedure, blocks if necessary, returns when done.
         *
         * For non-blocking channel, transit through the close procedure,
         * returns whether the close operation is complete.
         */
        private boolean run() throws IOException {
            insideCloseMethod.set(true);
            closeLock.lock();
            try {
                prepare();
                return transit();
            } catch (Throwable t) {
                logger.log(FINEST,
                           () ->
                           String.format(
                               "%s got exception when running close task: %s",
                               toShortString(),
                               CommonLoggerUtils.getStackTrace(t)));
                throw t;
            } finally {
                closeLock.unlock();
                insideCloseMethod.set(false);
            }
        }

        private void prepare() throws IOException {
            /*
             * Step 1: set close task status.
             */
            setTaskStatus(CloseTaskStatus.RUNNING);

            /*
             * Step 2: ensure close quiescent, after which the close procedure
             * is the only thread that does work for the channelWriteTask and
             * channelReadTask. This is done by two measures: (1) every channel
             * read/write task must run crossCloseQuiescentBarrier() before
             * doing any actual work; after step 1, no new read/write task can
             * do work unless inside close method; (2) we interrupt the
             * concurrent thread running read and write task here; interrupt
             * the thread during blocking I/O will close the channel; see
             * InterruptibleChannel.
             */
            channelWriteTask.interruptAndAwaitQuiescent();
            channelReadTask.interruptAndAwaitQuiescent();

            /*
             * Step 3: kick off close procedure. A close method is called on
             * two occasions: (1) Application initiates a close. (2)
             * Application reads eof then proceeds to call close in response.
             * In both cases application write is done.
             */
            sslEngine.closeOutbound();
            setOutStatus(OutStatus.NEED_LAST_WRAP);

            logger.log(FINEST,
                       () ->
                       String.format("%s prepared for channel closing",
                                     toShortString()));
        }

        /**
         * Transits the close status until blocked.
         *
         * Transits the close status until got stuck.
         */
        private boolean transit() throws IOException {
            logger.log(FINEST,
                       () ->
                       String.format("%s in close transition, " +
                                     "current state: (%s, %s)",
                                     toShortString(),
                                     inStatus, outStatus));
            while (!isDone()) {
                final OutStatus prevOS = outStatus;
                final InStatus prevIS = inStatus;
                TransitionStopCause cause = transitOnce();
                logger.log(FINEST,
                           () ->
                           String.format(
                               "%s close transitted one step, " +
                               "from (%s, %s) to (%s, %s), stop cause is %s",
                               toShortString(),
                               prevIS, prevOS, inStatus, outStatus, cause));
                switch(cause) {
                case PAUSED:
                    continue;
                case NB_IS_CHNL_WRITE_BUSY:
                case NB_NEEDS_CHNL_READ_DATA:
                case NB_NEEDS_CHNL_READ_HANDSHAKE:
                case NEEDS_HANDSHAKE_TASKS:
                    return false;
                default:
                    /* DONE and NEEDS_APP_READ invalid */
                    throw new IllegalStateException(
                                  String.format(
                                      "Invalid state for close transition: %s",
                                      cause));
                }
            }
            logger.log(FINEST,
                       () ->
                       String.format("%s in close transition done",
                                     toShortString()));
            return true;
        }

        /**
         * Transits the outbound close status once.
         *
         * Transits the outbound close status to the next one. When closing, we
         * do not care about the inbound close state.
         *
         * TransitionStopCause return:
         * - NB_IS_CHNL_WRITE_BUSY
         * - NB_NEEDS_CHNL_READ_DATA
         * - NB_NEEDS_CHNL_READ_HANDSHAKE
         * - NEEDS_HANDSHAKE_TASKS
         * - PAUSED
         */
        private TransitionStopCause transitOnce() throws IOException {
            final OutStatus ostatus = outStatus;
            switch(ostatus) {
            case STANDBY:
                throw new IllegalStateException(
                              "Close method should " +
                              "transit outbound state out from STANDBY");
            case NEED_LAST_WRAP:
                return doLastWraps();
            case WRAP_DONE:
                channelWriteTask.runOrAwait();
                /* Flushed once if non-blocking, or flushed everything */
                assert TestHookExecute.doHookIfSet(
                    preLockHook, SSLDataChannel.this);
                synchronized(netXmitLock) {
                    assert TestHookExecute.doHookIfSet(
                        postLockHook, SSLDataChannel.this);
                    if (netXmitBuffer.position() != 0) {
                        return TransitionStopCause.NB_IS_CHNL_WRITE_BUSY;
                    }
                }
                setOutStatus(OutStatus.CHNL_WRITE_DONE);
                return TransitionStopCause.PAUSED;
            case CHNL_WRITE_DONE:
                closeInbound();
                finish();
                return TransitionStopCause.PAUSED;
            default:
                throw new IllegalStateException(
                              "Invalid outbound state " + ostatus);
            }
        }

        private TransitionStopCause doLastWraps() throws IOException {
            /* Provide some room in netXmitBuffer if possible */
            channelWriteTask.runOrAwait();
            while (true) {
                final WrapStatus wrapStatus = wrap();
                logger.log(FINEST,
                           () ->
                           String.format(
                               "%s data channel close " +
                               "wrap stop cause is %s, consumed %s bytes",
                               toShortString(),
                               wrapStatus.cause, wrapStatus.consumed));
                switch(wrapStatus.cause) {
                case NEEDS_APP_READ:
                    /* We are closing, no one is reading. */
                    assert TestHookExecute.doHookIfSet(
                        preLockHook, SSLDataChannel.this);
                    synchronized(appRecvLock) {
                        assert TestHookExecute.doHookIfSet(
                            postLockHook, SSLDataChannel.this);
                        appRecvBuffer.clear();
                    }
                    continue;
                case NEEDS_HANDSHAKE_TASKS:
                case NB_IS_CHNL_WRITE_BUSY:
                case NB_NEEDS_CHNL_READ_DATA:
                case NB_NEEDS_CHNL_READ_HANDSHAKE:
                    return wrapStatus.cause;
                case DONE:
                    if (!sslEngine.isOutboundDone()) {
                        throw new IllegalStateException(
                                      "SSLEngine outbound not closed " +
                                      "after wrap() returns DONE " +
                                      "during doLastWraps");
                    }
                    setOutStatus(OutStatus.WRAP_DONE);
                    return TransitionStopCause.PAUSED;
                default:
                    throw new IllegalStateException(
                                  "Invalid stop cause " +
                                  "for wrap in doLastWraps: " +
                                  wrapStatus.cause);
                }
            }
        }

        private void finish() throws IOException {
            setOutStatus(OutStatus.CHNL_WRITE_DONE);
            setInStatus(InStatus.UNWRAP_DONE);
            setTaskStatus(CloseTaskStatus.DONE);
            socketChannel.close();
            logger.log(FINE,
                       () ->
                       String.format("%s close transition finishing",
                                     toShortString()));
        }

        public JsonObject toJson() {
            final CloseTaskStatus closeTaskStatusValue;
            final InStatus inStatusValue;
            final OutStatus outStatusValue;
            closeLock.lock();
            try {
                closeTaskStatusValue = taskStatus;
                inStatusValue = inStatus;
                outStatusValue = outStatus;
            } finally {
                closeLock.unlock();
            }
            final Map<String, Object> ret = new HashMap<>();
            ret.put("taskStatus", closeTaskStatusValue);
            ret.put("inStatus", inStatusValue);
            ret.put("outStatus", outStatusValue);
            return new JsonObject(ret);
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return toJson().toString();
    }

    public String toShortString() {
        return String.format("%s%s %s",
                             addressPair,
                             isBlocking() ? "BLK" : "NBL",
                             sslEngine.getUseClientMode() ?
                             "client" : "server");
    }

    private JsonObject toJson() {
        final Map<String, Object> ret = new HashMap<>();
        ret.put("addressPair", addressPair);
        ret.put("isBlocking", isBlocking());
        ret.put("isClient", sslEngine.getUseClientMode());
        ret.put("pktBufSize", sslSession.getPacketBufferSize());
        ret.put("appBufSize", sslSession.getApplicationBufferSize());
        ret.put("protocol", sslSession.getProtocol());
        ret.put("sessionCreatedTime", sslSession.getCreationTime());
        ret.put("sessionId", sslSession.getId().toString());
        ret.put("hsStatus", sslEngine.getHandshakeStatus());
        ret.put("writeTask", channelWriteTask.toJson());
        ret.put("readTask", channelReadTask.toJson());
        ret.put("closeTask", closeTask.toJson());
        return new JsonObject(ret);
    }

    /* For testing. */
    public void setNetXmitBuffer(Consumer<ByteBuffer> setter) {
        synchronized(netXmitLock) {
            setter.accept(netXmitBuffer);
        }
    }

    public void shrinkBuffers() {
        synchronized(netXmitLock) {
            netXmitBuffer =
                ByteBuffer.allocate(netXmitBuffer.capacity() / 2);
        }
        synchronized(netRecvLock) {
            netRecvBuffer =
                ByteBuffer.allocate(netRecvBuffer.capacity() / 2);
        }
        synchronized(appRecvLock) {
            appRecvBuffer =
                ByteBuffer.allocate(appRecvBuffer.capacity() / 2);
        }
    }
}
