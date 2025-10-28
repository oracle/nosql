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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.async.DialogHandlerFactoryMap;
import oracle.kv.impl.async.DialogResourceManager;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.EndpointHandlerManager;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.dialog.AbstractDialogEndpointHandler;
import oracle.kv.impl.async.dialog.ChannelDescription;
import oracle.kv.impl.async.dialog.ProtocolReader;
import oracle.kv.impl.async.dialog.ProtocolWriter;
import oracle.kv.impl.async.perf.NioEndpointHandlerPerfTracker;
import oracle.kv.impl.test.ExceptionTestHook;
import oracle.kv.impl.test.ExceptionTestHookExecute;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.EventTrackersManager;
import oracle.kv.impl.util.RateLimitingLogger;

import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.utilint.net.SSLDataChannel;

import org.checkerframework.checker.nullness.qual.Nullable;


public class NioEndpointHandler
    extends AbstractDialogEndpointHandler implements ChannelHandler {

    /** If not null, a hook for endpoint handler creation. */
    public static volatile TestHook<NioEndpointHandler> creationHook;
    /**
     * If not null, a hook for inserting exceptions into read operations, for
     * testing.
     */
    public static volatile
        ExceptionTestHook<NioEndpointHandler, IOException> readHook;

    /*
     * The number of retries allowed to attempt to close input in the backup
     * executor when the designated channel executor rejects the close task.
     */
    private static final int CLOSE_INPUT_IN_BACKUP_EXECUTOR_NUM_RETRIES = 10;

    private final NioChannelExecutor channelExecutor;
    private final ExecutorService backupExecutor;
    private final ScheduledExecutorService backupSchedExecutor;
    private final EndpointConfig endpointConfig;
    private final SocketChannel socketChannel;
    /*
     * The DataChannel wrapped around the socketChannel. Due to the fact that
     * data channels may hold large buffers (e.g., SSLDataChannel), it is
     * better to create it only when needed and de-reference it once not needed
     * for better heap usage. We have seen cases where the endpoint handler is
     * pinned due to a scheduled task or the executor overwhelmed causing
     * unnecessary OOM. See KVSTORE-390, KVSTORE-423, KVSTORE-700.
     */
    private volatile @Nullable DataChannel dataChannel = null;
    private final NioChannelInput channelInput;
    private final NioChannelOutput channelOutput;
    private final ProtocolReader protocolReader;
    private final ProtocolWriter protocolWriter;

    private volatile boolean handedOffToSync = false;

    private final CloseHandler closeHandler = new CloseHandler();
    private final IncompletionHandler incompletionHandler =
        new IncompletionHandler();
    private final ReadWriteInterest rwinterest = new ReadWriteInterest();

    /*
     * Whether is closing, initialized to false, only set to true, volatile for
     * thread-safety.
     */
    private volatile boolean isClosing = false;
    /* A rate limiting logger */
    private final RateLimitingLogger<String> rateLimitingLogger;

    /* Perf tracker. */
    private final NioEndpointHandlerPerfTracker perfTracker;

    /**
     * The reference to this handler to be used by tasks so that the reference
     * to this handler can be broke after the handler is terminated. Must set to
     * {@code null} during handler termination.
     */
    private final AtomicReference<NioEndpointHandler> handlerRef =
        new AtomicReference<>(this);
    /** The SSL task execution state. */
    private final SSLTaskState sslTaskState = new SSLTaskState();
    /** Reads after the SSL execution. */
    private final ReadAfterExecution readAfterExecution =
        new ReadAfterExecution(handlerRef);
    /** Writes after the SSL execution. */
    private final WriteAfterExecution writeAfterExecution =
        new WriteAfterExecution(handlerRef);
    /** Handles the SSL execution exception. */
    private final HandleTaskExecutionException handleTaskExecutionException =
        new HandleTaskExecutionException(handlerRef);
    /** Calls the close handler. */
    private final RunCloseAsync runCloseAsync = new RunCloseAsync(handlerRef);
    /** Closes the input channel. */
    private final ChannelInputCloseOrRetryAfterRejection
        channelInputCloseOrRetryAfterRejection =
            new ChannelInputCloseOrRetryAfterRejection(
                CLOSE_INPUT_IN_BACKUP_EXECUTOR_NUM_RETRIES);

    public NioEndpointHandler(
        Logger logger,
        EndpointHandlerManager parent,
        EndpointConfig endpointConfig,
        boolean isCreator,
        String perfName,
        NetworkAddress remoteAddress,
        NioChannelExecutor channelExecutor,
        ExecutorService backupExecutor,
        ScheduledExecutorService backupSchedExecutor,
        DialogHandlerFactoryMap dialogHandlerFactories,
        SocketChannel socketChannel,
        DialogResourceManager concurrentDialogsManager,
        @Nullable EventTrackersManager trackersManager) {

        super(logger, parent, endpointConfig, isCreator,
              perfName, remoteAddress,
              dialogHandlerFactories, concurrentDialogsManager);
        this.channelExecutor = channelExecutor;
        this.backupExecutor = backupExecutor;
        this.backupSchedExecutor = backupSchedExecutor;
        this.endpointConfig = endpointConfig;
        this.socketChannel = socketChannel;
        this.channelInput = new NioChannelInput(trackersManager, this);
        this.channelOutput = new NioChannelOutput(trackersManager, this);
        this.protocolReader =
            new ProtocolReader(channelInput, getMaxInputProtocolMesgLen());
        this.protocolWriter =
            new ProtocolWriter(channelOutput, getMaxOutputProtocolMesgLen());
        this.rateLimitingLogger =
            new RateLimitingLogger<>(60 * 1000 /* logSamplePeriodMs */,
                                     20 /* maxObjects */,
                                     logger);
        this.perfTracker = new NioEndpointHandlerPerfTracker(remoteAddress);
        channelExecutor.getPerfTracker()
            .addEndpointHandlerPerfTracker(perfTracker);
        onExecutorReady();
        assert TestHookExecute.doHookIfSet(creationHook, this);
        logger.log(
            Level.FINEST,
            () ->
            String.format(
                "Created endpoint handler: "
                + "handler=%s, socketChannel=%s, executor=%s",
                this, socketChannel, channelExecutor.getId()));
    }

    /* AbstractDialogEndpointHandler methods */

    /**
     * Returns the executor service associated with this context.
     */
    @Override
    public ScheduledExecutorService getSchedExecService() {
        return channelExecutor;
    }

    @Override
    public ChannelDescription getChannelDescription() {
        return () -> {
            final DataChannel ch = dataChannel;
            if (ch != null) {
                if (ch instanceof SSLDataChannel) {
                    return ((SSLDataChannel) ch).toShortString();
                }
                return ch.toString();
            }
            if (socketChannel.isOpen()) {
                socketChannel.toString();
            }
            return String.format(
                "%s (closed)", getRemoteAddress().toString());
        };
    }

    /**
     * Returns the {@link ProtocolReader}.
     */
    @Override
    public ProtocolReader getProtocolReader() {
        return protocolReader;
    }

    /**
     * Returns the {@link ProtocolWriter}.
     */
    @Override
    public ProtocolWriter getProtocolWriter() {
        return protocolWriter;
    }

    /**
     * Asserts that the method is called inside the executor thread.
     */
    @Override
    public void assertInExecutorThread() {
        if (!channelExecutor.inExecutorThread()) {
            throw new IllegalStateException(
                    "The method is not executed in the thread of executor");
        }
    }

    /*
     * Channel handler callback methods.
     *
     * All methods should hande errors internally if errors can occur, cancels
     * the handler and rethrow the error if necessary.
     */

    @Override
    public void onConnected() {
        /* This method should not be called due to the pre-write handler. */
        final Throwable t = new IllegalStateException();
        cancel(t);
        rethrowUnhandledError(t);
    }

    @Override
    public void onRead() {
        final long startTime = perfTracker.getHandlerReadStartTimestamp();
        try {
            assert ExceptionTestHookExecute.doHookIfSet(readHook, this);
            if (sslTaskState.isRunning()) {
                /*
                 * Skipping reading if there is ssl task running since we would
                 * not make progress. This is sort of a protective measure. The
                 * handleRead would de-register read interest, so this check is
                 * not expected to take effect, but just in case de-register did
                 * not work properly.
                 *
                 * We also skipping closeAsync. This is because for an ssl
                 * engine to close gracefully, we need to call wrap() repeatedly
                 * until the state returns CLOSED. Therefore, if a ssl task is
                 * running, we will not make progress on closeAsnc as well. The
                 * closeAsync call would de-register for read if the task is
                 * running. Therefore, again, this check is a protective measure
                 * for closeAsync as well in case the de-register did not work
                 * properly.
                 */
                return;
            }
            if (isClosing) {
                closeHandler.closeAsync();
                return;
            }
            read();
            incompletionHandler.flushOnRead();
        } catch (Throwable t) {
            cancel(t);
            rethrowUnhandledError(t);
        } finally {
            perfTracker.markHandlerReadFinish(startTime);
        }
    }

    private void read() throws IOException {
        boolean eos = false;
        while (true) {
            ByteBuffer[] buffers = channelInput.flipToChannelRead();
            boolean again = true;
            while (true) {
                final long startTime =
                    perfTracker.getChannelReadStartTimestamp();
                final long n = getDataChannel().read(buffers);
                perfTracker.markChannelReadFinish(startTime, n);
                if (n > 0) {
                    continue;
                }
                if (n < 0) {
                    eos = true;
                    again = false;
                } else {
                    again = incompletionHandler.handleRead();

                    /*
                     * If the read stopped was because the buffers were full,
                     * then try again since a new buffer will be available for
                     * reading the next time around
                     */
                    if ((buffers.length > 0) &&
                        (buffers[buffers.length - 1].remaining() == 0)) {
                        again = true;
                    }

                    /*
                     * Fall through and continue in the outer loop when again
                     * is true so that we can handle WAIT_FOR_CHNL_READ and
                     * APP_READ together.
                     */
                }
                break;
            }
            channelInput.flipToProtocolRead();
            onChannelInputRead();
            if (!again) {
                break;
            }
        }

        if (eos) {
            markTerminating(new IOException("Got eof when reading"));
            terminate();
        }
    }

    @Override
    public void onWrite() {
        final long startTime = perfTracker.getHandlerWriteStartTimestamp();
        try {
            if (isClosing) {
                closeHandler.closeAsync();
                return;
            }
            flush();
            incompletionHandler.readOnWrite();
        } catch (Throwable t) {
            cancel(t);
            rethrowUnhandledError(t);
        } finally {
            perfTracker.markHandlerWriteFinish(startTime);
        }
    }

    @Override
    public void onSelected() {
        rwinterest.onSelected();
    }

    @Override
    public void onProcessed() {
        try {
            rwinterest.onProcessed();
        } catch (Throwable t) {
            cancel(t);
            rethrowUnhandledError(t);
        }
    }

    @Override
    public void cancel(Throwable t) {
        markTerminating(t);
        terminate();
    }

    /* Other methods */

    void handedOffToSync() {
        handedOffToSync = true;
    }

    @Override
    protected void setReadInterest(boolean interest) throws IOException {
        rwinterest.setReadInterest(interest);
    }

    /**
     * Flush the channel output.
     *
     * The caller should already have acquired a flush lock such that buffer
     * data is not flushed to the data channel in a interleaved manner.
     */
    @Override
    protected boolean flushInternal(boolean again)
        throws IOException {

        if (handedOffToSync) {
            return true;
        }

        long writtenTotal = 0;
        while (true) {
            final NioChannelOutput.Bufs bufs = channelOutput.getBufs();
            final long startTime =
                perfTracker.getChannelWriteStartTimestamp();
            final long written = getDataChannel().write(
                bufs.array(), bufs.offset(), bufs.length());
            writtenTotal += written;
            perfTracker.markChannelWriteFinish(startTime, written);
            final boolean dataLeft = channelOutput.hasRemaining();
            if ((written > 0) && dataLeft) {
                /* Can write and have more data, write again */
                continue;
            }
            /* Either cannot write anymore or nothing to write, flush */
            final boolean flushDone = getDataChannel().flush();
            if ((writtenTotal != 0) && flushDone && dataLeft) {
                writtenTotal = 0;
                continue;
            }

            /* Cannot make any progress */

            /*
             * If the upper layer needs us to do it again, we need to register
             * for write. Fall through to handle incompletion afterwards since
             * it might clear the cause of incompletion and we might be able to
             * iterate once more. The incompletion handling might clear the
             * write registration, but that is desired since if the
             * incompletion cause is waiting for more read data, registering
             * and wake up for write just wastes CPU cycles.
             */
            if (again) {
                /*
                 * If we are not in the executor thread, it is possible that
                 * this registration will kick off writes in the executor
                 * thread, which will not be able to grab the flush lock until
                 * we exit. This is fine, as the
                 * AbstractDialogEndpointHandler#flush will guarantee that the
                 * flush in the executor thread will happen.
                 */
                rwinterest.setWriteInterest(true);
            } else if (flushDone && (!dataLeft)) {
                /* Flush done, no data left and do not need to write again */
                rwinterest.setWriteInterest(false);
                /*
                 * We are done, free up the buffers that has been consumed. We
                 * do not to call free in the other cases, because the
                 * NioChannelOutput#getBufs() will also call free.
                 */
                bufs.free();
                return true;
            }
            /*
             * Write or flush incomplete, deal with incompletion. If we already
             * registered for write, we may yield and not try again so as to
             * not blocking the executor thread.
             */
            final boolean canRetry = !again;
            if (dataLeft) {
                if (incompletionHandler.handleWriteOrFlush(
                        DataChannel.AsyncIO.Type.WRITE) && canRetry) {
                    continue;
                }
            } else {
                if (incompletionHandler.handleWriteOrFlush(
                        DataChannel.AsyncIO.Type.FLUSH) && canRetry) {
                    continue;
                }
            }
            return false;
        }
    }

    @Override
    protected void cleanup() throws IOException {
        channelExecutor.getPerfTracker()
            .removeEndpointHandlerPerfTracker(perfTracker);
        try {
            channelExecutor.execute(new Runnable() {

                @Override
                public void run() {
                    getLogger().log(
                        Level.FINEST,
                        () ->
                        String.format(
                            "Endpoint handler %s "
                            + "closing channel input and output",
                            getStringID()));
                    channelInput.close();
                    channelOutput.close();
                }

                @Override
                public String toString() {
                    return String.format(
                        "cleanup task for endpoint handler %s",
                        getStringID());
                }
            });
        } catch (RejectedExecutionException e) {
            channelOutput.close();
            scheduleChannelInputClose();
        }
        if (!handedOffToSync) {
            closeHandler.closeAsync();
        }

        handlerRef.set(null);
    }

    private void scheduleChannelInputClose() {
        getLogger().log(
            Level.FINEST,
            () ->
            String.format(
                "Endpoint handler %s "
                + "scheduling channel input close in backup executor",
                getStringID()));
        scheduleOrLogReject(
            () ->
            backupExecutor.submit(
                channelInputCloseOrRetryAfterRejection),
            ChannelInputCloseOrRetryAfterRejection.class.getSimpleName(),
            () ->
            String.format(
                "Scheduling of input clean-up for channel %s "
                + "is rejected",
                getStringID()));
    }

    private void scheduleOrLogReject(Runnable task,
                                     String key,
                                     Supplier<String> logMesgSupplier) {
        try {
            task.run();
        } catch (RejectedExecutionException e) {
            rateLimitingLogger.log(
                key, Level.INFO, logMesgSupplier);
        }
    }

    /**
     * A handler that asynchronously close the channel.
     */
    private class CloseHandler {

        private void closeAsync() {
            /*
             * Do not try the multi-step graceful asynchronous close if not
             * worth it. This could help allieviate a resource problem. For
             * example when we are under OOM, we do not want to do more work to
             * get rid of the channels.
             */
            if (!shouldCloseChannelGracefully()) {
                closeForcefully();
            }
            try {
                /*
                 * Reset for read interest since this callback may be invoked
                 * after an SSL task is being scheduled. In normal cases, when
                 * we are not closing during handshake, we do not need to do
                 * this, but it would not hurt.
                 */
                rwinterest.setReadInterest(true);
                isClosing = true;
                @Nullable DataChannel ch = dataChannel;
                if (ch == null) {
                    closeForcefully();
                    return;
                }
                while (true) {
                    final boolean finished = ch.closeAsync();
                    if (!finished) {
                        if (incompletionHandler.handleClose()) {
                            continue;
                        }
                    } else {
                        dataChannel = null;
                    }
                    return;
                }
            } catch (Throwable t) {
                if (getLogger().isLoggable(Level.FINE)) {
                    getLogger().log(Level.FINE,
                            "Error close channel asynchronously, " +
                            "handler={0}: {1}",
                            new Object[] {
                                NioEndpointHandler.this,
                                CommonLoggerUtils.getStackTrace(t) });
                }
                closeForcefully();
            }
        }

        private void closeForcefully() {
            @Nullable DataChannel ch = dataChannel;
            if (ch == null) {
                /*
                 * If the data channel has not been assigned yet, forcefully
                 * close the socketChannel directly.
                 */
                try {
                    socketChannel.close();
                } catch (Throwable t) {
                    /* do nothing. */
                }
            } else {
                try {
                    ch.closeForcefully();
                    channelExecutor.wakeup();
                } catch (Throwable t) {
                    /* do nothing */
                }
            }
            dataChannel = null;
        }
    }

    /**
     * Handling incompletion.
     *
     * The handler deals with read, write and close incompletions:
     * - Register write when necessary and deregister when onWrite called and
     *   no longer need to wait for write.
     * - Enable read inside onWrite when read is incompleted due to channel
     *   busy and disable when no longer needed
     * - Enable flush inside onRead when write/flush is incompleted due to need
     *   more channel read and disable when no longer needed
     * - Register the closeHandler if closing
     */
    private class IncompletionHandler {

        /*
         * Whether should call read() when onWrite(). Set when read is
         * incompleted due to channel write busy. Cleared after called.
         * Accessed inside the executor thread.
         */
        private boolean needReadOnWrite = false;
        /*
         * Whether should flush while calling onRead. Set when write
         * incompletion is cause by needing channel read. Cleared after called.
         * Accessed inside the executor thread.
         */
        private boolean needFlushOnRead = false;
        private final Runnable needFlushOnReadSetter =
            () -> { needFlushOnRead = true; };

        private final void handleAfterExecutionException(Throwable e) {
            final String prefix =
                "Got Exception after SSL task execution";
            if ((e instanceof IOException)
                || (e instanceof RejectedExecutionException)) {
                getLogger().fine(prefix + ": " + e);
            } else {
                rateLimitingLogger.log(
                    prefix, Level.INFO, () -> prefix + ": " + e);
            }
        }

        /**
         * Returns {@code true} if retry immediately.
         */
        private boolean handleRead() throws IOException {
            assertInExecutorThread();
            final DataChannel.AsyncIO.ContinueAction action =
                getDataChannel().
                getAsyncIOContinueAction(DataChannel.AsyncIO.Type.READ);
            switch(action) {
            case RETRY_NOW:
                return true;
            case WAIT_FOR_CHNL_READ:
                return false;
            case WAIT_FOR_CHNL_WRITE_THEN_FLUSH:
                needReadOnWrite = true;
                rwinterest.setWriteInterest(true);
                return false;
            case APP_READ:
                return true;
            case WAIT_FOR_TASKS_EXECUTION:
                sslTaskState.schedule().thenRun(readAfterExecution)
                    .exceptionally(handleTaskExecutionException);
                return false;
            default:
                throw new IllegalStateException(
                              "Unknown continue action: " + action);
            }
        }

        /**
         * Returns {@code true} if retry immediately.
         */
        boolean handleWriteOrFlush(DataChannel.AsyncIO.Type type)
            throws IOException {
            final DataChannel.AsyncIO.ContinueAction action =
                getDataChannel().getAsyncIOContinueAction(type);
            switch(action) {
            case RETRY_NOW:
                return true;
            case WAIT_FOR_CHNL_READ:
                runInExecutorThread(needFlushOnReadSetter);
                return false;
            case WAIT_FOR_CHNL_WRITE_THEN_FLUSH:
                rwinterest.setWriteInterest(true);
                return false;
            case WAIT_FOR_TASKS_EXECUTION:
                sslTaskState.schedule().thenRun(writeAfterExecution)
                    .exceptionally(handleTaskExecutionException);
                return false;
            case APP_READ:
                /*
                 * This means the channel executor does not invoke our channel
                 * read frequently enough. All we can do is wait.
                 */
                runInExecutorThread(needFlushOnReadSetter);
                return false;
            default:
                throw new IllegalStateException(
                              "Unexpected close incompletion " +
                              "due to " + action);
            }
        }

        /**
         * Returns {@code true} if retry immediately.
         */
        private boolean handleClose() throws IOException {
            final DataChannel.AsyncIO.ContinueAction action =
                getDataChannel().getAsyncIOContinueAction(
                    DataChannel.AsyncIO.Type.CLOSE);
            switch(action) {
            case RETRY_NOW:
                return true;
            case WAIT_FOR_CHNL_READ:
                rwinterest.setWriteInterest(false);
                return false;
            case WAIT_FOR_CHNL_WRITE_THEN_FLUSH:
                rwinterest.setWriteInterest(true);
                return false;
            case WAIT_FOR_TASKS_EXECUTION:
                sslTaskState.schedule().thenRun(runCloseAsync)
                    .exceptionally(handleTaskExecutionException);
                return false;
            case APP_READ:
            default:
                throw new IllegalStateException(
                              "Unexpected close incompletion " +
                              "due to " + action);
            }
        }

        private void readOnWrite() throws IOException {
            assertInExecutorThread();
            if (needReadOnWrite) {
                needReadOnWrite = false;
                read();
            }
        }

        private void flushOnRead() throws IOException {
            assertInExecutorThread();
            if (needFlushOnRead) {
                needFlushOnRead = false;
                flush();
            }
        }
    }

    private void runInExecutorThread(Runnable r) {
        if (channelExecutor.inExecutorThread()) {
            r.run();
        } else {
            channelExecutor.submit(r);
        }
    }

    /**
     * Sets the read/write notification.
     *
     * This class is intended to optimize for channel read/write interest
     * registration. In many cases, the methods from the endpoint handler class
     * are called inside the single executor thread. Hence we want to avoid
     * cross synchronization and memory barriers.
     *
     * For example, when a channel has reached the dialog resource capacity, we
     * will set to cancel the read interest. It would be nice to optimize so
     * that we do not actually call the channel methods when the read interest
     * is already cancelled. It may be common case that many channels are in
     * such state, and thus such optimization might save a lot of cost of
     * crossing memory barriers.
     *
     * The same could apply for when many channels are busy for write. There is
     * another optimization for the following scenario: a channel is busy for
     * writing, therefore set for write notification; when write is ready, the
     * AbstractDialogEndpointHandler starts to write messages to the channel;
     * it writes in batches and the first several writes find the channel not
     * busy for write and thus set for no write notification; The last set for
     * write notification again. The above senario does two notification set
     * but in fact none is needed.
     */
    private class ReadWriteInterest {

        /* Currently after onSelected() before onProcessed() */
        private final ThreadLocal<Boolean> processing =
            ThreadLocal.withInitial(() -> false);

        /* The below fields are accessed in the executor thread */

        /* Whether the channel should be registered for read interest */
        private boolean wantReadInterest = true;
        /* Whether the channel is currently registered for read interest */
        private boolean readInterest = false;
        /*
         * Whether the channel should be registered for write interest.
         *
         * This must set to false initially. Otherwise, the channel executor
         * will busy loop with the write procedure, and on lower capcity
         * machines, get the tests stuck (e.g., takes too long to do ssl
         * handshake)
         */
        private boolean wantWriteInterest = false;
        /* Whether the channel is currently registered for write interest */
        private boolean writeInterest = false;

        private void onSelected() {
            processing.set(true);
        }

        private void onProcessed() throws IOException {
            processing.set(false);
            /*
             * Explicitly disable read interest if the SSL task is running.
             * Otherwise, we will busy-loop getting notified with read and
             * submitting tasks causing OOME ([KVSTORE-1856], [KVSTORE-2260]).
             *
             * TODO: it is better that if we modularize this ReadWriteInterest
             * object better so that semantics is more clear that the interest
             * is set by the AbstractDialogEndpointHandler or by this
             * NioEndpointHandler [KVSTORE-2265].
             */
            if (sslTaskState.isRunning()) {
                wantReadInterest = false;
            }
            updateInterest();
        }

        private void updateInterest() throws IOException {
            assertInExecutorThread();
            if (wantReadInterest != readInterest) {
                channelExecutor.setReadInterest(
                    socketChannel, NioEndpointHandler.this, wantReadInterest);
                readInterest = wantReadInterest;
            }
            if (wantWriteInterest != writeInterest) {
                channelExecutor.setWriteInterest(
                    socketChannel, NioEndpointHandler.this, wantWriteInterest);
                writeInterest = wantWriteInterest;
            }
        }

        private void setReadInterest(boolean interest) throws IOException {
            if (channelExecutor.inExecutorThread()) {
                wantReadInterest = interest;
                if (processing.get()) {
                    /*
                     * We are processing, will actually do the set inside
                     * onProcessed().
                     */
                    return;
                }

                updateInterest();
                return;
            }

            /*
             * If we want to enable read register from another thread, just
             * schedule a task to do so. This should be rare though since we
             * usually change read register setting during read and thus inside
             * the executor thread.
             */
            channelExecutor.submit(() -> {
                wantReadInterest = interest;
                try {
                    updateInterest();
                } catch (IOException e) {
                    markTerminating(e);
                    terminate();
                }
            });
        }

        private void setWriteInterest(boolean interest) throws IOException {
            if (channelExecutor.inExecutorThread()) {
                wantWriteInterest = interest;
                if (processing.get()) {
                    return;
                }

                updateInterest();
            }
            /*
             * If we want to enable write register from another thread, just
             * schedule a task to do so. The more common case for write is to
             * not register, therefore, we can do a bit optimization here if we
             * skip in common case. It is OK to skip since the channel will
             * register interest for the handler which finds unable to write
             * and sets to no interest again inside the executor thread.
             */
            if (interest) {
                channelExecutor.submit(() -> {
                    wantWriteInterest = interest;
                    try {
                        updateInterest();
                    } catch (IOException e) {
                        markTerminating(e);
                        terminate();
                    }
                });
            }
        }
    }

    /**
     * Wraps the socket channel and saves the data channel if not already did
     * and returns the wrapped data channel.
     */
    private DataChannel getDataChannel() throws IOException {
        @Nullable DataChannel ch = dataChannel;
        if (ch != null) {
            return ch;
        }
        if (isClosing) {
            throw new IOException(
                "Endpoint handler already set closing, " +
                "new data channel will not be created");
        }
        ch = NioUtil.getDataChannel(
            socketChannel, endpointConfig,
            isCreator(), getRemoteAddress(), getLogger());
        dataChannel = ch;
        return ch;
    }

    /**
     * Represents the SSL task execution state.
     */
    private class SSLTaskState {

        private boolean isRunning = false;
        private CompletableFuture<Void> future;


        /**
         * Schedules the SSL task for running and returns the completable
         * future. If the SSL task is already running, then return the existing
         * future, otherwise schedules the task.
         */
        private synchronized CompletableFuture<Void> schedule() throws IOException {
            getLogger().fine(
                () -> String.format("%s schedule ssl task, %s", getStringID(),
                    CommonLoggerUtils.getStackTrace(Thread.currentThread())));
            if (isRunning) {
                assert future != null;
                return future;
            }
            assert future == null;
            isRunning = true;
            /*
             * Execute markDone asynchronously to avoid recursion, otherwise,
             * markDone might be executed before future is assigned.
             */
            future = getDataChannel().executeTasks(backupExecutor)
                .whenCompleteAsync((v, t) -> markDone(t), backupExecutor);
            return future;
        }

        /**
         * Marks that a running SSL task is done.
         */
        private synchronized void markDone(Throwable t) {
            assert isRunning == true;
            assert future != null;
            isRunning = false;
            future = null;
            if (t != null) {
                getLogger().fine(() -> String.format(
                    "%s ssl task done with exception %s", getStringID(), t));
            } else {
                getLogger().fine(
                    () -> String.format("%s ssl task done", getStringID()));
            }
        }

        /**
         * Returns {@code true} if there is an SSL task running.
         */
        private synchronized boolean isRunning() {
            return isRunning;
        }
    }

    /**
     * A handler task that dereference the handler after termination.
     */
    private static abstract class DereferencedAfterTerminationTask
        implements Runnable
    {

        protected final AtomicReference<NioEndpointHandler> handler;

        private DereferencedAfterTerminationTask(
                AtomicReference<NioEndpointHandler> handler) {
            this.handler = handler;
        }

        @Override
        public void run() {
            final NioEndpointHandler h = handler.get();
            if (h == null) {
                return;
            }
            runInternal(h);
        }

        protected abstract void runInternal(NioEndpointHandler h);
    }

    private static class OnReadTask extends DereferencedAfterTerminationTask {
        private OnReadTask(AtomicReference<NioEndpointHandler> handler) {
            super(handler);
        }

        @Override
        protected void runInternal(NioEndpointHandler h) {
            h.onRead();
        }
    }

    private static class OnWriteTask extends DereferencedAfterTerminationTask {
        private OnWriteTask(AtomicReference<NioEndpointHandler> handler) {
            super(handler);
        }

        @Override
        protected void runInternal(NioEndpointHandler h) {
            h.onWrite();
        }
    }

    private static class ReadAfterExecution
        extends DereferencedAfterTerminationTask
    {

        private ReadAfterExecution(AtomicReference<NioEndpointHandler> handler)
        {
            super(handler);
        }

        @Override
        protected void runInternal(NioEndpointHandler h) {
            try {
                /* Sets read interest since we unset before execution. */
                h.rwinterest.setReadInterest(true);
                h.channelExecutor.submit(new OnReadTask(handler));
            } catch (Throwable e) {
                h.incompletionHandler.handleAfterExecutionException(e);
            }
        }
    }

    private static class WriteAfterExecution
        extends DereferencedAfterTerminationTask
    {
        private WriteAfterExecution(AtomicReference<NioEndpointHandler> handler)
        {
            super(handler);
        }

        @Override
        protected void runInternal(NioEndpointHandler h) {
            try {
                /* Sets read interest since we unset before execution. */
                h.rwinterest.setReadInterest(true);
                h.channelExecutor.submit(new OnWriteTask(handler));
            } catch (Throwable e) {
                h.incompletionHandler.handleAfterExecutionException(e);
            }
        }
    }

    private static class RunCloseAsync
            extends DereferencedAfterTerminationTask {

        private RunCloseAsync(AtomicReference<NioEndpointHandler> handler) {
            super(handler);
        }

        @Override
        protected void runInternal(NioEndpointHandler h) {
            h.closeHandler.closeAsync();
        }
    }

    private static class HandleTaskExecutionException
            implements Function<Throwable, Void>
    {
        private final AtomicReference<NioEndpointHandler> handler;

        private HandleTaskExecutionException(
                AtomicReference<NioEndpointHandler> handler)
        {
            this.handler = handler;
        }

        @Override
        public Void apply(Throwable throwable) {
            final NioEndpointHandler h = handler.get();
            if (h == null) {
                return null;
            }
            /* First marks the termination. */
            h.markTerminating(throwable);
            /*
             * No need for a graceful close. Close the channel forcefully to
             * avoid unnecessary extra handshakes.
             */
            h.closeHandler.closeForcefully();
            /* Terminates. */
            h.terminate();
            return null;
        }
    }

    /**
     * Closes the channel input or retry.
     *
     * We need to close the channel input. The challenge is methods in channel
     * input close are not thread-safe since we assume these methods are
     * executed inside single-threaded nio channel executor. Now that the
     * executor is shutting down and rejected our clean up task, we do two
     * things:
     *
     * - Use a static executor service provided to do the rest of the work.
     *
     * - Do not clean up until the channel executor is terminated. This is
     * because the channel executor could be still running tasks during soft
     * shutdown. Checking the executor can achieve two purposes: (1) Running
     * close methods here will not interfere with other channel input methods
     * (2) NioChannelExecutor.terminate will write its violatile state field
     * which synchronizes-with reading the state. Therefore, methods running
     * here will see the impact from tasks running in the channel executor.
     */
    private class ChannelInputCloseOrRetryAfterRejection implements Runnable
    {
        private final int nretries;

        private ChannelInputCloseOrRetryAfterRejection(
                int nretries)
        {
            this.nretries = nretries;
        }

        @Override
        public void run() {
            if (channelExecutor.isTerminated()) {
                getLogger().log(
                    Level.FINEST,
                    () -> String.format(
                        "Endpoint handler %s closing channel input",
                        getStringID()));
                channelInput.close();
                return;
            }
            final int remaining = nretries - 1;
            if (remaining <= 0) {
                rateLimitingLogger.log(
                    ChannelInputCloseOrRetryAfterRejection.class.getName(),
                    Level.WARNING,
                    () -> String.format("Failed to clean up channel input. "
                        + "This can only happen if channel executors "
                        + "did not terminate correctly in time. "
                        + "This situation is not expected. "
                        + "The impact is that the input buffers "
                        + "may not be able to return to the IOBufferPool"));
                return;
            }
            scheduleOrLogReject(
                () -> backupSchedExecutor.schedule(
                    new ChannelInputCloseOrRetryAfterRejection(remaining),
                    1, TimeUnit.SECONDS),
                ChannelInputCloseOrRetryAfterRejection.class.getSimpleName()
                    + " reject",
                () -> String
                    .format("Scheduling of input clean-up for channel %s "
                        + "is rejected", getStringID()));
        }
    }
}
