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

package oracle.kv.impl.async.dialog;

import java.util.Formatter;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.RequestTimeoutException;
import oracle.kv.impl.async.BytesInput;
import oracle.kv.impl.async.DialogContext;
import oracle.kv.impl.async.DialogHandler;
import oracle.kv.impl.async.IOBufSliceList;
import oracle.kv.impl.async.MessageInput;
import oracle.kv.impl.async.MessageOutput;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.dialog.exception.ProtocolViolationException;
import oracle.kv.impl.async.exception.ConnectionEndpointShutdownException;
import oracle.kv.impl.async.exception.ConnectionException;
import oracle.kv.impl.async.exception.ConnectionIOException;
import oracle.kv.impl.async.exception.ConnectionUnknownException;
import oracle.kv.impl.async.exception.ContextWriteExceedsLimitException;
import oracle.kv.impl.async.exception.ContextWriteFinException;
import oracle.kv.impl.async.exception.DialogCancelledException;
import oracle.kv.impl.async.exception.DialogException;
import oracle.kv.impl.async.exception.DialogNoSuchTypeException;
import oracle.kv.impl.async.exception.DialogUnknownException;
import oracle.kv.impl.async.perf.DialogInfoPerfTracker;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.FormatUtils;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Implements {@link DialogContext}.
 *
 * Notes for the implementation:
 * - DialogHandler methods must be called inside the executor thread.
 * - onStart method must be the first to be called. The write method should
 *   throw exception and read method return null before onStart is called.
 * - After onAbort is called, onCanRead/onCanWrite should not be called; the
 *   write should fail; read should return null. Write may fail before onAbort
 *   is called.
 * - After DialogAbort is sent, we should not send DialogStart or DialogFrame.
 * - Endpoint handler methods may acquire locks, therefore, to prevent
 *   deadlock, these methods should not appear inside a synchronization block.
 */
public class DialogContextImpl implements DialogContext {

    public static final String TIMEOUT_INFO_TEMPLATE =
        "Dialog timed out %s. Dialog context before abort: %s";

    /* A constant marking the doneTimeNs is not assigned yet */
    private static final long UNASSIGNED_DONE_TIME = -1L;

    enum State {
        /* Inited, need to actually start the dialog */
        INITED_NEED_DIALOGSTART,

        /* Started */
        STARTED,

        /* Received last message, but not all messages are polled yet */
        READ_FIN0,

        /* Received last message, all messages are polled */
        READ_FIN,

        /* Wrote last message, but not yet sent out */
        WRITE_FIN0,

        /* Sent last message */
        WRITE_FIN,

        /* READ_FIN0 and WRITE_FIN0 */
        READ_FIN0_WRITE_FIN0,

        /* READ_FIN and WRITE_FIN0 */
        READ_FIN_WRITE_FIN0,

        /* READ_FIN0 and WRITE_FIN */
        READ_FIN0_WRITE_FIN,

        /* Finished normally */
        FIN,

        /* Aborted */
        ABORTED,
    }

    private final DialogEndpointHandler endpointHandler;
    private final Logger logger;
    private final ProtocolWriter protocolWriter;
    private final DialogHandler dialogHandler;
    private final boolean onCreatingSide;

    private final long contextId;
    private volatile long dialogId;
    private final int typeno;
    private final long timeoutMillis;

    /*
     * Dialog state. State transitions are all in synchroned block of the
     * context. To prevent deadlock, we guarantee not to acquire locks inside
     * the state synchronization block.
     */
    private volatile State state;
    /* The flag indicating we have a pending outgoing message */
    private static final int WS_PENDINGMESG = 0x01;
    /* The flag indiciating we should call onCanWrite when pending message is sent. */
    private static final int WS_CALLBACK = 0x02;
    /* The flag indicating we have an outstanding message. */
    private final AtomicInteger writeState = new AtomicInteger(0);
    /*
     * We have written the last message. The flag will be flipped to true when
     * we write the last message, it will never be false again.
     */
    private volatile boolean lastMesgWritten = false;
    /*
     * The frames to send. All access must be inside the context
     * synchronization block.
     */
    private Queue<IOBufSliceList> outputFrames = null;
    /* An object to sync on to prevent writing DialogFrame after DialogAbort */
    private final Object frameAndAbortSync = new Object();
    /*
     * The complete messages received. All accesses are inside the synchronized
     * block of this object.
     */
    private Queue<MessageInput> inputMessages = new LinkedList<MessageInput>();
    /*
     * The still-to-complete receiving message. All accesses must be inside
     * context synchronization block.
     */
    private MessageInput messageReceiving = new MessageInput();
    /*
     * The size of the current receiving message. All accesses are in one
     * single thread of the executor.
     */
    private int sizeOfMessageReceiving = 0;
    /*
     * We have received the last message. The flag will be flipped to true when
     * we receive the last frame of the last message, it will never be false
     * again. All accesses are in one single thread of the executor.
     */
    private boolean lastMesgReceived = false;

    /* Timeout task */
    private final DialogTimeoutTask timeoutTask = new DialogTimeoutTask();

    /* Information of abort, modified inside context synchronization block. */
    private volatile AbortInfo abortInfo = null;
    private final AtomicBoolean dialogAbortWritten = new AtomicBoolean(false);

    /*
     * Methods to call handler callbacks. We must ensure handler callbacks are
     * executed sequentially.
     */
    private final OnStartTask onStartTask = new OnStartTask();
    private final OnCanReadTask onCanReadTask = new OnCanReadTask();
    private final OnCanWriteTask onCanWriteTask = new OnCanWriteTask();
    private final OnAbortTask onAbortTask = new OnAbortTask();

    /* To ensure onStart is called only once */
    private final AtomicBoolean onStartEntered = new AtomicBoolean(false);

    private final Object onStartLock = new Object();
    /*
     * To ensure other handler callbacks are called after onStart. Modified
     * inside onStartLock synchronized block. Volatile for read.
     */
    private volatile boolean onStartExited = false;
    /*
     * Whether to invoke a callback after onStart, accessed when synchronized
     * on onStartLock.
     */
    private boolean callOnCanReadAfterStart = false;
    private boolean callOnCanWriteAfterStart = false;
    private boolean callOnAbortAfterStart = false;
    /* To ensure other handler callbacks are called after onStart */
    private final AtomicBoolean onAbortCalled = new AtomicBoolean(false);

    /**
     * A thread local flag to indicate a call onAbort is in progress.
     *
     * This is to make sure we do not have a onAbort - onStart - onAbort
     * recursion.
     */
    private static final ThreadLocal<Boolean> callingOnAbort =
        ThreadLocal.withInitial(() -> false);

    /*
     * Performance collection. Init time and done time stats are put in here as
     * primitive data type as they are always collected. A dialog perf object
     * is created if this dialog is sampled for event latency stats or detailed
     * event logging. See explaination in EndpointPerfTracker.
     */
    /* Wall clock init time in ms */
    private final long initTimeMillis = System.currentTimeMillis();
    /* Init time in ns for computing elapsed time */
    private final long initTimeNanos = System.nanoTime();
    /*
     * Done time in ns for computing elapsed time, also serve as the flag
     * indicating if the dialog is done
     */
    private final AtomicLong doneTimeNs = new AtomicLong(UNASSIGNED_DONE_TIME);
    /* Dialog info perf tracker, non-null if sampled */
    private final AtomicReference<DialogInfoPerfTracker> infoPerfTracker =
        new AtomicReference<>(null);

    /**
     * Indicates that this dialog has consumed a resource or a permit. Such
     * resource or permit include the local or remote resource specific to this
     * channel (i.e., AbstractDialogEndpointHandler.localMaxDlgs and
     * remoteMaxDlgs) or the global dialog permit (i.e., the permits managed by
     * DialogResourceManager).
     *
     * A dialog context may be created but not consuming a resource and/or a
     * permit before when it is ready to execute its request/response exchange.
     * For example, the associated endpoint handler may be still under
     * handshake, or there is a failure or shut down to the endpoint handler.
     *
     * This field is initialized to false and once will always be true once
     * set. When the dialog is done, the associated permits should be freed if
     * this flag is set to true.
     */
    private volatile boolean consumedResourcePermit = false;

    /* Test hooks */
    public volatile static TestHook<DialogContextImpl> createHook;

    DialogContextImpl(DialogEndpointHandler endpointHandler,
                      DialogHandler dialogHandler,
                      long contextId,
                      long dialogId,
                      int typeno,
                      long timeoutMillis) {
        this.endpointHandler = endpointHandler;
        this.logger = endpointHandler.getLogger();
        this.protocolWriter = endpointHandler.getProtocolWriter();
        this.dialogHandler = dialogHandler;
        this.onCreatingSide = (dialogId == 0);
        this.contextId = contextId;
        this.dialogId = dialogId;
        this.typeno = typeno;
        this.timeoutMillis = timeoutMillis;
        this.state = (dialogId == 0) ?
            State.INITED_NEED_DIALOGSTART : State.STARTED;

        assert TestHookExecute.doHookIfSet(createHook, this);

        logger.log(Level.FINEST,
                   () -> String.format("Created dialog context: %s", this));
    }

    @Override
    public boolean write(MessageOutput mesg, boolean finished) {
        final int mesgSize = mesg.size();
        final int maxTotLen = endpointHandler.getMaxOutputTotLen();
        if (mesgSize > maxTotLen) {
            throw new ContextWriteExceedsLimitException(mesgSize, maxTotLen);
        }

        final Queue<IOBufSliceList> frames = mesg.pollFrames(
            endpointHandler.getMaxOutputProtocolMesgLen(), this);

        State prev;
        synchronized(this) {
            /* Quick fail for the exception cases. */
            switch (state) {
            case WRITE_FIN0:
            case WRITE_FIN:
            case READ_FIN0_WRITE_FIN0:
            case READ_FIN_WRITE_FIN0:
            case READ_FIN0_WRITE_FIN:
            case FIN:
                throw new ContextWriteFinException();
            case ABORTED:
                return false;
            default:
                break;
            }
            if (frames == null) {
                /*
                 * The frame may be null if we had already discarded the mesg.
                 * In this case, the dialog must have been terminated. If we are
                 * here, then something is wrong.
                 */
                throw new IllegalStateException(
                    String.format(
                        "Null frames when the dialog is not terminated; "
                            + "State: %s",
                        state));
            }
            /* Return false if there is a pending outgoing message. */
            if (!setWriteState()) {
                return false;
            }

            prev = state;
            switch (state) {
            case INITED_NEED_DIALOGSTART:
            case STARTED:
                if (finished) {
                    state = State.WRITE_FIN0;
                } else {
                    state = State.STARTED;
                }
                break;
            case READ_FIN0:
                if (finished) {
                    state = State.READ_FIN0_WRITE_FIN0;
                }
                break;
            case READ_FIN:
                if (finished) {
                    state = State.READ_FIN_WRITE_FIN0;
                }
                break;
            default:
                throw new AssertionError();
            }

            if (finished) {
                lastMesgWritten = true;
            }
            outputFrames = frames;
        }

        runPerfCallback((tracker) -> tracker.onWriteMessage(mesgSize));

        /*
         * Only one thread will arrive here since all concurrent writes will
         * return after we cas on within the setWriteState method.
         */

        /*
         * Use local variable frames instead of outputFrames since it might be
         * cleaned up to null.
         */
        if ((prev == State.INITED_NEED_DIALOGSTART) &&
            (!frames.isEmpty())) {
            assert dialogId == 0;
            IOBufSliceList frame = frames.poll();
            boolean cont = !frames.isEmpty();
            boolean last = finished && !cont;
            dialogId = endpointHandler.writeDialogStartForContext(
                    last, cont, typeno, timeoutMillis, frame, this);

            runPerfCallback((tracker) -> tracker.onSendFrame());

            logger.log(Level.FINEST, () -> String.format(
                "Dialog Id assigned to the context: %s", this));
        }

        /*
         * If we are aborted here, we need to write a DialogAbort since when
         * onLocalAbort is called the dialogId may not be assigned yet.
         */
        if (isAborted()) {
            writeDialogAbort();
            return false;
        }

        final boolean framesDone = onWriteDialogFrame();

        if (!framesDone) {
            /* Notify the endpoint handler that we have frames to write. */
            endpointHandler.onContextNewWrite(this);
        } else {
            endpointHandler.flushOrTerminate();
        }

        if (isFin()) {
            cleanupContext();
        }

        return true;
    }

    private void runPerfCallback(Consumer<DialogInfoPerfTracker> consumer) {
        final DialogInfoPerfTracker tracker = infoPerfTracker.get();
        if (tracker == null) {
            return;
        }
        consumer.accept(tracker);
    }

    @Override
    public MessageInput read() {
        MessageInput input;
        synchronized(this) {
            if (inputMessages == null) {
                return null;
            }
            input = inputMessages.poll();
            if (input == null) {
                return null;
            }
            input.markPolled();
            if (inputMessages.isEmpty()) {
                switch(state) {
                case INITED_NEED_DIALOGSTART:
                case STARTED:
                    break;
                case READ_FIN0:
                    state = State.READ_FIN;
                    break;
                case READ_FIN:
                case WRITE_FIN0:
                case WRITE_FIN:
                    break;
                case READ_FIN0_WRITE_FIN0:
                    state = State.READ_FIN_WRITE_FIN0;
                    break;
                case READ_FIN_WRITE_FIN0:
                    break;
                case READ_FIN0_WRITE_FIN:
                    state = State.FIN;
                    break;
                case FIN:
                    throw new IllegalStateException();
                case ABORTED:
                    return null;
                default:
                    throw new AssertionError();
                }
            }
        }
        runPerfCallback((tracker) -> tracker.onReadMessage());
        if (isFin()) {
            cleanupContext();
        }

        return input;
    }

    @Override
    public void cancel(Throwable cause) {
        synchronized(this) {
            if (isAborted() || isFin()) {
                return;
            }
            final DialogException actualCause =
                (cause instanceof DialogException) ?
                (DialogException) cause :
                new DialogCancelledException(
                    hasSideEffect(),
                    false /* not from remote */,
                    cause);
            abortInfo = new AbortInfo(
                actualCause,
                ProtocolMesg.DialogAbort.Cause.UNKNOWN_REASON);
            state = State.ABORTED;
        }
        writeDialogAbort();
        cleanupContext();
    }

    @Override
    public long getDialogId() {
        return dialogId;
    }

    @Override
    public long getConnectionId() {
        return endpointHandler.getConnID();
    }

    @Override
    public NetworkAddress getRemoteAddress() {
        return endpointHandler.getRemoteAddress();
    }

    @Override
    public @NonNull ScheduledExecutorService getSchedExecService() {
        return endpointHandler.getSchedExecService();
    }

    public boolean isDone() {
        return doneTimeNs.get() != UNASSIGNED_DONE_TIME;
    }

    public boolean isFin() {
        return state == State.FIN;
    }

    public boolean isAborted() {
        return state == State.ABORTED;
    }

    public DialogHandler getDialogHandler() {
        return dialogHandler;
    }

    public void sample(boolean sampleForRecord) {
        if (!infoPerfTracker.compareAndSet(
            null,
            new DialogInfoPerfTracker(
                initTimeMillis, initTimeNanos, sampleForRecord))) {

            throw new IllegalStateException(
                "Sampling dialog for perf " +
                "is only expected to be called once");
        }
    }

    /**
     * Returns an optional dialog info perf tracker.
     *
     */
    public Optional<DialogInfoPerfTracker> getInfoPerfTracker() {
        final DialogInfoPerfTracker tracker = infoPerfTracker.get();
        if (tracker == null) {
            return Optional.empty();
        }
        /*
         * Sets the context ID before return the perf. The context ID is
         * assigned later when the dialog sends the first frame and therefore
         * cannot be assigned when we create the perf.
         */
        tracker.setId(getStringID());
        return Optional.of(tracker);
    }

    /**
     * Starts the timeout for the dialog.
     */
    void startTimeout() {
        timeoutTask.schedule();
    }

    /**
     * Called by the endpoint handler when reading a DialogFrame.
     *
     * This method will only be called inside the executor thread.
     */
    void onReadDialogFrame(boolean finish,
                           boolean cont,
                           BytesInput frame) {
        endpointHandler.assertInExecutorThread();

        frame.trackDialog(this);

        final int readableBytes = frame.remaining();
        final int size = sizeOfMessageReceiving + readableBytes;
        final int maxTotLen = endpointHandler.getMaxInputTotLen();
        if (size > maxTotLen) {
            throw new ProtocolViolationException(
                false /* local error */,
                true /* handshake done */,
                endpointHandler.getChannelDescription(),
                ProtocolViolationException.
                ERROR_MAX_TOTLEN_EXCEEDED +
                String.format(
                    "Received DialogFrame, limit=%d, mesgTotLen=%d, " +
                    "context=%s",
                    maxTotLen, size, this));
        }

        synchronized(this) {
            /*
             * Update the accounting first so that the sizeOfMessageReceiving
             * is always updated even if we are aborted and return early.
             */
            if (!cont) {
                sizeOfMessageReceiving = 0;
            } else {
                sizeOfMessageReceiving += readableBytes;
            }

            switch(state) {
            case INITED_NEED_DIALOGSTART:
                /*
                 * The endpoint handler should already see this as invalid
                 * since we don't have a dialogId yet.
                 */
                throw new AssertionError();
            case STARTED:
                if (finish) {
                    state = State.READ_FIN0;
                }
                break;
            case READ_FIN0:
            case READ_FIN:
                throw new ProtocolViolationException(
                    false /* local error */,
                    true /* handshaek done */,
                    endpointHandler.getChannelDescription(),
                    ProtocolViolationException.
                    ERROR_INVALID_DIALOG_STATE +
                    String.format(
                        "Received DialogFrame, context=%s", this));
            case WRITE_FIN0:
                if (finish) {
                    state = State.READ_FIN0_WRITE_FIN0;
                }
                break;
            case WRITE_FIN:
                if (finish) {
                    state = State.READ_FIN0_WRITE_FIN;
                }
                break;
            case READ_FIN0_WRITE_FIN0:
            case READ_FIN_WRITE_FIN0:
            case READ_FIN0_WRITE_FIN:
            case FIN:
                throw new ProtocolViolationException(
                    false /* local error */,
                    true /* handshake done */,
                    endpointHandler.getChannelDescription(),
                    ProtocolViolationException.
                    ERROR_INVALID_DIALOG_STATE +
                    String.format(
                        "Received DialogFrame, context=%s", this));
            case ABORTED:
                /* We were somehow aborted, just ignore the frame */
                return;
            default:
                throw new AssertionError();
            }

            messageReceiving.add(frame);
            runPerfCallback(
                (tracker) -> tracker.onReceiveFrame(readableBytes));

            if (cont) {
                return;
            }

            inputMessages.add(messageReceiving);
            messageReceiving = new MessageInput();
            lastMesgReceived = finish;
        }

        onCanReadTask.incNumInvokes();
        callOnCanRead(true);

        if (isFin()) {
            cleanupContext();
        }
    }

    /**
     * Called by the endpoint handler for the context to write next frame.
     *
     * The endpoint handler should ensure that only one thread is calling this
     * method.
     *
     * @return {@code true} if all frames are written
     */
    boolean onWriteDialogFrame() {
        IOBufSliceList frame = null;
        boolean lastFrame;

        synchronized(this) {
            if (isAborted()) {
                return true;
            }

            if (outputFrames == null) {
                lastFrame = true;
            } else {
                frame = outputFrames.poll();
                lastFrame = outputFrames.isEmpty();
            }

            if (lastFrame) {
                switch (state) {
                case INITED_NEED_DIALOGSTART:
                    /* Should already inited and written the DialogStart */
                    throw new AssertionError();
                case STARTED:
                case READ_FIN0:
                case READ_FIN:
                    break;
                case WRITE_FIN0:
                    state = State.WRITE_FIN;
                    break;
                case WRITE_FIN:
                    break;
                case READ_FIN0_WRITE_FIN0:
                    state = State.READ_FIN0_WRITE_FIN;
                    break;
                case READ_FIN_WRITE_FIN0:
                    state = State.FIN;
                    break;
                case READ_FIN0_WRITE_FIN:
                case FIN:
                    /* We already sent all the frames. */
                    throw new AssertionError();
                case ABORTED:
                    break;
                default:
                    throw new AssertionError();
                }

                outputFrames = null;
            }
        }

        if (frame != null) {
            /* Prevent write after a DialogAbort with a sync block. */
            synchronized(frameAndAbortSync) {
                if (state != State.ABORTED) {
                    protocolWriter.writeDialogFrame(
                            (lastMesgWritten && lastFrame), !lastFrame,
                            dialogId, frame);
                    runPerfCallback((tracker) -> tracker.onSendFrame());
                }
            }
        }

        if (lastFrame) {
            onWrittenLastFrame();
        }

        if (isFin()) {
            cleanupContext();
        }

        return lastFrame;
    }

    /**
     * Called by the endpoint handler when reading a DialogAbort.
     *
     * This method will only be called inside the executor thread.
     */
    void onReadDialogAbort(ProtocolMesg.DialogAbort.Cause cause, String detail) {
        endpointHandler.assertInExecutorThread();
        synchronized(this) {
            if (isAborted() || isFin()) {
                return;
            }
            /*
             * Creates abortInfo before assign state to ABORTED so that seeing
             * ABORTED state will guarantee to see abortInfo.
             */
            abortInfo = new AbortInfo(cause, detail);
            state = State.ABORTED;
        }
        cleanupContext();
        callOnAbort(true);
    }

    /**
     * Called when aborted locally because of timeout.
     *
     * This method will only be called inside the executor thread.
     */
    void onLocalAbortTimeout() {
        endpointHandler.assertInExecutorThread();
        synchronized(this) {
            if (isAborted() || isFin()) {
                return;
            }
            abortInfo = new AbortInfo(
                    new RequestTimeoutException(
                        (int) timeoutMillis,
                        String.format(TIMEOUT_INFO_TEMPLATE,
                                      "locally", toString()),
                        null,
                        false),
                    ProtocolMesg.DialogAbort.Cause.TIMED_OUT);
            state = State.ABORTED;
        }
        writeDialogAbort();
        cleanupContext();
        callOnAbort(true);
    }

    /**
     * Called when aborted locally because of a dialog exception.
     */
    void onLocalHandlerError(Throwable cause) {
        /*
         * Unexpected error, log as fine here, the error will be propagated to
         * the channel executor and logged as info there.
         */
        logger.log(Level.FINE, () -> String.format(
            "Encountered error with dialog handler: " +
            "context=%s, error=%s",
            DialogContextImpl.this,
             CommonLoggerUtils.getStackTrace(cause)));
        synchronized(this) {
            if (isAborted() || isFin()) {
                return;
            }
            abortInfo = new AbortInfo(
                    new DialogUnknownException(
                        hasSideEffect(),
                        false /* not from remote */,
                        cause.getMessage(),
                        cause),
                    ProtocolMesg.DialogAbort.Cause.UNKNOWN_REASON);
            state = State.ABORTED;
        }
        writeDialogAbort();
        cleanupContext();
        throw new IllegalStateException(
            "Unexpected dialog handler error", cause);
    }

    /**
     * Called when aborted locally because of a connection exception.
     */
    void onLocalAbortConnectionException(ConnectionException cause) {
        synchronized(this) {
            if (isAborted() || isFin()) {
                return;
            }
            abortInfo = new AbortInfo(cause);
            state = State.ABORTED;
        }
        /*
         * Do not write dialog abort for a connection exception since the
         * endpoint handler will write the connection abort at the end
         */
        cleanupContext();
        callOnAbort(false);
    }

    /**
     * Returns {@code true} if may have side effect on the peer endpoint.
     *
     * <p>We can decide whether this dialog may have any side effect on the peer
     * endpoint by checking if we have sent out any message.
     *
     * <p>This side effect flag is only useful for creating side of a dialog since
     * the responding side usually does not care about whether the dialog has
     * side effect on the creating side.
     *
     * <p>The method should be called inside a synchronization block of this
     * object.

     * <p>TODO: we can be more accurate on whether there is a side effect. For
     * example, we can have the responding side send a side effect flag on the
     * dialog abort message to get a more accurate view of this flag, but not
     * sure if worth the trouble.
     */
    private boolean hasSideEffect() {
        if (!onCreatingSide) {
            /*
             * Always mark as having side effect if we are the responding side.
             */
            return true;
        }
        return state != State.INITED_NEED_DIALOGSTART;
    }

    /**
     * Called when we finished writing all frames of the outstanding message.
     */
    private void onWrittenLastFrame() {
        final boolean callOnCanWrite = clearWriteState();
        if (!callOnCanWrite) {
            return;
        }

        callOnCanWrite();
    }

    /**
     * Transits the write state to indicate we have a pending outgoing message.
     *
     * Returns {@code false} if there is already a pending outgoing message.
     */
    private boolean setWriteState() {
        while (true) {
            final int curr = writeState.get();
            if ((curr & WS_PENDINGMESG) != 0) {
                /* already have pending outgoing message */
                if ((curr & WS_CALLBACK) != 0) {
                    /* already need call onCanWrite */
                    return false;
                }
                final int next = (curr | WS_CALLBACK);
                if (writeState.compareAndSet(curr, next)) {
                    return false;
                }
                continue;
            }

            /* no pending outgoing message */
            if ((curr & WS_CALLBACK) != 0) {
                throw new IllegalStateException(
                              String.format(
                                  "No pending write " +
                                  "but need callback is set. " +
                                  "Context=%s", this));
            }

            final int next = curr | WS_PENDINGMESG;
            if (writeState.compareAndSet(curr, next)) {
                return true;
            }
        }
    }

    /**
     * Transit the write state to indicate we no longer have a pending outgoing
     * message.
     *
     * Clear the callback flag as well and returns {@code true} if need to call
     * onCanWrite.
     */
    private boolean clearWriteState() {
        while (true) {
            final int curr = writeState.get();
            if ((curr & WS_PENDINGMESG) == 0) {
                throw new IllegalStateException(
                              String.format(
                                  "Message write is done, " +
                                  "but no pending write in the first place. " +
                                  "Context=%s", this));
            }

            final int next = 0;
            if (writeState.compareAndSet(curr, next)) {
                return (curr & WS_CALLBACK) != 0;
            }
        }
    }

    /**
     * Calls the onStart method.
     *
     * We must ensure the onStart is the first handler callback to execute and
     * no other handler callback can be called before it returns. We also want
     * to run this task in the calling thread instead of the executor thread to
     * avoid a context switch for the common case.
     */
    void callOnStart() {
        onStartTask.run();
    }


    /**
     * Calls or schedules the onCanRead method. Always execute in the executor
     * thread.
     *
     * @param inExecutorThread {@code true} if we are sure to be in the
     * executor thread
     */
    private void callOnCanRead(boolean inExecutorThread) {
        if (!inExecutorThread) {
            getSchedExecService().execute(onCanReadTask);
            return;
        }
        onCanReadTask.run();
    }

    /**
     * Schedules the onCanWrite method. Always execute in the executor thread.
     */
    private void callOnCanWrite() {
        getSchedExecService().execute(onCanWriteTask);
    }

    /**
     * Calls or schedules the onAbort method. Always execute in the executor
     * thread.
     *
     * @param inExecutorThread {@code true} if we are sure to be in the
     * executor thread
     */
    private void callOnAbort(boolean inExecutorThread) {
        /* Ensure we called onStart first */
        onStartTask.run();

        if (!callingOnAbort.get() && inExecutorThread) {
            onAbortTask.run();
            return;
        }
        getSchedExecService().execute(onAbortTask);
    }

    /**
     * The base class of dialog tasks.
     */
    private abstract class DialogTask implements Runnable {
        @Override
        public String toString() {
            return String.format("%s@Dialog:%s",
                                 getClass().getSimpleName(),
                                 getStringID());
        }
    }

    /**
     * The task that calls the onStart method.
     */
    private class OnStartTask extends DialogTask {
        @Override
        public void run() {
            /* Ensure onStart will only be called once. */
            if (onStartEntered.compareAndSet(false, true)) {
                try {
                    dialogHandler.onStart(DialogContextImpl.this, isAborted());
                } catch (Throwable t) {
                    onLocalHandlerError(t);
                }

                synchronized(onStartLock) {
                    onStartExited = true;
                    if (callOnCanReadAfterStart) {
                        callOnCanRead(false);
                        callOnCanReadAfterStart = false;
                    }
                    if (callOnCanWriteAfterStart) {
                        callOnCanWrite();
                        callOnCanWriteAfterStart = false;
                    }
                    if (callOnAbortAfterStart) {
                        callOnAbort(false);
                        callOnAbortAfterStart = false;
                    }
                }
            }
        }
    }

    /**
     * The task that calls the onCanRead method.
     */
    private class OnCanReadTask extends DialogTask {

        /*
         * To make sure we have the same number of onCanRead as the number of
         * messages arrived
         */
        private int numInvokes = 0;

        @Override
        public void run() {
            if (!onStartExited) {
                synchronized(onStartLock) {
                    if (!onStartExited) {
                        callOnCanReadAfterStart = true;
                        return;
                    }
                }
            }
            while (numInvokes > 0) {
                doWork();
                numInvokes--;
            }
        }

        private void incNumInvokes() {
            numInvokes ++;
        }

        private void doWork() {
            if (state == State.ABORTED) {
                /*
                 * We must check that if the dialog is already aborted
                 * [KVSTORE-880]. In a rare case when this task can be
                 * scheduled as a single task, instead of directly running
                 * inside the onReadDialogFrame method. This is because marking
                 * onStartExited can be delayed after onStart (which is not
                 * executed in the channel executor thread) is called such that
                 * when the dialog frames arrive, the onCanRead cannot run
                 * immediately. Another onAbort task could be run before this
                 * scheduled task causing double entry for the upper layer.
                 *
                 * We do not need to worry about an onAbort method running
                 * concurrently though as all these callbacks are run inside
                 * the channel executor.
                 */
                return;
            }
            try {
                dialogHandler.onCanRead(
                        DialogContextImpl.this, lastMesgReceived);
            } catch (Throwable t) {
                onLocalHandlerError(t);
            }
        }
    }

    /**
     * The task that calls the onCanWrite method.
     */
    private class OnCanWriteTask extends DialogTask {
        @Override
        public void run() {
            if (!onStartExited) {
                synchronized(onStartLock) {
                    if (!onStartExited) {
                        callOnCanWriteAfterStart = true;
                        return;
                    }
                }
            }
            doWork();
        }

        private void doWork() {
            /* Check to see if invoking the callback is valid */
            if ((state != State.ABORTED) && (!lastMesgWritten)) {
                try {
                    dialogHandler.onCanWrite(DialogContextImpl.this);
                } catch (Throwable t) {
                    onLocalHandlerError(t);
                }
            }
        }
    }

    /**
     * The task that calls the onAbort method.
     */
    private class OnAbortTask extends DialogTask {
        @Override
        public void run() {
            if (!onStartExited) {
                synchronized(onStartLock) {
                    if (!onStartExited) {
                        callOnAbortAfterStart = true;
                        return;
                    }
                }
            }
            doWork();
        }

        private void doWork() {
            /*
             * Do not call onAbort if we are in the FIN state. This is possible
             * due to rare races. For example, a dialog frame arrives and we
             * change the state to READ_FIN0_WRITE_FIN0. We want to call the
             * onCanRead but can only schedule it because onStartExited is not
             * yet marked. We then want to abort the dialog due to timeout but
             * again scheduled the task due to onStartExited not yet marked.
             * The onCanRead is called transition the state to FIN, after which
             * this task is run.
             *
             * We do not need to worry about the case when we are already in
             * the ABORTED state, since the later CAS for onAbortCalled will
             * guarantee we only call onAbort once.
             */
            if (state == State.FIN) {
                return;
            }
            if (abortInfo == null) {
                throw new AssertionError();
            }
            /* Ensure onAbort will only be called once. */
            if (onAbortCalled.compareAndSet(false, true)) {
                callingOnAbort.set(true);
                try {
                    dialogHandler.onAbort(
                            DialogContextImpl.this, abortInfo.throwable);
                } catch (Throwable t) {
                    onLocalHandlerError(t);
                } finally {
                    callingOnAbort.set(false);
                }
            }
        }
    }

    /* Information of the dialog abort. */
    private class AbortInfo {
        private final State preState;
        private final ProtocolMesg.DialogAbort.Cause cause;
        private final String detail;
        private final Throwable throwable;
        private final boolean fromRemote;

        /**
         * Creates an abort info due to receiving a DialogAbort message.
         */
        AbortInfo(ProtocolMesg.DialogAbort.Cause cause,
                  String detail) {
            this.preState = state;
            this.cause = cause;
            this.detail = getDetail(detail);
            this.throwable = causeToThrowable(cause, detail);
            this.fromRemote = true;
        }

        /**
         * Creates an abort info due to a connection abort. The connection abort
         * can be local or remote (receiving a ConnectionAbort message). The
         * exception contains that info.
         */
        AbortInfo(ConnectionException cause) {
            this.preState = state;
            this.cause = ProtocolMesg.DialogAbort.Cause.CONNECTION_ABORT;
            this.detail = getDetail(cause.getMessage());
            this.throwable = cause.getDialogException(hasSideEffect());
            this.fromRemote = cause.fromRemote();
        }

        /**
         * Creates an abort info due to local abort.
         */
        AbortInfo(Throwable throwable,
                  ProtocolMesg.DialogAbort.Cause cause) {
            this.preState = state;
            this.cause = cause;
            this.detail = getDetail(throwable.getMessage());
            this.throwable = throwable;
            this.fromRemote = false;
        }

        String getDetail(String s) {
            return (s == null) ? "null" : s;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("(");
            sb.append("preState=").append(preState);
            sb.append(", causeToSend=").append(cause);
            sb.append(", detail=").append(detail);
            sb.append(", throwable=").append(throwable);
            sb.append(", fromRemote=").append(fromRemote);
            sb.append(")");
            return sb.toString();
        }
    }

    private Throwable causeToThrowable(ProtocolMesg.DialogAbort.Cause c,
                                       String d)  {
        switch(c) {
        case UNKNOWN_REASON:
            return new DialogUnknownException(
                    hasSideEffect(),
                    true /* from remote */,
                    d, null /* no cause */);
        case CONNECTION_ABORT:
            /*
             * TODO: The following code is to support an old wire protocol that
             * the DialogAbort message is sent for connection abort. Remove
             * after 23.1 release.
             */
            return (new ConnectionIOException(
                true /* handshake done */,
                endpointHandler.getChannelDescription(), d))
                .getDialogException(hasSideEffect());
        case ENDPOINT_SHUTTINGDOWN:
            /*
             * The dialog was rejected before doing anything, so no side
             * effect
             */
            return (new ConnectionEndpointShutdownException(
                true /* remote */, true /* handshake done */,
                endpointHandler.getChannelDescription(), d)).
                getDialogException(false);
        case TIMED_OUT:
            return new RequestTimeoutException(
                    (int) timeoutMillis,
                    String.format(TIMEOUT_INFO_TEMPLATE,
                                  "remotely", toString()),
                    null, true);
        case UNKNOWN_TYPE:
            return new DialogNoSuchTypeException(d);
        default:
            throw new IllegalArgumentException(
                    String.format("Unknown dialog abort cause: %s", c));
        }
    }

    /**
     * Context is done, clean up.
     */
    private void cleanupContext() {
        if (doneTimeNs.compareAndSet(
            UNASSIGNED_DONE_TIME, System.nanoTime())) {

            synchronized(this) {
                if (outputFrames != null) {
                    while (true) {
                        IOBufSliceList outputFrame = outputFrames.poll();
                        if (outputFrame == null) {
                            break;
                        }
                        outputFrame.freeEntries();
                    }
                    outputFrames = null;
                }
                if (inputMessages != null) {
                    while (true) {
                        MessageInput inputMessage = inputMessages.poll();
                        if (inputMessage == null) {
                            break;
                        }
                        inputMessage.discard();
                    }
                    inputMessages = null;
                }
                messageReceiving.discard();
                messageReceiving = null;
            }
            timeoutTask.cancel();
            if (isAborted()) {
                runPerfCallback(
                    (tracker) -> tracker.onAbort(doneTimeNs.get()));
            } else {
                runPerfCallback(
                    (tracker) -> tracker.onFinish(doneTimeNs.get()));
            }
            logger.log(Level.FINEST, () -> String.format(
                "Dialog context done: %s", this));
            endpointHandler.onContextDone(this);
        }
    }

    /** Indicates that the dialog has consumed a permit. */
    public void setConsumedResourcePermit() {
        this.consumedResourcePermit = true;
    }

    /** Returns whether the dialog has consumed a permit. */
    public boolean getConsumedResourcePermit() {
        return consumedResourcePermit;
    }

    /**
     * Write DialogAbort if we should.
     */
    private void writeDialogAbort() {
        if (abortInfo == null) {
            return;
        }
        if (abortInfo.fromRemote) {
            /* Don't write a DialogAbort if it is aborted by remote. */
            return;
        }
        if (abortInfo.cause ==
            ProtocolMesg.DialogAbort.Cause.CONNECTION_ABORT) {
            /*
             * Do not write CONNECTION_ABORT since it is deprecated.  We have
             * already removed code that directly writes dialog abort messages
             * when there is a connection issue. However, race can happen that
             * at the time when the issue occurs we have not got the dialog Id
             * yet and thus cannot write anything. A later write will write the
             * dialog abort which will actually write the CONNECTION_ABORT.
             */
            return;
        }
        if (dialogId == 0) {
            /*
             * Don't write DialogAbort when the dialogId is not assigned. We
             * may still need to write the message since the dialogId
             * assignment is not in the synchronization block of the context.
             * The write method will follow up on this case.
             */
            return;
        }
        /* Only write DialogAbort once. */
        if (dialogAbortWritten.compareAndSet(false, true)) {
            /* Prevent write before a DialogFrame with sync block. */
            synchronized(frameAndAbortSync) {
                protocolWriter.writeDialogAbort(
                        abortInfo.cause, dialogId, abortInfo.detail);
            }
        }
    }

    private class DialogTimeoutTask extends DialogTask {

        private volatile Future<?> future = null;

        @Override
        public void run() {
            onLocalAbortTimeout();
        }

        void schedule() {
            if (future != null) {
                return;
            }
            final ScheduledExecutorService executor = getSchedExecService();
            try {
                future = executor.schedule(
                        DialogTimeoutTask.this, timeoutMillis,
                        TimeUnit.MILLISECONDS);
                logger.log(
                    Level.FINEST,
                    () ->
                    String.format(
                        "Scheduled timeout task: "
                        + "context=%s, timeout=%s ms, executor=%s",
                        DialogContextImpl.this, timeoutMillis, executor));
            } catch (RejectedExecutionException e) {
                /*
                 * The executor is shutting down, therefore the connection must
                 * be aborted. Just abort the dialog here.
                 */
                onLocalAbortConnectionException(
                    new ConnectionUnknownException(
                        /* unknown handshake state, assume done to be safe */
                        true, endpointHandler.getChannelDescription(), e));
            }
        }

        void cancel() {
            if (future != null) {
                future.cancel(false);
            }
        }
    }

    public String getStringID() {
        return String.format("%s:%s",
                Long.toString(dialogId, 16),
                endpointHandler.getStringID());
    }

    public long getLatencyNanos() {
        if (doneTimeNs.get() == UNASSIGNED_DONE_TIME) {
            return -1;
        }
        return doneTimeNs.get() - initTimeNanos;
    }

    @Override
    public synchronized String toString() {
        StringBuilder builder = new StringBuilder("DialogContext");
        try (final Formatter fmt = new Formatter(builder)) {
            builder.append("[");
            builder.append(" dialogId=").append(getStringID());
            builder.append(" contextId=").append(Long.toString(contextId, 16));
            builder.append(" dialogType=").append(typeno);
            builder.append(" dialogHandler=").append(dialogHandler);
            builder.append(" onCreatingEndpoint=").append(onCreatingSide);
            builder.append(" initTimeMillis=").
                append(FormatUtils.formatTimeMillis(initTimeMillis));
            fmt.format(" latencyMs=%.2f", getLatencyNanos() / 1.0e6);
            builder.append(" timeout=").append(timeoutMillis);
            builder.append(" state=").append(state);
            builder.append(" writeState=").append(writeState.get());
            builder.append(" abortInfo=").append(abortInfo);
            fmt.format(" infoPerf=%s",
                       (infoPerfTracker.get() == null) ?
                       "not sampled" :
                       infoPerfTracker.get().getDialogInfoPerf());
            builder.append("]");
        }
        return builder.toString();
    }

    public synchronized ObjectNode toJson() {
        final ObjectNode result = JsonUtils.createObjectNode();
        result.put("class", getClass().getSimpleName());
        result.put("dialogId", getStringID());
        result.put("contextId", Long.toString(contextId, 16));
        result.put("dialogType", typeno);
        result.put("dialogHandler", dialogHandler.toString());
        result.put("onCreatingEndpoint", onCreatingSide);
        result.put("initTimeMillis", initTimeMillis);
        result.put("latencyMs", getLatencyNanos() / 1.0e6);
        result.put("timeout", timeoutMillis);
        result.put("state", state.toString());
        result.put("writeState", writeState.get());
        result.put("abortInfo",
                   (abortInfo == null) ?
                   "null" : abortInfo.toString());
        final DialogInfoPerfTracker tracker = infoPerfTracker.get();
        if (tracker != null) {
            result.put("infoPerf", tracker.getDialogInfoPerf().toJson());
        }
        return result;
    }

    /* For testing */
    public Throwable getAbortThrowable() {
        final AbortInfo info = abortInfo;
        if (info == null) {
            return null;
        }
        return info.throwable;
    }
}
