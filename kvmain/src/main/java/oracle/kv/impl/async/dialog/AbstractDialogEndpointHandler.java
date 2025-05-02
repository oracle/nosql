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

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.async.AsyncOption;
import oracle.kv.impl.async.DialogHandler;
import oracle.kv.impl.async.DialogHandlerFactory;
import oracle.kv.impl.async.DialogHandlerFactoryMap;
import oracle.kv.impl.async.DialogResourceManager;
import oracle.kv.impl.async.DialogResourceManager.HandleCallback;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.EndpointHandlerManager;
import oracle.kv.impl.async.IOBufSliceList;
import oracle.kv.impl.async.MessageInput;
import oracle.kv.impl.async.MessageOutput;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.dialog.exception.ProtocolViolationException;
import oracle.kv.impl.async.exception.ConnectionEndpointShutdownException;
import oracle.kv.impl.async.exception.ConnectionException;
import oracle.kv.impl.async.exception.ConnectionIOException;
import oracle.kv.impl.async.exception.ConnectionIdleException;
import oracle.kv.impl.async.exception.ConnectionIncompatibleException;
import oracle.kv.impl.async.exception.ConnectionLimitExceededException;
import oracle.kv.impl.async.exception.ConnectionTimeoutException;
import oracle.kv.impl.async.exception.ConnectionUnknownException;
import oracle.kv.impl.async.exception.InitialConnectIOException;
import oracle.kv.impl.async.exception.InitialHandshakeIOException;
import oracle.kv.impl.async.perf.DialogEndpointPerfTracker;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

/**
 * Abstract dialog endpoint handler that manages dialog contexts and protocol
 * events.
 */
public abstract class AbstractDialogEndpointHandler
    implements DialogEndpointHandler {

    private final static SecureRandom random = new SecureRandom();

    private final Logger logger;
    private final long creationTime = System.currentTimeMillis();
    private final RateLimitingLogger<String> rateLimitingLogger;
    private final EndpointHandlerManager parent;
    private final boolean isCreator;
    private final NetworkAddress remoteAddress;

    private final AtomicLong sequencer = new AtomicLong(0);

    /* The uuid for the handler. */
    private final long uuid;
    /*
     * The uuid for the connection. It equals to the uuid field if the handler
     * is of a creator endpoint. It equals to 0 and are assigned during
     * handshake if the handler is of responder endpoint.
     */
    private volatile long connid;

    /* Options */

    /* maxDialogs for locally-started dialogs*/
    private volatile int localMaxDlgs;
    /* maxLen for outgoing protocol messages */
    private volatile int localMaxLen;
    /* max data size for outgoing dialog messages */
    private volatile int localMaxTotLen;
    /* maxDialogs for remotely-started dialogs */
    private final int remoteMaxDlgs;
    /* maxLen for incoming protocol messages */
    private final int remoteMaxLen;
    /* max data size for incoming dialog messages */
    private final int remoteMaxTotLen;
    /* the connect timeout */
    private final int connectTimeout;
    /* the heartbeat timeout */
    private final int heartbeatTimeout;
    /* the idle timeout */
    private final int idleTimeout;
    /* the heartbeat interval */
    private volatile int heartbeatInterval;
    /* the max number of dialog context we poll for each batch when we flush */
    private final int flushBatchNumContexts;
    /* the max number of batches when we flush */
    private final int flushNumBatches;

    private enum State {

        /*
         * The executor is not ready; the state is to ensure we call the
         * onExecutorReady method after construction.
         */
        NEED_EXECUTOR,

        /*
         * The channel is connecting to the remote; dialog activities are
         * allowed; but channel read/write not possible.
         */
        CONNECTING,

        /*
         * The connection established; the endpoint is doing handshaking step1;
         * dialog activities are allowed; but channel read/write for only
         * handshake message.
         */
        HANDSHAKING_STEP1,

        /*
         * The connection established; the endpoint is doing handshaking step2;
         * dialog activities are allowed, but channel read/write for only
         * handshake message.
         */
        HANDSHAKING_STEP2,

        /*
         * The handshake done; dialog activities allowed; channel read/write dialog
         * data allowed.
         */
        NORMAL,

        /*
         * Existing dialogs can finish up; new dialogs are not allowed; channel
         * read/write still allowed.
         */
        SHUTTINGDOWN,

        /*
         * A moment before TERMINATED. The channel is closing. No dialog or
         * channel read/write allowed.
         */
        TERMINATING,

        /*
         * All dialogs are finished; channel is closed. The handler is in its
         * final state.
         */
        TERMINATED,
    }

    /*
     * State of the endpoint. Updates must be inside the synchronization block
     * of this object. Volatile for read access.
     */
    private volatile State state = State.NEED_EXECUTOR;
    /* Verifies the message type */
    private final Predicate<Byte> messageTypeVerifier = this::verifyMessageType;
    /* Dialog factories this endpoint support */
    private final DialogHandlerFactoryMap dialogHandlerFactories;
    /* The concurrent responding dialogs resource handle */
    private final DialogResourceManager.Handle respondingResourceHandle;
    /**
     * The number of permits reserved for the remote dialogs. This field
     * accounts for the number of permits that we requested and the {@link
     * DialogResourceManager} reserved for us that cannot be granted for other
     * endpoint handlers. The permits are granted in two kinds of behaviors.
     * The first kind is that we call the reserve method and the permits is
     * granted. The second is that the reserve call failed in which case this
     * endpoint is queued and later some newly-freed permits are directly
     * granted. Note that even though each permit is reserved for a remote
     * dialog, the number of this field may temporarily be greater than the
     * number of remote dialogs. That is, we may reserve the permits, increment
     * this field and then later find the remote dialogs cannot be started for
     * some reason.
     */
    private final AtomicInteger numReservedPermits = new AtomicInteger(0);
    /**
     * A task to schedule channel read, called when permits become available.
     */
    private final HandleCallback scheduleChannelReadTask = (npermits) -> {
        int n = npermits;
        try {
            if (isTerminatingOrAfter()) {
                return;
            }
            final ScheduledExecutorService executor =
                getSchedExecService();
            if (executor != null) {
                executor.submit(() -> onChannelInputRead(npermits));
                n -= npermits;
            }
        } finally {
            /*
             * When the task submitted to the executor is successful, the
             * onChannelInputRead is responsible to do the proper accounting of
             * permits. However, if the task is rejected, we should free the
             * permits here.
             */
            if (n != 0) {
                freePermits(n);
            }
        }
    };
    /*
     * Latest started dialogId by this endpoint. For a dialog context, the
     * dialogId is obtained upon first write. We acquire the dialogLock to
     * increase the value. Volatile for read thread-safety.
     */
    private volatile long latestLocalStartedDialogId = 0;
    private final ReentrantLock dialogLock = new ReentrantLock();
    private final Semaphore localDialogResource = new Semaphore(0);
    /*
     * Latest started dialogId by the remote. It will only be accessed inside
     * the single excutor executor thread.
     */
    private long latestRemoteStartedDialogId = 0;
    private final Semaphore remoteDialogResource = new Semaphore(0);

    /*
     * Contexts for dialogs with a dialogId. Use concurrent hash map for
     * get/put thread safety. Iterations are not thread-safe. Therefore, when
     * we are terminating and aborting all contexts, some contexts may be
     * missed. Hence, after each put we should check if we are terminating and
     * if yes, we should abort the contexts after.
     */
    private final Map<Long, DialogContextImpl> dialogContexts =
        new ConcurrentHashMap<Long, DialogContextImpl>();
    /*
     * Contexts that are submitted before handshake is done (and therefore,
     * onStart is not called yet). Use synchronization block on this list for
     * all access.
     */
    private final List<DialogContextImpl> preHandshakeContexts =
        new ArrayList<DialogContextImpl>();
    /*
     * Contexts for dialogs that the dialogId is yet to be assigned.
     * Thread-safety issues are the same with dialogContexts.
     */
    private final Set<DialogContextImpl> pendingDialogContexts =
        Collections.newSetFromMap(
            new ConcurrentHashMap<DialogContextImpl, Boolean>());
    /*
     * Contexts that have some messages to write. Acquire the writing lock
     * before add, remove or iteration.
     */
    private final Set<DialogContextImpl> writingContexts =
        new LinkedHashSet<DialogContextImpl>();
    private final ReentrantLock writingLock = new ReentrantLock();

    /*
     * Indicating we have a pending shutdown when we shut down gracefully
     * before handshake is done.
     */
    private volatile boolean pendingShutdown = false;
    /*
     * Information of the termination. There are two kinds of termination: a
     * graceful shutdown (ShutdownInfo) or an abrupt abort (AbortInfo). A
     * shutdown info will be replaced by an abort info, but not a shut down
     * info; an abort info will not be replaced. Write should be inside a
     * synchronization block of this object. Volatile for read thread safety.
     */
    private volatile TerminationInfo terminationInfo = null;
    /* Atomic flag to make sure we only write connection abort message once. */
    private final AtomicBoolean connectionAbortWritten =
        new AtomicBoolean(false);

    /* The flush task after contexts write new messages */
    private final ContextNewWriteFlushTask contextNewWriteFlushTask =
        new ContextNewWriteFlushTask();

    /*
     * The flag to indicate if last flush sends all the data to the transport.
     */
    private volatile boolean lastFlushFinished = true;
    private volatile boolean needsFlush = false;
    private ReentrantLock flushLock = new ReentrantLock();

    /* Timeout and heartbeat tasks and variables. */
    private final ConnectTimeoutTask connectTimeoutTask;
    private volatile boolean noReadLastInterval = true;
    private volatile boolean noDialogFlushLastInterval = true;
    private volatile boolean noDialogActive = false;
    private final List<ChannelPeriodicTask> scheduledTasks =
        Collections.synchronizedList(new ArrayList<>());
    /**
     * Whether we have received no-op before. This is used to decide if we
     * should send out no-op as heartbeat to circumvent a incompatibility issue
     * caused by [KVSTORE-1462] change where we switched from no-op from both
     * endpoints to ping from creator endpoint for heartbeat in 22.3 release.
     * This change can be removed when previous behavior (in 22.2) is no longer
     * supported as a prerequisite. See [KVSTORE-1935].
     */
    private volatile boolean receivedNoOperation = false;

    /* Perf */
    private final DialogEndpointPerfTracker perfTracker;

    /* Test hooks */
    public volatile static
        TestHook<AbstractDialogEndpointHandler> handshakeDoneTestHook;
    public volatile static
        TestHook<AbstractDialogEndpointHandler> flushTestHook;

    /** If true, disable sending heartbeats -- for testing */
    public static volatile boolean testDisableHeartbeat;

    public AbstractDialogEndpointHandler(
        Logger logger,
        EndpointHandlerManager parent,
        EndpointConfig endpointConfig,
        boolean isCreator,
        String perfName,
        NetworkAddress remoteAddress,
        DialogHandlerFactoryMap dialogHandlerFactories,
        DialogResourceManager dialogResourceManager) {

        /*
         * Initialize the local value according to endpointConfig. It will be
         * updated after handshake. The updated value will be the min of the
         * set value and the value that the remote tells us.
         */
        this.localMaxDlgs =
            endpointConfig.getOption(AsyncOption.DLG_LOCAL_MAXDLGS);
        this.localMaxLen =
            endpointConfig.getOption(AsyncOption.DLG_LOCAL_MAXLEN);
        this.localMaxTotLen =
            endpointConfig.getOption(AsyncOption.DLG_LOCAL_MAXTOTLEN);
        /* Sets the remote value according to endpointConfig. */
        this.remoteMaxDlgs =
            endpointConfig.getOption(AsyncOption.DLG_REMOTE_MAXDLGS);
        this.remoteMaxLen =
            endpointConfig.getOption(AsyncOption.DLG_REMOTE_MAXLEN);
        this.remoteMaxTotLen =
            endpointConfig.getOption(AsyncOption.DLG_REMOTE_MAXTOTLEN);
        /* Sets the timeout */
        this.connectTimeout =
            endpointConfig.getOption(AsyncOption.DLG_CONNECT_TIMEOUT);
        this.heartbeatTimeout =
            endpointConfig.getOption(AsyncOption.DLG_HEARTBEAT_TIMEOUT);
        /*
         * The heartbeatInterval will be updated after handshake. The updated
         * value will be the max of the set value and the value that the remote
         * tells us.
         */
        this.heartbeatInterval =
            endpointConfig.getOption(AsyncOption.DLG_HEARTBEAT_INTERVAL);
        this.idleTimeout =
            endpointConfig.getOption(AsyncOption.DLG_IDLE_TIMEOUT);
        /* Sets the flush batch endpointConfig */
        this.flushBatchNumContexts =
            endpointConfig.getOption(AsyncOption.DLG_FLUSH_BATCHSZ);
        this.flushNumBatches =
            endpointConfig.getOption(AsyncOption.DLG_FLUSH_NBATCH);

        this.logger = logger;
        this.rateLimitingLogger =
            new RateLimitingLogger<>(60 * 60 * 1000 /* logSamplePeriodMs */,
                                     20 /* maxObjects */,
                                     logger);
        this.parent = parent;
        this.uuid = getNonZeroLongUUID();
        this.connid = (isCreator) ? uuid : 0;
        this.isCreator = isCreator;
        this.remoteAddress = remoteAddress;
        this.dialogHandlerFactories = dialogHandlerFactories;
        this.respondingResourceHandle =
            dialogResourceManager.createHandle(
                getStringID(), scheduleChannelReadTask);
        this.remoteDialogResource.release(remoteMaxDlgs);
        this.connectTimeoutTask = new ConnectTimeoutTask(this);
        this.perfTracker = parent.getEndpointGroupPerfTracker().
            getDialogEndpointPerfTracker(perfName, isCreator);
    }

    /* Implements the EndpointHandler interface */

    /**
     * Returns the logger.
     */
    @Override
    public Logger getLogger() {
        return logger;
    }

    /**
     * Returns the network address of the remote endpoint.
     */
    @Override
    public NetworkAddress getRemoteAddress() {
        return remoteAddress;
    }

    /**
     * Get the UUID for this handler.
     */
    @Override
    public long getUUID() {
        return uuid;
    }

    /**
     * Get the UUID for the connection.
     */
    @Override
    public long getConnID() {
        return connid;
    }

    /**
     * Returns a string format of the ID.
     */
    @Override
    public String getStringID() {
        if (isCreator) {
            return String.format("%x", uuid);
        }
        return String.format("%x:%x", connid, uuid);
    }

    /**
     * Called when {@code DialogContextImpl} is finished normally or aborted.
     *
     * The DialogContextImpl should guarantee this method is called only once.
     *
     * Remove the context from dialogContexts and pendingDialogContexts. Do not
     * remove context from writingContexts since we may be iterating on it. The
     * context will be removed during the iteration.
     */
    @Override
    public void onContextDone(DialogContextImpl context) {
        long dialogId = context.getDialogId();
        if (context.getConsumedResourcePermit()) {
            final boolean local =
                (dialogId == 0)
                || (isCreator && (dialogId > 0))
                || ((!isCreator) && (dialogId < 0));
            if (local) {
                localDialogResource.release();
            } else {
                remoteDialogResource.release();
                freePermits(1);
            }
        }
        if (dialogId != 0) {
            dialogContexts.remove(dialogId);
        } else {
            /*
             * Acquire dialogLock to protect race against
             * writeDialogStartForContext which moves a context from
             * pendingDialogContexts to dialogContexts.
             */
            dialogLock.lock();
            try {
                pendingDialogContexts.remove(context);
                dialogId = context.getDialogId();
                /*
                 * Also remove the dialogId from dialogContexts. The
                 * ConcurrentHashMap#remove does nothing if the key is not in
                 * the map.
                 */
                dialogContexts.remove(dialogId);
            } finally {
                dialogLock.unlock();
            }
        }
        if (context.isFin()) {
            perfTracker.onDialogFinished(context);
        } else {
            perfTracker.onDialogAborted(context);
        }
        logger.log(Level.FINEST, () -> String.format(
            "Endpoint handler done with dialog: " +
            "dialogId=%s, localActive=%s, remoteActive=%s",
            context.getStringID(),
            localMaxDlgs - localDialogResource.availablePermits(),
            remoteMaxDlgs - remoteDialogResource.availablePermits()));
    }

    /**
     * Called when {@code DialogContextImpl} has new message to write.
     */
    @Override
    public void onContextNewWrite(DialogContextImpl context) {

        assert context.getDialogId() != 0;
        writingLock.lock();
        try {
            writingContexts.add(context);
        } finally {
            writingLock.unlock();
        }
        contextNewWriteFlushTask.schedule();
    }

    private class ContextNewWriteFlushTask implements Runnable {

        private final AtomicBoolean alreadyScheduled =
            new AtomicBoolean(false);

        @Override
        public void run() {
            /*
             * Clear the flag first so that another thread attempting to
             * schedule a new task will not skip a flush.
             */
            alreadyScheduled.set(false);
            flushOrTerminate();
        }

        public void schedule() {
            if (alreadyScheduled.compareAndSet(false, true)) {
                getSchedExecService().execute(this);
            }
        }
    }

    /**
     * Gets the max value of the size of a {@link MessageInput}.
     *
     * @return the max value
     */
    @Override
    public int getMaxInputTotLen() {
        return remoteMaxTotLen;
    }

    /**
     * Gets the max value of the size of a {@link MessageOutput}.
     *
     * @return the max value
     */
    @Override
    public int getMaxOutputTotLen() {
        return localMaxTotLen;
    }

    /**
     * Gets the max length for incoming protocol message.
     */
    @Override
    public int getMaxInputProtocolMesgLen() {
        return remoteMaxLen;
    }

    /**
     * Gets the max length for outgoing protocol message.
     */
    @Override
    public int getMaxOutputProtocolMesgLen() {
        return localMaxLen;
    }


    /**
     * Writes a DialogStart message for {@code DialogContextImpl}.
     */
    @Override
    public long writeDialogStartForContext(boolean finish,
                                           boolean cont,
                                           int typeno,
                                           long timeoutMillis,
                                           IOBufSliceList frame,
                                           DialogContextImpl context) {

        if (context.getDialogId() != 0) {
            throw new IllegalStateException(
                          String.format("Writing dialog start " +
                                        "when it already has a dialogId, " +
                                        "context=%s, endpointHandlerId=%s",
                                        context, getStringID()));
        }

        long dialogId;
        dialogLock.lock();
        try {
            dialogId = isCreator ?
                ++latestLocalStartedDialogId : --latestLocalStartedDialogId;
            if (pendingDialogContexts.remove(context)) {
                dialogContexts.put(dialogId, context);
            } else {
                if (!context.isAborted()) {
                    throw new IllegalStateException(
                                  String.format(
                                      "Context not in the pending map " +
                                      "while writing dialog start: %s" +
                                      "endpointHandlerId=%s",
                                      context.toString(), getStringID()));
                }
            }
            /*
             * Write the dialog start after we move context from
             * pendingDialogContexts to dialogContexts, otherwise, race can
             * occur that when we get a response for the dialog, it is not in
             * the dialogContexts.
             */
            getProtocolWriter().
                writeDialogStart(context.getInfoPerfTracker()
                                 .map((p) -> p.isSampledForRecord())
                                 .orElse(false),
                                 finish, cont, typeno, dialogId,
                                 timeoutMillis, frame);
        } finally {
            dialogLock.unlock();
        }

        return dialogId;
    }

    /**
     * Starts a dialog.
     */
    @Override
    public void startDialog(int dialogType,
                            DialogHandler dialogHandler,
                            long timeoutMillis) {

        if (timeoutMillis < 0) {
            throw new IllegalArgumentException(
                          "Timeout value must not be less than zero");
        }

        if (timeoutMillis == 0) {
            timeoutMillis = Integer.MAX_VALUE;
        }

        final DialogContextImpl context =
            new DialogContextImpl(this, dialogHandler,
                                  sequencer.incrementAndGet(), 0,
                                  dialogType, timeoutMillis);

        /* check 1 */
        if (!isNormalOrAfter()) {
            synchronized(preHandshakeContexts) {
                if (!isNormalOrAfter()) {
                    if (preHandshakeContexts.size() > localMaxDlgs) {
                        dropDialog(context,
                            new ConnectionLimitExceededException(localMaxDlgs,
                                false /* handshake not done */,
                                getChannelDescription()));
                    } else {
                        preHandshakeContexts.add(context);
                        noDialogActive = false;
                        /*
                         * If we already have an executor, then we need to
                         * start the timeout task or otherwise the context will
                         * be hanging if the connection is in before-handshake
                         * state for a long time.
                         */
                        if (state.compareTo(State.NEED_EXECUTOR) > 0) {
                            context.startTimeout();
                        }
                    }
                    return;
                }
            }
        }

        /* check 2 */
        if (isShuttingDownOrAfter() || pendingShutdown) {
            dropDialog(context, terminationInfo.exception());
            return;
        }

        /*
         * We must ensure the following tryStartDialog method is called only
         * when handshake is done.
         *
         * After check1 there are only two possibilities (1) The handler is
         * normal; in this case handshake is already done (2) the handler is
         * shutting-down or after; in this case, check 2 will fail and the
         * following methods are not called
         *
         * Note that if check 2 is before check 1, then it is possible that the
         * tryStartDialog method will be called if the handler is terminated
         * before handshake could happen.
         */
        final boolean started = tryStartDialog(context);

        /* Avoid race with abortDialogs */
        if (isShuttingDownOrAfter() || pendingShutdown) {
            if (started) {
                abortContextAfterShuttingDown(context);
            }
        }
    }

    /**
     * Returns the limit on the number of dialogs this endpoint can
     * concurrently start.
     */
    @Override
    public int getNumDialogsLimit() {
        if (isNormalOrAfter()) {
            return localMaxDlgs;
        }
        return -1;
    }

    /**
     * Shuts down the handler.
     *
     * If force is false, we only need to set the state and the flush method
     * will terminate the handler if there is nothing to do; otherwise, we do
     * the termination procedure.
     */
    @Override
    public void shutdown(String detail, boolean force) {
        logger.log(Level.FINEST, () -> String.format(
            "Endpoint handler shutting down, detail=[%s], force=%s, %s",
            detail, force, toString()));
        if (force) {
            markTerminating(
                new ConnectionEndpointShutdownException(
                    false,
                    isNormalOrAfter(),
                    getChannelDescription(),
                    String.format(
                        "Shut down with force, detail=[%s]", detail)));
            terminate();
        } else {
            markShuttingDown(detail);
            terminateIfNotActive();
        }
    }

    /**
     * Awaits the handler to terminate.
     */
    public void awaitTermination(long timeoutMillis)
        throws InterruptedException {

        if (isTerminated()) {
            return;
        }
        if (timeoutMillis <= 0) {
            return;
        }

        final long ts = System.currentTimeMillis();
        final long te = ts + timeoutMillis;
        while (true) {
            if (isTerminated()) {
                break;
            }
            final long curr = System.currentTimeMillis();
            final long waitTime = te - curr;
            if (waitTime <= 0) {
                break;
            }
            synchronized(this) {
                if (!isTerminated()) {
                    wait(waitTime);
                }
            }
        }
    }

    /**
     * Awaits the handler to finish handshake for testing.
     */
    public void awaitHandshakeDone() {
        while (true) {
            try {
                synchronized(this) {
                    if (!isNormalOrAfter()) {
                        wait();
                    } else {
                        break;
                    }
                }
            } catch (InterruptedException e) {
                throw new Error(e);
            }
        }
    }

    /**
     * Flush the data.
     *
     * We only flushes a maximum of {@code flushNumBatches} batches. See {@link
     * AsyncOption#DLG_FLUSH_BATCHSZ} for the rationale. To do that, we only
     * flush the batch of the specified size and notify the transport layer
     * (registering for write interest) that we need to write again.
     *
     * An alternative of of registering for write interest is to submit a flush
     * task to do the rest of the write. There are two approaches for the
     * executor to do the flush: a standalone executor or the channel executor.
     * The standalone executor may incur extra context switch cost. The channel
     * executor will run the task right after the flush beating the purpose of
     * the batch. For these reasons we choose the registering write approach.
     */
    public void flush() throws IOException {
        final int written = writeDialogFrameBatches();
        if (written != 0) {
            noDialogFlushLastInterval = false;
        }
        /*
         * We must ensure there is always a flushInternal after
         * writeDialogFrameBatches. The following code achieve this. Consider a
         * thread T1,
         * - If one of the tryLock succeeds, then the requirement is
         *   satisfied.
         * - Otherwise, there is another thread T2 that has already executed
         *   tryLock, but yet to execute unlock
         *   * If "needsFlush = false" in T2 is executed before "needsFlush =
         *   true" in T1 then since only T2 can set needsFlush to false (i.e.,
         *   only the thread obtaining the lock sets needsFlush to false), T2
         *   will go through another iteration and flush. T2 goes through
         *   another iteration because T1 set needsFlush to true after T2 set
         *   it to false but while T2 was still holding the lock, the next
         *   iteration check for T2 will thus test to true.
         *   * Otherwise, "needsFlush = false" in T2 is executed after
         *   "needsFlush = true" in T1 and thus the requirement is met. The
         *   requirement is met here because T2 does the flush after setting
         *   needsFlush to false, which is after T1 called
         *   writeDialogFrameBatches and set it to true, so we know the flush
         *   happened after the writeDialogFrameBatches call.
         *
         * Note that the above proof sketch assumes that this is the only place
         * we lock the flushLock.
         */
        needsFlush = true;
        while (needsFlush) {
            if (flushLock.tryLock()) {
                needsFlush = false;
                final long ts = logger.isLoggable(Level.FINEST) ?
                    System.currentTimeMillis() : 0;
                logger.log(Level.FINEST, () -> String.format(
                    "Flushing for %s(%s) in %s",
                    getClass().getSimpleName(), getStringID(),
                    Thread.currentThread()));
                try {
                    lastFlushFinished = flushInternal(hasContextsToWrite());
                } finally {
                    logger.log( Level.FINEST, () -> String.format(
                        "Flush done for %s(%s) in %s, lasted %s ms.",
                        getClass().getSimpleName(), getStringID(),
                        Thread.currentThread(),
                        System.currentTimeMillis() - ts));
                    flushLock.unlock();
                    assert TestHookExecute.doHookIfSet(flushTestHook, this);
                }
            } else {
                break;
            }
        }
    }

    /**
     * Flush the data, terminate if IO exception or not active.
     */
    @Override
    public void flushOrTerminate() {
        try {
            flush();
        } catch (Throwable t) {
            markTerminating(t);
        }
        terminateIfNotActive();
    }

    /**
     * Is the endpoint a creator endpoint?
     */
    public boolean isCreator() {
        return isCreator;
    }

    /* Checks for the state */

    public boolean isNormal() {
        return (state == State.NORMAL);
    }

    public boolean isNormalOrAfter() {
        return State.NORMAL.compareTo(state) <= 0;
    }

    public boolean isShuttingDown() {
        return state == State.SHUTTINGDOWN;
    }

    public boolean isShuttingDownOrAfter() {
        return State.SHUTTINGDOWN.compareTo(state) <= 0;
    }

    public boolean isTerminatingOrAfter() {
        return State.TERMINATING.compareTo(state) <= 0;
    }

    public boolean isTerminated() {
        return (state == State.TERMINATED);
    }

    /**
     * Called when the channel is ready for our own protocol read/write after
     * connected and pre-read done.
     */
    public void onChannelReady() {
        logger.log(Level.FINEST, () -> String.format(
            "Endpoint handler channel ready: %s", this));

        assertInExecutorThread();

        if (!transitStateOrShuttingDownOrDie(
                 State.CONNECTING, State.HANDSHAKING_STEP1)) {
            return;
        }
        if (isCreator) {
            getProtocolWriter().
                writeProtocolVersion(ProtocolMesg.CURRENT_VERSION);
        }
        flushOrTerminate();
    }

    /**
     * Returns the cause of the termination.
     */
    public ConnectionException getTerminationCause() {
        if (terminationInfo == null) {
            return null;
        }
        return terminationInfo.exception();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getClass().getSimpleName());
        sb.append("(").append(getStringID()).append(")");
        sb.append("{");
        sb.append(" creationTime=").append(creationTime);
        sb.append(" isCreator=").append(isCreator);
        sb.append(" remoteAddress=").append(remoteAddress);
        sb.append(" state=").append(state);
        sb.append(" pendingShutdown=").append(pendingShutdown);
        sb.append(" dialogFactories=")
            .append(dialogHandlerFactories.describeActiveDialogTypes());
        synchronized(this) {
            sb.append(" latestLocalStartedDialogId=")
                .append(Long.toString(latestLocalStartedDialogId, 16));
            sb.append(" latestRemoteStartedDialogId=")
                .append(Long.toString(latestRemoteStartedDialogId, 16));
        }
        sb.append(" approx#dialogContexts=").append(dialogContexts.size());
        sb.append(" approx#pendingDialogContexts=")
            .append(pendingDialogContexts.size());
        writingLock.lock();
        try {
            sb.append(" #writingContexts=").append(writingContexts.size());
        } finally {
            writingLock.unlock();
        }
        sb.append(" terminationInfo=").append(terminationInfo);
        sb.append("}");
        return sb.toString();
    }

    public ObjectNode toJson() {
        final ObjectNode result = JsonUtils.createObjectNode();
        result.put("class", getClass().getSimpleName());
        result.put("id", getStringID());
        result.put("creationTime", creationTime);
        result.put("isCreator", isCreator);
        result.put("remoteAddress", remoteAddress.toString());
        result.put("state", state.toString());
        result.put("pendingShutdown", pendingShutdown);
        result.put(
            "activeDialogHandlerFactories",
            dialogHandlerFactories.describeActiveDialogTypes());
        synchronized(this) {
            result.put("latestLocalStartedDialogId",
                               Long.toString(latestLocalStartedDialogId, 16));
            result.put("latestRemoteStartedDialogId",
                               Long.toString(latestRemoteStartedDialogId, 16));
        }
        result.put("numDialogs", dialogContexts.size());
        result.put("numPendingDialogs", pendingDialogContexts.size());
        writingLock.lock();
        try {
            result.put("numWritingDialogs", writingContexts.size());
        } finally {
            writingLock.unlock();
        }
        result.put(
            "terminationInfo",
            (terminationInfo == null) ? null : terminationInfo.toString());
        return result;
    }

    /**
     * Specifies whether to register for read.
     */
    protected abstract void setReadInterest(boolean register)
        throws IOException;

    /**
     * Flush the channel output.
     *
     * @param again {@code true} if have more dialog data batches to write and
     * flush after this one
     * @return {@code true} if all data of this batch are flushed
     */
    protected abstract boolean flushInternal(boolean again)
        throws IOException;

    /**
     * Cleans up resources of the endpoint handler.
     */
    protected abstract void cleanup() throws IOException;

    /**
     * Called when the executor is ready.
     */
    protected void onExecutorReady() {
        synchronized(this) {
            if (state != State.NEED_EXECUTOR) {
                throw new IllegalStateException(
                              "onExecutorReady must be called first");
            }
            state = State.CONNECTING;
        }
        /*
         * Dialogs submitted before executor is ready does not have their
         * timeout task scheduled properly because the executor is not
         * associated with this handler yet. Start the timout task here.
         */
        synchronized(preHandshakeContexts) {
            for (DialogContextImpl context : preHandshakeContexts) {
                context.startTimeout();
            }
        }
        /* Start our connection timeout task. */
        this.connectTimeoutTask.schedule();
    }

    /**
     * Writes batches of dialog frames.
     *
     * @return the number of written frames
     */
    protected int writeDialogFrameBatches() {
        int written = 0;
        for (int i = 0; i < flushNumBatches; ++i) {
            final int n = writeOneDialogFrameBatch();
            if (n <= 0) {
                break;
            }
            written += n;
        }
        if (logger.isLoggable(Level.FINEST)) {
            if (written != 0) {
                final long maxBytesWritten =
                    written * getMaxOutputProtocolMesgLen();
                logger.log(Level.FINEST, String.format(
                    "Obtained %d pending frames " +
                    "(max %d bytes) to flush, handlerid=%s",
                    written, maxBytesWritten, getStringID()));
            }
        }
        return written;
    }

    /**
     * Called when channel input is ready for new protocol messages.
     */
    protected void onChannelInputRead() {
        onChannelInputRead(0);
    }

    /**
     * Called when channel input is ready for new protocol messages.
     *
     * @param numGrantingPermits the number of permits that is being granted by
     * the {@link DialogResourceManager}. These permits are granted in the form
     * of a callback which is the result of a previous unfullfilled reserver
     * permit request of this endpoint handler. We will need to free those
     * permits immediately, if we cannot consume all of the permits being
     * granted.
     */
    private void onChannelInputRead(int numGrantingPermits) {
        assert numGrantingPermits >= 0;
        assertInExecutorThread();

        numReservedPermits.getAndAdd(numGrantingPermits);

        noReadLastInterval = false;
        ProtocolMesg mesg = null;
        /*
         * The total number of permits that is still available. That is the
         * number equals to numGrantingPermits plus the additionally reserved
         * permits minus the ones we have consumed.
         */
        int npermits = numGrantingPermits;
        try {
            while (!isTerminated()) {
                while(npermits > 0) {
                    try {
                        mesg = getProtocolReader().read(messageTypeVerifier);
                    } catch (IllegalStateException e) {
                        if (!isNormalOrAfter()) {
                            /*
                             * Incorrect message type before handshake is done.
                             * Probably a security mismatch.
                             */
                            throw new ProtocolViolationException(
                                false, false /* handshake not done */,
                                getChannelDescription(),
                                String.format(
                                    "%s during handshake, " +
                                    "possible security mismatch",
                                    e.getMessage()));
                        }
                        throw new ProtocolViolationException(
                            false, true /* handshake done */,
                            getChannelDescription(),
                            e.getMessage());
                    }
                    if (mesg == null) {
                        /*
                         * Set for read interest so that this method will be
                         * called again when more data is ready.
                         */
                        setReadInterest(true);
                        return;
                    }
                    if (mesg.type() == ProtocolMesg.DIALOG_START_MESG) {
                        /*
                         * For each dialog start, we reserve one permit. The
                         * onReadDialogStart method will do the proper
                         * accounting (i.e., freeing the permit) for when the
                         * dialog cannot actually start.
                         */
                        npermits--;
                    }
                    onMessageReady(mesg);
                }
                /* no permits */
                if (reservePermit()) {
                    logger.log(Level.FINEST, () -> String.format(
                        "Endpoint handler reserved a permit: %s", this));
                    npermits++;
                } else {
                    logger.log(Level.FINEST, () -> String.format(
                        "Endpoint handler cannot reserve a permit: %s", this));
                    /*
                     * Cannot acquire a permit to start a new dialog, hence
                     * stop reading. This will also block reading frames
                     * from other dialogs or reading heartbeat messages.
                     * This is OK. When the endpoint group runs out of
                     * permits, it means that we are too busy to be
                     * responsive.
                     */
                    setReadInterest(false);
                    break;
                }
            }
        } catch (Throwable t) {
            if (mesg != null) {
                mesg.discard();
            }
            markTerminating(t);
            terminate();
        } finally {
            /* Free any remaining permits */
            freePermits(npermits);
        }
    }

    private boolean verifyMessageType(byte ident) {
        final State currState = state;
        switch (currState) {
        case NEED_EXECUTOR:
        case CONNECTING:
            return false;
        case HANDSHAKING_STEP1:
            if (ident == ProtocolMesg.CONNECTION_ABORT_MESG) {
                return true;
            }
            if (isCreator()) {
                return ident == ProtocolMesg.PROTOCOL_VERSION_RESPONSE_MESG;
            }
            return ident == ProtocolMesg.PROTOCOL_VERSION_MESG;
        case HANDSHAKING_STEP2:
            if (ident == ProtocolMesg.CONNECTION_ABORT_MESG) {
                return true;
            }
            if (isCreator()) {
                return ident == ProtocolMesg.CONNECTION_CONFIG_RESPONSE_MESG;
            }
            return ident == ProtocolMesg.CONNECTION_CONFIG_MESG;
        case NORMAL:
            if ((ident == ProtocolMesg.PROTOCOL_VERSION_MESG) ||
                (ident == ProtocolMesg.PROTOCOL_VERSION_RESPONSE_MESG) ||
                (ident == ProtocolMesg.CONNECTION_CONFIG_MESG) ||
                (ident == ProtocolMesg.CONNECTION_CONFIG_RESPONSE_MESG)) {
                return false;
            }
            return true;
        case SHUTTINGDOWN:
        case TERMINATING:
            /*
             * Allow all kinds of message just in case some race happens and
             * some dialog message slips through. We do not want to confuse
             * the shutting down procedure with a ProtocolViolationException.
             */
            return true;
        case TERMINATED:
            return false;
        default:
            throw new IllegalStateException(String.format(
                "Unknown dialog endpoint handler state: %s", currState));
        }
    }

    /**
     * Write a ConnectionAbort message.
     */
    protected void writeConnectionAbort() {
        if (!isShuttingDownOrAfter()) {
            throw new AssertionError();
        }
        if (isCreator) {
            /*
             * Do not write if this is the creator endpoint. The creator side
             * has some expectation of quality of service from the responder,
             * so the responder provides the connection abort message to
             * explain why it stopped responding.  The responder does not care
             * if the creator goes away, so no need to send the message.
             */
            return;
        }
        if (terminationInfo.fromRemote()) {
            /* Do not write if already aborted by remote. */
            return;
        }
        /* Only write one ConnectionAbort */
        if (connectionAbortWritten.compareAndSet(false, true)) {
            final ConnectionException e = terminationInfo.exception();
            getProtocolWriter().
                writeConnectionAbort(exceptionToCause(e), e.toString());
        }
    }

    /**
     * Marks the endpoint handler as shutting down.
     */
    protected synchronized void markShuttingDown(String detail) {
        if (isShuttingDownOrAfter()) {
            return;
        }
        setTerminationInfo(new ShutdownInfo(detail));
        if (!isNormalOrAfter()) {
            pendingShutdown = true;
            return;
        }
        state = State.SHUTTINGDOWN;
        pendingShutdown = false;
    }

    /**
     * Mark the endpoint handler as terminating.
     */
    protected synchronized void markTerminating(Throwable t) {
        if (isTerminatingOrAfter()) {
            return;
        }
        ConnectionException wrapped;
        if (t instanceof IOException) {
            wrapped = (state.compareTo(State.CONNECTING) <= 0)
                ? new InitialConnectIOException(
                    getChannelDescription(), (IOException) t)
                : ((state.compareTo(State.NORMAL) < 0)
                   ? new InitialHandshakeIOException(
                       getChannelDescription(), (IOException) t)
                   : new ConnectionIOException(
                       true /* handshake done */,
                       getChannelDescription(),
                       (IOException) t));
        } else if (t instanceof ConnectionException) {
            wrapped = (ConnectionException) t;
        } else {
            wrapped = new ConnectionUnknownException(
                isNormalOrAfter(), getChannelDescription(), t);
        }
        setTerminationInfo(new AbortInfo(wrapped));
        state = State.TERMINATING;
    }

    protected synchronized
        void markTerminating(ProtocolMesg.ConnectionAbort.Cause cause,
                             String detail) {
        if (isTerminatingOrAfter()) {
            return;
        }
        setTerminationInfo(new AbortInfo(cause, detail));
        state = State.TERMINATING;
    }

    protected synchronized void markTerminated() {
        if (isTerminated()) {
            return;
        }
        state = State.TERMINATED;
    }

    /**
     * Terminate the handler.
     */
    protected void terminate() {
        if (isTerminated()) {
            return;
        }
        if (!isShuttingDownOrAfter()) {
            /*
             * Should call markShuttingDown or markTerminating before calling
             * terminate.
             */
            throw new IllegalStateException(
                          "The method terminate() is called " +
                          "before transiting to a required state " +
                          "(SHUTTINGDOWN or after). " +
                          "This is a coding error.");
        }

        logger.log(Level.FINEST, () -> String.format(
            "Endpoint handler terminating: %s", this));

        abortDialogs();
        cancelScheduledTasks();
        writeConnectionAbort();
        try {
            flush();
        } catch (Throwable t) {
            /*
             * We expect the flush to throw exceptions since we are terminating
             * (probably because of some error in the first place). It will not
             * cause any more harm if we cannot flush. There is also no need
             * for logging since the root cause should already be logged. Just
             * do nothing.
             */
        } finally {
            try {
                cleanup();
            } catch (Throwable t) {
                logger.log(Level.FINE, () -> String.format(
                    "Error cleaning up for endpoint handler: %s",
                    CommonLoggerUtils.getStackTrace(t)));
            }
        }
        parent.onHandlerShutdown(this);
        parent.getEndpointGroupPerfTracker()
            .removeEndpointPerfTracker(perfTracker);
        markTerminated();
        /* Notify the threads calling awaitTermination. */
        synchronized(this) {
            notifyAll();
        }
        logger.log(Level.FINEST, () -> String.format(
            "Endpoint handler terminated: %s", this));
    }

    /**
     * Sets the termination info.
     */
    private synchronized void setTerminationInfo(TerminationInfo info) {
        if (terminationInfo instanceof AbortInfo) {
            return;
        }
        if ((terminationInfo instanceof ShutdownInfo) &&
            (info instanceof ShutdownInfo)) {
            return;
        }
        terminationInfo = info;
    }

    /**
     * Terminate the handler if it is not active.
     */
    private void terminateIfNotActive() {
        /*
         * If we are shutting down (and NOT terminating) and have nothing more
         * to do, then we can terminate.
         */
        if (isShuttingDown() && !hasActiveDialogs()) {
            try {
                flush();
            } catch (IOException e) {
                markTerminating(e);
                terminate();
                return;
            }
            if (lastFlushFinished) {
                markTerminating(
                    new ConnectionEndpointShutdownException(
                        false /* local */, isNormalOrAfter(),
                        getChannelDescription(),
                        "Shut down gracefully"));
                terminate();
            }
        } else if (isTerminatingOrAfter()) {
            terminate();
        } else if (isShuttingDown()){
            logger.log(
                Level.FINEST,
                () ->
                String.format(
                    "Marked shutting down, "
                    + "but cannot terminate due to activeness: %s",
                    this));
        } else {
            logger.log(
                Level.FINEST,
                () ->
                String.format(
                    "Marked pending shutdown, "
                    + "waiting for handshake to finish: %s",
                    this));

        }
    }

    /**
     * Returns a non-zero UUID.
     */
    private long getNonZeroLongUUID() {
        while (true) {
            long result = random.nextLong();
            if (result != 0) {
                return result;
            }
        }
    }

    /**
     * Drops the dialog when it cannot be executed at all.
     */
    private void dropDialog(DialogContextImpl context, ConnectionException cause) {
        context.onLocalAbortConnectionException(cause);
        perfTracker.onDialogDropped();
    }

    /**
     * Try to start the dialog and returns if the dialog is started.
     *
     * This method must be called when handshake is done such that localMaxDlgs
     * is negotiated and localDialogResource must have released enough permits.
     */
    private boolean tryStartDialog(DialogContextImpl context) {
        if (!localDialogResource.tryAcquire()) {
            dropDialog(context,
                new ConnectionLimitExceededException(localMaxDlgs,
                    true /* handshake done */, getChannelDescription()));
            return false;
        }
        /*
         * Sets the consumed permit flag before we add it to the
         * pendingDialogContexts and do callOnStart to ensure that the flag is
         * set before onContextDone is called. The onContextDone will be called
         * under normal case which is after callOnStart or under failure case
         * which is either after callOnStart or after the dialog is added to
         * the pendingDialogContexts.
         */
        context.setConsumedResourcePermit();
        pendingDialogContexts.add(context);
        noDialogActive = false;
        perfTracker.onDialogStarted(context);
        context.startTimeout();
        context.callOnStart();
        return true;
    }

    /**
     * Aborts DialogContextImpl after channel handler is about to close.
     */
    private void abortContextAfterShuttingDown(DialogContextImpl context) {
        if (!isShuttingDownOrAfter()) {
            throw new AssertionError();
        }
        context.onLocalAbortConnectionException(terminationInfo.exception());
    }

    /**
     * Returns {@code true} if there is still active dialogs.
     *
     * Need extra caution when calling this method as dialogs may be added
     * during the execution of the method.
     */
    private boolean hasActiveDialogs() {
        if (!dialogContexts.isEmpty()) {
            return true;
        }
        if (!pendingDialogContexts.isEmpty()) {
            return true;
        }
        if (dialogLock.tryLock()) {
            try {
                if ((!dialogContexts.isEmpty()) ||
                    (!pendingDialogContexts.isEmpty())) {
                    return true;
                }
            } finally {
                dialogLock.unlock();
            }
        } else {
            return true;
        }
        synchronized (preHandshakeContexts) {
            if (!preHandshakeContexts.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Writes one batch of dialog frames.
     *
     * @return the number of written frames, -1 if cannot acquire lock
     */
    private int writeOneDialogFrameBatch() {
        int written = 0;
        /*
         * Try acquire the lock. If someone else is trying to add or iterate on
         * writingContexts, i.e., they are writing or will write, let them do
         * their work.
         */
        if (writingLock.tryLock()) {
            try {
                Iterator<DialogContextImpl> iter = writingContexts.iterator();
                while (iter.hasNext()) {
                    DialogContextImpl context = iter.next();
                    if (context.isDone()) {
                        iter.remove();
                        continue;
                    }
                    if (context.onWriteDialogFrame()) {
                        iter.remove();
                    }
                    written ++;
                    if (written >= flushBatchNumContexts) {
                        break;
                    }
                }
            } finally {
                writingLock.unlock();
            }
        } else {
            return -1;
        }

        return written;
    }

    /**
     * Returns {@code true} if we have some dialog contexts to write.
     */
    private boolean hasContextsToWrite() {
        if (writingLock.tryLock()) {
            try {
                return !writingContexts.isEmpty();
            } finally {
                writingLock.unlock();
            }
        }
        return true;
    }

    /**
     * Called a protocol message is ready.
     */
    private void onMessageReady(ProtocolMesg mesg) throws IOException {
        switch(mesg.type()) {
        case ProtocolMesg.PROTOCOL_VERSION_MESG:
            onReadProtocolVersion(
                (ProtocolMesg.ProtocolVersion) mesg);
            break;
        case ProtocolMesg.PROTOCOL_VERSION_RESPONSE_MESG:
            onReadProtocolVersionResponse(
                (ProtocolMesg.ProtocolVersionResponse) mesg);
            break;
        case ProtocolMesg.CONNECTION_CONFIG_MESG:
            onReadConnectionConfig(
                (ProtocolMesg.ConnectionConfig) mesg);
            break;
        case ProtocolMesg.CONNECTION_CONFIG_RESPONSE_MESG:
            onReadConnectionConfigResponse(
                (ProtocolMesg.ConnectionConfigResponse) mesg);
            break;
        case ProtocolMesg.NO_OPERATION_MESG:
            onReadNoOperation();
            break;
        case ProtocolMesg.CONNECTION_ABORT_MESG:
            onReadConnectionAbort((ProtocolMesg.ConnectionAbort) mesg);
            break;
        case ProtocolMesg.PING_MESG:
            onReadPing((ProtocolMesg.Ping) mesg);
            break;
        case ProtocolMesg.PINGACK_MESG:
            onReadPingAck((ProtocolMesg.PingAck) mesg);
            break;
        case ProtocolMesg.DIALOG_START_MESG:
            onReadDialogStart((ProtocolMesg.DialogStart) mesg);
            break;
        case ProtocolMesg.DIALOG_FRAME_MESG:
            onReadDialogFrame((ProtocolMesg.DialogFrame) mesg);
            break;
        case ProtocolMesg.DIALOG_ABORT_MESG:
            onReadDialogAbort((ProtocolMesg.DialogAbort) mesg);
            break;
        default:
            throw new IllegalArgumentException(
                          String.format(
                              "Unexpected message type: %s", mesg.type()));
        }
    }

    private void onReadProtocolVersion(ProtocolMesg.ProtocolVersion mesg)
        throws IOException {
        ensureStateOrShuttingDownOrDie(State.HANDSHAKING_STEP1);
        if (isCreator) {
            throw new ProtocolViolationException(
                false, isNormalOrAfter(),
                getChannelDescription(),
                ProtocolViolationException.
                ERROR_INVALID_HANDLER_STATE +
                "Received ProtocolVersion on creator endpoint");
        }
        if (mesg.version != ProtocolMesg.CURRENT_VERSION) {
            throw new ConnectionIncompatibleException(
                false,
                getChannelDescription(),
                String.format(
                    "Incompatible version error: " +
                    "supported=%d, got=%d",
                    ProtocolMesg.CURRENT_VERSION, mesg.version));
        }
        if (transitStateOrShuttingDownOrDie(
                State.HANDSHAKING_STEP1, State.HANDSHAKING_STEP2)) {
            getProtocolWriter().writeProtocolVersionResponse(1);
            flush();
        }
        logger.log(Level.FINEST, () -> String.format(
            "Endpoint handler got protocol version v=%s: %s",
            mesg.version, this));
    }

    private void onReadProtocolVersionResponse(
                     ProtocolMesg.ProtocolVersionResponse mesg)
        throws IOException {
        ensureStateOrShuttingDownOrDie(State.HANDSHAKING_STEP1);
        if (!isCreator) {
            throw new ProtocolViolationException(
                false, isNormalOrAfter(),
                getChannelDescription(),
                ProtocolViolationException.
                ERROR_INVALID_HANDLER_STATE +
                "Received ProtocolVersion on responder endpoint");
        }
        if (mesg.version != ProtocolMesg.CURRENT_VERSION) {
            throw new ConnectionIncompatibleException(
                false,
                getChannelDescription(),
                String.format(
                    "Incompatible version error: " +
                    "supported=%d, got=%d",
                    ProtocolMesg.CURRENT_VERSION,
                    mesg.version));
        }
        if (transitStateOrShuttingDownOrDie(
                State.HANDSHAKING_STEP1, State.HANDSHAKING_STEP2)) {
            getProtocolWriter().writeConnectionConfig(
                                   connid, remoteMaxDlgs, remoteMaxLen, remoteMaxTotLen,
                                   heartbeatInterval);
            flush();
        }
        logger.log(Level.FINEST, () -> String.format(
            "Endpoint handler got protocol version response v=%s: %s",
            mesg.version, this));
    }

    private void onReadConnectionConfig(ProtocolMesg.ConnectionConfig mesg)
        throws IOException {
        ensureStateOrShuttingDownOrDie(State.HANDSHAKING_STEP2);
        if (isCreator) {
            throw new ProtocolViolationException(
                false, isNormalOrAfter(),
                getChannelDescription(),
                ProtocolViolationException.
                ERROR_INVALID_HANDLER_STATE +
                "Received ConnectionConfig on creator endpoint");
        }

        connid = mesg.uuid;

        setConfiguration(mesg.maxDialogs, mesg.maxLength, mesg.maxTotLen,
                         mesg.heartbeatInterval);

        getProtocolWriter().writeConnectionConfigResponse(
                               remoteMaxDlgs, remoteMaxLen, remoteMaxTotLen,
                               heartbeatInterval);
        flush();
        logger.log(Level.FINEST, () -> String.format(
            "Endpoint handler handshake done: %s", this));
        onHandshakeDone();
    }

    private void onReadConnectionConfigResponse(
                     ProtocolMesg.ConnectionConfigResponse mesg) {
        ensureStateOrShuttingDownOrDie(State.HANDSHAKING_STEP2);
        if (!isCreator) {
            throw new ProtocolViolationException(
                false, isNormalOrAfter(),
                getChannelDescription(),
                ProtocolViolationException.
                ERROR_INVALID_HANDLER_STATE +
                "Received ConnectionConfig on responder endpoint");
        }

        setConfiguration(mesg.maxDialogs, mesg.maxLength, mesg.maxTotLen,
                         mesg.heartbeatInterval);

        logger.log(Level.FINEST, () -> String.format(
            "Endpoint handler handshake done: %s", this));
        onHandshakeDone();
    }

    private void onReadNoOperation() {
        ensureStateOrShuttingDownOrDie(State.NORMAL);
        receivedNoOperation = true;
    }

    private void onReadConnectionAbort(ProtocolMesg.ConnectionAbort mesg) {
        markTerminating(mesg.cause, mesg.detail);
        terminate();
    }

    private void onReadPing(ProtocolMesg.Ping mesg)
        throws IOException {
        ensureStateOrShuttingDownOrDie(State.NORMAL);
        getProtocolWriter().writePingAck(mesg.cookie);
        flush();
    }

    private void onReadPingAck(ProtocolMesg.PingAck mesg) {
        ensureStateOrShuttingDownOrDie(State.NORMAL);
        perfTracker.onPingAck(mesg.cookie);
    }

    private void onReadDialogStart(ProtocolMesg.DialogStart mesg)
        throws IOException {
        ensureStateOrShuttingDownOrDie(State.NORMAL);

        long dialogId = mesg.dialogId;
        ensureDialogStartIdValidOrDie(dialogId);
        latestRemoteStartedDialogId = dialogId;

        if (isShuttingDownOrAfter()) {
            final String detail =
                String.format("cause=[%s]",
                              terminationInfo.exception().getMessage());
            getProtocolWriter().
                writeDialogAbort(
                    ProtocolMesg.DialogAbort.Cause.ENDPOINT_SHUTTINGDOWN,
                    dialogId,
                    String.format("Dialog rejected because " +
                                  "endpoint is shutting down, %s",
                                  detail));
            flush();
        }

        int dialogType = mesg.typeno;
        DialogHandlerFactory factory = dialogHandlerFactories.get(dialogType);
        if (factory == null) {
            handleNullFactory(mesg, dialogId, dialogType);
            return;
        }
        DialogHandler handler = factory.create();

        if (!remoteDialogResource.tryAcquire()) {
            String detail =
                ProtocolViolationException.ERROR_MAX_DIALOGS +
                String.format("limit=%d #active=%d",
                              remoteMaxDlgs,
                              (remoteMaxDlgs -
                               remoteDialogResource.availablePermits()));
            throw new ProtocolViolationException(
                false, isNormalOrAfter(), getChannelDescription(), detail);
        }

        DialogContextImpl context =
            new DialogContextImpl(this, handler,
                                  sequencer.incrementAndGet(),
                                  dialogId, dialogType, mesg.timeoutMillis);
        /*
         * Sets the consumed-resource-permit flag before we add this dialog to
         * the queue.
         */
        context.setConsumedResourcePermit();
        dialogContexts.put(dialogId, context);
        noDialogActive = false;

        if (isShuttingDownOrAfter()) {
            mesg.discard();
            abortContextAfterShuttingDown(context);
            return;
        }

        perfTracker.onDialogStarted(context, mesg.sampled);
        context.callOnStart();
        context.startTimeout();
        context.onReadDialogFrame(mesg.finish, mesg.cont, mesg.frame);
    }

    private void handleNullFactory(ProtocolMesg.DialogStart mesg,
                                   long dialogId,
                                   int dialogType)
        throws IOException
    {
        final StringBuilder sb = new StringBuilder();
        final String activeTypeInfo = "active type numbers:" +
            dialogHandlerFactories.describeActiveDialogTypes();
        if (!dialogHandlerFactories.isKnown(dialogType)) {
            /*
             * We got dialogType never known to us.  This could be a bug on
             * the server or the client side, an incompatibility issue or
             * probably we are under some kind of denial of service attack.
             * I do not think it is serious enough to wake people up during
             * night. Therefore logging it with INFO. Since the request is
             * not handled, we are not creating any data corruption, just
             * client requests not being served.  There are other alerts
             * when this is serious enough to wake people up.
             *
             * TODO: Use a 1 hour rate limiting logger for now. In the
             * future when our logger mechanism is more complete, this
             * message should be logged as a one-time only per release
             * based and limiting on the dialog type.
             */
            sb.append("Attempt to access unknown service with dialog type ")
                .append(mesg.typeno);
            rateLimitingLogger.log(
                "unknown dialog type",
                Level.INFO,
                () -> String.format(
                    "%s from remote address %s, %s",
                    sb, remoteAddress, activeTypeInfo));
        } else {
            /*
             * We know about this dialog type, but it is not active. This
             * is expected when the service is shutting down. A usual
             * procedure of service shutting down (see
             * RequestHandlerImpl#stop) is first unbind the dialog type
             * from the registry, then shuts down the listen handle which
             * removes the dialog type from the factory map and then if
             * there is no active dialog type, close the channel.  During
             * that time, existing clients with cached endpoint can still
             * connect with the removed dialog type.
             */
            sb.append("Attempt to access inactive service with dialog type ")
                .append(mesg.typeno);
        }
        /*
         * Discard the message. Do this after we print things out for
         * unknown type.
         */
        mesg.discard();
        sb.append(", ").append(activeTypeInfo);
        getProtocolWriter().
            writeDialogAbort(ProtocolMesg.DialogAbort.Cause.UNKNOWN_TYPE,
                             dialogId, sb.toString());
        flush();
        /*
         * We have reserved a global DialogResourceManager permit entering this
         * method and therefore need to free it if the dialog cannot be
         * started.
         */
        freePermits(1);
    }

    private void onReadDialogFrame(ProtocolMesg.DialogFrame mesg) {
        ensureStateOrShuttingDownOrDie(State.NORMAL);

        long dialogId = mesg.dialogId;
        ensureDialogFrameAbortIdValidOrDie(dialogId);

        DialogContextImpl context = dialogContexts.get(dialogId);
        if (context == null) {
            /*
             * This is allowed since the dialog might be aborted. It could also
             * be a protocol violation when the dialog was never started, but
             * this seems harmless. In either case, just ignore.
             */
            mesg.discard();
            return;
        }
        context.onReadDialogFrame(mesg.finish, mesg.cont, mesg.frame);
    }

    private void onReadDialogAbort(ProtocolMesg.DialogAbort mesg) {
        ensureStateOrShuttingDownOrDie(State.NORMAL);

        long dialogId = mesg.dialogId;
        ensureDialogFrameAbortIdValidOrDie(dialogId);

        DialogContextImpl context = dialogContexts.get(dialogId);
        if (context == null) {
            /* Same with onReadDialogFrame */
            mesg.discard();
            return;
        }
        context.onReadDialogAbort(mesg.cause, mesg.detail);
    }

    private synchronized void ensureStateOrShuttingDownOrDie(State target) {
        if (state == target) {
            return;
        }
        if (isShuttingDownOrAfter()) {
            return;
        }
        throw new ProtocolViolationException(
            false, isNormalOrAfter(),
            getChannelDescription(),
            ProtocolViolationException.
            ERROR_INVALID_HANDLER_STATE +
            String.format("expected=%s, got=%s", target, state));
    }

    private synchronized boolean
        transitStateOrShuttingDownOrDie(State from, State to) {

        if (state == from) {
            state = to;
            return true;
        } else if (isShuttingDownOrAfter()) {
            return false;
        } else {
            throw new IllegalStateException(
                          String.format(
                              "Trying to transit from %s, got %s",
                              from, state));
        }

    }

    private void setConfiguration(long maxDialogs,
                                  long maxLength,
                                  long maxTotLen,
                                  long interval) {
        localMaxDlgs = Math.min(localMaxDlgs, (int) maxDialogs);
        localMaxLen =  Math.min(localMaxLen, (int) maxLength);
        localMaxTotLen =  Math.min(localMaxTotLen, (int) maxTotLen);
        heartbeatInterval =  Math.max(heartbeatInterval, (int) interval);
        logger.log(Level.FINEST, () -> String.format(
            "Setting config, localMaxDlgs=%d, localMaxLen=%d, " +
            "localMaxTotLen=%d, heartbeatInterval=%d",
            localMaxDlgs, localMaxLen, localMaxTotLen, heartbeatInterval));
    }

    private void onHandshakeDone() {
        /* update configurations */
        getProtocolWriter().setMaxLength(localMaxLen);
        localDialogResource.release(localMaxDlgs);
        /* transit state */
        if (transitStateOrShuttingDownOrDie(
                State.HANDSHAKING_STEP2, State.NORMAL)) {
            synchronized(preHandshakeContexts) {
                for (DialogContextImpl context : preHandshakeContexts) {
                    tryStartDialog(context);
                }
                preHandshakeContexts.clear();
            }
        }
        /* Handshake is done. Do test hooks before we schedule tasks. */
        synchronized(this) {
            notifyAll();
        }
        /*
         * Deal with pending shutdown. We only need to mark shutting down, and
         * let the already submitted dialogs keep running.
         */
        if (pendingShutdown) {
            markShuttingDown(terminationInfo.exception().getMessage());
        }
        /* Cancel and schedule tasks. */
        connectTimeoutTask.cancel();
        (new HeartbeatTimeoutTask(this)).schedule();
        if (!testDisableHeartbeat) {
            (new HeartbeatTask(this)).schedule();
        }
        (new IdleTimeoutTask(this)).schedule();
        assert TestHookExecute.doHookIfSet(handshakeDoneTestHook, this);
    }

    private void ensureDialogStartIdValidOrDie(long dialogId) {
        boolean valid = isCreator ?
            (dialogId < latestRemoteStartedDialogId) :
            (dialogId > latestRemoteStartedDialogId);
        if (!valid) {
            throw new ProtocolViolationException(
                false, isNormalOrAfter(),
                getChannelDescription(),
                ProtocolViolationException.
                ERROR_INVALID_DIALOG_STATE +
                String.format(
                    "Received DialogStart, isCreator=%s " +
                    "latestId=%s got=%s",
                    isCreator,
                    Long.toString(latestRemoteStartedDialogId, 16),
                    Long.toString(dialogId, 16)));
        }
    }

    private void ensureDialogFrameAbortIdValidOrDie(long dialogId) {
        boolean validId =
            ((isCreator && (dialogId > 0) &&
              (dialogId <= latestLocalStartedDialogId)) ||
             (isCreator && (dialogId < 0) &&
              (dialogId >= latestRemoteStartedDialogId)) ||
             (!isCreator && (dialogId > 0) &&
              (dialogId <= latestRemoteStartedDialogId)) ||
             (!isCreator && (dialogId < 0) &&
              (dialogId >= latestLocalStartedDialogId)));
        boolean local = ((isCreator && (dialogId > 0)) ||
                         (!isCreator && (dialogId < 0)));
        long latest = (local) ? latestLocalStartedDialogId :
            latestRemoteStartedDialogId;
        if (!validId) {
            throw new ProtocolViolationException(
                false, isNormalOrAfter(),
                getChannelDescription(),
                ProtocolViolationException.
                ERROR_INVALID_DIALOG_STATE +
                String.format(
                    "Received DialogFrame/DialogAbort, " +
                    "isCreator=%s dialogLocal=%s " +
                    "latestId=%s got=%s",
                    isCreator, local,
                    Long.toString(latest, 16),
                    Long.toString(dialogId, 16)));
        }

    }

    /**
     * Abort dialogs in the context maps when terminating.
     *
     * We need to ensure that every active dialog is notified and aborted.
     * There are two races that will cause a context to miss this:
     * - Newly added dialog context may not be seen during the iteration. These
     *   new contexts will be aborted by these adding methods by checking our
     *   state after the context is added.
     * - The writeDialogStartForContext will move context from
     *   pendingDialogContexts to dialogContexts, creating a moment when
     *   neither of the collections holds this context. We grab the
     *   dialogLock when we iterating through the maps to avoid missing that.
     */
    private void abortDialogs() {
        if (!isTerminatingOrAfter()) {
            throw new IllegalStateException(
                          "Abort dialogs should only happen " +
                          "after handler is terminating");
            /*
             * This method is always called after the endpoint handler is
             * marked terminating which creates a happen-before relationship
             * between isTerminatingOrAfter() == true and the following
             * operations. Any operations seeing isTerminatingOrAfter() ==
             * false happens before the following operations.
             *
             * Specifically, in startDialog, if, before exiting, the method
             * sees isShuttingDownOrAfter() == false, the adding operation
             * happens before we enter this method and hence dialog will be
             * seen in one of the context maps and will be aborted here if not
             * finished earlier. Otherwise, the dialog will be aborted in the
             * startDialog method.
             */
        }

        /*
         * Lock the dialogLock so that we will not see an intermediate state
         * when we move contexts from pendingDialogContexts to dialogContexts.
         */
        dialogLock.lock();
        try {
            /* Abort all dialogs in pendingDialogContexts. */
            while (!pendingDialogContexts.isEmpty()) {
                Iterator<DialogContextImpl> iter =
                    pendingDialogContexts.iterator();
                while (iter.hasNext()) {
                    DialogContextImpl context = iter.next();
                    abortContextAfterShuttingDown(context);
                    iter.remove();
                }
            }

            /* Abort all dialogs in dialogContexts. */
            while (!dialogContexts.isEmpty()) {
                Iterator<Map.Entry<Long, DialogContextImpl>> iter =
                    dialogContexts.entrySet().iterator();
                while (iter.hasNext()) {
                    DialogContextImpl context = iter.next().getValue();
                    abortContextAfterShuttingDown(context);
                    iter.remove();
                }
            }
        } finally {
            dialogLock.unlock();
        }

        /* Abort all dialogs in preHandshakeContexts. */
        synchronized(preHandshakeContexts) {
            for (DialogContextImpl context : preHandshakeContexts) {
                context.onLocalAbortConnectionException(
                    terminationInfo.exception());
            }
            preHandshakeContexts.clear();
        }
        /* Also clear up writing contexts. */
        writingLock.lock();
        try {
            writingContexts.clear();
        } finally {
            writingLock.unlock();
        }
    }

    private void cancelScheduledTasks() {
        /*
         * Cacnel the channel scheduled tasks to prevent leak. If the tasks are
         * scheduled after this cancel loop, the scheduling routine will check
         * for shutdown and cancel it.
         */
        synchronized(scheduledTasks) {
            for (ChannelPeriodicTask task : scheduledTasks) {
                task.cancel();
            }
        }
        connectTimeoutTask.cancel();
    }

    /* Information of the termination. */
    private class TerminationInfo {
        private final long terminationTime = System.currentTimeMillis();
        private final State preState;
        private final ConnectionException exception;

        TerminationInfo(ConnectionException exception) {
            this.preState = state;
            this.exception = exception;
        }

        boolean fromRemote() {
            return exception.fromRemote();
        }

        ConnectionException exception() {
            return exception;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("(");
            sb.append("preState=").append(preState);
            sb.append(", terminationTime=").append(terminationTime);
            sb.append(", exception=").
                append(getExceptionString());
            sb.append(", fromRemote=").append(fromRemote());
            sb.append(getStackTrace());
            sb.append(")");
            return sb.toString();
        }

        private String getExceptionString() {
            if ((exception instanceof ConnectionEndpointShutdownException) ||
                (exception instanceof ConnectionIdleException)) {
                return String.format(
                    "expected termination (%s)", exception.getMessage());
            }
            return exception.toString();
        }

        private String getStackTrace() {
            if ((exception instanceof ConnectionEndpointShutdownException) ||
                (exception instanceof ConnectionIdleException)) {
                return "";
            }
            return String.format(", stackTrace=%s",
                                 CommonLoggerUtils.getStackTrace(exception));
        }
    }

    /**
     * Information for local graceful shutdown.
     */
    private class ShutdownInfo extends TerminationInfo {

        ShutdownInfo(String detail) {
            super(new ConnectionEndpointShutdownException(
                false /* local */, isNormalOrAfter(),
                getChannelDescription(), detail));
        }
    }

    /**
     * Information for abrupt abort, local or remote.
     */
    private class AbortInfo extends TerminationInfo {

        /**
         * Constructs an info that represents an abort by remote.
         */
        AbortInfo(ProtocolMesg.ConnectionAbort.Cause protocolCause,
                  String detail) {
            super(causeToException(protocolCause, detail));
        }

        /**
         * Info represents an abort by remote.
         */
        AbortInfo(ConnectionException exception) {
            super(exception);
        }

    }

    private ConnectionException
        causeToException(ProtocolMesg.ConnectionAbort.Cause cause,
                         String detail)  {
        switch (cause) {
        case UNKNOWN_REASON:
            return new ConnectionUnknownException(
                isNormalOrAfter(), getChannelDescription(), detail);
        case ENDPOINT_SHUTDOWN:
            return new ConnectionEndpointShutdownException(
                true /* remote */, isNormalOrAfter(),
                getChannelDescription(), detail);
        case HEARTBEAT_TIMEOUT:
            return new ConnectionTimeoutException(
                true /* remote */, isNormalOrAfter(),
                false /* not persistent */,
                getChannelDescription(), detail);
        case IDLE_TIMEOUT:
            return new ConnectionIdleException(
                true /* remote */, isNormalOrAfter(),
                getChannelDescription(), detail);
        case INCOMPATIBLE_ERROR:
            return new ConnectionIncompatibleException(
                true, getChannelDescription(), detail);
        case PROTOCOL_VIOLATION:
            return new ProtocolViolationException(
                true, isNormalOrAfter(), getChannelDescription(), detail);
        default:
            throw new IllegalArgumentException();
        }
    }

    private ProtocolMesg.ConnectionAbort.Cause
        exceptionToCause(ConnectionException e) {

        if (e instanceof ConnectionUnknownException) {
            return ProtocolMesg.ConnectionAbort.Cause.UNKNOWN_REASON;
        } else if (e instanceof ConnectionEndpointShutdownException) {
            return ProtocolMesg.ConnectionAbort.Cause.ENDPOINT_SHUTDOWN;
        } else if (e instanceof ConnectionTimeoutException) {
            return ProtocolMesg.ConnectionAbort.Cause.HEARTBEAT_TIMEOUT;
        } else if (e instanceof ConnectionIdleException) {
            return ProtocolMesg.ConnectionAbort.Cause.IDLE_TIMEOUT;
        } else if (e instanceof ConnectionIncompatibleException) {
            return ProtocolMesg.ConnectionAbort.Cause.INCOMPATIBLE_ERROR;
        } else if (e instanceof ProtocolViolationException) {
            return ProtocolMesg.ConnectionAbort.Cause.PROTOCOL_VIOLATION;
        } else if (e instanceof ConnectionIOException) {
            /*
             * We have an IOException here. The protocol cause does not
             * matter since we will not be able to write the cause anyway.
             * Returns unknown reason.
             */
            return ProtocolMesg.ConnectionAbort.Cause.UNKNOWN_REASON;
        } else {
            throw new IllegalArgumentException();
        }
    }

    private boolean reservePermit() {
        final boolean reserved = respondingResourceHandle.reserve();
        if (reserved) {
            numReservedPermits.getAndIncrement();
        }
        return reserved;
    }

    private void freePermits(int n) {
        respondingResourceHandle.free(n);
        numReservedPermits.getAndAdd(-n);
    }

    private abstract static class ChannelTask implements Runnable {
        /*
         * The fields are all accessed in the synchronization block of this
         * object.
         */

        /*
         * The parent channel handler, non-null at initialization and null when
         * th task is cancelled. Since tasks will be pinned in the executor
         * queue, setting the handler to null will prevent it from a delayed
         * gc.
         */
        protected AbstractDialogEndpointHandler handler;
        /*
         * The future, non-null when scheduled, null if not scheduled or
         * cancelled.
         */
        protected Future<?> future = null;
        /*
         * A flag for need scheduling. We want to schedule only once. We also
         * want to avoid a big coarse-grain lock (syncrhonized on ChannelTask)
         * for implementing the schedule method since
         * NioChannelExecutor#schedule methods may be blocked waiting for
         * quiescent check to be done.
         *
         * In particular, I have discovered an unexpected deadlock situation
         * where thread T1 having synchronized on the ChannelTask waits for the
         * quiscent check to be done (inside schedule) while T2 inside the
         * quiescent check doing a logging which calls toString and trying to
         * acquire the ChannelTask lock.
         */
        protected boolean needScheduling = true;

        protected ChannelTask(AbstractDialogEndpointHandler handler) {
            synchronized (this) {
                this.handler = handler;
            }
        }

        @Override
        public synchronized String toString() {
            return String.format("%s@EndpointHandler:%s",
                                 getClass().getSimpleName(),
                                 (handler == null) ? "cancelled" :
                                 handler.getStringID());
        }

        abstract void schedule();

        synchronized void cancel() {
            handler = null;
            if (future == null) {
                /*
                 * It is possible that the endpoint handler is terminated
                 * before this task is scheduled. In that case just return.
                 */
                return;
            }
            future.cancel(false);
            future = null;
        }

        synchronized AbstractDialogEndpointHandler getHandler() {
            return handler;
        }

        /**
         * Flags as start scheduling and returns the handler. Return {@code
         * null} if no need to schedule.
         */
        protected synchronized AbstractDialogEndpointHandler
            prepareScheduling()
        {
            if (!needScheduling) {
                return null;
            }
            needScheduling = false;
            return handler;
        }

        /**
         * Cleans up if scheduling has failed.
         */
        protected synchronized void cleanupAfterScheudleFailure(Throwable t) {
            handler.markTerminating(t);
            handler.terminate();
            handler = null;
        }
    }

    private static class ConnectTimeoutTask extends ChannelTask {

        private ConnectTimeoutTask(AbstractDialogEndpointHandler handler) {
            super(handler);
        }

        @Override
        public void run() {
            final AbstractDialogEndpointHandler h = getHandler();
            if (h == null) {
                return;
            }
            h.markTerminating(
                new ConnectionTimeoutException(
                    false, /* fromRemote */
                    h.isNormalOrAfter(),
                    true, /* persistent problem */
                    h.getChannelDescription(),
                    String.format(
                        "Connect timeout, "
                        + "handshake is not done within %d ms "
                        + "since the endpoint handler is created, "
                        + "endpointHandlerId=%s",
                        h.connectTimeout,
                        h.getStringID())));
            h.terminate();
        }

        @Override
        void schedule() {
            final AbstractDialogEndpointHandler h = prepareScheduling();
            if (h == null) {
                return;
            }
            Future<?> f = null;
            try {
                final ScheduledExecutorService executor =
                    h.getSchedExecService();
                f = executor.schedule(
                    this, handler.connectTimeout, TimeUnit.MILLISECONDS);
                h.getLogger().log(
                    Level.FINEST,
                    () ->
                    String.format(
                        "Endpoint handler (%s) "
                        + "scheduled connect timeout task, timeout=%s",
                        handler.getStringID(),
                        handler.connectTimeout));
            } catch (Throwable t) {
                cleanupAfterScheudleFailure(t);
            }
            if (f != null) {
                synchronized(this) {
                    future = f;
                }
            }
        }
    }

    private static abstract class ChannelPeriodicTask extends ChannelTask {

        private ChannelPeriodicTask(AbstractDialogEndpointHandler handler) {
            super(handler);
        }

        protected void schedule(long initialDelay, long period) {
            final AbstractDialogEndpointHandler h = prepareScheduling();
            if (h == null) {
                return;
            }
            Future<?> f = null;
            try {
                f = h.getSchedExecService()
                    .scheduleAtFixedRate(this, initialDelay, period,
                                         TimeUnit.MILLISECONDS);
            } catch (Throwable t) {
                cleanupAfterScheudleFailure(t);
            }
            if (f != null) {
                synchronized(this) {
                    future = f;
                    h.scheduledTasks.add(this);
                    if (h.isShuttingDownOrAfter()) {
                        cancel();
                    }
                }
            }
        }
    }

    private class HeartbeatTimeoutTask extends ChannelPeriodicTask {

        private HeartbeatTimeoutTask(AbstractDialogEndpointHandler handler) {
            super(handler);
        }

        @Override
        public void run() {
            final AbstractDialogEndpointHandler h = getHandler();
            if (h == null) {
                return;
            }
            if (h.noReadLastInterval) {
                h.markTerminating(
                    new ConnectionTimeoutException(
                        false, /* fromRemote */
                        h.isNormalOrAfter(),
                        false, /* not persistent */
                        h.getChannelDescription(),
                        String.format(
                            "Heartbeat timeout, "
                            + "no read event during last %d ms, "
                            + "endpointHandlerId=%s",
                            h.heartbeatTimeout * h.heartbeatInterval,
                            h.getStringID())));
                h.terminate();
            }
            h.noReadLastInterval = true;
        }

        @Override
        void schedule() {
            int period;
            try {
                period = Math.multiplyExact(
                    handler.heartbeatTimeout, handler.heartbeatInterval);
            } catch (ArithmeticException e) {
                period = Integer.MAX_VALUE;
            }
            schedule(0, period);
        }
    }

    private static class HeartbeatTask extends ChannelPeriodicTask {

        private HeartbeatTask(AbstractDialogEndpointHandler handler) {
            super(handler);
        }

        @Override
        public void run() {
            final AbstractDialogEndpointHandler h = getHandler();
            if (h == null) {
                return;
            }
            if (h.isCreator) {
                /*
                 * Send a ping instead of no-op so that we can obtain an
                 * estimation of the transport performance along the way while
                 * generates some heartbeat events. The transport include the
                 * async transport layer (managed with NioEndpointHandler and
                 * NioChannelExecutor) and the system transport layer (managed
                 * with Java SocketChannel, Selector and the OS). Basically, we
                 * are removing the performance impact of server-side request
                 * execution.
                 */
                h.ping();
                h.flushOrTerminate();
            } else if (h.receivedNoOperation && h.noDialogFlushLastInterval) {
                /*
                 * If we have received no-op on a responder endpoint before,
                 * then we should send no-op as heartbeat.
                 */
                h.getProtocolWriter().writeNoOperation();
                h.flushOrTerminate();
            }
            h.noDialogFlushLastInterval = true;
        }

        @Override
        void schedule() {
            schedule(0, handler.heartbeatInterval);
        }
    }

    /**
     * Ping the remote.
     *
     * Note that we overloaded the name ping between this layer and the
     * transport protocol. This ping serves the specific purposes of heartbeat
     * and network performance tracking. The transport protocol ping is more
     * general which takes an arbitrary cookie.
     *
     * Since we only use this ping for internal purposes, we make it a private
     * method. In the future if we want to support a more general ping
     * mechanism we can open it for public again. We need to take caution
     * though for a general callbck mechanism w.r.t. resource leak. See
     * [KVSTORE-1640].
     */
    private void ping() {
        if (isTerminatingOrAfter()) {
            throw new IllegalStateException(terminationInfo.exception());
        }
        final long cookie = System.nanoTime();
        getProtocolWriter().writePing(cookie);
        flushOrTerminate();
    }

    /**
     * Periodic task to terminate the endpoint if it has been idle for a while.
     *
     * The task works as follows.
     *
     * S1: An active dialog is queued in one of the three queues:
     * preHandshakeContexts, pendingDialogContexts, dialogContexts.
     *
     * S2: A dialog start includes the following two steps. S2.1: enqueue the
     * dialog (mentioned in S1); S2.2: noDialogActive set to false.
     *
     * S3: This task runs on a interval T. Denote the task execution T(0), T(1),
     * ... T(i), ... where T(i) is an execution at time i * T. Each task
     * execution may at most contain three steps. S3.1: if noDialogActive is
     * true then the handler will be terminated; S3.2: set noDialogActive to
     * true; S3.3: set noDialogActive to false if one of the three queues in S1
     * is not empty.
     *
     * S4: A dialog starts before T(i) if S2.2 of the dialog is before S3.1 of
     * T(i); a dialog starts after T(i) if S2.1 of the dialog is after S3.3 of
     * T(i). Otherwise, the dialog is started in concurrent with T(i).
     *
     * S5: A dialog can be considered finished when it is removed from the
     * queues in S1.
     *
     * We want to make two guarantees.
     *
     * G1: If all dialogs are finished before T(i - 1) and no dialog started
     * before or in concurrent with T(i), then the handler will be terminated in
     * T(i).
     *
     * G2: If any of the following happens, then the handler will not be
     * terminated in T(i). G2.1: There exists a dialog started and not finished
     * before T(i - 1); G2.2: There exists a dialog started in concurrent with
     * T(i - 1); G2.3: There exists a dialog started after T(i - 1) but before
     * T(i).
     *
     * G1 is satisfied because of the following.
     *
     * P1.1: noDialogActive will be set to true at S3.2 of T(i - 1).
     *
     * P1.2: because all dialogs are finished before T(i - 1), noDialogActive
     * will not be set to false at S3.3 of T(i - 1).
     *
     * P1.3: because no dialog started before or in concurrent with T(i), no
     * S2.3 of any dialog will be executed.
     *
     * P1.4: there are only two places where noDialogActive is set to false,
     * namely, S3.3 (which is not set according to G1.2) and S2.3 (which is not
     * set according to G1.3).
     *
     * According to P1.1 - P1.4, noDialogActive is kept to true at S3.1 of T(i)
     * and therefore the handler is terminated.
     *
     * G2 is satisfied because of the following.
     *
     * P2.1: Suppose there exists a dialog D satisfies G2.1, then the S3.3 of
     * T(i - 1) will set noDialogActive to false and therefore the handler will
     * not be terminated at S3.1 of T(i).
     *
     * P2.2: Suppose there exists a dialog D satisfies G2.2, then S2.1 of D is
     * before S3.3 of T(i - 1). Therefore, S3.3 of T(i - 1) will set
     * noDialogActive to be false and S3.1 of T(i) will not terminate the
     * handler.
     *
     * P2.3: Suppose there exists a dialog D satisifeds G2.3, then S2.2 of D is
     * before S3.1 of T(i) and set noDialogActive to false.
     *
     * According to P2.1 - P2.3, noDialogActive will be set to false before S3.1
     * of T(i), then the handler will not be terminated in T(i).
     */
    private static class IdleTimeoutTask extends ChannelPeriodicTask {

        private IdleTimeoutTask(AbstractDialogEndpointHandler handler) {
            super(handler);
        }

        @Override
        public void run() {
            final AbstractDialogEndpointHandler h = getHandler();
            if (h == null) {
                return;
            }
            /*
             * Terminate the endpoint when no dialog active. Note that the
             * following code can create cases when noDialogActive is checked
             * as true, but then some dialogs are added before we terminate,
             * causing these dialogs to be aborted immediately. There is no
             * correctness issue since the startDialog method will see the
             * state change and abort these dialogs. The issue is with
             * performance.
             *
             * To avoid this issue, we need to add a lock inside startDialog,
             * and since it is a rare case for this to happen while it is a
             * common routine for the startDialog method to add new context,
             * it seems not worth to solve the issue.
             */
            if (h.noDialogActive) {
                h.markTerminating(
                    new ConnectionIdleException(
                        false,
                        h.isNormalOrAfter(),
                        h.getChannelDescription(),
                        String.format(
                            "Idle timeout, "
                            + "connection is idle during last %s ms, "
                            + "endpointHandlerId=%s",
                            h.idleTimeout, h.getStringID())));
                h.terminate();
            }

            h.noDialogActive = true;
            if (h.hasActiveDialogs()) {
                h.noDialogActive = false;
            }
        }

        @Override
        void schedule() {
            schedule(handler.idleTimeout, handler.idleTimeout);
        }
    }

    /* Cancel heartbeat task for testing */
    public void cancelHeartbeatTask() {
        for (ChannelPeriodicTask task : scheduledTasks) {
            if (task instanceof HeartbeatTask) {
                task.cancel();
            }
        }
    }

    /**
     * Returns {@code true} if it is worth closing the underlying channel in
     * graceful manner. For example, IO exceptions and Out-of-memory exceptions
     * should just close the channel immediately. By default, we try our best
     * to close the channel gracefully.
     */
    public boolean shouldCloseChannelGracefully() {
        final ConnectionException e = terminationInfo.exception();
        final Throwable actualCause = e.getCause();
        if (actualCause instanceof IOException) {
            /* No point of shut down gracefully because it would not */
            return false;
        }
        if (actualCause instanceof Error) {
            /*
             * Error seems unexpected and dangerous, just deal with it quickly
             */
            return false;
        }
        return true;
    }
}
