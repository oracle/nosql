/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async.dialog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import oracle.kv.RequestTimeoutException;
import oracle.kv.TestBase;
import oracle.kv.impl.async.BytesUtil;
import oracle.kv.impl.async.CopyingBytesInput;
import oracle.kv.impl.async.DialogContext;
import oracle.kv.impl.async.DialogHandler;
import oracle.kv.impl.async.HeapIOBufferPool;
import oracle.kv.impl.async.IOBufSliceList;
import oracle.kv.impl.async.MessageInput;
import oracle.kv.impl.async.MessageOutput;
import oracle.kv.impl.async.InetNetworkAddress;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.exception.ConnectionException;
import oracle.kv.impl.async.exception.ConnectionUnknownException;
import oracle.kv.impl.async.exception.PersistentDialogException;
import oracle.kv.impl.async.exception.DialogCancelledException;
import oracle.kv.impl.async.exception.DialogException;
import oracle.kv.impl.async.exception.DialogNoSuchTypeException;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;

public class DialogContextImplTest extends TestBase {

    protected static final Random rand = new Random();

    private static final int OUT_ARRAY_SIZE = 128 * 128;
    private static final byte[] ARRAY = new byte[OUT_ARRAY_SIZE];
    private static final int MAX_INPUT_TOT_LEN = 128;
    private static final int MAX_OUTPUT_TOT_LEN = 128;
    private static final int MAX_IN_PROTO_LEN = 64;
    private static final int MAX_OUT_PROTO_LEN = 64;
    private static final int TYPENO = 42;
    private static final String ERROR_DETAIL = "error detail";
    private static final String TIMEOUT = "timed out";

    private enum Check {
        NONE, LOCAL, REMOTE, HAS_SIDE_EFFECT, NO_SIDE_EFFECT,
    }

    private ScheduledExecutorService executorST;
    private ScheduledExecutorService executorMT;

    @Override
    public void setUp() throws Exception {
        executorST = new ScheduledThreadPoolExecutor(1);
        executorMT = new ScheduledThreadPoolExecutor(16);
        Logged.resetT0();
    }

    @Override
    public void tearDown() throws Exception {
        executorST.shutdownNow();
        executorMT.shutdownNow();
    }

    /**
     * Test read/write only cases.
     */
    @Test(timeout=10000)
    public void testRWOnlyRandomST() {
        testRWOnlyRandom(executorST, executorST, 1024, 2, 8, 2, 8);
    }

    /**
     * Test read/write only cases.
     */
    @Test(timeout=60000)
    public void testRWOnlyRandomMT() {
        testRWOnlyRandom(executorST, executorMT, 64, 16, 8, 16, 8);
    }

    private void testRWOnlyRandom(ScheduledExecutorService handlerExecutor,
                                  ScheduledExecutorService handoffExecutor,
                                  int numIter,
                                  int nreadTasks,
                                  int nreadsPerTask,
                                  int nwriteTasks,
                                  int nwritesPerTask) {
        int timeout = 100000;
        for (int i = 0; i < numIter; ++i) {
            int dialogId = rand.nextBoolean() ? 0 : 1;
            ContextTester tester = new ContextTester(
                    dialogId, timeout, handlerExecutor, handoffExecutor);
            try {
                scheduleReadWrite(tester, nreadsPerTask, nwritesPerTask,
                        nreadTasks, nwriteTasks, false);
                tester.await(5000);
                tester.checkInput(nreadsPerTask * nreadTasks);
                tester.checkOutput(nwritesPerTask * nwriteTasks + 2);
                tester.checkStatus();
                tester.checkExceptions();
            } catch (Throwable t) {
                fail(LoggerUtils.getStackTrace(t) + "\n" +
                     tester.toString());
            }
        }
    }

    /**
     * Randomly test the abort behavior.
     */
    @Test(timeout=10000)
    public void testAbortRandomST() {
        testAbortRandom(executorST, executorST, 1024, 2, 8, 2, 8);
    }

    /**
     * Randomly test the abort behavior.
     */
    @Test(timeout=10000)
    public void testAbortRandomMT() {
        testAbortRandom(executorST, executorMT, 64, 16, 8, 16, 8);
    }

    private void testAbortRandom(ScheduledExecutorService handlerExecutor,
                                 ScheduledExecutorService handoffExecutor,
                                 int numIter,
                                 int nreadTasks,
                                 int nreadsPerTask,
                                 int nwriteTasks,
                                 int nwritesPerTask) {
        int timeout = 100000;
        for (int i = 0; i < numIter; ++i) {
            int dialogId = rand.nextBoolean() ? 0 : 1;
            ContextTester tester = new ContextTester(
                    dialogId, timeout, handlerExecutor, handoffExecutor);
            try {
                scheduleReadWrite(tester, nreadsPerTask, nwritesPerTask,
                        nreadTasks, nwriteTasks, true);
                tester.new LocalConnectionAbortTask(false).schedule();
                ProtocolMesg.DialogAbort.Cause cause;
                while (true) {
                    cause =
                        ProtocolMesg.DialogAbort.CAUSES[
                        rand.nextInt(
                                ProtocolMesg.DialogAbort.CAUSES.length)];
                    if (dialogId == 1) {
                        if (cause == ProtocolMesg.DialogAbort.Cause.
                                ENDPOINT_SHUTTINGDOWN) {
                            continue;
                        }
                        if (cause == ProtocolMesg.DialogAbort.Cause.
                                UNKNOWN_TYPE) {
                            continue;
                        }
                    }
                    break;
                }
                tester.new AbortReadTask(cause).schedule();
                tester.await(1000);
                tester.checkStatus();
                tester.checkExceptions();
                tester.checkOnAbort(
                        DialogException.class, Check.NONE, Check.NONE);
            } catch (Throwable e) {
                fail(LoggerUtils.getStackTrace(e) + "\n" +
                     tester.toString());
            }
        }
    }

    /**
     * Test abort happens before anything else.
     */
    @Test
    public void testAbortBeforeStart() {
        int timeout = 100000;
        for (int dialogId = 0; dialogId < 1; ++dialogId) {
            /* local */
            ContextTester tester =
                new ContextTester(dialogId, timeout, executorST, executorST);
            tester.new LocalConnectionAbortTask(false).scheduleOnce();
            tester.await(1000);
            tester.checkStatus();
            tester.checkOnAbortDialogException(
                    DialogException.class, Check.NONE, Check.LOCAL);
            /* remote */
            tester =
                new ContextTester(dialogId, timeout, executorST, executorST);
            tester.new AbortReadTask(
                    ProtocolMesg.DialogAbort.Cause.UNKNOWN_REASON).
                scheduleOnce();
            tester.await(1000);
            tester.checkStatus();
            tester.checkOnAbortDialogException(
                    DialogException.class, Check.NONE, Check.REMOTE);
        }
    }

    /**
     * Test abort happens after finish.
     */
    @Test
    public void testAbortAfterFinish() {
        int timeout = 100000;
        for (int dialogId = 0; dialogId < 1; ++dialogId) {
            ContextTester tester =
                new ContextTester(dialogId, timeout, executorST, executorST);
            scheduleReadWrite(tester, 1, 1, 1, 1, false);
            tester.await(1000);
            tester.new LocalConnectionAbortTask(false).scheduleOnce();
            tester.checkStatus();

            tester =
                new ContextTester(dialogId, timeout, executorST, executorST);
            scheduleReadWrite(tester, 1, 1, 1, 1, false);
            tester.await(1000);
            tester.new AbortReadTask(
                    ProtocolMesg.DialogAbort.Cause.UNKNOWN_REASON).
                scheduleOnce();
            tester.checkStatus();
        }
    }

    /**
     * Test for various abort cases.
     */
    @Test
    public void testAbortRequestTimeoutException() {
        ContextTester tester = null;
        try {
            /* local time out */
            tester = new ContextTester(1, 10, executorST, executorST);
            tester.await(1000);
            tester.checkOnAbortTimeout();
            /* remote time out */
            tester = new ContextTester(1, 100000, executorST, executorST);
            tester.new AbortReadTask(
                    ProtocolMesg.DialogAbort.Cause.TIMED_OUT).scheduleOnce();
            tester.await(1000);
            tester.checkOnAbortTimeout();
        } catch (Throwable t) {
            fail(LoggerUtils.getStackTrace(t) + "\n" + tester);
        }
    }

    @Test
    public void testAbortDialogHandlerError() {
        ContextTester tester = null;
        try {
            /* Error in onStart and onAbort on creator */
            tester = new ContextTester(0, 10000, executorST, executorST, true);
            tester.new InitTask().scheduleOnce();
            tester.await(1000);
            tester.checkCancelled();
            /* Error in onStart and onAbort on responder */
            tester = new ContextTester(1, 10000, executorST, executorST, true);
            tester.new InitTask().scheduleOnce();
            tester.await(1000);
            tester.checkCancelled();
            /* Error in onWrite and onAbort on creator */
            tester = new ContextTester(0, 10000, executorST, executorST, true);
            tester.new InitTask().scheduleOnce();
            tester.new ReqWriteTask(1).scheduleOnce();
            tester.new ReqWriteTask(1).scheduleOnce();
            tester.await(1000);
            tester.checkCancelled();
            /* Error in onRead and onAbort on responder */
            tester = new ContextTester(1, 10000, executorST, executorST, true);
            tester.new InitTask().scheduleOnce();
            tester.new FrameReadTask(1).schedule();
            tester.await(1000);
            tester.checkCancelled();
        } catch (Throwable t) {
            fail(LoggerUtils.getStackTrace(t) + "\n" + tester);
        }
    }

    @Test
    public void testAbortNoSuchType() {
        ContextTester tester = null;
        try {
            tester = new ContextTester(0, 100000, executorST, executorST);
            tester.new InitTask().scheduleOnce();
            tester.new AbortReadTask(
                    ProtocolMesg.DialogAbort.Cause.UNKNOWN_TYPE).
                scheduleOnce();
            tester.await(1000);
            tester.checkOnAbortDialogException(
                    DialogNoSuchTypeException.class,
                    Check.NO_SIDE_EFFECT, Check.REMOTE);
        } catch (Throwable t) {
            fail(LoggerUtils.getStackTrace(t) + "\n" + tester);
        }
    }

    @Test
    public void testAbortEndpointShuttingDown() {
        ContextTester tester = null;
        try {
            tester = new ContextTester(0, 100000, executorST, executorST);
            tester.new InitTask().scheduleOnce();
            tester.new AbortReadTask(
                    ProtocolMesg.DialogAbort.Cause.ENDPOINT_SHUTTINGDOWN).
                scheduleOnce();
            tester.await(1000);
            tester.checkOnAbortDialogException(
                    PersistentDialogException.class,
                    Check.NO_SIDE_EFFECT, Check.REMOTE);
        } catch (Throwable t) {
            fail(LoggerUtils.getStackTrace(t) + "\n" + tester);
        }
    }

    @Test
    public void testAbortConnectionException() {
        ContextTester tester = null;
        try {
            /* creator side local abort */
            tester = new ContextTester(0, 100000, executorST, executorST);
            tester.new LocalConnectionAbortTask(false).scheduleOnce();
            tester.await(1000);
            tester.checkOnAbortDialogException(
                    PersistentDialogException.class,
                    Check.NO_SIDE_EFFECT, Check.LOCAL);
            /* creator side remote abort */
            tester = new ContextTester(0, 100000, executorST, executorST);
            tester.new InitTask().scheduleOnce();
            tester.new LocalConnectionAbortTask(true).scheduleOnce();
            tester.await(1000);
            tester.checkOnAbortDialogException(
                    PersistentDialogException.class,
                    Check.HAS_SIDE_EFFECT, Check.REMOTE);
            /* responder side local abort */
            tester = new ContextTester(1, 100000, executorST, executorST);
            tester.new LocalConnectionAbortTask(false).scheduleOnce();
            tester.await(1000);
            tester.checkOnAbortDialogException(
                    PersistentDialogException.class,
                    Check.HAS_SIDE_EFFECT, Check.LOCAL);
            /* responder side remote abort */
            tester = new ContextTester(1, 100000, executorST, executorST);
            tester.new LocalConnectionAbortTask(true).scheduleOnce();
            tester.await(1000);
            tester.checkOnAbortDialogException(
                    PersistentDialogException.class,
                    Check.HAS_SIDE_EFFECT, Check.REMOTE);
        } catch (Throwable t) {
            fail(LoggerUtils.getStackTrace(t) + "\n" + tester);
        }
    }

    @SuppressWarnings("unused")
    private void scheduleReadWrite(ContextTester tester,
                                   int nreadsPerTask,
                                   int nwritesPerTask,
                                   int nreadTasks,
                                   int nwriteTasks,
                                   boolean ensureAbort) {
        /* Runs the init task. */
        tester.new InitTask().runOnce();
        /* Runs read tasks. */
        for (int n = 0; n < nreadTasks; ++n) {
            tester.new RespReadTask(nreadsPerTask);
        }
        /* Runs write tasks. */
        for (int n = 0; n < nwriteTasks; ++n) {
            if (!ensureAbort) {
                tester.new ReqWriteTask(nwritesPerTask);
            } else {
                tester.new ReqWriteAbortTask(nwritesPerTask);
            }
        }
        tester.scheduleTasks();
        /* Runs a frame read task. */
        tester.new FrameReadTask(nreadsPerTask * nreadTasks).
            schedule();
    }

    /**
     * Helper class for testing.
     */
    private class ContextTester extends Tester {

        /* Executor for the endpoint handler */
        private final ScheduledExecutorService handlerExecutor;
        /* Executor for write-requests or read-responses operations */
        private final ScheduledExecutorService handoffExecutor;
        private final byte[] outputArray;
        private final ByteArrayChannelOutput output;
        private final ProtocolWriter protocolWriter;
        private final DialogEndpointHandler endpointHandler;
        private final DialogHandler dialogHandler;
        private final boolean isCreator;
        private final DialogContextImpl context;
        /* Response reading tasks to release upon onCanRead */
        private final Set<RespReadTask> respReadTasks;
        /* Request writing tasks to release upon onCanWrite */
        private final Set<ReqWriteTask> reqWriteTasks;
        /* Ensure only one last write task is scheudled */
        private final AtomicBoolean lastWriteTaskScheduled =
            new AtomicBoolean(false);
        /* Ranges of written MessageOutput */
        private final ConcurrentLinkedQueue<byte[]> writtenBytes =
            new ConcurrentLinkedQueue<byte[]>();
        /* Ranges of read MessageInput */
        private final ConcurrentLinkedQueue<byte[]> readBytes =
            new ConcurrentLinkedQueue<byte[]>();
        /* Ranges of MessageInput to receive  */
        private final ConcurrentLinkedQueue<byte[]> recvBytes =
            new ConcurrentLinkedQueue<byte[]>();
        /* Accouting of handler methods */
        private final AtomicInteger numOnStart = new AtomicInteger(0);
        private final AtomicInteger numOnAbort = new AtomicInteger(0);
        private volatile boolean isContextDone = false;
        private volatile Throwable abortThrowable = null;
        ContextTester(int dialogId,
                      int timeout,
                      ScheduledExecutorService handlerExecutor,
                      ScheduledExecutorService handoffExecutor) {
            this(dialogId, timeout, handlerExecutor, handoffExecutor, false);
        }
        /**
         * Constructs the tester with possibly faulty handler.
         */
        ContextTester(int dialogId,
                      int timeout,
                      ScheduledExecutorService handlerExecutor,
                      ScheduledExecutorService handoffExecutor,
                      boolean faultyHandler) {
            super(true);
            this.handlerExecutor = handlerExecutor;
            this.handoffExecutor = handoffExecutor;

            this.outputArray = ARRAY;
            this.output = new ByteArrayChannelOutput(outputArray);
            this.protocolWriter = new ProtocolWriter(
                    output, MAX_OUT_PROTO_LEN);
            this.endpointHandler = new EndpointHandlerForTesting();
            this.dialogHandler = new DialogHandlerForTesting(faultyHandler);
            this.isCreator = (dialogId == 0);
            this.context = new DialogContextImpl(
                    endpointHandler, dialogHandler,
                    0, dialogId, TYPENO, timeout);

            this.respReadTasks = Collections.newSetFromMap(
                    new ConcurrentHashMap<RespReadTask, Boolean>());
            this.reqWriteTasks = Collections.newSetFromMap(
                    new ConcurrentHashMap<ReqWriteTask, Boolean>());

            context.startTimeout();
            /*
             * Acquire a semaphore which will be release when the context is
             * done and onAbort is called if necessary.
             */
            semaphoreAcquire();
        }
        void scheduleTasks() {
            for (Task task : respReadTasks) {
                task.schedule();
            }
            for (Task task : reqWriteTasks) {
                task.schedule();
            }
        }
        void checkInput(int expectedNum) {
            assertEquals("Incorrect size for recv.",
                    expectedNum, recvBytes.size());
            assertBytesCollectionEquals(recvBytes, readBytes);
        }
        void checkOutput(int expected) {
            assertEquals("Incorrect number of writes.",
                    expected, writtenBytes.size());
            output.flush();
            ReadResult result = readOutput();
            result.checkDialogBytes(writtenBytes);
        }
        void checkStatus() {
            assertEquals("Context should be done.",
                    true, context.isDone());
            assertEquals("onContextDone should be called.",
                    true, isContextDone);
            if (numOnAbort.get() != 0) {
                assertEquals("Context should be aborted " +
                        "when onAbort was called.",
                        true, context.isAborted());
            } else {
                assertEquals("Context should be fin " +
                        "when onAbort was not called.",
                        true, context.isFin());
            }
        }
        void checkOnAbortTimeout() {
            assertEquals("onAbort must be called once.", 1, numOnAbort.get());
            Class<RequestTimeoutException> cls = RequestTimeoutException.class;
            assertTrue(String.format("expectedClass=%s, got=%s",
                        cls, abortThrowable.getClass()),
                    (cls.isInstance(abortThrowable)));
            RequestTimeoutException exception =
                (RequestTimeoutException) abortThrowable;
            output.flush();
            ReadResult result = readOutput();
            if (exception.wasLoggedRemotely()) {
                result.checkNoDialogAbortMesg();
            } else {
                result.checkDialogAbortMesg(TIMEOUT);
            }
        }
        void checkOnAbortDialogException(Class<? extends DialogException> cls,
                                         Check hasSideEffect,
                                         Check fromRemote) {
            assertEquals("onAbort must be called once.", 1, numOnAbort.get());
            assertTrue(String.format("expectedClass=%s, got=%s",
                        cls, abortThrowable.getClass()),
                    (cls.isInstance(abortThrowable)));
            DialogException exception = (DialogException) abortThrowable;
            switch (hasSideEffect) {
            case HAS_SIDE_EFFECT:
                assertEquals("hasSideEffect mismatch",
                        true, exception.hasSideEffect());
                break;
            case NO_SIDE_EFFECT:
                assertEquals("hasSideEffect mismatch",
                        false, exception.hasSideEffect());
                break;
            case NONE:
                break;
            default:
                throw new AssertionError();
            }
            switch (fromRemote) {
            case REMOTE:
                assertEquals("fromRemote mismatch",
                        true, exception.fromRemote());
                break;
            case LOCAL:
                assertEquals("fromRemote mismatch",
                        false, exception.fromRemote());
                break;
            case NONE:
                break;
            default:
                throw new AssertionError();
            }
            if (!isCreator) {
                assertEquals("hasSideEffect mismatch",
                        true, exception.hasSideEffect());
            }
            output.flush();
            ReadResult result = readOutput();
            if (exception.fromRemote()
                || (exception.getCause() instanceof ConnectionException)) {
                result.checkNoDialogAbortMesg();
            } else {
                result.checkDialogAbortMesg(ERROR_DETAIL);
            }
            if (!isCreator) {
                assertEquals("responder should always have side effect",
                        true, exception.hasSideEffect());
            }
        }
        void checkCancelled() {
            assertTrue(context.isAborted() == true);
            assertTrue(context.getAbortThrowable()
                       instanceof DialogCancelledException);
        }
        @SuppressWarnings("unchecked")
        void checkOnAbort(Class<? extends Exception> cls,
                          Check hasSideEffect,
                          Check fromRemote) {
            try {
                checkOnAbortTimeout();
            } catch (AssertionError e1) {
                try {
                    checkOnAbortDialogException(
                            (Class<? extends DialogException>)cls,
                            hasSideEffect, fromRemote);
                } catch (AssertionError e2) {
                    throw new AssertionError(
                            String.format("Both check fails:\n%s\n%s",
                                LoggerUtils.getStackTrace(e1),
                                LoggerUtils.getStackTrace(e2)));
                }
            }
        }
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(super.toString());
            sb.append("context=" + context);
            sb.append("\noutput=" + output.toString());
            return sb.toString();
        }
        /**
         * Fake endpoint handler.
         */
        private class EndpointHandlerForTesting
                implements DialogEndpointHandler {
            @Override
            public NetworkAddress getRemoteAddress() {
                return new InetNetworkAddress("localhost", 0);
            }
            @Override
            public ScheduledExecutorService getSchedExecService() {
                return handlerExecutor;
            }
            @Override
            public long getUUID() {
                return 0;
            }
            @Override
            public long getConnID() {
                return 0;
            }
            @Override
            public String getStringID() {
                return "(0)";
            }
            @Override
            public ChannelDescription getChannelDescription() {
                return () -> "";
            }
            @Override
            public void startDialog(int dialogType,
                                    DialogHandler handler,
                                    long timeoutMillis) {
                return;
            }
            @Override
            public int getNumDialogsLimit() {
                return -1;
            }
            @Override
            public void shutdown(String detail, boolean force) {
                return;
            }
            @Override
            public Logger getLogger() {
                return logger;
            }
            @Override
            public ProtocolReader getProtocolReader() {
                return null;
            }
            @Override
            public ProtocolWriter getProtocolWriter() {
                return protocolWriter;
            }
            @Override
            public void assertInExecutorThread() {
                /* do nothing, we know we are in the right thread. */
            }
            @Override
            public void onContextDone(DialogContextImpl ctx) {
                logMesg(String.format("Context done called at %s ms, " +
                            " context=%s", now() / 1e6, context));
                isContextDone = true;
                if (context.isFin()
                    || (context.getAbortThrowable()
                        instanceof DialogCancelledException)) {
                    /*
                     * Release the semaphore if the context has finished or
                     * cancelled, otherwise, onAbort will release it.
                     */
                    semaphoreRelease();
                }
            }
            @Override
            public void onContextNewWrite(DialogContextImpl ctx) {
                logMesg(String.format("Context new write called at %s ms, " +
                            " context=%s", now() / 1e6, context));
                new FrameWriteTask().schedule();
            }

            @Override
            public int getMaxInputTotLen() {
                return MAX_INPUT_TOT_LEN;
            }
            @Override
            public int getMaxOutputTotLen() {
                return MAX_OUTPUT_TOT_LEN;
            }
            @Override
            public int getMaxInputProtocolMesgLen() {
                return -1;
            }
            @Override
            public int getMaxOutputProtocolMesgLen() {
                return MAX_OUT_PROTO_LEN;
            }
            @Override
            public synchronized long writeDialogStartForContext(
                    boolean finish,
                    boolean cont,
                    int typeno,
                    long timeoutMillis,
                    IOBufSliceList frame,
                    DialogContextImpl ctx) {
                protocolWriter.writeDialogStart(
                        false, finish, cont, typeno, 1, timeoutMillis, frame);
                return 1;
            }
            @Override
            public void flushOrTerminate() {
                logMesg(String.format(
                            "flush or terminate called at %s ms, context=%s",
                            now() / 1e6, context));
            }
        }

        /**
         * Fake dialog handler.
         */
        private class DialogHandlerForTesting implements DialogHandler {

            private final boolean faulty;
            private final RuntimeException faultException =
                new RuntimeException(ERROR_DETAIL);
            DialogHandlerForTesting(boolean faulty) {
                this.faulty = faulty;
            }
            @Override
            public void onStart(DialogContext ctx, boolean aborted) {
                numOnStart.incrementAndGet();
                if (aborted) {
                    return;
                }
                /* Write a small message to test writing in onStart. */
                MessageOutput mesg = new MessageOutput();
                mesg.writeInt(0);
                writtenBytes.add(new byte[0]);
                ctx.write(mesg, false);
                if (faulty) {
                    ctx.cancel(faultException);
                    throw faultException;
                }
            }
            @Override
            public void onCanWrite(DialogContext ctx) {
                assertEquals(
                        "onStart should be called once before onCanWrite.",
                        1, numOnStart.get());
                logMesg("onCanWrite called: reqWriteTasks.size=" +
                        reqWriteTasks.size());
                for (ReqWriteTask task : reqWriteTasks) {
                    task.schedule();
                }
                if (faulty) {
                    ctx.cancel(faultException);
                    throw faultException;
                }
            }
            @Override
            public void onCanRead(DialogContext ctx, boolean finished) {
                assertEquals(
                        "onStart should be called once before onCanRead.",
                        1, numOnStart.get());
                logMesg("onCanRead called: respReadTasks.size=" +
                        respReadTasks.size());
                for (RespReadTask task : respReadTasks) {
                    task.schedule();
                }
                if (faulty) {
                    ctx.cancel(faultException);
                    throw faultException;
                }
            }
            @Override
            public void onAbort(DialogContext ctx, Throwable cause) {
                assertEquals(
                        "onStart should be called once before onAbort.",
                        1, numOnStart.get());
                logMesg(String.format("onAbort called at %s: cause=%s",
                            now() / 1e6, cause.getMessage()));
                numOnAbort.incrementAndGet();
                abortThrowable = cause;
                if (context.isFin()) {
                    throw new AssertionError(
                            "Context is finished normally, " +
                            "but onAbort gets called.");
                }
                semaphoreRelease();
                if (faulty) {
                    ctx.cancel(faultException);
                    throw faultException;
                }
            }
        }

        /**
         * Task of calling DialogContext#onStart.
         */
        class InitTask extends Task {

            InitTask() {
                super(handlerExecutor);
            }
            @Override
            protected String doRun() throws Exception {
                long ts = System.nanoTime();
                context.callOnStart();
                long te = System.nanoTime();
                return String.format("duration=%s ns, context=%s",
                        (te - ts), context);
            }
            @Override
            protected boolean shouldRunAgain() {
                return false;
            }
        }

        /**
         * Task of writing request.
         */
        class ReqWriteTask extends Task {

            private final int nwrites;
            private final boolean last;
            private int count = 0;
            private boolean written;
            ReqWriteTask(int nwrites) {
                this(nwrites, false);
            }
            ReqWriteTask(int nwrites, boolean last) {
                super(handoffExecutor);
                this.nwrites = nwrites;
                this.last = last;
                reqWriteTasks.add(this);
            }
            @Override
            protected String doRun() throws Exception {
                if (count >= nwrites) {
                    return "Write task already finished.";
                }
                MessageOutput mesg =
                    new MessageOutput(new HeapIOBufferPool(MAX_OUTPUT_TOT_LEN));
                final int size = rand.nextInt(MAX_OUTPUT_TOT_LEN - 4);
                mesg.writeInt(size);
                byte[] bytes = new byte[size];
                rand.nextBytes(bytes);
                mesg.write(bytes);
                long ts = System.nanoTime();
                written = context.write(mesg, last);
                long te = System.nanoTime();
                if (written) {
                    writtenBytes.add(bytes);
                    count ++;
                }
                StringBuilder sb = new StringBuilder();
                sb.append(
                        String.format(
                            "bytes=%s size=%s, written=%s, last=%s, " +
                            "nwrites=%s, count=%s, duration=%s ns, " +
                            "context=%s. ",
                            BytesUtil.toString(bytes, 0, 4), size,
                            written, last, nwrites, count,
                            (te - ts), context));
                if (count == nwrites) {
                    reqWriteTasks.remove(this);
                    sb.append("Finished.");
                    if (reqWriteTasks.isEmpty()) {
                        Task task = scheduleLastWriteTask();
                        sb.append("Last written, scheduled ").append(task);
                    }
                }
                return sb.toString();
            }
            @Override
            protected boolean shouldRunAgain() {
                return written && (count < nwrites) && (!isContextDone);
            }
            protected Task scheduleLastWriteTask() {
                if (lastWriteTaskScheduled.compareAndSet(false, true)) {
                    Task task = new ReqWriteTask(1, true);
                    task.schedule();
                    return task;
                }
                return null;
            }
        }

        /**
         * Task of reading responses after onCanRead is called.
         */
        class RespReadTask extends Task {

            private final int nreads;
            private int count = 0;
            RespReadTask(int nreads) {
                super(handoffExecutor);
                this.nreads = nreads;
                respReadTasks.add(this);
            }
            @Override
            protected String doRun() throws Exception {
                StringBuilder sb = new StringBuilder();
                long d = 0;
                while (true) {
                    if (count == nreads) {
                        respReadTasks.remove(this);
                        break;
                    }
                    long ts = System.nanoTime();
                    MessageInput mesg = context.read();
                    long te = System.nanoTime();
                    d += (te - ts);
                    if (mesg == null) {
                        break;
                    }
                    count ++;
                    int size = mesg.readInt();
                    byte[] bytes = new byte[size];
                    mesg.readFully(bytes);
                    readBytes.add(bytes);
                    sb.append(String.format("(%s), ",
                                BytesUtil.toString(bytes, 0, 4)));
                }
                sb.append(String.format("nreads=%s, count=%s, " +
                            "duration=%s ns, context=%s",
                            nreads, count, d, context));
                return sb.toString();
            }
            @Override
            protected boolean shouldRunAgain() {
                return false;
            }
        }

        /**
         * Task of writing frames.
         */
        class FrameWriteTask extends Task {

            private volatile boolean allSent = false;
            FrameWriteTask() {
                super(handlerExecutor);
            }
            @Override
            protected String doRun() throws Exception {
                long ts = System.nanoTime();
                allSent = context.onWriteDialogFrame();
                long te = System.nanoTime();
                return String.format("allSent=%s, duration=%s ns, context=%s",
                        allSent, (te - ts), context);
            }
            @Override
            protected boolean shouldRunAgain() {
                return !allSent;
            }
        }

        /**
         * Task of reading frames.
         */
        class FrameReadTask extends Task {

            private final LinkedList<LinkedList<byte[]>> messages;
            FrameReadTask(int numToRecv) {
                super(handlerExecutor);
                this.messages = getMessages(numToRecv);
            }
            @Override
            protected String doRun() throws Exception {
                if (messages.size() == 0) {
                    return "";
                }
                LinkedList<byte[]> mesg = messages.peek();
                if (mesg.size() == 0) {
                    messages.poll();
                    return "";
                }
                byte[] bytes = mesg.poll();
                CopyingBytesInput frame = new CopyingBytesInput(bytes);
                boolean cont = mesg.size() != 0;
                boolean finish = (!cont) && (messages.size() == 1);
                long ts = System.nanoTime();
                context.onReadDialogFrame(finish, cont, frame);
                long te = System.nanoTime();
                if (mesg.size() == 0) {
                    messages.poll();
                }
                return String.format("cont=%s, finish=%s, " +
                        "start=%s, bytes.length=%s, messages.size=%s, " +
                        "duration=%s ns, context=%s",
                        cont, finish, bytes[0], bytes.length,
                        messages.size(), (te - ts), context);
            }
            @Override
            protected boolean shouldRunAgain() {
                return (messages.size() != 0) && (!isContextDone);
            }
            private LinkedList<LinkedList<byte[]>> getMessages(int num) {
                LinkedList<LinkedList<byte[]>> result =
                    new LinkedList<LinkedList<byte[]>>();
                for (int i = 0; i < num; ++i) {
                    LinkedList<byte[]> mesg = new LinkedList<byte[]>();
                    result.add(mesg);
                    int size = rand.nextInt(MAX_INPUT_TOT_LEN - 4);
                    byte[] sizeBytes = new byte[4];
                    ByteBuffer.wrap(sizeBytes).putInt(size);
                    mesg.add(sizeBytes);
                    byte[] data = new byte[size];
                    rand.nextBytes(data);
                    recvBytes.add(data);
                    int frameSize = MAX_IN_PROTO_LEN;
                    int nframes = size / frameSize +
                        ((size % frameSize == 0) ? 0 : 1);
                    int lastSize = size - (nframes - 1) * frameSize;
                    int offset = 0;
                    for (int f = 0; f < nframes; ++f) {
                        int fs = (f < nframes - 1) ? frameSize : lastSize;
                        byte[] bytes = new byte[fs];
                        System.arraycopy(data, offset, bytes, 0, fs);
                        mesg.add(bytes);
                        offset += fs;
                    }
                }
                return result;
            }
        }

        /**
         * Task of reading a dialog abort.
         */
        private class AbortReadTask extends Task {
            private final ProtocolMesg.DialogAbort.Cause cause;
            AbortReadTask(ProtocolMesg.DialogAbort.Cause cause) {
                super(handlerExecutor);
                this.cause = cause;
            }
            @Override
            protected String doRun() throws Exception {
                long ts = System.nanoTime();
                context.onReadDialogAbort(cause, ERROR_DETAIL);
                long te = System.nanoTime();
                return String.format("duration=%s ns, context=%s",
                        (te - ts), context);
            }
            @Override
            protected boolean shouldRunAgain() {
                return rand.nextBoolean();
            }
        }

        /**
         * Task of aborting the dialog due to connection exception.
         */
        private class LocalConnectionAbortTask extends Task {
            private final boolean fromRemote;
            LocalConnectionAbortTask(boolean fromRemote) {
                super(handoffExecutor);
                this.fromRemote = fromRemote;
            }
            @Override
            protected String doRun() throws Exception {
                final long ts = System.nanoTime();
                final ConnectionException exception =
                    fromRemote ?
                    new ConnectionUnknownException(true, null, ERROR_DETAIL) :
                    new ConnectionUnknownException(
                        true, null, new RuntimeException(ERROR_DETAIL));
                context.onLocalAbortConnectionException(exception);
                final long te = System.nanoTime();
                return String.format("duration=%s ns, context=%s",
                        (te - ts), context);
            }
            @Override
            protected boolean shouldRunAgain() {
                return rand.nextBoolean();
            }
        }

        /**
         * Task of aborting the context at the time of writing last.
         */
        class ReqWriteAbortTask extends ReqWriteTask {

            ReqWriteAbortTask(int nwrites) {
                super(nwrites);
            }
            @Override
            protected Task scheduleLastWriteTask() {
                Task task = new AbortReadTask(
                        ProtocolMesg.DialogAbort.Cause.UNKNOWN_REASON);
                task.schedule();
                return task;
            }
        }

        private void assertBytesCollectionEquals(
                Collection<byte[]> expected, Collection<byte[]> actual) {

            Iterator<byte[]> iterE = expected.iterator();
            while (iterE.hasNext()) {
                byte[] bytesE = iterE.next();
                boolean found = false;
                Iterator<byte[]> iterA = actual.iterator();
                while (iterA.hasNext()) {
                    byte[] bytesA = iterA.next();
                    if (Arrays.equals(bytesE, bytesA)) {
                        found = true;
                        iterA.remove();
                        iterE.remove();
                        break;
                    }
                }
                if (!found) {
                    throw new AssertionError(String.format(
                                "Bytes not found:",
                                BytesUtil.toString(bytesE, 0, 4)));
                }
            }
            if (expected.size() != 0) {
                throw new AssertionError();
            }
            StringBuilder sb = new StringBuilder();
            for (byte[] bytes : actual) {
                sb.append(BytesUtil.toString(bytes, 0, 4));
            }
            assertEquals("Some extra items in actual: " + sb.toString(),
                    0, actual.size());
        }

        private ReadResult readOutput() {
            ByteArrayChannelInput input =
                new ByteArrayChannelInput(outputArray);
            ProtocolReader reader =
                new ProtocolReader(input, MAX_OUT_PROTO_LEN);
            input.limit(output.position());
            ReadResult result = new ReadResult();
            while (true) {
                ProtocolMesg mesg = reader.read((id) -> true);
                if (mesg == null) {
                    break;
                }
                switch(mesg.type()) {
                case ProtocolMesg.DIALOG_START_MESG:
                    result.onReadDialogStart(
                            (ProtocolMesg.DialogStart) mesg);
                    break;
                case ProtocolMesg.DIALOG_FRAME_MESG:
                    result.onReadDialogFrame(
                            (ProtocolMesg.DialogFrame) mesg);
                    break;
                case ProtocolMesg.DIALOG_ABORT_MESG:
                    result.onReadDialogAbort(
                            (ProtocolMesg.DialogAbort) mesg);
                    break;
                default:
                    throw new AssertionError("Got a non-dialog message.");
                }
            }
            return result;
        }

        private class ReadResult  {

            private boolean gotStart = !isCreator;
            private boolean gotAbort = false;
            private boolean gotFinish = false;
            private List<MessageInput> inputs =
                new ArrayList<MessageInput>();
            private MessageInput curr = null;
            private String abortDetail = null;
            private void onReadDialogStart(ProtocolMesg.DialogStart mesg) {
                assertEquals("Received more than one DialogStart",
                        false, gotStart);
                assertEquals("Received DialogStart after DialogAbort",
                        false, gotAbort);
                gotStart = true;
                assertEquals("Wrong typeno", TYPENO, mesg.typeno);
                assertEquals("Wrong dialogId", 1, mesg.dialogId);
                curr = new MessageInput();
                curr.add(mesg.frame);
                if (!mesg.cont) {
                    inputs.add(curr);
                    curr = null;
                }
                logMesg("Reading " + mesg);
            }
            private void onReadDialogFrame(ProtocolMesg.DialogFrame mesg) {
                assertEquals("Received DialogFrame before DialogStart",
                        true, gotStart);
                assertEquals("Received DialogFrame after DialogAbort",
                        false, gotAbort);
                assertEquals("Received DialogFrame after finish",
                        false, gotFinish);
                assertEquals("Wrong dialogId", 1, mesg.dialogId);
                if (curr == null) {
                    curr = new MessageInput();
                }
                curr.add(mesg.frame);
                if (!mesg.cont) {
                    inputs.add(curr);
                    curr = null;
                }
                if (mesg.finish) {
                    gotFinish = true;
                }
                logMesg("Reading " + mesg);
            }
            private void onReadDialogAbort(ProtocolMesg.DialogAbort mesg) {
                assertEquals("Received DialogAbort before DialogStart",
                        true, gotStart);
                assertEquals("Received more than one DialogAbort",
                        false, gotAbort);
                assertEquals("Wrong dialogId", 1, mesg.dialogId);
                gotAbort = true;
                abortDetail = mesg.detail;
                logMesg("Reading " + mesg);
            }
            private void checkDialogBytes(Collection<byte[]> expected) {
                List<byte[]> actual = new ArrayList<byte[]>();
                for (MessageInput input : inputs) {
                    String inputString = input.toString();
                    try {
                        int size = input.readInt();
                        byte[] bytes = new byte[size];
                        input.readFully(bytes);
                        actual.add(bytes);
                    } catch (Throwable t) {
                        throw new AssertionError(
                                "Got error with input: " + inputString, t);
                    }
                }
                assertBytesCollectionEquals(expected, actual);
            }
            private void checkNoDialogAbortMesg() {
                assertEquals("DialogAbort should not be sent.",
                        false, gotAbort);
            }
            private void checkDialogAbortMesg(String key) {
                if (!gotStart) {
                    /* no dialog start, so there should be no dialog abort. */
                    assertTrue("No DialogStart, but has DialogAbort",
                            !gotAbort);
                    return;
                }
                assertTrue(String.format(
                            "Error detail '%s' does not contain key '%s'",
                            abortDetail, key),
                        abortDetail.contains(key));
            }
        }
    }
}
