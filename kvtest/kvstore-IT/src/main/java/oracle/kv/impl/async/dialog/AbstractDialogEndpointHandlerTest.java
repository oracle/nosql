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
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import oracle.kv.TestBase;
import oracle.kv.impl.async.AsyncOption;
import oracle.kv.impl.async.BytesUtil;
import oracle.kv.impl.async.DialogContext;
import oracle.kv.impl.async.DialogHandler;
import oracle.kv.impl.async.DialogHandlerFactory;
import oracle.kv.impl.async.DialogHandlerFactoryMap;
import oracle.kv.impl.async.DialogResourceManager;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.EndpointConfigBuilder;
import oracle.kv.impl.async.EndpointHandler;
import oracle.kv.impl.async.EndpointHandlerManager;
import oracle.kv.impl.async.IOBufSliceList;
import oracle.kv.impl.async.ListenerConfig;
import oracle.kv.impl.async.InetNetworkAddress;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.dialog.exception.ProtocolViolationException;
import oracle.kv.impl.async.exception.ConnectionException;
import oracle.kv.impl.async.exception.ConnectionIdleException;
import oracle.kv.impl.async.exception.ConnectionTimeoutException;
import oracle.kv.impl.async.exception.PersistentDialogException;
import oracle.kv.impl.async.exception.DialogLimitExceededException;
import oracle.kv.impl.async.perf.DialogEndpointGroupPerfTracker;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;


public class AbstractDialogEndpointHandlerTest extends TestBase {

    private static final int ARRAY_SIZE = 128 * 1024;
    private static final String CREATOR = "Creator";
    private static final String RESPONDER = "Responder";
    private static final byte CR_BYTE = (byte) 0x67;
    private static final byte RE_BYTE = (byte) 0x82;
    private static final byte REQ_BYTE = (byte) 0x71;
    private static final byte RESP_BYTE = (byte) 0x70;
    private static final int DEFAULT_MAX_PROTO_LEN = 32;
    private static final int DEFAULT_TIMEOUT = 5000;
    private static final int LIMITED_MAX_DIALOGS = 16;

    private enum TaskFail {
        YES, NO, MAYBE
    }

    private ScheduledExecutorService executorST1;
    private ScheduledExecutorService executorST2;
    private ScheduledExecutorService executorMT;

    @Override
    public void setUp() throws Exception {
        executorST1 = new ScheduledThreadPoolExecutor(1);
        executorST2 = new ScheduledThreadPoolExecutor(1);
        executorMT = new ScheduledThreadPoolExecutor(16);
        Logged.resetT0();
    }

    @Override
    public void tearDown() throws Exception {
        executorST1.shutdownNow();
        executorST2.shutdownNow();
        executorMT.shutdownNow();
    }

    /**
     * Tests starting dialogs before handshake is done.
     */
    @Test(timeout=2000)
    public void testDialogsBeforeHandshake() {
        final int numDialogs = 16;
        HandlerTester tester = new HandlerTester
            (executorST1, executorST2, executorMT);
        try {
            tester.setupDialogHandlerFactories(numDialogs);
            tester.startDialogTasks(numDialogs, 1, TaskFail.NO);
            tester.setupConnectionTasks();
            tester.await(1000);
            tester.checkDialogTasks();
        } catch (Throwable t) {
            fail(LoggerUtils.getStackTrace(t) + "\n" +
                 tester.toString());
        }
    }

    /**
     * Tests starting dialogs after graceful shutdown.
     */
    @Test(timeout=2000)
    public void testDialogsAfterGracefulShutdown() {

        final int numDialogs = 16;
        HandlerTester tester = new HandlerTester
            (executorST1, executorST2, executorMT);
        try {
            tester.setupDialogHandlerFactories(numDialogs);
            tester.setupConnectionTasks();
            tester.getHandler1().startDialogTasks(
                    numDialogs, 1, TaskFail.NO);
            tester.getHandler1().shutdown("", false);
            tester.getHandler1().startDialogTasks(
                    numDialogs, 1, TaskFail.YES);
            tester.await(1000);
            tester.checkDialogTasks(PersistentDialogException.class);
        } catch (Throwable t) {
            fail(LoggerUtils.getStackTrace(t) + "\n" +
                 tester.toString());
        }
    }

    /**
     * Tests the behavior of error when remote starts too many dialogs.
     */
    @Test(timeout=2000)
    public void testDialogsLimitException() {

        EndpointConfig cfg = getRemoteDialogsLimitedEndpointConfig();
        HandlerTester tester = new HandlerTester
            (executorST1, executorST2, executorMT, cfg, cfg);
        try {
            tester.setupDialogHandlerFactories(LIMITED_MAX_DIALOGS);
            tester.setupConnectionTasks();
            tester.getHandler1().awaitHandshakeDone();
            for (int i = 0; i < LIMITED_MAX_DIALOGS + 1; ++i) {
                long dialogId = -i - 1;
                tester.getHandler2().getProtocolWriter().writeDialogStart(
                        false, false, true, 1, dialogId, 5000,
                        new IOBufSliceList());
            }
            tester.getHandler2().flush();
            tester.getHandler1().awaitTermination(1000);
            tester.getHandler1().checkLocalProtocolViolation();
        } catch (Throwable t) {
            fail(LoggerUtils.getStackTrace(t) + "\n" +
                 tester.toString());
        }
    }

    /**
     * Tests that local controls the number of dialogs to start.
     *
     * This is a indirect test in that the test depends on the correctness of
     * testDialogsLimitException. If the number exceeds the limit, the test
     * should fail as we have tested with testDialogsLimitException.
     */
    @Test(timeout=2000)
    public void testDialogsLimit() {

        EndpointConfig cfg = getRemoteDialogsLimitedEndpointConfig();
        HandlerTester tester = new HandlerTester
            (executorST1, executorST2, executorMT, cfg, cfg);
        try {
            tester.setupDialogHandlerFactories(4 * LIMITED_MAX_DIALOGS);
            tester.setupConnectionTasks();
            tester.startDialogTasks(
                    4 * LIMITED_MAX_DIALOGS, 1, TaskFail.MAYBE);
            tester.await(1000);
            tester.checkDialogTasks(DialogLimitExceededException.class);
        } catch (Throwable t) {
            fail(LoggerUtils.getStackTrace(t) + "\n" +
                 tester.toString());
        }
    }

    /**
     * Tests normal dialog operations with a single thread.
     */
    @Test(timeout=20000)
    public void testDialogsRandomST() {
        testDialogsRandom(executorST1, executorST1, executorST1, 64, 16, 4);
    }

    /**
     * Tests normal dialog operations with multiple threads.
     */
    @Test(timeout=20000)
    public void testDialogsRandomMT() {
        testDialogsRandom(executorST1, executorST2, executorMT, 64, 16, 4);
    }

    private void testDialogsRandom(ScheduledExecutorService handler1Executor,
                                   ScheduledExecutorService handler2Executor,
                                   ScheduledExecutorService handoffExecutor,
                                   int numIter,
                                   int numDialogTypes,
                                   int numDialogsPerType) {
        for (int i = 0; i < numIter; ++i) {
            HandlerTester tester = new HandlerTester
                (handler1Executor, handler2Executor, handoffExecutor);
            try {
                tester.setupDialogHandlerFactories(numDialogTypes);
                tester.setupConnectionTasks();
                tester.startDialogTasks(
                        numDialogTypes, numDialogsPerType, TaskFail.NO);
                tester.await(1000);
                tester.checkDialogTasks();
            } catch (Throwable t) {
                fail(LoggerUtils.getStackTrace(t) + "\n" +
                     tester.toString());
            }
        }
    }

    /**
     * Tests the behavior that a large dialog does not block small ones.
     */
    @Test(timeout=2000)
    public void testDialogsNoStarve() {
        int dialogType = 32;
        HandlerTester tester = new HandlerTester
            (executorST1, executorST2, executorMT);
        try {
            tester.setupDialogHandlerFactories(new int[] { 0, dialogType });
            tester.setupConnectionTasks();
            tester.getHandler1().awaitHandshakeDone();
            tester.getHandler1().startDialogTask(dialogType, 1, TaskFail.NO);
            for (int i = 0; i < 16; ++i) {
                tester.getHandler1().startDialogTask(0, 1, TaskFail.NO);
            }
            tester.await(1000);
            tester.checkDialogTasks();
            assertEquals(
                    "The largest dialog should not be the first to finish.",
                    0, tester.getFirstDoneTask().getDialogType());
        } catch (Throwable t) {
            fail(LoggerUtils.getStackTrace(t) + "\n" +
                 tester.toString());
        }
    }

    /**
     * Tests the behavior of error when receiving invalid dialogId of
     * DialogStart.
     */
    @Test(timeout=10000)
    public void testDialogStartIdException() {
        testDialogStartIdException(true, new int[] { 1 });
        testDialogStartIdException(false, new int[] { -1 });
        testDialogStartIdException(true, new int[] { -1, -3, -2 });
        testDialogStartIdException(false, new int[] { 1, 3, 2 });
    }

    private void testDialogStartIdException(boolean test1, int[] dialogIds) {

        HandlerTester tester = new HandlerTester
            (executorST1, executorST2, executorMT);
        HandlerTester.TestingHandler testingHandler =
            test1 ? tester.getHandler1() : tester.getHandler2();
        HandlerTester.TestingHandler writingHandler =
            test1 ? tester.getHandler2() : tester.getHandler1();
        try {
            tester.setupDialogHandlerFactories(16);
            tester.setupConnectionTasks();
            tester.getHandler1().awaitHandshakeDone();
            for (int dialogId : dialogIds) {
                writingHandler.getProtocolWriter().writeDialogStart(
                        false, true, false, 1, dialogId, 5000,
                        new IOBufSliceList());
            }
            writingHandler.flush();
            testingHandler.awaitTermination(1000);
            testingHandler.checkLocalProtocolViolation();
        } catch (Throwable t) {
            fail(LoggerUtils.getStackTrace(t) + "\n" +
                 tester.toString());
        }
    }

    /**
     * Tests the behavior of error when receiving invalid dialogId of
     * DialogFrame or DialogAbort.
     */
    @Test(timeout=10000)
    public void testDialogFrameOrAbortIdException() {
        testDialogFrameOrAbortIdException(
                true, true, new int[] { -1, -2, -3 }, -5);
        testDialogFrameOrAbortIdException(
                true, false, new int[] { -1, -2, -3 }, -5);
        testDialogFrameOrAbortIdException(
                false, true, new int[] { 1, 2, 3 }, 5);
        testDialogFrameOrAbortIdException(
                false, false, new int[] { 1, 2, 3 }, 5);
    }

    private void testDialogFrameOrAbortIdException(boolean test1,
                                                   boolean testFrame,
                                                   int[] dialogStartIds,
                                                   int dialogId) {

        HandlerTester tester = new HandlerTester
            (executorST1, executorST2, executorMT);
        HandlerTester.TestingHandler testingHandler =
            test1 ? tester.getHandler1() : tester.getHandler2();
        HandlerTester.TestingHandler writingHandler =
            test1 ? tester.getHandler2() : tester.getHandler1();
        try {
            tester.setupDialogHandlerFactories(16);
            tester.setupConnectionTasks();
            tester.getHandler1().awaitHandshakeDone();
            for (int dialogStartId : dialogStartIds) {
                writingHandler.getProtocolWriter().writeDialogStart(
                        false, true, false, 1, dialogStartId, 5000,
                        new IOBufSliceList());
            }
            if (testFrame) {
                writingHandler.getProtocolWriter().writeDialogFrame(
                        true, false, dialogId,
                        new IOBufSliceList());
            } else {
                writingHandler.getProtocolWriter().writeDialogAbort(
                        ProtocolMesg.DialogAbort.Cause.UNKNOWN_REASON,
                        dialogId,
                        "");
            }
            writingHandler.flush();
            testingHandler.awaitTermination(1000);
            testingHandler.checkLocalProtocolViolation();
        } catch (Throwable t) {
            fail(LoggerUtils.getStackTrace(t) + "\n" +
                 tester.toString());
        }
    }

    /**
     * Tests the behavior of various timeout.
     */
    @Test(timeout=2000)
    public void testConnectTimeoutException() {
        testTimeoutException(
                AsyncOption.DLG_CONNECT_TIMEOUT, "Connect");
    }

    @Test(timeout=3000)
    public void testHeartbeatTimeoutException() {
        testTimeoutException(
                AsyncOption.DLG_HEARTBEAT_TIMEOUT, "Heartbeat");
    }

    @Test(timeout=2000)
    public void testIdleTimeoutException() {
        testTimeoutException(
                AsyncOption.DLG_IDLE_TIMEOUT, "Idle");
    }

    private void testTimeoutException(AsyncOption<Integer> timeoutOption,
                                      String key) {
        EndpointConfig cfg1 =
            getShortTimeoutEndpointConfig(timeoutOption);
        EndpointConfig cfg2 = getDefaultEndpointConfig();
        HandlerTester tester = new HandlerTester
            (executorST1, executorST2, executorMT, cfg1, cfg2);
        try {
            if (timeoutOption != AsyncOption.DLG_CONNECT_TIMEOUT) {
                tester.setupConnectionTasks();
                tester.getHandler1().awaitHandshakeDone();
            }
            tester.getHandler2().disableFlush();
            tester.getHandler1().awaitTermination(1000);
            tester.getHandler1().checkLocalTimeout(key);
        } catch (Throwable t) {
            fail(LoggerUtils.getStackTrace(t) + "\n" +
                 tester.toString());
        }
    }

    /**
     * Tests the handler sends heartbeat properly.
     */
    @Test(timeout=5000)
    public void testHeartbeat() {

        HandlerTester tester = new HandlerTester
            (executorST1, executorST2, executorMT);
        try {
            tester.setupConnectionTasks();
            Thread.sleep(2000);
            tester.getHandler1().checkNormal();
            tester.getHandler2().checkNormal();
        } catch (Throwable t) {
            fail(LoggerUtils.getStackTrace(t) + "\n" +
                 tester.toString());
        }
    }

    /**
     * Tests that flushes are not delayed due to race condition.
     *
     * Starts a batch of dialogs at the same time so that if there is a race
     * condition, one of the dialog messages will not be flushed and the dialog
     * wll not be finished. Do several iterations so that the race could happen
     * with high probablity during one test.
     *
     * Disable heartbeats so that if race happened and flush got delayed, a
     * dialog message will delay being flushed indefintely.
     *
     * Disable randomIO so that dialogs can surely be finished before the
     * expected short timeout.
     */
    @Test(timeout=20000)
    public void testFlushNoDelay() {
        final int numIter = 128;
        final int dialogType = 4;
        final int ndialogs = 4;
        EndpointConfig cfg1 = getNoHeartbeatConfig();
        EndpointConfig cfg2 = getNoHeartbeatConfig();
        HandlerTester tester =
            new HandlerTester(executorST1, executorST2, executorMT,
                              cfg1, cfg2);
        tester.getHandler1().disableRandomIO();
        tester.getHandler2().disableRandomIO();
        try {
            tester.setupDialogHandlerFactories(dialogType + 1);
            tester.setupConnectionTasks();
            for (int i = 0; i < numIter; ++i) {
                for (int j = 0; j < ndialogs; ++j) {
                    tester.getHandler1().
                        startDialogTask(
                            dialogType, 1, TaskFail.NO, 300);
                }
                tester.await(1000);
                tester.checkDialogTasks();
            }
        } catch (Throwable t) {
            fail(LoggerUtils.getStackTrace(t) + "\n" +
                 tester.toString());
        }
    }

    /**
     * Tests that if we close the handler before handshake, the dialogs are
     * aborted correctly.
     */
    @Test
    public void testStartDialogRaceAfterShutdown() throws Exception {
        final EndpointConfig cfg1 = getDefaultEndpointConfig();
        final EndpointConfig cfg2 = getDefaultEndpointConfig();
        final HandlerTester tester =
            new HandlerTester(executorST1, executorST1, executorST1,
                              cfg1, cfg2);
        final AtomicInteger naborts = new AtomicInteger(0);
        final AtomicReference<Throwable> abortCause =
            new AtomicReference<>(null);
        final DialogHandler dialogHandler = new DialogHandler() {
            @Override
            public void onStart(DialogContext context, boolean aborted) {
            }
            @Override
            public void onCanWrite(DialogContext context) {
            }
            @Override
            public void onCanRead(DialogContext context, boolean finished) {
            }
            @Override
            public void onAbort(DialogContext context, Throwable cause) {
                naborts.incrementAndGet();
                abortCause.set(cause);
            }
        };
        final HandlerTester.TestingHandler handler = tester.getHandler1();
        DialogContextImpl.createHook = (c) -> {
            handler.shutdown("Forceful shutdown", true);
        };
        handler.startDialog(0, dialogHandler, 5000);
        /* Sleep a bit so that the scheduled onAbort could be called. */
        Thread.sleep(500);
        assertEquals("incorrect number of onAbort called",
                     1, naborts.get());
        assertEquals("incorrect dialog abort message",
                     "Problem with channel (in-memory channel): "
                     + "Shut down with force, detail=[Forceful shutdown]",
                     abortCause.get().getMessage());
    }

    /**
     * Tests the bug that a dialog submitted before connection handshake is done
     * cannot prevent idle timeout.
     *
     * [KVSTORE-2214]
     */
    @Test
    public void testNoIdleTimeoutWithDialogActive() {
        final int dialogTimeout = 1000;
        final EndpointConfig config =
            setDefaultOptions(new EndpointConfigBuilder())
                .option(AsyncOption.DLG_IDLE_TIMEOUT, dialogTimeout)
                .option(AsyncOption.DLG_HEARTBEAT_TIMEOUT, Integer.MAX_VALUE)
                .build();
        final HandlerTester tester = new HandlerTester
            (executorST1, executorST2, executorMT, config, config);
        final AtomicBoolean proceed = new AtomicBoolean(false);
        class WaitingFactory implements DialogHandlerFactory {
            @Override
            public DialogHandler create() {
                while (!proceed.get()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        throw new RuntimeException();
                    }
                }
                final byte[] request = getBytes(CR_BYTE, REQ_BYTE, 0);
                final byte[] response = getBytes(CR_BYTE, RESP_BYTE, 0);
                return new Responder(request, response, tester);
            }
        }
        try {
            tester.addDialogFactory(0, new WaitingFactory());
            tester.getHandler1().startDialogTask(0, 1, TaskFail.NO);
            /* Sleep a bit so that dialogs are initialized, but not started. */
            Thread.sleep(100);
            /*
             * Set up the connection tasks so that handshake can be done and
             * idle timeout task can start.
             */
            tester.setupConnectionTasks();
            /*
             * Sleep beyond idle timeout. The idle timeout should not be
             * triggered since there is one active dialog.
             */
            Thread.sleep(2 * dialogTimeout);
            /* Let the dialog continue, it should finish without exception. */
            proceed.set(true);
            tester.await(1000);
            tester.checkDialogTasks();
        } catch (Throwable t) {
            fail(LoggerUtils.getStackTrace(t) + "\n" +
                 tester.toString());
        }
    }

    /**
     * Tests that a dialog does not call onAbort twice with connection handshake
     * timeout. The dialog will be aborted twice, one through its own timeout
     * and the other through connection timeout aborting all dialogs.
     *
     * [KVSTORE-2228]
     */
    @Test
    public void testDialogAbortWithConnectionTimeout() throws Exception {
        final int sleepTimeMillis = 1000;
        final EndpointConfig cfg1 =
            setDefaultOptions(new EndpointConfigBuilder())
            .option(AsyncOption.DLG_CONNECT_TIMEOUT, sleepTimeMillis / 2).build();
        final EndpointConfig cfg2 = getDefaultEndpointConfig();
        final HandlerTester tester =
            new HandlerTester(executorST1, executorST1, executorST1,
                              cfg1, cfg2);
        final AtomicInteger naborts = new AtomicInteger(0);
        final DialogHandler dialogHandler = new DialogHandler() {
            @Override
            public void onStart(DialogContext context, boolean aborted) {
            }
            @Override
            public void onCanWrite(DialogContext context) {
            }
            @Override
            public void onCanRead(DialogContext context, boolean finished) {
            }
            @Override
            public void onAbort(DialogContext context, Throwable cause) {
                naborts.incrementAndGet();
            }
        };
        final HandlerTester.TestingHandler handler = tester.getHandler1();
        handler.startDialog(0, dialogHandler, sleepTimeMillis / 3);
        Thread.sleep(sleepTimeMillis);
        assertEquals("incorrect number of onAbort called",
                     1, naborts.get());
    }

    /**
     * Helper class for testing.
     */
    private class HandlerTester extends Tester {

        /* Executor for handoff tasks. */
        private final ScheduledExecutorService handoffExecutor;
        /* Byte array for simulating connection from 1 to 2. */
        private final byte[] byteArray1to2 = new byte[ARRAY_SIZE];
        /* Byte array for simulating connection from 2 to 1. */
        private final byte[] byteArray2to1 = new byte[ARRAY_SIZE];
        /* Creator dialog endpoint handler. */
        private final DialogHandlerFactoryMap
            dialogHandlerFactories1 = new DialogHandlerFactoryMap();
        private final TestingHandler testingHandler1;
        /* Responder dialog endpoint handler and its components */
        private final DialogHandlerFactoryMap
            dialogHandlerFactories2 = new DialogHandlerFactoryMap();
        private final TestingHandler testingHandler2;
        /* Dialog tasks */
        private final Set<DialogTask> dialogTasks;
        HandlerTester(ScheduledExecutorService handler1Executor,
                      ScheduledExecutorService handler2Executor,
                      ScheduledExecutorService handoffExecutor) {
            this(handler1Executor, handler2Executor, handoffExecutor,
                    getDefaultEndpointConfig(),
                    getDefaultEndpointConfig());
        }
        HandlerTester(ScheduledExecutorService handler1Executor,
                      ScheduledExecutorService handler2Executor,
                      ScheduledExecutorService handoffExecutor,
                      EndpointConfig endpointConfig1,
                      EndpointConfig endpointConfig2) {
            super(true);
            this.handoffExecutor = handoffExecutor;
            this.testingHandler1 = new TestingHandler(
                    endpointConfig1, true, byteArray2to1, byteArray1to2,
                    dialogHandlerFactories1, handler1Executor);
            this.testingHandler2 = new TestingHandler(
                    endpointConfig2, false, byteArray1to2, byteArray2to1,
                    dialogHandlerFactories2, handler2Executor);
            this.dialogTasks = Collections.newSetFromMap(
                    new ConcurrentHashMap<DialogTask, Boolean>());
        }
        void setupDialogHandlerFactories(int numDialogTypes) {
            for (int dtype = 0; dtype < numDialogTypes; ++ dtype) {
                addDialogTypeToFactory(dtype);
            }
        }
        void setupDialogHandlerFactories(int[] dialogTypes) {
            for (int dtype : dialogTypes) {
                addDialogTypeToFactory(dtype);
            }
        }
        void addDialogTypeToFactory(int dtype) {
            /* Dialogs from responder to creator */
            dialogHandlerFactories1.put(
                    dtype, new TestingDialogFactory(RE_BYTE, dtype));
            /* Dialogs from creator to responder */
            dialogHandlerFactories2.put(
                    dtype, new TestingDialogFactory(CR_BYTE, dtype));
        }
        void addDialogFactory(int dtype, DialogHandlerFactory factory) {
            dialogHandlerFactories1.put(dtype, factory);
            dialogHandlerFactories2.put(dtype, factory);
        }
        void setupConnectionTasks() {
            /* Get the responder side ready first. */
            (new ConnectionTask(testingHandler2)).scheduleOnce();
            (new ConnectionTask(testingHandler1)).scheduleOnce();
        }
        TestingHandler getHandler1() {
            return testingHandler1;
        }
        TestingHandler getHandler2() {
            return testingHandler2;
        }
        void startDialogTasks(int numDialogTypes,
                              int numDialogsPerType,
                              TaskFail taskFail) {
            testingHandler1.startDialogTasks(
                    numDialogTypes, numDialogsPerType, taskFail);
            testingHandler2.startDialogTasks(
                    numDialogTypes, numDialogsPerType, taskFail);
        }
        void checkDialogTasks() {
            for (DialogTask task : dialogTasks) {
                task.check();
            }
        }
        void checkDialogTasks(Class<?> cls) {
            for (DialogTask task : dialogTasks) {
                task.check(cls);
            }
        }
        DialogTask getFirstDoneTask() {
            long tf = Long.MAX_VALUE;
            DialogTask first = null;
            for (DialogTask task : dialogTasks) {
                if (task.doneTime() < tf) {
                    tf = task.doneTime();
                    first = task;
                }
            }
            return first;
        }

        private class TestingHandler extends AbstractDialogEndpointHandler {

            private final ScheduledExecutorService executor;
            private final byte[] inArray;
            private final ByteArrayChannelInput chInput;
            private final byte[] outArray;
            private final ByteArrayChannelOutput chOutput;
            private final ProtocolReader protocolReader;
            private final ProtocolWriter protocolWriter;
            private volatile boolean flushDisabled = false;
            private volatile boolean randomIODisabled = false;
            TestingHandler(EndpointConfig endpointConfig,
                           boolean isCreator,
                           byte[] inArray,
                           byte[] outArray,
                           DialogHandlerFactoryMap
                           dialogHandlerFactories,
                           ScheduledExecutorService executor) {
                super(logger, new TestingManager(), endpointConfig, isCreator,
                      "Testing", createAddress(isCreator),
                      dialogHandlerFactories,
                      new DialogResourceManager(Integer.MAX_VALUE));
                this.inArray = inArray;
                this.chInput = new ByteArrayChannelInput(inArray);
                this.outArray = outArray;
                this.chOutput = new ByteArrayChannelOutput(outArray);
                this.protocolReader = new ProtocolReader(
                        chInput, getMaxInputProtocolMesgLen());
                this.protocolWriter = new ProtocolWriter(
                        chOutput, getMaxOutputProtocolMesgLen());
                this.executor = executor;
                onExecutorReady();
            }
            void disableFlush() {
                flushDisabled = true;
            }
            void disableRandomIO() {
                randomIODisabled = true;
            }
            @Override
            public ChannelDescription getChannelDescription() {
                return () -> "in-memory channel";
            }
            @Override
            protected void setReadInterest(boolean interest) {
            }
            @Override
            protected boolean flushInternal(boolean writeHasRemaining) {
                if (flushDisabled) {
                    return true;
                }
                final int prev = chOutput.position();
                chOutput.flush();
                final int curr = chOutput.position();
                if (curr - prev > 0) {
                    logMesg(String.format(
                                "Flushed data, writeHasRemaining=%s, " +
                                "handler=%s\n\tout=%s%s",
                                writeHasRemaining, this,
                                BytesUtil.toString(
                                    outArray, Math.max(0, prev - 4), 4),
                                BytesUtil.toString(
                                    outArray, prev, curr - prev)));
                }
                if (writeHasRemaining) {
                    (new FlushTask(this)).
                        schedule(randomIODisabled ? 100 : 50);
                }
                if (curr - prev > 0) {
                    (new ProtoReadTask((this == testingHandler1) ?
                                       testingHandler2 : testingHandler1)).
                        schedule(randomIODisabled ? 100 : 50);
                } else if (curr - prev < 0) {
                    throw new AssertionError();
                }
                return true;
            }
            @Override
            protected void cleanup() {
            }
            @Override
            public ProtocolReader getProtocolReader() {
                return protocolReader;
            }
            @Override
            public ProtocolWriter getProtocolWriter() {
                return protocolWriter;
            }
            @Override
            public void assertInExecutorThread() {
            }
            @Override
            public ScheduledExecutorService getSchedExecService() {
                return executor;
            }
            byte getCRByte() {
                return isCreator() ? CR_BYTE : RE_BYTE;
            }
            int outputPosition() {
                return chOutput.position();
            }
            String doProtocolRead(int lim) throws Exception {
                final int prev = chInput.position();
                chInput.limit(lim);
                onChannelInputRead();
                final int curr = chInput.position();
                if (curr - prev == 0) {
                    return null;
                }
                flush();
                return BytesUtil.toString(inArray, prev, curr - prev);
            }
            void startDialogTasks(int numDialogTypes,
                                  int numDialogsPerType,
                                  TaskFail taskFail) {
                for (int dtype = 0; dtype < numDialogTypes; ++ dtype) {
                    startDialogTask(dtype, numDialogsPerType, taskFail);
                }
            }
            void startDialogTask(int dialogType,
                                 int numDialogsPerType,
                                 TaskFail taskFail) {
                (new DialogTask(this, dialogType,
                                numDialogsPerType, taskFail,
                                DEFAULT_TIMEOUT)).
                    runOnce();
            }
            void startDialogTask(int dialogType,
                                 int numDialogsPerType,
                                 TaskFail taskFail,
                                 int timeout) {
                (new DialogTask(this, dialogType,
                                numDialogsPerType, taskFail,
                                timeout)).
                    runOnce();
            }
            void checkLocalProtocolViolation() {
                checkLocalException(
                        ProtocolViolationException.class, "");
            }
            void checkLocalTimeout(String key) {
                if (key.equals("Idle")) {
                    checkLocalException(
                            ConnectionIdleException.class, key);
                } else {
                    checkLocalException(
                            ConnectionTimeoutException.class, key);
                }
            }
            void checkLocalException(Class<?> cls, String key) {
                ConnectionException cause = getTerminationCause();
                assertTrue(String.format("Wrong termination cause: %s", cause),
                        cls.isInstance(cause));
                assertTrue(String.format("Wrong type of timeout: %s", cause),
                        cause.getMessage().contains(key));
                assertEquals(String.format(
                            "Exception is not from local: %s", this),
                        false, cause.fromRemote());
            }
            void checkNormal() {
                assertEquals(String.format(
                            "Handler is not running: %s", this),
                        true, isNormal());
            }
        }

        private class TestingManager implements EndpointHandlerManager {
            private final DialogEndpointGroupPerfTracker
                endpointGroupPerfTracker =
                new DialogEndpointGroupPerfTracker(logger,
                    false /* forClientOnly */);

            @Override
            public void onHandlerShutdown(EndpointHandler handler) {
            }
            @Override
            public DialogEndpointGroupPerfTracker getEndpointGroupPerfTracker() {
                return endpointGroupPerfTracker;
            }
            @Override
            public void shutdown(String detail, boolean force) {
            }
        }

        private class TestingDialogFactory implements DialogHandlerFactory {

            private final byte[] request;
            private final byte[] response;
            TestingDialogFactory(byte crByte, int dialogType) {
                this.request = getBytes(crByte, REQ_BYTE, dialogType);
                this.response = getBytes(crByte, RESP_BYTE, dialogType);
            }
            @Override
            public DialogHandler create() {
                return new Responder(request, response, HandlerTester.this);
            }
            @Override
            public void onChannelError(ListenerConfig config,
                                       Throwable t,
                                       boolean channelClosed) {
                logError(t);
            }
        }

        /**
         * Notifies connection ready.
         */
        private class ConnectionTask extends Task {

            private final TestingHandler handler;
            ConnectionTask(TestingHandler handler) {
                super(handler.getSchedExecService());
                this.handler = handler;
            }
            @Override
            protected String doRun() throws Exception {
                handler.onChannelReady();
                return handler.toString();
            }
            @Override
            protected boolean shouldRunAgain() {
                return false;
            }
        }

        /**
         * Starts dialogs.
         */
        private class DialogTask extends Task {

            private final TestingHandler handler;
            private final byte crByte;
            private final int dialogType;
            private final int numDialogs;
            private final TaskFail taskFail;
            private final int timeout;
            private volatile int cntFinished = 0;
            private volatile int cntAborted = 0;
            private volatile int cntDone = 0;
            private volatile Throwable errorCause = null;
            private volatile long doneTime = -1;
            DialogTask(TestingHandler handler,
                       int dialogType,
                       int numDialogs,
                       TaskFail taskFail,
                       int timeout) {
                super(handoffExecutor);
                this.handler = handler;
                this.crByte = handler.getCRByte();
                this.dialogType = dialogType;
                this.numDialogs = numDialogs;
                this.taskFail = taskFail;
                this.timeout = timeout;
                /* Adds to the set of dialog tasks for check. */
                dialogTasks.add(this);
                /* Acquire the semaphore until we finished all dialogs */
                semaphoreAcquire();
            }
            int getDialogType() {
                return dialogType;
            }
            long doneTime() {
                return doneTime;
            }
            @Override
            protected String doRun() throws Exception {
                Requester requester = new Requester(
                          getBytes(getId()),
                          getBytes(crByte, REQ_BYTE, dialogType),
                          getBytes(crByte, RESP_BYTE, dialogType),
                          HandlerTester.this,
                          handoffExecutor);
                requester.setOnReadHook(ctx -> dialogFin());
                requester.setOnAbortHook(cause -> {
                    errorCause = cause;
                    dialogAborted();
                });
                handler.startDialog(dialogType, requester, timeout);
                return String.format("Called startDialog for Requester, " +
                        "dtype=%d, n=%d, cntF=%d, handler=%s",
                        dialogType, numDialogs, cntFinished, handler);
            }
            @Override
            protected boolean shouldRunAgain() {
                return false;
            }
            private void dialogFin() {
                cntFinished ++;
                dialogDone();
                logMesg(String.format(
                            "Dialog done, task=%s, dtype=%d, " +
                            "n=%d, cntF=%d, cntA=%d, cntD=%d",
                            this, dialogType, numDialogs,
                            cntFinished, cntAborted, cntDone, handler));
            }
            private void dialogAborted() {
                cntAborted ++;
                dialogDone();
                logMesg(String.format(
                            "Dialog aborted, task=%s, dtype=%d, " +
                            "n=%d, cntF=%d, cntA=%d, cntD=%d",
                            this, dialogType, numDialogs,
                            cntFinished, cntAborted, cntDone, handler));
            }
            private void dialogDone() {
                cntDone ++;
                assert cntDone <= numDialogs;
                if ((cntDone < numDialogs) && (errorCause == null)) {
                    scheduleOnce();
                } else {
                    doneTime = now();
                    semaphoreRelease();
                }
            }
            void check() {
                checkFin(numDialogs);
            }
            void check(Class<?> cls) {
                if (taskFail == TaskFail.NO) {
                    checkFin(numDialogs);
                } else if (taskFail == TaskFail.NO) {
                    checkFin(0);
                    checkException(cls);
                } else {
                    if (cntFinished == numDialogs) {
                        return;
                    }
                    checkException(cls);
                }
            }
            void checkFin(int expected) {
                String info = (expected == 0) ?
                    "Some dialogs succeded: " : "Some dialogs failed: ";
                assertEquals(String.format(
                            "%s id=%x, dtype=%d, n=%d, cntF=%d, cntA=%d, cntD=%d",
                            info, getId(), dialogType, numDialogs,
                            cntFinished, cntAborted, cntDone),
                        expected, cntFinished);
            }
            void checkException(Class<?> cls) {
                    assertTrue(String.format("Instance %s not of class %s",
                                errorCause, cls),
                            cls.isInstance(errorCause));
            }
        }
        /**
         * Flushes the channel output.
         */
        private class FlushTask extends Task {

            private final TestingHandler handler;
            FlushTask(TestingHandler handler) {
                super(handoffExecutor);
                this.handler = handler;
            }
            @Override
            protected String doRun() throws Exception {

                final int prev = handler.chOutput.position();
                handler.flush();
                final int curr = handler.chOutput.position();
                return String.format(
                        "Flushed again, (%d, %d), %s, handler=%s",
                        prev, (curr - prev),
                        BytesUtil.toString(handler.outArray,
                           Math.max(0, curr - 4), 8),
                        handler);
            }
            @Override
            protected boolean shouldRunAgain() {
                return false;
            }
        }

        /**
         * Reads protocol message from the channel input.
         */
        private class ProtoReadTask extends Task {

            private final TestingHandler self;
            private final TestingHandler peer;
            ProtoReadTask(TestingHandler self) {
                super(self.getSchedExecService());
                this.self = self;
                this.peer = (self == testingHandler1) ?
                    testingHandler2 : testingHandler1;
            }
            @Override
            protected String doRun() throws Exception {
                final int lim = peer.outputPosition();
                String preinfo = BytesUtil.toString(
                        self.inArray, Math.max(0, lim - 8), 8);
                final String info = self.doProtocolRead(lim);
                if (info == null) {
                    return null;
                }
                return String.format(
                        "handler=%s\n\tpreinfo=%s\n\tread=%s",
                        self, preinfo, info);
            }
            @Override
            protected boolean shouldRunAgain() {
                return false;
            }
        }
    }
    private byte[] getBytes(int id) {
        byte[] bytes = new byte[4];
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        buf.putInt(id);
        return bytes;
    }
    private byte[] getBytes(byte crByte, byte pqByte, int dialogType) {
        int prefixSize = 6;
        int suffixSize = 4;
        int size = Math.max(
                prefixSize + suffixSize, dialogType * DEFAULT_MAX_PROTO_LEN);
        byte[] bytes = new byte[size];
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        buf.put(crByte);
        buf.put(pqByte);
        buf.putInt(dialogType);
        int i = 0;
        while (i < bytes.length - prefixSize - suffixSize) {
            bytes[i + prefixSize] = crByte;
            bytes[i + prefixSize + 1] = pqByte;
            bytes[i + prefixSize + 2] = (byte) dialogType;
            bytes[i + prefixSize + 3] =
                (byte) (i / DEFAULT_MAX_PROTO_LEN);
            i += 4;
        }
        Arrays.fill(bytes, bytes.length - suffixSize, bytes.length,
                (byte) 0xff);
        return bytes;
    }
    private  EndpointConfig getDefaultEndpointConfig() {
        return setDefaultOptions(new EndpointConfigBuilder()).build();
    }
    private EndpointConfigBuilder
        setDefaultOptions(EndpointConfigBuilder builder) {

        return builder.
            option(AsyncOption.DLG_LOCAL_MAXLEN, DEFAULT_MAX_PROTO_LEN).
            option(AsyncOption.DLG_REMOTE_MAXLEN, DEFAULT_MAX_PROTO_LEN).
            option(AsyncOption.DLG_HEARTBEAT_INTERVAL, 100);
    }
    private EndpointConfig getRemoteDialogsLimitedEndpointConfig() {
        return setDefaultOptions(new EndpointConfigBuilder()).
            option(AsyncOption.DLG_REMOTE_MAXDLGS, LIMITED_MAX_DIALOGS).
            build();
    }
    private EndpointConfig
        getShortTimeoutEndpointConfig(AsyncOption<Integer> timeoutOption) {

        EndpointConfigBuilder builder =
            setDefaultOptions(new EndpointConfigBuilder()).
            option(AsyncOption.DLG_CONNECT_TIMEOUT, 100000).
            option(AsyncOption.DLG_HEARTBEAT_TIMEOUT, 100).
            option(AsyncOption.DLG_IDLE_TIMEOUT, 100000);
        if (timeoutOption == AsyncOption.DLG_HEARTBEAT_TIMEOUT) {
            builder.option(AsyncOption.DLG_HEARTBEAT_TIMEOUT, 1);
        } else {
            builder.option(timeoutOption, 111);
        }
        return builder.build();
    }
    private EndpointConfig getNoHeartbeatConfig() {

        EndpointConfigBuilder builder =
            setDefaultOptions(new EndpointConfigBuilder()).
            option(AsyncOption.DLG_HEARTBEAT_TIMEOUT, 100000).
            option(AsyncOption.DLG_HEARTBEAT_INTERVAL, 100000).
            option(AsyncOption.DLG_CONNECT_TIMEOUT, 100000).
            option(AsyncOption.DLG_IDLE_TIMEOUT, 100000);
        return builder.build();
    }
    private NetworkAddress createAddress(boolean isCreator) {
        return new InetNetworkAddress((isCreator ? CREATOR : RESPONDER), 0);
    }
}
