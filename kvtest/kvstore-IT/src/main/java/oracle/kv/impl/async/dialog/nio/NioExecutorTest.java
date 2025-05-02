/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async.dialog.nio;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;
import java.util.stream.Collectors;

import oracle.kv.TestBase;
import oracle.kv.impl.async.dialog.Logged;
import oracle.kv.impl.async.dialog.nio.NioChannelExecutor.RunState;
import oracle.kv.impl.async.dialog.nio.NioChannelExecutor.State;
import oracle.kv.impl.fault.AsyncEndpointGroupFaultHandler;
import oracle.kv.impl.topo.util.FreePortLocator;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.TestUtils;

import org.junit.Test;

public class NioExecutorTest extends TestBase {

    private static final FreePortLocator portLocator =
        new FreePortLocator("localhost", 4242, 5555);
    private static final double TIME_DELTA = 50;
    private static final int SO_BUFSZ = 1024;
    private static final int RW_SIZE = SO_BUFSZ * 4;

    private static final long perfReportIntervalMillis = 100 /* 100ms */;
    private static final long heartbeatThresholdMillis = 1000 /* 1s */;
    private static final long heartbeatCheckIntervalMillis = 1000 /* 1s */;
    private static final long stuckExecutorDumpThresholdMillis = 10000 /* 10s */;

    private ScheduledExecutorService executorMT;
    private NioChannelThreadPool threadPoolST;

    @Override
    public void setUp() throws Exception {
        NioChannelExecutor.perfReportIntervalMillis =
            perfReportIntervalMillis;
        NioChannelThreadPoolPerfTracker.heartbeatThresholdMillis =
            heartbeatThresholdMillis;
        NioChannelThreadPoolPerfTracker.heartbeatCheckIntervalMillis =
            heartbeatCheckIntervalMillis;
        NioChannelThreadPoolPerfTracker.stuckExecutorDumpThresholdMillis =
            stuckExecutorDumpThresholdMillis;
        executorMT = new ScheduledThreadPoolExecutor(4);
        threadPoolST = new NioChannelThreadPool(
            logger, 1, 60, AsyncEndpointGroupFaultHandler.DEFAULT,
            Executors.newScheduledThreadPool(1));
        Logged.resetT0();
    }

    @Override
    public void tearDown() throws Exception {
        executorMT.shutdownNow();
        threadPoolST.shutdown(true);
        NioChannelExecutor.perfReportIntervalMillis =
            NioChannelExecutor.DEFAULT_PERF_REPORT_INTERNAL_MILLIS;
        NioChannelThreadPoolPerfTracker.heartbeatThresholdMillis =
            NioChannelThreadPoolPerfTracker
            .DEFAULT_HEARTBEAT_INTERVAL_THRESHOLD_MILLIS;
        NioChannelThreadPoolPerfTracker.heartbeatCheckIntervalMillis =
            NioChannelThreadPoolPerfTracker
            .DEFAULT_HEARTBEAT_CHECK_INTERVAL_MILLIS;
        NioChannelThreadPoolPerfTracker.stuckExecutorDumpThresholdMillis =
            NioChannelThreadPoolPerfTracker
            .DEFAULT_STUCK_EXECUTOR_DUMP_THRESHOLD_MILLIS;
    }

    /**
     * Tests the run state transitions.
     */
    @Test
    public void testRunStateTransition() {
        final State state = new State();
        /* Test initial state. */
        assertEquals(state.getRunState(), RunState.RUNNING);
        assertEquals(state.getNewEventCount(), 0);

        /* Test forward transition. */
        transitAndVerify(state, RunState.SHUTTINGDOWN);
        transitAndVerify(state, RunState.SHUTDOWN);
        transitAndVerify(state, RunState.STOP);
        transitAndVerify(state, RunState.TERMINATED);

        /* Test backward transition. */
        transitAndVerify(state, RunState.RUNNING, RunState.TERMINATED);
        transitAndVerify(state, RunState.STOP, RunState.TERMINATED);

        /* Test new-event count is not changed. */
        assertEquals(state.getNewEventCount(), 0);
    }

    private void transitAndVerify(State state,
                                  RunState runState) {
        transitAndVerify(state, runState, runState);
    }

    private void transitAndVerify(State state,
                                  RunState runState,
                                  RunState expected) {
        state.transit(runState);
        assertEquals(state.toString(), expected, state.getRunState());
    }

    /**
     * Tests the run state transitions with expected new-event count values.
     */
    @Test
    public void testRunStateTransitionWithExpectedNewEventCount() {
        final State state = new State();
        final int inc = 10;
        for (int i = 0; i < inc; ++i) {
            state.markNewEvent((s) -> true);
        }
        assertEquals(state.toString(), inc, state.getNewEventCount());

        boolean succeeded;
        /* Test successful transition. */
        succeeded = state.transit(inc, RunState.SHUTTINGDOWN);
        assertEquals(state.toString(), true, succeeded);
        assertEquals(state.toString(),
                     RunState.SHUTTINGDOWN, state.getRunState());

        /* Test failed transition due to count. */
        final long curr = state.getNewEventCount();
        state.markNewEvent((s) -> true);
        succeeded = state.transit(curr, RunState.SHUTDOWN);
        assertEquals(state.toString(), false, succeeded);
        assertEquals(state.toString(),
                     RunState.SHUTTINGDOWN, state.getRunState());

        /* Test failed transition due to run state. */
        succeeded = state.transit(curr, RunState.SHUTTINGDOWN);
        assertEquals(state.toString(), false, succeeded);
        assertEquals(state.toString(),
                     RunState.SHUTTINGDOWN, state.getRunState());
    }

    /**
     * Tests the incrementation of new event count.
     */
    @Test
    public void testMarkNewEvent() {
        final State state = new State();
        boolean succeeded;
        /* Test increment successful. */
        succeeded = state.markNewEvent((s) -> s.equals(RunState.RUNNING));
        assertEquals(state.toString(), true, succeeded);
        assertEquals(state.toString(), 1, state.getNewEventCount());
        /* Test increment failed. */
        succeeded = state.markNewEvent((s) -> s.equals(RunState.SHUTTINGDOWN));
        assertEquals(state.toString(), false, succeeded);
        assertEquals(state.toString(), 1, state.getNewEventCount());
    }

    /**
     * Tests the schedule service execute method.
     */
    @Test(timeout=2000)
    public void testSchedExecute() {
        int ntasks = 16;
        Task[] tasks = new Task[ntasks];
        for (int i = 0; i < ntasks; ++i) {
            tasks[i] = new Task();
        }
        NioChannelExecutor executor = threadPoolST.next();
        try {
            submitTasks(executor, tasks);
            Thread.sleep(50);
            for (Task task : tasks) {
                task.check();
            }
        } catch (Throwable e) {
            fail(String.format(
                        "\ntasks=%s\nexecutor=%s\nexecutorTasks=%s\nerror=%s",
                        tasksToString(tasks),
                        executor, executor.tasksToString(),
                        LoggerUtils.getStackTrace(e)));
        }
    }

    /**
     * Tests the schedule service schedule method.
     */
    @Test(timeout=2000)
    public void testSchedSchedule() {
        int ntasks = 16;
        Task[] tasks = new Task[ntasks];
        for (int i = 0; i < ntasks; ++i) {
            int time = i * 50 + 100;
            tasks[i] = new Task(time, TIME_DELTA);
        }
        NioChannelExecutor executor = threadPoolST.next();
        try {
            submitTasks(executor, tasks);
            Thread.sleep(1000);
            for (Task task : tasks) {
                task.check();
            }
        } catch (Throwable e) {
            fail(String.format(
                        "\ntasks=%s\nexecutor=%s\nexecutorTasks=%s\nerror=%s",
                        tasksToString(tasks),
                        executor, executor.tasksToString(),
                        LoggerUtils.getStackTrace(e)));
        }
    }

    /**
     * Tests the schedule service scheduleAtFixedRate method.
     */
    @Test(timeout=2000)
    public void testSchedScheduleAtFixedRate() {
        int ntasks = 4;
        Task[] tasks = new Task[ntasks];
        for (int i = 0; i < ntasks; ++i) {
            int time = i * 50 + 100;
            tasks[i] = new Task(time, 100, 1, TIME_DELTA);
        }
        NioChannelExecutor executor = threadPoolST.next();
        try {
            submitTasks(executor, tasks);
            Thread.sleep(1000);
            for (Task task : tasks) {
                task.check();
            }
        } catch (Throwable e) {
            fail(String.format(
                        "\ntasks=%s\nexecutor=%s\nexecutorTasks=%s\nerror=%s",
                        tasksToString(tasks),
                        executor, executor.tasksToString(),
                        LoggerUtils.getStackTrace(e)));
        }
    }

    /**
     * Tests the schedule service scheduleWithFixedDelay method.
     */
    @Test(timeout=2000)
    public void testSchedScheduleWithFixedDelay() {
        int ntasks = 4;
        Task[] tasks = new Task[ntasks];
        for (int i = 0; i < ntasks; ++i) {
            int time = i * 20 + 100;
            tasks[i] = new Task(time, -100, 10, TIME_DELTA);
        }
        NioChannelExecutor executor = threadPoolST.next();
        try {
            submitTasks(executor, tasks);
            Thread.sleep(1000);
            for (Task task : tasks) {
                task.check();
            }
        } catch (Throwable e) {
            fail(String.format(
                        "\ntasks=%s\nexecutor=%s\nexecutorTasks=%s\nerror=%s",
                        tasksToString(tasks),
                        executor, executor.tasksToString(),
                        LoggerUtils.getStackTrace(e)));
        }
    }

    /**
     * Tests registering for accept interst.
     */
    @Test
    public void testRegisterAccept() {
        int port = portLocator.next();
        NioChannelExecutor executor = threadPoolST.next();
        AcceptHandler handler = null;
        Client client = new Client(port);
        try {
            ServerSocketChannel channel = ServerSocketChannel.open();
            channel.bind(new InetSocketAddress(port));
            handler = new AcceptHandler(channel);
            executor.registerAcceptInterest(channel, handler);
            client.connect();
            handler.await(1000);
            handler.check();
            assertEquals("Channel not closed properly.",
                    false, channel.isOpen());
        } catch (Throwable e) {
            fail(String.format(
                        "\nhandler=%s\nclient=%s\nexecutor=%s\n:%s",
                        handler,
                        client,
                        executor,
                        LoggerUtils.getStackTrace(e)));
        }
    }

    /**
     * Tests registering for connect interst.
     */
    @Test
    public void testRegisterConnect() {
        int port = portLocator.next();
        NioChannelExecutor executor = threadPoolST.next();
        ConnectHandler handler = null;
        Server server = new IdleServer(port);
        try {
            server.accept();
            SocketChannel channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.connect(new InetSocketAddress("localhost", port));
            handler = new ConnectHandler(channel);
            executor.registerConnectInterest(channel, handler);
            handler.await(1000);
            handler.check();
            assertEquals("Channel not closed properly.",
                    false, channel.isOpen());
        } catch (Throwable e) {
            fail(String.format(
                        "\nhandler=%s\nserver=%s\nexecutor=%s\n:%s",
                        handler,
                        server,
                        executor,
                        LoggerUtils.getStackTrace(e)));
        }
    }

    /**
     * Tests registering for read interest.
     */
    @Test
    public void testRegisterRead() {
        int port = portLocator.next();
        NioChannelExecutor executor = threadPoolST.next();
        ReadHandler handler = null;
        WriteServer server = new WriteServer(port);
        try {
            server.accept();
            Thread.sleep(100);
            SocketChannel channel = SocketChannel.open();
            channel.connect(new InetSocketAddress("localhost", port));
            handler = new ReadHandler(channel);
            executor.setReadInterest(channel, handler, true);
            handler.await(1000);
            handler.check(server);
            assertEquals("Channel not closed properly.",
                    false, channel.isOpen());
        } catch (Throwable e) {
            fail(String.format(
                        "\nhandler=%s\nserver=%s\nexecutor=%s\n:%s",
                        handler,
                        server,
                        executor,
                        LoggerUtils.getStackTrace(e)));
        }
    }

    /**
     * Tests registering for read/write interest.
     */
    @Test
    public void testRegisterReadWrite() {
        int port = portLocator.next();
        NioChannelExecutor executor = threadPoolST.next();
        ReadWriteHandler handler = null;
        RespondServer server = new RespondServer(port);
        try {
            server.accept();
            Thread.sleep(100);
            SocketChannel channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.setOption(StandardSocketOptions.SO_RCVBUF, SO_BUFSZ);
            channel.setOption(StandardSocketOptions.SO_SNDBUF, SO_BUFSZ);
            channel.connect(new InetSocketAddress("localhost", port));
            handler = new ReadWriteHandler(executor, channel);
            executor.registerConnectInterest(channel, handler);
            /*
             * Note: this timeout is intentionally set to a larger value due to
             * unexpected delays on the blade. When running on the blade, TCP
             * packets are frequently missed by the server side, causing client
             * to re-transmit after some timeout. The RTOs are around 200ms.
             * The root cause is unknown yet.
             */
            handler.await(10000);
            handler.check(server);
            assertEquals("Channel not closed properly.",
                    false, channel.isOpen());
        } catch (Throwable e) {
            fail(String.format(
                        "\nhandler=%s\nserver=%s\nexecutor=%s\n:%s",
                        handler,
                        server,
                        executor,
                        LoggerUtils.getStackTrace(e)));
        }
    }

    /**
     * Tests multiple channels registering for read/write interest.
     */
    @Test
    public void testRegisterReadWriteMultiChannels() {
        int n = 2;
        NioChannelExecutor executor = threadPoolST.next();
        int[] ports = new int[n];
        SocketChannel[] channels = new SocketChannel[n];
        ReadWriteHandler[] handlers = new ReadWriteHandler[n];
        RespondServer[] servers = new RespondServer[n];
        try {
            for (int i = 0; i < n; ++i) {
                final int port = portLocator.next();
                ports[i] = port;
                servers[i] = new RespondServer(port);
                servers[i].accept();
            }
            Thread.sleep(100);
            for (int i = 0; i < n; ++i) {
                final SocketChannel channel = SocketChannel.open();
                channel.configureBlocking(false);
                channel.setOption(StandardSocketOptions.SO_RCVBUF, SO_BUFSZ);
                channel.setOption(StandardSocketOptions.SO_SNDBUF, SO_BUFSZ);
                channel.connect(new InetSocketAddress("localhost", ports[i]));
                channels[i] = channel;
                handlers[i] = new ReadWriteHandler(executor, channel);
                executor.registerConnectInterest(channel, handlers[i]);
            }
            for (int i = 0; i < n; ++i) {
                /*
                 * Note: this timeout is intentionally set to a larger value due to
                 * unexpected delays on the blade. When running on the blade, TCP
                 * packets are frequently missed by the server side, causing client
                 * to re-transmit after some timeout. The RTOs are around 200ms.
                 * The root cause is unknown yet.
                 */
                handlers[i].await(10000);
                handlers[i].check(servers[i]);
                assertEquals("Channel not closed properly.",
                        false, channels[i].isOpen());
            }
        } catch (Throwable e) {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("executor=%s\n", executor));
            for (int i = 0; i < n; ++i) {
                sb.append(String.format("i=%s\nhandler=%s\nserver=%s\n\n",
                            i, handlers[i], servers[i]));
            }
            sb.append(LoggerUtils.getStackTrace(e));
            fail(sb.toString());
        }
    }

    /**
     * Tests deregister.
     */
    @Test
    public void testDeregister() {
        int port = portLocator.next();
        NioChannelExecutor executor = threadPoolST.next();
        DeregisterHandler handler = null;
        SocketChannel channel = null;
        Server server = new WriteServer(port);
        try {
            server.accept();
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.connect(new InetSocketAddress("localhost", port));
            handler = new DeregisterHandler(executor, channel);
            executor.registerConnectInterest(channel, handler);
            handler.await(1000);
            handler.check();
            assertEquals("Channel not closed properly.",
                    false, channel.isOpen());
        } catch (Throwable e) {
            fail(String.format(
                        "\nhandler=%s\nserver=%s\nexecutor=%s\n:%s",
                        handler,
                        server,
                        executor,
                        LoggerUtils.getStackTrace(e)));
        }
    }

    /**
     * Tests thread pool restart executor.
     */
    @Test
    public void testPoolExecutorRestart() {
        try {
            for (int i = 0; i < 16; ++i) {
                executorMT.execute(new Runnable() {
                    @Override
                    public void run() {
                        final NioChannelExecutor executor =
                            threadPoolST.next();
                        if (executor != null) {
                            executor.shutdownNow();
                        }
                    }
                });
            }
            Thread.sleep(50);
            assertNotNull(threadPoolST.next());
        } catch (Throwable t) {
            fail(LoggerUtils.getStackTrace(t));
        }
    }

    private void submitTasks(NioChannelExecutor executor, Task[] tasks) {
        for (Task task : tasks) {
            if (task.expT0 == -1) {
                executorMT.execute(new Runnable() {
                    @Override
                    public void run() {
                        executor.execute(task);
                    }
                });
            } else if (task.period == 0) {
                executorMT.execute(new Runnable() {
                    @Override
                    public void run() {
                        executor.schedule(
                                task, task.expT0, TimeUnit.MILLISECONDS);
                    }
                });
            } else if (task.period > 0) {
                executorMT.execute(new Runnable() {
                    @Override
                    public void run() {
                        executor.scheduleAtFixedRate(
                                task, task.expT0, task.period,
                                TimeUnit.MILLISECONDS);
                    }
                });
            } else {
                executorMT.execute(new Runnable() {
                    @Override
                    public void run() {
                        executor.scheduleWithFixedDelay(
                                task, task.expT0, -task.period,
                                TimeUnit.MILLISECONDS);
                    }
                });
            }
        }
    }

    private class Task implements Runnable {

        private final long expT0;
        private final long period;
        private final long execTm;
        private final long rtLag;
        private final double delta;
        private final List<Long> runTs = new ArrayList<Long>();
        Task() {
            this(-1, 0, 0, 0);
        }
        Task(long expT0, double delta) {
            this(expT0, 0, 0, delta);
        }
        Task(long expT0,
             long period,
             long execTm,
             double delta) {
            this.expT0 = expT0;
            this.period = period;
            this.execTm = execTm;
            this.rtLag = (period > 0) ? period : -period + execTm;
            this.delta = delta;
        }
        @Override
        public void run() {
            runTs.add((long) (Logged.now() / 1e6));
            try {
                Thread.sleep(execTm);
            } catch (InterruptedException e) {
                throw new Error(e);
            }
        }
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(getClass().getSimpleName());
            sb.append(" expT0=" + expT0);
            sb.append(" period=" + period);
            sb.append(" execTm=" + execTm);
            sb.append(" rtLag=" + rtLag);
            sb.append(" delta=" + delta);
            sb.append(" runTs=[");
            for (Long rt : runTs) {
                sb.append(rt);
                sb.append(", ");
            }
            sb.append("]");
            return sb.toString();
        }
        void check() {
            assertEquals("Task did not run once: ",
                    false, runTs.isEmpty());
            if (period != 0) {
                assertTrue("Task did not run periodically: ",
                        runTs.size() > 1);
            }
            if (expT0 != -1) {
                if (period > 0) {
                    /* For fixed rate, check the exeuciton time. */
                    for (int i = 0; i < runTs.size(); ++i) {
                        assertEquals(
                                "Task did not run at the expected time: ",
                                expT0 + i * rtLag,
                                runTs.get(i),
                                delta);
                    }
                } else {
                    /* For fixed delay, check delay. */
                    for (int i = 1; i < runTs.size(); ++i) {
                        assertEquals(
                                "Task did not run at the expected delay: ",
                                rtLag,
                                runTs.get(i) - runTs.get(i - 1),
                                delta);
                    }
                }
            }
        }
    }

    private String tasksToString(Task[] tasks) {
        StringBuilder sb = new StringBuilder("Tasks: ");
        for (Task task : tasks) {
            sb.append(String.format("(%s) ", task));
        }
        return sb.toString();
    }

    private class Agent extends Logged {

        Agent() {
            super(false);
        }

        protected void safeClose(Closeable closeable) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (Throwable t) {
                    /* nothing */
                }
            }
        }
        String getPrefix() {
            return getClass().getSimpleName();
        }
    }


    private class Client extends Agent implements Runnable {

        private final int port;
        Client(int port) {
            this.port = port;
        }
        void connect() {
            (new Thread(this)).start();
        }
        @Override
        public void run() {
            Socket socket = null;
            try {
                logMesg(String.format("%s connecting.", getPrefix()));
                socket = new Socket("localhost", port);
                logMesg(String.format("%s connected.", getPrefix()));
                Thread.sleep(100);
            } catch (Throwable t) {
                logError(t);
            } finally {
                safeClose(socket);
            }
        }
    }

    private abstract class Server extends Agent implements Runnable {

        private final int port;
        Server(int port) {
            this.port = port;
        }
        void accept() {
            (new Thread(this)).start();
        }
        @Override
        public void run() {
            ServerSocket serverSocket = null;
            try {
                logMesg(String.format("%s accepting.", getPrefix()));
                serverSocket = new ServerSocket();
                serverSocket.bind(new InetSocketAddress(port));
                serverSocket.setReceiveBufferSize(SO_BUFSZ);
                Socket socket = serverSocket.accept();
                socket.setSendBufferSize(SO_BUFSZ);
                socket.setTcpNoDelay(true);
                logMesg(String.format(
                            "%s accepted and do work, " +
                            "recvBufSize=%s, sendBufSize=%s",
                            getPrefix(),
                            socket.getReceiveBufferSize(),
                            socket.getSendBufferSize()));
                doWork(socket);
            } catch (Throwable t) {
                logError(t);
            } finally {
                safeClose(serverSocket);
            }
        }
        abstract void doWork(Socket socket);
    }


    private class IdleServer extends Server {

        IdleServer(int port) {
            super(port);
        }
        @Override
        public void doWork(Socket socket) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                logError(e);
            } finally {
                safeClose(socket);
            }
        }
    }

    private class WriteServer extends Server {

        private volatile int timesWritten;
        WriteServer(int port) {
            super(port);
        }
        @Override
        public void doWork(Socket socket) {
            try {
                DataOutputStream out =
                    new DataOutputStream(socket.getOutputStream());
                final int timesToWrite = 8;
                for (int i = 0; i < timesToWrite; ++i) {
                    Thread.sleep(100);
                    out.write(42);
                    out.flush();
                    timesWritten ++;
                    logMesg(String.format(
                                "%s write a byte, #timesWritten=%s",
                                getPrefix(), timesWritten));
                }
                out.write(0);
                out.flush();
            } catch (Throwable t) {
                logError(t);
            } finally {
                safeClose(socket);
            }
        }
        int getTimesWritten() {
            return timesWritten;
        }
    }

    private class RespondServer extends Server {

        private final byte[] buffer = new byte[RW_SIZE];
        private volatile int timesResponded = 0;
        RespondServer(int port) {
            super(port);
        }
        @Override
        public void doWork(Socket socket) {
            try {
                final int timesToRespond = 8;
                DataInputStream in =
                    new DataInputStream(socket.getInputStream());
                DataOutputStream out =
                    new DataOutputStream(socket.getOutputStream());
                for (int i = 0; i < timesToRespond; ++i) {
                    Thread.sleep(100);
                    logMesg(String.format("%s reading data.", getPrefix()));
                    int nread = 0;
                    while (nread < RW_SIZE) {
                        final int n = in.read(buffer);
                        if (n < 0) {
                            break;
                        }
                        nread += n;
                        logMesg(String.format(
                                    "%d bytes remaining", RW_SIZE - nread));
                    }
                    logMesg(String.format("%s writing data.", getPrefix()));
                    out.write(42);
                    out.flush();
                    timesResponded ++;
                    logMesg(String.format(
                                "%s done read and write, timesResponded=%s",
                                getPrefix(), timesResponded));
                }
                out.write(0);
                out.flush();
                logMesg(String.format("%s done, timesResponded=%s",
                            getPrefix(), timesResponded));
            } catch (Throwable t) {
                logError(t);
            } finally {
                safeClose(socket);
            }
        }
        int getTimesResponded() {
            return timesResponded;
        }
    }

    private class Handler extends Agent implements NioHandler {

        protected volatile boolean isDone = false;
        synchronized void await(long timeout) throws InterruptedException {
            if (!isDone) {
                wait(timeout);
            }
        }
        synchronized void done() {
            if (isDone) {
                return;
            }
            isDone = true;
            logMesg(String.format("%s is done.", getPrefix()));
            notifyAll();
        }
        @Override
        public void cancel(Throwable t) {
            logError(t);
            done();
        }
        void check() {
            assertEquals(String.format("%s not done.", getPrefix()),
                    true, isDone);
        }
        void safeFail(Throwable t, Closeable closeable) {
            logMesg(String.format("Failed with %s, done=%s", t, isDone));
            logError(t);
            safeClose(closeable);
            done();
        }
    }

    private class AcceptHandler
            extends Handler implements ChannelAccepter {

        private final ServerSocketChannel serverSocketChannel;
        AcceptHandler(ServerSocketChannel serverSocketChannel) {
            this.serverSocketChannel = serverSocketChannel;
        }
        @Override
        public void onAccept(SocketChannel socketChannel) {
            safeClose(socketChannel);
            safeClose(serverSocketChannel);
            done();
        }
    }

    private class ConnectHandler
            extends Handler implements ChannelHandler {

        private final SocketChannel channel;
        ConnectHandler(SocketChannel channel) {
            this.channel = channel;
        }
        @Override
        public void onConnected() {
            safeClose(channel);
            done();
        }
        @Override
        public void onRead() {
            safeFail(new AssertionError("onRead should not be called."),
                    channel);
        }
        @Override
        public void onWrite() {
            safeFail(new AssertionError("onWrite should not be called."),
                    channel);
        }
    }

    private class ReadHandler
            extends Handler implements ChannelHandler {

        protected final ByteBuffer buf = ByteBuffer.allocate(1);
        protected final SocketChannel channel;
        protected volatile int timesRead = 0;
        ReadHandler(SocketChannel channel) {
            this.channel = channel;
        }
        @Override
        public void onConnected() {
            safeFail(new AssertionError("onRead should not be called."),
                    channel);
        }
        @Override
        public void onRead() {
            if (isDone) {
                return;
            }
            logMesg(String.format("%s onReadStart, done=%s, timesRead=%s",
                        getPrefix(), isDone, timesRead));
            try {
                while (true) {
                    buf.clear();
                    final int n = channel.read(buf);
                    if (n < 0) {
                        safeClose(channel);
                        done();
                    }
                    if (n <= 0) {
                        return;
                    }
                    buf.flip();
                    byte b = buf.get();
                    if (b == 42) {
                        timesRead ++;
                        logMesg(String.format("%s read once, timesRead=%s",
                                    getPrefix(), timesRead));
                    } else if (b == 0) {
                        logMesg(String.format("%s got 0, closing connection, " +
                                    "timesRead=%s",
                                    getPrefix(), timesRead));
                        safeClose(channel);
                        done();
                        return;
                    } else  {
                        logError(new AssertionError(
                                    String.format(
                                        "Wrong read data, " +
                                        "expected=42 or 0, got=%s",
                                        b)));
                    }
                }
            } catch (Throwable t) {
                safeFail(t, channel);
            }
        }
        @Override
        public void onWrite() {
            safeFail(new AssertionError("On read should not be called."),
                    channel);
        }
        void check(WriteServer server) {
            super.check();
            assertEquals("Number of read incorrect",
                    server.getTimesWritten(), timesRead);
            checkExceptions();
        }
    }

    private class ReadWriteHandler extends ReadHandler {

        private final ByteBuffer outbuf = ByteBuffer.allocate(RW_SIZE);
        private final NioChannelExecutor executor;
        private volatile boolean connected = false;
        private volatile boolean readOnly = false;
        private volatile int timesWritten = 0;
        ReadWriteHandler(NioChannelExecutor executor, SocketChannel channel) {
            super(channel);
            this.executor = executor;
        }
        @Override
        public void onConnected() {
            if (connected) {
                logError(new AssertionError("onRead should not be called."));
                return;
            }
            logMesg(String.format(
                        "%s connected, channel=%s, register read write, ",
                        getPrefix(), channel));
            try {
                logMesg(String.format("recvBufSize=%s, sendBufSize=%s",
                            channel.socket().getReceiveBufferSize(),
                            channel.socket().getSendBufferSize()));
                executor.setInterest(
                    channel, this,
                    SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                connected = true;
            } catch (Throwable t) {
                safeFail(t, channel);
            }
        }
        @Override
        public void onRead() {
            super.onRead();
            if (!isDone) {
                try {
                    readOnly = false;
                    outbuf.clear();
                    executor.setWriteInterest(channel, this, true);
                } catch (Throwable t) {
                    safeFail(t, channel);
                }
            }
            logMesg(String.format(
                        "%s onRead done, done=%s, timesRead=%s, readOnly=%s",
                        getPrefix(), isDone, timesRead, readOnly));
        }
        @Override
        public void onWrite() {
            if (isDone) {
                return;
            }
            try {
                if (readOnly) {
                    logError(new AssertionError(
                                "onWrite should not be called after " +
                                "readOnly."));
                    safeClose(channel);
                    return;
                }
                final int n = channel.write(outbuf);
                logMesg(String.format("%s write %s bytes, remaining=%s",
                            getPrefix(), n, outbuf.remaining()));
                if (outbuf.remaining() == 0) {
                    timesWritten ++;
                    readOnly = true;
                    executor.setWriteInterest(channel, this, false);
                }
                logMesg(String.format(
                            "%s onWrite done, done=%s, " +
                            "timesWritten=%s, readOnly=%s",
                            getPrefix(), isDone, timesWritten, readOnly));
            } catch (Throwable t) {
                safeFail(t, channel);
            }
        }
        public void check(RespondServer server) {
            super.check();
            assertEquals("Number of write incorrect",
                    server.getTimesResponded(), timesWritten);
            checkExceptions();
        }
    }

    private class DeregisterHandler
            extends Handler implements ChannelHandler {

        private final NioChannelExecutor executor;
        private final SocketChannel channel;
        DeregisterHandler(NioChannelExecutor executor, SocketChannel channel) {
            this.executor = executor;
            this.channel = channel;
        }
        @Override
        public void onConnected() {
            try {
                executor.deregister(channel);
                channel.configureBlocking(true);
                logMesg(String.format("%s deregistered channel.",
                            getPrefix()));
                Socket socket = channel.socket();
                DataInputStream in =
                    new DataInputStream(socket.getInputStream());
                byte[] buffer = new byte[1];
                while (true) {
                    in.readFully(buffer);
                    if (buffer[0] == 0) {
                        safeClose(channel);
                        done();
                        return;
                    }
                    assertEquals("Wrong read data.", 42, buffer[0]);
                }
            } catch (Throwable t) {
                safeFail(t, channel);
            }
        }
        @Override
        public void onRead() {
            safeFail(new AssertionError("onRead should not be called."),
                    channel);
        }
        @Override
        public void onWrite() {
            safeFail(new AssertionError("onWrite should not be called."),
                    channel);
        }
    }

    /**
     * Tests executor creation.
     */
    @Test
    public void testExecutorCreation() {
        final NioChannelThreadPool pool = new NioChannelThreadPool(
            logger, 2, 1, AsyncEndpointGroupFaultHandler.DEFAULT,
            Executors.newScheduledThreadPool(1));
        final Set<NioChannelExecutor> executors = new HashSet<>();
        for (int i = 0; i < 64; ++i) {
            final NioChannelExecutor executor = pool.next();
            executor.scheduleWithFixedDelay(
                () -> {}, 50, 50, TimeUnit.MILLISECONDS);
            executors.add(executor);
        }
        assertEquals(2, executors.size());
        pool.shutdown(true);
    }

    /**
     * Tests executor quiescent shutdown.
     */
    @Test
    public void testExecutorQuiescentShutdown() throws Exception {
        final long maxQuiescentNanos = TimeUnit.MILLISECONDS.toNanos(100);
        NioChannelExecutor.setDefaultMaxQuiescentTimeNanos(
            maxQuiescentNanos, tearDowns);
        final NioChannelThreadPool pool = new NioChannelThreadPool(
            logger, 2, 1, AsyncEndpointGroupFaultHandler.DEFAULT,
            Executors.newScheduledThreadPool(1));
        final Set<NioChannelExecutor> executors = new HashSet<>();
        for (int i = 0; i < 64; ++i) {
            executors.add(pool.next());
        }
        assertEquals(2, executors.size());
        Thread.sleep(2 * TimeUnit.NANOSECONDS.toMillis(maxQuiescentNanos));
        executors.forEach((e) -> assertEquals(e.toString(), true, e.isTerminated()));
        pool.shutdown(true);
    }

    /**
     * Tests the race between task submission and quiescent shutdown.
     * [KVSTORE-1882]
     */
    @Test
    public void testTaskSubmissionQuiescenceRace() throws Exception {
        final long maxQuiescentNanos = TimeUnit.MILLISECONDS.toNanos(10);
        NioChannelExecutor.setDefaultMaxQuiescentTimeNanos(
            maxQuiescentNanos, tearDowns);
        final NioChannelThreadPool pool = new NioChannelThreadPool(
            logger, 2, 1, AsyncEndpointGroupFaultHandler.DEFAULT,
            Executors.newScheduledThreadPool(1));
        final int niters = 128;
        tearDowns.add(() -> {
            NioChannelExecutor.quiescentMonitorHook = null;
        });
        final Object signal = new Object();
        final AtomicReference<NioChannelExecutor> currExecutor =
            new AtomicReference<>(null);
        final AtomicBoolean enteredHook = new AtomicBoolean(false);
        final AtomicBoolean raceStart = new AtomicBoolean(false);
        NioChannelExecutor.quiescentMonitorHook = (e) -> {
            if (currExecutor.get() != e) {
                return;
            }
            /*
             * The first time we entered the hook, we will start to enter
             * quiescent period.
             */
            if (enteredHook.compareAndSet(false, true)) {
                return;
            }
            /*
             * The second time we entered the hook, sleep past the quiescence
             * duration so that we are sure to past the threshold.
             */
            try {
                Thread.sleep(TimeUnit.NANOSECONDS.toMillis(maxQuiescentNanos));
            } catch (InterruptedException ie) {
                throw new IllegalStateException(ie);
            }
            enteredHook.set(false);
            /* Signals the other thread so that it will start to race. */
            raceStart.set(true);
            synchronized(signal) {
                signal.notifyAll();
            }
            /* Yield to give a bit more chance to the other thread. */
            Thread.yield();
        };
        int rejectCount = 0;
        for (int i = 0; i < niters; ++i) {
            final NioChannelExecutor executor = pool.next();
            currExecutor.set(executor);
            raceStart.set(false);
            final AtomicInteger count = new AtomicInteger(0);
            /* Waits for the race to start. */
            while (!raceStart.get()) {
                synchronized(signal) {
                    signal.wait();
                }
            }
            /* Submits a task and check the submission and execution state. */
            boolean rejected;
            try {
                executor.submit(() -> count.incrementAndGet());
                rejected = false;
            } catch (RejectedExecutionException e) {
                rejected = true;
            }
            executor.awaitTermination(2 * maxQuiescentNanos, TimeUnit.NANOSECONDS);
            if (rejected) {
                assertEquals(0, count.get());
                rejectCount++;
            } else {
                assertEquals(1, count.get());
            }
        }
        assertTrue(String.format("rejectCount=%s", rejectCount),
                   0 < rejectCount && rejectCount < niters);
    }

    /**
     * Tests executor leak.
     */
    @Test
    public void testExecutorLeak() throws Exception {
        final int threadCountBefore = Thread.activeCount();
        final long numOpenFDBefore = TestUtils.getNumOpenFileDescriptors();
        final NioChannelThreadPool pool = new NioChannelThreadPool(
            logger, 128, 1, AsyncEndpointGroupFaultHandler.DEFAULT,
            Executors.newScheduledThreadPool(1));
        final Set<NioChannelExecutor> executors = new HashSet<>();
        for (int i = 0; i < 4096; ++i) {
            /* Create executors concurrently */
            (new Thread(() -> {
                executors.add(pool.next());
            })).start();
        }
        Thread.sleep(2000);
        /* All executors should be terminated */
        executors.forEach((e) -> assertTrue(e.toString(), e.isTerminated()));
        /* Check for created threads */
        final int newThreadCount = Thread.activeCount() - threadCountBefore;
        try {
            assertTrue(
                String.format("New lingering threads: %s", newThreadCount),
                /*
                 * There might be threads created here and there for the
                 * processk, e.g., buffer pool leak detection. So specify a
                 * small non-zero threshold.
                 */
                newThreadCount < 8);
        } catch (AssertionError e) {
            LoggerUtils.fullThreadDump(logger, 0, "");
            throw e;
        }
        /*
         * Check for open file descriptors
         */
        final long newOpenFD =
            TestUtils.getNumOpenFileDescriptors() - numOpenFDBefore;
        assertTrue(
            String.format( "New opened FD: %s", newOpenFD),
            /*
             * Assumes there is not many new file descriptors opened by other
             * processes. Specifies a relatively safe value.
             */
            newOpenFD < 128);
    }

    /**
     * Tests that a task is either rejected or executed.
     *
     * <p>
     * [KVSTORE-458] issue 2
     * [KVSTORE-453] channel handshake stack trace after the channel is
     * rejected by the executor
     */
    @Test
    public void testRejectionException() throws Exception {
        final NioChannelThreadPool pool = new NioChannelThreadPool(
            logger, 1, 1, AsyncEndpointGroupFaultHandler.DEFAULT,
            Executors.newScheduledThreadPool(1));
        for (int i = 0; i < 1024; ++i) {
            final AtomicInteger count = new AtomicInteger(0);
            final NioChannelExecutor executor =
                (new NioChannelThreadPool(
                    logger, 1, 1, AsyncEndpointGroupFaultHandler.DEFAULT,
                    Executors.newScheduledThreadPool(1))).next();
            executor.submit(() -> count.getAndIncrement());
            pool.next().submit(() -> executor.shutdown());
            boolean rejected = false;
            try {
                executor.submit(() -> count.getAndIncrement());
            } catch (RejectedExecutionException e) {
                rejected = true;
            }
            while (true) {
                try {
                    executor.awaitTermination(1, TimeUnit.SECONDS);
                    break;
                } catch (IllegalStateException e) {
                    if (e.getMessage().contains(
                        "Should only call awaitTermination " +
                        "after executor is shutting down")) {
                        Thread.sleep(1);
                        continue;
                    }
                }
            }
            if (rejected) {
                assertEquals(1, count.get());
            } else {
                assertEquals(2, count.get());
            }
        }
    }

    /**
     * Tests that the heartbeat check dumps stack trace when there an executor
     * blocks.
     */
    @Test
    public void testThreadPoolHeartbeatCheckDump() throws Exception {
        final List<LogRecord> records = new ArrayList<>();
        final Logger recordingLogger = createRecordingLogger(records);
        final NioChannelThreadPool threadPool =
            new NioChannelThreadPool(
                recordingLogger, 1, 60,
                AsyncEndpointGroupFaultHandler.DEFAULT,
                Executors.newScheduledThreadPool(1));
        final AtomicBoolean isShutdown = new AtomicBoolean(false);
        threadPool.next().execute(() -> {
            while (!isShutdown.get()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    continue;
                }
            }
        });
        /*
         * Sleep and check that we dumped the stack trace. Sleep the right
         * amount of time to dump once and only once.
         */
        Thread.sleep(stuckExecutorDumpThresholdMillis + heartbeatCheckIntervalMillis);
        assertEquals(1, records.size());
        final String message = records.get(0).getMessage();
        assertTrue(String.format("Incorrect message: %s", message),
                   message.contains("not responsive")
                   && message.contains("Thread.sleep"));
        isShutdown.set(true);
    }

    private Logger createRecordingLogger(List<LogRecord> records) {
        final Logger recordingLogger = Logger.getLogger("recordingLogger");
        class RecordingHandler extends StreamHandler {
            @Override
            public synchronized void publish(LogRecord record) {
                if (record != null) {
                    records.add(record);
                }
            }
        }
        recordingLogger.addHandler(new RecordingHandler());
        return recordingLogger;
    }

    /**
     * Tests that there is no dump when there are no blocking behavior while
     * executors come and go.
     */
    @Test
    public void testThreadPoolHeartbeatCheckNoDump() throws Exception {
        final List<LogRecord> records = new ArrayList<>();
        final Logger recordingLogger = createRecordingLogger(records);
        final int nthreads = 8;
        final int quiescentSeconds = 5;
        final NioChannelThreadPool threadPool =
            new NioChannelThreadPool(
                recordingLogger, nthreads, quiescentSeconds,
                AsyncEndpointGroupFaultHandler.DEFAULT,
                Executors.newScheduledThreadPool(1));
        final AtomicInteger count = new AtomicInteger();
        for (int i = 0; i < nthreads; ++i) {
            threadPool.next().execute(() -> {
                count.getAndIncrement();
            });
        }
        /*
         * Sleep enough time and check that there is no dump.
         */
        Thread.sleep(stuckExecutorDumpThresholdMillis + heartbeatCheckIntervalMillis);
        assertEquals(nthreads, count.get());
        assertEquals(
            records.stream()
            .map((r) -> r.getMessage())
            .collect(Collectors.joining("\n")),
            0, records.size());
        assertEquals(0, threadPool.getPerfTracker().getNumAliveExecutors());
    }

}
