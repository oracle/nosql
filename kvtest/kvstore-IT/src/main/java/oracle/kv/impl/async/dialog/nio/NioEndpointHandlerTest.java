/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async.dialog.nio;

import static oracle.kv.impl.security.ssl.SSLConfig.CLIENT_AUTHENTICATOR;
import static oracle.kv.impl.security.ssl.SSLConfig.KEYSTORE_FILE;
import static oracle.kv.impl.security.ssl.SSLConfig.SERVER_HOST_VERIFIER;
import static oracle.kv.impl.security.ssl.SSLConfig.TRUSTSTORE_FILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import oracle.kv.TestBase;
import oracle.kv.impl.async.AsyncOption;
import oracle.kv.impl.async.DialogContext;
import oracle.kv.impl.async.DialogHandler;
import oracle.kv.impl.async.DialogHandlerFactory;
import oracle.kv.impl.async.DialogHandlerFactoryMap;
import oracle.kv.impl.async.DialogResourceManager;
import oracle.kv.impl.async.EndpointConfigBuilder;
import oracle.kv.impl.async.EndpointHandler;
import oracle.kv.impl.async.EndpointHandlerManager;
import oracle.kv.impl.async.ListenerConfig;
import oracle.kv.impl.async.ListenerConfigBuilder;
import oracle.kv.impl.async.ListenerPortRange;
import oracle.kv.impl.async.MessageOutput;
import oracle.kv.impl.async.dialog.AbstractDialogEndpointHandler;
import oracle.kv.impl.async.dialog.Requester;
import oracle.kv.impl.async.dialog.Responder;
import oracle.kv.impl.async.exception.ConnectionUnknownException;
import oracle.kv.impl.async.exception.InitialHandshakeIOException;
import oracle.kv.impl.async.perf.DialogEndpointGroupPerfTracker;
import oracle.kv.impl.fault.AsyncEndpointGroupFaultHandler;
import oracle.kv.impl.security.ssl.SSLConfig;
import oracle.kv.impl.util.FilterableParameterized;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.SSLTestUtils;
import oracle.kv.util.TestUtils;

import com.sleepycat.je.rep.utilint.net.SSLDataChannel;
import com.sleepycat.je.utilint.TestHook;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

@RunWith(FilterableParameterized.class)
public class NioEndpointHandlerTest extends TestBase {

    private final boolean secure;
    private final EndpointHandlerManager nullManager =
        new EndpointHandlerManager() {
            private final DialogEndpointGroupPerfTracker endpointGroupPerfTracker =
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
        };
    private NioChannelThreadPool threadPool = null;
    private ListenerConfig listenerConfig = null;
    private final byte[] id = new byte[] { (byte) 0x42 };
    private final byte[] smallRequest = new byte[] { (byte) 0x42 };
    private final byte[] largeRequest = new byte[512 * 1024];
    private final byte[] response = new byte[] { (byte) 0x42 };
    private final DialogHandlerFactory dialogHandlerFactory0 =
        new DialogHandlerFactory() {
            @Override
            public void onChannelError(ListenerConfig lc,
                                       Throwable cause,
                                       boolean channelClosed) {
            }
            @Override
            public DialogHandler create() {
                return new Responder(smallRequest, response);
            }};
    private final DialogHandlerFactory dialogHandlerFactory1 =
        new DialogHandlerFactory() {
            @Override
            public void onChannelError(ListenerConfig lc,
                                       Throwable cause,
                                       boolean channelClosed) {
            }
            @Override
            public DialogHandler create() {
                return new Responder(largeRequest, response);
            }};
    private DialogHandlerFactoryMap dialogHandlerFactories =
        new DialogHandlerFactoryMap();

    public NioEndpointHandlerTest(boolean secure) {
        this.secure = secure;
    }

    @Parameters(name="secure={0}")
    public static List<Object[]> genParams() {
        if (PARAMS_OVERRIDE != null) {
            return PARAMS_OVERRIDE;
        }
        return Arrays.asList(new Object[][]{{false}, {true}});
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dialogHandlerFactories.put(0, dialogHandlerFactory0);
        dialogHandlerFactories.put(1, dialogHandlerFactory1);
        threadPool = new NioChannelThreadPool(
            logger, 1, 60, AsyncEndpointGroupFaultHandler.DEFAULT,
            Executors.newScheduledThreadPool(1));
        listenerConfig = new ListenerConfigBuilder()
            .portRange(new ListenerPortRange(6000, 8000))
            .endpointConfigBuilder(createEndpointConfigBuilder(true))
            .build();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (threadPool != null) {
            threadPool.shutdown(true);
        }
    }

    @Test
    public void testConnection() throws Exception {
        final ConnPair connPair = new ConnPair();
        threadPool.next().setReadInterest(
            connPair.clientChannel, connPair.clientHandler, true);
        threadPool.next().setReadInterest(
            connPair.serverChannel, connPair.serverHandler, true);
        final Requester requester = new Requester(id, smallRequest, response);
        threadPool.next().submit(() -> {
            connPair.serverHandler.onChannelReady();
            connPair.clientHandler.onChannelReady();
        });
        connPair.clientHandler.startDialog(0, requester, 5000);
        requester.awaitDone(5000);
        requester.check();
    }

    @Test
    public void testManyConnection() throws Exception {
        final int nConn = 64;
        final ConnPair[] connPairs = new ConnPair[nConn];
        for (int i = 0; i < nConn; ++i) {
            final ConnPair connPair = new ConnPair();
            threadPool.next().setReadInterest(
                connPair.clientChannel, connPair.clientHandler, true);
            threadPool.next().setReadInterest(
                connPair.serverChannel, connPair.serverHandler, true);
            connPairs[i] = connPair;
        }
        final List<Requester> requesters = new ArrayList<>();
        for (int i = 0; i < nConn; ++i) {
            final ConnPair connPair = connPairs[i];
            final Requester requester =
                new Requester(id, smallRequest, response);
            requesters.add(requester);
            threadPool.next().submit(() -> {
                connPair.serverHandler.onChannelReady();
                connPair.clientHandler.onChannelReady();
            });
            connPair.clientHandler.startDialog(0, requester, 5000);
        }
        for (Requester requester: requesters) {
            requester.awaitDone(5000);
            requester.check();
        }
    }

    @Test(timeout=10000)
    public void testChannelWriteBusy() throws Exception {
        final AtomicBoolean flushed = new AtomicBoolean(false);
        AbstractDialogEndpointHandler.handshakeDoneTestHook = (h) -> {
            if (h.isCreator()) {
                AbstractDialogEndpointHandler.flushTestHook = (hh) -> {
                    if (hh.isCreator()) {
                        flushed.set(true);
                    }
                };
            }
        };
        final ConnPair connPair = new ConnPair();
        threadPool.next().setReadInterest(
            connPair.clientChannel, connPair.clientHandler, true);
        threadPool.next().setReadInterest(
            connPair.serverChannel, connPair.serverHandler, true);
        threadPool.next().submit(() -> {
            connPair.serverHandler.onChannelReady();
            connPair.clientHandler.onChannelReady();
        });
        connPair.clientHandler.awaitHandshakeDone();
        connPair.serverHandler.awaitHandshakeDone();
        threadPool.next().deregister(connPair.serverChannel);
        final List<Requester> requesters = new ArrayList<>();
        /*
         * Wait to circumvent a subtle test issue. Immediately after the
         * handshake we will send a heartbeat in the executor thread, which may
         * grab a write lock so that the following request writes will return
         * immediately without actually write, causing writing too much data.
         */
        while (true) {
            if (flushed.get()) {
                break;
            }
            Thread.sleep(50);
        }
        AbstractDialogEndpointHandler.flushTestHook = null;
        while (true) {
            final Requester requester =
                new Requester(id, largeRequest, response);
            final AtomicBoolean onStartCalled = new AtomicBoolean(false);
            requester.setOnStartHook((ctx) -> {
                onStartCalled.set(true);
            });
            connPair.clientHandler.startDialog(1, requester, 5000);
            requesters.add(requester);
            while (!onStartCalled.get()) {
                Thread.sleep(10);
            }
            if (registeredFor(connPair.clientChannel, SelectionKey.OP_WRITE)) {
                break;
            }
        }
        threadPool.next().setReadInterest(
            connPair.serverChannel, connPair.serverHandler, true);
        for (Requester requester : requesters) {
            requester.awaitDone(5000);
            requester.check();
        }
        assertTrue("Socket channel still registered write interest when done",
                   (connPair.clientChannel.
                    keyFor(threadPool.next().getSelector()).interestOps() &
                    SelectionKey.OP_WRITE) == 0);
    }

    private boolean registeredFor(SocketChannel channel, int op) {
        return (channel.keyFor(threadPool.next().getSelector()).
                interestOps() & op) != 0;
    }

    @Test(timeout=10000)
    public void testChannelDialogBusy() throws Exception {
        final AtomicInteger numRequesterDone = new AtomicInteger(0);
        final DialogHandler requester =
            new PermitsTestRequester(numRequesterDone);
        final AtomicInteger numResponding = new AtomicInteger(0);
        final AtomicBoolean proceed = new AtomicBoolean(false);
        final DialogHandler responder =
            new PermitsTestResponder(numResponding, proceed);
        dialogHandlerFactories.put(0, new PermitsTestDialogFactory() {
            @Override
            public DialogHandler create() { return responder; }
        });
        final ConnPair connPair = new ConnPair(2);
        connPair.kickoff();
        /* Starts three requests */
        connPair.clientHandler.startDialog(0, requester, 5000);
        connPair.clientHandler.startDialog(0, requester, 5000);
        connPair.clientHandler.startDialog(0, requester, 5000);
        waitUntilGreaterThanOrEqual(numResponding, 2);
        /* None done, only two responding and read disabled */
        assertEquals(0, numRequesterDone.get());
        assertEquals(2, numResponding.get());
        assertEquals(false, registeredFor(
            connPair.serverChannel, SelectionKey.OP_READ));
        /* Proceed */
        proceed.set(true);
        /* All done and read should be enabled */
        while (true) {
            if (numRequesterDone.get() >= 3) {
                break;
            }
            Thread.sleep(500);
        }
        assertEquals(true, registeredFor(
            connPair.serverChannel, SelectionKey.OP_READ));
    }

    private void waitUntilGreaterThanOrEqual(AtomicInteger counter, int val)
        throws Exception {
        while (true) {
            if (counter.get() >= val) {
                break;
            }
            Thread.sleep(500);
        }
    }

    private class PermitsTestRequester implements DialogHandler {
        private final AtomicInteger numRequesterDone;
        private volatile boolean completedExceptionally = false;

        private PermitsTestRequester(AtomicInteger numRequesterDone) {
            this.numRequesterDone = numRequesterDone;
        }

        @Override
        public void onStart(DialogContext ctx, boolean aborted) {
            ctx.write(new MessageOutput(), true);
        }

        @Override
        public void onCanWrite(DialogContext ctx) { }

        @Override
        public void onCanRead(DialogContext ctx, boolean finished) {
            ctx.read();
            if (finished) {
                numRequesterDone.getAndIncrement();
            }
        }

        @Override
        public void onAbort(DialogContext ctx, Throwable c) {
            completedExceptionally = true;
        }
    }

    private class PermitsTestResponder implements DialogHandler {
        private final AtomicInteger numResponding;
        private final AtomicBoolean proceed;

        private PermitsTestResponder(AtomicInteger numResponding,
                                     AtomicBoolean proceed) {
            this.numResponding = numResponding;
            this.proceed = proceed;
        }

        @Override
        public void onStart(DialogContext ctx, boolean aborted) {
            numResponding.getAndIncrement();
            final Runnable writeTask = new Runnable() {
                    @Override
                    public void run() {
                        if (proceed.get()) {
                            ctx.write(new MessageOutput(), true);
                            numResponding.getAndDecrement();
                        } else {
                            threadPool.next().schedule(
                                this, 100, TimeUnit.MILLISECONDS);
                        }
                    }
            };
            threadPool.next().submit(writeTask);
        }

        @Override
        public void onCanWrite(DialogContext ctx) { }


        @Override
        public void onCanRead(DialogContext ctx, boolean finished) {
            ctx.read();
        }

       @Override
       public void onAbort(DialogContext ctx, Throwable c) { }
    }

    private abstract class PermitsTestDialogFactory
        implements DialogHandlerFactory {
        @Override
        public void onChannelError(
            ListenerConfig lc, Throwable c, boolean closed) { }
        @Override
        abstract public DialogHandler create();
    }

    /**
     * Tests that the channel does not hold permit when there is no activity.
     */
    @Test(timeout=10000)
    public void testNotHoldPermits() throws Exception {
        final AtomicInteger numRequesterDone = new AtomicInteger(0);
        final DialogHandler requester =
            new PermitsTestRequester(numRequesterDone);
        final AtomicInteger numResponding = new AtomicInteger(0);
        final AtomicBoolean proceed = new AtomicBoolean(true);
        final DialogHandler responder =
            new PermitsTestResponder(numResponding, proceed);
        dialogHandlerFactories.put(0, new PermitsTestDialogFactory() {
            @Override
            public DialogHandler create() { return responder; }
        });
        final ConnPair connPair = new ConnPair(2);
        connPair.kickoff();
        connPair.clientHandler.startDialog(0, requester, 5000);
        waitUntilGreaterThanOrEqual(numRequesterDone, 1);
        assertEquals(true, registeredFor(
            connPair.serverChannel, SelectionKey.OP_READ));
        assertEquals(
            2, connPair.getServerResourceManager().getNumAvailablePermits());
    }

    /**
     * Tests that we do not lose permits when connections are closed
     * forcefully.
     */
    @Test(timeout=10000)
    public void testPermitsCleanup() throws Exception {
        final AtomicInteger numRequesterDone = new AtomicInteger(0);
        final DialogHandler requester =
            new PermitsTestRequester(numRequesterDone);
        final AtomicInteger numResponding = new AtomicInteger(0);
        final AtomicBoolean proceed = new AtomicBoolean(false);
        final DialogHandler responder =
            new PermitsTestResponder(numResponding, proceed);
        dialogHandlerFactories.put(0, new PermitsTestDialogFactory() {
            @Override
            public DialogHandler create() { return responder; }
        });
        final ConnPair connPair = new ConnPair(2);
        connPair.kickoff();
        /* Starts two requests */
        connPair.clientHandler.startDialog(0, requester, 5000);
        connPair.clientHandler.startDialog(0, requester, 5000);
        waitUntilGreaterThanOrEqual(numResponding, 2);
        /* None done, only two responding and read disabled */
        assertEquals(0, numRequesterDone.get());
        assertEquals(2, numResponding.get());
        assertEquals(false, registeredFor(
            connPair.serverChannel, SelectionKey.OP_READ));
        /* Re create channel pairs */
        connPair.recreateChannelPair();
        connPair.kickoff();
        /* Starts another two requests */
        connPair.clientHandler.startDialog(0, requester, 5000);
        connPair.clientHandler.startDialog(0, requester, 5000);
        /* Proceed */
        proceed.set(true);
        /* The two new requests should be done and read should be enabled */
        while (true) {
            if (numRequesterDone.get() >= 2) {
                break;
            }
            Thread.sleep(500);
        }
        assertEquals(2, numRequesterDone.get());
        assertEquals(true, registeredFor(
            connPair.serverChannel, SelectionKey.OP_READ));
    }

    /**
     * Tests that we do not leak permits for unknown dialog type.
     */
    @Test(timeout=10000)
    public void testPermitLeakForUnknownDialogType() throws Exception {
        final AtomicInteger numRequesterDone = new AtomicInteger(0);
        final AtomicInteger numResponding = new AtomicInteger(0);
        final AtomicBoolean proceed = new AtomicBoolean(true);
        final DialogHandler responder =
            new PermitsTestResponder(numResponding, proceed);
        dialogHandlerFactories.put(0, new PermitsTestDialogFactory() {
            @Override
            public DialogHandler create() { return responder; }
        });
        final ConnPair connPair = new ConnPair(2);
        connPair.kickoff();
        for (int i = 0; i < 100; ++i) {
            final PermitsTestRequester requester =
                new PermitsTestRequester(numRequesterDone);
            connPair.clientHandler.startDialog(2, requester, 5000);
            PollCondition.await(10, 100, () -> requester.completedExceptionally == true);
        }
        assertEquals(true, registeredFor(
            connPair.serverChannel, SelectionKey.OP_READ));
        assertEquals(
            2, connPair.getServerResourceManager().getNumAvailablePermits());
    }


    private class ConnPair {
        private final ServerSocketChannel serverSocketChannel;
        private final DialogResourceManager serverResourceManager;
        private volatile SocketChannel clientChannel;
        private volatile SocketChannel serverChannel;
        private volatile NioEndpointHandler clientHandler;
        private volatile NioEndpointHandler serverHandler;

        private ConnPair() throws Exception {
            this(Integer.MAX_VALUE);
        }

        private ConnPair(int npermits) throws Exception {
            this.serverSocketChannel = NioUtil.listen(listenerConfig);
            this.serverResourceManager = new DialogResourceManager(npermits);
            init(createChannelPair(serverSocketChannel));
        }

        private DialogResourceManager getServerResourceManager() {
            return serverResourceManager;
        }

        private SocketChannel[] createChannelPair(ServerSocketChannel svchnl)
            throws Exception {

            final int port = svchnl.socket().getLocalPort();
            final SocketChannel cchnl = SocketChannel.open();
            NioUtil.configureSocketChannel(
                cchnl, createEndpointConfigBuilder(false).build());
            cchnl.connect(new InetSocketAddress("localhost", port));
            final SocketChannel schnl = serverSocketChannel.accept();
            NioUtil.configureSocketChannel(
                schnl, createEndpointConfigBuilder(true).build());
            cchnl.finishConnect();
            return new SocketChannel[] { cchnl, schnl };
        }

        private void init(SocketChannel[] channels) throws Exception {
            clientChannel = channels[0];
            serverChannel = channels[1];
            this.clientHandler =
                new NioEndpointHandler(
                    logger, nullManager,
                    createEndpointConfigBuilder(false).build(),
                    true,
                    "Testing",
                    NioUtil.getRemoteAddress(channels[0]).get(),
                    threadPool.next(),
                    threadPool.getBackupExecutor(),
                    dialogHandlerFactories,
                    clientChannel,
                    new DialogResourceManager(Integer.MAX_VALUE),
                    null /* trackerUtils */);
            this.serverHandler =
                new NioEndpointHandler(
                    logger, nullManager,
                    createEndpointConfigBuilder(true).build(),
                    false,
                    "Testing",
                    NioUtil.getRemoteAddress(channels[1]).get(),
                    threadPool.next(),
                    threadPool.getBackupExecutor(),
                    dialogHandlerFactories,
                    serverChannel,
                    serverResourceManager,
                    null /* trackerUtils */);
        }

        private void recreateChannelPair() throws Exception {
            if (clientChannel != null) {
                clientChannel.close();
                clientChannel = null;
            }
            if (serverChannel != null) {
                serverChannel.close();
                clientChannel = null;
            }
            init(createChannelPair(serverSocketChannel));
        }

        private void kickoff() throws Exception {
            threadPool.next().
                setReadInterest(clientChannel, clientHandler, true);
            threadPool.next().
                setReadInterest(serverChannel, serverHandler, true);
            threadPool.next().submit(() -> {
                serverHandler.onChannelReady();
                clientHandler.onChannelReady();
            });
        }
    }

    public EndpointConfigBuilder createEndpointConfigBuilder(boolean server)
        throws Exception {

        final EndpointConfigBuilder builder =
            (new EndpointConfigBuilder()).
            option(AsyncOption.DLG_LOCAL_MAXLEN, 1024 * 1024).
            option(AsyncOption.DLG_REMOTE_MAXLEN, 1024 * 1024).
            option(AsyncOption.DLG_HEARTBEAT_TIMEOUT, 100000).
            option(AsyncOption.DLG_HEARTBEAT_INTERVAL, 100000).
            option(AsyncOption.DLG_CONNECT_TIMEOUT, 100000).
            option(AsyncOption.DLG_IDLE_TIMEOUT, 100000);
        if (secure) {
            builder.sslControl(
                createSSLConfig().makeSSLControl(server, logger));
        }
        return builder;
    }

    private SSLConfig createSSLConfig() {
        final File sslDir = SSLTestUtils.getTestSSLDir();
        final String clientAuthenticator = "dnmatch(CN=Unit Test)";
        final String serverhostVerifier = "dnmatch(CN=Unit Test)";
        final String keystorePath =
            new File(sslDir.getPath(), SSLTestUtils.SSL_KS_NAME).getPath();
        final String truststorePath =
            new File(sslDir.getPath(), SSLTestUtils.SSL_CTS_NAME).getPath();

        final Properties props = new Properties();
        props.setProperty(CLIENT_AUTHENTICATOR, clientAuthenticator);
        props.setProperty(SERVER_HOST_VERIFIER, serverhostVerifier);
        props.setProperty(KEYSTORE_FILE, keystorePath);
        props.setProperty(TRUSTSTORE_FILE, truststorePath);
        SSLConfig config = new SSLConfig(props);
        config.setKeystorePassword(SSLTestUtils.SSL_KS_PWD_DEF.toCharArray());
        return config;
    }

    /**
     * Tests that SSL task execution error at the client throws the right
     * exception.
     *
     * [KVSTORE-1816]
     */
    @Test
    public void testSSLTaskExceptionClient() throws Exception {
        assumeTrue("Skipping non-ssl test", secure);
        tearDowns.add(() -> { SSLDataChannel.taskHook = null; });
        final String injectedMessage = "injected";
        SSLDataChannel.taskHook = new TestHook<SSLDataChannel>() {
            @Override
            public void doHook(SSLDataChannel ch) {
                if (ch.getSSLEngine().getUseClientMode()) {
                    throw new RuntimeException(injectedMessage);
                }
            }
        };
        final ConnPair connPair = new ConnPair();
        threadPool.next().setReadInterest(
            connPair.clientChannel, connPair.clientHandler, true);
        threadPool.next().setReadInterest(
            connPair.serverChannel, connPair.serverHandler, true);
        final Requester requester = new Requester(id, smallRequest, response);
        final AtomicReference<Throwable> errorRef = new AtomicReference<>();
        requester.setOnAbortHook((e) -> errorRef.set(e));
        threadPool.next().submit(() -> {
            connPair.serverHandler.onChannelReady();
            connPair.clientHandler.onChannelReady();
        });
        connPair.clientHandler.startDialog(0, requester, 5000);
        try {
            requester.awaitDone(5000);
            requester.check();
            fail("request should fail");
        } catch (AssertionError e) {
            final Throwable dialogException = errorRef.get();
            if (isRunningAlone("testSSLTaskExceptionClient")) {
                dialogException.printStackTrace();
            }
            TestUtils.checkCause(
                dialogException,
                ConnectionUnknownException.class,
                injectedMessage);
        }
    }

    /**
     * Tests that SSL task execution error at the server terminates the
     * connection quickly instead of hanging until timeout.
     *
     * [KVSTORE-1816]
     */
    @Test
    public void testSSLTaskExceptionServer() throws Exception {
        assumeTrue("Skipping non-ssl test", secure);
        tearDowns.add(() -> { SSLDataChannel.taskHook = null; });
        final String injectedMessage = "injected";
        SSLDataChannel.taskHook = new TestHook<SSLDataChannel>() {
            @Override
            public void doHook(SSLDataChannel ch) {
                if (!ch.getSSLEngine().getUseClientMode()) {
                    throw new RuntimeException(injectedMessage);
                }
            }
        };
        final ConnPair connPair = new ConnPair();
        threadPool.next().setReadInterest(
            connPair.clientChannel, connPair.clientHandler, true);
        threadPool.next().setReadInterest(
            connPair.serverChannel, connPair.serverHandler, true);
        final Requester requester = new Requester(id, smallRequest, response);
        final AtomicReference<Throwable> errorRef = new AtomicReference<>();
        requester.setOnAbortHook((e) -> errorRef.set(e));
        threadPool.next().submit(() -> {
            connPair.serverHandler.onChannelReady();
            connPair.clientHandler.onChannelReady();
        });
        final long ts = System.nanoTime();
        connPair.clientHandler.startDialog(0, requester, 5000);
        try {
            requester.awaitDone(5000);
            requester.check();
            fail("request should fail");
        } catch (AssertionError e) {
            final Throwable dialogException = errorRef.get();
            TestUtils.checkCause(
                dialogException,
                InitialHandshakeIOException.class,
                "Got eof when reading");
            final long durationMillis =
                TimeUnit.MILLISECONDS.convert(
                    System.nanoTime() - ts, TimeUnit.NANOSECONDS);
            assertTrue(
                String.format("failure took too long: %s ms", durationMillis),
                durationMillis < 1000);
        }
    }

    /**
     * Tests that the read interest is de-registered properly when the SSL task
     * needs to be completed.
     */
    @Test
    public void testReadInterestDuringSSLTask() throws Exception {
        assumeTrue("Skipping non-ssl test", secure);
        tearDowns.add(() -> {
            SSLDataChannel.taskHook = null;
            NioEndpointHandler.readHook = null;
        });
        final AtomicBoolean proceed = new AtomicBoolean(false);
        final AtomicInteger onReadCount = new AtomicInteger(0);
        SSLDataChannel.taskHook = new TestHook<SSLDataChannel>() {
            @Override
            public void doHook(SSLDataChannel ch) {
                PollCondition.await(100 /* check every 100 ms */,
                    10000 /* wait at most 10 seconds. */, () -> proceed.get());
            }
        };
        NioEndpointHandler.readHook = r -> {
            onReadCount.getAndIncrement();
        };
        final ConnPair connPair = new ConnPair();
        threadPool.next().setReadInterest(
            connPair.clientChannel, connPair.clientHandler, true);
        threadPool.next().setReadInterest(
            connPair.serverChannel, connPair.serverHandler, true);
        threadPool.next().submit(() -> {
            connPair.serverHandler.onChannelReady();
            connPair.clientHandler.onChannelReady();
        });
        /* Sleep a bit so that the SSL handshake bytes are communicated. */
        Thread.sleep(1000);
        /*
         * Write directly through the socket channel so that there are extra
         * data on the wire to trigger notification. Note that writing through
         * the data channel will not do since it is blocked by the SSL
         * handshake.
         */
        connPair.clientChannel.write(ByteBuffer.allocate(1));
        /*
         * Sleep a bit so that if the issue persists there will be a lot of read
         * notifications.
         */
        Thread.sleep(1000);
        proceed.set(true);
        assertTrue(String.format("too many onRead: %s", onReadCount.get()),
            onReadCount.get() < 3);
    }
}

