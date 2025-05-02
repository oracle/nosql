/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async.dialog;

import static oracle.kv.impl.security.ssl.SSLConfig.CLIENT_AUTHENTICATOR;
import static oracle.kv.impl.security.ssl.SSLConfig.KEYSTORE_FILE;
import static oracle.kv.impl.security.ssl.SSLConfig.SERVER_HOST_VERIFIER;
import static oracle.kv.impl.security.ssl.SSLConfig.TRUSTSTORE_FILE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import javax.net.ssl.SSLSession;

import oracle.kv.TestBase;
import oracle.kv.impl.async.AbstractCreatorEndpoint;
import oracle.kv.impl.async.AsyncOption;
import oracle.kv.impl.async.BytesUtil;
import oracle.kv.impl.async.CreatorEndpoint;
import oracle.kv.impl.async.DialogContext;
import oracle.kv.impl.async.DialogHandler;
import oracle.kv.impl.async.DialogHandlerFactory;
import oracle.kv.impl.async.EndpointConfigBuilder;
import oracle.kv.impl.async.EndpointGroup;
import oracle.kv.impl.async.FutureUtils.CheckedBiConsumer;
import oracle.kv.impl.async.IOBufferPool;
import oracle.kv.impl.async.ListenerConfig;
import oracle.kv.impl.async.ListenerConfigBuilder;
import oracle.kv.impl.async.ListenerPortRange;
import oracle.kv.impl.async.InetNetworkAddress;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.SocketPrepared;
import oracle.kv.impl.async.dialog.netty.NettyEndpointGroup;
import oracle.kv.impl.async.dialog.nio.NioChannelExecutor;
import oracle.kv.impl.async.dialog.nio.NioEndpointGroup;
import oracle.kv.impl.async.dialog.nio.NioEndpointHandler;
import oracle.kv.impl.async.exception.ConnectionTimeoutException;
import oracle.kv.impl.async.exception.InitialConnectIOException;
import oracle.kv.impl.async.exception.InitialHandshakeIOException;
import oracle.kv.impl.async.exception.TemporaryDialogException;
import oracle.kv.impl.fault.AsyncEndpointGroupFaultHandler;
import oracle.kv.impl.security.ssl.SSLConfig;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.EventTrackersManager;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.SSLTestUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.TestUtils;

import com.sleepycat.je.rep.utilint.net.SSLDataChannel;
import com.sleepycat.je.utilint.TestHook;

import org.junit.Test;


public class ServerClientTest extends TestBase {

    private static final Logger staticLogger =
        LoggerUtils.getLogger(ServerClientTest.class, "test");
    private static final byte[] EMPTYBYTES = new byte[0];
    private static final byte[] REQ_SMALL = "Request".getBytes();
    private static final byte[] RESP_SMALL = "Response".getBytes();
    private static final byte[] REQ_LARGE = new byte[10000];
    private static final byte[] RESP_LARGE = new byte[10000];
    private static final byte[] REQ_VERY_LARGE = new byte[1000000];
    private static final byte[] RESP_VERY_LARGE = new byte[1000000];
    private static final int NUM_CORES =
        Runtime.getRuntime().availableProcessors();
    private static final int DEFAULT_DIALOG_TYPE = 42;
    private static final int DEFAULT_TIMEOUT = 5000;
    private static final Random rand = new Random();

    static {
        NettyEndpointGroup.enableLogHandler();
        rand.nextBytes(REQ_LARGE);
        rand.nextBytes(RESP_LARGE);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Logged.resetT0();
        NioChannelExecutor.acceptTestHook = null;
        NettyEndpointGroup.acceptTestHook = null;
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        NioChannelExecutor.acceptTestHook = null;
        NettyEndpointGroup.acceptTestHook = null;
    }

    /**
     * Tests simple one-request-one-response dialogs with NIO.
     */
    @Test
    public void testNioNonSSL() throws Exception {
        EndpointGroup sgroup = new NioEndpointGroup(logger, NUM_CORES);
        EndpointGroup cgroup = new NioEndpointGroup(logger, NUM_CORES);
        testServerClient(sgroup, cgroup, false);
    }

    @Test(timeout=10000)
    public void testNioNonSSLLargePackets() throws Exception {
        EndpointGroup sgroup = new NioEndpointGroup(logger, NUM_CORES);
        EndpointGroup cgroup = new NioEndpointGroup(logger, NUM_CORES);
        testServerClientLargePackets(sgroup, cgroup, false);
    }

    /**
     * Tests simple one-request-one-response dialogs with NIO SSL.
     */
    @Test
    public void testNioSSL() throws Exception {
        EndpointGroup sgroup = new NioEndpointGroup(logger, NUM_CORES);
        EndpointGroup cgroup = new NioEndpointGroup(logger, NUM_CORES);
        testServerClient(sgroup, cgroup, true);
    }

    @Test(timeout=10000)
    public void testNioSSLLargePackets() throws Exception {
        EndpointGroup sgroup = new NioEndpointGroup(logger, NUM_CORES);
        EndpointGroup cgroup = new NioEndpointGroup(logger, NUM_CORES);
        testServerClientLargePackets(sgroup, cgroup, true);
    }

    /**
     * Tests simple one-request-one-response dialogs with Netty.
     */
    @Test
    public void testNettyNonSSL() throws Exception {
        EndpointGroup sgroup = new NettyEndpointGroup(logger, NUM_CORES);
        EndpointGroup cgroup = new NettyEndpointGroup(logger, NUM_CORES);
        testServerClient(sgroup, cgroup, false);
    }

    /**
     * Tests simple one-request-one-response dialogs with Netty SSL.
     */
    @Test
    public void testNettySSL() throws Exception {
        EndpointGroup sgroup = new NettyEndpointGroup(logger, NUM_CORES);
        EndpointGroup cgroup = new NettyEndpointGroup(logger, NUM_CORES);
        testServerClient(sgroup, cgroup, true);
    }

    /**
     * Tests if socket-prepared works correctly.
     */
    @Test
    public void testSocketPrepared() {
        try {
            EndpointGroup serverGroup = new NioEndpointGroup(logger, 1);
            ListenerConfig listenerConfig =
                (new ListenerConfigBuilder()).
                portRange(new ListenerPortRange()).
                build();
            final int size = 8;
            final byte[] actual = new byte[size];
            EndpointGroup.ListenHandle handle =
                serverGroup.listen(
                        listenerConfig,
                        new SocketPrepared() {
                            @Override
                            public void onPrepared(
                                    ByteBuffer preReadData,
                                    Socket socket) {
                                try {
                                    readDataForSocketPrepared(
                                            actual, preReadData, socket);
                                } catch (IOException e) {
                                    throw new Error(e);
                                }
                            }
                            @Override
                            public void onChannelError(
                                    ListenerConfig config,
                                    Throwable t,
                                    boolean channelClosed) {
                                /* do nothing */
                            }
                        });
            final byte[] expected =
                sendDataForSocketPrepared(getPortFromHandle(handle), size);
            Thread.sleep(100);
            assertArrayEquals(expected, actual);
            serverGroup.shutdown(true);
        } catch (Throwable t) {
            fail(LoggerUtils.getStackTrace(t));
        }
    }

    private class SocketPreparedImpl implements SocketPrepared {

        private final CheckedBiConsumer<ByteBuffer, Socket> onPrepared;

        public SocketPreparedImpl(
            CheckedBiConsumer<ByteBuffer, Socket> onPrepared)
        {
            this.onPrepared = onPrepared;
        }

        @Override
        public void onPrepared(ByteBuffer preReadData, Socket socket) {
            try {
                onPrepared.accept(preReadData, socket);
            } catch (Throwable e) {
                throw new Error(e);
            }
        }

        @Override
        public void onChannelError(ListenerConfig config,
                                   Throwable t,
                                   boolean channelClosed) {
            /* do nothing */
        }
    }

    /**
     * Tests error handling for server channel.
     */
    @Test
    public void testServerChannelErrorNio() {
        try {
            testServerChannelError(new NioEndpointGroup(logger, 1));
        } catch (Throwable t) {
            fail(LoggerUtils.getStackTrace(t));
        }
    }

    @Test
    public void testServerChannelErrorNetty() {
        try {
            testServerChannelError(new NettyEndpointGroup(logger, 1));
        } catch (Throwable t) {
            fail(LoggerUtils.getStackTrace(t));
        }
    }

    /**
     * Tests that clients start dialog in onAbort should not cause recursion
     * when reaching dialog limit.
     */
    @Test
    public void testClientRecursionOnLimitNio() throws Exception {
        EndpointGroup group = new NioEndpointGroup(logger, 1);
        try {
            Server server = new Server(group, false /* ssl */,
                                       false /* queueMesgs */);
            EndpointConfigBuilder builder =
                (new EndpointConfigBuilder()).
                option(AsyncOption.DLG_LOCAL_MAXDLGS, 1).
                option(AsyncOption.DLG_REMOTE_MAXDLGS, 1);
            CreatorEndpoint endpoint =
                group.getCreatorEndpoint(
                    "perfName",
                    new InetNetworkAddress("localhost", server.getPort()),
                    InetNetworkAddress.ANY_LOCAL_ADDRESS, builder.build());
            final AtomicInteger stackCount = new AtomicInteger(0);
            final AtomicBoolean overflow = new AtomicBoolean(false);
            DialogHandler handler =
                new DialogHandler() {
                    @Override
                    public void onStart(DialogContext ctx, boolean a) {
                    }
                    @Override
                    public void onCanWrite(DialogContext ctx) {
                    }
                    @Override
                    public void onCanRead(DialogContext ctx, boolean f) {
                    }
                    @Override
                    public void onAbort(DialogContext ctx, Throwable t) {
                        if (stackCount.getAndIncrement() > 64) {
                            overflow.set(true);
                        }
                        endpoint.startDialog(
                            DEFAULT_DIALOG_TYPE, this, DEFAULT_TIMEOUT);
                        stackCount.getAndDecrement();
                    }
                };
            Requester requester =
                new Requester(EMPTYBYTES, REQ_SMALL, RESP_SMALL,
                              new Tester(false /* queueMesgs */), null);
            requester.setOnReadHook(ctx -> {
                endpoint.startDialog(
                    DEFAULT_DIALOG_TYPE, handler, DEFAULT_TIMEOUT);
                endpoint.startDialog(
                    DEFAULT_DIALOG_TYPE, handler, DEFAULT_TIMEOUT);
            });
            endpoint.startDialog(
                DEFAULT_DIALOG_TYPE, requester, DEFAULT_TIMEOUT);
            for (int i = 0; i < 10; ++i) {
                Thread.sleep(100);
                assertFalse("Overflow", overflow.get());
            }
        } catch (Throwable t) {
            fail(LoggerUtils.getStackTrace(t));
        } finally {
            group.shutdown(true);
        }
    }


    /**
     * Tests that clients start dialog in onAbort should not cause recursion
     * when error in onStart.
     */
    @Test
    public void testClientRecursionOnStartErrorNio() throws Exception {
        EndpointGroup group =
            new NioEndpointGroup(logger, 1);
        try {
            Server server = new Server(group, false /* ssl */,
                                       false /* queueMesgs */);
            EndpointConfigBuilder builder = new EndpointConfigBuilder();
            final CreatorEndpoint endpoint =
                group.getCreatorEndpoint(
                    "perfName",
                    new InetNetworkAddress("localhost", server.getPort()),
                    InetNetworkAddress.ANY_LOCAL_ADDRESS, builder.build());
            final AtomicBoolean recursion = new AtomicBoolean(false);
            final AtomicBoolean recursionError = new AtomicBoolean(false);
            final Requester requester =
                new Requester(EMPTYBYTES, REQ_SMALL, RESP_SMALL,
                              new Tester(false /* queueMesgs */), null);
            requester.setOnStartHook(ctx -> {
                throw new RuntimeException("Error to trigger onAbort");
            });
            requester.setOnAbortHook(ctx -> {
                if (recursion.get()) {
                    recursionError.set(true);
                    return;
                }
                recursion.set(true);
                try {
                    endpoint.startDialog(
                        DEFAULT_DIALOG_TYPE, requester, DEFAULT_TIMEOUT);
                } finally {
                    recursion.set(false);
                }
            });
            endpoint.startDialog(
                DEFAULT_DIALOG_TYPE, requester, DEFAULT_TIMEOUT);
            for (int i = 0; i < 10; ++i) {
                Thread.sleep(100);
                assertFalse("Recursion error", recursionError.get());
            }
        } catch (Throwable t) {
            fail(LoggerUtils.getStackTrace(t));
        } finally {
            group.shutdown(true);
        }
    }

    /**
     * Tests if there is a leak in IO buffer pool during normal execution.
     */
    @Test
    public void testNioIOBufferPoolLeakWithNormalExecution() throws Exception {
        /* Clear the impact of other tests */
        clearIOBufferPool();

        EndpointGroup sgroup = new NioEndpointGroup(logger, NUM_CORES);
        EndpointGroup cgroup = new NioEndpointGroup(logger, NUM_CORES);
        testServerClient(sgroup, cgroup,
                         true /* doSSL */, 1 /* nthreads */,
                         1024 /* nrequests */, false /* queueMesgs */,
                         false /* no shutdown */);
        ensureIOBufferPoolNoLeak(sgroup, cgroup, 4, 0, 0, null);
    }

    /**
     * Tests if there is a leak in IO buffer pool when experiencing abrupt
     * socket channel errors.
     */
    @Test
    public void testNioIOBufferPoolLeakWithChannelError()
        throws Exception {
        /* Clear the impact of other tests */
        clearIOBufferPool();

        final int maxQuiescentMillis = 100;
        final int channelTimeoutMillis = 50;
        final int dialogTimeoutMillis = 10;
        NioChannelExecutor.setDefaultMaxQuiescentTimeNanos(
            TimeUnit.MILLISECONDS.toNanos(maxQuiescentMillis), tearDowns);

        final EndpointGroup sgroup =
            new NioEndpointGroup(
                logger, false /* forClientOnly */, 1 /* nthreads */,
                1 /* quiescent */, Integer.MAX_VALUE /* permits */,
                AsyncEndpointGroupFaultHandler.DEFAULT,
                (t, e) -> {});
        final EndpointGroup cgroup =
            new NioEndpointGroup(
                logger, false /* forClientOnly */, 1 /* nthreads */,
                1 /* quiescent */, Integer.MAX_VALUE /* permits */,
                AsyncEndpointGroupFaultHandler.DEFAULT,
                (t, e) -> {});

        /*
         * Cancel the logging task so that the executor can be quiescent for
         * testing
         */
        cgroup.getEventTrackersManager().getIOBufPoolTrackers()
            .cancelLoggingTask();

        final EndpointConfigBuilder builder =
            (new EndpointConfigBuilder()).
            option(AsyncOption.DLG_CONNECT_TIMEOUT, channelTimeoutMillis).
            option(AsyncOption.DLG_IDLE_TIMEOUT, channelTimeoutMillis);
        final Server server = new Server(sgroup, false /* queueMesgs */,
                                         REQ_VERY_LARGE, RESP_VERY_LARGE,
                                         builder);
        try {
            for (int i = 0; i < 64; ++i) {
                testNioIOBufferPoolLeakWithChannelError(
                    cgroup, sgroup, server.getPort(), builder,
                    maxQuiescentMillis, dialogTimeoutMillis,
                    channelTimeoutMillis);
            }
        } finally {
            cgroup.shutdown(true);
            sgroup.shutdown(true);
        }
    }

    private void testNioIOBufferPoolLeakWithChannelError(
        EndpointGroup cgroup,
        EndpointGroup sgroup,
        int port,
        EndpointConfigBuilder builder,
        int maxQuiescentMillis,
        int dialogTimeoutMillis,
        int channelTimeoutMillis) throws Exception
    {
        Client client =
            new Client(cgroup, port,
                       1 /* nthreads */,
                       Integer.MAX_VALUE /* nrequests */,
                       false /* queueMesgs */,
                       REQ_VERY_LARGE, RESP_VERY_LARGE,
                       builder,
                       dialogTimeoutMillis);
        client.run();
        NioChannelExecutor executor =
            (NioChannelExecutor) cgroup.getSchedExecService();
        /* Sleep so that we close at various time */
        Thread.sleep(rand.nextInt(channelTimeoutMillis / 2));
        executor.closeSocketChannels();
        Thread.sleep(10);
        /*
         * Close again in case the first close was issued too early
         * before any connection even started.
         */
        executor.closeSocketChannels();
        /*
         * Sleep until channel timeout or dialog timeout for when the
         * channel was closed but either endpoint does not notice.
         */
        Thread.sleep(
            channelTimeoutMillis + dialogTimeoutMillis + 2 * maxQuiescentMillis);
        assertEquals(executor.toString(), true, executor.isTerminated());
        ensureIOBufferPoolNoLeak(sgroup, cgroup, 0, 0, 0, null);
    }

    /**
     * Tests if there is a leak in IO buffer pool when endpoint groups are
     * created and shut down repeatedly.
     */
    @Test
    public void testNioIOBufferPoolLeakWithGroupShutdown()
        throws Exception {

        /* Clear the impact of other tests */
        clearIOBufferPool();

        for (int i = 0; i < 1024; ++i) {
            getLogger().fine(String.format("iteration %s start", i));
            /*
             * Set up the NioEndpointHandler creation hook, so that we always
             * pass the creation before we start the shutdown.
             */
            final Set<NioEndpointHandler> handlers = ConcurrentHashMap.newKeySet();
            NioEndpointHandler.creationHook = (h) -> {
                handlers.add(h);
            };
            final NioEndpointGroup group = new NioEndpointGroup(logger, 1);
            final EndpointConfigBuilder builder =
                (new EndpointConfigBuilder()).
                option(AsyncOption.DLG_CONNECT_TIMEOUT, 10).
                option(AsyncOption.DLG_IDLE_TIMEOUT, 10);
            final Server server =
                new Server(group,
                           false /* queueMesgs */,
                           REQ_VERY_LARGE, RESP_VERY_LARGE,
                           builder);
            Client client =
                new Client(group, server.getPort(),
                           1 /* nthreads */, 1 /* nrequests */,
                           false /* queueMesgs */,
                           REQ_VERY_LARGE, RESP_VERY_LARGE,
                           builder, 10 /* dialogTimeout */);
            client.run();
            PollCondition.await(10 /* 10ms check period */,
                1000 /* 1s timeout */, () -> handlers.size() == 2);
            Thread.sleep(rand.nextInt(10));
            final NioChannelExecutor executor =
                (NioChannelExecutor) group.getSchedExecService();
            getLogger().fine("shutting down the group");
            group.shutdown(false);
            final int awaitTimeoutSecs = 5;
            boolean terminated =
                executor.awaitTermination(awaitTimeoutSecs, TimeUnit.SECONDS);
            assertTrue(String.format("Executor not terminated after %d s: %s",
                                     awaitTimeoutSecs, executor),
                       terminated);
            getLogger().fine("channel executor terminated");
            /*
             * Waits for handlers to terminate, since that is also done
             * asynchronously.
             */
            for (NioEndpointHandler h : handlers) {
                h.awaitTermination(1000);
            }
            /*
             * Sleep a bit more to make sure the task submit to the backup
             * executor is not rejected.
             */
            Thread.sleep(50);
            /*
             * Waits for the client request to be done.
             */
            for (Requester requester : client.getRequesters()) {
                requester.awaitDone(1000);
            }
            terminated =
                group.awaitBackupExecutorQuiescence(
                    awaitTimeoutSecs, TimeUnit.SECONDS);
            assertTrue(
                String.format(
                    "Backup executor not terminated after %d s",
                    awaitTimeoutSecs),
                terminated);
            getLogger().fine("back-up executor terminated");
            ensureIOBufferPoolNoLeak(group, group, 0, 0, 0, "iteration " + i);
        }

    }

    private void clearIOBufferPool() {
        IOBufferPool.CHNL_IN_POOL.clearUse();
        IOBufferPool.MESG_OUT_POOL.clearUse();
        IOBufferPool.CHNL_OUT_POOL.clearUse();
    }

    private void ensureIOBufferPoolNoLeak(EndpointGroup sgroup,
                                          EndpointGroup cgroup,
                                          int chnlInUse,
                                          int mesgOutUse,
                                          int chnlOutUse,
                                          String message) {
        final EventTrackersManager strackers =
            sgroup.getEventTrackersManager();
        final String sleak =
            (strackers == null)
                ? "no data"
                : strackers.getIOBufPoolTrackers()
                .getLeakTrackersJson(System.currentTimeMillis(), true)
                .toString();
        final EventTrackersManager ctrackers =
            cgroup.getEventTrackersManager();
        final String cleak =
            (strackers == null)
                ? "no data"
                : ctrackers.getIOBufPoolTrackers()
                .getLeakTrackersJson(System.currentTimeMillis(), true)
                .toString();
        final String leak = String.format("sleak=%s, cleak=%s, message=%s",
            sleak, cleak, message);
        int use;
        use = IOBufferPool.CHNL_IN_POOL.getNumInUse();
        assertTrue(
            String.format(
                "Channel input leaking, curr use=%s, leak=%s",
                use, leak),
            use <= chnlInUse);
        use = IOBufferPool.MESG_OUT_POOL.getNumInUse();
        assertTrue(
            String.format(
                "Message output leaking, curr use=%s, leak=%s",
                use, leak),
            use <= mesgOutUse);
        use = IOBufferPool.CHNL_OUT_POOL.getNumInUse();
        assertTrue(
            String.format(
                "Channel output leaking, curr use=%s, leak=%s",
                use, leak),
            use <= chnlOutUse);
    }

    private void testServerClient(EndpointGroup sgroup,
                                  EndpointGroup cgroup,
                                  boolean doSSL) throws Exception {
        testServerClient(sgroup, cgroup, doSSL, 64, 64, true, false);
    }

    private void testServerClient(EndpointGroup sgroup,
                                  EndpointGroup cgroup,
                                  boolean doSSL,
                                  int numThreads,
                                  int numRequestsPerThread,
                                  boolean queueMesgs,
                                  boolean noShutdown) throws Exception {
        Server server = null;
        Client client = null;
        try {
            server = new Server(sgroup, doSSL, true /* queueMesgs */);
            client =
                new Client(cgroup, server.getPort(), doSSL,
                           numThreads,
                           numRequestsPerThread,
                           queueMesgs);
            client.run();
            client.await(10000);
            client.check();
            server.check();
        } catch (Exception e) {
            StringBuilder sb = new StringBuilder(LoggerUtils.getStackTrace(e));
            if (client != null) {
                sb.append(String.format("Client logging\n%s", client));
            }
            if (server != null) {
                sb.append(String.format("Server logging\n%s", server));
            }
            throw new Exception(sb.toString());
        } finally {
            if (!noShutdown) {
                cgroup.shutdown(true);
                sgroup.shutdown(true);
            }
        }
    }

    private void testServerClientLargePackets(
                     EndpointGroup sgroup,
                     EndpointGroup cgroup,
                     boolean doSSL) throws Exception {
        try {
            Server server = new Server(sgroup, doSSL, false /* queueMesgs */,
                                       REQ_LARGE, RESP_LARGE);
            Client client =
                new Client(cgroup, server.getPort(), doSSL,
                           4 /* num threads */,
                           64 /* num requests per thread */,
                           false /*queueMesgs */,
                           REQ_LARGE, RESP_LARGE);
            client.run();
            client.await(5000);
            client.check();
            server.check();
        } finally {
            cgroup.shutdown(true);
            sgroup.shutdown(true);
        }
    }

    public static SSLConfig createSSLConfig() {
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

    private static class Server extends Tester {
        private final Queue<Responder> responders =
            new ConcurrentLinkedQueue<Responder>();
        private final EndpointGroup.ListenHandle handle;
        private final byte[] request;
        private final byte[] response;
        Server(EndpointGroup endpointGroup,
               boolean doSSL,
               boolean queueMesgs) throws Exception {
            this(endpointGroup, doSSL, queueMesgs, REQ_SMALL, RESP_SMALL);
        }
        Server(EndpointGroup endpointGroup,
               boolean doSSL,
               boolean queueMesgs,
               byte[] request,
               byte[] response) throws Exception {
            this(endpointGroup, queueMesgs, request, response,
                 getEndpointConfigBuilder(doSSL));
        }
        Server(EndpointGroup endpointGroup,
               boolean queueMesgs,
               byte[] request,
               byte[] response,
               EndpointConfigBuilder builder) throws Exception {
            this(endpointGroup, queueMesgs, request, response,
                 (new ListenerConfigBuilder()).
                 portRange(new ListenerPortRange()).
                 endpointConfigBuilder(builder));
        }
        Server(EndpointGroup endpointGroup,
               boolean queueMesgs,
               byte[] request,
               byte[] response,
               ListenerConfigBuilder builder) throws Exception {
            super(queueMesgs);
            this.handle =
                endpointGroup.listen(
                    builder.build(),
                    DEFAULT_DIALOG_TYPE, new ResponderFactory());
            this.request = request;
            this.response = response;
        }
        int getPort() throws Exception {
            return getPortFromHandle(handle);
        }
        void check() {
            checkExceptions();
            for (Responder r : responders) {
                r.check();
            }
        }
        class ResponderFactory implements DialogHandlerFactory {
            @Override
            public DialogHandler create() {
                Responder responder =
                    new Responder(request, response, Server.this);
                responders.add(responder);
                return responder;
            }
            @Override
            public void onChannelError(ListenerConfig config,
                                       Throwable t,
                                       boolean channelClosed) {
                logError(t);
            }
        }
    }

    private static class Client extends Tester {
        private final ExecutorService executor;
        private final CreatorEndpoint endpoint;
        private final int nthreads;
        private final int nrequests;
        private final Queue<Requester> requesters =
            new ConcurrentLinkedQueue<Requester>();
        private final byte[] request;
        private final byte[] response;
        private final int dialogTimeout;
        Client(EndpointGroup endpointGroup,
               int port,
               boolean doSSL,
               int nthreads,
               int nrequests,
               boolean queueMesgs) throws Exception {
            this(endpointGroup, port, nthreads, nrequests, queueMesgs,
                 REQ_SMALL, RESP_SMALL, getEndpointConfigBuilder(doSSL),
                 DEFAULT_TIMEOUT);
        }
        Client(EndpointGroup endpointGroup,
               int port,
               boolean doSSL,
               int nthreads,
               int nrequests,
               boolean queueMesgs,
               byte[] request,
               byte[] response) throws Exception {
            this(endpointGroup, port, nthreads, nrequests, queueMesgs,
                 request, response, getEndpointConfigBuilder(doSSL),
                 DEFAULT_TIMEOUT);
        }
        Client(EndpointGroup endpointGroup,
               int port,
               int nthreads,
               int nrequests,
               boolean queueMesgs,
               byte[] request,
               byte[] response,
               EndpointConfigBuilder configBuilder,
               int dialogTimeout ) throws Exception {
            super(queueMesgs);
            this.executor = endpointGroup.getSchedExecService();
            this.endpoint =
                endpointGroup.getCreatorEndpoint(
                    "perfName",
                    new InetNetworkAddress("localhost", port),
                    InetNetworkAddress.ANY_LOCAL_ADDRESS,
                    configBuilder.build());
            this.nthreads = nthreads;
            this.nrequests = nrequests;
            this.request = request;
            this.response = response;
            this.dialogTimeout = dialogTimeout;
        }
        public void run() {
            semaphoreAcquire();
            try {
                for (int i = 0; i < nthreads; ++i) {
                    semaphoreAcquire();
                    startRequest();
                }
            } catch (Throwable t) {
                logError(t);
            } finally {
                semaphoreRelease();
            }
        }
        void check() {
            checkExceptions();
            for (Requester requester : requesters) {
                requester.check();
            }
        }
        void startRequest() {
            startRequest(nrequests);
        }
        void startRequest(int n) {
            executor.submit(() -> {
                Requester requester =
                    new Requester(
                            EMPTYBYTES, request, response,
                            Client.this, null);
                requester.setOnReadHook(ctx -> {
                    int left = n - 1;
                    if (left > 0) {
                        startRequest(left);
                    } else {
                        semaphoreRelease();
                    }
                });
                requester.setOnAbortHook(c -> {
                    semaphoreRelease();
                });
                requesters.add(requester);
                endpoint.startDialog(
                        DEFAULT_DIALOG_TYPE, requester, dialogTimeout);
            });
        }

        public Queue<Requester> getRequesters() {
            return requesters;
        }
    }

    private static EndpointConfigBuilder
        getEndpointConfigBuilder(boolean doSSL) throws Exception {

        EndpointConfigBuilder builder =
            (new EndpointConfigBuilder()).
            option(AsyncOption.DLG_LOCAL_MAXDLGS, Integer.MAX_VALUE).
            option(AsyncOption.DLG_REMOTE_MAXDLGS, Integer.MAX_VALUE);
        if (doSSL) {
            builder.sslControl(
                createSSLConfig().makeSSLControl(false, staticLogger));
        }
        return builder;
    }

    private byte[] sendDataForSocketPrepared(int port, int size)
        throws IOException {

        Socket socket = new Socket("localhost", port);
        return sendDataForSocketPrepared(socket, size);
    }

    private byte[] sendDataForSocketPrepared(Socket socket, int size)
        throws IOException {

        DataOutputStream out =
            new DataOutputStream(socket.getOutputStream());
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; ++i) {
            bytes[i] = (byte) i;
        }
        out.write(bytes);
        out.flush();
        try {
            socket.close();
        } catch (IOException e) {
            /* do nothing */
        }
        return bytes;
    }

    private void readDataForSocketPrepared(byte[] bytes,
                                           ByteBuffer preReadData,
                                           Socket socket) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        buf.put(preReadData);
        DataInputStream in =
            new DataInputStream(socket.getInputStream());
        int pos = buf.position();
        in.readFully(bytes, pos, bytes.length - pos);
        logger.log(Level.INFO, "Read data bytes: {0}",
                BytesUtil.toString(bytes, 0, bytes.length));
        try {
            socket.close();
        } catch (IOException e) {
            /* do nothing */
        }
    }

    private static int getPortFromHandle(EndpointGroup.ListenHandle handle)
        throws Exception {

        return handle.getLocalAddress().port();
    }

    private void testServerChannelError(EndpointGroup endpointGroup)
        throws Exception {

        NioChannelExecutor.acceptTestHook = (ch) -> {
            throw new RuntimeException();
        };
        NettyEndpointGroup.acceptTestHook = (ch) -> {
            throw new RuntimeException();
        };
        ListenerConfig listenerConfig =
            (new ListenerConfigBuilder()).
            portRange(new ListenerPortRange()).
            endpointConfigBuilder(getEndpointConfigBuilder(false)).
            build();
        ChannelErrorHandler handler1 = new ChannelErrorHandler();
        ChannelErrorHandler handler2 = new ChannelErrorHandler();
        EndpointGroup.ListenHandle handle =
             endpointGroup.listen(listenerConfig, handler1);
        endpointGroup.listen(listenerConfig, 0, handler2);
        Socket socket = null;
        try {
            socket = new Socket("localhost", getPortFromHandle(handle));
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    /* do nothing */
                }
            }
        }
        Thread.sleep(50);
        handler1.check();
        handler2.check();
        endpointGroup.shutdown(true);
    }

    private class ChannelErrorHandler
            implements SocketPrepared, DialogHandlerFactory {
        private volatile boolean onPreparedCalled = false;
        private volatile boolean createCalled = false;
        private volatile boolean onErrorCalled = false;
        @Override
        public void onPrepared(ByteBuffer preReadData, Socket socket) {
            onPreparedCalled = true;
        }
        @Override
        public DialogHandler create() {
            createCalled = true;
            return new DialogHandler() {
                @Override
                public void onStart(DialogContext dc, boolean a) {
                    fail("Called onStart");
                }
                @Override
                public void onCanWrite(DialogContext dc) {
                    fail("Called onCanWrite");
                }
                @Override
                public void onCanRead(DialogContext dc, boolean f) {
                    fail("Called onCanRead");
                }
                @Override
                public void onAbort(DialogContext dc, Throwable c) {
                    fail("Called onAbort");
                }
            };
        }
        @Override
        public void onChannelError(ListenerConfig config,
                                   Throwable t,
                                   boolean channelClosed) {
            onErrorCalled = true;
        }
        public void check() {
            assertFalse(onPreparedCalled);
            assertFalse(createCalled);
            assertTrue(onErrorCalled);
        }
    }

    /**
     * Tests the usual case for connection accept management where connections
     * timed out inside backlog.
     */
    @Test
    public void testNioAcceptBasic() throws Exception {

        final EndpointGroup sgroup = new NioEndpointGroup(logger, 1);
        try {
            for (int nconn : new int[] { 2, 8 }) {
                testAcceptBasic(sgroup, nconn);
            }
        } finally {
            sgroup.shutdown(false);
        }
    }

    @Test
    public void testNettyAcceptBasic() throws Exception {

        final EndpointGroup sgroup = new NettyEndpointGroup(logger, 1);
        try {
            for (int nconn : new int[] { 2, 8 }) {
                testAcceptBasic(sgroup, nconn);
            }
        } finally {
            sgroup.shutdown(false);
        }
    }

    private void testAcceptBasic(EndpointGroup sgroup,
                                 int nconn) throws Exception {
        final List<EndpointGroup> groups = new ArrayList<>();
        try {
            final EndpointConfigBuilder builder =
                (new EndpointConfigBuilder()).
                option(AsyncOption.DLG_CONNECT_TIMEOUT, 500).
                option(AsyncOption.DLG_HEARTBEAT_INTERVAL, 100).
                option(AsyncOption.DLG_HEARTBEAT_TIMEOUT, 2).
                option(AsyncOption.DLG_IDLE_TIMEOUT, Integer.MAX_VALUE);
            final ListenerConfigBuilder listenBuilder =
                (new ListenerConfigBuilder()).
                portRange(new ListenerPortRange()).
                endpointConfigBuilder(builder).
                option(AsyncOption.DLG_ACCEPT_MAX_ACTIVE_CONNS, nconn);
            final Server server = new Server(sgroup, false /* queueMesgs */,
                                             REQ_SMALL, RESP_SMALL,
                                             listenBuilder);
            final NetworkAddress addr =
                new InetNetworkAddress("localhost", server.getPort());
            final List<Requester> requesters = new ArrayList<>();

            /* Establish with nconn connections */
            startEndpointGroupAndRequests(
                groups, requesters, addr, builder, nconn, 0);
            /* Check requester status */
            for (int i = 0; i < nconn; ++i) {
                final Requester requester = requesters.get(i);
                checkRequester(requester);
            }
            /*
             * Establish another nconn / 2 connections and they should fail due
             * to connect timeout exception.
             */
            startEndpointGroupAndRequests(
                groups, requesters, addr, builder, nconn / 2, nconn);
            for (int i = nconn; i < nconn + nconn / 2; ++i) {
                final Requester requester = requesters.get(i);
                requester.awaitDone(DEFAULT_TIMEOUT);
                try {
                    requester.check();
                    fail(String.format("Requester %s should be rejected", i));
                } catch (Throwable t) {
                    /* error expected */
                    logger.log(Level.FINE,
                               String.format("Request %s failed", i));
                }
            }
            /*
             * Shutdown nconn / 2 groups so that new connections can be
             * accepted. Due to the backlog, the last conn / 2 will still get
             * accepted even when the client side is closed. These connections
             * will terminate agan due to connect timeout.
             */
            for (int i = 0; i < nconn / 2; ++i) {
                groups.get(i).shutdown(true);
            }
            Thread.sleep(1000);
            /* Establish another nconn/2 connections and they should succeed */
            startEndpointGroupAndRequests(
                groups, requesters, addr, builder, nconn/2, 2 * nconn);
            for (int i = nconn + nconn / 2; i < 2 * nconn; ++i) {
                final Requester requester = requesters.get(i);
                checkRequester(requester);
            }
        } finally {
            for (EndpointGroup group : groups) {
                group.shutdown(true);
            }
        }
    }

    private void checkRequester(Requester requester) throws Exception {
        try {
            requester.awaitDone(DEFAULT_TIMEOUT);
            requester.check();
        } catch (AssertionError e) {
            logger.log(
                Level.INFO,
                String.format(
                    "Request %s failed", new String(requester.getId())));
            throw e;
        }
        logger.log(
            Level.FINE,
            String.format(
                "Request %s succeeded", new String(requester.getId())));
    }

    private void startEndpointGroupAndRequests(List<EndpointGroup> groups,
                                               List<Requester> requesters,
                                               NetworkAddress addr,
                                               EndpointConfigBuilder builder,
                                               int count,
                                               int offset)
        throws Exception {
        for (int i = 0; i < count; ++i) {
            final EndpointGroup group = new NioEndpointGroup(logger, 1);
            groups.add(group);
            final CreatorEndpoint endpoint =
                group.getCreatorEndpoint(
                    "perfName", addr,
                    InetNetworkAddress.ANY_LOCAL_ADDRESS, builder.build());
            final Requester requester =
                new Requester((String.format("%s, %s", (offset + i), endpoint))
                              .getBytes(),
                              REQ_SMALL, RESP_SMALL);
            requesters.add(requester);
            endpoint.startDialog(
                DEFAULT_DIALOG_TYPE, requester, DEFAULT_TIMEOUT);
        }

    }

    /**
     * Tests accept management with long timeout.
     */
    @Test
    public void testNioAcceptLongTimeout() throws Exception {
        final EndpointGroup sgroup = new NioEndpointGroup(logger, 1);
        final List<EndpointGroup> groups = new ArrayList<>();
        final int nconn = 2;
        try {
            final EndpointConfigBuilder builder =
                (new EndpointConfigBuilder()).
                option(AsyncOption.DLG_CONNECT_TIMEOUT, Integer.MAX_VALUE).
                option(AsyncOption.DLG_IDLE_TIMEOUT, Integer.MAX_VALUE);
            final ListenerConfigBuilder listenBuilder =
                (new ListenerConfigBuilder()).
                portRange(new ListenerPortRange()).
                endpointConfigBuilder(builder).
                option(AsyncOption.DLG_ACCEPT_MAX_ACTIVE_CONNS, nconn).
                option(AsyncOption.DLG_CLEAR_BACKLOG_INTERVAL,
                       Integer.MAX_VALUE);
            final Server server = new Server(sgroup, false /* queueMesgs */,
                                             REQ_SMALL, RESP_SMALL,
                                             listenBuilder);
            final NetworkAddress addr =
                new InetNetworkAddress("localhost", server.getPort());
            final List<Requester> requesters = new ArrayList<>();

            /* Establish with nconn connections */
            startEndpointGroupAndRequests(
                groups, requesters, addr, builder, nconn, 0);
            Thread.sleep(1000);
            /* Establish another nconn connections. */
            startEndpointGroupAndRequests(
                groups, requesters, addr, builder, nconn, nconn);
            Thread.sleep(1000);
            /*
             * Shutdown nconn groups so that backlog'ed connections can be
             * accepted.
             */
            for (int i = 0; i < nconn; ++i) {
                groups.get(i).shutdown(true);
            }
            Thread.sleep(1000);
            /* Establish another nconn/2 connections and they should succeed */
            for (Requester requester : requesters) {
                requester.awaitDone(DEFAULT_TIMEOUT);
                requester.check();
            }
        } finally {
            for (EndpointGroup group : groups) {
                group.shutdown(true);
            }
            sgroup.shutdown(true);
        }
    }

    /**
     * Tests backlog clear for accept management.
     */
    @Test
    public void testNioAcceptClearBacklog() throws Exception {
        final EndpointGroup sgroup = new NioEndpointGroup(logger, 1);
        final List<EndpointGroup> groups = new ArrayList<>();
        final int nconn = 2;
        try {
            final EndpointConfigBuilder builder =
                (new EndpointConfigBuilder()).
                option(AsyncOption.DLG_CONNECT_TIMEOUT, Integer.MAX_VALUE).
                option(AsyncOption.DLG_IDLE_TIMEOUT, Integer.MAX_VALUE);
            final ListenerConfigBuilder listenBuilder =
                (new ListenerConfigBuilder()).
                portRange(new ListenerPortRange()).
                endpointConfigBuilder(builder).
                option(AsyncOption.DLG_CLEAR_BACKLOG_INTERVAL, 1000).
                option(AsyncOption.DLG_ACCEPT_MAX_ACTIVE_CONNS, nconn);
            final Server server = new Server(sgroup, false /* queueMesgs */,
                                             REQ_SMALL, RESP_SMALL,
                                             listenBuilder);
            final NetworkAddress addr =
                new InetNetworkAddress("localhost", server.getPort());
            final List<Requester> requesters = new ArrayList<>();

            /* Establish with nconn connections */
            startEndpointGroupAndRequests(
                groups, requesters, addr, builder, nconn, 0);
            Thread.sleep(1000);
            /* Establish another nconn connections. */
            startEndpointGroupAndRequests(
                groups, requesters, addr, builder, nconn, nconn);
            Thread.sleep(1000);
            /* Establish another nconn/2 connections and they should succeed */
            for (int i = 0; i < nconn; ++i) {
                final Requester requester = requesters.get(i);
                requester.awaitDone(DEFAULT_TIMEOUT);
                requester.check();
                logger.log(Level.FINE,
                           String.format("Request %s succeeded", i));
            }
            for (int i = nconn; i < nconn + nconn; ++i) {
                final Requester requester = requesters.get(i);
                requester.awaitDone(1000);
                try {
                    requester.check();
                    fail(String.format("Requester %s should be rejected", i));
                } catch (Throwable t) {
                    /* error expected */
                    logger.log(Level.FINE,
                               String.format("Request %s failed", i));
                }
            }
        } finally {
            for (EndpointGroup group : groups) {
                group.shutdown(true);
            }
            sgroup.shutdown(true);
        }
    }

    /**
     * Tests with selector throwing IOException.
     */
    @Test(timeout=20000)
    public void testSelectorIOException() throws Exception {
        final AtomicBoolean errorPeriod = new AtomicBoolean(false);
        final Set<Selector> selectors = ConcurrentHashMap.newKeySet();
        /*
         * Simulates that selectors created in the error period always throws
         * exception
         */
        NioChannelExecutor.selectorTestHook = (s) -> {
            if (errorPeriod.get()) {
                selectors.add(s);
                throw new IOException(
                    "Selector has error during error period");
            }
            if (selectors.contains(s)) {
                throw new IOException(
                    "Selector created during the error period remains bad");
            }
        };

        final EndpointGroup sgroup = new NioEndpointGroup(logger, NUM_CORES);
        final EndpointGroup cgroup = new NioEndpointGroup(logger, NUM_CORES);
        final Server server = new Server(sgroup, false, false);
        final CreatorEndpoint endpoint = cgroup.getCreatorEndpoint("perfName",
            new InetNetworkAddress("localhost", server.getPort()),
            InetNetworkAddress.ANY_LOCAL_ADDRESS,
            getEndpointConfigBuilder(false).build());
        /* Initially things are fine */
        for (int i = 0; i < 4; ++i) {
            final Requester requester =
                new Requester(EMPTYBYTES, REQ_SMALL, RESP_SMALL);
            endpoint.startDialog(DEFAULT_DIALOG_TYPE, requester, 1000);
            Thread.sleep(1000);
            requester.check();
        }
        /* Starts the error period */
        errorPeriod.set(true);
        for (int i = 0; i < 4; ++i) {
            final Requester requester =
                new Requester(EMPTYBYTES, REQ_SMALL, RESP_SMALL);
            endpoint.startDialog(DEFAULT_DIALOG_TYPE, requester, 1000);
            Thread.sleep(1000);
            try {
                requester.check();
                fail("Requester should fail");
            } catch (AssertionError e) {
                /* expected */
            }
        }
        /* Ends the error period */
        errorPeriod.set(false);
        Thread.sleep(1000);
        for (int i = 0; i < 4; ++i) {
            final Requester requester =
                new Requester(EMPTYBYTES, REQ_SMALL, RESP_SMALL);
            endpoint.startDialog(DEFAULT_DIALOG_TYPE, requester, 1000);
            Thread.sleep(1000);
            requester.check();
        }
    }

    /**
     * Tests if many non-async connections should not create {@link
     * SSLDataChannel}s, so that it does not use more resources than it should.
     *
     * [KVSTORE-390], [KVSTORE-700].
     */
    @Test
    public void testNonAsyncConnNoSSLDataChannel() throws Exception {
        final int numSockets = 4;
        final Socket[] sockets = new Socket[numSockets];
        /*
         * Make sure that no SSLDataChannels are created -- this hook will
         * throw on a create.
         */
        SSLDataChannel.creationHook = new SSLDataChannelCreationHook();

        try {
            EndpointGroup serverGroup = new NioEndpointGroup(logger, 1);
            ListenerConfig listenerConfig =
                (new ListenerConfigBuilder()).
                portRange(new ListenerPortRange()).
                endpointConfigBuilder(
                    getEndpointConfigBuilder(true /* doSSL */)).
                build();
            final int size = 8;
            final List<byte[]> actualList =
                Collections.synchronizedList(new ArrayList<>());
            EndpointGroup.ListenHandle handle = serverGroup.listen(
                listenerConfig,
                new SocketPreparedImpl((p, s) -> {
                    final byte[] actual = new byte[size];
                    actualList.add(actual);
                    readDataForSocketPrepared(actual, p, s);
                }));
            /*
             * Creates a lot sockets but do not send data so that the
             * connections will linger there. The server-side will create the
             * endpoint handlers, but since there is no data, no new ssl data
             * channels will be created. Any such creation will trigger the
             * SSLDataChannelCreationHook and throws exception.
             */
            final int port = getPortFromHandle(handle);
            for (int i = 0; i < numSockets; ++i) {
                sockets[i] = new Socket("localhost", port);
            }
            Thread.sleep(1000);
            /*
             * Proceeds to send data just to make sure the server is working,
             * i.e., no exceptions were thrown. Still, no ssl data channel
             * should be created because the pre-read/write handshake does not
             * need SSL.
             */
            final List<byte[]> expectList = new ArrayList<>();
            for (Socket s : sockets) {
                expectList.add(sendDataForSocketPrepared(s, size));
            }
            final boolean done =
                new PollCondition(1000, 10000) {
                    @Override
                    protected boolean condition() {
                        if (actualList.size() == numSockets) {
                            return true;
                        }
                        return false;
                    }
            }.await();
            assertTrue(done);
            for (int i = 0; i < numSockets; ++i) {
                assertArrayEquals(expectList.get(i), actualList.get(i));
            }
            serverGroup.shutdown(true);
        } catch (Throwable t) {
            fail(LoggerUtils.getStackTrace(t));
            for (Socket s : sockets) {
                if (s != null) {
                    s.close();
                }
            }
        } finally {
            SSLDataChannel.creationHook = null;
        }
    }

    private class SSLDataChannelCreationHook implements TestHook<SSLSession> {
        @Override
        public void doHook() {
            throw new UnsupportedOperationException();
        }
        @Override
        public SSLSession getHookValue() {
            throw new UnsupportedOperationException();
        }
        @Override
        public void doIOHook() {
            throw new UnsupportedOperationException();
        }
        @Override
        public void hookSetup() {
            throw new UnsupportedOperationException();
        }
        @Override
        public void doHook(SSLSession session) {
            throw new IllegalStateException(
                "Not expected to create ssl data channel");
        }
    }

    /**
     * Tests a connect exception throws a InitialConnectIOException or an
     * InitialHandshakeIOException.
     *
     * The test throws exception when accepting a connection. The client side
     * may detect the problem during or after pre-write. It will throw
     * InitialConnectIOException during pre-write or
     * InitialHandshakeIOException after.
     *
     * [KVSTORE-1237] [KVSTORE-1355]
     */
    @Test
    public void testInitialException() throws Exception {
        NioChannelExecutor.acceptTestHook = (s) -> {
            throw new RuntimeException();
        };
        final EndpointGroup sgroup = new NioEndpointGroup(logger, NUM_CORES);
        final EndpointGroup cgroup = new NioEndpointGroup(logger, NUM_CORES);
        final Server server = new Server(sgroup, false, false);
        final NetworkAddress rAddr =
            new InetNetworkAddress("localhost", server.getPort());
        final CreatorEndpoint endpoint = cgroup.getCreatorEndpoint("perfName",
                rAddr,
                InetNetworkAddress.ANY_LOCAL_ADDRESS,
                getEndpointConfigBuilder(false).build());
        final AtomicReference<Throwable> errorRef = new AtomicReference<>();
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
                errorRef.set(cause);
            }
        };
        endpoint.startDialog(DEFAULT_DIALOG_TYPE, dialogHandler, 1000);
        PollCondition.await(100, 1000, () -> errorRef.get() != null);
        final Throwable cause = errorRef.get().getCause();
        assertTrue(String.format("Expected %s or %s, got %s",
                                 InitialConnectIOException.class.getSimpleName(),
                                 InitialHandshakeIOException.class.getSimpleName(),
                                 CommonLoggerUtils.getStackTrace(cause)),
                   ((cause instanceof InitialConnectIOException)
                    || (cause instanceof InitialHandshakeIOException)));
        String errMsg = "";
        if (cause instanceof InitialConnectIOException) {
            errMsg = ((InitialConnectIOException)cause).
                        getUserException().getMessage();
        } else {
            errMsg = ((InitialHandshakeIOException)cause).
                        getUserException().getMessage();
        }
        final Pattern addrPattern =
            Pattern.compile(
                String.format("%s.*%s",
                              rAddr.getHostName(), rAddr.getPort()));
        assertTrue(String.format("'%s' does not contain '%s'",
                                 errMsg, rAddr.toString()),
                   addrPattern.matcher(errMsg).find());
    }

    /**
     * Tests that we do not have lingering endpoint perf trackers for terminated
     * connections.
     *
     * [KVSTORE-1538]
     */
    @Test
    public void testEndpointPerfTrackerLeak() throws Exception {
        final NioEndpointGroup sgroup = new NioEndpointGroup(logger, NUM_CORES);
        final NioEndpointGroup cgroup = new NioEndpointGroup(logger, NUM_CORES);
        final Server server = new Server(sgroup, false, false);
        final int numCreators = 16;
        final Thread[] threads = new Thread[numCreators];
        final ConcurrentLinkedQueue<Throwable> errors =
            new ConcurrentLinkedQueue<>();
        for (int i = 0; i < numCreators; ++i) {
            final int j = i;
            final Thread thread = new Thread(() -> {
                try {
                    final CreatorEndpoint endpoint =
                        cgroup.getCreatorEndpoint(
                            String.format("perfName%s", j),
                            new InetNetworkAddress("localhost", server.getPort()),
                            InetNetworkAddress.ANY_LOCAL_ADDRESS,
                            (new EndpointConfigBuilder())
                            .option(AsyncOption.DLG_LOCAL_MAXDLGS, (j + 1) * 1024)
                            .build());
                    final Requester requester =
                        new Requester(EMPTYBYTES, REQ_SMALL, RESP_SMALL);
                    endpoint.startDialog(DEFAULT_DIALOG_TYPE, requester, 1000);
                    Thread.sleep(1000);
                    requester.check();
                    ((AbstractCreatorEndpoint) endpoint).shutdown("shutdown", true);
                    Thread.sleep(1000);
                } catch (Throwable t) {
                    errors.add(t);
                }
            });
            thread.start();
            threads[i] = thread;
        }
        for (int i = 0; i < numCreators; ++i) {
            threads[i].join();
        }
        assertEquals(
            0,
            cgroup.getDialogEndpointGroupPerfTracker()
            .getNumOfEndpointPerfTrackers());
        assertEquals(
            0,
            sgroup.getDialogEndpointGroupPerfTracker()
            .getNumOfEndpointPerfTrackers());
    }

    /**
     * Tests the channel description in connection exceptions with ssl data
     * channel.
     */
    @Test
    public void testSSLDataChannelDescription() throws Exception {
        final boolean runningAlone =
            isRunningAlone("testSSLDataChannelDescription");
        EndpointGroup sgroup = new NioEndpointGroup(logger, 1);
        EndpointGroup cgroup = new NioEndpointGroup(logger, 1);
        final EndpointConfigBuilder builder =
            (new EndpointConfigBuilder())
            .sslControl(createSSLConfig().makeSSLControl(false, logger))
            .option(AsyncOption.DLG_HEARTBEAT_INTERVAL, 100)
            .option(AsyncOption.DLG_HEARTBEAT_TIMEOUT, 2)
            .option(AsyncOption.DLG_IDLE_TIMEOUT, 500);
        final ListenerConfigBuilder listenBuilder =
            (new ListenerConfigBuilder()).
            portRange(new ListenerPortRange()).
            endpointConfigBuilder(builder);
        final Server server = new Server(
            sgroup, false /* queueMesgs */,
            REQ_SMALL, RESP_SMALL, listenBuilder);
        final NetworkAddress addr =
            new InetNetworkAddress("localhost", server.getPort());
        final CreatorEndpoint endpoint =
            cgroup.getCreatorEndpoint(
                "perfName", addr,
                InetNetworkAddress.ANY_LOCAL_ADDRESS, builder.build());
        Requester requester;
        requester = new Requester(EMPTYBYTES, REQ_SMALL, RESP_SMALL);
        endpoint.startDialog(
            DEFAULT_DIALOG_TYPE, requester, DEFAULT_TIMEOUT);
        requester.awaitDone(DEFAULT_TIMEOUT);
        requester.check();
        sgroup.getSchedExecService().execute(() -> {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                /* ignore */
            }
        });
        requester = new Requester(EMPTYBYTES, REQ_SMALL, RESP_SMALL);
        final AtomicReference<Throwable> error = new AtomicReference<>(null);
        requester.setOnAbortHook((e) -> error.set(e));
        endpoint.startDialog(DEFAULT_DIALOG_TYPE, requester, 1000);
        requester.awaitDone(1000);
        TestUtils.checkCause(
            TestUtils.checkException(
                error.get(), TemporaryDialogException.class, null),
            ConnectionTimeoutException.class,
            "Problem with channel.*NBL client");
        if (runningAlone) {
            System.out.println(CommonLoggerUtils.getStackTrace(error.get()));
        }
    }
}
