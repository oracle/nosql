/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static oracle.kv.impl.async.FutureUtils.failedFuture;
import static oracle.kv.impl.util.ObjectUtil.checkNull;
import static oracle.kv.impl.util.TestUtils.compareLevels;
import static oracle.kv.impl.util.TestUtils.removeIfCount;
import static oracle.kv.impl.util.VersionUtil.getJavaMajorVersion;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;
import java.util.stream.Collectors;

import oracle.kv.RequestTimeoutException;
import oracle.kv.impl.async.AsyncVersionedRemote.MethodCall;
import oracle.kv.impl.async.AsyncVersionedRemote.MethodOp;
import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.async.exception.ConnectionIOException;
import oracle.kv.impl.async.exception.DialogException;
import oracle.kv.impl.async.exception.InitialHandshakeIOException;
import oracle.kv.impl.async.exception.PersistentDialogException;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.impl.util.FilterableParameterized;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;

import org.checkerframework.checker.nullness.qual.Nullable;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Simple test of the asynchronous remote call facility defined by {@link
 * AsyncVersionedRemote}.
 */
@RunWith(FilterableParameterized.class)
public class AsyncVersionedRemoteTest extends AsyncVersionedHelloTestBase {

    private static final Logger kvLogger = Logger.getLogger("oracle.kv");

    private static final Formatter logFormatter = new SimpleFormatter();

    /**
     * Contains log records at level WARNING or higher that were logged during
     * the current test. Tests that expect WARNING or SEVERE logging should
     * filter out the expected records. Any remaining records will cause test
     * failures.
     */
    private static List<LogRecord> warnings =
        Collections.synchronizedList(new ArrayList<>());

    private static final Handler warningsHandler = new StreamHandler() {
        @Override
        public synchronized void publish(@Nullable LogRecord record) {
            if ((record != null) &&
                (compareLevels(record.getLevel(), Level.WARNING) >= 0))
            {
                warnings.add(record);
            }
        }
    };

    public AsyncVersionedRemoteTest(boolean secure) {
        super(secure);
    }

    @BeforeClass
    public static void staticSetUp() {
        kvLogger.addHandler(warningsHandler);
    }

    @AfterClass
    public static void staticTearDown() {
        kvLogger.removeHandler(warningsHandler);
    }

    @Override
    public void setUp() throws Exception {
        warnings.clear();
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        synchronized (warnings) {
            if (!warnings.isEmpty()) {
                fail("Unexpected warning or severe logging:\n" +
                     warnings.stream()
                     .map(logFormatter::format)
                     .collect(Collectors.joining()));
            }
        }
    }

    /* -- Tests -- */

    @Test
    public void testSimple() throws Exception {
        CreatorEndpoint endpoint =
            createEndpoint(helloDialogType, new HelloResponder());
        String greeting = "Hello at " + System.currentTimeMillis();
        String result = hello(endpoint, greeting);
        assertTrue("Result: " + result,
                   (result != null) && result.contains(greeting));
    }

    @Test
    public void testNullOK() throws Exception {
        CreatorEndpoint endpoint = createEndpoint(
            helloDialogType,
            new HelloResponder(
                new HelloImpl() {
                    @Override
                    public CompletableFuture<String>
                        hello(short serialVersion,
                              @Nullable String greeting,
                              long timeout) {
                        return completedFuture(null);
                    }
                }));
        assertEquals(null, hello(endpoint, null));
    }

    @Test
    public void testSerializeResponseFailure() throws Exception {
        final String msg = "Test response serialization failure";
        CreatorEndpoint endpoint = createEndpoint(
            helloDialogType,
            new HelloResponder() {
                /*
                 * Replace the method call with one that fails when serializing
                 * the response
                 */
                @Override
                protected void handleRequest(short serialVersion,
                                             MethodCall<?> methodCall,
                                             long timeoutMillis,
                                             DialogContext context) {
                    if (methodCall instanceof HelloCall) {
                        methodCall =
                            new HelloCall(((HelloCall) methodCall).greeting) {
                                @Override
                                public void
                                    writeResponse(@Nullable String response,
                                                  DataOutput out,
                                                  short sv)
                                {
                                    throw new IllegalArgumentException(msg);
                                }
                            };
                    }
                    super.handleRequest(serialVersion, methodCall,
                                        timeoutMillis, context);
                }
            });
        try {
            hello(endpoint, null);
            fail("Expected RequestTimeoutException");
        } catch (RequestTimeoutException e) {
            assertTrue("Exception: " + e,
                       e.getMessage().contains("Dialog timed out"));
        }

        /*
         * We expect a single WARNING logging entry containing the exception
         * message
         */
        assertEquals(1, removeWarnings(msg));
    }

    @Test
    public void testDeserializeRequestFailure() throws Exception {
        final String msg = "Test request deserialization failure";
        CreatorEndpoint endpoint = createEndpoint(
            helloDialogType,
            new HelloResponder() {
                /*
                 * Replace the method op with one that fails when deserializing
                 * the request
                 */
                @Override
                protected MethodOp getMethodOp(int methodOpVal) {
                    if (methodOpVal != HelloMethodOp.HELLO.getValue()) {
                        return super.getMethodOp(methodOpVal);
                    }
                    return new MethodOp() {
                        @Override
                        public int getValue() { return 42; }
                        @Override
                        public MethodCall<?> readRequest(DataInput in,
                                                         short serialVersion)
                            throws IOException
                        {
                            throw new IllegalArgumentException(msg);
                        }
                    };
                }
            });
        try {
            hello(endpoint, null);
            fail("Expected RuntimeException");
        } catch (RuntimeException e) {
            /*
             * Expects an exception that contains the specified message if the
             * peer manages to report back or otherwise an io exception
             */
            if (!(e.getCause() instanceof ConnectionIOException)) {
                assertTrue("Exception: " + e, e.getMessage().contains(msg));
            }
            // TODO: This seems to be a DialogUnknownException that includes
            // the exception message thrown on the server. Seems like a
            // server-side deserialization error should not produce a response
            // to the client at all?
            assertTrue("Exception: " + e, e.getMessage().contains(msg));
        }

        /*
         * We expect a single WARNING logging entry containing the exception
         * message
         */
        assertEquals(1, removeWarnings(msg));
    }

    @Test
    public void testDeserializeResponseFailure() throws Exception {
        final String msg = "Test response deserialization failure";
        CreatorEndpoint endpoint =
            createEndpoint(helloDialogType, new HelloResponder());
        Hello hello = new HelloInitiator(endpoint, logger) {
            /*
             * Replace the method call with one that fails when deserializing
             * the response
             */
            @Override
            MethodCall<String> createMethodCall(@Nullable String greeting) {
                return new HelloCall(greeting) {
                    @Override
                    public String readResponse(DataInput in,
                                               short serialVersion) {
                        throw new NumberFormatException(msg);
                    }
                };
            }
        };
        try {
            hello(hello, null);
            fail("Expected NumberFormatException");
        } catch (NumberFormatException e) {
            assertEquals(msg, e.getMessage());
        }

        /*
         * We expect a single WARNING logging entry containing the exception
         * message
         */
        assertEquals(1, removeWarnings(msg));
    }

    @Test
    public void testDeserializeResponseError() throws Exception {
        final String msg = "Test response deserialization error";
        CreatorEndpoint endpoint =
            createEndpoint(helloDialogType, new HelloResponder());
        Hello hello = new HelloInitiator(endpoint, logger) {
            @Override
            MethodCall<String> createMethodCall(@Nullable String greeting) {
                return new HelloCall(greeting) {
                    @Override
                    public String readResponse(DataInput in,
                                               short serialVersion) {
                        throw new Error(msg);
                    }
                };
            }
        };
        try {
            hello(hello, null);
            fail("Expected exception");
        } catch (Error e) {
            assertEquals(msg, e.getMessage());
        }

        /*
         * We expect a single WARNING logging entry containing the exception
         * message
         */
        assertEquals(1, removeWarnings(msg));
    }

    @Test
    public void testSerializeRequestFailure() throws Exception {
        final String msg = "Test request serialization failure";
        CreatorEndpoint endpoint =
            createEndpoint(helloDialogType, new HelloResponder());
        Hello hello = new HelloInitiator(endpoint, logger) {
            @Override
            MethodCall<String> createMethodCall(@Nullable String greeting) {
                return new HelloCall(greeting) {
                    @Override
                    public void writeFastExternal(DataOutput out,
                                                  short serialVersion) {
                        throw new NumberFormatException(msg);
                    }
                };
            }
        };
        try {
            hello(hello, null);
            fail("Expected RuntimeException");
        } catch (NumberFormatException e) {
            assertEquals(msg, e.getMessage());
        }

        /*
         * We expect a single WARNING logging entry containing the exception
         * message
         */
        assertEquals(1, removeWarnings(msg));
    }

    @Test
    public void testServerThrowsException() throws Exception {
        final String msg = "Test server exception";
        class HelloWithLocalException extends HelloImpl {
            @Override
            public CompletableFuture<String> hello(short serialVersion,
                                                   @Nullable String greeting,
                                                   long timeout) {
                return failedFuture(new LocalException(msg));
            }
        }
        CreatorEndpoint endpoint = createEndpoint(
            helloDialogType,
            new HelloResponder(new HelloWithLocalException()));
        try {
            hello(endpoint, null);
            fail("Expected LocalException");
        } catch (LocalException e) {
            assertEquals(msg, e.getMessage());
            if (!Arrays.stream(e.getStackTrace()).anyMatch(
                    ste ->
                    ste.getClassName().endsWith("HelloWithLocalException") &&
                    ste.getMethodName().equals("hello"))) {
                throw new RuntimeException(
                    "Didn't find HelloWithLocalException.hello",
                    e);
            }
        }
    }

    @Test
    public void testServerThrowsError() throws Exception {
        final String msg = "Test server error";
        CreatorEndpoint endpoint = createEndpoint(
            helloDialogType,
            new HelloResponder(
                new HelloImpl() {
                    @Override
                    public CompletableFuture<String>
                        hello(short serialVersion,
                              @Nullable String greeting,
                              long timeout) {
                        return failedFuture(new Error(msg));
                    }
                }));
        try {
            hello(endpoint, null);
            fail("Expected Error");
        } catch (Error e) {
            assertEquals(msg, e.getMessage());
        } catch (DialogException e) {
            /*
             * If the server cannot send the dialog abort in time, we will have
             * a DialogException with ConnectionIOException as cause.
             */
            assertTrue(e.getCause() instanceof ConnectionIOException);
        }
    }

    @Test
    public void testServerVersionBelowMinimum() throws Exception {
        CreatorEndpoint endpoint = createEndpoint(
            helloDialogType,
            new HelloResponder(
                new HelloImpl() {
                    @Override
                    public CompletableFuture<Short>
                        getSerialVersion(short serialVersion,
                                         long timeoutMillis) {
                        return completedFuture(
                            (short) (SerialVersion.MINIMUM - 1));
                    }
                }));
        try {
            hello(endpoint, "Hello");
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage(),
                       e.getMessage().contains("server is incompatible"));
        }
    }

    @Test
    public void testClientVersionBelowMinimum() throws Exception {
        CreatorEndpoint endpoint =
            createEndpoint(helloDialogType, new HelloResponder());
        Hello hello = new HelloInitiator(endpoint, logger) {
            @Override
            public CompletableFuture<Short>
                getSerialVersion(short serialVersion, long timeoutMillis)
            {
                return super.getSerialVersion(
                    (short) (SerialVersion.MINIMUM - 1), timeoutMillis);
            }
        };
        try {
            hello(hello, "Hello");
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage(),
                       e.getMessage().contains("client is incompatible"));
        }
    }

    @Test
    public void testNegativeTimeout() throws Exception {
        CreatorEndpoint endpoint =
            createEndpoint(helloDialogType, new HelloResponder());
        try {
            hello(endpoint, "Hello", -3);
            fail("Expected RequestTimeoutException");
        } catch (RequestTimeoutException e) {
            assertTrue(e.toString(), e.getMessage().contains("-3"));
        }
    }

    @Test
    public void testZeroTimeout() throws Exception {
        CreatorEndpoint endpoint =
            createEndpoint(helloDialogType, new HelloResponder());
        try {
            hello(endpoint, "Hello", 0);
            fail("Expected RequestTimeoutException");
        } catch (RequestTimeoutException e) {
            /* expected */
        }
    }

    @Test
    public void testSingleThread() throws Exception {
        multiTest(10000, 0);
    }

    @Test
    public void testMultiThread() throws Exception {
        multiTest(50000, 5);
    }

    @Test
    public void testConnectionRejected() throws Exception {

        /* Only permit a single connection */
        listenerConfig = createListenerConfigBuilder()
            .option(AsyncOption.DLG_ACCEPT_MAX_ACTIVE_CONNS, 1)
            .build();

        HelloResponder responder = new HelloResponder();
        final CreatorEndpoint endpoint1 =
            createEndpoint(helloDialogType, responder);
        hello(endpoint1, "endpoint1");

        /* Create and use a second endpoint */
        final CreatorEndpoint endpoint2 =
            getEndpointGroup().getCreatorEndpoint(
                "perfName", endpoint1.getRemoteAddress(),
                InetNetworkAddress.ANY_LOCAL_ADDRESS,
                createEndpointConfigBuilder(false)

                /*
                 * Set a non-default option value to make sure we get second
                 * endpoint and trigger the maximum connections limit
                 */
                .option(AsyncOption.DLG_CONNECT_TIMEOUT,
                        EndpointConfigBuilder.getOptionDefault(
                            AsyncOption.DLG_CONNECT_TIMEOUT) + 1000)
                .build());
        try {
            hello(endpoint2, "endpoint2");
            fail("Expected PersistentDialogException");
        } catch (PersistentDialogException e) {
            assertTrue("Cause should be InitialHandshakeIOException: " +
                       e.getCause(),
                       e.getCause() instanceof InitialHandshakeIOException);
        }
    }

    /**
     * Test using a method call that includes the timeout [KVSTORE-641]
     */
    @Test
    public void testIncludeTimeout() throws Exception {
        class HelloCallWithTimeout extends HelloCall {
            final long timeout;
            HelloCallWithTimeout(@Nullable String greeting, long timeout) {
                super(greeting);
                this.timeout = timeout;
            }
            HelloCallWithTimeout(DataInput in, short serialVersion)
                throws IOException
            {
                this(SerializationUtil.readString(in, serialVersion),
                     in.readLong());
            }
            @Override
            public boolean includesTimeout() { return true; }
            @Override
            public long getTimeoutMillis() { return timeout; }
            @Override
            public void writeFastExternal(final DataOutput out,
                                          final short serialVersion)
                throws IOException
            {
                SerializationUtil.writeString(out, serialVersion, greeting);
                out.writeLong(timeout);
            }
            @Override
            public String describeCall() {
                return "Hello.HelloCallWithTimeout[greeting=" + greeting +
                    " timeout=" + timeout + "]";
            }
        }
        CreatorEndpoint endpoint = createEndpoint(
            helloDialogType,
            new HelloResponder() {
                /* Replace the method op with one that includes the timeout */
                @Override
                protected MethodOp getMethodOp(int methodOpVal) {
                    final MethodOp op = super.getMethodOp(methodOpVal);
                    if (methodOpVal != HelloMethodOp.HELLO.getValue()) {
                        return op;
                    }
                    return new MethodOp() {
                        @Override
                        public int getValue() { return op.getValue(); }
                        @Override
                        public MethodCall<?> readRequest(DataInput in,
                                                         short serialVersion)
                            throws IOException
                        {
                            return new HelloCallWithTimeout(in, serialVersion);
                        }
                    };
                }
            });

        final Hello hello = new HelloInitiator(endpoint, logger) {
            @Override
            MethodCall<String> createMethodCall(@Nullable String greeting) {
                return new HelloCallWithTimeout(greeting, TIMEOUT);
            }
        };
        hello(hello, "Hello!");
    }

    @Test
    public void testUnixDomainSockets() throws Exception {
        assumeTrue("Requires Java 16 or later, found " + getJavaMajorVersion(),
                   getJavaMajorVersion() >= 16);
        assumeFalse("Run in non-secure mode only", secure);

        /* Make sure the directory name is short enough */
        File dir = Files.createTempDirectory("unxsoc").toFile();
        tearDowns.add(() -> FileUtils.deleteDirectory(dir));

        ListenerConfig listenerConfig2 = new ListenerConfigBuilder()
            .portRange(new ListenerPortRange(6000, 8000))
            .hostName("unix_domain:" + dir + "/unix-socket")
            .build();
        ListenHandle listenHandle = getEndpointGroup().listen(
            listenerConfig2, helloDialogType.getDialogTypeId(),
            HelloResponder::new);
        tearDowns.add(() -> listenHandle.shutdown(true));

        CreatorEndpoint endpoint = getEndpointGroup().getCreatorEndpoint(
            "perfName",
            listenHandle.getLocalAddress().toNetworkAddress(), /* remoteAddress */
            null, /* localAddress */
            new EndpointConfigBuilder().build());

        String greeting = "Hello at " + System.currentTimeMillis();
        String result = hello(endpoint, greeting);
        assertTrue("Result: " + result,
                   (result != null) && result.contains(greeting));
    }

    /**
     * Test that a server implementation that shuts down the listener with
     * force=false still allows the call to complete.
     */
    @Test
    public void testShutdownListener() throws Exception {
        class Impl extends HelloImpl {
            @Nullable ListenHandle listenHandle;
            @Override
            public CompletableFuture<String> hello(short serialVersion,
                                                   @Nullable String greeting,
                                                   long timeout) {
                checkNull("listenHandle", listenHandle).shutdown(
                    false /* force */);
                return super.hello(serialVersion, greeting, timeout);
            }
        }
        final Impl helloImpl = new Impl();
        final ListenHandle listenHandle =
            listen(helloDialogType, new HelloResponder(helloImpl));
        tearDowns.add(() -> listenHandle.shutdown(true));
        helloImpl.listenHandle = listenHandle;
        CreatorEndpoint endpoint = createEndpoint(listenHandle);
        String greeting = "Hello at " + System.currentTimeMillis();
        String result = hello(endpoint, greeting);
        assertTrue("Result: " + result,
                   (result != null) && result.contains(greeting));
    }

    /* -- Other methods -- */

    private void multiTest(int requestCount, int threadCount)
        throws Exception {

        ListenHandle listenHandle = getEndpointGroup().listen(
            getListenerConfig(), helloDialogType.getDialogTypeId(),
            HelloResponder::new);
        NetworkAddress localAddress =
            checkNull("localAddress",
                      listenHandle.getLocalAddress().toNetworkAddress());
        CreatorEndpoint endpoint = getEndpointGroup().getCreatorEndpoint(
            "perfName", localAddress, InetNetworkAddress.ANY_LOCAL_ADDRESS,
            createEndpointConfigBuilder(false).build());
        final CompletableFuture<Void> done = new CompletableFuture<>();
        final AtomicInteger pending = new AtomicInteger(requestCount);
        class Handler implements Function<HelloAPI, CompletableFuture<Void>> {
            void fail(Throwable e) {
                pending.set(0);
                done.completeExceptionally(e);
            }
            void call(HelloAPI api, int threadNum, int callNum) {
                try {
                    if (pending.decrementAndGet() <= 0) {
                        done.complete(null);
                        return;
                    }
                    final String msg = "Call threadNum=" + threadNum +
                        " callNum=" + callNum;
                    api.hello(msg, TIMEOUT)
                        .thenApply(response -> {
                                if (response == null) {
                                    throw new IllegalArgumentException(
                                        "Response is null");
                                }
                                if (!response.contains(msg)) {
                                    throw new RuntimeException(
                                        "Expected to contain '" + msg +
                                        "', found '" + response + "'");
                                }
                                return response;
                            })
                        .whenComplete((response, e) -> {
                                if (e != null) {
                                    fail(e);
                                    return;
                                }
                                call(api, threadNum, callNum+1);
                            });
                } catch (Throwable e) {
                    fail(e);
                }
            }
            @Override
            public CompletableFuture<Void> apply(@Nullable HelloAPI api) {
                try {
                    if (api == null) {
                        throw new IllegalArgumentException("API is null");
                    }
                    final Handler handler = new Handler();
                    if (threadCount < 1) {
                        handler.call(api, 0, 0);
                    } else {
                        for (int i = 0; i < threadCount; i++) {
                            final int threadNum = i;
                            CompletableFuture.runAsync(
                                () -> handler.call(api, threadNum, 0));
                        }
                    }
                    return done;
                } catch (Throwable e) {
                    return failedFuture(e);
                }
            }
        }
        try {
            HelloAPI.wrap(new HelloInitiator(endpoint, logger))
                .thenCompose(new Handler())
                .get(5, TimeUnit.MINUTES);
        } catch (TimeoutException e) {
            System.err.println("All threads:");
            for (final Entry<Thread, StackTraceElement[]> stme :
                     Thread.getAllStackTraces().entrySet()) {
                System.err.println("\n" + stme.getKey() + ":");
                for (final StackTraceElement ste : stme.getValue()) {
                    System.err.println("  " + ste);
                }
            }
            throw e;
        }
    }

    private int removeWarnings(String message) {
        return removeIfCount(
            warnings,
            record -> record.getMessage().contains(message) &&
            (record.getLevel() == Level.WARNING));
    }
}
