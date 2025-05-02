/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async;

import static oracle.kv.impl.async.FutureUtils.failedFuture;
import static oracle.kv.impl.async.StandardDialogTypeFamily.SERVICE_REGISTRY_TYPE_FAMILY;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.concurrent.CompletableFuture;

import oracle.kv.impl.async.AsyncInitiatorProxy.MethodCallClass;
import oracle.kv.impl.async.AsyncVersionedRemote.MethodCall;
import oracle.kv.impl.util.FilterableParameterized;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.registry.AsyncControl;

import org.checkerframework.checker.nullness.qual.Nullable;

import org.junit.Test;
import org.junit.runner.RunWith;

/** Test AsyncInitiatorProxy. */
@RunWith(FilterableParameterized.class)
public class AsyncInitiatorProxyTest extends AsyncVersionedHelloTestBase {

    public AsyncInitiatorProxyTest(boolean secure) {
        super(secure);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        assumeTrue("Requires async", AsyncControl.serverUseAsync);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /* Test methods */

    @Test
    public void testCreateWrongDialogType() throws Exception {
        CreatorEndpoint endpoint =
            createEndpoint(helloDialogType, new HelloResponder());
        checkException(() ->
                       AsyncInitiatorProxy.createProxy(
                           Hello.class, SERVICE_REGISTRY_TYPE_FAMILY,
                           endpoint, helloDialogType, logger),
                       IllegalArgumentException.class,
                       "dialog type family");
    }

    @Test
    public void testCallHello() throws Exception {
        CreatorEndpoint endpoint =
            createEndpoint(helloDialogType, new HelloResponder());
        Hello initiator = AsyncInitiatorProxy.createProxy(
            Hello.class, helloDialogTypeFamily, endpoint, helloDialogType,
            logger);
        String result = hello(initiator, "hi");
        assertTrue("Result: " + result,
                   (result != null) && result.contains("hi"));
    }

    @Test
    public void testCallHelloThrows() throws Exception {
        Exception exception = new ArithmeticException("oops");
        CreatorEndpoint endpoint = createEndpoint(
            helloDialogType,
            new HelloResponder(
                new HelloImpl() {
                    @Override
                    public CompletableFuture<String> hello(
                        short serialVersion,
                        @Nullable String greeting,
                        long timeout)
                    {
                        return failedFuture(exception);
                    }
                }));
        Hello initiator = AsyncInitiatorProxy.createProxy(
            Hello.class, helloDialogTypeFamily, endpoint, helloDialogType,
            logger);
        checkException(() -> hello(initiator, "hi"),
                       ArithmeticException.class,
                       "oops");
    }

    @Test
    public void testMissingAnnotation() throws Exception {
        CreatorEndpoint endpoint =
            createEndpoint(helloDialogType, new HelloResponder());
        HelloNoAnnotation initiator = AsyncInitiatorProxy.createProxy(
            HelloNoAnnotation.class, helloDialogTypeFamily, endpoint,
            helloDialogType, logger);
        checkException(() ->
                       initiator.noAnnotation(SerialVersion.CURRENT,
                                              Long.MAX_VALUE),
                       IllegalArgumentException.class,
                       "no MethodCallClass annotation");
    }

    @Test
    public void testNotAsync() throws Exception {
        CreatorEndpoint endpoint =
            createEndpoint(helloDialogType, new HelloResponder());
        HelloNotAsync initiator =
            AsyncInitiatorProxy.createProxy(
                HelloNotAsync.class, helloDialogTypeFamily,
                endpoint, helloDialogType, logger);
        checkException(() ->
                       initiator.likeHello(SerialVersion.CURRENT, "hi",
                                           Long.MAX_VALUE),
                       IllegalArgumentException.class,
                       "does not implement AsyncVersionedRemote");
    }

    @Test
    public void testBadMethodCallConstructor() throws Exception {
        CreatorEndpoint endpoint =
            createEndpoint(helloDialogType, new HelloResponder());
        HelloBadMethodCallConstructor initiator =
            AsyncInitiatorProxy.createProxy(
                HelloBadMethodCallConstructor.class, helloDialogTypeFamily,
                endpoint, helloDialogType, logger);
        checkException(() ->
                       initiator.badConstructor(SerialVersion.CURRENT,
                                                Long.MAX_VALUE),
                       IllegalStateException.class,
                       "Unable to find constructor");
    }

    /* Other methods and classes */

    interface HelloNoAnnotation extends Hello {
        CompletableFuture<Void> noAnnotation(short serialVersion,
                                             long timeoutMillis);
    }

    interface NotAsync {
        @MethodCallClass(HelloCall.class)
        CompletableFuture<String> likeHello(short serialVersion,
                                            @Nullable String greeting,
                                            long timeoutMillis);
    }

    interface HelloNotAsync extends Hello, NotAsync { }

    interface HelloBadMethodCallConstructor extends Hello {

        @MethodCallClass(BadConstructorCall.class)
        CompletableFuture<Void> badConstructor(short serialVersion,
                                               long timeoutMillis);
    }

    static class BadConstructorCall implements MethodCall<Void> {
        BadConstructorCall(@SuppressWarnings("unused") int extraArg) { }
        @Override
        public HelloMethodOp getMethodOp() { return HelloMethodOp.HELLO; }
        @Override
        public void writeFastExternal(final DataOutput out,
                                      final short serialVersion) { }
        @Override
        public void writeResponse(final @Nullable Void response,
                                  final DataOutput out,
                                  final short serialVersion) { }
        @Override
        public @Nullable Void readResponse(final DataInput in,
                                           final short serialVersion) {
            return null;
        }
        @Override
        public String describeCall() {
            return "BadConstructorCall";
        }
    }
}
