/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static oracle.kv.impl.async.FutureUtils.handleFutureGetException;
import static oracle.kv.impl.async.FutureUtils.unwrapExceptionVoid;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

import oracle.kv.impl.async.AsyncInitiatorProxy.MethodCallClass;
import oracle.kv.impl.async.AsyncVersionedRemote.AbstractGetSerialVersionCall;
import oracle.kv.impl.async.AsyncVersionedRemote.MethodCall;
import oracle.kv.impl.async.AsyncVersionedRemote.MethodOp;
import oracle.kv.impl.util.ReadFastExternal;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Provides a basic Hello async interface for async testing.
 */
public abstract class AsyncVersionedHelloTestBase
        extends AsyncVersionedRemoteTestBase {

    /** Dialog type family */
    static final DialogTypeFamily helloDialogTypeFamily =
        new HelloDialogTypeFamily();

    /** Dialog type */
    static final DialogType helloDialogType =
        new DialogType(helloDialogTypeFamily, 1);

    protected AsyncVersionedHelloTestBase(boolean secure) {
        super(secure);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /* -- Dialog infrastructure -- */

    /** Method opcode */
    enum HelloMethodOp implements MethodOp {
        GET_SERIAL_VERSION(GetSerialVersionCall::new),
        HELLO(HelloCall::new);
        private final ReadFastExternal<MethodCall<?>> reader;
        HelloMethodOp(ReadFastExternal<MethodCall<?>> reader) {
            this.reader = reader;
        }
        @Override
        public int getValue() { return ordinal(); }
        @Override
        public MethodCall<?> readRequest(final DataInput in,
                                         final short serialVersion)
            throws IOException
        {
            return reader.readFastExternal(in, serialVersion);
        }
    }

    /** Dialog type family class */
    static class HelloDialogTypeFamily implements DialogTypeFamily {
        HelloDialogTypeFamily() {
            DialogType.registerTypeFamily(this);
        }
        @Override
        public int getFamilyId() {
            return 99;
        }
        @Override
        public String getFamilyName() {
            return "HelloDialogTypeFamily";
        }
        @Override
        public String toString() {
            return getFamilyName() + "(" + getFamilyId() + ")";
        }
    }

    /** Remote interface */
    interface Hello extends AsyncVersionedRemote {

        @MethodCallClass(GetSerialVersionCall.class)
        @Override
        CompletableFuture<Short> getSerialVersion(short serialVersion,
                                                  long timeoutMillis);

        @MethodCallClass(HelloCall.class)
        CompletableFuture<String> hello(short serialVersion,
                                        @Nullable String greeting,
                                        long timeoutMillis);
    }

    /** Method call for getSerialVersion */
    static class GetSerialVersionCall extends AbstractGetSerialVersionCall {
        GetSerialVersionCall() { }
        @SuppressWarnings("unused")
        GetSerialVersionCall(DataInput in, short serialVersion) { }
        @Override
        public HelloMethodOp getMethodOp() {
            return HelloMethodOp.GET_SERIAL_VERSION;
        }
        @Override
        public String describeCall() {
            return "Hello.GetSerialVersionCall";
        }
    }

    /** Method call for hello */
    static class HelloCall implements MethodCall<String> {
        final @Nullable String greeting;
        HelloCall(@Nullable String greeting) { this.greeting = greeting; }
        HelloCall(DataInput in, short serialVersion) throws IOException {
            this(SerializationUtil.readString(in, serialVersion));
        }
        @Override
        public HelloMethodOp getMethodOp() { return HelloMethodOp.HELLO; }
        @Override
        public void writeFastExternal(final DataOutput out,
                                      final short serialVersion)
            throws IOException
        {
            SerializationUtil.writeString(out, serialVersion, greeting);
        }
        @Override
        public void writeResponse(final @Nullable String response,
                                  final DataOutput out,
                                  final short serialVersion)
            throws IOException
        {
            SerializationUtil.writeString(out, serialVersion, response);
        }
        @Override
        public String readResponse(final DataInput in,
                                   final short serialVersion)
            throws IOException
        {
            return SerializationUtil.readString(in, serialVersion);
        }
        @Override
        public String describeCall() {
            return "Hello.HelloCall[greeting=" + greeting + "]";
        }
    }

    /** Client API class */
    static class HelloAPI extends AsyncVersionedRemoteAPI {
        final Hello proxyRemote;
        HelloAPI(Hello remote, short serialVersion) {
            super(serialVersion);
            proxyRemote = remote;
        }
        static CompletableFuture<HelloAPI> wrap(final Hello initiator) {
            return computeSerialVersion(initiator, TIMEOUT)
                .thenApply(version -> new HelloAPI(initiator, version));
        }
        CompletableFuture<String> hello(@Nullable String greeting,
                                        long timeoutMillis) {
            return proxyRemote.hello(getSerialVersion(), greeting,
                                     timeoutMillis);
        }
    }

    /** Server implementation */
    static class HelloImpl implements Hello {
        @Override
        public CompletableFuture<Short> getSerialVersion(short serialVersion,
                                                         long timeoutMillis) {
            return completedFuture(SerialVersion.CURRENT);
        }
        @Override
        public CompletableFuture<String> hello(short serialVersion,
                                               @Nullable String greeting,
                                               long timeout) {
            return completedFuture(
                "Hi!  Got your greeting: '" + greeting + "'");
        }
    }

    /** Client-side stub */
    static class HelloInitiator extends AsyncVersionedRemoteInitiator
            implements Hello {

        HelloInitiator(CreatorEndpoint endpoint, Logger logger) {
            super(endpoint, helloDialogType, logger);
        }
        @Override
        protected AbstractGetSerialVersionCall getSerialVersionCall() {
            return new GetSerialVersionCall();
        }
        MethodCall<String> createMethodCall(@Nullable String greeting) {
            return new HelloCall(greeting);
        }
        @Override
        public CompletableFuture<String> hello(short serialVersion,
                                               @Nullable String greeting,
                                               long timeoutMillis) {
            return startDialog(serialVersion, createMethodCall(greeting),
                               timeoutMillis);
        }
    }

    /** Server-side dialog handler */
    class HelloResponder extends AsyncVersionedRemoteDialogResponder {
        private final Hello server;
        HelloResponder() {
            this(new HelloImpl());
        }
        HelloResponder(Hello server) {
            super(helloDialogTypeFamily,
                  AsyncVersionedHelloTestBase.this.logger);
            this.server = server;
        }
        @Override
        protected MethodOp getMethodOp(int methodOpVal) {
            return HelloMethodOp.values()[methodOpVal];
        }
        @Override
        protected void handleRequest(short serialVersion,
                                     MethodCall<?> methodCall,
                                     long timeoutMillis,
                                     DialogContext context) {
            switch ((HelloMethodOp) methodCall.getMethodOp()) {
            case GET_SERIAL_VERSION:
                getSerialVersion(serialVersion,
                                 (GetSerialVersionCall) methodCall,
                                 timeoutMillis, server);
                break;
            case HELLO:
                hello(serialVersion, (HelloCall) methodCall, timeoutMillis);
                break;
            default:
                throw new AssertionError();
            }
        }
        private void hello(short serialVersion,
                           HelloCall hello,
                           long timeout) {
            server.hello(serialVersion, hello.greeting, timeout)
                .whenComplete(
                    unwrapExceptionVoid(
                        getResponseConsumer(serialVersion, hello)));
        }
    }

    /* -- Other methods -- */

    @Nullable String hello(CreatorEndpoint endpoint,
                           @Nullable String greeting) {
        return hello(endpoint, greeting, TIMEOUT);
    }

    @Nullable String hello(CreatorEndpoint endpoint,
                           @Nullable String greeting,
                           long timeout) {
        try {
            return HelloAPI.wrap(new HelloInitiator(endpoint, logger))
                .thenCompose(api -> api.hello(greeting, timeout))
                /*
                 * Wait an additional TIMEOUT to make sure we get the
                 * response
                 */
                .get(timeout + TIMEOUT, MILLISECONDS);
        } catch (Throwable exception) {
            try {
                throw handleFutureGetException(exception);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException("Unexpected exception: " + e,
                                                e);
            }
        }
    }

    @Nullable String hello(final Hello initiator,
                           final @Nullable String greeting) {
        try {
            return HelloAPI.wrap(initiator)
                .thenCompose(api -> api.hello(greeting, TIMEOUT))
                .get(TIMEOUT, MILLISECONDS);
        } catch (Throwable exception) {
            try {
                throw handleFutureGetException(exception);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException("Unexpected exception: " + e,
                                                e);
            }
        }
    }
}
