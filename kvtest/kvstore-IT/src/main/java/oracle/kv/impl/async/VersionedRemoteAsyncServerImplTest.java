/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async;

import static oracle.kv.impl.async.FutureUtils.handleFutureGetException;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;

import java.rmi.RemoteException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import oracle.kv.UncaughtExceptionTestBase;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.registry.VersionedRemote;

import org.junit.Test;

/** Test VersionedRemoteAsyncServerImpl. */
public class VersionedRemoteAsyncServerImplTest
        extends UncaughtExceptionTestBase {

    private static final long TIMEOUT = 12345;

    private static Executor DUMMY_EXECUTOR = r -> r.run();

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /* Tests */

    @Test
    public void testCallGetSerialVersion() throws Exception {
        final FooImpl impl = new FooImpl();
        final AsyncFoo foo =
            VersionedRemoteAsyncServerImpl.createProxy(AsyncFoo.class, impl,
                                                       DUMMY_EXECUTOR, logger);
        assertEquals(Short.valueOf(SerialVersion.MINIMUM),
                     foo.getSerialVersion(SerialVersion.MINIMUM, TIMEOUT)
                     .get());
   }

    @Test
    public void testCallFoo() throws Exception {
        final String msg = "Called foo";
        final FooImpl impl = new FooReturns(msg);
        final AsyncFoo foo =
            VersionedRemoteAsyncServerImpl.createProxy(AsyncFoo.class, impl,
                                                       DUMMY_EXECUTOR, logger);
        assertEquals(msg, foo.foo(SerialVersion.MINIMUM, TIMEOUT).get());
    }

    public static class FooReturns extends FooImpl {
        private String msg;
        FooReturns(String msg) {
            this.msg = msg;
        }
        @Override
        public String foo(short sv) {
            assertEquals(SerialVersion.MINIMUM, sv);
            return msg;
        }
    }

    @Test
    public void testCallFooRemoteException() throws Exception {
        final String msg = "Called foo: RemoteException";
        final FooImpl impl = new FooRemoteException(msg);
        final AsyncFoo foo =
            VersionedRemoteAsyncServerImpl.createProxy(AsyncFoo.class, impl,
                                                       DUMMY_EXECUTOR, logger);
        checkException(
            () -> {
                try {
                    foo.foo(SerialVersion.MINIMUM, TIMEOUT).get();
                } catch (Throwable t) {
                    throw handleFutureGetException(t);
                }
            },
            RemoteException.class, msg);
    }

    public static class FooRemoteException extends FooImpl {
        String msg;
        FooRemoteException(String msg) {
            this.msg = msg;
        }
        @Override
        public String foo(short sv) throws RemoteException {
            assertEquals(SerialVersion.MINIMUM, sv);
            throw new RemoteException(msg);
        }
    }

    @Test
    public void testCallFooArithmeticException() throws Exception {
        final String msg = "Called foo: ArithmeticException";
        final FooImpl impl = new FooArithmeticException(msg);
        final AsyncFoo foo =
            VersionedRemoteAsyncServerImpl.createProxy(AsyncFoo.class, impl,
                                                       DUMMY_EXECUTOR, logger);
        checkException(
            () -> {
                try {
                    foo.foo(SerialVersion.MINIMUM, TIMEOUT).get();
                } catch (Throwable t) {
                    throw handleFutureGetException(t);
                }
            },
            ArithmeticException.class, msg);
    }

    public static class FooArithmeticException extends FooImpl {
        private String msg;
        FooArithmeticException(String msg) {
            this.msg = msg;
        }
        @Override
        public String foo(short sv) {
            assertEquals(SerialVersion.MINIMUM, sv);
            throw new ArithmeticException(msg);
        }
    }

    @Test
    public void testCallBar() throws Exception {
        final FooImpl impl = new FooBarReturns();
        final AsyncFoo foo =
            VersionedRemoteAsyncServerImpl.createProxy(AsyncFoo.class, impl,
                                                       DUMMY_EXECUTOR, logger);
        assertEquals(Integer.valueOf(30),
                     foo.bar(SerialVersion.MINIMUM, 13, 17, TIMEOUT).get());
    }

    public static class FooBarReturns extends FooImpl {
        @Override
        public int bar(int x, int y, short sv) {
            assertEquals(SerialVersion.MINIMUM, sv);
            return x + y;
        }
    }

    @Test
    public void testCallMissingMethod() throws Exception {
        final FooImpl impl = new FooImpl();
        final AsyncFooPlus fooPlus =
            VersionedRemoteAsyncServerImpl.createProxy(AsyncFooPlus.class,
                                                       impl, DUMMY_EXECUTOR,
                                                       logger);
        checkException(() -> fooPlus.baz(SerialVersion.MINIMUM, TIMEOUT).get(),
                       IllegalStateException.class,
                       "Method baz\\(short\\) not found");
    }

    /* Other methods and classes */

    public interface Foo extends VersionedRemote {
        public String foo(short serialVersion) throws RemoteException;
        public int bar(int x, int y, short serialVersion)
            throws RemoteException;
    }

    public interface AsyncFoo extends AsyncVersionedRemote {
        public CompletableFuture<String> foo(short sv, long timeoutMs);
        public CompletableFuture<Integer>
            bar(short sv, int x, int y, long timeoutMs);
    }

    public interface AsyncFooPlus extends AsyncFoo {
        public CompletableFuture<Void> baz(short sv, long timeoutMs);
    }

    public static class FooImpl implements Foo {
        @Override
        public short getSerialVersion() throws RemoteException {
            return SerialVersion.MINIMUM;
        }
        @Override
        public String foo(short sv) throws RemoteException {
            throw new RuntimeException("Unexpected");
        }
        @Override
        public int bar(int x, int y, short sv) throws RemoteException {
            throw new RuntimeException("Unexpected");
        }
    }
}
