/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static oracle.kv.impl.async.FutureUtils.failedFuture;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;

import java.rmi.RemoteException;
import java.util.concurrent.CompletableFuture;

import oracle.kv.UncaughtExceptionTestBase;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.registry.VersionedRemote;

import org.junit.Test;

/** Test VersionedRemoteAsyncImpl. */
public class VersionedRemoteAsyncImplTest extends UncaughtExceptionTestBase {
    private static final long TIMEOUT = 12345;

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
        final AsyncFooImpl impl = new AsyncFooImpl();
        final Foo foo =
            VersionedRemoteAsyncImpl.createProxy(Foo.class, impl, TIMEOUT);
        assertEquals(SerialVersion.MINIMUM, foo.getSerialVersion());
   }

    @Test
    public void testCallFoo() throws Exception {
        final String msg = "Called foo";
        final AsyncFooImpl impl = new AsyncFooReturns(msg);
        final Foo foo =
            VersionedRemoteAsyncImpl.createProxy(Foo.class, impl, TIMEOUT);
        assertEquals(msg, foo.foo(SerialVersion.MINIMUM));
    }

    public static class AsyncFooReturns extends AsyncFooImpl {
        private String msg;
        AsyncFooReturns(String msg) {
            this.msg = msg;
        }
        @Override
        public CompletableFuture<String> foo(short sv, long timeoutMs) {
            assertEquals(SerialVersion.MINIMUM, sv);
            assertEquals(TIMEOUT, timeoutMs);
            return completedFuture(msg);
        }
    }

    @Test
    public void testCallFooRemoteException() throws Exception {
        final String msg = "Called foo: RemoteException";
        final AsyncFooImpl impl = new AsyncFooRemoteException(msg);
        final Foo foo =
            VersionedRemoteAsyncImpl.createProxy(Foo.class, impl, TIMEOUT);
        checkException(() -> foo.foo(SerialVersion.MINIMUM),
                       RemoteException.class,
                       msg);
    }

    public static class AsyncFooRemoteException extends AsyncFooImpl {
        private String msg;
        AsyncFooRemoteException(String msg) {
            this.msg = msg;
        }
        @Override
        public CompletableFuture<String> foo(short sv, long timeoutMs) {
            assertEquals(SerialVersion.MINIMUM, sv);
            assertEquals(TIMEOUT, timeoutMs);
            return failedFuture(new RemoteException(msg));
        }
    }

    @Test
    public void testCallFooArithmeticException() throws Exception {
        final String msg = "Called foo: ArithmeticException";
        final AsyncFooImpl impl = new AsyncFooArithmeticException(msg);
        final Foo foo =
            VersionedRemoteAsyncImpl.createProxy(Foo.class, impl, TIMEOUT);
        checkException(() -> foo.foo(SerialVersion.MINIMUM),
                       ArithmeticException.class,
                       msg);
    }

    public static class AsyncFooArithmeticException extends AsyncFooImpl {
        private final String msg;
        AsyncFooArithmeticException(String msg) {
            this.msg = msg;
        }
        @Override
        public CompletableFuture<String> foo(short sv, long timeoutMs) {
            assertEquals(SerialVersion.MINIMUM, sv);
            assertEquals(TIMEOUT, timeoutMs);
            return failedFuture(new ArithmeticException(msg));
        }
    }

    @Test
    public void testCallBar() throws Exception {
        final AsyncFooImpl impl = new AsyncFooBarReturns();
        final Foo foo =
            VersionedRemoteAsyncImpl.createProxy(Foo.class, impl, TIMEOUT);
        assertEquals(30, foo.bar(13, 17, SerialVersion.MINIMUM));
    }

    public static class AsyncFooBarReturns extends AsyncFooImpl {
        @Override
        public CompletableFuture<Integer> bar(short sv, int x, int y,
                                              long timeoutMs) {
            assertEquals(SerialVersion.MINIMUM, sv);
            assertEquals(TIMEOUT, timeoutMs);
            return completedFuture(x + y);
        }
    }

    @Test
    public void testCallMissingMethod() throws Exception {
        final AsyncFooImpl impl = new AsyncFooImpl();
        final FooPlus fooPlus =
            VersionedRemoteAsyncImpl.createProxy(FooPlus.class, impl, TIMEOUT);
        checkException(() -> fooPlus.baz(SerialVersion.MINIMUM),
                       IllegalStateException.class,
                       "Method baz\\(short, long\\) not found");
    }

    /* Other methods and classes */

    public interface Foo extends VersionedRemote {
        public String foo(short serialVersion) throws RemoteException;
        public int bar(int x, int y, short serialVersion)
            throws RemoteException;
    }

    public interface FooPlus extends Foo {
        public void baz(short serialVersion)
            throws RemoteException;
    }

    public interface AsyncFoo extends AsyncVersionedRemote {
        public CompletableFuture<String> foo(short sv, long timeoutMs);
        public CompletableFuture<Integer>
            bar(short sv, int x, int y, long timeoutMs);
    }

    public static class AsyncFooImpl implements AsyncFoo {
         @Override
        public CompletableFuture<Short> getSerialVersion(short sv,
                                                         long timeoutMs) {
            assertEquals(SerialVersion.CURRENT, sv);
            assertEquals(TIMEOUT, timeoutMs);
            return completedFuture(SerialVersion.MINIMUM);
        }
        @Override
        public CompletableFuture<String> foo(short sv, long timeoutMs) {
            throw new RuntimeException("Unexpected");
        }
        @Override
        public CompletableFuture<Integer> bar(short sv, int x, int y,
                                              long timeoutMs) {
            throw new RuntimeException("Unexpected");
        }
    }
}
