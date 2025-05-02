/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static oracle.kv.impl.security.ProxyUtils.findInterfaceMethods;
import static oracle.kv.impl.security.ProxyUtils.findInterfaces;
import static org.junit.Assert.assertEquals;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import oracle.kv.TestBase;
import oracle.kv.impl.async.AsyncVersionedRemote;

import org.junit.Test;

public class ProxyUtilsTest extends TestBase {

    /* Tests */

    @Test
    public void testFindInterfacesRemote() {
        assertEquals(set(RemoteInterface1.class),
                     findInterfaces(RemoteInterface1.class, Remote.class));
        assertEquals(set(RemoteInterface1.class),
                     findInterfaces(RemoteClass1.class, Remote.class));
        assertEquals(set(RemoteInterface2.class),
                     findInterfaces(RemoteInterface2.class, Remote.class));
        assertEquals(set(RemoteInterface1.class, RemoteInterface2.class),
                     findInterfaces(RemoteClass2.class, Remote.class));
    }

    @Test
    public void testFindInterfacesAsync() {
        assertEquals(set(AsyncInterface1.class),
                     findInterfaces(AsyncInterface1.class,
                                    AsyncVersionedRemote.class));
        assertEquals(set(AsyncInterface1.class),
                     findInterfaces(AsyncClass1.class,
                                    AsyncVersionedRemote.class));
        assertEquals(set(AsyncInterface2.class),
                     findInterfaces(AsyncInterface2.class,
                                    AsyncVersionedRemote.class));
        assertEquals(set(AsyncInterface1.class, AsyncInterface2.class),
                     findInterfaces(AsyncClass2.class,
                                    AsyncVersionedRemote.class));
    }

    @Test
    public void testFindInterfaceMethodsRemote() throws Exception {
        assertEquals(set(RemoteInterface1.class.getMethod("remoteMethod1")),
                     findInterfaceMethods(RemoteInterface1.class,
                                          Remote.class));
        assertEquals(set(RemoteInterface1.class.getMethod("remoteMethod1")),
                     findInterfaceMethods(RemoteClass1.class, Remote.class));
        assertEquals(set(RemoteInterface2.class.getMethod("remoteMethod2a"),
                         RemoteInterface2.class.getMethod("remoteMethod2b")),
                     findInterfaceMethods(RemoteInterface2.class,
                                          Remote.class));
        assertEquals(set(RemoteInterface1.class.getMethod("remoteMethod1"),
                         RemoteInterface2.class.getMethod("remoteMethod2a"),
                         RemoteInterface2.class.getMethod("remoteMethod2b")),
                     findInterfaceMethods(RemoteClass2.class,
                                          Remote.class));
    }

    @Test
    public void testFindInterfaceMethodsAsync() throws Exception {
        assertEquals(
            set(AsyncVersionedRemote.class.getMethod(
                    "getSerialVersion", Short.TYPE, Long.TYPE),
                AsyncInterface1.class.getMethod(
                    "remoteMethod1", Short.TYPE, Long.TYPE)),
            findInterfaceMethods(AsyncInterface1.class,
                                 AsyncVersionedRemote.class));
        assertEquals(
            set(AsyncVersionedRemote.class.getMethod(
                    "getSerialVersion", Short.TYPE, Long.TYPE),
                AsyncInterface1.class.getMethod(
                    "remoteMethod1", Short.TYPE, Long.TYPE)),
            findInterfaceMethods(AsyncClass1.class,
                                 AsyncVersionedRemote.class));
        assertEquals(
            set(AsyncInterface2.class.getMethod(
                    "getSerialVersion", Short.TYPE, Long.TYPE),
                AsyncInterface2.class.getMethod(
                    "remoteMethod2a", Short.TYPE, Long.TYPE),
                AsyncInterface2.class.getMethod(
                    "remoteMethod2b", Short.TYPE, Long.TYPE)),
            findInterfaceMethods(AsyncInterface2.class,
                                 AsyncVersionedRemote.class));
        assertEquals(
            set(AsyncInterface2.class.getMethod(
                    "getSerialVersion", Short.TYPE, Long.TYPE),
                AsyncInterface1.class.getMethod(
                    "remoteMethod1", Short.TYPE, Long.TYPE),
                AsyncInterface2.class.getMethod(
                    "remoteMethod2a", Short.TYPE, Long.TYPE),
                AsyncInterface2.class.getMethod(
                    "remoteMethod2b", Short.TYPE, Long.TYPE)),
            findInterfaceMethods(AsyncClass2.class,
                                 AsyncVersionedRemote.class));
    }

    /* Other methods and classes */

    public interface RemoteInterface1 extends Remote {
        void remoteMethod1() throws RemoteException;
    }

    static class RemoteClass1 implements RemoteInterface1 {
        @Override
        public void remoteMethod1() { }
    }

    public interface RemoteInterface2 extends Remote {
        void remoteMethod2a() throws RemoteException;
        void remoteMethod2b() throws RemoteException;
    }

    static class RemoteClass2 implements RemoteInterface1, RemoteInterface2 {
        @Override
        public void remoteMethod1() { }
        @Override
        public void remoteMethod2a() { }
        @Override
        public void remoteMethod2b() { }
    }

    public interface AsyncInterface1 extends AsyncVersionedRemote {
        CompletableFuture<Void> remoteMethod1(short serialVersion,
                                              long timeout);
    }

    static class AsyncClass1 implements AsyncInterface1 {
        @Override
        public CompletableFuture<Short> getSerialVersion(short serialVersion,
                                                         long timeout) {
            return completedFuture((short) 0);
        }
        @Override
        public CompletableFuture<Void> remoteMethod1(short serialVersion,
                                                     long timeout) {
            return completedFuture(null);
        }
    }

    public interface AsyncInterface2 extends AsyncVersionedRemote {
        @Override
        CompletableFuture<Short> getSerialVersion(short serialVersion,
                                                  long timeout);
        CompletableFuture<Void> remoteMethod2a(short serialVersion,
                                               long timeout);
        CompletableFuture<Void> remoteMethod2b(short serialVersion,
                                               long timeout);
    }

    static class AsyncClass2 implements AsyncInterface1, AsyncInterface2 {
        @Override
        public CompletableFuture<Short> getSerialVersion(short serialVersion,
                                                         long timeout) {
            return completedFuture((short) 0);
        }
        @Override
        public CompletableFuture<Void> remoteMethod1(short serialVersion,
                                                     long timeout) {
            return completedFuture(null);
        }
        @Override
        public CompletableFuture<Void> remoteMethod2a(short serialVersion,
                                                      long timeout) {
            return completedFuture(null);
        }
        @Override
        public CompletableFuture<Void> remoteMethod2b(short serialVersion,
                                                      long timeout) {
            return completedFuture(null);
        }
    }

    @SafeVarargs
    private static <T> Set<T> set(T... elements) {
        final Set<T> result = new HashSet<>();
        for (final T e : elements) {
            result.add(e);
        }
        return result;
    }
}
