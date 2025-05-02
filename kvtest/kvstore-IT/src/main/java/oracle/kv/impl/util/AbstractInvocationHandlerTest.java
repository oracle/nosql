/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import oracle.kv.TestBase;

import org.junit.Test;

/** Tests for the AbstractInvocationHandler class. */
public class AbstractInvocationHandlerTest extends TestBase {

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
    public void testEquals() {
        InvocationHandler ih = new ObjectOnlyInvocationHandler();
        Runnable proxy = createProxy(Runnable.class, ih);
        assertTrue(proxy.equals(proxy));
        assertTrue(proxy.equals(createProxy(Runnable.class, ih)));
        assertTrue(proxy.equals(
                        createProxy(Runnable.class,
                                    new ObjectOnlyInvocationHandler())));
        Object other = createProxy(Iterable.class, ih);
        assertFalse(proxy.equals(other));
        assertFalse(proxy.equals(
                        createProxy(
                            Runnable.class,
                            new AnotherObjectOnlyInvocationHandler())));
        assertFalse(proxy.equals(
                        new Runnable() {
                            @Override
                            public void run() { }
                        }));
        assertFalse(proxy.equals(null));
        other = "hi";
        assertFalse(proxy.equals(other));
    }

    @Test
    public void testHashCode() {
        InvocationHandler handler = new ObjectOnlyInvocationHandler();
        Runnable proxy = createProxy(Runnable.class, handler);
        assertEquals(handler.hashCode(), proxy.hashCode());
    }

    @Test
    public void testToString() {
        Runnable proxy = createProxy(Runnable.class,
                                     new ObjectOnlyInvocationHandler());
        String string = proxy.toString();
        final String pattern = "Proxy@[0-9a-f]+\\[Runnable, [^]]+]";
        assertTrue("Expected pattern: '" + pattern + "', found: " + string,
                   string.matches(pattern));
    }

    @Test
    public void testInvoke() throws Exception {
        Method fMethod = I.class.getMethod("f", String.class);
        assertEquals("R",
                     createProxy(I.class,
                                 (proxy, method, args) -> {
                                     assertEquals(fMethod, method);
                                     assertEquals(1, args.length);
                                     assertEquals("A", args[0]);
                                     return "R";
                                 })
                     .f("A"));
        Method gMethod = I.class.getMethod("g");
        assertEquals(3,
                     createProxy(I.class,
                                 (proxy, method, args) -> {
                                     assertEquals(gMethod, method);
                                     assertArrayEquals(null, args);
                                     return 3;
                                 })
                     .g());
    }

    interface I {
        String f(String x);
        int g();
    }

    /* Other methods and classes */

    @SuppressWarnings("unchecked")
    static <T> T createProxy(Class<T> iface, InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(
            AbstractInvocationHandlerTest.class.getClassLoader(),
            new Class<?>[] { iface }, handler);
    }

    static class ObjectOnlyInvocationHandler
            extends AbstractInvocationHandler {
        @Override
        protected Object invokeNonObject(Method method, Object[] args) {
            throw new RuntimeException("Unexpected non-Object call");
        }
        @Override
        public int hashCode() {
            return 42;
        }
        @Override
        public boolean equals(Object object) {
            return object instanceof ObjectOnlyInvocationHandler;
        }
    }

    static class AnotherObjectOnlyInvocationHandler
            extends AbstractInvocationHandler {
        @Override
        protected Object invokeNonObject(Method method, Object[] args) {
            throw new RuntimeException("Unexpected non-Object call");
        }
    }
}
