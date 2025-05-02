/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv.impl.util;

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Formatter;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * A base class for invocation handlers that provides a standard implementation
 * of Object methods.
 *
 * <p>By default, this class has identity-based hashCode and equals methods,
 * but subclasses may override this behavior. Note that the invocation
 * handler's hashCode and equals methods are used to implement those methods on
 * the proxy, so subclasses need to be sure that they are defined properly.
 */
public abstract class AbstractInvocationHandler implements InvocationHandler {

    /** Creates an instance of this class. */
    protected AbstractInvocationHandler() { }

    /**
     * Call the method with the provided arguments and handle reflection
     * exceptions.
     *
     * @param target the target object of an invocation
     * @param method a Method that should be called
     * @param args an argument list that should be passed to the method
     * @return an unspecified return type
     * @throws Exception anything that the underlying method could produce,
     * except that anything that is not Error or Exception is wrapped in an
     * UndeclaredThrowableException.
     */
    public static Object invokeMethod(Object target,
                                      Method method,
                                      Object[] args)
        throws Exception
    {
        try {
            try {
                return method.invoke(target, args);
            } catch (InvocationTargetException ite) {
                throw ite.getCause();
            }
        } catch (Exception e) {
            throw e;
        } catch (Error e) {
            throw e;
        } catch (Throwable t) {
            throw new UndeclaredThrowableException(t, "Unexpected throw: " + t);
        }
    }

    /**
     * Handle Object methods and defer other methods to invokeNonObject. The
     * hashCode method calls the associated method on this invocation handler.
     * The equals method returns true if the argument is a dynamic proxy of the
     * same class as the specified proxy and its invocation handler equals this
     * one.
     */
    @Override
    public Object invoke(Object proxy,
                         Method method,
                         Object[] args)
        throws Exception {

        final Method methodNonNull = checkNull("method", method);
        if (methodNonNull.getDeclaringClass() == Object.class) {
            switch (methodNonNull.getName()) {
            case "hashCode":
                return hashCode();
            case "equals":
                final Object object = args[0];
                if (this == object) {
                    return true;
                }
                return (object != null) &&
                    (proxy.getClass() == object.getClass()) &&
                    Proxy.isProxyClass(object.getClass()) &&
                    equals(Proxy.getInvocationHandler(object));
            case "toString":
                return proxyToString(checkNull("proxy", proxy));
            default:
                throw new IllegalArgumentException(
                    "Unexpected Object method: " + methodNonNull);
            }
        }
        return invokeNonObject(methodNonNull, args);
    }

    /**
     * Called to implement methods not defined on Object.
     *
     * @param method the method
     * @param args the method arguments or null
     * @return the result of the method call
     * @throws Exception if the method call throws an exception
     */
    protected abstract Object invokeNonObject(@NonNull Method method,
                                              Object[] args)
        throws Exception;

    /**
     * Implement Object.toString for proxies. Include the proxy's identity hash
     * code, the unqualified name of the first implemented interface, and the
     * invocation handler.
     */
    protected String proxyToString(Object proxy) {
        final StringBuilder sb = new StringBuilder();
        try (final Formatter fmt = new Formatter(sb)) {
            fmt.format("Proxy@%x[", System.identityHashCode(proxy));
            final Class<?>[] interfaces = proxy.getClass().getInterfaces();
            if (interfaces.length > 0) {
                String iface = interfaces[0].getName();
                final int dot = iface.lastIndexOf('.');
                if (dot >= 0) {
                    iface = iface.substring(dot + 1);
                }
                fmt.format("%s, ", iface);
            }
            fmt.format("%s]", this);
            return sb.toString();
        }
    }

    /** Returns a value with the unqualified class name and the hash code. */
    @Override
    public String toString() {
        String className = getClass().getName();
        final int dot = className.lastIndexOf('.');
        if (dot >= 0) {
            className = className.substring(dot + 1);
        }
        return String.format("%s@%x",
                             className,
                             hashCode());
    }
}
