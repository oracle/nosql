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
package oracle.kv.impl.security;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * A collection of common code used by proxy-building code in this package.
 */
public final class ProxyUtils {

    /* Not instantiable */
    private ProxyUtils() {
    }

    /**
     * Returns a set of the most-derived interfaces implemented by the
     * specified class, or extended by the specified interface, that implement
     * the specified interface, possibly including the requested interface
     * itself.
     *
     * @param <T> the type of the interface to check for
     * @param type the class or interface to check
     * @param findIface the interface to check for
     * @return a set of the implemented interfaces
     */
    static <T> Set<Class<?>> findInterfaces(Class<? extends T> type,
                                            Class<T> findIface) {
        final Set<Class<?>> interfaces = new HashSet<>();
        if (type.isInterface() && findIface.isAssignableFrom(type)) {
            interfaces.add(type);
        }
        for (final Class<?> iface : type.getInterfaces()) {
            if (findIface.isAssignableFrom(iface)) {
                final Iterator<Class<?>> iter = interfaces.iterator();
                boolean skip = false;
                while (iter.hasNext()) {
                    final Class<?> known = iter.next();
                    if (known.isAssignableFrom(iface)) {
                        /* iface is more derived -- replace known  */
                        iter.remove();
                    } else if (iface.isAssignableFrom(known)) {
                        /* known is more derived -- skip iface */
                        skip = true;
                        break;
                    }
                }
                if (!skip) {
                    interfaces.add(iface);
                }
            }
        }
        return interfaces;
    }

    /**
     * Returns a set of the methods defined on all of the most-derived
     * interfaces implemented by the specified class, or interfaces extended by
     * the specified interface, that implement the specified interface,
     * possibly including the interface itself. Excludes overridden methods.
     *
     * @param <T> the type of interface to check for
     * @param type the type to check
     * @param findIface the interface to check for
     * @return a set of the methods
     */
    public static <T>
        Set<Method> findInterfaceMethods(Class<? extends T> type,
                                         Class<T> findIface) {
        final Set<Method> methods = new HashSet<>();
        for (final Class<?> iface : findInterfaces(type, findIface)) {
            for (final Method meth : iface.getMethods()) {
                final Iterator<Method> iter = methods.iterator();
                boolean skip = false;
                while (iter.hasNext()) {
                    final Method known = iter.next();
                    if (meth.getName().equals(known.getName()) &&
                        Arrays.equals(meth.getParameterTypes(),
                                      known.getParameterTypes())) {
                        if (known.getDeclaringClass().isAssignableFrom(
                                meth.getDeclaringClass())) {
                            /* meth overrides known -- replace known */
                            iter.remove();
                        } else if (meth.getDeclaringClass().isAssignableFrom(
                                       known.getDeclaringClass())) {
                            /* known overrides meth -- skip meth */
                            skip = true;
                            break;
                        }
                    }
                }
                if (!skip) {
                    methods.add(meth);
                }
            }
        }
        return methods;
    }
}
