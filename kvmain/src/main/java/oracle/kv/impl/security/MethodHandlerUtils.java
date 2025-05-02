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

import static oracle.kv.impl.util.AbstractInvocationHandler.invokeMethod;

import java.lang.reflect.Method;

/**
 * Common code for MethodHandler interaction
 */
final class MethodHandlerUtils {

    /* Not instantiable */
    private MethodHandlerUtils() {
    }

    /**
     * An implementation of MethodHandler that provides basic method
     * invocation support.
     */
    static class DirectHandler implements MethodHandler {
        private final Object target;

        DirectHandler(Object target) {
            this.target = target;
        }

        @Override
        public Object invoke(Method method, Object[] args)
            throws Exception {

            return invokeMethod(target, method, args);
        }
    }
}
