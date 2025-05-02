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

/**
 * A common interface for calling proxy methods through reflection.
 */
interface MethodHandler {

    /**
     * Call the method with the provided arguments.  The target of the method
     * call is implied by the implementation.
     *
     * @param method a Method that should be called
     * @param args an argument list that should be passed to the method
     * @return an unspecified return type
     * @throws Exception
     */
    Object invoke(Method method, Object[] args)
        throws Exception;
}
