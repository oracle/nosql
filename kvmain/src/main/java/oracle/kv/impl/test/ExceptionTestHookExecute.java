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
package oracle.kv.impl.test;

/**
 * Identical to TestHookExecute except that it makes provisions for checked
 * exceptions.
 *
 * @see TestHookExecute
 */
public class ExceptionTestHookExecute {

    public static <T, E extends Exception>
        boolean doHookIfSet(ExceptionTestHook<T,E> testHook, T obj)
            throws E {

        if (testHook != null) {
            testHook.doHook(obj);
        }
        return true;
    }
}

