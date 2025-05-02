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
 * Similar to TestHookExecute except that it takes one more input.
 *
 * @see TestHookExecute
 */
public class TwoArgTestHookExecute {

    public static <T, S>
        boolean doHookIfSet(TwoArgTestHook<T,S> testHook, T arg1, S arg2) {
        if (testHook != null) {
            testHook.doHook(arg1, arg2);
        }
        return true;
    }
}
