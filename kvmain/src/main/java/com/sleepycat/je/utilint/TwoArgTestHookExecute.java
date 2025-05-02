/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.utilint;

import com.sleepycat.je.rep.impl.node.Feeder.ExitException;

import java.io.IOException;

/**
 * Similar to TestHookExecute except that it takes two inputs.
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

    public static <T, S>
        boolean doExitandIOHookIfSet(TwoArgTestHook<T,S> testHook, 
                                     T arg1, S arg2)
                                     throws ExitException, IOException {
        if (testHook != null) {
            testHook.doExitandIOHook(arg1, arg2);
        }
        return true;
    }
}
