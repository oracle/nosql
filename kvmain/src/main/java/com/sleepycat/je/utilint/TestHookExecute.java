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

import java.io.IOException;

/**
 * Execute a test hook if set. This wrapper is used so that test hook execution
 * can be packaged into a single statement that can be done within an assert
 * statement.
 */
public class TestHookExecute {

    public static boolean doHookSetupIfSet(TestHook<?> testHook) {
        if (testHook != null) {
            testHook.hookSetup();
        }
        return true;
    }

    public static boolean doIOHookIfSet(TestHook<?> testHook)
        throws IOException {

        if (testHook != null) {
            testHook.doIOHook();
        }
        return true;
    }

    public static boolean doHookIfSet(TestHook<?> testHook) {
        if (testHook != null) {
            testHook.doHook();
        }
        return true;
    }

    public static <T> boolean doHookIfSet(TestHook<T> testHook, T obj) {
        if (testHook != null) {
            testHook.doHook(obj);
        }
        return true;
    }

    public static boolean doExpHookIfSet(TestHook<?> testHook)
        throws Exception {

        if (testHook != null) {
            testHook.doExceptionHook();
        }
        return true;
    }

    public static <T> boolean doExpHookIfSet(TestHook<T> testHook, T obj)
        throws Exception {

        if (testHook != null) {
            testHook.doExceptionHook(obj);
        }
        return true;
    }
}
