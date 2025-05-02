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

import java.io.IOException;

/**
 * Execute a test hook if set. This wrapper is used so that test hook execution
 * can be packaged into a single statement that can be done within an assert
 * statement.
 */
public class TestHookExecute {

    public static <T> boolean doHookIfSet(TestHook<T> testHook, T obj) {
        if (testHook != null) {
            testHook.doHook(obj);
        }
        return true;
    }
    
    public static <T> boolean doIOHookIfSet(TestIOHook<T> testHook, T obj)
        throws IOException {
        
        if (testHook != null) {
            testHook.doHook(obj);
        }
        return true;
    }
}
