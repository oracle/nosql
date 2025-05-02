/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv;

import org.junit.Test;

/**
 * Try class timeouts. This is a unit test class, but the class name doesn't
 * end with "Test", so it won't be run automatically.
 */
@TestClassTimeoutMillis(1*1000)
public class TryClassTimeout extends TestBase {

    @Test
    public void testQuick() {
        System.out.println("testQuick");
    }

    @Test
    public void testSlow() throws Exception {
        Thread.sleep(2000);
        System.out.println("testSlow");
    }

    @Test
    public void testSlow2() throws Exception {
        Thread.sleep(2000);
        System.out.println("testSlow2");
    }

    @Test
    public void testQuick2() throws Exception {
        System.out.println("testQuick2");
    }

    @Test
    public void testCatchInterrupt() {
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
        }
    }
}
