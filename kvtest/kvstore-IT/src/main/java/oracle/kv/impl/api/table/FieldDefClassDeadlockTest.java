/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertTrue;

import oracle.kv.TestBase;

import org.junit.Test;

/**
 * Test that we don't get a static class initialization deadlock when
 * referencing the FieldDefImpl[.Constants].timestamps field while
 * simultaneously creating a TimestampDefImpl. Of course, the Constants inner
 * class breaks the deadlock, so this test shouldn't deadlock anymore, but just
 * in case. [KVSTORE-1484]
 */
public class FieldDefClassDeadlockTest extends TestBase {

    /**
     * Reference FieldDefImpl class indirectly so it doesn't get resolved too
     * early.
     */
    private static class GetTimestamp {
        static boolean getTimestamp() {
            return FieldDefImpl.Constants.timestampDefs[0] != null;
        }
    }

    @Test
    public void testDeadlock() throws Exception {
        final Thread thread = new Thread() {
            @SuppressWarnings("unused")
            @Override
            public void run() {
                new TimestampDefImpl(1);
            }
        };
        thread.start();

        assertTrue(GetTimestamp.getTimestamp());
        thread.join();
   }
}
