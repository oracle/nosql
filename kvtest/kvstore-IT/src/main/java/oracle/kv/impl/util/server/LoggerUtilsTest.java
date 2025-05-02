/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util.server;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.logging.Logger;

import org.junit.Test;

public class LoggerUtilsTest {

    /**
     * Verify that thread dumps are generated as expected.
     */
    @Test
    public void testFullThreadDump() throws InterruptedException {

        Logger logger =
            LoggerUtils.getLogger(LoggerUtilsTest.class, "test logger");

        /* Always log the first time around. */
        assertTrue(LoggerUtils.fullThreadDump(logger, 1000, "first"));

        /* Logging too soon. */
        assertFalse(LoggerUtils.fullThreadDump(logger, 10000, "no dump"));

        Thread.sleep(1000);

        /* Timely interval spaced logging. */
        assertTrue(LoggerUtils.fullThreadDump(logger, 500, "timely dump"));
    }
}
