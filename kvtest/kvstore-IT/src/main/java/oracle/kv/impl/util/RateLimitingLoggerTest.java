/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.logging.Level;

import oracle.kv.TestBase;

import org.junit.Test;

public class RateLimitingLoggerTest extends TestBase {

    @Test
    public void test() throws InterruptedException {
       final int logSamplePeriodMs = 5 * 1000;

       long startMs = System.currentTimeMillis();
       final int maxObjects = 10;
       RateLimitingLogger<Object> rlogger = new RateLimitingLogger<>
           (logSamplePeriodMs, maxObjects, logger);

       Object obj1 = new Object();
       Object obj2 = new Object();
       generateLogMessages(rlogger, obj1, 100);
       generateLogMessages(rlogger, obj2, 100);

       /* Sanity check, above should take time << logSamplePeriodMs */
       assertTrue(System.currentTimeMillis() < (startMs + logSamplePeriodMs));

       /* Just two messages should actually be logged. */
       assertEquals(2, rlogger.getLimitedMessageCount());

       /*
        * Thread sleeps a little longer than logSamplePeriodMs to
        * prevent that sleep time is not enough because of the precision
        * of the system.
        */
       Thread.sleep(logSamplePeriodMs + 100);

       generateLogMessages(rlogger, obj1, 100);
       generateLogMessages(rlogger, obj2, 100);

       /* Two more for the next sample period */
       assertEquals(4, rlogger.getLimitedMessageCount());

       /* Verify that map size is limited. */
       startMs = System.currentTimeMillis();
       rlogger = new RateLimitingLogger<>(logSamplePeriodMs,
                                          maxObjects, logger);

       for (int i=0; i < 2 * maxObjects; i++) {
           rlogger.log(new Object(), Level.INFO, "message:" + i);
       }

       assertTrue(System.currentTimeMillis() < (startMs + logSamplePeriodMs));

       /* Verify that the map stays within bounds. */
       assertEquals(maxObjects, rlogger.getMapSize());

       /* A message should have been written for each distinct object. */
       assertEquals(2 * maxObjects, rlogger.getLimitedMessageCount());
    }

    /**
     * Test rate limit logger can account the suppressed messages
     */
    @Test
    public void testSuppressed() throws InterruptedException {

        long startMs = System.currentTimeMillis();
        final int maxObjects = 2;
        final int logSamplePeriodMs = 5 * 1000;

        /* # of log messages */
        int logMsgs = 100;
        /* # of log messages that get suppressed */
        int suppressed;
        RateLimitingLogger<Object> rlogger = new RateLimitingLogger<>
            (logSamplePeriodMs, maxObjects, logger);

        final Object obj = new Object();
        generateLogMessages(rlogger, obj, logMsgs);

        /* Sanity check, above should take time << logSamplePeriodMs */
        assertTrue(System.currentTimeMillis() < (startMs + logSamplePeriodMs));

        /* only the first msg should be logged. */
        assertEquals(1, rlogger.getLimitedMessageCount());
        /* verify all except messages should be suppressed */
        suppressed = logMsgs - 1;
        assertEquals(suppressed, rlogger.getNumSuppressedMsgs(obj));

        /* sleep longer than the log dump interval */
        Thread.sleep(logSamplePeriodMs + 100);

        /* write one log, should be logged, no suppressed messages */
        logMsgs = 1;
        suppressed = 0;
        generateLogMessages(rlogger, obj, logMsgs);
        /* two logs have been dumped in total */
        assertEquals(2, rlogger.getLimitedMessageCount());
        assertEquals(suppressed, rlogger.getNumSuppressedMsgs(obj));

        /* write another log, should be suppressed */
        suppressed = 1;
        generateLogMessages(rlogger, obj, 1);
        assertEquals(suppressed, rlogger.getNumSuppressedMsgs(obj));

        /* one more round of test */
        Thread.sleep(logSamplePeriodMs + 100);
        logMsgs = 100;
        suppressed = logMsgs - 1;
        generateLogMessages(rlogger, obj, 100);
        assertEquals(3, rlogger.getLimitedMessageCount());
        assertEquals(suppressed, rlogger.getNumSuppressedMsgs(obj));
    }

    private void generateLogMessages(RateLimitingLogger<Object> rlogger,
                                     Object obj,
                                     int nMessages) {
        for (int i=0; i < nMessages; i++) {
            rlogger.log(obj, Level.INFO, "message:" + i);
        }
    }
}
