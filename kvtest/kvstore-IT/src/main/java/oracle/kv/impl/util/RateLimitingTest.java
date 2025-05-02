/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import oracle.kv.TestBase;

import org.junit.Test;

public class RateLimitingTest extends TestBase {

    @Test
    public void test() throws InterruptedException {
       final int eventSamplePeriodMs = 5 * 1000;
       final int maxObjects = 10;
       RateLimiting<Object> limiter =
           new RateLimiting<>(eventSamplePeriodMs, maxObjects);

       /*
        * 1. Verify message rate can be limited.
        */
       Object obj1 = new Object();
       Object obj2 = new Object();
       assertTrue(limiter.isHandleable(obj1));
       assertTrue(limiter.isHandleable(obj2));
       assertFalse(limiter.isHandleable(obj1));
       assertFalse(limiter.isHandleable(obj2));
       assertFalse(limiter.isHandleable(obj1));
       assertFalse(limiter.isHandleable(obj2));

       /* Just two messages should actually be handled. */
       assertEquals(2, limiter.getLimitedMessageCount());

       /* 
        * 2. Verify rate limiting is reset after eventSamplePeriodMs time.
        * Thread sleeps a little longer than eventSamplePeriodMs to
        * prevent that sleep time is not enough because of the precision
        * of the system.
        */
       Thread.sleep(eventSamplePeriodMs + 100);

       assertTrue(limiter.isHandleable(obj1));
       assertFalse(limiter.isHandleable(obj1));
       assertTrue(limiter.isHandleable(obj2));
       assertFalse(limiter.isHandleable(obj2));
       assertFalse(limiter.isHandleable(obj1));
       assertFalse(limiter.isHandleable(obj2));

       /* Two more for the next sample period */
       assertEquals(4, limiter.getLimitedMessageCount());

       /*
        * 3. Verify map size is limited.
        */
       limiter = new RateLimiting<>(eventSamplePeriodMs, maxObjects);

       for (int i = 0; i < 2 * maxObjects; i++) {
          limiter.isHandleable(new Object());
       }

       /* Verify that the map stays within bounds. */
       assertEquals(maxObjects, limiter.getMapSize());

       /* A message should have been written for each distinct object. */
       assertEquals(2 * maxObjects, limiter.getLimitedMessageCount());
    }
}
