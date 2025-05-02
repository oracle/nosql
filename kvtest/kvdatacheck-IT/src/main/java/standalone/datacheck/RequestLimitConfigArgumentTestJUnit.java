/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import oracle.kv.RequestLimitConfig;

import org.junit.Test;

/** Test {@link RequestLimitConfigArgument}. */
public class RequestLimitConfigArgumentTestJUnit extends JUnitTestBase {

    /** Tests */

    @Test(expected=IllegalArgumentException.class)
    public void testParseNull() {
        RequestLimitConfigArgument.parseRequestLimitConfig(null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testToStringNull() {
        RequestLimitConfigArgument.toString(null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testParseJunk() {
        RequestLimitConfigArgument.parseRequestLimitConfig("Foobar");
    }

    @Test
    public void testLimit() {
        final RequestLimitConfig requestLimit =
                new RequestLimitConfig(100, 90, 90);
        final String limitString = "maxActiveRequests=100," +
                "requestThresholdPercent=90,nodeLimitPercent=90";
        /*
         * Because the RequestLimtiConfig does not override the equals() method,
         * we compare the string representation of them.
         */
        assertEquals(requestLimit.toString(),
                     RequestLimitConfigArgument.parseRequestLimitConfig(
                         limitString).toString());
        assertEquals(limitString,
                     RequestLimitConfigArgument.toString(requestLimit));
    }

    @Test
    public void testParseInvalid() {
        try {
            RequestLimitConfigArgument.parseRequestLimitConfig(
                "maxActiveRequests=100");
            fail("Expected exception");
        } catch (IllegalArgumentException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */
        try {
            RequestLimitConfigArgument.parseRequestLimitConfig(
                "maxActiveRequests=100,requestThresholdPercent=90");
            fail("Expected exception");
        } catch (IllegalArgumentException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */
        try {
            RequestLimitConfigArgument.parseRequestLimitConfig(
                "requestThresholdPercent=90,nodeLimitPercent=90");
            fail("Expected exception");
        } catch (IllegalArgumentException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */
        try {
            RequestLimitConfigArgument.parseRequestLimitConfig(
                "maxActiveRequests=,requestThresholdPercent=90," +
                "nodeLimitPercent=90");
            fail("Expected exception");
        } catch (IllegalArgumentException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */
        try {
            RequestLimitConfigArgument.parseRequestLimitConfig(
                "maxActiveRequests=100,requestThresholdPercent=," +
                "nodeLimitPercent=90");
            fail("Expected exception");
        } catch (IllegalArgumentException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */
        try {
            RequestLimitConfigArgument.parseRequestLimitConfig(
                "maxActiveRequests=100,requestThresholdPercent=90," +
                "nodeLimitPercent=");
            fail("Expected exception");
        } catch (IllegalArgumentException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */
        try {
            RequestLimitConfigArgument.parseRequestLimitConfig(
                "maxActiveRequests=abc,requestThresholdPercent=90," +
                "nodeLimitPercent=90");
            fail("Expected exception");
        } catch (IllegalArgumentException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */
        try {
            RequestLimitConfigArgument.parseRequestLimitConfig(
                "maxActiveRequests=100,requestThresholdPercent=90," +
                "nodeLimitPercent=90.3");
            fail("Expected exception");
        } catch (IllegalArgumentException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */
        try {
            RequestLimitConfigArgument.parseRequestLimitConfig(
                "foo=100,bar=90,nodeLimitPercent=90");
            fail("Expected exception");
        } catch (IllegalArgumentException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */
    }
}
