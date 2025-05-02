/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.util.UUID;

import oracle.kv.Consistency;
import oracle.kv.Version;

import org.junit.Test;

/** Test {@link ConsistencyArgument}. */
public class ConsistencyArgumentTestJUnit extends JUnitTestBase {

    /* Tests */

    @Test(expected = IllegalArgumentException.class)
    public void testParseNull() {
        ConsistencyArgument.parseConsistency(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testToStringNull() {
        ConsistencyArgument.toString(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseJunk() {
        ConsistencyArgument.parseConsistency("FooBar");
    }

    @Test
    public void testAbsolute() {
        assertSame(Consistency.ABSOLUTE,
                   ConsistencyArgument.parseConsistency("ABSOLUTE"));
        assertSame(Consistency.ABSOLUTE,
                   ConsistencyArgument.parseConsistency("absolute"));
        assertEquals("ABSOLUTE",
                     ConsistencyArgument.toString(Consistency.ABSOLUTE));
    }

    @Test
    public void testNoneRequired() {
        assertSame(Consistency.NONE_REQUIRED,
                   ConsistencyArgument.parseConsistency("NONE_REQUIRED"));
        assertSame(Consistency.NONE_REQUIRED,
                   ConsistencyArgument.parseConsistency("none_required"));
        assertEquals("NONE_REQUIRED",
                     ConsistencyArgument.toString(Consistency.NONE_REQUIRED));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testNoneRequiredNoMaster() {
        assertSame(Consistency.NONE_REQUIRED_NO_MASTER,
                   ConsistencyArgument.parseConsistency(
                       "NONE_REQUIRED_NO_MASTER"));
        assertSame(Consistency.NONE_REQUIRED_NO_MASTER,
                   ConsistencyArgument.parseConsistency(
                       "none_required_no_master"));
        assertEquals("NONE_REQUIRED_NO_MASTER",
                     ConsistencyArgument.toString(
                         Consistency.NONE_REQUIRED_NO_MASTER));
    }

    @Test
    public void testTime() {
        final Consistency consistency =
            new Consistency.Time(12, MILLISECONDS, 13, MILLISECONDS);
        final String string = "lag=12,timeout=13";
        assertEquals(consistency, ConsistencyArgument.parseConsistency(string));
        assertEquals(string, ConsistencyArgument.toString(consistency));
    }

    @Test
    public void testParseTimeInvalid() {
        try {
            ConsistencyArgument.parseConsistency("foo=12");
            fail("Expected exception");
        } catch (IllegalArgumentException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */
        try {
            ConsistencyArgument.parseConsistency("timeout=12");
            fail("Expected exception");
        } catch (IllegalArgumentException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */
        try {
            ConsistencyArgument.parseConsistency("lag=12,timeout=");
            fail("Expected exception");
        } catch (IllegalArgumentException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */
        try {
            ConsistencyArgument.parseConsistency("lag=abc,timeout=12");
            fail("Expected exception");
        } catch (IllegalArgumentException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */
    }

    @Test
    public void testToStringVersion() {
        final Version version = new Version(UUID.randomUUID(), 42);
        final Consistency consistency =
            new Consistency.Version(version, 3, MILLISECONDS);
        try {
            ConsistencyArgument.toString(consistency);
            fail("Expected exception");
        } catch (IllegalArgumentException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */
    }
}

