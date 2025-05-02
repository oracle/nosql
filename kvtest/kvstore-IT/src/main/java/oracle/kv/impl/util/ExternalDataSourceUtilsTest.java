/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.TestBase;

import org.junit.Test;

public class ExternalDataSourceUtilsTest extends TestBase {

    @SuppressWarnings("deprecation")
    @Test
    public void testParseConsistency() {
        assertEquals(Consistency.NONE_REQUIRED,
                     ExternalDataSourceUtils.
                     parseConsistency("none_required"));
        assertEquals(Consistency.NONE_REQUIRED_NO_MASTER,
                     ExternalDataSourceUtils.parseConsistency
                     ("none_required_no_master"));
        assertEquals(Consistency.ABSOLUTE,
                     ExternalDataSourceUtils.parseConsistency("absolute"));
        final Consistency.Time timeBasedConsistency =
            new Consistency.Time(1, TimeUnit.SECONDS, 2, TimeUnit.SECONDS);
        assertEquals(timeBasedConsistency,
                     ExternalDataSourceUtils.parseConsistency
                     ("Time (1000 ms,2000 ms )"));
        assertEquals(timeBasedConsistency,
                     ExternalDataSourceUtils.parseConsistency
                     ("Time(1000 ms,2000 ms)"));
        try {
            ExternalDataSourceUtils.parseConsistency("Time (");
            fail("expected EDSE");
        } catch (ExternalDataSourceException edse) /* CHECKSTYLE:OFF */ {
            /* Expected. */
        }/* CHECKSTYLE:ON */

        try {
            ExternalDataSourceUtils.parseConsistency("Time )");
            fail("expected EDSE");
        } catch (ExternalDataSourceException edse) /* CHECKSTYLE:OFF */ {
            /* Expected. */
        }/* CHECKSTYLE:ON */

        try {
            ExternalDataSourceUtils.parseConsistency("Time )(");
            fail("expected EDSE");
        } catch (ExternalDataSourceException edse) /* CHECKSTYLE:OFF */ {
            /* Expected. */
        }/* CHECKSTYLE:ON */

        try {
            ExternalDataSourceUtils.parseConsistency("Time (1000 ms)");
            fail("expected EDSE");
        } catch (ExternalDataSourceException edse) /* CHECKSTYLE:OFF */ {
            /* Expected. */
        }/* CHECKSTYLE:ON */

        try {
            ExternalDataSourceUtils.
                parseConsistency("Time (1000 ms, 2000ms,3000ms)");
            fail("expected EDSE");
        } catch (ExternalDataSourceException edse) /* CHECKSTYLE:OFF */ {
            /* Expected. */
        }/* CHECKSTYLE:ON */

        try {
            ExternalDataSourceUtils.parseConsistency("Time(ms,3000ms)");
            fail("expected EDSE");
        } catch (ExternalDataSourceException edse) /* CHECKSTYLE:OFF */ {
            /* Expected. */
        }/* CHECKSTYLE:ON */

        try {
            ExternalDataSourceUtils.parseConsistency("Time(3000ms,ms)");
            fail("expected EDSE");
        } catch (ExternalDataSourceException edse) /* CHECKSTYLE:OFF */ {
            /* Expected. */
        }/* CHECKSTYLE:ON */

        try {
            ExternalDataSourceUtils.parseConsistency("weird");
            fail("expected EDSE");
        } catch (ExternalDataSourceException edse) /* CHECKSTYLE:OFF */ {
            /* Expected. */
        }/* CHECKSTYLE:ON */
    }
}
