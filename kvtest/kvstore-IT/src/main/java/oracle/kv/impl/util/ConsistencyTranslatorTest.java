/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.TestBase;
import oracle.kv.Version;

import com.sleepycat.je.ReplicaConsistencyPolicy;
import com.sleepycat.je.rep.AbsoluteConsistencyPolicy;
import com.sleepycat.je.rep.CommitPointConsistencyPolicy;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.TimeConsistencyPolicy;

import org.junit.Test;

/**
 * Tests oracle.kv.impl.util.ConsistencyTranslator.
 */
public class ConsistencyTranslatorTest extends TestBase {

    /*
     * Directly test the DurationTranslator.translate() functions
     * using ABSOLUTE consistency
     */
    @Test
    public void testAbsolute() {

        /* Check ABSOLUTE consistency */
        ReplicaConsistencyPolicy jePolicy =
            ConsistencyTranslator.translate(true, Consistency.ABSOLUTE, 100);
        assertTrue(jePolicy instanceof NoConsistencyRequiredPolicy);
        jePolicy =
            ConsistencyTranslator.translate(false, Consistency.ABSOLUTE, 100);
        assertTrue(jePolicy instanceof AbsoluteConsistencyPolicy);
        jePolicy =
            ConsistencyTranslator.translate(Consistency.ABSOLUTE);
        assertTrue(jePolicy instanceof AbsoluteConsistencyPolicy);
    }

    /*
     * Directly test the DurationTranslator.translate() functions
     * using NONE_REQUIRED consistency
     */
    @Test
    public void testNoneRequired() {

        /* Check NONE_REQUIRED consistency */
        final ReplicaConsistencyPolicy rcpResult =
            ConsistencyTranslator.translate(Consistency.NONE_REQUIRED);
        assertEquals(rcpResult, NoConsistencyRequiredPolicy.NO_CONSISTENCY);

        /* Check reverse translation */
        Consistency consResult = ConsistencyTranslator.translate(rcpResult);
        assertEquals(consResult, Consistency.NONE_REQUIRED);

        /* Check reverse translation with kvConsistency */
        consResult = ConsistencyTranslator.translate(rcpResult,
                                                     Consistency.NONE_REQUIRED);
        assertEquals(consResult, Consistency.NONE_REQUIRED);

        /* Check special case for reverse translation of NONE_REQUIRED */
        try {
            ConsistencyTranslator.translate(rcpResult,
                                            Consistency.ABSOLUTE);
            fail("Expected translate(null) to throw UOE");
        } catch (UnsupportedOperationException uoe) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
    }

    /*
     * Directly test the DurationTranslator.translate() function
     * using NONE_REQUIRED_NO_MASTER consistency
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testNoneRequiredNoMaster() {

        /* Check NONE_REQUIRED_NO_MASTER consistency */
        final ReplicaConsistencyPolicy rcpResult =
            ConsistencyTranslator.translate(
                Consistency.NONE_REQUIRED_NO_MASTER);
        assertEquals(rcpResult, NoConsistencyRequiredPolicy.NO_CONSISTENCY);

        /* Check reverse translation with kvConsistency */
        final Consistency consResult =
            ConsistencyTranslator.translate(
                rcpResult,
                Consistency.NONE_REQUIRED_NO_MASTER);
        assertEquals(consResult, Consistency.NONE_REQUIRED_NO_MASTER);
    }

    /*
     * Directly test the DurationTranslator.translate() functions
     * using TIME consistency
     */
    @Test
    public void testTime() {

        /* Check TIME consistency */
        final ReplicaConsistencyPolicy rcpResult =
            ConsistencyTranslator.translate(
                new Consistency.Time(100L,
                                     TimeUnit.MILLISECONDS,
                                     1L,
                                     TimeUnit.SECONDS));
        assertTrue(rcpResult instanceof TimeConsistencyPolicy);

        final Consistency consResult =
            ConsistencyTranslator.translate(rcpResult);
        assertTrue(consResult instanceof Consistency.Time);
    }

    /*
     * Directly test the DurationTranslator.translate() function
     * using VERSION consistency
     */
    @Test
    public void testVersion() {

        /* Check VERSION consistency */
        final long UID_MSB = 123456789L;
        final long UID_LSB = 123456789L;
        final long SOME_VLSN = 5123L;
        final ReplicaConsistencyPolicy rcpResult =
            ConsistencyTranslator.translate(
                new Consistency.Version(new Version(new UUID(UID_MSB, UID_LSB),
                                                    SOME_VLSN),
                                        100L,
                                        TimeUnit.MILLISECONDS));
        assertTrue(rcpResult instanceof CommitPointConsistencyPolicy);

        final Consistency consResult =
            ConsistencyTranslator.translate(rcpResult);
        assertTrue(consResult instanceof Consistency.Version);
    }

    /*
     * Directly test the DurationTranslator.translate() function
     * using invalid consistency
     */
    @Test
    public void testInvalid() {

        /* Check null */
        try {
            ConsistencyTranslator.translate((oracle.kv.Consistency) null);
            fail("Expected translate(null) to throw UOE");
        } catch (UnsupportedOperationException uoe) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        /* Check null in reverse */
        try {
            ConsistencyTranslator.translate((ReplicaConsistencyPolicy) null);
            fail("Expected translate(null) to throw UOE");
        } catch (UnsupportedOperationException uoe) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
    }
}
