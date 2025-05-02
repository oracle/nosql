/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertEquals;
import oracle.kv.TestBase;

import org.junit.Test;

/**
 * Tests oracle.kv.impl.util.DurationTranslator.
 */
public class DurationTranslatorTest extends TestBase {

    /*
     * Directly test the DurationTranslator.translate() function
     */
    @Test
    public void testTranslate() {

        final long inputDuration = 30L;
        final String inputUnits = "SECONDS";
        final long expectedResult = 30000L;

        final long durationValue =
            DurationTranslator.translate(inputDuration, inputUnits);
        assertEquals(durationValue, expectedResult);
    }
}
