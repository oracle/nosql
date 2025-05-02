/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import oracle.kv.TestBase;

import org.junit.Test;

/**
 * A dead simple test of the oracle.kv.impl.util.Pair class.
 */
public class PairTest extends TestBase {

    @Test
    public void testPair() {

        final String sX = "X";
        final Integer i3 = 3;
        final Pair<String, Integer> psi = new Pair<String, Integer>(sX, i3);
        assertEquals(sX, psi.first());
        assertEquals(i3, psi.second());

        final Pair<Object, Object> pnn = new Pair<Object, Object>(null, null);
        assertTrue(null == pnn.first());
        assertTrue(null == pnn.second());
    }
}
