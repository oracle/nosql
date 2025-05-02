/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/** Test the Permutation class. */
public class PermutationTestJUnit extends JUnitTestBase {

    /* Tests */

    @Test
    public void testLong() {
        Permutation p = new Permutation(33);
        long l = 0x123456789abcdef0L;
        long transformed = p.transformLong(l);
        assertNotEquals(l, transformed);
        long untransformed = p.untransformLong(transformed);
        assertEquals(l, untransformed);
    }

    @Test
    public void testSixByteLong() {
        Permutation p = new Permutation(12345);
        long l = 0x123456789abcL;
        long transformed = p.transformSixByteLong(l);
        assertNotEquals(l, transformed);
        long untransformed = p.untransformSixByteLong(transformed);
        assertEquals(l, untransformed);
    }

    @Test
    public void testFiveByteLong() {
        Permutation p = new Permutation(-37);
        long l = 0x123456789aL;
        long transformed = p.transformFiveByteLong(l);
        assertNotEquals(l, transformed);
        long untransformed = p.untransformFiveByteLong(transformed);
        assertEquals(l, untransformed);
    }

    @Test
    public void testShort() {
        Permutation p = new Permutation(-838383);
        short s = (short) 0x1234;
        short transformed = p.transformShort(s);
        assertNotEquals(s, transformed);
        short untransformed = p.untransformShort(transformed);
        assertEquals(s, untransformed);
    }

    /* Misc */

    static void assertNotEquals(long x, long y) {
        assertTrue("Both values are equal: " + x, x != y);
    }
}
