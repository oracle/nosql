/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static oracle.kv.impl.util.NumberUtil.roundUpMultiple64;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;

import oracle.kv.TestBase;

import org.junit.Test;

public class NumberUtilTest extends TestBase {

    @Test
    public void testRoundUpMultiple64() {
        checkException(() -> roundUpMultiple64(-1),
                       IllegalArgumentException.class, "out of range");
        checkException(() -> roundUpMultiple64(0),
                       IllegalArgumentException.class, "out of range");
        checkException(() -> roundUpMultiple64(2147483585),
                       IllegalArgumentException.class, "out of range");
        checkException(() -> roundUpMultiple64(Integer.MAX_VALUE),
                       IllegalArgumentException.class, "out of range");
        assertEquals(64, roundUpMultiple64(1));
        assertEquals(64, roundUpMultiple64(63));
        assertEquals(64, roundUpMultiple64(64));
        assertEquals(128, roundUpMultiple64(65));
        assertEquals(128, roundUpMultiple64(100));
        assertEquals(128, roundUpMultiple64(127));
        assertEquals(128, roundUpMultiple64(128));
        assertEquals(192, roundUpMultiple64(129));
        assertEquals(1024, roundUpMultiple64(1024));
        assertEquals(2147483584, roundUpMultiple64(2147483550));
        assertEquals(2147483584, roundUpMultiple64(2147483584));
    }
}
