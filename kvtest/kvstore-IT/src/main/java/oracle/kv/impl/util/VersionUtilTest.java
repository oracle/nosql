/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static oracle.kv.impl.util.VersionUtil.getJavaMajorVersion;
import static org.junit.Assert.assertEquals;

import oracle.kv.TestBase;

import org.junit.Test;

public class VersionUtilTest extends TestBase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void getJavaMajorVersionTest() {
        assertEquals(-1, getJavaMajorVersion(null));
        assertEquals(-1, getJavaMajorVersion("humpty dumpty"));
        assertEquals(-1, getJavaMajorVersion("1.6b"));
        assertEquals(-1, getJavaMajorVersion("9c.0.1"));
        assertEquals(-1, getJavaMajorVersion("10-abc"));

        assertEquals(6, getJavaMajorVersion("1.6.0_45"));
        assertEquals(7, getJavaMajorVersion("1.7.0_80"));
        assertEquals(8, getJavaMajorVersion("1.8.0_161"));
        assertEquals(9, getJavaMajorVersion("9.0.4"));
        assertEquals(10, getJavaMajorVersion("10-ea"));
        assertEquals(10, getJavaMajorVersion("10.0.2"));
        assertEquals(17, getJavaMajorVersion("17.6.45"));
        assertEquals(19, getJavaMajorVersion("19"));
        assertEquals(20, getJavaMajorVersion("20.0.2-ea"));
        assertEquals(23, getJavaMajorVersion("23-ea"));
    }
}
