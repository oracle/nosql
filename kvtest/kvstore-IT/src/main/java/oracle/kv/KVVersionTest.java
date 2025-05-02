/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv;

import static oracle.kv.impl.util.TestUtils.checkSerialize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import oracle.kv.impl.util.VersionUtil;

import org.junit.Test;

/**
 * Tests KVVersion class and version utilities in isolation.
 */
public class KVVersionTest extends TestBase {

    /**
     * Tests converting the version into a numeric string and back.
     */
    @Test
    public void testStringConversion() {

        /* Test a full version string */
        final String fullString =
            KVVersion.CURRENT_VERSION.toString();

        KVVersion copy = KVVersion.parseVersion(fullString);

        assertTrue(KVVersion.CURRENT_VERSION.equals(copy));

        /* Test a numeric string */
        final String numericString =
            KVVersion.CURRENT_VERSION.getNumericVersionString();

        copy = KVVersion.parseVersion(numericString);

        assertTrue(KVVersion.CURRENT_VERSION.equals(copy));
    }

    /**
     * Tests the minor version compare function.
     */
    @Test
    public void testCompareMinorVersion() {
        /* Equal */
        assertEquals(
           VersionUtil.compareMinorVersion(new KVVersion(11, 2, 1, 1, 1, null),
                                           new KVVersion(11, 2, 1, 1, 1, null)),
           0);

        /* Earlier patch */
        assertEquals(
           VersionUtil.compareMinorVersion(new KVVersion(11, 2, 1, 1, 1, null),
                                           new KVVersion(11, 2, 1, 1, 0, null)),
           0);

        /* Later patch */
        assertEquals(
           VersionUtil.compareMinorVersion(new KVVersion(11, 2, 1, 1, 0, null),
                                           new KVVersion(11, 2, 1, 1, 1, null)),
           0);

        /* Earlier minor  */
        assertEquals(
           VersionUtil.compareMinorVersion(new KVVersion(11, 2, 1, 0, 1, null),
                                           new KVVersion(11, 2, 1, 1, 1, null)),
           -1);

        /* Later minor  */
        assertEquals(
           VersionUtil.compareMinorVersion(new KVVersion(11, 2, 1, 1, 1, null),
                                           new KVVersion(11, 2, 1, 0, 1, null)),
           1);
    }

    @Test
    public void testCompare11gTo12c() {
        KVVersion kvv =
            KVVersion.parseVersion(new KVVersion(11, 2, 1, 2, 1, null).
                                   getVersionString());
        assertEquals(11, kvv.getOracleMajor());
        assertEquals(2, kvv.getOracleMinor());
        assertEquals(1, kvv.getMajor());
        assertEquals(2, kvv.getMinor());
        kvv = KVVersion.parseVersion(new KVVersion(11, 2, 2, 0, 1, null).
                                     getVersionString());
        assertEquals(11, kvv.getOracleMajor());
        assertEquals(2, kvv.getOracleMinor());
        assertEquals(2, kvv.getMajor());
        assertEquals(0, kvv.getMinor());
        kvv = KVVersion.parseVersion(new KVVersion(12, 1, 2, 1, 8, null).
                                     getVersionString());
        assertEquals(12, kvv.getOracleMajor());
        assertEquals(1, kvv.getOracleMinor());
        assertEquals(2, kvv.getMajor());
        assertEquals(1, kvv.getMinor());
    }

    /**
     * Tests various upgrade conditions.
     */
    @Test
    public void testPrerequisite() {

        /* Test either end of the check */
        VersionUtil.checkUpgrade(KVVersion.CURRENT_VERSION, "test");
        VersionUtil.checkUpgrade(KVVersion.PREREQUISITE_VERSION, "test");

        /* R1 should never work */
        try {
            VersionUtil.checkUpgrade(new KVVersion(11, 2, 1, 0, 0, null),
                                    "test");
            fail("prerequisite check failed");
        } catch (IllegalStateException ise) {
            /* expected */
        }

        /* Newer patch should work */
        VersionUtil.checkUpgrade
            (new KVVersion(KVVersion.CURRENT_VERSION.getOracleMajor(),
                           KVVersion.CURRENT_VERSION.getOracleMinor(),
                           KVVersion.CURRENT_VERSION.getMajor(),
                           KVVersion.CURRENT_VERSION.getMinor(),
                           KVVersion.CURRENT_VERSION.getPatch() + 1,
                           null), "test");

        /* Newer versions */
        try {
            VersionUtil.checkUpgrade
                (new KVVersion(KVVersion.CURRENT_VERSION.getOracleMajor() + 1,
                               KVVersion.CURRENT_VERSION.getOracleMinor(),
                               KVVersion.CURRENT_VERSION.getMajor(),
                               KVVersion.CURRENT_VERSION.getMinor(),
                               KVVersion.CURRENT_VERSION.getPatch(),
                               null), "test");
            fail("new oracle major # check failed");
        } catch (IllegalStateException ise) {
            /* expected */
        }

        try {
            VersionUtil.checkUpgrade
                (new KVVersion(KVVersion.CURRENT_VERSION.getOracleMajor(),
                               KVVersion.CURRENT_VERSION.getOracleMinor() + 2,
                               KVVersion.CURRENT_VERSION.getMajor(),
                               KVVersion.CURRENT_VERSION.getMinor(),
                               KVVersion.CURRENT_VERSION.getPatch(),
                               null), "test");
            fail("new oracle minor # check failed");
        } catch (IllegalStateException ise) {
            /* expected */
        }

        try {
            VersionUtil.checkUpgrade
                (new KVVersion(KVVersion.CURRENT_VERSION.getOracleMajor(),
                               KVVersion.CURRENT_VERSION.getOracleMinor(),
                               KVVersion.CURRENT_VERSION.getMajor() + 1,
                               KVVersion.CURRENT_VERSION.getMinor(),
                               KVVersion.CURRENT_VERSION.getPatch(),
                               null), "test");
            fail("new major # check failed");
        } catch (IllegalStateException ise) {
            /* expected */
        }

        try {
            VersionUtil.checkUpgrade
                (new KVVersion(KVVersion.CURRENT_VERSION.getOracleMajor(),
                               KVVersion.CURRENT_VERSION.getOracleMinor(),
                               KVVersion.CURRENT_VERSION.getMajor(),
                               KVVersion.CURRENT_VERSION.getMinor() + 1,
                               KVVersion.CURRENT_VERSION.getPatch(),
                               null), "test");
            fail("new minor # check failed");
        } catch (IllegalStateException ise) {
            /* expected */
        }
    }

    @Test
    public void testSerialize()
        throws Exception {

        checkSerialize(new KVVersion(1, 2, 3, 4, 5, "myversion"));
    }
}
