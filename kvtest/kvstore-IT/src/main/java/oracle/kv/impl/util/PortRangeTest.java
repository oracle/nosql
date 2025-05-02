/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.mgmt.jmx.JmxAgent;

import org.junit.Test;

/**
 * A test of the oracle.kv.impl.util.PortRange class.
 */
public class PortRangeTest extends TestBase {

    /* used across multiple test methods */
    private static final String VALIDATE_HA_INVALID_FORMAT =
        " is not valid; format should be [firstPort,secondPort]";

    /*
     * 1. PortRange.validateHA()
     */
    @Test
    public void testValidateHAEmptyRange() {

        /* Check a range consisting of the empty string */
        final String invalidRange = "";
        try {
            PortRange.validateHA(invalidRange);
            fail("expected IllegalArgumentException for empty range");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage(),
                       iae.getMessage().contains(VALIDATE_HA_INVALID_FORMAT));
        }
    }

    @Test
    public void testValidateHASingleValue() {

        /* Check a range consisting of one number */
        final String invalidRange = "3000";
        try {
            PortRange.validateHA(invalidRange);
            fail("expected IllegalArgumentException for range: " +
                 invalidRange);
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage(),
                       iae.getMessage().contains(VALIDATE_HA_INVALID_FORMAT));
        }
    }

    @Test
    public void testValidateHATooManyValues() {

        /* Check a range consisting of three numbers */
        final String invalidRange = "3000,4000,5000";
        try {
            PortRange.validateHA(invalidRange);
            fail("expected IllegalArgumentException for range: " +
                 invalidRange);
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage(),
                       iae.getMessage().contains(VALIDATE_HA_INVALID_FORMAT));
        }
    }

    @Test
    public void testValidateHAValuesTooLow() {

        /*
         * Check ranges that have 2 numbers, but which contain at least one
         * number that is invalid as a port range endpoint.  These checks
         * assume that single value checks precede the ordering check.
         */
        final String[] invalidRanges = new String[] {
            "0,3000", "-1,3000", "2000,0", "2000,-1", "0,0", "-1,-1" };
        final String portTooLowMsg = ", ports must be > 0";


        for (String invalidRange : invalidRanges) {
            try {
                PortRange.validateHA(invalidRange);
                fail("expected IllegalArgumentException for range: " +
                     invalidRange);
            } catch (IllegalArgumentException iae) {
                assertTrue(iae.getMessage(),
                           iae.getMessage().contains(portTooLowMsg));
            }
        }
    }

    @Test
    public void testValidateHAInvalidNumber() {

        /*
         * Check ranges that have 2 components, but where at least one is not
         * a number at all.
         */
        final String[] invalidRanges = new String[] {
            "*3,1", "X,1", "1,*3", "1,X" };
        final String invalidNumberMsg = ", not a valid number";

        for (String invalidRange : invalidRanges) {
            try {
                PortRange.validateHA(invalidRange);
                fail("expected IllegalArgumentException for range: " +
                     invalidRange);
            } catch (IllegalArgumentException iae) {
                assertTrue(iae.getMessage(),
                           iae.getMessage().contains(
                               invalidNumberMsg));
            }
        }
    }

    @Test
    public void testValidateHAOrdered() {

        /*
         * Test for ports not being in ascending order
         */
        final String invalidRange = "2000,1000";
        final String invalidOrderMsg = ", firstPort must be <= secondPort";

        try {
            PortRange.validateHA(invalidRange);
            fail("expected IllegalArgumentException for range: " +
                 invalidRange);
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage(),
                       iae.getMessage().contains(invalidOrderMsg));
        }
    }

    @Test
    public void testValidateHAValid() {

        /*
         * Test to make sure that valid ranges are accepted.
         */
        final String[] validRanges = new String[] {
            "1,2", "1000,1000", "1,65535", "65534,65535" };

        for (String validRange : validRanges) {
            try {
                PortRange.validateHA(validRange);
            } catch (IllegalArgumentException iae) {
                fail("unexpected IllegalArgumentException: " +
                     iae.getMessage() + " for range: " + validRange);
            }
        }
    }

    /*
     * 2. PortRange.validateService()
     */
    @Test
    public void testValidateServiceInvalidRange() {

        /* Make sure we complain for a invalid range */
        final String invalidRange = "-1,-1";

        try {
            PortRange.validateService(invalidRange);
            fail("expected IllegalArgumentException for range: " +
                 invalidRange);
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage() != null);
        }
    }

    @Test
    public void testValidateServiceValidRange() {

        /* Make sure that a valid range is accepted */
        final String[] validRanges = new String[] {
            PortRange.UNCONSTRAINED, "1000,2000" };

        for (String validRange : validRanges) {
            try {
                PortRange.validateService(validRange);
            } catch (IllegalArgumentException iae) {
                fail("unexpected IllegalArgumentException: " +
                     iae.getMessage() + " for range: " + validRange);
            }
        }
    }

    /*
     * 3. PortRange.isUnconstrained()
     */
    @Test
    public void testIsUnconstrainedNO() {

        /* Check ranges that are not UNCONSTRAINED */
        final String[] notUnconstrainedRanges = new String[] {
            "3", "-1", "X", "1,2" };

        for (String range : notUnconstrainedRanges) {
            try {
                assertFalse(range, PortRange.isUnconstrained(range));
            } catch (Exception e) {
                fail("unexpected Exception: " + e + " : " + e.getMessage() +
                     " for range: " + range);
            }
        }
    }

    @Test
    public void testIsUnconstrainedYES() {

        /* Check that UNCONSTRAINED is recognized as such */
        try {
            assertTrue(PortRange.isUnconstrained(PortRange.UNCONSTRAINED));
        } catch (Exception e) {
            fail("unexpected Exception: " + e + " : " + e.getMessage() +
                 " for range: " + PortRange.UNCONSTRAINED);
        }
    }

    /*
     * 3. PortRange.validateDisjoint()
     */
    @Test
    public void testValidateDisjointNO() {

        /* Check ranges that overlap */
        final String[][] overlappedRanges = new String[][] {
            new String[] { "1,4", "2,3" },
            new String[] { "1,3", "2,4" },
            new String[] { "1,3", "1,4" },
            new String[] { "1,2", "2,3" } };
        final String overlappingMsg = "Overlapping port ranges: ";

        for (String[] ranges : overlappedRanges) {
            try {
                PortRange.validateDisjoint(ranges[0], ranges[1]);
                fail("expected IllegalArgumentException for ranges: " +
                     ranges[0] + "/" + ranges[1]);
            } catch (IllegalArgumentException iae) {
                assertTrue("ranges " + ranges[0] + "/" + ranges[1] + ":" +
                           iae.getMessage(),
                           iae.getMessage().contains(overlappingMsg));
            }

            /* Try in reverse order as well */
            try {
                PortRange.validateDisjoint(ranges[1], ranges[0]);
                fail("expected IllegalArgumentException for ranges: " +
                     ranges[1] + "/" + ranges[0]);
            } catch (IllegalArgumentException iae) {
                assertTrue("ranges " + ranges[1] + "/" + ranges[0] + ":" +
                           iae.getMessage(),
                           iae.getMessage().contains(overlappingMsg));
            }
        }
    }

    @Test
    public void testValidateDisjointYES() {

        /* Check ranges that do not overlap */
        final String[][] disjointRanges = new String[][] {
            new String[] { "1,2", "3,4" },
            new String[] { "1,2", PortRange.UNCONSTRAINED },
            new String[] { PortRange.UNCONSTRAINED, PortRange.UNCONSTRAINED } };

        for (String[] ranges : disjointRanges) {
            try {
                PortRange.validateDisjoint(ranges[0], ranges[1]);
            } catch (Exception e) {
                fail("unexpected Exception: " + e + ": " + e.getMessage() +
                     " for ranges: " + ranges[0] + "/" + ranges[1]);
            }

            /* Try in reverse order as well */
            try {
                PortRange.validateDisjoint(ranges[1], ranges[0]);
            } catch (Exception e) {
                fail("unexpected Exception: " + e + ": " + e.getMessage() +
                     " for ranges: " + ranges[1] + "/" + ranges[0]);
            }
        }
    }

    /*
     * 4. PortRange.rangeSize()
     */
    @Test
    public void testRangeSizeExceptions() {

        /* Check ranges that are invalid and which give rise to IAE */
        final String[]invalidRangesIAE = new String[] {
            "1", "1,2,3" };

        for (String range : invalidRangesIAE) {
            try {
                PortRange.rangeSize(range);
                fail("expected IllegalArgumentException for range: " + range);
            } catch (IllegalArgumentException iae) {
                assertTrue("range " + range, iae.getMessage() != null);
            } catch (Exception e) {
                fail("range " + range + ": expected IllegalArgumentException" +
                     " instead of " + e + ": " + e.getMessage());
            }
        }

        /* Check ranges that are invalid and which give rise to NFE */
        final String[] invalidRangesNFE = new String[] {
            "1,XYZ", "ABC,65535", "ABC,XYZ" };

        for (String range : invalidRangesNFE) {
            try {
                PortRange.rangeSize(range);
                fail("expected NumberFormatException for range: " + range);
            } catch (NumberFormatException nfe) {
                assertTrue("range " + range, nfe.getMessage() != null);
            } catch (Exception e) {
                fail("range " + range + ": expected NumberFormatException" +
                     " instead of " + e + ": " + e.getMessage());
            }
        }
    }

    @Test
    public void testRangeSizeValid() {

        /* Check that valid yield correct values */
        assertEquals(Integer.MAX_VALUE,
                     PortRange.rangeSize(PortRange.UNCONSTRAINED));
        assertEquals(1, PortRange.rangeSize("1000,1000"));
        assertEquals(1001, PortRange.rangeSize("1000,2000"));
        assertEquals(65535, PortRange.rangeSize("1,65535"));
    }

    /*
     * 5. BootstrapParams constructor (was PortRange.validateSufficientPorts)
     */
    @Test
    public void testValidateSufficientPorts() {

        /*
         * Test various permutations of configuration values, and for each,
         * check the fencepost values of range sizes for too small, and
         * large enough.
         */
        final int[] capacityVals = { 1, 2, 3 };
        final boolean[] isSecureVals = { true, false };
        final boolean[] includeAdminVals = { true, false };
        final String[] mgmtClassVals = { null, JmxAgent.class.getName() };
        final String rangeTooSmallMsg = "Service port range is too small. ";

        for (int capacity : capacityVals) {
            for (boolean isSecure : isSecureVals) {
                for (boolean includeAdmin : includeAdminVals) {
                    for (String mgmtClass : mgmtClassVals) {

                        /*
                         * Simplified version from BootstrapParams, ignoring
                         * socket backlogs
                         */
                        final int required =
                            (isSecure ? 3 : 1) /* SN */ +
                            capacity /* RNs */ +
                            ((mgmtClass != null) ? 1 : 0) +
                            (!includeAdmin ? 0 : isSecure ? 2 : 1);

                        final String config =
                            "capacity:" + capacity +
                            " isSecure:" + isSecure +
                            " includeAdmin:" + includeAdmin +
                            " mgmtClass:" + mgmtClass;

                        /*
                         * Try with a port range that is large enough for our
                         * configuration.
                         */
                        final String validPortRange = "1," + required;
                        @SuppressWarnings("unused")
                        BootstrapParams params;
                        try {
                            params = new BootstrapParams(
                                "/root1", "host1", "haHost1", "1000,1200",
                                validPortRange, "store1", 2000, -1,
                                capacity, null /*storageType*/,
                                isSecure ? "secure1" : null, includeAdmin,
                                mgmtClass);
                        } catch (Exception e) {
                            fail("unexpected exception: " + e + ": " +
                                 e.getMessage() + "for range:" +
                                 validPortRange + " " + config);
                        }

                        /*
                         * Try again with a port range that is one element
                         * smaller than before.
                         */
                        final String smallPortRange = "1," + (required - 1);
                        try {
                            params = new BootstrapParams(
                                "/root1", "host1", "haHost1", "1000,1200",
                                smallPortRange, "store1", 2000, -1,
                                capacity, null /*storageType*/,
                                isSecure ? "secure1" : null, includeAdmin,
                                mgmtClass);
                            fail("for range:" + smallPortRange + " " + config +
                                 ", expected IllegalArgumentException");
                        } catch (IllegalArgumentException iae) {
                            assertTrue("for range:" + smallPortRange + " " +
                                       config + ": " + iae.getMessage(),
                                       iae.getMessage().contains(
                                           rangeTooSmallMsg));
                        } catch (Exception e) {
                            fail("for range:" + smallPortRange + " " + config +
                                 ", expected IllegalArgumentException" +
                                 " instead of " + e + ": " + e.getMessage());
                        }
                    }
                }
            }
        }
    }

    /*
     * 6. PortRange.getRange()
     */
    @Test
    public void testGetRangeUnconstrained() {

        /* Calling getRange for UNCONSTRAINED is illegal */
        final String unconstrainedMsg = "Unconstrained port range";
        try {
            PortRange.getRange(PortRange.UNCONSTRAINED);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage(),
                       iae.getMessage().contains(unconstrainedMsg));
        }
    }

    @Test
    public void testGetRangeExpression() {

        /* Check that an invalid range format yields IAE */
        final String invalidRange = "1";
        final String expressionMsg = "is not a valid port range expression";
        try {
            PortRange.getRange(invalidRange);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage(),
                       iae.getMessage().contains(expressionMsg));
        }
    }

    @Test
    public void testGetRangeNumberFormat() {

        /* Check that a valid range format with non-numbers yields NF */
        try {
            PortRange.getRange("ABC,DEF");
            fail("expected IllegalArgumentException");
        } catch (NumberFormatException nfe) {
            assertTrue(nfe.getMessage(), nfe.getMessage() != null);
        }
    }

    @Test
    public void testGetRangeOK() {

        /* Check that a valid range yields the correct values */
        final String validRange = "1000,2000";
        final List<Integer> range = PortRange.getRange(validRange);
        assertEquals(range.size(), 2);
        assertEquals(range.get(0), Integer.valueOf(1000));
        assertEquals(range.get(1), Integer.valueOf(2000));
    }
}
