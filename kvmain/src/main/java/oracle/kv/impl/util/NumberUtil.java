/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv.impl.util;

/**
 * Utility methods for numbers.
 */
public class NumberUtil {

    /** This class should not be instantiated. */
    private NumberUtil() {
        throw new AssertionError();
    }

    /**
     * Convert a long to an int, returning Integer.MAX_VALUE if the value is
     * too big, Integer.MIN_VALUE if it is too small, and otherwise just
     * converting to the proper type.
     *
     * @param l the long
     * @return the associated int
     */
    public static int longToIntOrLimit(long l) {
        return l > Integer.MAX_VALUE ?
            Integer.MAX_VALUE :
            l < Integer.MIN_VALUE ?
            Integer.MIN_VALUE :
            (int) l;
    }

    /**
     * Round an integer up to a multiple of 64. The argument must be in the
     * range 1 to 2147483584 inclusive. The upper bound is the largest valid
     * int value that is a multiple of 64.
     *
     * @param i the integer
     * @return the value rounded up to a multiple of 64
     * @throws IllegalArgumentException if the argument is out of range
     */
    public static int roundUpMultiple64(int i) {
        if ((i < 1) || (i > 2147483584)) {
            throw new IllegalArgumentException(
                "Argument to roundUpMultiple64 is out of range: " + i);
        }

        /* Convert 1-64 to 63, and then add 1 to get 64 */
        return ((i - 1) | 63) + 1;
    }
}
