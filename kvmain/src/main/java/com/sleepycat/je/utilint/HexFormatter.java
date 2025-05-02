/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.utilint;

import java.util.Arrays;

public class HexFormatter {

    /**
     * Format a long in hex with a "0x" prefix. Coded to do the minimum amount
     * of object allocation.
     */
    public static String formatLong(long l) {
        /* 2 chars for "0x" plus 16 for the hex value */
        final char[] chars = new char[18];
        Arrays.fill(chars, '0');
        chars[1] = 'x';
        final int zeroBits = Long.numberOfLeadingZeros(l);
        final int zeroChars = zeroBits >> 4;
        final int prefix = zeroChars + 2;
        /* Least significant place has highest index */
        for (int i = 17; i >= prefix; i--) {
            final int d = ((int) l) & 0xf;
            chars[i] = Character.forDigit(d, 16);
            l >>>= 4;
        }
        return new String(chars);
    }
}
