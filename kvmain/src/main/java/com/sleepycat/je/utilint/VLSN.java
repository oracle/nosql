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

public class VLSN {
    public static final int LOG_SIZE = 8;

    public static final long NULL_VLSN = -1;
    public static final long FIRST_VLSN = 1;

    /*
     * The distinguished value used to represent VLSN values that have not
     * been set in log entry fields, because the field did not exist in that
     * version of the log or in a non-HA commit/abort variant of a log entry.
     */
    public static final long UNINITIALIZED_VLSN = 0;

    public static final long INVALID_VLSN = -2;

    public static boolean isNull(long vlsn) {
        return vlsn == NULL_VLSN;
    }

    /**
     * Returns true if the VLSN sequence are terms in the following sequence:
     *
     * NULL_VLSN, i, i + 1, i + 2 ....
     *
     * where i > 0
     */
    public static boolean isNext(long preceding, long curr) {
        return (preceding > 0 && ((preceding + 1) == curr)) ||
               (preceding == NULL_VLSN) ;
    }

    /**
     * Return a VLSN which would follow this one.
     */
    public static long getNext(long vlsn) {
        if (vlsn == NULL_VLSN) {
            return FIRST_VLSN;
        }
        if ((vlsn + 1) < vlsn) {
            throw new ArithmeticException(
              "VLSN value overflowed when calling VLSN.getNext.");
        }
        return vlsn + 1;
    }

    /**
     * Return a VLSN which would precede this one.
     */
    public static long getPrev(long vlsn) {
        if ((vlsn == NULL_VLSN) || (vlsn == 1)) {
            return NULL_VLSN;
        }
        if ((vlsn - 1) > vlsn) {
            throw new ArithmeticException(
              "VLSN value underflowed when calling VLSN.getPrev.");
        }
        return vlsn - 1;
    }

    /**
     * Return true if this VLSN's sequence directly follows the "other"
     * VLSN. This handles the case where "other" is a NULL_VLSN.
     */
    public static boolean follows(long vlsn, long other) {
        return ((other == NULL_VLSN && vlsn == 1) ||
                ((other != NULL_VLSN) &&
                 (other == (vlsn - 1))));
    }
}
