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

package com.sleepycat.utilint;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.DecimalFormat;
import java.text.Format;
import java.util.Iterator;
import java.util.SortedSet;

import com.sleepycat.je.EnvironmentFailureException;

/**
 * A home for misc formatting utilities.
 */
public class FormatUtil {

    private static final ThreadLocal<DecimalFormat> DECIMAL_SCALE0 =
        new ThreadLocal<DecimalFormat>() {
            @Override
            public DecimalFormat initialValue() {
                return new DecimalFormat("###,###,###,###,###,###,###");
            }
        };

    private static final ThreadLocal<DecimalFormat> DECIMAL_SCALE2 =
        new ThreadLocal<DecimalFormat>() {
            @Override
            public DecimalFormat initialValue() {
                return new DecimalFormat("###,###,###,###,###,###,###.##");
            }
        };

    /**
     * Returns a thread-local DecimalFormat with US formatting and scale zero:
     * ###,###,###,###,###,###,###
     *
     * <p>A ThreadLocal value is returned because {@link Format} objects are
     * generally not synchronized. The returned value should only be used in
     * the current thread.</p>
     */
    public static DecimalFormat decimalScale0() {
        return DECIMAL_SCALE0.get();
    }

    /**
     * Returns a thread-local DecimalFormat with US formatting and scale two:
     * ###,###,###,###,###,###,###.##
     *
     * <p>A ThreadLocal value is returned because {@link Format} objects are
     * generally not synchronized. The returned value should only be used in
     * the current thread.</p>
     */
    public static DecimalFormat decimalScale2() {
        return DECIMAL_SCALE2.get();
    }

    /**
     * Utility class to convert a sorted set of long values to a compact string
     * suitable for printing. The representation is made compact by identifying
     * ranges so that the sorted set can be represented as a sequence of hex
     * ranges and singletons.
     */
    public static String asHexString(final SortedSet<Long> set) {

        if (set.isEmpty()) {
            return "";
        }

        final StringBuilder sb = new StringBuilder();
        final Iterator<Long> i = set.iterator();
        long rstart = i.next();
        long rend = rstart;

        while (i.hasNext()) {
            final long f = i.next();
            if (f == (rend + 1)) {
                /* Continue the existing range. */
                rend++;
                continue;
            }

            /* flush and start new range */
            flushRange(sb, rstart, rend);
            rstart = rend = f;
        }

        flushRange(sb, rstart, rend);

        /* Discard initial space in return value. */
        sb.deleteCharAt(0);

        return sb.toString();
    }

    private static void flushRange(final StringBuilder sb,
                                   final long rstart,
                                   final long rend) {
        if (rstart == -1) {
            return;
        }

        if (rstart == rend) {
            sb.append(" 0x").append(Long.toHexString(rstart));
        } else {
            sb.append(" 0x").append(Long.toHexString(rstart)).
            append("-").
            append("0x").append(Long.toHexString(rend));
        }
    }

    /**
     * Clones an object by serializing it and then de-serializing it.
     */
    public static <T> T cloneBySerialization(final T obj) {
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            (new ObjectOutputStream(baos)).writeObject(obj);
            final byte[] bytes = baos.toByteArray();

            final ObjectInputStream ois = new ObjectInputStream(
                new ByteArrayInputStream(bytes));

            @SuppressWarnings("unchecked")
            final T ret = (T) ois.readObject();

            return ret;

        } catch (ClassNotFoundException|IOException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }
    }
}
