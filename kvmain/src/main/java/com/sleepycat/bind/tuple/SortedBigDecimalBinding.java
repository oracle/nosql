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

package com.sleepycat.bind.tuple;

import java.math.BigDecimal;

import com.sleepycat.je.DatabaseEntry;

/**
 * A concrete <code>TupleBinding</code> for a sorted <code>BigDecimal</code>
 * value.
 *
 * <p>There are two ways to use this class:</p>
 * <ol>
 * <li>When using the {@link com.sleepycat.je} package directly, the static
 * methods in this class can be used to convert between primitive values and
 * {@link DatabaseEntry} objects.</li>
 * <li>When using the {@link com.sleepycat.collections} package, an instance of
 * this class can be used with any stored collection.</li>
 * </ol>
 *
 * @see <a href="package-summary.html#bigDecimalFormats">BigDecimal Formats</a>
 */
public class SortedBigDecimalBinding extends TupleBinding<BigDecimal> {

    // javadoc is inherited
    public BigDecimal entryToObject(TupleInput input) {

        return input.readSortedBigDecimal();
    }

    // javadoc is inherited
    public void objectToEntry(BigDecimal object, TupleOutput output) {

        output.writeSortedBigDecimal(object);
    }

    // javadoc is inherited
    protected TupleOutput getTupleOutput(BigDecimal object) {

        return sizedOutput(object);
    }

    /**
     * Converts an entry buffer into a <code>BigDecimal</code> value.
     *
     * @param entry is the source entry buffer.
     *
     * @return the resulting value.
     */
    public static BigDecimal entryToBigDecimal(DatabaseEntry entry) {

        return entryToInput(entry).readSortedBigDecimal();
    }

    /**
     * Converts a <code>BigDecimal</code> value into an entry buffer.
     *
     * @param val is the source value.
     *
     * @param entry is the destination entry buffer.
     */
    public static void bigDecimalToEntry(BigDecimal val, DatabaseEntry entry) {

        outputToEntry(sizedOutput(val).writeSortedBigDecimal(val), entry);
    }

    /**
     * Returns a tuple output object of the maximum size needed, to avoid
     * wasting space when a single primitive is output.
     */
    private static TupleOutput sizedOutput(BigDecimal val) {

        int len = TupleOutput.getSortedBigDecimalMaxByteLength(val);
        return new TupleOutput(new byte[len]);
    }
}
