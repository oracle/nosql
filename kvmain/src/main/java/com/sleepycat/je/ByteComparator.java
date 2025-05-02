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
package com.sleepycat.je;

import com.sleepycat.je.tree.Key;

/**
 * Key and duplicate data comparator that supports optimizations for avoiding
 * memory allocations.
 *
 * <p>To reduce GC costs, this interface should be implemented rather than
 * {@link java.util.Comparator} with a byte array param. a Comparator with a
 * byte array param requires that a complete byte array be constructed, which
 * almost always requires allocation of a new byte array for every
 * comparison.</p>
 *
 * <p>However, a ByteComparator only applies in general when bytes can be
 * compared one at a time. More specifically, a ByteComparator must be able
 * to compare any number of bytes in the key at a time, and therefore
 * typically can't rely on materialization of the bytes as an object. If
 * such materialization is required, use of a Comparator with a byte array
 * param is the only option.</p>
 *
 * @see SecondaryConfig#setBtreeByteComparator(Class)
 * @see SecondaryConfig#setBtreeByteComparator(ByteComparator)
 */
public interface ByteComparator {

    ByteComparator DEFAULT = Key::compareUnsignedBytes;

    /**
     * This method may be called for any portion of a key or duplicate data
     * item contained in an array, as indicate by the offset and length params.
     */
    int compare(byte[] key1,
                int key1Offset,
                int key1Length,
                byte[] key2,
                int key2Offset,
                int key2Length);
}
