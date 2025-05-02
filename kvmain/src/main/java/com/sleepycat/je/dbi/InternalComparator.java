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
package com.sleepycat.je.dbi;

import java.util.Comparator;

import com.sleepycat.je.ByteComparator;
import com.sleepycat.je.DatabaseEntry;

/**
 * Interface for calling the appropriate user comparator.
 */
public interface InternalComparator {

    InternalComparator DEFAULT = create(ByteComparator.DEFAULT);

    int compare(byte[] key1,
                int key1Offset,
                int key1length,
                byte[] key2Prefix,
                int key2PrefixOffset,
                int key2PrefixLength,
                byte[] key2Suffix,
                int key2SuffixOffset,
                int key2SuffixLength);

    default int compare(byte[] key1, byte[] key2) {
        return compare(
            key1, 0, key1.length,
            null, 0, 0,
            key2, 0, key2.length);
    }

    default int compare(DatabaseEntry key1, DatabaseEntry key2) {
        return compare(
            key1.getData(), key1.getOffset(), key1.getSize(),
            null, 0, 0,
            key2.getData(), key2.getOffset(), key2.getSize());
    }

    default Comparator<byte[]> asJavaComparator() {
        return this::compare;
    }

    static InternalComparator create(final ByteComparator comparator) {

        return (key1, key1Offset, key1Length,
                key2Prefix, key2PrefixOffset, key2PrefixLength,
                key2Suffix, key2SuffixOffset, key2SuffixLength) -> {

            if (key2Prefix == null || key2PrefixLength == 0) {

                return comparator.compare(
                    key1, key1Offset, key1Length,
                    key2Suffix, key2SuffixOffset, key2SuffixLength);
            }

            final int cmp = comparator.compare(
                key1, key1Offset, Math.min(key1Length, key2PrefixLength),
                key2Prefix, key2PrefixOffset, key2PrefixLength);

            if (cmp != 0) {
                return cmp;
            }

            return comparator.compare(
                key1,
                key1Offset + key2PrefixLength,
                key1Length - key2PrefixLength,
                key2Suffix, key2SuffixOffset, key2SuffixLength);
        };
    }

    static InternalComparator create(final Comparator<byte[]> comparator) {

        return (key1, key1Offset, key1Length,
                key2Prefix, key2PrefixOffset, key2PrefixLength,
                key2Suffix, key2SuffixOffset, key2SuffixLength) -> {

            final byte[] fullKey1;
            final byte[] fullKey2;

            if (key1Offset != 0 || key1Length != key1.length) {
                fullKey1 = new byte[key1Length];
                System.arraycopy(key1, key1Offset, fullKey1, 0, key1Length);
            } else {
                fullKey1 = key1;
            }

            final boolean usePrefix =
                key2Prefix != null && key2PrefixLength != 0;

            if (usePrefix || key2SuffixOffset != 0 ||
                key2SuffixLength != key2Suffix.length) {

                if (usePrefix) {

                    fullKey2 = new byte[key2PrefixLength + key2SuffixLength];

                    System.arraycopy(
                        key2Prefix, key2PrefixOffset,
                        fullKey2, 0, key2PrefixLength);

                    System.arraycopy(
                        key2Suffix, key2SuffixOffset,
                        fullKey2, key2PrefixLength, key2SuffixLength);

                } else {

                    fullKey2 = new byte[key2SuffixLength];

                    System.arraycopy(
                        key2Suffix, key2SuffixOffset,
                        fullKey2, 0, key2SuffixLength);

                }

            } else {
                fullKey2 = key2Suffix;
            }

            return comparator.compare(fullKey1, fullKey2);
        };
    }
}
