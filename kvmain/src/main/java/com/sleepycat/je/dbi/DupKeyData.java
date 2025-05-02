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

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.LN;
import com.sleepycat.util.PackedInteger;

/**
 * Utility methods for combining, splitting and comparing two-part key values
 * for duplicates databases.
 *
 * At the Btree storage level, for the key/data pairs in a duplicates database,
 * the data is always zero length and the key is a two-part key. For embedded
 * records, the key and data parts are visible at the BTree level as well. In
 * both cases, the 'key' parameter in the API is the first part of the key. 
 * The the 'data' parameter in the API is the second part of the key.
 *
 * The length of the first part is stored at the end of the combined key as a
 * packed integer, so that the two parts can be split, combined, and compared
 * separately.  The length is stored at the end, rather than the start, to
 * enable key prefixing for the first part, e.g., for Strings with different
 * lengths but common prefixes.
 */
public class DupKeyData {

    static final int PREFIX_ONLY = -1;

    /**
     * Returns twoPartKey as:
     *   paramKey bytes,
     *   paramData bytes,
     *   reverse-packed len of paramKey bytes.
     *
     * The byte array in the resulting twoPartKey will be copied again by JE at
     * a lower level.  It would be nice if there were a way to give ownership
     * of the array to JE, to avoid the extra copy.
     */
    public static DatabaseEntry combine(final DatabaseEntry paramKey,
                                        final DatabaseEntry paramData) {
        final byte[] buf = combine
            (paramKey.getData(), paramKey.getOffset(), paramKey.getSize(),
             paramData.getData(), paramData.getOffset(), paramData.getSize());
        return new DatabaseEntry(buf);
    }

    public static byte[] combine(final byte[] key, final byte[] data) {
        return combine(key, 0, key.length, data, 0, data.length);
    }

    public static byte[] combine(final byte[] key,
                                 final int keyOff,
                                 final int keySize,
                                 final byte[] data,
                                 final int dataOff,
                                 final int dataSize) {
        final int keySizeLen = PackedInteger.getWriteIntLength(keySize);
        final byte[] buf = new byte[keySizeLen + keySize + dataSize];
        final int nextOff =
            combine(key, keyOff, keySize, data, dataOff, dataSize, buf);
        assert nextOff == buf.length;
        return buf;
    }

    public static int combine(final byte[] key,
                              final int keyOff,
                              final int keySize,
                              final byte[] data,
                              final int dataOff,
                              final int dataSize,
                              final byte[] buf) {
        System.arraycopy(key, keyOff, buf, 0, keySize);
        System.arraycopy(data, dataOff, buf, keySize, dataSize);
        return
            PackedInteger.writeReverseInt(buf, keySize + dataSize, keySize);
    }

    /**
     * Splits twoPartKey, previously set by combine, into original paramKey and
     * paramData if they are non-null.
     *
     * The offset of the twoPartKey must be zero.  This can be assumed because
     * the entry is read from the database and JE always returns entries with a
     * zero offset.
     *
     * This method copies the bytes into to new arrays rather than using the
     * DatabaseEntry offset and size to shared the array, to keep with the
     * convention that JE always returns whole arrays.  It would be nice to
     * avoid the copy, but that might break user apps.
     */
    public static void split(final DatabaseEntry twoPartKey,
                             final DatabaseEntry paramKey,
                             final DatabaseEntry paramData) {
        assert twoPartKey.getOffset() == 0;
        split(twoPartKey.getData(), twoPartKey.getSize(), paramKey, paramData);
    }

    /**
     * Same as split method above, but with twoPartKey/twoPartKeySize byte
     * array and array size params.
     */
    public static void split(final byte[] twoPartKey,
                             final int twoPartKeySize,
                             final DatabaseEntry paramKey,
                             final DatabaseEntry paramData) {
        final int keySize =
            PackedInteger.readReverseInt(twoPartKey, twoPartKeySize - 1);
        assert keySize != PREFIX_ONLY;

        if (paramKey != null) {
            final byte[] keyBuf = new byte[keySize];
            System.arraycopy(twoPartKey, 0, keyBuf, 0, keySize);

            if (keySize == 0 || paramKey.getPartial()) {
                LN.setEntry(paramKey, keyBuf);
            } else {
                paramKey.setData(keyBuf, 0, keySize);
            }
        }

        if (paramData != null) {
            final int keySizeLen =
                PackedInteger.getReadIntLength(twoPartKey, twoPartKeySize - 1);

            final int dataSize = twoPartKeySize - keySize - keySizeLen;
            final byte[] dataBuf = new byte[dataSize];
            System.arraycopy(twoPartKey, keySize, dataBuf, 0, dataSize);

            if (dataSize == 0 || paramData.getPartial()) {
                LN.setEntry(paramData, dataBuf);
            } else {
                paramData.setData(dataBuf, 0, dataSize);
            }
        }
    }

    /**
     * Splits twoPartKey and returns a two-part key entry containing the key
     * portion of twoPartKey combined with newData.
     */
    public static byte[] replaceData(final byte[] twoPartKey,
                                     final byte[] newData) {
        final int origKeySize =
            PackedInteger.readReverseInt(twoPartKey, twoPartKey.length - 1);
        final int keySize = (origKeySize == PREFIX_ONLY) ?
            (twoPartKey.length - 1) :
            origKeySize;
        return combine(twoPartKey, 0, keySize, newData, 0, newData.length);
    }

    /**
     * Splits twoPartKey and returns a two-part key entry containing the key
     * portion from twoPartKey, no data, and the special PREFIX_ONLY value for
     * the key length.  When used for a search, this will compare as less than
     * any other entry having the same first part, i.e., in the same duplicate
     * set.
     */
    public static DatabaseEntry removeData(final byte[] twoPartKey) {
        final int keySize =
            PackedInteger.readReverseInt(twoPartKey, twoPartKey.length - 1);
        assert keySize != PREFIX_ONLY;
        return new DatabaseEntry(makePrefixKey(twoPartKey, 0, keySize));
    }

    /**
     * Returns a two-part key entry with the given key portion, no data, and
     * the special PREFIX_ONLY value for the key length.  When used for a
     * search, this will compare as less than any other entry having the same
     * first part, i.e., in the same duplicate set.
     */
    public static byte[] makePrefixKey(
        final byte[] key,
        final int keyOff,
        final int keySize) {

        final byte[] buf = new byte[keySize + 1];
        System.arraycopy(key, keyOff, buf, 0, keySize);
        buf[keySize] = (byte) PREFIX_ONLY;
        return buf;
    }

    public static int getKeyLength(final byte[] buf, int off, int len) {

        assert(buf.length >= off + len);

        int keyLen = PackedInteger.readReverseInt(buf, off + len - 1);
        assert(keyLen != PREFIX_ONLY);
        assert(keyLen >= 0 && keyLen <= len);

        return keyLen;
    }

    public static byte[] getKey(final byte[] buf, int off, int len) {

        assert(buf.length >= off + len);

        int keyLen = PackedInteger.readReverseInt(buf, off + len - 1);
        assert(keyLen != PREFIX_ONLY);
        assert(keyLen >= 0 && keyLen <= len);

        byte[] key = new byte[keyLen];
        System.arraycopy(buf, off, key, 0, keyLen);

        return key;
    }

    public static byte[] getData(final byte[] buf,
                                 final int off,
                                 final int len) {

        OffsetSize os = getDataOffsetSize(buf, off, len);
        byte[] data = new byte[os.size];
        System.arraycopy(buf, os.offset, data, 0, os.size);
        return data;
    }

    public static void getData(final DatabaseEntry outputEntry,
                               final byte[] buf,
                               final int off,
                               final int len) {

        OffsetSize os = getDataOffsetSize(buf, off, len);
        LN.outputBytes(outputEntry, buf, os.offset, os.size);
    }

    private static class OffsetSize {
        final int offset;
        final int size;

        OffsetSize(final int offset, final int size) {
            this.offset = offset;
            this.size = size;
        }
    }

    private static OffsetSize getDataOffsetSize(final byte[] buf,
                                                final int off,
                                                final int len) {
        assert(buf.length >= off + len);

        final int keyLen = PackedInteger.readReverseInt(buf, off + len - 1);
        assert(keyLen != PREFIX_ONLY);
        assert(keyLen >= 0 && keyLen <= len);

        final int keyLenSize =
            PackedInteger.getReadIntLength(buf, off + len - 1);

        final int dataLen = len - keyLen - keyLenSize;
        assert(dataLen >= 0);
        assert(keyLen + dataLen <= len);

        return new OffsetSize(off + keyLen, dataLen);
    }

    /**
     * Comparator that compares the combined key/data two-part key, calling the
     * user-defined btree and duplicate comparator as needed.
     *
     * This class doubles as a main-key comparator when null is passed for
     * the dupComparator param.
     */
    public static class TwoPartKeyComparator implements InternalComparator {

        private final InternalComparator btreeComparator;
        private final InternalComparator duplicateComparator;

        TwoPartKeyComparator(final InternalComparator btreeComparator,
                             final InternalComparator dupComparator) {
            this.btreeComparator = btreeComparator;
            this.duplicateComparator = dupComparator;
        }

        public int compare(final byte[] twoPartKey1,
                           final int twoPartKey1Off,
                           final int twoPartKey1Len,
                           final byte[] twoPartKey2Prefix,
                           final int twoPartKey2PrefixOff,
                           int twoPartKey2PrefixLen,
                           byte[] twoPartKey2Suffix,
                           int twoPartKey2SuffixOff,
                           int twoPartKey2SuffixLen) {

            /*
             * If the suffix is empty (a rare case), swap the prefix and suffix
             * so that the suffix always contains the key length.
             */
            if (twoPartKey2SuffixLen == 0) {
                assert twoPartKey2Prefix != null;
                twoPartKey2Suffix = twoPartKey2Prefix;
                twoPartKey2SuffixLen = twoPartKey2PrefixLen;
                twoPartKey2SuffixOff = twoPartKey2PrefixOff;
                twoPartKey2PrefixLen = 0;
            }

            /*
             * Get the number of bytes of the size of key2. Note that
             * getReadIntLength only needs access to one byte (the last byte)
             * of the size, and we know at this point that the suffix contains
             * this byte.
             */
            final int keySizeLen2 = PackedInteger.getReadIntLength(
                twoPartKey2Suffix,
                twoPartKey2SuffixOff + twoPartKey2SuffixLen - 1);

            /*
             * If the key length overlaps the prefix and suffix (a rare case)
             * then combine them into a single array used as the suffix, so
             * we can call readReverseInt.
             */
            if (keySizeLen2 > twoPartKey2SuffixLen) {

                assert twoPartKey2PrefixLen > 0;

                final byte[] tmp =
                    new byte[twoPartKey2PrefixLen + twoPartKey2SuffixLen];

                System.arraycopy(
                    twoPartKey2Prefix, twoPartKey2PrefixOff,
                    tmp, 0, twoPartKey2PrefixLen);

                System.arraycopy(
                    twoPartKey2Suffix, twoPartKey2SuffixOff,
                    tmp, twoPartKey2PrefixLen, twoPartKey2SuffixLen);

                twoPartKey2Suffix = tmp;
                twoPartKey2SuffixOff = 0;
                twoPartKey2SuffixLen = tmp.length;
                twoPartKey2PrefixLen = 0;
            }

            /*
             * Get size of the first part of each two-part key.
             */
            final int origKeySize1 = PackedInteger.readReverseInt(
                twoPartKey1, twoPartKey1Off + twoPartKey1Len - 1);

            final int keySize1 = (origKeySize1 == PREFIX_ONLY) ?
                (twoPartKey1Len - 1) : origKeySize1;

            final int origKeySize2 = PackedInteger.readReverseInt(
                twoPartKey2Suffix,
                twoPartKey2SuffixOff + twoPartKey2SuffixLen - 1);

            final int keySize2 = (origKeySize2 == PREFIX_ONLY) ?
                (twoPartKey2PrefixLen + twoPartKey2SuffixLen - 1) :
                origKeySize2;

            /*
             * Compare key parts using the leading portion of the
             * twoPartKey2 prefix and suffix.
             */
            final int keyPrefixLen = Math.min(twoPartKey2PrefixLen, keySize2);
            final int keySuffixLen = keySize2 - keyPrefixLen;

            final int keyCmp = btreeComparator.compare(
                twoPartKey1, twoPartKey1Off, keySize1,
                twoPartKey2Prefix, twoPartKey2PrefixOff, keyPrefixLen,
                twoPartKey2Suffix, twoPartKey2SuffixOff, keySuffixLen);

            if (keyCmp != 0) {
                return keyCmp;
            }

            /*
             * When used as a main key comparator, we do not return a special
             * value for PREFIX_ONLY.
             */
            if (duplicateComparator == null) {
                return 0;
            }

            /*
             * If this is a prefix-only search, we return LT (-1) if the
             * key sizes are unequal, and we're done.
             */
            if (origKeySize1 == PREFIX_ONLY || origKeySize2 == PREFIX_ONLY) {
                if (origKeySize1 == origKeySize2) {
                    return 0;
                }
                return (origKeySize1 == PREFIX_ONLY) ? -1 : 1;
            }

            /*
             * Compare data parts using the trailing portion of the
             * twoPartKey2 prefix and suffix.
             */
            final int keySizeLen1 = PackedInteger.getReadIntLength(
                twoPartKey1, twoPartKey1Off + twoPartKey1Len - 1);

            final int dataSize1 = twoPartKey1Len - (keySize1 + keySizeLen1);
            final int dataPrefixLen = twoPartKey2PrefixLen - keyPrefixLen;
            final int dataSuffixLen =
                twoPartKey2SuffixLen - (keySuffixLen + keySizeLen2);

            return duplicateComparator.compare(
                twoPartKey1, keySize1, dataSize1,
                twoPartKey2Prefix,
                twoPartKey2PrefixOff + keyPrefixLen,
                dataPrefixLen,
                twoPartKey2Suffix,
                twoPartKey2SuffixOff + keySuffixLen,
                dataSuffixLen);
        }
    }

    /**
     * Compares the first part of the two keys.
     */
    public static int compareMainKey(
        final byte[] keyBytes1,
        final byte[] keyBytes2,
        final Comparator<byte[]> btreeComparator) {

        final int origKeySize2 =
            PackedInteger.readReverseInt(keyBytes2, keyBytes2.length - 1);
        final int keySize2 = (origKeySize2 == PREFIX_ONLY) ?
            (keyBytes2.length - 1) :
            origKeySize2;
        return compareMainKey(keyBytes1, keyBytes2, 0, keySize2,
            btreeComparator);
    }

    /**
     * Compares the first part of the two keys.
     */
    public static int compareMainKey(
        final byte[] keyBytes1,
        final byte[] keyBytes2,
        final int keyOff2,
        final int keySize2,
        final Comparator<byte[]> btreeComparator) {

        final int origKeySize1 =
            PackedInteger.readReverseInt(keyBytes1, keyBytes1.length - 1);
        final int keySize1 = (origKeySize1 == PREFIX_ONLY) ?
            (keyBytes1.length - 1) :
            origKeySize1;
        final int keyCmp;
        if (btreeComparator == null) {
            keyCmp = Key.compareUnsignedBytes
                (keyBytes1, 0, keySize1,
                    keyBytes2, keyOff2, keySize2);
        } else {
            final byte[] key1 = new byte[keySize1];
            final byte[] key2 = new byte[keySize2];
            System.arraycopy(keyBytes1, 0, key1, 0, keySize1);
            System.arraycopy(keyBytes2, keyOff2, key2, 0, keySize2);
            keyCmp = btreeComparator.compare(key1, key2);
        }
        return keyCmp;
    }
}
