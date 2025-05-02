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
package com.sleepycat.je.tree;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.dbi.DupKeyData;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.InternalComparator;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.evictor.Evictor;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.tree.BINDeltaBloomFilter.HashContext;
import com.sleepycat.je.utilint.SizeofMarker;

/**
 * The abstract class that defines the various formats used to represent
 * the keys associated with the IN node. The class is also used to store
 * embedded records, where the actual key and the data portion of a record are
 * stored together as a single byte sequence.
 * 
 * There are currently two supported representations or formats:
 * <ol>
 * <li>A default representation <code>Default</code> that's capable of holding
 * any set of keys. A separate byte array is stored for each key.</li>
 * <li>
 * A compact representation <code>MaxKeySize</code> that's more efficient for
 * holding small keys, stored using a single byte array for all keys. This
 * format has a maxKeySize, calculated when it is created.</li>
 * </ol>
 *
 * <p>If key prefixing is in use, both formats only store the suffix of each
 * key (the portion of the key following the prefix).</p>
 *
 * <p>The very first node is created with the default format. The
 * {@link #compact} method is called at certain key points (after a split for
 * example) to determine whether the format  should be mutated to the compact
 * form in order to save memory.</p>
 *
 * <p>Once it has been mutated to the compact format, it is only mutated back
 * to the default format if a key is added that is larger than the maxKeySize.
 * When an IN is split, the new sibling uses the format of the original
 * node to avoid immediately mutating it and wasting allocations.</p>
 *
 * <p>When an IN is logged it stores the maxKeySize of the compact format,
 * or -1 if the default format is used. This value is used by the {@link
 * #create} method when the IN is fetched from disk, so that the
 * compact format can be used initially; otherwise, the default format would
 * be immediately mutated, wasting allocations. This approach ensures that the
 * compact format is only used as determined by {@link Default#compact}.</p>
 *
 * <p>Note that no attempt is currently made to optimize the storage
 * representation as keys are added or removed, to minimize the chances of
 * transitionary "back and forth" representation changes that could prove to
 * be expensive.</p>
 */
public abstract class INKeyRep
    extends INArrayRep<INKeyRep, INKeyRep.Type, byte[]> {

    /* The different representations for keys. */
    public enum Type { DEFAULT, MAX_KEY_SIZE }

    private static final byte[][] EMPTY_KEYS_ARRAY = new byte[0][];

    public INKeyRep() {
    }

    public abstract int length();

    /**
     * Returns true if the key bytes mem usage is accounted for internally
     * here, or false if each key has a separate byte array and its mem usage
     * is accounted for by the parent.
     */
    public abstract boolean accountsForKeyByteMemUsage();

    public abstract int size(int idx);

    public abstract INKeyRep set(int idx, byte[] key, byte[] data, IN parent);

    public abstract INKeyRep setData(int idx, byte[] data, IN parent);

    public abstract byte[] getData(int idx);

    public abstract void getData(int idx, DatabaseEntry entry);

    public abstract byte[] getKey(int idx, boolean embeddedData);

    public abstract byte[] getFullKey(
        byte[] prefix,
        int idx,
        boolean embeddedData);

    public abstract int compareKeys(
        byte[] searchKey,
        byte[] prefix,
        int idx,
        boolean embeddedData,
        InternalComparator comparator);

    public abstract void write(int idx, ByteBuffer buffer);

    public abstract INKeyRep read(int idx, ByteBuffer buffer, IN parent);

    public abstract void addToBloomFilter(int idx,
                                          boolean embeddedData,
                                          byte[] bf,
                                          HashContext hc);

    public abstract int getCompactMaxKeySize();

    public static INKeyRep create(int nodeMaxEntries,
                                  int maxKeySize,
                                  EnvironmentImpl envImpl) {

        final int compactMaxKeyLength = envImpl.getCompactMaxKeyLength();

        return (nodeMaxEntries <= MaxKeySize.MAX_KEYS &&
                maxKeySize > 0 &&
                compactMaxKeyLength > 0 &&
                maxKeySize <= compactMaxKeyLength) ?
            new MaxKeySize(nodeMaxEntries, (short) maxKeySize) :
            new Default(nodeMaxEntries);
    }

    /**
     * The default representation that's capable of storing keys of any size.
     */
    public static class Default extends INKeyRep {

        private final byte[][] keys;

        Default(int nodeMaxEntries) {
            this.keys = new byte[nodeMaxEntries][];
        }

        public Default(@SuppressWarnings("unused") SizeofMarker marker) {
            keys = EMPTY_KEYS_ARRAY;
        }

        private Default(byte[][] keys) {
            this.keys = keys;
        }

        @Override
        public INKeyRep resize(int capacity) {
            return new Default(Arrays.copyOfRange(keys, 0, capacity));
        }

        @Override
        public Type getType() {
            return Type.DEFAULT;
        }

        @Override
        public int length() {
            return keys.length;
        }

        @Override
        public INKeyRep set(int idx, byte[] key, IN parent) {
            keys[idx] = key;
            return this;
        }

        @Override
        public INKeyRep set(int idx, byte[] key, byte[] data, IN parent) {
            assert key != null;

            if (data == null || data.length == 0) {
                keys[idx] = key;
            } else {
                keys[idx] = DupKeyData.combine(key, data);
            }
            return this;
        }

        @Override
        public INKeyRep setData(int idx, byte[] data, IN parent) {

            /*
             * TODO #21488: optimize this to avoid creation of new combined
             * key, when possible.
             */
            return set(idx, getKey(idx, true), data, parent);
        }

        @Override
        public int size(int idx) {
            return keys[idx].length;
        }

        @Override
        public void write(int idx, ByteBuffer buffer) {
            LogUtils.writeByteArray(buffer, keys[idx]);
        }

        @Override
        public INKeyRep read(int idx, ByteBuffer buffer, IN parent) {
            keys[idx] = LogUtils.readByteArray(buffer);
            return this;
        }

        @Override
        public byte[] get(int idx) {
            return keys[idx];
        }

        @Override
        public byte[] getData(int idx) {

            assert(keys[idx] != null);
            return DupKeyData.getData(keys[idx], 0, keys[idx].length);
        }

        @Override
        public void getData(int idx, DatabaseEntry entry) {

            assert(keys[idx] != null);
            DupKeyData.getData(entry, keys[idx], 0, keys[idx].length);
        }

        @Override
        public byte[] getKey(int idx, boolean embeddedData) {

            byte[] suffix = keys[idx];

            if (suffix == null) {
                return Key.EMPTY_KEY;
            } else if (embeddedData) {
                return DupKeyData.getKey(suffix, 0, suffix.length);
            } else {
                return suffix;
            }
        }

        @Override
        public byte[] getFullKey(
            byte[] prefix,
            int idx, 
            boolean embeddedData) {

            if (prefix == null || prefix.length == 0) {
                return getKey(idx, embeddedData);
            }

            byte[] suffix = keys[idx];

            if (suffix == null) {
                assert(!embeddedData);
                suffix = Key.EMPTY_KEY;
            }

            int prefixLen = prefix.length;
            int suffixLen;

            if (embeddedData) {
                suffixLen = DupKeyData.getKeyLength(suffix, 0, suffix.length);
            } else {
                suffixLen = suffix.length;
            }

            final byte[] key = new byte[prefixLen + suffixLen];
            System.arraycopy(prefix, 0, key, 0, prefixLen);
            System.arraycopy(suffix, 0, key, prefixLen, suffixLen);

            return key;
        }

        @Override
        public int compareKeys(
            final byte[] searchKey,
            final byte[] prefix,
            final int idx,
            final boolean embeddedData,
            final InternalComparator comparator) {

            byte[] suffix = keys[idx];
            final int suffixLen;

            if (suffix == null) {
                suffix = Key.EMPTY_KEY;
                suffixLen = 0;
            } else if (embeddedData) {
                suffixLen = DupKeyData.getKeyLength(suffix, 0, suffix.length);
            } else {
                suffixLen = suffix.length;
            }

            return comparator.compare(
                searchKey, 0, searchKey.length,
                prefix, 0, (prefix != null) ? prefix.length : 0,
                suffix, 0, suffixLen);
        }

        @Override
        public void addToBloomFilter(int idx,
                                     boolean embeddedData,
                                     byte[] bf,
                                     HashContext hc) {

            byte[] suffix = keys[idx];
            final int suffixLen;

            if (suffix == null) {
                suffix = Key.EMPTY_KEY;
                suffixLen = 0;
            } else if (embeddedData) {
                suffixLen = DupKeyData.getKeyLength(suffix, 0, suffix.length);
            } else {
                suffixLen = suffix.length;
            }

            BINDeltaBloomFilter.add(bf, suffix, 0, suffixLen, hc);
        }

        @Override
        public INKeyRep copy(int from, int to, int n, IN parent) {
            System.arraycopy(keys, from, keys, to, n);
            return this;
        }

        /**
         * Evolves to the MaxKeySize representation if that is more efficient
         * for the current set of keys. Note that since all the keys must be
         * examined to make the decision, there is a reasonable cost to the
         * method and it should not be invoked indiscriminately.
         */
        @Override
        public INKeyRep compact(IN parent) {

            if (keys.length > MaxKeySize.MAX_KEYS) {
                return this;
            }

            final int compactMaxKeyLength = parent.getCompactMaxKeyLength();
            if (compactMaxKeyLength <= 0) {
                return this;
            }

            int keyCount = 0;
            int maxKeyLength = 0;
            int defaultKeyBytes = 0;

            for (byte[] key : keys) {
                if (key != null) {
                    keyCount++;
                    if (key.length > maxKeyLength) {
                        maxKeyLength = key.length;
                        if (maxKeyLength > compactMaxKeyLength) {
                            return this;
                        }
                    }
                    defaultKeyBytes += MemoryBudget.byteArraySize(key.length);
                }
            }

            if (keyCount == 0) {
                return this;
            }

            long defaultSizeWithKeys = calculateMemorySize() + defaultKeyBytes;

            if (defaultSizeWithKeys >
                MaxKeySize.calculateMemorySize(keys.length, maxKeyLength)) {
                return compactToMaxKeySizeRep(maxKeyLength, parent);
            }

            return this;
        }

        private MaxKeySize compactToMaxKeySizeRep(
            int maxKeyLength,
            IN parent) {
            
            MaxKeySize newRep =
                new MaxKeySize(keys.length, (short) maxKeyLength);

            for (int i = 0; i < keys.length; i++) {
                INKeyRep rep = newRep.set(i, keys[i], parent);
                assert rep == newRep; /* Rep remains unchanged. */
            }

            noteRepChange(newRep, parent);

            return newRep;
        }

        @Override
        public long calculateMemorySize() {

            /*
             * Assume empty keys array. The memory consumed by the actual keys
             * is accounted for by the IN.getEntryInMemorySize() method.
             */
            return MemoryBudget.DEFAULT_KEYVALS_OVERHEAD +
                MemoryBudget.objectArraySize(keys.length);
        }

        @Override
        public boolean accountsForKeyByteMemUsage() {
            return false;
        }

        @Override
        void updateCacheStats(@SuppressWarnings("unused") boolean increment,
                              @SuppressWarnings("unused") Evictor evictor) {
            /* No stats for the default representation. */
        }

        @Override
        public int getCompactMaxKeySize() {
            return -1;
        }
    }

    /**
     * The compact representation that can be used to represent keys LTE
     * {@link EnvironmentConfig#TREE_COMPACT_MAX_KEY_LENGTH} bytes in length.
     * The keys are all represented inside a single byte array
     * instead of having one byte array per key. Within the array, all keys are
     * assigned a storage size equal to that taken up by the longest key, plus
     * one byte to hold the actual key length. This makes key retrieval fast.
     * However, insertion and deletion for larger keys moves bytes proportional
     * to the storage length of the keys. This is why the representation is
     * restricted to keys LTE TREE_COMPACT_MAX_KEY_LENGTH bytes in size.
     * <p>
     * On a 32 bit VM the per key overhead for the Default representation is 4
     * bytes for the pointer + 16 bytes for each byte array key object, for a
     * total of 20 bytes/key. On a 64 bit machine the overheads are much
     * larger: 8 bytes for the pointer plus 24 bytes per key.
     * <p>
     * The more fully populated the IN the more the savings with this
     * representation since the single byte array is sized to hold all the keys
     * regardless of the actual number of keys that are present.
     * <p>
     * It's worth noting that the storage savings here are realized in addition
     * to the storage benefits of key prefixing, since the keys stored in the
     * key array are the smaller key values after the prefix has been stripped,
     * reducing the length of the key and making it more likely that it's small
     * enough for this specialized representation.
     */
    public static class MaxKeySize extends INKeyRep {

        private static final int LENGTH_BYTES = 1;
        static final int MAX_KEYS = 256;
        private static final byte NULL_KEY = Byte.MAX_VALUE;

        /*
         * The array is sized to hold all the keys associated with the IN node.
         * Each key is allocated a fixed amount of storage equal to the maximum
         * length of all the keys in the IN node + 1 byte to hold the size of
         * each key. The length is biased, by -128. That is, a zero length
         * key is represented by -128, a 1 byte key by -127, etc.
         */
        private final byte[] keys;

        /* 
         * The number of bytes used to store each key ==
         * DEFAULT_MAX_KEY_LENGTH (16) + LENGTH_BYTES (1) 
         */
        private final short fixedKeyLen;

        public MaxKeySize(int nodeMaxEntries, short maxKeyLen) {

            assert maxKeyLen < 255;
            this.fixedKeyLen = (short) (maxKeyLen + LENGTH_BYTES);
            this.keys = new byte[fixedKeyLen * nodeMaxEntries];
            initNulls(0, nodeMaxEntries);
        }

        /**
         * Null keys are not represented as zero byte values, so they must be
         * explicitly initialized.
         */
        private void initNulls(int from, int to) {
            for (int i = from; i < to; ++i) {
                INKeyRep rep = set(i, null, null);
                assert rep == this; /* Rep remains unchanged. */
            }
        }

        /* Only for use by Sizeof */
        public MaxKeySize(@SuppressWarnings("unused") SizeofMarker marker) {
            keys = null;
            fixedKeyLen = 0;
        }

        private MaxKeySize(byte[] keys, short fixedKeyLen) {
            this.keys = keys;
            this.fixedKeyLen = fixedKeyLen;
        }

        @Override
        public INKeyRep resize(int capacity) {
            int oldLen = length();

            MaxKeySize newRep = new MaxKeySize(
                Arrays.copyOfRange(keys, 0, capacity * fixedKeyLen),
                fixedKeyLen);

            newRep.initNulls(oldLen, capacity);
            return newRep;
        }

        @Override
        public Type getType() {
            return Type.MAX_KEY_SIZE;
        }

        @Override
        public int length() {
            return keys.length / fixedKeyLen;
        }

        @Override
        public INKeyRep set(int idx, byte[] key, IN parent) {

            int slotOff = idx * fixedKeyLen;

            if (key == null) {
                keys[slotOff] = NULL_KEY;
                return this;
            }

            if (key.length >= fixedKeyLen) {
                Default newRep = expandToDefaultRep(parent);
                return newRep.set(idx, key, parent);
            }

            keys[slotOff] = (byte) (key.length + Byte.MIN_VALUE);

            slotOff += LENGTH_BYTES;

            System.arraycopy(key, 0, keys, slotOff, key.length);

            return this;
        }

        @Override
        public INKeyRep set(int idx, byte[] key, byte[] data, IN parent) {
            assert key != null;

            if (data == null || data.length == 0) {
                return set(idx, key, parent);
            }

            byte[] twoPartKey = DupKeyData.combine(key, data);

            return set(idx, twoPartKey, parent);
        }

        @Override
        public INKeyRep setData(int idx, byte[] data, IN parent) {

            /*
             * TODO #21488: optimize this to avoid creation of new combined
             * key, when possible.
             */
            return set(idx, getKey(idx, true), data, parent);
        }

        private Default expandToDefaultRep(IN parent) {

            final int capacity = length();
            final Default newRep = new Default(capacity);

            for (int i = 0; i < capacity; i++) {
                final byte[] k = get(i);
                INKeyRep rep = newRep.set(i, k, parent);
                assert rep == newRep; /* Rep remains unchanged. */
            }

            noteRepChange(newRep, parent);
            return newRep;
        }

        @Override
        public int size(int idx) {

            int slotOff = idx * fixedKeyLen;

            assert keys[slotOff] != NULL_KEY;

            return keys[slotOff] - Byte.MIN_VALUE;
        }

        @Override
        public void write(int idx, ByteBuffer buffer) {

            int slotOff = idx * fixedKeyLen;

            assert keys[slotOff] != NULL_KEY;

            int slotLen = keys[slotOff] - Byte.MIN_VALUE;

            slotOff += LENGTH_BYTES;

            LogUtils.writeByteArray(buffer, keys, slotOff, slotLen);
        }

        @Override
        public INKeyRep read(int idx, ByteBuffer buffer, IN parent) {

            int origPos = buffer.position();
            int len = LogUtils.readPackedInt(buffer);

            if (len >= fixedKeyLen) {
                Default newRep = expandToDefaultRep(parent);
                buffer.position(origPos);
                return newRep.read(idx, buffer, parent);
            }

            int slotOff = idx * fixedKeyLen;

            keys[slotOff] = (byte) (len + Byte.MIN_VALUE);

            slotOff += LENGTH_BYTES;

            LogUtils.readByteArrayNoLength(buffer, keys, slotOff, len);

            return this;
        }

        @Override
        public byte[] get(int idx) {

            int slotOff = idx * fixedKeyLen;

            if (keys[slotOff] == NULL_KEY) {
                return null;
            }

            int slotLen = keys[slotOff] - Byte.MIN_VALUE;

            slotOff += LENGTH_BYTES;

            byte[] info = new byte[slotLen];
            System.arraycopy(keys, slotOff, info, 0, slotLen);
            return info;
        }

        @Override
        public byte[] getData(int idx) {

            int slotOff = idx * fixedKeyLen;

            assert(keys[slotOff] != NULL_KEY);

            int slotLen = keys[slotOff] - Byte.MIN_VALUE;

            slotOff += LENGTH_BYTES;

            return DupKeyData.getData(keys, slotOff, slotLen);
        }

        @Override
        public void getData(int idx, DatabaseEntry entry) {

            int slotOff = idx * fixedKeyLen;

            assert(keys[slotOff] != NULL_KEY);

            int slotLen = keys[slotOff] - Byte.MIN_VALUE;

            slotOff += LENGTH_BYTES;

            DupKeyData.getData(entry, keys, slotOff, slotLen);
        }

        @Override
        public byte[] getKey(int idx, boolean embeddedData) {

            int slotOff = idx * fixedKeyLen;

            if (keys[slotOff] == NULL_KEY) {
                assert(!embeddedData);
                return Key.EMPTY_KEY;
            }

            int slotLen = keys[slotOff] - Byte.MIN_VALUE;

            slotOff += LENGTH_BYTES;

            if (embeddedData) {
                return DupKeyData.getKey(keys, slotOff, slotLen);
            }

            byte[] key = new byte[slotLen];
            System.arraycopy(keys, slotOff, key, 0, slotLen);
            return key;
        }

        @Override
        public byte[] getFullKey(
            byte[] prefix,
            int idx,
            boolean embeddedData) {

            if (prefix == null || prefix.length == 0) {
                return getKey(idx, embeddedData);
            }

            int slotOff = idx * fixedKeyLen;

            if (keys[slotOff] == NULL_KEY) {
                assert(!embeddedData);
                return prefix;
            }

            int slotLen = keys[slotOff] - Byte.MIN_VALUE;

            slotOff += LENGTH_BYTES;

            int prefixLen = prefix.length;
            int suffixLen;

            if (embeddedData) {
                suffixLen = DupKeyData.getKeyLength(keys, slotOff, slotLen);
            } else {
                suffixLen = slotLen;
            }

            byte[] key = new byte[suffixLen + prefixLen];
            System.arraycopy(prefix, 0, key, 0, prefixLen);
            System.arraycopy(keys, slotOff, key, prefixLen, suffixLen);
            return key;
        }

        @Override
        public int compareKeys(
            byte[] searchKey,
            byte[] prefix,
            int idx,
            boolean embeddedData,
            InternalComparator comparator) {

            int myKeyOff = idx * fixedKeyLen;
            int myKeyLen = 0;

            if (keys[myKeyOff] != NULL_KEY) {

                myKeyLen = keys[myKeyOff] - Byte.MIN_VALUE;

                myKeyOff += LENGTH_BYTES;

                if (embeddedData) {
                    myKeyLen = DupKeyData.getKeyLength(
                        keys, myKeyOff, myKeyLen);
                }
            } else {
                assert(!embeddedData);
            }

            return comparator.compare(
                searchKey, 0, searchKey.length,
                prefix, 0, (prefix != null) ? prefix.length : 0,
                keys, myKeyOff, myKeyLen);
        }

        @Override
        public void addToBloomFilter(int idx,
                                     boolean embeddedData,
                                     byte[] bf,
                                     HashContext hc) {

            int myKeyOff = idx * fixedKeyLen;
            int myKeyLen = 0;

            if (keys[myKeyOff] != NULL_KEY) {

                myKeyLen = keys[myKeyOff] - Byte.MIN_VALUE;

                myKeyOff += LENGTH_BYTES;

                if (embeddedData) {
                    myKeyLen = DupKeyData.getKeyLength(
                        keys, myKeyOff, myKeyLen);
                }
            }

            BINDeltaBloomFilter.add(bf, keys, myKeyOff, myKeyLen, hc);
        }

        @Override
        public INKeyRep copy(int from, int to, int n, IN parent) {
            System.arraycopy(keys, (from * fixedKeyLen),
                             keys, (to * fixedKeyLen),
                             n * fixedKeyLen);
            return this;
        }

        @Override
        public INKeyRep compact(@SuppressWarnings("unused") IN parent) {
            /* It's as compact as it gets. */
            return this;
        }

        @Override
        public long calculateMemorySize() {
            return MemoryBudget.MAX_KEY_SIZE_KEYVALS_OVERHEAD +
                   MemoryBudget.byteArraySize(keys.length);
        }

        private static long calculateMemorySize(int maxKeys, int maxKeySize) {
            return MemoryBudget.MAX_KEY_SIZE_KEYVALS_OVERHEAD +
                   MemoryBudget.byteArraySize(maxKeys *
                                              (maxKeySize + LENGTH_BYTES));
        }

        @Override
        public boolean accountsForKeyByteMemUsage() {
            return true;
        }

        @Override
        void updateCacheStats(boolean increment, Evictor evictor) {
            if (increment) {
                evictor.getNINCompactKey().incrementAndGet();
            } else {
                evictor.getNINCompactKey().decrementAndGet();
            }
        }

        @Override
        public int getCompactMaxKeySize() {
            return fixedKeyLen - LENGTH_BYTES;
        }
    }
}
