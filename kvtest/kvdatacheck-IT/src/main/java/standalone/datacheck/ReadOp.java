/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import static standalone.datacheck.DataCheck.DEBUG;

import standalone.datacheck.DataCheck.KeyVal;
import standalone.datacheck.DataCheck.ValVer;

import java.util.Collection;
import java.util.Iterator;

/** Represents a random read operation on the store. */
abstract class ReadOp<K, V, O> extends Op<K, V, O> {
    final long index;
    final boolean firstThread;
    final long keynum;

    /**
     * Creates an instance of this class.
     *
     * @param dc the data check instance
     * @param index the operation index
     * @param firstThread whether the caller is the first exercise thread
     * @param keynum the random keynum
     */
    ReadOp(DataCheck<K, V, O> dc, long index,
           boolean firstThread, long keynum) {
        super(dc);
        this.index = index;
        this.firstThread = firstThread;
        this.keynum = keynum;
    }

    /** Retrying doesn't matter for read operations. */
    @Override
    final void doOp(long timeoutMillis, boolean ignore /* retrying */) {
        doOp(timeoutMillis);
    }
    abstract void doOp(long timeoutMillis);

    @Override
    public String toString() {
        int partitionId = dc.getPartitionId(dc.keynumToKey(keynum));
        return String.format(
                "%s[index=%#x, keynum=%#x, partition=p%d]",
                getName(), index, keynum, partitionId);
    }

    /** Check that the value for the key is correct. */
    void checkValue(K key, V value) {
        if (dc.verbose >= DEBUG) {
            dc.log("op=%s, partition=%d, key=%s, value=%s",
                   this, dc.getPartitionId(key), dc.keyString(key),
                   dc.valueString(value));
        }
        long otherIndex = dc.getOtherExerciseIndex();
        dc.checkValue(this, key, value, firstThread ? index : otherIndex,
                      firstThread ? otherIndex : index);
    }

    /** Check that the fact that the key is present is correct. */
    void checkKeyPresent(K key) {
        if (dc.verbose >= DEBUG) {
            dc.log("op=%s, partition=%d, key=%s, keyPresent",
                   this, dc.getPartitionId(key), dc.keyString(key));
        }
        long otherIndex = dc.getOtherExerciseIndex();
        dc.checkKeyPresent(this, key, firstThread ? index : otherIndex,
                           firstThread ? otherIndex : index);
    }

    /**
     * Returns true if shard primary key is complete or false if it is partial
     */
    boolean isShardPrimaryKeyComplete() {
        return true;
    }

    /**
     * Perform get operation.
     */
    static class GetOp<K, V, O> extends ReadOp<K, V, O> {
        GetOp(DataCheck<K, V, O> dc,
              long index,
              boolean firstThread,
              long keynum) {
            super(dc, index, firstThread, keynum);
        }
        @Override
        void doOp(long timeoutMillis) {
            K key = dc.keynumToKey(keynum);
            ValVer<V> vv = dc.doGet(key, null, timeoutMillis);
            checkValue(key, vv.getValue());
        }
    }

    /**
     * Perform MultiGetOp operation.
     */
    static class MultiGetOp<K, V, O> extends ReadOp<K, V, O> {
        MultiGetOp(DataCheck<K, V, O> dc,
                   long index,
                   boolean firstThread,
                   long keynum) {
            super(dc, index, firstThread, keynum);
        }

        @Override
        void doOp(long timeoutMillis) {
            K key = dc.keynumToKey(keynum);
            Collection<V> results = dc.doMultiGet(key, null, timeoutMillis);
            int size = results.size();
            if (size > 1) {
                dc.unexpectedResult("Too many results: " +
                                    "op=%s, partition=%d, key=%s, count=%d",
                                    this, dc.getPartitionId(key),
                                    dc.keyString(key), size);
            } else if (size == 1) {
                checkValue(key, results.iterator().next());
            }
        }
    }

    /**
     * Perform MultiGetKeys operation.
     */
    static class MultiGetKeysOp<K, V, O> extends ReadOp<K, V, O> {
        MultiGetKeysOp(DataCheck<K, V, O> dc,
                       long index,
                       boolean firstThread,
                       long keynum) {
            super(dc, index, firstThread, keynum);
        }
        @Override
        void doOp(long timeoutMillis) {
            K key = dc.keynumToKey(keynum);
            Collection<K> keys = dc.doMultiGetKeys(key, null, timeoutMillis);
            int size = keys.size();
            if (size > 1) {
                dc.unexpectedResult("Too many results: " +
                                    "op=%s, partition=%d, key=%s, count=%d",
                                    this, dc.getPartitionId(key),
                                    dc.keyString(key), size);
            } else if (size == 1) {
                checkKeyPresent(keys.iterator().next());
            }
        }
    }

    /**
     * Perform MultiGetIterator operation.
     */
    static class MultiGetIteratorOp<K, V, O> extends ReadOp<K, V, O> {

        MultiGetIteratorOp(DataCheck<K, V, O> dc,
                           long index,
                           boolean firstThread,
                           long keynum) {
            super(dc, index, firstThread, keynum);
        }

        @Override
        void doOp(long timeoutMillis) {
            K key = dc.keynumToKey(keynum);
            Iterator<KeyVal<K, V>> iter =
                dc.doMultiGetIterator(key, null, timeoutMillis);
            if (iter.hasNext()) {
                KeyVal<K, V> kv = iter.next();
                checkValue(kv.getKey(), kv.getValue());
            }
            if (iter.hasNext()) {
                dc.unexpectedResult("More than one result: " +
                                    "op=%s, partition=%d, key=%s",
                                    this, dc.getPartitionId(key),
                                    dc.keyString(key));
            }
        }
    }


    /**
     * Perform MultiGetKeysIteratorOp operation.
     */
    static class MultiGetKeysIteratorOp<K, V, O>
        extends ReadOp<K, V, O> {

        MultiGetKeysIteratorOp(DataCheck<K, V, O> dc,
                               long index,
                               boolean firstThread,
                               long keynum) {
            super(dc, index, firstThread, keynum);
        }

        @Override
        void doOp(long timeoutMillis) {
            K key = dc.keynumToKey(keynum);
            Iterator<K> iter =
                dc.doMultiGetKeysIterator(key, null, timeoutMillis);
            if (iter.hasNext()) {
                final K eKey = iter.next();
                checkKeyPresent(eKey);
            }
            if (iter.hasNext()) {
                dc.unexpectedResult("More than one result: " +
                                    "op=%s, partition=%d, key=%s",
                                    this, dc.getPartitionId(key), key);
            }
        }
    }
}
