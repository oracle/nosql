/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import standalone.datacheck.DataCheck.KeyVal;
import standalone.datacheck.DataCheck.ValVer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** Represents a store operation. */
abstract class Op<K, V, O> {
    final DataCheck<K, V, O> dc;

    Op(DataCheck<K, V, O> dc) {
        this.dc = dc;
    }

    /** Returns whether this Op is a LOB operator. */
    boolean isLOBOp() {
        return false;
    }

    /** Returns the name of the operation. */
    String getName() {
        return getClass().getSimpleName();
    }

    /**
     * Performs the operation.
     *
     * @param timeoutMillis the timeout for the operation
     * @param retrying whether the operation is being retried
     */
    abstract void doOp(long timeoutMillis, boolean retrying);

    /** Performs a series of execute operations. */
    static class ExecuteOp<K, V, O> extends Op<K, V, O> {
        private final List<O> operations;

        /**
         * Creates an instance of this class which performs the specified
         * operations.
         *
         * @param dc the data check instance
         * @param operations the operations
         */
        ExecuteOp(DataCheck<K, V, O> dc, List<O> operations) {
            super(dc);
            this.operations = new ArrayList<O>(operations);
        }

        @Override
        void doOp(long timeoutMillis, boolean ignore /* retrying */) {
            dc.execute(operations, timeoutMillis);
        }

        @Override
        public String toString() {
            return "ExecuteOp" + operations;
        }
    }

    /**
     * Performs a multiGet on a range of keys and checks associated values, for
     * use during the check phase.
     */
    static class MultiGetIteratorCheckOp<K, V, O> extends Op<K, V, O> {

        /**
         * The check operation occurs after all exercise operations have
         * completed, so specify a large index to represent that all exercise
         * operations have been completed.  Choose a value that is larger than
         * all possible operation indices, but not too large that it causes
         * integer overflow.
         */
        static final long CHECK_INDEX = 0x1000000000000L;

        final long index;

        /**
         * Creates an instance of this class which checks all of the minor keys
         * associated with the major key for the specified index.
         *
         * @param dc the data check instance
         * @param index the first operation index
         */
        MultiGetIteratorCheckOp(DataCheck<K, V, O> dc, long index) {
            super(dc);
            this.index = index;
        }

        @Override
        void doOp(long timeoutMillis, boolean ignore /* retrying */) {
            long stop = System.currentTimeMillis() + timeoutMillis;
            long firstKeynum = DataCheck.indexToKeynum(index);
            K firstKey = dc.keynumToMajorKey(firstKeynum);
            Iterator<KeyVal<K, V>> iter =
                dc.doMultiGetIterator(firstKey, null, timeoutMillis);
            long max = firstKeynum + DataCheck.MINOR_KEY_MAX;
            KeyVal<K, V> kv = null;
            K lastFoundKey = null;
            K nextFoundKey = null;
            for (long keynum = firstKeynum; keynum < max; keynum++) {
                if (System.currentTimeMillis() > stop) {
                    dc.unexpectedResult("Request timeout for key: " +
                                        "op=%s, partition=%d, key=%s",
                                         this, dc.getPartitionId(firstKey),
                                         dc.keyString(firstKey));
                }
                if (kv == null && iter.hasNext()) {
                    kv = iter.next();
                    lastFoundKey = nextFoundKey;
                    nextFoundKey = kv.getKey();
                }

                K key = dc.keynumToKey(keynum);
                V value;
                if (kv != null && key.equals(kv.getKey())) {
                    value = getValue(keynum, kv);
                    kv = null;
                } else {
                    value = null;
                }
                if (!dc.checkValue(this, key, value, CHECK_INDEX,
                                   CHECK_INDEX)) {
                    /*
                     * The check using the iterator value failed. Try a direct
                     * probe to see if the problem is that the iterator
                     * returned the wrong value.
                     */
                    final ValVer<V> vv = dc.doGet(key, null, timeoutMillis);
                    final boolean directCheck =
                        dc.checkValue(this, key, vv.getValue(), CHECK_INDEX,
                                      CHECK_INDEX);
                    dc.log(
                        String.format(
                            "Single element check for missing value:" +
                            " keynum=%#x key=%s checkResult=%s" +
                            " lastFoundKey=%s nextFoundKey=%s" +
                            " iter=%s",
                            keynum,
                            dc.keyString(key),
                            directCheck ? "pass" : "fail",
                            lastFoundKey,
                            nextFoundKey,
                            iter));
                }
            }
            if (iter.hasNext()) {
                dc.unexpectedResult("Too many entries for key: " +
                                    "op=%s, partition=%d, key=%s",
                                    this, dc.getPartitionId(firstKey),
                                    dc.keyString(firstKey));
            }
        }

        V getValue(@SuppressWarnings("unused") long keynum, KeyVal<K, V> kv) {
            return kv.getValue();
        }

        @Override
        public String toString() {
            return String.format("MultiGetIteratorCheckOp[index=%#x]", index);
        }
    }
}
