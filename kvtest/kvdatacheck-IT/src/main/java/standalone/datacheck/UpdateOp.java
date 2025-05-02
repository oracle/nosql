/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import static java.util.Collections.singletonList;
import static standalone.datacheck.DataCheck.DEBUG;

import java.util.ArrayList;
import java.util.List;

import standalone.datacheck.DataCheck.OpResult;
import standalone.datacheck.DataCheck.ValVer;

/** Represents an update operation on the store. */
abstract class UpdateOp<K, V, O> extends Op<K, V, O> {
    final long index;
    final boolean firstThread;
    final boolean requestPrevValue;

    /**
     * Creates an instance of this class.
     *
     * @param dc the data check instance
     * @param index the operation index
     * @param firstThread whether the caller is the first exercise thread
     * @param requestPrevValue whether the update operation should request
     *        previous value
     */
    UpdateOp(DataCheck<K, V, O> dc, long index, boolean firstThread,
             boolean requestPrevValue) {
        super(dc);
        this.index = index;
        this.firstThread = firstThread;
        this.requestPrevValue = requestPrevValue;
    }

    @Override
    public String toString() {
        int partitionId = dc.getPartitionId(dc.keynumToKey(
                dc.exerciseIndexToKeynum(index, firstThread)));
        return String.format(
                "%s[index=%#x, thread%s, partition=p%d]",
                getName(), index,
                (firstThread ? "A" : "B"),
                partitionId);
    }

    /** Check that the previous value from a put operation is correct. */
    void checkPut(K key, long keynum, V value,
                  V prevValue, boolean retrying) {
        if (dc.verbose >= DEBUG) {
            final String prevValueString;
            if (requestPrevValue) {
                prevValueString = dc.valueString(prevValue);
            } else {
                prevValueString = (prevValue != null) ? "found" : "none";
            }
            dc.log("op=%s, keynum=%#x, partition=%d, key=%s, value=%s" +
                   ", previousValue=%s",
                   this, keynum, dc.getPartitionId(key), dc.keyString(key),
                   dc.valueString(value), prevValueString);
        }
        if (isCheckPrevValue()) {
            dc.checkPreviousValue(key, keynum, prevValue, index,
                                  dc.getOtherExerciseIndex(),
                                  firstThread, !requestPrevValue, retrying);
        }
    }

    /**
     * Check that the previous value from a delete operation is correct,
     * returning whether the check succeeded.
     */
    boolean checkDelete(K key, long keynum, V prevValue, boolean retrying) {
        if (dc.verbose >= DEBUG) {
            final String prevValueString;
            if (requestPrevValue) {
                prevValueString = dc.valueString(prevValue);
            } else {
                prevValueString = (prevValue != null) ? "found" : "none";
            }
            dc.log("op=%s, keynum=%#x, partition=%d, key=%s" +
                   ", previousValue=%s",
                   this, keynum, dc.getPartitionId(key), dc.keyString(key),
                   prevValueString);
        }
        if (!isCheckPrevValue()) {
            return true;
        }
        return dc.checkPreviousValue(key, keynum, prevValue, index,
                                     dc.getOtherExerciseIndex(),
                                     firstThread, !requestPrevValue, retrying);
    }

    V getPrevValue(ValVer<V> vv) {
        if (requestPrevValue)
            return vv.getValue();
        else if (vv.getSuccess()) {
            return dc.getEmptyValue();
        } else {
            return null;
        }
    }

    V getPrevValue(OpResult<V> operationResult) {
        if (requestPrevValue) {
            return operationResult.getPreviousValue();
        } else if (operationResult.getSuccess()) {
            return dc.getEmptyValue();
        } else {
            return null;
        }
    }

    boolean isCheckPrevValue() {
        return true;
    }

    static class PutOp<K, V, O> extends UpdateOp<K, V, O> {
        PutOp(DataCheck<K, V, O> dc,
              long index,
              boolean firstThread,
              boolean requestPrevValue) {
            super(dc, index, firstThread, requestPrevValue);
        }

        @Override
        void doOp(long timeoutMillis, boolean retrying) {
            long keynum = dc.exerciseIndexToKeynum(index, firstThread);
            K key = dc.keynumToKey(keynum);
            V value = dc.getExerciseValue(index, firstThread);
            ValVer<V> vv = doStoreOp(key, value, timeoutMillis);
            /*
             * If we're retrying, then the previous attempt may have already
             * stored the new value.  Otherwise, the retry should find the old
             * value as in a first attempt.  But that is not entirely true for
             * putIfAbsent or putIfPresent, where whether the original effort
             * should have stored the new value depends on what was there
             * before.  Punt on checking that case for now, to save complexity.
             */
            V prevValue = getPrevValue(vv);
            if (!retrying ||
               (requestPrevValue && !dc.valuesEqual(value, prevValue))) {
                checkPut(key, keynum, value, prevValue, retrying);
            }
        }

        ValVer<V> doStoreOp(K key, V value, long timeoutMillis) {
            ValVer<V> vv =
                dc.doPut(key, value, requestPrevValue, timeoutMillis);
            dc.tallyPut(key, value);
            return vv;
        }

        @Override
        boolean isCheckPrevValue(){
            return requestPrevValue;
        }
    }

    static class PutExecuteOp<K, V, O> extends UpdateOp<K, V, O> {

        PutExecuteOp(DataCheck<K, V, O> dc,
                     long index,
                     boolean firstThread,
                     boolean requestPrevValue) {
            super(dc, index, firstThread, requestPrevValue);
        }

        @Override
        void doOp(long timeoutMillis, boolean retrying) {
            long keynum = dc.exerciseIndexToKeynum(index, firstThread);
            K key = dc.keynumToKey(keynum);
            V value = dc.getExerciseValue(index, firstThread);
            O operation = getOperation(key, value);
            if (operation != null) {
                List<OpResult<V>> results =
                    dc.execute(singletonList(operation), timeoutMillis);
                V previousValue = getPrevValue(results.get(0));
                if (!retrying || (requestPrevValue &&
                                  !dc.valuesEqual(value, previousValue))) {
                    checkPut(key, keynum, value, previousValue, retrying);
                }
            }
        }

        O getOperation(K key, V value) {
            dc.tallyPut(key, value);
            return dc.createPutOp(key, value, requestPrevValue);
        }

        @Override
        boolean isCheckPrevValue() {
            return requestPrevValue;
        }
    }

    static class PutIfAbsentOp<K, V, O> extends PutOp<K, V, O> {
        PutIfAbsentOp(DataCheck<K, V, O> dc,
                      long index,
                      boolean firstThread,
                      boolean requestPrevValue) {
            super(dc, index, firstThread, requestPrevValue);
        }

        @Override
        ValVer<V> doStoreOp(K key, V value, long timeoutMillis) {
            ValVer<V> vv = dc.doPutIfAbsent(
                key, value, requestPrevValue, timeoutMillis);
            dc.tallyPut(key, value);
            return vv;
        }

        /*
         * PutIfAbsent operation executed failed if a previous value
         * associated with given key is present. So in this case, return
         * an empty value if the previous value has not been requested.
         */
        @Override
        V getPrevValue(ValVer<V> vv) {
            if (requestPrevValue)
                return vv.getValue();
            else if (!vv.getSuccess()) {
                return dc.getEmptyValue();
            } else {
                return null;
            }
        }

        @Override
        boolean isCheckPrevValue(){
            return true;
        }
    }

    static class PutIfAbsentExecuteOp<K, V, O> extends PutExecuteOp<K, V, O> {

        PutIfAbsentExecuteOp(DataCheck<K, V, O> dc,
                             long index,
                             boolean firstThread,
                             boolean requestPrevValue) {
            super(dc, index, firstThread, requestPrevValue);
        }

        @Override
        O getOperation(K key, V value) {
            dc.tallyPut(key, value);
            return dc.createPutIfAbsentOp(key, value, requestPrevValue);
        }

        /*
         * PutIfAbsent operation executed failed if a previous value
         * associated with given key is present. So in this case, return
         * an empty value if the previous value has not been requested.
         */
        @Override
        V getPrevValue(OpResult<V> operationResult) {
            if (requestPrevValue) {
                return operationResult.getPreviousValue();
            } else if (!operationResult.getSuccess()) {
                return dc.getEmptyValue();
            } else {
                return null;
            }
        }

        @Override
        boolean isCheckPrevValue() {
            return true;
        }
    }

    static class PutIfPresentOp<K, V, O> extends PutOp<K, V, O> {

        PutIfPresentOp(DataCheck<K, V, O> dc,
                       long index,
                       boolean firstThread,
                       boolean requestPrevValue) {
            super(dc, index, firstThread, requestPrevValue);
        }

        @Override
        ValVer<V> doStoreOp(K key, V value, long timeoutMillis) {
            ValVer<V> vv = dc.doPutIfPresent(
                key, value, requestPrevValue, timeoutMillis);
            dc.tallyPut(key, value);
            return vv;
        }

        @Override
        boolean isCheckPrevValue(){
            return true;
        }
    }

    static class PutIfPresentExecuteOp<K, V, O> extends PutExecuteOp<K, V, O> {

        PutIfPresentExecuteOp(DataCheck<K, V, O> dc,
                              long index,
                              boolean firstThread,
                              boolean requestPrevValue) {
            super(dc, index, firstThread, requestPrevValue);
        }

        @Override
        O getOperation(K key, V value) {
            dc.tallyPut(key, value);
            return dc.createPutIfPresentOp(key, value, requestPrevValue);
        }

        @Override
        boolean isCheckPrevValue() {
            return true;
        }
    }

    static class PutIfVersionOp<K, V, O> extends PutOp<K, V, O> {
        PutIfVersionOp(DataCheck<K, V, O> dc,
                       long index,
                       boolean firstThread,
                       boolean requestPrevValue) {
            super(dc, index, firstThread, requestPrevValue);
        }

        @Override
        ValVer<V> doStoreOp(K key, V value, long timeoutMillis) {
            ValVer<V> vv = dc.doGet(key, null, timeoutMillis);
            if (vv.getValue() != null) {
                dc.doPutIfVersion(key, value, vv.getVersion(),
                                  requestPrevValue, timeoutMillis);
                dc.tallyPut(key, value);
            }
            return vv;
        }

        /*
         * The previous valueVersion is obtained by GET operation
         * which was executed just before doPutIfVersion.
         */
        @Override
        V getPrevValue(ValVer<V> vv) {
            return vv.getValue();
        }

        @Override
        boolean isCheckPrevValue(){
            return true;
        }
    }

    static class PutIfVersionExecuteOp<K, V, O>
        extends PutIfVersionOp<K, V, O> {

        PutIfVersionExecuteOp(DataCheck<K, V, O> dc,
                              long index,
                              boolean firstThread,
                              boolean requestPrevValue) {
            super(dc, index, firstThread, requestPrevValue);
        }

        @Override
        ValVer<V> doStoreOp(K key, V value, long timeoutMillis) {
            ValVer<V> vv = dc.doGet(key, null, timeoutMillis);
            if (vv.getValue() != null) {
                O operation = dc.createPutIfVersionOp(
                    key, value, vv.getVersion(), requestPrevValue);
                dc.tallyPut(key, value);
                dc.execute(singletonList(operation), timeoutMillis);
            }
            return vv;
        }

        @Override
        boolean isCheckPrevValue() {
            return true;
        }
    }

    static class TwoPutExecuteOp<K, V, O> extends UpdateOp<K, V, O> {

        TwoPutExecuteOp(DataCheck<K, V, O> dc,
                        long index,
                        boolean firstThread,
                        boolean requestPrevValue) {
            super(dc, index, firstThread, requestPrevValue);
        }

        @Override
        void doOp(long timeoutMillis, boolean retrying) {
            long keynum = dc.exerciseIndexToKeynum(index, firstThread);
            List<O> operations = new ArrayList<O>(2);
            K key1 = dc.keynumToKey(keynum);
            V value = dc.getExerciseValue(index, firstThread);
            operations.add(dc.createPutOp(key1, value, requestPrevValue));
            dc.tallyPut(key1, value);
            K key2 = dc.appendStringToKey(key1, "x");
            operations.add(
                dc.createPutIfPresentOp(key2, value, requestPrevValue));
            dc.tallyPut(key2, value);
            List<OpResult<V>> results = dc.execute(operations, timeoutMillis);
            V prevValue = results.get(0).getPreviousValue();

            /*
             * If we're retrying, then the previous attempt may have already
             * stored the new value.  Otherwise, the retry should find the old
             * value as in a first attempt.
             */
            if (!retrying || !dc.valuesEqual(value, prevValue)) {
                checkPut(key1, keynum, value, prevValue, retrying);
            }
            prevValue = results.get(1).getPreviousValue();

            /*
             * Here a retry may find the new value but otherwise should find
             * nothing.
             */
            if ((!retrying || !dc.valuesEqual(value, prevValue)) &&
                (prevValue != null)) {
                dc.unexpectedResult("Value should not be present: " +
                                    "op=%s, partition=%d, key=%s" +
                                    ", previousValue=%s",
                                    this, dc.getPartitionId(key2),
                                    dc.keyString(key2),
                                    dc.valueString(prevValue));
            }
        }

        @Override
        boolean isCheckPrevValue() {
            return requestPrevValue;
        }
    }

    static class BulkPutOp<K, V, O> extends UpdateOp<K, V, O> {

        BulkPutOp(DataCheck<K, V, O> dc, long index, boolean firstThread) {
            super(dc, index, firstThread,
                  /*
                   * Bulk put does not support returning previous values, but
                   * always detects if they were present
                   */
                  false /* requestPrevValue */);
        }

        @Override
        void doOp(long timeoutMillis, boolean retrying) {
            final long keynum = dc.exerciseIndexToKeynum(index, firstThread);
            final K key = dc.keynumToKey(keynum);
            final V value = dc.getExerciseValue(index, firstThread);
            final boolean keyExists = dc.doBulkPut(key, value, timeoutMillis);
            dc.tallyPut(key, value);

            /*
             * If we're retrying, then the previous attempt may have already
             * stored the new value. Otherwise, the retry should find the old
             * value as in a first attempt.
             */
            final V prevValue = keyExists ? dc.getEmptyValue() : null;
            if (!retrying || !dc.valuesEqual(value, prevValue)) {
                checkPut(key, keynum, value, prevValue, retrying);
            }
        }
    }

    static class DeleteOp<K, V, O> extends UpdateOp<K, V, O> {
        DeleteOp(DataCheck<K, V, O> dc,
                 long index,
                 boolean firstThread,
                 boolean requestPrevValue) {
            super(dc, index, firstThread, requestPrevValue);
        }

        @Override
        void doOp(long timeoutMillis, boolean retrying) {
            long keynum = dc.exerciseIndexToKeynum(index, firstThread);
            K key = dc.keynumToKey(keynum);
            ValVer<V> vv = doStoreOp(key, timeoutMillis);
            /*
             * If we're retrying, then the previous attempt may have already
             * deleted the value for the key.  Otherwise, the retry should find
             * the old value as in a first attempt.
             */
            V prevValue = getPrevValue(vv);
            if (!retrying || prevValue != null) {
                checkDelete(key, keynum, prevValue, retrying);
            }
        }

        ValVer<V> doStoreOp(K key, long timeoutMillis) {
            return dc.doDelete(key, requestPrevValue, timeoutMillis);
        }

        @Override
        boolean isCheckPrevValue() {
            return true;
        }
    }

    static class MultiDeleteOp<K, V, O> extends UpdateOp<K, V, O> {

        /**
         * @param requestPrevValue
         */
        MultiDeleteOp(DataCheck<K, V, O> dc,
                      long index,
                      boolean firstThread,
                      boolean requestPrevValue) {
            super(dc, index, firstThread, false);
        }

        @Override
        void doOp(long timeoutMillis, boolean retrying) {
            long keynum = dc.exerciseIndexToKeynum(index, firstThread);
            K key = dc.keynumToKey(keynum);
            int count = dc.doMultiDelete(key, timeoutMillis);

            /*
             * Only check if the operation is not retrying.
             */
            if (!retrying) {
                checkDelete(key, keynum,
                            count == 0 ? null : dc.getEmptyValue(),
                            false /* retrying */);
            }
            if (dc.verbose >= DEBUG) {
                dc.log("op=%s, partition=%d, key=%s, keynum=%#x" +
                       ", result=%d",
                       this, dc.getPartitionId(key), key, keynum, count);
            }

            /*
             * Retries will decrease but not increase the number of objects
             * deleted, so this check is still valid on retry.
             */
            if (count > 1) {
                dc.unexpectedResult("Too many deletes:" +
                                    "op=%s, partition=%d, key=%s, keynum=%#x" +
                                    ", result=%d",
                                    this, dc.getPartitionId(key),
                                    dc.keyString(key), keynum, count);
            }
        }
    }

    static class DeleteExecuteOp<K, V, O> extends UpdateOp<K, V, O> {

        DeleteExecuteOp(DataCheck<K, V, O> dc,
                        long index,
                        boolean firstThread,
                        boolean requestPrevValue) {
            super(dc, index, firstThread, requestPrevValue);
        }

        @Override
        void doOp(long timeoutMillis, boolean retrying) {
            long keynum = dc.exerciseIndexToKeynum(index, firstThread);
            K key = dc.keynumToKey(keynum);
            O operation = getOperation(key);
            if (operation != null) {
                List<OpResult<V>> results =
                    dc.execute(singletonList(operation), timeoutMillis);
                V prevValue = getPrevValue(results.get(0));
                if (!retrying || prevValue != null) {
                    checkDelete(key, keynum, prevValue, retrying);
                }
            }
        }

        O getOperation(K key) {
            return dc.createDeleteOp(key, requestPrevValue);
        }

        @Override
        boolean isCheckPrevValue() {
            return true;
        }
    }

    static class DeleteIfVersionOp<K, V, O> extends DeleteOp<K, V, O> {

        DeleteIfVersionOp(DataCheck<K, V, O> dc,
                          long index,
                          boolean firstThread,
                          boolean requestPrevValue) {
            super(dc, index, firstThread, requestPrevValue);
        }

        @Override
        ValVer<V> doStoreOp(K key, long timeoutMillis) {
            ValVer<V> vv = dc.doGet(key, null, timeoutMillis);
            if (vv.getValue() != null) {
                dc.doDeleteIfVersion(key, vv.getVersion(), requestPrevValue,
                                     timeoutMillis);
            }
            return vv;
        }

        /*
         * The previous valueVersion is obtained by GET operation
         * which was executed just before doDeleteIfVersion.
         */
        @Override
        V getPrevValue(ValVer<V> vv) {
            return vv.getValue();
        }

        @Override
        boolean isCheckPrevValue() {
            return true;
        }
    }

    static class DeleteIfVersionExecuteOp<K, V, O>
        extends DeleteIfVersionOp<K, V, O> {

        DeleteIfVersionExecuteOp(DataCheck<K, V, O> dc,
                                 long index,
                                 boolean firstThread,
                                 boolean requestPrevValue) {
            super(dc, index, firstThread, requestPrevValue);
        }

        @Override
        ValVer<V> doStoreOp(K key, long timeoutMillis) {
            ValVer<V> vv = dc.doGet(key, null, timeoutMillis);
            if (vv.getValue() != null) {
                O operation = dc.createDeleteIfVersionOp(
                    key, vv.getVersion(), requestPrevValue);
                dc.execute(singletonList(operation), timeoutMillis);
            }
            return vv;
        }

        @Override
        boolean isCheckPrevValue() {
            return true;
        }
    }
}
