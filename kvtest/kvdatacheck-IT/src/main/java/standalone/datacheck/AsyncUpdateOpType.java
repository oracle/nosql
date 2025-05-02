/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import oracle.kv.Version;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.TableOperation;
import oracle.kv.table.TableOperationResult;

import standalone.datacheck.AsyncOp.AsyncDeleteExecuteOp;
import standalone.datacheck.AsyncOp.AsyncDeleteIfVersionExecuteOp;
import standalone.datacheck.AsyncOp.AsyncDeleteIfVersionOp;
import standalone.datacheck.AsyncOp.AsyncDeleteOp;
import standalone.datacheck.AsyncOp.AsyncMultiDeleteOp;
import standalone.datacheck.AsyncOp.AsyncPutExecuteOp;
import standalone.datacheck.AsyncOp.AsyncPutIfAbsentExecuteOp;
import standalone.datacheck.AsyncOp.AsyncPutIfAbsentOp;
import standalone.datacheck.AsyncOp.AsyncPutIfPresentExecuteOp;
import standalone.datacheck.AsyncOp.AsyncPutIfPresentOp;
import standalone.datacheck.AsyncOp.AsyncPutIfVersionExecuteOp;
import standalone.datacheck.AsyncOp.AsyncPutIfVersionOp;
import standalone.datacheck.AsyncOp.AsyncPutOp;
import standalone.datacheck.AsyncOp.AsyncTwoPutExecuteOp;
import standalone.datacheck.AsyncOp.AsyncUpdateOp;
import standalone.datacheck.DataCheck.OpResult;
import standalone.datacheck.DataCheck.ValVer;

interface AsyncUpdateOpType<T, R>
        extends UpdateOpType<PrimaryKey, Row, TableOperation> {
    AsyncUpdateOp<T, R> getUpdateOp(DataCheckTableDirect dc,
                                    long index,
                                    boolean firstThread,
                                    boolean requestPrevValue,
                                    AtomicLong otherExerciseIndex);

    /*
     * All the types inherits the corresponding versions in UpdateOpType, and
     * also implement the interface of AsyncUpdateOpType.
     */
    static class AsyncPutOpType
            extends UpdateOpType.Put<PrimaryKey, Row, TableOperation>
            implements AsyncUpdateOpType<Version, ValVer<Row>> {

        @Override
        public AsyncUpdateOp<Version, ValVer<Row>>
            getUpdateOp(DataCheckTableDirect dc,
                        long index,
                        boolean firstThread,
                        boolean requestPrevValue,
                        AtomicLong otherExerciseIndex) {
            return new AsyncPutOp(dc, index, firstThread, requestPrevValue,
                                  otherExerciseIndex);
        }
    }

    static class AsyncPutExecuteOpType extends
            UpdateOpType.PutExecute<PrimaryKey, Row, TableOperation> implements
            AsyncUpdateOpType<List<TableOperationResult>, List<OpResult<Row>>> {

        @Override
        public AsyncUpdateOp<List<TableOperationResult>, List<OpResult<Row>>>
            getUpdateOp(DataCheckTableDirect dc,
                        long index,
                        boolean firstThread,
                        boolean requestPrevValue,
                        AtomicLong otherExerciseIndex) {
            return new AsyncPutExecuteOp(dc, index, firstThread,
                                         requestPrevValue, otherExerciseIndex);
        }
    }

    static class AsyncPutIfAbsentOpType
            extends UpdateOpType.PutIfAbsent<PrimaryKey, Row, TableOperation>
            implements AsyncUpdateOpType<Version, ValVer<Row>> {

        @Override
        public AsyncUpdateOp<Version, ValVer<Row>>
            getUpdateOp(DataCheckTableDirect dc,
                        long index,
                        boolean firstThread,
                        boolean requestPrevValue,
                        AtomicLong otherExerciseIndex) {
            return new AsyncPutIfAbsentOp(dc, index, firstThread,
                                          requestPrevValue,
                                          otherExerciseIndex);
        }
    }

    static class AsyncPutIfAbsentExecuteOpType extends
            UpdateOpType.PutIfAbsentExecute<PrimaryKey, Row, TableOperation>
            implements
            AsyncUpdateOpType<List<TableOperationResult>, List<OpResult<Row>>> {

        @Override
        public AsyncUpdateOp<List<TableOperationResult>, List<OpResult<Row>>>
            getUpdateOp(DataCheckTableDirect dc,
                        long index,
                        boolean firstThread,
                        boolean requestPrevValue,
                        AtomicLong otherExerciseIndex) {
            return new AsyncPutIfAbsentExecuteOp(dc, index, firstThread,
                                                 requestPrevValue,
                                                 otherExerciseIndex);
        }
    }

    static class AsyncPutIfPresentOpType
            extends UpdateOpType.PutIfPresent<PrimaryKey, Row, TableOperation>
            implements AsyncUpdateOpType<Version, ValVer<Row>> {

        @Override
        public AsyncUpdateOp<Version, ValVer<Row>>
            getUpdateOp(DataCheckTableDirect dc,
                        long index,
                        boolean firstThread,
                        boolean requestPrevValue,
                        AtomicLong otherExerciseIndex) {
            return new AsyncPutIfPresentOp(dc, index, firstThread,
                                           requestPrevValue,
                                           otherExerciseIndex);
        }
    }

    static class AsyncPutIfPresentExecuteOpType extends
            UpdateOpType.PutIfPresentExecute<PrimaryKey, Row, TableOperation>
            implements
            AsyncUpdateOpType<List<TableOperationResult>, List<OpResult<Row>>> {

        @Override
        public AsyncUpdateOp<List<TableOperationResult>, List<OpResult<Row>>>
            getUpdateOp(DataCheckTableDirect dc,
                        long index,
                        boolean firstThread,
                        boolean requestPrevValue,
                        AtomicLong otherExerciseIndex) {
            return new AsyncPutIfPresentExecuteOp(dc, index, firstThread,
                                                  requestPrevValue,
                                                  otherExerciseIndex);
        }
    }

    static class AsyncPutIfVersionOpType
            extends UpdateOpType.PutIfVersion<PrimaryKey, Row, TableOperation>
            implements AsyncUpdateOpType<Version, ValVer<Row>> {

        @Override
        public AsyncUpdateOp<Version, ValVer<Row>>
            getUpdateOp(DataCheckTableDirect dc,
                        long index,
                        boolean firstThread,
                        boolean requestPrevValue,
                        AtomicLong otherExerciseIndex) {
            return new AsyncPutIfVersionOp(dc, index, firstThread,
                                           requestPrevValue,
                                           otherExerciseIndex);
        }
    }

    static class AsyncPutIfVersionExecuteOpType extends
            UpdateOpType.PutIfVersionExecute<PrimaryKey, Row, TableOperation>
            implements
            AsyncUpdateOpType<List<TableOperationResult>, List<OpResult<Row>>> {

        @Override
        public AsyncUpdateOp<List<TableOperationResult>, List<OpResult<Row>>>
            getUpdateOp(DataCheckTableDirect dc,
                        long index,
                        boolean firstThread,
                        boolean requestPrevValue,
                        AtomicLong otherExerciseIndex) {
            return new AsyncPutIfVersionExecuteOp(dc, index, firstThread,
                                                  requestPrevValue,
                                                  otherExerciseIndex);
        }
    }

    static class AsyncTwoPutExecuteOpType
            extends UpdateOpType.TwoPutExecute<PrimaryKey, Row, TableOperation>
            implements
            AsyncUpdateOpType<List<TableOperationResult>, List<OpResult<Row>>> {

        @Override
        public AsyncUpdateOp<List<TableOperationResult>, List<OpResult<Row>>>
            getUpdateOp(DataCheckTableDirect dc,
                        long index,
                        boolean firstThread,
                        boolean requestPrevValue,
                        AtomicLong otherExerciseIndex) {
            return new AsyncTwoPutExecuteOp(dc, index, firstThread,
                                            requestPrevValue,
                                            otherExerciseIndex);
        }
    }

    static class AsyncDeleteOpType
            extends UpdateOpType.Delete<PrimaryKey, Row, TableOperation>
            implements AsyncUpdateOpType<Boolean, ValVer<Row>> {

        @Override
        public AsyncUpdateOp<Boolean, ValVer<Row>>
            getUpdateOp(DataCheckTableDirect dc,
                        long index,
                        boolean firstThread,
                        boolean requestPrevValue,
                        AtomicLong otherExerciseIndex) {
            return new AsyncDeleteOp(dc, index, firstThread, requestPrevValue,
                                     otherExerciseIndex);
        }
    }

    static class AsyncDeleteExecuteOpType
            extends UpdateOpType.DeleteExecute<PrimaryKey, Row, TableOperation>
            implements
            AsyncUpdateOpType<List<TableOperationResult>, List<OpResult<Row>>> {

        @Override
        public AsyncUpdateOp<List<TableOperationResult>, List<OpResult<Row>>>
            getUpdateOp(DataCheckTableDirect dc,
                        long index,
                        boolean firstThread,
                        boolean requestPrevValue,
                        AtomicLong otherExerciseIndex) {
            return new AsyncDeleteExecuteOp(dc, index, firstThread,
                                            requestPrevValue,
                                            otherExerciseIndex);
        }
    }

    static class AsyncDeleteIfVersionOpType extends
            UpdateOpType.DeleteIfVersion<PrimaryKey, Row, TableOperation>
            implements AsyncUpdateOpType<Boolean, ValVer<Row>> {

        @Override
        public AsyncUpdateOp<Boolean, ValVer<Row>>
            getUpdateOp(DataCheckTableDirect dc,
                        long index,
                        boolean firstThread,
                        boolean requestPrevValue,
                        AtomicLong otherExerciseIndex) {
            return new AsyncDeleteIfVersionOp(dc, index, firstThread,
                                              requestPrevValue,
                                              otherExerciseIndex);
        }
    }

    static class AsyncDeleteIfVersionExecuteOpType extends
            UpdateOpType.DeleteIfVersionExecute<PrimaryKey, Row, TableOperation>
            implements
            AsyncUpdateOpType<List<TableOperationResult>, List<OpResult<Row>>> {

        @Override
        public AsyncUpdateOp<List<TableOperationResult>, List<OpResult<Row>>>
            getUpdateOp(DataCheckTableDirect dc,
                        long index,
                        boolean firstThread,
                        boolean requestPrevValue,
                        AtomicLong otherExerciseIndex) {
            return new AsyncDeleteIfVersionExecuteOp(dc, index, firstThread,
                                                     requestPrevValue,
                                                     otherExerciseIndex);
        }
    }

    static class AsyncMultiDeleteOpType
            extends UpdateOpType.MultiDelete<PrimaryKey, Row, TableOperation>
            implements AsyncUpdateOpType<Integer, Integer> {

        @Override
        public AsyncUpdateOp<Integer, Integer>
            getUpdateOp(DataCheckTableDirect dc,
                        long index,
                        boolean firstThread,
                        boolean requestPrevValue,
                        AtomicLong otherExerciseIndex) {
            return new AsyncMultiDeleteOp(dc, index, firstThread,
                                          requestPrevValue,
                                          otherExerciseIndex);
        }
    }
}
