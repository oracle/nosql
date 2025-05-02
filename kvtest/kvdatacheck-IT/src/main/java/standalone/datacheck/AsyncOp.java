/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static standalone.datacheck.DataCheck.DEBUG;
import static standalone.datacheck.Op.MultiGetIteratorCheckOp.CHECK_INDEX;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import oracle.kv.Direction;
import oracle.kv.FaultException;
import oracle.kv.RequestTimeoutException;
import oracle.kv.StoreIteratorException;
import oracle.kv.Version;
import oracle.kv.table.IndexKey;
import oracle.kv.table.KeyPair;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.ReturnRow.Choice;
import oracle.kv.table.Row;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableOperation;
import oracle.kv.table.TableOperationResult;
import oracle.kv.table.WriteOptions;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import standalone.datacheck.DataCheck.OpResult;
import standalone.datacheck.DataCheck.ValVer;
import standalone.datacheck.DataCheckTableDirect.OpResultImpl;
import standalone.datacheck.DataCheckTableDirect.ValVerImpl;

abstract class AsyncOp<T, R> extends Op<PrimaryKey, Row, TableOperation> {
    private final long opTimeout;
    protected Throwable exception = null;
    protected OpStatus status = null;
    private final CountDownLatch latch;
    protected R result = null;
    private Runnable next = null;
    protected final TableAPI tableImpl;
    protected final DataCheckTableDirect dcTable;

    enum OpStatus {SUCCEED, EXCEPTION, TIMEOUT}

    /*
     * Non-Null inLatch : This is used when running a bunch of operations and
     * wait them to finish.
     * Null inLatch: This is used when running single operation, just like
     * passing nothing to the constructor.
     */
    public AsyncOp(DataCheckTableDirect dc, CountDownLatch inLatch) {
        super(dc);
        this.opTimeout = dc.requestTimeout;
        this.dcTable = dc;
        this.tableImpl = dcTable.tableImpl;
        this.latch = inLatch;
    }

    public AsyncOp(DataCheckTableDirect dc) {
        this(dc, null);
    }

    public CountDownLatch getLatch() {
        return this.latch;
    }

    public void setNext(Runnable next) {
        this.next = next;
    }

    public Throwable getException() {
        return exception;
    }

    public OpStatus getStatus() {
        return status;
    }

    protected abstract void setResult(T ret);

    private void releaseLatch() {
        if (latch != null) {
            latch.countDown();
        }
    }

    /*
     * This is used when we want to run some checks after the operation
     * succeeds, like 'checkValue', 'checkPreviousValue', 'checkKeyPresent'.
     */
    protected void postSucceed() { }

    protected void finishSucceed() {
        try {
            postSucceed();
            status = OpStatus.SUCCEED;
            releaseLatch();
            moveToNext();
        } catch (RuntimeException ex) {
            finishException(ex);
        }
    }

    protected void finishNoOp() {
        status = OpStatus.SUCCEED;
        releaseLatch();
        moveToNext();
    }

    protected void finishTimeout(Throwable e) {
        exception = new RuntimeException(
            "Retry timeout exceeded for operation " + this + ": " + e, e);
        status = OpStatus.TIMEOUT;
        releaseLatch();
        moveToNext();
    }

    /*
     * The exceptions must be caught and recorded in all async operations,
     * since the operations may be running inside the underlying threads.
     * We should let the phase threads to handle these exceptions.
     */
    protected void finishException(Throwable e) {
        exception = e;
        status = OpStatus.EXCEPTION;
        releaseLatch();
        moveToNext();
    }

    protected void moveToNext() {
        if (next != null) {
            next.run();
        }
    }

    public void perform() {
        try {
            doOp(opTimeout, false);
        } catch (RuntimeException ex) {
           finishException(ex);
        }
    }

    protected void handleFuture(CompletableFuture<T> future,
                                long timeout,
                                long start) {
        future.whenCompleteAsync((v, e) -> {
            if (e != null) {
                handleAsyncOpException(e, timeout, start);
            } else {
                try {
                    setResult(v);
                } catch (Exception ex) {
                    finishException(ex);
                    return;
                }
                finishSucceed();
            }
        });
    }

    protected void handleAsyncOpException(Throwable e,
                                          long timeout,
                                          long start) {
        if (e instanceof FaultException) {
            /*
             * Retry remote exceptions. RequestTimeoutException can be signaled
             * even if the requested amount of time has not elapsed, so filter
             * out here,check the timeout below.
             */
            if (!DataCheck.hasRemoteExceptionCause((FaultException) e) &&
                !(e instanceof RequestTimeoutException)) {
                finishException(e);
                return;
            }
            long now = System.currentTimeMillis();
            long left = timeout - (now - start);
            if (left <= 0) {
                finishTimeout(e);
                return;
            }
            dc.log("Retrying operation: " + this + " exception: " + e);
            dc.retryCount.incrementAndGet();
            try {
                doOp(left, true);
            } catch (RuntimeException ex) {
                finishException(ex);
            }
        } else {
            finishException(e);
        }
    }

    abstract static class AsyncExerciseOp<T, R> extends AsyncOp<T, R> {
        protected final long index;
        protected final boolean firstThread;
        protected final AtomicLong otherExerciseIndex;

        public AsyncExerciseOp(DataCheckTableDirect dc,
                               long index,
                               boolean firstThread,
                               AtomicLong otherExerciseIndex) {
            super(dc);
            this.index = index;
            this.firstThread = firstThread;
            this.otherExerciseIndex = otherExerciseIndex;
        }

        protected long getOtherExerciseIndex() {
            return otherExerciseIndex.get();
        }
    }

    abstract static class AsyncReadOp<R> extends AsyncExerciseOp<R, R> {
        protected final long keynum;
        protected PrimaryKey key;

        public AsyncReadOp(DataCheckTableDirect dc,
                           long index,
                           boolean firstThread,
                           long keynum,
                           AtomicLong otherExerciseIndex) {
            super(dc, index, firstThread, otherExerciseIndex);
            this.keynum = keynum;
            this.key = dc.keynumToKey(keynum);
        }

        @Override
        public String toString() {
            int partitionId = dc.getPartitionId(dc.keynumToKey(keynum));
            return String.format("%s[index=%#x, keynum=%#x, partition=p%d]",
                                 getName(), index, keynum,
                                 partitionId);
        }

        /** Check that the value for the key is correct. */
        protected void checkValue(PrimaryKey key1, Row value) {
            if (dc.verbose >= DEBUG) {
                dc.log("op=%s, partition=%d, key=%s, value=%s", this,
                       dc.getPartitionId(key1), dc.keyString(key1),
                       dc.valueString(value));
            }
            long otherIndex = getOtherExerciseIndex();
            dc.checkValue(this, key1, value, firstThread ? index : otherIndex,
                          firstThread ? otherIndex : index);
        }

        /** Check that the fact that the key is present is correct. */
        protected void checkKeyPresent(PrimaryKey key1) {
            if (dc.verbose >= DEBUG) {
                dc.log("op=%s, partition=%d, key=%s, keyPresent", this,
                       dc.getPartitionId(key1), dc.keyString(key1));
            }
            long otherIndex = getOtherExerciseIndex();
            dc.checkKeyPresent(this, key1, firstThread ? index : otherIndex,
                               firstThread ? otherIndex : index);
        }

        @Override
        protected void setResult(R ret) {
            this.result = ret;
        }
    }

    static class AsyncGetOp extends AsyncReadOp<Row> {

        public AsyncGetOp(DataCheckTableDirect dc,
                          long index,
                          boolean firstThread,
                          long keynum,
                          AtomicLong otherExerciseIndex) {
            super(dc, index, firstThread, keynum, otherExerciseIndex);
        }

        @Override
        public void doOp(long timeout, boolean ignore /* retrying */) {
            long start = System.currentTimeMillis();
            CompletableFuture<Row> future = tableImpl.getAsync(key,
                new ReadOptions(null, timeout, MILLISECONDS));
            handleFuture(future, timeout, start);
        }

        @Override
        protected void postSucceed() {
            checkValue(key, result);
        }
    }

    static class AsyncMultiGetOp extends AsyncReadOp<List<Row>> {

        public AsyncMultiGetOp(DataCheckTableDirect dc,
                               long index,
                               boolean firstThread,
                               long keynum,
                               AtomicLong otherExerciseIndex) {
            super(dc, index, firstThread, keynum, otherExerciseIndex);
        }

        @Override
        public void doOp(long timeout, boolean ignore /* retrying */) {
            long start = System.currentTimeMillis();
            CompletableFuture<List<Row>> future = tableImpl.multiGetAsync(key,
                null, new ReadOptions(null, timeout, MILLISECONDS));
            handleFuture(future, timeout, start);
        }

        @Override
        protected void postSucceed() {
            int size = result.size();
            if (size > 1) {
                dc.unexpectedResult("Too many results: " +
                                    "op=%s, partition=%d, key=%s, count=%d",
                                    this, dc.getPartitionId(key),
                                    dc.keyString(key), size);
            } else if (size == 1) {
                checkValue(key, result.iterator().next());
            }
        }
    }

    static class AsyncMultiGetKeysOp extends AsyncReadOp<List<PrimaryKey>> {

        public AsyncMultiGetKeysOp(DataCheckTableDirect dc,
                                   long index,
                                   boolean firstThread,
                                   long keynum,
                                   AtomicLong otherExerciseIndex) {
            super(dc, index, firstThread, keynum, otherExerciseIndex);
        }

        @Override
        public void doOp(long timeout, boolean ignore /* retrying */) {
            long start = System.currentTimeMillis();
            CompletableFuture<List<PrimaryKey>> future =
                tableImpl.multiGetKeysAsync(key, null,
                new ReadOptions(null, timeout, MILLISECONDS));
            handleFuture(future, timeout, start);
        }

        @Override
        protected void postSucceed() {
            int size = result.size();
            if (size > 1) {
                dc.unexpectedResult("Too many results: " +
                                    "op=%s, partition=%d, key=%s, count=%d",
                                    this, dc.getPartitionId(key),
                                    dc.keyString(key), size);
            } else if (size == 1) {
                checkKeyPresent(result.iterator().next());
            }
        }
    }

    /*
     * The ReadOp.MultiGetIterator and ReadOp.MultiGetKeysIterator are only
     * used in DataCheckKV, so they will not have corresponding async versions.
     */

    static abstract class AsyncBasicIteratorOp extends AsyncReadOp<Void> {

        public AsyncBasicIteratorOp(DataCheckTableDirect dc,
                                    long index,
                                    boolean firstThread,
                                    long keynum,
                                    AtomicLong otherExerciseIndex) {
            super(dc, index, firstThread, keynum, otherExerciseIndex);
        }

        /*
         * The functions of 'TableIterator' can throw 'StoreIteratorException',
         * but the retry logic does not work for this exception, since it only
         * accepts 'FaultException'. In order to retry, the caught exception
         * needs to be converted to 'FaultException'.
         *
         * The logic is learned from 'DataCheckTableDirect$IndexIteratorOp.doScan'.
         */
        private Throwable wrapIteratorException(Throwable ex) {
            if (ex instanceof StoreIteratorException) {
                Throwable cause = ex.getCause();
                if (cause != null && cause instanceof FaultException) {
                    return cause;
                }
                /*
                 * If a new 'FaultException' is created, a 'RemoteException' is
                 * set to be the cause. This is to pass the check of
                 * 'hasRemoteExceptionCause'.
                 */
                return new FaultException(ex.getMessage(),
                                          new RemoteException(), false);
            }
            return ex;
        }

        protected void asyncIterTableScanRows(Publisher<Row> pub,
                                              AsyncOp<Void, Void> op,
                                              int recordCount,
                                              long timeout,
                                              long start) {
            pub.subscribe(new Subscriber<Row>() {
                private Subscription subscription = null;
                private int count = 0;

                @Override
                public void onComplete() {
                    finishSucceed();
                }

                @Override
                public void onError(Throwable ex) {

                    /*
                     * Use in a separate thread because the
                     * handleAsyncOpException method may recurse and we could
                     * exceed the maximum stack depth
                     */
                    CompletableFuture.runAsync(
                        () -> op.handleAsyncOpException(
                            wrapIteratorException(ex), timeout, start));
                }

                @Override
                public void onNext(Row row) {
                    count++;
                    checkValue(row.createPrimaryKey(), row);
                    if (count >= recordCount) {
                        subscription.cancel();
                        /* The onComplete will be called */
                    }
                }

                @Override
                public void onSubscribe(Subscription sub) {
                    this.subscription = sub;
                    this.subscription.request(recordCount);
                }
            });
        }

        protected void asyncIterTableScanKeys(Publisher<PrimaryKey> pub,
                                              AsyncOp<Void, Void> op,
                                              int recordCount,
                                              long timeout,
                                              long start) {
            pub.subscribe(new Subscriber<PrimaryKey>() {
                private Subscription subscription = null;
                private int count = 0;

                @Override
                public void onComplete() {
                    finishSucceed();
                }

                @Override
                public void onError(Throwable ex) {

                    /*
                     * Use in a separate thread because the
                     * handleAsyncOpException method may recurse and we could
                     * exceed the maximum stack depth
                     */
                    CompletableFuture.runAsync(
                        () -> op.handleAsyncOpException(
                            wrapIteratorException(ex), timeout, start));
                }

                @Override
                public void onNext(PrimaryKey pk) {
                    count++;
                    checkKeyPresent(pk);
                    if (count == recordCount) {
                        subscription.cancel();
                        /* The onComplete will be called */
                    }
                }

                @Override
                public void onSubscribe(Subscription sub) {
                    this.subscription = sub;
                    this.subscription.request(recordCount);
                }
            });
        }

        protected void asyncIterTableScanKeyPairs(Publisher<KeyPair> pub,
                                                  AsyncOp<Void, Void> op,
                                                  int recordCount,
                                                  long timeout,
                                                  long start) {
            pub.subscribe(new Subscriber<KeyPair>() {
                private Subscription subscription = null;
                private int count = 0;

                @Override
                public void onComplete() {
                    finishSucceed();
                }

                @Override
                public void onError(Throwable ex) {

                    /*
                     * Use in a separate thread because the
                     * handleAsyncOpException method may recurse and we could
                     * exceed the maximum stack depth
                     */
                    CompletableFuture.runAsync(
                        () -> op.handleAsyncOpException(
                            wrapIteratorException(ex), timeout, start));
                }

                @Override
                public void onNext(KeyPair pair) {
                    count++;
                    checkKeyPresent(pair.getPrimaryKey());
                    if (count == recordCount) {
                        subscription.cancel();
                        /* The onComplete will be called */
                    }
                }

                @Override
                public void onSubscribe(Subscription sub) {
                    this.subscription = sub;
                    this.subscription.request(recordCount);
                }
            });
        }
    }

    static class AsyncIteratorOp extends AsyncBasicIteratorOp {

        public AsyncIteratorOp(DataCheckTableDirect dc,
                               long index,
                               boolean firstThread,
                               long keynum,
                               AtomicLong otherExerciseIndex) {
            super(dc, index, firstThread, keynum, otherExerciseIndex);
            key = dc.keynumToParentKey(keynum);
        }

        @Override
        public void doOp(long timeout, boolean ignore /* retrying */) {
            long start = System.currentTimeMillis();
            Publisher<Row> pub = tableImpl.tableIteratorAsync(key,
                null /* multiRowOptions */,
                dcTable.getTableIteratorOptions(Direction.UNORDERED, null,
                                                timeout, MILLISECONDS));
            asyncIterTableScanRows(pub, this, DataCheck.MINOR_KEY_MAX, timeout,
                                   start);
        }
    }

    static class AsyncKeysIteratorOp extends AsyncBasicIteratorOp {

        public AsyncKeysIteratorOp(DataCheckTableDirect dc,
                                   long index,
                                   boolean firstThread,
                                   long keynum,
                                   AtomicLong otherExerciseIndex) {
            super(dc, index, firstThread, keynum, otherExerciseIndex);
            key = dc.keynumToParentKey(keynum);
        }

        @Override
        public void doOp(long timeout, boolean ignore /* retrying */) {
            long start = System.currentTimeMillis();
            Publisher<PrimaryKey> pub = tableImpl.tableKeysIteratorAsync(key,
                null /* multiRowOptions */,
                dcTable.getTableIteratorOptions(Direction.UNORDERED, null,
                                                timeout, MILLISECONDS));
            asyncIterTableScanKeys(pub, this, DataCheck.MINOR_KEY_MAX, timeout,
                                   start);
        }
    }

    static class AsyncIndexIteratorOp extends AsyncBasicIteratorOp {
        private final IndexKey indexKey;

        public AsyncIndexIteratorOp(DataCheckTableDirect dc,
                                    long index,
                                    boolean firstThread,
                                    long keynum,
                                    AtomicLong otherExerciseIndex) {
            super(dc, index, firstThread, keynum, otherExerciseIndex);
            indexKey = dcTable.getIndexKey(index);
        }

        @Override
        public void doOp(long timeout, boolean ignore /* retrying */) {
            long start = System.currentTimeMillis();
            Publisher<Row> pub = tableImpl.tableIteratorAsync(indexKey,
                null /* multiRowOptions */,
                dcTable.getTableIteratorOptions(Direction.UNORDERED, null,
                                                timeout, MILLISECONDS));
            asyncIterTableScanRows(pub, this, DataCheck.MINOR_KEY_MAX, timeout,
                                   start);
        }
    }

    static class AsyncIndexKeysIteratorOp extends AsyncBasicIteratorOp {
        private final IndexKey indexKey;

        public AsyncIndexKeysIteratorOp(DataCheckTableDirect dc,
                                        long index,
                                        boolean firstThread,
                                        long keynum,
                                        AtomicLong otherExerciseIndex) {
            super(dc, index, firstThread, keynum, otherExerciseIndex);
            indexKey = dcTable.getIndexKey(index);
        }

        @Override
        public void doOp(long timeout, boolean ignore /* retrying */) {
            long start = System.currentTimeMillis();
            Publisher<KeyPair> pub = tableImpl.tableKeysIteratorAsync(indexKey,
                null /* multiRowOptions */,
                dcTable.getTableIteratorOptions(Direction.UNORDERED, null,
                                                timeout, MILLISECONDS));
            asyncIterTableScanKeyPairs(pub, this, DataCheck.MINOR_KEY_MAX,
                                       timeout, start);
        }
    }

    abstract static class AsyncUpdateOp<T, R> extends AsyncExerciseOp<T, R> {
        protected final boolean requestPrevValue;
        protected final long keynum;
        protected final PrimaryKey key;
        protected final ReturnRow returnRow;
        protected boolean retrying;

        AsyncUpdateOp(DataCheckTableDirect dc,
                      long index,
                      boolean firstThread,
                      boolean requestPrevValue,
                      AtomicLong otherExerciseIndex) {
            super(dc, index, firstThread, otherExerciseIndex);
            this.requestPrevValue = requestPrevValue;
            this.keynum = dc.exerciseIndexToKeynum(index, firstThread);
            this.key = dc.keynumToKey(keynum);
            this.returnRow = dcTable.getDataCheckTable().createReturnRow(
                requestPrevValue ? Choice.VALUE : Choice.NONE);
            this.retrying = false;
        }

        @Override
        public String toString() {
            int partitionId = dc.getPartitionId(dc.keynumToKey(
                dc.exerciseIndexToKeynum(index, firstThread)));
            return String.format("%s[index=%#x, thread%s, partition=p%d]",
                                 getName(), index,
                                 (firstThread ? "A" : "B"), partitionId);
        }

        /** Check that the previous value from a put operation is correct. */
        void checkPut(PrimaryKey key1, long keynum1, Row value, Row prevValue,
                      boolean retrying1) {
            if (dc.verbose >= DEBUG) {
                final String prevValueString;
                if (requestPrevValue) {
                    prevValueString = dc.valueString(prevValue);
                } else {
                    prevValueString = (prevValue != null) ? "found" : "none";
                }
                dc.log("op=%s, keynum=%#x, partition=%d, key=%s, value=%s" +
                       ", previousValue=%s", this, keynum1,
                       dc.getPartitionId(key1), dc.keyString(key1),
                       dc.valueString(value), prevValueString);
            }
            if (isCheckPrevValue()) {
                dc.checkPreviousValue(key1, keynum1, prevValue, index,
                                      getOtherExerciseIndex(), firstThread,
                                      !requestPrevValue, retrying1);
            }
        }

        /**
         * Check that the previous value from a delete operation is correct.
         */
        void checkDelete(PrimaryKey key1, long keynum1, Row prevValue,
                         boolean retrying1) {
            if (dc.verbose >= DEBUG) {
                final String prevValueString;
                if (requestPrevValue) {
                    prevValueString = dc.valueString(prevValue);
                } else {
                    prevValueString = (prevValue != null) ? "found" : "none";
                }
                dc.log("op=%s, keynum=%#x, partition=%d, key=%s" +
                       ", previousValue=%s", this, keynum1,
                       dc.getPartitionId(key1), dc.keyString(key1),
                       prevValueString);
            }
            if (isCheckPrevValue()) {
                dc.checkPreviousValue(key1, keynum1, prevValue, index,
                                      getOtherExerciseIndex(), firstThread,
                                      !requestPrevValue, retrying1);
            }
        }

        protected Row getPrevValue(ValVer<Row> vv) {
            if (requestPrevValue) {
                return vv.getValue();
            } else if (vv.getSuccess()) {
                return dc.getEmptyValue();
            } else {
                return null;
            }
        }

        protected Row getPrevValue(OpResult<Row> operationResult) {
            if (requestPrevValue) {
                return operationResult.getPreviousValue();
            } else if (operationResult.getSuccess()) {
                return dc.getEmptyValue();
            } else {
                return null;
            }
        }

        protected Row getPrevValue() {
            return null;
        }

        /*
         * PutIfAbsent operation executed failed if a previous value associated
         * with given key is present. So in this case, return an empty value if
         * the previous value has not been requested.
         */
        protected Row getPrevValueIfAbsent(ValVer<Row> vv) {
            if (requestPrevValue) {
                return vv.getValue();
            } else if (!vv.getSuccess()) {
                return dc.getEmptyValue();
            } else {
                return null;
            }
        }

        protected Row getPrevValueIfAbsent(OpResult<Row> operationResult) {
            if (requestPrevValue) {
                return operationResult.getPreviousValue();
            } else if (!operationResult.getSuccess()) {
                return dc.getEmptyValue();
            } else {
                return null;
            }
        }

        protected List<OpResult<Row>>
            setResultForExecute(List<TableOperationResult> ret) {
            List<OpResult<Row>> res = new ArrayList<OpResult<Row>>();
            for (TableOperationResult r : ret) {
                res.add(new OpResultImpl(r));
            }
            return res;
        }

        boolean isCheckPrevValue() {
            return true;
        }
    }

    static class AsyncPutOp extends AsyncUpdateOp<Version, ValVer<Row>> {
        protected final Row value;

        AsyncPutOp(DataCheckTableDirect dc,
                   long index,
                   boolean firstThread,
                   boolean requestPrevValue,
                   AtomicLong otherExerciseIndex) {
            super(dc, index, firstThread, requestPrevValue,
                  otherExerciseIndex);
            value = dc.getExerciseValue(index, firstThread);
        }

        @Override
        protected void setResult(Version ret) {
            this.result = new ValVerImpl(returnRow, (ret != null));
        }

        @Override
        void doOp(long timeout, boolean isRetry) {
            retrying = isRetry;
            long start = System.currentTimeMillis();
            CompletableFuture<Version> future = tableImpl.putAsync(value,
                returnRow, new WriteOptions(null /* durability */,
                                            timeout, MILLISECONDS));
            handleFuture(future, timeout, start);
        }

        @Override
        boolean isCheckPrevValue() {
            return requestPrevValue;
        }

        @Override
        protected void postSucceed() {
            dc.tallyPut(key, value);
            Row prevValue = getPrevValue();
            if (!retrying || (requestPrevValue &&
                !dc.valuesEqual(value, prevValue))) {
                checkPut(key, keynum, value, prevValue, retrying);
            }
        }

        @Override
        protected Row getPrevValue() {
            return getPrevValue(result);
        }
    }


    static class AsyncPutExecuteOp extends
            AsyncUpdateOp<List<TableOperationResult>, List<OpResult<Row>>> {
        protected final Row value;

        AsyncPutExecuteOp(DataCheckTableDirect dc,
                          long index,
                          boolean firstThread,
                          boolean requestPrevValue,
                          AtomicLong otherExerciseIndex) {
            super(dc, index, firstThread, requestPrevValue,
                  otherExerciseIndex);
            value = dc.getExerciseValue(index, firstThread);
        }

        @Override
        protected void setResult(List<TableOperationResult> ret) {
            result = setResultForExecute(ret);
        }

        @Override
        void doOp(long timeout, boolean isRetry) {
            retrying = isRetry;
            long start = System.currentTimeMillis();
            TableOperation op = getOp();
            CompletableFuture<List<TableOperationResult>> future =
                tableImpl.executeAsync(singletonList(op),
                                       new WriteOptions(null /* durability */,
                                                        timeout, MILLISECONDS));
            handleFuture(future, timeout, start);
        }

        protected TableOperation getOp() {
            return dc.createPutOp(key, value, requestPrevValue);
        }

        @Override
        boolean isCheckPrevValue() {
            return requestPrevValue;
        }

        @Override
        protected void postSucceed() {
            dc.tallyPut(key, value);
            Row prevValue = getPrevValue();
            if (!retrying || (requestPrevValue &&
                !dc.valuesEqual(value, prevValue))) {
                checkPut(key, keynum, value, prevValue, retrying);
            }
        }

        @Override
        protected Row getPrevValue() {
            return getPrevValue(result.get(0));
        }
    }

    static class AsyncPutIfAbsentOp extends AsyncPutOp {

        AsyncPutIfAbsentOp(DataCheckTableDirect dc,
                           long index,
                           boolean firstThread,
                           boolean requestPrevValue,
                           AtomicLong otherExerciseIndex) {
            super(dc, index, firstThread, requestPrevValue,
                  otherExerciseIndex);
        }

        @Override
        void doOp(long timeout, boolean isRetry) {
            retrying = isRetry;
            long start = System.currentTimeMillis();
            CompletableFuture<Version> future = tableImpl.putIfAbsentAsync(
                value, returnRow,
                new WriteOptions(null /* durability */, timeout, MILLISECONDS));
            handleFuture(future, timeout, start);
        }

        @Override
        protected Row getPrevValue() {
            return getPrevValueIfAbsent(result);
        }

        @Override
        boolean isCheckPrevValue() {
            return true;
        }
    }

    static class AsyncPutIfAbsentExecuteOp extends AsyncPutExecuteOp {

        AsyncPutIfAbsentExecuteOp(DataCheckTableDirect dc,
                                  long index,
                                  boolean firstThread,
                                  boolean requestPrevValue,
                                  AtomicLong otherExerciseIndex) {
            super(dc, index, firstThread, requestPrevValue,
                  otherExerciseIndex);
        }

        @Override
        protected TableOperation getOp() {
            return dc.createPutIfAbsentOp(key, value, requestPrevValue);
        }

        @Override
        protected Row getPrevValue() {
            return getPrevValueIfAbsent(result.get(0));
        }

        @Override
        boolean isCheckPrevValue() {
            return true;
        }
    }

    static class AsyncPutIfPresentOp extends AsyncPutOp {

        AsyncPutIfPresentOp(DataCheckTableDirect dc,
                            long index,
                            boolean firstThread,
                            boolean requestPrevValue,
                            AtomicLong otherExerciseIndex) {
            super(dc, index, firstThread, requestPrevValue,
                  otherExerciseIndex);
        }

        @Override
        void doOp(long timeout, boolean isRetry) {
            retrying = isRetry;
            long start = System.currentTimeMillis();
            CompletableFuture<Version> future = tableImpl.putIfPresentAsync(
                value, returnRow,
                new WriteOptions(null /* durability */, timeout, MILLISECONDS));
            handleFuture(future, timeout, start);
        }

        @Override
        boolean isCheckPrevValue() {
            return true;
        }
    }

    static class AsyncPutIfPresentExecuteOp extends AsyncPutExecuteOp {

        AsyncPutIfPresentExecuteOp(DataCheckTableDirect dc,
                                   long index,
                                   boolean firstThread,
                                   boolean requestPrevValue,
                                   AtomicLong otherExerciseIndex) {
            super(dc, index, firstThread, requestPrevValue,
                  otherExerciseIndex);
        }

        @Override
        protected TableOperation getOp() {
            return dc.createPutIfPresentOp(key, value, requestPrevValue);
        }

        @Override
        boolean isCheckPrevValue() {
            return true;
        }
    }

    static class AsyncPutIfVersionOp extends AsyncPutOp {

        AsyncPutIfVersionOp(DataCheckTableDirect dc,
                            long index,
                            boolean firstThread,
                            boolean requestPrevValue,
                            AtomicLong otherExerciseIndex) {
            super(dc, index, firstThread, requestPrevValue,
                  otherExerciseIndex);
        }

        @Override
        void doOp(long timeout, boolean isRetry) {
            retrying = isRetry;
            long start = System.currentTimeMillis();
            CompletableFuture<Row> getFuture = tableImpl.getAsync(key,
                new ReadOptions(null, timeout, MILLISECONDS));
            getFuture.whenCompleteAsync((row, e) -> {
                if (e != null) {
                    handleAsyncOpException(e, timeout, start);
                } else if (row == null) {
                    finishNoOp();
                } else {
                    try {
                        result = new ValVerImpl(row, true);
                        Version v = row.getVersion();
                        CompletableFuture<Version> future =
                            tableImpl.putIfVersionAsync(value, v, returnRow,
                            new WriteOptions(null /* durability */, timeout,
                                             MILLISECONDS));
                        handleFuture(future, timeout, start);
                    } catch (Exception ex) {
                        handleAsyncOpException(ex, timeout, start);
                    }
                }
            });
        }

        @Override
        protected void setResult(Version ret) {
            /*
             * The result has been set when get succeeds, so no op here.
             */
        }

        @Override
        boolean isCheckPrevValue() {
            return true;
        }
    }

    static class AsyncPutIfVersionExecuteOp extends AsyncPutExecuteOp {
        protected ValVer<Row> prevValue;

        AsyncPutIfVersionExecuteOp(DataCheckTableDirect dc,
                                   long index,
                                   boolean firstThread,
                                   boolean requestPrevValue,
                                   AtomicLong otherExerciseIndex) {
            super(dc, index, firstThread, requestPrevValue,
                  otherExerciseIndex);
        }

        @Override
        void doOp(long timeout, boolean isRetry) {
            retrying = isRetry;
            long start = System.currentTimeMillis();
            CompletableFuture<Row> getFuture = tableImpl.getAsync(key,
                new ReadOptions(null, timeout, MILLISECONDS));
            getFuture.whenCompleteAsync((row, e) -> {
                if (e != null) {
                    handleAsyncOpException(e, timeout, start);
                } else if (row == null) {
                    finishNoOp();
                } else {
                    try {
                        prevValue = new ValVerImpl(row, true);
                        Version v = row.getVersion();
                        TableOperation op = dc.createPutIfVersionOp(key, value,
                            v, requestPrevValue);
                        CompletableFuture<List<TableOperationResult>> future =
                            tableImpl.executeAsync(singletonList(op),
                            new WriteOptions(null /* durability */, timeout,
                                             MILLISECONDS));
                        handleFuture(future, timeout, start);
                    } catch (Exception ex) {
                        handleAsyncOpException(ex, timeout, start);
                    }
                }
            });
        }

        @Override
        boolean isCheckPrevValue() {
            return true;
        }

        @Override
        protected void setResult(List<TableOperationResult> ret) {
            /*
             * We don't use result, so no op here.
             */
        }

        /*
         * The previous value is obtained by GET operation which was executed
         * just before the 'IfVersion' operation.
         */
        @Override
        protected Row getPrevValue() {
            return prevValue.getValue();
        }

    }

    static class AsyncTwoPutExecuteOp extends
            AsyncUpdateOp<List<TableOperationResult>, List<OpResult<Row>>> {
        protected final Row value;
        protected final PrimaryKey key2;

        AsyncTwoPutExecuteOp(DataCheckTableDirect dc,
                             long index,
                             boolean firstThread,
                             boolean requestPrevValue,
                             AtomicLong otherExerciseIndex) {
            super(dc, index, firstThread, requestPrevValue,
                  otherExerciseIndex);
            value = dc.getExerciseValue(index, firstThread);
            key2 = dc.appendStringToKey(key, "x");
        }

        @Override
        protected void setResult(List<TableOperationResult> ret) {
            result = setResultForExecute(ret);
        }

        @Override
        void doOp(long timeout, boolean isRetry) {
            retrying = isRetry;
            long start = System.currentTimeMillis();
            List<TableOperation> operations = new ArrayList<TableOperation>(2);
            operations.add(dc.createPutOp(key, value, requestPrevValue));
            operations.add(dc.createPutIfPresentOp(key2, value,
                                                   requestPrevValue));
            CompletableFuture<List<TableOperationResult>> future =
                tableImpl.executeAsync(operations,
                new WriteOptions(null /* durability */, timeout, MILLISECONDS));
            handleFuture(future, timeout, start);
        }

        @Override
        protected void postSucceed() {
            dc.tallyPut(key, value);
            Row prevValue = result.get(0).getPreviousValue();
            if (!retrying || !dc.valuesEqual(value, prevValue)) {
                checkPut(key, keynum, value, prevValue, retrying);
            }

            dc.tallyPut(key2, value);
            prevValue = result.get(1).getPreviousValue();
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

    static class AsyncDeleteOp extends AsyncUpdateOp<Boolean, ValVer<Row>> {

        AsyncDeleteOp(DataCheckTableDirect dc,
                      long index,
                      boolean firstThread,
                      boolean requestPrevValue,
                      AtomicLong otherExerciseIndex) {
            super(dc, index, firstThread, requestPrevValue,
                  otherExerciseIndex);
        }

        @Override
        protected void setResult(Boolean ret) {
            this.result = new ValVerImpl(returnRow, ret);
        }

        @Override
        void doOp(long timeout, boolean isRetry) {
            retrying = isRetry;
            long start = System.currentTimeMillis();
            CompletableFuture<Boolean> future = tableImpl.deleteAsync(key,
                returnRow,
                new WriteOptions(null /* durability */, timeout, MILLISECONDS));
            handleFuture(future, timeout, start);
        }

        @Override
        boolean isCheckPrevValue() {
            return true;
        }

        @Override
        protected void postSucceed() {
            Row prevValue = getPrevValue();
            if (!retrying || prevValue != null) {
                checkDelete(key, keynum, prevValue, retrying);
            }
        }

        @Override
        protected Row getPrevValue() {
            return getPrevValue(this.result);
        }
    }

    static class AsyncMultiDeleteOp extends AsyncUpdateOp<Integer, Integer> {

        @SuppressWarnings("unused")
        AsyncMultiDeleteOp(DataCheckTableDirect dc,
                           long index,
                           boolean firstThread,
                           boolean unused /* requestPrevValue */,
                           AtomicLong otherExerciseIndex) {
            super(dc, index, firstThread, false, otherExerciseIndex);
        }

        @Override
        void doOp(long timeout, boolean isRetry) {
            retrying = isRetry;
            long start = System.currentTimeMillis();
            CompletableFuture<Integer> future = tableImpl.multiDeleteAsync(
                key, null,
                new WriteOptions(null /* durability */, timeout, MILLISECONDS));
            handleFuture(future, timeout, start);
        }

        @Override
        protected void postSucceed() {
            int count = result.intValue();
            /*
             * Only check if the operation is not retrying.
             */
            if (!retrying) {
                checkDelete(key, keynum,
                            count == 0 ? null : dc.getEmptyValue(),
                            false /* retrying */);
            }
            if (dc.verbose >= DEBUG) {
                dc.log("op=%s, partition=%d, key=%s, keynum=%#x, result=%d",
                       this, dc.getPartitionId(key), key, keynum, count);
            }

            /*
             * Retries will decrease but not increase the number of objects
             * deleted, so this check is still valid on retry.
             */
            if (count > 1) {
                dc.unexpectedResult("Too many deletes:" +
                                    "op=%s, partition=%d, key=%s, keynum=%#x" +
                                    ", result=%d", this, dc.getPartitionId(key),
                                    dc.keyString(key), keynum, count);
            }
        }

        @Override
        protected void setResult(Integer ret) {
            this.result = ret;

        }
    }

    static class AsyncDeleteExecuteOp extends
            AsyncUpdateOp<List<TableOperationResult>, List<OpResult<Row>>> {

        AsyncDeleteExecuteOp(DataCheckTableDirect dc,
                             long index,
                             boolean firstThread,
                             boolean requestPrevValue,
                             AtomicLong otherExerciseIndex) {
            super(dc, index, firstThread, requestPrevValue,
                  otherExerciseIndex);
        }

        @Override
        void doOp(long timeout, boolean isRetry) {
            retrying = isRetry;
            long start = System.currentTimeMillis();
            TableOperation op = dc.createDeleteOp(key, requestPrevValue);
            CompletableFuture<List<TableOperationResult>> future =
                tableImpl.executeAsync(singletonList(op),
                new WriteOptions(null /* durability */, timeout, MILLISECONDS));
            handleFuture(future, timeout, start);
        }

        @Override
        boolean isCheckPrevValue() {
            return true;
        }

        @Override
        protected void setResult(List<TableOperationResult> ret) {
            result = setResultForExecute(ret);
        }

        @Override
        protected void postSucceed() {
            Row prevValue = getPrevValue();
            if (!retrying || prevValue != null) {
                checkDelete(key, keynum, prevValue, retrying);
            }
        }

        @Override
        protected Row getPrevValue() {
            return getPrevValue(result.get(0));
        }
    }

    static class AsyncDeleteIfVersionOp extends AsyncDeleteOp {

        AsyncDeleteIfVersionOp(DataCheckTableDirect dc,
                               long index,
                               boolean firstThread,
                               boolean requestPrevValue,
                               AtomicLong otherExerciseIndex) {
            super(dc, index, firstThread, requestPrevValue,
                  otherExerciseIndex);
        }

        @Override
        void doOp(long timeout, boolean isRetry) {
            retrying = isRetry;
            long start = System.currentTimeMillis();
            CompletableFuture<Row> getFuture = tableImpl.getAsync(key,
                new ReadOptions(null, timeout, MILLISECONDS));
            getFuture = getFuture.whenCompleteAsync((row, e) -> {
                if (e != null) {
                    handleAsyncOpException(e, timeout, start);
                } else if (row == null) {
                    finishNoOp();
                } else {
                    try {
                        result = new ValVerImpl(row, true);
                        Version v = row.getVersion();
                        CompletableFuture<Boolean> future =
                            tableImpl.deleteIfVersionAsync(key, v, returnRow,
                            new WriteOptions(null /* durability */, timeout,
                                             MILLISECONDS));
                        handleFuture(future, timeout, start);
                    } catch (Exception ex) {
                        handleAsyncOpException(ex, timeout, start);
                    }
                }
            });
        }

        @Override
        protected void setResult(Boolean ret) {
            /*
             * The result has been set after the get succeeds, so no op here.
             */
        }

    }

    static class AsyncDeleteIfVersionExecuteOp extends AsyncDeleteExecuteOp {
        protected ValVer<Row> prevValue = null;

        AsyncDeleteIfVersionExecuteOp(DataCheckTableDirect dc,
                                      long index,
                                      boolean firstThread,
                                      boolean requestPrevValue,
                                      AtomicLong otherExerciseIndex) {
            super(dc, index, firstThread, requestPrevValue,
                  otherExerciseIndex);
        }

        @Override
        void doOp(long timeout, boolean isRetry) {
            retrying = isRetry;
            long start = System.currentTimeMillis();
            CompletableFuture<Row> getFuture = tableImpl.getAsync(key,
                new ReadOptions(null, timeout, MILLISECONDS));
            getFuture.whenCompleteAsync((row, e) -> {
                if (e != null) {
                    handleAsyncOpException(e, timeout, start);
                } else if (row == null) {
                    finishNoOp();
                } else {
                    try {
                        prevValue = new ValVerImpl(row, row != null);
                        Version v = row.getVersion();
                        TableOperation op = dc.createDeleteIfVersionOp(
                            key, v, requestPrevValue);
                        CompletableFuture<List<TableOperationResult>> future =
                            tableImpl.executeAsync(singletonList(op),
                            new WriteOptions(null /* durability */,
                                             timeout,
                                              MILLISECONDS));
                        handleFuture(future, timeout, start);
                    } catch (Exception ex) {
                        handleAsyncOpException(ex, timeout, start);
                    }
                }
            });
        }

        @Override
        protected void setResult(List<TableOperationResult> ret) {
            /*
             * We don't use result, so no op here.
             */
        }

        @Override
        protected Row getPrevValue() {
            return getPrevValue(prevValue);
        }

    }

    static class AsyncExecuteOp extends
            AsyncOp<List<TableOperationResult>, List<TableOperationResult>> {
        protected final List<TableOperation> opList;

        public AsyncExecuteOp(DataCheckTableDirect dc, TableOperation op) {
            super(dc);
            opList = singletonList(op);
        }

        public AsyncExecuteOp(DataCheckTableDirect dc, List<TableOperation> ops) {
            super(dc);
            opList = new ArrayList<>(ops);
        }

        public AsyncExecuteOp(DataCheckTableDirect dc,
                              TableOperation op,
                              CountDownLatch inLatch) {
            super(dc, inLatch);
            opList = singletonList(op);
        }

        public AsyncExecuteOp(DataCheckTableDirect dc,
                              List<TableOperation> ops,
                              CountDownLatch inLatch) {
            super(dc, inLatch);
            opList = new ArrayList<>(ops);
        }

        @Override
        void doOp(long timeout, boolean ignore /* retrying */) {
            long start = System.currentTimeMillis();
            CompletableFuture<List<TableOperationResult>> future =
                tableImpl.executeAsync(opList,
                                       new WriteOptions(null /* durability */,
                                                        timeout,
                                                        MILLISECONDS));
            handleFuture(future, timeout, start);
        }

        @Override
        protected void setResult(List<TableOperationResult> ret) {
            this.result = ret;

        }

        @Override
        public String toString() {
            return getName();
        }
    }

    static class AsyncMultiGetIteratorCheckOp
            extends AsyncOp<List<Row>, List<Row>> {
        private final long firstKeynum;
        private final PrimaryKey firstKey;
        private final long index;
        private long stop;

        public AsyncMultiGetIteratorCheckOp(DataCheckTableDirect dc,
                                            CountDownLatch inLatch,
                                            long index) {
            super(dc, inLatch);
            this.index = index;
            this.firstKeynum = DataCheck.indexToKeynum(index);
            this.firstKey = dc.keynumToMajorKey(firstKeynum);
        }

        @Override
        void doOp(long timeout, boolean ignore /* retrying */) {
            stop = System.currentTimeMillis() + timeout;
            long start = System.currentTimeMillis();
            CompletableFuture<List<Row>> future = tableImpl.multiGetAsync(
                firstKey, null,
                new ReadOptions(null, timeout, MILLISECONDS));
            handleFuture(future, timeout, start);
        }

        @Override
        protected void postSucceed() {
            long max = firstKeynum + DataCheck.MINOR_KEY_MAX;
            Iterator<Row> iter = result.iterator();
            Row iterValue = null;
            for (long keynum = firstKeynum; keynum < max; keynum++) {
                if (System.currentTimeMillis() > stop) {
                    dc.unexpectedResult("Request timeout for key: " +
                                        "op=%s, partition=%d, key=%s", this,
                                        dc.getPartitionId(firstKey),
                                        dc.keyString(firstKey));
                }
                if (iterValue == null && iter.hasNext()) {
                    iterValue = iter.next();
                }
                PrimaryKey key = dc.keynumToKey(keynum);
                Row value;
                if (iterValue != null &&
                    key.equals(iterValue.createPrimaryKey())) {
                    value = iterValue;
                    iterValue = null;
                } else {
                    value = null;
                }
                dc.checkValue(this, key, value, CHECK_INDEX, CHECK_INDEX);
            }
            if (iter.hasNext()) {
                dc.unexpectedResult("Too many entries for key: " +
                                    "op=%s, partition=%d, key=%s", this,
                                    dc.getPartitionId(firstKey),
                                    dc.keyString(firstKey));
            }
        }

        @Override
        public String toString() {
            return String.format("AsyncMultiGetIteratorCheckOp[index=%#x]",
                                 index);
        }

        @Override
        protected void setResult(List<Row> ret) {
            this.result = ret;
        }
    }
}
