/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import java.io.PrintStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.IOException;
import java.io.File;
import java.util.Collection;
import java.util.Formatter;
import java.util.List;
import java.util.Random;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.FaultException;
import oracle.kv.MetadataNotFoundException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.StoreIteratorConfig;
import oracle.kv.ParallelScanIterator;
import oracle.kv.StoreIteratorException;
import oracle.kv.KeyValueVersion;
import oracle.kv.StatementResult;
import oracle.kv.ExecutionFuture;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.util.ThreadUtils;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;
import oracle.kv.table.TableOperation;
import oracle.kv.table.TableOpExecutionException;

/**
 * Implement API for data checking used by {@link DataCheckMain}.
 * This class is to test drop table functionality.
 */
public class DataCheckDropTable extends DataCheckTableDirect {

    /**
     * Drop table test uses the following additional arguments:
     * -dropTable - instructs DataCheckMain to run drop table test (this will
     *  also happen if any other -dropTable... parameter described below is
     *  specified) with default values for the parameters described below if
     *  they are not specified
     * -dropTableBeforeDropWaitTime - how long to wait after starting
     *  the exercise phase before initiating drop table operation, in
     *  milliseconds.
     * -dropTableRunDatacheckExercise - whether to run the regular datacheck
     *  exercise operations during the exercise phase of this test, true or
     *  false, true by default.  Basically this will invoke
     *  DataCheck.exercise() including all exercise threads it starts.
     * -dropTableDropConcurrently - whether issue drop table operation
     *  concurrently with regular datacheck exercise operations (provided that
     *  dropTableRunDatacheckExercise is true).  Setting to false will stop
     *  datacheck exercise threads before issuing drop table.  Defaults to
     *  true.
     *  -dropTableDropSync - whether to issue drop table operation using
     *  KVStore.executeSync() or using KVStore.execute() and waiting on
     *  ExecutionFuture.  Defaults to true (use KVStore.executeSync()).
     *  -dropTableUseSingleKVStore - whether to issue drop table operation
     *  using the same KVStore instance that is used for issuing all other
     *  test operations or to use use separate KVStore instance for drop table
     *  operation.  Defaults to false (use separate KVStore instance).
     * -dropTableRunTableIterator - whether to run table iterator thread
     *  during the exercise phase, true or false, true by default.
     * -dropTablePerBlockDropTimeout - timeout for drop table operation, in
     *  milliseconds per block (multiply by number of blocks to get total
     *  timeout), this is useful if running in the old mode (without drop
     *  table optimization) where the drop table operation can take
     *  significant time proportional to the number of records in the table
     * -dropTableDropRetryCount - how many times to retry drop table operation
     *  if it fails
     * -dropTableDropRetryWaitTime - how long to wait before drop table
     *  retries, in milliseconds
     * -dropTableAfterDropWaitTime - how long to wait after drop table
     *  operation has finished before stopping exercise phase, in milliseconds
     * -dropTablePerBlockCheckWaitTime - in check phase, how long to wait
     *  between successive iterations to check for record extinction, in
     *  milliseconds per block (multiply by number of blocks to get
     *  total wait time)
     * -dropTablePerBlockCheckTimeout maximum time the check phase should take
     *  before declaring failure, in milliseconds per block (multiply by
     *  number of blocks to get total timeout)
     */
    public static class InitParams {

        public static final long INIT_WAIT_TIME = 75000;
        public static final long DROP_TIMEOUT = 60000;
        public static final long PB_DROP_TIMEOUT = 1000;
        public static final int DROP_RETRY_CNT = 5;
        public static final long DROP_RETRY_WAIT_TIME = 5000;
        public static final long AFTER_DROP_WAIT_TIME = 25000;
        public static final long CHECK_WAIT_TIME = 20000;
        public static final long PB_CHECK_WAIT_TIME = 60;
        public static final long CHECK_TIMEOUT = 300000;
        public static final long PB_CHECK_TIMEOUT = 10000;

        public static final String USAGE = String.format(
            "Arguments for drop table test:\n" +
            "  -dropTable\n" +
            "    Run drop table test instead of regular datacheck test,\n" +
            "    any -dropTable... option implies this\n" +
            "  -dropTableBeforeDropWaitTime <ms> (default: %d)\n" +
            "  -dropTableRunDatacheckExercise [true|false] (default: true)\n" +
            "  -dropTableDropConcurrently [true|false] (default: true)\n" +
            "  -dropTableDropSync [true|false] (default: true)\n" +
            "  -dropTableUseSingleKVStore [true|false] (default: false)\n" +
            "  -dropTableRunTableIterator [true|false] (default: true)\n" +
            "  -dropTablePerBlockDropTimeout <ms> (default: %d)\n" +
            "  -dropTableDropRetryCount <ms> (default: %d)\n" +
            "  -dropTableDropRetryWaitTime <ms> (default: %d)\n" +
            "  -dropTableAfterDropWaitTime <ms> (default: %d)\n" +
            "  -dropTablePerBlockCheckWaitTime <ms> (default: %d)\n" +
            "  -dropTablePerBlockCheckTimeout <ms> (default: %d)\n",
            INIT_WAIT_TIME,
            PB_DROP_TIMEOUT,
            DROP_RETRY_CNT,
            DROP_RETRY_WAIT_TIME,
            AFTER_DROP_WAIT_TIME,
            PB_CHECK_WAIT_TIME,
            PB_CHECK_TIMEOUT);

        /* TODO:
         * 1) Use accessor methods instead of public fields
         * 2) Define parameters dropTimeout, checkWaitTime, checkTimeout as
         *    alternative to per-block basis instead of only using defaults
         */
        public long beforeDropWaitTime = INIT_WAIT_TIME;
        public boolean runDatacheckExercise = true;
        public boolean dropConcurrently = true;
        public boolean dropSync = true;
        public boolean useSingleKVStore = false;
        public boolean runTableIterator = true;
        public long perBlockDropTimeout = PB_DROP_TIMEOUT;
        public int dropRetryCnt = DROP_RETRY_CNT;
        public long dropRetryWaitTime = DROP_RETRY_WAIT_TIME;
        public long afterDropWaitTime = AFTER_DROP_WAIT_TIME;
        public long perBlockCheckWaitTime = PB_CHECK_WAIT_TIME;
        public long perBlockCheckTimeout = PB_CHECK_TIMEOUT;

        void parseArg(String arg, String[] argv, int argc) {
            if ("-dropTable".equals(arg)) {
                return;
            }
            if ("-dropTableBeforeDropWaitTime".equals(arg)) {
                beforeDropWaitTime = DataCheckMain.getLongArg(arg, argv, argc);
            }
            else if ("-dropTableRunDatacheckExercise".equals(arg)) {
                runDatacheckExercise = DataCheckMain.getBooleanArg(arg, argv,
                        argc);
            }
            else if ("-dropTableDropConcurrently".equals(arg)) {
                dropConcurrently = DataCheckMain.getBooleanArg(arg, argv,
                        argc);
            }
            else if ("-dropTableDropSync".equals(arg)) {
                dropSync = DataCheckMain.getBooleanArg(arg, argv, argc);
            }
            else if ("-dropTableUseSingleKVStore".equals(arg)) {
                useSingleKVStore = DataCheckMain.getBooleanArg(arg, argv, argc);
            }
            else if ("-dropTableRunTableIterator".equals(arg)) {
                runTableIterator = DataCheckMain.getBooleanArg(arg, argv,
                        argc);
            }
            else if ("-dropTableDropRetryCount".equals(arg)) {
                dropRetryCnt = DataCheckMain.getIntArg(arg, argv, argc);
            }
            else if ("-dropTableDropRetryWaitTime".equals(arg)) {
                dropRetryWaitTime = DataCheckMain.getLongArg(arg, argv, argc);
            }
            else if ("-dropTableAfterDropWaitTime".equals(arg)) {
                afterDropWaitTime = DataCheckMain.getLongArg(arg, argv, argc);
            }
            else if ("-dropTablePerBlockCheckWaitTime".equals(arg)) {
                perBlockCheckWaitTime =
                        DataCheckMain.getLongArg(arg, argv, argc);
            }
            else if ("-dropTablePerBlockCheckTimeout".equals(arg)) {
                perBlockCheckTimeout =
                        DataCheckMain.getLongArg(arg, argv, argc);
            }
            else {
                DataCheckMain.usage("Unknown argument for drop table test: " +
                        arg);
            }
        }

        @Override
        public String toString() {
            return "-dropTableBeforeDropWaitTime " + beforeDropWaitTime +
                   "\n -dropTableRunDatacheckExercise " +
                        runDatacheckExercise +
                   "\n -dropTableDropConcurrently" + dropConcurrently +
                   "\n -droTableRunTableIterator " + runTableIterator +
                   "\n -dropTableDropRetryCount " + dropRetryCnt +
                   "\n -dropTableDropRetryWaitTime " + dropRetryWaitTime +
                   "\n -dropTableAfterDropWaitTime " + afterDropWaitTime +
                   "\n -dropTablePerBlockCheckWaitTime " +
                   perBlockCheckWaitTime +
                   "\n -dropTablePerBlockCheckTimeout " + perBlockCheckTimeout;
        }

        public long getDropTimeout(int blocks) {
            return Math.max(DROP_TIMEOUT, perBlockDropTimeout * blocks);
        }

        public long getCheckWaitTime(int blocks) {
            return Math.max(CHECK_WAIT_TIME, perBlockCheckWaitTime * blocks);
        }

        public long getCheckTimeout(int blocks) {
            return Math.max(CHECK_TIMEOUT, perBlockCheckTimeout * blocks);
        }

    }

    enum DropState { BEFORE, STARTED, FINISHED, ERROR }

    /*
     * Since we are not measuring DML performance here, we don't need to vary
     * this parameter.
     */
    private static final long DEF_BLOCK_TIMEOUT = 1000000;

    /*
     * How many expected exercise failures after drop table we should log.
     * These failures are expected so we don't want to log too many.
     */
    private static final long FAILED_AFTER_LOG_MAX = 10;

    private static final File TBL_IMPL_FILE =
            new File(System.getenv("SCRATCHDIR"), "datacheck-tbl-impl");

    private final KVStoreConfig config;
    private final InitParams params;
    private volatile long dropTableTime;
    private volatile TableImpl dataCheckTbl;
    private volatile DropState dropState = DropState.BEFORE;
    private volatile List<PhaseThread> phaseThreads;

    private final ThreadLocal<Boolean> absoluteConsistency =
            new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    private final AtomicLong succeededDuringCnt = new AtomicLong();
    private final AtomicLong failedDuringCnt = new AtomicLong();
    private final AtomicLong failedAfterCnt = new AtomicLong();
    private final AtomicLong retryAbsConsCnt = new AtomicLong();
    private final AtomicLong succeededAfterCnt = new AtomicLong();

    /**
     * Creates an instance of this class.  Use a new instance for each test
     * phase.
     *
     * @param store the store
     * @param config the configuration used to create the store
     * @param reportingInterval the interval in milliseconds between reporting
     *        progress and status of the store
     * @param seed the random seed, or -1 to generate a seed randomly
     * @param verbose the verbose level, with increasing levels of verbosity
     *        for values beyond {@code 0}
     * @param err the error output stream
     * @param useParallelScan use parallel scan in exercise phase if specified.
     * @param scanMaxConcurrentRequests the max concurrent threads are allowed
     *        for a parallel scan.
     * @param scanMaxResultsBatches the max batches to temporally store the
     *        parallel scan results, each batch can store one scan results.
     * @param params additional parameters for drop table test, see
     *        {@link DataCheckDropTable.InitParams}
     */
    public DataCheckDropTable(KVStore store,
                          KVStoreConfig config,
                          long reportingInterval,
                          long seed,
                          int verbose,
                          PrintStream err,
                          boolean useParallelScan,
                          int scanMaxConcurrentRequests,
                          int scanMaxResultsBatches,
                          int indexReadPercent,
                          InitParams params) {

        super(store, config, reportingInterval, null /* partitions */, seed,
              verbose, err, DEF_BLOCK_TIMEOUT, useParallelScan,
              scanMaxConcurrentRequests, scanMaxResultsBatches,
              indexReadPercent, 0 /* maxThroughput */, 0 /* maxExecTime */,
              false /* useTTL */, null /* adminHostName */,
              null /* adminPortName */);

        this.config = config;
        this.params = params;
    }

    /*
     * We need TableImpl object in the check phase so that we can check
     * whether the records we retrieve during the store iteration belong
     * to the datacheck table.  However during the check phase the table
     * no longer exists.  So instead we serialize TableImpl object in the
     * beginning of the exercise phase (when table still exists) and
     * deserialize it in the beginning of the check phase.
     */

    private void saveDataCheckTbl() throws IOException
    {
        dataCheckTbl = (TableImpl)getDataCheckTable();
        assert dataCheckTbl != null;
        ObjectOutputStream os = null;
        try {
            os = new ObjectOutputStream(new FileOutputStream(TBL_IMPL_FILE));
            os.writeObject(dataCheckTbl);
            os.flush();
            /* Table id will be needed for erasure test script, the easiest
             * way being to grep from the test output.
             */
            log("DataCheck table id: " + dataCheckTbl.getId());
            log("Saved TableImpl into " + TBL_IMPL_FILE);
        }
        finally {
            if (os != null) {
                os.close();
            }
        }
    }

    private void loadDataCheckTbl() throws IOException, ClassNotFoundException
    {
        ObjectInputStream is = null;
        try {
            is = new ObjectInputStream(new FileInputStream(TBL_IMPL_FILE));
            dataCheckTbl = (TableImpl)is.readObject();
            assert dataCheckTbl != null;
            log("Loaded TableImpl from " + TBL_IMPL_FILE);
        }
        finally {
            if (is != null) {
                is.close();
            }
        }
    }

    /*
     * If operation is started after drop table finished (dropState =
     * DropState.FINISHED) then we expect it to fail.  If operation is
     * finished before drop table starts (dropState = DropState.BEFORE),
     * we expect the same behavior as for normal exercise phase.  If
     * operation time intersects with drop table time (operation starts
     * or finishes when dropState = DropState.STARTED), we can't really
     * guarantee anything, since it will depend on the timing, so we just
     * log the occurrence.
     */

    private void doOpDDT(Op<PrimaryKey, Row, TableOperation> op,
            long timeoutMillis, boolean retrying) {
        DropState startDropState = dropState;
        try {
            op.doOp(timeoutMillis, retrying);
        }
        catch(FaultException | MetadataNotFoundException |
              StoreIteratorException | IllegalStateException e) {

            /*
             * IllegalStateException can be thrown by update ops that call
             * DataCheck.execute() and would have TableOpExecutionException as
             * the cause.  Any other IllegalStateException we consider as
             * abnormal.
             */
            if ((e instanceof IllegalStateException) && !(e.getCause()
                    instanceof TableOpExecutionException)) {
                throw e;
            }
            if (dropState == DropState.BEFORE) {
                throw e;
            }
            if (dropState == DropState.STARTED ||
                dropState == DropState.ERROR ||
                startDropState != DropState.FINISHED) {
                failedDuringCnt.incrementAndGet();
                log("%s failed while drop table is in progress, " +
                    "exception: %s", op.toString(), e.toString());
            }
            else { /* dropState = FINISHED and startDropState = FINISHED */
                if (failedAfterCnt.incrementAndGet() <= FAILED_AFTER_LOG_MAX) {
                    log("%s failed expectedly after drop table, exception: %s",
                            op.toString(),
                            e.toString());
                }
            }
            return;
        }
        if (startDropState == DropState.FINISHED) {

            /*
             * If read operation succeeds after drop table, retry with
             * absolute consistency.
             */
            if ((op instanceof ReadOp) && !absoluteConsistency.get()) {
                absoluteConsistency.set(true);
                try {
                    retryAbsConsCnt.incrementAndGet();
                    log(op + " succeded started after drop table and " +
                            "succeeded, retrying with absolute " +
                            "consistency");
                    doOpDDT(op, timeoutMillis, retrying);
                    return;
                }
                finally {
                    absoluteConsistency.set(false);
                }
            }
            succeededAfterCnt.incrementAndGet();
            unexpectedResult(op + " started after drop table and " +
                    "succeeded");
        }
        else if (dropState != DropState.BEFORE) {
            succeededDuringCnt.incrementAndGet();
            log(op + "succeeded while drop table is in progress");
        }
    }

    /* Read exercise operation during drop table scenario */
    private class ReadOpDDT extends ReadOp<PrimaryKey, Row, TableOperation> {

        private final ReadOp<PrimaryKey, Row, TableOperation> op;

        ReadOpDDT(ReadOp<PrimaryKey, Row, TableOperation> op) {
            super(op.dc, op.index, op.firstThread, op.keynum);
            this.op = op;
        }

        @Override
        void doOp(long timeoutMillis) {
            doOpDDT(op, timeoutMillis, false);
        }
    }

    @Override
    ReadOp<PrimaryKey, Row, TableOperation> getReadOp(long keynum, long index,
                                                      boolean firstThread,
                                                      Random threadRandom) {
        return new ReadOpDDT(super.getReadOp(
                keynum, index, firstThread, threadRandom));
    }

    /* Update exercise operation during drop table scenario */
    private class UpdateOpDDT
        extends UpdateOp<PrimaryKey, Row, TableOperation> {

        private final UpdateOp<PrimaryKey, Row, TableOperation> op;

        UpdateOpDDT(UpdateOp<PrimaryKey, Row, TableOperation> op) {
            super(op.dc, op.index, op.firstThread, op.requestPrevValue);
            this.op = op;
        }

        @Override
        void doOp(long timeoutMillis, boolean retrying) {
            doOpDDT(op, timeoutMillis, retrying);
        }
    }

    @Override
    UpdateOp<PrimaryKey, Row, TableOperation> getUpdateOp(
            long index, boolean firstThread, boolean requestPrevValue) {
        return new UpdateOpDDT(super.getUpdateOp(
                index, firstThread, requestPrevValue));
    }

    /*
     * Start long iteration over table records (by default over the whole
     * table), then issue drop table.  Iteration should fail after table is
     * dropped.
     */
    private class TableIteratorThread extends Thread {

        private volatile boolean isStopRequested;

        void requestStop() {
            isStopRequested = true;
        }

        TableIteratorOptions getTableIteratorOptions(boolean abs) {
            return DataCheckDropTable.this.getTableIteratorOptions(
                    Direction.FORWARD,
                    abs ? Consistency.ABSOLUTE : null,
                    requestTimeout,
                    TimeUnit.MILLISECONDS);
        }

        private void doRun() {
            log("TableIteratorThread: starting");

            /* TODO: try with index key, use different ranges and iterator
             * options */
            PrimaryKey pKey = table.createPrimaryKey();
            boolean absCons = true;

            while(!isStopRequested) {
                TableIterator<Row> iter = null;

                /*
                 * For better testing, we flip consistency between absolute
                 * and default for each round of iteration. We could also use
                 * random value instead.
                 */
                absCons = !absCons;
                DropState startDropState = dropState;
                try {
                    iter = tableImpl.tableIterator(pKey, null,
                            getTableIteratorOptions(absCons));
                    if (startDropState == DropState.FINISHED) {
                        if (!absCons) {
                            log("TableIteratorThread: tableIterator() " +
                                    "succeeded after table is dropped, " +
                                    "retrying with absolute consistency");
                            iter = tableImpl.tableIterator(pKey, null,
                                    getTableIteratorOptions(true));
                        }
                        unexpectedResult("TableIteratorThread: " +
                                "tableIterator() succeeded after " +
                                "table is dropped");
                        break;
                    }
                    while(iter.hasNext()) {
                        startDropState = dropState;
                        Row row = iter.next();
                        if (startDropState == DropState.FINISHED) {
                            if (absCons) {
                                PrimaryKey rowPK = row.asPrimaryKey();
                                unexpectedResult("TableIteratorThread: " +
                                        "next() succeeded after table is " +
                                        "dropped, key=%s, keynum=%s",
                                        keyString(rowPK),
                                        keyToKeynum(rowPK));
                            }

                            /*
                             * If consistency is not set to absolute, we
                             * retry with absolute consistency.  But for that
                             * we need to create new iterator, so we handle it
                             * there.
                             */
                            break;
                        }
                        if (isStopRequested) {
                            break;
                        }
                    }
                }
                catch(StoreIteratorException | FaultException |
                        MetadataNotFoundException e) {
                    if (dropState != DropState.BEFORE) {
                        log("TableIteratorThread: exception thrown " +
                                "after drop table: " + e);
                        break;
                    }
                    if (e instanceof StoreIteratorException ||
                        e instanceof FaultException) {
                        logException(e,
                                "TableIteratorThread: retrying iteration " +
                                "after exception was thrown");
                    }
                    else {
                        throw e;
                    }
                }
                finally {
                    if (iter != null)
                        iter.close();
                }
            }
        }

        @Override
        public void run() {
            try {
                doRun();
            }
            catch(Throwable e) {
                unexpectedExceptionWithContext("TableIteratorThread", e);
            }
        }

    }

    /*
     * Although currently it is not necessary to drop the table in a separate
     * thread, in future we will test with multiple tables and then we would
     * want to drop them all in parallel.
     */
    private class DropTableThread extends Thread {

        private final long timeOut;
        /* Whether this thread is invoked in clean phase */
        private final boolean cleaning;

        DropTableThread(long count, boolean cleaning)
        {
            this.timeOut = params.getDropTimeout((int)(count / BLOCK_COUNT));
            this.cleaning = cleaning;
        }

        private ExecuteOptions getExecuteOptions() {
            ExecuteOptions execOpt = new ExecuteOptions();
            execOpt.setTimeout(timeOut, TimeUnit.MILLISECONDS);
            return execOpt;
        }

        private String getDropStmt() {
            return "DROP TABLE " + (cleaning ? "IF EXISTS " : "") + tableName;
        }

        private StatementResult dropAsync(KVStore kvStore) throws Throwable {
            log("DropTableThread: using KVStore.execute(), waiting with " +
                    "timeout of " + timeOut + " ms");
            ExecutionFuture fut = kvStore.execute(getDropStmt());
            return fut.get(timeOut, TimeUnit.MILLISECONDS);
        }

        private StatementResult dropSync(KVStore kvStore) throws Throwable {
            log("DropTableThread: using KVStore.executeSync() with " +
                    "timeout of " + timeOut + " ms");
            final StatementResult [] res = new StatementResult[1];
            final Throwable [] ex = new Throwable[1];

            /*
             * Give a little extra to allow timeout set in ExecuteOptions to
             * kick in first.
             */
            final long threadTimeout = timeOut + 3000;

            /*
             * We run executeSync() in a separate thread to guard against
             * the case where it never returns.
             */
            Thread t = new Thread() {
                @Override
                public void run() {
                    try {
                        res[0] = kvStore.executeSync(getDropStmt(),
                            getExecuteOptions());
                    }
                    catch (Throwable e) {
                        ex[0] = e;
                    }
                }
            };
            t.start();
            t.join(threadTimeout);
            if (t.isAlive()) {
                throw new Exception("executeSync still running after " +
                        threadTimeout + " ms");
            }

            /*
             * Safe to access these variables because thread start/stop
             * provides memory barrier.
             */
            if(ex[0] != null) {
                throw ex[0];
            }
            return res[0];
        }

        @Override
        public void run() {
            KVStore store2 = null;
            try {
                if (!params.useSingleKVStore) {
                    store2 = KVStoreFactory.getStore(config);
                }
                if (!cleaning && params.beforeDropWaitTime > 0) {
                    Thread.sleep(params.beforeDropWaitTime);
                }

                /*
                 * This might no longer be thread-safe once we run multiple
                 * DropTableThreads.  We'll have to synchronize access to
                 * phaseThreads variable instead of just using volatile.
                 */
                if (!params.dropConcurrently && phaseThreads != null) {
                    stopPhaseThreadsSync(phaseThreads);
                    phaseThreads = null;
                }

                dropState = DropState.STARTED;
                int retryCnt = 0;
                while(true) {
                    log("DropTableThread: starting drop table");
                    long startTime = System.currentTimeMillis();
                    KVStore kvs = params.useSingleKVStore ? store : store2;
                    StatementResult res = params.dropSync ? dropSync(kvs) :
                        dropAsync(kvs);
                    if (res.isSuccessful()) {
                        /* check if we can still access the table */
                        dropState = DropState.FINISHED;
                        dropTableTime = System.currentTimeMillis() -
                                startTime;
                        log("DropTableThread : drop table succeeded in " +
                                "%d ms, info: %s",
                                dropTableTime,
                                res.getInfo());
                        if (kvs.getTableAPI().getTable(tableName) != null) {
                            log("DropTableThread: table %s can " +
                                    "still be accessed after drop table " +
                                    "statement has succeeded",
                                    tableName);
                        }
                        if (!cleaning && params.afterDropWaitTime > 0) {
                            Thread.sleep(params.afterDropWaitTime);
                        }
                        break;
                    }
                    dropState = DropState.ERROR;
                    unexpectedResult("DropTableThread: drop table " +
                            "failed, error: %s; info: %s",
                            res.getErrorMessage(),
                            res.getInfo());
                    if (retryCnt++ == params.dropRetryCnt) {
                        break;
                    }
                    log("DropTableThread: retrying after %s ms",
                            params.dropRetryWaitTime);
                    Thread.sleep(params.dropRetryWaitTime);
                }
            }
            catch(Throwable e) {
                unexpectedExceptionWithContext("DropTableThread", e);
            }
            finally {
                if (store2 != null)
                    store2.close();
            }
        }
    }

    private class ExtinctionCheckThread extends Thread {

        private final Consistency consistency;
        private final long waitTime;
        private final long waitTimeout;

        ExtinctionCheckThread(Consistency consistency, long count) {
            this.consistency = consistency;
            waitTime = params.getCheckWaitTime((int)(count / BLOCK_COUNT));
            waitTimeout = params.getCheckTimeout((int)(count / BLOCK_COUNT));
        }

        private void checkForRecordExtinction()
        {
            final long startTime = System.currentTimeMillis();

            String dcTblName = dataCheckTbl.getFullName();
            log("%s:\n starting to look for records from table %s, id=%d",
                    this, dcTblName, dataCheckTbl.getId());
            StoreIteratorConfig iterConfig = new StoreIteratorConfig();
            iterConfig.setMaxConcurrentRequests(0);
            ParallelScanIterator<KeyValueVersion> iter = null;

            while(true) {
                try {
                    log(this + ": starting iterator");
                    iter = store.storeIterator(Direction.UNORDERED,
                                    0,      /* batchSize */
                                    null,   /* parentKey */
                                    null,   /* subrange */
                                    null,   /* depth */
                                    consistency, /* consistency */
                                    0,      /* timeout */
                                    null,   /* timeoutUnit */
                                    iterConfig /* storeIteratorConfig */
                                    );
                    long cnt = 0;
                    HashSet<String> tblNames = new HashSet<String>();
                    while(iter.hasNext()) {
                        KeyValueVersion kvv = iter.next();
                        byte [] keyBytes = kvv.getKey().toByteArray();
                        TableImpl targetTbl = dataCheckTbl.findTargetTable(
                                keyBytes);
                        if (targetTbl == null) {
                            continue;
                        }
                        String targetTblName = targetTbl.getFullName();
                        /* To avoid excessive logging of found records */
                        if (!tblNames.contains(targetTblName)) {
                            log("%s:\n found record from table %s, id=%d",
                                    this, targetTblName, targetTbl.getId());
                            tblNames.add(targetTblName);
                        }
                        if (dcTblName.equals(targetTblName)) {
                            cnt++;
                        }
                    }
                    final long runTime = System.currentTimeMillis() - startTime;
                    if (cnt == 0) {
                        log("\n%s:\n extinction check complete in %d ms, " +
                                "no more records detected.", this, runTime);
                        break;
                    }
                    log("\n%s:\n found %d remaining records\n", this, cnt);
                    if (runTime > waitTimeout) {
                        unexpectedResult("%s:\n extinction check exceeded " +
                                "timeout interval of %d ms",
                                this,
                                waitTimeout);
                        break;
                    }
                    log("%s:\n waiting %d ms", this, waitTime);
                    Thread.sleep(waitTime);
                }
                catch(Throwable e) {
                    if (e instanceof FaultException ||
                        e instanceof StoreIteratorException) {
                        logException(e,
                                "%s:\n extinction check throws exception, " +
                                " will retry after %d ms",
                                this,
                                waitTime);
                        if (iter != null) {
                            iter.close();
                            iter = null;
                        }
                        try {
                            Thread.sleep(waitTime);
                        }
                        catch(InterruptedException ie) {
                        }
                    }
                    else {
                        unexpectedExceptionWithContext(toString(), e);
                        break;
                    }
                }
                finally {
                    if (iter != null) {
                        iter.close();
                        iter = null;
                    }
                }
            }
        }

        @Override
        public void run() {
            try {
                checkForRecordExtinction();
            }
            catch(Throwable e) {
                unexpectedExceptionWithContext(toString(), e);
            }
        }

        @Override
        public String toString() {
            return String.format("ExtinctionCheckThread[Id=%d,Consistency=%s]",
                    ThreadUtils.threadId(Thread.currentThread()),
                    consistency.getName());
        }

    }

    /*
     * This function has the same purpose as finish() in DataCheck class,
     * but outputs different information that is relevant to the drop table
     * test.  It omits output of performance and stats done in
     * DataCheck.finish() since it is not relevant in drop table test.
     */
    private void finish(Phase phase, long startTime) {
        long time = System.currentTimeMillis() - startTime;
        boolean passed = testCompleted();

        StringBuilder sb = new StringBuilder();
        Formatter fmt = new Formatter(sb);
        fmt.format("DataCheckDropTable test result: %s", (passed ? "Passed" :
                "Failed"));
        if (unexpectedExceptionCount.get() > 0) {
            fmt.format("\n  UNEXPECTED_EXCEPTIONS=%d",
                       unexpectedExceptionCount.get());
        }
        if (unexpectedResultCount.get() > 0) {
            fmt.format("\n  UNEXPECTED_RESULTS=%d",
                       unexpectedResultCount.get());
        }
        fmt.format("\n  timeMillis=%d", time);
        if (phase == Phase.EXERCISE) {
            fmt.format("\nDrop table time: %d ms", dropTableTime);
            fmt.format("\nOps succeded during drop table: %d",
                    succeededDuringCnt.get());
            fmt.format("\nOps failed during drop table: %d",
                    failedDuringCnt.get());
            fmt.format("\nOps failed after drop table: %d",
                    failedAfterCnt.get());
            fmt.format("\nReadOps retried with absolute consistency after " +
                    "drop table: %d",
                    retryAbsConsCnt.get());
            fmt.format("\nOps succeded after drop table(!): %d",
                    succeededAfterCnt.get());
        }
        fmt.close();
        String message = sb.toString();
        log(message);
        if (!passed) {
            throw new TestFailedException(message);
        }
    }

    /* We don't actually start terminating until drop table is finished */
    @Override
    protected void startTerminatorThread(List<PhaseThread> listOfTarget) {
        phaseThreads = listOfTarget;
    }

    /*
     * The following methods are overridden so that exercise read operations
     * can be issued with absolute consistency if needed.
     */

    @Override
    ValVer<Row> doGet(PrimaryKey key,
                      Object consistency,
                      long timeoutMillis) {
        return super.doGet(key,
                absoluteConsistency.get() ? Consistency.ABSOLUTE : consistency,
                timeoutMillis);
    }

    @Override
    Collection<Row> doMultiGet(PrimaryKey key,
                               Object consistency,
                               long timeoutMillis) {
        return super.doMultiGet(key,
                absoluteConsistency.get() ? Consistency.ABSOLUTE : consistency,
                timeoutMillis);
    }

    @Override
    Collection<PrimaryKey> doMultiGetKeys(PrimaryKey key,
                                          Object consistency,
                                          long timeoutMillis) {
        return super.doMultiGetKeys(key,
                absoluteConsistency.get() ? Consistency.ABSOLUTE : consistency,
                timeoutMillis);
    }

    @Override
    TableIteratorOptions getTableIteratorOptions(Direction direction,
                                                 Consistency consistency,
                                                 long timeout,
                                                 TimeUnit timeoutUnit) {
        return super.getTableIteratorOptions(direction,
                absoluteConsistency.get() ? Consistency.ABSOLUTE : consistency,
                timeout,
                timeoutUnit);
    }

    /* Once the table is dropped, we only need to rename the markers */
    private class CleanThread extends
        DataCheck<PrimaryKey, Row, TableOperation>.CleanThread {

        CleanThread(long start, long count) {
            super((start/BLOCK_COUNT)*BLOCK_COUNT, count, 0);
        }

        @Override
        public void run() {
            try {
                for (long index = start; index < max; index += BLOCK_COUNT) {
                    if (isStopRequested) {
                        break;
                    }
                    setIndex(index);
                    if (verbose >= VERBOSE) {
                        log("%s: Renaming markers at index=%#x", getName(),
                            index);
                    }

                    long keynum = indexToKeynum(index);
                    long parent = (keynum >> PARENT_KEY_SHIFT) &
                            PARENT_KEY_MASK;
                    renameMarkers(parent);
                }
                log("%s: Renaming markers completed", getName());
            } catch (RuntimeException e) {
                Error error = new AssertionError(e.toString());
                error.initCause(e);
                unexpectedException(error);
            } catch (Error e) {
                unexpectedException(e);
            }
        }
    }

    /**
     * DataCheckDropTable exercise phase will do the following:
     * 1) Start regular exercise phase in a separate thread (if
     * params.runDatacheckExercise is true).
     * 2) Start TableIteratorThread and DropTableThread.
     * 3) DropTableThread will sleep for predefined time interval before
     * dropping the table.
     * 4) Join DropTableThread.
     * 5) If table dropped successfully, wait predefined time interval and
     * stop the phase threads and TableIteratorThread, we are done.
     * If drop table failed, stop these threads immediately since the test has
     * failed.
     * 6) In either case, join the thread running the regular exercise phase.
     */

    @Override
    public void exercise(long start,
            long count,
            int threads,
            double readPercent,
            boolean noCheck) {

        long startTime = System.currentTimeMillis();
        try {
            log("DataCheckDropTable: starting exercise phase");
            saveDataCheckTbl();

            Thread exerciseThread = params.runDatacheckExercise ?
                    new Thread() {
                @Override
                public void run() {
                    DataCheckDropTable.super.exercise(start, count, threads,
                            readPercent, noCheck);
                }
            } : null;
            TableIteratorThread tblIterThread = params.runTableIterator ?
                    new TableIteratorThread() : null;
            DropTableThread dropTblThread = new DropTableThread(count, false);

            if (exerciseThread != null) {
                exerciseThread.start();
            }
            else { /* If testState was not set by the DataCheck.exercise() */
                setTestStateActive();
            }
            if (tblIterThread != null) {
                tblIterThread.start();
            }
            dropTblThread.start();
            dropTblThread.join();

            if (tblIterThread != null) {
                tblIterThread.requestStop();
            }
            if (phaseThreads != null) {
                stopPhaseThreadsSync(phaseThreads);
                phaseThreads = null;
            }
            if (tblIterThread != null) {
                tblIterThread.join();
            }
            if (exerciseThread != null) {
                exerciseThread.join();
            }
        }
        catch(Throwable e) {
            unexpectedExceptionWithContext("DataCheckDropTable exercise phase",
                    e);
        }
        finally {
            finish(Phase.EXERCISE, startTime);
        }
    }

    /**
     * DataCheckDropTable check phase will only check for the record
     * extinction.  Arguments start and threads are ignored.
     */

    @SuppressWarnings("deprecation")
    @Override
    public void check(long start, long count, int threads) {
        long startTime = System.currentTimeMillis();
        try {
            log("DataCheckDropTable: starting check phase");
            setTestStateActive();

            /*
             * The table is dropped in the exercise phase and should no longer
             * exist in the check phase.
             */
            if (tableImpl.getTable(tableName) != null) {
                unexpectedResult("Table %s still exists in check phase",
                        tableName);
            }
            loadDataCheckTbl();

            ExtinctionCheckThread checkNoCons = new ExtinctionCheckThread(
                    Consistency.NONE_REQUIRED,
                    count);
            ExtinctionCheckThread checkAbsCons = new ExtinctionCheckThread(
                    Consistency.ABSOLUTE,
                    count);
            ExtinctionCheckThread checkNoMaster = new ExtinctionCheckThread(
                    Consistency.NONE_REQUIRED_NO_MASTER,
                    count);

            checkNoCons.start();
            checkAbsCons.start();
            checkNoMaster.start();
            checkNoCons.join();
            checkAbsCons.join();
            checkNoMaster.join();
        }
        catch(Throwable e) {
            unexpectedExceptionWithContext("DataCheckDropTable check phase",
                    e);
        }
        finally {
            finish(Phase.CHECK, startTime);
        }
    }

    @Override
    public void clean(long start, long count, int threads) {
        long startTime = System.currentTimeMillis();
        try {
            log("DataCheckDropTable: starting clean phase");
            setTestStateActive();
            DropTableThread dropTblThread = new DropTableThread(count, true);
            loadDataCheckTbl();
            ExtinctionCheckThread checkThread = new ExtinctionCheckThread(
                    Consistency.NONE_REQUIRED,
                    count);
            CleanThread cleanThread = new CleanThread(start, count);
            dropTblThread.start();
            checkThread.start();
            cleanThread.start();
            dropTblThread.join();
            checkThread.join();
            cleanThread.join();

            removeStoreSeed();
        }
        catch(Throwable e) {
            unexpectedExceptionWithContext("DataCheckDropTable clean phase",
                    e);
        }
        finally {
            finish(Phase.CLEAN, startTime);
        }
    }

}
