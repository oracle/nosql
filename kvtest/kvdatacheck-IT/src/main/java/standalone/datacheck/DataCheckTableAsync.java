/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import oracle.kv.ConsistencyException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.stats.KVStats;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.TableOperation;

import standalone.datacheck.AsyncOp.AsyncExecuteOp;
import standalone.datacheck.AsyncOp.AsyncGetOp;
import standalone.datacheck.AsyncOp.AsyncIndexIteratorOp;
import standalone.datacheck.AsyncOp.AsyncIndexKeysIteratorOp;
import standalone.datacheck.AsyncOp.AsyncIteratorOp;
import standalone.datacheck.AsyncOp.AsyncKeysIteratorOp;
import standalone.datacheck.AsyncOp.AsyncMultiGetIteratorCheckOp;
import standalone.datacheck.AsyncOp.AsyncMultiGetKeysOp;
import standalone.datacheck.AsyncOp.AsyncMultiGetOp;
import standalone.datacheck.AsyncOp.AsyncReadOp;
import standalone.datacheck.AsyncOp.AsyncUpdateOp;
import standalone.datacheck.AsyncOp.OpStatus;

/**
 * Implement phase threads for data checking used by {@link DataCheckMain}.
 * This class is for threads checking async style functions for TableAPI.
 */
public class DataCheckTableAsync extends DataCheckTableDirect {

    /**
     * The names of store non-index-iteration read operations that should be
     * called in the exercise phase if reads are performed.
     */
    private static final Set<String> EXERCISE_READ_OPCODE_NAMES =
        Stream.of(OpCode.GET,
                  OpCode.MULTI_GET_TABLE,
                  OpCode.MULTI_GET_TABLE_KEYS,
                  OpCode.TABLE_ITERATE,
                  OpCode.TABLE_KEYS_ITERATE)
        .map(KVStats::getOpCodeName)
        .collect(Collectors.toSet());

    /**
     * The names of store index-iteration read operations that should be called
     * in the exercise phase if index iteration reads are performed.
     */
    private static final Set<String> EXERCISE_INDEX_ITER_READ_OPCODE_NAMES =
        Stream.of(OpCode.INDEX_ITERATE,
                  OpCode.INDEX_KEYS_ITERATE)
        .map(KVStats::getOpCodeName)
        .collect(Collectors.toSet());

    /**
     * The names of store update operations that should be called in the
     * exercise phase if updates are performed.
     */
    private static final Set<String> EXERCISE_UPDATE_OPCODE_NAMES =
        Stream.of(OpCode.DELETE,
                  OpCode.DELETE_IF_VERSION,
                  OpCode.EXECUTE,
                  OpCode.MULTI_DELETE_TABLE,
                  OpCode.PUT,
                  OpCode.PUT_IF_ABSENT,
                  OpCode.PUT_IF_PRESENT,
                  OpCode.PUT_IF_VERSION)
        .map(KVStats::getOpCodeName)
        .collect(Collectors.toSet());

    /**
     * A list of AsyncUpdateOpType(s) selected so that they will create no net
     * increase or decrease on the number of entries if chosen randomly.
     * All these types are also subclasses of UpdateOpType.
     */
    private final List<AsyncUpdateOpType<?,?>>
        asyncUpdateOpTypes = Arrays.asList(
            new AsyncUpdateOpType.AsyncPutOpType(),
            new AsyncUpdateOpType.AsyncDeleteOpType(),
            new AsyncUpdateOpType.AsyncPutExecuteOpType(),
            new AsyncUpdateOpType.AsyncDeleteExecuteOpType(),
            new AsyncUpdateOpType.AsyncPutIfAbsentOpType(),
            new AsyncUpdateOpType.AsyncDeleteIfVersionOpType(),
            new AsyncUpdateOpType.AsyncPutIfAbsentExecuteOpType(),
            new AsyncUpdateOpType.AsyncDeleteIfVersionExecuteOpType(),
            new AsyncUpdateOpType.AsyncTwoPutExecuteOpType(),
            new AsyncUpdateOpType.AsyncMultiDeleteOpType(),

            /*
             * The remaining operations have no effect on the number of
             * entries.
             */
            new AsyncUpdateOpType.AsyncPutIfPresentOpType(),
            new AsyncUpdateOpType.AsyncPutIfVersionOpType(),

            /* Pairs of these to fill out the 16 choices */
            new AsyncUpdateOpType.AsyncPutIfPresentExecuteOpType(),
            new AsyncUpdateOpType.AsyncPutIfVersionExecuteOpType(),
            new AsyncUpdateOpType.AsyncPutIfPresentExecuteOpType(),
            new AsyncUpdateOpType.AsyncPutIfVersionExecuteOpType());

    public DataCheckTableAsync(KVStore store,
                               KVStoreConfig config,
                               long reportingInterval,
                               int[] partitions,
                               long seed,
                               int verbose,
                               PrintStream err,
                               long blockTimeout,
                               boolean useParallelScan,
                               int scanMaxConcurrentRequests,
                               int scanMaxResultsBatches,
                               int indexReadPercent,
                               double maxThroughput,
                               int maxExecTime,
                               boolean useTTL,
                               String adminHostName,
                               String adminPortName) {
        super(store, config, reportingInterval, partitions, seed, verbose, err,
              blockTimeout, useParallelScan, scanMaxConcurrentRequests,
              scanMaxResultsBatches, indexReadPercent,
              maxThroughput, maxExecTime, useTTL, adminHostName,
              adminPortName);
    }

    static void awaitLatch(CountDownLatch latch, PhaseThread phase) {
        while (!phase.isStopRequested) {
            try {
                latch.await();
                break;
            } catch (InterruptedException ex) {
                // Nothing to do currently.
            }
        }
    }

    @Override
    protected PhaseThread createPopulateThread(long start,
                                               long count,
                                               double targetThrptMs) {
        return new AsyncPopulateThread(start, count, targetThrptMs);
    }

    class AsyncPopulateThread extends PopulateThread {

        AsyncPopulateThread(long start, long count, double targetThrptMs) {
            super(start, count, targetThrptMs);
        }

        @Override
        protected void executeOps() {
            CountDownLatch latch = new CountDownLatch(1);
            AsyncOp<?,?> asyncOp = new AsyncExecuteOp(DataCheckTableAsync.this,
                                                      operations, latch);
            asyncOp.perform();
            awaitLatch(latch, this);
            opsDone += operations.size();
            operations.clear();
            throttle();
            checkOpStatus(asyncOp, this);
        }
    }

    @Override
    protected PhaseThread createCleanThread(long start,
                                            long count,
                                            double targetThrptMs) {
        return new AsyncCleanThread(start, count, targetThrptMs);
    }

    class AsyncCleanThread extends CleanThread {

        AsyncCleanThread(long start, long count, double targetThrptMs) {
            super(start, count, targetThrptMs);
        }

        /*
         * Here, we execute all operations as a whole asynchronously, and wait
         * them to finish.
         */
        @Override
        protected void executeOps() {
            CountDownLatch latch = new CountDownLatch(1);
            AsyncOp<?,?> asyncOp = new AsyncExecuteOp(DataCheckTableAsync.this,
                                                      operations, latch);
            asyncOp.perform();
            awaitLatch(latch, this);
            opsDone += operations.size();
            operations.clear();
            throttle();
            checkOpStatus(asyncOp, this);
        }
    }

    @Override
    protected PhaseThread createCheckThread(long start,
                                            long count,
                                            double targetThrptMs) {
        return new AsyncCheckThread(start, count, targetThrptMs);
    }

    class AsyncCheckThread extends CheckThread {

        AsyncCheckThread(long start, long count, double targetThrptMs) {
            super(start, count, targetThrptMs);
        }

        @Override
        void runOp(Op<PrimaryKey, Row, TableOperation> op) {
            /* defensive check */
            if (op instanceof AsyncOp<?,?>) {
                AsyncOp<?,?> asyncOp = (AsyncOp<?,?>) op;
                asyncOp.perform();
                awaitLatch(asyncOp.getLatch(), this);
                opsDone++;
                throttle();
                checkOpStatus(asyncOp, this);
            } else {
                super.runOp(op);
            }
        }
    }

    @Override
    protected PhaseThread createExerciseThread(long threadStart,
                                               long threadCount,
                                               boolean firstThread,
                                               CyclicBarrier barrier,
                                               AtomicLong exerciseIndex,
                                               AtomicLong otherExerciseIndex,
                                               double readPercent,
                                               double targetThrPerMs) {
        return new AsyncExerciseThread(threadStart, threadCount, firstThread,
                                       barrier, exerciseIndex,
                                       otherExerciseIndex, readPercent,
                                       targetThrPerMs);
    }

    class AsyncExerciseThread extends ExerciseThread {
        private CountDownLatch endLatch;
        private RuntimeException ex = null;
        private AsyncOp<?,?> lastOp = null;
        private int curStack = 0;

        AsyncExerciseThread(long start,
                            long count,
                            boolean firstThread,
                            CyclicBarrier barrier,
                            AtomicLong exerciseIndex,
                            AtomicLong otherExerciseIndex,
                            double readPercent,
                            double targetThrptMs) {
            super(start, count, firstThread, barrier, exerciseIndex,
                  otherExerciseIndex, readPercent, targetThrptMs);
        }

        public void next() {
            AtomicLong curIndex = getIndex();
            /*
             * The checkOpStatus will only throw exception when it requires the
             * exercise thread to exit, and it will throw TestExitException. We
             * just record the exception, since current function may not be
             * running inside an exercise thread. We will let exercise thread
             * check it and quit if necessary.
             */
            try {
                if (lastOp != null) {
                    checkOpStatus(lastOp, this);
                    lastOp = null;
                }

                curIndex.incrementAndGet();

                if (curIndex.get() >= max || isStopRequested) {
                    if (firstThread && curIndex.get() >= max) {
                        completedBlocks.incrementAndGet();
                    }
                    releaseLatch();
                    return;
                }

                curStack++;
                if (curStack > BLOCK_COUNT) {
                    releaseLatch();
                    return;
                }

                boolean hasWork = false;
                try {
                    hasWork = performExerciseOp(curIndex.get());
                } catch (Exception e) {
                    Error error = new AssertionError(e.toString());
                    error.initCause(e);
                    unexpectedException(error);
                    requestStop();
                    releaseLatch();
                    return;
                } catch (Error e) {
                    unexpectedException(e);
                    requestStop();
                    releaseLatch();
                    return;
                }

                /*
                 * If the async operation does not execute, the next 'index'
                 * will not be operated. Call it directly then.
                 */
                if (!hasWork) {
                    lastOp = null;
                    next();
                }
            } catch (RuntimeException rex) {
                if (ex == null) {
                    ex = rex;
                }
                releaseLatch();
            }
        }

        private void releaseLatch() {
            /* Defensive check */
            if (endLatch != null) {
                endLatch.countDown();
            }
        }

        @Override
        public void run() {
            for (long index1 = start - 1; index1 < max - 1;
                index1 += BLOCK_COUNT) {
                if (isStopRequested) {
                    break;
                }
                ex = null;
                curStack = 0;
                endLatch = new CountDownLatch(1);
                setIndex(index1);
                next();
                awaitLatch(endLatch, this);
                /*
                 * If there is an exception finally, it means current thread
                 * should quit, so throw it.
                 */
                if (ex != null) {
                    throw ex;
                }
            }
        }

        /*
         * The next action should be set outside.
         */
        private void runOp(AsyncOp<?,?> op) {
            op.perform();
            opsDone++;
            throttle();
        }

        private class OpPair {
            AsyncOp<?,?> firstOp = null;
            AsyncOp<?,?> secondOp = null;
        }

        private void AddOp(OpPair pair, final AsyncOp<?,?> curOp) {
            if (curOp == null)
                return;

            if (pair.firstOp == null) {
                pair.firstOp = curOp;
            }

            if (pair.secondOp == null) {
                pair.secondOp = curOp;
            } else {
                AsyncOp<?,?> prevOp = pair.secondOp;
                prevOp.setNext(() -> {
                    try {
                        /* Check previous result before doing next operation. */
                        if (lastOp != null) {
                            checkOpStatus(lastOp, this);
                        }
                        lastOp = curOp;
                        runOp(curOp);
                    } catch (RuntimeException rex) {
                        if (ex == null) {
                            ex = rex;
                        }
                        releaseLatch();
                    }
                });
                pair.secondOp = curOp;
            }
        }

        private boolean performExerciseOp(long index1)
            throws InterruptedException {
            if ((index1 % BLOCK_COUNT) == 0) {
                startExerciseBlock(index1);
            }

            /*
             * Note that the count field specifies the number of update
             * operations, independent of read operations specified by
             * readRatio, to make sure that the check phase finds that the
             * expected set of update operations have been performed.  An
             * exception is when a read percentage of 100 is requested, which
             * means that only read operations will be performed, and the store
             * will be unchanged from the populate phase.
             */
            if (readRatio == Double.POSITIVE_INFINITY) {
                /* All reads */
                AsyncReadOp<?> op =
                    getAsyncReadOp(index1, firstThread, threadRandom,
                                   this.otherExerciseIndex);
                if (op != null) {
                    op.setNext(this::next);
                    lastOp = op;
                    runOp(op);
                    return true;
                }
                return false;
            }

            if (partitions != null) {
                long keynum = exerciseIndexToKeynum(index1, firstThread);
                if (!keynumInPartitions(keynum)) {
                    return false;
                }
            }

            /* The number of reads for each write if one or greater */
            int readCount = (int) readRatio;
            double readCountRemainder = readRatio - readCount;

            /*
             * Treat the fractional portion of the readRatio as a probability
             * of including a read for a given write.  For example, a read
             * percentage of 68 converts to a read ratio of 68.0/32.0 = 2.125.
             * The read count gets set to 2.  Then, with a probability of
             * 0.125, increase the read count to 3, to account for the
             * fractional portion.
             */
            if (threadRandom.nextDouble() < readCountRemainder) {
                readCount++;
            }

            OpPair pair = new OpPair();
            for (int i = 0; i < readCount; i++) {
                AsyncReadOp<?> op = getAsyncReadOp(index1, firstThread,
                                                   threadRandom,
                                                   this.otherExerciseIndex);
                AddOp(pair, op);
            }

            AsyncOp<?,?> op = getAsyncUpdateOp(index1, firstThread,
                                               threadRandom.nextBoolean(),
                                               this.otherExerciseIndex);
            AddOp(pair, op);
            if (pair.firstOp != null) {
                pair.secondOp.setNext(this::next);
                lastOp = pair.firstOp;
                runOp(pair.firstOp);
                return true;
            }
            return false;
        }
    }

    @Override
    Op<PrimaryKey, Row, TableOperation> getCheckOp(long index) {
        CountDownLatch latch = new CountDownLatch(1);
        return new AsyncMultiGetIteratorCheckOp(this, latch, index);
    }

    @Override
    void checkMissingOps(Map<String, Long> exerciseOpCounts) {
        final Set<String> missing = new HashSet<>();
        if (exerciseReadPercent > 0.0) {
            EXERCISE_READ_OPCODE_NAMES.stream()
                .filter(Predicate.not(exerciseOpCounts::containsKey))
                .forEach(missing::add);
            if (indexReadPercent > 0) {
                EXERCISE_INDEX_ITER_READ_OPCODE_NAMES.stream()
                    .filter(Predicate.not(exerciseOpCounts::containsKey))
                    .forEach(missing::add);
            }
        }
        if (exerciseReadPercent < 100.0) {
            EXERCISE_UPDATE_OPCODE_NAMES.stream()
                .filter(Predicate.not(exerciseOpCounts::containsKey))
                .forEach(missing::add);
        }
        if (!missing.isEmpty()) {
            unexpectedResult("Missing operations in exercise phase: " +
                             missing);
        }
    }

    /** Get a random read operation for the specified operation index. */
    private AsyncReadOp<?> getAsyncReadOp(long index,
                                          boolean firstThread,
                                          Random threadRandom,
                                          AtomicLong otherExerciseIndex) {
        long keynum = getRandomKeynum(index, threadRandom);
        if (partitions != null && !keynumInPartitions(keynum)) {
            return null;
        }
        if (threadRandom.nextInt(1000) < 999) {
            if (chooseIndexReadOp(threadRandom)) {
                return getAsyncIndexReadOp(threadRandom, keynum, index,
                                           firstThread, otherExerciseIndex);
            }
            return getAsyncTableReadOp(threadRandom, keynum, index,
                                       firstThread, otherExerciseIndex);
        } else if (threadRandom.nextBoolean()) {
            return new AsyncIteratorOp(this, index, firstThread, keynum,
                                       otherExerciseIndex);
        } else {
            return new AsyncKeysIteratorOp(this, index, firstThread, keynum,
                                           otherExerciseIndex);
        }
    }

    /** Get index read operation. */
    private AsyncReadOp<?> getAsyncIndexReadOp(Random rand,
                                               long keynum,
                                               long index,
                                               boolean firstThread,
                                               AtomicLong otherExerciseIndex) {

        if (rand.nextBoolean()) {
            return new AsyncIndexIteratorOp(this, index, firstThread, keynum,
                                            otherExerciseIndex);
        }
        return new AsyncIndexKeysIteratorOp(this, index, firstThread, keynum,
                                            otherExerciseIndex);
    }

    /** Get table read operation. */
    private AsyncReadOp<?> getAsyncTableReadOp(Random rand,
                                               long keynum,
                                               long index,
                                               boolean firstThread,
                                               AtomicLong otherExerciseIndex) {

        int val = rand.nextInt(3);
        switch (val) {
            case 0:
                return new AsyncGetOp(this, index, firstThread, keynum,
                                      otherExerciseIndex);
            case 1:
                return new AsyncMultiGetOp(this, index, firstThread, keynum,
                                           otherExerciseIndex);
            case 2:
                return new AsyncMultiGetKeysOp(this, index, firstThread,
                                               keynum, otherExerciseIndex);
            default:
                throw new AssertionError("Unexpected op: " + rand);
        }
    }

    /** Get the update operation type for the specified exercise operation. */
    @Override
    UpdateOpType<PrimaryKey, Row, TableOperation>
        getUpdateOpType(long index, boolean firstThread) {
        return getAsyncUpdateOpType(index, firstThread);
    }

    private AsyncUpdateOpType<?,?> getAsyncUpdateOpType(long index,
                                                        boolean firstThread) {
        long permutedIndex = getPermutedIndex(index, firstThread);
        return asyncUpdateOpTypes.get((int) (permutedIndex %
            asyncUpdateOpTypes.size()));
    }

    /** Get the update operation for the specified exercise operation. */
    private AsyncUpdateOp<?,?> getAsyncUpdateOp(long index,
                                                boolean firstThread,
                                                boolean requestPrevValue,
                                                AtomicLong otherExerciseIndex) {
        AsyncUpdateOpType<?, ?> opType = getAsyncUpdateOpType(index,
                                                              firstThread);
        return opType.getUpdateOp(this, index, firstThread, requestPrevValue,
                                  otherExerciseIndex);
    }

    private void checkOpStatus(AsyncOp<?,?> op, PhaseThread p)
        throws RuntimeException {
        OpStatus status = op.getStatus();
        if (status == OpStatus.EXCEPTION || status == OpStatus.TIMEOUT) {
            Throwable e = op.getException();
            if ((e instanceof RuntimeException) ||
                (e instanceof ConsistencyException)) {
                p.unexpectedException(e);
            }
        }
    }
}
