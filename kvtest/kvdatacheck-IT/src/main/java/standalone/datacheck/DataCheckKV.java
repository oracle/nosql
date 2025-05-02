/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Formatter;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import oracle.kv.BulkWriteOptions;
import oracle.kv.Consistency;
import oracle.kv.ConsistencyException;
import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.Key;
import oracle.kv.KeyValue;
import oracle.kv.KeyValueVersion;
import oracle.kv.Operation;
import oracle.kv.OperationExecutionException;
import oracle.kv.OperationFactory;
import oracle.kv.OperationResult;
import oracle.kv.ParallelScanIterator;
import oracle.kv.ReturnValueVersion;
import oracle.kv.ReturnValueVersion.Choice;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.lob.InputStreamVersion;
import oracle.kv.lob.PartialLOBException;
import oracle.kv.stats.KVStats;

import standalone.datacheck.UpdateOpType.AbstractUpdateOpType;
import standalone.datacheck.UpdateOpType.BulkPut;
import standalone.datacheck.UpdateOpType.Delete;
import standalone.datacheck.UpdateOpType.Put;
import standalone.datacheck.UpdateOpType.PutIfAbsent;
import standalone.datacheck.UpdateOpType.PutIfPresent;

/** Implement the API for data checking used by {@link DataCheckMain}.
 *  This class is for checking standard Key/Value entries.
 */
public class DataCheckKV extends DataCheck<Key, Value, Operation> {

    /** LOB suffix. */
    private static final String LOB_SUFFIX = ".lob";

    /* Update operation types */
    /* 50% chance of adding an entry */
    private static final UpdateOpType<Key, Value, Operation> BULK_PUT =
        new BulkPut<>();

    /**
     * The store operations that should be called in the exercise phase if
     * reads are performed.
     */
    private static final Set<String> EXERCISE_READ_OPCODE_NAMES =
        Stream.of(OpCode.GET,
                  OpCode.MULTI_GET,
                  OpCode.MULTI_GET_BATCH,
                  OpCode.MULTI_GET_BATCH_KEYS,
                  OpCode.MULTI_GET_ITERATE,
                  OpCode.MULTI_GET_KEYS,
                  OpCode.MULTI_GET_KEYS_ITERATE,
                  OpCode.STORE_ITERATE,
                  OpCode.STORE_KEYS_ITERATE)
        /* Convert to names */
        .map(KVStats::getOpCodeName).collect(Collectors.toSet());

    /**
     * The store operations that should be called in the exercise phase if
     * updates are performed.
     */
    private static final Set<String> EXERCISE_UPDATE_OPCODE_NAMES =
        Stream.of(OpCode.DELETE,
                  OpCode.DELETE_IF_VERSION,
                  OpCode.EXECUTE,
                  OpCode.MULTI_DELETE,
                  OpCode.PUT,
                  OpCode.PUT_BATCH,
                  OpCode.PUT_IF_ABSENT,
                  OpCode.PUT_IF_PRESENT,
                  OpCode.PUT_IF_VERSION)
        /* Convert to names */
        .map(KVStats::getOpCodeName).collect(Collectors.toSet());

    /* For LOB operation types */
    private final UpdateOpType<Key, Value, Operation> PUT_LOB = new PutLOB();
    private final UpdateOpType<Key, Value, Operation> PUT_LOB_IF_ABSENT =
        new PutLOBIfAbsent();
    private final UpdateOpType<Key, Value, Operation> PUT_LOB_IF_PRESENT =
        new PutLOBIfPresent();
    private final UpdateOpType<Key, Value, Operation> APPEND_LOB =
        new AppendLOB();
    private final UpdateOpType<Key, Value, Operation> DELETE_LOB =
        new DeleteLOB();

    private final OperationFactory factory;

    /** The LOB chunk timeout, in milliseconds. */
    private final long lobChunkTimeout;

    /** The LOB operation timeout, in milliseconds. */
    private final long lobOpTimeout;

    /**
     * Permutation for making a keynum another random number, which will be
     * used in determing whether the keynum is a LOB key.
     */
    private final Permutation permuteLOBKeynum;

    /* We tally the LOB operations and data size separately. */

    /** The number of putLOB operations. */
    private final AtomicLong putLOBCount = new AtomicLong();

    /** The total number of key bytes for putLOB operations. */
    private final AtomicLong putLOBKeyBytes = new AtomicLong();

    /** The total number of value bytes for putLOB operations. */
    private final AtomicLong putLOBValueBytes = new AtomicLong();

    /** The number of appendLOB operations. */
    private final AtomicLong appendLOBCount = new AtomicLong();

    /** The total number of key bytes for appendLOB operations. */
    private final AtomicLong appendLOBKeyBytes = new AtomicLong();

    /** The total number of value bytes for appendLOB operations. */
    private final AtomicLong appendLOBValueBytes = new AtomicLong();

    /* Whether to perform LOB operation during the testing. */
    private boolean useLOBs = false;

    /* The ratio of LOB operations in all operations. */
    private double lobRatio = 0.0;

    /* LOB value size, default to 1M bytes. */
    long lobSize = 0x100000;

    /** The readPercent value specified in the last exercise call. */
    private volatile double exerciseReadPercent;

    /*
     * A list of UpdateOpTypes selected so that they will create no net
     * increase or decrease on the number of entries if chosen randomly.
     */
    {
        Collections.addAll(
            updateOpTypes,

            /*
             * In each of the following pairs of operations, the first adds,
             * and the second removes, an entry half of the time by default, so
             * they cancel out.
             */
            PUT,
            DELETE,
            PUT_EXECUTE,
            DELETE_EXECUTE,
            PUT_IF_ABSENT,
            DELETE_IF_VERSION,
            PUT_IF_ABSENT_EXECUTE,
            DELETE_IF_VERSION_EXECUTE,
            BULK_PUT,
            MULTI_DELETE,

            /*
             * The remaining operations have no effect on the number of
             * entries.
             */
            PUT_IF_PRESENT,
            PUT_IF_VERSION,

            /* Pairs of these to fill out the 16 choices */
            PUT_IF_PRESENT_EXECUTE,
            PUT_IF_VERSION_EXECUTE,
            PUT_IF_PRESENT_EXECUTE,
            PUT_IF_VERSION_EXECUTE);
    }

    /**
     * A list of UpdateOpTypes for LOB operation selected, which corresponds
     * the updateOpTypes array. If a LOB keynum is encountered, we choose
     * UpdateOpType from this array using the same operation type index,
     * otherwise we use the above array.
     */
    private final List<UpdateOpType<Key, Value, Operation>> updateLOBOpTypes =
        Arrays.asList(
            PUT_LOB,
            DELETE_LOB,
            PUT_LOB_IF_ABSENT,
            DELETE_LOB,

            /*
             * The remaining operations have no effect on the number of
             * entries.
             */
            PUT_LOB_IF_PRESENT,
            APPEND_LOB);

    /* Constructor */

    /**
     * Creates an instance of this class.  Use a new instance for each test
     * phase.
     *
     * @param store the store
     * @param config the configuration used to create the store
     * @param reportingInterval the interval in milliseconds between reporting
     *        progress and status of the store
     * @param partitions limit operations to keys falling in the specified
     *        partitions, or null for no restriction
     * @param seed the random seed, or -1 to generate a seed randomly
     * @param verbose the verbose level, with increasing levels of verbosity
     *        for values beyond {@code 0}
     * @param err the error output stream
     * @param blockTimeout the timeout in milliseconds for a set of operations
     *        on a block of entries
     * @param lobOpTimeout the timeout in milliseconds for a complete LOB
     *        operation
     * @param lobPercent the percentage of LOB operations
     * @param useParallelScan use parallel scan in exercise phase if specified.
     * @param scanMaxConcurrentRequests the max concurrent threads are allowed
     *        for a parallel scan.
     * @param scanMaxResultsBatches the max batches to temporally store the
     *        parallel scan results, each batch can store one scan results.
     * @param maxThroughput max throughput for the client.
     * @param maxExecTime the max execution for the client workload.
     * @throws IllegalArgumentException if {@code lobPercent} is out of the
     *         range of [0.0, 100.0], or {@code lobOpTimeout} is negative.
     */
    public DataCheckKV(KVStore store,
                       KVStoreConfig config,
                       long reportingInterval,
                       int[] partitions,
                       long seed,
                       int verbose,
                       PrintStream err,
                       long blockTimeout,
                       long lobOpTimeout,
                       double lobPercent,
                       boolean useParallelScan,
                       int scanMaxConcurrentRequests,
                       int scanMaxResultsBatches,
                       double maxThroughput,
                       int maxExecTime) {
        super(store, config, reportingInterval, partitions, seed, verbose, err,
              config.getRequestTimeout(MILLISECONDS), blockTimeout,
              useParallelScan, scanMaxConcurrentRequests, scanMaxResultsBatches,
              maxThroughput, maxExecTime);
        factory = store.getOperationFactory();

        lobChunkTimeout = config.getLOBTimeout(MILLISECONDS);
        if (lobOpTimeout < 0) {
            throw new IllegalArgumentException(
                "LOB operation timeout value must not be less that 0 :" +
                lobOpTimeout);
        }

        /*
         * Retry timeout value for LOB operation. The value is set to sum of
         * the LOBtimeout of all chunks in a LOB value. Given the default chunk
         * size of 128k and the LOB size of 1M, the retry timeout is (default
         * chunk timeout) * 8 chunks. If LOBOpTime is set to O, use the default
         * value, otherwise the given value.
         */
        this.lobOpTimeout = lobOpTimeout == 0 ?
                            lobChunkTimeout * 8 :
                            lobOpTimeout;
        if (lobPercent < 0.0 || lobPercent > 100.0) {
            throw new IllegalArgumentException(
                "lobPercent must be between 0.0 and 100.0: " + lobPercent);
        }

        /* Compute LOB ratio, if > 0, set LOB flag as true. */
        this.lobRatio = lobPercent * 0.01;
        if (lobRatio > 0.0) {
            useLOBs = true;
        }

        initializeRandom(seed);
        permuteLOBKeynum = new Permutation(random.nextLong());
    }

    /* Phases */

    /** Create exercise thread. */
    @Override
    protected PhaseThread createExerciseThread(long threadStart,
                                long threadCount,
                                boolean firstThread,
                                CyclicBarrier barrier,
                                AtomicLong exerciseIndex,
                                AtomicLong otherExerciseIndex,
                                double readPercent,
                                double targetThrPerMs) {
        return new ExerciseThreadEx(threadStart, threadCount, firstThread,
            barrier, exerciseIndex, otherExerciseIndex, readPercent,
                targetThrPerMs);
    }

    /** Record readPercent */
    @Override
    public void exercise(long start,
                         long count,
                         int threads,
                         double readPercent,
                         boolean noCheck,
                         DataCheck<Key, Value, Operation> pairDc) {
        exerciseReadPercent = readPercent;
        super.exercise(start, count, threads, readPercent, noCheck, pairDc);
    }

    /** Get a empty value. */
    @Override
    Value getEmptyValue() {
        return Value.EMPTY_VALUE;
    }

    /** Check if the given value equals to the expected value. */
    @Override
    boolean valuesEqual(Value value, Value expected) {
        return value.equals(expected);
    }

    /** Get the statistics of LOB operations. */
    @Override
    String getStatInfoEx() {
        /* Statistics of LOB operations. */
        long lobCount = putLOBCount.get();
        double lobKeySize = 0;
        double lobValueSize = 0;
        if (lobCount > 0) {
            lobKeySize = ((double) putLOBKeyBytes.get()) / lobCount;
            lobValueSize = ((double) putLOBValueBytes.get()) / lobCount;
        }

        long lobAppendCount = appendLOBCount.get();
        double lobAppendKeySize = 0;
        double lobAppendValueSize = 0;
        if (lobAppendCount > 0) {
            lobAppendKeySize =
                ((double) appendLOBKeyBytes.get()) / lobAppendCount;
            lobAppendValueSize =
                ((double) appendLOBValueBytes.get()) / lobAppendCount;
        }

        StringBuilder sb = new StringBuilder();
        Formatter fmt = new Formatter(sb);
        if (lobCount > 0) {
            fmt.format("\n  putLOBCount=%d", lobCount);
            fmt.format("\n  LOBKeySize=%.1f", lobKeySize);
            fmt.format("\n  LOBValueSize=%.1f", lobValueSize);
        }

        if (lobAppendCount > 0) {
            fmt.format("\n  appendLOBCount=%d", lobAppendCount);
            fmt.format("\n  AppendLOBKeySize=%.1f", lobAppendKeySize);
            fmt.format("\n  AppendLOBValueSize=%.1f", lobAppendValueSize);
        }
        fmt.close();
        return sb.toString();
    }

    /** Check for expected store operations */
    @Override
    Map<String, Long> getPhaseOpCounts(Phase phase,
                                       ReportingThread reportingThread) {
        final Map<String, Long> counts =
            super.getPhaseOpCounts(phase, reportingThread);

        if ((phase == Phase.EXERCISE) && !exerciseNoCheck) {
            final Set<String> missing = new HashSet<>();
            if (exerciseReadPercent > 0.0) {
                EXERCISE_READ_OPCODE_NAMES.stream()
                    .filter(Predicate.not(counts::containsKey))
                    .forEach(missing::add);
            }
            if (exerciseReadPercent < 100.0) {
                EXERCISE_UPDATE_OPCODE_NAMES.stream()
                    .filter(Predicate.not(counts::containsKey))
                    .forEach(missing::add);
            }
            if (!missing.isEmpty()) {
                unexpectedResult("Missing operations in exercise phase: " +
                                 missing);
            }
        }
        return counts;
    }

    /* Check values */

    /**
     * Check if the LOB key's associated keynum says it should be a LOB key.
     *
     * @param op the operation performed
     * @param key the key
     * @param indexA the current index in exercise thread A
     * @param indexB the current index in exercise thread B
     */
    void checkLOBKeyLegal(Op<Key, Value, Operation> op, Key key, long indexA,
                          long indexB) {
        if (exerciseNoCheck) {
            return;
        }
        long keynum = keyToKeynum(key);
        /*
         * A LOB key is being checked to see if its associated keynum
         * says it should be that.
         */
        if (!useLOB(keynum)) {
            Formatter msg = new Formatter();
            msg.format("Unexpected key: " +
                       "threadAIndex=%#x, threadBIndex=%#x, op=%s" +
                       ", partition=%d, key=%s, value=null",
                       indexA, indexB, op.getName(),
                       getPartitionId(key), key);
            unexpectedResult(msg.toString());
            msg.close();
        }
    }

    /* Exercise methods */

    /** Get the update operation type for the specified exercise operation. */
    @Override
    UpdateOpType<Key, Value, Operation> getUpdateOpType(long index,
                                                        boolean firstThread) {

        long permutedIndex = getPermutedIndex(index, firstThread);
        long keynum = exerciseIndexToKeynum(index, firstThread);
        /*
         * We use the keynum to distinguish the LOB operations and normal
         * operations. If it's a LOB keynum, we choose operation type from the
         * LOB operation type array, otherwise the normal operation type array.
         */
        return useLOB(keynum) ?
            updateLOBOpTypes.get(
                (int) (permutedIndex % updateLOBOpTypes.size())) :
            updateOpTypes.get((int) (permutedIndex % updateOpTypes.size()));
    }

    /** Get a random read operation for the specified operation index */
    @Override
    ReadOp<Key, Value, Operation> getReadOp(
        long keynum, long index, boolean firstThread, Random threadRandom) {

        boolean isLOB = useLOB(keynum);
        /*
         * If the keynum is a LOB keynum, use the GetLOB operation instead
         * of Get, MultiGet and MultiGetIterator. TODO: But this code still
         * does MultiGet and MultiGetIterator -- does that work with LOBs?
         */
        if (isLOB) {
            int rand = threadRandom.nextInt(3);
            switch (rand) {
            case 0:
                return new GetLOBOp(this, index, firstThread, keynum);
            case 1:
                return new ReadOp.MultiGetKeysOp<>(
                    this, index, firstThread, keynum);
            case 2:
                return new ReadOp.MultiGetKeysIteratorOp<>(
                    this, index, firstThread, keynum);
            default:
                throw new AssertionError("Unexpected op: " + rand);
            }
        }

        if (threadRandom.nextInt(1000) < 999) {

            /*
             * For non-LOB keynums, we keep the original operation choosing.
             */
            int rand = threadRandom.nextInt(5);
            switch (rand) {
            case 0:
                return new ReadOp.GetOp<>(this, index, firstThread, keynum);
            case 1:
                return new ReadOp.MultiGetOp<>(
                    this, index, firstThread, keynum);
            case 2:
                return new ReadOp.MultiGetKeysOp<>(
                    this, index, firstThread, keynum);
            case 3:
                return new ReadOp.MultiGetIteratorOp<>(
                    this, index, firstThread, keynum);
            case 4:
                return new ReadOp.MultiGetKeysIteratorOp<>(
                    this, index, firstThread, keynum);
            default:
                throw new AssertionError("Unexpected op: " + rand);
            }
        } else if (threadRandom.nextBoolean()) {
            return threadRandom.nextBoolean() ?
                new StoreIteratorOp(index, firstThread, keynum) :
                new StoreBulkIteratorOp(index, firstThread, keynum);
        } else {
            return threadRandom.nextBoolean() ?
                new StoreKeysIteratorOp(index, firstThread, keynum) :
                new StoreBulkKeysIteratorOp(index, firstThread, keynum);
        }
    }

    /** Represent the GetLOB operation. */
    static class GetLOBOp extends ReadOp<Key, Value, Operation> {
        GetLOBOp(DataCheck<Key, Value, Operation> dc,
                 long index,
                 boolean firstThread,
                 long keynum) {
            super(dc, index, firstThread, keynum);
        }

        @Override
        boolean isLOBOp() {
            return true;
        }

        @Override
        void doOp(long timeoutMillis) {
            DataCheckKV dcKV = (DataCheckKV) dc;
            Key key = dcKV.keynumToKey(keynum);
            InputStreamVersion isv =
                dcKV.store.getLOB(key, null /* consistency*/, timeoutMillis,
                                  MILLISECONDS);
            checkValue(key,
                (isv == null) ?
                null :
                dcKV.stringValue(dcKV.getLOBPattern(isv.getInputStream())));
        }
    }

    /** Perform storeIterator operation */
    private class StoreIteratorOp extends ReadOp<Key, Value, Operation> {
        StoreIteratorOp(long index, boolean firstThread, long keynum) {
            super(DataCheckKV.this, index, firstThread, keynum);
        }

        @Override
        void doOp(long timeoutMillis) {
            Key key = getKeyForKeynum();
            final Iterator<KeyValueVersion> iter =
                getIterator(key, null, timeoutMillis);
            try {
                iterateScanRecords(iter);
            } finally {
                if (iter instanceof ParallelScanIterator) {
                    ((ParallelScanIterator<?>) iter).close();
                }
            }
        }

        Key getKeyForKeynum() {
            /* The standard iterator expects a partial key */
            return keynumToParentKey(keynum);
        }

        Iterator<KeyValueVersion> getIterator(Key key,
                                              Consistency consistency,
                                              long timeoutMillis) {
            return doStoreIterator(key, consistency, timeoutMillis);
        }

        private void iterateScanRecords(Iterator<KeyValueVersion> iter) {
            int count = 0;
            while (iter.hasNext()) {
                if (++count > DataCheck.MINOR_KEY_MAX) {
                    break;
                }
                final KeyValueVersion kv = iter.next();
                checkValue(kv.getKey(), kv.getValue());
            }
        }
    }

    /** Perform storeIterator(Iterator<Key>) operation */
    private class StoreBulkIteratorOp extends StoreIteratorOp {
        StoreBulkIteratorOp(long index, boolean firstThread, long keynum) {
            super(index, firstThread, keynum);
        }

        @Override
        Key getKeyForKeynum() {
            /* The bulk iterator expects a complete key */
            return keynumToKey(keynum);
        }

        @Override
        Iterator<KeyValueVersion> getIterator(Key key,
                                              Consistency consistency,
                                              long timeoutMillis) {
            return doStoreBulkIterator(key, consistency, timeoutMillis);
        }
    }

    /** Perform storeKeysIterator operation */
    private class StoreKeysIteratorOp extends ReadOp<Key, Value, Operation> {
        StoreKeysIteratorOp(long index, boolean firstThread, long keynum) {
            super(DataCheckKV.this, index, firstThread, keynum);
        }

        @Override
        void doOp(long timeoutMillis) {
            Key key = getKeyForKeynum();
            final Iterator<Key> iter = getIterator(key, null, timeoutMillis);
            try {
                iterateScanRecords(iter);
            } finally {
                if (iter instanceof ParallelScanIterator) {
                    ((ParallelScanIterator<?>) iter).close();
                }
            }
        }

        Key getKeyForKeynum() {
            /* The standard iterator expects a partial key */
            return keynumToParentKey(keynum);
        }

        Iterator<Key> getIterator(Key key,
                                  Consistency consistency,
                                  long timeoutMillis) {
            return doStoreKeysIterator(key, consistency, timeoutMillis);
        }

        private void iterateScanRecords(Iterator<Key> iter) {
            int count = 0;
            while (iter.hasNext()) {
                if (++count > DataCheck.MINOR_KEY_MAX) {
                    break;
                }
                final Key key = iter.next();
                checkKeyPresent(key);
            }
        }
    }

    /** Perform storeKeysIterator(Iterator<Key>) operation */
    private class StoreBulkKeysIteratorOp extends StoreKeysIteratorOp {
        StoreBulkKeysIteratorOp(long index, boolean firstThread, long keynum) {
            super(index, firstThread, keynum);
        }

        @Override
        Key getKeyForKeynum() {
            /* The bulk iterator expects a complete key */
            return keynumToKey(keynum);
        }

        @Override
        Iterator<Key> getIterator(Key key,
                                  Consistency consistency,
                                  long timeoutMillis) {
            return doStoreBulkKeysIterator(key, consistency, timeoutMillis);
        }
    }

    /**
     * Read data from the LOB value stream, get the repeated pattern (see
     * {@link RepeatedInputStream}) and then return. The return value is mainly
     * used for checking.
     *
     * @param ins InputStream from the LOB's inputstreamVersion
     * @return wrapped pattern string
     */
    String getLOBPattern(final InputStream ins) {
        /*
         * buffer size should be long enough to save at least one complete
         * pattern.
         */
        final int bufferSize = RepeatedInputStream.MAX_PATTERN_SIZE * 3;
        byte[] buffer = new byte[bufferSize];
        try {
            /* get the prefix of pattern "xx-index=" as delimiter. */
            int readBytes = ins.read(buffer, 0,
                RepeatedInputStream.MAX_PATTERN_SIZE);
            String retStr = new String(buffer);
            if ((readBytes <= 0) || !retStr.matches("^(pp|ea|eb).*")) {
                throw new IllegalStateException("Invalid LOB value - read " +
                    readBytes + " bytes:" + retStr);
            }
            String delimiter = retStr.split("0x")[0];
            /* skip to a mid position. */
            if (ins.skip(lobSize / 3) != 0) {
                ins.read(buffer);
            }
            return delimiter + new String(buffer).split(delimiter)[1];
        } catch (IOException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new LOBInputStreamException(
                "IOException in reading LOB value", e);
        }
    }

    /**
     * Execute a list of operations with the specified timeout, without
     * retrying if FaultException is thrown.
     *
     * @param operations the list of operations
     * @param timeoutMillis the timeout for all the operations
     * @return the operation results
     * @throws IllegalStateException if the operation fails
     */
    @Override
    List<OpResult<Value>> execute(
        List<Operation> operations, long timeoutMillis) {
        try {
            List<OpResult<Value>> results = new ArrayList<>();
            for (OperationResult r :
                     store.execute(operations, null /* durability */,
                                   timeoutMillis, MILLISECONDS)) {
                results.add(new OpResultImpl(r));
            }
            return results;
        } catch (OperationExecutionException e) {
            throw new IllegalStateException(
                "Operation execution failed: " + e.getMessage(), e);
        }
    }

    static class OpResultImpl implements OpResult<Value> {
        private final OperationResult r;
        OpResultImpl(OperationResult r) {
            this.r = r;
        }
        @Override
        public Value getPreviousValue() {
            return r.getPreviousValue();
        }
        @Override
        public boolean getSuccess() {
            return r.getSuccess();
        }
    }

    /* Keynum and index operations */

    /** Get the value for the specified populate operation. */
    @Override
    Value getPopulateValueInternal(long index) {
        return dataSerializer.populateToValue(index);
    }

    /** Get the value to insert for the specified exercise operation. */
    @Override
    Value getExerciseValueInternal(long index, boolean firstThread) {
        return dataSerializer.exerciseToValue(index, firstThread);
    }

    /* Keys and Keynums */

    /** Get minor path. */
    @Override
    String minorValueToPath(long keynum, int minor) {
        String path = super.minorValueToPath(keynum, minor);
        return useLOB(keynum) ? path.concat(LOB_SUFFIX) : path;
    }

    /** Parse the minor component, tear the LOB suffix if exists. */
    @Override
    String parseMinorPath(List<String> minorPath) {
        return minorPath.get(0).replaceAll(LOB_SUFFIX, "");
    }

    /** Convert a keynum to the associated key. */
    @Override
    Key keynumToKey(long keynum) {
        return keynumToKeyInternal(keynum);
    }

    /** Convert a keynum to the major portion of the associated key. */
    @Override
    Key keynumToMajorKey(long keynum) {
        return keynumToMajorKeyInternal(keynum);
    }

    /** Convert a keynum to the parent portion of the associated key. */
    @Override
    Key keynumToParentKey(long keynum) {
        return keynumToParentKeyInternal(keynum);
    }

    /** Convert a key to a keynum, returning -1 if the key is not valid */
    @Override
    long keyToKeynum(Key key) {
        return keyToKeynumInternal(key);
    }

    /**
     *  Judge if a keynum can be chosen to be a LOB key. The permuted keynum
     *  with its lower 16-bit number less than the LOB threshold in its block
     *  range will be chosen.
     *
     *  @param keynum keynum
     *  @param pct LOB percentage
     *  @return judgment result
     */
    boolean isLOBKeynum(long keynum, double pct) {
        long permutedKeynum = permuteLOBKeynum.transformSixByteLong(keynum);
        int keynumPos = (int) (permutedKeynum & BLOCK_MASK);
        return keynumPos < (int)(BLOCK_MASK * pct);
    }

    boolean useLOB(long keynum) {
        return useLOBs && isLOBKeynum(keynum, lobRatio);
    }

    /**
     * Checks if the key, along with it's associated keynum, is in the
     * partitions specified for this test.
     */
    @Override
    boolean keyInPartitions(Key key, long keynum) {
        return keyInPartitionsInternal(key, keynum);
    }

    /** Returns the partition ID, if known, else -1. */
    @Override
    int getPartitionId(Key key) {
        return getPartitionIdInternal(key);
    }

    /* Logging and reporting */

    /** Convert a value to a string. */
    @Override
    String valueString(Value value) {
        if (value == null) {
            return "null";
        }
        return new String(value.getValue());
    }

    /** Convert a key to a string. */
    @Override
    String keyString(Key key) {
        return key.toString();
    }

    /** Convert a data string to value. */
    Value stringValue(String pattern) {
        return dataSerializer.dataStringToValue(pattern);
    }

    /** Get the length of the key. */
    @Override
    int keyLength(Key key) {
        return key.toByteArray().length;
    }

    /** Get the length of the value. */
    @Override
    int valueLength(Value value) {
        return value.getValue().length;
    }

    /**
     * Tally a putLOB operation on the store. Size of each LOB value is given by
     * lobSize.
     *
     * @param key the key
     */
    void tallyLOBPut(Key key) {
        putLOBCount.incrementAndGet();
        putLOBKeyBytes.addAndGet(key.toByteArray().length);
        putLOBValueBytes.addAndGet(lobSize);
    }

    /**
     * Tally a appendLOB operation on the store. Size of each LOB value is
     * given by lobSize.
     *
     * @param key the key
     */
    void tallyLOBAppend(Key key) {
        appendLOBCount.incrementAndGet();
        appendLOBKeyBytes.addAndGet(key.toByteArray().length);
        appendLOBValueBytes.addAndGet(lobSize);
    }

    /* Miscellaneous */

    /**
     * A mechanism to perform the LOB operation. If FaultException, PartialLOB
     * or the ConcurrentModification exception happens, retries will be
     * triggered.
     */
    abstract class FaultingLOB {
        /**
         * Perform the LOB operation, retrying if RemoteException,
         * PartialLOBException or ConcurrentModificationException is thrown
         * until the LOB operation timeout expires.
         */
        void perform() {
            boolean retrying = false;
            long endTimeMs = System.currentTimeMillis() + lobOpTimeout;
            while (true) {
                Exception exception;
                try {
                    call(lobChunkTimeout, retrying);
                    return;
                } catch (ConcurrentModificationException e) {
                    exception = e;
                } catch (PartialLOBException e) {
                    exception = e;
                } catch (FaultException e) {
                    if (!hasRemoteExceptionCause(e)) {
                        throw e;
                    }
                    exception = e;
                }
                /*
                 * Retry only for RemoteExceptions, PartialLOBException
                 * and ConcurrentModificationException.
                 */
                long now = System.currentTimeMillis();
                /*
                 * Use the (endTimeMs - now - 1) here to allow a retry when the
                 * delayMs is smaller than lobChunkTimeout.
                 */
                long delayMs = Math.min(lobChunkTimeout,
                                        (endTimeMs - now - 1));
                /*
                 * Delay some time before retrying for potential PartialLOB or
                 * ConcurrentModificationException, to avoid conflict with the
                 * other thread updating the same LOB at the same time.
                 */
                if (delayMs > 0) {
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        /* Ignore */
                    }
                    /* Refresh the 'now' timestamp before retrying. */
                    now = System.currentTimeMillis();
                }
                /*
                 * Stop retrying if the LOB operation timeout has been exceeded
                 * before we start it.
                 */
                if ((endTimeMs - now) <= 0) {
                    throw new RuntimeException(
                        "Retry timeout exceeded for LOB operation " +
                         this + ": " + exception, exception);
                }
                synchronized (err) {
                    log("Retrying operation: " + this);
                    exception.printStackTrace(err);
                }
                retrying = true;
                noteRetry();
            }
        }

        /* perform the operation once. */
        abstract void call(long chunkTimeoutMillis, boolean retrying);
    }

    /**
     * Faulting PutLOB operation, used for population phase to insert LOB
     * values.
     */
    class FaultingPutLOB extends FaultingLOB {
        final Key key;
        final String lobPattern;
        final Durability durability;
        /**
         * We do not pass the large LOB value directly here. Instead we pass
         * a value pattern, and the pattern will be repeated via the
         * {@link RepeatedInputStream} and then passed to the pubLOB API.
         *
         * @param key key
         * @param pattern repeated pattern
         */
        FaultingPutLOB(Key key, String pattern) {
            this.key = key;
            this.lobPattern = pattern;
            /* Set the durability to default value of null. */
            this.durability = null;
        }

        @Override
        void call(long chunkTimeoutMillis, boolean ignore /* retrying */) {
            try{
                store.putLOBIfAbsent(
                    key, new RepeatedInputStream(lobSize, lobPattern),
                    durability, chunkTimeoutMillis, MILLISECONDS);
            } catch (IOException e) {
                final Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                /* Re-throw the IOException */
                throw new LOBInputStreamException(
                    "IOException in putLOBIfAbsent", e);
            }
            tallyLOBPut(key);
        }

        @Override
        public String toString() {
            return "FaultingPutLOB[key=" + key + ", repeated value=" +
                lobPattern + ", durability=" + durability + "]";
        }
    }
    class FaultingDeleteLOB extends FaultingLOB {
        private final Key key;
        private final Durability durability;
        FaultingDeleteLOB(Key key) {
            this.key = key;
            /* Set the durability to default value of null. */
            this.durability = null;
        }
        @Override
        void call(long chunkTimeoutMillis, boolean ignore /* retrying */) {
            store.deleteLOB(key, durability, chunkTimeoutMillis, MILLISECONDS);
        }
        @Override
        public String toString() {
            return "FaultingDeleteLOB[key=" + key + ", durability=" +
                durability + "]";
        }
    }

    class FaultingLOBOp extends FaultingLOB {
        private final Op<Key, Value, Operation> op;
        FaultingLOBOp(Op<Key, Value, Operation> op) {
            this.op = op;
        }
        @Override
        public void call(long chunkTimeoutMillis, boolean retrying) {
            op.doOp(chunkTimeoutMillis, retrying);
        }
        @Override
        public String toString() {
            return "FaultingLOBOp[op=" + op + "]";
        }
    }

    /* Threads */

    @Override
    Operation getPopulateOp(Key key, Value value, long keynum, long index) {
        Operation operation = null;
        boolean isLOB = useLOB(keynum);
        if (isLOB) {
            try {
                new FaultingPutLOB(key, valueString(value)).perform();
            } catch (ConsistencyException e) {
                throw e;
            } catch (RuntimeException e) {
                throw e;
            }
        } else {
            operation = factory.createPutIfAbsent(key, value,
                null /* prevReturn */, true /* abortIfUnsuccessful */);
            tallyPut(key, value);
        }
        if (verbose >= DEBUG) {
            log("op=" + (isLOB ? "putLOB" : "put") +
                " index=%#x, partition=%d, key=%s, keynum=%#x, value=%s",
                index, getPartitionId(key), key, keynum,
                valueString(value));
        }
        return operation;
    }

    @Override
    Op<Key, Value, Operation> getCheckOp(long index) {
        return new MultiGetIteratorCheckOpKV(DataCheckKV.this, index);
    }

    /** Check operation. */
    private class MultiGetIteratorCheckOpKV
        extends Op.MultiGetIteratorCheckOp<Key, Value, Operation> {

        MultiGetIteratorCheckOpKV(DataCheck<Key, Value, Operation> dc,
                                  long index) {
            super(dc, index);
        }

        @Override
        Value getValue(long keynum, KeyVal<Key, Value> kv) {
            Value value;
            /* Re-get the LOB value if it's a LOB keynum. */
            if (useLOB(keynum)) {
                Key key = kv.getKey();
                InputStreamVersion isv =
                    store.getLOB(key, null /* consistency */,
                                 0 /* default chunk timeout */,
                                 null /* default time unit */);
                value = (isv == null) ?
                    null :
                    stringValue(getLOBPattern(isv.getInputStream()));
            } else {
                value = kv.getValue();
            }
            return value;
        }
    }

    @Override
    Operation getCleanUpOp(Key key, long index, long keynum) {
        Operation operation = null;
        boolean isLOB = useLOB(keynum);
        if (isLOB) {
            try {
                new FaultingDeleteLOB(key).perform();
            } catch (ConsistencyException e) {
                throw e;
            } catch (RuntimeException e) {
                throw e;
            }
        } else {
            operation = factory.createDelete(key);
        }
        if (verbose >= DEBUG) {
            log("op=" + (isLOB ? "deleteLOB" : "delete") +
                " index=%#x, partition=%d, key=%s, keynum=%#x",
                index, getPartitionId(key), key, keynum);
        }
        return operation;
    }

    /** Exercise thread. */
    class ExerciseThreadEx extends ExerciseThread {

        private final ExecutorService singleThreadPool;

        ExerciseThreadEx(long start,
                         long count,
                         boolean firstThread,
                         CyclicBarrier barrier,
                         AtomicLong exerciseIndex,
                         AtomicLong otherExerciseIndex,
                         double readPercent,
                         double targetThrPerMs) {
            super(start, count, firstThread, barrier, exerciseIndex,
                  otherExerciseIndex, readPercent, targetThrPerMs);
            /* A single thread to perform LOB operation task. */
            singleThreadPool = Executors.newSingleThreadExecutor();
        }

        /* Task for performing a LOB operation. */
        class LOBOpTask implements Runnable {
            private final Op<Key, Value, Operation> LOBOp;
            LOBOpTask(Op<Key, Value, Operation> op) {
                this.LOBOp = op;
            }
            @Override
            public void run() {
                threadOtherExerciseIndex.set(otherExerciseIndex);
                try {
                    new FaultingLOBOp(LOBOp).perform();
                } catch (Throwable t) {
                    unexpectedException(t);
                }
            }
        }

        /*
         * Since the exercise requires a synchronization between the two threads
         * A & B, the time of retrying and backing-off in performing the LOB
         * operation may be intolerable. So we override the runOp method, to put
         * the processing of LOB operations into independent worker threads.
         */
        @Override
        void runOp(Op<Key, Value, Operation> op) {
            if (op.isLOBOp()) {
                singleThreadPool.execute(new LOBOpTask(op));
            } else {
                super.runOp(op);
            }
        }

        @Override
        void waitForComplete() throws InterruptedException {
            /*
             * Wait until all LOB tasks end, or throw RuntimeException if
             * the maximum time for performing all LOB operations in an
             * exercise thread is exceeded.
             */
            long exeLOBOpTimeout = ((long)((max - start) * lobRatio) + 1) *
                    lobOpTimeout;
            singleThreadPool.shutdown();
            boolean isShutdown = singleThreadPool.awaitTermination(
                exeLOBOpTimeout, MILLISECONDS);
            /* Timeout exceeds, force all tasks to stop. */
            if (!isShutdown) {
                singleThreadPool.shutdownNow();
                throw new RuntimeException(
                    "Timeout exceeded for performing LOB tasks " +
                    this + ": " + exeLOBOpTimeout + " ms.");
            }
        }
    }

    /** Thrown when reading from the InputStream of LOB fails. */
    public static class LOBInputStreamException extends RuntimeException {
        private static final long serialVersionUID = 1;
        LOBInputStreamException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * An InputStream to generate the LOB value following the repeated pattern.
     */
    public static class RepeatedInputStream extends InputStream {
        static final int MAX_PATTERN_SIZE = 22;
        private final long LOBSize;
        private final String repeatedPattern;
        private final int patternLength;
        private long position = 0;

        /**
         * Constructor for RepeatedInputStream.
         * @param size size of the LOB value
         * @param pattern the repeated string pattern
         */
        RepeatedInputStream(long size, String pattern) {
            this.LOBSize = size;
            this.repeatedPattern = pattern;
            this.patternLength = pattern.length();
            if (patternLength > MAX_PATTERN_SIZE) {
                throw new IllegalArgumentException(
                    "The length of the pattern must less or equal to " +
                    MAX_PATTERN_SIZE + ": " + patternLength);
            }
        }
        @Override
        public int read() {
            /*
             * For example, give the string of "pp-index=0x00000000ea", the LOB
             * value will be
             *
             * "pp-index=0x00000000eapp-index=0x00000000ea ... pp-index=0x0000"
             * |---------------------------  LOBSize  ------------------------|
             *
             * Note that he string pattern would be truncated to fit the given
             * size.
             */
            return position > LOBSize ?
                   -1 :
                   repeatedPattern.charAt((int)((position++) % patternLength));
        }
    }

    /* Operations */

    @Override
    ValVer<Value> doGet(Key key, Object consistency, long timeoutMillis) {
        return new ValVerImpl(
            store.get(key,
                      (Consistency) consistency,
                      timeoutMillis,
                      MILLISECONDS));
    }

    static class ValVerImpl implements ValVer<Value> {
        final ValueVersion vv;
        final boolean result;
        ValVerImpl(ValueVersion vv) {
            this(vv, true);
        }
        ValVerImpl(ValueVersion vv, boolean result) {
            this.vv = vv;
            this.result = result;
        }
        @Override
        public Value getValue() {
            return (vv != null) ? vv.getValue() : null;
        }
        @Override
        public Object getVersion() {
            return (vv != null) ? vv.getVersion() : null;
        }
        @Override
        public boolean getSuccess() {
            return result;
        }
    }

    @Override
    Collection<Value> doMultiGet(Key key,
                                 Object consistency,
                                 long timeoutMillis) {
        List<Value> result = new ArrayList<>();
        for (ValueVersion vv : store.multiGet(
                 key, null /* subRange */, Depth.PARENT_AND_DESCENDANTS,
                 (Consistency) consistency,
                 timeoutMillis, MILLISECONDS).values()) {
            result.add(vv.getValue());
        }
        return result;
    }

    @Override
    Collection<Key> doMultiGetKeys(Key key,
                                   Object consistency,
                                   long timeoutMillis) {
        return store.multiGetKeys(key, null /* subRange */,
                                  Depth.PARENT_AND_DESCENDANTS,
                                  (Consistency) consistency,
                                  timeoutMillis, MILLISECONDS);
    }

    @Override
    Iterator<KeyVal<Key, Value>> doMultiGetIterator(Key key,
                                                    Object consistency,
                                                    long timeoutMillis) {
        return new KeyValIter<>(
            store.multiGetIterator(
                Direction.FORWARD, 1, key, null /* subRange */,
                Depth.PARENT_AND_DESCENDANTS, (Consistency) consistency,
                timeoutMillis,
                MILLISECONDS));
    }

    private static
    class KeyValIter<I extends Iterator<KeyValueVersion>>
            implements Iterator<KeyVal<Key, Value>> {
        final I iter;
        KeyValIter(I iter) {
            this.iter = iter;
        }
        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }
        @Override
        public KeyVal<Key, Value> next() {
            return new KeyValImpl(iter.next());
        }
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private static class KeyValImpl implements KeyVal<Key, Value> {
        private final KeyValueVersion kvv;
        KeyValImpl(KeyValueVersion kvv) {
            this.kvv = kvv;
        }
        @Override
        public Key getKey() {
            return kvv.getKey();
        }
        @Override
        public Value getValue() {
            return kvv.getValue();
        }
    }

    @Override
    Iterator<Key> doMultiGetKeysIterator(Key key,
                                         Object consistency,
                                         long timeoutMillis) {
        return store.multiGetKeysIterator(
            Direction.FORWARD, 1, key,
            null /* subRange */, Depth.PARENT_AND_DESCENDANTS,
            (Consistency) consistency, timeoutMillis, MILLISECONDS);
    }

    private Iterator<KeyValueVersion> doStoreIterator(Key key,
                                                      Consistency consistency,
                                                      long timeoutMillis) {
        return useParallelScan ?
            store.storeIterator(
                Direction.UNORDERED, DataCheck.MINOR_KEY_MAX, key,
                null /* subRange */, Depth.PARENT_AND_DESCENDANTS,
                consistency, timeoutMillis, MILLISECONDS,
                storeIteratorConfig) :
            store.storeIterator(
                Direction.UNORDERED, DataCheck.MINOR_KEY_MAX, key,
                null /* subRange */, Depth.PARENT_AND_DESCENDANTS,
                consistency, timeoutMillis, MILLISECONDS);
    }

    private Iterator<KeyValueVersion>
        doStoreBulkIterator(Key key,
                            Consistency consistency,
                            long timeoutMillis) {
        return store.storeIterator(
            singletonList(key).iterator(), DataCheck.MINOR_KEY_MAX,
            null /* subRange */, Depth.PARENT_AND_DESCENDANTS,
            consistency, timeoutMillis, MILLISECONDS, storeIteratorConfig);
    }

    private Iterator<Key> doStoreKeysIterator(Key key,
                                              Consistency consistency,
                                              long timeoutMillis) {
        return useParallelScan ?
            store.storeKeysIterator(
                Direction.UNORDERED, DataCheck.MINOR_KEY_MAX, key, null,
                Depth.PARENT_AND_DESCENDANTS, consistency, timeoutMillis,
                MILLISECONDS, storeIteratorConfig) :
            store.storeKeysIterator(
                Direction.UNORDERED, DataCheck.MINOR_KEY_MAX, key, null,
                Depth.PARENT_AND_DESCENDANTS, consistency, timeoutMillis,
                MILLISECONDS);
    }

    private Iterator<Key> doStoreBulkKeysIterator(Key key,
                                                  Consistency consistency,
                                                  long timeoutMillis) {
        return store.storeKeysIterator(
            singletonList(key).iterator(), DataCheck.MINOR_KEY_MAX,
            null /* subRange */, Depth.PARENT_AND_DESCENDANTS,
            consistency, timeoutMillis, MILLISECONDS, storeIteratorConfig);
    }

    @Override
    ValVer<Value> doPut(Key key, Value value,
                        boolean requestPrevValue,
                        long timeoutMillis) {

        ReturnValueVersion rvv = new ReturnValueVersion(
            requestPrevValue ? Choice.VALUE : Choice.NONE);
        store.put(key, value, rvv, null /* durability */,
                  timeoutMillis, MILLISECONDS);
        return new ValVerImpl(rvv);
    }

    @Override
    Operation createPutOp(Key key, Value value,
                          boolean requestPrevValue) {

        return factory.createPut(
            key, value, (requestPrevValue ? Choice.VALUE : Choice.NONE),
            false /* abortIfUnsuccessful */);
    }

    @Override
    ValVer<Value> doPutIfAbsent(Key key, Value value,
                                boolean requestPrevValue,
                                long timeoutMillis) {

        ReturnValueVersion rvv = new ReturnValueVersion(
            requestPrevValue ? Choice.VALUE : Choice.NONE);
        Version v = store.putIfAbsent(key, value, rvv, null /* durability */,
                                      timeoutMillis, MILLISECONDS);
        return new ValVerImpl(rvv, (v != null));
    }

    @Override
    Operation createPutIfAbsentOp(Key key, Value value,
                                  boolean requestPrevValue) {

        return factory.createPutIfAbsent(
            key, value, (requestPrevValue ? Choice.VALUE : Choice.NONE),
            false /* abortIfUnsuccessful */);
    }

    @Override
    ValVer<Value> doPutIfPresent(Key key, Value value,
                                 boolean requestPrevValue,
                                 long timeoutMillis) {

        ReturnValueVersion rvv = new ReturnValueVersion(
            requestPrevValue ? Choice.VALUE : Choice.NONE);
        Version v = store.putIfPresent(key, value, rvv, null /* durability */,
                                       timeoutMillis, MILLISECONDS);
        return new ValVerImpl(rvv, (v != null));
    }

    @Override
    Operation createPutIfPresentOp(Key key, Value value,
                                   boolean requestPrevValue) {

        return factory.createPutIfPresent(
            key, value, (requestPrevValue ? Choice.VALUE : Choice.NONE),
            false /* abortIfUnsuccessful */);
    }

    @Override
    ValVer<Value> doPutIfVersion(Key key, Value value, Object version,
                                 boolean requestPrevValue,
                                 long timeoutMillis) {

        ReturnValueVersion rvv = new ReturnValueVersion(
            requestPrevValue ? Choice.VALUE : Choice.NONE);
        Version v = store.putIfVersion(key, value, (Version) version, rvv,
                                       null /* durability */,
                                       timeoutMillis, MILLISECONDS);
        return new ValVerImpl(rvv, (v != null));
    }

    @Override
    Operation createPutIfVersionOp(Key key, Value value, Object version,
                                   boolean requestPrevValue) {

        return factory.createPutIfVersion(
            key, value, (Version) version,
            (requestPrevValue ? Choice.VALUE : Choice.NONE),
            false /* abortIfUnsuccessful */);
        }

    @Override
    boolean doBulkPut(Key key, Value value, long timeoutMillis) {
        final SingleItemEntryStream<KeyValue> entryStream =
            new SingleItemEntryStream<KeyValue>(new KeyValue(key, value)) {
                @Override
                boolean keysMatch(KeyValue x, KeyValue y) {
                    return x.getKey().equals(y.getKey());
                }
            };
        store.put(singletonList(entryStream),
                  new BulkWriteOptions(
                      null /* durability */, timeoutMillis, MILLISECONDS));
        entryStream.await(timeoutMillis);
        return entryStream.keyExists();
    }

    @Override
    ValVer<Value> doDelete(Key key,
                           boolean requestPrevValue,
                           long timeoutMillis) {

        ReturnValueVersion rvv = new ReturnValueVersion(
            requestPrevValue ? Choice.VALUE : Choice.NONE);
        boolean exist = store.delete(key, rvv, null /* durability */,
                                     timeoutMillis, MILLISECONDS);
        return new ValVerImpl(rvv, exist);
    }

    @Override
    Operation createDeleteOp(Key key, boolean requestPrevValue) {
        return factory.createDelete(
            key, (requestPrevValue ? Choice.VALUE : Choice.NONE),
            false /* abortIfUnsuccessful */);
    }

    @Override
    int doMultiDelete(Key key, long timeoutMillis) {
        return store.multiDelete(
            key, null /* subRange */, Depth.PARENT_AND_DESCENDANTS,
            null /* durability */, timeoutMillis, MILLISECONDS);
    }

    @Override
    ValVer<Value> doDeleteIfVersion(Key key, Object version,
                                    boolean requestPrevValue,
                                    long timeoutMillis) {

        ReturnValueVersion rvv = new ReturnValueVersion(
            requestPrevValue ? Choice.VALUE : Choice.NONE);
        store.deleteIfVersion(
            key, (Version) version, rvv, null, timeoutMillis, MILLISECONDS);
        return new ValVerImpl(rvv);
    }

    @Override
    Operation createDeleteIfVersionOp(Key key, Object version,
                                      boolean requestPrevValue) {

        return factory.createDeleteIfVersion(
            key, (Version) version, (requestPrevValue ? Choice.VALUE
                                                      : Choice.NONE),
            false /* abortIfUnsuccessful */);
    }

    @Override
    Key appendStringToKey(Key key, String x) {
        return Key.fromString(key + x);
    }

    /*
     * LOB operation classes. In these classes, we do NOT do the checking, since
     * the LOB operation is not atomic, and kvstore does not provide
     * transactional method to get the previous values while doing putting or
     * deleting.
     */

    /* PutLOB operation. */
    private class PutLOBOp extends UpdateOp<Key, Value, Operation> {
        PutLOBOp(long index, boolean firstThread, boolean requestPrevValue) {
            super(DataCheckKV.this, index, firstThread, requestPrevValue);
        }

        @Override
        boolean isLOBOp() {
            return true;
        }

        @Override
        void doOp(long timeoutMillis, boolean retrying) {
            long keynum = exerciseIndexToKeynum(index, firstThread);
            Key key = keynumToKey(keynum);
            Value value = getExerciseValue(index, firstThread);
            try {
                doStoreLOBOp(key, valueString(value), timeoutMillis);
            } catch (IOException e) {
                final Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                /* Re-throw the IOException */
                throw new LOBInputStreamException(
                    "IOException in " + getName(), e);
            }
            tallyLOBPut(key);
        }

        void doStoreLOBOp(Key key,
                          String valuePattern,
                          long chunkTimeoutMillis) throws IOException {
            store.putLOB(key,
                new RepeatedInputStream(lobSize, valuePattern),
                null /* durability */, chunkTimeoutMillis, MILLISECONDS);
        }
     }

    /* PutLOBIfAbsent operation. */
    private class PutLOBIfAbsentOp extends PutLOBOp {
        PutLOBIfAbsentOp(long index, boolean firstThread) {
            super(index, firstThread, false);
        }

        /* Use putLOBIfAbsent for LOB keys. */
        @Override
        void doStoreLOBOp(Key key,
                          String valuePattern,
                          long chunkTimeoutMillis) throws IOException {
            store.putLOBIfAbsent(key,
                new RepeatedInputStream(lobSize, valuePattern),
                null /* durability */, chunkTimeoutMillis, MILLISECONDS);
        }
    }

    /* PutLOBIfPresent operation. */
    private class PutLOBIfPresentOp extends PutLOBOp {
        PutLOBIfPresentOp(long index, boolean firstThread) {
            super(index, firstThread, false);
        }

        /* Use PutLOBIfPresent for LOB keys. */
        @Override
        void doStoreLOBOp(Key key,
                          String valuePattern,
                          long chunkTimeoutMillis) throws IOException {
            store.putLOBIfPresent(key,
                new RepeatedInputStream(lobSize, valuePattern),
                null /* durability */, chunkTimeoutMillis, MILLISECONDS);
        }
    }

    /* AppendLOB operation. */
    private class AppendLOBOp extends UpdateOp<Key, Value, Operation> {
        AppendLOBOp(long index, boolean firstThread) {
            super(DataCheckKV.this, index, firstThread, false);
        }

        @Override
        boolean isLOBOp() {
            return true;
        }

       @Override
       void doOp(long timeoutMillis, boolean retrying) {
           long keynum = exerciseIndexToKeynum(index, firstThread);
           Key key = keynumToKey(keynum);
           Value value = getExerciseValue(index, firstThread);
           try {
               doStoreLOBOp(key, valueString(value), timeoutMillis);
           } catch (IOException e) {
               final Throwable cause = e.getCause();
               if (cause instanceof RuntimeException) {
                   throw (RuntimeException) cause;
               }
               throw new LOBInputStreamException(
                   "IOException in " + getName(), e);
           }
           tallyLOBAppend(key);
       }

       void doStoreLOBOp(Key key,
                         String valuePattern,
                         long chunkTimeoutMillis) throws IOException {
           ValueVersion vv = store.get(key);
           if (vv != null) {
               store.appendLOB(key,
                   new RepeatedInputStream(lobSize, valuePattern),
                   null /* durability */, chunkTimeoutMillis, MILLISECONDS);
           }
       }
    }

    /* DeleteLOB operation. */
    private class DeleteLOBOp extends UpdateOp<Key, Value, Operation> {
        DeleteLOBOp(long index, boolean firstThread) {
            super(DataCheckKV.this, index, firstThread, false);
        }

        @Override
        boolean isLOBOp() {
            return true;
        }

        @Override
        void doOp(long timeoutMillis, boolean retrying) {
            long keynum = exerciseIndexToKeynum(index, firstThread);
            Key key = keynumToKey(keynum);
            store.deleteLOB(key, null /* durability */, timeoutMillis,
                            MILLISECONDS);
         }
    }

    /* Represents the various types of LOB update operations */

    private class PutLOB extends Put<Key, Value, Operation> {
        @Override
        public UpdateOp<Key, Value, Operation> getOp(
            DataCheck<Key, Value, Operation> dc,
            long index,
            boolean firstThread,
            boolean requestPrevValue) {

            return ((DataCheckKV) dc).new PutLOBOp(index, firstThread,
                                                   requestPrevValue);
        }
    }

    private static class PutLOBIfAbsent
            extends PutIfAbsent<Key, Value, Operation> {

        @Override
        public UpdateOp<Key, Value, Operation> getOp(
            DataCheck<Key, Value, Operation> dc,
            long index,
            boolean firstThread,
            boolean requestPrevValue) {

            return ((DataCheckKV) dc).new PutLOBIfAbsentOp(index, firstThread);
        }
    }

    private class PutLOBIfPresent extends PutIfPresent<Key, Value, Operation> {

        @Override
        public UpdateOp<Key, Value, Operation> getOp(
            DataCheck<Key, Value, Operation> dc,
            long index,
            boolean firstThread,
            boolean requestPrevValue) {

            return ((DataCheckKV) dc).new PutLOBIfPresentOp(index,
                                                            firstThread);
        }
    }

    private class AppendLOB
        extends AbstractUpdateOpType<Key, Value, Operation> {

        @Override
        public UpdateOp<Key, Value, Operation> getOp(
            DataCheck<Key, Value, Operation> dc,
            long index,
            boolean firstThread,
            boolean requestPrevValue) {

            return ((DataCheckKV) dc).new AppendLOBOp(index, firstThread);
        }

        @Override
        public Value getResult(Value newValue, Value previousValue) {
            return (previousValue == null) ? null : newValue;
        }
    }

    private class DeleteLOB extends Delete<Key, Value, Operation> {

        @Override
        public UpdateOp<Key, Value, Operation> getOp(
            DataCheck<Key, Value, Operation> dc,
            long index,
            boolean firstThread,
            boolean requestPrevValue) {

            return ((DataCheckKV) dc).new DeleteLOBOp(index, firstThread);
        }
    }

    @Override
    protected void updateTTLTestHook(Phase phase, long start, long count) {
        /* TTL is not supported by the KV API */
    }

    @Override
    void faultingPutInternal(Key key, Value value, Object durability) {
        new FaultingPut(key, value, (Durability) durability).perform();
    }

    @Override
    Set<Key> faultingMultiGetKeysInternal(Key key) {
        return new FaultingMultiGetKeys(key).perform();
    }

    @Override
    void faultingMultiDeleteInternal(Key key, Object durability) {
        new FaultingMultiDelete(key, (Durability) durability).perform();
    }

    @Override
    ValueVersion faultingGetInternal(Key key) {
        return new FaultingGet(key).perform();
    }
}
