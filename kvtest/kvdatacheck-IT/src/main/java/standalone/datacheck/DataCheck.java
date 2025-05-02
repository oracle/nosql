/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import static java.lang.Math.abs;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.synchronizedMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.io.PrintStream;
import java.rmi.RemoteException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import oracle.kv.Consistency;
import oracle.kv.ConsistencyException;
import oracle.kv.Depth;
import oracle.kv.Durability;
import oracle.kv.EntryStream;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.Key;
import oracle.kv.RequestTimeoutException;
import oracle.kv.StoreIteratorConfig;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.stats.KVStats;
import oracle.kv.stats.OperationMetrics;

import standalone.datacheck.UpdateOpType.Delete;
import standalone.datacheck.UpdateOpType.DeleteExecute;
import standalone.datacheck.UpdateOpType.DeleteIfVersion;
import standalone.datacheck.UpdateOpType.DeleteIfVersionExecute;
import standalone.datacheck.UpdateOpType.MultiDelete;
import standalone.datacheck.UpdateOpType.Put;
import standalone.datacheck.UpdateOpType.PutExecute;
import standalone.datacheck.UpdateOpType.PutIfAbsent;
import standalone.datacheck.UpdateOpType.PutIfAbsentExecute;
import standalone.datacheck.UpdateOpType.PutIfPresent;
import standalone.datacheck.UpdateOpType.PutIfPresentExecute;
import standalone.datacheck.UpdateOpType.PutIfVersion;
import standalone.datacheck.UpdateOpType.PutIfVersionExecute;

/**
 * Implement the API for data checking used by {@link DataCheckMain}.
 *
 * @param <K> the key type
 * @param <V> the value type
 * @param <O> the operation type
 */
public abstract class DataCheck<K, V, O> {

    /** Number of entries in the store for a block. */
    public static final int BLOCK_COUNT = 0x8000;

    /** The maximum index value for the start of a block. */
    public static final long MAX_INDEX = 0x7fffffff8000L;

    /** Verbose value for standard verbose output. */
    public static final int VERBOSE = 1;

    /** Verbose value for debugging output. */
    public static final int DEBUG = 2;

    /** Maximum number of minor keys for a given major key. */
    static final int MINOR_KEY_MAX = 256;

    /**
     * Number of minor key entries for a given major key -- half of the
     * maximum.
     */
    static final int MINOR_KEY_COUNT = 128;

    /*
     * Populate and exercise operations are performed in block units, to make
     * sure that the tests work on complete units of modification to the store.
     */

    /** Number of keys in a block. */
    static final int BLOCK_MAX = 0x10000;

    /** Mask for block value. */
    static final long BLOCK_MASK = 0xffffL;

    /**
     * Timestamp format, using the UTC timezone:
     * 2012-01-31 13:22:31.137 UTC
     */
    private static final SimpleDateFormat utcDateFormat =
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z");
    static {
        utcDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    /*
     * Fields -- positions of bits in the keynum:
     *
     * 0000ffff ffffff00       Major
     * 0000ffff ffff0000       Parent
     * 00000000 0000ff00       Child
     * 00000000 000000ff       Minor
     */

    @SuppressWarnings("unused")
    private static final long MAJOR_KEY_FIELD = 0xffffffffff00L;
    private static final long PARENT_KEY_FIELD = 0xffffffff0000L;
    @SuppressWarnings("unused")
    private static final long CHILD_KEY_FIELD = 0xff00L;
    @SuppressWarnings("unused")
    private static final long MINOR_KEY_FIELD = 0xffL;

    /* Masks and shifts */

    @SuppressWarnings("unused")
    private static final long MAJOR_KEY_MASK = 0xffffffffffL;

    /** Shift major key into keynum. */
    private static final int MAJOR_KEY_SHIFT = 8;

    protected static final long PARENT_KEY_MASK = 0xffffffffL;

    /** Shift parent key into keynum. */
    protected static final int PARENT_KEY_SHIFT = 16;

    /** Shift parent key into major portion of keynum. */
    private static final int PARENT_MAJOR_KEY_SHIFT = 8;

    private static final long CHILD_KEY_MASK = 0xffL;

    /** Shift child key into keynum. */
    @SuppressWarnings("unused")
    private static final int CHILD_KEY_SHIFT = 8;

    private static final long MINOR_KEY_MASK = 0xffL;

    /** Number of bits to shift an index left to produce a keynum. */
    private static final int KEYNUM_SHIFT = 1;

    /**
     * Mask for bits in a keynum that should be clear if the keynum corresponds
     * to an index.
     */
    private static final long KEYNUM_POPULATE_CLEAR_MASK = 1;

    private static final Key SEED_KEY = Key.fromString("/m/seed");

    private static final Value EMPTY_VALUE = Value.createValue(new byte[0]);

    /** Major key components for storing update history. */
    protected static List<String> UPDATE_HISTORY =
        Arrays.asList("m", "updates");

    /** Insert errors, for testing error reporting. */
    protected static boolean INSERT_ERRORS = false;

    /** Exit the test when unexpected exception occurs, used for debugging */
    static boolean EXIT_ON_UNEXPECTED_EXCEPTION = false;

    /** Exit the test when unexpected result occurs, used for debugging */
    static boolean EXIT_ON_UNEXPECTED_RESULT = false;

    /* Update operation types */
    /* 50% chance of adding an entry */
    final UpdateOpType<K, V, O> PUT = new Put<>();
    final UpdateOpType<K, V, O> PUT_EXECUTE = new PutExecute<>();
    final UpdateOpType<K, V, O> PUT_IF_ABSENT = new PutIfAbsent<>();
    final UpdateOpType<K, V, O> PUT_IF_ABSENT_EXECUTE =
        new PutIfAbsentExecute<>();
    /* No effect on number of entries */
    final UpdateOpType<K, V, O> PUT_IF_PRESENT = new PutIfPresent<>();
    final UpdateOpType<K, V, O> PUT_IF_PRESENT_EXECUTE =
        new PutIfPresentExecute<>();
    final UpdateOpType<K, V, O> PUT_IF_VERSION = new PutIfVersion<>();
    final UpdateOpType<K, V, O> PUT_IF_VERSION_EXECUTE =
        new PutIfVersionExecute<>();
    /* 50% chance of removing an entry */
    final UpdateOpType<K, V, O> DELETE = new Delete<>();
    final UpdateOpType<K, V, O> DELETE_EXECUTE = new DeleteExecute<>();
    final UpdateOpType<K, V, O> DELETE_IF_VERSION = new DeleteIfVersion<>();
    final UpdateOpType<K, V, O> DELETE_IF_VERSION_EXECUTE =
        new DeleteIfVersionExecute<>();
    final UpdateOpType<K, V, O> MULTI_DELETE = new MultiDelete<>();

    /**
     * A list of UpdateOpTypes selected so that they will create no net
     * increase or decrease on the number of entries if chosen randomly.
     * Operations should be defined in pairs that either include an operation
     * that increases the count paired with one that decreases it, or that
     * include two operations where neither changes the count.
     * <p>
     * Operations are selected by computing the permuted index number, a value
     * between 0 and 2^48 - 1, and choosing the operation with that index
     * modulo the number of operations.
     * <p>
     * If the number of operations and the number of entries in a block are
     * both even, and operations that increase or decrease the count are
     * defined in pairs that are aligned to an even index in the list of
     * operations, this procedure will always include both entries of an
     * increase/decrease pair.
     */
    final List<UpdateOpType<K, V, O>> updateOpTypes = new ArrayList<>();

    final KVStore store;

    /** For serializing and deserializing values. */
    final DataSerializer dataSerializer;

    /** Only perform operations for keys in these partitions unless null. */
    protected final Set<Integer> partitions;

    /** The number of milliseconds between status reports */
    private final long reportingInterval;

    /** The verbose level, to control quantity of output. */
    final int verbose;

    /** The error output stream. */
    final PrintStream err;

    /**
     * Time to wait for a set of operations on a block to complete, in
     * milliseconds.
     */
    final long blockTimeout;

    /** The request timeout, in milliseconds. */
    final long requestTimeout;

    /** The max throughput for this client, in ops/second **/
    private final double maxThroughput;

    /** The max execution time for this client, in seconds **/
    private final int maxExecTime;

    /** The random number generator. */
    volatile Random random;

    /** Permutation for major keys. */
    private volatile Permutation permuteMajor;

    /** Permutation for exercise operations. */
    private volatile Permutation permuteOperation;

    /** The number of put operations. */
    private final AtomicLong putCount = new AtomicLong();

    /** The total number of key bytes for put operations. */
    private final AtomicLong putKeyBytes = new AtomicLong();

    /** The total number of value bytes for put operations. */
    private final AtomicLong putValueBytes = new AtomicLong();

    protected final AtomicLong unexpectedExceptionCount = new AtomicLong();
    protected final AtomicLong unexpectedResultCount = new AtomicLong();

    /**
     * The number of times a result from a retried operation was not from an
     * expected ordering of operations, but still matched one of the exercise
     * values or null.
     */
    private final AtomicLong otherRetryResultCount = new AtomicLong();

    protected final AtomicLong retryCount = new AtomicLong();

    /** The state of the test. */
    enum TestState {

        /** The test is being initialized. */
        INIT,

        /** The test is underway. */
        ACTIVE,

        /** The test has passed. */
        PASSED,

        /** The test has failed. */
        FAILED
    }

    /** Whether to perform parallel scan for exercise phase */
    boolean useParallelScan = false;

    /**
     * Store the iterator configuration for parallel scan.
     *
     * maxConcurrentRequests - Specify the max allowed internal threads used
     * in the client thread pool to concurrently retrieve records from
     * partitions.
     *
     * maxResultsBatches - Specify the number of slots for batch results to
     * return. 1 slot will allow 1 batch of results to return.
     */
    final StoreIteratorConfig storeIteratorConfig;

    private final AtomicReference<TestState> testState =
        new AtomicReference<>(TestState.INIT);

    /* Information about checking lag. */

    private final Object lagSync = new Object();
    private long lagCount = 0;
    private long lagSum = 0;
    private long lagMax = 0;

    /**
     * Whether to check results of update operations during the exercise
     * phase.
     */
    volatile boolean exerciseNoCheck = false;

    /**
     * A thread local that holds the exercise index for the other exercise
     * thread during the exercise phase.
     */
    final ThreadLocal<AtomicLong> threadOtherExerciseIndex =
        new ThreadLocal<>();

    /** The number of blocks processed, for progress reporting. */
    protected final AtomicLong completedBlocks = new AtomicLong();

    /** Performance information about datacheck operations. */
    private final Map<String, DCOpInfo> dcOpInfoMap =
        synchronizedMap(new HashMap<>());

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
     * @param requestTimeout the maximum permitted timeout in milliseconds for
              store operations, including the network read timeout
     * @param blockTimeout the timeout in milliseconds for a set of operations
     *        on a block of entries
     * @param useParallelScan use parallel scan in exercise phase if specified.
     * @param scanMaxConcurrentRequests the max concurrent threads are allowed
     *        for a parallel scan.
     * @param scanMaxResultsBatches the max batches to temporally store the
     *        parallel scan results, each batch can store one scan results.
     * @param maxThroughput max throughput for the client, in ops/second.
     * @param maxExecTime the max execution for the client workload, in seconds.
     */
    public DataCheck(KVStore store,
                     KVStoreConfig config,
                     long reportingInterval,
                     int[] partitions,
                     long seed,
                     int verbose,
                     PrintStream err,
                     long requestTimeout,
                     long blockTimeout,
                     boolean useParallelScan,
                     int scanMaxConcurrentRequests,
                     int scanMaxResultsBatches,
                     double maxThroughput,
                     int maxExecTime) {
        this.store = store;
        dataSerializer = new DataSerializer();

        if (partitions != null) {
            if (partitions.length == 0) {
                throw new IllegalArgumentException
                    ("partitions must not be empty");
            }
        }

        if (partitions == null) {
            this.partitions = null;
        } else {
            this.partitions = new HashSet<>();
            for (int p : partitions) {
                this.partitions.add(p);
            }
        }

        this.reportingInterval = reportingInterval;
        this.verbose = verbose;
        this.err = err;
        this.requestTimeout = requestTimeout;
        this.blockTimeout = blockTimeout;

        this.useParallelScan = useParallelScan;
        this.storeIteratorConfig = new StoreIteratorConfig();
        this.storeIteratorConfig.
            setMaxConcurrentRequests(scanMaxConcurrentRequests);
        this.maxThroughput = maxThroughput;
        this.maxExecTime = maxExecTime;
    }

    /**
     * persist the random number generator seed in populate phase
     * use this seed to initialize the random number generator in all phases
     * initialize the major keys and operations Permutation
     * this method needs to be called from the subclass constructor
     */
    protected void initializeRandom(long seed) {
        random = getRandom(seed);
        permuteMajor = new Permutation(random.nextLong());
        permuteOperation = new Permutation(random.nextLong());
    }

    /**
     * Return a random number generator given the specified seed, checking and
     * updating the stored value.
     */
    private Random getRandom(long seed) {
        ValueVersion vv = faultingGetInternal(SEED_KEY);
        long storeSeed =
            (vv == null) ? -1 : dataSerializer.valueToSeed(vv.getValue());
        boolean update = false;

        if (seed == -1) {
            if (storeSeed == -1) {
                seed = System.currentTimeMillis();
                update = true;
            } else {
                seed = storeSeed;
            }
        } else if (storeSeed != seed) {
            if (storeSeed != -1) {
                unexpectedResult
                    ("Using seed " + seed +
                     ", which differs from previous seed " + storeSeed);
            }
            update = true;
        }

        if (update) {
            faultingPutInternal(SEED_KEY,
                                dataSerializer.seedToValue(seed),
                                Durability.COMMIT_SYNC);
        }

        log("Using seed " + seed);
        return new Random(seed);
    }

    protected void removeStoreSeed() {
        log("Removing store seed");
        faultingMultiDeleteInternal(SEED_KEY, Durability.COMMIT_SYNC);
    }

    /* Phases */

    /**
     * Update the remote TTL test hook.
     * In populate phase, the remote TTL current time will be set to client
     * current time when the client call this method.
     * In exercise and check phase, the remote TTL current time will be set to
     * the MEDIUM_TTL_DAY. This will make half of TTL records expired.
     * Do nothing for clean phase.
     * @param phase The current Datacheck testing phase.
     * @param start Start index of the phase. Will be used to Identify the
     * phase.
     * @param count Count number of indexes of the phase. Will be used to
     * Identify the phase.
     */
    protected abstract void updateTTLTestHook(Phase phase,
                                              long start,
                                              long count);

    /**
     * Populate the store with entries.
     *
     * @param start the starting index
     * @param count the number of operations
     * @param threads the number of threads
     * @throws IllegalArgumentException if either {@code start} or {@code
     *         count} is negative, if {@code start} is not a multiple of {@link
     *         #BLOCK_COUNT}, if {@code count} is not a multiple of the product
     *         of {@code BLOCK_COUNT} and {@code threads}, or if the sum of
     *         {@code start} and {@code count} is greater than {@link
     *         #MAX_INDEX}
     * @throws IllegalStateException if a test phase has already been started
     *         for this instance
     * @throws TestFailedException if the test fails
     */
    public void populate(long start, long count, int threads) {
        updateTTLTestHook(Phase.POPULATE, start, count);
        long startTime = System.currentTimeMillis();
        ReportingThread reportingThread = start(start, count, threads);
        try {
            showHistory();
            startUpdate("populate", start, count);
            List<PhaseThread> threadList = new ArrayList<>();
            long threadStart = start;
            long threadCount = count / threads;
            /* Calculate the throughput per thread per ms for populate phase */
            double targetThrptMs = maxThroughput > 0 ?
                                   maxThroughput/1000/threads :
                                   0;
            for (int i = 0; i < threads; i++) {
                PhaseThread t = createPopulateThread(threadStart, threadCount,
                    targetThrptMs);
                threadList.add(t);
                t.start();
                threadStart += threadCount;
            }
            reportingThread.startThrPutReport(threadList);
            startTerminatorThread(threadList);
            join(threadList);
        } catch (Throwable e) {
            unexpectedExceptionWithContext("During populate phase", e);
        } finally {
            finish(Phase.POPULATE, startTime, reportingThread,
                   true /* finishUpdate */, start, count);
        }
    }

    protected PhaseThread createPopulateThread(long start,
                                               long count,
                                               double targetThrptMs) {
        return new PopulateThread(start, count, targetThrptMs);
    }

    /**
     * Exercise the store with read and update operations after it has
     * completed the populate phase. For the threads in a pair, it supports
     * different ways of testing. This is to support mixed mode testing, like
     * testing sync style API and async style API in a run, or(inclusive-or)
     * testing RMI and async protocols in a run.
     *
     * @param start the starting index
     * @param count the number of operations
     * @param threads the number of threads
     * @param readPercent the percentage of read operations
     * @param noCheck whether to suppress checking the results of update
     *        operations
     * @param pairDc the 'DataCheck' object used by thread B
     * @throws IllegalArgumentException if either {@code start} or {@code
     *         count} is negative, if {@code start} is not a multiple of {@link
     *         #BLOCK_COUNT}, if {@code count} is not a multiple of the product
     *         of {@code BLOCK_COUNT} and {@code threads}, if the sum of {@code
     *         start} and {@code count} is greater than {@link #MAX_INDEX}, or
     *         if readPercent is less than 0.0 or greater than 100.0
     * @throws IllegalStateException if a test phase has already been started
     *         for this instance
     * @throws TestFailedException if the test fails
     */
    public void exercise(long start,
                         long count,
                         int threads,
                         double readPercent,
                         boolean noCheck,
                         DataCheck<K,V,O> pairDc) {
        if (readPercent < 0.0 || readPercent > 100.0) {
            throw new IllegalArgumentException
                ("readPercent must be between 0.0 and 100.0: " + readPercent);
        }

        /*
         * When passing null, it means thread B uses the same 'DataCheck'
         * object as thread A, so not doing mixed mode testing.
         */
        if (pairDc == null) {
            pairDc = DataCheck.this;
        }

        updateTTLTestHook(Phase.EXERCISE, start, count);
        exerciseNoCheck = noCheck;
        long startTime = System.currentTimeMillis();
        ReportingThread reportingThread = start(start, count, threads);
        /* Calculate the throughput per thread per ms for exercise phase */
        double targetThrptMs = maxThroughput > 0 ?
                               maxThroughput/1000/(threads*2) :
                               0;
        try {
            showHistory();
            if (readPercent != 100.0) {
                startUpdate("exercise", start, count);
            }
            List<PhaseThread> threadList = new ArrayList<>();
            long threadStart = start;
            long threadCount = count / threads;
            for (int i = 0; i < threads; i++) {
                CyclicBarrier barrier = new CyclicBarrier(2);
                AtomicLong exerciseIndexA = new AtomicLong();
                AtomicLong exerciseIndexB = new AtomicLong();
                PhaseThread t = createExerciseThread(threadStart, threadCount,
                    true, barrier, exerciseIndexA, exerciseIndexB, readPercent,
                        targetThrptMs);
                threadList.add(t);
                t.start();
                t = pairDc.createExerciseThread(threadStart, threadCount,
                                                   false, barrier,
                                                   exerciseIndexB,
                                                   exerciseIndexA, readPercent,
                                                   targetThrptMs);
                threadList.add(t);
                t.start();
                threadStart += threadCount;
            }
            reportingThread.startThrPutReport(threadList);
            startTerminatorThread(threadList);
            join(threadList);
        } catch (Throwable e) {
            unexpectedExceptionWithContext("During exercise phase", e);
        } finally {
            finish(Phase.EXERCISE, startTime, reportingThread,
                   readPercent != 100.0, /* finishUpdate */
                   start, count);
        }
    }

    public void exercise(long start,
                         long count,
                         int threads,
                         double readPercent,
                         boolean noCheck) {
        exercise(start, count, threads, readPercent, noCheck, DataCheck.this);
    }

    /** Create exercise thread. */
    protected PhaseThread createExerciseThread(long threadStart,
                                long threadCount,
                                boolean firstThread,
                                CyclicBarrier barrier,
                                AtomicLong exerciseIndex,
                                AtomicLong otherExerciseIndex,
                                double readPercent,
                                double targetThrPerMs) {
        return new ExerciseThread(threadStart, threadCount, firstThread,
            barrier, exerciseIndex, otherExerciseIndex, readPercent,
                targetThrPerMs);
    }

    /**
     * Check the contents of the store after it has been updated by the
     * exercise phase.
     *
     * @param start the starting index
     * @param count the number of operations
     * @param threads the number of threads
     * @throws IllegalArgumentException if either {@code start} or {@code
     *         count} is negative, if {@code start} is not a multiple of {@link
     *         #BLOCK_COUNT}, if {@code count} is not a multiple of the product
     *         of {@code BLOCK_COUNT} and {@code threads}, or if the sum of
     *         {@code start} and {@code count} is greater than {@link
     *         #MAX_INDEX}
     * @throws IllegalStateException if a test phase has already been started
     *         for this instance
     * @throws TestFailedException if the test fails
     */
    public void check(long start, long count, int threads) {
        updateTTLTestHook(Phase.CHECK, start, count);
        long startTime = System.currentTimeMillis();
        ReportingThread reportingThread = start(start, count, threads);
        try {
            showHistory();
            List<PhaseThread> threadList = new ArrayList<>();
            long threadStart = start;
            long threadCount = count / threads;
            /* Calculate the throughput per thread per ms for check phase */
            double targetThrptMs = maxThroughput > 0 ?
                                   maxThroughput/1000/threads :
                                   0;
            for (int i = 0; i < threads; i++) {
                PhaseThread t = createCheckThread(threadStart, threadCount,
                    targetThrptMs);
                threadList.add(t);
                t.start();
                threadStart += threadCount;
            }
            reportingThread.startThrPutReport(threadList);
            startTerminatorThread(threadList);
            join(threadList);
            synchronized (lagSync) {
                if (lagMax > Op.MultiGetIteratorCheckOp.CHECK_INDEX) {
                    throw new RuntimeException(
                        String.format(
                            "A maximum lag count this large (%d ms) during" +
                            " the check phase means exercise phase failed",
                            lagMax));
                }
            }
        } catch (Throwable e) {
            unexpectedExceptionWithContext("During check phase", e);
        } finally {
            finish(Phase.CHECK, startTime, reportingThread,
                   false /* finishUpdate */, 0, 0);
        }
    }

    protected PhaseThread createCheckThread(long start,
                                            long count,
                                            double targetThrptMs) {
        return new CheckThread(start, count, targetThrptMs);
    }

    /**
     * Clean the store with entries.
     *
     * @param start the starting index
     * @param count the number of operations
     * @param threads the number of threads
     * @throws IllegalArgumentException if either {@code start} or {@code
     *         count} is negative, if {@code start} is not a multiple of {@link
     *         #BLOCK_COUNT}, if {@code count} is not a multiple of the product
     *         of {@code BLOCK_COUNT} and {@code threads}, or if the sum of
     *         {@code start} and {@code count} is greater than {@link
     *         #MAX_INDEX}
     * @throws IllegalStateException if a test phase has already been started
     *         for this instance
     * @throws TestFailedException if the test fails
     */
    public void clean(long start, long count, int threads) {
        long startTime = System.currentTimeMillis();
        ReportingThread reportingThread = start(start, count, threads);

        /* reuse exercise's delete operations without checking */
        exerciseNoCheck = true;
        try {
            showHistory();
            startUpdate("clean", start, count);
            List<PhaseThread> threadList = new ArrayList<>();
            long threadStart = start;
            long threadCount = count / threads;
            /* Calculate the throughput per thread per ms for clean phase */
            double targetThrptMs = maxThroughput > 0 ?
                                   maxThroughput/1000/threads :
                                   0;
            for (int i = 0; i < threads; i++) {
                PhaseThread t = createCleanThread(threadStart, threadCount,
                    targetThrptMs);
                threadList.add(t);
                t.start();
                threadStart += threadCount;
            }
            reportingThread.startThrPutReport(threadList);
            startTerminatorThread(threadList);
            join(threadList);
            removeStoreSeed();
        } catch (Throwable e) {
            unexpectedExceptionWithContext("During clean phase", e);
        } finally {
            finish(Phase.CLEAN, startTime, reportingThread,
                   true /* finishUpdate */, start, count);
        }
    }

    protected PhaseThread createCleanThread(long start,
                                            long count,
                                            double targetThrptMs) {
        return new CleanThread(start, count, targetThrptMs);
    }

    protected void startTerminatorThread(List<PhaseThread> listOfTarget) {
        if (maxExecTime > 0) {
            TerminatorThread terminator =
                new TerminatorThread(listOfTarget);
            terminator.start();
        }
    }

    protected void setTestStateActive()
    {
        if (!testState.compareAndSet(TestState.INIT, TestState.ACTIVE)) {
            throw new IllegalStateException
                ("A test has already started for this instance");
        }
    }

    private ReportingThread start(long start, long count, int threads) {
        if (start < 0 || (start % BLOCK_COUNT) != 0) {
            throw new IllegalArgumentException
                ("Start must be a non-negative multiple of " + BLOCK_COUNT +
                 ": " + start);
        }
        if (count < 0 || (count % (BLOCK_COUNT * threads)) != 0) {
            throw new IllegalArgumentException
                ("Count must be a non-negative multiple of " + BLOCK_COUNT +
                 " * threads = " + (BLOCK_COUNT * threads) + ": " + count);
        }
        if (start + count > MAX_INDEX) {
            throw new IllegalArgumentException
                ("Start plus count must not be greater than " +
                 MAX_INDEX + ": " + (start + count));
        }
        setTestStateActive();
        log("Starting");
        ReportingThread reportingThread = new ReportingThread();
        reportingThread.start();
        return reportingThread;
    }

    private void finish(Phase phase,
                        long startTime,
                        ReportingThread reportingThread,
                        boolean finishUpdate,
                        long updateStart,
                        long updateCount) {
        reportingThread.done();
        final long time = System.currentTimeMillis() - startTime;

        log("Final storeOpCounts=" + getPhaseOpCounts(phase, reportingThread));
        log("Final datacheckOpInfo=" + dcOpInfoMap);

        final long unexpectedExceptions = unexpectedExceptionCount.get();
        final long unexpectedResults = unexpectedResultCount.get();
        final boolean passed = testCompleted();
        if (finishUpdate) {
            finishUpdate(phase.toString().toLowerCase(), updateStart,
                         updateCount);
        }

        long lagCount1;
        long lagAverage;
        long lagMax1;
        synchronized (lagSync) {
            lagCount1 = lagCount;
            lagAverage = lagCount != 0 ? lagSum / lagCount : 0;
            lagMax1 = lagMax;
        }
        long count = putCount.get();
        double keySize = 0;
        double valueSize = 0;
        if (count > 0) {
            keySize = ((double) putKeyBytes.get()) / count;
            valueSize = ((double) putValueBytes.get()) / count;
        }

        StringBuilder sb = new StringBuilder();
        Formatter fmt = new Formatter(sb);
        fmt.format("Result: %s", (passed ? "Passed" : "Failed"));
        if (unexpectedExceptions > 0) {
            fmt.format("\n  UNEXPECTED_EXCEPTIONS=%d",
                       unexpectedExceptions);
        }
        if (unexpectedResults > 0) {
            fmt.format("\n  UNEXPECTED_RESULTS=%d",
                       unexpectedResults);
        }
        fmt.format("\n  timeMillis=%d", time);
        fmt.format("\n  retryCount=%d", retryCount.get());
        fmt.format("\n  otherRetryResults=%d", otherRetryResultCount.get());
        fmt.format("\n  lagCount=%d", lagCount1);
        fmt.format("\n  lagAverage=%d", lagAverage);
        fmt.format("\n  lagMax=%d", lagMax1);
        fmt.format("\n  putCount=%d", count);
        fmt.format("\n  keySize=%.1f", keySize);
        fmt.format("\n  valueSize=%.1f", valueSize);
        String moreInfo = getStatInfoEx();
        if (moreInfo != null) {
            fmt.format("%s", moreInfo);
        }
        fmt.close();
        String message = sb.toString();
        log(message);
        if (!passed) {
            throw new TestFailedException(message);
        }
    }

    /** Use to add more statistic information. */
    String getStatInfoEx() {
        return null;
    }

    /**
     * Called to return the op counts at the end of the specified phase, or
     * null if no information should be reported. The map only contains entries
     * for operations called at least once.
     */
    Map<String, Long> getPhaseOpCounts(@SuppressWarnings("unused")
                                       Phase phase,
                                       ReportingThread reportingThread) {
        return reportingThread.opCounts;
    }

    /**
     * The test is completed -- update the test state, making sure that the
     * decision about whether the test passed or failed is only made once.
     * Returns whether the test passed.
     */
    protected boolean testCompleted() {
        assert testState.get() != TestState.INIT;
        if (testState.get() == TestState.ACTIVE) {
            boolean passed = (unexpectedExceptionCount.get() == 0) &&
                (unexpectedResultCount.get() == 0);
            testState.compareAndSet(
                TestState.ACTIVE, passed ? TestState.PASSED : TestState.FAILED);
        }
        return testState.get() == TestState.PASSED;
    }

    private void showHistory() {
        StringBuilder sb = new StringBuilder();
        sb.append("Update history:");
        Set<Key> history =
            faultingMultiGetKeysInternal(Key.createKey(UPDATE_HISTORY));
        if (history.isEmpty()) {
            sb.append(" Empty");
        } else {
            for (Key key : history) {
                sb.append("\n ").append(key);
            }
        }
        log(sb.toString());
    }

    private void startUpdate(String phase, long start, long count) {
        Formatter minor = new Formatter();
        String timestamp = getTimestamp();
        minor.format("%s-%s-started-start-%#x-count-%#x",
                     timestamp, phase, start, count);
        if (partitions != null) {
            minor.format("-partitions");
            for (int partition : partitions) {
                minor.format("-%d", partition);
            }
        }
        faultingPutInternal(Key.createKey(UPDATE_HISTORY, minor.toString()),
                            EMPTY_VALUE,
                            Durability.COMMIT_SYNC);
        minor.close();
    }

    private void finishUpdate(String phase,
                              long start,
                              long count) {
        boolean success = testCompleted();
        String timestamp = getTimestamp();
        String minor = String.format("%s-%s-%s-start-%#x-count-%#x",
                                     timestamp, phase,
                                     (success ? "passed" : "failed"),
                                     start, count);
        faultingPutInternal(Key.createKey(UPDATE_HISTORY, minor),
                            EMPTY_VALUE,
                            Durability.COMMIT_SYNC);
    }

    private void join(Collection<? extends Thread> threads) {
        for (Thread t : threads) {
            while (true) {
                try {
                    t.join();
                    break;
                } catch (InterruptedException e) {
                    unexpectedExceptionWithContext
                        ("Waiting for threads -- continuing", e);
                    continue;
                }
            }
        }
    }

    /* Check values */

    /**
     * Check that the previous value returned by an exercise update operation
     * was expected.
     *
     * <p>The checkPreviousValue method is called when an exercise thread has
     * just performed an update operation on the store, and has obtained the
     * value that was present before the operation was performed.  In some
     * cases -- e.g. delete -- the operation only has access to whether or not
     * the key had an associated value, but not the value itself, so the check
     * is limited to checking whether or not a previous value should be found.
     * In other cases -- e.g. put with ReturnValueVersion.Choice.NONE -- the
     * operation returns no previous value information, so the check is not
     * performed.
     *
     * <p>There are three types of operations that can store a value for a
     * given key: inserting a value during the populate phase, performing an
     * operation in exercise thread A, and performing an operation in exercise
     * thread B.  For a given key, we can determine which of these operations
     * should have occurred for the key, what value was supplied, and, for the
     * exercise operations, the position (index) of the operation in the
     * ordered series of exercise operations for the particular exercise
     * thread.
     *
     * <p>In normal circumstances, where each exercise operation is performed
     * just once by DataCheck, the previous value should have been generated in
     * one of the following ways:
     *
     * <ul>
     *
     * <li>No populate performed, no operation in other exercise thread.  There
     * should be no previous value.
     *
     * <li>No populate performed, operation performed in other exercise thread.
     * The previous value should be the one returned by the operation in the
     * other exercise thread when performed with no previous entry present.
     *
     * <li>Populate performed, no operation in other exercise thread.  The
     * previous value should be the populate value.
     *
     * <li>Populate performed, operation performed in other exercise thread.
     * The previous value should be the one returned by the operation in the
     * other exercise thread when performed with the populate value present.
     *
     * </ul>
     *
     * The two exercise threads run independently without synchronization, so,
     * although the indexes of operations in the two threads gives an
     * indication of how likely it is that one operation preceded the other,
     * that order is not guaranteed.  For that reason, the system also reports
     * a "lag" value that represents how far out of order the operations in the
     * two exercise threads would need to be in order to explain the observed
     * value.
     *
     * <p>If DataCheck encounters an error when performing an operation, say
     * because it is being used in a test that injects failures, it will always
     * retry the operation, even though the operation may have succeeded on the
     * server before the error occurred.  Uncertainty about the completion of
     * operations is in the nature of failures in a distributed environment, so
     * it seems appropriate to retry these cases to check for unexpected
     * behavior.  As a result, retrying an operation can produce other
     * sequences of operations that lead to different results from the ones
     * listed above, specifically for operations that depend on previous
     * values.  A simple case involves retrying a single operation, but there
     * are others.  For example:
     *
     * <ol>
     *
     * <li>Exercise A operation succeeds on the server but fails before the
     * call can return to data check
     *
     * <li>Exercise B operation succeeds using the previous value specified by
     * the exercise A operation
     *
     * <li>Exercise A operation is retried using the previous value from step 2
     *
     * </ol>
     *
     * Because these other orderings are unusual and we don't want to further
     * complicate the logic that determines expected previous values, we modify
     * the check logic when retrying to confirm that the previous value is one
     * of the values that could have been inserted (populate, exercise A,
     * exercise B) or a missing value.
     *
     * @param key the operation key
     * @param keynum the associated keynum
     * @param previousValue the previous value returned by the operation
     * @param currentIndex the current index
     * @param otherThreadIndex the index in the other exercise thread
     * @param firstThread whether the caller is the first exercise thread
     * @param checkPresentOnly whether to only check for the presence of the
     *        previous value
     * @param retrying whether this operation is in the progress of retrying
     * @return false if a problem was detected, otherwise true
     */
    boolean checkPreviousValue(K key,
                               long keynum,
                               V previousValue,
                               long currentIndex,
                               long otherThreadIndex,
                               boolean firstThread,
                               boolean checkPresentOnly,
                               boolean retrying) {
        if (exerciseNoCheck) {
            return true;
        }
        long populateIndex = keynumToPopulateIndex(keynum);

        /* The index of the operation in the other exercise thread */
        long exerciseIndex = keynumToExerciseIndex(keynum, !firstThread);
        boolean found = false;
        /* The shortest lag of any lagging value */
        long lag = Long.MAX_VALUE;
        StringBuilder ops = new StringBuilder();
        V populateValue =
            (populateIndex != -1) ? getPopulateValue(populateIndex) : null;

        /*
         * Populate value appears: exercise operation in the other thread did
         * not occur or should occur after the current time
         */
        if (isExpectedValue(previousValue, populateValue, checkPresentOnly)) {
            if (exerciseIndex == -1 || otherThreadIndex < exerciseIndex) {
                return true;
            }
            ops.append("pp,");
            found = true;

            /*
             * If the thread and exercise indices are the same, then we don't
             * know if the operation has occurred, because the combination of
             * incrementing the index and performing the operation are not
             * protected by a transaction.  Treat that case as a lag of 1.
             */
            lag = max(1, otherThreadIndex - exerciseIndex);
        }
        UpdateOpType<K, V, O> exerciseOpType = null;
        V defaultExerciseValue = null;
        V exerciseValue = null;
        if (exerciseIndex != -1) {
            defaultExerciseValue =
                getExerciseValue(exerciseIndex, !firstThread);
            exerciseOpType = getUpdateOpType(exerciseIndex, !firstThread);
            exerciseValue =
                getResult(exerciseOpType, defaultExerciseValue, populateValue);

            /*
             * Exercise value appears: exercise operation occurred before the
             * current time
             */
            if (isExpectedValue(previousValue, exerciseValue,
                                checkPresentOnly)) {
                if (exerciseIndex < otherThreadIndex) {
                    return true;
                }
                ops.append("e").append(!firstThread ? "a" : "b");
                found = true;
                long thisLag = max(1, exerciseIndex - otherThreadIndex);
                /* Favor the explanation with the lowest lag */
                lag = min(lag, thisLag);
            }
        }
        boolean isOtherRetry = false;
        V otherRetryValue = null;
        if (retrying && !found) {
            /*
             * If we are retrying, allow null, or either of the exercise thread
             * values, because retrying opens up other possible orders of
             * operations and could produce any of these results.  The populate
             * value was already checked, so skip that here.
             */
            if (isExpectedValue(previousValue, null, checkPresentOnly)) {
                isOtherRetry = true;
            } else if (isExpectedValue(previousValue, defaultExerciseValue,
                                       checkPresentOnly)) {
                isOtherRetry = true;
                otherRetryValue = defaultExerciseValue;
            } else {
                final V thisExerciseValue =
                    getExerciseValue(currentIndex, firstThread);
                if (isExpectedValue(previousValue, thisExerciseValue,
                                    checkPresentOnly)) {
                    isOtherRetry = true;
                    otherRetryValue = thisExerciseValue;
                }
            }
        }
        /* Report problem or info about lag */
        boolean ok = true;
        if (!found || verbose >= VERBOSE) {
            Formatter info = new Formatter();
            UpdateOpType<K, V, O> thisOpType =
                getUpdateOpType(currentIndex, firstThread);
            info.format("currentIndex=%#x, otherThreadIndex=%#x, op=%s" +
                        ", currentThread=%s, keynum=%#x, partition=%d" +
                        ", key=%s",
                        currentIndex, otherThreadIndex, thisOpType,
                        firstThread ? "a" : "b", keynum, getPartitionId(key),
                        keyString(key));
            info.format(", previousValue=%s",
                        valueString(previousValue, checkPresentOnly));
            info.format(", retrying=%s", retrying ? "true" : "false");
            if (populateValue != null) {
                info.format(", populateIndex=%#x, populateValue=%s",
                            populateIndex, valueString(populateValue));
            }
            if (exerciseIndex != -1) {
                info.format(", exercise%sIndex=%#x, exercise%1$sOp=%s" +
                            ", exercise%1$sValue=%s",
                            /* Name of the other thread */
                            (firstThread ? "B" : "A"), exerciseIndex,
                            exerciseOpType, valueString(exerciseValue));
            }
            if (found) {
                log("Lag: " + lag + ", ops=" + ops + " " + info);
            } else if (isOtherRetry) {
                info.format(", otherRetryValue=%s",
                            valueString(otherRetryValue));
                otherRetryResult(info.toString());
            } else if (previousValue == null) {
                unexpectedResult("Missing previous value: " + info);
                ok = false;
            } else {
                unexpectedResult("Unexpected previous value: " + info);
                ok = false;
            }
            info.close();
        }
        if (found) {
            noteLag(lag);
        }
        return ok;
    }

    /**
     * Returns the expected result for an operation given the specified new and
     * previous values.  Inserts errors if requested, which involves selecting
     * one of new value, previous value, or null that is different from the
     * expected value.
     */
    final V getResult(
        UpdateOpType<K, V, O> exerciseOpType, V newValue, V previousValue) {

        final V expectedValue =
            exerciseOpType.getResult(newValue, previousValue);
        if (INSERT_ERRORS && random.nextInt(10000) == 1) {
            final Object[] values = { null, newValue, previousValue };
            final int offset = random.nextInt(values.length);
            for (int i = 0; i < values.length; i++) {
                /* Can't use generic types with arrays */
                @SuppressWarnings("unchecked")
                final V value = (V) values[(i + offset) % values.length];
                if (!isExpectedValue(value, expectedValue, false)) {
                    return value;
                }
            }
        }
        return expectedValue;
    }

    /**
     * Determine if the next operation should be bulkPut (if true) or else
     * regular put (if false). Bulk put is much more expensive, so this allows
     * us to reduce the cost.
     */
    boolean chooseBulkPut() {
        return random.nextInt(500) == 0;
    }

    /**
     * Check if the specified key should have an associated value.
     *
     * @param op the operation performed
     * @param key the key
     * @param indexA the current index in exercise thread A
     * @param indexB the current index in exercise thread B
     */
    void checkKeyPresent(Op<K, V, O> op, K key, long indexA, long indexB) {
        checkValueInternal(op, key, null, indexA, indexB, true);
    }

    /**
     * Check if the store should have an entry with the specified value.
     *
     * @param op the operation performed
     * @param key the key
     * @param value the value
     * @param indexA the current index in exercise thread A
     * @param indexB the current index in exercise thread B
     * @return whether the check passed
     */
    boolean checkValue(Op<K, V, O> op,
                       K key,
                       V value,
                       long indexA,
                       long indexB) {
        return checkValueInternal(op, key, value, indexA, indexB, false);
    }

    /**
     * Get an empty value, which will be used to distinguish between present
     * and missing values (which will be null) when the previous value has not
     * been requested.
     */
    abstract V getEmptyValue();

    /**
     * Check if the store should have the specified entry.
     *
     * <p>The checkValueInternal method is called to perform checks similar to
     * those in {@link #checkPreviousValue}, but applied to the return value of
     * a read-only operation.  These operations can be expected to see the
     * results of one or both exercise thread operations, which produces a
     * larger set of expected operations:
     *
     * <ul>
     *
     * <li>No operations, results in no value
     *
     * <li>Exercise A, produces result of exercise A operation with no previous
     * value
     *
     * <li>Exercise B, produces result of exercise B operation with no previous
     * value
     *
     * <li>Exercise A then exercise B, produces result of exercise operation B
     * with previous value returned by exercise A with no previous value
     *
     * <li>Exercise B then exercise A, produces result of exercise operation A
     * with previous value returned by exercise B with no previous value
     *
     * <li>Populate, results in populate value
     *
     * <li>Populate then exercise A, produces result of exercise A operation
     * with previous value inserted by populate
     *
     * <li>Populate then exercise B, produces result of exercise B operation
     * with previous value inserted by populate
     *
     * <li>Populate, exercise A, then exercise B, produces result of exercise B
     * operation with previous value returned by exercise A with previous value
     * inserted by populate
     *
     * <li>Populate, exercise B, then exercise A, produces result of exercise A
     * operation with previous value returned by exercise B with previous value
     * inserted by populate
     *
     * </ul>
     *
     * Also, as with checkPreviousValue, there are other possible orders of
     * operations if we consider retries.  Since for read operations we don't
     * know if a retry was performed, we check for this possibility in all
     * cases.
     *
     * @param op the operation performed
     * @param key the key
     * @param value the value
     * @param indexA the current index in exercise thread A
     * @param indexB the current index in exercise thread B
     * @param checkPresentOnly whether to perform the check given an entry was
     *        found but the value is unknown
     * @return whether the check passed
     */
    @SuppressWarnings("null")
    private boolean checkValueInternal(Op<K, V, O> op,
                                       K key,
                                       V value,
                                       long indexA,
                                       long indexB,
                                       boolean checkPresentOnly) {
        if (exerciseNoCheck) {
            return true;
        }
        long keynum = keyToKeynum(key);
        if (keynum == -1) {
            /*
             * Unexpected key.
             */
            if (checkPresentOnly || value != null) {
                Formatter msg = new Formatter();
                msg.format("Unexpected key: " +
                           "threadAIndex=%#x, threadBIndex=%#x, op=%s" +
                           ", partition=%d, key=%s",
                           indexA, indexB, op.getName(),
                           getPartitionId(key), keyString(key));
                if (!checkPresentOnly) {
                    msg.format(", value=%s", valueString(value));
                }
                unexpectedResult(msg.toString());
                msg.close();
                return false;
            }
            return true;
        }

        if (checkPresentOnly && (value == null)) {
            value = getEmptyValue();
        }

        long pop = keynumToPopulateIndex(keynum);

        long exA = keynumToExerciseIndex(keynum, true);
        long exB = keynumToExerciseIndex(keynum, false);
        boolean found = false;
        long lag = Long.MAX_VALUE;
        StringBuilder ops = new StringBuilder();
        V popVal = (pop != -1) ? getPopulateValue(pop) : null;
        UpdateOpType<K, V, O> exAOpType = null;
        V defaultExAVal = null;
        V exAVal = null;
        if (exA != -1) {
            exAOpType = getUpdateOpType(exA, true);
            defaultExAVal = getExerciseValue(exA, true);
            exAVal = getResult(exAOpType, defaultExAVal, popVal);
        }
        UpdateOpType<K, V, O> exBOpType = null;
        V defaultExBVal = null;
        V exBVal = null;
        if (exB != -1) {
            exBOpType = getUpdateOpType(exB, false);
            defaultExBVal = getExerciseValue(exB, false);
            exBVal = getResult(exBOpType, defaultExBVal, popVal);
        }

        /*
         * Populate value appears: Exercise ops either didn't occur or both
         * occurred after the current time
         */
        if (isExpectedValue(value, popVal, checkPresentOnly)) {
            if ((exA == -1 || exA > indexA) && (exB == -1 || exB > indexB)) {
                return true;
            }
            ops.append("pp,");
            found = true;
            long thisLag = Long.MAX_VALUE;
            /* Exercise A occurred before current time */
            if (exA != -1 && exA < indexA) {
                thisLag = min(thisLag, indexA - exA);
            }
            /* Exercise B occurred before current time */
            if (exB != -1 && exB < indexB) {
                thisLag = min(thisLag, indexB - exB);
            }

            /*
             * If thisLag is still MAX_VALUE, then all of the comparisons must
             * have been for equal indices.  Use a lag of 1 in this case.
             */
            if (thisLag == Long.MAX_VALUE) {
                thisLag = 1;
            }
            lag = min(lag, thisLag);
        }
        if (exA != -1) {

            /*
             * Exercise A value appears: Exercise A occurred before current
             * time, and either exercise B didn't occur or it occurred after
             * current time
             */
            if (isExpectedValue(value, exAVal, checkPresentOnly)) {
                if ((exA < indexA) && (exB == -1 || exB > indexB)) {
                    return true;
                }
                ops.append("ea,");
                found = true;
                long thisLag = Long.MAX_VALUE;
                /* Exercise A occurred after current time */
                if (exA > indexA) {
                    thisLag = min(thisLag, exA - indexA);
                }
                if (exB != -1) {
                    /* Exercise B occurred before current time */
                    if (exB < indexB) {
                        thisLag = min(thisLag, indexB - exB);
                    }

                    /*
                     * Exercise B is versioned and the results from exercise A
                     * were not seen by exercise B
                     */
                    if (exBOpType.versioned()) {
                        thisLag = min(thisLag, abs(exA - exB));
                    }
                }
                if (thisLag == Long.MAX_VALUE) {
                    thisLag = 1;
                }
                lag = min(lag, thisLag);
            }
        }
        if (exB != -1) {

            /*
             * Exercise B value appears: Exercise B occurred before current
             * time, and either exercise A didn't occur or it occurred after
             * current time
             */
            if (isExpectedValue(value, exBVal, checkPresentOnly)) {
                if ((exB < indexB) && (exA == -1 || exA > indexA)) {
                    return true;
                }
                ops.append("eb,");
                found = true;
                long thisLag = Long.MAX_VALUE;
                /* Exercise B occurred after current time */
                if (exB > indexB) {
                    thisLag = min(thisLag, exB - indexB);
                }
                if (exA != -1) {
                    /* Exercise A occurred before current time */
                    if (exA < indexA) {
                        thisLag = min(thisLag, indexA - exA);
                    }

                    /*
                     * Exercise A is versioned and the results from exercise B
                     * were not seen by exercise A
                     */
                    if (exAOpType.versioned()) {
                        thisLag = min(thisLag, abs(exA - exB));
                    }
                }
                if (thisLag == Long.MAX_VALUE) {
                    thisLag = 1;
                }
                lag = min(lag, thisLag);
            }
        }
        boolean isOtherRetry = false;
        V otherRetryValue = null;
        if (exA != -1 && exB != -1) {
            /* Exercise A occurred, then exercise B */
            V result = getResult(exBOpType, defaultExBVal, exAVal);

            /*
             * Times of exercise relative to current time -- negative if
             * happened before
             */
            long exAOffset = exA - indexA;
            long exBOffset = exB - indexB;
            if (isExpectedValue(value, result, checkPresentOnly)) {

                /*
                 * Exercise A occurred before exercise B, which occurred before
                 * current time
                 */
                if (exAOffset < exBOffset && exB < indexB) {
                    return true;
                }
                ops.append("eab,");
                found = true;
                long thisLag = Long.MAX_VALUE;
                /* Exercise A occurred after exercise B */
                if (exAOffset > exBOffset) {
                    thisLag = min(thisLag, exAOffset - exBOffset);
                }
                /* Exercise B occurred after current time */
                if (exB > indexB) {
                    thisLag = min(thisLag, exB - indexB);
                }
                if (thisLag == Long.MAX_VALUE) {
                    thisLag = 1;
                }
                lag = min(lag, thisLag);
            }
            /* Exercise B occurred, then exercise A */
            result = getResult(exAOpType, defaultExAVal, exBVal);
            if (isExpectedValue(value, result, checkPresentOnly)) {

                /*
                 * Exercise B occurred before exercise A, which occurred before
                 * current time
                 */
                if (exBOffset < exAOffset && exA < indexA) {
                    return true;
                }
                ops.append("eba,");
                found = true;
                long thisLag = Long.MAX_VALUE;
                /* Exercise B occurred after exercise A */
                if (exBOffset > exAOffset) {
                    thisLag = min(thisLag, exBOffset - exAOffset);
                }
                /* Exercise A occurred after current time */
                if (exA > indexA) {
                    thisLag = min(thisLag, exA - indexA);
                }
                if (thisLag == Long.MAX_VALUE) {
                    thisLag = 1;
                }
                lag = min(lag, thisLag);
            }
            /*
             * Maybe some other ordering of exercise A and B operations
             * occurred due to retries.  Allow null or either of the exercise
             * thread values to appear even if the normal operation orderings
             * don't produce them.
             */
            if (!found) {
                if (isExpectedValue(value, null, checkPresentOnly)) {
                    isOtherRetry = true;
                } else if (isExpectedValue(value, defaultExAVal,
                                           checkPresentOnly)) {
                    isOtherRetry = true;
                    otherRetryValue = defaultExAVal;
                } else if (isExpectedValue(value, defaultExBVal,
                                           checkPresentOnly)) {
                    isOtherRetry = true;
                    otherRetryValue = defaultExBVal;
                }
            }
        }
        /* Report problem or info about lag */
        boolean ok = true;
        if (!found || verbose >= VERBOSE) {
            Formatter info = new Formatter();
            info.format("threadAIndex=%#x, threadBIndex=%#x, op=%s" +
                        ", keynum=%#x, partition=%d, key=%s",
                        indexA, indexB, op.getName(),
                        keynum, getPartitionId(key), keyString(key));
            info.format(", value=%s",
                        (value != null ? valueString(value) : "null"));
            if (popVal != null) {
                info.format(", populateIndex=%#x, populateValue=%s",
                            pop, valueString(popVal));
            }
            if (exA != -1) {
                info.format(", exerciseAIndex=%#x, exerciseAOp=%s" +
                            ", exerciseAValue=%s",
                            exA, exAOpType, valueString(exAVal));
            }
            if (exB != -1) {
                info.format(", exerciseBIndex=%#x, exerciseBOp=%s" +
                            ", exerciseBValue=%s",
                            exB, exBOpType, valueString(exBVal));
            }
            if (found) {
                log("Lag: " + lag + ", ops=" + ops + " " + info);
            } else if (isOtherRetry) {
                info.format(", otherRetryValue=%s",
                            valueString(otherRetryValue));
                otherRetryResult(info.toString());
            } else if (value == null) {
                unexpectedResult("Missing value: " + info);
                ok = false;
            } else if (checkPresentOnly) {
                unexpectedResult("No value expected: " + info);
                ok = false;
            } else {
                unexpectedResult("Unexpected value: " + info);
                ok = false;
            }
            info.close();
        }
        if (found) {
            noteLag(lag);
        }
        return ok;
    }

    /**
     * Note that a store value was found which only matches the expected value
     * given that there was a lag in consistency of the specified number of
     * operations.
     */
    void noteLag(long lag) {
        assert lag > 0 : "Lag less than 1: " + lag;
        synchronized (lagSync) {
            lagCount++;
            lagSum += lag;
            lagMax = max(lag, lagMax);
        }
    }

    /**
     * Check whether the value matches the expected value in different
     * comparison modes.
     *
     * @param value the value to be checked.
     * @param expected the value that is expected.
     * @param checkPresentOnly if this parameter is true, we will compare
     *        values in null vs non-null mode. Otherwise we will compare
     *        the contents in the values.
     * @return boolean to indicate whether the value matches the expected
     *         value in different mode.
     */
    private boolean isExpectedValue(V value,
                                    V expected,
                                    boolean checkPresentOnly) {
        if (checkPresentOnly) {
            return (value == null) == (expected == null);
        } else if (value == null) {
            return (expected == null);
        } else {
            return valuesEqual(value, expected);
        }
    }

    /** Check if the given values are equal. */
    abstract boolean valuesEqual(V value1, V value2);

    /* Exercise methods */

    /** Get the update operation type for the specified exercise operation. */
    abstract UpdateOpType<K, V, O> getUpdateOpType(long index,
                                                   boolean firstThread);

    /** Get the permuted index for the specified operation index. */
    long getPermutedIndex(long index, boolean firstThread) {
        long permutedIndex = permuteOperation.transformSixByteLong(index);
        /* Use different values for the two threads */
        if (firstThread) {
            permutedIndex >>= 8;
        }
        return permutedIndex;
    }

    /** Get the update operation for the specified exercise operation. */
    UpdateOp<K, V, O> getUpdateOp(long index, boolean firstThread,
                                          boolean requestPrevValue) {
        UpdateOpType<K, V, O> type = getUpdateOpType(index, firstThread);
        return type.getOp(this, index, firstThread, requestPrevValue);
    }

    /**
     * Get a random operation for the specified operation index, choosing a key
     * from the same block.  Returns null if the random key doesn't fall in the
     * restricted set of partitions, if specified.
     */
    ReadOp<K, V, O> getReadOp(long index, boolean firstThread,
                              Random threadRandom) {
        long keynum = getRandomKeynum(index, threadRandom);
        if (partitions != null && !keynumInPartitions(keynum)) {
            return null;
        }
        return getReadOp(keynum, index, firstThread, threadRandom);
    }

    /** Get the read operation for the specified operation index. */
    abstract ReadOp<K, V, O> getReadOp(long keynum, long index,
        boolean firstThread, Random threadRandom);

    /**
     * Execute a list of operations with the specified timeout, without
     * retrying if FaultException is thrown.
     *
     * @param operations the list of operations
     * @param timeoutMillis the timeout for all the operations
     * @return the operation results
     * @throws IllegalStateException if the operation fails
     */
    abstract List<OpResult<V>> execute(List<O> operations,
                                       long timeoutMillis);

    /**
     * Returns a random keynum chosen in the same block as the specified index.
     */
    static long getRandomKeynum(long index, Random random) {
        long keynum = index << KEYNUM_SHIFT;
        long parent = keynum & PARENT_KEY_FIELD;
        return parent | random.nextInt(BLOCK_MAX);
    }

    /* Keynum and index operations */

    /** Convert an operation index to a keynum. */
    static long indexToKeynum(long index) {
        assert index >= 0 : "Index is negative: " + index;
        return index << KEYNUM_SHIFT;
    }

    /**
     * Convert a keynum to an operation index, returning -1 if the keynum does
     * not correspond to an operation.
     */
    static long keynumToIndex(long keynum) {
        assert keynum >= 0 : "Keynum is negative: " + keynum;
        if ((keynum & KEYNUM_POPULATE_CLEAR_MASK) != 0) {
            return -1;
        }
        return keynum >> KEYNUM_SHIFT;
    }

    /**
     * Convert a keynum to a populate index, returning -1 if the keynum does
     * not correspond to an operation. Calls keynumToIndex by default.
     */
    long keynumToPopulateIndex(long keynum) {
        return keynumToIndex(keynum);
    }

    /**
     * Get the value for the specified populate operation:
     *  It does error checking and inserts errors if requested,
     *  then internally call getPopulateValueInternal() to get
     *  the value.
     */
    final V getPopulateValue(long index) {
        assert index >= 0 : "Index is negative: " + index;
        if (INSERT_ERRORS && random.nextInt(10000) == 1) {
            index ^= random.nextInt(Integer.MAX_VALUE);
        }
        return getPopulateValueInternal(index);
    }

    /** Get the value for the specified populate operation. */
    abstract V getPopulateValueInternal(long index);

    /** Compute the keynum for the specified exercise operation. */
    long exerciseIndexToKeynum(long index, boolean firstThread) {
        if (INSERT_ERRORS && random.nextInt(10000) == 1) {
            index ^= random.nextInt(Integer.MAX_VALUE);
        }
        long keynum = indexToKeynum(index);
        long parent = keynum & PARENT_KEY_FIELD;
        int parentVal = (int) (parent >> PARENT_KEY_SHIFT);
        short blockVal = (short) (keynum & BLOCK_MASK);
        Permutation permutation =
            getExerciseBlockPermutation(parentVal, firstThread);
        short permutedBlockVal = permutation.transformShort(blockVal);
        return parent | (((long) permutedBlockVal) & 0xffff);
    }

    /**
     * Convert a keynum to an index in one of the two series of exercise
     * operations, returning -1 if the keynum does not correspond to an
     * exercise operation for the specified thread.
     */
    long keynumToExerciseIndex(long keynum, boolean firstThread) {
        assert keynum >= 0 : "Keynum is negative: " + keynum;
        if (INSERT_ERRORS && random.nextInt(10000) == 1) {
            keynum ^= random.nextInt(Integer.MAX_VALUE);
        }
        long parent = keynum & PARENT_KEY_FIELD;
        int parentVal = (int) (parent >> PARENT_KEY_SHIFT);
        short blockVal = (short) (keynum & BLOCK_MASK);
        Permutation permutation =
            getExerciseBlockPermutation(parentVal, firstThread);
        short permutedBlockVal = permutation.untransformShort(blockVal);
        return keynumToIndex(parent | (((long) permutedBlockVal) & 0xffff));
    }

    /** Get the value to insert for the specified exercise operation:
     *    It does error checking and inserts errors if requested,
     *    then internally call getExerciseValueInternal() to get
     *    the value.
     */
    final V getExerciseValue(long index, boolean firstThread) {
        assert index >= 0 : "Index is negative: " + index;
        if (INSERT_ERRORS && random.nextInt(10000) == 1) {
            index ^= random.nextInt(Integer.MAX_VALUE);
        }
        if (INSERT_ERRORS && random.nextInt(20000) == 1) {
            throw new RuntimeException("Testing exception");
        }
        return getExerciseValueInternal(index, firstThread);
    }

    /** Get the value to insert for the specified exercise operation. */
    abstract V getExerciseValueInternal(long index, boolean firstThread);

    /**
     * Get the key for permuting exercise operations for the specified block.
     * Use the same key for the entire block so that it permutes the values
     * without duplicates, but a different key each thread and each block, for
     * better coverage.
     */
    private Permutation getExerciseBlockPermutation(int parent,
                                                    boolean firstThread) {
        long key = ((long) parent) << 8;
        key |= (firstThread ? 1 : 2);
        /* TODO: Maybe cache the permutation if a performance issue */
        return new Permutation(key);
    }

    /* Keys and Keynums */

    /** Convert a keynum to the associated key. */
    final Key keynumToKeyInternal(long keynum) {
        assert keynum >= 0 : "Keynum is negative: " + keynum;
        /* /k12345678/12/-/12[.lob] */
        long major = keynum >> MAJOR_KEY_SHIFT;
        long permutedMajor = permuteMajor.transformFiveByteLong(major);
        long parent = permutedMajor >> PARENT_MAJOR_KEY_SHIFT;
        int child = (int) (permutedMajor & CHILD_KEY_MASK);
        int minor = (int) (keynum & MINOR_KEY_MASK);
        List<String> majorPath = new ArrayList<>(2);
        majorPath.add(String.format("k%08x", parent));
        majorPath.add(String.format("%02x", child));
        return Key.createKey(majorPath, minorValueToPath(keynum, minor));
    }

    /** Get minor path. */
    String minorValueToPath(@SuppressWarnings("unused") long keynum,
                            int minor) {
        return String.format("%02x", minor);
    }

    /** Convert a keynum to the associated K object. */
    abstract K keynumToKey(long keynum);

    /** Convert a keynum to the major portion of the associated key. */
    final Key keynumToMajorKeyInternal(long keynum) {
        assert keynum >= 0 : "Keynum is negative: " + keynum;
        /* /k12345678/12 */
        long major = keynum >> MAJOR_KEY_SHIFT;
        long permutedMajor = permuteMajor.transformFiveByteLong(major);
        long parent = permutedMajor >> PARENT_MAJOR_KEY_SHIFT;
        int child = (int) (permutedMajor & CHILD_KEY_MASK);
        List<String> majorPath = new ArrayList<>(2);
        majorPath.add(String.format("k%08x", parent));
        majorPath.add(String.format("%02x", child));
        return Key.createKey(majorPath);
    }

    /** Convert a keynum to the major portion of the associated K object. */
    abstract K keynumToMajorKey(long keynum);

    /** Convert a keynum to the parent portion of the associated key. */
    final Key keynumToParentKeyInternal(long keynum) {
        assert keynum >= 0 : "Keynum is negative: " + keynum;
        /* /k12345678 */
        long major = keynum >> MAJOR_KEY_SHIFT;
        long permutedMajor = permuteMajor.transformFiveByteLong(major);
        long parent = permutedMajor >> PARENT_MAJOR_KEY_SHIFT;
        List<String> majorPath = new ArrayList<>(1);
        majorPath.add(String.format("k%08x", parent));
        return Key.createKey(majorPath);
    }

    /** Convert a keynum to the parent portion of the associated K object. */
    abstract K keynumToParentKey(long keynum);

    /** Convert a key to a keynum, returning -1 if the key is not valid. */
    final long keyToKeynumInternal(Key key) {
        /* /k12345678/12/-/12[.lob] */
        int majorSize = 2;
        List<String> majorPath = key.getMajorPath();
        List<String> minorPath = key.getMinorPath();
        if (majorPath.size() != majorSize || minorPath.size() != 1) {
            return -1;
        }
        int iParent = 0;
        String parentString = majorPath.get(iParent);
        if (!parentString.startsWith("k")) {
            return -1;
        }
        long parent;
        int child;
        int minor;
        try {
            parent = Long.parseLong(parentString.substring(1), 16);
            child = Integer.parseInt(majorPath.get(iParent + 1), 16);
            minor = Integer.parseInt(parseMinorPath(minorPath), 16);
        } catch (NumberFormatException e) {
            return -1;
        }
        if ((parent & ~PARENT_KEY_MASK) != 0 ||
            (child & ~CHILD_KEY_MASK) != 0 ||
            (minor & ~MINOR_KEY_MASK) != 0)
        {
            return -1;
        }
        long major = (parent << PARENT_MAJOR_KEY_SHIFT) | child;
        long permutedMajor = permuteMajor.untransformFiveByteLong(major);
        return (permutedMajor << MAJOR_KEY_SHIFT) | minor;
    }

    /* Parse the minor component. */
    String parseMinorPath(List<String> minorPath) {
        return minorPath.get(0);
    }

    /** Convert a K object to a keynum, returning -1 if the key is not valid. */
    abstract long keyToKeynum(K key);

    /**
     * Checks if the key associated with the keynum is in the partitions
     * specified for this test.
     */
    protected boolean keynumInPartitions(long keynum) {
        return keyInPartitions(keynumToKey(keynum), keynum);
    }

    /**
     * Checks if the key, along with it's associated keynum, is in the
     * partitions specified for this test.
     */
    final boolean keyInPartitionsInternal(Key key, long keynum) {
        if (!(store instanceof KVStoreImpl)) {
            throw new IllegalStateException
                ("Partition filtering only supported for KVStoreImpl");
        }
        KVStoreImpl storeImpl = (KVStoreImpl) store;
        Integer partitionId = storeImpl.getPartitionId(key).getPartitionId();
        return isPartitionInPartitionSet(key, keynum, partitionId);
    }

    /**
     * Checks if the partition (in which the key along with it's associated
     * keynum is present) is in the partitions specified for this test.
     */
    final boolean isPartitionInPartitionSet(Key key,
                                            long keynum,
                                            Integer partitionId) {
        boolean result = partitions.contains(partitionId);
        if (!result && verbose >= DEBUG) {
            log("Not in partitions: keynum=%#x, key=%s, partition=%d",
                keynum, key, partitionId);
        }
        return result;
    }

    /**
     * Checks if the K object, along with it's associated keynum, is in the
     * partitions specified for this test.
     */
    abstract boolean keyInPartitions(K key, long keynum);

    /** Returns the partition ID of key, if known, else -1.*/
    final int getPartitionIdInternal(Key key) {
        if (store instanceof KVStoreImpl) {
            return ((KVStoreImpl) store).getPartitionId(key).getPartitionId();
        }
        return -1;
    }

    /** Returns the partition ID of K object, if known, else -1. */
    abstract int getPartitionId(K key);

    /* Logging and reporting */

    /**
     * Report an unexpected result using a formatted message for the
     * description.
     *
     * @param formatString the format string
     * @param formatArgs the format arguments
     */
    void unexpectedResult(String formatString, Object... formatArgs) {
        unexpectedResult(String.format(formatString, formatArgs));
    }

    /**
     * Report an unexpected result.
     *
     * @param description the description
     */
    void unexpectedResult(String description) {
        unexpectedResultCount.incrementAndGet();
        synchronized (err) {
            log("UNEXPECTED_RESULT: " + description);
            if (verbose >= DEBUG) {
                new Exception("Stack trace").printStackTrace(err);
            }
        }
        if (EXIT_ON_UNEXPECTED_RESULT) {
            throw new TestExitException(this);
        }
    }

    /**
     * Reports a result during a retry that is one of the possible valid
     * exercise values or null, but that is not an expected value for the given
     * exercise operations.
     */
    void otherRetryResult(String description) {
        otherRetryResultCount.incrementAndGet();
        synchronized (err) {
            log("Other retry result: " + description);
            if (verbose >= DEBUG) {
                new Exception("Stack trace").printStackTrace(err);
            }
        }
    }

    /**
     * Log a formatted message to standard error.
     *
     * @param formatString the format string
     * @param formatArgs the format arguments
     */
    void log(String formatString, Object... formatArgs) {
        log(String.format(formatString, formatArgs));
    }

    /**
     * Log a message to standard error.
     *
     * @param message the message
     */
    void log(String message) {
        synchronized (err) {
            err.print(now());
            err.print(" [DataCheck] ");
            err.println(message);
        }
    }

    /**
     * Log an exception and a formatted message to standard error.
     *
     * @param exception exception to log
     * @param formatString the format string
     * @param formatArgs the format arguments
     */
    void logException(Throwable exception,
            String formatString,
            Object... formatArgs) {
        logException(exception, String.format(formatString, formatArgs));
    }

    /**
     * Log an exception and a message to standard error.
     *
     * @param exception exception to log
     * @param message the message
     */
    void logException(Throwable exception, String message) {
        synchronized(err) {
            log(message);
            err.println("Exception stack trace:");
            exception.printStackTrace(err);
        }
    }

    /** Convert a value to a string. */
    abstract String valueString(V value);

    /**
     * Convert value to a specific string used for debugging info.
     */
    String valueString(V value, boolean checkPresentOnly) {
        if (checkPresentOnly) {
            if (value == null) {
                return "not present";
            }
            return "present";
        }
        if (value == null) {
            return "null";
        }
        return valueString(value);
    }

    /** Convert a key to a string. */
    abstract String keyString(K Key);

    /**
     * Note that an unexpected exception was thrown in the specified context.
     */
    void unexpectedExceptionWithContext(String context,
            Throwable exception) {

        /*
         * Handle the cases where TestExitException was thrown by
         * unexpectedResult() or unexpectedException() already and is being
         * caught at the end of the phase.
         */
        if (exception instanceof TestExitException) {
            throw (TestExitException)exception;
        }
        if (exception.getCause() instanceof TestExitException) {
            throw (TestExitException)exception.getCause();
        }

        unexpectedExceptionCount.incrementAndGet();
        synchronized (err) {
            log(context + ": UNEXPECTED_EXCEPTION:");
            exception.printStackTrace(err);
        }
        if (EXIT_ON_UNEXPECTED_EXCEPTION) {
            throw new TestExitException(this);
        }
    }

    /**
     * Tally a put operation on the store.
     *
     * @param key the key
     * @param value the value
     */
    private void _tallyPut(Key key, Value value) {
        putCount.incrementAndGet();
        putKeyBytes.addAndGet(key.toByteArray().length);
        if (value != null) {
            putValueBytes.addAndGet(value.getValue().length);
        }
    }

    /** Tally a put operation on the store.*/
    void tallyPut(K key, V value) {
        putCount.incrementAndGet();
        putKeyBytes.addAndGet(keyLength(key));
        if (value != null) {
            putValueBytes.addAndGet(valueLength(value));
        }
    }

    /** Get the length of the key. */
    abstract int keyLength(K key);

    /** Get the length of the value. */
    abstract int valueLength(V value);

    /* Miscellaneous */

    /** Format the current time into a string. */
    private static String now() {
        return utcDateFormat.format(new Date());
    }

    /** Format the current time into a string, replacing spaces with dashes. */
    protected static String nowDashes() {
        return now().replace(" ", "-");
    }

    /**
     * Generate start and finish timestamp of a phase.
     * Default format is yyyy-MM-dd-HH:mm:ss.SSS-UTC
     */
    protected String getTimestamp() {
        return nowDashes();
    }

    /**
     * Returns the current index of the other exercise thread, throwing an
     * exception if the value is not available, presumably because it is not
     * being called in the exercise phase.
     */
    long getOtherExerciseIndex() {
        AtomicLong atomicLong = threadOtherExerciseIndex.get();
        if (atomicLong == null) {
            throw new IllegalStateException
                ("Other exercise thread index not available");
        }
        return atomicLong.get();
    }

    /**
     * A mechanism for performing an operation that should be retried if it
     * throws FaultException.
     */
    abstract class Faulting<T> {
        /**
         * Perform the operation, retrying if FaultException is thrown until
         * the request timeout expires.
         */
        final T perform() {
            boolean retrying = false;
            long timeout = requestTimeout;
            long stop = System.currentTimeMillis() + timeout;
            while (true) {
                try {
                    return call(timeout, retrying);
                } catch (RuntimeException e) {
                    handleStoreException(e);
                    timeout = stop - System.currentTimeMillis();
                    if (timeout <= 0) {
                        throw new RuntimeException
                            ("Retry timeout exceeded for operation " +
                             this + ": " + e,
                             e);
                    }
                    log("Retrying operation: " + this);
                    retrying = true;
                    retryCount.incrementAndGet();
                }
            }
        }

        /** Perform the operation once. */
        abstract T call(long timeoutMillis, boolean retrying);
    }

    /*
     * Handle an exception thrown by a store operation, returning normally if
     * the operation should be retried, and otherwise rethrowing the exception.
     *
     * The exceptions that this overloading retries:
     * o RequestTimeoutException which can be signaled even if the
     *   requested amount of time has not elapsed
     * o FaultExceptions whose cause is an OperationFaultException, since
     *   that means the operation only affected that one operation, and the
     *   operation should be retried.
     * o remote exceptions
     */
    void handleStoreException(RuntimeException re) {
        if (!(re instanceof FaultException)) {
            throw re;
        }
        final FaultException e = (FaultException) re;
        if (!hasRemoteExceptionCause(e) &&
            !(e instanceof RequestTimeoutException) &&
            !(e.getCause() instanceof OperationFaultException)) {
            throw e;
        }
    }

    /** Note that the number of retries. */
    void noteRetry() {
        retryCount.incrementAndGet();
    }

    /**
     * Checks if a FaultException was caused by a network failure, represented
     * by a RemoteException in sync/RMI calls, and IOException for async.
     */
    static boolean hasRemoteExceptionCause(
        final FaultException exception) {

        /* For sync/RMI calls. */
        if (exception.getCause() instanceof RemoteException) {
            return true;
        }

        /* For async calls. */
        if (exception.getCause() instanceof IOException) {
            return true;
        }

        if (exception.wasLoggedRemotely()) {

            /*
             * Fault exceptions where the original exception occurred remotely
             * do not have a cause, so check the fault class name.
             */
            try {
                final Class<?> causeClass =
                    Class.forName(exception.getFaultClassName());
                return RemoteException.class.isAssignableFrom(causeClass) ||
                    IOException.class.isAssignableFrom(causeClass);
            } catch (ClassNotFoundException e) {
            }
        }
        return false;
    }

    /** A convenience class for faulting operations with no return value. */
    abstract class FaultingVoid extends Faulting<Void> {
        @Override
        final Void call(long timeoutMillis, boolean retrying) {
            run(timeoutMillis, retrying);
            return null;
        }
        abstract void run(long timeoutMillis, boolean retrying);
    }

    class FaultingPut extends FaultingVoid {
        final Key key;
        final Value value;
        final Durability durability;
        FaultingPut(Key key, Value value, Durability durability) {
            this.key = key;
            this.value = value;
            this.durability = durability;
        }
        @Override
        void run(long timeoutMillis, boolean ignore /* retrying */) {
            store.put(key, value, null /* prevValue */, durability,
                      timeoutMillis, MILLISECONDS);
            _tallyPut(key, value);
        }

        @Override
        public String toString() {
            return "FaultingPut[key=" + key + ", value=" + value +
                ", durability=" + durability + "]";
        }
    }

    abstract void faultingPutInternal(Key key,
                                      Value value,
                                      Object durability);

    class FaultingMultiGetKeys extends Faulting<Set<Key>> {
        private final Key key;
        FaultingMultiGetKeys(Key key) {
            this.key = key;
        }
        @Override
        public Set<Key> call(long timeoutMillis,
                             boolean ignore /* retrying */) {
            return store.multiGetKeys(key, null /* subRange */,
                                      Depth.DESCENDANTS_ONLY,
                                      Consistency.ABSOLUTE, timeoutMillis,
                                      MILLISECONDS);
        }
        @Override
        public String toString() {
            return "FaultingMultiGetKeys[key=" + key + "]";
        }
    }

    abstract Set<Key> faultingMultiGetKeysInternal(Key key);

    class FaultingMultiDelete extends FaultingVoid {
        private final Key key;
        private final Durability durability;
        FaultingMultiDelete(Key key, Durability durability) {
            this.key = key;
            this.durability = durability;
        }
        @Override
        void run(long timeoutMillis, boolean ignore /* retrying */) {
            store.multiDelete(key, null, Depth.PARENT_AND_DESCENDANTS,
                              durability, timeoutMillis, MILLISECONDS);
        }
        @Override
        public String toString() {
            return "FaultingMultiDelete[key=" + key +
                ", durability=" + durability + "]";
        }
    }

    abstract void faultingMultiDeleteInternal(Key key, Object durability);

    class FaultingGet extends Faulting<ValueVersion> {
        private final Key key;
        FaultingGet(Key key) {
            this.key = key;
        }
        @Override
        public ValueVersion call(long timeoutMillis,
                                 boolean ignore /* retrying */) {
            return store.get(key, Consistency.ABSOLUTE, timeoutMillis,
                             MILLISECONDS);
        }
        @Override
        public String toString() {
            return "FaultingGet[key=" + key + "]";
        }
    }

    abstract ValueVersion faultingGetInternal(Key key);

    class FaultingOp extends FaultingVoid {
        private final Op<K, V, O> op;
        FaultingOp(Op<K, V, O> op) {
            this.op = op;
        }
        @Override
        public void run(long timeoutMillis, boolean retrying) {
            op.doOp(timeoutMillis, retrying);
        }
        @Override
        public String toString() {
            return "FaultingOp[op=" + op + "]";
        }
    }

    /* Threads */

    /** Base class for phase operations. */
    abstract class PhaseThread extends Thread {
        final long start;
        final long max;
        private final AtomicLong index;
        protected long opsDone;
        private final double targetThrptMs;
        private long startTime;
        protected volatile boolean isStopRequested;

        /**
         * Information about datacheck operations performed by this thread for
         * this phase
         */
        final Map<String, DCOpInfo> phaseDCOpInfoMap = new HashMap<>();

        PhaseThread(String name, long start, long count, double targetThrptMs) {
            this(name, start, count, new AtomicLong(), targetThrptMs);
        }
        PhaseThread(String name, long start, long count,
                    AtomicLong index, double targetThrptMs) {
            super(name);
            this.start = start;
            max = start + count;
            this.index = index;
            this.targetThrptMs = targetThrptMs;
        }

        public void requestStop() {
            isStopRequested = true;
        }

        void runOp(Op<K, V, O> op) {
            final long startNanos = System.nanoTime();
            try {
                new FaultingOp(op).perform();
            } catch (ConsistencyException e) {
                unexpectedException(op, e);
            } catch (RuntimeException e) {
                unexpectedException(op, e);
            } finally {
                final long timeNanos = System.nanoTime() - startNanos;
                phaseDCOpInfoMap.compute(
                    op.getName(),
                    (k, v) -> (v == null ?
                               new DCOpInfo(timeNanos) :
                               v.update(timeNanos)));
            }
        }

        private void unexpectedException(Op<K, V, O> op, Exception e) {
            unexpectedExceptionWithContext(
                String.format("phase=%s, op=%s", getName(), op), e);
        }

        @Override
        public synchronized void start() {
            startTime = System.currentTimeMillis();
            super.start();
        }

        public synchronized long getOpsDone() {
            return opsDone;
        }

        /** Add delay to throttle the operations */
        protected void throttle() {
            if (targetThrptMs == 0) {
                return;
            }
            final long currentDiff = System.currentTimeMillis() - startTime;
            final long targetDiff = Math.round(opsDone/targetThrptMs);
            if (currentDiff < targetDiff) {
                try {
                    sleep(targetDiff - currentDiff);
                }
                catch (InterruptedException e) {
                    // Do nothing
                }
            }
        }

        void threadComplete() {
            phaseDCOpInfoMap.forEach(
                (op, newInfo) -> dcOpInfoMap.compute(
                    op, (k, v) -> v == null ? newInfo : v.update(newInfo)));
        }

        AtomicLong getIndex() {
            return this.index;
        }

        void setIndex(long index) {
            this.index.set(index);
        }

        void unexpectedException(Throwable e) {
            unexpectedExceptionWithContext
                (String.format("phase=%s, index=%#x", getName(), index.get()),
                 e);
        }
    }

    /** Perform populate operations. */
    class PopulateThread extends PhaseThread {
        protected final List<O> operations = new ArrayList<>();

        PopulateThread(long start, long count, double targetThrptMs) {
            super(String.format("PopulateThread[start=%#x, count=%#x]",
                                start, count),
                  start, count, targetThrptMs);
        }

        @Override
        public void run() {
            try {
                for (long index = start; index < max; index++) {
                    if (isStopRequested) {
                        break;
                    }
                    setIndex(index);
                    performPopulateOp(index);
                }
                if (!operations.isEmpty() && !isStopRequested) {
                    executeOps();
                    completedBlocks.incrementAndGet();
                }
            } catch (RuntimeException e) {
                unexpectedException(new AssertionError(e.toString(), e));
            } catch (Error e) {
                unexpectedException(e);
            } finally {
                threadComplete();
            }
        }

        private void performPopulateOp(long index) {
            if ((index % MINOR_KEY_COUNT) == 0 && !operations.isEmpty()) {
                executeOps();
            }
            if ((index % BLOCK_COUNT) == 0) {
                startPopulateBlock(index);
            }
            long keynum = indexToKeynum(index);
            if (INSERT_ERRORS && random.nextInt(10000) == 1) {
                keynum += 1;
            }
            if (partitions != null && !keynumInPartitions(keynum)) {
                return;
            }
            K key = keynumToKey(keynum);
            V value = getPopulateValue(index);
            try {
                O op = getPopulateOp(key, value, keynum, index);
                if (op != null) {
                    operations.add(op);
                }
            } catch (Exception e) {
                unexpectedException(e);
            }
        }

        protected void startPopulateBlock(long index) {
            if (index > start) {
                completedBlocks.incrementAndGet();
            }
            if (verbose >= VERBOSE) {
                log("%s: Starting populate index=%#x", getName(), index);
            }
            long keynum = indexToKeynum(index);
            long parent = (keynum >> PARENT_KEY_SHIFT) & PARENT_KEY_MASK;
            List<String> major =
                Arrays.asList("m", "populateBlock", Long.toString(parent, 16));
            Set<Key> existing =
                faultingMultiGetKeysInternal(Key.createKey(major));
            if (!existing.isEmpty()) {
                unexpectedResult("Populate block already started: " + existing);
            }
            faultingPutInternal(Key.createKey(major, nowDashes()),
                                EMPTY_VALUE,
                                Durability.COMMIT_SYNC);
        }

        protected void executeOps() {
            runOp(new Op.ExecuteOp<>(DataCheck.this, operations));
            opsDone += operations.size();
            operations.clear();
            throttle();
        }
    }

    /** Create a populate operation for putting the specified key and value. */
    abstract O getPopulateOp(K key, V value, long keynum, long index)
        throws Exception;

    /** Perform exercise operations. */
    class ExerciseThread extends PhaseThread {
        protected final boolean firstThread;
        protected final CyclicBarrier barrier;
        protected final AtomicLong otherExerciseIndex;
        protected final Random threadRandom;
        protected final double readRatio;

        ExerciseThread(long start,
                       long count,
                       boolean firstThread,
                       CyclicBarrier barrier,
                       AtomicLong exerciseIndex,
                       AtomicLong otherExerciseIndex,
                       double readPercent,
                       double targetThrptMs) {
            super(String.format("ExerciseThread%s[start=%#x, count=%#x]",
                                (firstThread ? "A" : "B"), start, count),
                  start, count, exerciseIndex, targetThrptMs);
            this.firstThread = firstThread;
            this.barrier = barrier;
            this.otherExerciseIndex = otherExerciseIndex;

            /*
             * Convert from a percentage of reads out of the total collection
             * of reads and writes to a ratio of reads to writes.  That makes
             * it easier to compute the number of reads to perform for each
             * write.
             */
            readRatio = (readPercent == 100) ?
                Double.POSITIVE_INFINITY :
                readPercent / (100.0 - readPercent);
            threadRandom = new Random(random.nextLong());

        }

        @Override
        public void run() {
            try {
                threadOtherExerciseIndex.set(otherExerciseIndex);
                for (long index = start; index < max; index++) {
                    if (isStopRequested) {
                        break;
                    }
                    setIndex(index);
                    performExerciseOp(index);
                }

                waitForComplete();
                if (firstThread) {
                    completedBlocks.incrementAndGet();
                }
            } catch (Exception e) {
                unexpectedException(new AssertionError(e.toString(), e));
            } catch (Error e) {
                unexpectedException(e);
            } finally {
                threadComplete();
            }
        }

        @Override
        void runOp(Op<K, V, O> op) {
            super.runOp(op);
            opsDone ++;
            throttle();
        }

        void waitForComplete() throws Exception {
            /* Do nothing here. */
        }

        private void performExerciseOp(long index)
            throws InterruptedException {
            if ((index % BLOCK_COUNT) == 0) {
                startExerciseBlock(index);
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
                ReadOp<K, V, O> op =
                    getReadOp(index, firstThread, threadRandom);
                if (op != null) {
                    runOp(op);
                }
                return;
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
            for (int i = 0; i < readCount; i++) {
                ReadOp<K, V, O> op =
                    getReadOp(index, firstThread, threadRandom);
                if (op != null) {
                    runOp(op);
                }
            }
            if (partitions != null) {
                long keynum = exerciseIndexToKeynum(index, firstThread);
                if (!keynumInPartitions(keynum)) {
                    return;
                }
            }
            runOp(getUpdateOp(index, firstThread, threadRandom.nextBoolean()));
        }

        protected void startExerciseBlock(long index)
            throws InterruptedException {
            if (index > start) {
                try {
                    barrier.await(blockTimeout, MILLISECONDS);
                } catch (TimeoutException e) {
                    throw new RuntimeException(
                        "Timeout exceeded for block", e);
                } catch (BrokenBarrierException e) {
                    throw new RuntimeException(
                        "Failure in other thread for block", e);
                }
                if (firstThread) {
                    completedBlocks.incrementAndGet();
                }
            }
            if (verbose >= VERBOSE) {
                log("%s: Starting exercise index=%#x", getName(), index);
            }
            long keynum = indexToKeynum(index);
            long parent = (keynum >> PARENT_KEY_SHIFT) & PARENT_KEY_MASK;
            List<String> major =
                Arrays.asList("m", "populateBlock", Long.toString(parent, 16));
            Set<Key> existing =
                faultingMultiGetKeysInternal(Key.createKey(major));
            if (existing.isEmpty()) {
                unexpectedResult("Populate block not started: " + existing);
            }
            if (readRatio != Double.POSITIVE_INFINITY) {
                major = Arrays.asList("m", "exerciseBlock",
                                      firstThread ? "A" : "B",
                                      Long.toString(parent, 16));
                existing = faultingMultiGetKeysInternal(Key.createKey(major));
                if (!existing.isEmpty()) {
                    unexpectedResult("Exercise block already started: " +
                                     existing);
                }
                faultingPutInternal(Key.createKey(major, nowDashes()),
                                    EMPTY_VALUE,
                                    Durability.COMMIT_SYNC);
            }
        }
    }

    /** Perform check operations. */
    class CheckThread extends PhaseThread {

        CheckThread(long start, long count, double targetThrptMs) {
            super(String.format("CheckThread[start=%#x, count=%#x]",
                                start, count),
                  start, count, targetThrptMs);
        }

        @Override
        void runOp(Op<K, V, O> op) {
            super.runOp(op);
            opsDone++;
            throttle();
        }

        @Override
        public void run() {
            try {
                for (long index = start;
                     index < max;
                     index += MINOR_KEY_COUNT)
                {
                    if (isStopRequested) {
                        break;
                    }
                    setIndex(index);
                    if ((index % BLOCK_COUNT) == 0) {
                        if (index > start) {
                            completedBlocks.incrementAndGet();
                        }
                        if (verbose >= VERBOSE) {
                            log("%s: Starting check index=%#x",
                                getName(), index);
                        }
                    }
                    if (partitions != null &&
                        !keynumInPartitions(indexToKeynum(index)))
                    {
                        continue;
                    }
                    runOp(getCheckOp(index));
                }
                completedBlocks.incrementAndGet();
            } catch (RuntimeException e) {
                unexpectedException(new AssertionError(e.toString(), e));
            } catch (Error e) {
                unexpectedException(e);
            } finally {
                threadComplete();
            }
        }
    }

    /** Create a check operation for the specified index. */
    abstract Op<K, V, O> getCheckOp(long index);

    /** Perform clean operations. */
    class CleanThread extends PhaseThread {
        protected final List<O> operations = new ArrayList<>();

        CleanThread(long start, long count, double targetThrptMs) {
            super(String.format("CleanThread[start=%#x, count=%#x]",
                                start, count),
                  start, count, targetThrptMs);
        }

        @Override
        public void run() {
            try {
                for (long index = start; index < max; index++) {
                    if (isStopRequested) {
                        break;
                    }
                    setIndex(index);
                    performCleanOp(index);
                }
                if (!operations.isEmpty() && !isStopRequested) {
                    executeOps();
                    completedBlocks.incrementAndGet();
                }
            } catch (RuntimeException e) {
                unexpectedException(new AssertionError(e.toString(), e));
            } catch (Error e) {
                unexpectedException(e);
            } finally {
                threadComplete();
            }
        }

        private void performCleanOp(long index) {
            if ((index % MINOR_KEY_COUNT) == 0 && !operations.isEmpty()) {
                executeOps();
            }

            if ((index % BLOCK_COUNT) == 0) {
                startCleanBlock(index);
            }

            long keynum = indexToKeynum(index);

            /*
             * Operate on the keynum and one greater than the keynum, which
             * will operate on all keys for this block.
             */
            for (int i = 0; i < 2; i++, keynum++) {
                if (partitions != null && !keynumInPartitions(keynum)) {
                    continue;
                }
                K key = keynumToKey(keynum);
                try {
                    O op = getCleanUpOp(key, index, keynum);
                    if (op != null) {
                        operations.add(op);
                    }
                } catch (Exception e) {
                    unexpectedException(e);
                }
            }
        }

        protected void startCleanBlock(long index) {
            if (index > start) {
                completedBlocks.incrementAndGet();
            }
            if (verbose >= VERBOSE) {
                log("%s: Starting clean index=%#x", getName(), index);
            }
            long keynum = indexToKeynum(index);
            long parent = (keynum >> PARENT_KEY_SHIFT) & PARENT_KEY_MASK;
            renameMarkers(parent);

            List<String> major = Arrays.asList("m", "cleanBlock",
                                  Long.toString(parent, 16));
            faultingPutInternal(Key.createKey(major, nowDashes()),
                                EMPTY_VALUE,
                                Durability.COMMIT_SYNC);
        }

        protected void renameMarkers(long parent) {
            List<String> major = Arrays.asList("m", "populateBlock",
                                               Long.toString(parent, 16));
            renameMarker(major);
            major = Arrays.asList("m", "exerciseBlock", "A",
                                  Long.toString(parent, 16));
            renameMarker(major);
            major = Arrays.asList("m", "exerciseBlock", "B",
                                  Long.toString(parent, 16));
            renameMarker(major);
        }

        private void renameMarker(List<String> major){
            Set<Key> existing =
                faultingMultiGetKeysInternal(Key.createKey(major));
            if (existing.isEmpty()) {
                /* No need to clean markers if it does not exist */
                return;
            }
            for(Key key : existing){
                List<String> cleanedMajor = new ArrayList<>();
                for(int i=0; i < major.size(); i++){
                    if(i == 1){
                        final String cleanedPhase = "cleaned-" + major.get(i);
                        cleanedMajor.add(cleanedPhase);
                        continue;
                    }
                    cleanedMajor.add(major.get(i));
                }
                faultingPutInternal(Key.createKey(cleanedMajor,
                                                  key.getMinorPath()),
                                    EMPTY_VALUE,
                                    Durability.COMMIT_SYNC);
            }

            faultingMultiDeleteInternal(Key.createKey(major),
                                        Durability.COMMIT_SYNC);
        }

        protected void executeOps() {
            runOp(new Op.ExecuteOp<>(DataCheck.this, operations));
            opsDone += operations.size();
            operations.clear();
            throttle();
        }
    }

    /** Create a cleanup operation for the specified key. */
    abstract O getCleanUpOp(K key, long index, long keynum)
        throws Exception;

    /** Report status. */
    class ReportingThread extends Thread {
        final Map<String, Long> opCounts = new TreeMap<>();
        private boolean done = false;
        private List<PhaseThread> threadList;
        private long startTime;

        ReportingThread() {
            super("ReportingThread");
            setDaemon(true);
        }

        public synchronized void startThrPutReport(
            List<PhaseThread> listOfTargets) {
            startTime = System.currentTimeMillis();
            threadList = listOfTargets;
        }

        void done() {
            synchronized (this) {
                done = true;
                notify();
            }
            /* Wait for last report */
            try {
                join(1000);
            } catch (InterruptedException e) {
            }
        }
        @Override
        public void run() {
            long blocks = 0;
            while (true) {
                synchronized (this) {
                    if (done) {
                        break;
                    }
                    try {
                        wait(reportingInterval);
                    } catch (InterruptedException e) {
                    }
                }
                long newCompletedBlocks = completedBlocks.get();
                if (newCompletedBlocks > blocks) {
                    blocks = newCompletedBlocks;
                    log("Completed blocks: " + blocks);
                }
                synchronized (this) {
                    /* Add reporting for throughput and execution time */
                    if (threadList != null && startTime != 0) {
                        long timeDiff =
                            System.currentTimeMillis() - startTime;
                        /* use at least 1 ms, or risk divide by zero, below */
                        if (timeDiff == 0) {
                            timeDiff = 1;
                        }
                        long allOps = 0;
                        for (PhaseThread t: threadList) {
                            allOps += t.getOpsDone();
                        }
                        log("allOps: " + allOps);
                        log("Current throughput: " +
                            allOps*1000/timeDiff + " ops/s");
                        log("Time elapsed: " + (timeDiff/1000) + " seconds");
                    }
                }
                KVStats stats = null;
                if (store != null) {
                    stats = store.getStats(true);
                    log("Stats:\n" + stats);
                }
                /* Stats might be null in some unit test cases */
                if (stats != null) {
                    synchronized (this) {
                        for (final OperationMetrics om :
                                 stats.getOpMetrics()) {
                            final long count = om.getTotalOpsLong();
                            if (count > 0) {
                                opCounts.merge(om.getOperationName(), count,
                                               Long::sum);
                            }
                        }
                    }
                }
            }
        }
    }

    void stopPhaseThreadsSync(List<PhaseThread> threadList) {
        for (PhaseThread t : threadList) {
            t.requestStop();
        }
        for (Thread t : threadList) {
            while (t.isAlive()) {
                try {
                    t.join(5000);
                    if (t.isAlive()) {
                        log("Waiting for thread " +
                            t.getName() + " to complete. ");
                    }
                } catch (InterruptedException e) {
                    // Do nothing.
                }
            }
        }
    }

    /** Terminate threads after specific time. */
    class TerminatorThread extends Thread {
        private final List<PhaseThread> threadList;

        TerminatorThread(List<PhaseThread> listOfTarget) {
            super("TerminatorThread");
            setDaemon(true);
            threadList = listOfTarget;
        }

        @Override
        public void run() {
            try {
                runInternal();
            } catch (Exception e) {
                synchronized (err) {
                    log("Terminator thread failed with exception message: " +
                        e.getMessage());
                    e.printStackTrace(err);
                }
            }
        }

        private void runInternal() {
            try {
                Thread.sleep(maxExecTime * 1000);
            } catch (InterruptedException e) {
                log("TerminatorThread interrupted before " +
                    "specific time: " + maxExecTime + " seconds");
                return;
            }

            log("Maximum execution time: " + maxExecTime +
                " seconds elapsed. Requesting stop for threads");

            stopPhaseThreadsSync(threadList);
        }
    }

    /** Thrown when a test fails. */
    public static class TestFailedException extends RuntimeException {
        private static final long serialVersionUID = 1;
        TestFailedException(String message) {
            super(message);
        }
    }

    /*
     * We throw this exception where we need to stop the test before it is
     * completed, in particular when EXIT_ON_UNEXPECTED_EXCEPTION or
     * EXIT_ON_UNEXPECTED_RESULT flag is set.  This exception may be thrown
     * from any of the DataCheck threads.  We let the main class handle it via
     * Thread.UncaughtExceptionHandler.  The handler may either exit
     * the process, attempt to stop the test gracefully or do something else.
     * For the latter options we provide reference to the DataCheck object
     * with this exception (although the functionality for graceful stop is
     * not implemented yet).
     */
    public static class TestExitException extends RuntimeException {
        private static final long serialVersionUID = 1;
        private final DataCheck<?,?,?> dc;
        TestExitException(DataCheck<?,?,?> dc) {
            super("REQUESTED TEST EXIT");
            this.dc = dc;
        }

        public DataCheck<?,?,?> getDataCheck() {
            return dc;
        }
    }

    /** Store a key and a value. */
    interface KeyVal<K, V> {
        K getKey();
        V getValue();
    }

    /** Store a value and a version, and result of the execution. */
    interface ValVer<V> {
        V getValue();
        Object getVersion();
        boolean getSuccess();
    }

    /** Store the result of an operation. */
    interface OpResult<V> {
        V getPreviousValue();
        boolean getSuccess();
    }

    /** Simple stream for bulk put */
    abstract static class SingleItemEntryStream<E> implements EntryStream<E> {
        private final E entry;
        private boolean supplied;
        private boolean completed;
        private boolean keyExists;

        SingleItemEntryStream(E entry) {
            this.entry = entry;
        }

        /**
         * Returns whether the keys associated with two entries are the same.
         */
        abstract boolean keysMatch(E x, E y);

        /**
         * Wait for the specified number of milliseconds for the processing of
         * the stream to complete. Throws IllegalStateException if the stream
         * is not complete by the timeout.
         */
        synchronized void await(long timeoutMillis) {
            final long until = System.currentTimeMillis() + timeoutMillis;
            while (!completed) {
                final long wait = until - System.currentTimeMillis();
                if (wait <= 0) {
                    throw new IllegalStateException(
                        "Not completed in " + timeoutMillis + " ms");
                }
                try {
                    Thread.sleep(wait);
                } catch (InterruptedException e) {
                    throw new IllegalStateException("Interrupted", e);
                }
            }
        }

        /** Returns whether the key was found to exist. */
        synchronized boolean keyExists() {
            return keyExists;
        }

        @Override
        public String name() {
            return "SingleItemEntryStream[" + entry + "]";
        }

        @Override
        public synchronized E getNext() {
            if (supplied) {
                return null;
            }
            supplied = true;
            return entry;
        }

        @Override
        public synchronized void completed() {
            if (completed) {
                throw new IllegalStateException("Already completed");
            }
            completed = true;
            notifyAll();
        }

        @Override
        public synchronized void keyExists(E e) {
            if (!keysMatch(entry, e)) {
                throw new IllegalStateException(
                    "keyExists for entry " + e +
                    " but original entry was " + entry);
            }
            keyExists = true;
        }

        @Override
        public void catchException(RuntimeException exception, E e) {
            if (!keysMatch(entry, e)) {
                throw new IllegalStateException(
                    "catchException with exception " + exception +
                    " for entry " + e + " but original entry was " + entry);
            }
            throw exception;
        }
    }

    /** The different datacheck phases */
    enum Phase { POPULATE, EXERCISE, CHECK, CLEAN }

    /** Collects counts and total time for a datacheck operation */
    private static class DCOpInfo {
        long count;
        long totalNanos;
        DCOpInfo(long nanos) {
            count = 1;
            totalNanos = nanos;
        }
        DCOpInfo update(long nanos) {
            count++;
            totalNanos += nanos;
            return this;
        }
        DCOpInfo update(DCOpInfo more) {
            count += more.count;
            totalNanos += more.totalNanos;
            return this;
        }
        @Override
        public String toString() {
            return "{count=" + count + " totalNanos=" + totalNanos +
                " avgNanos=" + (totalNanos / count) + "}";
        }
    }

    /* Operations */

    abstract ValVer<V> doGet(K key,
                             Object consistency,
                             long timeoutMillis);

    abstract Collection<V> doMultiGet(K key,
                                      Object consistency,
                                      long timeoutMillis);

    abstract Collection<K> doMultiGetKeys(K key,
                                          Object consistency,
                                          long timeoutMillis);

    abstract Iterator<KeyVal<K, V>> doMultiGetIterator(K key,
                                                       Object consistency,
                                                       long timeoutMillis);

    abstract Iterator<K> doMultiGetKeysIterator(K key,
                                                Object consistency,
                                                long timeoutMillis);

    abstract ValVer<V> doPut(K key, V value,
                             boolean requestPrevValue,
                             long timeoutMillis);

    abstract O createPutOp(K key, V value, boolean requestPrevValue);

    abstract ValVer<V> doPutIfAbsent(K key, V value,
                                     boolean requestPrevValue,
                                     long timeoutMillis);

    abstract O createPutIfAbsentOp(K key, V value, boolean requestPrevValue);

    abstract ValVer<V> doPutIfPresent(K key, V value,
                                      boolean requestPrevValue,
                                      long timeoutMillis);

    abstract O createPutIfPresentOp(K key, V value, boolean requestPrevValue);

    abstract ValVer<V> doPutIfVersion(K key, V value,
                                      Object version,
                                      boolean requestPrevValue,
                                      long timeoutMillis);

    abstract O createPutIfVersionOp(K key, V value, Object version,
                                    boolean requestPrevValue);

    /** Perform a bulk put and return whether the key already existed. */
    abstract boolean doBulkPut(K key, V value, long timeoutMillis);

    abstract ValVer<V> doDelete(K key, boolean requestPrevValue,
                                long timeoutMillis);

    abstract O createDeleteOp(K key, boolean requestPrevValue);

    abstract int doMultiDelete(K key, long timeoutMillis);

    abstract ValVer<V> doDeleteIfVersion(K key, Object version,
                                         boolean requestPrevValue,
                                         long timeoutMillis);

    abstract O createDeleteIfVersionOp(K key, Object version,
                                       boolean requestPrevValue);

    /** Append a string to the end of the key. */
    abstract K appendStringToKey(K key, String x);
}
