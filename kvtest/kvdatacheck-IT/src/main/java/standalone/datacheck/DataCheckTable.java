/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import java.io.PrintStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.ValueVersion;

import standalone.datacheck.UpdateOpType.AbstractUpdateOpType;

/**
 * Implement API for data checking used by {@link DataCheckMain}.
 * This class is for checking Table entries.
 */
public abstract class DataCheckTable<K, V, O> extends DataCheck<K, V, O> {

    protected static final int DEFAULT_DDL_TIMEOUT_MS = 90 * 1000;

    /**
     * The name of the Boolean system property to set to enable the use of
     * query operations. These operations are currently disabled by default.
     */
    public static final String ENABLE_QUERIES_PROP =
        "test.datacheck.enableQueries";

    private static final String ENABLE_QUERIES_DEFAULT = "false";
    protected static final boolean enableQueries =
        Boolean.parseBoolean(
            System.getProperty(ENABLE_QUERIES_PROP, ENABLE_QUERIES_DEFAULT));

    protected final static String TAB_DATACHECK = "DataCheck";
    protected final static String IDX_INDEX = "IdxIndex";
    protected final static String FLD_MAJORKEY1 = "majorKey1";
    protected final static String FLD_MAJORKEY2 = "majorKey2";
    protected final static String FLD_MINORKEY = "minorKey";
    protected final static String FLD_OPERATION = "operation";
    protected final static String FLD_FIRSTTHREAD = "firstThread";
    protected final static String FLD_INDEX = "index";
    protected final static String OPERATION_POPULATE = "POPULATE";
    protected final static String OPERATION_EXERCISE = "EXERCISE";
    protected final static String TAB_DATACHECKMD = "DataCheckMD";
    protected final static String FLD_MDMAJORKEY1 = "mjkey1";
    protected final static String FLD_MDMAJORKEY2 = "mjkey2";
    protected final static String FLD_MDMAJORKEY3 = "mjkey3";
    protected final static String FLD_MDMINORKEY = "mikey";
    protected final static String FLD_MDVALUE = "value";

    /**
     * SQL to create the table. Use "IF NOT EXISTS" in case getting the table
     * returns a spurious null.
     */
    private static final String CREATE_TABLE_SQL = String.format(
        "CREATE TABLE IF NOT EXISTS %s (\n" +
        "    %s STRING,\n" +
        "    %s STRING,\n" +
        "    %s STRING,\n" +
        "    %s ENUM (%s, %s),\n" +
        "    %s BOOLEAN,\n" +
        "    %s LONG,\n" +
        "    PRIMARY KEY (SHARD(%s, %s), %s))",
        TAB_DATACHECK,
        FLD_MAJORKEY1,
        FLD_MAJORKEY2,
        FLD_MINORKEY,
        FLD_OPERATION, OPERATION_POPULATE, OPERATION_EXERCISE,
        FLD_FIRSTTHREAD,
        FLD_INDEX,
        FLD_MAJORKEY1, FLD_MAJORKEY2, FLD_MINORKEY);

    /**
     * SQL to create the metada tatable. Use "IF NOT EXISTS" in case getting
     * the table returns a spurious null.
     */
    private static final String CREATE_MDTABLE_SQL = String.format(
        "CREATE TABLE IF NOT EXISTS %s (\n" +
        "    %s STRING,\n" +
        "    %s STRING,\n" +
        "    %s STRING,\n" +
        "    %s STRING,\n" +
        "    %s LONG,\n" +
        "    PRIMARY KEY (SHARD(%s, %s, %s), %s))",
        TAB_DATACHECKMD,
        FLD_MDMAJORKEY1,
        FLD_MDMAJORKEY2,
        FLD_MDMAJORKEY3,
        FLD_MDMINORKEY,
        FLD_MDVALUE,
        FLD_MDMAJORKEY1, FLD_MDMAJORKEY2, FLD_MDMAJORKEY3, FLD_MDMINORKEY);

    /**
     * SQL to create index. Here, too, use "IF NOT EXISTS" just in case there
     * is the possibility of a race condition.
     */
    private static final String CREATE_INDEX_SQL = String.format(
        "CREATE INDEX IF NOT EXISTS %s ON %s (%s)",
        IDX_INDEX,
        TAB_DATACHECK,
        FLD_INDEX);

    protected final static int METADATA_RETRY = 1000;
    /*
     * The maximum TTL days and minimum TTL days will be set in the test.
     * Note that the days are not related to the actual testing time. During
     * the populate phase, each populate index will hash to a day value between
     * the MINIMUM_TTL_DAYS and MAXIMUM_TTL_DAYS(inclusive). During the
     * exercise and check phase, the TTL current time will be set to
     * MEDIUM_TTL_DAY. This will make half of TTL records expired, the program
     * will check such expectation during phases.
     */
    private final static long MAXIMUM_TTL_DAYS = 1000;
    protected final static long MEDIUM_TTL_DAY = 500;
    private final static long MINIMUM_TTL_DAYS = 0;

    protected final static long MILLI_SECONDS_PER_DAY = 3600L * 24L * 1000L;

    private final static String HASH_ALGORITHM = "MD5";

    /* Update operation types */
    /* 50% chance of adding an entry */
    protected final UpdateOpType<K, V, O> QUERY_INSERT = new QueryInsert();
    protected final UpdateOpType<K, V, O> QUERY_UPSERT = new QueryUpsert();
    /* No effect on number of entries */
    protected final UpdateOpType<K, V, O> QUERY_UPDATE = new QueryUpdate();
    /* 50% chance of removing an entry */
    protected final UpdateOpType<K, V, O> QUERY_DELETE = new QueryDelete();

    final String tableName;
    final String mdtableName;
    final int indexReadPercent;
    private final boolean useTTL;

    /** Objects to create ReadOps and their associated weights */
    private final Map<CreateReadOp<K, V, O>, Integer> readOpWeights;
    /** Total CreateReadOp weights */
    private final int readOpTotalWeights;

    private static final ThreadLocal<MessageDigest> perThreadMessageDigest =
        new ThreadLocal<MessageDigest>() {
            @Override
            protected MessageDigest initialValue() {
                try {
                    return MessageDigest.getInstance(HASH_ALGORITHM);
                } catch (NoSuchAlgorithmException e) {
                    throw new ExceptionInInitializerError(e);
                }
            }
        };

    protected final static String COMPLETE_PRIMARY_KEY_QUERY_STMT =
        "DECLARE $v0 STRING;" +
        " $v1 STRING;" +
        " $v2 STRING;\n" +
        "SELECT * FROM " + TAB_DATACHECK +
        " WHERE " + FLD_MAJORKEY1 + " = $v0" +
        " AND " + FLD_MAJORKEY2 + " = $v1" +
        " AND " + FLD_MINORKEY + " = $v2";

    protected final static String COMPLETE_SHARD_PRIMARY_KEY_QUERY_STMT =
        "DECLARE $v0 STRING;" +
        " $v1 STRING;" +
        " $v2 STRING;\n" +
        "SELECT * FROM " + TAB_DATACHECK +
        " WHERE " + FLD_MAJORKEY1 + " = $v0" +
        " AND " + FLD_MAJORKEY2 + " = $v1" +
        /*
         * Limit the query to a small range starting with the minor
         * key
         */
        " AND " + FLD_MINORKEY + " >= $v2" +
        " AND " + FLD_MINORKEY + " < $v2 || 'z'";

    protected final static String PARTIAL_SHARD_PRIMARY_KEY_QUERY_STMT =
        "DECLARE $v0 STRING;" +
        " $v1 STRING;" +
        " $v2 STRING;\n" +
        "SELECT * FROM " + TAB_DATACHECK +
        " WHERE " + FLD_MAJORKEY1 + " = $v0" +
        /*
         * Limit the query to a small range starting with the
         * second major key and the minor key
         */
        " AND " + FLD_MAJORKEY2 + " >= $v1" +
        " AND " + FLD_MAJORKEY2 + " < $v1 || 'z'" +
        " AND " + FLD_MINORKEY + " >= $v2" +
        " AND " + FLD_MINORKEY + " < $v2 || 'z'";

    protected final static String SECONDARY_KEY_QUERY_STMT =
        "DECLARE $v0 LONG;" +
        "SELECT * FROM " + TAB_DATACHECK +
        " WHERE " + FLD_INDEX + " = $v0";

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
     * @param indexReadPercent index read percentage
     * @param maxThroughput max throughput for the client.
     * @param maxExecTime the max execution for the client workload.
     * @param useTTL whether to test the TTL feature
     */
    public DataCheckTable(KVStore store,
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
                          int indexReadPercent,
                          double maxThroughput,
                          int maxExecTime,
                          boolean useTTL) {

        super(store, config, reportingInterval, partitions,
              seed, verbose, err, requestTimeout, blockTimeout,
              useParallelScan, scanMaxConcurrentRequests, scanMaxResultsBatches,
              maxThroughput, maxExecTime);

        tableName = TAB_DATACHECK;
        mdtableName = TAB_DATACHECKMD;
        this.indexReadPercent = indexReadPercent;
        this.useTTL = useTTL;
        readOpWeights = computeReadOpWeights(getReadOpCosts());
        readOpTotalWeights = readOpWeights.values().stream()
            .reduce(Integer::sum)
            .orElse(0);
    }

    /* Phases */

    /** Create the table and index first, if needed */
    @Override
    public void populate(long start, long count, int threads) {
        maybeInstallSchema();
        super.populate(start, count, threads);
    }

    /** Create the table and index first, if needed */
    @Override
    public void clean(long start, long count, int threads) {
        maybeInstallSchema();
        super.clean(start, count, threads);
    }

    abstract void executeDDL(String stmt);

    abstract boolean doesTableExist(String tName);

    abstract boolean doesIndexExist(String iName);

    protected void maybeInstallSchema() {
        maybeInstallTable(tableName, CREATE_TABLE_SQL);
        maybeInstallTable(mdtableName, CREATE_MDTABLE_SQL);

        if (!doesIndexExist(IDX_INDEX)) {
            boolean done = false;
            Exception ex = null;
            for (int i = 0; i < 10; i++) {
                try {
                    executeDDL(CREATE_INDEX_SQL);
                    log("Created index, ddl=" + CREATE_INDEX_SQL);
                    done = true;
                    break;
                } catch (Exception e) {
                    ex = e;
                }
            }
            if (!done) {
                throw new IllegalStateException(
                    "Failed to create index. Last exception: " + ex, ex);
            }
        }
    }

    private void maybeInstallTable(String name, String statement) {
        if (!doesTableExist(name)) {
            boolean done = false;
            Exception ex = null;
            for (int i = 0; i < 10; i++) {
                try {
                    executeDDL(statement);
                    log("Created table, ddl=" + statement);
                    done = true;
                    break;
                } catch (Exception e) {
                    ex = e;
                }
            }
            if (!done) {
                throw new IllegalStateException(
                    "Failed to create table. Last exception: " + ex, ex);
            }
        }
    }

    /* Exercise methods */

    /** Get the update operation type for the specified exercise operation. */
    @Override
    UpdateOpType<K, V, O> getUpdateOpType(long index, boolean firstThread) {

        long permutedIndex = getPermutedIndex(index, firstThread);
        return updateOpTypes.get((int) (permutedIndex % updateOpTypes.size()));
    }

    /** Get a random read operation for the specified operation index. */
    @Override
    ReadOp<K, V, O> getReadOp(long keynum, long index,
                              boolean firstThread,
                              Random threadRandom) {
        if (chooseIndexReadOp(threadRandom)) {
            return getIndexReadOp(threadRandom, keynum, index, firstThread);
        }
        return getTableReadOp(threadRandom, keynum, index, firstThread);
    }

    /** Determine if choose index read operation. */
    protected boolean chooseIndexReadOp(Random rand) {
        return rand.nextInt(100) < indexReadPercent;
    }

    /** Get index read operation. */
    private ReadOp<K, V, O> getIndexReadOp(Random rand,
        long keynum, long index, boolean firstThread) {

        if (rand.nextBoolean()) {
            return new IndexIteratorOp(index, firstThread, keynum);
        }
        return new IndexKeysIteratorOp(index, firstThread, keynum);
    }

    /** Get table read operation. */
    private ReadOp<K, V, O> getTableReadOp(
        Random rand, long keynum, long index, boolean firstThread)
    {
        /* Select a random value less than the sum of all ReadOp weights */
        int randValue = rand.nextInt(readOpTotalWeights);

        /*
         * For each ReadOp, check if the random value is less than the op's
         * weight. If not, subtract the weight from the random value and move
         * to the next op. This should select an op with a probably
         * proportional to its weight.
         */
        for (final Entry<CreateReadOp<K, V, O>, Integer> e :
                 readOpWeights.entrySet()) {
            final int weight = e.getValue();
            if (randValue < weight) {
                return e.getKey().createReadOp(index, firstThread, keynum);
            }
            randValue -= weight;
        }
        throw new IllegalStateException("ReadOp not found");
    }

    /**
     * Hash function to convert the input index to a fixed TTL day.
     * Return 0 means no TTL expiration.
     */
    private long indexToTTLTimeInDays(long index) {
        if (!useTTL) {
            return 0;
        }

        MessageDigest md = perThreadMessageDigest.get();
        final byte[] digest = md.digest(ByteUtils.longToBytes(index));
        final long value = Math.abs(ByteUtils.bytesToLong(digest));
        return (value % (MAXIMUM_TTL_DAYS - MINIMUM_TTL_DAYS + 1)) +
            MINIMUM_TTL_DAYS;
    }

    /* Keynum and index operations */

    /** Get the row for the specified populate operation. */
    @Override
    V getPopulateValueInternal(long index) {
        long keynum = indexToKeynum(index);
        long ttlDays = indexToTTLTimeInDays(index);
        return getPopulateValueTableInternal(index, keynum, ttlDays);
    }

    abstract V getPopulateValueTableInternal(long index,
                                             long keynum,
                                             long ttlDays);

    /** Get the row to insert for the specified exercise operation. */
    @Override
    V getExerciseValueInternal(long index, boolean firstThread) {
        long keynum = exerciseIndexToKeynum(index, firstThread);
        return getExerciseValueTableInternal(index, firstThread, keynum);
    }

    abstract V getExerciseValueTableInternal(long index,
                                             boolean firstThread,
                                             long keynum);

    /* Keys and Keynums */

    /** Convert a keynum to the associated primary key. */
    @Override
    K keynumToKey(long keynum) {
        Key key = keynumToKeyInternal(keynum);
        return keynumToKeyTableInternal(key);
    }

    abstract K keynumToKeyTableInternal(Key key);

    /** Convert a keynum to the associated primary key. */
    @Override
    K keynumToMajorKey(long keynum) {
        Key key = keynumToMajorKeyInternal(keynum);
        return keynumToMajorKeyTableInternal(key);
    }

    abstract K keynumToMajorKeyTableInternal(Key key);

    /** Convert a keynum to the parent portion of the associated primary key. */
    @Override
    K keynumToParentKey(long keynum) {
        Key key = keynumToParentKeyInternal(keynum);
        return keynumToParentKeyTableInternal(key);
    }

    abstract K keynumToParentKeyTableInternal(Key key);

    /* Logging and reporting */

    /**
     * Convert row to format like:
     *
     * {majorKey1=k12345678, majorKey2=12, minorKey=12, operation=EXERCISE,
     *  firstThread=false, index=0x12345[, ttl=538]}
     */
    @Override
    String valueString(V row) {
        if (row == null) {
            return "null";
        }
        final StringBuilder sb = new StringBuilder();
        try (final Formatter formatter = new Formatter(sb)) {
            final String[] allvaluesList = valueStringArray(row);
            final String operation = allvaluesList[3];
            final long index = Long.parseLong(allvaluesList[5]);
            formatter.format("{%s=%s, " +
                             "%s=%s, " +
                             "%s=%s, " +
                             "%s=%s, " +
                             "%s=%s, " +
                             "%s=%#x",
                             FLD_MAJORKEY1, allvaluesList[0],
                             FLD_MAJORKEY2, allvaluesList[1],
                             FLD_MINORKEY, allvaluesList[2],
                             FLD_OPERATION, operation,
                             FLD_FIRSTTHREAD, allvaluesList[4],
                             FLD_INDEX, index);
            if (useTTL && (OPERATION_POPULATE.equals(operation))) {
                final long ttl = indexToTTLTimeInDays(index);
                if (ttl != 0) {
                    formatter.format(", ttl=%d", ttl);
                }
            }
            formatter.format("}");
            return sb.toString();
        }
    }

    abstract String[] valueStringArray(V row);

    /**
     * Convert key to format like:
     *
     * {majorKey1=k12345678, majorKey2=12, minorKey=12}
     */
    @Override
    String keyString(K pKey) {
        final String[] allkeysList = keyStringArray(pKey);
        return String.format("{%s=%s, %s=%s, %s=%s}",
                             FLD_MAJORKEY1, allkeysList[0],
                             FLD_MAJORKEY2, allkeysList[1],
                             FLD_MINORKEY, allkeysList[2]);
    }

    abstract String[] keyStringArray(K pKey);

    /* Threads */

    @Override
    O getPopulateOp(K key, V value, long keynum, long index) throws Exception {
        O op = getPopulateOpInternal(value);
        tallyPut(key, value);

        if (verbose >= DEBUG) {
            log("op=put index=%#x, partition=%d, key=%s, keynum=%#x, " +
                "value=%s", index, getPartitionId(key), keyString(key), keynum,
                valueString(value));
        }
        return op;
    }

    abstract O getPopulateOpInternal(V value) throws Exception;

    @Override
    O getCleanUpOp(K key, long index, long keynum) {
        O op = getCleanUpOpInternal(key);
        if (verbose >= DEBUG) {
            log("op=delete index=%#x, partition=%d, key=%s, keynum=%#x",
                index, getPartitionId(key), keyString(key), keynum);
        }
        return op;
    }

    abstract O getCleanUpOpInternal(K key);

    /** IndexIterator operation. */
    class IndexIteratorOp extends ReadOp<K, V, O> {

        IndexIteratorOp(long index,
                        boolean firstThread,
                        long keynum) {
            super(DataCheckTable.this, index, firstThread, keynum);
        }

        @Override
        void doOp(long timeoutMillis) {
            doIndexIteratorOp(this, index, timeoutMillis);
        }
    }

    abstract void doIndexIteratorOp(ReadOp<K, V, O> readOp,
                                    long index,
                                    long timeoutMillis);

    /** Index keys iterator operation. */
    class IndexKeysIteratorOp extends ReadOp<K, V, O> {

        IndexKeysIteratorOp(long index,
                            boolean firstThread,
                            long keynum) {
            super(DataCheckTable.this, index, firstThread, keynum);
        }

        @Override
        void doOp(long timeoutMillis) {
            doIndexKeysIteratorOp(this, index, timeoutMillis);
        }
    }

    abstract void doIndexKeysIteratorOp(ReadOp<K, V, O> readOp,
                                        long index,
                                        long timeoutMillis);

    /*
     * SELECT query read operations
     */

    /** SELECT query using complete primary key */
    class CompletePrimaryKeyQueryOp extends ReadOp<K, V, O> {

        CompletePrimaryKeyQueryOp(long index,
                                  boolean firstThread,
                                  long keynum) {
            super(DataCheckTable.this, index, firstThread, keynum);
        }

        @Override
        void doOp(long timeoutMillis) {
            doCompletePrimaryKeyQueryOp(this, keynum, timeoutMillis);
        }
    }

    abstract void doCompletePrimaryKeyQueryOp(
        ReadOp<K, V, O> readOp, long keynum, long timeoutMillis);

    /** SELECT query using partial primary key with complete shard key */
    class CompleteShardPrimaryKeyQueryOp extends ReadOp<K, V, O> {

        CompleteShardPrimaryKeyQueryOp(long index,
                                       boolean firstThread,
                                       long keynum) {
            super(DataCheckTable.this, index, firstThread, keynum);
        }

        @Override
        void doOp(long timeoutMillis) {
            doCompleteShardPrimaryKeyQueryOp(this, keynum, timeoutMillis);
        }
    }

    abstract void doCompleteShardPrimaryKeyQueryOp(
        ReadOp<K, V, O> readOp, long keynum, long timeoutMillis);

    /** SELECT query using partial primary key with partial shard key */
    class PartialShardPrimaryKeyQueryOp
            extends CompleteShardPrimaryKeyQueryOp {

        PartialShardPrimaryKeyQueryOp(long index,
                                      boolean firstThread,
                                      long keynum) {
            super(index, firstThread, keynum);
        }

        @Override
        void doOp(long timeoutMillis) {
            doPartialShardPrimaryKeyQueryOp(this, keynum, timeoutMillis);
        }
    }

    abstract void doPartialShardPrimaryKeyQueryOp(
        ReadOp<K, V, O> readOp, long keynum, long timeoutMillis);

    /** SELECT query using secondary key */
    class SecondaryKeyQueryOp extends ReadOp<K, V, O> {

        SecondaryKeyQueryOp(long index, boolean firstThread, long keynum) {
            super(DataCheckTable.this, index, firstThread, keynum);
        }

        @Override
        void doOp(long timeoutMillis) {
            doSecondaryKeyQueryOp(this, timeoutMillis);
        }
    }

    abstract void doSecondaryKeyQueryOp(SecondaryKeyQueryOp readOp,
                                        long timeoutMillis);

    /**
     * Determine whether the record of current index should be expired.
     */
    private boolean isTTLExpired(long index) {
        if (!useTTL) {
            return false;
        }

        long days = indexToTTLTimeInDays(index);

        if (days == 0) {
            /* Ignore TTL check */
            return false;
        }

        /* Pre-set days longer than current day, not expect to expire */
        if (days >= MEDIUM_TTL_DAY) {
            return false;
        }
        /* Expired */
        return true;
    }

    /**
     * Override the default implementation to return -1 if the populate value
     * is TTL expired.
     */
    @Override
    long keynumToPopulateIndex(long keynum) {
        long index = super.keynumToPopulateIndex(keynum);
        if ((index != -1) && isTTLExpired(index)) {
            index = -1;
        }
        return index;
    }

    /*
     * Query update operations
     */

    /* INSERT query */
    class QueryInsert extends AbstractUpdateOpType<K, V, O> {
        @Override
        public UpdateOp<K, V, O> getOp(DataCheck<K, V, O> dc,
                                       long index,
                                       boolean firstThread,
                                       boolean requestPrevValue) {
            return new QueryInsertOp(index,
                                     firstThread,
                                     requestPrevValue);
        }

        @Override
        public V getResult(V newValue, V previousValue) {
            /*
             * INSERT is like putIfAbsent: it only takes effect if there is no
             * previous value for the primary key
             */
            return (previousValue != null) ? previousValue : newValue;
        }
    }

    /** Base class for query operations. */
    abstract class QueryOp extends UpdateOp<K, V, O> {

        QueryOp(long index,
                boolean firstThread,
                boolean requestPrevValue) {
            super(DataCheckTable.this, index, firstThread, requestPrevValue);
        }

        /**
         * Get the cached prepared query for this class and the value of
         * requestPrevValue.
         */
        abstract Object getPreparedStatement(V row);

        /**
         * Add the body of the query to the query string being built up for the
         * specified row. The string already includes the declaration of the
         * $v0, $v1, etc. variables. After this method adds the body of the
         * query, the caller will add the returning part, if requested.
         */
        abstract void getQueryBody(V row, Formatter fmt);

        @Override
        void doOp(long timeoutMillis, boolean retrying) {
            final long keynum = dc.exerciseIndexToKeynum(index, firstThread);
            final K key = dc.keynumToKey(keynum);
            final V row = dc.getExerciseValue(index, firstThread);
            final Object boundStmt = getBoundStatement(row);
            final V prevRow =
                doQueryOp(boundStmt, timeoutMillis, requestPrevValue, this);
            checkValue(key, keynum, row, prevRow, retrying);
        }

        Object getBoundStatement(V row) {
            final Object prepared = getPreparedStatement(row);
            return DataCheckTable.this.getBoundStatement(prepared, row);
        }

        /**
         * Gets the result count from the return value of a query that did not
         * specify "RETURNING", meaning it just returns the number of rows, not
         * the row data.
         * The count is an INTEGER for INSERT, UPSERT and UPDATE operations
         * and long for DELETE operation.
         */
        abstract long getReturnCount(Object recordValue);

        /** Check that the previous value is correct. */
        abstract void checkValue(K key,
                                 long keynum,
                                 V row,
                                 V prevRow,
                                 boolean retrying);

        /** Build the query string */
        String getQuery(V row) {
            final StringBuilder sb = new StringBuilder();
            try (Formatter fmt = new Formatter(sb)) {

                /* Declare external variables $v0, $v1, etc. */
                fmt.format("DECLARE $v0 STRING;" +      /* majorKey1 */
                           " $v1 STRING;" +             /* majorKey2 */
                           " $v2 STRING;" +             /* minorKey */
                           " $v3 ENUM(%s, %s);" +       /* operation */
                           " $v4 BOOLEAN;" +            /* firstThread */
                           " $v5 LONG;\n",              /* index */
                           OPERATION_POPULATE, OPERATION_EXERCISE);

                getQueryBody(row, fmt);

                /* Return all fields from the previous value if requested */
                if (requestPrevValue) {
                    String sep = " RETURNING ";
                    List<String> fieldNames = getFieldNames(row);
                    for (final String fieldName : fieldNames) {
                        fmt.format(sep);
                        fmt.format(fieldName);
                        sep = ", ";
                    }
                }
            }
            return sb.toString();
        }
    }

    abstract V doQueryOp(Object boundStmt,
                         long timeoutMillis,
                         boolean requestPrevValue,
                         QueryOp queryOp);

    abstract Object getBoundStatement(Object prepared, V row);

    class QueryInsertOp extends QueryOp {
        QueryInsertOp(long index,
                      boolean firstThread,
                      boolean requestPrevValue) {
            super(index, firstThread, requestPrevValue);
        }

        /** Returns the name of the update operation */
        String getOpName() {
            return "INSERT";
        }

        @Override
        void checkValue(K key,
                        long keynum,
                        V row,
                        V prevRow,
                        boolean retrying) {
            tallyPut(key, row);

            /*
             * If the actual previous value was not returned, then we have no
             * information about the previous value because INSERT always
             * returns something, so nothing to check.
             */
            if (!requestPrevValue) {
                return;
            }

            /*
             * INSERT returns the new value if there was no previous entry, so
             * use that information to tell if there was no previous entry. But
             * the new entry is also returned on a retry if the previous
             * attempt succeeded, so we can't check that case.
             */
            if (valuesEqual(row, prevRow)) {
                if (retrying) {
                    return;
                }
                prevRow = null;
            }
            checkPut(key, keynum, row, prevRow, retrying);
        }

        @Override
        Object getPreparedStatement(V row) {
            return getQueryPreparedStatement(getQuery(row));
        }

        /* Insert value constructed from the external variables */
        @Override
        void getQueryBody(V row, Formatter fmt) {
            fmt.format("%s INTO %s VALUES(", getOpName(), tableName);
            int rowSize = getRowSize(row);
            for (int i = 0; i < rowSize; i++) {
                if (i > 0) {
                    fmt.format(", ");
                }
                fmt.format("$v%d", i);
            }
            fmt.format(")");
        }

        @Override
        long getReturnCount(Object recordValue) {
            return getInsertReturnCount(recordValue);
        }
    }

    abstract Object getQueryPreparedStatement(String queryStmt);

    abstract long getInsertReturnCount(Object recordValue);

    /* UPSERT query */
    class QueryUpsert extends AbstractUpdateOpType<K, V, O> {

        @Override
        public UpdateOp<K, V, O> getOp(DataCheck<K, V, O> dc,
                                       long index,
                                       boolean firstThread,
                                       boolean requestPrevValue) {
            return new QueryUpsertOp(index,
                                     firstThread,
                                     requestPrevValue);
        }

        @Override
        public V getResult(V newValue, V previousValue) {
            /* UPSERT always stores the new value */
            return newValue;
        }
    }

    class QueryUpsertOp extends QueryInsertOp {
        QueryUpsertOp(long index,
                      boolean firstThread,
                      boolean requestPrevValue) {
            super(index, firstThread, requestPrevValue);
        }

        @Override
        void checkValue(K key,
                        long keynum,
                        V row,
                        V prevRow,
                        boolean retrying) {
            tallyPut(key, row);

            /* UPSERT always returns the new value, so nothing to check */
        }

        @Override
        Object getPreparedStatement(V row) {
            return getQueryPreparedStatement(getQuery(row));
        }

        @Override
        String getOpName() {
            return "UPSERT";
        }
    }

    /* UPDATE query */
    class QueryUpdate extends AbstractUpdateOpType<K, V, O> {

        @Override
        public UpdateOp<K, V, O> getOp(DataCheck<K, V, O> dc,
                                       long index,
                                       boolean firstThread,
                                       boolean requestPrevValue) {
            return new QueryUpdateOp(index,
                                     firstThread,
                                     requestPrevValue);
        }

        @Override
        public V getResult(V newValue, V previousValue) {
            /*
             * UPDATE is like putIfPresent: it only stores the new value if a
             * previous value was present
             */
            return (previousValue != null) ? newValue : null;
        }
    }

    class QueryUpdateOp extends QueryOp {
        QueryUpdateOp(long index,
                      boolean firstThread,
                      boolean requestPrevValue) {
            super(index, firstThread, requestPrevValue);
        }

        @Override
        void checkValue(K key,
                        long keynum,
                        V row,
                        V prevRow,
                        boolean retrying) {
            tallyPut(key, row);

            /*
             * UPDATE returns null if there was no previous value, otherwise
             * returns the new value. So we can only check the null case.
             */
            if (prevRow == null) {
                checkPut(key, keynum, row, prevRow, retrying);
            }
        }

        @Override
        Object getPreparedStatement(V row) {
            return getQueryPreparedStatement(getQuery(row));
        }

        @Override
        void getQueryBody(V row, Formatter fmt) {
            fmt.format("UPDATE %s SET ", tableName);

            /* Set the fields following the three primary key fields */
            int rowSize = getRowSize(row);
            for (int i = 3; i < rowSize; i++) {
                if (i > 3) {
                    fmt.format(", ");
                }
                String fieldName = getFieldName(row, i);
                fmt.format("%s = $v%d", fieldName, i);
            }
            fmt.format(" WHERE %s = $v0" +
                       " AND %s = $v1" +
                       " AND %s = $v2",
                       FLD_MAJORKEY1,
                       FLD_MAJORKEY2,
                       FLD_MINORKEY);
        }

        @Override
        long getReturnCount(Object recordValue) {
            return getUpdateReturnCount(recordValue);
        }
    }

    abstract long getUpdateReturnCount(Object recordValue);

    /* DELETE query */
    class QueryDelete extends AbstractUpdateOpType<K, V, O> {

        @Override
        public UpdateOp<K, V, O> getOp(DataCheck<K, V, O> dc,
                                       long index,
                                       boolean firstThread,
                                       boolean requestPrevValue) {
            return new QueryDeleteOp(index,
                                     firstThread,
                                     requestPrevValue);
        }

        @Override
        public V getResult(V newValue, V previousValue) {
            /* DELETE removes the previous value, if any */
            return null;
        }
    }

    class QueryDeleteOp extends QueryOp {
        QueryDeleteOp(long index,
                      boolean firstThread,
                      boolean requestPrevValue) {
            super(index, firstThread, requestPrevValue);
        }

        @Override
        Object getPreparedStatement(V row) {
            return getQueryPreparedStatement(getQuery(row));
        }

        @Override
        void checkValue(K key,
                        long keynum,
                        V row,
                        V prevRow,
                        boolean retrying) {

            /*
             * If we're retrying, then the previous delete may have already
             * deleted the row. But if a previous value is found, then it
             * should be the one expected in the first attempt.
             */
            if (!retrying || (prevRow != null)) {
                checkDelete(key, keynum, prevRow, retrying);
            }
        }

        /* Delete value constructed from the external variables */
        @Override
        void getQueryBody(V row, Formatter fmt) {
            fmt.format("DELETE FROM %s WHERE ", tableName);

            /* Just specify the primary key values */
            for (int i = 0; i < 3; i++) {
                if (i > 0) {
                    fmt.format(" AND ");
                }
                String fieldName = getFieldName(row, i);
                fmt.format("%s = $v%d", fieldName, i);
            }
        }

        @Override
        long getReturnCount(Object recordValue) {
            return getDeleteReturnCount(recordValue);
        }
    }

    abstract long getDeleteReturnCount(Object recordValue);

    abstract int getRowSize(V row);

    abstract List<String> getFieldNames(V row);

    abstract String getFieldName(V row, int fieldPos);

    class FaultingPutTable extends FaultingVoid {
        final V row;
        final Object durability;

        FaultingPutTable(Key key, Value value, Object durability) {
            row = getRow(key, value);
            this.durability = durability;
        }

        @Override
        void run(long timeoutMillis, boolean ignore /* retrying */) {
            runFaultingPutTable(row, durability, timeoutMillis);
        }

        @Override
        public String toString() {
            return "FaultingPut[row=" + row +
                ", durability=" + durability + "]";
        }
    }

    abstract V getRow(Key key, Value value);

    abstract void runFaultingPutTable(V row, Object durability,
        long timeoutMillis);

    class FaultingMultiGetKeysTable extends Faulting<Set<Key>> {
        private final K pKey;

        FaultingMultiGetKeysTable(Key key) {
            pKey = keyToMDPrimaryKey(key, true);
        }

        @Override
        public Set<Key> call(long timeoutMillis,
                             boolean ignore /* retrying */) {
            return callFaultingMultiGetKeysTable(pKey, timeoutMillis);
        }

        @Override
        public String toString() {
            return "FaultingMultiGetKeys[pKey=" + pKey + "]";
        }
    }

    abstract K keyToMDPrimaryKey(Key key, boolean skipMinor);

    abstract Set<Key> callFaultingMultiGetKeysTable(K pKey, long timeoutMillis);

    class FaultingMultiDeleteTable extends FaultingVoid {
        private final K pKey;
        private final Object durability;

        FaultingMultiDeleteTable(Key key, Object durability) {
            pKey = keyToMDPrimaryKey(key, true);
            this.durability = durability;
        }

        @Override
        void run(long timeoutMillis, boolean ignore /* retrying */) {
            runFaultingMultiDeleteTable(pKey, durability, timeoutMillis);
        }

        @Override
        public String toString() {
            return "FaultingMultiDelete[pKey=" + pKey +
                ", durability=" + durability + "]";
        }
    }

    abstract void runFaultingMultiDeleteTable(K pKey, Object durability,
        long timeoutMillis);

    class FaultingGetTable extends Faulting<ValueVersion> {
        private final K pKey;

        FaultingGetTable(Key key) {
            pKey = keyToMDPrimaryKey(key, false);
        }

        @Override
        public ValueVersion call(long timeoutMillis,
                                 boolean ignore /* retrying */) {
            return callFaultingGetTable(pKey, timeoutMillis);
        }

        @Override
        public String toString() {
            return "FaultingGet[pKey=" + pKey + "]";
        }
    }

    abstract ValueVersion callFaultingGetTable(K pKey, long timeoutMillis);

    @Override
    void faultingPutInternal(Key key, Value value, Object durability) {
        new FaultingPutTable(key, value, durability).perform();
    }

    @Override
    Set<Key> faultingMultiGetKeysInternal(Key key) {
        return new FaultingMultiGetKeysTable(key).perform();
    }

    @Override
    void faultingMultiDeleteInternal(Key key, Object durability) {
        new FaultingMultiDeleteTable(key, durability).perform();
    }

    @Override
    ValueVersion faultingGetInternal(Key key) {
        return new FaultingGetTable(key).perform();
    }

    /**
     * Convert a map of ReadOp costs into one of ReadOp weights. Weights
     * represent the relative likelihood of running a given operation, which is
     * inversely proportional to its cost.
     */
    private Map<CreateReadOp<K, V, O>, Integer>
        computeReadOpWeights(Map<CreateReadOp<K, V, O>, Integer> readOpCosts)
    {
        /* Compute the sum of all weights, where weight is 1/cost */
        double totalWeights = 0.0;
        for (final int cost : readOpCosts.values()) {
            totalWeights += (1.0 / cost);
        }

        /* Scale so the weights add up to approximately 1000 */
        final double scale = 1000 / totalWeights;

        /* Return a map with the scaled weights */
        final Map<CreateReadOp<K, V, O>, Integer> map = new HashMap<>();
        for (final Entry<CreateReadOp<K, V, O>, Integer> entry :
                 readOpCosts.entrySet()) {
            final int cost = entry.getValue();
            final int weight = (int) Math.round((1.0 / cost) * scale);
            map.put(entry.getKey(), weight);
        }
        return map;
    }

    /**
     * Returns a map of CreateReadOps ops and their associated costs. The cost
     * values can be arbitrary and are relative. We will choose operations in
     * inverse proportion to their costs so that each type of operation gets
     * run for roughly the same amount of time.
     */
    abstract Map<CreateReadOp<K, V, O>, Integer> getReadOpCosts();

    /** Represents an operation that creates a ReadOp. */
    interface CreateReadOp<K, V, O> {
        ReadOp<K, V, O> createReadOp(long index,
                                     boolean firstThread,
                                     long keynum);
    }

    /** Like CreateRepOp, but with a DataCheck argument */
    interface CreateReadOpWithDataCheck<K, V, O> {
        ReadOp<K, V, O> createReadOp(DataCheck<K, V, O> dc,
                                     long index,
                                     boolean firstThread,
                                     long keynum);
    }

    /** Add in the DataCheck argument */
    CreateReadOp<K, V, O> withDC(CreateReadOpWithDataCheck<K, V, O> creator) {
        return (index, firstThread, keynum) ->
            creator.createReadOp(this, index, firstThread, keynum);
    }
}
