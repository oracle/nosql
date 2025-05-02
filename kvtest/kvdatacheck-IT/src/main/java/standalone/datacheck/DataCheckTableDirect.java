/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static oracle.kv.impl.param.ParameterState.JVM_RN_OVERRIDE_NO_RESTART;
import static oracle.kv.table.TableUtils.getDataSize;
import static oracle.kv.table.TableUtils.getKeySize;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import oracle.kv.BulkWriteOptions;
import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.ExecutionFuture;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVVersion;
import oracle.kv.Key;
import oracle.kv.StatementResult;
import oracle.kv.StoreIteratorException;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.test.RemoteTestAPI;
import oracle.kv.impl.test.TTLTestHook;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils.InterfaceType;
import oracle.kv.query.BoundStatement;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.PreparedStatement;
import oracle.kv.stats.KVStats;
import oracle.kv.table.FieldValue;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.KeyPair;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.RecordValue;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.ReturnRow.Choice;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;
import oracle.kv.table.TableOpExecutionException;
import oracle.kv.table.TableOperation;
import oracle.kv.table.TableOperationFactory;
import oracle.kv.table.TableOperationResult;
import oracle.kv.table.TimeToLive;
import oracle.kv.table.WriteOptions;

import standalone.datacheck.UpdateOpType.BulkPut;

/**
 * Implement API for data checking used by {@link DataCheckMain}.
 * This class is for checking Table entries.
 */
public class DataCheckTableDirect
        extends DataCheckTable<PrimaryKey, Row, TableOperation> {

    /* Update operation types */
    /* 50% chance of adding an entry */
    private static final UpdateOpType<PrimaryKey, Row, TableOperation>
        BULK_PUT = new BulkPut<>();

    /**
     * The names of store non-index-iteration read operations that should be
     * called in the exercise phase if reads are performed.
     */
    private static final Set<String> EXERCISE_READ_OPCODE_NAMES =
        Stream.concat(
            /* Non-query */
            Stream.of(OpCode.GET,
                      OpCode.MULTI_GET_BATCH_TABLE,
                      OpCode.MULTI_GET_BATCH_TABLE_KEYS,
                      OpCode.MULTI_GET_TABLE,
                      OpCode.MULTI_GET_TABLE_KEYS,
                      OpCode.TABLE_ITERATE,
                      OpCode.TABLE_KEYS_ITERATE),
            /* Query */
            (enableQueries ?
             Stream.of(OpCode.QUERY_MULTI_PARTITION,
                       OpCode.QUERY_MULTI_SHARD,
                       OpCode.QUERY_SINGLE_PARTITION) :
             Stream.empty()))
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
    private static final Set<String> EXERCISE_UPDATE_OPCODE_NAMES
        = Stream.concat(
            /* Non-query */
            Stream.of(OpCode.DELETE,
                      OpCode.DELETE_IF_VERSION,
                      OpCode.EXECUTE,
                      OpCode.MULTI_DELETE_TABLE,
                      OpCode.PUT,
                      OpCode.PUT_BATCH,
                      OpCode.PUT_IF_ABSENT,
                      OpCode.PUT_IF_PRESENT,
                      OpCode.PUT_IF_VERSION),

            /* Query */
            (enableQueries ?
             Stream.of(OpCode.QUERY_SINGLE_PARTITION) :
             Stream.empty()))
        .map(KVStats::getOpCodeName)
        .collect(Collectors.toSet());

    /*
     * A list of UpdateOpTypes selected so that they will create no net
     * increase or decrease on the number of entries if chosen randomly.
     */
    {
        Collections.addAll(
            updateOpTypes,

            /* Pairs of operations with matching add and remove */
            PUT,
            DELETE,

            PUT_EXECUTE,
            DELETE_EXECUTE,

            PUT_IF_ABSENT,
            DELETE_IF_VERSION,

            PUT_IF_ABSENT_EXECUTE,
            DELETE_IF_VERSION_EXECUTE,

            BULK_PUT,
            MULTI_DELETE);

        if (enableQueries) {
            Collections.addAll(
                updateOpTypes,

                QUERY_INSERT,
                QUERY_DELETE,

                /*
                 * There is only one query operation for deletes, so use it
                 * again to pair with UPSERT
                 */
                QUERY_UPSERT,
                QUERY_DELETE);
        }

        Collections.addAll(
            updateOpTypes,

            /*
             * The remaining operations have no effect on the number of
             * entries.
             */
            PUT_IF_PRESENT,
            PUT_IF_VERSION,
            PUT_IF_PRESENT_EXECUTE,
            PUT_IF_VERSION_EXECUTE);

        if (enableQueries) {
            Collections.addAll(
                updateOpTypes,

                /* Need an even number, so use this one twice */
                QUERY_UPDATE,
                QUERY_UPDATE);
        }
    }

    final TableAPI tableImpl;
    TableOperationFactory tableFactory;
    Table table;
    Table mdtable;
    private Index idxIndex;
    private Row emptyRow;
    private ByteArrayOutputStream output;
    private final boolean useTTL;
    private final String adminHostName;
    private final String adminPortNum;
    volatile double exerciseReadPercent;

    /* Cache prepared queries */
    private final Map<String, PreparedStatement> preparedStatements =
        Collections.synchronizedMap(new HashMap<>());

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
     * @param useParallelScan use parallel scan in exercise phase if specified.
     * @param scanMaxConcurrentRequests the max concurrent threads are allowed
     *        for a parallel scan.
     * @param scanMaxResultsBatches the max batches to temporally store the
     *        parallel scan results, each batch can store one scan results.
     * @param indexReadPercent index read percentage
     * @param maxThroughput max throughput for the client.
     * @param maxExecTime the max execution for the client workload.
     * @param useTTL whether to test the TTL feature
     * @param adminHostName admin host name
     * @param adminPortNum admin port number
     */
    public DataCheckTableDirect(KVStore store,
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
                                String adminPortNum) {

        super(store, config, reportingInterval, partitions,
              seed, verbose, err,
              config.getRequestTimeout(MILLISECONDS), blockTimeout,
              useParallelScan, scanMaxConcurrentRequests, scanMaxResultsBatches,
              indexReadPercent, maxThroughput, maxExecTime, useTTL);

        tableImpl = store.getTableAPI();
        tableFactory = tableImpl.getTableOperationFactory();
        this.useTTL = useTTL;
        this.adminHostName = adminHostName;
        this.adminPortNum = adminPortNum;
        maybeInstallSchema();
        initializeRandom(seed);
    }

    /** Print information about whether queries are enabled */
    @Override
    public void exercise(long start,
                         long count,
                         int threads,
                         double readPercent,
                         boolean noCheck,
                         DataCheck<PrimaryKey, Row, TableOperation> pairDc) {
        log("Queries " + (enableQueries ? "enabled" : "disabled"));
        exerciseReadPercent = readPercent;
        super.exercise(start, count, threads, readPercent, noCheck, pairDc);
    }

    @Override
    void executeDDL(String stmt) {

        final ExecutionFuture future = store.execute(stmt);

        /* Wait for the operation to finish */
        StatementResult result;
        try {
            result = future.get(DEFAULT_DDL_TIMEOUT_MS, MILLISECONDS);
        } catch (TimeoutException te) {
            final String msg = "Timeout in executing ddl=" + stmt +
                               ", timeoutMs=" + DEFAULT_DDL_TIMEOUT_MS;
            log(msg);
            throw new IllegalStateException(msg, te);
        } catch (ExecutionException | InterruptedException exp) {
            final String msg = "Fail to execute ddl=" + stmt + ", error=" + exp;
            log(msg);
            throw new IllegalStateException(msg, exp);
        }

        if (!result.isSuccessful()) {
            throw new IllegalStateException(
                "Execution failed, statement=" + stmt +
                ", plan id=" + result.getPlanId() +
                ", done=" + result.isDone() +
                ", result=" + result.getResult());
        }
    }

    @Override
    boolean doesTableExist(String tName) {
        return tableImpl.getTable(tName) != null;
    }

    @Override
    boolean doesIndexExist(String iName) {
        return getDataCheckTableIndex(IDX_INDEX) != null;
    }

    /** Return a row with default value. */
    @Override
    synchronized Row getEmptyValue() {
        if (emptyRow == null) {
            emptyRow = getDataCheckTable().createRow();
            emptyRow.put(FLD_MAJORKEY1, "");
            emptyRow.put(FLD_MAJORKEY2, "");
            emptyRow.put(FLD_MINORKEY, "");
            emptyRow.putEnum(FLD_OPERATION, OPERATION_POPULATE);
            emptyRow.put(FLD_FIRSTTHREAD, true);
            emptyRow.put(FLD_INDEX, 0L);
        }
        return emptyRow;
    }

    /**
     * Check if the given value equals to the expected value.
     *
     *  Row.equals: compare the value of each field.
     *  ReturRow.equals: compare not only the value of fields but also
     *      the value of Choice.
     *  If comparing a ReturnRow object with a Row ject:
     *      valuesEquals(ReturnRow value1, Row value2)
     *  internally call value2.equals(value1) to compare values only.
     */
    @Override
    boolean valuesEqual(Row value1, Row value2) {
        if (value2 == null) {
            return false;
        }
        if (value1 instanceof ReturnRow) {
            return value2.equals(value1);
        }
        return value1.equals(value2);
    }

    /* Exercise methods */

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
    List<OpResult<Row>> execute(List<TableOperation> operations,
                                long timeoutMillis) {
        try {
            List<OpResult<Row>> results = new ArrayList<>();
            for (TableOperationResult r :
                     tableImpl.execute(
                         operations,
                         new WriteOptions(null /* durability */, timeoutMillis,
                                          MILLISECONDS))) {
                results.add(new OpResultImpl(r));
            }
            return results;
        } catch (TableOpExecutionException e) {
            throw new IllegalStateException(String.format(
                "Operation execution failed: %s\nFailed operation: %s",
                e.getMessage(), e.getFailedOperation().toString()),
                e);
        }
    }

    static class OpResultImpl implements OpResult<Row> {
        private final TableOperationResult r;
        OpResultImpl(TableOperationResult r) {
            this.r = r;
        }
        @Override
        public Row getPreviousValue() {
            return r.getPreviousRow();
        }
        @Override
        public boolean getSuccess() {
            return r.getSuccess();
        }
    }

    @Override
    Row getPopulateValueTableInternal(long index, long keynum, long ttlDays) {
        Row row = createRowFromKeynum(keynum);
        row.putEnum(FLD_OPERATION, OPERATION_POPULATE);
        row.put(FLD_FIRSTTHREAD, false);
        row.put(FLD_INDEX, index);
        row.setTTL(TimeToLive.ofDays(ttlDays));
        return row;
    }

    @Override
    Row getExerciseValueTableInternal(long index,
                                      boolean firstThread,
                                      long keynum) {
        Row row = createRowFromKeynum(keynum);
        row.putEnum(FLD_OPERATION, OPERATION_EXERCISE);
        row.put(FLD_FIRSTTHREAD, firstThread);
        row.put(FLD_INDEX, index);
        return row;
    }

    private Row createRowFromKeynum(long keynum) {
        return getDataCheckTable().createRow(keynumToKey(keynum));
    }

    /** Get the index key for index idxIndex. */
    IndexKey getIndexKey(long idx) {
        Index index = getDataCheckTableIndex(IDX_INDEX);
        if (index == null) {
            throw new IllegalStateException(
                "Index does not exist: " + IDX_INDEX +
                " of Table: " + tableName);
        }
        IndexKey key = index.createIndexKey();
        key.put(FLD_INDEX, idx);
        return key;
    }

    @Override
    PrimaryKey keynumToKeyTableInternal(Key key) {
        return keyToPrimaryKey(key);
    }

    @Override
    PrimaryKey keynumToMajorKeyTableInternal(Key key) {
        return keyToPrimaryKey(key);
    }

    @Override
    PrimaryKey keynumToParentKeyTableInternal(Key key) {
        return keyToPrimaryKey(key);
    }

    /**Convert a primaryKey to a keynum, returning -1 if the key is not valid.*/
    @Override
    long keyToKeynum(PrimaryKey pKey) {
        return keyToKeynumInternal(primaryKeyToKey(pKey));
    }

    /**
     * Checks if the key, along with it's associated keynum, is in the
     * partitions specified for this test.
     */
    @Override
    boolean keyInPartitions(PrimaryKey pKey, long keynum) {
        Key internalKey = primaryKeyToInternalKey(pKey);
        return keyInPartitionsInternal(internalKey, keynum);
    }

    /** Returns the partition ID, if known, else -1. */
    @Override
    int getPartitionId(PrimaryKey pKey) {
        return getPartitionIdInternal(primaryKeyToInternalKey(pKey));
    }

    /** Convert primaryKey to internal KV key. */
    private Key primaryKeyToInternalKey(PrimaryKey pKey) {
        TableImpl tbDataCheck = (TableImpl)getDataCheckTable();
        return tbDataCheck.createKey(tbDataCheck.createRow(pKey), true);
    }

    /** Convert primaryKey to KV key. */
    private Key primaryKeyToKey(PrimaryKey pKey) {
        List<String> majorPath = new ArrayList<>();
        List<String> minorPath = new ArrayList<>();
        int nMajor = pKey.getTable().getShardKey().size();
        List<String> pkFields = pKey.getTable().getPrimaryKey();
        for (int i = 0; i < pkFields.size(); i++) {
            final String path = pKey.get(pkFields.get(i)).asString().get();
            if (i < nMajor) {
                majorPath.add(path);
            } else {
                minorPath.add(path);
            }
        }
        return Key.createKey(majorPath, minorPath);
    }

    /** Convert KV key to primaryKey. */
    private PrimaryKey keyToPrimaryKey(Key key) {
        Table tbDataCheck = getDataCheckTable();
        PrimaryKey pKey = table.createPrimaryKey();
        int keySize = key.getFullPath().size();
        int idxKey = 0;
        if (table.getPrimaryKey().size() < keySize) {
            throw new IllegalStateException(
                "The number of key's component is greater than " +
                "the number of fields of the primary key.");
        }
        int nPath = Math.min(tbDataCheck.getPrimaryKey().size(), keySize);
        for(int i = 0; i < nPath; i++, idxKey++) {
            pKey.put(tbDataCheck.getPrimaryKey().get(i),
                     key.getFullPath().get(idxKey));
        }
        return pKey;
    }

    @Override
    String[] valueStringArray(Row row) {
        return new String[] {
            row.get(0).asString().get(),
            row.get(1).asString().get(),
            row.get(2).asString().get(),
            row.get(3).asEnum().get(),
            String.valueOf(row.get(4).asBoolean().get()),
            String.valueOf(row.get(5).asLong().get())
        };
    }

    @Override
    String[] keyStringArray(PrimaryKey pKey) {
        return new String[] {
            pKey.get(0).asString().get(),
            pKey.get(1).asString().get(),
            pKey.get(2).asString().get()
        };
    }

    /** Get the length of the internal key for the given primary key.*/
    @Override
    int keyLength(PrimaryKey pKey) {
        return getKeySize(pKey);
    }

    /** Get the length of the internal value for the given row.*/
    @Override
    int valueLength(Row row) {
        return getDataSize(row);
    }

    /* Miscellaneous */

    /** Get the table handle */
    Table getTable(String name, Table tableH) {
        int retry = 0;
        if (tableH == null) {
            while (retry++ < METADATA_RETRY) {
                tableH = tableImpl.getTable(name);
                if (tableH != null) {
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
            }
        }
        if (tableH != null) {
            if (retry > 1) {
                log("Got table after retrying for %d times.", retry);
            }
            return tableH;
        }
        throw new IllegalStateException(
            "Table does not exist: " + name);
    }

    /** Get the DataCheck table. */
    Table getDataCheckTable() {
        if (table == null) {
            table = getTable(tableName, table);
        }
        return table;
    }

    /** Get the DataCheckMD table. */
    Table getDataCheckMDTable() {
        if (mdtable == null) {
            mdtable = getTable(mdtableName, mdtable);
        }
        return mdtable;
    }

    /** Get the index of DataCheck table. */
    private Index getDataCheckTableIndex(String idxName) {
        if (idxIndex == null) {
            idxIndex = getDataCheckTable().getIndex(idxName);
        }
        return idxIndex;
    }

    /**
     * Create TableIteratorOptions with the specified timeout in milliseconds,
     * unordered direction, default consistency, and default setting for
     * parallel scan.
     */
    TableIteratorOptions getTableIteratorOptions(long timeoutMillis) {
        return getTableIteratorOptions(Direction.UNORDERED,
                                       null /* consistency */,
                                       timeoutMillis, MILLISECONDS);
    }

    /** Create TableIteratorOptions with given parameters. */
    TableIteratorOptions getTableIteratorOptions(Direction direction,
                                                 Consistency consistency,
                                                 long timeout,
                                                 TimeUnit timeoutUnit) {
        return new TableIteratorOptions(
            direction, consistency, timeout, timeoutUnit,
            storeIteratorConfig.getMaxConcurrentRequests(),
            MINOR_KEY_MAX);
    }

    /**
     * Create ExecuteOptions with the specified timeout in milliseconds,
     * unordered direction, and default consistency, durability, and parallel
     * scan options.
     */
    ExecuteOptions getExecuteOptions(long timeoutMillis) {
        final ExecuteOptions options = new ExecuteOptions();
        options.setTimeout(timeoutMillis, MILLISECONDS);
        options.setMaxConcurrentRequests(
            storeIteratorConfig.getMaxConcurrentRequests());
        options.setResultsBatchSize(MINOR_KEY_MAX);
        return options;
    }

    /** Check for expected operations */
    @Override
    Map<String, Long> getPhaseOpCounts(Phase phase,
                                       ReportingThread reportingThread) {
        final Map<String, Long> counts =
            super.getPhaseOpCounts(phase, reportingThread);
        if ((phase == Phase.EXERCISE) && !exerciseNoCheck) {
            checkMissingOps(counts);
        }
        return counts;
    }

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

    @Override
    Map<CreateReadOp<PrimaryKey, Row, TableOperation>, Integer>
        getReadOpCosts()
    {
        final Map<CreateReadOp<PrimaryKey, Row, TableOperation>, Integer>
            map = new HashMap<>();

        /* Make regular reads the cheapest */
        map.put(withDC(ReadOp.GetOp::new), 1);
        map.put(withDC(ReadOp.MultiGetOp::new), 1);
        map.put(withDC(ReadOp.MultiGetKeysOp::new), 1);

        /*
         * Complete primary and shard key non-bulk iterations are also cheap,
         * but make them less common
         */
        map.put(CompletePrimaryKeyIteratorOp::new, 2);
        map.put(CompletePrimaryKeyKeysIteratorOp::new, 2);
        map.put(CompleteShardPrimaryKeyIteratorOp::new, 2);
        map.put(CompleteShardPrimaryKeyKeysIteratorOp::new, 2);

        /* Partial primary key iterations are very expensive */
        map.put(PartialShardPrimaryKeyIteratorOp::new, 250);
        map.put(PartialShardPrimaryKeyKeysIteratorOp::new, 250);

        /* Bulk iterations are at least sometimes expensive */
        map.put(CompletePrimaryKeyBulkIteratorOp::new, 200);
        map.put(CompletePrimaryKeyBulkKeysIteratorOp::new, 200);
        map.put(CompleteShardPrimaryKeyBulkIteratorOp::new, 200);
        map.put(CompleteShardPrimaryKeyBulkKeysIteratorOp::new, 200);

        if (enableQueries) {

            /*
             * Complete primary and shard key queries are cheap because they
             * are handled by a single RN
             */
            map.put(CompletePrimaryKeyQueryOp::new, 2);
            map.put(CompleteShardPrimaryKeyQueryOp::new, 2);

            /*
             * Secondary index queries are more expensive because they need to
             * consult each shard, although the actual cost depends on the
             * number of shards
             */
            map.put(SecondaryKeyQueryOp::new, 10);

            /* Partial primary key queries are very expensive */
            map.put(PartialShardPrimaryKeyQueryOp::new, 250);
        }

        return map;
    }

    /* Threads */

    @Override
    TableOperation getPopulateOpInternal(Row value) {
        return tableFactory.createPutIfAbsent(value, null /* prevReturn */,
                                              true /* abortIfUnsuccessful */);
    }

    @Override
    Op<PrimaryKey, Row, TableOperation> getCheckOp(long index) {
        return new Op.MultiGetIteratorCheckOp<>(this, index);
    }

    @Override
    TableOperation getCleanUpOpInternal(PrimaryKey key) {
        return tableFactory.createDelete(key,
            null /* ReturnRow.Choice */, false /* abortIfUnsuccessful */);
    }

    /* Operations */

    @Override
    ValVer<Row> doGet(PrimaryKey key,
                      Object consistency,
                      long timeoutMillis) {
        return new ValVerImpl(
            tableImpl.get(
                key,
                new ReadOptions((Consistency) consistency,
                                timeoutMillis, MILLISECONDS)));
    }

    static class ValVerImpl implements ValVer<Row> {
        private final Row row;
        private final boolean result;
        ValVerImpl(Row row) {
            this(row, true);
        }
        ValVerImpl(Row row, boolean result) {
            this.row = row;
            this.result = result;
        }
        @Override
        public Row getValue() {
            if (row == null || row.size() == 0) {
                return null;
            }
            return row;
        }
        @Override
        public Object getVersion() {
            return (row != null) ? row.getVersion() : null;
        }
        @Override
        public boolean getSuccess() {
            return result;
        }
    }

    @Override
    Collection<Row> doMultiGet(PrimaryKey key,
                               Object consistency,
                               long timeoutMillis) {
        return tableImpl.multiGet(
            key, null /* MultiRowOptions */,
            new ReadOptions((Consistency) consistency,
                            timeoutMillis,
                            MILLISECONDS));
    }

    @Override
    Collection<PrimaryKey> doMultiGetKeys(PrimaryKey key,
                                          Object consistency,
                                          long timeoutMillis) {
        return tableImpl.multiGetKeys(
            key, null /* MultiRowOptions */,
            new ReadOptions((Consistency) consistency,
                            timeoutMillis,
                            MILLISECONDS));
    }

    @Override
    Iterator<KeyVal<PrimaryKey, Row>> doMultiGetIterator(
        PrimaryKey key, Object consistency, long timeoutMillis) {
        return new KeyValIter(doMultiGet(key, consistency, timeoutMillis));
    }

    private static class KeyValIter
            implements Iterator<KeyVal<PrimaryKey, Row>> {
        private final Collection<Row> collection;
        private final Iterator<Row> iter;
        KeyValIter(Collection<Row> collection) {
            this.collection = collection;
            iter = collection.iterator();
        }
        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }
        @Override
        public KeyVal<PrimaryKey, Row> next() {
            return new KeyValImpl(iter.next());
        }
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
        /** Display the contents of the iteration, for debugging. */
        @Override
        public String toString() {
            return "KeyValIter{count=" + collection.size() +
                " items=" + collection + "}";
        }
    }

    private static class KeyValImpl implements KeyVal<PrimaryKey, Row> {
        private final Row row;
        KeyValImpl(Row row) {
            this.row = row;
        }
        @Override
        public PrimaryKey getKey() {
            return row.createPrimaryKey();
        }
        @Override
        public Row getValue() {
            return row;
        }
    }

    @Override
    Iterator<PrimaryKey> doMultiGetKeysIterator(PrimaryKey key,
                                                Object consistency,
                                                long timeoutMillis) {
        throw new UnsupportedOperationException(
            "DataCheckTableDirect.doMultiGetKeysIterator is not supported");
    }

    @Override
    ValVer<Row> doPut(PrimaryKey key,
                      Row value,
                      boolean requestPrevValue,
                      long timeoutMillis) {
        ReturnRow rr = getDataCheckTable().createReturnRow(
            requestPrevValue ? Choice.VALUE : Choice.NONE);
        tableImpl.put(value, rr,
                      new WriteOptions(
                          null /* durability */, timeoutMillis, MILLISECONDS));
        return new ValVerImpl(rr);
    }

    @Override
    TableOperation createPutOp(PrimaryKey key, Row value,
                               boolean requestPrevValue) {
        return tableFactory.createPut(
            value, (requestPrevValue ? Choice.VALUE : Choice.NONE),
            false /* abortIfUnsuccessful */);
    }

    @Override
    ValVer<Row> doPutIfAbsent(PrimaryKey key, Row value,
                              boolean requestPrevValue,
                              long timeoutMillis) {
        ReturnRow rr = getDataCheckTable().createReturnRow(
            requestPrevValue ? Choice.VALUE : Choice.NONE);
        Version v = tableImpl.putIfAbsent(
            value, rr,
            new WriteOptions(
                null /* durability */, timeoutMillis, MILLISECONDS));
        return new ValVerImpl(rr, (v != null));
    }

    @Override
    TableOperation createPutIfAbsentOp(PrimaryKey key, Row value,
                                       boolean requestPrevValue) {
        return tableFactory.createPutIfAbsent(
            value, (requestPrevValue ? Choice.VALUE : Choice.NONE),
            false /* abortIfUnsuccessful */);
    }

    @Override
    ValVer<Row> doPutIfPresent(PrimaryKey key, Row value,
                               boolean requestPrevValue,
                               long timeoutMillis) {
        ReturnRow rr = getDataCheckTable().createReturnRow(
            requestPrevValue ? Choice.VALUE : Choice.NONE);
        Version v = tableImpl.putIfPresent(
            value, rr,
            new WriteOptions(
                null /* durability */, timeoutMillis, MILLISECONDS));
        return new ValVerImpl(rr, (v != null));
    }

    @Override
    TableOperation createPutIfPresentOp(PrimaryKey key, Row value,
                                        boolean requestPrevValue) {
        Row row;
        if (!value.createPrimaryKey().equals(key)) {
            row = value.getTable().createRow();
            row.copyFrom(value);
            copyPrimaryKey(row, key);
        } else {
            row = value;
        }
        return tableFactory.createPutIfPresent(
            row, (requestPrevValue ? Choice.VALUE : Choice.NONE),
            false /* abortIfUnsuccessful */);
    }

    @Override
    ValVer<Row> doPutIfVersion(PrimaryKey key, Row value,
                               Object version,
                               boolean requestPrevValue,
                               long timeoutMillis) {
        ReturnRow rr = getDataCheckTable().createReturnRow(
            requestPrevValue ? Choice.VALUE : Choice.NONE);
        Version v = tableImpl.putIfVersion(
            value, (Version) version, rr,
            new WriteOptions(
                null /* durability */, timeoutMillis, MILLISECONDS));
        return new ValVerImpl(rr, (v != null));
    }

    @Override
    TableOperation createPutIfVersionOp(
        PrimaryKey key, Row value, Object version, boolean requestPrevValue) {
        return tableFactory.createPutIfVersion(
            value, (Version) version,
            (requestPrevValue ? Choice.VALUE : Choice.NONE),
            false /* abortIfUnsuccessful */);
    }

    @Override
    boolean doBulkPut(PrimaryKey key, Row value, long timeoutMillis) {
        final SingleItemEntryStream<Row> entryStream =
            new SingleItemEntryStream<>(value) {
                @Override
                boolean keysMatch(Row x, Row y) {
                    return x.createPrimaryKey().equals(y.createPrimaryKey());
                }
            };
        tableImpl.put(singletonList(entryStream),
                      new BulkWriteOptions(
                          null /* durability */, timeoutMillis, MILLISECONDS));
        entryStream.await(timeoutMillis);
        return entryStream.keyExists();
    }

    @Override
    ValVer<Row> doDelete(PrimaryKey key,
                         boolean requestPrevValue,
                         long timeoutMillis) {
        ReturnRow rr = getDataCheckTable().createReturnRow(
            requestPrevValue ? Choice.VALUE : Choice.NONE);
        boolean exist = tableImpl.delete(
            key, rr,
            new WriteOptions(
                null /* durability */, timeoutMillis, MILLISECONDS));
        return new ValVerImpl(rr, exist);
    }

    @Override
    TableOperation createDeleteOp(PrimaryKey key, boolean requestPrevValue) {
        return tableFactory.createDelete(
            key, (requestPrevValue ? Choice.VALUE : Choice.NONE),
            false /* abortIfUnsuccessful */);
    }

    @Override
    int doMultiDelete(PrimaryKey key, long timeoutMillis) {
        return tableImpl.multiDelete(
            key, null/* MultiRowOptions */,
            new WriteOptions(
                null /* durability */, timeoutMillis, MILLISECONDS));
    }

    @Override
    ValVer<Row> doDeleteIfVersion(PrimaryKey key, Object version,
                                  boolean requestPrevValue,
                                  long timeoutMillis) {
        ReturnRow rr = getDataCheckTable().createReturnRow(
            requestPrevValue ? Choice.VALUE : Choice.NONE);
        tableImpl.deleteIfVersion(
            key, (Version) version, rr,
            new WriteOptions(
                null /* durability */, timeoutMillis, MILLISECONDS));
        return new ValVerImpl(rr);
    }

    @Override
    TableOperation createDeleteIfVersionOp(PrimaryKey key,
                                           Object version,
                                           boolean requestPrevValue) {
        return tableFactory.createDeleteIfVersion(
            key, (Version) version, (requestPrevValue ? Choice.VALUE
                                                      : Choice.NONE),
            false /* abortIfUnsuccessful */);
    }

    @Override
    PrimaryKey appendStringToKey(PrimaryKey key, String x) {
        Table tabDC = key.getTable();
        PrimaryKey retKey = tabDC.createPrimaryKey();
        retKey.copyFrom(key);
        List<String> fields = tabDC.getPrimaryKey();
        String fname = fields.get(fields.size() - 1);
        String minorKey = key.get(fname).asString().get();
        retKey.put(fname, minorKey.concat(x));
        return retKey;
    }

    @Override
    void doIndexIteratorOp(ReadOp<PrimaryKey, Row, TableOperation> readOp,
                           long index, long timeoutMillis) {
        IndexKey key = getIndexKey(index);
        doScanForIndexIteratorOp(readOp, key, timeoutMillis);
    }

    private void doScanForIndexIteratorOp(
        ReadOp<PrimaryKey, Row, TableOperation> readOp, IndexKey key,
        long timeoutMillis) {
        final TableIterator<Row> iter =
            doIndexIterator(key, null, timeoutMillis);
        try {
            iterateScanRecordsForIndexIteratorOp(readOp, iter);
        } catch (StoreIteratorException sie){
            Throwable cause = sie.getCause();
            if (cause != null && cause instanceof FaultException) {
                throw (FaultException)cause;
            }
            throw new FaultException(sie.getMessage(),
                                     new RemoteException(), false);
        } finally {
            iter.close();
        }
    }

    private void iterateScanRecordsForIndexIteratorOp(
        ReadOp<PrimaryKey, Row, TableOperation> readOp, Iterator<Row> iter) {
        while (iter.hasNext()) {
            final Row row = iter.next();
            readOp.checkValue(row.createPrimaryKey(), row);
        }
    }

    /** Perform IndexIterator operation. */
    private TableIterator<Row> doIndexIterator(IndexKey key,
                                               Consistency consistency,
                                               long timeoutMillis) {
        return tableImpl.tableIterator(key, null /*multiRowOptions*/,
                getTableIteratorOptions(Direction.UNORDERED, consistency,
                    timeoutMillis, MILLISECONDS));
    }

    @Override
    void doIndexKeysIteratorOp(ReadOp<PrimaryKey, Row, TableOperation> readOp,
                               long index, long timeoutMillis) {
        IndexKey key = getIndexKey(index);
        doScanForIndexKeysIteratorOp(readOp, key, timeoutMillis);
    }

    private void doScanForIndexKeysIteratorOp(
        ReadOp<PrimaryKey, Row, TableOperation> readOp, IndexKey key,
        long timeoutMillis) {
        final TableIterator<KeyPair> iter =
            doIndexKeysIterator(key, null, timeoutMillis);
        try {
            iterateScanRecordsForIndexKeysIteratorOp(readOp, iter);
        } catch (StoreIteratorException sie){
            Throwable cause = sie.getCause();
            if (cause != null && cause instanceof FaultException) {
                throw (FaultException)cause;
            }
            throw new FaultException(sie.getMessage(),
                                     new RemoteException(), false);
        } finally {
            iter.close();
        }
    }

    private void iterateScanRecordsForIndexKeysIteratorOp(
        ReadOp<PrimaryKey, Row, TableOperation> readOp,
        Iterator<KeyPair> iter) {
        while (iter.hasNext()) {
            final KeyPair kp = iter.next();
            readOp.checkKeyPresent(kp.getPrimaryKey());
        }
    }

    /** Perform IndexKeysIterator operation. */
    private TableIterator<KeyPair> doIndexKeysIterator(IndexKey key,
                                                       Consistency consistency,
                                                       long timeoutMillis) {
        return tableImpl.tableKeysIterator(key, null /*multiRowOptions*/,
            getTableIteratorOptions(Direction.UNORDERED, consistency,
                timeoutMillis, MILLISECONDS));
    }

    private static Iterator<PrimaryKey> getKeyIterator(PrimaryKey key) {
        return singletonList(key).iterator();
    }

    abstract class AbstractKeyIteratorOp<R>
            extends ReadOp<PrimaryKey, Row, TableOperation> {

        AbstractKeyIteratorOp(long index, boolean firstThread, long keynum) {
            super(DataCheckTableDirect.this, index, firstThread, keynum);
        }

        /** Check for the expected result */
        abstract void checkResult(PrimaryKey key, R foundResult);
    }

    abstract class AbstractCompletePrimaryKeyIteratorOp<R>
            extends AbstractKeyIteratorOp<R> {

        AbstractCompletePrimaryKeyIteratorOp(long index,
                                             boolean firstThread,
                                             long keynum) {
            super(index, firstThread, keynum);
        }

        /** Return an iterator over the results */
        abstract TableIterator<R> getIterator(PrimaryKey key,
                                              TableIteratorOptions options);

        @Override
        void doOp(long timeoutMillis) {
            final PrimaryKey key = keynumToKey(keynum);
            final TableIterator<R> iter =
                getIterator(key, getTableIteratorOptions(timeoutMillis));
            checkResult(key, iter.hasNext() ? iter.next() : null);
        }
    }

    class CompletePrimaryKeyIteratorOp
            extends AbstractCompletePrimaryKeyIteratorOp<Row> {

        CompletePrimaryKeyIteratorOp(long index,
                                     boolean firstThread,
                                     long keynum) {
            super(index, firstThread, keynum);
        }

        @Override
        TableIterator<Row> getIterator(PrimaryKey key,
                                       TableIteratorOptions options) {
            return tableImpl.tableIterator(key, null /* MultiRowOptions */,
                                           options);
        }

        @Override
        void checkResult(PrimaryKey key, Row foundResult) {
            checkValue(key, foundResult);
        }
    }

    class CompletePrimaryKeyBulkIteratorOp
            extends CompletePrimaryKeyIteratorOp {
        CompletePrimaryKeyBulkIteratorOp(long index,
                                         boolean firstThread,
                                         long keynum) {
            super(index, firstThread, keynum);
        }

        @Override
        TableIterator<Row> getIterator(PrimaryKey key,
                                       TableIteratorOptions options) {
            return tableImpl.tableIterator(
                getKeyIterator(key), null /* multiRowOptions */, options);
        }
    }

    class CompletePrimaryKeyKeysIteratorOp
            extends AbstractCompletePrimaryKeyIteratorOp<PrimaryKey> {

        CompletePrimaryKeyKeysIteratorOp(long index,
                                         boolean firstThread,
                                         long keynum) {
            super(index, firstThread, keynum);
        }

        @Override
        TableIterator<PrimaryKey> getIterator(PrimaryKey key,
                                              TableIteratorOptions options) {
            return tableImpl.tableKeysIterator(key, null /* MultiRowOptions */,
                                               options);
        }

        @Override
        void checkResult(PrimaryKey key, PrimaryKey foundResult) {
            if (foundResult != null) {
                checkKeyPresent(foundResult);
            }
        }
    }

    class CompletePrimaryKeyBulkKeysIteratorOp
            extends CompletePrimaryKeyKeysIteratorOp {
        CompletePrimaryKeyBulkKeysIteratorOp(long index,
                                             boolean firstThread,
                                             long keynum) {
            super(index, firstThread, keynum);
        }

        @Override
        TableIterator<PrimaryKey> getIterator(PrimaryKey key,
                                              TableIteratorOptions options) {
            return tableImpl.tableKeysIterator(
                getKeyIterator(key), null /* multiRowOptions */, options);
        }
    }

    abstract class AbstractIncompletePrimaryKeyIteratorOp<R>
            extends AbstractKeyIteratorOp<R> {

        AbstractIncompletePrimaryKeyIteratorOp(long index,
                                               boolean firstThread,
                                               long keynum) {
            super(index, firstThread, keynum);
        }

        /** Return an iterator over the results */
        abstract TableIterator<R>
            getIterator(PrimaryKey key,
                        MultiRowOptions multiRowOptions,
                        TableIteratorOptions tableIterOptions);

        /** Check if the primary key matches the result */
        abstract boolean compareResult(PrimaryKey key, R result);
    }

    abstract class AbstractCompleteShardPrimaryKeyIteratorOp<R>
            extends AbstractIncompletePrimaryKeyIteratorOp<R> {

        AbstractCompleteShardPrimaryKeyIteratorOp(long index,
                                                  boolean firstThread,
                                                  long keynum) {
            super(index, firstThread, keynum);
        }

        @Override
        void doOp(long timeoutMillis) {
            final PrimaryKey completeKey = keynumToKey(keynum);
            final PrimaryKey partialKey = table.createPrimaryKey();
            partialKey.put(FLD_MAJORKEY1, completeKey.get(FLD_MAJORKEY1));
            partialKey.put(FLD_MAJORKEY2, completeKey.get(FLD_MAJORKEY2));
            final String minorKey =
                completeKey.get(FLD_MINORKEY).asString().get();
            final MultiRowOptions multiRowOptions =
                completeKey.getTable()
                .createFieldRange(FLD_MINORKEY)
                .setStart(minorKey, true)
                .setEnd(minorKey + "z", false)
                .createMultiRowOptions();
            final TableIterator<R> iter = getIterator(
                partialKey, multiRowOptions,
                getTableIteratorOptions(timeoutMillis));
            R foundResult = null;
            while (iter.hasNext()) {
                final R result = iter.next();
                if (compareResult(completeKey, result)) {
                    foundResult = result;
                    break;
                }
            }
            checkResult(completeKey, foundResult);
        }
    }

    class CompleteShardPrimaryKeyIteratorOp
            extends AbstractCompleteShardPrimaryKeyIteratorOp<Row> {

        CompleteShardPrimaryKeyIteratorOp(long index,
                                          boolean firstThread,
                                          long keynum) {
            super(index, firstThread, keynum);
        }

        @Override
        TableIterator<Row> getIterator(PrimaryKey partialKey,
                                       MultiRowOptions multiRowOptions,
                                       TableIteratorOptions tableIterOptions) {
            return tableImpl.tableIterator(
                partialKey, multiRowOptions, tableIterOptions);
        }

        @Override
        boolean compareResult(PrimaryKey completeKey, Row result) {
            return completeKey.equals(result.createPrimaryKey());
        }

        @Override
        void checkResult(PrimaryKey completeKey, Row foundResult) {
            checkValue(completeKey, foundResult);
        }
    }

    class CompleteShardPrimaryKeyBulkIteratorOp
            extends CompleteShardPrimaryKeyIteratorOp {
        CompleteShardPrimaryKeyBulkIteratorOp(long index,
                                              boolean firstThread,
                                              long keynum) {
            super(index, firstThread, keynum);
        }

        @Override
        TableIterator<Row> getIterator(PrimaryKey partialKey,
                                       MultiRowOptions multiRowOptions,
                                       TableIteratorOptions tableIterOptions) {
            return tableImpl.tableIterator(getKeyIterator(partialKey),
                                           multiRowOptions, tableIterOptions);
        }
    }

    class CompleteShardPrimaryKeyKeysIteratorOp
            extends AbstractCompleteShardPrimaryKeyIteratorOp<PrimaryKey> {

        CompleteShardPrimaryKeyKeysIteratorOp(long index,
                                              boolean firstThread,
                                              long keynum) {
            super(index, firstThread, keynum);
        }

        @Override
        TableIterator<PrimaryKey>
            getIterator(PrimaryKey partialKey,
                        MultiRowOptions multiRowOptions,
                        TableIteratorOptions tableIterOptions) {
            return tableImpl.tableKeysIterator(
                partialKey, multiRowOptions, tableIterOptions);
        }

        @Override
        boolean compareResult(PrimaryKey key, PrimaryKey result) {
            return key.equals(result);
        }

        @Override
        void checkResult(PrimaryKey completeKey, PrimaryKey foundResult) {
            if (foundResult != null) {
                checkKeyPresent(foundResult);
            }
        }
    }

    class CompleteShardPrimaryKeyBulkKeysIteratorOp
            extends CompleteShardPrimaryKeyKeysIteratorOp {
        CompleteShardPrimaryKeyBulkKeysIteratorOp(long index,
                                                  boolean firstThread,
                                                  long keynum) {
            super(index, firstThread, keynum);
        }

        @Override
        TableIterator<PrimaryKey>
            getIterator(PrimaryKey partialKey,
                        MultiRowOptions multiRowOptions,
                        TableIteratorOptions tableIterOptions) {
            return tableImpl.tableKeysIterator(getKeyIterator(partialKey),
                                               multiRowOptions,
                                               tableIterOptions);
        }
    }

    abstract class AbstractPartialShardPrimaryKeyIteratorOp<R>
            extends AbstractIncompletePrimaryKeyIteratorOp<R> {

        AbstractPartialShardPrimaryKeyIteratorOp(long index,
                                                 boolean firstThread,
                                                 long keynum) {
            super(index, firstThread, keynum);
        }

        @Override
        void doOp(long timeoutMillis) {
            final PrimaryKey completeKey = keynumToKey(keynum);
            final PrimaryKey partialKey = table.createPrimaryKey();
            partialKey.put(FLD_MAJORKEY1, completeKey.get(FLD_MAJORKEY1));
            final String majorKey2 =
                completeKey.get(FLD_MAJORKEY2).asString().get();
            final MultiRowOptions multiRowOptions =
                completeKey.getTable()
                .createFieldRange(FLD_MAJORKEY2)
                .setStart(majorKey2, true)
                .setEnd(majorKey2 + "z", false)
                .createMultiRowOptions();
            final TableIterator<R> iter = getIterator(
                partialKey, multiRowOptions,
                getTableIteratorOptions(timeoutMillis));
            R foundResult = null;
            while (iter.hasNext()) {
                final R result = iter.next();
                if (compareResult(completeKey, result)) {
                    foundResult = result;
                    break;
                }
            }
            checkResult(completeKey, foundResult);
        }
    }

    class PartialShardPrimaryKeyIteratorOp
            extends AbstractPartialShardPrimaryKeyIteratorOp<Row> {

        PartialShardPrimaryKeyIteratorOp(long index,
                                         boolean firstThread,
                                         long keynum) {
            super(index, firstThread, keynum);
        }

        @Override
        TableIterator<Row> getIterator(PrimaryKey key,
                                       MultiRowOptions multiRowOptions,
                                       TableIteratorOptions tableIterOptions) {
            return tableImpl.tableIterator(
                key, multiRowOptions, tableIterOptions);
        }

        @Override
        void checkResult(PrimaryKey completeKey, Row foundResult) {
            checkValue(completeKey, foundResult);
        }

        @Override
        boolean compareResult(PrimaryKey completeKey, Row result) {
            return completeKey.equals(result.createPrimaryKey());
        }

    }

    class PartialShardPrimaryKeyKeysIteratorOp
            extends AbstractPartialShardPrimaryKeyIteratorOp<PrimaryKey> {

        PartialShardPrimaryKeyKeysIteratorOp(long index,
                                             boolean firstThread,
                                             long keynum) {
            super(index, firstThread, keynum);
        }

        @Override
        TableIterator<PrimaryKey>
            getIterator(PrimaryKey key,
                        MultiRowOptions multiRowOptions,
                        TableIteratorOptions tableIterOptions) {
            return tableImpl.tableKeysIterator(
                key, multiRowOptions, tableIterOptions);
        }

        @Override
        boolean compareResult(PrimaryKey key, PrimaryKey result) {
            return key.equals(result);
        }

        @Override
        void checkResult(PrimaryKey completeKey, PrimaryKey foundResult) {
            if (foundResult != null) {
                checkKeyPresent(foundResult);
            }
        }
    }

    @Override
    void doCompletePrimaryKeyQueryOp(
        ReadOp<PrimaryKey, Row, TableOperation> readOp,
        long keynum,
        long timeoutMillis)
    {
        final PrimaryKey key = keynumToKey(keynum);
        final BoundStatement bound =
            getBoundStatement(COMPLETE_PRIMARY_KEY_QUERY_STMT)
            .setVariable("$v0", key.get(FLD_MAJORKEY1))
            .setVariable("$v1", key.get(FLD_MAJORKEY2))
            .setVariable("$v2", key.get(FLD_MINORKEY));
        final TableIterator<RecordValue> iter =
            store.executeSync(bound, getExecuteOptions(timeoutMillis))
                 .iterator();
        final Row row = iter.hasNext() ? table.createRow(iter.next()) : null;
        readOp.checkValue(key, row);
    }

    /**
     * Return a bound statement for the specified query, preparing the query
     * if needed.
     */
    private BoundStatement getBoundStatement(String query) {
        return getQueryPreparedStatement(query).createBoundStatement();
    }

    @Override
    void doCompleteShardPrimaryKeyQueryOp(
        ReadOp<PrimaryKey, Row, TableOperation> readOp,
        long keynum,
        long timeoutMillis)
    {
        final PrimaryKey completeKey = keynumToKey(keynum);
        final BoundStatement bound =
            getBoundStatement(COMPLETE_SHARD_PRIMARY_KEY_QUERY_STMT)
            .setVariable("$v0", completeKey.get(FLD_MAJORKEY1))
            .setVariable("$v1", completeKey.get(FLD_MAJORKEY2))
            .setVariable("$v2", completeKey.get(FLD_MINORKEY));
        Row foundRow = null;
        for (final RecordValue recordValue :
                 store.executeSync(bound, getExecuteOptions(timeoutMillis))) {
            final PrimaryKey key = table.createPrimaryKey(recordValue);
            if (completeKey.equals(key)) {
                foundRow = table.createRow(recordValue);
                break;
            }
        }
        readOp.checkValue(completeKey, foundRow);
    }

    @Override
    void doPartialShardPrimaryKeyQueryOp(
        ReadOp<PrimaryKey, Row, TableOperation> readOp,
        long keynum,
        long timeoutMillis)
    {
        final PrimaryKey completeKey = keynumToKey(keynum);
        final BoundStatement bound =
            getBoundStatement(PARTIAL_SHARD_PRIMARY_KEY_QUERY_STMT)
            .setVariable("$v0", completeKey.get(FLD_MAJORKEY1))
            .setVariable("$v1", completeKey.get(FLD_MAJORKEY2))
            .setVariable("$v2", completeKey.get(FLD_MINORKEY));
        Row foundRow = null;
        for (final RecordValue recordValue :
                 store.executeSync(bound, getExecuteOptions(timeoutMillis))) {
            final PrimaryKey key = table.createPrimaryKey(recordValue);
            if (completeKey.equals(key)) {
                foundRow = table.createRow(recordValue);
                break;
            }
        }
        readOp.checkValue(completeKey, foundRow);
    }

    @Override
    void doSecondaryKeyQueryOp(SecondaryKeyQueryOp readOp,
                               long timeoutMillis)
    {
        final BoundStatement bound =
            getBoundStatement(SECONDARY_KEY_QUERY_STMT)
            .setVariable("$v0", readOp.index);
        for (final RecordValue recordValue :
                 store.executeSync(bound, getExecuteOptions(timeoutMillis))) {
            final Row row = table.createRow(recordValue);
            readOp.checkValue(row.createPrimaryKey(), row);
        }
    }

    /*
     * Query update operations
     */

    @Override
    Row doQueryOp(Object boundStmt,
                  long timeoutMillis,
                  boolean requestPrevValue,
                  QueryOp queryOp) {

        final TableIterator<RecordValue> iter =
            store.executeSync((BoundStatement) boundStmt,
                              getExecuteOptions(timeoutMillis))
            .iterator();
        final Row prevRow;
        if (!iter.hasNext()) {
            prevRow = null;
        } else {
            final RecordValue recordValue = iter.next();
            if (requestPrevValue) {
                prevRow = table.createRow(recordValue);
            } else if (queryOp.getReturnCount(recordValue) > 0) {
                prevRow = getEmptyValue();
            } else {
                prevRow = null;
            }
        }
        return prevRow;
    }

    @Override
    Object getBoundStatement(Object prepared, Row row) {
        final BoundStatement bound =
            ((PreparedStatement) prepared).createBoundStatement();
        for (int i = 0; i < row.size(); i++) {
            bound.setVariable("$v" + i, row.get(i));
        }
        return bound;
    }

    @Override
    long getInsertReturnCount(Object recordValue) {
        return ((RecordValue) recordValue).get(0).asInteger().get();
    }

    @Override
    PreparedStatement getQueryPreparedStatement(String queryStmt) {
        return preparedStatements.computeIfAbsent(queryStmt, store::prepare);
    }

    @Override
    int getRowSize(Row row) {
        return row.size();
    }

    @Override
    long getUpdateReturnCount(Object recordValue) {
        return ((RecordValue) recordValue).get(0).asInteger().get();
    }

    @Override
    long getDeleteReturnCount(Object recordValue) {
        return ((RecordValue) recordValue).get(0).asLong().get();
    }

    @Override
    List <String> getFieldNames(Row row) {
        return row.getFieldNames();
    }

    @Override
    String getFieldName(Row row, int fieldPos) {
        return row.getFieldName(fieldPos);
    }

    @Override
    protected void updateTTLTestHook(Phase phase, long start, long count) {
        if (!useTTL) {
            return;
        }
        /* Generate minor key using the start and count */
        final Key minorKey = Key.createKey(UPDATE_HISTORY,
            Phase.POPULATE + "-" + start + "-" + count);
        switch (phase) {
        case POPULATE:
            long currentTime = System.currentTimeMillis();
            setRemoteTTLTime(currentTime);
            /* Save the populate time information into store */
            faultingPutInternal(minorKey,
                                dataSerializer.seedToValue(currentTime),
                                Durability.COMMIT_SYNC);
            break;
        case EXERCISE:
            /* Retrieve the previous populate phase time information */
            ValueVersion vv = faultingGetInternal(minorKey);
            if (vv == null) {
                throw new RuntimeException(
                    "Cannot find previous populate phase time information");
            }
            long popStartTime = dataSerializer.valueToSeed(vv.getValue());
            /* Calculate the exercise TTL current time in milliseconds */
            long exerCurrentTime =
                popStartTime + MEDIUM_TTL_DAY * MILLI_SECONDS_PER_DAY;
            setRemoteTTLTime(exerCurrentTime);
            break;
        case CHECK:
            /* The current time has been set in exercise phase */
            break;
        case CLEAN:
            break;
        default:
            throw new AssertionError();
        }
    }

    /**
     * Set the server side TTL current time.
     */
    private void setRemoteTTLTime(long currentTime) {
        if (!useTTL) {
            return;
        }
        try {

            /*
             * Fix for KVSTORE-2058: upgrade test failure
             * By the time the code path reaches here the upgrade is done
             * which causes the CommandShell object cs to be stale.
             * Hence moved the initialization of CommandShell from constructor
             */
            KVStoreLogin storeLogin = new KVStoreLogin();
            storeLogin.loadSecurityProperties();
            output = new ByteArrayOutputStream();
            PrintStream out = new PrintStream(output);
            final CommandShell cs = new CommandShell(null, out);
            cs.connectAdmin(adminHostName,
                            Integer.parseInt(adminPortNum),
                            null,
                            storeLogin.getSecurityFilePath());
            /*
             * Get the store version to see if we can use the new approach to
             * setting the TTL time or have to stick with the old one because
             * we are testing an upgrade. This check and the old way of setting
             * the TTL time are only needed while we still need to test
             * versions older than 23.1.
             */
            output.reset();
            cs.execute("show parameters -global -hidden");
            final String globalParams = output.toString();
            final Pattern storeVersionPattern =
                Pattern.compile("storeSoftwareVersion=([0-9.]*)");
            final Matcher storeVersionMatcher =
                storeVersionPattern.matcher(globalParams);
            boolean useNewTTLSettingApproach = false;
            if (storeVersionMatcher.find()) {
                final String storeVersionString = storeVersionMatcher.group(1);
                final KVVersion storeVersion =
                    KVVersion.parseVersion(storeVersionString);
                if (storeVersion.compareTo(KVVersion.R23_1) >= 0) {
                    /* OK to use new approach with 23.1 and later */
                    useNewTTLSettingApproach = true;
                }
            }
            if (!useNewTTLSettingApproach) {
                output.reset();
                cs.execute("show parameters -policy");
                String paramInPolicy =
                    buildJavaMiscParamWithTTL(output.toString(), currentTime);
                output.reset();
                /* Set the javaMiscParams to add TTL time globally, no output */
                cs.execute("change-policy -params javaMiscParams='" +
                           paramInPolicy.trim() + "'");
                /* no plan output for change-policy command */

                Topology topo = cs.getAdmin().getTopology();
                for (RepNodeId rnId : topo.getRepNodeIds()) {
                    output.reset();
                    cs.execute("show parameters -service " + rnId);
                    String paramInRN = buildJavaMiscParamWithTTL(
                        output.toString(), currentTime);
                    output.reset();
                    /* Set the javaMiscParams to add TTL time for all RNs */
                    cs.execute("plan change-param -service " + rnId +
                               " -wait -params " + "javaMiscParams='" +
                               paramInRN.trim() + "'");
                    checkPlanOutput("successfully");
                }
                return;
            }

            /*
             * Put the TTL time setting into the policy value of
             * JVM_RN_OVERRIDE_NO_RESTART so that RNs added in the future get
             * it.
             */
            final Pattern paramPattern =
                Pattern.compile(JVM_RN_OVERRIDE_NO_RESTART + "=.*");
            final String setTime =
                "-D" + TTLTestHook.TTL_CURRENT_TIME_PROP + "=";
            final String setAnyTime = setTime + "[0-9]*";
            final String setCurrentTime = " " + setTime + currentTime;

            output.reset();
            cs.execute("show parameters -policy -hidden");
            final String allPolicy = output.toString();
            Matcher paramMatcher = paramPattern.matcher(allPolicy);

            /*
             * Check if JVM_RN_OVERRIDE_NO_RESTART is specified in the policy.
             * If it is, remove any TTL time settings, and otherwise use an
             * empty setting.
             */
            String setParam = paramMatcher.find() ?
                paramMatcher.group(0).replaceAll(setAnyTime, "").trim() :
                JVM_RN_OVERRIDE_NO_RESTART + "=";

            output.reset();
            cs.execute("change-policy -hidden -params '" + setParam +
                       setCurrentTime + "'");
            /* no plan output for change-policy command */
            if (!"".equals(output.toString().trim())) {
                throw new RuntimeException("Change policy failed: " + output);
            }

            /* Set the TTL time for current RNs */
            final Topology topo = cs.getAdmin().getTopology();
            for (final RepNode rn : topo.getSortedRepNodes()) {
                final RepNodeId rnId = rn.getResourceId();

                /*
                 * Put the TTL time setting into JVM_RN_OVERRIDE_NO_RESTART so
                 * it will take effect on restart
                 */
                output.reset();
                cs.execute("show parameters -hidden -service " +
                           rnId.getFullName());
                final String params = output.toString();
                paramMatcher = paramPattern.matcher(params);
                setParam = paramMatcher.find() ?
                    paramMatcher.group(0).replaceAll(setAnyTime, "").trim() :
                    JVM_RN_OVERRIDE_NO_RESTART + "=";

                output.reset();
                cs.execute("plan change-parameters -hidden -wait" +
                           " -service " + rnId.getFullName() +
                           " -params '" + setParam + setCurrentTime + "'");
                checkPlanOutput("successfully");

                /*
                 * Then call RemoteTestAPI.setTTLTime for running RNs so we can
                 * set the TTL time without restarting them.
                 */
                final StorageNode sn = topo.get(rn.getStorageNodeId());
                final String bindingName = RegistryUtils.bindingName(
                    topo.getKVStoreName(), rn.getResourceId().getFullName(),
                    InterfaceType.TEST);
                final Logger logger =
                    Logger.getLogger(DataCheckTable.class.getName());
                final RemoteTestAPI testAPI = RegistryUtils.getRemoteTest(
                    topo.getKVStoreName(), sn.getHostname(),
                    sn.getRegistryPort(), bindingName, logger);
                testAPI.setTTLTime(currentTime);
            }
        } catch (Exception e) {
            throw new RuntimeException("Fail to set remote ttl time. " +
                "Note that it requires to use kvstoretest jar for server " +
                "if TTL test is enabled.\n " +
                e.getMessage(), e);
        }
    }

    /**
     * Check whether output of plan execution contain the expected message.
     */
    private void checkPlanOutput(String keyword) {
        if (!output.toString().contains(keyword)) {
            throw new RuntimeException(output.toString());
        }
    }

    /**
     * Construct javaMiscParam value with added ttl time property.
     */
    private String buildJavaMiscParamWithTTL(String showResult, long ttlTime) {
        String[] lines = showResult.split("\n");
        String value = null;
        for (String line : lines) {
            line = line.trim();
            if (line.contains("javaMiscParams")) {
                int start = line.indexOf("=");
                value = line.substring(start + 1);
                break;
            }
        }
        if (value == null) {
            throw new RuntimeException(
                "Current javaMiscParams settings was not found");
        }
        String[] parameters = value.trim().split("\\s+");
        StringBuilder newParams = new StringBuilder();
        boolean isUpdated = false;
        String newTTLTime = "-D" + TTLTestHook.TTL_CURRENT_TIME_PROP +
                "=" + ttlTime;
        for (String param : parameters) {
            if (param.contains(TTLTestHook.TTL_CURRENT_TIME_PROP)) {
                newParams.append(newTTLTime);
                isUpdated = true;
            } else {
                newParams.append(param + " ");
            }
        }
        if (!isUpdated) {
            newParams.append(newTTLTime);
        }
        return newParams.toString();
    }

    /*
     * Copy the field values from primary key to target row's fields
     */
    private void copyPrimaryKey(Row row, PrimaryKey key) {
        for (String fieldName : row.getDefinition().getFieldNames()) {
            try {
               FieldValue val = key.get(fieldName);
               if (val != null) {
                   row.put(fieldName, val);
               }
            } catch (IllegalArgumentException iae) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }
    }

    @Override
    Row getRow(Key key, Value value) {
        final Row r = keyToMDRow(key);

        /*
         * Store the value if represents a random seed, and store nothing
         * if the operation represents EMPTY_VALUE.
         */
        if (value.getValue().length > 0) {
            final long seed = dataSerializer.valueToSeed(value);
            r.put(FLD_MDVALUE, seed);
        }

        return r;
    }

    @Override
    void runFaultingPutTable(Row row, Object durability,
        long timeoutMillis) {
        tableImpl.put(row,
                      null /* prevValue */,
                      new WriteOptions((Durability) durability,
                                       timeoutMillis,
                                       MILLISECONDS));

        final PrimaryKey pKey = getDataCheckMDTable().createPrimaryKey();
        final int npKeys = getDataCheckMDTable().getPrimaryKey().size();
        for (int i = 0; i < npKeys; i++) {
            pKey.put(i, row.get(i));
        }
        tallyPut(pKey, row);
    }

    @Override
    Set<Key> callFaultingMultiGetKeysTable(PrimaryKey pKey,
        long timeoutMillis) {
        List<PrimaryKey> pKeys =
            tableImpl.multiGetKeys(pKey,
                                   null,
                                   new ReadOptions(Consistency.ABSOLUTE,
                                                   timeoutMillis,
                                                   MILLISECONDS));

        final SortedSet<Key> keys = new TreeSet<>();
        for (final PrimaryKey pk : pKeys) {
            keys.add(mdPrimaryKeyToKey(pk));
        }
        return keys;
    }

    @Override
    void runFaultingMultiDeleteTable(PrimaryKey pKey, Object durability,
        long timeoutMillis) {
        tableImpl.multiDelete(pKey,
                              null,
                              new WriteOptions((Durability) durability,
                                               timeoutMillis,
                                               MILLISECONDS));
    }

    @Override
    ValueVersion callFaultingGetTable(PrimaryKey pKey,
        long timeoutMillis) {
        final Row row = tableImpl.get(pKey,
                                      new ReadOptions(Consistency.ABSOLUTE,
                                                      timeoutMillis,
                                                      MILLISECONDS));

        /* Return null if the entry was not found */
        if (row == null) {
            return null;
        }

        /*
         * Otherwise, treat the value as a long. The caller will only use
         * the value if it represents a random seed
         */
        final long seed = row.get(FLD_MDVALUE).asLong().get();
        return new ValueVersion(dataSerializer.seedToValue(seed), null);
    }

    Row keyToMDRow(Key key) {
        final Row row = getDataCheckMDTable().createRow();
        addMDKeyFields(key, row, false);
        return row;
    }

    @Override
    PrimaryKey keyToMDPrimaryKey(Key key, boolean skipMinor) {
        final PrimaryKey pKey = getDataCheckMDTable().createPrimaryKey();
        addMDKeyFields(key, pKey, skipMinor);
        return pKey;
    }

    /**
     * Adds major and minor components from the key to the row, which may
     * actually be a primary key.
     * Skips the first major key component ("m") since that component is used
     * to mark all metadata entries.
     * Puts the remaining major key components, up to a maximum of 3, into the
     * three major key columns viz. mjkey1, mjkey2 and mjkey3 which also form
     * a composite shard key. If any major key component is not expected (see
     * below for the various key formats) then put an empty string for that
     * component as null values are not allowed by all api's expecting a
     * partial or full primary key.
     *
     * The key representing the seed /m/seed is stored as follows:
     * mjkey1      mjkey2    mjkey3    mikey                           value
     * =====================================================================
     * "seed"      ""        ""        ""                              seed
     *
     * The keys representing UPDATE_HISTORY /m/updates are stored as follows:
     * mjkey1      mjkey2    mjkey3    mikey                           value
     * =====================================================================
     * "updates"   ""        ""        ts-<phase>-<start>-<count>
     * "updates"   ""        ""        ts-<phase>-passed/failed-<start>-<count>
     *
     * The keys representing start of a phase block have the following
     * structure:
     * - /m/populateBlock/<parent>/-/<timestamp>
     * - /m/exerciseBlock/A/<parent>/-/<timestamp>
     * - /m/exerciseBlock/B/<parent>/-/<timestamp>
     * - /m/cleanBlock/<parent>/-/<timestamp>
     * - /m/cleaned-populateBlock/<parent>/-/<timestamp>
     * - /m/cleaned-exerciseBlock/A/<parent>/-/<timestamp>
     * - /m/cleaned-exerciseBlock/B/<parent>/-/<timestamp>
     * and are stored as follows:
     * mjkey1                   mjkey2    mjkey3    mikey
     * =====================================================================
     * "populateBlock"          <parent>  ""        <timestamp>
     * "exerciseBlock"          "A"       <parent>  <timestamp>
     * "exerciseBlock"          "B"       <parent>  <timestamp>
     * "cleanBlock"             <parent>  ""        <timestamp>
     * "cleaned-populateBlock"  <parent>  ""        <timestamp>
     * "cleaned-exerciseBlock"  "A"       <parent>  <timestamp>
     * "cleaned-exerciseBlock"  "B"       <parent>  <timestamp>
     *
     * If skipMinor is false then puts a single minor key component, if
     * present, into the minor key field, else puts an empty string. In other
     * words if skipMinor is true then a partial primary key is constructed
     * by populating values only in the composite shard key columns
     * (mjkey1, mjkey2, mjkey3). This is for multi key api's like multiGetKeys
     * and multiDelete. If skipMinor is false then a full primary key is
     * constructed by populating values in all primary key components
     * ((mjkey1, mjkey2, mjkey3), mikey). This is for single key api's like get.
     *
     * Throws an IllegalArgumentException if there are fewer than 2 or more
     * than 4 major key components, or if there is more than 1 minor key
     * component.
     */
    private void addMDKeyFields(Key key, Row row, boolean skipMinor) {
        final List<String> majorKeys =  key.getMajorPath();

        if (majorKeys.size() < 2 || majorKeys.size() > 4) {
            throw new IllegalArgumentException(
                "Incorrect number of major keys: " + majorKeys);
        }

        row.put(FLD_MDMAJORKEY1, majorKeys.get(1));
        row.put(FLD_MDMAJORKEY2,
                majorKeys.size() > 2 ? majorKeys.get(2) : "");
        row.put(FLD_MDMAJORKEY3,
                majorKeys.size() > 3 ? majorKeys.get(3) : "");

        if (!skipMinor) {
            final List<String> minorKeys =  key.getMinorPath();

            if (minorKeys.size() > 1) {
                throw new IllegalArgumentException(
                    "Incorrect number of minor keys: " + minorKeys);
            }

            row.put(FLD_MDMINORKEY,
                    minorKeys.size() > 0 ? minorKeys.get(0) : "");
        }
    }

    /**
     * converts a metadata table primary key to a Key by reversing the behavior
     * of addMDKeyFields.
     */
    Key mdPrimaryKeyToKey(PrimaryKey pKey) {
        final int npKeys = getDataCheckMDTable().getPrimaryKey().size();
        final List<String> major = new ArrayList<>();

        /*
         * skip columns with empty strings to support keys of the following
         * format
         * - /m/populateBlock/<parent key>
         * - /m/cleanBlock/<parent key>
         * - /m/cleaned-populateBlock/<parent key>
         * See addMDKeyFields method's comments section for details on the
         * various metadata key formats
         */
        major.add("m");
        for (int i = 0; i < npKeys; i++) {
            final String majorVal = pKey.get(i).asString().get();
            if (majorVal.isEmpty()) {
                continue;
            }
            major.add(majorVal);
        }

        final String minorVal = pKey.get(FLD_MDMINORKEY).asString().get();

        return Key.createKey(major, minorVal);
    }
}
