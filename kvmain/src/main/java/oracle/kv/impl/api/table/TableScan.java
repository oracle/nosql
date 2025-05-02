/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv.impl.api.table;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static oracle.kv.impl.async.FutureUtils.failedFuture;
import static oracle.kv.impl.async.FutureUtils.unwrapExceptionVoid;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.KeyValueVersion;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.KeySerializer;
import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.StoreIteratorParams;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.ops.MultiTableOperation;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.ops.ResultKey;
import oracle.kv.impl.api.ops.ResultKeyValueVersion;
import oracle.kv.impl.api.ops.TableIterate;
import oracle.kv.impl.api.ops.TableKeysIterate;
import oracle.kv.impl.api.parallelscan.PartitionScanIterator;
import oracle.kv.impl.async.AsyncTableIterator;
import oracle.kv.impl.async.IterationHandleNotifier;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.contextlogger.LogContext;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.stats.DetailedMetrics;
import oracle.kv.table.MultiGetResult;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.TableIteratorOptions;

import com.sleepycat.util.PackedInteger;

/**
 * Implementation of the table iterators. These iterators are partition- vs
 * shard-based. They extend the parallel scan code.
 */
public class TableScan {

    /* Prevent construction */
    private TableScan() {}

    /**
     * Creates a table iterator returning rows.
     *
     * @param apiImpl
     * @param key
     * @param getOptions
     * @param iterateOptions
     * @param partitions
     *
     * @return a table iterator
     */
    static AsyncTableIterator<Row>
        createTableIterator(final TableAPIImpl apiImpl,
                            final TableKey key,
                            final MultiRowOptions getOptions,
                            final TableIteratorOptions iterateOptions,
                            final Set<Integer> partitions,
                            IterationHandleNotifier iterHandleNotifier) {

        final TargetTables targetTables =
            TableAPIImpl.makeTargetTables(key.getTable(), getOptions);

        byte[] initResumeKey = getInitResumeKey(iterateOptions);

        ExecuteOptions options = new ExecuteOptions(iterateOptions);
        options.setContinuationKey(initResumeKey);

        final boolean excludeTombstones =
            (iterateOptions == null) || !iterateOptions.getIncludeTombstones();
        final StoreIteratorParams params =
            new StoreIteratorParams(TableAPIImpl.getDirection(iterateOptions,
                                                              key),
                                    TableAPIImpl.getBatchSize(iterateOptions),
                                    key.getKeyBytes(),
                                    TableAPIImpl.makeKeyRange(key, getOptions),
                                    Depth.PARENT_AND_DESCENDANTS,
                                    TableAPIImpl.getConsistency(iterateOptions),
                                    TableAPIImpl.getTimeout(iterateOptions),
                                    TableAPIImpl.getTimeoutUnit(iterateOptions),
                                    partitions,
                                    excludeTombstones);

        /*
         * If the major key is complete do single-partition iteration.
         */
        if (key.getMajorKeyComplete()) {
            return createPartitionRowIterator(
                apiImpl, params, key, targetTables, initResumeKey,
                iterHandleNotifier,
                iterateOptions == null ? null : iterateOptions.getLogContext(),
                iterateOptions == null ?
                    null : iterateOptions.getAuthContext(),
                iterateOptions == null ? false : iterateOptions.getNoCharge());
        }

        return new PartitionScanIterator<Row>(apiImpl.getStore(), options,
                                              params, iterHandleNotifier) {
            @Override
            protected TableIterate generateGetterOp(byte[] resumeKey) {
                return new TableIterate(params,
                                        targetTables,
                                        key.getMajorKeyComplete(),
                                        resumeKey,
                                        1 /* emptyReadFactor */);
            }

            @Override
            protected void convertResult(Result result, List<Row> elementList) {

                convertTableRowResults(apiImpl, key.getTable(), targetTables,
                                       result.getKeyValueVersionList(),
                                       elementList);
            }

            @Override
            protected int compare(Row one, Row two) {
                /*
                 * compare based on primary keys, not row. Scans return rows
                 * sorted by key, not value.
                 */
                return ((RowImpl) one).compareKeys(two);
            }
        };
    }

    /**
     * Returns a batch of rows associated with the specified partial primary
     * key and a continuation key if has more elements to read, if no more
     * elements the continuation key in result is null.
     */
    static MultiGetResult<Row> multiGet(TableAPIImpl apiImpl,
                                        TableKey key,
                                        byte[] continuationKey,
                                        MultiRowOptions getOptions,
                                        TableIteratorOptions iterateOptions) {
        return new PartitionMultiGetHandler(apiImpl, key, continuationKey,
                                            getOptions, iterateOptions)
            .execute();
    }

    static CompletableFuture<MultiGetResult<Row>>
        multiGetAsync(TableAPIImpl apiImpl,
                      TableKey key,
                      byte[] continuationKey,
                      MultiRowOptions getOptions,
                      TableIteratorOptions iterateOptions) {
        try {
            return new PartitionMultiGetHandler(apiImpl, key, continuationKey,
                                                getOptions, iterateOptions)
                .executeAsync();
        } catch (Throwable e) {
            return failedFuture(e);
        }
    }

    private static class PartitionMultiGetHandler
            extends BasicPartitionMultiGetHandler<Row> {
        PartitionMultiGetHandler(TableAPIImpl apiImpl,
                                 TableKey key,
                                 byte[] continuationKey,
                                 MultiRowOptions getOptions,
                                 TableIteratorOptions iterateOptions) {
            super(apiImpl, key, continuationKey, getOptions,
                  iterateOptions);
        }

        @Override
        InternalOperation createIterateOp(int readEmptyFactor) {
            return new TableIterate(params,
                                    targetTables,
                                    key.getMajorKeyComplete(),
                                    resumeKey,
                                    readEmptyFactor);
        }

        @Override
        void convertToResults(Result result) {
            convertTableRowResults(apiImpl, key.getTable(), targetTables,
                                   result.getKeyValueVersionList(), rows);
        }
    }

    /**
     * Returns a batch of primary keys associated with the specified partial
     * primary key and a continuation key if has more elements to read, if no
     * more elements the continuation key in result is null.
     */
    static MultiGetResult<PrimaryKey> multiGetKeys(
        TableAPIImpl apiImpl,
        TableKey key,
        byte[] continuationKey,
        MultiRowOptions getOptions,
        TableIteratorOptions iterateOptions) {

        return new PartitionMultiGetKeysHandler(apiImpl, key, continuationKey,
                                                getOptions, iterateOptions)
            .execute();
    }

    static CompletableFuture<MultiGetResult<PrimaryKey>>
        multiGetKeysAsync(TableAPIImpl apiImpl,
                          TableKey key,
                          byte[] continuationKey,
                          MultiRowOptions getOptions,
                          TableIteratorOptions iterateOptions) {
        try {
            return new PartitionMultiGetKeysHandler(apiImpl, key,
                                                    continuationKey,
                                                    getOptions, iterateOptions)
                .executeAsync();
        } catch (Throwable e) {
            return failedFuture(e);
        }
    }

    private static class PartitionMultiGetKeysHandler
            extends BasicPartitionMultiGetHandler<PrimaryKey> {
        PartitionMultiGetKeysHandler(TableAPIImpl apiImpl,
                                     TableKey key,
                                     byte[] continuationKey,
                                     MultiRowOptions getOptions,
                                     TableIteratorOptions iterateOptions) {
            super(apiImpl, key, continuationKey,
                  getOptions, iterateOptions);
        }

        @Override
        InternalOperation createIterateOp(int readEmptyFactor) {
            return new TableKeysIterate(params,
                                        targetTables,
                                        key.getMajorKeyComplete(),
                                        resumeKey,
                                        readEmptyFactor);
        }

        @Override
        void convertToResults(Result result) {
            convertTableKeyResults(key.getTable(), targetTables,
                                   result.getKeyList(), rows);
        }
    }

    /**
     * Creates a table iterator returning primary keys.
     *
     * @param apiImpl
     * @param key
     * @param getOptions
     * @param iterateOptions
     *
     * @return a table iterator
     */
    static AsyncTableIterator<PrimaryKey> createTableKeysIterator(
        final TableAPIImpl apiImpl,
        final TableKey key,
        final MultiRowOptions getOptions,
        final TableIteratorOptions iterateOptions,
        final IterationHandleNotifier iterHandleNotifier) {

        final TargetTables targetTables =
            TableAPIImpl.makeTargetTables(key.getTable(), getOptions);

        byte[] initResumeKey = getInitResumeKey(iterateOptions);

        ExecuteOptions options = new ExecuteOptions(iterateOptions);
        options.setContinuationKey(initResumeKey);

        final StoreIteratorParams params = new StoreIteratorParams(
            TableAPIImpl.getDirection(iterateOptions,
                                      key),
            TableAPIImpl.getBatchSize(iterateOptions),
            key.getKeyBytes(),
            TableAPIImpl.makeKeyRange(key, getOptions),
            Depth.PARENT_AND_DESCENDANTS,
            TableAPIImpl.getConsistency(iterateOptions),
            TableAPIImpl.getTimeout(iterateOptions),
            TableAPIImpl.getTimeoutUnit(iterateOptions),
            true /* excludeTombstones */);

        /*
         * If the major key is complete do single-partition iteration.
         */
        if (key.getMajorKeyComplete()) {
            return createPartitionKeyIterator(
                apiImpl, params, key, targetTables, initResumeKey,
                iterHandleNotifier,
                iterateOptions == null ? null : iterateOptions.getLogContext(),
                iterateOptions == null ?
                    null : iterateOptions.getAuthContext(),
                iterateOptions == null ? false : iterateOptions.getNoCharge());
        }

        return new PartitionScanIterator<PrimaryKey>(
            apiImpl.getStore(), options, params, iterHandleNotifier) {
            @Override
            protected TableKeysIterate generateGetterOp(byte[] resumeKey) {
                return new TableKeysIterate(params,
                                            targetTables,
                                            key.getMajorKeyComplete(),
                                            resumeKey,
                                            1 /* emptyReadFactor */);
            }

            @Override
            protected void convertResult(Result result,
                                         List<PrimaryKey> elementList) {

                convertTableKeyResults(key.getTable(), targetTables,
                                       result.getKeyList(), elementList);

            }

            @Override
            protected int compare(PrimaryKey one, PrimaryKey two) {
                return one.compareTo(two);
            }
        };
    }

    /**
    * Creates a table iterator returning table record key/values.
    *
    * @param apiImpl
    * @param key
    * @param getOptions
    * @param iterateOptions
    * @param partitions
    *
    * @return a table iterator
    */
   static AsyncTableIterator<KeyValueVersion>
       createTableKVIterator(final TableAPIImpl apiImpl,
                             final TableKey key,
                             final MultiRowOptions getOptions,
                             final TableIteratorOptions iterateOptions,
                             final Set<Integer> partitions) {

       final TargetTables targetTables =
           TableAPIImpl.makeTargetTables(key.getTable(), getOptions);

       ExecuteOptions options = new ExecuteOptions(iterateOptions);

       final StoreIteratorParams params =
           new StoreIteratorParams(TableAPIImpl.getDirection(iterateOptions,
                                                             key),
                                   TableAPIImpl.getBatchSize(iterateOptions),
                                   key.getKeyBytes(),
                                   TableAPIImpl.makeKeyRange(key, getOptions),
                                   Depth.PARENT_AND_DESCENDANTS,
                                   TableAPIImpl.getConsistency(iterateOptions),
                                   TableAPIImpl.getTimeout(iterateOptions),
                                   TableAPIImpl.getTimeoutUnit(iterateOptions),
                                   partitions,
                                   true /* excludeTombstones */);

       /*
        * If the major key is complete do single-partition iteration.
        */
       if (key.getMajorKeyComplete()) {
           throw new IllegalArgumentException("The major path cannot be " +
               "complete for the key.");
       }

       return new PartitionScanIterator<KeyValueVersion>(apiImpl.getStore(),
                                                         options, params) {
           @Override
           protected TableIterate generateGetterOp(byte[] resumeKey) {
               return new TableIterate(params,
                                       targetTables,
                                       key.getMajorKeyComplete(),
                                       resumeKey,
                                       1 /* emptyReadFactor */);
           }

           @Override
           protected void convertResult(Result result,
                                        List<KeyValueVersion> elementList) {

               final List<ResultKeyValueVersion> byteKeyResults =
                       result.getKeyValueVersionList();

                   int cnt = byteKeyResults.size();
                   if (cnt == 0) {
                       assert (!result.hasMoreElements());
                       return;
                   }
                   for (int i = 0; i < cnt; i += 1) {
                       final ResultKeyValueVersion entry =
                           byteKeyResults.get(i);
                       KeySerializer keySerializer =
                           storeImpl.getKeySerializer();
                       elementList.add(KVStoreImpl.createKeyValueVersion(
                           keySerializer.fromByteArray(entry.getKeyBytes()),
                           entry.getValue(),
                           entry.getVersion(),
                           entry.getExpirationTime(),
                           entry.getModificationTime()));
                   }
           }

           @Override
           protected int compare(KeyValueVersion one, KeyValueVersion two) {
               return one.getKey().compareTo(two.getKey());
           }
       };
   }

    /**
     * Common routine to convert a list of ResultKeyValueVersion objects into
     * Rows and add them to the input List of Row.
     */
    private static void
        convertTableRowResults(TableAPIImpl apiImpl,
                               TableImpl table,
                               TargetTables targetTables,
                               final List<ResultKeyValueVersion> byteKeyResults,
                               List<Row> rowResults) {

        if (byteKeyResults.isEmpty()) {
            return;
        }

        /*
         * Convert byte[] keys and values to Row objects.
         */
        for (ResultKeyValueVersion entry : byteKeyResults) {
            rowResults.add(convertToRow(apiImpl, entry, table, targetTables));
        }
    }

    /**
     * Common routine to convert a list of ResultKeyValueVersion into an array
     * of Row.
     */
    private static Row[]
        convertTableRowResults(TableAPIImpl apiImpl,
                               TableImpl table,
                               TargetTables targetTables,
                               final List<ResultKeyValueVersion> byteKeyResults) {

        if (byteKeyResults.isEmpty()) {
            return null;
        }

        /*
         * Convert byte[] keys and values to Row objects.
         */
        Row[] rows = new Row[byteKeyResults.size()];
        int i = 0;
        for (ResultKeyValueVersion entry : byteKeyResults) {
            rows[i++] = convertToRow(apiImpl, entry, table, targetTables);
        }
        return rows;
    }

    /**
     * Converts a ResultKeyValueVersion to a Row.
     */
    private static Row convertToRow(TableAPIImpl apiImpl,
                                    final ResultKeyValueVersion rkvv,
                                    TableImpl table,
                                    TargetTables targetTables) {
        /*
         * If there are ancestor tables, start looking at the top
         * of the hierarchy to catch them.
         */
        if (targetTables.hasAncestorTables()) {
            table = table.getTopLevelTable();
        }

        final RowImpl fullKey = table.createRowFromKeyBytes(rkvv.getKeyBytes());

        if (fullKey != null) {
            final Version version = rkvv.getVersion();
            assert version != null;
            final ValueVersion vv = new ValueVersion(rkvv.getValue(), version);
            return apiImpl.getRowFromValueVersion(vv,
                                                  fullKey,
                                                  rkvv.getExpirationTime(),
                                                  rkvv.getModificationTime(),
                                                  false,
                                                  rkvv.getIsTombstone());
        }
        return null;
    }

    /**
     * Common routine to convert a list of ResultKey representing table keys
     * into PrimaryKeys and add them to the input List of PrimaryKey.
     */
    private static void
        convertTableKeyResults(TableImpl table,
                               TargetTables targetTables,
                               final List<ResultKey> byteKeyResults,
                               List<PrimaryKey> keyResults) {

        if (byteKeyResults.isEmpty()) {
            return;
        }

        for (ResultKey entry : byteKeyResults) {
            keyResults.add(convertToPrimaryKey(table, targetTables, entry));
        }
    }

    /**
     * Common routine to convert a list of ResultKey representing table keys
     * into an array of PrimaryKey.
     */
    private static PrimaryKey[]
        convertTableKeyResults(TableImpl table,
                               TargetTables targetTables,
                               final List<ResultKey> byteKeyResults) {

        if (byteKeyResults.isEmpty()) {
            return null;
        }

        /*
         * Convert byte[] keys to PrimaryKey objects.
         */
        final PrimaryKey[] keyResults = new PrimaryKey[byteKeyResults.size()];
        int i = 0;
        for (ResultKey entry : byteKeyResults) {
            keyResults[i++] = convertToPrimaryKey(table, targetTables, entry);
        }

        return keyResults;
    }

    /**
     * Converts a ResultKey to a PrimaryKey.
     */
    private static PrimaryKey convertToPrimaryKey(TableImpl table,
                                                  TargetTables targetTables,
                                                  ResultKey byteKeyResult) {
        /*
         * If there are ancestor tables, start looking at the top
         * of the hierarchy to catch them.
         */
        if (targetTables.hasAncestorTables()) {
            table = table.getTopLevelTable();
        }
        return table.createPrimaryKeyFromResultKey(byteKeyResult);
    }

    /**
     * Returns the key bytes of TableIteratorOptions.resumePrimaryKey.
     */
    private static byte[] getInitResumeKey(TableIteratorOptions options) {
        PrimaryKey resumeKey = (options != null)?
                               options.getResumePrimaryKey() : null;
        if (resumeKey != null) {
            return TableKey.createKey(resumeKey.getTable(), resumeKey, true)
                        .getKeyBytes();
        }
        return null;
    }

    /**
     * Creates a single-partition table row iterator.
     */
    private static AsyncTableIterator<Row> createPartitionRowIterator(
        final TableAPIImpl apiImpl,
        final StoreIteratorParams params,
        final TableKey key,
        final TargetTables targetTables,
        final byte[] initResumeKey,
        final IterationHandleNotifier iterHandleNotifier,
        final LogContext lc,
        final AuthContext auth,
        final boolean noCharge) {

        final KVStoreImpl store = apiImpl.getStore();
        final byte[] parentKeyBytes =
            store.getKeySerializer().toByteArray(key.getKey());
        final PartitionId partitionId =
            store.getDispatcher().getPartitionId(parentKeyBytes);

        /*
         * If there was a list of partitions specified, then we should check to
         * make sure the target partition is in the list. If not, then return
         * an iterator which has no elements.
         */
        final Set<Integer> partitions = params.getPartitions();
        if ((partitions != null) &&
            !partitions.contains(partitionId.getPartitionId())) {
            return new EmptyTableIterator<Row>(iterHandleNotifier);
        }

        final TableImpl table = key.getTable();

        return new MultiGetIteratorWrapper<Row>(store, partitionId, params,
                                                initResumeKey,
                                                iterHandleNotifier,
                                                lc, auth, noCharge) {
            @Override
            TableIterate createOp() {
                return new TableIterate(params, targetTables, true, resumeKey,
                                        1 /* emptyReadFactor */);
            }

            @Override
            Row[] processResult(Result result) {
                moreElements = result.hasMoreElements();
                final List<ResultKeyValueVersion> byteKeyResults =
                    result.getKeyValueVersionList();
                if (byteKeyResults.isEmpty()) {
                    assert !moreElements;
                    return null;
                }
                resumeKey =
                    byteKeyResults.get(byteKeyResults.size() - 1).getKeyBytes();
                return convertTableRowResults(apiImpl, table, targetTables,
                                              byteKeyResults);
            }
        };
    }

    /**
     * Creates a single-partition table key iterator.
     */
    private static AsyncTableIterator<PrimaryKey> createPartitionKeyIterator(
        final TableAPIImpl apiImpl,
        final StoreIteratorParams params,
        final TableKey key,
        final TargetTables targetTables,
        final byte[] initResumeKey,
        final IterationHandleNotifier iterHandleNotifier,
        final LogContext lc,
        final AuthContext auth,
        final boolean noCharge) {

        final KVStoreImpl store = apiImpl.getStore();
        final byte[] parentKeyBytes =
            store.getKeySerializer().toByteArray(key.getKey());
        final PartitionId partitionId =
            store.getDispatcher().getPartitionId(parentKeyBytes);

        /*
         * If there was a list of partitions specified, then we should check to
         * make sure the target partition is in the list. If not, then return
         * an iterator which has no elements.
         */
        final Set<Integer> partitions = params.getPartitions();
        if ((partitions != null) &&
            !partitions.contains(partitionId.getPartitionId())) {
            return new EmptyTableIterator<PrimaryKey>(iterHandleNotifier);
        }

        final TableImpl table = key.getTable();

        return new MultiGetIteratorWrapper<PrimaryKey>(store, partitionId,
                                                       params,
                                                       initResumeKey,
                                                       iterHandleNotifier,
                                                       lc, auth, noCharge) {
            @Override
            TableKeysIterate createOp() {
                return new TableKeysIterate(params, targetTables, true,
                                            resumeKey, 1 /* emptyReadFactor */);
            }

            @Override
            PrimaryKey[] processResult(Result result) {
                moreElements = result.hasMoreElements();
                final List<ResultKey> byteKeyResults = result.getKeyList();
                if (byteKeyResults.isEmpty()) {
                    assert !moreElements;
                    return null;
                }
                resumeKey = byteKeyResults.
                    get(byteKeyResults.size() - 1).getKeyBytes();
                return convertTableKeyResults(table,
                                              targetTables,
                                              byteKeyResults);
            }
        };
    }

    /**
     * Wrapper class for ParallelScanIterator when it is a single-partition
     * iteration.
     */
    private static abstract class MultiGetIteratorWrapper<E>
            extends BasicMultiGetIteratorWrapper<E> {
        private final KVStoreImpl store;
        private final PartitionId partitionId;
        private final StoreIteratorParams params;
        private final LogContext lc;
        private final AuthContext auth;
        private final boolean noCharge;
        private CompletableFuture<E[]> future;
        boolean moreElements = true;
        byte[] resumeKey = null;

        MultiGetIteratorWrapper(KVStoreImpl store,
                                PartitionId partitionId,
                                StoreIteratorParams params,
                                byte[] initResumeKey,
                                IterationHandleNotifier iterHandlerNotifier,
                                LogContext lc,
                                AuthContext auth,
                                boolean noCharge) {
            super(iterHandlerNotifier);
            this.store = store;
            this.partitionId = partitionId;
            this.params = params;
            this.lc = lc;
            this.auth = auth;
            resumeKey = initResumeKey;
            this.noCharge = noCharge;
        }

        @Override
        E[] getMoreElements() {
            if (!moreElements) {
                return null;
            }
            return processResult(store.executeRequest(createRequest()));
        }

        @Override
        boolean hasMoreElements() {
            return moreElements;
        }

        @Override
        CompletableFuture<E[]> getMoreElementsAsync() {
            try {
                assert !Thread.holdsLock(this);
                final Request request;
                final CompletableFuture<E[]> f;
                synchronized (this) {
                    if (future != null) {
                        return future;
                    }
                    if (!moreElements) {
                        return completedFuture(null);
                    }
                    request = createRequest();
                    f = new CompletableFuture<>();
                    future = f;
                }
                store.executeRequestAsync(request)
                    .whenComplete(
                        unwrapExceptionVoid(
                            (result, exception) -> {
                                synchronized (this) {
                                    future = null;
                                }
                                if (exception != null) {
                                    f.completeExceptionally(exception);
                                } else {
                                    handleResultCompleted(f, result);
                                }
                            }));
                return f;
            } catch (Throwable e) {
                return failedFuture(e);
            }
        }

        void handleResultCompleted(CompletableFuture<E[]> f, Result result) {
            try {
                assert !Thread.holdsLock(this);
                final E[] elements;
                synchronized (this) {
                    elements = processResult(result);
                }
                f.complete(elements);
            } catch (Throwable e) {
                f.completeExceptionally(e);
            }
        }

        abstract MultiTableOperation createOp();

        abstract E[] processResult(Result result);

        private Request createRequest() {
            final Request req = store.makeReadRequest(
                createOp(), partitionId, params.getConsistency(),
                params.getTimeout(), params.getTimeoutUnit());
            req.setLogContext(lc);
            req.setAuthContext(auth);
            req.setNoCharge(noCharge);
            return req;
        }
    }

    /**
     * A base class for implementing a ParallelScanIterator when it is a
     * single-partition iteration.  This one is sufficient to support an empty
     * iterator.
     *
     * This needs to implement TableIterator<E> but in this case the methods
     * specific to TableIterator<E> (actually ParallelScanIterator<E>) are
     * no-ops.  There are no relevant statistics in this path.
     */
    private static abstract class BasicMultiGetIteratorWrapper<E>
        implements AsyncTableIterator<E> {

        private final IterationHandleNotifier iterHandleNotifier;
        private E[] elements = null;
        private int nextElement = 0;
        private volatile boolean closed;
        private volatile Throwable closeException;

        BasicMultiGetIteratorWrapper(
            IterationHandleNotifier iterHandleNotifier) {
            this.iterHandleNotifier = iterHandleNotifier;
        }

        /**
         * Returns more elements or null if there are none.  May not return a
         * zero length array.
         */
        abstract E[] getMoreElements();

        /**
         * Returns whether there are more elements to be fetched.
         */
        abstract boolean hasMoreElements();

        /**
         * Returns more elements or null if there are none, returning the
         * results through the future. Will not return a zero length array.
         */
        abstract CompletableFuture<E[]> getMoreElementsAsync();

        synchronized void close(Throwable exception) {
            if (!closed) {
                closeException = exception;
                closed = true;
            }
        }

        /* -- From Iterator -- */

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() {
            if (elements != null && nextElement < elements.length) {
                return true;
            }

            elements = getMoreElements();

            if (elements == null) {
                return false;
            }

            assert (elements.length > 0);
            nextElement = 0;
            return true;
        }

        @Override
        public E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return elements[nextElement++];
        }

        /* -- From ParallelScanIterator -- */

        @Override
        public synchronized void close() {
            closed = true;
        }

        @Override
        public List<DetailedMetrics> getPartitionMetrics() {
            return Collections.emptyList();
        }

        @Override
        public List<DetailedMetrics> getShardMetrics() {
            return Collections.emptyList();
        }

        /* -- From AsyncTableIterator -- */

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Override
        public Throwable getCloseException() {
            return closeException;
        }

        @Override
        public E nextLocal() {
            assert !Thread.holdsLock(this);
            if (closed) {
                return null;
            }
            final E next;
            synchronized (this) {
                if ((elements != null) && (nextElement < elements.length)) {
                    next = elements[nextElement++];

                    /*
                     * There are still more elements, so OK to return without
                     * making a new request.
                     */
                    if (nextElement < elements.length) {
                        return next;
                    }
                } else {
                    next = null;
                }
            }
            getMoreElementsAsync()
                .whenComplete(
                    unwrapExceptionVoid(this::handleNewElementsResult))
                .whenComplete(
                    unwrapExceptionVoid(exception -> close(exception)));

            return next;
        }

        void handleNewElementsResult(E[] newElements, Throwable exception) {
            assert !Thread.holdsLock(this);
            synchronized (this) {
                if (exception != null) {
                    close(closeException);
                } else {
                    elements = newElements;
                    if (elements == null) {
                        closed = true;
                    }
                }
            }
            iterHandleNotifier.notifyNext();
        }
    }

    /**
     * A handler to fetch object associated with the partial key partition by
     * partition.
     */
    private static abstract
        class BasicPartitionMultiGetHandler<T extends Row> {

        final TableAPIImpl apiImpl;
        final KVStoreImpl store;
        final PartitionId[] partitionIds;
        final TableKey key;
        final boolean singlePartition;
        final byte[] continuationKey;
        final TargetTables targetTables;
        final StoreIteratorParams params;
        final int batchResultSize;
        final int maxReadKB;
        LogContext lc;
        AuthContext auth;
        private boolean noCharge = false;

        final List<T> rows = new ArrayList<T>();
        byte[] resumeKey = null;
        private PartitionId partition;
        private int numRead = 0;
        private int readKB = 0;
        private int writeKB = 0;
        private byte[] contdKey = null;

        BasicPartitionMultiGetHandler(TableAPIImpl apiImpl,
                                      TableKey key,
                                      byte[] continuationKey,
                                      MultiRowOptions getOptions,
                                      TableIteratorOptions iterateOptions) {
            this.apiImpl = apiImpl;
            store = apiImpl.getStore();

            Set<PartitionId> pids =
                store.getTopology().getPartitionMap().getAllIds();
            partitionIds = pids.toArray(new PartitionId[pids.size()]);
            this.key = key;
            singlePartition = key.getMajorKeyComplete();
            this.continuationKey = continuationKey;

            targetTables = TableAPIImpl.makeTargetTables(key.getTable(),
                                                         getOptions);

            params = new StoreIteratorParams
                    (Direction.FORWARD,
                     TableAPIImpl.getBatchSize(iterateOptions),
                     TableAPIImpl.getMaxReadKB(iterateOptions),
                     key.getKeyBytes(),
                     TableAPIImpl.makeKeyRange(key, getOptions),
                     Depth.PARENT_AND_DESCENDANTS,
                     TableAPIImpl.getConsistency(iterateOptions),
                     TableAPIImpl.getTimeout(iterateOptions),
                     TableAPIImpl.getTimeoutUnit(iterateOptions),
                     null,
                     !iterateOptions.getIncludeTombstones());

            batchResultSize = params.getBatchSize();
            maxReadKB = params.getMaxReadKB();
            if (iterateOptions != null) {
                lc = iterateOptions.getLogContext();
                auth = iterateOptions.getAuthContext();
                noCharge = iterateOptions.getNoCharge();
            }
        }

        /* Abstract method to create TableIterate operation */
        abstract InternalOperation createIterateOp(int emptyReadFactor);

        /* Abstract method to convert to the results */
        abstract void convertToResults(Result result);

        MultiGetResult<T> execute() {
            initIteration();
            while (true) {
                final Request request = createRequest();
                final Result result = store.executeRequest(request);
                if (processResult(result)) {
                    break;
                }
            }
            return createResult();
        }

        /**
         * Initializes the partition and resumeKey fields for the start of the
         * iteration.
         */
        private void initIteration() {
            if (continuationKey != null) {
                /* Extract partition id and resume key */
                int pid = PackedInteger.readInt(continuationKey, 0);
                if (pid < 1 || pid > partitionIds.length) {
                    throw new IllegalArgumentException("Invalid partition " +
                        "id in continuation key: " + pid);
                }
                partition = new PartitionId(pid);
                int idLen = PackedInteger.getReadIntLength(continuationKey, 0);
                if (continuationKey.length > idLen) {
                    resumeKey = Arrays.copyOfRange(continuationKey, idLen,
                                                   continuationKey.length);
                }
            } else {
                partition = singlePartition ?
                            getPartitionId(key) : getNextPartition(null);
            }
        }

        /**
         * Creates a request to get the next batch from the current partition
         * using the current resume key.
         */
        private Request createRequest() {
            final int emptyReadFactor =
                (singlePartition ||
                 (numRead == 0 &&
                  partition.getPartitionId() == partitionIds.length)) ? 1 : 0;
            final InternalOperation op = createIterateOp(emptyReadFactor);
            final Request req = store.makeReadRequest(
                op, partition, params.getConsistency(),
                params.getTimeout(), params.getTimeoutUnit());
            req.setLogContext(lc);
            req.setAuthContext(auth);
            req.setNoCharge(noCharge);
            return req;
        }

        /**
         * Process the results of a single server operation, updating fields
         * with the progress of the iteration.  Returns whether the iteration
         * is done.  Returns true if iteration is complete for now and the
         * result should be returned, and false if the iteration should
         * continue.
         */
        private boolean processResult(Result result) {
            numRead += result.getNumRecords();
            readKB += result.getReadKB();
            writeKB += result.getWriteKB();

            if (result.getNumRecords() > 0) {
                convertToResults(result);
                resumeKey = result.getPrimaryResumeKey();
            }

            /*
             * Stop fetching if there are still more elements to fetch from
             * the current partition.
             */
            if (result.hasMoreElements()) {
                contdKey = genContinuationKey(partition, resumeKey);
                return true;
            }

            /*
             * Move to next partition
             */

            if (singlePartition) {
                partition = null;
                return true;
            }

            partition = getNextPartition(partition);
            if (partition == null) {
                /* Stop if no more partition to scan */
                return true;
            }

            /*
             * If maxReadKB is specified, check the actual read cost and
             * stop fetching if current read cost has reached the maxReadKB,
             * the continuation key points to the beginning of the current
             * partition.
             */
            if (maxReadKB != 0) {
                if (readKB >= maxReadKB) {
                    contdKey = genContinuationKey(partition, null);
                    return true;
                }
                params.setMaxReadKB(maxReadKB - readKB);
            }

            /*
             * If batchResultSize is specified, check the total number of
             * rows read and stop fetching if it has reached the specified
             * batchResultSize,  the continuation key points to the
             * beginning of the current partition.
             */
            if (batchResultSize != 0) {
                if (numRead >= batchResultSize) {
                    contdKey = genContinuationKey(partition, null);
                    return true;
                }
                params.setBatchSize(batchResultSize - numRead);
            }

            if (resumeKey != null) {
                resumeKey = null;
            }

            return false;
        }

        /** Returns the result object that should be returned. */
        private MultiGetResult<T> createResult() {
            return new MultiGetResult<T>(rows, contdKey, readKB, writeKB);
        }

        /** Executes the iteration and returns the results asynchronously. */
        CompletableFuture<MultiGetResult<T>> executeAsync() {
            try {
                initIteration();
                final CompletableFuture<MultiGetResult<T>> future =
                    new CompletableFuture<>();
                class ExecuteAsyncHandler
                    implements BiConsumer<Result, Throwable> {

                    void execute() {
                        store.executeRequestAsync(createRequest())
                            .whenComplete(unwrapExceptionVoid(this));
                    }

                    @Override
                    public void accept(Result result, Throwable exception) {
                        try {
                            if (exception != null) {
                                future.completeExceptionally(exception);
                            } else if (processResult(result)) {
                                future.complete(createResult());
                            } else {

                                /* TODO: Use dialog layer executor */
                                CompletableFuture.runAsync(this::execute);
                            }
                        } catch (Throwable t) {
                            future.completeExceptionally(t);
                        }
                    }
                }
                new ExecuteAsyncHandler().execute();
                return future;
            } catch (Throwable t) {
                return failedFuture(t);
            }
        }

        /**
         * Generates the continuation key: <partition-id, resume-key-bytes>
         */
        private static byte[] genContinuationKey(PartitionId partitionId,
                                                 byte[] resumeKey) {

            int pid = partitionId.getPartitionId();
            int pidLen = PackedInteger.getWriteIntLength(pid);
            int len = pidLen + ((resumeKey != null) ? resumeKey.length : 0);

            final byte[] bytes = new byte[len];
            PackedInteger.writeInt(bytes, 0, pid);
            if (resumeKey != null) {
                System.arraycopy(resumeKey, 0, bytes, pidLen, resumeKey.length);
            }
            return bytes;
        }

        /**
         * Returns the PartitionId of the partition where the key located.
         */
        private PartitionId getPartitionId(TableKey tKey) {
            return store.getTopology().getPartitionId
                    (store.getKeySerializer().toByteArray(tKey.getKey()));
        }

        /**
         * Returns the next partition id of the specified partition, if the input
         * partition is null, then return the first partition id.
         */
        private PartitionId getNextPartition(PartitionId partitionId) {

            if (partitionId == null) {
                return partitionIds[0];
            }

            if (partitionId.getPartitionId() == partitionIds.length) {
                return null;
            }
            return partitionIds[partitionId.getPartitionId()];
        }
    }

    /*
     * A table iterator which has no elements.
     */
    private static class EmptyTableIterator<E>
        extends BasicMultiGetIteratorWrapper<E> {
        EmptyTableIterator(IterationHandleNotifier iterHandlerNotifier) {
            super(iterHandlerNotifier);
        }
        @Override
        E[] getMoreElements() {
            return null;
        }
        @Override
        boolean hasMoreElements() {
            return false;
        }
        @Override
        CompletableFuture<E[]> getMoreElementsAsync() {
            return completedFuture(null);
        }
    }
}
