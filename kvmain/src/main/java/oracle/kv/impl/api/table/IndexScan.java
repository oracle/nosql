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

import static oracle.kv.impl.async.FutureUtils.failedFuture;
import static oracle.kv.impl.async.FutureUtils.unwrapExceptionVoid;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.Key;
import oracle.kv.ValueVersion;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.ops.IndexIterate;
import oracle.kv.impl.api.ops.IndexKeysIterate;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.ops.ResultIndexKeys;
import oracle.kv.impl.api.ops.ResultIndexRows;
import oracle.kv.impl.api.parallelscan.ShardScanIterator;
import oracle.kv.impl.async.AsyncTableIterator;
import oracle.kv.impl.async.IterationHandleNotifier;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.util.contextlogger.LogContext;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.table.KeyPair;
import oracle.kv.table.MultiGetResult;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.Row;
import oracle.kv.table.TableIteratorOptions;

/**
 * Implementation of a scatter-gather iterator for secondary indexes. The
 * iterator will access the store by shards.
 * {@code ShardIndexStream} will use to read a single shard.
 * <p>
 * Discussion of inclusive/exclusive iterations
 * <p>
 * Each request sent to the server side needs a start or resume key and an
 * optional end key. By default these are inclusive.  A {@code FieldRange}
 * object may be included to exercise fine control over start/end values for
 * range queries.  {@code FieldRange} indicates whether the values are inclusive
 * or exclusive.  {@code FieldValue} objects are typed so the
 * inclusive/exclusive state is handled here (on the client side) where they
 * can be controlled per-type rather than on the server where they are simple
 * {@code byte[]}. This means that the start/end/resume keys are always
 * inclusive on the server side.
 */
public class IndexScan {

    static final Comparator<byte[]> KEY_BYTES_COMPARATOR =
        new Key.BytesComparator();

    /* Prevent construction */
    private IndexScan() {}

    /**
     * Creates a table iterator returning ordered rows.
     *
     * @return a table iterator
     */
    static AsyncTableIterator<Row> createTableIterator(
        final TableAPIImpl tableAPI,
        final IndexKeyImpl indexKey,
        final MultiRowOptions mro,
        final TableIteratorOptions tio,
        final IterationHandleNotifier iterHandleNotifier) {

       return createTableIterator(tableAPI, indexKey, mro, tio, null,
                                  iterHandleNotifier);
    }

    static AsyncTableIterator<Row> createTableIterator(
        final TableAPIImpl tableAPI,
        final IndexKeyImpl indexKey,
        final MultiRowOptions mro,
        final TableIteratorOptions tio,
        final Set<RepGroupId> shardSet,
        final IterationHandleNotifier iterHandleNotifier) {

        final TargetTables targetTables =
            TableAPIImpl.makeTargetTables(indexKey.getTable(), mro);

        final IndexImpl index = (IndexImpl) indexKey.getIndex();
        final TableImpl table = index.getTable();
        final IndexRange range = new IndexRange(indexKey, mro, tio);

        final boolean needDupElim = needDupElimination(indexKey);

        ExecuteOptions options = new ExecuteOptions(tio);

        Direction dir = (tio != null ? tio.getDirection() : Direction.FORWARD);

        return new ShardScanIterator<Row>(tableAPI.getStore(),
                                          null, // baseTopology
                                          options,
                                          dir,
                                          shardSet,
                                          iterHandleNotifier,
                                          true /*isTopologyListner*/) {

            @Override
            protected ShardStream createStream(RepGroupId groupId) {
                return new IndexRowScanStream(groupId);
            }

            @Override
            protected InternalOperation createOp(byte[] resumeSecondaryKey,
                                                 byte[] resumePrimaryKey) {

                return new IndexIterate(index.getName(),
                                        targetTables,
                                        range,
                                        resumeSecondaryKey,
                                        resumePrimaryKey,
                                        batchSize,
                                        0 /* maxReadKB */,
                                        1 /* emptyReadFactor */);
            }

            @Override
            protected void convertResult(Result result, List<Row> rows) {
                convertResultRows(tableAPI, table, targetTables, result, rows);
            }

            @Override
            protected byte[] extractResumeSecondaryKey(Result result) {
                return result.getSecondaryResumeKey();
            }

            @Override
            protected int compare(Row one, Row two) {
                throw new IllegalStateException("Unexpected call");
            }

            /**
             * IndexRowScanStream subclasses ShardIndexStream in order to
             * implement correct ordering of the streams used by an
             * IndexRowScanIterator. Specifically, the problem is that
             * IndexRowScanIterator returns Row objs, and as a result
             * IndexRowScanIterator.compare(), which compares Rows, does not
             * do correct ordering. Instead we must compare index keys. If
             * two index keys (from different shards) are equal, then the
             * associated primary keys are also compared, to make sure that 2
             * streams will never have the same order magnitude (the only way
             * that 2 streams may both return the same index-key, primary-key
             * pair is when both streams retrieve the same row from multiple
             * shards in the event of partition migration.
             */
            class IndexRowScanStream extends ShardStream {

                HashSet<BinaryValueImpl> thePrimKeysSet;

                IndexRowScanStream(RepGroupId groupId) {
                    super(groupId, null, null);
                    if (needDupElim) {
                        thePrimKeysSet = new HashSet<BinaryValueImpl>(1000);
                    }
                }

                @Override
                protected void setResumeKey(Result result) {

                    super.setResumeKey(result);

                    if (!needDupElim) {
                        return;
                    }

                    ListIterator<ResultIndexRows> listIter =
                        result.getIndexRowList().listIterator();

                    while (listIter.hasNext()) {
                        ResultIndexRows indexRow = listIter.next();

                        BinaryValueImpl binPrimKey =
                            FieldDefImpl.Constants.binaryDef.
                            createBinary(indexRow.getKeyBytes());

                        boolean added = thePrimKeysSet.add(binPrimKey);

                        if (!added) {
                            listIter.remove();
                        }
                    }
                }

                @Override
                protected int compareInternal(Stream o) {

                    IndexRowScanStream other = (IndexRowScanStream)o;

                    ResultIndexRows res1 =
                        currentResultSet.getIndexRowList().
                        get(currentResultPos);

                    ResultIndexRows res2 =
                        other.currentResultSet.getIndexRowList().
                        get(other.currentResultPos);

                    byte[] key1 = res1.getIndexKeyBytes();
                    byte[] key2 = res2.getIndexKeyBytes();

                    int cmp = IndexImpl.compareUnsignedBytes(key1, key2);

                    if (cmp == 0) {
                        cmp = KEY_BYTES_COMPARATOR.compare(res1.getKeyBytes(),
                                                           res2.getKeyBytes());
                    }

                    return itrDirection == Direction.FORWARD ? cmp : (cmp * -1);
                }
            }
        };
    }

    /**
     * Check whether elimination of duplicate table rows is needed. This is
     * true only if the index is multikey. For example, let "array" be a table
     * column that is an array of ints, and we are searching for rows whose
     * "array" contains a value > 10. Since the index contains an entry for
     * each value of "array", and a given row may contain many values  > 10
     * in its "array"Even then, no elimination is needed
     * in the following case:
     *
     * Let R be the set of index entries that satisfy the search conditions.
     * If all entries in R have the same index key (not including the prim
     * key columns), then there cannot be 2 entries in R that contain the same
     * prim key (i.e. point to the same table row). This is because at the JE
     * level, the index key includes both the declared index fileds and the prim
     * key columns, and these "physical" keys must be uniqye.
     *
     * The above case can arise in 2 situations:
     * - All the multi-key fields have equality conditions on them.
     * - The index is a MapBoth index and there is an equality condition on the
     *   map-key field.
     */
    private static boolean needDupElimination(IndexKeyImpl key) {

        IndexImpl index = (IndexImpl)key.getIndex();

        if (!index.isMultiKey() || key.isComplete()) {
            return false;
        }

        if (key.size() == 0) {
            return true;
        }

        List<IndexImpl.IndexField> ipaths = index.getIndexFields();

        /*
         * If the index is a MapBoth one, and the map-key field is set in the
         * index key, no dup elim is needed.
         */
        if (index.isMapBothIndex()) {

            for (int i = 0; i < key.size(); ++i) {
                if (ipaths.get(i).isMapKeys()) {
                    return false;
                }
            }
        }

        /*
         * If any of the index fields that are not set in the index key are
         * multi-key fields, dup elim is needed.
         */
        for (int i = key.size(); i < index.numFields(); ++i) {
            if (ipaths.get(i).isMultiKey()) {
                return true;
            }
        }

        return false;
    }

    /**
     * Creates a table iterator returning ordered key pairs.
     *
     * @return a table iterator
     */
    static AsyncTableIterator<KeyPair> createTableKeysIterator(
        final TableAPIImpl apiImpl,
        final IndexKeyImpl indexKey,
        final MultiRowOptions mro,
        final TableIteratorOptions tio,
        final IterationHandleNotifier iterHandleNotifier) {

        final TargetTables targetTables =
            TableAPIImpl.makeTargetTables(indexKey.getTable(), mro);
        final IndexImpl index = (IndexImpl) indexKey.getIndex();
        final IndexRange range = new IndexRange(indexKey, mro, tio);

        ExecuteOptions options = new ExecuteOptions(tio);

        Direction dir = (tio != null ? tio.getDirection() : Direction.FORWARD);

        return new ShardScanIterator<KeyPair>(apiImpl.getStore(),
                                              null, // baseTopology
                                              options,
                                              dir,
                                              null,
                                              iterHandleNotifier,
                                              true /*isTopologyListner*/) {
            @Override
            protected ShardStream createStream(RepGroupId groupId) {
                return new IndexKeyScanStream(groupId);
            }

            @Override
            protected InternalOperation createOp(byte[] resumeSecondaryKey,
                                                 byte[] resumePrimaryKey) {
                return new IndexKeysIterate(index.getName(),
                                            targetTables,
                                            range,
                                            resumeSecondaryKey,
                                            resumePrimaryKey,
                                            batchSize,
                                            0, /* maxReadKB */
                                            1 /* emptyReadFactor */);
            }

            /**
             * Convert the results to KeyPair instances.  Note that in the
             * case where ancestor and/or child table returns are requested
             * the IndexKey returned is based on the the index and the table
             * containing the index, but the PrimaryKey returned may be from
             * a different, ancestor or child table.
             */
            @Override
            protected void convertResult(Result result,
                                         List<KeyPair> elementList) {
                convertResultKeyPairs(index, targetTables, result, elementList);
            }

            /**
             * IndexKeyScanStream exists so that the index and primary keys
             * from resulting KeyPair instances can be compared in binary
             * format for sorting. With the addition of JSON support it is
             * possible for the value types in the IndexKey to be different from
             * one row to the next and the FieldValue instances will not compare
             * across types.
             *
             * This comparison ensures the same sort order as in the
             * database.
             */
            class IndexKeyScanStream extends ShardStream {

                IndexKeyScanStream(RepGroupId groupId) {
                    super(groupId, null, null);
                }

                @Override
                protected int compareInternal(Stream o) {

                    IndexKeyScanStream other = (IndexKeyScanStream)o;

                    ResultIndexKeys res1 =
                        currentResultSet.getIndexKeyList().
                        get(currentResultPos);

                    ResultIndexKeys res2 =
                        other.currentResultSet.getIndexKeyList().
                        get(other.currentResultPos);

                    byte[] key1 = res1.getIndexKeyBytes();
                    byte[] key2 = res2.getIndexKeyBytes();

                    int cmp = IndexImpl.compareUnsignedBytes(key1, key2);

                    if (cmp == 0) {
                        cmp = KEY_BYTES_COMPARATOR.compare(
                            res1.getPrimaryKeyBytes(),
                            res2.getPrimaryKeyBytes());
                    }

                    return itrDirection == Direction.FORWARD ? cmp : (cmp * -1);
                }
            }

            @Override
            protected int compare(KeyPair one, KeyPair two) {
                throw new IllegalStateException("Unexpected call");
            }
        };
    }

    static MultiGetResult<Row> multiGet(TableAPIImpl apiImpl,
                                        IndexKeyImpl indexKey,
                                        byte[] continuationKey,
                                        MultiRowOptions mro,
                                        TableIteratorOptions tio) {
        return new ShardMultiGetHandler(apiImpl, indexKey, continuationKey,
                                        mro, tio)
            .execute();
    }

    static CompletableFuture<MultiGetResult<Row>>
        multiGetAsync(TableAPIImpl apiImpl,
                      IndexKeyImpl indexKey,
                      byte[] continuationKey,
                      MultiRowOptions mro,
                      TableIteratorOptions tio) {
        try {
            return new ShardMultiGetHandler(apiImpl, indexKey,
                                            continuationKey, mro, tio)
                .executeAsync();
        } catch (Throwable e) {
            return failedFuture(e);
        }
    }

    private static class ShardMultiGetHandler
            extends BasicShardMultiGetHandler<Row> {
        ShardMultiGetHandler(TableAPIImpl apiImpl,
                             IndexKeyImpl indexKey,
                             byte[] continuationKey,
                             MultiRowOptions mro,
                             TableIteratorOptions tio) {
            super(apiImpl, indexKey, continuationKey, mro, tio);
        }

        @Override
        InternalOperation createIterateOp(int batchSize,
                                          int readKBLimit,
                                          int emptyReadFactor) {
            return new IndexIterate(index.getName(),
                                    targetTables,
                                    range,
                                    resumeSecondaryKey,
                                    resumePrimaryKey,
                                    batchSize,
                                    readKBLimit,
                                    emptyReadFactor);
        }

        @Override
        void convertResult(Result result) {
            convertResultRows(apiImpl, table, targetTables, result, rows);
        }
    }

    static MultiGetResult<KeyPair> multiGetKeys(TableAPIImpl apiImpl,
                                                IndexKeyImpl indexKey,
                                                byte[] continuationKey,
                                                MultiRowOptions mro,
                                                TableIteratorOptions tio) {
        return new ShardMultiGetKeysHandler(apiImpl, indexKey, continuationKey,
                                            mro, tio)
            .execute();
    }

    static CompletableFuture<MultiGetResult<KeyPair>>
        multiGetKeysAsync(TableAPIImpl apiImpl,
                          IndexKeyImpl indexKey,
                          byte[] continuationKey,
                          MultiRowOptions mro,
                          TableIteratorOptions tio) {
        try {
            return new ShardMultiGetKeysHandler(apiImpl, indexKey,
                                                continuationKey, mro, tio)
                .executeAsync();
        } catch (Throwable e) {
            return failedFuture(e);
        }
    }

    private static class ShardMultiGetKeysHandler
            extends BasicShardMultiGetHandler<KeyPair> {
        ShardMultiGetKeysHandler(TableAPIImpl apiImpl,
                                 IndexKeyImpl indexKey,
                                 byte[] continuationKey,
                                 MultiRowOptions mro,
                                 TableIteratorOptions tio) {
            super(apiImpl, indexKey, continuationKey, mro, tio);
        }

        @Override
        InternalOperation createIterateOp(int batchSize,
                                          int readKBLimit,
                                          int emptyReadFactor) {
            return new IndexKeysIterate(index.getName(),
                                        targetTables,
                                        range,
                                        resumeSecondaryKey,
                                        resumePrimaryKey,
                                        batchSize,
                                        readKBLimit,
                                        emptyReadFactor);
        }

        @Override
        void convertResult(Result result) {
            convertResultKeyPairs(index, targetTables, result, rows);
        }
    }

    private static void convertResultRows(TableAPIImpl apiImpl,
                                          TableImpl table,
                                          TargetTables targetTables,
                                          Result result,
                                          List<Row> rows) {

        final List<ResultIndexRows> indexRowList = result.getIndexRowList();
        for (ResultIndexRows indexRow : indexRowList) {
            Row converted = convertRow(apiImpl, targetTables, table, indexRow);
            rows.add(converted);
        }
    }

    /**
     * Converts a single key value into a row.
     */
    private static Row convertRow(TableAPIImpl apiImpl,
                                  TargetTables targetTables,
                                  TableImpl table,
                                  ResultIndexRows rowResult) {
        /*
         * If ancestor table returns may be involved, start at the
         * top level table of this hierarchy.
         */
        final TableImpl startingTable =
            targetTables.hasAncestorTables() ?
            table.getTopLevelTable() : table;

        final RowImpl fullKey = startingTable.createRowFromKeyBytes(
            rowResult.getKeyBytes());

        if (fullKey == null) {
            throw new IllegalStateException
                ("Unable to deserialize a row from an index result");
        }

        final ValueVersion vv =
            new ValueVersion(rowResult.getValue(),
                             rowResult.getVersion());

        RowImpl row =
            apiImpl.getRowFromValueVersion(
                vv,
                fullKey,
                rowResult.getExpirationTime(),
                rowResult.getModificationTime(),
                false,
                false);
        return row;
    }

    private static void convertResultKeyPairs(IndexImpl index,
                                              TargetTables targetTables,
                                              Result result,
                                              List<KeyPair> keyPairs) {

        final TableImpl table = index.getTable();

        final List<ResultIndexKeys> results =
            result.getIndexKeyList();

        for (ResultIndexKeys res : results) {

            final IndexKeyImpl indexKeyImpl =
                convertIndexKey(index, res.getIndexKeyBytes());

            final PrimaryKeyImpl pkey = convertPrimaryKey(table, targetTables,
                                                          res);

            if (indexKeyImpl != null && pkey != null) {
                keyPairs.add(new KeyPair(pkey, indexKeyImpl));
            } else {
                keyPairs.add(null);
            }
        }
    }

    private static IndexKeyImpl convertIndexKey(IndexImpl index, byte[] bytes) {
        /* don't allow partial keys */
        return index.deserializeIndexKey(bytes, false);
    }

    private static PrimaryKeyImpl convertPrimaryKey(TableImpl table,
                                                    TargetTables targetTables,
                                                    ResultIndexKeys res) {
        /*
         * If ancestor table returns may be involved, start at the
         * top level table of this hierarchy.
         */
        final TableImpl startingTable =
            targetTables.hasAncestorTables() ?
            table.getTopLevelTable() : table;
        final PrimaryKeyImpl pkey = startingTable.
            createPrimaryKeyFromKeyBytes(res.getPrimaryKeyBytes());
        pkey.setExpirationTime(res.getExpirationTime());
        return pkey;
    }

    /**
     * A handler to fetch matching rows shard by shard.
     */
    private static abstract class BasicShardMultiGetHandler<T> {

        final TableAPIImpl apiImpl;
        final KVStoreImpl store;
        final RepGroupId[] repGroupIds;
        final IndexImpl index;
        final TableImpl table;
        final byte[] continuationKey;
        final TargetTables targetTables;
        final IndexRange range;

        final Consistency consistency;
        final long requestTimeout;
        final TimeUnit timeoutUnit;
        final int batchResultSize;
        final int maxReadKB;
        private LogContext lc;
        private AuthContext auth;
        private int opBatchSize;
        private int opMaxReadKB;

        final List<T> rows = new ArrayList<T>();
        byte[] resumeSecondaryKey = null;
        byte[] resumePrimaryKey = null;
        private RepGroupId groupId;
        private int numRead = 0;
        private int readKB = 0;
        private int writeKB = 0;
        private byte[] contdKey = null;
        private boolean noCharge = false;

        BasicShardMultiGetHandler(TableAPIImpl apiImpl,
                                  IndexKeyImpl key,
                                  byte[] continuationKey,
                                  MultiRowOptions mro,
                                  TableIteratorOptions tio) {
            this.apiImpl = apiImpl;
            store = apiImpl.getStore();

            Set<RepGroupId> rgids = store.getTopology().getRepGroupIds();
            repGroupIds = rgids.toArray(new RepGroupId[rgids.size()]);

            index = key.getIndexImpl();
            table = key.getTable();
            this.continuationKey = continuationKey;

            targetTables = TableAPIImpl.makeTargetTables(key.getTable(), mro);
            range = new IndexRange(key, mro, tio);

            consistency = TableAPIImpl.getConsistency(tio);
            requestTimeout = TableAPIImpl.getTimeout(tio);
            timeoutUnit = TableAPIImpl.getTimeoutUnit(tio);

            batchResultSize = TableAPIImpl.getBatchSize(tio);
            maxReadKB = TableAPIImpl.getMaxReadKB(tio);

            opBatchSize = batchResultSize;
            opMaxReadKB = maxReadKB;
            if (tio != null) {
                lc = tio.getLogContext();
                auth = tio.getAuthContext();
                noCharge = tio.getNoCharge();
            }
        }

        /* Abstract method to create IndexIterate operation */
        abstract InternalOperation createIterateOp(int batchSize,
                                                   int readKBLimit,
                                                   int emptyReadFactor);

        /* Abstract method to convert to the results */
        abstract void convertResult(Result result);

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
         * Initializes the groupId, resumeSecondaryKey, and resumePrimaryKey
         * fields for the start of the iteration.
         */
        private void initIteration() {
            if (continuationKey != null && continuationKey.length > 0) {
                /*
                 * Extract the shard id, resume secondary key and resume
                 * primary key.
                 */
                int pos = 0;
                int gid = continuationKey[pos++];
                if (gid < 1 || gid > repGroupIds.length) {
                    throw new IllegalArgumentException("Invalid shard id " +
                        "in continuation key: " + gid);
                }
                groupId = new RepGroupId(gid);
                if (continuationKey.length > 1) {
                    int len = continuationKey[pos++];
                    if (len > 0) {
                        resumeSecondaryKey = Arrays.copyOfRange(
                            continuationKey, pos, pos + len);
                        pos += len;
                        assert(pos < continuationKey.length);
                        len = continuationKey[pos++];
                        resumePrimaryKey = Arrays.copyOfRange(
                            continuationKey, pos, pos + len);
                    }
                }
            } else {
                groupId = getNextRepGroup(null);
            }
        }

        /**
         * Creates a request to get the next batch of results from the current
         * shard and resume keys.
         */
        private Request createRequest() {
            final int emptyReadFactor = (readKB == 0 &&
                groupId.getGroupId() == repGroupIds.length) ? 1 : 0;
            final InternalOperation op = createIterateOp(opBatchSize,
                                                         opMaxReadKB,
                                                         emptyReadFactor);
            final Request req =
                store.makeReadRequest(op, groupId, consistency,
                                      requestTimeout, timeoutUnit);

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
                convertResult(result);
                resumeSecondaryKey = result.getSecondaryResumeKey();
                resumePrimaryKey = result.getPrimaryResumeKey();
            }

            /*
             * Stop fetching if still has more element to fetch from current
             * shard.
             */
            if (result.hasMoreElements()) {
                contdKey = genContinuationKey(groupId, resumeSecondaryKey,
                                              resumePrimaryKey);
                return true;
            }

            /*
             * Move to next shard
             */

            groupId = getNextRepGroup(groupId);
            if (groupId == null) {
                return true;
            }

            /*
             * If maxReadKB is specified, check the actual read cost and
             * stop fetching if current read cost has reached the maxReadKB,
             * the continuation key points to the beginning of the current
             * RepGroup.
             */
            if (maxReadKB != 0) {
                if (readKB >= maxReadKB) {
                    contdKey = genContinuationKey(groupId, null, null);
                    return true;
                }
                opMaxReadKB = maxReadKB - readKB;
            }

            /*
             * If batchResultSize is specified, check on the number of rows
             * fetched and stop fetching if the number of rows has reached
             * the batchResultSize, the continuation key points to the
             * beginning of the current RepGroup.
             */
            if (batchResultSize != 0) {
                if (numRead >= batchResultSize) {
                    contdKey = genContinuationKey(groupId, null, null);
                    return true;
                }
                opBatchSize = batchResultSize - numRead;
            }

            if (resumeSecondaryKey != null) {
                resumeSecondaryKey = null;
                resumePrimaryKey = null;
            }

            return false;
        }

        /** Returns the result object that should be returned. */
        private MultiGetResult<T> createResult() {
            return new MultiGetResult<T>(rows, contdKey, readKB, writeKB);
        }

        /** Executes the iteration and returns the result asynchronously. */
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
         * Generates the continuation key:
         *  <repGroupid, resumeSecondaryKey, resumePrimaryKey>
         */
        private static byte[] genContinuationKey(RepGroupId repGroupId,
                                                 byte[] resumeSecondKey,
                                                 byte[] resumePrimaryKey) {

            int len = (resumeSecondKey != null ?
                        resumeSecondKey.length + 1 : 0) +
                      (resumePrimaryKey != null ?
                          resumePrimaryKey.length + 1 : 0) + 1;
            final byte[] bytes = new byte[len];
            int pos = 0;

            /* repGroupId */
            bytes[pos++] = (byte)repGroupId.getGroupId();

            if (resumeSecondKey != null) {
                /* resumeSecondaryKey */
                bytes[pos++] = (byte)resumeSecondKey.length;
                System.arraycopy(resumeSecondKey, 0, bytes, pos,
                                 resumeSecondKey.length);
                pos += resumeSecondKey.length;

                /* resumePrimaryKey */
                if (resumePrimaryKey != null) {
                    bytes[pos++] = (byte)resumePrimaryKey.length;
                    System.arraycopy(resumePrimaryKey, 0, bytes, pos,
                                     resumePrimaryKey.length);
                }
            }
            return bytes;
        }

        /**
         * Returns the next partition id of the specified partition, if the
         * input RepGroupId is null, then return the first RepGroupId.
         */
        private RepGroupId getNextRepGroup(RepGroupId repGroupId) {

            if (repGroupId == null) {
                return repGroupIds[0];
            }

            if (repGroupId.getGroupId() == repGroupIds.length) {
                return null;
            }
            return repGroupIds[repGroupId.getGroupId()];
        }
    }
}
