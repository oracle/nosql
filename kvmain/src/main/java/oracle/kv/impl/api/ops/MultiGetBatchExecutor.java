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

package oracle.kv.impl.api.ops;

import java.util.ArrayList;
import java.util.List;

import com.sleepycat.je.Transaction;

import oracle.kv.impl.topo.PartitionId;

/**
 * A class encapsulates the execute function for multi-get-batch operation.
 *
 * @param <T> the type of the associated operation
 * @param <V> the result type
 */
class MultiGetBatchExecutor<T extends InternalOperation, V> {

    final MultiGetBatchHandler<T, V> handler;

    MultiGetBatchExecutor(MultiGetBatchHandler<T, V> handler) {
        this.handler = handler;
    }

    public Result execute(T op,
                          Transaction txn,
                          PartitionId partitionId,
                          List<byte[]> keys,
                          byte[] resumeKey,
                          int batchSize) {

        final List<V> results = new ArrayList<V>();
        int index = 0;
        boolean hasMore = false;
        byte[] resumeSubKey = resumeKey;
        final List<V> subResults = new ArrayList<V>();
        for (; index < keys.size(); index++) {
            final byte[] parentKey = keys.get(index);
            final int subBatchSize = (batchSize > results.size()) ?
                                        batchSize - results.size() : 1;
            final boolean moreElements =
                handler.iterate(op, txn, partitionId, parentKey, subBatchSize,
                                resumeSubKey, subResults);

            results.addAll(subResults);

            if (moreElements) {
                hasMore = true;
                break;
            }
            if (resumeSubKey != null) {
                resumeSubKey = null;
            }

            subResults.clear();
        }
        /*
         * If the total number of fetched records exceeds the batchSize, then
         * get batch operation will stop fetching and return the number of
         * keys retrieved.
         */
        final int resumeParentKeyIndex = (hasMore ? index : -1);
        return handler.createIterateResult(op, results, hasMore,
                                           resumeParentKeyIndex);
    }

    /**
     * The interface to be implemented by multi-get-batch operations.
     */
    static interface MultiGetBatchHandler<T, V> {

        /* Iterate values and return the next batch. */
        boolean iterate(T op,
                        Transaction txn,
                        PartitionId partitionId,
                        byte[] parentKey,
                        int subBatchSize,
                        byte[] resumeSubKey,
                        List<V> results);

        /* Create bulk get iterate result. */
        Result createIterateResult(T op,
                                   List<V> results,
                                   boolean hasMore,
                                   int resumeParentKeyIndex);
    }
}
