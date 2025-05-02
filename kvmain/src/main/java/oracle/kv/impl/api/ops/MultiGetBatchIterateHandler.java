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

import static oracle.kv.impl.api.ops.OperationHandler.CURSOR_DEFAULT;

import java.util.List;

import oracle.kv.Direction;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.OperationHandler.KVAuthorizer;
import oracle.kv.impl.topo.PartitionId;

import com.sleepycat.je.Transaction;

/**
 * Server handler for {@link MultiGetBatchIterate}.
 *
 * Throughput calculation
 * +---------------------------------------------------------------------------+
 * |    Op         | Choice | # |          Read        |       Write           |
 * |---------------+--------+---+----------------------+-----------------------|
 * | MultiGetBatch.|  N/A   | - | sum of record sizes  |           0           |
 * +---------------------------------------------------------------------------+
 */
class MultiGetBatchIterateHandler
    extends MultiGetBatchIterateOperationHandler<MultiGetBatchIterate,
                                                     ResultKeyValueVersion> {

    MultiGetBatchIterateHandler(OperationHandler handler) {
        super(handler, OpCode.MULTI_GET_BATCH, MultiGetBatchIterate.class);
    }

    @Override
    public boolean iterate(MultiGetBatchIterate op,
                           Transaction txn,
                           PartitionId partitionId,
                           byte[] parentKey,
                           int subBatchSize,
                           byte[] resumeSubKey,
                           List<ResultKeyValueVersion> results) {

        final KVAuthorizer kvAuth = checkPermission(parentKey);
        return iterate(op, txn, partitionId,
                       parentKey, true /*majorPathComplete*/,
                       op.getSubRange(), op.getDepth(), Direction.FORWARD,
                       subBatchSize, resumeSubKey, CURSOR_DEFAULT, results,
                       kvAuth, op.getExcludeTombstones());
    }

    @Override
    public Result createIterateResult(MultiGetBatchIterate op,
                                      List<ResultKeyValueVersion> results,
                                      boolean hasMore,
                                      int resumeParentKeyIndex) {

        return new Result.BulkGetIterateResult(getOpCode(),
                                               op.getReadKB(), op.getWriteKB(),
                                               results, hasMore,
                                               resumeParentKeyIndex);
    }
}
