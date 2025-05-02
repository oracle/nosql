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

import java.util.List;

import com.sleepycat.je.Transaction;

import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.topo.PartitionId;

/**
 * Server handler for {@link MultiGetBatchTableKeys}.
 */
class MultiGetBatchTableKeysHandler
    extends MultiGetBatchTableOperationHandler<MultiGetBatchTableKeys,
                                                   ResultKey> {

    MultiGetBatchTableKeysHandler(OperationHandler handler) {
        super(handler, OpCode.MULTI_GET_BATCH_TABLE_KEYS,
              MultiGetBatchTableKeys.class);
    }

    @Override
    public boolean iterate(MultiGetBatchTableKeys op,
                           Transaction txn,
                           PartitionId partitionId,
                           byte[] parentKey,
                           int subBatchSize,
                           byte[] resumeSubKey,
                           final List<ResultKey> results) {

        return iterateTable(op, txn, partitionId, parentKey, subBatchSize,
                            resumeSubKey,
                            new TableScanKeyVisitor<MultiGetBatchTableKeys>(
                                op, this, results));
    }

    @Override
    public Result createIterateResult(MultiGetBatchTableKeys op,
                                      List<ResultKey> results,
                                      boolean hasMore,
                                      int resumeParentKeyIndex) {

        return new Result.BulkGetKeysIterateResult(getOpCode(),
                                                   op.getReadKB(),
                                                   op.getWriteKB(),
                                                   results, hasMore,
                                                   resumeParentKeyIndex);
    }
}
