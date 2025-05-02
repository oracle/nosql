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

import java.util.ArrayList;
import java.util.List;

import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.OperationHandler.KVAuthorizer;
import oracle.kv.impl.topo.PartitionId;

import com.sleepycat.je.Transaction;

/**
 * Server handler for {@link MultiGetIterate}.
 *
 * Throughput calculation
 * +---------------------------------------------------------------------------+
 * |    Op         | Choice | # |          Read        |       Write           |
 * |---------------+--------+---+----------------------+-----------------------|
 * | MultiGetIter..|  N/A   | - | sum of record sizes  |           0           |
 * +---------------------------------------------------------------------------+
 */
class MultiGetIterateHandler extends MultiKeyIterateHandler<MultiGetIterate> {

    MultiGetIterateHandler(OperationHandler handler) {
        super(handler, OpCode.MULTI_GET_ITERATE, MultiGetIterate.class);
    }

    @Override
    Result execute(MultiGetIterate op,
                   Transaction txn,
                   PartitionId partitionId) {

        final KVAuthorizer kvAuth = checkPermission(op);

        final List<ResultKeyValueVersion> results =
            new ArrayList<ResultKeyValueVersion>();

        final boolean moreElements = iterate(op,
            txn, partitionId, op.getParentKey(), true /*majorPathComplete*/,
            op.getSubRange(), op.getDepth(), op.getDirection(),
            op.getBatchSize(), op.getResumeKey(), CURSOR_DEFAULT,
            results, kvAuth, op.getExcludeTombstones());

        return new Result.IterateResult(getOpCode(),
                                        op.getReadKB(), op.getWriteKB(),
                                        results, moreElements);
    }
}
