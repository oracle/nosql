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
import java.util.logging.Level;

import com.sleepycat.je.Transaction;

import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.async.BytesUtil;
import oracle.kv.impl.topo.PartitionId;

/**
 * Server handler for {@link MultiGetTable}.
 */
class MultiGetTableHandler
        extends MultiGetTableOperationHandler<MultiGetTable> {

    MultiGetTableHandler(OperationHandler handler) {
        super(handler, OpCode.MULTI_GET_TABLE, MultiGetTable.class);
    }

    @Override
    Result execute(MultiGetTable op,
                   Transaction txn,
                   PartitionId partitionId) {

        verifyTableAccess(op);

        final List<ResultKeyValueVersion> results =
            new ArrayList<ResultKeyValueVersion>();

        final boolean moreElements =
            iterateTable(op, txn, partitionId, op.getParentKey(),
                         0, null,
                         new TableScanValueVisitor<MultiGetTable>(op, this,
                                                                  results));

        /*
         * For debugging problems with TableAPI.multiGet -- change level to
         * INFO or higher for debugging
         */
        getLogger().log(
            Level.FINEST,
            () ->
            "MultiGetTableHandler.execute" +
            " parentKey=" + BytesUtil.toString(op.getParentKey()) +
            " partitionId=" + partitionId +
            " resultCount=" + results.size() +
            " moreElements=" + moreElements);
        assert (!moreElements);
        return new Result.IterateResult(getOpCode(),
                                        op.getReadKB(), op.getWriteKB(),
                                        results, moreElements);
    }
}
