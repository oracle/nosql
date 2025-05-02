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

import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.topo.PartitionId;

/**
 * Server handler for {@link MultiGetTableKeys}.
 */
public class MultiGetTableKeysHandler
        extends MultiGetTableOperationHandler<MultiGetTableKeys> {

    MultiGetTableKeysHandler(OperationHandler handler) {
        super(handler, OpCode.MULTI_GET_TABLE_KEYS, MultiGetTableKeys.class);
    }

    @Override
    Result execute(MultiGetTableKeys op,
                   Transaction txn,
                   PartitionId partitionId) {

        verifyTableAccess(op);

        final List<ResultKey> results = new ArrayList<ResultKey>();

        final boolean moreElements =
            iterateTable(op, txn, partitionId, op.getParentKey(),
                         0, null,
                         new TableScanKeyVisitor<MultiGetTableKeys>(
                             op, this, results));

        assert (!moreElements);
        return new Result.KeysIterateResult(getOpCode(),
                                            op.getReadKB(), op.getWriteKB(),
                                            results, moreElements);
    }
}
