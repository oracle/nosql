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

import com.sleepycat.je.Transaction;

import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.MultiGetBatchExecutor.MultiGetBatchHandler;
import oracle.kv.impl.topo.PartitionId;

/**
 * Base server handler for subclasses of MultiGetBatchTableOperation.
 */
abstract class MultiGetBatchTableOperationHandler
    <T extends MultiGetBatchTableOperation, V>
        extends MultiGetTableOperationHandler<T>
        implements MultiGetBatchHandler<T, V> {

    MultiGetBatchTableOperationHandler(OperationHandler handler,
                                       OpCode opCode,
                                       Class<T> operationType) {
        super(handler, opCode, operationType);
    }

    @Override
    Result execute(T op,
                   Transaction txn,
                   PartitionId partitionId) {

        verifyTableAccess(op);

        final MultiGetBatchExecutor<T, V> executor =
            new MultiGetBatchExecutor<T, V>(this);
        return executor.execute(op, txn, partitionId, op.getParentKeys(),
                                op.getResumeKey(), op.getBatchSize());
    }
}
