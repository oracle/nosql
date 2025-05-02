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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import oracle.kv.impl.api.ops.Execute.OperationImpl;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.TxnUtil;

import com.sleepycat.je.Transaction;

/**
 * Server handler for {@link Execute}.
 *
 * The throughput calculation for execute is the sum of the read and
 * wright throughput of each of the operations. The individual Result objects
 * will contain the read/write data for that operation. The ExecuteResult
 * object will contain the total.
 */
class ExecuteHandler extends InternalOperationHandler<Execute> {

    ExecuteHandler(OperationHandler handler) {
        super(handler, OpCode.EXECUTE, Execute.class);
    }

    @Override
    Result execute(Execute op,
                   Transaction txn,
                   PartitionId partitionId) {

        /*
         * Sort operation indices by operation key, to avoid deadlocks when two
         * txns access records in a different order.
         */
        final List<OperationImpl> ops = op.getOperations();
        final int listSize = ops.size();
        final Integer[] sortedIndices = new Integer[listSize];
        for (int i = 0; i < listSize; i += 1) {
            sortedIndices[i] = i;
        }
        Arrays.sort(sortedIndices, new Comparator<Integer>() {
            @Override
            public int compare(Integer i1, Integer i2) {
                return KEY_BYTES_COMPARATOR.compare(
                    ops.get(i1).getInternalOp().getKeyBytes(),
                    ops.get(i2).getInternalOp().getKeyBytes());
            }
        });

        /* Initialize result list with nulls, so we can call List.set below. */
        final List<Result> results = new ArrayList<Result>(listSize);
        for (int i = 0; i < listSize; i += 1) {
            results.add(null);
        }

        int totalReadKB = 0;
        int totalWriteKB = 0;
        /* Process operations in key order. */
        for (final int i : sortedIndices) {

            final OperationImpl opImpl = ops.get(i);
            final SingleKeyOperation internalOp = opImpl.getInternalOp();
            final Result result =
                operationHandler.execute(internalOp, txn, partitionId);

            totalReadKB += result.getReadKB();
            totalWriteKB += result.getWriteKB();

            /* Abort if operation fails and user requests abort-on-failure. */
            if (opImpl.getAbortIfUnsuccessful() && !result.getSuccess()) {
                TxnUtil.abort(txn);
                return new Result.ExecuteResult(getOpCode(),
                                                totalReadKB,
                                                totalWriteKB,
                                                i, result);
            }
            results.set(i, result);
        }

        /* All operations succeeded, or failed without causing an abort. */
        return new Result.ExecuteResult(getOpCode(),
                                        totalReadKB, totalWriteKB,
                                        results);
    }

    @Override
    List<? extends KVStorePrivilege> getRequiredPrivileges(Execute op) {
        /*
         * Check all privileges required by all single operations in list. The
         * execute operation will be performed if and only if all the
         * privileges are met.
         */
        final Set<KVStorePrivilege> privSet = new HashSet<KVStorePrivilege>();
        for (final OperationImpl opImpl : op.getOperations()) {
            privSet.addAll(
                operationHandler.getRequiredPrivileges(
                    opImpl.getInternalOp()));
        }
        return Collections.unmodifiableList(
            new ArrayList<KVStorePrivilege>(privSet));
    }
}
