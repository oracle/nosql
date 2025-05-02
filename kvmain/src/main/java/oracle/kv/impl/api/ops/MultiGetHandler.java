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
import java.util.Collections;
import java.util.List;

import oracle.kv.Direction;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.OperationHandler.KVAuthorizer;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.NamespacePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.topo.PartitionId;

import com.sleepycat.je.Transaction;

/**
 * Server handler for {@link MultiGet}.
 *
 * Throughput calculation
 * +---------------------------------------------------------------------------+
 * |    Op         | Choice | # |          Read        |       Write           |
 * |---------------+--------+---+----------------------+-----------------------|
 * | MultiGet      |  N/A   | - | sum of record sizes  |           0           |
 * +---------------------------------------------------------------------------+
 */
class MultiGetHandler extends MultiKeyOperationHandler<MultiGet> {

    MultiGetHandler(OperationHandler handler) {
        super(handler, OpCode.MULTI_GET, MultiGet.class);
    }

    /**
     * The multi-get operation holds read locks (does not use read-committed
     * isolation) because it is defined to provide repeatable-read isolation.
     */
    @Override
    public boolean getReadCommitted() {
        return false;
    }

    @Override
    Result execute(MultiGet op,
                   Transaction txn,
                   PartitionId partitionId) {

        final KVAuthorizer kvAuth = checkPermission(op);

        final List<ResultKeyValueVersion> results =
            new ArrayList<ResultKeyValueVersion>();

        final boolean moreElements = iterate(op,
            txn, partitionId, op.getParentKey(), true /*majorPathComplete*/,
            op.getSubRange(), op.getDepth(), Direction.FORWARD, 0 /*batchSize*/,
             null /*resumeKey*/, CURSOR_DEFAULT, results, kvAuth,
             op.getExcludeTombstones());

        assert (!moreElements);

        return new Result.IterateResult(getOpCode(),
                                        op.getReadKB(), op.getWriteKB(),
                                        results, moreElements);
    }

    @Override
    List<? extends KVStorePrivilege> schemaAccessPrivileges() {
        return SystemPrivilege.schemaReadPrivList;
    }

    @Override
    List<? extends KVStorePrivilege> generalAccessPrivileges() {
        return SystemPrivilege.readOnlyPrivList;
    }

    @Override
    public List<? extends KVStorePrivilege>
        tableAccessPrivileges(long tableId) {
        return tableReadPrivileges(tableId);
    }

    @Override
    public List<? extends KVStorePrivilege>
    namespaceAccessPrivileges(String namespace) {
        return Collections.singletonList(
            new NamespacePrivilege.ReadInNamespace(namespace));
    }
}
