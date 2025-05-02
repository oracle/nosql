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

import static com.sleepycat.je.Get.SEARCH;
import static oracle.kv.impl.api.ops.OperationHandler.CURSOR_DEFAULT;

import java.util.Collections;
import java.util.List;

import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.NamespacePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.TxnUtil;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.ReadOptions;
import com.sleepycat.je.Transaction;

/**
 * Server handler for {@link Get}.
 *
 * Throughput calculation
 * +---------------------------------------------------------------------------+
 * |    Op         | Choice | # |          Read        |       Write           |
 * |---------------+--------+---+----------------------+-----------------------|
 * |               |        | P |      record size     |           0           |
 * | Get           |   N/A  |---+----------------------+-----------------------|
 * |               |        | A |        MIN_READ      |           0           |
 * +---------------------------------------------------------------------------+
 *      # = Target record is present (P) or absent (A)
 */
class GetHandler extends SingleKeyOperationHandler<Get> {
    GetHandler(OperationHandler operationHandler) {
        super(operationHandler, OpCode.GET, Get.class);
    }

    @Override
    Result execute(Get op, Transaction txn, PartitionId partitionId) {

        verifyDataAccess(op);

        final ResultValueVersion resultValueVersion =
            get(op, txn, partitionId, op.getKeyBytes());

        reserializeResultValue(op, resultValueVersion);
        return new Result.GetResult(getOpCode(),
                                    op.getReadKB(), op.getWriteKB(),
                                    resultValueVersion);
    }

    /**
     * Gets the value associated with the key.
     */
    private ResultValueVersion get(Get op, Transaction txn,
                                   PartitionId partitionId,
                                   byte[] keyBytes) {

        assert (keyBytes != null);

        final Database db = getRepNode().getPartitionDB(partitionId);
        final DatabaseEntry dataEntry = new DatabaseEntry();
        final DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);
        final Cursor cursor = db.openCursor(txn, CURSOR_DEFAULT);
        try {
            ReadOptions options;
            if (op.getTableId() != 0 || op.getExcludeTombstones()) {
                /*
                 * Filter out tombstones for table api but return them for
                 * kvstore api for internal and administrative use, if
                 * requested.
                 */
                options = InternalOperationHandler.DEFAULT_EXCLUDE_TOMBSTONES;
            } else {
                options = LockMode.DEFAULT.toReadOptions();
            }
            final OperationResult result =
                cursor.get(keyEntry, dataEntry,
                           SEARCH, options);
            if (result == null) {
                op.addReadBytes(MIN_READ);
                return null;
            }
            op.addReadBytes(getStorageSize(cursor));
            return makeValueVersion(cursor, dataEntry, result);

        } finally {
            TxnUtil.close(cursor);
        }
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
