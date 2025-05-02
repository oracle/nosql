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

import java.util.Collections;
import java.util.List;

import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.KeyRange;
import oracle.kv.impl.api.lob.KVLargeObjectImpl;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.OperationHandler.KVAuthorizer;
import oracle.kv.impl.fault.WrappedClientException;
import oracle.kv.impl.rep.migration.MigrationStreamHandle;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.NamespacePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.TablePrivilege;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.UserDataControl;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.Transaction;

/**
 * Server handler for {@link MultiDelete}.
 *
 * Throughput calculation
 * +---------------------------------------------------------------------------+
 * |    Op         | Choice | # |          Read        |       Write           |
 * |---------------+--------+---+----------------------+-----------------------|
 * | MultiDelete   |   N/A  |N/A|   MIN_READ * number  | sum of deleted record |
 * |               |        |   |  of records scanned  |        sizes          |
 * |               |        |   |          (1)         |                       |
 * +---------------------------------------------------------------------------+
 * (1) Records which are not visible to the caller are not counted.
 */
class MultiDeleteHandler extends MultiKeyOperationHandler<MultiDelete> {

    MultiDeleteHandler(OperationHandler handler) {
        super(handler, OpCode.MULTI_DELETE, MultiDelete.class);
    }

    @Override
    Result execute(MultiDelete op,
                   Transaction txn,
                   PartitionId partitionId) {

        final KVAuthorizer kvAuth = checkPermission(op);

        final int result = multiDelete(
            op, txn, partitionId, op.getParentKey(), op.getSubRange(),
            op.getDepth(), op.getLobSuffixBytes(), kvAuth);

        return new Result.MultiDeleteResult(getOpCode(),
                                            op.getReadKB(), op.getWriteKB(),
                                            result);
    }

    /**
     * Deletes the keys in a multi-key scan.
     */
    private int multiDelete(MultiDelete op, Transaction txn,
                            PartitionId partitionId,
                            byte[] parentKey,
                            KeyRange subRange,
                            Depth depth,
                            final byte[] lobSuffixBytes,
                            final KVAuthorizer auth) {

        int nDeletions = 0;

        Scanner scanner = new Scanner(op, txn,
                                      partitionId,
                                      getRepNode(),
                                      parentKey,
                                      true, // majorPathComplete
                                      subRange,
                                      depth,
                                      Direction.FORWARD,
                                      null, // resumeKey
                                      true, /*moveAfterResumeKey*/
                                      CURSOR_DEFAULT,
                                      LockMode.RMW,
                                      true); /* key-only */

        DatabaseEntry keyEntry = scanner.getKey();
        Cursor cursor = scanner.getCursor();
        boolean moreElements;
        try {
            while ((moreElements = scanner.next()) == true) {
                if (!auth.allowAccess(keyEntry)) {

                    /*
                     * The requestor is not permitted to see this entry,
                     * so silently skip it.
                     */
                    continue;
                }

                if (KVLargeObjectImpl.hasLOBSuffix(keyEntry.getData(),
                                                    lobSuffixBytes)) {
                    final String msg =
                        "Operation: multiDelete" +
                        " Illegal LOB key argument: " +
                        UserDataControl.displayKey(keyEntry.getData()) +
                        ". Use LOB-specific APIs to modify a " +
                        "LOB key/value pair.";
                    throw new WrappedClientException
                        (new IllegalArgumentException(msg));
                }

                final int recordSize = getStorageSize(cursor);
                final OperationResult result = cursor.delete(null);
                op.addWriteBytes(recordSize, getNIndexWrites(cursor),
                                 partitionId, -recordSize);
                assert (result != null);
                MigrationStreamHandle.get().addDelete(keyEntry, cursor);
                nDeletions++;
            }
        } finally {
            scanner.close();
        }
        assert(!moreElements);
        return nDeletions;
    }

    @Override
    List<? extends KVStorePrivilege> schemaAccessPrivileges() {
        return SystemPrivilege.schemaWritePrivList;
    }

    @Override
    List<? extends KVStorePrivilege> generalAccessPrivileges() {
        return SystemPrivilege.writeOnlyPrivList;
    }

    @Override
    public List<? extends KVStorePrivilege>
        tableAccessPrivileges(long tableId) {
        return Collections.singletonList(
            new TablePrivilege.DeleteTable(tableId));
    }

    @Override
    public List<? extends KVStorePrivilege>
    namespaceAccessPrivileges(String namespace) {
        return Collections.singletonList(
            new NamespacePrivilege.DeleteInNamespace(namespace));
    }
}
