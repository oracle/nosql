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

import oracle.kv.Version;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.rep.migration.MigrationStreamHandle;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.TxnUtil;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.ReadOptions;
import com.sleepycat.je.Transaction;

/**
 * Server handler for {@link DeleteIfVersion}.
 *
 * Throughput calculation
 * +---------------------------------------------------------------------------+
 * |    Op         | Choice | # |          Read        |       Write            |
 * |---------------+--------+---+----------------------+----------------------- |
 * |               |        | Y |      MIN_READ x 2    |  deleted record size + |
 * |               |        |   |                      |  (added tombstone size)|
 * |               |  NONE  |---+----------------------+----------------------- |
 * |               |        | N |      MIN_READ x 2    |           0            |
 * |               +--------+---+----------------------+----------------------- |
 * |               |        | Y |      MIN_READ x 2    |  deleted record size + |
 * |               |        |   |                      |  (added tombstone size)|
 * | DeleteIfVersio| VERSION|---+----------------------+----------------------- |
 * |               |        | N |      MIN_READ x 2    |           0            |
 * |               +--------+---+----------------------+----------------------- |
 * |               |        | Y |      MIN_READ x 2    |  deleted record size + |
 * |               |        |   |                      |  (added tombstone size)|
 * |               |  VALUE |---+----------------------+----------------------- |
 * |               |   (1)  |   |      MIN_READ x 2    |                        |
 * |               |        | N |----------------------|           0            |
 * |               |        |   |    record size x 2   |                        |
 * +---------------------------------------------------------------------------+
 *      # = Target record matches (Y) or not (N)
 *     (1) If a record is present but the version does not matched the read
 *         charge is for the returned record
 */
class DeleteIfVersionHandler extends BasicDeleteHandler<DeleteIfVersion> {

    DeleteIfVersionHandler(OperationHandler handler) {
        super(handler, OpCode.DELETE_IF_VERSION, DeleteIfVersion.class);
    }

    @Override
    Result execute(DeleteIfVersion op,
                   Transaction txn,
                   PartitionId partitionId) {

        verifyDataAccess(op);

        final ReturnResultValueVersion prevVal =
            new ReturnResultValueVersion(op.getReturnValueVersionChoice());

        final boolean result = deleteIfVersion(op,
            txn, partitionId, op.getKeyBytes(), op.getMatchVersion(), prevVal);

        reserializeResultValue(op, prevVal.getValueVersion());
        return new Result.DeleteResult(getOpCode(),
                                       op.getReadKB(), op.getWriteKB(),
                                       prevVal.getValueVersion(),
                                       result);
    }

    /**
     * Delete a key/value pair, if the existing record has the given version.
     */
    private boolean deleteIfVersion(DeleteIfVersion op, Transaction txn,
                                    PartitionId partitionId,
                                    byte[] keyBytes,
                                    Version matchVersion,
                                    ReturnResultValueVersion prevValue) {

        assert (keyBytes != null) && (matchVersion != null);

        final Database db = getRepNode().getPartitionDB(partitionId);
        final DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);
        final Cursor cursor = db.openCursor(txn, CURSOR_DEFAULT);
        try {
            /*
             * Ignore an existing tombstone since users are not supposed
             * to have the version for a tombstone.
             */
            ReadOptions readOptions =
                InternalOperationHandler.RMW_EXCLUDE_TOMBSTONES;
            OperationResult result =
                cursor.get(keyEntry, NO_DATA,
                           Get.SEARCH, readOptions);
            if (result == null) {
                op.addReadBytes(MIN_READ);
                /* Not present, return false. */
                return false;
            }
            final int oldRecordSize = getStorageSize(cursor);
            if (versionMatches(cursor, matchVersion)) {
                /* Version matches, delete and return true. */
                final TableImpl tbl = getAndCheckTable(op.getTableId());
                if (tbl != null && (tbl.isMultiRegion() || op.doTombstone())) {
                    /* insert tombstone for multi-region table. */
                    MultiDeleteTableHandler.insertTombstone(cursor,
                        operationHandler, keyEntry, getTombstoneTTL(),
                        op, oldRecordSize, partitionId,
                        tbl.isMultiRegion());
                } else {
                    cursor.delete(null);
                    MigrationStreamHandle.get().addDelete(keyEntry, cursor);
                    op.addReadBytes(MIN_READ);
                    op.addWriteBytes(oldRecordSize, getNIndexWrites(cursor),
                                     partitionId, -oldRecordSize);
                }
                return true;
            }
            /* No match, get previous value/version and return false. */
            final DatabaseEntry prevData;
            if (prevValue.getReturnChoice().needValue()) {
                prevData = new DatabaseEntry();
                result = cursor.get(keyEntry, prevData,
                                    Get.CURRENT, LockMode.RMW.toReadOptions());
                op.addReadBytes(oldRecordSize);
            } else {
                prevData = NO_DATA;
                op.addReadBytes(MIN_READ);
            }
            getPrevValueVersion(cursor, prevData, prevValue, result);
            return false;
        } finally {
            TxnUtil.close(cursor);
        }
    }
}
