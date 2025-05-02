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

import oracle.kv.Value;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.Result.PutResult;
import oracle.kv.impl.api.table.Region;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.rep.migration.MigrationStreamHandle;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.TxnUtil;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Get;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.Transaction;

/**
 * Server handler for {@link Delete}.
 *
 * Throughput calculation
 * +---------------------------------------------------------------------------+
 * |    Op         | Choice | # |          Read        |       Write           |
 * |---------------+--------+---+----------------------+-----------------------|
 * |               |        | P |      MIN_READ x 2    |  deleted record size  |
 * |               |  NONE  |---+----------------------+-----------------------|
 * |               |        | A |      MIN_READ x 2    |           0           |
 * |               +--------+---+----------------------+-----------------------|
 * |               |        | P |      MIN_READ x 2    |  deleted record size  |
 * | Delete        | VERSION|---+----------------------+-----------------------|
 * |               |        | A |      MIN_READ x 2    |           0           |
 * |               +--------+---+----------------------+-----------------------|
 * |               |        | P |    deleted record    |  deleted record size  |
 * |               |        |   |      size x 2        |                       |
 * |               |  VALUE |---+----------------------+-----------------------|
 * |               |        | A |      MIN_READ x 2    |           0           |
 * +---------------------------------------------------------------------------+
 *
 *  * Throughput calculation for multi-region tables
 * +---------------------------------------------------------------------------+
 * |    Op         | Choice | # |          Read        |       Write           |
 * |---------------+--------+---+----------------------+-----------------------|
 * |               |        | P |           0          |    tombstone size +   |
 * |               |        |   |                      |    old record size    |
 * |               |  NONE  |---+----------------------+-----------------------|
 * |               |        | A |           0          |    tombstone size     |
 * |               +--------+---+----------------------+-----------------------|
 * |               |        | P |        MIN_READ      |    tombstone size +   |
 * |               |        |   |                      |    old record size    |
 * | Put           | VERSION|---+----------------------+-----------------------|
 * |               |        | A |           0          |    tombstone size     |
 * |               +--------+---+----------------------+-----------------------|
 * |               |        | P |    old record size   |    tombstone size +   |
 * |               |        |   |                      |    old record size    |
 * |               |  VALUE |---+----------------------+-----------------------|
 * |               |        | A |           0          |    tombstone size     |
 * +---------------------------------------------------------------------------+
 *      # = Target record is present (P) or absent (A)
 */
public class DeleteHandler extends BasicDeleteHandler<Delete> {

    public DeleteHandler(OperationHandler handler) {
        super(handler, OpCode.DELETE, Delete.class);
    }

    @Override
    public Result execute(Delete op, Transaction txn, PartitionId partitionId) {
        verifyDataAccess(op);

        final ReturnResultValueVersion prevVal =
            new ReturnResultValueVersion(op.getReturnValueVersionChoice());

        return delete(op, txn, partitionId, op.getKeyBytes(), prevVal);
    }

    private Result insertTombstone(Delete op,
                                   Transaction txn,
                                   PartitionId partitionId,
                                   ReturnResultValueVersion prevVal,
                                   boolean isMultiRegion) {
        Value value = isMultiRegion ?
                Value.createTombstoneValue(Region.LOCAL_REGION_ID) :
                Value.createTombstoneNoneValue();

        Put put = new Put(op.getKeyBytes(), value,
                          op.getReturnValueVersionChoice(),
                          op.getTableId(),
                          getRepNode().getRepNodeParams().getTombstoneTTL(),
                          true /* updateTTL */,
                          false /* isSQLUpdate */);
        /*
         * Delete is converted to put tombstone, continue to track throughput.
         */
        put.setResourceTracker(op);
        put.addReadKB(op.getReadKB());
        put.addWriteKB(op.getWriteKB());

        final PutResult putResult = PutHandler.put(put, txn, partitionId,
                                                   getRepNode(),
                                                   operationHandler, OpCode.PUT,
                                                   true);
        byte[] returnValueBytes = putResult.getPreviousValueBytes();
        prevVal.setValueVersion(
            returnValueBytes,
            putResult.getPreviousVersion(),
            putResult.getPreviousExpirationTime(),
            putResult.getPreviousModificationTime(),
            putResult.getPreviousStorageSize());
        reserializeResultValue(op, prevVal.getValueVersion());
        return new Result.DeleteResult(getOpCode(),
                                       put.getReadKB(),
                                       put.getWriteKB(),
                                       prevVal.getValueVersion(),
                                       putResult.getWasUpdate());
    }

    /**
     * Delete the key/value pair associated with the key.
     */
    private Result delete(Delete op,
                          Transaction txn,
                          PartitionId partitionId,
                          byte[] keyBytes,
                          ReturnResultValueVersion prevValue) {

        assert (keyBytes != null);

        final Database db = getRepNode().getPartitionDB(partitionId);
        final DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);

        /*
         * To return previous value/version, we must first position on the
         * existing record and then delete it.
         */
        final Cursor cursor = db.openCursor(txn, CURSOR_DEFAULT);
        try {
            final DatabaseEntry prevData =
                prevValue.getReturnChoice().needValue() ?
                new DatabaseEntry() :
                NO_DATA;

            final OperationResult result =
                cursor.get(keyEntry, prevData,
                           Get.SEARCH,
                           InternalOperationHandler.RMW_EXCLUDE_TOMBSTONES);

            boolean exist;
            if (result == null) {
                op.addReadBytes(MIN_READ);
                exist = false;
            } else {
                final TableImpl tbl = getAndCheckTable(op.getTableId());
                if (tbl != null && (tbl.isMultiRegion() || op.doTombstone())) {
                    /*
                     * It's a multi-region table, so insert tombstone instead of
                     * deleting.
                     */
                    return insertTombstone(op, txn, partitionId, prevValue,
                                           tbl.isMultiRegion());
                }

                final int recordSize = getStorageSize(cursor);

                if (prevValue.getReturnChoice().needValueOrVersion()) {
                    getPrevValueVersion(cursor, prevData, prevValue, result);
                    if (prevValue.getReturnChoice().needValue()) {
                        op.addReadBytes(recordSize);
                    } else {
                        op.addReadBytes(MIN_READ);
                    }
                } else {
                    op.addReadBytes(MIN_READ);
                }

                cursor.delete(null);
                op.addWriteBytes(recordSize, getNIndexWrites(cursor),
                                 partitionId, -recordSize);
                MigrationStreamHandle.get().addDelete(keyEntry, cursor);
                exist = true;
            }
            reserializeResultValue(op, prevValue.getValueVersion());
            return new Result.DeleteResult(getOpCode(),
                                           op.getReadKB(),
                                           op.getWriteKB(),
                                           prevValue.getValueVersion(),
                                           exist);
        } finally {
            TxnUtil.close(cursor);
        }
    }
}
