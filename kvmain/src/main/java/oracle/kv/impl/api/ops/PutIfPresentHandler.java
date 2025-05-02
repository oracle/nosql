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

import oracle.kv.ReturnValueVersion.Choice;
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
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.Put;
import com.sleepycat.je.ReadOptions;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.WriteOptions;

/**
 * Server handler for {@link PutIfPresent}.
 *
 * Throughput calculation
 * +---------------------------------------------------------------------------+
 * |    Op         | Choice | # |          Read        |       Write           |
 * |---------------+--------+---+----------------------+-----------------------|
 * |               |        | P |        MIN_READ      |    new record size +  |
 * |               |        |   |                      |    old record size    |
 * |               |  NONE  |---+----------------------+-----------------------|
 * |               |        | A |        MIN_READ      |           0           |
 * |               +--------+---+----------------------+-----------------------|
 * |               |        | P |        MIN_READ      |    new record size +  |
 * |               |        |   |                      |    old record size    |
 * | PutIfPresent  | VERSION|---+----------------------+-----------------------|
 * |               |        | A |        MIN_READ      |           0           |
 * |               +--------+---+----------------------+-----------------------|
 * |               |        | P |    old record size   |    new record size +  |
 * |               |        |   |                      |    old record size    |
 * |               |  VALUE |---+----------------------+-----------------------|
 * |               |        | A |        MIN_READ      |           0           |
 * +---------------------------------------------------------------------------+
 *      # = Target record is present (P) or absent (A)
 */
class PutIfPresentHandler extends BasicPutHandler<PutIfPresent> {

    PutIfPresentHandler(OperationHandler handler) {
        super(handler, OpCode.PUT_IF_PRESENT, PutIfPresent.class);
    }

    @Override
    Result execute(PutIfPresent op, Transaction txn, PartitionId partitionId) {
        verifyDataAccess(op);

        ResultValueVersion prevVal = null;
        long expTime = 0L;
        long modificationTime = 0L;
        int storageSize = -1;
        Version version = null;
        boolean wasUpdate;

        byte[] keyBytes = op.getKeyBytes();
        byte[] valueBytes = op.getValueBytes();

        assert (keyBytes != null) && (valueBytes != null);

        final DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);
        DatabaseEntry dataEntry = valueDatabaseEntry(valueBytes);

        OperationResult opres;
        WriteOptions jeOptions = makeOption(op.getTTL(), op.getUpdateTTL());

        final Database db = getRepNode().getPartitionDB(partitionId);

        final Cursor cursor = db.openCursor(txn, CURSOR_DEFAULT);

        try {
            final Choice choice = op.getReturnValueVersionChoice();

            long tableId = op.getTableId();
            TableImpl table = null;
            boolean hasCRDT = false;
            if (tableId > 0) {
                table = operationHandler.
                    getAndCheckTable(op.getTableId());
                hasCRDT = getHasMRCounters(table);
            }

            /* For table rows with counter CRDTs, always need the
             * prev data. */
            final DatabaseEntry prevData =
                ((hasCRDT ||choice.needValue()) ? new DatabaseEntry() :
                    NO_DATA);
            /*
             * Ignore an existing tombstone since putIfPresent is only invoked
             * locally and should always win
             */
            final ReadOptions readOptions =
                InternalOperationHandler.RMW_EXCLUDE_TOMBSTONES;
            opres = cursor.get(keyEntry, prevData, Get.SEARCH,
                               readOptions);

            if (opres == null) {
                op.addReadBytes(MIN_READ);
                wasUpdate = false;

            } else {
                if (table != null && hasCRDT) {
                    /* Copy over CRDT values from the previous row. */
                    dataEntry = PutHandler.copyCRDTFromPrevRow(table, keyBytes,
                                                               valueBytes,
                                                               prevData,
                                                               getRepNode());
                }

                prevVal = getBeforeUpdateInfo(choice, cursor,
                                              operationHandler,
                                              prevData, opres);

                final int oldRecordSize = getStorageSize(cursor);
                op.addWriteBytes(oldRecordSize, 0, partitionId, -oldRecordSize);

                if (choice.needValue() || hasCRDT) {
                    op.addReadBytes(getStorageSize(cursor));
                } else {
                    op.addReadBytes(MIN_READ);
                }

                opres = putEntry(cursor, null, dataEntry,
                                 Put.CURRENT, jeOptions);

                expTime = opres.getExpirationTime();
                version = getVersion(cursor);
                wasUpdate = true;
                modificationTime = opres.getModificationTime();
                storageSize = getStorageSize(cursor);

                op.addWriteBytes(storageSize, getNIndexWrites(cursor),
                                 partitionId, storageSize);

                reserializeResultValue(op, prevVal);

                MigrationStreamHandle.get().addPut(keyEntry,
                                                   dataEntry,
                                                   version.getVLSN(),
                                                   modificationTime,
                                                   expTime,
                                                   false /*isTombstone*/);
            }

            return new Result.PutResult(getOpCode(),
                                        op.getReadKB(),
                                        op.getWriteKB(),
                                        prevVal,
                                        version,
                                        expTime,
                                        wasUpdate,
                                        modificationTime,
                                        storageSize,
                                        getRepNode().getRepNodeId().
                                        getGroupId());
        } finally {
            TxnUtil.close(cursor);
        }
    }
}
